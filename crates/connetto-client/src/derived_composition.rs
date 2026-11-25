use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl, SqliteConnection};
use diesel_sqlite_session::{ApplyFlags, ConflictAction, SqliteSessionExt};
use sqlite_diff_rs::{
    ChangeDelete, ChangeSet, ChangesetOp, DiffOps, Indirect, Insert, ParsedDiffSet, SchemaWithPK,
    TableSchema, Update, Value,
};

use crate::{
    ClientError, HashSet, derived::_connetto_derived, invert_changeset, replay, shared_route,
};

type Capture = ChangeSet<TableSchema<String>, String, Vec<u8>>;
type ParsedCapture =
    sqlite_diff_rs::DiffSet<sqlite_diff_rs::ChangesetFormat, TableSchema<String>, String, Vec<u8>>;
type Operation<'operation> = ChangesetOp<'operation, TableSchema<String>, String, Vec<u8>>;

pub(crate) fn invert(
    database: &mut SqliteConnection,
    shared: &HashSet<String>,
    position: i64,
    changeset: &[u8],
) -> Result<(), ClientError> {
    let protected = match next_capture(database, shared, position, true)? {
        Some(first) => protect_later(database, shared, changeset, first)?,
        None => None,
    };
    let changeset = protected.as_deref().unwrap_or(changeset);
    if changeset.is_empty() {
        return Ok(());
    }
    database
        .apply_changeset_with(
            changeset,
            ApplyFlags::INVERT,
            |_| true,
            |_| ConflictAction::Omit,
        )
        .map_err(apply_error)?;
    if !shared.is_empty() {
        let inverse = invert_changeset(changeset).map_err(apply_error)?;
        replay::apply(
            database,
            &inverse,
            replay::Policy::Omit,
            &shared_route(shared),
        )?;
    }
    Ok(())
}

fn protect_later(
    database: &mut SqliteConnection,
    shared: &HashSet<String>,
    changeset: &[u8],
    first: Candidate,
) -> Result<Option<Vec<u8>>, ClientError> {
    let parsed = parse_capture(changeset)?;
    let mut remaining: Vec<_> = parsed
        .iter()
        .map(|operation| match operation {
            ChangesetOp::Update { values, .. } => Remaining::Columns(
                values
                    .iter()
                    .map(|(old, new)| old.is_some() && new.is_some())
                    .collect(),
            ),
            _ => Remaining::Row(true),
        })
        .collect();
    let initial: usize = remaining.iter().map(Remaining::count).sum();
    let mut count = initial;
    let mut next_candidate = Some(first);
    while let Some(held) = next_candidate {
        let candidate = &held;
        if candidate.changeset.is_empty() {
            next_candidate = next_capture(database, shared, candidate.position, candidate.marked)?;
            continue;
        }
        let existing = if candidate.marked || candidate.writer == crate::derived::UNSENT_WRITER {
            None
        } else {
            crate::derived::load_local_changeset(database, &candidate.writer, candidate.sequence)?
        };
        let bytes = existing.as_deref().unwrap_or(&candidate.changeset);
        let next = if bytes.is_empty() {
            None
        } else {
            Some(parse_capture(bytes)?)
        };
        let uploaded_operations = existing
            .as_ref()
            .map(|_| parse_capture(&candidate.changeset))
            .transpose()?;
        let mut prefix = Capture::new();
        for (operation, remaining) in parsed.iter().zip(&mut remaining) {
            if remaining.count() > 0
                && let Some(later) = next
                    .as_ref()
                    .and_then(|next| next.iter().find(|later| same_row(&operation, later)))
                    .or_else(|| {
                        uploaded_operations.as_ref().and_then(|prefix| {
                            prefix.iter().find(|later| same_row(&operation, later))
                        })
                    })
            {
                let reserved;
                (prefix, reserved) = transfer(prefix, &operation, &later, remaining)?;
                count -= reserved;
            }
        }
        if !prefix.is_empty() {
            persist_prefix(database, candidate, &prefix.build(), existing.as_deref())?;
        }
        if count == 0 {
            break;
        }
        let position = candidate.position;
        let marked = candidate.marked;
        next_candidate = next_capture(database, shared, position, marked)?;
    }
    if count == initial {
        return Ok(None);
    }
    let mut unprotected = Capture::new();
    for (operation, remaining) in parsed.iter().zip(remaining) {
        match remaining {
            Remaining::Row(true) => {
                unprotected = append_selected(unprotected, &operation, |_| true)?;
            }
            Remaining::Columns(columns) => {
                unprotected = append_selected(unprotected, &operation, |column| columns[column])?;
            }
            Remaining::Row(false) => {}
        }
    }
    Ok(Some(unprotected.build()))
}
fn persist_prefix(
    database: &mut SqliteConnection,
    candidate: &Candidate,
    prefix: &[u8],
    existing: Option<&[u8]>,
) -> Result<(), ClientError> {
    if candidate.writer == crate::derived::UNSENT_WRITER {
        return crate::derived::prepend_unsent(database, candidate.marked, prefix);
    }
    if candidate.marked {
        let combined = diesel_sqlite_session::concat_changesets(prefix, &candidate.changeset)
            .map_err(apply_error)?;
        diesel::update(
            _connetto_derived::table
                .filter(_connetto_derived::writer.eq(&candidate.writer))
                .filter(_connetto_derived::seq.eq(candidate.sequence)),
        )
        .set(_connetto_derived::changeset.eq(combined))
        .execute(database)?;
    } else {
        let local = existing.unwrap_or(&candidate.changeset);
        let combined =
            diesel_sqlite_session::concat_changesets(prefix, local).map_err(apply_error)?;
        crate::derived::store_local_changeset(
            database,
            &candidate.writer,
            candidate.sequence,
            &combined,
            &candidate.changeset,
        )?;
    }
    Ok(())
}

enum Remaining {
    Row(bool),
    Columns(Vec<bool>),
}

impl Remaining {
    fn count(&self) -> usize {
        match self {
            Self::Row(held) => usize::from(*held),
            Self::Columns(columns) => columns.iter().filter(|held| **held).count(),
        }
    }
}

fn transfer(
    builder: Capture,
    operation: &Operation<'_>,
    later: &Operation<'_>,
    remaining: &mut Remaining,
) -> Result<(Capture, usize), ClientError> {
    match remaining {
        Remaining::Row(held) => {
            let reserved = usize::from(core::mem::replace(held, false));
            let continues = match operation {
                ChangesetOp::Insert { values, .. } => {
                    !matches!(later, ChangesetOp::Insert { .. })
                        && values.iter().enumerate().all(|(column, value)| {
                            old_value(later, column).is_none_or(|old| old == value)
                        })
                }
                ChangesetOp::Delete { .. } => matches!(later, ChangesetOp::Insert { .. }),
                ChangesetOp::Update { .. } => false,
            };
            Ok((
                if continues {
                    append_selected(builder, operation, |_| true)?
                } else {
                    builder
                },
                reserved,
            ))
        }
        Remaining::Columns(columns) => {
            let mut reserved = 0;
            let builder = append_selected(builder, operation, |column| {
                let touched = match later {
                    ChangesetOp::Update { values, .. } => values
                        .get(column)
                        .is_some_and(|(old, new)| old.is_some() && new.is_some()),
                    _ => true,
                };
                if !columns[column] || !touched {
                    return false;
                }
                columns[column] = false;
                reserved += 1;
                let ChangesetOp::Update { values, .. } = operation else {
                    return false;
                };
                old_value(later, column) == values[column].1.as_ref()
            })?;
            Ok((builder, reserved))
        }
    }
}

#[derive(diesel::QueryableByName)]
struct Candidate {
    #[diesel(sql_type = diesel::sql_types::Text)]
    writer: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    sequence: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    position: i64,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    changeset: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    marked: bool,
}

fn next_capture(
    database: &mut SqliteConnection,
    shared: &HashSet<String>,
    position: i64,
    marked: bool,
) -> Result<Option<Candidate>, ClientError> {
    let queued = diesel::sql_query(
        "SELECT writer, seq AS sequence, position, changeset, marked FROM (
             SELECT writer, seq, position, changeset, 0 AS marked FROM _connetto_pending
             UNION ALL
             SELECT writer, seq, position, changeset, 1 AS marked FROM _connetto_derived
         ) WHERE position > ? OR (position = ? AND marked > ?)
         ORDER BY position, marked LIMIT 1",
    )
    .bind::<diesel::sql_types::BigInt, _>(position)
    .bind::<diesel::sql_types::BigInt, _>(position)
    .bind::<diesel::sql_types::Bool, _>(marked)
    .get_result(database)
    .optional()?;
    if queued.is_some() || (position == i64::MAX && marked) {
        return Ok(queued);
    }
    let Some(built) = crate::outbox::build(database, shared)? else {
        return Ok(None);
    };
    let marked = position == i64::MAX;
    Ok(Some(Candidate {
        writer: crate::derived::UNSENT_WRITER.to_owned(),
        sequence: i64::from(marked),
        position: i64::MAX,
        changeset: if marked {
            built.marked
        } else {
            built.local_changeset.unwrap_or(built.changeset)
        },
        marked,
    }))
}

fn parse_capture(changeset: &[u8]) -> Result<ParsedCapture, ClientError> {
    match ParsedDiffSet::parse(changeset).map_err(apply_error)? {
        ParsedDiffSet::Changeset(parsed) => Ok(parsed),
        ParsedDiffSet::Patchset(_) => Err(ClientError::Apply(
            "a derived capture is not a changeset".to_owned(),
        )),
    }
}

fn apply_error(error: impl core::fmt::Display) -> ClientError {
    ClientError::Apply(error.to_string())
}

fn key_value<'operation>(
    operation: &'operation Operation<'_>,
    column: usize,
) -> Option<&'operation Value<String, Vec<u8>>> {
    match operation {
        ChangesetOp::Insert { values, .. } => values.get(column),
        ChangesetOp::Delete { old_values, .. } => old_values.get(column),
        ChangesetOp::Update { values, .. } => values
            .get(column)
            .and_then(|(old, new)| old.as_ref().or(new.as_ref())),
    }
}

fn same_row(left: &Operation<'_>, right: &Operation<'_>) -> bool {
    left.table() == right.table()
        && left
            .table()
            .primary_key_columns()
            .all(|column| key_value(left, column) == key_value(right, column))
}

fn old_value<'operation>(
    operation: &'operation Operation<'_>,
    column: usize,
) -> Option<&'operation Value<String, Vec<u8>>> {
    match operation {
        ChangesetOp::Insert { .. } => None,
        ChangesetOp::Delete { old_values, .. } => old_values.get(column),
        ChangesetOp::Update { values, .. } => values.get(column).and_then(|(old, _)| old.as_ref()),
    }
}

fn append_selected(
    builder: Capture,
    operation: &Operation<'_>,
    mut selected: impl FnMut(usize) -> bool,
) -> Result<Capture, ClientError> {
    Ok(match operation {
        ChangesetOp::Insert {
            table,
            values,
            indirect,
        } => {
            let mut insert = Insert::from((*table).clone()).indirect(*indirect);
            for (column, value) in values.iter().enumerate() {
                insert = insert.set(column, value.clone()).map_err(apply_error)?;
            }
            builder.insert(insert)
        }
        ChangesetOp::Delete {
            table,
            old_values,
            indirect,
        } => {
            let mut delete = ChangeDelete::from((*table).clone()).indirect(*indirect);
            for (column, value) in old_values.iter().enumerate() {
                delete = delete.set(column, value.clone()).map_err(apply_error)?;
            }
            builder.delete(delete)
        }
        ChangesetOp::Update {
            table,
            values,
            indirect,
        } => {
            let mut update = Update::<_, sqlite_diff_rs::ChangesetFormat, String, Vec<u8>>::from(
                (*table).clone(),
            )
            .indirect(*indirect);
            let mut changed = false;
            for (column, (old, new)) in values.iter().enumerate() {
                if table.primary_key_columns().any(|key| key == column) {
                    if let Some(old) = old {
                        update = update.set_old(column, old.clone()).map_err(apply_error)?;
                    }
                } else if let (Some(old), Some(new)) = (old, new)
                    && selected(column)
                {
                    update = update
                        .set(column, old.clone(), new.clone())
                        .map_err(apply_error)?;
                    changed = true;
                }
            }
            if changed {
                builder.update(update)
            } else {
                builder
            }
        }
    })
}
