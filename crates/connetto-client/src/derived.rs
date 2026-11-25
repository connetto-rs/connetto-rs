use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl, SqliteConnection};

use crate::{
    _connetto_pending, ClientError, HashSet, invert_changeset, replay, rollback_omit, shared_route,
};
use diesel_sqlite_session::SqliteSessionExt;

diesel::table! {
    _connetto_derived (writer, seq) {
        writer -> Text,
        seq -> BigInt,
        changeset -> Binary,
        position -> BigInt,
    }
}

diesel::table! {
    _connetto_derived_marks (writer) {
        writer -> Text,
        through_seq -> BigInt,
    }
}

diesel::table! {
    _connetto_rollback (writer, seq) {
        writer -> Text,
        seq -> BigInt,
        changeset -> Binary,
    }
}
diesel::allow_tables_to_appear_in_same_query!(_connetto_derived, _connetto_pending);
diesel::allow_tables_to_appear_in_same_query!(_connetto_rollback, _connetto_pending);
diesel::allow_tables_to_appear_in_same_query!(_connetto_derived, crate::acked::_connetto_acked);

pub(crate) const DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_derived \
    (writer TEXT NOT NULL, seq INTEGER NOT NULL, changeset BLOB NOT NULL, position INTEGER NOT NULL DEFAULT 0, \
    PRIMARY KEY (writer, seq)); \
    CREATE TABLE IF NOT EXISTS _connetto_derived_marks \
    (writer TEXT PRIMARY KEY NOT NULL, through_seq INTEGER NOT NULL); \
    CREATE TABLE IF NOT EXISTS _connetto_rollback \
    (writer TEXT NOT NULL, seq INTEGER NOT NULL, changeset BLOB NOT NULL, PRIMARY KEY (writer, seq))";

pub(crate) const UNSENT_WRITER: &str = "connetto-unsent";

pub(crate) fn clear_unsent(database: &mut SqliteConnection) -> Result<(), ClientError> {
    diesel::delete(_connetto_rollback::table.filter(_connetto_rollback::writer.eq(UNSENT_WRITER)))
        .execute(database)?;
    Ok(())
}

pub(crate) fn has_held_through(
    database: &mut SqliteConnection,
    writer: &str,
    through: i64,
) -> Result<bool, ClientError> {
    use crate::overlay::_connetto_overlay_tags;
    use diesel::dsl::exists;

    if diesel::select(exists(
        _connetto_derived::table
            .filter(_connetto_derived::writer.eq(writer))
            .filter(_connetto_derived::seq.le(through)),
    ))
    .get_result::<bool>(database)?
        || diesel::select(exists(
            _connetto_rollback::table
                .filter(_connetto_rollback::writer.eq(writer))
                .filter(_connetto_rollback::seq.le(through)),
        ))
        .get_result::<bool>(database)?
    {
        return Ok(true);
    }
    Ok(diesel::select(exists(
        _connetto_overlay_tags::table
            .filter(_connetto_overlay_tags::writer.eq(writer))
            .filter(_connetto_overlay_tags::seq.le(through)),
    ))
    .get_result(database)?)
}

pub(crate) fn prepend_unsent(
    database: &mut SqliteConnection,
    marked: bool,
    prefix: &[u8],
) -> Result<(), ClientError> {
    let sequence = i64::from(marked);
    let existing = load_local_changeset(database, UNSENT_WRITER, sequence)?;
    let Some(existing) = existing.filter(|changeset| !changeset.is_empty()) else {
        return store_local_changeset(database, UNSENT_WRITER, sequence, prefix, &[]);
    };
    let combined = diesel_sqlite_session::concat_changesets(prefix, &existing)
        .map_err(|error| ClientError::Apply(error.to_string()))?;
    store_local_changeset(database, UNSENT_WRITER, sequence, &combined, &[])
}

pub(crate) fn compose_unsent(
    database: &mut SqliteConnection,
    marked: bool,
    changeset: &[u8],
) -> Result<Option<Vec<u8>>, ClientError> {
    load_local_changeset(database, UNSENT_WRITER, i64::from(marked))?
        .map(|prefix| {
            if changeset.is_empty() {
                return Ok(prefix);
            }
            diesel_sqlite_session::concat_changesets(&prefix, changeset)
                .map_err(|error| ClientError::Apply(error.to_string()))
        })
        .transpose()
}

pub(crate) fn store(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
    position: i64,
    changeset: &[u8],
) -> Result<(), ClientError> {
    if !changeset.is_empty() {
        diesel::replace_into(_connetto_derived::table)
            .values((
                _connetto_derived::writer.eq(writer),
                _connetto_derived::seq.eq(sequence),
                _connetto_derived::changeset.eq(changeset),
                _connetto_derived::position.eq(position),
            ))
            .execute(database)?;
    }
    Ok(())
}

pub(crate) fn load(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
) -> Result<Option<Vec<u8>>, ClientError> {
    Ok(_connetto_derived::table
        .filter(_connetto_derived::writer.eq(writer))
        .filter(_connetto_derived::seq.eq(sequence))
        .select(_connetto_derived::changeset)
        .first(database)
        .optional()?)
}

pub(crate) fn dropped_mark(
    database: &mut SqliteConnection,
    writer: &str,
) -> Result<i64, ClientError> {
    Ok(_connetto_derived_marks::table
        .filter(_connetto_derived_marks::writer.eq(writer))
        .select(_connetto_derived_marks::through_seq)
        .first(database)
        .optional()?
        .unwrap_or(0))
}

pub(crate) fn all_marks(
    database: &mut SqliteConnection,
) -> Result<Vec<(crate::WriterRef, u64)>, ClientError> {
    let rows: Vec<(String, i64)> = _connetto_derived_marks::table
        .select((
            _connetto_derived_marks::writer,
            _connetto_derived_marks::through_seq,
        ))
        .load(database)?;
    rows.into_iter()
        .map(|(writer, through)| {
            let writer = crate::writer_from_id(&writer).ok_or_else(|| {
                ClientError::Protocol("a derived mark has an invalid writer".to_owned())
            })?;
            let through =
                u64::try_from(through).map_err(|error| ClientError::Protocol(error.to_string()))?;
            Ok((writer, through))
        })
        .collect()
}

pub(crate) fn record_mark(
    database: &mut SqliteConnection,
    writer: &str,
    through: i64,
) -> Result<(), ClientError> {
    diesel::insert_into(_connetto_derived_marks::table)
        .values((
            _connetto_derived_marks::writer.eq(writer),
            _connetto_derived_marks::through_seq.eq(through),
        ))
        .on_conflict(_connetto_derived_marks::writer)
        .do_update()
        .set(_connetto_derived_marks::through_seq.eq(through))
        .execute(database)?;
    Ok(())
}

pub(crate) fn drop_through(
    database: &mut SqliteConnection,
    shared: &HashSet<String>,
    writer: &str,
    through: i64,
) -> Result<(), ClientError> {
    if through <= dropped_mark(database, writer)? {
        return Ok(());
    }
    let sequences: Vec<i64> = _connetto_derived::table
        .filter(_connetto_derived::writer.eq(writer))
        .filter(_connetto_derived::seq.le(through))
        .select(_connetto_derived::seq)
        .order(_connetto_derived::seq.desc())
        .load(database)?;
    for sequence in sequences {
        drop_entry(database, shared, writer, sequence)?;
    }
    crate::overlay::drop_through(database, writer, through)?;
    record_mark(database, writer, through)
}

pub(crate) fn invert(
    database: &mut SqliteConnection,
    shared: &HashSet<String>,
    changeset: &[u8],
) -> Result<(), ClientError> {
    if changeset.is_empty() {
        return Ok(());
    }
    let inverse =
        invert_changeset(changeset).map_err(|error| ClientError::Apply(error.to_string()))?;
    database
        .apply_changeset(&inverse, rollback_omit)
        .map_err(|error| ClientError::Apply(error.to_string()))?;
    if !shared.is_empty() {
        replay::apply(
            database,
            &inverse,
            replay::Policy::Omit,
            &shared_route(shared),
        )?;
    }
    Ok(())
}

pub(crate) fn drop_entry(
    database: &mut SqliteConnection,
    shared: &HashSet<String>,
    writer: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    let held: Option<(i64, Vec<u8>)> = _connetto_derived::table
        .filter(_connetto_derived::writer.eq(writer))
        .filter(_connetto_derived::seq.eq(sequence))
        .select((_connetto_derived::position, _connetto_derived::changeset))
        .first(database)
        .optional()?;
    if let Some((position, changeset)) = held {
        if !changeset.is_empty() {
            crate::derived_composition::invert(database, shared, position, &changeset)?;
        }
        diesel::delete(
            _connetto_derived::table
                .filter(_connetto_derived::writer.eq(writer))
                .filter(_connetto_derived::seq.eq(sequence)),
        )
        .execute(database)?;
    }
    crate::overlay::drop_entry(database, writer, sequence)
}

pub(crate) fn acknowledged(
    database: &mut SqliteConnection,
) -> Result<Vec<(String, i64)>, ClientError> {
    use crate::overlay::_connetto_overlay_tags;
    use diesel::NullableExpressionMethods;
    let mut rows: Vec<(String, i64)> = _connetto_derived::table
        .filter(diesel::dsl::not(diesel::dsl::exists(
            _connetto_pending::table
                .filter(_connetto_pending::writer.eq(_connetto_derived::writer))
                .filter(_connetto_pending::seq.eq(_connetto_derived::seq)),
        )))
        .select((_connetto_derived::writer, _connetto_derived::seq))
        .order((_connetto_derived::writer, _connetto_derived::seq.desc()))
        .load(database)?;
    rows.extend(
        _connetto_overlay_tags::table
            .filter(_connetto_overlay_tags::writer.is_not_null())
            .filter(_connetto_overlay_tags::dirty.eq(false))
            .filter(_connetto_overlay_tags::seq.is_not_null())
            .filter(diesel::dsl::not(diesel::dsl::exists(
                _connetto_pending::table
                    .filter(
                        _connetto_pending::writer
                            .nullable()
                            .eq(_connetto_overlay_tags::writer),
                    )
                    .filter(
                        _connetto_pending::seq
                            .nullable()
                            .eq(_connetto_overlay_tags::seq),
                    ),
            )))
            .select((
                _connetto_overlay_tags::writer.assume_not_null(),
                _connetto_overlay_tags::seq.assume_not_null(),
            ))
            .load::<(String, i64)>(database)?,
    );
    rows.sort_unstable_by(|left, right| left.0.cmp(&right.0).then(right.1.cmp(&left.1)));
    rows.dedup();
    Ok(rows)
}

pub(crate) fn rekey(
    database: &mut SqliteConnection,
    from: &str,
    to: &str,
) -> Result<(), ClientError> {
    diesel::update(_connetto_derived::table.filter(_connetto_derived::writer.eq(from)))
        .set(_connetto_derived::writer.eq(to))
        .execute(database)?;
    diesel::update(_connetto_derived_marks::table.filter(_connetto_derived_marks::writer.eq(from)))
        .set(_connetto_derived_marks::writer.eq(to))
        .execute(database)?;
    diesel::update(_connetto_rollback::table.filter(_connetto_rollback::writer.eq(from)))
        .set(_connetto_rollback::writer.eq(to))
        .execute(database)?;
    crate::overlay::rekey(database, from, to)
}

pub(crate) fn delete_writer(
    database: &mut SqliteConnection,
    writer: &str,
) -> Result<(), ClientError> {
    diesel::delete(_connetto_derived::table.filter(_connetto_derived::writer.eq(writer)))
        .execute(database)?;
    diesel::delete(
        _connetto_derived_marks::table.filter(_connetto_derived_marks::writer.eq(writer)),
    )
    .execute(database)?;
    diesel::delete(_connetto_rollback::table.filter(_connetto_rollback::writer.eq(writer)))
        .execute(database)?;
    crate::overlay::drop_writer(database, writer)
}

pub(crate) fn drop_writer(
    database: &mut SqliteConnection,
    shared: &HashSet<String>,
    writer: &str,
) -> Result<(), ClientError> {
    let sequences: Vec<i64> = _connetto_derived::table
        .filter(_connetto_derived::writer.eq(writer))
        .select(_connetto_derived::seq)
        .order(_connetto_derived::seq.desc())
        .load(database)?;
    for sequence in sequences {
        drop_entry(database, shared, writer, sequence)?;
    }
    delete_writer(database, writer)
}

pub(crate) fn move_entry(
    database: &mut SqliteConnection,
    from: &str,
    original: i64,
    to: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    diesel::update(
        _connetto_derived::table
            .filter(_connetto_derived::writer.eq(from))
            .filter(_connetto_derived::seq.eq(original)),
    )
    .set((
        _connetto_derived::writer.eq(to),
        _connetto_derived::seq.eq(sequence),
    ))
    .execute(database)?;
    diesel::update(
        _connetto_rollback::table
            .filter(_connetto_rollback::writer.eq(from))
            .filter(_connetto_rollback::seq.eq(original)),
    )
    .set((
        _connetto_rollback::writer.eq(to),
        _connetto_rollback::seq.eq(sequence),
    ))
    .execute(database)?;
    crate::overlay::move_entry(database, from, original, to, sequence)
}

pub(crate) fn all_changesets(database: &mut SqliteConnection) -> Result<Vec<Vec<u8>>, ClientError> {
    Ok(_connetto_derived::table
        .select(_connetto_derived::changeset)
        .load(database)?)
}

pub(crate) fn load_local_changeset(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
) -> Result<Option<Vec<u8>>, ClientError> {
    Ok(_connetto_rollback::table
        .filter(_connetto_rollback::writer.eq(writer))
        .filter(_connetto_rollback::seq.eq(sequence))
        .select(_connetto_rollback::changeset)
        .first(database)
        .optional()?)
}

pub(crate) fn store_local_changeset(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
    local: &[u8],
    uploaded: &[u8],
) -> Result<(), ClientError> {
    if local == uploaded {
        return delete_local_changeset(database, writer, sequence);
    }
    diesel::replace_into(_connetto_rollback::table)
        .values((
            _connetto_rollback::writer.eq(writer),
            _connetto_rollback::seq.eq(sequence),
            _connetto_rollback::changeset.eq(local),
        ))
        .execute(database)?;
    Ok(())
}

pub(crate) fn delete_local_changeset(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    diesel::delete(
        _connetto_rollback::table
            .filter(_connetto_rollback::writer.eq(writer))
            .filter(_connetto_rollback::seq.eq(sequence)),
    )
    .execute(database)?;
    Ok(())
}

pub(crate) fn local_changeset<'changeset>(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
    changeset: &'changeset [u8],
) -> Result<std::borrow::Cow<'changeset, [u8]>, ClientError> {
    Ok(load_local_changeset(database, writer, sequence)?.map_or(
        std::borrow::Cow::Borrowed(changeset),
        std::borrow::Cow::Owned,
    ))
}
