//! The outbox: every application write to a table in `main`, recorded by
//! `TEMP` triggers inside the transaction that makes it, so an unsent write
//! is a committed row from the moment it is written.
//!
//! The triggers are generated from the replica's own catalog at every open
//! and by every [`change_schema`](crate::ConnettoConnection::change_schema),
//! and they record nothing while connetto's write exemption is raised, so
//! server frames, rollbacks, replays and bookkeeping never enter the outbox.
//! An authorizer refuses any other `CREATE`, `ALTER` or `DROP` of an
//! application table in `main`, which is what keeps the triggers in step with
//! the tables. Each row carries the old and new values as a JSON object keyed
//! by column name, each value encoded by its storage class: integers, text
//! and null as plain JSON, a real as `{"r": "<17 significant digits>"}` so it
//! round trips exactly, and a blob as `{"b": "<hex>"}`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use connetto_core::WRITE_EXEMPTION_FUNCTION;
use connetto_core::sql::quote_ident;
use diesel::SqliteConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::{AuthorizerContext, AuthorizerDecision};

use crate::{ClientError, is_internal_table, schema_ddl_rows};

/// The outbox table, created beside connetto's other bookkeeping.
pub(crate) const OUTBOX_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_outbox \
    (seq INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, op TEXT NOT NULL, \
     old TEXT, new TEXT)";

/// The prefix of every outbox trigger name.
const TRIGGER_PREFIX: &str = "_connetto_outbox_";

/// One column of a captured table, in column order.
#[derive(diesel::QueryableByName)]
pub(crate) struct Column {
    /// The column name.
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub(crate) name: String,
    /// The column's position in the primary key, from 1, or 0 outside it.
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub(crate) pk: i64,
    /// The SQL text of the column's default, absent without one.
    #[cfg_attr(
        not(debug_assertions),
        expect(dead_code, reason = "read by the debug check")
    )]
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub(crate) dflt_value: Option<String>,
}

/// The columns of `table` in `main`, in column order.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read.
fn columns(db: &mut SqliteConnection, table: &str) -> Result<Vec<Column>, ClientError> {
    Ok(
        diesel::sql_query(
            "SELECT name, pk, dflt_value FROM main.pragma_table_info(?) ORDER BY cid",
        )
        .bind::<diesel::sql_types::Text, _>(table)
        .load(db)?,
    )
}

/// The application tables in `main` the outbox records, the keyed ones, each
/// with its columns.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read.
fn recorded_tables(db: &mut SqliteConnection) -> Result<Vec<(String, Vec<Column>)>, ClientError> {
    let mut tables = Vec::new();
    for row in schema_ddl_rows(db, "main")? {
        if is_internal_table(&row.name) {
            continue;
        }
        let columns = columns(db, &row.name)?;
        if columns.iter().any(|column| column.pk > 0) {
            tables.push((row.name, columns));
        }
    }
    Ok(tables)
}

/// Drop every outbox trigger, so DDL can change a table its triggers name.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read or a trigger dropped.
pub(crate) fn drop_triggers(db: &mut SqliteConnection) -> Result<(), ClientError> {
    #[derive(diesel::QueryableByName)]
    struct Trigger {
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
    }
    let triggers: Vec<Trigger> = diesel::sql_query(format!(
        "SELECT name FROM temp.sqlite_master WHERE type = 'trigger' AND name GLOB '{TRIGGER_PREFIX}*'"
    ))
    .load(db)?;
    for trigger in triggers {
        db.batch_execute(&format!("DROP TRIGGER temp.{}", quote_ident(&trigger.name)))?;
    }
    Ok(())
}

/// Create the outbox triggers for every keyed application table in `main`
/// as the catalog stands now, on a connection that holds none.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read or a trigger created.
pub(crate) fn install(db: &mut SqliteConnection) -> Result<(), ClientError> {
    for (table, columns) in recorded_tables(db)? {
        db.batch_execute(&triggers(&table, &columns))?;
    }
    Ok(())
}

/// The three `TEMP` triggers recording writes to `table`.
fn triggers(table: &str, columns: &[Column]) -> String {
    let target = format!("main.{}", quote_ident(table));
    let literal = table.replace('\'', "''");
    [
        ("I", "INSERT", None, Some("NEW")),
        ("U", "UPDATE", Some("OLD"), Some("NEW")),
        ("D", "DELETE", Some("OLD"), None),
    ]
    .into_iter()
    .map(|(op, event, old, new)| {
        let name = quote_ident(&format!("{TRIGGER_PREFIX}{op}_{table}"));
        let old = old.map_or_else(|| "NULL".to_owned(), |row| encoded(row, columns));
        let new = new.map_or_else(|| "NULL".to_owned(), |row| encoded(row, columns));
        format!(
            "CREATE TEMP TRIGGER {name} AFTER {event} ON {target} \
             WHEN NOT {WRITE_EXEMPTION_FUNCTION}() BEGIN \
             INSERT INTO _connetto_outbox (tbl, op, old, new) \
             VALUES ('{literal}', '{op}', {old}, {new}); END; "
        )
    })
    .collect::<Vec<_>>()
    .concat()
}

/// The SQL encoding the value of `value`, an SQL expression, as JSON text.
fn cell(value: &str) -> String {
    format!(
        "CASE typeof({value}) \
         WHEN 'blob' THEN '{{\"b\":\"' || hex({value}) || '\"}}' \
         WHEN 'real' THEN '{{\"r\":\"' || printf('%!.17g', {value}) || '\"}}' \
         ELSE json_quote({value}) END"
    )
}

/// The SQL building one row's JSON object. Concatenation rather than
/// `json_object`, so a wide table never meets the function argument limit.
fn encoded(row: &str, columns: &[Column]) -> String {
    let cells: Vec<String> = columns
        .iter()
        .map(|column| {
            let key = serde_json::Value::from(column.name.as_str())
                .to_string()
                .replace('\'', "''");
            format!(
                "'{key}:' || {}",
                cell(&format!("{row}.{}", quote_ident(&column.name)))
            )
        })
        .collect();
    format!("'{{' || {} || '}}'", cells.join(" || ',' || "))
}

/// Refuse every `CREATE`, `ALTER` and `DROP` of an application table in
/// `main` on `db` while `open` is down, since the outbox triggers would no
/// longer match the table.
pub(crate) fn guard_schema(db: &mut SqliteConnection, open: &Arc<AtomicBool>) {
    let open = Arc::clone(open);
    db.on_authorize(move |action| {
        let (database, table) = match action {
            AuthorizerContext::CreateTable(create) => (create.database, create.table),
            AuthorizerContext::AlterTable(alter) => (alter.database, alter.table),
            AuthorizerContext::DropTable(drop) => (drop.database, drop.table),
            _ => return AuthorizerDecision::Allow,
        };
        if database == Some("main")
            && table.is_some_and(|table| !is_internal_table(table))
            && !open.load(Ordering::Relaxed)
        {
            AuthorizerDecision::Deny
        } else {
            AuthorizerDecision::Allow
        }
    });
}

/// Admits schema changes to application tables in `main` while it lives.
pub(crate) struct SchemaWindow<'a> {
    open: &'a AtomicBool,
}

impl<'a> SchemaWindow<'a> {
    /// Open the window over `open`.
    pub(crate) fn new(open: &'a AtomicBool) -> Self {
        open.store(true, Ordering::Relaxed);
        Self { open }
    }
}

impl Drop for SchemaWindow<'_> {
    fn drop(&mut self) {
        self.open.store(false, Ordering::Relaxed);
    }
}

/// Delete the recorded writes up to and including `last`.
///
/// # Errors
///
/// [`ClientError`] when the outbox cannot be written.
pub(crate) fn clear_through(db: &mut SqliteConnection, last: i64) -> Result<(), ClientError> {
    diesel::sql_query("DELETE FROM main._connetto_outbox WHERE seq <= ?")
        .bind::<diesel::sql_types::BigInt, _>(last)
        .execute(db)?;
    Ok(())
}

/// The newest recorded write's place in the outbox, if any.
///
/// # Errors
///
/// [`ClientError`] when the outbox cannot be read.
pub(crate) fn last_seq(db: &mut SqliteConnection) -> Result<Option<i64>, ClientError> {
    #[derive(diesel::QueryableByName)]
    struct Last {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
        last: Option<i64>,
    }
    let rows: Vec<Last> =
        diesel::sql_query("SELECT max(seq) AS last FROM main._connetto_outbox").load(db)?;
    Ok(rows.first().and_then(|row| row.last))
}

/// Delete every recorded write.
///
/// # Errors
///
/// [`ClientError`] when the outbox cannot be written.
pub(crate) fn clear(db: &mut SqliteConnection) -> Result<(), ClientError> {
    db.batch_execute("DELETE FROM main._connetto_outbox")?;
    Ok(())
}

#[cfg(debug_assertions)]
pub(crate) use check::assert_matches;

/// The debug check that the outbox records what the session captures.
#[cfg(debug_assertions)]
mod check {
    use std::collections::{BTreeMap, HashMap};

    use diesel::SqliteConnection;
    use diesel::prelude::*;
    use sqlite_diff_rs::{ChangesetOp, ParsedDiffSet, Value};

    use super::{Column, cell, columns, recorded_tables};
    use crate::ClientError;

    /// One recorded write.
    #[derive(diesel::QueryableByName)]
    struct Recorded {
        /// The table written, as the trigger named it.
        #[diesel(sql_type = diesel::sql_types::Text)]
        tbl: String,
        /// The row before the write, absent for an insert.
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        old: Option<String>,
        /// The row after the write, absent for a delete.
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        new: Option<String>,
    }

    /// One encoded value read back from SQL.
    #[derive(diesel::QueryableByName)]
    struct Encoded {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }

    /// One cell, comparable and orderable.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    enum Cell {
        Null,
        Integer(i64),
        Real(u64),
        Text(String),
        Blob(Vec<u8>),
    }

    /// A row's net change, in the changeset's own terms.
    #[derive(Debug, PartialEq, Eq)]
    enum Net {
        Insert(Vec<Cell>),
        Delete(Vec<Cell>),
        Update(Vec<(Option<Cell>, Option<Cell>)>),
    }

    type Changes = BTreeMap<(String, Vec<Cell>), Net>;

    /// Panic unless the outbox's writes amount to the changes the session
    /// captured in `changeset` for the tables the outbox records.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the catalog or either side cannot be read.
    pub(crate) fn assert_matches(
        db: &mut SqliteConnection,
        changeset: &[u8],
    ) -> Result<(), ClientError> {
        let recorded: Vec<Recorded> =
            diesel::sql_query("SELECT tbl, old, new FROM main._connetto_outbox ORDER BY seq")
                .load(db)?;
        let from_outbox = from_recorded(db, &recorded)?;
        let tables: Vec<String> = recorded_tables(db)?
            .into_iter()
            .map(|(table, _)| table)
            .collect();
        let from_session = from_changeset(changeset, &tables)?;
        assert_eq!(
            from_outbox, from_session,
            "the outbox and the session disagree on the writes to upload"
        );
        Ok(())
    }

    fn from_value(value: &Value<String, Vec<u8>>) -> Cell {
        match value {
            Value::Null => Cell::Null,
            Value::Integer(integer) => Cell::Integer(*integer),
            Value::Real(real) => Cell::Real(real.to_bits()),
            Value::Text(text) => Cell::Text(text.clone()),
            Value::Blob(blob) => Cell::Blob(blob.clone()),
        }
    }

    fn from_changeset(changeset: &[u8], tables: &[String]) -> Result<Changes, ClientError> {
        let mut changes = Changes::new();
        if changeset.is_empty() {
            return Ok(changes);
        }
        let ParsedDiffSet::Changeset(diff) = ParsedDiffSet::parse(changeset)
            .map_err(|err| ClientError::Apply(format!("parsing the capture: {err:?}")))?
        else {
            return Ok(changes);
        };
        for op in diff.iter() {
            let table = op.table().name().clone();
            if !tables.contains(&table) {
                continue;
            }
            let key: Vec<Cell> = op.primary_key().iter().map(from_value).collect();
            let net = match op {
                ChangesetOp::Insert { values, .. } => {
                    Net::Insert(values.iter().map(from_value).collect())
                }
                ChangesetOp::Delete { old_values, .. } => {
                    Net::Delete(old_values.iter().map(from_value).collect())
                }
                ChangesetOp::Update { values, .. } => Net::Update(
                    values
                        .iter()
                        .map(|(old, new)| {
                            (old.as_ref().map(from_value), new.as_ref().map(from_value))
                        })
                        .collect(),
                ),
            };
            changes.insert((table, key), net);
        }
        Ok(changes)
    }

    /// One encoded value as a cell.
    fn decode_value(value: serde_json::Value, text: &str) -> Result<Cell, ClientError> {
        let malformed = || ClientError::Apply(format!("a malformed outbox value: {text}"));
        match value {
            serde_json::Value::Null => Ok(Cell::Null),
            serde_json::Value::Number(number) => {
                number.as_i64().map(Cell::Integer).ok_or_else(malformed)
            }
            serde_json::Value::String(text) => Ok(Cell::Text(text)),
            serde_json::Value::Object(object) => {
                if let Some(serde_json::Value::String(hex)) = object.get("b") {
                    decode_hex(hex).map(Cell::Blob).ok_or_else(malformed)
                } else if let Some(serde_json::Value::String(real)) = object.get("r") {
                    real.parse::<f64>()
                        .map(|real| Cell::Real(real.to_bits()))
                        .map_err(|_| malformed())
                } else {
                    Err(malformed())
                }
            }
            serde_json::Value::Bool(_) | serde_json::Value::Array(_) => Err(malformed()),
        }
    }

    /// The value a column added after a row was recorded holds in that row,
    /// its default, which is what the session reports for it too.
    fn default_cell(db: &mut SqliteConnection, column: &Column) -> Result<Cell, ClientError> {
        let Some(default) = &column.dflt_value else {
            return Ok(Cell::Null);
        };
        let rows: Vec<Encoded> = diesel::sql_query(format!(
            "SELECT {} AS value FROM (SELECT ({default}) AS v)",
            cell("v")
        ))
        .load(db)?;
        let text = rows.first().map_or("null", |row| row.value.as_str());
        let value = serde_json::from_str(text)
            .map_err(|_| ClientError::Apply(format!("a malformed default: {text}")))?;
        decode_value(value, text)
    }

    /// One recorded row as cells in the order of `columns`.
    fn decode(
        db: &mut SqliteConnection,
        text: &str,
        columns: &[Column],
    ) -> Result<Vec<Cell>, ClientError> {
        let mut row: serde_json::Map<String, serde_json::Value> = serde_json::from_str(text)
            .map_err(|_| ClientError::Apply(format!("a malformed outbox row: {text}")))?;
        columns
            .iter()
            .map(|column| match row.remove(&column.name) {
                Some(value) => decode_value(value, text),
                None => default_cell(db, column),
            })
            .collect()
    }

    fn decode_hex(hex: &str) -> Option<Vec<u8>> {
        (0..hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
            .collect()
    }

    /// The row before the first write and after the last, per key.
    struct Span {
        before: Option<Vec<Cell>>,
        after: Option<Vec<Cell>>,
    }

    fn key(row: &[Cell], columns: &[Column]) -> Vec<Cell> {
        let mut keyed: Vec<(i64, Cell)> = columns
            .iter()
            .zip(row)
            .filter(|(column, _)| column.pk > 0)
            .map(|(column, cell)| (column.pk, cell.clone()))
            .collect();
        keyed.sort_by_key(|(pk, _)| *pk);
        keyed.into_iter().map(|(_, cell)| cell).collect()
    }

    fn from_recorded(
        db: &mut SqliteConnection,
        recorded: &[Recorded],
    ) -> Result<Changes, ClientError> {
        let mut schemas: HashMap<String, Vec<Column>> = HashMap::new();
        let mut spans: BTreeMap<(String, Vec<Cell>), Span> = BTreeMap::new();
        let mut write =
            |table: &str, key: Vec<Cell>, before: Option<Vec<Cell>>, after: Option<Vec<Cell>>| {
                spans
                    .entry((table.to_owned(), key))
                    .and_modify(|span| span.after.clone_from(&after))
                    .or_insert(Span { before, after });
            };
        for row in recorded {
            if !schemas.contains_key(&row.tbl) {
                let found = columns(db, &row.tbl)?;
                schemas.insert(row.tbl.clone(), found);
            }
            let columns = &schemas[&row.tbl];
            let old = row
                .old
                .as_deref()
                .map(|old| decode(db, old, columns))
                .transpose()?;
            let new = row
                .new
                .as_deref()
                .map(|new| decode(db, new, columns))
                .transpose()?;
            match (old, new) {
                (None, Some(new)) => write(&row.tbl, key(&new, columns), None, Some(new)),
                (Some(old), None) => write(&row.tbl, key(&old, columns), Some(old), None),
                (Some(old), Some(new)) => {
                    let (from, to) = (key(&old, columns), key(&new, columns));
                    if from == to {
                        write(&row.tbl, from, Some(old), Some(new));
                    } else {
                        write(&row.tbl, from, Some(old), None);
                        write(&row.tbl, to, None, Some(new));
                    }
                }
                (None, None) => {}
            }
        }
        let mut changes = Changes::new();
        for ((table, key), span) in spans {
            let net = match (span.before, span.after) {
                (None, None) => continue,
                (None, Some(after)) => Net::Insert(after),
                (Some(before), None) => Net::Delete(before),
                (Some(before), Some(after)) => {
                    if before == after {
                        continue;
                    }
                    Net::Update(
                        schemas[&table]
                            .iter()
                            .zip(before.into_iter().zip(after))
                            .map(|(column, (old, new))| {
                                if column.pk > 0 {
                                    (Some(old), None)
                                } else if old == new {
                                    (None, None)
                                } else {
                                    (Some(old), Some(new))
                                }
                            })
                            .collect(),
                    )
                }
            };
            changes.insert((table, key), net);
        }
        Ok(changes)
    }
}
