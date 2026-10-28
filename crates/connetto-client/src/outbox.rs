//! The outbox: every application write to a synced table, recorded by
//! `TEMP` triggers inside the transaction that makes it, so an unsent write
//! is a committed row from the moment it is written.
//!
//! The triggers are generated from the replica's own catalog at every open
//! and by every [`change_schema`](crate::ConnettoConnection::change_schema),
//! and they record nothing while connetto's write exemption is raised, so
//! server frames, rollbacks, replays and bookkeeping never enter the outbox.
//! A table in the shared store gets the same triggers on each identity's own
//! connection, which write that identity's outbox in `main` and spare the
//! written key in the store's `_connetto_spare` in the same transaction, so
//! another identity's pass keeps the row before it is pushed. An authorizer
//! refuses any other `CREATE`, `ALTER` or `DROP` of an application table in
//! `main` and every one in the shared store, which is what keeps the
//! triggers in step with the tables. Each row carries the old and new values
//! as a JSON object keyed by column name, each value encoded by its storage
//! class: integers, text and null as plain JSON, a real as
//! `{"r": "<17 significant digits>"}` so it round trips exactly, and a blob
//! as `{"b": "<hex>"}`.

use core::fmt::Write as _;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use connetto_core::WRITE_EXEMPTION_FUNCTION;
use connetto_core::sql::quote_ident;
use diesel::SqliteConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::{AuthorizerContext, AuthorizerDecision};

use crate::{ClientError, SHARED_SCHEMA, is_internal_table, is_shared, schema_ddl_rows};

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

/// The columns of `table` in `schema`, in column order.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read.
fn columns(
    db: &mut SqliteConnection,
    schema: &str,
    table: &str,
) -> Result<Vec<Column>, ClientError> {
    Ok(diesel::sql_query(format!(
        "SELECT name, pk, dflt_value FROM {}.pragma_table_info(?) ORDER BY cid",
        quote_ident(schema)
    ))
    .bind::<diesel::sql_types::Text, _>(table)
    .load(db)?)
}

/// The schema holding `table`, an outbox record's table: the shared store
/// for a table the build shares, `main` otherwise.
fn schema_of(shared: &HashSet<String>, table: &str) -> &'static str {
    if is_shared(shared, table) {
        SHARED_SCHEMA
    } else {
        "main"
    }
}

/// The keyed application tables in `schema` that `keep` admits, each with
/// its columns.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read.
fn keyed_tables(
    db: &mut SqliteConnection,
    schema: &str,
    keep: impl Fn(&str) -> bool,
) -> Result<Vec<(String, Vec<Column>)>, ClientError> {
    let mut tables = Vec::new();
    for row in schema_ddl_rows(db, schema)? {
        if is_internal_table(&row.name) || !keep(&row.name) {
            continue;
        }
        let columns = columns(db, schema, &row.name)?;
        if columns.iter().any(|column| column.pk > 0) {
            tables.push((row.name, columns));
        }
    }
    Ok(tables)
}

/// The application tables in `main` the outbox records, the keyed ones, each
/// with its columns.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read.
fn recorded_tables(db: &mut SqliteConnection) -> Result<Vec<(String, Vec<Column>)>, ClientError> {
    keyed_tables(db, "main", |_| true)
}

/// The tables of `shared` in the shared store the outbox records, each with
/// its columns.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read.
fn shared_tables(
    db: &mut SqliteConnection,
    shared: &HashSet<String>,
) -> Result<Vec<(String, Vec<Column>)>, ClientError> {
    keyed_tables(db, SHARED_SCHEMA, |table| is_shared(shared, table))
}

/// Drop every outbox trigger, so DDL can change a table its triggers name.
///
/// The tables recorded now are the ones the triggers were made for, since a
/// table changes only through [`change_schema`](crate::ConnettoConnection::change_schema),
/// which runs this before its DDL.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read or a trigger dropped.
pub(crate) fn drop_triggers(db: &mut SqliteConnection) -> Result<(), ClientError> {
    let drops = recorded_tables(db)?
        .iter()
        .flat_map(|(table, _)| {
            OPS.iter().map(move |(op, ..)| {
                format!("DROP TRIGGER IF EXISTS temp.{}; ", trigger_name(op, table))
            })
        })
        .collect::<Vec<_>>()
        .concat();
    db.batch_execute(&drops)?;
    Ok(())
}

/// The writes each table gets a trigger for: the operation's letter, the
/// event, and which rows the record carries as old and new.
const OPS: [(&str, &str, Option<&str>, Option<&str>); 3] = [
    ("I", "INSERT", None, Some("NEW")),
    ("U", "UPDATE", Some("OLD"), Some("NEW")),
    ("D", "DELETE", Some("OLD"), None),
];

/// The quoted name of `table`'s trigger for `op`.
fn trigger_name(op: &str, table: &str) -> String {
    quote_ident(&format!("{TRIGGER_PREFIX}{op}_{table}"))
}

/// Create the outbox triggers for every keyed application table in `main`
/// as the catalog stands now, on a connection that holds none.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read or a trigger created.
pub(crate) fn install(db: &mut SqliteConnection) -> Result<(), ClientError> {
    for (table, columns) in recorded_tables(db)? {
        db.batch_execute(&triggers("main", &table, &columns, None))?;
    }
    Ok(())
}

/// Create the outbox triggers for every table of `shared` in the shared
/// store, which also spare each written key under `identity`, on a
/// connection that holds none. Only a `TEMP` trigger may write another
/// schema, so each identity's connection records its own writes alone.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read or a trigger created.
pub(crate) fn install_shared(
    db: &mut SqliteConnection,
    identity: &str,
    shared: &HashSet<String>,
) -> Result<(), ClientError> {
    for (table, columns) in shared_tables(db, shared)? {
        db.batch_execute(&triggers(SHARED_SCHEMA, &table, &columns, Some(identity)))?;
    }
    Ok(())
}

/// The three `TEMP` triggers recording writes to `table` in `schema`, each
/// also sparing the keys it touched under `spare`'s identity when given. The
/// unqualified `_connetto_outbox` resolves to `main` and `_connetto_spare` to
/// the shared store, the only schemas holding them.
fn triggers(schema: &str, table: &str, columns: &[Column], spare: Option<&str>) -> String {
    let target = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let literal = table.replace('\'', "''");
    OPS.into_iter()
        .map(|(op, event, old, new)| {
            let name = trigger_name(op, table);
            let mut spares = String::new();
            if let Some(identity) = spare {
                let identity = identity.replace('\'', "''");
                let lowered = table.to_lowercase().replace('\'', "''");
                for row in old.into_iter().chain(new) {
                    let _ = write!(
                        spares,
                        "INSERT OR IGNORE INTO _connetto_spare (identity, tbl, clause) \
                         VALUES ('{identity}', '{lowered}', {}); ",
                        spare_clause(row, columns)
                    );
                }
            }
            let old = old.map_or_else(|| "NULL".to_owned(), |row| encoded(row, columns));
            let new = new.map_or_else(|| "NULL".to_owned(), |row| encoded(row, columns));
            format!(
                "CREATE TEMP TRIGGER {name} AFTER {event} ON {target} \
             WHEN NOT {WRITE_EXEMPTION_FUNCTION}() BEGIN \
             INSERT INTO _connetto_outbox (tbl, op, old, new) \
             VALUES ('{literal}', '{op}', {old}, {new}); {spares}END; "
            )
        })
        .collect::<Vec<_>>()
        .concat()
}

/// The key columns of `columns`, in key order.
fn key_columns(columns: &[Column]) -> Vec<&Column> {
    let mut keyed: Vec<&Column> = columns.iter().filter(|column| column.pk > 0).collect();
    keyed.sort_by_key(|column| column.pk);
    keyed
}

/// The SQL rendering `row`'s key as the clause `record_spare` writes,
/// `("a" IS 1 AND "b" IS 'x')`, each value through `quote()`.
fn spare_clause(row: &str, columns: &[Column]) -> String {
    let parts: Vec<String> = key_columns(columns)
        .into_iter()
        .map(|column| {
            let name = quote_ident(&column.name);
            format!("'{} IS ' || quote({row}.{name})", name.replace('\'', "''"))
        })
        .collect();
    format!("'(' || {} || ')'", parts.join(" || ' AND ' || "))
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
/// `main` on `db` while `open` is down, and of one in the shared store
/// always, since the outbox triggers would no longer match the table. The
/// shared store's tables change only with a release, through the layout
/// programs every identity runs at open.
pub(crate) fn guard_schema(db: &mut SqliteConnection, open: &Arc<AtomicBool>) {
    let open = Arc::clone(open);
    db.on_authorize(move |action| {
        let (database, table) = match action {
            AuthorizerContext::CreateTable(create) => (create.database, create.table),
            AuthorizerContext::AlterTable(alter) => (alter.database, alter.table),
            AuthorizerContext::DropTable(drop) => (drop.database, drop.table),
            _ => return AuthorizerDecision::Allow,
        };
        let guarded = match database {
            Some("main") => !open.load(Ordering::Relaxed),
            Some(SHARED_SCHEMA) => true,
            _ => false,
        };
        if guarded && table.is_some_and(|table| !is_internal_table(table)) {
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

/// Delete every recorded write.
///
/// # Errors
///
/// [`ClientError`] when the outbox cannot be written.
pub(crate) fn clear(db: &mut SqliteConnection) -> Result<(), ClientError> {
    db.batch_execute("DELETE FROM main._connetto_outbox")?;
    Ok(())
}

diesel::table! {
    /// The recorded writes, in `main`.
    _connetto_outbox (seq) {
        /// The order the writes were made in.
        seq -> BigInt,
        /// The table written, as the trigger named it.
        tbl -> Text,
        /// The operation, `I`, `U` or `D`.
        op -> Text,
        /// The row before the write, absent for an insert.
        old -> Nullable<Text>,
        /// The row after the write, absent for a delete.
        new -> Nullable<Text>,
    }
}

/// One recorded write.
struct Recorded {
    /// The table written, as the trigger named it.
    tbl: String,
    /// The row before the write, absent for an insert.
    old: Option<String>,
    /// The row after the write, absent for a delete.
    new: Option<String>,
}

/// Every recorded write, oldest first.
///
/// # Errors
///
/// [`ClientError`] when the outbox cannot be read.
fn recorded(db: &mut SqliteConnection) -> Result<Vec<Recorded>, ClientError> {
    let rows: Vec<(String, Option<String>, Option<String>)> = _connetto_outbox::table
        .select((
            _connetto_outbox::tbl,
            _connetto_outbox::old,
            _connetto_outbox::new,
        ))
        .order(_connetto_outbox::seq)
        .load(db)?;
    Ok(rows
        .into_iter()
        .map(|(tbl, old, new)| Recorded { tbl, old, new })
        .collect())
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

/// `cell` as an SQL literal.
fn literal(cell: &Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_owned(),
        Cell::Integer(integer) => integer.to_string(),
        Cell::Real(bits) => {
            let real = f64::from_bits(*bits);
            if real.is_finite() {
                format!("{real:?}")
            } else if real > 0.0 {
                "9e999".to_owned()
            } else {
                "-9e999".to_owned()
            }
        }
        Cell::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Cell::Blob(blob) => {
            const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
            let hex: String = blob
                .iter()
                .flat_map(|byte| {
                    [
                        char::from(DIGITS[usize::from(byte >> 4)]),
                        char::from(DIGITS[usize::from(byte & 0x0f)]),
                    ]
                })
                .collect();
            format!("X'{hex}'")
        }
    }
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

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
        .collect()
}

/// One recorded row as a JSON object.
fn parsed(text: &str) -> Result<serde_json::Map<String, serde_json::Value>, ClientError> {
    serde_json::from_str(text)
        .map_err(|_| ClientError::Apply(format!("a malformed outbox row: {text}")))
}

/// The key clauses of every write the outbox holds, the old key and the new
/// one alike, by lowercased table, in the form `record_spare` writes, so a
/// pass spares a row written but not yet pushed. A table of `shared` is read
/// from the shared store's catalog.
///
/// # Errors
///
/// [`ClientError`] when the outbox or the catalog cannot be read, or a record
/// is malformed.
pub(crate) fn spare_clauses(
    db: &mut SqliteConnection,
    shared: &HashSet<String>,
) -> Result<HashMap<String, Vec<String>>, ClientError> {
    let mut schemas: HashMap<String, Vec<Column>> = HashMap::new();
    let mut clauses: HashMap<String, Vec<String>> = HashMap::new();
    for row in recorded(db)? {
        if !schemas.contains_key(&row.tbl) {
            let found = columns(db, schema_of(shared, &row.tbl), &row.tbl)?;
            schemas.insert(row.tbl.clone(), found);
        }
        let keyed = key_columns(&schemas[&row.tbl]);
        for text in [&row.old, &row.new].into_iter().flatten() {
            let mut values = parsed(text)?;
            let mut wheres = Vec::with_capacity(keyed.len());
            for column in &keyed {
                let value = values.remove(&column.name).unwrap_or_default();
                let cell = decode_value(value, text)?;
                wheres.push(format!(
                    "{} IS {}",
                    quote_ident(&column.name),
                    literal(&cell)
                ));
            }
            let clause = format!("({})", wheres.join(" AND "));
            let table = clauses.entry(row.tbl.to_lowercase()).or_default();
            if !table.contains(&clause) {
                table.push(clause);
            }
        }
    }
    Ok(clauses)
}

#[cfg(debug_assertions)]
pub(crate) use check::assert_matches;

/// The debug check that the outbox records what the session captures.
#[cfg(debug_assertions)]
mod check {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    use diesel::SqliteConnection;
    use diesel::prelude::*;
    use sqlite_diff_rs::{ChangesetOp, ParsedDiffSet, Value};

    use connetto_core::sql::quote_ident;

    use super::{
        Cell, Column, Recorded, cell, columns, decode_value, encoded, key_columns, literal, parsed,
        recorded, recorded_tables, schema_of, shared_tables,
    };
    use crate::{ClientError, SHARED_SCHEMA};

    /// One encoded value read back from SQL.
    #[derive(diesel::QueryableByName)]
    struct Encoded {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }

    /// A row's net change, in the changeset's own terms.
    #[derive(Debug, PartialEq, Eq)]
    enum Net {
        Insert(Vec<Cell>),
        Delete(Vec<Cell>),
        Update(Vec<(Option<Cell>, Option<Cell>)>),
    }

    /// A row of one table, by its table and key.
    type RowKey = (String, Vec<Cell>);

    type Changes = BTreeMap<RowKey, Net>;

    /// Panic unless the outbox's writes amount to the changes the session
    /// captured in `changeset` for the tables the outbox records, those in
    /// `main` and the tables of `shared` in the shared store.
    ///
    /// A key whose row no longer holds the outbox's last recorded values is
    /// left out, in a table of `exempt_written`, which a write under the
    /// exemption changed since the last push, and in every shared table,
    /// which another identity's connection writes unseen. The session uploads
    /// the row as it is now and the outbox the values the application wrote,
    /// which is the difference the outbox exists to make.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the catalog or either side cannot be read.
    pub(crate) fn assert_matches(
        db: &mut SqliteConnection,
        changeset: &[u8],
        exempt_written: &HashSet<String>,
        shared: &HashSet<String>,
    ) -> Result<(), ClientError> {
        let recorded = recorded(db)?;
        let (from_outbox, overwritten) = from_recorded(db, &recorded, exempt_written, shared)?;
        let mut tables: Vec<String> = recorded_tables(db)?
            .into_iter()
            .map(|(table, _)| table)
            .collect();
        if !shared.is_empty() {
            tables.extend(
                shared_tables(db, shared)?
                    .into_iter()
                    .map(|(table, _)| table),
            );
        }
        let mut from_session = from_changeset(changeset, &tables)?;
        from_session.retain(|key, _| !overwritten.contains(key));
        assert_eq!(
            from_outbox, from_session,
            "the outbox and the session disagree on the writes to upload"
        );
        Ok(())
    }

    /// The row of `table` in `schema` under `key` as it is now, absent when
    /// none is.
    fn current_row(
        db: &mut SqliteConnection,
        schema: &str,
        table: &str,
        columns: &[Column],
        key: &[Cell],
    ) -> Result<Option<Vec<Cell>>, ClientError> {
        let wheres: Vec<String> = key_columns(columns)
            .iter()
            .zip(key)
            .map(|(column, cell)| format!("{} IS {}", quote_ident(&column.name), literal(cell)))
            .collect();
        let target = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let rows: Vec<Encoded> = diesel::sql_query(format!(
            "SELECT {} AS value FROM {target} WHERE {}",
            encoded(&target, columns),
            wheres.join(" AND ")
        ))
        .load(db)?;
        rows.first()
            .map(|row| decode(db, &row.value, columns))
            .transpose()
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
        let mut row = parsed(text)?;
        columns
            .iter()
            .map(|column| match row.remove(&column.name) {
                Some(value) => decode_value(value, text),
                None => default_cell(db, column),
            })
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

    /// The net change the outbox records per key, and the keys whose row no
    /// longer holds the outbox's last recorded values, in a table of
    /// `exempt_written` or of `shared`, which the net change leaves out.
    fn from_recorded(
        db: &mut SqliteConnection,
        recorded: &[Recorded],
        exempt_written: &HashSet<String>,
        shared: &HashSet<String>,
    ) -> Result<(Changes, BTreeSet<RowKey>), ClientError> {
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
                let found = columns(db, schema_of(shared, &row.tbl), &row.tbl)?;
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
        let mut overwritten = BTreeSet::new();
        for ((table, key), span) in spans {
            let schema = schema_of(shared, &table);
            if (schema == SHARED_SCHEMA || exempt_written.contains(&table))
                && current_row(db, schema, &table, &schemas[&table], &key)? != span.after
            {
                overwritten.insert((table, key));
                continue;
            }
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
        Ok((changes, overwritten))
    }
}
