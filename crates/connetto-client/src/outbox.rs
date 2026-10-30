//! The outbox: every application write to a synced table, recorded by
//! `TEMP` triggers inside the transaction that makes it, so an unsent write
//! is a committed row from the moment it is written, and the upload
//! [`push`](crate::ConnettoConnection::push) builds from those rows.
//!
//! The triggers are generated from the replica's own catalog at every open
//! and by every [`change_schema`](crate::ConnettoConnection::change_schema),
//! and they record nothing while connetto's write exemption is raised, so
//! server frames, rollbacks, replays and bookkeeping never enter the outbox,
//! nor while [`UNRECORDED_FUNCTION`] answers true, which keeps out a write
//! the policy guards still judge, as a peer's entry applied as its author is.
//! A table in the shared store gets the same triggers on each identity's own
//! connection, which write that identity's outbox in `main` and spare the
//! written key in the store's `_connetto_spare` in the same transaction, so
//! another identity's pass keeps the row before it is pushed. An authorizer
//! refuses any other `CREATE`, `ALTER` or `DROP` of an application table in
//! `main` and every one in the shared store, which is what keeps the
//! triggers in step with the tables.
//!
//! Each row carries the old and new values as JSON objects keyed by column
//! name, holding what a SQLite changeset holds. An insert carries the whole
//! new row and a delete the whole old row, and an update the key and the
//! cells it changed, or both whole rows when it moves the key. Each value is
//! encoded by its storage class, integers, text and null as plain JSON and a
//! real as `{"r": "<17 significant digits>"}` so it round trips exactly. A
//! blob is the marker `{"b": 0}`, its bytes stored raw in
//! `_connetto_outbox_blob` under the row's sequence, the column and the
//! side, `o` for old and `n` for new, so a blob costs its own size once.

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
use sqlite_diff_rs::{ChangeDelete, ChangeSet, DiffOps, Insert, TableSchema, Update, Value};

use crate::{ClientError, SHARED_SCHEMA, is_internal_table, is_shared, schema_ddl_rows};

/// The outbox and its blob cells, created beside connetto's other
/// bookkeeping.
pub(crate) const OUTBOX_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_outbox \
    (seq INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, op TEXT NOT NULL, \
     old TEXT, new TEXT); \
    CREATE TABLE IF NOT EXISTS _connetto_outbox_blob \
    (seq INTEGER NOT NULL, col TEXT NOT NULL, side TEXT NOT NULL, value BLOB NOT NULL, \
     PRIMARY KEY (seq, col, side))";

/// The prefix of every outbox trigger name.
const TRIGGER_PREFIX: &str = "_connetto_outbox_";

/// The JSON a blob cell holds in place of its bytes.
const BLOB_MARKER: &str = r#"{"b":0}"#;

/// The SQL function the outbox triggers call beside
/// [`WRITE_EXEMPTION_FUNCTION`], true while a write the policy guards judge
/// must still stay out of the outbox, as a peer's entry applied as its author
/// does.
pub(crate) const UNRECORDED_FUNCTION: &str = "connetto_unrecorded";

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
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub(crate) dflt_value: Option<String>,
}

/// The columns of `table` in `schema`, in column order.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read.
pub(crate) fn columns(
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
            OPS.iter()
                .map(move |op| format!("DROP TRIGGER IF EXISTS temp.{}; ", trigger_name(op, table)))
        })
        .collect::<Vec<_>>()
        .concat();
    db.batch_execute(&drops)?;
    Ok(())
}

/// The letters of the writes each table gets a trigger for.
const OPS: [&str; 3] = ["I", "U", "D"];

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

/// The SQL condition under which an update's record carries `column`.
///
/// A key column is always carried. Any other is carried when the update
/// changed it, compared as stored bytes the way SQLite's session compares
/// them, so a change of case under a `NOCASE` collation or of storage class
/// alone counts, and every column is carried when the update moves the key.
fn carried(column: &Column, columns: &[Column]) -> String {
    if column.pk > 0 {
        return "1".to_owned();
    }
    let differs = |column: &Column| {
        let name = quote_ident(&column.name);
        format!(
            "(OLD.{name} IS NOT NEW.{name} COLLATE BINARY \
             OR typeof(OLD.{name}) IS NOT typeof(NEW.{name}))"
        )
    };
    let moved: Vec<String> = key_columns(columns).into_iter().map(differs).collect();
    format!("({} OR {})", differs(column), moved.join(" OR "))
}

/// The three `TEMP` triggers recording writes to `table` in `schema`, each
/// also sparing the keys it touched under `spare`'s identity when given, and
/// holding the row it wrote under that identity's unsent-write marker. The
/// unqualified `_connetto_outbox` and `_connetto_outbox_blob` resolve to
/// `main`, and `_connetto_spare` and `_connetto_shared_holding` to the shared
/// store, the only schemas holding them.
fn triggers(schema: &str, table: &str, columns: &[Column], spare: Option<&str>) -> String {
    let target = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let literal = table.replace('\'', "''");
    let whole = |_: &Column| "1".to_owned();
    let changed = |column: &Column| carried(column, columns);
    let (insert, delete) = (whole, whole);
    [
        (
            "I",
            "INSERT",
            None,
            Some(("NEW", &insert as &dyn Fn(&Column) -> String)),
        ),
        (
            "U",
            "UPDATE",
            Some(("OLD", &changed as &dyn Fn(&Column) -> String)),
            Some(("NEW", &changed as &dyn Fn(&Column) -> String)),
        ),
        (
            "D",
            "DELETE",
            Some(("OLD", &delete as &dyn Fn(&Column) -> String)),
            None,
        ),
    ]
    .into_iter()
    .map(|(op, event, old, new)| {
        let name = trigger_name(op, table);
        let mut spares = String::new();
        if let Some(identity) = spare {
            let identity = identity.replace('\'', "''");
            let lowered = table.to_lowercase().replace('\'', "''");
            for (row, _) in old.into_iter().chain(new) {
                let _ = write!(
                    spares,
                    "INSERT OR IGNORE INTO _connetto_spare (identity, tbl, clause) \
                     VALUES ('{identity}', '{lowered}', {}); ",
                    key_clause(row, columns)
                );
            }
            if let Some((row, _)) = new {
                let _ = write!(
                    spares,
                    "INSERT OR IGNORE INTO _connetto_shared_holding \
                     (identity, holder, tbl, key) \
                     VALUES ('{identity}', '{}', '{lowered}', {}); ",
                    crate::holdings::UNSENT_HOLDER,
                    key_clause(row, columns)
                );
            }
        }
        let mut blobs = String::new();
        for ((row, carry), side) in old.into_iter().zip(["o"]).chain(new.into_iter().zip(["n"])) {
            for column in columns {
                let value = format!("{row}.{}", quote_ident(&column.name));
                let _ = write!(
                    blobs,
                    "INSERT INTO _connetto_outbox_blob (seq, col, side, value) \
                     SELECT (SELECT max(seq) FROM _connetto_outbox), '{}', '{side}', {value} \
                     WHERE typeof({value}) = 'blob' AND {}; ",
                    column.name.replace('\'', "''"),
                    carry(column)
                );
            }
        }
        let old = old.map_or_else(
            || "NULL".to_owned(),
            |(row, carry)| encoded(row, columns, carry),
        );
        let new = new.map_or_else(
            || "NULL".to_owned(),
            |(row, carry)| encoded(row, columns, carry),
        );
        format!(
            "CREATE TEMP TRIGGER {name} AFTER {event} ON {target} \
             WHEN NOT {WRITE_EXEMPTION_FUNCTION}() AND NOT {UNRECORDED_FUNCTION}() BEGIN \
             INSERT INTO _connetto_outbox (tbl, op, old, new) \
             VALUES ('{literal}', '{op}', {old}, {new}); {blobs}{spares}END; "
        )
    })
    .collect::<Vec<_>>()
    .concat()
}

/// The key columns of `columns`, in key order.
pub(crate) fn key_columns(columns: &[Column]) -> Vec<&Column> {
    let mut keyed: Vec<&Column> = columns.iter().filter(|column| column.pk > 0).collect();
    keyed.sort_by_key(|column| column.pk);
    keyed
}

/// The SQL rendering `row`'s key as the clause `record_spare` writes and a
/// holding names, `("a" IS 1 AND "b" IS 'x')`, each value through `quote()`.
pub(crate) fn key_clause(row: &str, columns: &[Column]) -> String {
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
         WHEN 'blob' THEN '{BLOB_MARKER}' \
         WHEN 'real' THEN '{{\"r\":\"' || printf('%!.17g', {value}) || '\"}}' \
         ELSE json_quote({value}) END"
    )
}

/// The SQL building one row's JSON object from the columns `carry` admits.
/// Concatenation rather than `json_object`, so a wide table never meets the
/// function argument limit. Each member starts with a comma, and the first
/// is cut, since a key column is always carried.
fn encoded(row: &str, columns: &[Column], carry: &dyn Fn(&Column) -> String) -> String {
    let members: Vec<String> = columns
        .iter()
        .map(|column| {
            let key = serde_json::Value::from(column.name.as_str())
                .to_string()
                .replace('\'', "''");
            format!(
                "CASE WHEN {} THEN ',{key}:' || {} ELSE '' END",
                carry(column),
                cell(&format!("{row}.{}", quote_ident(&column.name)))
            )
        })
        .collect();
    format!("'{{' || substr({}, 2) || '}}'", members.join(" || "))
}

/// Refuse every `CREATE`, `ALTER` and `DROP` of an application table in
/// `main` on `db` while `open` is down, and of one in the shared store
/// always, since the outbox triggers would no longer match the table. The
/// shared store's tables change only with a release, through the layout
/// programs every identity runs at open.
///
/// Every `DELETE` is answered with `SQLITE_IGNORE`, which runs it row by row
/// rather than through SQLite's truncate optimization, so a `DELETE` without
/// a `WHERE` on a table no trigger watches, a device-private tier table
/// above all, still reports each row to the update hook that drives
/// reactivity. A `DROP` asks for `DELETE` too, on the schema table and on the
/// object it drops, and gives up silently on any answer but `SQLITE_OK`, so
/// those two are admitted.
pub(crate) fn guard_schema(db: &mut SqliteConnection, open: &Arc<AtomicBool>) {
    let open = Arc::clone(open);
    let mut dropping: Option<String> = None;
    db.on_authorize(move |action| {
        let dropped = dropping.take();
        let (database, table) = match action {
            AuthorizerContext::CreateTable(create) => (create.database, create.table),
            AuthorizerContext::AlterTable(alter) => (alter.database, alter.table),
            AuthorizerContext::DropTable(drop) => {
                dropping = drop.table.map(str::to_owned);
                (drop.database, drop.table)
            }
            AuthorizerContext::DropTempTable(drop) => {
                dropping = drop.table.map(str::to_owned);
                return AuthorizerDecision::Allow;
            }
            AuthorizerContext::DropView(drop) => {
                dropping = drop.view.map(str::to_owned);
                return AuthorizerDecision::Allow;
            }
            AuthorizerContext::DropTempView(drop) => {
                dropping = drop.view.map(str::to_owned);
                return AuthorizerDecision::Allow;
            }
            AuthorizerContext::DropVTable(drop) => {
                dropping = drop.table.map(str::to_owned);
                return AuthorizerDecision::Allow;
            }
            AuthorizerContext::Delete(delete) => {
                let table = delete.table.unwrap_or_default();
                let part_of_a_drop = SCHEMA_TABLES
                    .iter()
                    .any(|schema| schema.eq_ignore_ascii_case(table))
                    || dropped.is_some_and(|dropped| dropped.eq_ignore_ascii_case(table));
                return if part_of_a_drop {
                    AuthorizerDecision::Allow
                } else {
                    AuthorizerDecision::Ignore
                };
            }
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

/// The names SQLite gives the schema table a `DROP` deletes from.
const SCHEMA_TABLES: [&str; 4] = [
    "sqlite_master",
    "sqlite_temp_master",
    "sqlite_schema",
    "sqlite_temp_schema",
];

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

/// Delete every recorded write up to and including sequence `through`,
/// with its blob cells.
///
/// # Errors
///
/// [`ClientError`] when the outbox cannot be written.
pub(crate) fn clear_through(db: &mut SqliteConnection, through: i64) -> Result<(), ClientError> {
    diesel::sql_query("DELETE FROM main._connetto_outbox_blob WHERE seq <= ?")
        .bind::<diesel::sql_types::BigInt, _>(through)
        .execute(db)?;
    diesel::sql_query("DELETE FROM main._connetto_outbox WHERE seq <= ?")
        .bind::<diesel::sql_types::BigInt, _>(through)
        .execute(db)?;
    Ok(())
}

/// Whether the outbox holds any write.
///
/// # Errors
///
/// [`ClientError`] when the outbox cannot be read.
pub(crate) fn holds_writes(db: &mut SqliteConnection) -> Result<bool, ClientError> {
    Ok(diesel::select(diesel::dsl::exists(
        _connetto_outbox::table.select(_connetto_outbox::seq),
    ))
    .get_result(db)?)
}

/// The tables the outbox holds writes to that no longer exist in their
/// schema, sorted, so a schema change that drops or renames one is refused.
///
/// # Errors
///
/// [`ClientError`] when the outbox or the catalog cannot be read.
pub(crate) fn stranded_tables(
    db: &mut SqliteConnection,
    shared: &HashSet<String>,
) -> Result<Vec<String>, ClientError> {
    let named: Vec<String> = _connetto_outbox::table
        .select(_connetto_outbox::tbl)
        .distinct()
        .order(_connetto_outbox::tbl)
        .load(db)?;
    let mut stranded = Vec::new();
    for table in named {
        if columns(db, schema_of(shared, &table), &table)?.is_empty() {
            stranded.push(table);
        }
    }
    Ok(stranded)
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
    /// The write's place in the outbox.
    seq: i64,
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
    let rows: Vec<(i64, String, Option<String>, Option<String>)> = _connetto_outbox::table
        .select((
            _connetto_outbox::seq,
            _connetto_outbox::tbl,
            _connetto_outbox::old,
            _connetto_outbox::new,
        ))
        .order(_connetto_outbox::seq)
        .load(db)?;
    Ok(rows
        .into_iter()
        .map(|(seq, tbl, old, new)| Recorded { seq, tbl, old, new })
        .collect())
}

/// One cell, comparable.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Cell {
    fn into_value(self) -> Value<String, Vec<u8>> {
        match self {
            Self::Null => Value::Null,
            Self::Integer(integer) => Value::Integer(integer),
            Self::Real(real) => Value::Real(real),
            Self::Text(text) => Value::Text(text),
            Self::Blob(blob) => Value::Blob(blob),
        }
    }

    /// The cell as stored bytes compare, a real by its bits.
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Real(left), Self::Real(right)) => left.to_bits() == right.to_bits(),
            _ => self == other,
        }
    }
}

/// `cell` as an SQL literal.
fn literal(cell: &Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_owned(),
        Cell::Integer(integer) => integer.to_string(),
        Cell::Real(real) => {
            if real.is_finite() {
                format!("{real:?}")
            } else if *real > 0.0 {
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

/// Which row of a write a value belongs to.
#[derive(Clone, Copy)]
enum Side {
    Old,
    New,
}

impl Side {
    const fn letter(self) -> &'static str {
        match self {
            Self::Old => "o",
            Self::New => "n",
        }
    }
}

/// A blob cell stored beside its outbox row.
#[derive(diesel::QueryableByName)]
struct StoredBlob {
    #[diesel(sql_type = diesel::sql_types::Binary)]
    value: Vec<u8>,
}

/// One encoded value as a cell, reading a blob from `side` of the write
/// `seq` under `column`.
fn decode_value(
    db: &mut SqliteConnection,
    seq: i64,
    side: Side,
    column: &str,
    value: serde_json::Value,
) -> Result<Cell, ClientError> {
    let malformed = || ClientError::Apply(format!("a malformed outbox value in write {seq}"));
    match value {
        serde_json::Value::Null => Ok(Cell::Null),
        serde_json::Value::Number(number) => {
            number.as_i64().map(Cell::Integer).ok_or_else(malformed)
        }
        serde_json::Value::String(text) => Ok(Cell::Text(text)),
        serde_json::Value::Object(object) => {
            if object.contains_key("b") {
                let stored: StoredBlob = diesel::sql_query(
                    "SELECT value FROM main._connetto_outbox_blob \
                     WHERE seq = ? AND col = ? AND side = ?",
                )
                .bind::<diesel::sql_types::BigInt, _>(seq)
                .bind::<diesel::sql_types::Text, _>(column)
                .bind::<diesel::sql_types::Text, _>(side.letter())
                .get_result(db)
                .optional()?
                .ok_or_else(malformed)?;
                Ok(Cell::Blob(stored.value))
            } else if let Some(serde_json::Value::String(real)) = object.get("r") {
                real.parse::<f64>().map(Cell::Real).map_err(|_| malformed())
            } else {
                Err(malformed())
            }
        }
        serde_json::Value::Bool(_) | serde_json::Value::Array(_) => Err(malformed()),
    }
}

/// The cells of one recorded row whose column `want` admits, by column
/// name, a column the row does not carry left out.
fn decode_row(
    db: &mut SqliteConnection,
    seq: i64,
    side: Side,
    text: &str,
    want: impl Fn(&str) -> bool,
) -> Result<HashMap<String, Cell>, ClientError> {
    let object: serde_json::Map<String, serde_json::Value> = serde_json::from_str(text)
        .map_err(|_| ClientError::Apply(format!("a malformed outbox row in write {seq}")))?;
    let mut cells = HashMap::with_capacity(object.len());
    for (column, value) in object {
        if want(&column) {
            let cell = decode_value(db, seq, side, &column, value)?;
            cells.insert(column, cell);
        }
    }
    Ok(cells)
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
        let keyed: Vec<String> = key_columns(&schemas[&row.tbl])
            .into_iter()
            .map(|column| column.name.clone())
            .collect();
        for (side, text) in [(Side::Old, &row.old), (Side::New, &row.new)] {
            let Some(text) = text else { continue };
            let mut values = decode_row(db, row.seq, side, text, |column| {
                keyed.iter().any(|key| key == column)
            })?;
            let wheres: Vec<String> = keyed
                .iter()
                .map(|column| {
                    let cell = values.remove(column).unwrap_or(Cell::Null);
                    format!("{} IS {}", quote_ident(column), literal(&cell))
                })
                .collect();
            let clause = format!("({})", wheres.join(" AND "));
            let table = clauses.entry(row.tbl.to_lowercase()).or_default();
            if !table.contains(&clause) {
                table.push(clause);
            }
        }
    }
    Ok(clauses)
}

/// One cell's storage class and value, read back from SQL.
#[derive(diesel::QueryableByName)]
struct Typed {
    #[diesel(sql_type = diesel::sql_types::Text)]
    kind: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    integer: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    real: Option<f64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    text: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Binary>)]
    blob: Option<Vec<u8>>,
}

/// The value `column` holds in a row recorded before the column existed,
/// its default, which is what the row reads as now.
fn default_cell(db: &mut SqliteConnection, column: &Column) -> Result<Cell, ClientError> {
    let Some(default) = &column.dflt_value else {
        return Ok(Cell::Null);
    };
    let typed: Typed = diesel::sql_query(format!(
        "SELECT typeof(v) AS kind, \
         CASE typeof(v) WHEN 'integer' THEN v END AS integer, \
         CASE typeof(v) WHEN 'real' THEN v END AS real, \
         CASE typeof(v) WHEN 'text' THEN v END AS text, \
         CASE typeof(v) WHEN 'blob' THEN v END AS blob \
         FROM (SELECT ({default}) AS v)"
    ))
    .get_result(db)?;
    Ok(match typed.kind.as_str() {
        "integer" => typed.integer.map_or(Cell::Null, Cell::Integer),
        "real" => typed.real.map_or(Cell::Null, Cell::Real),
        "text" => typed.text.map_or(Cell::Null, Cell::Text),
        "blob" => typed.blob.map_or(Cell::Null, Cell::Blob),
        _ => Cell::Null,
    })
}

/// A recorded table as the upload names it, with its columns.
struct Shape {
    schema: TableSchema<String>,
    columns: Vec<Column>,
    /// Each column's default, read once.
    defaults: Vec<Option<Cell>>,
}

impl Shape {
    /// The table's current shape. The key check refuses a table whose key
    /// has more columns than a changeset's one byte per column can number,
    /// so every key position fits.
    fn read(db: &mut SqliteConnection, schema: &str, table: &str) -> Result<Self, ClientError> {
        let columns = columns(db, schema, table)?;
        let flags = columns
            .iter()
            .map(|column| u8::try_from(column.pk))
            .collect::<Result<Vec<u8>, _>>()
            .map_err(|_| ClientError::Apply(format!("{table} has over 255 key columns")))?;
        let defaults = columns.iter().map(|_| None).collect();
        Ok(Self {
            schema: TableSchema::new(table.to_owned(), columns.len(), flags),
            columns,
            defaults,
        })
    }

    /// `row` in column order, a column it does not carry at its default.
    fn whole(
        &mut self,
        db: &mut SqliteConnection,
        mut row: HashMap<String, Cell>,
    ) -> Result<Vec<Cell>, ClientError> {
        let mut cells = Vec::with_capacity(self.columns.len());
        for (index, column) in self.columns.iter().enumerate() {
            let cell = match row.remove(&column.name) {
                Some(cell) => cell,
                None => {
                    if let Some(cell) = &self.defaults[index] {
                        cell.clone()
                    } else {
                        let cell = default_cell(db, column)?;
                        self.defaults[index] = Some(cell.clone());
                        cell
                    }
                }
            };
            cells.push(cell);
        }
        Ok(cells)
    }

    fn key_of(&self, row: &HashMap<String, Cell>) -> Vec<Option<Cell>> {
        key_columns(&self.columns)
            .into_iter()
            .map(|column| row.get(&column.name).cloned())
            .collect()
    }

    /// Whether an update from `old` to `new` moves the row's key, a key cell
    /// either record lacks counting as moved.
    fn moves_key(&self, old: &HashMap<String, Cell>, new: &HashMap<String, Cell>) -> bool {
        self.key_of(old)
            .iter()
            .zip(&self.key_of(new))
            .any(|(left, right)| {
                left.as_ref()
                    .zip(right.as_ref())
                    .is_none_or(|(l, r)| !l.same(r))
            })
    }
}

/// A changeset built from the outbox, and the last write it covers.
pub(crate) struct Built {
    /// The upload, empty when the writes cancel out.
    pub(crate) changeset: Vec<u8>,
    /// The sequence of the newest write read.
    pub(crate) through: i64,
}

/// Build the changeset the outbox's writes amount to, merged per row in the
/// order they were made as SQLite's session merges them, each table under
/// its physical name and its current columns. A column a row does not carry
/// reads as its default in an insert or a delete and as unchanged in an
/// update, and a column the table no longer has is left out. `None` when the
/// outbox is empty.
///
/// # Errors
///
/// [`ClientError`] when the outbox or the catalog cannot be read, or a record
/// is malformed.
pub(crate) fn build(
    db: &mut SqliteConnection,
    shared: &HashSet<String>,
) -> Result<Option<Built>, ClientError> {
    let writes = recorded(db)?;
    let Some(through) = writes.last().map(|write| write.seq) else {
        return Ok(None);
    };
    let builder_err =
        |err: sqlite_diff_rs::Error| ClientError::Apply(format!("building the upload: {err}"));
    let mut shapes: HashMap<String, Shape> = HashMap::new();
    let mut builder = ChangeSet::<TableSchema<String>, String, Vec<u8>>::new();
    for write in writes {
        let shape = match shapes.entry(write.tbl.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Shape::read(db, schema_of(shared, &write.tbl), &write.tbl)?)
            }
        };
        let known = |column: &str| shape.columns.iter().any(|known| known.name == column);
        let old = write
            .old
            .as_deref()
            .map(|text| decode_row(db, write.seq, Side::Old, text, known))
            .transpose()?;
        let new = write
            .new
            .as_deref()
            .map(|text| decode_row(db, write.seq, Side::New, text, known))
            .transpose()?;
        let (delete, insert, update) = match (old, new) {
            (None, Some(new)) => (None, Some(new), None),
            (Some(old), None) => (Some(old), None, None),
            (Some(old), Some(new)) if shape.moves_key(&old, &new) => (Some(old), Some(new), None),
            (Some(old), Some(new)) => (None, None, Some((old, new))),
            (None, None) => {
                return Err(ClientError::Apply(format!(
                    "outbox write {} records neither an old nor a new row",
                    write.seq
                )));
            }
        };
        if let Some(old) = delete {
            let mut op = ChangeDelete::from(shape.schema.clone());
            for (index, cell) in shape.whole(db, old)?.into_iter().enumerate() {
                op = op.set(index, cell.into_value()).map_err(builder_err)?;
            }
            builder = builder.delete(op);
        }
        if let Some(new) = insert {
            let mut op = Insert::from(shape.schema.clone());
            for (index, cell) in shape.whole(db, new)?.into_iter().enumerate() {
                op = op.set(index, cell.into_value()).map_err(builder_err)?;
            }
            builder = builder.insert(op);
        }
        if let Some((mut old, mut new)) = update {
            let mut op = Update::from(shape.schema.clone());
            let mut changed = false;
            for (index, column) in shape.columns.iter().enumerate() {
                if column.pk > 0 {
                    let key = old.remove(&column.name).unwrap_or(Cell::Null);
                    op = op.set_old(index, key.into_value()).map_err(builder_err)?;
                } else if let (Some(before), Some(after)) =
                    (old.remove(&column.name), new.remove(&column.name))
                    && !before.same(&after)
                {
                    op = op
                        .set(index, before.into_value(), after.into_value())
                        .map_err(builder_err)?;
                    changed = true;
                }
            }
            if changed {
                builder = builder.update(op);
            }
        }
    }
    Ok(Some(Built {
        changeset: builder.build(),
        through,
    }))
}
