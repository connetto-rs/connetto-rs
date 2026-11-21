//! The client resting tables for server-computed answers (R83, R117).
//!
//! Every answer the server computes rests so an offline restart reads the
//! last synced value instead of nothing. Each statistic, one `_connetto_query`
//! row, rests in its own `STRICT` table `_connetto_stat_<query_id>`, keyed by
//! the binds and the row's key. The run-local wire subscription id cannot key
//! it, since those ids are minted fresh every run. The binds travel verbatim
//! in one canonical blob (see [`crate::subscriptions::encode_binds`]), so
//! identity is exact by construction with no hashing.
//!
//! The data columns `c0`, `c1`, ... hold the answer's select list in select
//! order, declared with the types the server sends in
//! [`AggregateColumns`](connetto_core::messages::AggregateColumns). Each cell
//! holds the value as the replica stores the column it comes from, so a handle
//! loads it through diesel with the query's own SQL types. A whole answer
//! rests one row per answer row, keyed by its position, and a scalar is the
//! row at position zero.
//!
//! Written only under capture suspension, like the rest of `_connetto_*`, so
//! it never rides a mutation upload.

use core::fmt::Write as _;

use connetto_core::messages::{AggregateChange, ColumnType, SqliteValue};
use diesel::deserialize::{self, QueryableByName};
use diesel::prelude::*;
use diesel::query_builder::{BoxedSqlQuery, SqlQuery};
use diesel::row::{Field, NamedRow, Row};
use diesel::sql_types::{BigInt, Binary, Double, Nullable, Text};
use diesel::sqlite::{Sqlite, SqliteConnection, SqliteType};

use crate::ClientError;
use crate::live::Loadable;
use crate::subscriptions::{encode_binds, query_text};

/// Drops the single resting table of protocol 6, whose JSON values no handle
/// decodes any more. They return with the server's next push.
pub(crate) const LEGACY_DDL: &str = "DROP TABLE IF EXISTS _connetto_aggregates";

/// The prefix of every statistic's resting table.
const TABLE_PREFIX: &str = "_connetto_stat_";

/// The key columns every resting table starts with, ahead of its data columns.
const KEY_COLUMNS: usize = 3;

/// The resting table of the statistic `query_id` names.
fn table_name(query_id: i32) -> String {
    format!("{TABLE_PREFIX}{query_id}")
}

/// The data column holding the select-list item at `position`.
fn data_column(position: usize) -> String {
    format!("c{position}")
}

/// The data column list of a table with `count` data columns, in select order.
fn data_columns(count: usize) -> String {
    (0..count).map(data_column).collect::<Vec<_>>().join(", ")
}

/// The query-text row id for `query`, or `None` when the text was never
/// recorded, so a reader against an absent statistic is a no-op.
fn query_id_of(conn: &mut SqliteConnection, query: &str) -> QueryResult<Option<i32>> {
    query_text::table
        .filter(query_text::sql.eq(query))
        .select(query_text::id)
        .first(conn)
        .optional()
}

/// One column of a table, as `pragma_table_info` describes it.
#[derive(QueryableByName)]
struct DeclaredColumn {
    /// The declared type, upper case.
    #[diesel(sql_type = Text)]
    r#type: String,
}

/// The declared types of the data columns of `query_id`'s resting table, in
/// select order, or `None` when the table does not exist.
fn declared_types(conn: &mut SqliteConnection, query_id: i32) -> QueryResult<Option<Vec<String>>> {
    let columns: Vec<DeclaredColumn> = diesel::sql_query(format!(
        "SELECT type FROM pragma_table_info('{}') ORDER BY cid",
        table_name(query_id)
    ))
    .load(conn)?;
    if columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        columns
            .into_iter()
            .skip(KEY_COLUMNS)
            .map(|column| column.r#type.to_ascii_uppercase())
            .collect(),
    ))
}

/// Make `query_id`'s resting table hold exactly `columns`, in select order.
///
/// A table already declaring those types is kept with its rows. One declaring
/// others, which a server whose schema moved sends, is dropped and created
/// anew, and its values return with the server's next push. Returns whether
/// the table was created.
///
/// # Errors
///
/// [`ClientError::Session`] when the replica rejects the statements.
pub(crate) fn declare(
    conn: &mut SqliteConnection,
    query_id: i32,
    columns: &[ColumnType],
) -> Result<bool, ClientError> {
    let wanted: Vec<&str> = columns.iter().map(|column| column.sql_name()).collect();
    let current = declared_types(conn, query_id)?;
    if current.as_ref().is_some_and(|current| {
        current
            .iter()
            .map(String::as_str)
            .eq(wanted.iter().copied())
    }) {
        return Ok(false);
    }
    let table = table_name(query_id);
    let mut data = String::new();
    for (position, column) in columns.iter().enumerate() {
        let _ = write!(data, ", {} {}", data_column(position), column.sql_name());
    }
    conn.transaction::<_, ClientError, _>(|conn| {
        if current.is_some() {
            diesel::sql_query(format!("DROP TABLE \"{table}\"")).execute(conn)?;
        }
        diesel::sql_query(format!(
            "CREATE TABLE \"{table}\" (binds BLOB NOT NULL, key BLOB NOT NULL, \
             updated_at INTEGER NOT NULL{data}, PRIMARY KEY (binds, key)) STRICT"
        ))
        .execute(conn)?;
        Ok(())
    })?;
    Ok(true)
}

/// Bind one cell to a boxed raw statement, as the storage class it travels as.
fn bind_cell<'a>(
    query: BoxedSqlQuery<'a, Sqlite, SqlQuery>,
    cell: &SqliteValue,
) -> BoxedSqlQuery<'a, Sqlite, SqlQuery> {
    match cell {
        SqliteValue::Null => query.bind::<Nullable<Text>, _>(None::<String>),
        SqliteValue::Integer(integer) => query.bind::<BigInt, _>(*integer),
        SqliteValue::Real(real) => query.bind::<Double, _>(*real),
        SqliteValue::Text(text) => query.bind::<Text, _>(text.clone()),
        SqliteValue::Blob(bytes) => query.bind::<Binary, _>(bytes.clone()),
    }
}

/// Apply one server answer movement to `query_id`'s resting table, under the
/// `binds` the caller resolved from the frame's subscription.
///
/// # Errors
///
/// [`ClientError::Session`] when the table was never declared, a row does not
/// hold one cell per declared column, or the replica rejects the write.
pub(crate) fn apply(
    conn: &mut SqliteConnection,
    query_id: i32,
    binds: &[SqliteValue],
    change: &AggregateChange,
    now: i64,
) -> Result<(), ClientError> {
    let Some(types) = declared_types(conn, query_id)? else {
        return Err(ClientError::Session(format!(
            "a computed answer for query {query_id} arrived before its columns"
        )));
    };
    let binds_blob = encode_binds(binds);
    let table = table_name(query_id);
    match change {
        AggregateChange::Upsert { key, row } => {
            write_row(conn, &table, types.len(), &binds_blob, key, row, now)
        }
        AggregateChange::Remove { key } => {
            diesel::sql_query(format!(
                "DELETE FROM \"{table}\" WHERE binds = ? AND key = ?"
            ))
            .bind::<Binary, _>(&binds_blob)
            .bind::<Binary, _>(key)
            .execute(conn)?;
            Ok(())
        }
        AggregateChange::Replace { rows } => conn.transaction::<_, ClientError, _>(|conn| {
            diesel::sql_query(format!("DELETE FROM \"{table}\" WHERE binds = ?"))
                .bind::<Binary, _>(&binds_blob)
                .execute(conn)?;
            for (position, row) in rows.iter().enumerate() {
                let key = u64::try_from(position).unwrap_or(u64::MAX).to_be_bytes();
                write_row(conn, &table, types.len(), &binds_blob, &key, row, now)?;
            }
            Ok(())
        }),
    }
}

/// Upsert one row under `key`.
fn write_row(
    conn: &mut SqliteConnection,
    table: &str,
    width: usize,
    binds_blob: &[u8],
    key: &[u8],
    row: &[SqliteValue],
    now: i64,
) -> Result<(), ClientError> {
    if row.len() != width {
        return Err(ClientError::Session(format!(
            "a computed row of {} cells arrived for {table}, which declares {width} columns",
            row.len()
        )));
    }
    let columns = data_columns(width);
    let separator = if width == 0 { "" } else { ", " };
    let places = vec!["?"; width].join(", ");
    let mut updates = String::new();
    for position in 0..width {
        let column = data_column(position);
        let _ = write!(updates, ", {column} = excluded.{column}");
    }
    let mut query = diesel::sql_query(format!(
        "INSERT INTO \"{table}\" (binds, key, updated_at{separator}{columns}) \
         VALUES (?, ?, ?{separator}{places}) ON CONFLICT (binds, key) DO UPDATE SET \
         updated_at = excluded.updated_at{updates}"
    ))
    .into_boxed::<Sqlite>()
    .bind::<Binary, _>(binds_blob.to_vec())
    .bind::<Binary, _>(key.to_vec())
    .bind::<BigInt, _>(now);
    for cell in row {
        query = bind_cell(query, cell);
    }
    query.execute(conn)?;
    Ok(())
}

/// Delete every row of the statistic under `query_id` and `binds`, the whole
/// resting answer a full-resync notice takes before the re-bootstrap rebuilds
/// it (R95).
///
/// # Errors
///
/// [`ClientError::Session`] when the replica rejects the delete.
pub(crate) fn clear(
    conn: &mut SqliteConnection,
    query_id: i32,
    binds: &[SqliteValue],
) -> Result<(), ClientError> {
    if declared_types(conn, query_id)?.is_none() {
        return Ok(());
    }
    diesel::sql_query(format!(
        "DELETE FROM \"{}\" WHERE binds = ?",
        table_name(query_id)
    ))
    .bind::<Binary, _>(encode_binds(binds))
    .execute(conn)?;
    Ok(())
}

/// One rested row: its stored key, its cells loaded as `U`, and when it was
/// last written in seconds since the epoch on the local clock.
pub(crate) type RestedRow<U> = (Vec<u8>, U, i64);

/// Which data columns of a rested row a load reads.
#[derive(Clone, Copy)]
pub(crate) enum Cells {
    /// Every data column, a value or a whole row.
    All,
    /// Every data column but the last, a grouped row's key.
    Init,
    /// The last data column, a grouped row's aggregate.
    Last,
}

impl Cells {
    /// The positions this selection reads out of `width` data columns.
    fn positions(self, width: usize) -> core::ops::Range<usize> {
        match self {
            Self::All => 0..width,
            Self::Init => 0..width.saturating_sub(1),
            Self::Last => width.saturating_sub(1)..width,
        }
    }
}

/// A blob as an SQL literal, so a statement carries it without a bind.
fn blob_literal(bytes: &[u8]) -> String {
    let mut literal = String::with_capacity(bytes.len() * 2 + 3);
    literal.push_str("X'");
    for byte in bytes {
        let _ = write!(literal, "{byte:02X}");
    }
    literal.push('\'');
    literal
}

/// The rested rows of the statistic for `query` under `binds`, every row or
/// only the one stored under `key`, with the `cells` they hold loaded as
/// `U` from their SQL type `ST`. Rows come in stored key order, which is
/// answer order for a whole answer. An unrecorded query or an undeclared
/// table holds no rows.
///
/// # Errors
///
/// [`ClientError::Session`] when the replica cannot be read, when the
/// selected columns are not as many as `U` reads, or when a cell does not
/// load as `U`.
pub(crate) fn load<ST, U: Loadable<ST>>(
    conn: &mut SqliteConnection,
    query: &str,
    binds: &[SqliteValue],
    key: Option<&[u8]>,
    cells: Cells,
) -> Result<Vec<RestedRow<U>>, ClientError> {
    let Some(query_id) = query_id_of(conn, query)? else {
        return Ok(Vec::new());
    };
    let Some(types) = declared_types(conn, query_id)? else {
        return Ok(Vec::new());
    };
    let positions = cells.positions(types.len());
    if positions.len() != U::WIDTH {
        return Err(ClientError::Session(format!(
            "the rested answer of {query} holds {} columns, the watched type reads {}",
            positions.len(),
            U::WIDTH
        )));
    }
    let mut filter = format!(
        "FROM \"{}\" WHERE binds = {}",
        table_name(query_id),
        blob_literal(&encode_binds(binds))
    );
    if let Some(key) = key {
        filter.push_str(" AND key = ");
        filter.push_str(&blob_literal(key));
    }
    filter.push_str(" ORDER BY key");
    let stored: Vec<(Vec<u8>, i64)> =
        diesel::dsl::sql::<(Binary, BigInt)>(&format!("SELECT key, updated_at {filter}"))
            .load(conn)?;
    let columns = positions.map(data_column).collect::<Vec<_>>().join(", ");
    let loaded = U::load_all(conn, &format!("SELECT {columns} {filter}"))?;
    Ok(stored
        .into_iter()
        .zip(loaded)
        .map(|((key, at), row)| (key, row, at))
        .collect())
}

/// A rested answer as the server sent it: the declared column types and every
/// row with its stored key, in key order. A relay replays it to a tab as the
/// frames the server would have sent.
#[derive(Debug, Clone, PartialEq)]
pub struct RestedAnswer {
    /// One declared type per select-list item, in select order.
    pub columns: Vec<ColumnType>,
    /// Every row, as its stored key and its cells in select order.
    pub rows: Vec<(Vec<u8>, Vec<SqliteValue>)>,
}

/// The column type a `STRICT` table declares under `name`.
fn column_type(name: &str) -> Result<ColumnType, ClientError> {
    [
        ColumnType::Integer,
        ColumnType::Real,
        ColumnType::Text,
        ColumnType::Blob,
        ColumnType::Any,
    ]
    .into_iter()
    .find(|column| column.sql_name() == name)
    .ok_or_else(|| ClientError::Session(format!("a resting table declares type {name}")))
}

/// One rested row read cell by cell in its storage class: the key first,
/// then the data columns.
struct RawRow(Vec<SqliteValue>);

impl QueryableByName<Sqlite> for RawRow {
    fn build<'a>(row: &impl NamedRow<'a, Sqlite>) -> deserialize::Result<Self> {
        let mut cells = Vec::with_capacity(row.field_count());
        for index in 0..row.field_count() {
            let field = Row::get(row, index).ok_or("a rested row lost a column")?;
            cells.push(match field.value() {
                None => SqliteValue::Null,
                Some(mut value) => match value.value_type() {
                    None => SqliteValue::Null,
                    Some(SqliteType::Text) => SqliteValue::Text(value.read_text().to_owned()),
                    Some(SqliteType::Binary) => SqliteValue::Blob(value.read_blob().to_vec()),
                    Some(SqliteType::Double | SqliteType::Float) => {
                        SqliteValue::Real(value.read_double())
                    }
                    Some(SqliteType::Long | SqliteType::Integer | SqliteType::SmallInt) => {
                        SqliteValue::Integer(value.read_long())
                    }
                },
            });
        }
        Ok(Self(cells))
    }
}

/// The rested answer of the statistic for `query` under `binds` as stored,
/// or `None` when its table was never declared.
///
/// # Errors
///
/// [`ClientError::Session`] when the replica cannot be read.
pub(crate) fn answer(
    conn: &mut SqliteConnection,
    query: &str,
    binds: &[SqliteValue],
) -> Result<Option<RestedAnswer>, ClientError> {
    let Some(query_id) = query_id_of(conn, query)? else {
        return Ok(None);
    };
    let Some(types) = declared_types(conn, query_id)? else {
        return Ok(None);
    };
    let columns = types
        .iter()
        .map(|name| column_type(name))
        .collect::<Result<Vec<_>, _>>()?;
    let raw: Vec<RawRow> = diesel::sql_query(format!(
        "SELECT key, {} FROM \"{}\" WHERE binds = ? ORDER BY key",
        data_columns(types.len()),
        table_name(query_id)
    ))
    .bind::<Binary, _>(encode_binds(binds))
    .load(conn)?;
    let mut rows = Vec::with_capacity(raw.len());
    for RawRow(mut cells) in raw {
        let SqliteValue::Blob(key) = cells.remove(0) else {
            return Err(ClientError::Session(format!(
                "a rested row of {query} has a key that is not a blob"
            )));
        };
        rows.push((key, cells));
    }
    Ok(Some(RestedAnswer { columns, rows }))
}

/// Whether `query_id`'s resting table holds any row.
///
/// # Errors
///
/// [`ClientError::Session`] when the replica cannot be read.
pub(crate) fn holds_rows(conn: &mut SqliteConnection, query_id: i32) -> Result<bool, ClientError> {
    if declared_types(conn, query_id)?.is_none() {
        return Ok(false);
    }
    Ok(
        diesel::select(diesel::dsl::sql::<diesel::sql_types::Bool>(&format!(
            "EXISTS (SELECT 1 FROM \"{}\")",
            table_name(query_id)
        )))
        .get_result(conn)?,
    )
}

/// Drop `query_id`'s resting table, if it exists.
///
/// # Errors
///
/// [`ClientError::Session`] when the replica rejects the statement.
pub(crate) fn drop_table(conn: &mut SqliteConnection, query_id: i32) -> Result<(), ClientError> {
    diesel::sql_query(format!("DROP TABLE IF EXISTS \"{}\"", table_name(query_id)))
        .execute(conn)?;
    Ok(())
}

/// One name from `sqlite_master`.
#[derive(QueryableByName)]
struct TableName {
    /// The table's name.
    #[diesel(sql_type = Text)]
    name: String,
}

/// The query id of every statistic that has a resting table.
fn rested_queries(conn: &mut SqliteConnection) -> Result<Vec<i32>, ClientError> {
    let tables: Vec<TableName> = diesel::sql_query(format!(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '{TABLE_PREFIX}%'"
    ))
    .load(conn)?;
    Ok(tables
        .into_iter()
        .filter_map(|table| table.name.strip_prefix(TABLE_PREFIX)?.parse().ok())
        .collect())
}

/// Drop every statistic's resting table, the whole rested state a new shared
/// generation invalidates. The next [`declare`] recreates each one.
///
/// # Errors
///
/// [`ClientError::Session`] when the replica cannot be read or written.
pub(crate) fn drop_all(conn: &mut SqliteConnection) -> Result<(), ClientError> {
    for query_id in rested_queries(conn)? {
        drop_table(conn, query_id)?;
    }
    Ok(())
}

/// One statistic's binds and its newest write.
#[derive(QueryableByName)]
struct Recent {
    /// The statistic's binds blob.
    #[diesel(sql_type = Binary)]
    binds: Vec<u8>,
    /// When its newest row was written.
    #[diesel(sql_type = BigInt)]
    updated_at: i64,
}

/// Evict rested statistics down to `cap`, oldest updated first, never one that
/// is currently watched.
///
/// A statistic is one query plus binds, however many rows it holds, and its
/// age is that of its most recent row. `protected` names the (query id,
/// canonical binds) of every subscription still on record, the ones a handle
/// still watches, so eviction spares them: a live handle must keep reading its
/// value through a restart-crossing table that never drops it. When every
/// statistic over the cap is watched the tables stay over the cap rather than
/// evicting a value in use.
///
/// # Errors
///
/// [`ClientError::Session`] when the replica cannot be read or written.
pub(crate) fn enforce_cap(
    conn: &mut SqliteConnection,
    cap: usize,
    protected: &[(i32, Vec<u8>)],
) -> Result<(), ClientError> {
    let mut stats: Vec<(i32, Vec<u8>, i64)> = Vec::new();
    for query_id in rested_queries(conn)? {
        let recent: Vec<Recent> = diesel::sql_query(format!(
            "SELECT binds, max(updated_at) AS updated_at FROM \"{}\" GROUP BY binds",
            table_name(query_id)
        ))
        .load(conn)?;
        stats.extend(
            recent
                .into_iter()
                .map(|recent| (query_id, recent.binds, recent.updated_at)),
        );
    }
    if stats.len() <= cap {
        return Ok(());
    }
    let protected: std::collections::HashSet<&(i32, Vec<u8>)> = protected.iter().collect();
    // Oldest most-recent update first, so the statistic used longest ago goes
    // before a fresher one.
    stats.sort_by_key(|(_, _, recent)| *recent);
    let mut over = stats.len() - cap;
    for (query_id, binds_blob, _) in stats {
        if over == 0 {
            break;
        }
        if protected.contains(&(query_id, binds_blob.clone())) {
            continue;
        }
        diesel::sql_query(format!(
            "DELETE FROM \"{}\" WHERE binds = ?",
            table_name(query_id)
        ))
        .bind::<Binary, _>(&binds_blob)
        .execute(conn)?;
        over -= 1;
    }
    Ok(())
}
