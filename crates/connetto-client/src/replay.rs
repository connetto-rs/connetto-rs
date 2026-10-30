//! Replay a diffset as statements, each table into the schema its caller
//! routes it to, under one of the native apply's conflict policies.
//!
//! `sqlite3changeset_apply` writes into `main` only and skips a table that
//! lives in an attached schema while reporting success.

use std::collections::HashMap;

use diesel::SqliteConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::query_builder::{BoxedSqlQuery, SqlQuery};
use diesel::sql_types::{BigInt, Binary, Double, Nullable, Text};
use sqlite_diff_rs::{
    ChangesetFormat, ChangesetOp, ChangesetUpdatePair, DiffSet, DynTable, ParsedDiffSet,
    PatchsetFormat, PatchsetOp, TableSchema, Value,
};

// The op the diffs this build parses carry, borrowing the parsed diff.
#[cfg(feature = "peer")]
pub(crate) type PeerOp<'a> = ChangesetOp<'a, TableSchema<String>, String, Vec<u8>>;

/// A parsed peer entry body, the changeset its ops borrow from.
#[cfg(feature = "peer")]
pub(crate) type PeerChangeset = DiffSet<ChangesetFormat, TableSchema<String>, String, Vec<u8>>;

use crate::ClientError;
use crate::quote_ident;

/// Savepoint bounding one replay, mirroring the native apply's
/// `changeset_apply` savepoint (sqlite3.c `sessionChangesetApply`).
const REPLAY_SAVEPOINT: &str = "_connetto_replay";

/// One value as a diffset carries it.
type Cell = Value<String, Vec<u8>>;

/// How a replay settles a conflict, each the outcome of one native handler.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Policy {
    /// `server_wins`, skipping a missing or differently shaped table.
    ServerWins,
    /// `rollback_omit`, skipping a missing or differently shaped table.
    Omit,
    /// `ConflictAction::Abort`, refusing a missing or differently shaped table.
    Abort,
}

/// Replay `bytes` under `policy`, each table into the schema `schema_of`
/// names, leaving a table it names no schema for alone.
///
/// The caller's `defer_foreign_keys` survives, because the local tier defers
/// its references for the whole transaction.
pub(crate) fn apply(
    conn: &mut SqliteConnection,
    bytes: &[u8],
    policy: Policy,
    schema_of: &dyn Fn(&str) -> Option<&'static str>,
) -> Result<(), ClientError> {
    let parsed = ParsedDiffSet::parse(bytes)
        .map_err(|err| ClientError::Apply(format!("parsing a diffset to replay: {err:?}")))?;
    let deferring = defers_foreign_keys(conn)?;
    conn.batch_execute(&format!("SAVEPOINT {REPLAY_SAVEPOINT}"))?;
    conn.batch_execute("PRAGMA defer_foreign_keys = 1")?;
    let result = match (&parsed, policy) {
        (ParsedDiffSet::Changeset(diff), Policy::ServerWins) => {
            replay_changeset(conn, diff, schema_of)
        }
        (ParsedDiffSet::Patchset(diff), Policy::ServerWins) => {
            replay_patchset(conn, diff, schema_of)
        }
        (ParsedDiffSet::Changeset(diff), Policy::Omit | Policy::Abort) => {
            replay_local(conn, diff, policy, schema_of)
        }
        (ParsedDiffSet::Patchset(_), Policy::Omit | Policy::Abort) => Err(ClientError::Apply(
            "a local write must be a changeset, not a patchset".to_owned(),
        )),
    };
    // Neither cleanup may replace the replay's own outcome.
    if !deferring {
        let _ = conn.batch_execute("PRAGMA defer_foreign_keys = 0");
    }
    if result.is_ok() {
        conn.batch_execute(&format!("RELEASE {REPLAY_SAVEPOINT}"))?;
    } else {
        let _ = conn.batch_execute(&format!("ROLLBACK TO {REPLAY_SAVEPOINT}"));
        let _ = conn.batch_execute(&format!("RELEASE {REPLAY_SAVEPOINT}"));
    }
    result
}

/// Replay one changeset under `ConflictAction::Abort`'s rule, each table into
/// the schema `schema_of` names, writing the sections in the changeset's
/// order.
///
/// # Errors
///
/// [`ClientError::Apply`] when a row is gone or no longer holds what the
/// writer saw, an insert meets an occupied key, a statement fails, or a table
/// is missing or shaped differently, and [`ClientError::Db`] when the
/// replay's savepoint cannot be opened or released.
pub fn replay_aborting(
    conn: &mut SqliteConnection,
    changeset: &[u8],
    schema_of: &dyn Fn(&str) -> &'static str,
) -> Result<(), ClientError> {
    apply(conn, changeset, Policy::Abort, &|table| {
        Some(schema_of(table))
    })
}

#[derive(diesel::QueryableByName)]
struct DeferRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    defer_foreign_keys: i32,
}

/// Whether the connection already defers its foreign keys.
fn defers_foreign_keys(conn: &mut SqliteConnection) -> Result<bool, ClientError> {
    let row: DeferRow = diesel::sql_query("PRAGMA defer_foreign_keys").get_result(conn)?;
    Ok(row.defer_foreign_keys != 0)
}

/// The local shape of a table as the native apply sees it: the effective
/// column names and primary-key ordinals, with a synthetic rowid slot first
/// when the table declares no primary key (sqlite3.c `sessionTableInfo`).
struct TableShape {
    columns: Vec<String>,
    pk: Vec<u8>,
}

/// One table a replay writes, with the schema it was routed to.
struct Target {
    schema: &'static str,
    shape: TableShape,
}

#[derive(diesel::QueryableByName)]
struct ColumnRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pk: i32,
}

/// One table's or view's declared columns in `schema`, declaration order,
/// empty when it is absent.
fn declared_columns(
    conn: &mut SqliteConnection,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnRow>, ClientError> {
    Ok(diesel::sql_query(format!(
        "SELECT name, pk FROM {}.pragma_table_info(?) ORDER BY cid",
        quote_ident(schema)
    ))
    .bind::<Text, _>(table)
    .load(conn)?)
}

/// Read one table's effective shape in `schema`, or `None` when it is absent.
fn table_shape(
    conn: &mut SqliteConnection,
    schema: &str,
    table: &str,
) -> Result<Option<TableShape>, ClientError> {
    let rows = declared_columns(conn, schema, table)?;
    if rows.is_empty() {
        return Ok(None);
    }
    let has_pk = rows.iter().any(|row| row.pk != 0);
    let mut columns = Vec::with_capacity(rows.len() + usize::from(!has_pk));
    let mut pk = Vec::with_capacity(rows.len() + usize::from(!has_pk));
    if !has_pk {
        // No declared primary key: the native apply keys the table by its
        // implicit rowid, which it models as a column of its own ahead of the
        // declared ones, so a diffset that names no key can never match it.
        columns.push("rowid".to_owned());
        pk.push(1);
    }
    for row in rows {
        columns.push(row.name);
        pk.push(u8::try_from(row.pk).map_err(|_| {
            ClientError::Apply(format!("{table} declares an out-of-range key ordinal"))
        })?);
    }
    Ok(Some(TableShape { columns, pk }))
}

/// Whether the section's width and primary-key ordinals fit the local
/// shape. A table is skipped by the native apply when it is absent, narrower
/// than the section, missing a primary-key column the section carries, or
/// disagreeing on the primary-key flags (sqlite3.c `sessionChangesetApply`).
fn shape_matches(shape: &TableShape, section_ncol: usize, section_pk: &[u8]) -> bool {
    let local_ncol = shape.columns.len();
    if local_ncol == 0 || local_ncol < section_ncol {
        return false;
    }
    let mut min_col = 0;
    for (i, &flag) in shape.pk.iter().enumerate() {
        if flag != 0 {
            min_col = i + 1;
        }
    }
    if section_ncol < min_col {
        return false;
    }
    shape.pk[..section_ncol] == section_pk[..section_ncol]
}

/// The routed tables of one diffset and their shapes, keyed by lowercased name.
fn targets<'a>(
    conn: &mut SqliteConnection,
    tables: impl Iterator<Item = &'a TableSchema<String>>,
    policy: Policy,
    schema_of: &dyn Fn(&str) -> Option<&'static str>,
) -> Result<HashMap<String, Target>, ClientError> {
    let mut targets = HashMap::new();
    for table in tables {
        let name = table.name();
        let key = name.to_lowercase();
        if targets.contains_key(&key) {
            continue;
        }
        let Some(schema) = schema_of(name) else {
            continue;
        };
        let shape = table_shape(conn, schema, name)?;
        let fits = shape
            .as_ref()
            .is_some_and(|shape| shape_matches(shape, table.number_of_columns(), table.pk_flags()));
        match (shape, fits) {
            (Some(shape), true) => {
                targets.insert(key, Target { schema, shape });
            }
            (None, _) if policy == Policy::Abort => {
                return Err(ClientError::Apply(format!(
                    "no such table: {schema}.{name}"
                )));
            }
            (Some(_), false) if policy == Policy::Abort => {
                return Err(ClientError::Apply(format!(
                    "{schema}.{name}: the changeset's columns do not match the table"
                )));
            }
            _ => {}
        }
    }
    Ok(targets)
}

fn replay_changeset(
    conn: &mut SqliteConnection,
    diff: &DiffSet<ChangesetFormat, TableSchema<String>, String, Vec<u8>>,
    schema_of: &dyn Fn(&str) -> Option<&'static str>,
) -> Result<(), ClientError> {
    let targets = targets(conn, diff.tables(), Policy::ServerWins, schema_of)?;
    for op in diff.iter() {
        let table = op.table();
        let Some(target) = targets.get(&table.name().to_lowercase()) else {
            continue;
        };
        let section_pk = table.pk_flags();
        match op {
            ChangesetOp::Insert { values, .. } => {
                let key_vals = pk_values_by_column(section_pk, |i| values[i].clone());
                apply_insert(conn, target, table.name(), values, &key_vals)?;
            }
            ChangesetOp::Update { values, .. } => {
                let set_cols = values
                    .iter()
                    .enumerate()
                    .filter(|(_, (old, new))| is_changed(old.as_ref(), new.as_ref()))
                    .map(|(i, (_, new))| {
                        (
                            i,
                            new.clone().expect("a changed column carries a new value"),
                        )
                    })
                    .collect::<Vec<_>>();
                let key_vals = pk_values_by_column(section_pk, |i| {
                    values[i]
                        .0
                        .clone()
                        .or_else(|| values[i].1.clone())
                        .unwrap_or(Value::Null)
                });
                apply_update(conn, target, table.name(), &set_cols, &key_vals)?;
            }
            ChangesetOp::Delete { old_values, .. } => {
                let key_vals = pk_values_by_column(section_pk, |i| old_values[i].clone());
                delete_by_key(conn, target, table.name(), &key_vals)?;
            }
        }
    }
    Ok(())
}

fn replay_patchset(
    conn: &mut SqliteConnection,
    diff: &DiffSet<PatchsetFormat, TableSchema<String>, String, Vec<u8>>,
    schema_of: &dyn Fn(&str) -> Option<&'static str>,
) -> Result<(), ClientError> {
    let targets = targets(conn, diff.tables(), Policy::ServerWins, schema_of)?;
    for op in diff.iter() {
        let table = op.table();
        let Some(target) = targets.get(&table.name().to_lowercase()) else {
            continue;
        };
        let section_pk = table.pk_flags();
        match op {
            PatchsetOp::Insert { values, .. } => {
                let key_vals = pk_values_by_column(section_pk, |i| values[i].clone());
                apply_insert(conn, target, table.name(), values, &key_vals)?;
            }
            PatchsetOp::Update { pk, entries, .. } => {
                let set_cols = entries
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| section_pk[*i] == 0)
                    .filter_map(|(i, ((), new))| new.clone().map(|value| (i, value)))
                    .collect::<Vec<_>>();
                let key_vals = patchset_key_vals(section_pk, pk);
                apply_update(conn, target, table.name(), &set_cols, &key_vals)?;
            }
            PatchsetOp::Delete { pk, .. } => {
                let key_vals = patchset_key_vals(section_pk, pk);
                delete_by_key(conn, target, table.name(), &key_vals)?;
            }
        }
    }
    Ok(())
}

/// Replay a locally captured changeset under the omit or the abort policy.
///
/// Every old value the changeset carries is in the predicate, so only one
/// affected row means the row still held what the writer saw.
fn replay_local(
    conn: &mut SqliteConnection,
    diff: &DiffSet<ChangesetFormat, TableSchema<String>, String, Vec<u8>>,
    policy: Policy,
    schema_of: &dyn Fn(&str) -> Option<&'static str>,
) -> Result<(), ClientError> {
    let targets = targets(conn, diff.tables(), policy, schema_of)?;
    for op in diff.iter() {
        let table = op.table().name();
        let Some(target) = targets.get(&table.to_lowercase()) else {
            continue;
        };
        let (sql, binds) = match &op {
            ChangesetOp::Insert { values, .. } => render_insert(target, table, values),
            ChangesetOp::Update { values, .. } => render_update(target, table, values)?,
            ChangesetOp::Delete { old_values, .. } => render_delete(target, table, old_values),
        };
        let mut query = diesel::sql_query(sql).into_boxed::<diesel::sqlite::Sqlite>();
        for value in &binds {
            query = bind_one(query, value);
        }
        match (query.execute(conn), policy) {
            (Ok(1), _) => {}
            (Ok(_), Policy::Abort) => {
                return Err(ClientError::Apply(format!(
                    "conflict on {table}: the row was not there or no longer held what the writer saw"
                )));
            }
            (Err(err), Policy::Abort) => return Err(ClientError::Apply(err.to_string())),
            (Ok(_), _) => {}
            (Err(err), _) if omit_on_constraint(&err) => {}
            (Err(err), _) => return Err(err.into()),
        }
    }
    Ok(())
}

/// One rendered statement and the values to bind, in order.
type Rendered = (String, Vec<Cell>);

/// One `column IS ?` term, or `column IS NULL` when the value is null. A null
/// is written rather than bound, because a bind carries no type to compare.
fn predicate_term(terms: &mut Vec<String>, binds: &mut Vec<Cell>, column: &str, value: &Cell) {
    if matches!(value, Value::Null) {
        terms.push(format!("{} IS NULL", quote_ident(column)));
    } else {
        terms.push(format!("{} IS ?", quote_ident(column)));
        binds.push(value.clone());
    }
}

fn render_insert(target: &Target, table: &str, values: &[Cell]) -> Rendered {
    let columns = target
        .shape
        .columns
        .iter()
        .take(values.len())
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    (
        format!(
            "INSERT INTO {} ({columns}) VALUES ({})",
            table_ref(target.schema, table),
            vec!["?"; values.len()].join(", ")
        ),
        values.to_vec(),
    )
}

fn render_update(
    target: &Target,
    table: &str,
    values: &[ChangesetUpdatePair<String, Vec<u8>>],
) -> Result<Rendered, ClientError> {
    let mut assignments = Vec::new();
    let mut binds = Vec::new();
    let mut terms = Vec::new();
    let mut predicate_binds = Vec::new();
    for (column, (old, new)) in target.shape.columns.iter().zip(values) {
        if let Some(new) = new {
            assignments.push(format!("{} = ?", quote_ident(column)));
            binds.push(new.clone());
        }
        if let Some(old) = old {
            predicate_term(&mut terms, &mut predicate_binds, column, old);
        }
    }
    if assignments.is_empty() || terms.is_empty() {
        return Err(ClientError::Apply(format!(
            "{table}: an update with nothing to set or nothing to match"
        )));
    }
    binds.extend(predicate_binds);
    Ok((
        format!(
            "UPDATE {} SET {} WHERE {}",
            table_ref(target.schema, table),
            assignments.join(", "),
            terms.join(" AND ")
        ),
        binds,
    ))
}

fn render_delete(target: &Target, table: &str, old_values: &[Cell]) -> Rendered {
    let mut terms = Vec::new();
    let mut binds = Vec::new();
    for (column, value) in target.shape.columns.iter().zip(old_values) {
        predicate_term(&mut terms, &mut binds, column, value);
    }
    (
        format!(
            "DELETE FROM {} WHERE {}",
            table_ref(target.schema, table),
            terms.join(" AND ")
        ),
        binds,
    )
}

/// Bind one value to a boxed SQL query by its own storage class, so a blob
/// stays a blob and text holding a NUL survives.
fn bind_one<'a>(
    query: BoxedSqlQuery<'a, diesel::sqlite::Sqlite, SqlQuery>,
    value: &Cell,
) -> BoxedSqlQuery<'a, diesel::sqlite::Sqlite, SqlQuery> {
    match value {
        Value::Null => query.bind::<Nullable<Text>, _>(None::<String>),
        Value::Integer(number) => query.bind::<BigInt, _>(*number),
        Value::Real(number) => query.bind::<Double, _>(*number),
        Value::Text(text) => query.bind::<Text, _>(text.clone()),
        Value::Blob(bytes) => query.bind::<Binary, _>(bytes.clone()),
    }
}

/// The primary-key conjunction of one operation, in the order the keys were
/// collected.
fn where_clause(shape: &TableShape, key_vals: &[(usize, Cell)]) -> String {
    key_vals
        .iter()
        .map(|(i, _)| format!("{} = ?", quote_ident(&shape.columns[*i])))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The statement deleting the row the key names.
fn render_key_delete(target: &Target, table: &str, key_vals: &[(usize, Cell)]) -> Rendered {
    (
        format!(
            "DELETE FROM {} WHERE {}",
            table_ref(target.schema, table),
            where_clause(&target.shape, key_vals)
        ),
        key_vals.iter().map(|(_, value)| value.clone()).collect(),
    )
}

/// Delete the row the key names, a no-op when it is absent. A row-level
/// constraint failure is dropped like the native apply does.
fn delete_by_key(
    conn: &mut SqliteConnection,
    target: &Target,
    table: &str,
    key_vals: &[(usize, Cell)],
) -> Result<(), ClientError> {
    let (sql, binds) = render_key_delete(target, table, key_vals);
    let mut query = diesel::sql_query(sql).into_boxed::<diesel::sqlite::Sqlite>();
    for value in &binds {
        query = bind_one(query, value);
    }
    match query.execute(conn) {
        Ok(_) => Ok(()),
        Err(err) if omit_on_constraint(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Insert one row, replacing the row its key names, under the native apply's
/// conflict outcome.
fn apply_insert(
    conn: &mut SqliteConnection,
    target: &Target,
    table: &str,
    values: &[Cell],
    key_vals: &[(usize, Cell)],
) -> Result<(), ClientError> {
    delete_by_key(conn, target, table, key_vals)?;
    let (sql, binds) = render_insert(target, table, values);
    let mut query = diesel::sql_query(sql).into_boxed::<diesel::sqlite::Sqlite>();
    for value in &binds {
        query = bind_one(query, value);
    }
    match query.execute(conn) {
        Ok(_) => Ok(()),
        Err(err) if omit_on_constraint(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Update the carried columns of the row the key names, a no-op when the key
/// is absent or nothing was carried.
fn apply_update(
    conn: &mut SqliteConnection,
    target: &Target,
    table: &str,
    set_cols: &[(usize, Cell)],
    key_vals: &[(usize, Cell)],
) -> Result<(), ClientError> {
    if set_cols.is_empty() {
        return Ok(());
    }
    let set_sql = set_cols
        .iter()
        .map(|(i, _)| format!("{} = ?", quote_ident(&target.shape.columns[*i])))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "UPDATE {} SET {} WHERE {}",
        table_ref(target.schema, table),
        set_sql,
        where_clause(&target.shape, key_vals)
    );
    let mut query = diesel::sql_query(sql).into_boxed::<diesel::sqlite::Sqlite>();
    for (_, value) in set_cols {
        query = bind_one(query, value);
    }
    for (_, value) in key_vals {
        query = bind_one(query, value);
    }
    match query.execute(conn) {
        Ok(_) => Ok(()),
        Err(err) if omit_on_constraint(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Whether a statement error is a row-level constraint the server-wins and
/// omit policies drop rather than fail on: a uniqueness, not-null, or check
/// failure. Foreign keys are deferred, so they never surface mid-statement,
/// and anything else is a hard failure.
fn omit_on_constraint(err: &diesel::result::Error) -> bool {
    use diesel::result::{DatabaseErrorKind, Error};
    matches!(
        err,
        Error::DatabaseError(
            DatabaseErrorKind::UniqueViolation
                | DatabaseErrorKind::NotNullViolation
                | DatabaseErrorKind::CheckViolation,
            _
        )
    )
}

/// The qualified name of a table in `schema`.
fn table_ref(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

/// A changeset (old, new) pair changed, by the same rule the wire uses:
/// undefined-new is unchanged, both-present changed on difference, and
/// new-without-old is changed.
fn is_changed(old: Option<&Cell>, new: Option<&Cell>) -> bool {
    match (old, new) {
        (_, None) => false,
        (Some(o), Some(n)) => o != n,
        (None, Some(_)) => true,
    }
}

/// The primary-key columns and their values, in section-column order,
/// reading the value at each column from `value_at`.
fn pk_values_by_column<F>(section_pk: &[u8], value_at: F) -> Vec<(usize, Cell)>
where
    F: Fn(usize) -> Cell,
{
    section_pk
        .iter()
        .enumerate()
        .filter(|(_, flag)| **flag != 0)
        .map(|(i, _)| (i, value_at(i)))
        .collect()
}

/// The primary-key columns and their values, in section-column order, from a
/// patchset's key-ordered `pk` list.
fn patchset_key_vals(section_pk: &[u8], pk: &[Cell]) -> Vec<(usize, Cell)> {
    section_pk
        .iter()
        .enumerate()
        .filter(|(_, flag)| **flag != 0)
        .map(|(i, &flag)| (i, pk[usize::from(flag - 1)].clone()))
        .collect()
}

/// Whether a write through a view stood, the caller's refusal.
#[cfg(feature = "peer")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerVerdict {
    /// The row stands as the write left it.
    Landed,
    /// A constraint or a guard stopped the write, or the row stands otherwise.
    Refused,
}

/// The view `table` as a peer write addresses it, its columns in the order
/// the section names them. The section carries the key, so the shape holds
/// no key flags and no synthetic rowid slot, which a view never declares.
#[cfg(feature = "peer")]
fn view_target(conn: &mut SqliteConnection, table: &str) -> Result<Target, ClientError> {
    let rows = declared_columns(conn, "main", table)?;
    if rows.is_empty() {
        return Err(ClientError::Apply(format!("no such table: main.{table}")));
    }
    Ok(Target {
        schema: "main",
        shape: TableShape {
            pk: vec![0; rows.len()],
            columns: rows.into_iter().map(|row| row.name).collect(),
        },
    })
}

/// Run one peer statement, `false` when a constraint or a policy guard
/// refused it.
#[cfg(feature = "peer")]
fn execute_peer(conn: &mut SqliteConnection, (sql, binds): Rendered) -> Result<bool, ClientError> {
    let mut query = diesel::sql_query(sql).into_boxed::<diesel::sqlite::Sqlite>();
    for value in &binds {
        query = bind_one(query, value);
    }
    match query.execute(conn) {
        Ok(_) => Ok(true),
        Err(err) if omit_on_constraint(&err) => Ok(false),
        Err(diesel::result::Error::DatabaseError(_, info))
            if pg2sqlite::prelude::is_policy_refusal(info.message()) =>
        {
            Ok(false)
        }
        Err(err) => Err(err.into()),
    }
}

/// One row write through a view: a peer op as it was captured, or the
/// write that puts its row back.
#[cfg(feature = "peer")]
enum ViewWrite<'a> {
    /// Replace the row the key names with these values.
    Insert(&'a [Cell]),
    /// Move the carried columns from the pair's old values to its new ones.
    Update(std::borrow::Cow<'a, [ChangesetUpdatePair<String, Vec<u8>>]>),
    /// Delete the row holding these values.
    Delete(&'a [Cell]),
}

#[cfg(feature = "peer")]
impl<'a> ViewWrite<'a> {
    /// The write `op` makes.
    fn of(op: &PeerOp<'a>) -> Self {
        match *op {
            ChangesetOp::Insert { values, .. } => Self::Insert(values),
            ChangesetOp::Update { values, .. } => Self::Update(std::borrow::Cow::Borrowed(values)),
            ChangesetOp::Delete { old_values, .. } => Self::Delete(old_values),
        }
    }

    /// The write that puts back the row `op` changed: an insert's row goes,
    /// a delete's old row returns, an update's old values stand again.
    fn undoing(op: &PeerOp<'a>) -> Self {
        match *op {
            ChangesetOp::Insert { values, .. } => Self::Delete(values),
            ChangesetOp::Update { values, .. } => Self::Update(std::borrow::Cow::Owned(
                values
                    .iter()
                    .map(|(old, new)| (new.clone(), old.clone()))
                    .collect(),
            )),
            ChangesetOp::Delete { old_values, .. } => Self::Insert(old_values),
        }
    }
}

/// Make one write through the view `table`, the view's guards judging it as
/// whoever the caller bound.
///
/// SQLite counts no change through an `INSTEAD OF` trigger, and a guard may
/// skip a write silently, so the verdict reads the backing table `backing`
/// afterwards: the write landed when the row stands as it left it. An insert
/// replaces the row its key names, the view judging that delete too.
#[cfg(feature = "peer")]
fn write_through_view(
    conn: &mut SqliteConnection,
    target: &Target,
    table: &str,
    backing: &str,
    section_pk: &[u8],
    write: &ViewWrite<'_>,
) -> Result<PeerVerdict, ClientError> {
    // The columns the row holds afterwards, and whether a row then stands on
    // them.
    let (after, present): (Vec<(usize, Cell)>, bool) = match write {
        ViewWrite::Insert(values) => {
            let key_vals = pk_values_by_column(section_pk, |i| values[i].clone());
            if !key_vals.is_empty() {
                execute_peer(conn, render_key_delete(target, table, &key_vals))?;
            }
            if !execute_peer(conn, render_insert(target, table, values))? {
                return Ok(PeerVerdict::Refused);
            }
            (values.iter().cloned().enumerate().collect(), true)
        }
        ViewWrite::Update(pairs) => {
            if !execute_peer(conn, render_update(target, table, pairs)?)? {
                return Ok(PeerVerdict::Refused);
            }
            let after = pairs
                .iter()
                .enumerate()
                .filter_map(|(i, (old, new))| {
                    if section_pk.get(i).is_some_and(|flag| *flag != 0) {
                        let key = new.clone().or_else(|| old.clone());
                        Some((i, key.unwrap_or(Value::Null)))
                    } else {
                        new.clone().map(|new| (i, new))
                    }
                })
                .collect();
            (after, true)
        }
        ViewWrite::Delete(old_values) => {
            if !execute_peer(conn, render_delete(target, table, old_values))? {
                return Ok(PeerVerdict::Refused);
            }
            (
                pk_values_by_column(section_pk, |i| old_values[i].clone()),
                false,
            )
        }
    };
    Ok(
        if backing_row_matches(conn, &target.shape, backing, &after)? == present {
            PeerVerdict::Landed
        } else {
            PeerVerdict::Refused
        },
    )
}

/// Apply one op of a peer entry through the view `table` names, the view's
/// guards judging it as whoever the caller's author binding bound (R77). A
/// delete of a row the receiver does not hold as the op found it is refused,
/// since nothing the receiver holds stands behind it.
#[cfg(feature = "peer")]
pub(crate) fn apply_peer_op(
    conn: &mut SqliteConnection,
    table: &str,
    backing: &str,
    op: &PeerOp,
) -> Result<PeerVerdict, ClientError> {
    let target = view_target(conn, table)?;
    if let ChangesetOp::Delete { old_values, .. } = *op {
        let held: Vec<(usize, Cell)> = old_values.iter().cloned().enumerate().collect();
        if !backing_row_matches(conn, &target.shape, backing, &held)? {
            return Ok(PeerVerdict::Refused);
        }
    }
    let write = ViewWrite::of(op);
    write_through_view(conn, &target, table, backing, op.table().pk_flags(), &write)
}

/// Put back the row one peer op changed, through the view `table` (R77).
#[cfg(feature = "peer")]
pub(crate) fn undo_peer_op(
    conn: &mut SqliteConnection,
    table: &str,
    backing: &str,
    op: &PeerOp,
) -> Result<PeerVerdict, ClientError> {
    let target = view_target(conn, table)?;
    let write = ViewWrite::undoing(op);
    write_through_view(conn, &target, table, backing, op.table().pk_flags(), &write)
}

/// Whether the backing table holds a row matching every `(column, value)`.
#[cfg(feature = "peer")]
fn backing_row_matches(
    conn: &mut SqliteConnection,
    shape: &TableShape,
    backing: &str,
    columns: &[(usize, Cell)],
) -> Result<bool, ClientError> {
    #[derive(diesel::QueryableByName)]
    struct Found {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut terms = Vec::new();
    let mut binds = Vec::new();
    for (i, value) in columns {
        predicate_term(&mut terms, &mut binds, &shape.columns[*i], value);
    }
    // The backing table is named by the policy map, so the DSL cannot reach it.
    let sql = format!(
        "SELECT COUNT(*) AS n FROM {} WHERE {}",
        table_ref("main", backing),
        terms.join(" AND ")
    );
    let mut query = diesel::sql_query(sql).into_boxed::<diesel::sqlite::Sqlite>();
    for value in &binds {
        query = bind_one(query, value);
    }
    Ok(query.get_result::<Found>(conn)?.n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_wins;
    use diesel_sqlite_session::SqliteSessionExt;
    use sqlite_diff_rs::{DiffOps, Insert, PatchDelete, PatchSet, SimpleTable, Update};

    type PatchInsert = Insert<SimpleTable, String, Vec<u8>>;
    type PatchUpdate = Update<SimpleTable, PatchsetFormat, String, Vec<u8>>;
    type Patchset = PatchSet<SimpleTable, String, Vec<u8>>;

    const NOTES: &str = "(id INTEGER PRIMARY KEY, title TEXT, body TEXT)";
    const PAYLOAD: &str = "(id INTEGER PRIMARY KEY, payload BLOB, label TEXT)";
    const COMP: &str = "(a TEXT, b INTEGER, c TEXT, PRIMARY KEY (a, b))";
    const CHECKED: &str = "(id INTEGER PRIMARY KEY, val INTEGER, CHECK (val > 0))";

    fn notes_schema() -> SimpleTable {
        SimpleTable::new("notes", &[("id", ""), ("title", ""), ("body", "")], &[0])
    }

    /// Routes the one shared table into the shared store, as a frame's replay
    /// does.
    fn to_shared(table: &str) -> Option<&'static str> {
        table.eq_ignore_ascii_case("notes").then_some(SHARED)
    }

    const SHARED: &str = "connetto_shared";

    fn insert(schema: &SimpleTable, id: i64, title: &str, body: &str) -> PatchInsert {
        PatchInsert::from(schema.clone())
            .set(0, id)
            .expect("id")
            .set(1, title)
            .expect("title")
            .set(2, body)
            .expect("body")
    }

    fn new_conn() -> SqliteConnection {
        SqliteConnection::establish(":memory:").expect("an in-memory database")
    }

    /// Apply the same frame both ways and let `compare` check the outcome:
    /// natively into `main` on one connection, and through the shared replay
    /// into `connetto_shared` on another.
    fn differential<R>(
        frame: &[u8],
        ddl: &str,
        shared_ddl: Option<&str>,
        seed: impl Fn(&str) -> Vec<String>,
        compare: impl Fn(&mut SqliteConnection, &mut SqliteConnection) -> R,
    ) {
        let mut native = new_conn();
        native
            .batch_execute(&format!("CREATE TABLE main.notes {ddl}"))
            .expect("create the native table");
        for statement in seed("main") {
            native
                .batch_execute(&statement)
                .expect("seed the native table");
        }
        native
            .apply_patchset(frame, server_wins)
            .expect("the native apply");

        let mut shared = new_conn();
        shared
            .batch_execute("ATTACH DATABASE ':memory:' AS connetto_shared")
            .expect("attach the shared store");
        if let Some(shared_ddl) = shared_ddl {
            shared
                .batch_execute(&format!("CREATE TABLE connetto_shared.notes {shared_ddl}"))
                .expect("create the shared table");
        }
        for statement in seed("connetto_shared") {
            shared
                .batch_execute(&statement)
                .expect("seed the shared table");
        }
        apply(&mut shared, frame, Policy::ServerWins, &to_shared).expect("the shared replay");

        compare(&mut native, &mut shared);
    }

    #[derive(diesel::QueryableByName, PartialEq, Debug)]
    struct NoteRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        title: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        body: Option<String>,
    }

    fn read_notes(conn: &mut SqliteConnection, schema: &str) -> Vec<NoteRow> {
        diesel::sql_query(format!(
            "SELECT id, title, body FROM {schema}.notes ORDER BY id"
        ))
        .load(conn)
        .expect("read the notes rows")
    }

    fn same_notes(native: &mut SqliteConnection, shared: &mut SqliteConnection) {
        assert_eq!(
            read_notes(native, "main"),
            read_notes(shared, "connetto_shared"),
            "the shared replay must land the rows the native apply lands"
        );
    }

    #[test]
    fn inserts_and_replaces_match_native() {
        let schema = notes_schema();
        let frame: Vec<u8> = Patchset::new()
            .insert(insert(&schema, 1, "one", "first"))
            .insert(insert(&schema, 2, "two", "second"))
            // Re-inserting id 1 replaces the row it conflicts with.
            .insert(insert(&schema, 1, "one", "replaced"))
            .build();
        differential(&frame, NOTES, Some(NOTES), |_| Vec::new(), same_notes);
    }

    fn seeded_notes(schema: &str) -> Vec<String> {
        vec![format!(
            "INSERT INTO {schema}.notes VALUES (1, 'one', 'first'), (2, 'two', 'second'), (3, 'three', NULL)"
        )]
    }

    #[test]
    fn updates_to_stored_rows_match_native() {
        let schema = notes_schema();
        let update = |id: i64, column: usize, value: &str| {
            PatchUpdate::from(schema.clone())
                .set(0, id)
                .expect("key")
                .set(column, value)
                .expect("value")
        };
        let frame: Vec<u8> = Patchset::new()
            .update(update(1, 2, "updated"))
            .update(update(3, 1, "retitled"))
            // An update naming a key that is not stored changes nothing.
            .update(update(99, 2, "ghost"))
            .build();
        differential(&frame, NOTES, Some(NOTES), seeded_notes, same_notes);
    }

    #[test]
    fn a_captured_changeset_matches_native() {
        let mut source = new_conn();
        source
            .batch_execute(&format!("CREATE TABLE notes {NOTES}"))
            .expect("create the source table");
        for statement in seeded_notes("main") {
            source.batch_execute(&statement).expect("seed the source");
        }
        let mut session = source.create_session().expect("a capture session");
        session.attach_all().expect("capture every table");
        source
            .batch_execute(
                "UPDATE notes SET title = 'renamed' WHERE id = 1;
                 DELETE FROM notes WHERE id = 2;
                 UPDATE notes SET body = 'filled' WHERE id = 3;
                 INSERT INTO notes VALUES (4, 'four', 'fourth');",
            )
            .expect("change the source");
        let frame = session.changeset().expect("the captured changeset");
        drop(session);
        differential(&frame, NOTES, Some(NOTES), seeded_notes, same_notes);
    }

    #[test]
    fn deletes_match_native() {
        let schema = notes_schema();
        let frame: Vec<u8> = Patchset::new()
            .insert(insert(&schema, 1, "one", "first"))
            .insert(insert(&schema, 2, "two", "second"))
            .delete(PatchDelete::new(schema.clone(), vec![1i64.into()]))
            // Deleting a key that is not there is a no-op on both sides.
            .delete(PatchDelete::new(schema.clone(), vec![99i64.into()]))
            .build();
        differential(&frame, NOTES, Some(NOTES), |_| Vec::new(), same_notes);
    }

    #[test]
    fn a_wider_frame_is_skipped_like_native() {
        // The local table has three columns; the frame section has four, so
        // both sides must skip it and leave the seeded row standing.
        let wide = SimpleTable::new(
            "notes",
            &[("id", ""), ("title", ""), ("body", ""), ("extra", "")],
            &[0],
        );
        let frame: Vec<u8> = Patchset::new()
            .insert(
                PatchInsert::from(wide.clone())
                    .set(0, 1i64)
                    .expect("id")
                    .set(1, "one")
                    .expect("title")
                    .set(2, "first")
                    .expect("body")
                    .set(3, "extra")
                    .expect("extra"),
            )
            .build();
        differential(
            &frame,
            NOTES,
            Some(NOTES),
            |schema| {
                vec![format!(
                    "INSERT INTO {schema}.notes VALUES (1, 'keep', 'me')"
                )]
            },
            same_notes,
        );
    }

    #[test]
    fn a_missing_shared_table_is_skipped_like_native() {
        let schema = notes_schema();
        let frame: Vec<u8> = Patchset::new()
            .insert(insert(&schema, 1, "one", "first"))
            .build();
        // The shared store has no table the frame names: the replay skips it
        // and lands nothing, while the native apply applies to its table.
        differential(
            &frame,
            NOTES,
            None,
            |_| Vec::new(),
            |native, shared| {
                assert_eq!(
                    read_notes(native, "main").len(),
                    1,
                    "the native apply lands the row its table can hold"
                );
                let tables: Vec<Name> = diesel::sql_query(
                    "SELECT name FROM connetto_shared.sqlite_master WHERE type = 'table'",
                )
                .load(shared)
                .expect("read the shared store's tables");
                assert!(tables.is_empty(), "an absent shared table takes no rows");
            },
        );
    }

    #[derive(diesel::QueryableByName)]
    struct Name {
        #[diesel(sql_type = diesel::sql_types::Text)]
        #[expect(dead_code, reason = "only the row count is asserted")]
        name: String,
    }
    #[derive(diesel::QueryableByName, PartialEq, Debug)]
    struct PayloadRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Binary>)]
        payload: Option<Vec<u8>>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        label: Option<String>,
    }

    fn read_payload(conn: &mut SqliteConnection, schema: &str) -> Vec<PayloadRow> {
        diesel::sql_query(format!(
            "SELECT id, payload, label FROM {schema}.notes ORDER BY id"
        ))
        .load(conn)
        .expect("read the payload rows")
    }

    #[test]
    fn nulls_and_blobs_match_native() {
        let schema = SimpleTable::new("notes", &[("id", ""), ("payload", ""), ("label", "")], &[0]);
        let frame: Vec<u8> = Patchset::new()
            .insert(
                PatchInsert::from(schema.clone())
                    .set(0, 1i64)
                    .expect("id")
                    .set(1, vec![0u8, 1, 2, 3])
                    .expect("blob")
                    .set(2, Value::Null)
                    .expect("null label"),
            )
            .insert(
                PatchInsert::from(schema.clone())
                    .set(0, 2i64)
                    .expect("id")
                    .set(1, Value::Null)
                    .expect("null blob")
                    .set(2, "hi")
                    .expect("label"),
            )
            .build();
        differential(
            &frame,
            PAYLOAD,
            Some(PAYLOAD),
            |_| Vec::new(),
            |native, shared| {
                assert_eq!(
                    read_payload(native, "main"),
                    read_payload(shared, "connetto_shared")
                );
            },
        );
    }

    #[derive(diesel::QueryableByName, PartialEq, Debug)]
    struct CompRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        a: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        b: i64,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        c: Option<String>,
    }

    fn read_comp(conn: &mut SqliteConnection, schema: &str) -> Vec<CompRow> {
        diesel::sql_query(format!("SELECT a, b, c FROM {schema}.notes ORDER BY a, b"))
            .load(conn)
            .expect("read the composite rows")
    }

    #[test]
    fn composite_keys_match_native() {
        let schema = SimpleTable::new("notes", &[("a", ""), ("b", ""), ("c", "")], &[0, 1]);
        let comp_insert = |schema: &SimpleTable, a: &str, b: i64, c: &str| {
            PatchInsert::from(schema.clone())
                .set(0, a)
                .expect("a")
                .set(1, b)
                .expect("b")
                .set(2, c)
                .expect("c")
        };
        let frame: Vec<u8> = Patchset::new()
            .insert(comp_insert(&schema, "one", 1, "x"))
            .insert(comp_insert(&schema, "one", 2, "y"))
            .update(
                PatchUpdate::from(schema.clone())
                    .set(0, "one")
                    .expect("a")
                    .set(1, 1i64)
                    .expect("b")
                    .set(2, "z")
                    .expect("c"),
            )
            .delete(PatchDelete::new(
                schema.clone(),
                vec!["one".into(), 2i64.into()],
            ))
            .build();
        differential(
            &frame,
            COMP,
            Some(COMP),
            |_| Vec::new(),
            |native, shared| {
                assert_eq!(
                    read_comp(native, "main"),
                    read_comp(shared, "connetto_shared")
                );
            },
        );
    }

    #[derive(diesel::QueryableByName, PartialEq, Debug)]
    struct CheckedRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        val: i64,
    }

    fn read_checked(conn: &mut SqliteConnection, schema: &str) -> Vec<CheckedRow> {
        diesel::sql_query(format!("SELECT id, val FROM {schema}.notes ORDER BY id"))
            .load(conn)
            .expect("read the checked rows")
    }

    #[test]
    fn a_check_violation_is_omitted_like_native() {
        let schema = SimpleTable::new("notes", &[("id", ""), ("val", "")], &[0]);
        let checked_insert = |schema: &SimpleTable, id: i64, val: i64| {
            PatchInsert::from(schema.clone())
                .set(0, id)
                .expect("id")
                .set(1, val)
                .expect("val")
        };
        let frame: Vec<u8> = Patchset::new()
            .insert(checked_insert(&schema, 1, 5))
            // Violates CHECK (val > 0): both sides must drop it, leaving the
            // valid row standing.
            .insert(checked_insert(&schema, 2, -3))
            .build();
        differential(
            &frame,
            CHECKED,
            Some(CHECKED),
            |_| Vec::new(),
            |native, shared| {
                assert_eq!(
                    read_checked(native, "main"),
                    read_checked(shared, "connetto_shared"),
                    "a check violation is omitted on both sides"
                );
                assert_eq!(
                    read_checked(shared, "connetto_shared").len(),
                    1,
                    "only the valid row survives the check violation"
                );
            },
        );
    }

    #[test]
    fn a_replace_conflict_omitted_by_check_matches_native() {
        // A stored id 1 meets an incoming id 1 the CHECK refuses: the native
        // apply is the oracle for whether the stored row survives.
        let schema = SimpleTable::new("notes", &[("id", ""), ("val", "")], &[0]);
        let frame: Vec<u8> = Patchset::new()
            .insert(
                PatchInsert::from(schema)
                    .set(0, 1i64)
                    .expect("id")
                    .set(1, -3i64)
                    .expect("val"),
            )
            .build();
        let seed = |schema: &str| vec![format!("INSERT INTO {schema}.notes VALUES (1, 5)")];
        differential(&frame, CHECKED, Some(CHECKED), seed, |native, shared| {
            assert_eq!(
                read_checked(native, "main"),
                read_checked(shared, "connetto_shared"),
                "a refused replace leaves the same rows on both sides"
            );
        });
    }

    /// The local write every local-policy case replays: an update, a delete,
    /// a null filled in and an insert, captured against `seeded_notes`.
    fn local_write() -> Vec<u8> {
        let mut source = new_conn();
        source
            .batch_execute(&format!("CREATE TABLE notes {NOTES}"))
            .expect("create the source table");
        for statement in seeded_notes("main") {
            source.batch_execute(&statement).expect("seed the source");
        }
        let mut session = source.create_session().expect("a capture session");
        session.attach_all().expect("capture every table");
        source
            .batch_execute(
                "UPDATE notes SET title = 'renamed' WHERE id = 1;
                 DELETE FROM notes WHERE id = 2;
                 UPDATE notes SET body = 'filled' WHERE id = 3;
                 INSERT INTO notes VALUES (4, 'four', 'fourth');",
            )
            .expect("change the source");
        session.changeset().expect("the captured changeset")
    }

    /// Replay the local write onto a target seeded with `seeded_notes` and
    /// then changed by `divergence`, natively into `main` under `handler` and
    /// through the replay into `connetto_shared` under `policy`. Both sides
    /// must agree on refusing and on the rows left behind. Returns whether
    /// the write was refused.
    fn local_differential(
        divergence: &str,
        policy: Policy,
        handler: fn(diesel_sqlite_session::ConflictType) -> diesel_sqlite_session::ConflictAction,
    ) -> bool {
        let changeset = local_write();
        let mut native = new_conn();
        native
            .batch_execute(&format!("CREATE TABLE main.notes {NOTES}"))
            .expect("create the native table");
        let mut replayed = new_conn();
        replayed
            .batch_execute(&format!(
                "ATTACH DATABASE ':memory:' AS {SHARED}; CREATE TABLE {SHARED}.notes {NOTES}"
            ))
            .expect("create the shared table");
        for (conn, schema) in [(&mut native, "main"), (&mut replayed, SHARED)] {
            for statement in seeded_notes(schema) {
                conn.batch_execute(&statement).expect("seed the target");
            }
            if !divergence.is_empty() {
                conn.batch_execute(&divergence.replace("{schema}", schema))
                    .expect("diverge the target");
            }
        }
        let native_refused = native.apply_changeset(&changeset, handler).is_err();
        let replay_refused = apply(&mut replayed, &changeset, policy, &to_shared).is_err();
        assert_eq!(
            native_refused, replay_refused,
            "the replay refuses exactly when the native apply does"
        );
        assert_eq!(
            read_notes(&mut native, "main"),
            read_notes(&mut replayed, SHARED),
            "the replay leaves the rows the native apply leaves"
        );
        native_refused
    }

    fn omit(_: diesel_sqlite_session::ConflictType) -> diesel_sqlite_session::ConflictAction {
        diesel_sqlite_session::ConflictAction::Omit
    }

    fn abort(_: diesel_sqlite_session::ConflictType) -> diesel_sqlite_session::ConflictAction {
        diesel_sqlite_session::ConflictAction::Abort
    }

    /// Each way a target can stop holding what the writer saw, the clean
    /// target first.
    const DIVERGENCES: [&str; 5] = [
        "",
        "UPDATE {schema}.notes SET title = 'theirs' WHERE id = 1",
        "DELETE FROM {schema}.notes WHERE id = 2",
        "UPDATE {schema}.notes SET body = 'text' WHERE id = 3",
        "INSERT INTO {schema}.notes VALUES (4, 'four', 'theirs')",
    ];

    #[test]
    fn the_omit_policy_matches_a_native_apply_omitting_every_conflict() {
        for divergence in DIVERGENCES {
            assert!(
                !local_differential(divergence, Policy::Omit, omit),
                "omitting never refuses, diverged by {divergence:?}"
            );
        }
    }

    #[test]
    fn the_abort_policy_matches_a_native_apply_aborting_on_any_conflict() {
        assert!(!local_differential("", Policy::Abort, abort));
        for divergence in &DIVERGENCES[1..] {
            assert!(
                local_differential(divergence, Policy::Abort, abort),
                "a conflict refuses the whole write, diverged by {divergence:?}"
            );
        }
    }

    #[test]
    fn the_abort_policy_refuses_a_table_its_schema_lacks() {
        let mut conn = new_conn();
        conn.batch_execute(&format!("ATTACH DATABASE ':memory:' AS {SHARED}"))
            .expect("attach the shared store");
        let outcome = apply(&mut conn, &local_write(), Policy::Abort, &to_shared);
        assert!(
            matches!(outcome, Err(ClientError::Apply(_))),
            "a write naming a missing table is refused, got {outcome:?}"
        );
    }

    #[test]
    fn each_table_lands_in_the_schema_it_is_routed_to() {
        let mut source = new_conn();
        source
            .batch_execute(&format!(
                "CREATE TABLE notes {NOTES}; CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)"
            ))
            .expect("create the source tables");
        let mut session = source.create_session().expect("a capture session");
        session.attach_all().expect("capture every table");
        source
            .batch_execute(
                "INSERT INTO tags VALUES (1, 'red'); INSERT INTO notes VALUES (1, 'one', 'first')",
            )
            .expect("write both tables");
        let changeset = session.changeset().expect("the captured changeset");
        drop(session);

        let mut target = new_conn();
        target
            .batch_execute(&format!(
                "ATTACH DATABASE ':memory:' AS {SHARED};
                 CREATE TABLE {SHARED}.notes {NOTES};
                 CREATE TABLE main.tags (id INTEGER PRIMARY KEY, label TEXT)"
            ))
            .expect("create the target tables");
        replay_aborting(&mut target, &changeset, &|table| {
            to_shared(table).unwrap_or("main")
        })
        .expect("the routed replay");
        assert_eq!(
            read_notes(&mut target, SHARED).len(),
            1,
            "notes went to the shared store"
        );
        let tags: Vec<Name> = diesel::sql_query("SELECT label AS name FROM main.tags")
            .load(&mut target)
            .expect("read the tags");
        assert_eq!(tags.len(), 1, "tags stayed in main");
    }

    #[test]
    fn a_caller_deferring_its_references_still_defers_after_a_replay() {
        let mut conn = new_conn();
        conn.batch_execute(&format!(
            "ATTACH DATABASE ':memory:' AS {SHARED}; CREATE TABLE {SHARED}.notes {NOTES};
             BEGIN; PRAGMA defer_foreign_keys = ON;"
        ))
        .expect("open a deferring transaction");
        apply(&mut conn, &local_write(), Policy::Omit, &to_shared).expect("the replay");
        assert!(
            defers_foreign_keys(&mut conn).expect("read the pragma"),
            "the caller's deferral survives the replay"
        );
        conn.batch_execute("COMMIT").expect("commit");
    }

    #[test]
    fn the_local_policies_refuse_a_patchset() {
        let frame: Vec<u8> = Patchset::new()
            .insert(insert(&notes_schema(), 1, "one", "first"))
            .build();
        let mut conn = new_conn();
        conn.batch_execute(&format!(
            "ATTACH DATABASE ':memory:' AS {SHARED}; CREATE TABLE {SHARED}.notes {NOTES}"
        ))
        .expect("create the shared table");
        for policy in [Policy::Omit, Policy::Abort] {
            let outcome = apply(&mut conn, &frame, policy, &to_shared);
            assert!(
                matches!(outcome, Err(ClientError::Apply(_))),
                "{policy:?} needs old values to match, got {outcome:?}"
            );
        }
    }

    #[test]
    fn a_differently_shaped_table_is_refused_by_abort_and_skipped_by_omit() {
        let mut conn = new_conn();
        conn.batch_execute(&format!(
            "ATTACH DATABASE ':memory:' AS {SHARED};
             CREATE TABLE {SHARED}.notes (id INTEGER PRIMARY KEY, title TEXT);
             INSERT INTO {SHARED}.notes VALUES (1, 'kept')"
        ))
        .expect("create a narrower shared table");
        let write = local_write();
        let outcome = apply(&mut conn, &write, Policy::Abort, &to_shared);
        assert!(
            matches!(outcome, Err(ClientError::Apply(_))),
            "abort refuses a table narrower than the write, got {outcome:?}"
        );
        apply(&mut conn, &write, Policy::Omit, &to_shared).expect("omit skips the table");
        let titles: Vec<Name> =
            diesel::sql_query(format!("SELECT title AS name FROM {SHARED}.notes"))
                .load(&mut conn)
                .expect("read the table");
        assert_eq!(titles.len(), 1, "the skipped table is left as it was");
    }
}
