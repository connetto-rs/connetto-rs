//! Row holdings (R11 Part B): which holder keeps which replica row.
//!
//! A holding names a holder, a table and one row's key. A holder is the
//! subscription a server frame was addressed to, or a hidden membership
//! subscription's label (R27). The key is the clause the outbox triggers
//! spare a written row under, `("a" IS 1 AND "b" IS 'x')`, each value through
//! `quote()` over the stored row, so one computed text matches a row to its
//! holdings.
//!
//! A table the build does not share keeps its holdings in `main`. A shared
//! table keeps them in the shared store, under the identity holding the row,
//! where an identity's unsent writes hold their rows too (O3), so every
//! identity on the device sees what every other one keeps.

use std::collections::{HashMap, HashSet};

use diesel::prelude::*;
use diesel::sql_types::Text;
use sqlite_diff_rs::{DiffSet, DynTable, PatchsetFormat, PatchsetOp, TableSchema};

use crate::outbox;
use crate::{
    ClientConfig, ClientError, MEMBERSHIP_LABEL_PREFIX, SHARED_SCHEMA, is_shared, pk_ordinals,
    quote_ident,
};

/// A server patch as the parser reads it.
pub(crate) type ParsedPatch = DiffSet<PatchsetFormat, TableSchema<String>, String, Vec<u8>>;

/// The holdings table, keyed so one holder's holdings delete by prefix and
/// indexed so a row's holdings are found by table and key.
pub(crate) const HOLDING_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_holding \
    (holder TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, \
    PRIMARY KEY (holder, tbl, key)) WITHOUT ROWID; \
    CREATE INDEX IF NOT EXISTS _connetto_holding_row ON _connetto_holding (tbl, key)";

/// The shared store's holdings table, the same with the identity holding.
/// Its own name, since a `TEMP` trigger writing it may not qualify it and
/// an unqualified `_connetto_holding` resolves to `main`'s.
pub(crate) const SHARED_HOLDING_DDL: &str = "CREATE TABLE IF NOT EXISTS \
    connetto_shared._connetto_shared_holding \
    (identity TEXT NOT NULL, holder TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, \
    PRIMARY KEY (identity, holder, tbl, key)) WITHOUT ROWID; \
    CREATE INDEX IF NOT EXISTS connetto_shared._connetto_shared_holding_row \
    ON _connetto_shared_holding (tbl, key)";

/// The holder of an identity's unsent writes to a shared table (O3), in the
/// reserved `connetto-` namespace a membership label also uses.
pub(crate) const UNSENT_HOLDER: &str = "connetto-unsent";

/// The shared store's holdings table, qualified.
const SHARED_HOLDINGS: &str = "connetto_shared._connetto_shared_holding";

/// Keys matched per statement, under SQLite's expression depth and
/// compound-select limits.
const CHUNK: usize = 200;

diesel::table! {
    /// One holder's claim on one replica row.
    #[sql_name = "_connetto_holding"]
    holding (holder, tbl, key) {
        /// The subscription or membership label holding the row.
        holder -> Text,
        /// The lowercased physical table.
        tbl -> Text,
        /// The row's key clause.
        key -> Text,
    }
}

diesel::table! {
    /// One identity's claim on one shared row.
    connetto_shared._connetto_shared_holding (identity, holder, tbl, key) {
        /// The identity's client id, its replica name when signed in.
        identity -> Text,
        /// The subscription, membership label or unsent-write marker holding
        /// the row.
        holder -> Text,
        /// The lowercased physical table.
        tbl -> Text,
        /// The row's key clause.
        key -> Text,
    }
}

use _connetto_shared_holding as shared_holding;

/// `value` as an SQL text literal.
fn text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// One key value as the SQL literal matching it in the stored row. An
/// infinity is `9e999`, which SQLite reads back as the infinity it stores,
/// and a `NaN` is `NULL`, which is what SQLite stores for one.
fn key_literal(value: &sqlite_diff_rs::Value<String, Vec<u8>>) -> String {
    use core::fmt::Write as _;
    use sqlite_diff_rs::Value;
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Integer(integer) => integer.to_string(),
        Value::Real(real) if real.is_nan() => "NULL".to_owned(),
        Value::Real(real) if real.is_infinite() => if real.is_sign_positive() {
            "9e999"
        } else {
            "-9e999"
        }
        .to_owned(),
        Value::Real(real) => format!("{real:?}"),
        Value::Text(text) => text_literal(text),
        Value::Blob(bytes) => {
            let mut literal = String::with_capacity(bytes.len() * 2 + 3);
            literal.push_str("X'");
            for byte in bytes {
                let _ = write!(literal, "{byte:02X}");
            }
            literal.push('\'');
            literal
        }
    }
}

/// The keys one server patch names in one table, each key as the SQL
/// literals of its key columns in key order.
struct TableKeys {
    /// The lowercased physical table, as holdings name it.
    table: String,
    /// Whether the table lives in the shared store.
    shared: bool,
    /// The quoted key columns, in key order.
    columns: Vec<String>,
    /// The SQL computing a row's key from the row aliased `r`.
    key: String,
    /// Rows the patch inserts or updates.
    written: Vec<Vec<String>>,
    /// Rows that left the addressed subscription (R44).
    left: Vec<Vec<String>>,
    /// Rows deleted upstream.
    gone: Vec<Vec<String>>,
}

impl TableKeys {
    /// The table's rows, qualified by the schema holding them.
    fn rows(&self) -> String {
        let schema = if self.shared { SHARED_SCHEMA } else { "main" };
        format!("{schema}.{}", quote_ident(&self.table))
    }

    /// `r."a" IS 1 AND r."b" IS 'x'` for each key in `keys`, joined by `OR`.
    fn matching(&self, keys: &[Vec<String>]) -> String {
        keys.iter()
            .map(|literals| {
                let parts: Vec<String> = self
                    .columns
                    .iter()
                    .zip(literals)
                    .map(|(column, literal)| format!("r.{column} IS {literal}"))
                    .collect();
                format!("({})", parts.join(" AND "))
            })
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    /// The keys of `keys` as text, computed from the stored rows and, for a
    /// row no longer stored, from the literals, so a key the replica's
    /// column affinity converted still matches its holding.
    fn keys_of(&self, keys: &[Vec<String>]) -> String {
        let literal_rows: Vec<String> = keys
            .iter()
            .map(|literals| {
                let parts: Vec<String> = self
                    .columns
                    .iter()
                    .zip(literals)
                    .map(|(column, literal)| format!("{literal} AS {column}"))
                    .collect();
                format!("SELECT {}", parts.join(", "))
            })
            .collect();
        format!(
            "SELECT {key} FROM {rows} AS r WHERE {matching} \
             UNION SELECT {key} FROM ({literals}) AS r",
            key = self.key,
            rows = self.rows(),
            matching = self.matching(keys),
            literals = literal_rows.join(" UNION ALL "),
        )
    }
}

/// What one server patch does to its holder's holdings, read once from the
/// patch and applied on either side of it.
pub(crate) struct PatchHoldings {
    tables: Vec<TableKeys>,
    /// The identity holding the shared rows, as an SQL literal.
    identity: String,
}

impl PatchHoldings {
    /// Read the keys of `set`, a patch in the wire's logical names. A table
    /// whose columns or key differ from the replica's is left out, as
    /// SQLite's apply skips it too.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the catalog cannot be read.
    pub(crate) fn read(
        db: &mut SqliteConnection,
        config: &ClientConfig,
        set: &ParsedPatch,
    ) -> Result<Self, ClientError> {
        let mut tables: Vec<TableKeys> = Vec::new();
        let mut skipped: HashSet<String> = HashSet::new();
        for op in set.iter() {
            let schema = op.table();
            let physical = config
                .policy_tables
                .physical(schema.name())
                .unwrap_or(schema.name())
                .to_lowercase();
            if skipped.contains(&physical) {
                continue;
            }
            let index = if let Some(index) = tables.iter().position(|keys| keys.table == physical) {
                index
            } else {
                let shared = is_shared(&config.shared_tables, &physical);
                let place = if shared { SHARED_SCHEMA } else { "main" };
                let columns = outbox::columns(db, place, &physical)?;
                if !keyed_alike(&columns, schema) {
                    skipped.insert(physical);
                    continue;
                }
                tables.push(TableKeys {
                    key: outbox::key_clause("r", &columns),
                    columns: outbox::key_columns(&columns)
                        .into_iter()
                        .map(|column| quote_ident(&column.name))
                        .collect(),
                    table: physical,
                    shared,
                    written: Vec::new(),
                    left: Vec::new(),
                    gone: Vec::new(),
                });
                tables.len() - 1
            };
            let keys = &mut tables[index];
            let literals: Vec<String> = op.primary_key().iter().map(key_literal).collect();
            match op {
                PatchsetOp::Delete { .. } if op.indirect() => keys.left.push(literals),
                PatchsetOp::Delete { .. } => keys.gone.push(literals),
                PatchsetOp::Insert { .. } | PatchsetOp::Update { .. } => {
                    keys.written.push(literals);
                }
            }
        }
        Ok(Self {
            tables,
            identity: text_literal(&config.client_id),
        })
    }

    /// Before the apply, while the deleted rows are still stored: release
    /// `holder`'s holding of every row that left it, and every holding of
    /// every row deleted upstream, whichever identity holds it and whether
    /// or not for an unsent write.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when a holding cannot be deleted.
    pub(crate) fn release(
        &self,
        db: &mut SqliteConnection,
        holder: &str,
    ) -> Result<(), ClientError> {
        for keys in &self.tables {
            let (holdings, mine) = if keys.shared {
                (
                    SHARED_HOLDINGS,
                    format!("identity = {} AND ", self.identity),
                )
            } else {
                ("_connetto_holding", String::new())
            };
            for chunk in keys.left.chunks(CHUNK) {
                diesel::sql_query(format!(
                    "DELETE FROM {holdings} WHERE {mine}holder = ? AND tbl = ? AND key IN ({})",
                    keys.keys_of(chunk)
                ))
                .bind::<Text, _>(holder)
                .bind::<Text, _>(&keys.table)
                .execute(db)?;
            }
            for chunk in keys.gone.chunks(CHUNK) {
                diesel::sql_query(format!(
                    "DELETE FROM {holdings} WHERE tbl = ? AND key IN ({})",
                    keys.keys_of(chunk)
                ))
                .bind::<Text, _>(&keys.table)
                .execute(db)?;
            }
        }
        Ok(())
    }

    /// After the apply: `holder` holds every row the patch wrote and the
    /// replica now stores.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when a holding cannot be written.
    pub(crate) fn take(&self, db: &mut SqliteConnection, holder: &str) -> Result<(), ClientError> {
        for keys in &self.tables {
            let (holdings, columns, mine) = if keys.shared {
                (
                    SHARED_HOLDINGS,
                    "identity, holder, tbl, key",
                    format!("{}, ", self.identity),
                )
            } else {
                ("_connetto_holding", "holder, tbl, key", String::new())
            };
            for chunk in keys.written.chunks(CHUNK) {
                diesel::sql_query(format!(
                    "INSERT OR IGNORE INTO {holdings} ({columns}) \
                     SELECT {mine}?, ?, {} FROM {} AS r WHERE {}",
                    keys.key,
                    keys.rows(),
                    keys.matching(chunk)
                ))
                .bind::<Text, _>(holder)
                .bind::<Text, _>(&keys.table)
                .execute(db)?;
            }
        }
        Ok(())
    }
}

/// Whether the replica's `columns` can take the patch's `schema` as SQLite's
/// apply decides before it writes a table: at least as many columns, and key
/// columns at the same positions in the same order. A table the replica
/// lacks has no columns and fails it.
fn keyed_alike(columns: &[outbox::Column], schema: &TableSchema<String>) -> bool {
    let mut keyed: Vec<(i64, usize)> = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.pk > 0)
        .map(|(position, column)| (column.pk, position))
        .collect();
    keyed.sort_unstable();
    columns.len() >= schema.number_of_columns()
        && !keyed.is_empty()
        && keyed
            .into_iter()
            .map(|(_, position)| position)
            .eq(pk_ordinals(schema))
}

/// Whether `holder` may hold rows: a subscription on record, or a hidden
/// membership subscription, which the server declares on the client's
/// behalf. A frame still in flight past its unsubscribe names neither.
///
/// # Errors
///
/// [`ClientError`] when the subscription record cannot be read.
pub(crate) fn is_holder(db: &mut SqliteConnection, holder: &str) -> Result<bool, ClientError> {
    if holder.starts_with(MEMBERSHIP_LABEL_PREFIX) {
        return Ok(true);
    }
    crate::subscriptions::is_declared(db, holder)
}

/// Release every holding of `holder`, this identity's in the shared store
/// included.
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_holder(
    db: &mut SqliteConnection,
    config: &ClientConfig,
    holder: &str,
) -> Result<(), ClientError> {
    diesel::delete(holding::table.filter(holding::holder.eq(holder))).execute(db)?;
    if !config.shared_tables.is_empty() {
        diesel::delete(
            shared_holding::table
                .filter(shared_holding::identity.eq(&config.client_id))
                .filter(shared_holding::holder.eq(holder)),
        )
        .execute(db)?;
    }
    Ok(())
}

/// Release every subscription's holding of a row of `table`, a lowercased
/// physical name. A shared table's go for every identity, and only an unsent
/// write still holds its row (O5).
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_table(
    db: &mut SqliteConnection,
    config: &ClientConfig,
    table: &str,
) -> Result<(), ClientError> {
    if is_shared(&config.shared_tables, table) {
        diesel::delete(
            shared_holding::table
                .filter(shared_holding::tbl.eq(table))
                .filter(shared_holding::holder.ne(UNSENT_HOLDER)),
        )
        .execute(db)?;
    } else {
        diesel::delete(holding::table.filter(holding::tbl.eq(table))).execute(db)?;
    }
    Ok(())
}

/// Release every subscription's holding of this identity, leaving its
/// unsent writes held.
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_all(
    db: &mut SqliteConnection,
    config: &ClientConfig,
) -> Result<(), ClientError> {
    release_account(db)?;
    if !config.shared_tables.is_empty() {
        diesel::delete(
            shared_holding::table
                .filter(shared_holding::identity.eq(&config.client_id))
                .filter(shared_holding::holder.ne(UNSENT_HOLDER)),
        )
        .execute(db)?;
    }
    Ok(())
}

/// Release every holding in `main`, as a new shared store generation's
/// replays deliver every subscription's rows again.
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_account(db: &mut SqliteConnection) -> Result<(), ClientError> {
    diesel::delete(holding::table).execute(db)?;
    Ok(())
}

/// Release every holding `identity` has in the shared store, its unsent
/// writes' included, as it leaves the store (decision 7).
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_identity(
    db: &mut SqliteConnection,
    identity: &str,
) -> Result<(), ClientError> {
    diesel::delete(shared_holding::table.filter(shared_holding::identity.eq(identity)))
        .execute(db)?;
    Ok(())
}

/// Rewrite the holdings of this identity's unsent shared writes from
/// `unsent`, the key clauses of its unacknowledged writes by lowercased
/// physical table, so an acknowledged or rolled back write holds nothing
/// more and a moved key is held where it now is. Only a stored row is held.
///
/// # Errors
///
/// [`ClientError`] when the catalog cannot be read or the holdings written.
pub(crate) fn rewrite_unsent(
    db: &mut SqliteConnection,
    config: &ClientConfig,
    unsent: &HashMap<String, Vec<String>>,
) -> Result<(), ClientError> {
    diesel::delete(
        shared_holding::table
            .filter(shared_holding::identity.eq(&config.client_id))
            .filter(shared_holding::holder.eq(UNSENT_HOLDER)),
    )
    .execute(db)?;
    let identity = text_literal(&config.client_id);
    for (table, clauses) in unsent
        .iter()
        .filter(|(table, _)| is_shared(&config.shared_tables, table))
    {
        let columns = outbox::columns(db, SHARED_SCHEMA, table)?;
        let key = outbox::key_clause("r", &columns);
        for chunk in clauses.chunks(CHUNK) {
            diesel::sql_query(format!(
                "INSERT OR IGNORE INTO {SHARED_HOLDINGS} (identity, holder, tbl, key) \
                 SELECT {identity}, ?, ?, {key} FROM {SHARED_SCHEMA}.{} AS r WHERE {}",
                quote_ident(table),
                chunk.join(" OR ")
            ))
            .bind::<Text, _>(UNSENT_HOLDER)
            .bind::<Text, _>(table)
            .execute(db)?;
        }
    }
    Ok(())
}

/// Release the holdings no live holder of this identity keeps any longer:
/// those of a subscription ended, forgotten or past its grace, and those of
/// a membership label whose table no live subscription's query may read a
/// membership term from (O4). An unsent write's holdings are the queue's.
///
/// # Errors
///
/// [`ClientError`] when the records or the holdings cannot be read, or a
/// holding cannot be deleted.
pub(crate) fn prune(db: &mut SqliteConnection, config: &ClientConfig) -> Result<(), ClientError> {
    let mut holders: Vec<String> = holding::table.select(holding::holder).distinct().load(db)?;
    if !config.shared_tables.is_empty() {
        holders.extend(
            shared_holding::table
                .filter(shared_holding::identity.eq(&config.client_id))
                .filter(shared_holding::holder.ne(UNSENT_HOLDER))
                .select(shared_holding::holder)
                .distinct()
                .load::<String>(db)?,
        );
    }
    if holders.is_empty() {
        return Ok(());
    }
    let mut live: HashSet<String> = HashSet::new();
    let mut terms: HashSet<String> = HashSet::new();
    for record in crate::subscriptions::declared(db)? {
        if record.live {
            terms.extend(crate::live::membership_tables(&record.spec)?);
            live.insert(record.sub_id);
        }
    }
    for holder in holders {
        let kept = match holder.strip_prefix(MEMBERSHIP_LABEL_PREFIX) {
            Some(table) => terms.contains(&table.to_lowercase()),
            None => live.contains(&holder),
        };
        if !kept {
            release_holder(db, config, &holder)?;
        }
    }
    Ok(())
}
