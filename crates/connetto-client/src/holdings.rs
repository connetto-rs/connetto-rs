//! Row holdings (R11 Part B): which holder keeps which replica row.
//!
//! A holding names a holder, a table and one row's key. A holder is the
//! subscription a server frame was addressed to, or a hidden membership
//! subscription's label (R27). The key is the clause the outbox triggers
//! spare a written row under, `("a" IS 1 AND "b" IS 'x')`, each value through
//! `quote()` over the stored row, so one computed text matches a row to its
//! holdings. The table holds the holdings of the tables this build does not
//! share.

use std::collections::HashSet;

use diesel::prelude::*;
use diesel::sql_types::Text;
use sqlite_diff_rs::{DiffSet, DynTable, PatchsetFormat, PatchsetOp, TableSchema};

use crate::outbox;
use crate::{
    ClientConfig, ClientError, MEMBERSHIP_LABEL_PREFIX, is_shared, pk_ordinals, quote_ident,
};

/// A server patch as the parser reads it.
pub(crate) type ParsedPatch = DiffSet<PatchsetFormat, TableSchema<String>, String, Vec<u8>>;

/// The holdings table, keyed so one holder's holdings delete by prefix and
/// indexed so a row's holdings are found by table and key.
pub(crate) const HOLDING_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_holding \
    (holder TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, \
    PRIMARY KEY (holder, tbl, key)) WITHOUT ROWID; \
    CREATE INDEX IF NOT EXISTS _connetto_holding_row ON _connetto_holding (tbl, key)";

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

/// The keys one server patch names in one table, each key as the SQL
/// literals of its key columns in key order.
struct TableKeys {
    /// The lowercased physical table, as holdings name it.
    table: String,
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
            "SELECT {key} FROM main.{table} AS r WHERE {matching} \
             UNION SELECT {key} FROM ({literals}) AS r",
            key = self.key,
            table = quote_ident(&self.table),
            matching = self.matching(keys),
            literals = literal_rows.join(" UNION ALL "),
        )
    }
}

/// What one server patch does to its holder's holdings, read once from the
/// patch and applied on either side of it.
pub(crate) struct PatchHoldings {
    tables: Vec<TableKeys>,
}

impl PatchHoldings {
    /// Read the keys of `set`, a patch in the wire's logical names. A table
    /// the build shares is left out, and so is one whose columns or key
    /// differ from the replica's, which SQLite's apply skips too.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the catalog cannot be read or a key value has no
    /// SQL literal.
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
            if is_shared(&config.shared_tables, &physical) || skipped.contains(&physical) {
                continue;
            }
            let index = if let Some(index) = tables.iter().position(|keys| keys.table == physical) {
                index
            } else {
                let columns = outbox::columns(db, "main", &physical)?;
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
                    written: Vec::new(),
                    left: Vec::new(),
                    gone: Vec::new(),
                });
                tables.len() - 1
            };
            let keys = &mut tables[index];
            let literals = op
                .primary_key()
                .iter()
                .map(crate::live::bind_literal_of)
                .collect::<Result<Vec<_>, _>>()?;
            match op {
                PatchsetOp::Delete { .. } if op.indirect() => keys.left.push(literals),
                PatchsetOp::Delete { .. } => keys.gone.push(literals),
                PatchsetOp::Insert { .. } | PatchsetOp::Update { .. } => {
                    keys.written.push(literals);
                }
            }
        }
        Ok(Self { tables })
    }

    /// Before the apply, while the deleted rows are still stored: release
    /// `holder`'s holding of every row that left it, and every holding of
    /// every row deleted upstream.
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
            for chunk in keys.left.chunks(CHUNK) {
                diesel::sql_query(format!(
                    "DELETE FROM _connetto_holding WHERE holder = ? AND tbl = ? AND key IN ({})",
                    keys.keys_of(chunk)
                ))
                .bind::<Text, _>(holder)
                .bind::<Text, _>(&keys.table)
                .execute(db)?;
            }
            for chunk in keys.gone.chunks(CHUNK) {
                diesel::sql_query(format!(
                    "DELETE FROM _connetto_holding WHERE tbl = ? AND key IN ({})",
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
            for chunk in keys.written.chunks(CHUNK) {
                diesel::sql_query(format!(
                    "INSERT OR IGNORE INTO _connetto_holding (holder, tbl, key) \
                     SELECT ?, ?, {} FROM main.{} AS r WHERE {}",
                    keys.key,
                    quote_ident(&keys.table),
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

/// Release every holding of `holder`.
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_holder(db: &mut SqliteConnection, holder: &str) -> Result<(), ClientError> {
    diesel::delete(holding::table.filter(holding::holder.eq(holder))).execute(db)?;
    Ok(())
}

/// Release every holding of a row of `table`, a lowercased physical name.
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_table(db: &mut SqliteConnection, table: &str) -> Result<(), ClientError> {
    diesel::delete(holding::table.filter(holding::tbl.eq(table))).execute(db)?;
    Ok(())
}

/// Release every holding.
///
/// # Errors
///
/// [`ClientError`] when the holdings cannot be deleted.
pub(crate) fn release_all(db: &mut SqliteConnection) -> Result<(), ClientError> {
    diesel::delete(holding::table).execute(db)?;
    Ok(())
}

/// Release the holdings no live holder keeps any longer: those of a
/// subscription ended, forgotten or past its grace, and those of a membership
/// label whose table no live subscription's query reads outside its own
/// `FROM`, which is where a membership term reads it (O4).
///
/// # Errors
///
/// [`ClientError`] when the records or the holdings cannot be read, or a
/// holding cannot be deleted.
pub(crate) fn prune(db: &mut SqliteConnection) -> Result<(), ClientError> {
    let holders: Vec<String> = holding::table.select(holding::holder).distinct().load(db)?;
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
            release_holder(db, &holder)?;
        }
    }
    Ok(())
}
