//! connetto's side of subql's drift guard.
//!
//! The guard judges every table of the catalog against the live database at
//! each connect and at every schema message, drops the events of the tables it
//! refuses, and surfaces one [`DriftSignal`] per refusal. connetto keeps what
//! the guard cannot: a refused set that outlives each guard, so a table stays
//! refused across reconnects until the process ends, and the policy readers a
//! weakened identity refuses with the table (R107 decision 10).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use diesel::Connection as _;
use sql_traits::structs::TargetName;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::drift::PgLiveCatalog;
use subql::{
    BlockingLiveReader, DriftGuard, DriftGuardError, DriftSignal, Incompatibility, ParserDB,
    PgRelation, PgStreamingCdcSource, PgStreamingConfig, RefusalView, RefusedTables, TokioTimer,
};
use thiserror::Error;

use crate::rules::LiveDatabase;

/// The change source every connect builds, the stream behind subql's guard.
pub type GuardedSource =
    DriftGuard<PgStreamingCdcSource, ParserDB, BlockingLiveReader<PgLiveCatalog>, TokioTimer>;

/// Why a guarded source could not be built.
#[derive(Debug, Error)]
pub enum GuardConnectError {
    /// The catalog DDL did not parse.
    #[error("parsing catalog DDL: {0}")]
    Catalog(String),
    /// The CDC stream would not open.
    #[error("opening the CDC stream: {0}")]
    Open(String),
    /// The guard's live read of the database failed, so no table was judged.
    #[error("reading the live schema: {0}")]
    LiveShape(String),
    /// The guard failed in a way this server does not tell apart.
    #[error("building the drift guard: {0}")]
    Guard(String),
}

/// Open a stream and read its live rules before constructing its drift guard.
///
/// # Errors
///
/// [`GuardConnectError`] naming the step that failed.
pub async fn connect_guarded(
    config: PgStreamingConfig,
    ddl: &str,
    database_url: &str,
    publication: &str,
    interval: Duration,
) -> Result<PendingGuard, GuardConnectError> {
    let catalog = guard_catalog(ddl)?;
    let inner = PgStreamingCdcSource::connect(config, catalog.clone())
        .await
        .map_err(|err| GuardConnectError::Open(err.to_string()))?;
    let (reader, rules) = read_rules_for(&catalog, database_url, publication).await?;
    Ok(PendingGuard {
        inner,
        catalog,
        reader,
        rules,
        interval,
    })
}

/// A connected stream whose live rules must be followed before its guard starts.
pub struct PendingGuard {
    inner: PgStreamingCdcSource,
    catalog: ParserDB,
    reader: BlockingLiveReader<PgLiveCatalog>,
    rules: subql::LiveRules<LiveDatabase>,
    interval: Duration,
}

impl PendingGuard {
    /// The baseline read on this connection.
    #[must_use]
    pub const fn rules(&self) -> &subql::LiveRules<LiveDatabase> {
        &self.rules
    }

    /// Start guarding the stream from its accepted live baseline.
    ///
    /// # Errors
    /// Returns the stream or live schema failure.
    pub async fn start(self) -> Result<GuardedSource, GuardConnectError> {
        let marker = subql::catalog_helpers::table_id::<Postgres, _>(
            &self.catalog,
            crate::epoch::POLICY_CHANGES_TABLE,
        )
        .ok_or_else(|| GuardConnectError::Catalog("rules marker is absent".to_owned()))?;
        DriftGuard::new(
            self.inner,
            self.catalog,
            self.reader,
            self.interval,
            TokioTimer::new(),
            Some(subql::RulesFollowing::new(self.rules, Some(marker))),
        )
        .await
        .map_err(|error| match error {
            DriftGuardError::Inner(error) => GuardConnectError::Open(error.to_string()),
            DriftGuardError::Shape(error) => GuardConnectError::LiveShape(error.to_string()),
            other => GuardConnectError::Guard(other.to_string()),
        })
    }
}

pub(crate) async fn read_rules(
    ddl: &str,
    database_url: &str,
    publication: &str,
) -> Result<subql::LiveRules<LiveDatabase>, GuardConnectError> {
    let catalog = guard_catalog(ddl)?;
    read_rules_for(&catalog, database_url, publication)
        .await
        .map(|(_, rules)| rules)
}

async fn read_rules_for(
    catalog: &ParserDB,
    database_url: &str,
    publication: &str,
) -> Result<
    (
        BlockingLiveReader<PgLiveCatalog>,
        subql::LiveRules<LiveDatabase>,
    ),
    GuardConnectError,
> {
    let url = database_url.to_owned();
    let connection = tokio::task::spawn_blocking(move || diesel::PgConnection::establish(&url))
        .await
        .map_err(|error| GuardConnectError::LiveShape(error.to_string()))?
        .map_err(|error| GuardConnectError::LiveShape(error.to_string()))?;
    let mut reader = BlockingLiveReader::new(PgLiveCatalog::new(connection, publication));
    let rules = subql::read_live_rules(catalog, &mut reader)
        .await
        .map_err(|error| GuardConnectError::LiveShape(error.to_string()))?;
    Ok((reader, rules))
}

fn guard_catalog(ddl: &str) -> Result<ParserDB, GuardConnectError> {
    ParserDB::parse::<PostgreSqlDialect>(ddl)
        .map_err(|error| GuardConnectError::Catalog(format!("{error:?}")))
}

/// A schema item a change source yields in place of row changes.
///
/// The guarded stream yields [`DriftSignal`]s, and an unguarded source, which
/// only a test drives, yields items connetto has no use for.
pub trait StreamSignal: Send {
    /// The drift signal this item carries, if any.
    fn drift(self) -> Option<DriftSignal<LiveDatabase>>;
}

impl StreamSignal for DriftSignal<LiveDatabase> {
    fn drift(self) -> Option<DriftSignal<LiveDatabase>> {
        Some(self)
    }
}

impl StreamSignal for PgRelation {
    fn drift(self) -> Option<DriftSignal<LiveDatabase>> {
        None
    }
}

impl StreamSignal for core::convert::Infallible {
    fn drift(self) -> Option<DriftSignal<LiveDatabase>> {
        match self {}
    }
}

/// The tables refused while this process runs, shared by the apply path, the
/// subscribe gate and the ingest loop.
///
/// A table enters when its refusal is handled and never leaves, so a schema
/// that reverts serves again only after a restart (2026-11-03). Between a
/// guard's construction and the ingest loop handling its queued refusals, the
/// guard's own handle answers for the tables it refused at boot.
#[derive(Clone, Default)]
pub struct RefusedSet {
    inner: Arc<RwLock<Refused>>,
}

#[derive(Default)]
struct Refused {
    tables: BTreeMap<String, Vec<Incompatibility>>,
    guard: Option<RefusedTables>,
}

impl RefusedSet {
    /// Answer for the tables the current guard refuses, until its refusals are handled.
    pub(crate) fn watch(&self, guard: RefusedTables) {
        self.write().guard = Some(guard);
    }

    /// Record `table` as refused, answering whether it was served until now.
    pub(crate) fn insert(&self, table: &str, reasons: Vec<Incompatibility>) -> bool {
        let mut refused = self.write();
        if refused
            .tables
            .keys()
            .any(|name| name.eq_ignore_ascii_case(table))
        {
            return false;
        }
        refused.tables.insert(table.to_owned(), reasons);
        true
    }

    /// Whether this process refused `table`, whatever the current guard says.
    #[must_use]
    pub fn holds(&self, table: &str) -> bool {
        self.read()
            .tables
            .keys()
            .any(|name| name.eq_ignore_ascii_case(table))
    }

    /// Whether this process or the current guard refuses `table`.
    #[must_use]
    pub fn refuses(&self, table: &str) -> bool {
        self.refusal(table).is_some()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Refused> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Refused> {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl RefusalView for RefusedSet {
    fn refusal(&self, table: &str) -> Option<Vec<Incompatibility>> {
        let refused = self.read();
        refused
            .tables
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(table))
            .map(|(_, reasons)| reasons.clone())
            .or_else(|| refused.guard.as_ref()?.refusal(table))
    }
}

/// The tables whose policies read each table, which a weakened identity
/// refuses together with the table (R107 decision 10).
#[derive(Clone, Debug, Default)]
pub struct PolicyReaders {
    readers: BTreeMap<subql::TableId, BTreeSet<subql::TableId>>,
    /// Tables guarded by a policy whose reads the schema cannot name, which
    /// counts as reading every table.
    reading_everything: BTreeSet<subql::TableId>,
}

impl PolicyReaders {
    /// Index `reads` by the tables each policy reads, against `catalog`'s ids.
    ///
    /// A table the catalog cannot place is left out, since no event of it
    /// reaches the engine.
    #[must_use]
    pub fn of<DB: sql_traits::prelude::DatabaseLike>(
        reads: &[rls2fga::types::PolicyReads],
        catalog: &DB,
    ) -> Self {
        // rls2fga names a table by the schema and name the database stores.
        let id = |table: &rls2fga::types::TableId| {
            let target = TargetName::new(table.name(), true);
            let target = match table.schema() {
                Some(schema) => target.with_schema(schema, true),
                None => target,
            };
            subql::catalog_helpers::table_id_for_name::<Postgres, _>(catalog, target)
        };
        let mut index = Self::default();
        for policy in reads {
            let Some(guarded) = id(&policy.table) else {
                continue;
            };
            if !policy.unresolved.is_empty() {
                index.reading_everything.insert(guarded);
                continue;
            }
            for read in policy.reads.iter().filter_map(id) {
                if read != guarded {
                    index.readers.entry(read).or_default().insert(guarded);
                }
            }
        }
        index
    }

    /// The tables a policy guards that reads `table`.
    pub fn of_table(&self, table: subql::TableId) -> impl Iterator<Item = subql::TableId> + '_ {
        self.readers
            .get(&table)
            .into_iter()
            .flatten()
            .chain(&self.reading_everything)
            .copied()
            .filter(move |reader| *reader != table)
    }
}

/// Whether `reasons` include a weakened replica identity, which refuses every
/// table whose policy reads the refused one as well (R107 decision 10).
#[must_use]
pub fn weakens_identity(reasons: &[Incompatibility]) -> bool {
    reasons
        .iter()
        .any(|reason| matches!(reason, Incompatibility::IdentityWeakened { .. }))
}

#[cfg(test)]
mod tests {
    use super::guard_catalog;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::ParserDB;
    use subql::backend::Postgres;
    use subql::catalog_helpers::table_id;

    #[test]
    fn settled_table_is_known_to_the_guard() {
        let documents = crate::schema::engine_ddl::<crate::defaults::ConnettoDefaults>(
            "CREATE TABLE public.documents (id INTEGER PRIMARY KEY);",
            false,
        );
        let guarded = guard_catalog(&documents).expect("guard catalog");
        assert!(table_id::<Postgres, _>(&guarded, "_connetto_settled").is_some());
    }

    #[test]
    fn configured_settled_table_is_known_to_the_guard() {
        let documents = crate::schema::engine_ddl::<crate::defaults::ConnettoDefaults>(
            "CREATE SCHEMA writer_drift; CREATE TABLE writer_drift.app_settled (writer BIGINT, seq BIGINT, PRIMARY KEY (writer, seq));",
            false,
        );
        let guarded = guard_catalog(&documents).expect("guard catalog");
        assert!(table_id::<Postgres, _>(&guarded, "writer_drift.app_settled").is_some());
    }

    #[test]
    fn marker_uses_shared_engine_table_numbers() {
        let documents = crate::schema::engine_ddl::<crate::defaults::ConnettoDefaults>(
            "CREATE TABLE public.zebra (id INTEGER PRIMARY KEY); CREATE TABLE public.apple (id INTEGER PRIMARY KEY);",
            false,
        );
        let original = ParserDB::parse::<PostgreSqlDialect>(&documents).expect("catalog");
        let guarded = guard_catalog(&documents).expect("guard catalog");
        for table in [
            "public.zebra",
            "public.apple",
            crate::epoch::POLICY_CHANGES_TABLE,
        ] {
            assert_eq!(
                table_id::<Postgres, _>(&original, table),
                table_id::<Postgres, _>(&guarded, table)
            );
        }
        assert!(table_id::<Postgres, _>(&guarded, crate::epoch::POLICY_CHANGES_TABLE).is_some());
    }
}
