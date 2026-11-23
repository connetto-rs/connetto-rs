//! Live authorization installation and its accepted database baseline.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use openfga_client::client::OpenFgaServiceClient;
use openfga_client::tonic::transport::Channel;
use sql_traits::prelude::{DatabaseLike, TableLike};
use sql_traits::structs::TargetName;
use subql::ParserDB;
use subql::backend::{Backend, Postgres};
use subql::drift::TableRules;
use subql::visibility::openfga::OpenFgaPolicy;

use crate::builder::OpenFga;
use crate::openfga::{
    Counted, FactSource, FgaAuth, LiveTranslation, ModelState, ModelSubject, SetupError, Translated,
};

/// The database read by the PostgreSQL drift reader.
pub type LiveDatabase = <subql::drift::PgLiveCatalog as subql::drift::LiveCatalog>::Database;

/// A prepared authorization generation installed under the dispatch gate.
pub trait RulesSwap: Send {
    /// Publish the prepared authorization generation.
    fn install(self: Box<Self>);
}

/// Preparing a generation for installation under the dispatch gate.
pub type RulesInstallation<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn RulesSwap>, SetupError>> + Send + 'a>>;

/// Translation and installation against the last accepted live rules.
pub trait RulesFollower: Send + Sync {
    /// The served tables whose rules differ from the accepted baseline.
    fn changed(&self, database: &LiveDatabase) -> Vec<String>;

    /// Translate live rules against the served catalog.
    ///
    /// # Errors
    /// Returns a setup failure when the live catalog cannot be translated.
    fn translate(
        &self,
        database: &LiveDatabase,
        changed: &[String],
    ) -> Result<LiveTranslation, SetupError>;

    /// Prepare the store and a complete replacement generation.
    ///
    /// # Errors
    /// Returns the failing store or database operation.
    fn install<'a>(&'a self, translated: &'a Translated) -> RulesInstallation<'a>;

    /// Remember the generation and database accepted by the manager.
    fn accept(&self, translated: Translated, database: Arc<LiveDatabase>);

    /// Discard an unfinished installation without changing the accepted rules.
    fn abandon(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

type Authorization<Id> = FgaAuth<Id, String, Counted<Channel>>;

struct AcceptedRules {
    translated: Translated,
    database: Option<Arc<LiveDatabase>>,
}

pub(crate) struct Coordinator<Id> {
    auth: Authorization<Id>,
    current: Mutex<AcceptedRules>,
    openfga: OpenFga,
    facts: FactSource,
    pending: tokio::sync::Mutex<Option<InstalledModel>>,
}

impl<Id> Coordinator<Id> {
    /// A coordinator running `translated`, read from `database` when the boot
    /// followed the live rules and from the documents otherwise.
    pub(crate) fn new(
        auth: Authorization<Id>,
        translated: Translated,
        database: Option<Arc<LiveDatabase>>,
        openfga: OpenFga,
        facts: FactSource,
    ) -> Self {
        Self {
            auth,
            current: Mutex::new(AcceptedRules {
                translated,
                database,
            }),
            openfga,
            facts,
            pending: tokio::sync::Mutex::new(None),
        }
    }
}

/// What the boot's read of the live rules leaves the manager to settle before
/// the stream starts.
pub(crate) enum BootRules {
    /// The live rules are installed.
    Followed {
        /// The database the rules were read from, which the engine follows.
        database: Arc<LiveDatabase>,
        /// The tables the live rules refuse.
        refused: Vec<String>,
        /// Each table's fingerprint under the live rules.
        fingerprints: BTreeMap<String, String>,
        /// Each table's fingerprint under the documents, for a table no
        /// earlier process stored one for (decision 22).
        documents: BTreeMap<String, String>,
        /// The tables each live policy reads.
        policy_reads: Vec<rls2fga::types::PolicyReads>,
    },
    /// The live rules could not be planned, so the documents' rules serve and
    /// the tables whose live rules differ from them are refused.
    Unplannable {
        /// The tables whose live rules differ from the documents'.
        changed: Vec<String>,
    },
}

impl BootRules {
    /// Translate `database`'s live rules for a boot whose documents translated
    /// as `documents`, and answer the rules the server starts running beside
    /// what the manager settles.
    ///
    /// # Errors
    /// Returns [`SetupError::Unplannable`] when a table's share does not serialize.
    pub(crate) fn read(
        documents: Translated,
        database: Arc<LiveDatabase>,
    ) -> Result<(Translated, Self), SetupError> {
        let changed = changed_since(&documents, None, &database);
        match documents.follow(&database, &changed) {
            Ok(live) => {
                let boot = Self::Followed {
                    fingerprints: live.translated.fingerprints()?,
                    documents: documents.fingerprints()?,
                    policy_reads: live.translated.policy_reads().to_vec(),
                    refused: live.refused,
                    database,
                };
                Ok((live.translated, boot))
            }
            Err(error) => {
                tracing::error!(error = %error, "live authorization rules cannot be planned");
                Ok((documents, Self::Unplannable { changed }))
            }
        }
    }

    /// The database the running rules were read from, when the boot followed it.
    pub(crate) fn database(&self) -> Option<Arc<LiveDatabase>> {
        match self {
            Self::Followed { database, .. } => Some(Arc::clone(database)),
            Self::Unplannable { .. } => None,
        }
    }
}

/// The served tables whose rules in `database` differ from `baseline`, or from
/// `translated`'s own catalog without one.
fn changed_since(
    translated: &Translated,
    baseline: Option<&LiveDatabase>,
    database: &LiveDatabase,
) -> Vec<String> {
    let shapes = translated.shapes_arc();
    match baseline {
        Some(baseline) => changed_tables(shapes.catalog(), baseline, database),
        None => changed_tables(shapes.catalog(), shapes.catalog(), database),
    }
}

struct Swap<Id> {
    current: Authorization<Id>,
    replacement: Authorization<Id>,
}

impl<Id: Send + Sync + 'static> RulesSwap for Swap<Id> {
    fn install(self: Box<Self>) {
        self.current.replace_from(&self.replacement);
    }
}

impl<Id: std::fmt::Display + Send + Sync + 'static> RulesFollower for Coordinator<Id> {
    fn changed(&self, database: &LiveDatabase) -> Vec<String> {
        let current = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        changed_since(&current.translated, current.database.as_deref(), database)
    }

    fn translate(
        &self,
        database: &LiveDatabase,
        changed: &[String],
    ) -> Result<LiveTranslation, SetupError> {
        self.current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .translated
            .follow(database, changed)
    }

    fn install<'a>(&'a self, translated: &'a Translated) -> RulesInstallation<'a> {
        Box::pin(async move {
            let mut pending = self.pending.lock().await;
            let (replacement, _upkeep) =
                install_authorization(translated, &self.facts, &self.openfga, &mut pending).await?;
            Ok(Box::new(Swap {
                current: self.auth.clone(),
                replacement,
            }) as Box<dyn RulesSwap>)
        })
    }

    fn accept(&self, translated: Translated, database: Arc<LiveDatabase>) {
        *self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = AcceptedRules {
            translated,
            database: Some(database),
        };
    }

    fn abandon(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            *self.pending.lock().await = None;
        })
    }
}

fn changed_tables<Baseline: DatabaseLike, Live: DatabaseLike>(
    catalog: &ParserDB,
    baseline: &Baseline,
    live: &Live,
) -> Vec<String> {
    catalog
        .tables()
        .filter_map(|table| {
            let target = TargetName::new(table.table_name(), table.table_name_is_quoted());
            let target = match table.table_schema() {
                Some(schema) => target.with_schema(schema, table.table_schema_is_quoted()),
                None => target,
            };
            let before = baseline
                .resolve_target_table(
                    target.clone(),
                    <Postgres as Backend>::WRITTEN_TABLE_NAME_CASE,
                )
                .ok()
                .flatten()
                .map(|table| TableRules::of(table, baseline));
            let after = live
                .resolve_target_table(target, <Postgres as Backend>::WRITTEN_TABLE_NAME_CASE)
                .ok()
                .flatten()
                .map(|table| TableRules::of(table, live));
            (before != after).then(|| table.table_name().to_owned())
        })
        .collect()
}

pub(crate) struct InstalledModel {
    shapes: Arc<subql::visibility::shapes::Shapes<ParserDB>>,
    model: ModelState,
    channel: Channel,
}

pub(crate) async fn install_authorization<Id: std::fmt::Display + Send + Sync + 'static>(
    translated: &Translated,
    facts: &FactSource,
    openfga: &OpenFga,
    pending: &mut Option<InstalledModel>,
) -> Result<(Authorization<Id>, Arc<dyn crate::openfga::StoreUpkeep>), SetupError> {
    let shapes = translated.shapes_arc();
    if pending
        .as_ref()
        .is_some_and(|installed| !Arc::ptr_eq(&installed.shapes, &shapes))
    {
        *pending = None;
    }
    if pending.is_none() {
        let channel = openfga.connect().await?;
        let mut setup = OpenFgaServiceClient::new(channel.clone());
        let model = translated.install_model(&mut setup, &openfga.store).await?;
        *pending = Some(InstalledModel {
            shapes: Arc::clone(&shapes),
            model,
            channel,
        });
    }
    let installed = pending.as_ref().expect("model installation completed");
    let loader = OpenFgaPolicy::<_, _, ModelSubject<String, String>, Postgres>::new(
        Arc::clone(&shapes),
        OpenFgaServiceClient::new(installed.channel.clone()),
        openfga.store.clone(),
        installed.model.model(),
    )?;
    match &installed.model {
        ModelState::Written(_) => {
            let loaded_facts = translated.load_into(facts, &loader).await?;
            tracing::info!(
                model = installed.model.id(),
                facts = loaded_facts,
                "authorization rules are new, loading the facts behind them"
            );
        }
        ModelState::Adopted(_) => {
            let (added, removed) = translated.reconcile_store(facts, &loader).await?;
            tracing::info!(
                model = installed.model.id(),
                added,
                removed,
                "authorization rules already installed, the store reconciled with the database"
            );
        }
    }
    let delegate = OpenFgaPolicy::new(
        Arc::clone(&shapes),
        OpenFgaServiceClient::new(Counted::new(installed.channel.clone())),
        openfga.store.clone(),
        installed.model.model(),
    )?;
    let auth =
        FgaAuth::new(shapes, delegate, translated.naming()).with_outputs(&translated.outputs_arc());
    let upkeep = auth.upkeep(
        translated.reach_arc(),
        translated.translator().clone(),
        facts.clone(),
    );
    Ok((auth, upkeep))
}

/// Compare live fingerprints with persisted fingerprints or their document baseline.
#[must_use]
pub fn changed_fingerprints(
    stored: &std::collections::BTreeMap<String, String>,
    documents: &std::collections::BTreeMap<String, String>,
    live: &std::collections::BTreeMap<String, String>,
) -> Vec<String> {
    live.iter()
        .filter(|(table, fingerprint)| {
            stored.get(*table).or_else(|| documents.get(*table)) != Some(*fingerprint)
        })
        .map(|(table, _)| table.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::changed_tables;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::ParserDB;

    #[test]
    fn missing_fingerprints_compare_against_documents() {
        use std::collections::BTreeMap;
        let documents = BTreeMap::from([("docs".to_owned(), "document".to_owned())]);
        assert_eq!(
            super::changed_fingerprints(&BTreeMap::new(), &documents, &documents),
            Vec::<String>::new()
        );
        let live = BTreeMap::from([("docs".to_owned(), "live".to_owned())]);
        assert_eq!(
            super::changed_fingerprints(&BTreeMap::new(), &documents, &live),
            vec!["docs"]
        );
        assert_eq!(
            super::changed_fingerprints(&live, &documents, &live),
            Vec::<String>::new()
        );
    }

    #[test]
    fn unchanged_rules_have_no_changed_tables() {
        let catalog =
            ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE items (id INTEGER PRIMARY KEY);")
                .expect("catalog");
        assert_eq!(
            changed_tables(&catalog, &catalog, &catalog),
            Vec::<String>::new()
        );
    }

    #[test]
    fn policy_change_names_only_its_served_table() {
        let documents = "CREATE ROLE reader; CREATE TABLE items (id INTEGER PRIMARY KEY); CREATE TABLE other (id INTEGER PRIMARY KEY); ALTER TABLE items ENABLE ROW LEVEL SECURITY; CREATE POLICY visible ON items TO reader USING (id > 1);";
        let catalog = ParserDB::parse::<PostgreSqlDialect>(documents).expect("catalog");
        let live = ParserDB::parse::<PostgreSqlDialect>(&documents.replace("id > 1", "id > 2"))
            .expect("live catalog");
        assert_eq!(changed_tables(&catalog, &catalog, &live), vec!["items"]);
        assert_eq!(changed_tables(&catalog, &live, &live), Vec::<String>::new());
    }

    #[test]
    fn missing_live_table_is_changed() {
        let catalog =
            ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE items (id INTEGER PRIMARY KEY);")
                .expect("catalog");
        let live = ParserDB::parse::<PostgreSqlDialect>("").expect("live catalog");
        assert_eq!(changed_tables(&catalog, &catalog, &live), vec!["items"]);
    }
}
