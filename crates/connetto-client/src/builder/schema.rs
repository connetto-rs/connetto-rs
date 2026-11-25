//! The schema half of a client build.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use connetto_core::schema::UUID_FUNCTION;
use connetto_core::schema::{ReplicaCopy, SchemaBundle};
use connetto_core::{CallerKind, Major};

use crate::PolicyTables;
use crate::SqlFunctions;
use crate::tier_migration::{TierPlan, TierStep};

/// Why a tier step was refused at registration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TierStepRefused {
    /// A data step registered for a boundary this build carries no
    /// generated step for, so it could never run.
    #[error(
        "a tier step starts at major {from}, outside the boundaries this build carries, from {oldest} up to {build}"
    )]
    Outside {
        /// The major the step starts at.
        from: Major,
        /// The oldest major the build migrates from.
        oldest: Major,
        /// The build's major.
        build: Major,
    },
}

/// A translated schema and the client-side registrations its DDL reaches, as
/// one value, opening the copy of the kind the client acts as.
///
/// A build starts anonymous and signing in moves it to the signed-in copy,
/// since each replica file holds one kind only.
///
/// Every build registers `uuidv4()`, because a synced column `DEFAULT` may
/// call it and a device without a server still has to mint the id, and the
/// `jsonb` functions and collation the translation names, so any deployment
/// may declare `jsonb`. It also registers each helper any kind's DDL calls as
/// a table-valued function. The application adds its own functions with
/// [`with_sql_functions`](Self::with_sql_functions).
#[derive(Clone)]
pub struct SyncSchema {
    bundle: SchemaBundle,
    /// The kind whose copy this client opens.
    kind: CallerKind,
    policy_tables: PolicyTables,
    sql_functions: SqlFunctions,
    unrecorded: HashSet<String>,
    /// Both tiers as one main-schema DDL, for a relay tab's mirror.
    mirror_ddl: Option<String>,
    shared_store: bool,
    /// Whether this is a relay tab's mirror.
    relay: bool,
    /// The application's data step for each tier boundary, keyed by the
    /// major it starts at (R31).
    tier_steps: BTreeMap<Major, Arc<dyn TierStep>>,
}

impl SyncSchema {
    /// Wrap a translated schema, opening the anonymous copy, and register
    /// connetto's own functions and the helpers.
    #[must_use]
    pub fn new(bundle: SchemaBundle) -> Self {
        let kind = CallerKind::Anon;
        let policy_tables = policy_tables_of(bundle.copy(kind));
        let sql_functions = connetto_installers().merged(helper_installer(&bundle));
        Self {
            bundle,
            kind,
            policy_tables,
            sql_functions,
            unrecorded: HashSet::new(),
            mirror_ddl: None,
            shared_store: true,
            relay: false,
            tier_steps: BTreeMap::new(),
        }
    }

    /// The same schema opening `kind`'s copy, which the build stage that
    /// fixes the client's kind calls, signing in for the signed-in copy.
    #[must_use]
    pub fn acting_as(mut self, kind: CallerKind) -> Self {
        self.kind = kind;
        self.policy_tables = policy_tables_of(self.copy());
        if self.relay {
            self.mirror_ddl = self.mirror_of();
        }
        self
    }

    /// The kind whose copy this client opens.
    #[must_use]
    pub const fn kind(&self) -> CallerKind {
        self.kind
    }

    /// The copy this client opens.
    fn copy(&self) -> &ReplicaCopy {
        self.bundle.copy(self.kind)
    }

    /// Both tiers as one main-schema DDL, absent without a tier.
    fn mirror_of(&self) -> Option<String> {
        self.bundle
            .local_tier_ddl()
            .map(|tier| format!("{}\n{tier}", self.copy().replica_ddl()))
    }

    /// Add the application's own function installers alongside connetto's.
    #[must_use]
    pub fn with_sql_functions(mut self, functions: SqlFunctions) -> Self {
        self.sql_functions = self.sql_functions.merged(functions);
        self
    }

    /// Register the application's data step for the tier boundary starting at
    /// `from`, run after the structural step the build generated for it, in
    /// the same transaction (R31).
    ///
    /// # Errors
    ///
    /// [`TierStepRefused`] when `from` is not a boundary this build carries,
    /// one from the oldest exported major up to the major before its own.
    pub fn with_tier_step(
        mut self,
        from: Major,
        step: impl TierStep,
    ) -> Result<Self, TierStepRefused> {
        let history = self.bundle.tier_history();
        let build = self.bundle.major();
        if from < history.oldest() || from >= build {
            return Err(TierStepRefused::Outside {
                from,
                oldest: history.oldest(),
                build,
            });
        }
        self.tier_steps.insert(from, Arc::new(step));
        Ok(self)
    }

    /// What brings an existing device-private tier to this build's major,
    /// absent when the build has no tier or is a relay mirror.
    pub(crate) fn tier_plan(&self) -> Option<TierPlan> {
        self.local_tier_ddl().map(|ddl| TierPlan {
            build: self.bundle.major(),
            ddl: ddl.to_owned(),
            history: self.bundle.tier_history().clone(),
            steps: Arc::new(self.tier_steps.clone()),
        })
    }

    /// Name the tables whose missing key is intentional, so connetto records
    /// nothing for them and the schema's unkeyed tables are not refused.
    #[must_use]
    pub fn with_unrecorded_tables(
        mut self,
        tables: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.unrecorded = tables.into_iter().map(Into::into).collect();
        self
    }

    /// Enable the device-wide public cache, whose major upgrades preserve account queues and whose newer major refuses older builds.
    #[must_use]
    pub fn with_shared_store(mut self, enabled: bool) -> Self {
        self.shared_store = enabled;
        self
    }

    /// The shared-file initialization programs when this connection shares rows.
    #[must_use]
    pub fn shared_layout(&self) -> Option<&connetto_core::schema::SharedLayout> {
        if self.shared_store && self.mirror_ddl.is_none() {
            self.copy().shared_layout()
        } else {
            None
        }
    }

    /// The physical tables whose rows live in the shared file.
    pub fn shared_tables(&self) -> impl Iterator<Item = &str> {
        self.copy()
            .shared_tables()
            .iter()
            .map(|logical| self.policy_tables.physical(logical).unwrap_or(logical))
    }

    /// Connection-scoped statements for the unshared layout.
    #[must_use]
    pub fn connection_ddl(&self) -> &str {
        self.copy().connection_ddl()
    }

    /// Which caller settings the deployment's policies may find unset, which
    /// decides what an absent caller half renders as on the replica.
    #[must_use]
    pub fn unset_settings(&self) -> connetto_core::auth::UnsetSettings {
        self.bundle.unset_settings()
    }

    /// The same schema as a relay tab's mirror runs it, both tiers in the
    /// main schema.
    ///
    /// A relay applies every patch it serves a tab, synced and device-private
    /// alike, to the tab's main schema, and the relay itself keeps the tiers
    /// apart. The version stays the bundle's, which is what the worker the
    /// relay speaks for presents.
    #[must_use]
    pub fn relay_mirror(mut self) -> Self {
        self.shared_store = false;
        self.mirror_ddl = self.mirror_of();
        self.relay = true;
        self
    }

    /// Whether this schema is a relay tab's mirror, whose worker holds the
    /// share keys.
    #[must_use]
    pub(crate) fn is_relay_mirror(&self) -> bool {
        self.relay
    }

    /// The translated replica DDL a first boot applies.
    #[must_use]
    pub fn replica_ddl(&self) -> &str {
        self.mirror_ddl
            .as_deref()
            .unwrap_or_else(|| self.copy().replica_ddl())
    }

    /// The translated device-private tier DDL, absent when the build has none
    /// or when the schema is a relay mirror, whose tier lives in main.
    #[must_use]
    pub fn local_tier_ddl(&self) -> Option<&str> {
        match self.mirror_ddl {
            Some(_) => None,
            None => self.bundle.local_tier_ddl(),
        }
    }

    /// The schema version the handshake reports.
    #[must_use]
    pub fn version(&self) -> connetto_core::schema::SchemaVersion {
        self.bundle.version()
    }

    /// The application schema major the handshake declares and the replica
    /// records (R31).
    #[must_use]
    pub const fn major(&self) -> connetto_core::Major {
        self.bundle.major()
    }

    /// The RLS split the sync boundaries rename through.
    #[must_use]
    pub fn policy_tables(&self) -> &PolicyTables {
        &self.policy_tables
    }

    /// The function installers, connetto's first and the application's after.
    #[must_use]
    pub fn sql_functions(&self) -> &SqlFunctions {
        &self.sql_functions
    }

    /// The device-only tables, as given.
    #[must_use]
    pub fn unrecorded_tables(&self) -> &HashSet<String> {
        &self.unrecorded
    }
}

/// The installers that register `uuidv4()` and the `jsonb` set.
fn connetto_installers() -> SqlFunctions {
    SqlFunctions::new()
        .with(Arc::new(|conn| {
            // diesel's own registrar seam, hidden API by design, since the
            // function is client-side only and must exist before any DDL runs.
            conn.register_noarg_sql_function::<diesel::sql_types::Binary, _, _>(
                UUID_FUNCTION,
                diesel::sqlite::SqliteFunctionBehavior::INNOCUOUS,
                || uuid::Uuid::new_v4().into_bytes(),
            )
        }))
        .with(Arc::new(|conn| {
            connetto_core::schema::jsonb_functions().register(conn)
        }))
}

/// The RLS split `copy` renames its tables through.
fn policy_tables_of(copy: &ReplicaCopy) -> PolicyTables {
    PolicyTables::from_translation(
        copy.policy_tables()
            .iter()
            .map(|(logical, physical)| (logical.as_str(), physical.as_str())),
        copy.policy_views().iter().cloned(),
    )
}

/// The installer that registers every helper any kind's DDL calls as a
/// table-valued function, each once, none when no copy calls one.
fn helper_installer(bundle: &SchemaBundle) -> SqlFunctions {
    let mut functions = Vec::new();
    for kind in CallerKind::ALL {
        for function in bundle.copy(kind).helper_functions() {
            if !functions.contains(function) {
                functions.push(function.clone());
            }
        }
    }
    if functions.is_empty() {
        return SqlFunctions::new();
    }
    SqlFunctions::new().with(Arc::new(move |conn| {
        functions
            .iter()
            .try_for_each(|function| function.register(conn))
    }))
}
