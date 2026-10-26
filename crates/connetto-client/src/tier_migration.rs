//! The device-private tier's migration across application schema majors (R31).
//!
//! The tier records its major in its own `PRAGMA user_version`. A tier behind
//! the build is migrated on a connection of its own, opened on the tier file
//! alone as its `main` database under the replica key, before the tier is
//! attached to the replica connection. Unqualified SQL and SQLite's table
//! rebuild therefore land in the tier, synced tables are out of reach, and
//! turning foreign keys off touches this connection alone. The handles are
//! sequential, never concurrent, which the browser's storage pool requires.
//!
//! Each boundary runs in one transaction: the structural step the build
//! generated from the exported tier history, then the application's data
//! step for that boundary, then on the last boundary a check that the tier's
//! shape is the build's tier DDL, then the foreign key check and the
//! `user_version` bump. A failure rolls that boundary back and refuses the
//! open, and the boundaries committed before it stay.

use core::fmt;
use std::collections::BTreeMap;
use std::sync::Arc;

use connetto_core::{Major, ShapeDifference, TierHistory, read_tier_shape};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::{ClientError, ReplicaKey, SqlFunctions, cipher, harden};

/// What an application step returns when it cannot convert its data. The
/// step is the application's code, so its error type is the application's.
pub type TierStepError = Box<dyn std::error::Error + Send + Sync>;

/// The application's data conversion for one tier boundary, run after the
/// structural step the build generated for it, in the same transaction.
///
/// A step talks to the tier in SQL against the shape that boundary produces,
/// never through the build's current `table!` types, which describe the
/// newest major and not the one this step runs at.
pub trait TierStep: Send + Sync + 'static {
    /// Convert the tier's data across this boundary.
    ///
    /// # Errors
    ///
    /// Whatever stops the conversion, which rolls the boundary back and
    /// refuses the open.
    fn migrate(&self, tier: &mut SqliteConnection) -> Result<(), TierStepError>;
}

impl<F> TierStep for F
where
    F: Fn(&mut SqliteConnection) -> Result<(), TierStepError> + Send + Sync + 'static,
{
    fn migrate(&self, tier: &mut SqliteConnection) -> Result<(), TierStepError> {
        self(tier)
    }
}

/// Everything an open needs to bring a tier to the build's major.
#[derive(Clone)]
pub(crate) struct TierPlan {
    /// The build's major.
    pub(crate) build: Major,
    /// The build's translated tier DDL, unqualified.
    pub(crate) ddl: String,
    /// The exported history the build carries.
    pub(crate) history: TierHistory,
    /// The application's data steps, keyed by the major each starts at.
    pub(crate) steps: Arc<BTreeMap<Major, Arc<dyn TierStep>>>,
}

impl fmt::Debug for TierPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TierPlan")
            .field("build", &self.build)
            .field("history", &self.history)
            .field("steps", &self.steps.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl TierPlan {
    /// Whether a replica that recorded `prior` predates the first major with
    /// a tier, so a missing tier file was never created rather than lost.
    pub(crate) fn introduced_after(&self, prior: Major) -> bool {
        self.history
            .first_with_tier()
            .is_some_and(|first| prior < first)
    }
}

/// Why one tier boundary could not be crossed.
#[derive(Debug, thiserror::Error)]
pub enum TierFailure {
    /// A statement of the generated step, or the bump, failed.
    #[error(transparent)]
    Sql(#[from] diesel::result::Error),
    /// The application's data step failed.
    #[error("the application's step failed: {0}")]
    Step(TierStepError),
    /// The migrated tier is not the shape the build's tier DDL creates.
    #[error("the migrated tier differs from the build's tier DDL: {}", describe(.0))]
    Shape(Vec<ShapeDifference>),
    /// Rows violate a foreign key after the boundary.
    #[error("{0} rows violate a foreign key after the boundary")]
    ForeignKeys(usize),
    /// Reading a shape failed.
    #[error(transparent)]
    ShapeRead(#[from] connetto_core::ShapeError),
    /// The scratch database the build's tier DDL is checked through did not
    /// open.
    #[error("opening the scratch database for the shape check: {0}")]
    Scratch(#[from] diesel::ConnectionError),
}

/// The differences as one line, for the error message.
fn describe(differences: &[ShapeDifference]) -> String {
    differences
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// One `user_version` reading.
#[derive(QueryableByName)]
struct UserVersion {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    user_version: i32,
}

/// One row `PRAGMA foreign_key_check` reports.
#[derive(QueryableByName)]
struct Violation {
    #[diesel(sql_type = diesel::sql_types::Text)]
    #[diesel(column_name = table)]
    #[expect(
        dead_code,
        reason = "diesel reads the column through QueryableByName and only the count is reported"
    )]
    violating: String,
}

/// The major the tier attached as `schema` records, 0 for none.
pub(crate) fn recorded(db: &mut SqliteConnection, schema: &str) -> Result<u32, ClientError> {
    // A vendor pragma on an attached schema, which the typed DSL cannot express.
    let reading: UserVersion = diesel::sql_query(format!(
        "PRAGMA {}.user_version",
        connetto_core::quote_ident(schema)
    ))
    .get_result(db)?;
    Ok(u32::try_from(reading.user_version).unwrap_or(0))
}

/// Record `major` on the tier attached as `schema`.
pub(crate) fn stamp(
    db: &mut SqliteConnection,
    schema: &str,
    major: Major,
) -> Result<(), ClientError> {
    // A vendor pragma, which the typed DSL cannot express. The value is a
    // number, so formatting it in is no injection.
    db.batch_execute(&format!(
        "PRAGMA {}.user_version = {}",
        connetto_core::quote_ident(schema),
        major.get()
    ))?;
    Ok(())
}

/// What the tier needs, from the major it records.
pub(crate) enum Needed {
    /// It records the build's major.
    Nothing,
    /// It was made before majors existed and takes the build's.
    Stamp,
    /// It is behind and migrates from this major.
    Migrate(Major),
}

/// Decide what a tier recording `recorded` needs under `plan`.
///
/// # Errors
///
/// [`ClientError::TierNewer`] for a tier ahead of the build and
/// [`ClientError::TierTooOld`] for one older than the oldest export.
pub(crate) fn needed(recorded: u32, plan: &TierPlan) -> Result<Needed, ClientError> {
    let Some(major) = Major::new(recorded) else {
        return Ok(Needed::Stamp);
    };
    if major == plan.build {
        Ok(Needed::Nothing)
    } else if major > plan.build {
        Err(ClientError::TierNewer {
            recorded: major,
            build: plan.build,
        })
    } else if major < plan.history.oldest() {
        Err(ClientError::TierTooOld {
            recorded: major,
            oldest: plan.history.oldest(),
        })
    } else {
        Ok(Needed::Migrate(major))
    }
}

/// Migrate the tier at `path` from `from` to the build's major, on a
/// connection of its own that is closed before this returns.
///
/// # Errors
///
/// [`ClientError::TierMigration`] naming the boundary that failed and why,
/// or the failure to open the tier alone.
pub(crate) fn migrate_alone(
    path: &str,
    key: Option<&ReplicaKey>,
    functions: &SqlFunctions,
    plan: &TierPlan,
    from: Major,
) -> Result<(), ClientError> {
    let mut tier =
        SqliteConnection::establish(path).map_err(|e| ClientError::Connect(e.to_string()))?;
    if let Some(key) = key {
        cipher::unlock(&mut tier, key)?;
    }
    harden::harden_replica_connection(&mut tier)?;
    // A column DEFAULT calling an application function fires on the rows a
    // table rebuild copies, so the functions exist before the first step.
    functions
        .install(&mut tier)
        .map_err(|e| ClientError::Session(e.to_string()))?;
    // Outside any transaction, as SQLite's table rebuild requires, and on
    // this connection alone.
    tier.batch_execute("PRAGMA foreign_keys = OFF")?;
    for boundary in plan
        .history
        .boundaries()
        .iter()
        .filter(|boundary| boundary.from() >= from)
    {
        let start = boundary.from();
        let to = Major::new(start.get() + 1)
            .ok_or_else(|| ClientError::Session(format!("major {start} has no successor")))?;
        let last = to == plan.build;
        tier.immediate_transaction(|tier| cross(tier, plan, boundary.sql(), start, to, last))
            .map_err(|cause| ClientError::TierMigration {
                from: start,
                to,
                cause,
            })?;
    }
    Ok(())
}

/// One boundary's body, inside its transaction.
fn cross(
    tier: &mut SqliteConnection,
    plan: &TierPlan,
    generated: &str,
    start: Major,
    to: Major,
    last: bool,
) -> Result<(), TierFailure> {
    tier.batch_execute(generated)?;
    if let Some(step) = plan.steps.get(&start) {
        step.migrate(tier).map_err(TierFailure::Step)?;
    }
    if last {
        let migrated = read_tier_shape(tier)?;
        let mut scratch = SqliteConnection::establish(":memory:")?;
        scratch.batch_execute(&plan.ddl)?;
        let expected = read_tier_shape(&mut scratch)?;
        let differences = expected.diff(&migrated);
        if !differences.is_empty() {
            return Err(TierFailure::Shape(differences));
        }
    }
    // A vendor pragma, which the typed DSL cannot express.
    let violations: Vec<Violation> = diesel::sql_query("PRAGMA foreign_key_check").load(tier)?;
    if !violations.is_empty() {
        return Err(TierFailure::ForeignKeys(violations.len()));
    }
    // A vendor pragma, which the typed DSL cannot express. The value is a
    // number, so formatting it in is no injection.
    tier.batch_execute(&format!("PRAGMA user_version = {}", to.get()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientConfig, ConnettoConnection, Replica};
    use connetto_core::{TierBoundary, test_support::FakeTransport};

    const TIER_V1: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";
    const TIER_V2: &str =
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, pinned INTEGER NOT NULL DEFAULT 0)";
    const ADD_PINNED: &str = "ALTER TABLE notes ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0";

    fn major(value: u32) -> Major {
        Major::new(value).expect("a test major is from 1 up")
    }

    /// A plan migrating `from` to `to` with one generated step and no data steps.
    fn plan(from: Major, to: Major, ddl: &str, generated: &str) -> TierPlan {
        TierPlan {
            build: to,
            ddl: ddl.to_owned(),
            history: TierHistory::new(from, Some(from), vec![TierBoundary::new(from, generated)]),
            steps: Arc::new(BTreeMap::new()),
        }
    }

    /// An unencrypted tier at `path`, stamped `stamped`, with one row.
    fn plain_tier(path: &std::path::Path, stamped: u32) {
        let mut tier =
            SqliteConnection::establish(path.to_str().expect("utf-8")).expect("open the tier");
        tier.batch_execute(TIER_V1).expect("tier schema");
        tier.batch_execute("INSERT INTO notes (id, body) VALUES (7, 'kept')")
            .expect("tier row");
        tier.batch_execute(&format!("PRAGMA user_version = {stamped}"))
            .expect("stamp");
    }

    /// One `user_version` reading, which no `table!` can name.
    #[derive(QueryableByName)]
    struct UserVersion {
        /// The major the tier records.
        #[diesel(sql_type = diesel::sql_types::Integer)]
        user_version: i32,
    }

    fn tier_major(path: &std::path::Path) -> i32 {
        let mut tier =
            SqliteConnection::establish(path.to_str().expect("utf-8")).expect("open the tier");
        // A vendor pragma, which the typed DSL cannot express.
        diesel::sql_query("PRAGMA user_version")
            .get_result::<UserVersion>(&mut tier)
            .expect("read the tier's major")
            .user_version
    }

    /// A plan's `Debug` names its build, history and steps, so a log shows what a migration carried.
    #[test]
    fn a_plan_debug_names_its_build_history_and_steps() {
        let mut steps: BTreeMap<Major, Arc<dyn TierStep>> = BTreeMap::new();
        steps.insert(
            major(1),
            Arc::new(|_tier: &mut SqliteConnection| -> Result<(), TierStepError> { Ok(()) }),
        );
        let plan = TierPlan {
            build: major(2),
            ddl: TIER_V2.to_owned(),
            history: TierHistory::new(major(1), Some(major(1)), Vec::new()),
            steps: Arc::new(steps),
        };
        let text = format!("{plan:?}");
        assert!(text.starts_with("TierPlan {"), "{text}");
        assert!(text.contains("build"), "{text}");
        assert!(text.contains("history"), "{text}");
        assert!(text.contains("steps"), "{text}");
    }

    /// A tier written without a key migrates on its own connection and records the build's major.
    #[test]
    fn a_tier_without_a_key_migrates_on_its_own() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("tier.db");
        plain_tier(&path, 1);

        migrate_alone(
            path.to_str().expect("the path is utf-8"),
            None,
            &SqlFunctions::new(),
            &plan(major(1), major(2), TIER_V2, ADD_PINNED),
            major(1),
        )
        .expect("an unkeyed tier migrates");
        assert_eq!(tier_major(&path), 2);
    }

    /// A migration whose tier file is missing refuses to open it rather than starting an empty one.
    #[test]
    fn a_missing_tier_file_refuses_the_alone_migration() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("absent").join("tier.db");
        match migrate_alone(
            missing.to_str().expect("the path is utf-8"),
            None,
            &SqlFunctions::new(),
            &plan(major(1), major(2), TIER_V2, ADD_PINNED),
            major(1),
        ) {
            Err(ClientError::Connect(_)) => {}
            other => panic!("expected the open refusal, got {other:?}"),
        }
    }

    /// A tier this key did not write refuses the migration before a step runs.
    #[test]
    fn a_wrong_key_refuses_the_alone_migration() {
        let dir = tempfile::tempdir().expect("temp dir");
        let replica_path = dir.path().join("replica.db");
        {
            let replica = Replica::encrypted_file(
                replica_path.to_str().expect("the path is utf-8"),
                Some(ReplicaKey::from_bytes([0x11; ReplicaKey::LEN])),
            )
            .expect("replica")
            .with_tier(TIER_V1);
            ConnettoConnection::<FakeTransport>::open(
                &replica,
                "CREATE TABLE items (id INTEGER PRIMARY KEY)",
                &ClientConfig::new("wrong-key-test"),
                None,
            )
            .expect("first boot");
        }

        let tier_path = dir.path().join("replica.db-tier");
        match migrate_alone(
            tier_path.to_str().expect("the path is utf-8"),
            Some(&ReplicaKey::from_bytes([0x22; ReplicaKey::LEN])),
            &SqlFunctions::new(),
            &plan(major(1), major(2), TIER_V2, ADD_PINNED),
            major(1),
        ) {
            Err(ClientError::ReplicaUndecryptable(_)) => {}
            other => panic!("expected the wrong key refusal, got {other:?}"),
        }
    }

    /// A function installer that fails refuses the migration before a step runs.
    #[test]
    fn a_failing_function_installer_refuses_the_alone_migration() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("tier.db");
        plain_tier(&path, 1);
        let functions = SqlFunctions::new().with(Arc::new(|tier: &mut SqliteConnection| {
            tier.batch_execute("THIS IS NOT SQL")
        }));
        match migrate_alone(
            path.to_str().expect("the path is utf-8"),
            None,
            &functions,
            &plan(major(1), major(2), TIER_V2, ADD_PINNED),
            major(1),
        ) {
            Err(ClientError::Session(message)) => {
                assert!(message.contains("syntax"), "{message}");
            }
            other => panic!("expected the installer's refusal, got {other:?}"),
        }
    }

    /// A boundary past the last major the build can reach is refused by name.
    #[test]
    fn a_boundary_without_a_successor_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("tier.db");
        plain_tier(&path, 1);
        let plan = TierPlan {
            build: major(2),
            ddl: TIER_V2.to_owned(),
            history: TierHistory::new(
                major(u32::MAX),
                Some(major(u32::MAX)),
                vec![TierBoundary::new(major(u32::MAX), "")],
            ),
            steps: Arc::new(BTreeMap::new()),
        };
        match migrate_alone(
            path.to_str().expect("the path is utf-8"),
            None,
            &SqlFunctions::new(),
            &plan,
            major(1),
        ) {
            Err(ClientError::Session(message)) => {
                assert!(message.contains("no successor"), "{message}");
            }
            other => panic!("expected the successor refusal, got {other:?}"),
        }
    }

    /// Reading a major from a schema the connection does not carry is refused.
    #[test]
    fn a_recorded_read_on_an_absent_schema_is_refused() {
        let mut db = SqliteConnection::establish(":memory:").expect("scratch");
        assert!(
            recorded(&mut db, "absent").is_err(),
            "a missing schema cannot record a major"
        );
    }

    /// Stamping a major on a schema the connection does not carry is refused.
    #[test]
    fn a_stamp_on_an_absent_schema_is_refused() {
        let mut db = SqliteConnection::establish(":memory:").expect("scratch");
        assert!(
            stamp(&mut db, "absent", major(1)).is_err(),
            "a missing schema cannot take a major"
        );
    }
}
