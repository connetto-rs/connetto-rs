//! The read budget one re-execution spends, and the session setup that
//! carries it into subql's shipped connector.
//!
//! R58 bounded the row snapshot with a per-tier `SET LOCAL statement_timeout`
//! (see [`snapshot`](crate::snapshot)) and the aggregate paths never got it: an
//! aggregate's seed and every re-execution triggered by a change ran a full
//! query with no time limit, so one aggregate over an unindexed table could
//! stall every read that shares the connector's pool with it.
//!
//! subql's `PgAsyncDieselConnector` runs the reads (the scalar and seed
//! shapes, budgeted pages, and the held cursors a whole-answer read needs) and
//! takes a [`SessionSetup`] whose statements run inside every transaction it
//! opens, the cursor's held transaction included. connetto's setup carries the
//! one statement its ceilings need today, the timeout, and the R85 per-viewer
//! binding rides beside it, the identity and the packed subjects the caller
//! holds. The timeout cannot live upstream because it is per tier, and a tier
//! is whether the handshake resolved an identity, which subql does not model.
//!
//! The setup travels as the connector's `AuthContext`, stored per registered
//! subscription and passed verbatim to each call, so the caller of the moment
//! decides: a fold's seed spends its own caller's tier, and everything the
//! engine drives spends the shorter shared bound
//! ([`ThrottleConfig::reexec_timeout`](crate::ThrottleConfig::reexec_timeout))
//! because what it delays is the change stream rather than its owner.

use core::future::Future;
use core::time::Duration;

use diesel::pg::sql_state;
use diesel::result::{SqlState, SqlStateClass};

use subql::backend::{Postgres, ScalarFamily, Value as PgValue};
use subql::reexec::{
    AsyncConnector, DieselAsyncError, PgAsyncDieselConnector, ReadQuery, RowPage, SessionSetup,
    Snapshot,
};
use subql::{PgCommitPosition, PgSnapshotFence};

/// What one re-execution read may spend, passed per call.
///
/// One number today. It is a struct rather than a bare [`Duration`] because
/// R58's other two read limits (the page budget and the row ceiling) belong to
/// a row read that returns rows, and the shapes this connector serves return
/// one value or one row of values, so the ceilings that would join it here are
/// the ones the grouped and per-viewer tiers bring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBudget {
    /// Wall-clock ceiling on this read, enforced by Postgres itself.
    pub timeout: Duration,
}

impl ReadBudget {
    /// A budget of `timeout`.
    #[must_use]
    pub const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

/// The transaction-scoped statements connetto's reads run under, rendered
/// once per registration and handed to the connector as its auth context.
///
/// Today one statement, the budget's `SET LOCAL statement_timeout`. `SET
/// LOCAL` lasts exactly as long as the transaction and leaves nothing behind
/// on a pooled connection the ingest loop takes next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnettoReadSetup {
    statements: Vec<String>,
}

impl ConnettoReadSetup {
    /// The setup enforcing `budget`.
    #[must_use]
    pub fn of(budget: ReadBudget) -> Self {
        let timeout_ms = statement_timeout_ms(budget.timeout);
        Self {
            statements: vec![format!("SET LOCAL statement_timeout = {timeout_ms}")],
        }
    }

    /// The same setup with `extra` statements appended, in order, after the
    /// budget. This is where a per-viewer registration's caller binding rides
    /// (R85): the statements run inside every transaction the connector opens
    /// for the subscription, so each of its reads answers as that viewer.
    #[must_use]
    pub fn with_statements(mut self, extra: Vec<String>) -> Self {
        self.statements.extend(extra);
        self
    }
}

impl From<ReadBudget> for ConnettoReadSetup {
    fn from(budget: ReadBudget) -> Self {
        Self::of(budget)
    }
}

impl SessionSetup for ConnettoReadSetup {
    fn setup_statements(&self) -> &[String] {
        &self.statements
    }
}

/// The connector connetto hands the engine and the session: subql's shipped
/// async Postgres connector carrying connetto's setup. An alias rather than a
/// wrapper, because the seam made the downstream reimplementation deletable.
pub type PgReadConnector = PgAsyncDieselConnector<ConnettoReadSetup>;

/// The disposition one connector failure earns (R89 decision 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFailure {
    /// Postgres cancelled at connetto's own limit. Policy, the subscription ends at once (R81 decision 3).
    Timeout,
    /// The database or its pool was unreachable or cut off. Delivery pauses and retries in place (R89 decision 2).
    Transient,
    /// The database answered with a code outside the transient list. Asking again asks the same question, so the subscription ends at once (R106 decision 2).
    Deterministic,
    /// A failure carrying no code. One retry, then the subscription ends.
    Other,
}

impl core::fmt::Display for ReadFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Timeout => "timeout",
            Self::Transient => "transient",
            Self::Deterministic => "deterministic",
            Self::Other => "unknown",
        })
    }
}

/// The failure class of a connector error, asked where the limit and the
/// connection live.
pub trait FailedRead {
    /// The class this failure belongs to.
    fn read_failure(&self) -> ReadFailure;
}

/// upstream's `#[non_exhaustive]` forces the catch-all, so rows-unsupported
/// and future variants arrive as `Other`, the class that costs one retry and
/// no stall.
impl FailedRead for DieselAsyncError {
    fn read_failure(&self) -> ReadFailure {
        match self {
            Self::Pool(_) => ReadFailure::Transient,
            Self::Diesel(err) => diesel_failure(err),
            _ => ReadFailure::Other,
        }
    }
}

/// The codes besides class `08` that clear on retry (R106 decision 1).
///
/// Named one by one, because their classes also hold codes that never clear:
/// `40002`, `53400`, `57P04` and `58P01` would pause delivery for ever.
const TRANSIENT_CODES: [SqlState; 12] = [
    sql_state::T_R_SERIALIZATION_FAILURE,
    sql_state::T_R_DEADLOCK_DETECTED,
    sql_state::READ_ONLY_SQL_TRANSACTION,
    sql_state::DISK_FULL,
    sql_state::OUT_OF_MEMORY,
    sql_state::TOO_MANY_CONNECTIONS,
    sql_state::ADMIN_SHUTDOWN,
    sql_state::CRASH_SHUTDOWN,
    sql_state::CANNOT_CONNECT_NOW,
    sql_state::IDLE_SESSION_TIMEOUT,
    sql_state::SYSTEM_ERROR,
    sql_state::IO_ERROR,
];

/// The class of a diesel error, the one classifier the read, write, snapshot
/// and oplog paths share.
///
/// The SQLSTATE decides wherever the database reported one, and the kind
/// decides only for an error without one (R106 decision 3).
pub(crate) fn diesel_failure(err: &diesel::result::Error) -> ReadFailure {
    use diesel::result::{DatabaseErrorKind, Error};
    let Error::DatabaseError(kind, info) = err else {
        return ReadFailure::Other;
    };
    match info.sqlstate() {
        Some(sql_state::QUERY_CANCELED) => ReadFailure::Timeout,
        Some(code)
            if code.class() == SqlStateClass::CONNECTION_EXCEPTION
                || TRANSIENT_CODES.contains(&code) =>
        {
            ReadFailure::Transient
        }
        Some(_) => ReadFailure::Deterministic,
        None => match kind {
            DatabaseErrorKind::SerializationFailure
            | DatabaseErrorKind::ReadOnlyTransaction
            | DatabaseErrorKind::ClosedConnection
            | DatabaseErrorKind::UnableToSendCommand => ReadFailure::Transient,
            _ => ReadFailure::Other,
        },
    }
}

impl FailedRead for std::io::Error {
    fn read_failure(&self) -> ReadFailure {
        ReadFailure::Other
    }
}

impl FailedRead for core::convert::Infallible {
    fn read_failure(&self) -> ReadFailure {
        match *self {}
    }
}

impl FailedRead for String {
    fn read_failure(&self) -> ReadFailure {
        ReadFailure::Other
    }
}

impl FailedRead for crate::snapshot::SnapshotError {
    fn read_failure(&self) -> ReadFailure {
        match self {
            Self::TimedOut(_) => ReadFailure::Timeout,
            _ => ReadFailure::Other,
        }
    }
}

/// A diesel database error carrying `code` as its SQLSTATE, the shape `diesel_async` reports.
#[cfg(test)]
pub(crate) fn coded_error(
    kind: diesel::result::DatabaseErrorKind,
    code: &str,
    message: &str,
) -> diesel::result::Error {
    struct Coded {
        code: diesel::result::SqlState,
        message: String,
    }
    impl diesel::result::DatabaseErrorInformation for Coded {
        fn message(&self) -> &str {
            &self.message
        }
        fn details(&self) -> Option<&str> {
            None
        }
        fn hint(&self) -> Option<&str> {
            None
        }
        fn table_name(&self) -> Option<&str> {
            None
        }
        fn column_name(&self) -> Option<&str> {
            None
        }
        fn constraint_name(&self) -> Option<&str> {
            None
        }
        fn statement_position(&self) -> Option<i32> {
            None
        }
        fn sqlstate(&self) -> Option<diesel::result::SqlState> {
            Some(self.code)
        }
    }
    diesel::result::Error::DatabaseError(
        kind,
        Box::new(Coded {
            code: code.parse().expect("a five-character SQLSTATE"),
            message: message.to_owned(),
        }),
    )
}

/// The value Postgres accepts for `statement_timeout`, in milliseconds, for a
/// read that must be bounded.
///
/// Both ends matter and both ends are traps. Zero means no limit at all to
/// Postgres, which is the opposite of a spent budget, so the floor is one
/// millisecond. The setting is a signed 32-bit integer, so a budget past about
/// 24 days is outside its range and Postgres refuses the `SET` itself, failing
/// every read underneath a limit meant to be generous, so the ceiling clamps
/// rather than refuses.
pub(crate) fn statement_timeout_ms(budget: Duration) -> u32 {
    let ceiling = u128::try_from(i32::MAX).unwrap_or(u128::MAX);
    u32::try_from(budget.as_millis().clamp(1, ceiling)).unwrap_or(u32::MAX)
}

/// An [`AsyncConnector`] that fails every call: the default when a manager runs
/// no re-execution backend, appropriate when no computed subscriptions exist.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoConnector;

#[expect(
    clippy::manual_async_fn,
    reason = "the trait names the future type so the Send bound is stated on the signature"
)]
impl AsyncConnector for NoConnector {
    type AuthContext = ConnettoReadSetup;
    type Error = std::io::Error;
    type Checkpoint = PgCommitPosition;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _setup: &ConnettoReadSetup,
    ) -> impl Future<Output = Result<(PgValue<Postgres>, Option<PgSnapshotFence>), std::io::Error>> + Send
    {
        async {
            Err(std::io::Error::other(
                "no re-execution connector configured",
            ))
        }
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _setup: &ConnettoReadSetup,
    ) -> impl Future<Output = Result<Snapshot<RowPage<Postgres>, PgCommitPosition>, std::io::Error>> + Send
    {
        async {
            Err(std::io::Error::other(
                "no re-execution connector configured",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConnettoReadSetup, Duration, FailedRead, ReadBudget, ReadFailure, SessionSetup,
        coded_error, statement_timeout_ms,
    };
    use diesel::result::{DatabaseErrorKind, Error};
    use subql::reexec::DieselAsyncError;

    /// The floor is the one that would fail silently: `statement_timeout = 0`
    /// is Postgres for "no limit", so a budget already spent would buy an
    /// unbounded read, which is the failure this phase exists to remove.
    #[test]
    fn a_spent_budget_still_sets_a_limit() {
        assert_eq!(statement_timeout_ms(Duration::ZERO), 1);
        assert_eq!(statement_timeout_ms(Duration::from_millis(250)), 250);
    }

    /// The ceiling clamps rather than refuses: a value Postgres cannot accept
    /// would fail every read underneath a limit meant to be generous.
    #[test]
    fn an_oversized_budget_clamps_to_what_postgres_accepts() {
        let clamped = statement_timeout_ms(Duration::from_secs(u64::MAX / 4));
        assert_eq!(clamped, u32::try_from(i32::MAX).expect("positive"));
    }

    /// The setup renders the budget as the one transaction-scoped statement
    /// the connector runs before the read.
    #[test]
    fn the_setup_carries_the_timeout_statement() {
        let setup = ConnettoReadSetup::of(ReadBudget::new(Duration::from_millis(1500)));
        assert_eq!(
            setup.setup_statements(),
            ["SET LOCAL statement_timeout = 1500".to_owned()]
        );
    }

    /// A database error carrying no code, `String` being diesel's own information type.
    fn codeless(kind: DatabaseErrorKind, message: &str) -> Error {
        Error::DatabaseError(kind, Box::new(message.to_owned()))
    }

    fn class_of(err: Error) -> ReadFailure {
        DieselAsyncError::Diesel(err).read_failure()
    }

    /// A pool that cannot hand out a connection is an outage, not an unknown.
    #[test]
    fn pool_exhaustion_is_transient() {
        let err = DieselAsyncError::Pool(diesel_async::pooled_connection::bb8::RunError::TimedOut);
        assert_eq!(err.read_failure(), ReadFailure::Transient);
    }

    /// `57014` is the timeout whatever the message says, and the message alone is nothing.
    #[test]
    fn the_statement_timeout_is_known_by_its_code_alone() {
        assert_eq!(
            class_of(coded_error(
                DatabaseErrorKind::Unknown,
                "57014",
                "query canceled"
            )),
            ReadFailure::Timeout
        );
        assert_eq!(
            class_of(codeless(
                DatabaseErrorKind::Unknown,
                "canceling statement due to statement timeout"
            )),
            ReadFailure::Other
        );
    }

    /// Decision 1's list, every code on it, class `08` sampled across its standard members.
    #[test]
    fn every_listed_code_is_transient() {
        for code in [
            "08000", "08001", "08003", "08004", "08006", "08007", "08P01", "40001", "40P01",
            "25006", "53100", "53200", "53300", "57P01", "57P02", "57P03", "57P05", "58000",
            "58030",
        ] {
            assert_eq!(
                class_of(coded_error(DatabaseErrorKind::Unknown, code, "listed")),
                ReadFailure::Transient,
                "for {code}"
            );
        }
    }

    /// Codes beside the list in its own classes never clear on retry, and a schema drift ends at once.
    #[test]
    fn every_other_code_is_deterministic() {
        for code in [
            "40002", "40003", "53400", "57000", "57P04", "58P01", "58P02", "42P01", "42703",
            "42501", "22012", "23505", "P0001",
        ] {
            assert_eq!(
                class_of(coded_error(DatabaseErrorKind::Unknown, code, "unlisted")),
                ReadFailure::Deterministic,
                "for {code}"
            );
        }
    }

    /// Where a code is present the kind decides nothing, in either direction.
    #[test]
    fn the_code_overrides_the_kind() {
        assert_eq!(
            class_of(coded_error(
                DatabaseErrorKind::ClosedConnection,
                "42601",
                "syntax"
            )),
            ReadFailure::Deterministic
        );
        assert_eq!(
            class_of(coded_error(
                DatabaseErrorKind::UniqueViolation,
                "40P01",
                "deadlock"
            )),
            ReadFailure::Transient
        );
    }

    /// Without a code the kind decides, and only the kinds an outage arrives as are transient.
    #[test]
    fn without_a_code_the_kind_decides() {
        for kind in [
            DatabaseErrorKind::SerializationFailure,
            DatabaseErrorKind::ReadOnlyTransaction,
            DatabaseErrorKind::ClosedConnection,
            DatabaseErrorKind::UnableToSendCommand,
        ] {
            assert_eq!(
                class_of(codeless(kind, "whatever the server said")),
                ReadFailure::Transient,
                "for kind {kind:?}"
            );
        }
        for kind in [
            DatabaseErrorKind::Unknown,
            DatabaseErrorKind::UniqueViolation,
        ] {
            assert_eq!(
                class_of(codeless(kind, "relation \"v\" does not exist")),
                ReadFailure::Other,
                "for kind {kind:?}"
            );
        }
    }

    /// Errors that never reached the database carry no code and keep the one retry.
    #[test]
    fn errors_outside_the_database_are_other() {
        let builder = Error::QueryBuilderError(Box::new(std::io::Error::other("nope")));
        assert_eq!(class_of(builder), ReadFailure::Other);
        assert_eq!(
            DieselAsyncError::RowsUnsupported.read_failure(),
            ReadFailure::Other
        );
        let err = std::io::Error::other("no re-execution connector configured");
        assert_eq!(err.read_failure(), ReadFailure::Other);
        assert_eq!("boom".to_owned().read_failure(), ReadFailure::Other);
    }

    /// The snapshot path keeps its own timeout marker and classifies with it.
    #[test]
    fn the_snapshot_timeout_keeps_the_timeout_class() {
        let timed_out = crate::snapshot::SnapshotError::TimedOut(Duration::from_millis(1));
        assert_eq!(timed_out.read_failure(), ReadFailure::Timeout);
    }
}
