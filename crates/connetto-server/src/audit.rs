//! The structural contract for `auth_events`, the durable record of changes to
//! who can reach what.
//!
//! connetto owns no schema. A deployment declares the table (see
//! `docs/architecture/08-authorization.md`) and implements [`ConnettoAuditSchema`]
//! for it, by hand or through [`connetto_schema!`](crate::connetto_schema), and
//! names it as the `Audit` member of its
//! [`ConnettoSchema`](crate::schema::ConnettoSchema). The same arrangement as [`ConnettoStoreSchema`](crate::authn::schema::ConnettoStoreSchema)
//! and [`ConnettoWriterSchema`](crate::writer_schema::ConnettoWriterSchema).
//!
//! **This table holds state changes, never denials.** A caller probing keys
//! generates one refusal per attempt, so refusals go to structured logging and
//! would drown this table at exactly the moment it matters. What lands here is
//! the durable: a login began or ended, a share key was issued, a ban was
//! imposed, a person's data was downloaded or erased. The split is normative
//! in `08-authorization.md` and `denials_never_reach_the_audit_table` pins it.
//!
//! Typed reads return minted keys and courier receipt sequences.

use crate::writer_schema::SettlePath;
use connetto_core::SessionId;
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel_async::AsyncPgConnection;
use diesel_async::methods::ExecuteDsl;
use diesel_async::pooled_connection::bb8::Pool;
use std::io::Write as _;
use subql::backend::{Postgres, Value};

/// The Postgres enum type backing the `op` column.
///
/// A deployment creates it beside the table. Naming it here is what lets the
/// column bind as its own type rather than as text.
pub const AUTH_OP_TYPE: &str = "connetto_auth_op";

/// The Postgres enum type marker for [`AuthOp`].
#[derive(diesel::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "connetto_auth_op"))]
pub struct AuthOpSql;

/// What changed about who can reach what.
///
/// **A login ending is three values rather than one.** Collapsed, the table
/// cannot tell an ordinary logout from a stolen credential, which is the most
/// valuable thing it reports, and the cause is known at the moment the row is
/// written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = AuthOpSql)]
pub enum AuthOp {
    /// A sign-in created a session, the row naming the provider and issuer it
    /// came through.
    LoggedIn,
    /// The caller ended their own login through the logout endpoint.
    LoggedOut,
    /// The embedding application revoked the login itself.
    SessionRevoked,
    /// The person ended one of their sessions from their list of signed-in
    /// places.
    EndedByOwner,
    /// The person ended every one of their sessions.
    EndedAll,
    /// The person ended every one of their sessions but the current one.
    EndedOthers,
    /// A ban ended the session, one row per session of the banned identity
    /// beside the ban's own row.
    EndedByBan,
    /// The theft defence saw a rotated-out refresh token presented and killed
    /// the login.
    TokenReplayed,
    /// A share key was issued over one row.
    CapabilityMinted,
    /// A permission changed. Produced by the grant-change watcher (R7).
    PermissionChange,
    /// The authorization model changed. Produced by R5b.
    ModelChange,
    /// An identity was banned, on crossing an abuse threshold.
    Banned,
    /// A ban was lifted through [`BanStore::lift`](crate::ban::BanStore::lift),
    /// which is the only thing that produces this. An expiry that merely lapses
    /// leaves its row behind and no record.
    BanLifted,
    /// A different entry arrived at an already-settled `(writer, sequence)`,
    /// which stands; the second was refused `Forked`. State, because a fork
    /// decides which history stands.
    WriteForked,
    /// An already-settled entry arrived again through another path and was
    /// answered from the settled record. State, because the cross-path
    /// delivery is the trace the courier needs.
    WriteRedelivered,
    /// A couriered entry settled, applied or refused alike, since both fix
    /// the sequence. The row names the author in `user_id` and the courier's
    /// account in `courier_user_id` (R78 decision 10).
    WriteCouriered,
    /// A person's data left the server as a portability download (R61). The
    /// row names the query-list hash and the archive size.
    DataDownloaded,
    /// The application finished erasing a person's data and recorded it
    /// (R61). connetto deletes nothing itself, so this records the
    /// application's act.
    DataErased,
    /// A device of another account uploaded a file for its owner and the
    /// commit is durable (R79 decision 17). The row names the owner in
    /// `user_id`, the carrier's account in `courier_user_id` and the file id
    /// in `subject`, under the carrier's minting session.
    ContentCarried,
}

impl AuthOp {
    /// The label this value carries in Postgres, and the one the enum type
    /// declares.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::LoggedIn => "logged_in",
            Self::LoggedOut => "logged_out",
            Self::SessionRevoked => "session_revoked",
            Self::EndedByOwner => "ended_by_owner",
            Self::EndedAll => "ended_all",
            Self::EndedOthers => "ended_others",
            Self::EndedByBan => "ended_by_ban",
            Self::TokenReplayed => "token_replayed",
            Self::CapabilityMinted => "capability_minted",
            Self::PermissionChange => "permission_change",
            Self::ModelChange => "model_change",
            Self::Banned => "banned",
            Self::BanLifted => "ban_lifted",
            Self::WriteForked => "write_forked",
            Self::WriteRedelivered => "write_redelivered",
            Self::WriteCouriered => "write_couriered",
            Self::DataDownloaded => "data_downloaded",
            Self::DataErased => "data_erased",
            Self::ContentCarried => "content_carried",
        }
    }
}

impl ToSql<AuthOpSql, Pg> for AuthOp {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(self.label().as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<AuthOpSql, Pg> for AuthOp {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        match bytes.as_bytes() {
            b"logged_in" => Ok(Self::LoggedIn),
            b"logged_out" => Ok(Self::LoggedOut),
            b"session_revoked" => Ok(Self::SessionRevoked),
            b"ended_by_owner" => Ok(Self::EndedByOwner),
            b"ended_all" => Ok(Self::EndedAll),
            b"ended_others" => Ok(Self::EndedOthers),
            b"ended_by_ban" => Ok(Self::EndedByBan),
            b"token_replayed" => Ok(Self::TokenReplayed),
            b"capability_minted" => Ok(Self::CapabilityMinted),
            b"permission_change" => Ok(Self::PermissionChange),
            b"model_change" => Ok(Self::ModelChange),
            b"banned" => Ok(Self::Banned),
            b"ban_lifted" => Ok(Self::BanLifted),
            b"write_forked" => Ok(Self::WriteForked),
            b"write_redelivered" => Ok(Self::WriteRedelivered),
            b"write_couriered" => Ok(Self::WriteCouriered),
            b"data_downloaded" => Ok(Self::DataDownloaded),
            b"data_erased" => Ok(Self::DataErased),
            b"content_carried" => Ok(Self::ContentCarried),
            other => Err(format!(
                "unrecognised connetto_auth_op label {:?}",
                String::from_utf8_lossy(other)
            )
            .into()),
        }
    }
}

/// One thing that changed about who can reach what.
///
/// `at` is not carried: the column defaults to `now()`, so the database clock
/// decides rather than whichever process happened to emit.
#[derive(Debug, Clone)]
pub struct AuthEvent<Id> {
    /// The session the change concerns. Absent only on a data request staff
    /// made with no session behind it, where [`acted_by`](Self::acted_by)
    /// names who acted instead.
    pub session: Option<SessionId>,
    /// Who it concerns, absent when the caller has no identity.
    pub user_id: Option<Id>,
    /// What changed.
    pub op: AuthOp,
    /// The table a share key names, absent for everything else.
    pub table_name: Option<String>,
    /// The row a share key names, as the typed key values connetto observed on
    /// it, absent for everything else.
    ///
    /// **Not encoded here, deliberately.** connetto does not know what type the
    /// application stores a row key as, and it is not connetto's to decide: it
    /// is almost always a distributed id such as a UUID. So the values travel
    /// as they were read and
    /// [`ConnettoAuditSchema::row_key`] maps them to the column.
    pub pk: Option<Vec<Value<Postgres>>>,
    /// The server-issued writer named by a write-state event.
    pub writer: Option<i64>,
    /// The writer sequence named by a write-state event.
    pub seq: Option<i64>,
    /// The standing entry's settlement path.
    pub path: Option<SettlePath>,
    /// The arriving write's path.
    pub other_path: Option<SettlePath>,
    /// The courier's account, on a couriered write the author's identity came
    /// through, absent for every other event (R78 decision 10).
    pub courier_user_id: Option<Id>,
    /// The configured provider a sign-in came through, absent for everything
    /// else.
    pub provider: Option<String>,
    /// The issuer a sign-in's identity was verified from, absent for
    /// everything else.
    pub issuer: Option<String>,
    /// The key a share mint issued, as its grant row names it, absent for
    /// everything else.
    pub subject: Option<String>,
    /// The hash of the registered query list a download ran, absent for
    /// everything else.
    pub query_list_hash: Option<String>,
    /// The size in bytes of the archive a download produced, absent for
    /// everything else.
    pub archive_bytes: Option<i64>,
    /// Who acted on a data request made with no session, as the application
    /// names them, absent for everything else.
    pub acted_by: Option<String>,
}

impl<Id> AuthEvent<Id> {
    /// An event naming no row, which is every kind except a share mint.
    #[must_use]
    pub const fn new(session: SessionId, user_id: Option<Id>, op: AuthOp) -> Self {
        Self {
            session: Some(session),
            user_id,
            op,
            table_name: None,
            pk: None,
            writer: None,
            seq: None,
            path: None,
            other_path: None,
            courier_user_id: None,
            provider: None,
            issuer: None,
            subject: None,
            query_list_hash: None,
            archive_bytes: None,
            acted_by: None,
        }
    }

    /// An event staff caused through a data request with no session behind
    /// it, naming who acted as the application names them.
    #[must_use]
    pub fn by_operator(user_id: Option<Id>, op: AuthOp, acted_by: impl Into<String>) -> Self {
        Self {
            session: None,
            user_id,
            op,
            table_name: None,
            pk: None,
            writer: None,
            seq: None,
            path: None,
            other_path: None,
            courier_user_id: None,
            provider: None,
            issuer: None,
            subject: None,
            query_list_hash: None,
            archive_bytes: None,
            acted_by: Some(acted_by.into()),
        }
    }

    /// Name the query-list hash and the archive size a download row carries.
    #[must_use]
    pub fn about_download(
        mut self,
        query_list_hash: impl Into<String>,
        archive_bytes: i64,
    ) -> Self {
        self.query_list_hash = Some(query_list_hash.into());
        self.archive_bytes = Some(archive_bytes);
        self
    }

    /// Name the row this event concerns.
    #[must_use]
    pub fn about_row(mut self, table_name: impl Into<String>, pk: Vec<Value<Postgres>>) -> Self {
        self.table_name = Some(table_name.into());
        self.pk = Some(pk);
        self
    }

    /// Name the writer, its sequence and the two settle paths this event
    /// concerns, the shape the two write-state changes carry.
    #[must_use]
    pub fn about_write(
        mut self,
        writer: i64,
        seq: i64,
        path: SettlePath,
        other_path: SettlePath,
    ) -> Self {
        self.writer = Some(writer);
        self.seq = Some(seq);
        self.path = Some(path);
        self.other_path = Some(other_path);
        self
    }

    /// Name the provider and issuer a sign-in came through, the shape a login
    /// row carries.
    #[must_use]
    pub fn about_login(mut self, provider: impl Into<String>, issuer: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self.issuer = Some(issuer.into());
        self
    }

    /// Name the courier's account a couriered write came through, the shape a
    /// `write_couriered` row carries, or the carrier's account a
    /// `content_carried` row names.
    #[must_use]
    pub fn about_courier(mut self, courier_user_id: Id) -> Self {
        self.courier_user_id = Some(courier_user_id);
        self
    }

    /// Name the file a `content_carried` row concerns, as its id in lowercase
    /// hex.
    #[must_use]
    pub fn about_file(mut self, file_id: &[u8; 32]) -> Self {
        use core::fmt::Write as _;
        let mut hex = String::with_capacity(64);
        for byte in file_id {
            let _ = write!(hex, "{byte:02x}");
        }
        self.subject = Some(hex);
        self
    }

    /// Name the key a share mint issued, as the grant row the application
    /// writes names it.
    #[must_use]
    pub fn about_key(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }
}

/// One share key a session minted, read back from its `capability_minted`
/// row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintedKey<RowKey> {
    /// The session that minted it.
    pub session: SessionId,
    /// The key, as the application's grant row names it.
    pub subject: String,
    /// The table of the shared row.
    pub table: String,
    /// The shared row's key, absent when the table stores no key for its
    /// shape.
    pub row: Option<RowKey>,
}

/// The deployment's audit catalog, insertion and authorized reads.
pub trait ConnettoAuditSchema: Send + Sync + 'static {
    /// The deployment's typed distributed user id, the same one
    /// [`ConnettoStoreSchema`](crate::authn::schema::ConnettoStoreSchema)
    /// carries.
    type Id: Clone + core::fmt::Display + Send + Sync + 'static;

    /// The tables this member reads and writes, by name, which startup
    /// requires to exist (R98 decision 5).
    const TABLES: &'static [&'static str];

    /// The audit enum and table declarations used by the server catalog.
    const CATALOG_DDL: &'static str;

    /// Selects `writer, seq` for `write_couriered` at writer `$1` and sequence `$2`.
    const COURIER_RECEIPT_FILTER_SQL: &'static str;

    /// The type this table stores a shared row's key as.
    ///
    /// The application's choice, like `Id` beside it, because connetto has no
    /// business deciding it: in practice it is a distributed id such as a UUID.
    /// An earlier version of this contract stored the key as `BYTEA` holding a
    /// `MessagePack` encoding of the values, which no policy could join against
    /// and no person could read, in a table whose neighbouring column is text
    /// precisely so a person can read it.
    type RowKey: core::fmt::Debug + Clone + Send + Sync + 'static;

    /// The whole `INSERT INTO auth_events ...` statement, laundered as one
    /// executable value, because the diesel trait solver cannot name an insert
    /// statement type generically.
    type Insert: ExecuteDsl<AsyncPgConnection> + Send;

    /// Map the key values connetto read off the row to the stored type.
    ///
    /// `None` when the shape does not fit, which writes no key rather than a
    /// wrong one. connetto reads a row's key as a list of typed values and
    /// cannot know which of them the application keys on, so this is where that
    /// knowledge lives.
    fn row_key(values: &[Value<Postgres>]) -> Option<Self::RowKey>;

    /// Build the insert for one event.
    fn audit_insert(event: AuthEvent<Self::Id>) -> Self::Insert;

    /// The keys `sessions` minted, from their `capability_minted` rows.
    fn minted_by(
        conn: &mut AsyncPgConnection,
        sessions: Vec<SessionId>,
    ) -> crate::authn::schema::Statement<'_, Vec<MintedKey<Self::RowKey>>>;

    /// Distinct requested sequences couriered by or authored by `viewer`.
    fn courier_receipts<'c>(
        conn: &'c mut AsyncPgConnection,
        viewer: &'c Self::Id,
        writer: i64,
        sequences: &'c [i64],
    ) -> crate::authn::schema::Statement<'c, Vec<i64>>;
}

/// Records one change to who can reach what.
///
/// Fired synchronously by the producer, so an implementation that writes to a
/// database must move the write onto a spawned task rather than blocking the
/// caller. `08-authorization.md` requires audit writing to stay off the
/// synchronous hot path, and [`pg_audit_hook`] is the ready-made implementation
/// that obeys it. Same shape and same wiring point as
/// [`SessionRevocationHook`](crate::authn::service::SessionRevocationHook),
/// which is why it is a callback rather than the schema trait itself: the trait
/// carries an associated statement type and so cannot be a trait object.
pub type AuditHook<Id> = std::sync::Arc<dyn Fn(AuthEvent<Id>) + Send + Sync>;

/// An [`AuditHook`] that appends through the deployment's audit table, `D`'s
/// [`Audit`] member, on the given pool.
///
/// The hook only queues the event, so the producer is never delayed, and one
/// writer task drains the queue, so the rows land in the order the hook
/// received them and the database clock stamps them in that order. Every
/// producer sharing the hook shares that order. The writer waits for a
/// connection rather than dropping a row it could not get one for, since a
/// later row would otherwise land ahead of it. An insert that fails is logged
/// and dropped rather than propagated: losing an audit row must not fail the
/// logout, revocation or mint that produced it, nor stall the rows behind it.
///
/// # Panics
///
/// When called outside a Tokio runtime, which the writer task runs on.
///
/// [`Audit`]: crate::schema::ConnettoSchema::Audit
pub fn pg_audit_hook<D>(pool: Pool<AsyncPgConnection>) -> AuditHook<D::Id>
where
    D: crate::schema::ConnettoSchema,
{
    let (queue, events) = tokio::sync::mpsc::unbounded_channel();
    // The writer's warnings carry the span the hook was made under, as every
    // task the server spawns for a caller does.
    tokio::spawn(tracing::Instrument::instrument(
        write_in_order::<D>(pool, events),
        tracing::Span::current(),
    ));
    std::sync::Arc::new(move |event: AuthEvent<D::Id>| {
        let op = event.op;
        if queue.send(event).is_err() {
            tracing::warn!(op = op.label(), "audit row dropped, the writer has stopped");
        }
    })
}

/// How long the writer waits before asking the pool again after it could not
/// hand out a connection.
const CONNECTION_RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// Drain `events` into the audit table one row at a time, in queue order,
/// until every hook feeding the queue is gone.
async fn write_in_order<D>(
    pool: Pool<AsyncPgConnection>,
    mut events: tokio::sync::mpsc::UnboundedReceiver<AuthEvent<D::Id>>,
) where
    D: crate::schema::ConnettoSchema,
{
    while let Some(event) = events.recv().await {
        let op = event.op;
        let mut conn = loop {
            match pool.get().await {
                Ok(conn) => break conn,
                Err(error) => {
                    tracing::warn!(%error, op = op.label(), "audit row waiting for a connection");
                    tokio::time::sleep(CONNECTION_RETRY).await;
                }
            }
        };
        if let Err(error) = ExecuteDsl::execute(D::Audit::audit_insert(event), &mut conn).await {
            tracing::warn!(%error, op = op.label(), "audit row dropped, insert failed");
        }
    }
}

/// Append one event through the deployment's audit table and wait for it.
///
/// For the data requests of R61, whose row exists for disputes: unlike
/// [`pg_audit_hook`], a failure is returned rather than logged, so the request
/// that produced the event fails with it.
///
/// # Errors
///
/// [`AuditWriteError`] when no connection can be had or the insert fails.
pub async fn record_awaited<D>(
    pool: &Pool<AsyncPgConnection>,
    event: AuthEvent<D::Id>,
) -> Result<(), AuditWriteError>
where
    D: crate::schema::ConnettoSchema,
{
    let mut conn = pool.get().await.map_err(AuditWriteError::Connection)?;
    ExecuteDsl::execute(D::Audit::audit_insert(event), &mut conn)
        .await
        .map(drop)
        .map_err(AuditWriteError::Insert)
}

/// An awaited audit row could not be written.
#[derive(Debug, thiserror::Error)]
pub enum AuditWriteError {
    /// No connection could be had from the audit pool.
    #[error("no connection for the audit row")]
    Connection(#[source] diesel_async::pooled_connection::bb8::RunError),
    /// The insert failed.
    #[error("the audit row could not be inserted")]
    Insert(#[source] diesel::result::Error),
}

/// Returns reference audit declarations or idempotent provisioning SQL.
#[macro_export]
macro_rules! connetto_audit_ddl {
    ($id:literal, $pk:literal) => {
        $crate::connetto_audit_ddl!(@ddl $id, $pk, "", "", "", "")
    };
    (provision, $id:literal, $pk:literal) => {
        $crate::connetto_audit_ddl!(
            @ddl $id, $pk,
            "DO $$ BEGIN ",
            " EXCEPTION WHEN duplicate_object THEN NULL; END $$;",
            "IF NOT EXISTS ",
            // A type made before an operation joined the set gains it here.
            " ALTER TYPE connetto_auth_op ADD VALUE IF NOT EXISTS 'content_carried';"
        )
    };
    (@ddl $id:literal, $pk:literal, $before:literal, $after:literal, $guard:literal, $upgrade:literal) => {
        concat!(
            $before,
            "CREATE TYPE connetto_auth_op AS ENUM (",
            "'logged_in', 'logged_out', 'session_revoked', 'token_replayed', ",
            "'ended_by_owner', 'ended_all', 'ended_others', 'ended_by_ban', ",
            "'capability_minted', 'permission_change', 'model_change', ",
            "'banned', 'ban_lifted', 'write_forked', 'write_redelivered', ",
            "'write_couriered', 'data_downloaded', 'data_erased', 'content_carried');",
            $after,
            $upgrade,
            " CREATE TABLE ", $guard, "auth_events (",
            "at TIMESTAMPTZ NOT NULL DEFAULT now(), ",
            "session UUID, ",
            "user_id ", $id, ", ",
            "op connetto_auth_op NOT NULL, ",
            "table_name TEXT, ",
            "pk ", $pk, ", ",
            "subject TEXT, ",
            "writer BIGINT, ",
            "seq BIGINT, ",
            "path connetto_settle_path, ",
            "other_path connetto_settle_path, ",
            "courier_user_id ", $id, ", ",
            "provider TEXT, ",
            "issuer TEXT, ",
            "query_list_hash TEXT, ",
            "archive_bytes BIGINT, ",
            "acted_by TEXT);"
        )
    };
}

/// Generate the default `auth_events` table and its [`ConnettoAuditSchema`]
/// impl, parameterized by the deployment's `Id` type and the type it stores a
/// shared row's key as, each with the diesel SQL type it maps to.
///
/// A convenience default only: the table is the deployment's to define however
/// it likes, and [`ConnettoAuditSchema`] is the real contract, implementable by
/// hand against any table. The reference SQL, including the
/// `connetto_auth_op` enum this depends on, is in
/// `docs/architecture/08-authorization.md`.
///
/// The default `row_key` accepts a single-column UUID key, which is what a
/// distributed id almost always is, and writes no key for anything else rather
/// than a wrong one. A composite or non-UUID key implements the trait by hand.
///
/// Invoked at module scope, it emits the `auth_events` `diesel::table!` module,
/// an insertable row struct, and a unit struct `ConnettoAudit` implementing the
/// trait. The caller needs `diesel` in scope.
///
/// ```
/// connetto_server::connetto_audit_table!(
///     String, diesel::sql_types::Text,
///     uuid::Uuid, diesel::sql_types::Uuid,
///     connetto_server::connetto_audit_ddl!("TEXT", "UUID"),
/// );
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! connetto_audit_table {
    ($id:ty, $id_sql:ty, $pk:ty, $pk_sql:ty, $ddl:expr $(,)?) => {
        diesel::table! {
            /// The durable record of changes to who can reach what. Holds state
            /// changes only: denials go to structured logging.
            auth_events (at, op) {
                /// When it happened, defaulted by the column so one clock
                /// decides rather than whichever process emitted.
                at -> diesel::sql_types::Timestamptz,
                /// The session the change concerns, absent only on a data
                /// request staff made with no session behind it.
                session -> diesel::sql_types::Nullable<diesel::sql_types::Uuid>,
                /// Who it concerns, absent when the caller has no identity.
                user_id -> diesel::sql_types::Nullable<$id_sql>,
                /// What changed, as a closed set both ends agree on.
                op -> $crate::audit::AuthOpSql,
                /// The table a share key names, absent for everything else.
                table_name -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
                /// The row a share key names, absent for everything else.
                pk -> diesel::sql_types::Nullable<$pk_sql>,
                /// The key a share mint issued.
                subject -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
                /// The server-issued writer named by a write-state event.
                writer -> diesel::sql_types::Nullable<diesel::sql_types::BigInt>,
                /// The writer sequence named by a write-state event.
                seq -> diesel::sql_types::Nullable<diesel::sql_types::BigInt>,
                /// The standing entry's settlement path.
                path -> diesel::sql_types::Nullable<$crate::writer_schema::SettlePathSql>,
                /// The arriving write's path.
                other_path -> diesel::sql_types::Nullable<$crate::writer_schema::SettlePathSql>,
                /// The courier's account, on a couriered write the author's
                /// identity came through, absent for every other event.
                courier_user_id -> diesel::sql_types::Nullable<$id_sql>,
                /// The configured provider a sign-in came through.
                provider -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
                /// The issuer a sign-in's identity was verified from.
                issuer -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
                /// The hash of the query list a download ran.
                query_list_hash -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
                /// The size in bytes of the archive a download produced.
                archive_bytes -> diesel::sql_types::Nullable<diesel::sql_types::BigInt>,
                /// Who acted on a data request made with no session.
                acted_by -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
            }
        }

        /// Insertable audit row for [`ConnettoAudit`]. `at` is omitted so the
        /// column default decides, keeping one clock.
        #[derive(diesel::Insertable)]
        #[diesel(table_name = auth_events)]
        pub struct ConnettoNewAuthEvent {
            session: Option<$crate::SessionId>,
            user_id: Option<$id>,
            op: $crate::audit::AuthOp,
            table_name: Option<String>,
            pk: Option<$pk>,
            writer: Option<i64>,
            seq: Option<i64>,
            path: Option<$crate::writer_schema::SettlePath>,
            other_path: Option<$crate::writer_schema::SettlePath>,
            courier_user_id: Option<$id>,
            provider: Option<String>,
            issuer: Option<String>,
            subject: Option<String>,
            query_list_hash: Option<String>,
            archive_bytes: Option<i64>,
            acted_by: Option<String>,
        }

        /// The default connetto audit schema over the deployment's `Id` type.
        #[derive(Debug, Clone, Copy, Default)]
        pub struct ConnettoAudit;

        impl $crate::audit::ConnettoAuditSchema for ConnettoAudit {
            type Id = $id;
            const TABLES: &'static [&'static str] = &["auth_events"];
            const CATALOG_DDL: &'static str = $ddl;
            const COURIER_RECEIPT_FILTER_SQL: &'static str = "SELECT writer, seq FROM auth_events \
                 WHERE op = 'write_couriered' AND writer = $1 AND seq = $2";
            type RowKey = $pk;
            type Insert = diesel::query_builder::InsertStatement<
                auth_events::table,
                <ConnettoNewAuthEvent as diesel::Insertable<auth_events::table>>::Values,
            >;

            fn row_key(
                values: &[subql::backend::Value<subql::backend::Postgres>],
            ) -> Option<Self::RowKey> {
                match values {
                    [subql::backend::Value::Uuid(id)] => Some(<$pk>::from(*id)),
                    _ => None,
                }
            }

            fn audit_insert(event: $crate::audit::AuthEvent<Self::Id>) -> Self::Insert {
                let pk = event.pk.as_deref().and_then(Self::row_key);
                diesel::insert_into(auth_events::table).values(ConnettoNewAuthEvent {
                    session: event.session,
                    user_id: event.user_id,
                    op: event.op,
                    table_name: event.table_name,
                    pk,
                    writer: event.writer,
                    seq: event.seq,
                    path: event.path,
                    other_path: event.other_path,
                    courier_user_id: event.courier_user_id,
                    provider: event.provider,
                    issuer: event.issuer,
                    subject: event.subject,
                    query_list_hash: event.query_list_hash,
                    archive_bytes: event.archive_bytes,
                    acted_by: event.acted_by,
                })
            }

            fn minted_by(
                conn: &mut diesel_async::AsyncPgConnection,
                sessions: Vec<$crate::SessionId>,
            ) -> $crate::authn::schema::Statement<'_, Vec<$crate::audit::MintedKey<Self::RowKey>>>
            {
                Box::pin(async move {
                    use diesel::BoolExpressionMethods as _;
                    use diesel::ExpressionMethods as _;
                    let rows: Vec<(
                        Option<$crate::SessionId>,
                        Option<String>,
                        Option<String>,
                        Option<$pk>,
                    )> = diesel_async::RunQueryDsl::load(
                        diesel::QueryDsl::select(
                            diesel::QueryDsl::filter(
                                auth_events::table,
                                auth_events::session.eq_any(sessions).and(
                                    auth_events::op.eq($crate::audit::AuthOp::CapabilityMinted),
                                ),
                            ),
                            (
                                auth_events::session,
                                auth_events::subject,
                                auth_events::table_name,
                                auth_events::pk,
                            ),
                        ),
                        conn,
                    )
                    .await?;
                    Ok(rows
                        .into_iter()
                        .filter_map(|(session, subject, table, row)| {
                            Some($crate::audit::MintedKey {
                                session: session?,
                                subject: subject?,
                                table: table?,
                                row,
                            })
                        })
                        .collect())
                })
            }

            fn courier_receipts<'c>(
                conn: &'c mut diesel_async::AsyncPgConnection,
                viewer: &'c Self::Id,
                writer: i64,
                sequences: &'c [i64],
            ) -> $crate::authn::schema::Statement<'c, Vec<i64>> {
                Box::pin(async move {
                    use diesel::BoolExpressionMethods as _;
                    use diesel::ExpressionMethods as _;
                    use diesel::NullableExpressionMethods as _;
                    diesel_async::RunQueryDsl::load(
                        diesel::QueryDsl::distinct(diesel::QueryDsl::select(
                            diesel::QueryDsl::filter(
                                auth_events::table,
                                auth_events::op
                                    .eq($crate::audit::AuthOp::WriteCouriered)
                                    .and(auth_events::writer.eq(writer))
                                    .and(auth_events::seq.eq_any(sequences))
                                    .and(
                                        auth_events::user_id
                                            .eq(viewer)
                                            .or(auth_events::courier_user_id.eq(viewer)),
                                    ),
                            ),
                            auth_events::seq.assume_not_null(),
                        )),
                        conn,
                    )
                    .await
                })
            }
        }
    };
}
