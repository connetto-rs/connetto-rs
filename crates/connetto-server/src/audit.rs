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
//! imposed. The split is normative in `08-authorization.md` and
//! `denials_never_reach_the_audit_table` pins it.
//!
//! The insert is laundered as an associated type, and one read is a
//! statement function: the keys a set of sessions minted, which ending those
//! sessions hands back so the application can withdraw them (R105 decision
//! 5). Every other row is written once and never read back by connetto.

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
            Self::TokenReplayed => "token_replayed",
            Self::CapabilityMinted => "capability_minted",
            Self::PermissionChange => "permission_change",
            Self::ModelChange => "model_change",
            Self::Banned => "banned",
            Self::BanLifted => "ban_lifted",
            Self::WriteForked => "write_forked",
            Self::WriteRedelivered => "write_redelivered",
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
            b"token_replayed" => Ok(Self::TokenReplayed),
            b"capability_minted" => Ok(Self::CapabilityMinted),
            b"permission_change" => Ok(Self::PermissionChange),
            b"model_change" => Ok(Self::ModelChange),
            b"banned" => Ok(Self::Banned),
            b"ban_lifted" => Ok(Self::BanLifted),
            b"write_forked" => Ok(Self::WriteForked),
            b"write_redelivered" => Ok(Self::WriteRedelivered),
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
    /// The session the change concerns. Always present, because every run has a
    /// handle whether or not anyone is logged in.
    pub session: SessionId,
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
    /// The configured provider a sign-in came through, absent for everything
    /// else.
    pub provider: Option<String>,
    /// The issuer a sign-in's identity was verified from, absent for
    /// everything else.
    pub issuer: Option<String>,
    /// The key a share mint issued, as its grant row names it, absent for
    /// everything else.
    pub subject: Option<String>,
}

impl<Id> AuthEvent<Id> {
    /// An event naming no row, which is every kind except a share mint.
    #[must_use]
    pub const fn new(session: SessionId, user_id: Option<Id>, op: AuthOp) -> Self {
        Self {
            session,
            user_id,
            op,
            table_name: None,
            pk: None,
            writer: None,
            seq: None,
            path: None,
            other_path: None,
            provider: None,
            issuer: None,
            subject: None,
        }
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

/// The `auth_events` table, the pre-built insert connetto appends through,
/// and the one read connetto makes, the keys a set of sessions minted.
///
/// connetto reads nothing else back, so the application is free to index
/// and query it however it likes.
pub trait ConnettoAuditSchema: Send + Sync + 'static {
    /// The deployment's typed distributed user id, the same one
    /// [`ConnettoStoreSchema`](crate::authn::schema::ConnettoStoreSchema)
    /// carries.
    type Id: Clone + core::fmt::Display + Send + Sync + 'static;

    /// The tables this member reads and writes, by name, which startup
    /// requires to exist (R98 decision 5).
    const TABLES: &'static [&'static str];

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
/// The write is spawned, so the producer is never delayed by it, and a failure
/// is logged rather than propagated: losing an audit row must not fail the
/// logout, revocation or mint that produced it.
///
/// [`Audit`]: crate::schema::ConnettoSchema::Audit
pub fn pg_audit_hook<D>(pool: Pool<AsyncPgConnection>) -> AuditHook<D::Id>
where
    D: crate::schema::ConnettoSchema,
{
    std::sync::Arc::new(move |event: AuthEvent<D::Id>| {
        let pool = pool.clone();
        let op = event.op;
        tokio::spawn(async move {
            let mut conn = match pool.get().await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::warn!(%error, op = op.label(), "audit row dropped, no connection");
                    return;
                }
            };
            if let Err(error) = ExecuteDsl::execute(D::Audit::audit_insert(event), &mut conn).await
            {
                tracing::warn!(%error, op = op.label(), "audit row dropped, insert failed");
            }
        });
    })
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
/// ```ignore
/// connetto_server::connetto_audit_table!(
///     String, diesel::sql_types::Text,
///     uuid::Uuid, diesel::sql_types::Uuid,
/// );
/// // now `ConnettoAudit` implements `ConnettoAuditSchema`.
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! connetto_audit_table {
    ($id:ty, $id_sql:ty, $pk:ty, $pk_sql:ty $(,)?) => {
        diesel::table! {
            /// The durable record of changes to who can reach what. Holds state
            /// changes only: denials go to structured logging.
            auth_events (at, session) {
                /// When it happened, defaulted by the column so one clock
                /// decides rather than whichever process emitted.
                at -> diesel::sql_types::Timestamptz,
                /// The session the change concerns, always present because every
                /// run has a handle whether or not anyone is logged in.
                session -> diesel::sql_types::Uuid,
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
                /// The configured provider a sign-in came through.
                provider -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
                /// The issuer a sign-in's identity was verified from.
                issuer -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
            }
        }

        /// Insertable audit row for [`ConnettoAudit`]. `at` is omitted so the
        /// column default decides, keeping one clock.
        #[derive(diesel::Insertable)]
        #[diesel(table_name = auth_events)]
        pub struct ConnettoNewAuthEvent {
            session: $crate::SessionId,
            user_id: Option<$id>,
            op: $crate::audit::AuthOp,
            table_name: Option<String>,
            pk: Option<$pk>,
            writer: Option<i64>,
            seq: Option<i64>,
            path: Option<$crate::writer_schema::SettlePath>,
            other_path: Option<$crate::writer_schema::SettlePath>,
            provider: Option<String>,
            issuer: Option<String>,
            subject: Option<String>,
        }

        /// The default connetto audit schema over the deployment's `Id` type.
        #[derive(Debug, Clone, Copy, Default)]
        pub struct ConnettoAudit;

        impl $crate::audit::ConnettoAuditSchema for ConnettoAudit {
            type Id = $id;
            const TABLES: &'static [&'static str] = &["auth_events"];
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
                    provider: event.provider,
                    issuer: event.issuer,
                    subject: event.subject,
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
                        $crate::SessionId,
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
                                session,
                                subject: subject?,
                                table: table?,
                                row,
                            })
                        })
                        .collect())
                })
            }
        }
    };
}
