//! The structural contract the Postgres auth store writes its queries against.
//!
//! connetto owns no schema. A deployment declares its own `sessions` and
//! `provider_tokens` tables (see `docs/architecture/11-authentication.md`) and
//! implements [`ConnettoStoreSchema`] for them, either by hand or through
//! [`connetto_schema!`](crate::connetto_schema), and names it as the `Auth`
//! member of its [`ConnettoSchema`](crate::schema::ConnettoSchema).
//! [`DbAuthStore`](super::store::DbAuthStore) is generic over this trait and
//! keeps every decision (identity resolution, refresh-token rotation,
//! reuse-is-theft, deadline capping); the trait runs one statement per
//! function on the connection it is handed, inside the transaction the store
//! opened.
//!
//! A deadline the tables hold is written as `now() + interval` and compared
//! against Postgres `now()` in the statement that reads it. The process clock
//! never appears on either side of such a comparison (chapter 21). The
//! deadline values are SQL expressions from [`crate::db_clock`], which the
//! statement builders run and return rather than build.

use core::pin::Pin;
use std::time::{Duration, SystemTime};

use connetto_core::SessionId;
use diesel::QueryResult;
use diesel_async::AsyncPgConnection;

use crate::authn::provider::StoredProviderToken;

/// The zone-aware instant a `Timestamptz` column carries.
///
/// Named once so the trait, the macro and the store cannot disagree about it.
pub type Instant = chrono::DateTime<chrono::Utc>;

/// One statement of a [`ConnettoStoreSchema`], run on a borrowed connection.
pub type Statement<'c, T> = Pin<Box<dyn Future<Output = QueryResult<T>> + Send + 'c>>;

/// The rotation columns a locked session read returns, with the database's
/// verdict on the session's two deadlines.
#[derive(Debug, Clone)]
pub struct SessionRow<Id> {
    /// The typed user id the session carries.
    pub user_id: Id,
    /// The refresh-secret hash currently valid for the session.
    pub current_refresh_hash: Vec<u8>,
    /// Whether the session was ended before either deadline.
    pub revoked: bool,
    /// Whether the database clock is past the idle deadline.
    pub idle_lapsed: bool,
    /// Whether the database clock is past the absolute deadline.
    pub absolute_lapsed: bool,
}

/// The row a sign-in inserts, every value the session table records about
/// how the session began.
#[derive(Debug, Clone)]
pub struct NewSession<Id, Desc> {
    /// The connetto-minted session id.
    pub session_id: SessionId,
    /// The typed user id the session carries.
    pub user_id: Id,
    /// Hash of the first refresh secret.
    pub current_refresh_hash: Vec<u8>,
    /// How long the session stays refreshable without a refresh.
    pub idle_window: Duration,
    /// How long the session stays refreshable however often it is refreshed.
    pub absolute_ceiling: Duration,
    /// The configured provider the sign-in came through.
    pub provider: String,
    /// The issuer the identity was verified from.
    pub issuer: String,
    /// The application's description of where the sign-in came from.
    pub description: Desc,
}

/// The times a session row's insert stamped from the database's clock.
#[derive(Debug, Clone, Copy)]
pub struct SessionStamps {
    /// When the session began.
    pub created_at: Instant,
    /// When the session lapses if it is never refreshed again.
    pub idle_deadline: Instant,
    /// When the session lapses however often it is refreshed.
    pub absolute_deadline: Instant,
}

/// How one session began and when it last refreshed, what a person needs to
/// tell their sessions apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord<Id, Desc> {
    /// The connetto-minted session id.
    pub session_id: SessionId,
    /// Whose session it is.
    pub user_id: Id,
    /// The configured provider the sign-in came through.
    pub provider: String,
    /// The issuer the identity was verified from.
    pub issuer: String,
    /// The application's description of where the sign-in came from.
    pub description: Desc,
    /// When the session began.
    pub created_at: SystemTime,
    /// When the session last refreshed, absent until its first refresh.
    pub refreshed_at: Option<SystemTime>,
}

/// The statements [`DbAuthStore`](super::store::DbAuthStore) runs over the
/// deployment's session and provider-token tables.
///
/// Every function runs at most one statement on the connection it is handed
/// and decides nothing. A deadline the tables hold is stamped from the
/// database's clock and judged against it (chapter 21), so the values are
/// [`crate::db_clock`] expressions the statement runs.
pub trait ConnettoStoreSchema: Send + Sync + 'static {
    /// The developer's typed distributed user id, the `sessions.user_id`
    /// value.
    type Id: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + core::fmt::Display
        + Send
        + Sync
        + 'static;

    /// The application's description of a session, one column per field,
    /// which its describe callback returns at each sign-in. `()` when the
    /// deployment declares none.
    type Descriptor: Clone + Send + Sync + 'static;

    /// The tables this member reads and writes, by name, which startup
    /// requires to exist (R98 decision 5).
    const TABLES: &'static [&'static str];

    /// The description every session carries when the deployment declares
    /// none, `None` when it declares one, so a server without a describe
    /// callback refuses to start.
    fn undescribed() -> Option<Self::Descriptor>;

    /// Insert a session row, stamping its creation time and both deadlines
    /// from the database's clock, and return the three.
    fn new_session(
        conn: &mut AsyncPgConnection,
        session: NewSession<Self::Id, Self::Descriptor>,
    ) -> Statement<'_, SessionStamps>;

    /// How the session began and when it last refreshed, absent when the id
    /// names no session.
    fn session_record(
        conn: &mut AsyncPgConnection,
        session_id: SessionId,
    ) -> Statement<'_, Option<SessionRecord<Self::Id, Self::Descriptor>>>;

    /// Every live session of `user`, oldest first: not revoked, and neither
    /// deadline passed by the database's clock.
    fn sessions_of(
        conn: &mut AsyncPgConnection,
        user: Self::Id,
    ) -> Statement<'_, Vec<SessionRecord<Self::Id, Self::Descriptor>>>;

    /// Revoke `session` when it is `user`'s and not yet revoked, answering
    /// whether it was.
    fn end_owned(
        conn: &mut AsyncPgConnection,
        user: Self::Id,
        session: SessionId,
    ) -> Statement<'_, bool>;

    /// Revoke every live session of `user` but `keep`, answering the ones it
    /// revoked.
    fn end_all_of(
        conn: &mut AsyncPgConnection,
        user: Self::Id,
        keep: Option<SessionId>,
    ) -> Statement<'_, Vec<SessionId>>;

    /// The `revoked` flag of a session whose absolute deadline has not passed
    /// by the database's clock, absent when the id names no such row.
    fn live_session(
        conn: &mut AsyncPgConnection,
        session_id: SessionId,
    ) -> Statement<'_, Option<bool>>;

    /// The session's identity, refresh hash and the database's verdict on its
    /// two deadlines, locked `FOR UPDATE` until the transaction ends, absent
    /// when the id names no session.
    fn session_row_for_update(
        conn: &mut AsyncPgConnection,
        session_id: SessionId,
    ) -> Statement<'_, Option<SessionRow<Self::Id>>>;

    /// Rotate the refresh hash, slide the idle deadline and stamp the last
    /// refresh time by the database's clock, returning the row's idle and
    /// absolute deadlines.
    fn rotation_update(
        conn: &mut AsyncPgConnection,
        session_id: SessionId,
        current_refresh_hash: Vec<u8>,
        idle_window: Duration,
    ) -> Statement<'_, (Instant, Instant)>;

    /// Mark the session revoked.
    fn revoke_update(conn: &mut AsyncPgConnection, session_id: SessionId) -> Statement<'_, ()>;

    /// Mark every session not yet revoked as revoked, returning how many were.
    fn revoke_every_update(conn: &mut AsyncPgConnection) -> Statement<'_, usize>;

    /// Store a session's retained provider tokens, stamping the expiry from
    /// the database's clock when the provider stated a lifetime, replacing
    /// whatever row the session already holds.
    fn set_provider_token(
        conn: &mut AsyncPgConnection,
        session_id: SessionId,
        issuer: String,
        access_token: String,
        refresh_token: Option<String>,
        expires_in: Option<Duration>,
    ) -> Statement<'_, ()>;

    /// The session's retained provider tokens, with the database's verdict on
    /// the access token's lifetime, absent when none were stored.
    fn provider_token(
        conn: &mut AsyncPgConnection,
        session_id: SessionId,
    ) -> Statement<'_, Option<StoredProviderToken>>;
}

/// Generate the default connetto auth tables and their [`ConnettoStoreSchema`]
/// impl, parameterized by the developer's `Id` type and its diesel SQL type,
/// and optionally the application's session descriptor with each field's SQL
/// type.
///
/// This is a convenience default only: the tables are the developer's to define
/// however they like, and [`ConnettoStoreSchema`] is the real contract,
/// implementable by hand against any tables. The reference SQL for these tables
/// is in `docs/architecture/11-authentication.md`.
///
/// Invoked at module scope, it emits the `connetto_sessions` and
/// `connetto_provider_tokens` `diesel::table!` modules and a unit struct
/// `ConnettoAuthSchema` implementing the trait. The caller needs `diesel` in
/// scope. The descriptor is destructured field by field, so a field list that
/// disagrees with the type does not compile. With no descriptor it is `()`.
///
/// ```ignore
/// connetto_server::connetto_auth_tables!(String, diesel::sql_types::Text;);
/// let store = DbAuthStore::<ConnettoAuthSchema>::new(pool, lifetimes, resolver);
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! connetto_auth_tables {
    ($id:ty, $id_sql:ty; $($desc:ident)::+ { $($field:ident : $field_sql:ty),* $(,)? }) => {
        $crate::connetto_auth_tables!(
            @emit $id, $id_sql; {$($desc)::+}; ($($desc)::+ { $($field),* }); None;
            $($field : $field_sql),*
        );
    };
    ($id:ty, $id_sql:ty $(;)?) => {
        $crate::connetto_auth_tables!(@emit $id, $id_sql; {()}; (()); Some(()););
    };
    (
        @emit $id:ty, $id_sql:ty; {$($desc:tt)*}; ($($shape:tt)*); $undescribed:expr;
        $($field:ident : $field_sql:ty),*
    ) => {
        diesel::table! {
            /// connetto sessions: connetto-minted session id, the developer's
            /// typed user id, the rotating refresh-secret hash, the refresh
            /// deadlines, and how the session began.
            connetto_sessions (session_id) {
                /// The connetto-minted session id, the durable handle every
                /// other contract keys on.
                session_id -> diesel::sql_types::Uuid,
                /// Whose session it is, in the deployment's own id type.
                user_id -> $id_sql,
                /// Hash of the refresh secret currently valid for this session.
                /// Presenting a rotated-out one is treated as theft.
                current_refresh_hash -> diesel::sql_types::Binary,
                /// When the session lapses if it is never refreshed again.
                idle_deadline -> diesel::sql_types::Timestamptz,
                /// When the session lapses however often it is refreshed.
                absolute_deadline -> diesel::sql_types::Timestamptz,
                /// Whether the session was ended before either deadline.
                revoked -> diesel::sql_types::Bool,
                /// When the session began.
                created_at -> diesel::sql_types::Timestamptz,
                /// When the session last refreshed, absent until its first
                /// refresh.
                refreshed_at -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
                /// The configured provider the sign-in came through.
                provider -> diesel::sql_types::Text,
                /// The issuer the identity was verified from.
                issuer -> diesel::sql_types::Text,
                $( $field -> $field_sql, )*
            }
        }

        diesel::table! {
            /// connetto retained provider tokens, keyed by session id.
            connetto_provider_tokens (session_id) {
                /// The session these provider tokens belong to.
                session_id -> diesel::sql_types::Uuid,
                /// The identity provider that issued them, as its issuer URL.
                issuer -> diesel::sql_types::Text,
                /// The provider access token, never emitted to a client.
                access_token -> diesel::sql_types::Text,
                /// The provider refresh token, absent when the provider issued
                /// none.
                refresh_token -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
                /// When the access token expires, absent when the provider said
                /// nothing about it.
                expires_at -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
            }
        }

        /// The default connetto auth schema over the developer's `Id` type.
        #[derive(Debug, Clone, Copy, Default)]
        pub struct ConnettoAuthSchema;

        impl $crate::authn::schema::ConnettoStoreSchema for ConnettoAuthSchema {
            type Id = $id;
            type Descriptor = $($desc)*;
            const TABLES: &'static [&'static str] =
                &["connetto_sessions", "connetto_provider_tokens"];

            fn undescribed() -> Option<Self::Descriptor> {
                $undescribed
            }

            fn new_session(
                conn: &mut diesel_async::AsyncPgConnection,
                session: $crate::authn::schema::NewSession<Self::Id, Self::Descriptor>,
            ) -> $crate::authn::schema::Statement<'_, $crate::authn::schema::SessionStamps> {
                Box::pin(async move {
                    use diesel::ExpressionMethods as _;
                    let $crate::authn::schema::NewSession {
                        session_id,
                        user_id,
                        current_refresh_hash,
                        idle_window,
                        absolute_ceiling,
                        provider,
                        issuer,
                        description,
                    } = session;
                    let $($shape)* = description;
                    // The creation time and the deadlines are the database's
                    // clock plus the windows, in the statement (chapter 21).
                    let (created_at, idle_deadline, absolute_deadline) =
                        diesel_async::RunQueryDsl::get_result(
                            diesel::insert_into(connetto_sessions::table)
                                .values((
                                    connetto_sessions::session_id.eq(session_id),
                                    connetto_sessions::user_id.eq(user_id),
                                    connetto_sessions::current_refresh_hash
                                        .eq(current_refresh_hash),
                                    connetto_sessions::idle_deadline.eq(diesel::dsl::sql::<
                                        diesel::sql_types::Timestamptz,
                                    >(
                                        "now() + make_interval(secs => "
                                    )
                                    .bind::<diesel::sql_types::Double, _>(
                                        idle_window.as_secs_f64(),
                                    )
                                    .sql(")")),
                                    connetto_sessions::absolute_deadline.eq(diesel::dsl::sql::<
                                        diesel::sql_types::Timestamptz,
                                    >(
                                        "now() + make_interval(secs => ",
                                    )
                                    .bind::<diesel::sql_types::Double, _>(
                                        absolute_ceiling.as_secs_f64(),
                                    )
                                    .sql(")")),
                                    connetto_sessions::revoked.eq(false),
                                    connetto_sessions::created_at.eq($crate::db_clock::now()),
                                    connetto_sessions::provider.eq(provider),
                                    connetto_sessions::issuer.eq(issuer),
                                    $( connetto_sessions::$field.eq($field), )*
                                ))
                                .returning((
                                    connetto_sessions::created_at,
                                    connetto_sessions::idle_deadline,
                                    connetto_sessions::absolute_deadline,
                                )),
                            conn,
                        )
                        .await?;
                    Ok($crate::authn::schema::SessionStamps {
                        created_at,
                        idle_deadline,
                        absolute_deadline,
                    })
                })
            }

            fn session_record(
                conn: &mut diesel_async::AsyncPgConnection,
                session_id: $crate::SessionId,
            ) -> $crate::authn::schema::Statement<
                '_,
                Option<$crate::authn::schema::SessionRecord<Self::Id, Self::Descriptor>>,
            > {
                Box::pin(async move {
                    use diesel::ExpressionMethods as _;
                    use diesel::OptionalExtension as _;
                    let found: Option<(
                        Self::Id,
                        String,
                        String,
                        $crate::authn::schema::Instant,
                        Option<$crate::authn::schema::Instant>,
                        $( $crate::__connetto_type_hole!($field), )*
                    )> = diesel_async::RunQueryDsl::first(
                        diesel::QueryDsl::select(
                            diesel::QueryDsl::filter(
                                connetto_sessions::table,
                                connetto_sessions::session_id.eq(session_id),
                            ),
                            (
                                connetto_sessions::user_id,
                                connetto_sessions::provider,
                                connetto_sessions::issuer,
                                connetto_sessions::created_at,
                                connetto_sessions::refreshed_at,
                                $( connetto_sessions::$field, )*
                            ),
                        ),
                        conn,
                    )
                    .await
                    .optional()?;
                    Ok(found.map(
                        |(user_id, provider, issuer, created_at, refreshed_at, $($field,)*)| {
                            $crate::authn::schema::SessionRecord {
                                session_id,
                                user_id,
                                provider,
                                issuer,
                                description: $($shape)*,
                                created_at: created_at.into(),
                                refreshed_at: refreshed_at.map(Into::into),
                            }
                        },
                    ))
                })
            }

            fn sessions_of(
                conn: &mut diesel_async::AsyncPgConnection,
                user: Self::Id,
            ) -> $crate::authn::schema::Statement<
                '_,
                Vec<$crate::authn::schema::SessionRecord<Self::Id, Self::Descriptor>>,
            > {
                Box::pin(async move {
                    use diesel::ExpressionMethods as _;
                    use diesel::BoolExpressionMethods as _;
                    let rows: Vec<(
                        $crate::SessionId,
                        Self::Id,
                        String,
                        String,
                        $crate::authn::schema::Instant,
                        Option<$crate::authn::schema::Instant>,
                        $( $crate::__connetto_type_hole!($field), )*
                    )> = diesel_async::RunQueryDsl::load(
                        diesel::QueryDsl::select(
                            diesel::QueryDsl::order(
                                diesel::QueryDsl::filter(
                                    connetto_sessions::table,
                                    connetto_sessions::user_id
                                        .eq(user)
                                        .and(connetto_sessions::revoked.eq(false))
                                        .and(
                                            connetto_sessions::idle_deadline
                                                .ge($crate::db_clock::now()),
                                        )
                                        .and(
                                            connetto_sessions::absolute_deadline
                                                .ge($crate::db_clock::now()),
                                        ),
                                ),
                                connetto_sessions::created_at,
                            ),
                            (
                                connetto_sessions::session_id,
                                connetto_sessions::user_id,
                                connetto_sessions::provider,
                                connetto_sessions::issuer,
                                connetto_sessions::created_at,
                                connetto_sessions::refreshed_at,
                                $( connetto_sessions::$field, )*
                            ),
                        ),
                        conn,
                    )
                    .await?;
                    Ok(rows
                        .into_iter()
                        .map(
                            |(
                                session_id,
                                user_id,
                                provider,
                                issuer,
                                created_at,
                                refreshed_at,
                                $($field,)*
                            )| {
                                $crate::authn::schema::SessionRecord {
                                    session_id,
                                    user_id,
                                    provider,
                                    issuer,
                                    description: $($shape)*,
                                    created_at: created_at.into(),
                                    refreshed_at: refreshed_at.map(Into::into),
                                }
                            },
                        )
                        .collect())
                })
            }

            fn end_owned(
                conn: &mut diesel_async::AsyncPgConnection,
                user: Self::Id,
                session: $crate::SessionId,
            ) -> $crate::authn::schema::Statement<'_, bool> {
                Box::pin(async move {
                    use diesel::ExpressionMethods as _;
                    use diesel::BoolExpressionMethods as _;
                    let ended = diesel_async::RunQueryDsl::execute(
                        diesel::update(diesel::QueryDsl::filter(
                            connetto_sessions::table,
                            connetto_sessions::session_id
                                .eq(session)
                                .and(connetto_sessions::user_id.eq(user))
                                .and(connetto_sessions::revoked.eq(false)),
                        ))
                        .set(connetto_sessions::revoked.eq(true)),
                        conn,
                    )
                    .await?;
                    Ok(ended == 1)
                })
            }

            fn end_all_of(
                conn: &mut diesel_async::AsyncPgConnection,
                user: Self::Id,
                keep: Option<$crate::SessionId>,
            ) -> $crate::authn::schema::Statement<'_, Vec<$crate::SessionId>> {
                Box::pin(async move {
                    use diesel::ExpressionMethods as _;
                    use diesel::BoolExpressionMethods as _;
                    diesel_async::RunQueryDsl::get_results(
                        diesel::update(diesel::QueryDsl::filter(
                            connetto_sessions::table,
                            connetto_sessions::user_id
                                .eq(user)
                                .and(connetto_sessions::revoked.eq(false))
                                .and(connetto_sessions::idle_deadline.ge($crate::db_clock::now()))
                                .and(
                                    connetto_sessions::absolute_deadline
                                        .ge($crate::db_clock::now()),
                                )
                                .and(connetto_sessions::session_id.ne_all(keep.into_iter().collect::<Vec<_>>())),
                        ))
                        .set(connetto_sessions::revoked.eq(true))
                        .returning(connetto_sessions::session_id),
                        conn,
                    )
                    .await
                })
            }

            fn live_session(
                conn: &mut diesel_async::AsyncPgConnection,
                session_id: $crate::SessionId,
            ) -> $crate::authn::schema::Statement<'_, Option<bool>> {
                Box::pin(async move {
                    use diesel::ExpressionMethods as _;
                    use diesel::OptionalExtension as _;
                    // Qualified calls, so a caller's own named diesel imports
                    // cannot make `filter` ambiguous (macro_hygiene.rs).
                    diesel_async::RunQueryDsl::first(
                        diesel::QueryDsl::select(
                            diesel::QueryDsl::filter(
                                diesel::QueryDsl::filter(
                                    connetto_sessions::table,
                                    connetto_sessions::session_id.eq(session_id),
                                ),
                                connetto_sessions::absolute_deadline.ge($crate::db_clock::now()),
                            ),
                            connetto_sessions::revoked,
                        ),
                        conn,
                    )
                    .await
                    .optional()
                })
            }

            fn session_row_for_update(
                conn: &mut diesel_async::AsyncPgConnection,
                session_id: $crate::SessionId,
            ) -> $crate::authn::schema::Statement<
                '_,
                Option<$crate::authn::schema::SessionRow<Self::Id>>,
            > {
                Box::pin(async move {
                    use diesel::ExpressionMethods;
                    use diesel::OptionalExtension as _;
                    use diesel_async::RunQueryDsl;
                    let found: Option<(Self::Id, Vec<u8>, bool, bool, bool)> =
                        diesel::QueryDsl::for_update(diesel::QueryDsl::select(
                            diesel::QueryDsl::filter(
                                connetto_sessions::table,
                                connetto_sessions::session_id.eq(session_id),
                            ),
                            (
                                connetto_sessions::user_id,
                                connetto_sessions::current_refresh_hash,
                                connetto_sessions::revoked,
                                connetto_sessions::idle_deadline.lt($crate::db_clock::now()),
                                connetto_sessions::absolute_deadline.lt($crate::db_clock::now()),
                            ),
                        ))
                        .first(conn)
                        .await
                        .optional()?;
                    Ok(found.map(
                        |(user_id, current_refresh_hash, revoked, idle_lapsed, absolute_lapsed)| {
                            $crate::authn::schema::SessionRow {
                                user_id,
                                current_refresh_hash,
                                revoked,
                                idle_lapsed,
                                absolute_lapsed,
                            }
                        },
                    ))
                })
            }

            fn rotation_update(
                conn: &mut diesel_async::AsyncPgConnection,
                session_id: $crate::SessionId,
                current_refresh_hash: Vec<u8>,
                idle_window: std::time::Duration,
            ) -> $crate::authn::schema::Statement<
                '_,
                (
                    $crate::authn::schema::Instant,
                    $crate::authn::schema::Instant,
                ),
            > {
                Box::pin(async move {
                    use diesel::ExpressionMethods;
                    use diesel_async::RunQueryDsl as _;
                    diesel::update(diesel::QueryDsl::filter(
                        connetto_sessions::table,
                        connetto_sessions::session_id.eq(session_id),
                    ))
                    .set((
                        connetto_sessions::current_refresh_hash.eq(current_refresh_hash),
                        connetto_sessions::refreshed_at.eq(
                            diesel::NullableExpressionMethods::nullable($crate::db_clock::now()),
                        ),
                        connetto_sessions::idle_deadline.eq(diesel::dsl::sql::<
                            diesel::sql_types::Timestamptz,
                        >(
                            "now() + make_interval(secs => "
                        )
                        .bind::<diesel::sql_types::Double, _>(idle_window.as_secs_f64())
                        .sql(")")),
                    ))
                    .returning((
                        connetto_sessions::idle_deadline,
                        connetto_sessions::absolute_deadline,
                    ))
                    .get_result(conn)
                    .await
                })
            }

            fn revoke_update(
                conn: &mut diesel_async::AsyncPgConnection,
                session_id: $crate::SessionId,
            ) -> $crate::authn::schema::Statement<'_, ()> {
                Box::pin(async move {
                    use diesel::ExpressionMethods;
                    use diesel_async::RunQueryDsl as _;
                    diesel::update(diesel::QueryDsl::filter(
                        connetto_sessions::table,
                        connetto_sessions::session_id.eq(session_id),
                    ))
                    .set(connetto_sessions::revoked.eq(true))
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            }

            fn revoke_every_update(
                conn: &mut diesel_async::AsyncPgConnection,
            ) -> $crate::authn::schema::Statement<'_, usize> {
                Box::pin(async move {
                    use diesel::ExpressionMethods;
                    use diesel_async::RunQueryDsl as _;
                    diesel::update(diesel::QueryDsl::filter(
                        connetto_sessions::table,
                        connetto_sessions::revoked.eq(false),
                    ))
                    .set(connetto_sessions::revoked.eq(true))
                    .execute(conn)
                    .await
                })
            }

            fn set_provider_token(
                conn: &mut diesel_async::AsyncPgConnection,
                session_id: $crate::SessionId,
                issuer: String,
                access_token: String,
                refresh_token: Option<String>,
                expires_in: Option<std::time::Duration>,
            ) -> $crate::authn::schema::Statement<'_, ()> {
                Box::pin(async move {
                    use diesel::ExpressionMethods;
                    use diesel_async::RunQueryDsl as _;
                    // The deadline is the database's clock plus the provider's
                    // lifetime, or NULL. The DSL cannot name that value, so the
                    // statement spells it (chapter 21).
                    let lifetime = expires_in.map(|lifetime| lifetime.as_secs_f64());
                    let expiry = || {
                        diesel::dsl::sql::<
                            diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
                        >("CASE WHEN ")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Double>, _>(lifetime)
                        .sql(" IS NULL THEN NULL ELSE now() + make_interval(secs => ")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Double>, _>(lifetime)
                        .sql(") END")
                    };
                    diesel::insert_into(connetto_provider_tokens::table)
                        .values((
                            connetto_provider_tokens::session_id.eq(session_id),
                            connetto_provider_tokens::issuer.eq(issuer.clone()),
                            connetto_provider_tokens::access_token.eq(access_token.clone()),
                            connetto_provider_tokens::refresh_token.eq(refresh_token.clone()),
                            connetto_provider_tokens::expires_at.eq(expiry()),
                        ))
                        .on_conflict(connetto_provider_tokens::session_id)
                        .do_update()
                        .set((
                            connetto_provider_tokens::issuer.eq(issuer),
                            connetto_provider_tokens::access_token.eq(access_token),
                            connetto_provider_tokens::refresh_token.eq(refresh_token),
                            connetto_provider_tokens::expires_at.eq(expiry()),
                        ))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            }

            fn provider_token(
                conn: &mut diesel_async::AsyncPgConnection,
                session_id: $crate::SessionId,
            ) -> $crate::authn::schema::Statement<
                '_,
                Option<$crate::authn::provider::StoredProviderToken>,
            > {
                Box::pin(async move {
                    use diesel::ExpressionMethods;
                    use diesel::OptionalExtension as _;
                    use diesel_async::RunQueryDsl;
                    let found: Option<(String, String, Option<String>, bool, Option<bool>)> =
                        diesel::QueryDsl::select(
                            diesel::QueryDsl::filter(
                                connetto_provider_tokens::table,
                                connetto_provider_tokens::session_id.eq(session_id),
                            ),
                            (
                                connetto_provider_tokens::issuer,
                                connetto_provider_tokens::access_token,
                                connetto_provider_tokens::refresh_token,
                                connetto_provider_tokens::expires_at.is_null(),
                                connetto_provider_tokens::expires_at.gt(diesel::dsl::sql::<
                                    diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
                                >(
                                    "now() + make_interval(secs => ",
                                )
                                .bind::<diesel::sql_types::Double, _>(
                                    $crate::authn::provider::PROVIDER_TOKEN_MARGIN.as_secs_f64(),
                                )
                                .sql(")")),
                            ),
                        )
                        .first(conn)
                        .await
                        .optional()?;
                    Ok(
                        found.map(|(issuer, access_token, refresh_token, open, fresh)| {
                            $crate::authn::provider::StoredProviderToken {
                                issuer,
                                access_token,
                                refresh_token,
                                still_valid: open || fresh.unwrap_or(false),
                            }
                        }),
                    )
                })
            }
        }
    };
}
