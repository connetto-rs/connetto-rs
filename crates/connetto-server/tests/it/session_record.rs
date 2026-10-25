//! Needs Docker: the fixture starts its own Postgres.
//!
//! A sign-in through the assembled server records how its session began (R105
//! decisions 1, 2 and 9). The describe callback reads the request the server
//! saw, its address among it, and the session row keeps the description, the
//! provider, the issuer and the times, never the address. A refresh stamps the
//! last refresh, the login row names the provider, and the sign-in callback
//! hears of the sign-in.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use connetto_server::{BuildError, DefaultUuidResolver, LoginRequest, SignInNotice};
use connetto_test_harness::{Fixture, MOCK_OAUTH_PROVIDER, isolated_session_keyring};
use diesel::{ExpressionMethods as _, QueryDsl as _};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl as _;
use diesel_async::pooled_connection::bb8::Pool;
use tokio::net::TcpListener;

use super::e2e::{PG_SERIAL, exec, mint_tokens, reset_fixture};
use super::lifecycle::{admin_pool, builder_over, wait_ready};

/// A deployment whose sessions carry where each sign-in came from.
mod app {
    /// Where a sign-in came from, as the application words it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Place {
        pub place: String,
    }

    connetto_server::connetto_schema! {
        pub struct Described;
        id: String => diesel::sql_types::Text,
        audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
        session_descriptor: Place { place: diesel::sql_types::Text },
    }
}

/// How long a spawned audit write may take to land.
const AUDIT_BOUND: Duration = Duration::from_secs(10);

/// The deployment's auth tables, its session table carrying the `place`
/// column the descriptor maps onto.
async fn create_described_tables(pool: &Pool<AsyncPgConnection>) {
    for stmt in [
        "CREATE TABLE connetto_sessions (\
         session_id UUID PRIMARY KEY, user_id TEXT NOT NULL, \
         current_refresh_hash BYTEA NOT NULL, idle_deadline TIMESTAMPTZ NOT NULL, \
         absolute_deadline TIMESTAMPTZ NOT NULL, revoked BOOLEAN NOT NULL DEFAULT FALSE, \
         created_at TIMESTAMPTZ NOT NULL, refreshed_at TIMESTAMPTZ, \
         provider TEXT NOT NULL, issuer TEXT NOT NULL, place TEXT NOT NULL)",
        "CREATE TABLE connetto_provider_tokens (\
         session_id UUID PRIMARY KEY REFERENCES connetto_sessions (session_id) ON DELETE CASCADE, \
         issuer TEXT NOT NULL, access_token TEXT NOT NULL, refresh_token TEXT, \
         expires_at TIMESTAMPTZ)",
    ] {
        exec(pool, stmt).await;
    }
}

/// The provider and issuer of `session`'s login row, waited for since the
/// audit write is spawned.
async fn login_row(
    conn: &mut AsyncPgConnection,
    session: uuid::Uuid,
) -> (Option<String>, Option<String>) {
    let deadline = Instant::now() + AUDIT_BOUND;
    loop {
        let found: Vec<(Option<String>, Option<String>)> = app::auth_events::table
            .filter(app::auth_events::session.eq(session))
            .filter(app::auth_events::op.eq(connetto_server::audit::AuthOp::LoggedIn))
            .select((app::auth_events::provider, app::auth_events::issuer))
            .load(conn)
            .await
            .expect("read the audit table");
        if let Some(row) = found.into_iter().next() {
            return row;
        }
        assert!(Instant::now() < deadline, "the login row never landed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_sign_in_records_where_it_came_from_and_tells_the_application() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    create_described_tables(&pool).await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, idp, _keys) = builder_over(&fixture, port).await;
    let told = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let sink = Arc::clone(&told);
    let builder = builder
        .deployment_schema::<app::Described>(Arc::new(DefaultUuidResolver))
        .session_describer(|request: LoginRequest| async move {
            app::Place {
                place: format!(
                    "{} through {}",
                    request
                        .peer
                        .map_or_else(|| "nowhere".to_owned(), |peer| peer.ip().to_string()),
                    request.provider
                ),
            }
        })
        .on_sign_in(move |notice| sink.lock().push(notice));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(builder.serve(listener, async move {
        let _ = stopped.await;
    }));
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base).await;

    let before = SystemTime::now();
    let (_token, user_id, refresh) = mint_tokens(&base, "milan").await;
    let place = format!("127.0.0.1 through {MOCK_OAUTH_PROVIDER}");

    let mut conn = pool.get().await.expect("a connection");
    let (session, stored_user, provider, issuer, stored_place, refreshed_at): (
        uuid::Uuid,
        String,
        String,
        String,
        String,
        Option<connetto_server::authn::schema::Instant>,
    ) = app::connetto_sessions::table
        .select((
            app::connetto_sessions::session_id,
            app::connetto_sessions::user_id,
            app::connetto_sessions::provider,
            app::connetto_sessions::issuer,
            app::connetto_sessions::place,
            app::connetto_sessions::refreshed_at,
        ))
        .first(&mut conn)
        .await
        .expect("the sign-in stored its session");
    assert_eq!(stored_user, user_id);
    assert_eq!(provider, MOCK_OAUTH_PROVIDER);
    assert_eq!(issuer, idp.issuer());
    assert_eq!(
        stored_place, place,
        "the describe callback saw the address the server saw, and the row keeps its words"
    );
    assert_eq!(refreshed_at, None, "never refreshed yet");

    let notices = told.lock().clone();
    let [SignInNotice::SignedIn(signed_in)] = notices.as_slice() else {
        panic!("expected one sign-in notice, got {notices:?}");
    };
    assert_eq!(*signed_in.session_id.as_uuid(), session);
    assert_eq!(signed_in.user_id, user_id);
    assert_eq!(signed_in.provider, MOCK_OAUTH_PROVIDER);
    assert_eq!(signed_in.issuer, idp.issuer());
    assert_eq!(signed_in.description, app::Place { place });
    assert!(signed_in.created_at >= before - Duration::from_secs(5));

    let (login_provider, login_issuer) = login_row(&mut conn, session).await;
    assert_eq!(login_provider.as_deref(), Some(MOCK_OAUTH_PROVIDER));
    assert_eq!(login_issuer.as_deref(), Some(idp.issuer()));

    let refreshed = reqwest::Client::new()
        .post(format!("{base}/auth/refresh"))
        .json(&serde_json::json!({ "refresh_token": refresh }))
        .send()
        .await
        .expect("POST /auth/refresh");
    assert!(refreshed.status().is_success(), "{}", refreshed.status());
    let refreshed_at: Option<connetto_server::authn::schema::Instant> =
        app::connetto_sessions::table
            .filter(app::connetto_sessions::session_id.eq(session))
            .select(app::connetto_sessions::refreshed_at)
            .first(&mut conn)
            .await
            .expect("read the session");
    assert!(refreshed_at.is_some(), "the refresh is stamped");

    let _ = stop.send(());
    server
        .await
        .expect("join the server")
        .expect("the server stopped cleanly");
}

#[tokio::test]
async fn a_described_schema_without_a_describer_refuses_to_start() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let (builder, _idp, _keys) = builder_over(&fixture, 0).await;
    let refused = builder
        .deployment_schema::<app::Described>(Arc::new(DefaultUuidResolver))
        .build()
        .await;
    assert!(
        matches!(refused, Err(BuildError::Undescribed)),
        "a session descriptor needs a describe callback"
    );
}
