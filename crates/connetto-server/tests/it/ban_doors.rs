//! Needs Docker: the fixture starts its own Postgres.
//!
//! A ban shuts off everything (R105 decision 8). The ban the guard imposes
//! ends every session of the banned identity, each recording `ended_by_ban`,
//! and a ban in the store, however it got there, refuses the identity's
//! refresh, its next sign-in and the liveness check.

use std::time::{Duration, Instant};

use connetto_core::messages::{ControlMessage, Subscribe, SubscriptionSpec};
use connetto_core::traits::Transport;
use connetto_server::{
    AbuseConfig, AbuseLimits, ConnectionLimits, LivenessError, NewBan, PersonLimits, ServerHandle,
    ServerParts, pg_ban_store,
};
use connetto_test_harness::{
    ConnettoDefaults, Fixture, MOCK_OAUTH_PROVIDER, isolated_session_keyring,
};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::Pool;
use tokio::net::TcpListener;

use super::e2e::{PG_SERIAL, audit_ops, exec, mint_tokens, reset_fixture};
use super::lifecycle::{admin_pool, builder_over, live_session, next_control, wait_ready};

/// How long a ban's consequences may take to land.
const SETTLE: Duration = Duration::from_secs(10);

/// A server with bans on, a person banned on naming two tables that do not
/// exist, and the admin pool to read its tables.
struct Served {
    pool: Pool<AsyncPgConnection>,
    handle: ServerHandle,
    base: String,
    addr: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Served {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Two unresolvable subscriptions ban their person. The per-connection limit
/// must sit below the person's, so one strike cannot ban.
fn two_strikes() -> AbuseConfig {
    AbuseLimits::new()
        .with_person(
            PersonLimits::new().with_unresolvable_subscriptions(2, Duration::from_secs(300)),
        )
        .with_connection(ConnectionLimits::new().with_unresolvable_subscriptions(1))
        .build()
        .expect("a valid threshold")
}

async fn served(fixture: &Fixture) -> Served {
    let pool = admin_pool(fixture).await;
    reset_fixture(&pool, fixture).await;
    fixture.provision_auth_tables().await;
    exec(&pool, "DROP TABLE IF EXISTS connetto_bans").await;
    exec(
        &pool,
        "CREATE TABLE connetto_bans (user_id TEXT PRIMARY KEY, session UUID NOT NULL, \
         reason TEXT NOT NULL, banned_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ)",
    )
    .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, idp, keys) = builder_over(fixture, port).await;
    let ServerParts {
        router,
        change_stream,
        handle,
        ..
    } = builder
        .bans(true)
        .abuse(two_strikes())
        .build()
        .await
        .expect("the deployment assembles");
    let stream = tokio::spawn(async move {
        let _ = change_stream.await;
    });
    let http = tokio::spawn(async move {
        let _keep = (idp, keys);
        let _ = axum::serve(listener, router).await;
    });
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base).await;
    Served {
        pool,
        handle,
        base,
        addr: format!("127.0.0.1:{port}"),
        tasks: vec![stream, http],
    }
}

/// The status the sign-in callback answers `subject`, the whole login dance
/// run through the server's auth endpoints.
pub(super) async fn sign_in_status(base: &str, subject: &str) -> reqwest::StatusCode {
    let agent = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("an HTTP client");
    let login = agent
        .get(format!("{base}/auth/login"))
        .query(&[("provider", MOCK_OAUTH_PROVIDER)])
        .send()
        .await
        .expect("GET /auth/login");
    let authorize = login.headers()["location"]
        .to_str()
        .expect("utf-8")
        .to_owned();
    let authorized = agent
        .post(&authorize)
        .form(&[("username", subject)])
        .send()
        .await
        .expect("POST the provider's authorize");
    let callback = authorized.headers()["location"]
        .to_str()
        .expect("utf-8")
        .to_owned();
    agent
        .get(&callback)
        .send()
        .await
        .expect("GET /auth/callback")
        .status()
}

/// The status `POST /auth/refresh` answers `refresh_token`.
pub(super) async fn refresh_status(base: &str, refresh_token: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("{base}/auth/refresh"))
        .json(&serde_json::json!({ "refresh_token": refresh_token }))
        .send()
        .await
        .expect("POST /auth/refresh")
        .status()
}

#[tokio::test]
async fn a_ban_ends_every_session_of_the_identity_and_shuts_its_doors() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;
    let handle = &served.handle;

    let (probing, _, _) = mint_tokens(&served.base, "berlin").await;
    let (elsewhere, _, refresh) = mint_tokens(&served.base, "berlin").await;
    let (bystander, _, _) = mint_tokens(&served.base, "milan").await;

    let mut client = live_session(&served.addr, &probing).await;
    for ghost in ["ghost-one", "ghost-two"] {
        client
            .send_control(ControlMessage::Subscribe(Subscribe {
                sub_id: ghost.to_owned(),
                spec: SubscriptionSpec::new("SELECT * FROM ghost"),
            }))
            .await
            .expect("name a table that does not exist");
        let _ = next_control(&mut client).await;
    }

    // The ban answers the liveness check as soon as it is written, so waiting
    // for any refusal would pass before the hook ends anything. Wait for the
    // sessions themselves to end.
    let deadline = Instant::now() + SETTLE;
    for token in [&probing, &elsewhere] {
        while !matches!(handle.verify_live(token).await, Err(LivenessError::Revoked)) {
            assert!(
                Instant::now() < deadline,
                "the ban never ended every session of the identity"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert!(
        handle.verify_live(&bystander).await.is_ok(),
        "a ban names one identity and no one else"
    );
    let deadline = Instant::now() + SETTLE;
    loop {
        let ended = audit_ops(&served.pool)
            .await
            .iter()
            .filter(|op| op.as_str() == "ended_by_ban")
            .count();
        if ended == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{ended} ended_by_ban rows, expected 2"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        refresh_status(&served.base, &refresh).await,
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        sign_in_status(&served.base, "berlin").await,
        reqwest::StatusCode::UNAUTHORIZED,
        "a banned identity cannot sign in again"
    );
    assert_eq!(
        sign_in_status(&served.base, "milan").await,
        reqwest::StatusCode::OK
    );
}

#[tokio::test]
async fn a_ban_in_the_store_alone_refuses_refresh_sign_in_and_liveness() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;

    let (token, user, refresh) = mint_tokens(&served.base, "berlin").await;
    let session = served
        .handle
        .verify_live(&token)
        .await
        .expect("live")
        .session_id;
    pg_ban_store::<ConnettoDefaults>(served.pool.clone())
        .impose(NewBan::starting_now(
            user,
            session,
            "the operator's call",
            None,
        ))
        .await
        .expect("impose a ban");

    assert!(
        served.handle.verify_live(&token).await.is_err(),
        "the liveness check reads the ban list"
    );
    assert_eq!(
        refresh_status(&served.base, &refresh).await,
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        sign_in_status(&served.base, "berlin").await,
        reqwest::StatusCode::UNAUTHORIZED
    );
}

/// A ban list that cannot be read refuses every door, so an outage never lifts
/// a ban, and a refresh it refused leaves the caller's token standing, so the
/// retry after the outage is not taken for a stolen token.
#[tokio::test]
async fn a_ban_list_that_cannot_be_read_shuts_the_doors_and_burns_no_token() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;

    let (token, _, refresh) = mint_tokens(&served.base, "berlin").await;
    exec(
        &served.pool,
        "ALTER TABLE connetto_bans RENAME TO connetto_bans_away",
    )
    .await;

    assert!(
        matches!(
            served.handle.verify_live(&token).await,
            Err(LivenessError::BanList(_))
        ),
        "the liveness check fails closed"
    );
    assert_eq!(
        refresh_status(&served.base, &refresh).await,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "a refresh fails closed"
    );
    assert_eq!(
        sign_in_status(&served.base, "berlin").await,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "a sign-in fails closed"
    );

    exec(
        &served.pool,
        "ALTER TABLE connetto_bans_away RENAME TO connetto_bans",
    )
    .await;
    assert_eq!(
        refresh_status(&served.base, &refresh).await,
        reqwest::StatusCode::OK,
        "the refused refresh rotated nothing, so its token still refreshes"
    );
    assert!(
        served.handle.verify_live(&token).await.is_ok(),
        "the session survived the outage"
    );
}
