//! R107 proof, the sunset (slice 1). The acknowledgement carries the
//! major's configured sunset and raises the event with it, a handshake
//! after the instant is refused with the reason the client classifies
//! update-required, and a live connection is closed at the instant with
//! the same reason and never redials.
//!
//! Needs Docker: the fixture starts its own Postgres.

use std::sync::Arc;
use std::time::Duration;

use connetto_client::{
    ClientBuilder, ClientError, ClientEvent, CoreClient, CorePump, DataDir, Grant, HeldCredential,
    MemoryRefreshStore, ReconnectPolicy, SyncSchema,
};
use connetto_core::messages::FatalErrorReason;
use connetto_core::schema::SchemaBundle;
use connetto_core::test_support::TestGrantChecker;
use connetto_core::{MAJOR_DIRECTIVE, Major, SchemaVersion};
use connetto_server::{
    ManagerBuilder, Materializer, PgReadConnector, PgSnapshotSource, RuntimeWritableCatalog,
    SessionConfig, SessionManager, WebSocketTransport, pg_write_target,
};
use connetto_test_harness::events::await_event;
use connetto_test_harness::router::{HostedEntry, Relay, Router};
use connetto_test_harness::{ConnettoDefaults, Fixture, RosterAuth};
use tempfile::tempdir;
use tokio::net::TcpStream;

/// The Postgres source the manager parses, its major line the way the
/// deployment's schema file carries it.
const PG_DDL: &str = "-- connetto:major 1\nCREATE TABLE items (id INT PRIMARY KEY, label TEXT);";

/// The replica DDL the build bakes, `IF NOT EXISTS` because `connect`
/// replays the caller's DDL on every open.
const DDL: &str = "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, label TEXT)";

/// A manager whose computed subscriptions read through connetto's connector.
type Manager = SessionManager<PgSnapshotSource, RosterAuth, ConnettoDefaults, PgReadConnector>;

fn major() -> Major {
    Major::new(1).expect("one is a major")
}

/// The router entry that serves `manager` with whatever connection the
/// router routed, the replayed handshake included.
fn entry(manager: Arc<Manager>) -> HostedEntry {
    Arc::new(move |relay: Relay| {
        let manager = Arc::clone(&manager);
        Box::pin(async move {
            let _ = manager.serve(relay).await;
        })
    })
}

/// A manager for the one hosted major over the fixture's Postgres, with the
/// major's sunset, when the scenario names one.
fn manager(fixture: &Fixture, sunset: Option<time::OffsetDateTime>) -> Arc<Manager> {
    let pool = fixture.admin().clone();
    ManagerBuilder::new(
        Materializer::builder(PG_DDL)
            .with_write_catalog(RuntimeWritableCatalog::builder().writable("items").build())
            .with_read_connector(PgReadConnector::with_session_setup(pool.clone()))
            .build()
            .expect("build materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), PG_DDL).expect("snapshot source"),
        RosterAuth::granting("token"),
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(pool.clone()),
        pg_write_target::<ConnettoDefaults>(pool, PG_DDL).expect("build write target"),
    )
    .with_session(
        SessionConfig::new()
            .with_schema_major(Some(major()))
            .with_schema_version(Some(advertised()))
            .with_sunset(sunset),
    )
    .build()
}

/// The build at the one major.
#[must_use]
fn build() -> SyncSchema {
    SyncSchema::new(
        SchemaBundle::new(
            format!("{MAJOR_DIRECTIVE} 1"),
            "",
            DDL,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares its major"),
    )
}

/// The version a manager built from the same source advertises, so a
/// matching build passes the hash check at the ack.
#[must_use]
fn advertised() -> SchemaVersion {
    build().version()
}

/// The held credential for a device, the grant the test grant checker
/// resolves.
#[must_use]
fn credential(device: &str) -> HeldCredential {
    super::support::held_grant(Grant::new(format!("user:token#{device}")), "token")
}

/// The reconnect policy the test pumps drive, with a short backoff so a
/// broken dial surfaces inside the test bounds.
#[must_use]
fn test_policy() -> ReconnectPolicy {
    ReconnectPolicy::new()
        .with_initial_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(20))
}

/// Connect the build at `dir` through the router with the live pump, the
/// path a deployed app takes.
async fn connect_pumped(
    router: &Router,
    schema: SyncSchema,
    device: &str,
    dir: &std::path::Path,
) -> Result<(CoreClient<WebSocketTransport<TcpStream>, ()>, CorePump), ClientError> {
    let held = credential(device);
    let store = super::support::key_store(&held).await;
    ClientBuilder::new(schema, router.dialer())
        .with_reconnect(test_policy())
        .with_sleeper(|d| tokio::time::sleep(d))
        .signed_in(held)
        .durable(
            DataDir::new(dir.to_path_buf()),
            store,
            MemoryRefreshStore::default(),
        )
        .connect_with_pump()
        .await
}

/// The bound every wait in this file honors, so a wedged path fails, not
/// hangs.
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(15)
}

/// The acknowledgement carries the configured sunset and raises the event
/// with it, and the connection stays live after the announcement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ack_carries_the_configured_sunset_and_raises_the_event() {
    let fixture = Fixture::acquire().await;
    fixture
        .exec("CREATE TABLE items (id INT PRIMARY KEY, label TEXT)")
        .await;
    let instant = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let at_secs = u64::try_from(instant.unix_timestamp()).expect("after the epoch");
    let manager = manager(&fixture, Some(instant));
    let router = Router::start(vec![(major(), entry(manager))]).await;
    let dir = tempdir().expect("the replica dir");
    let (core, pump) = connect_pumped(&router, build(), "sunset", dir.path())
        .await
        .expect("a sunset that has not been reached still serves");
    let client = core.client();
    let _pump = tokio::spawn(pump);
    let mut events = client.events();
    await_event(
        &mut events,
        deadline(),
        |event| matches!(event, ClientEvent::Sunset { at_secs: got } if *got == at_secs),
        |_| false,
    )
    .await;
    // The connection stays live after the announcement.
    client.ping(7).await.expect("the connection is live");
    await_event(
        &mut events,
        deadline(),
        |event| matches!(event, ClientEvent::Pong { nonce: 7 }),
        |_| false,
    )
    .await;
}

/// A handshake after the instant is refused with the reason the client
/// classifies update-required, the way a decommissioned major is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handshake_after_the_sunset_is_update_required() {
    let fixture = Fixture::acquire().await;
    fixture
        .exec("CREATE TABLE items (id INT PRIMARY KEY, label TEXT)")
        .await;
    let past = time::OffsetDateTime::now_utc() - time::Duration::seconds(5);
    let manager = manager(&fixture, Some(past));
    let router = Router::start(vec![(major(), entry(manager))]).await;
    let dir = tempdir().expect("the replica dir");
    match connect_pumped(&router, build(), "sunset", dir.path()).await {
        Err(ClientError::UpdateRequired { client, server }) => {
            assert_eq!(client, Some(major()), "the build's major is named");
            assert_eq!(server, major(), "the server's major is named");
        }
        Err(other) => panic!("expected the typed update-required signal, got {other:?}"),
        Ok(_) => panic!("a build connected after the sunset"),
    }
}

/// A live connection is closed at the instant with the same reason, and
/// never redials, the way a decommissioned major's close is handled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_connection_is_closed_at_the_sunset_with_the_reason_and_never_redials() {
    let fixture = Fixture::acquire().await;
    fixture
        .exec("CREATE TABLE items (id INT PRIMARY KEY, label TEXT)")
        .await;
    let soon = time::OffsetDateTime::now_utc() + time::Duration::seconds(3);
    let manager = manager(&fixture, Some(soon));
    let router = Router::start(vec![(major(), entry(manager))]).await;
    let dir = tempdir().expect("the replica dir");
    let (core, pump) = connect_pumped(&router, build(), "sunset", dir.path())
        .await
        .expect("a sunset ahead of the clock still serves");
    let client = core.client();
    let _pump = tokio::spawn(pump);
    let mut events = client.events();
    // Collect until the pump ends, bounded, so a wedged path fails, not hangs.
    let mut seen: Vec<ClientEvent> = Vec::new();
    let until = deadline();
    while let Ok(Ok(event)) = tokio::time::timeout_at(until, events.recv()).await {
        seen.push(event);
        if matches!(
            seen.last().expect("an event was just pushed"),
            ClientEvent::Closed
        ) {
            break;
        }
    }
    assert!(
        !seen
            .iter()
            .any(|event| matches!(event, ClientEvent::Reconnecting { .. })),
        "a terminal close never redials, got {seen:?}"
    );
    let server_closed = seen.iter().position(|event| {
        matches!(
            event,
            ClientEvent::ServerClosed {
                reason: FatalErrorReason::Sunset { server, .. }
            } if *server == major()
        )
    });
    let stopped = seen.iter().position(|event| {
        matches!(
            event,
            ClientEvent::Stopped { detail } if detail.starts_with("update required")
        )
    });
    let closed = seen
        .iter()
        .position(|event| matches!(event, ClientEvent::Closed));
    assert!(
        server_closed.is_some() && stopped.is_some() && closed.is_some(),
        "the close, the stop and the end all arrive, got {seen:?}"
    );
    assert!(
        server_closed.expect("the close") < stopped.expect("the stop"),
        "the close is announced before the stop, got {seen:?}"
    );
    assert!(
        stopped.expect("the stop") < closed.expect("the end"),
        "the stop is announced before the end, got {seen:?}"
    );
}
