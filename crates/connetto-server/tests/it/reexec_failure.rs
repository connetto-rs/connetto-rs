//! R89 and R106 read-failure classification end-to-end tests.
//!
//! Needs Docker.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use connetto_core::messages::{ControlMessage, PauseCause, SUBSCRIPTION_REFUSED};
use connetto_core::test_support::TestGrantChecker;
use connetto_server::{
    AbuseConfig, ManagerBuilder, Materializer, PgReadConnector, PgSnapshotSource, ReconnectEvent,
    RequestGuard, RuntimeWritableCatalog, SessionManager, ThrottleConfig, TierLimits, loopback,
    pg_write_target,
};
use connetto_test_harness::{Client, ConnettoDefaults, Fixture, RosterAuth, committed_at};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use subql::{CdcSource, PgChangeEvent, PgCommit, PgSqliteEmuSource, SourceItem};
use tracing::Instrument;

use crate::one_event::OneEvent;

const DDL: &str = "CREATE TABLE counts (id INT PRIMARY KEY, n INT)";

type Manager = SessionManager<PgSnapshotSource, RosterAuth, ConnettoDefaults, PgReadConnector>;

fn connect_to(manager: &Arc<Manager>) -> Client {
    let (server_end, client_end) = loopback();
    let session = Arc::clone(manager);
    tokio::spawn(
        async move {
            let _ = session.serve(server_end).await;
        }
        .instrument(tracing::Span::current()),
    );
    Client::new(client_end)
}

fn guard(seed: Duration, triggered: Duration) -> Arc<RequestGuard<String>> {
    Arc::new(RequestGuard::new(
        ThrottleConfig::default()
            .with_identified(TierLimits::identified().with_read_timeout(seed))
            .with_reexec_timeout(triggered),
        AbuseConfig::default(),
    ))
}

fn full_manager(
    connector_pool: Pool<AsyncPgConnection>,
    catalog_ddl: &str,
    schema_ddl: &str,
    auth: RosterAuth,
    guard: Arc<RequestGuard<String>>,
    snapshot_pool: Pool<AsyncPgConnection>,
    write_pool: Pool<AsyncPgConnection>,
) -> Arc<Manager> {
    ManagerBuilder::new(
        Materializer::builder(catalog_ddl)
            .with_write_catalog(RuntimeWritableCatalog::default())
            .with_read_connector(PgReadConnector::with_session_setup(connector_pool.clone()))
            .build()
            .expect("build materializer"),
        PgSnapshotSource::from_ddl(snapshot_pool, schema_ddl).expect("snapshot source"),
        auth,
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(connector_pool),
        pg_write_target::<ConnettoDefaults>(write_pool, schema_ddl).expect("build write target"),
    )
    .with_guard(guard)
    .build()
}

/// The ingest loop on its own task, with the reconnect events it reported.
struct WatchedIngest {
    task: tokio::task::JoinHandle<()>,
    read_retrying: Arc<AtomicBool>,
    stream_retry: Arc<AtomicBool>,
    gave_up: Arc<AtomicBool>,
}

impl WatchedIngest {
    fn spawn<Src>(manager: &Arc<Manager>, mut source: Src) -> Self
    where
        Src: CdcSource<Event = PgChangeEvent, Commit = PgCommit> + 'static,
        Src::Error: core::fmt::Display,
    {
        let read_retrying = Arc::new(AtomicBool::new(false));
        let stream_retry = Arc::new(AtomicBool::new(false));
        let gave_up = Arc::new(AtomicBool::new(false));
        let flags = (
            Arc::clone(&read_retrying),
            Arc::clone(&stream_retry),
            Arc::clone(&gave_up),
        );
        let manager = Arc::clone(manager);
        let task = tokio::spawn(
            async move {
                let (read_retrying, stream_retry, gave_up) = flags;
                let _ = manager
                    .ingest(&mut source, &mut |event| match event {
                        ReconnectEvent::ReadRetrying { .. } => {
                            read_retrying.store(true, Ordering::Relaxed);
                        }
                        ReconnectEvent::Retrying { .. } => {
                            stream_retry.store(true, Ordering::Relaxed);
                        }
                        ReconnectEvent::GaveUp { .. } => gave_up.store(true, Ordering::Relaxed),
                        ReconnectEvent::AuthRetrying { .. } => {}
                    })
                    .await;
            }
            .instrument(tracing::Span::current()),
        );
        Self {
            task,
            read_retrying,
            stream_retry,
            gave_up,
        }
    }

    /// Delivery pauses for the database within `within`, and stays paused in place: the read retries while the change stream neither reconnects nor gives up.
    async fn assert_paused_in_place(&self, client: &mut Client, within: Duration) {
        let paused = tokio::time::timeout(within, client.next_control())
            .await
            .expect("DeliveryPaused in time");
        assert!(
            matches!(
                paused,
                ControlMessage::DeliveryPaused {
                    cause: PauseCause::DatabaseUnreachable
                }
            ),
            "expected DeliveryPaused(DatabaseUnreachable), got {paused:?}"
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            self.read_retrying.load(Ordering::Relaxed),
            "the read retries in place while the database refuses it"
        );
        assert!(
            !self.stream_retry.load(Ordering::Relaxed),
            "the change stream must not be reconnected"
        );
        assert!(
            !self.gave_up.load(Ordering::Relaxed),
            "the ingest loop must not give up"
        );
    }

    /// The held event's re-execution answers `value`, then delivery resumes, with nothing ended.
    async fn assert_resumed_with(self, client: &mut Client, value: &str) {
        // AggregateUpdate is sent inside the dispatch, DeliveryResumed after it returns.
        let reexec = tokio::time::timeout(Duration::from_secs(10), client.next_control())
            .await
            .expect("AggregateUpdate within 10 s");
        let ControlMessage::AggregateUpdate(result) = reexec else {
            panic!("expected AggregateUpdate after resume, got {reexec:?}");
        };
        assert_eq!(result.result_json.as_deref(), Some(value));
        let resumed = tokio::time::timeout(Duration::from_secs(5), client.next_control())
            .await
            .expect("DeliveryResumed within 5 s");
        assert!(
            matches!(resumed, ControlMessage::DeliveryResumed),
            "expected DeliveryResumed, got {resumed:?}"
        );
        self.task.abort();
    }
}

/// Dropping column n poisons re-execution of SELECT MIN(n): the database returns
/// 42703, a code outside the transient list, which is `ReadFailure::Deterministic`.
/// The session refuses the subscription at the first failure (attempts=1,
/// class=deterministic in the log), while a sibling `SELECT *` subscription
/// receives its live patch from the same event unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poisoned_computed_read_ends_alone_without_a_retry() {
    crate::logging::with_capture(
        "a_poisoned_computed_read_ends_alone_without_a_retry",
        |logs| async move {
            let fixture = Fixture::acquire().await;
            fixture.exec("DROP TABLE IF EXISTS counts CASCADE").await;
            fixture
                .exec("CREATE TABLE counts (id INT PRIMARY KEY, n INT)")
                .await;
            fixture
                .exec("INSERT INTO counts (id, n) VALUES (10, 10)")
                .await;

            let pool = fixture.admin().clone();
            let manager = full_manager(
                pool.clone(),
                DDL,
                DDL,
                RosterAuth::granting("reader"),
                guard(Duration::from_secs(30), Duration::from_secs(30)),
                pool.clone(),
                pool,
            );

            let mut computed_client = connect_to(&manager);
            computed_client
                .handshake_with("aggregator", "user:aggregator")
                .await;
            computed_client
                .subscribe("cheapest", "SELECT MIN(n) FROM counts")
                .await;
            let ControlMessage::AggregateUpdate(seeded) = computed_client.next_control().await
            else {
                panic!("expected aggregate seed");
            };
            assert_eq!(seeded.result_json.as_deref(), Some("10"));

            let mut row_client = connect_to(&manager);
            row_client.handshake_with("reader", "user:reader").await;
            row_client.subscribe("rows", "SELECT * FROM counts").await;
            row_client.expect_snapshot("rows").await;

            // Drop column n from Postgres; the next connector re-execution of
            // SELECT MIN(n) FROM counts fails with 42703, arriving as
            // `ReadFailure::Deterministic`.
            fixture.exec("ALTER TABLE counts DROP COLUMN n").await;

            let mut source = PgSqliteEmuSource::open_in_memory(DDL).expect("open emu source");
            // Stage the extreme row in the emulator so DELETE can reference it,
            // then discard that CDC event without dispatching it.
            source
                .execute_sql("INSERT INTO counts (id, n) VALUES (10, 10)")
                .expect("seed emu");
            source
                .next_item()
                .await
                .expect("drain emu")
                .expect("insert event");

            // Deleting the tracked extreme forces a re-execution, which now
            // fails because column n is gone from Postgres.
            source
                .execute_sql("DELETE FROM counts WHERE id = 10")
                .expect("execute dml");
            let at = fixture.commit_now().await;
            while let Some(item) = source.next_item().await.expect("poll source") {
                let SourceItem::Event(event) = item else {
                    continue;
                };
                manager
                    .dispatch_event(&committed_at(event, at))
                    .await
                    .unwrap_or_else(|err| panic!("dispatch failed: {err:?}"));
            }

            let ControlMessage::NonFatalError(refusal) = computed_client.next_control().await
            else {
                panic!("expected refusal of the poisoned subscription");
            };
            assert_eq!(refusal.related_to.as_deref(), Some("cheapest"));
            assert_eq!(refusal.detail, SUBSCRIPTION_REFUSED);

            let log_lines = logs.lines();
            let refused_log = log_lines.iter().any(|line| {
                line["message"]
                    == "a computed subscription's read was refused, ending the subscription"
                    && line["sub_id"] == "cheapest"
                    && line["class"] == "deterministic"
                    && line["attempts"].as_u64() == Some(1)
            });
            assert!(
                refused_log,
                "log must record sub_id=cheapest class=deterministic attempts=1: {log_lines:?}"
            );

            let live = row_client.wait_for_live(Duration::from_secs(5)).await;
            assert_eq!(live.sub_id, "rows");
        },
    )
    .await;
}

/// A transient connector failure pauses delivery in place and resumes once the
/// database is reachable again, without ending the subscription or touching
/// the change stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transient_read_pause_resumes_delivery_without_ending_anything() {
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS counts CASCADE").await;
    fixture
        .exec("CREATE TABLE counts (id INT PRIMARY KEY, n INT)")
        .await;
    fixture
        .exec("INSERT INTO counts (id, n) VALUES (10, 10)")
        .await;

    let tiny_pool = Pool::builder()
        .max_size(1)
        // Opened at build, so the 150 ms checkout bounds only the outage and never the first connect.
        .min_idle(1)
        .connection_timeout(Duration::from_millis(150))
        .build(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            fixture.admin_url().to_owned(),
        ))
        .await
        .expect("build tiny pool");

    let manager = full_manager(
        tiny_pool.clone(),
        DDL,
        DDL,
        RosterAuth::granting_nobody(),
        guard(Duration::from_secs(30), Duration::from_secs(30)),
        fixture.admin().clone(),
        fixture.admin().clone(),
    );

    let mut client = connect_to(&manager);
    client.handshake_with("watcher", "user:watcher").await;
    client
        .subscribe("smallest", "SELECT MIN(n) FROM counts")
        .await;
    let ControlMessage::AggregateUpdate(seeded) = client.next_control().await else {
        panic!("expected aggregate seed");
    };
    assert_eq!(seeded.result_json.as_deref(), Some("10"));

    // Process INSERT before holding the pool so the connector can re-execute.
    let mut source = PgSqliteEmuSource::open_in_memory(DDL).expect("open emu source");
    source
        .execute_sql("INSERT INTO counts (id, n) VALUES (1, 1)")
        .expect("stage insert");
    let at = fixture.commit_now().await;
    while let Some(item) = source.next_item().await.expect("poll source") {
        let SourceItem::Event(event) = item else {
            continue;
        };
        manager
            .dispatch_event(&committed_at(event, at))
            .await
            .expect("dispatch insert");
    }
    let ControlMessage::AggregateUpdate(_) = client.next_control().await else {
        panic!("expected AggregateUpdate from INSERT");
    };

    // Hold the pool's only connection; the DELETE re-execution below times out.
    let held = tiny_pool.get().await.expect("hold the only connection");

    // Stage just the DELETE of the extreme so the next re-execution needs the pool.
    source
        .execute_sql("DELETE FROM counts WHERE id = 1")
        .expect("stage delete");

    let ingest = WatchedIngest::spawn(&manager, source);
    ingest
        .assert_paused_in_place(&mut client, Duration::from_secs(3))
        .await;
    drop(held);
    ingest.assert_resumed_with(&mut client, "10").await;
}

/// The catalog shape of the gated view below, as the materializer and the emulator see it.
const GATED_DDL: &str = "CREATE TABLE gated (id INT PRIMARY KEY, n INT)";

/// A listed code raised by Postgres itself pauses delivery and resumes it once
/// the database stops raising it, ending nothing (R106 decision 1).
///
/// `gated` is a view over `gated_base` whose rows pass through a function that
/// raises `40P01` (`deadlock_detected`) while `gated_fail` holds a row, so the
/// re-execution fails with exactly that code and nothing else differs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listed_code_raised_for_real_pauses_and_resumes_delivery() {
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP VIEW IF EXISTS gated").await;
    fixture.exec("DROP FUNCTION IF EXISTS gated_open()").await;
    fixture
        .exec("DROP TABLE IF EXISTS gated_base, gated_fail CASCADE")
        .await;
    fixture
        .exec("CREATE TABLE gated_base (id INT PRIMARY KEY, n INT)")
        .await;
    fixture.exec("CREATE TABLE gated_fail (on_ INT)").await;
    fixture
        .exec(
            "CREATE FUNCTION gated_open() RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$ \
             BEGIN \
               IF EXISTS (SELECT 1 FROM gated_fail) THEN \
                 RAISE EXCEPTION 'a deadlock stand-in' USING ERRCODE = '40P01'; \
               END IF; \
               RETURN true; \
             END $$",
        )
        .await;
    fixture
        .exec("CREATE VIEW gated AS SELECT id, n FROM gated_base WHERE gated_open()")
        .await;
    fixture
        .exec("INSERT INTO gated_base (id, n) VALUES (10, 10)")
        .await;

    let pool = fixture.admin().clone();
    let manager = full_manager(
        pool.clone(),
        GATED_DDL,
        GATED_DDL,
        RosterAuth::granting_nobody(),
        guard(Duration::from_secs(30), Duration::from_secs(30)),
        pool.clone(),
        pool,
    );

    let mut client = connect_to(&manager);
    client.handshake_with("watcher", "user:watcher").await;
    client
        .subscribe("smallest", "SELECT MIN(n) FROM gated")
        .await;
    let ControlMessage::AggregateUpdate(seeded) = client.next_control().await else {
        panic!("expected aggregate seed");
    };
    assert_eq!(seeded.result_json.as_deref(), Some("10"));

    // The emulator builds the DELETE of the seeded extreme, which forces a
    // re-execution, and the event is stamped where Postgres can place it.
    let mut emu = PgSqliteEmuSource::open_in_memory(GATED_DDL).expect("open emu source");
    emu.execute_sql("INSERT INTO gated (id, n) VALUES (10, 10)")
        .expect("seed emu");
    emu.next_item()
        .await
        .expect("drain emu")
        .expect("insert event");
    emu.execute_sql("DELETE FROM gated WHERE id = 10")
        .expect("stage delete");
    let mut delete = None;
    while let Some(item) = emu.next_item().await.expect("poll emu") {
        if let SourceItem::Event(event) = item {
            delete = Some(event);
        }
    }
    let delete = delete.expect("the delete event");

    fixture
        .exec("INSERT INTO gated_fail (on_) VALUES (1)")
        .await;
    let source = OneEvent(Some(committed_at(delete, fixture.commit_now().await)));

    let ingest = WatchedIngest::spawn(&manager, source);
    ingest
        .assert_paused_in_place(&mut client, Duration::from_secs(5))
        .await;
    fixture.exec("DELETE FROM gated_fail").await;
    // Postgres still holds the row, so the re-execution answers the same extreme.
    ingest.assert_resumed_with(&mut client, "10").await;
}
