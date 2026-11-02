//! R82: grouped delivery over the wire, end to end.
//!
//! A grouped fold's seeds and deltas reach the client as keyed upserts, a
//! group emptied by a change arrives as a keyed removal, and a subscription
//! whose groups outgrow the fold budget demotes to whole re-reads with the
//! transition in the log and nothing on the wire beyond the answers.
//!
//! Needs Docker: the fixture starts its own Postgres.

use std::sync::Arc;
use std::time::Duration;

use connetto_core::messages::{BulkMessage, ControlMessage, FatalErrorReason};
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_server::{
    AbuseConfig, LoopbackError, LoopbackTransport, ManagerBuilder, Materializer, PgReadConnector,
    PgSnapshotSource, RequestGuard, RuntimeWritableCatalog, SessionConfig, SessionManager,
    ThrottleConfig, TierLimits, loopback, pg_write_target,
};
use connetto_test_harness::{Client, ConnettoDefaults, Fixture, RosterAuth, committed_at};
use subql::{CdcSource, PgSqliteEmuSource, SourceItem};

const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);";

/// A manager whose computed subscriptions read through connetto's connector.
type Manager = SessionManager<PgSnapshotSource, RosterAuth, ConnettoDefaults, PgReadConnector>;

fn manager(fixture: &Fixture) -> Arc<Manager> {
    manager_with_session(fixture, SessionConfig::default())
}

fn manager_with_session(fixture: &Fixture, session: SessionConfig) -> Arc<Manager> {
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default()
            .with_identified(TierLimits::identified().with_read_timeout(Duration::from_secs(30))),
        AbuseConfig::default(),
    ));
    let pool = fixture.admin().clone();
    ManagerBuilder::new(
        Materializer::builder(PG_DDL)
            .with_write_catalog(RuntimeWritableCatalog::default())
            .with_read_connector(PgReadConnector::with_session_setup(pool.clone()))
            .build()
            .expect("build materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), PG_DDL).expect("snapshot source"),
        // A grouped count is global, so the policy never sees it.
        RosterAuth::granting_nobody(),
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(pool.clone()),
        pg_write_target::<ConnettoDefaults>(pool, PG_DDL).expect("build write target"),
    )
    .with_guard(guard)
    .with_session(session)
    .build()
}

/// One in-process client on its own session.
fn connect(manager: &Arc<Manager>) -> Client {
    super::loopback_session::connect(|end| Arc::clone(manager).serve(end))
}

/// Pump the client for exactly `n` aggregate frames on `sub_id`.
async fn take_aggregates(
    client: &mut Client,
    sub_id: &str,
    n: usize,
) -> Vec<connetto_core::messages::AggregateUpdate> {
    let mut frames = Vec::with_capacity(n);
    while frames.len() < n {
        let ControlMessage::AggregateUpdate(update) = client.next_control().await else {
            panic!("expected an aggregate frame, {} of {n} seen", frames.len());
        };
        assert_eq!(update.sub_id, sub_id);
        frames.push(update);
    }
    frames
}

/// Run `sql` on the emulated source and dispatch every event it produced.
async fn apply(
    fixture: &Fixture,
    source: &mut PgSqliteEmuSource,
    manager: &Arc<Manager>,
    sql: &str,
) {
    source.execute_sql(sql).expect("execute dml");
    let at = fixture.commit_now().await;
    while let Some(item) = source.next_item().await.expect("poll source") {
        let SourceItem::Event(event) = item else {
            continue;
        };
        manager
            .dispatch_event(&committed_at(event, at))
            .await
            .expect("dispatch");
    }
}

/// The R82 done-when, first half: seeds and deltas arrive keyed, a group born
/// on the change stream arrives as a new keyed upsert, and emptying it again
/// arrives as a keyed removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grouped_subscription_delivers_per_group_deltas_with_the_key_populated() {
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS orders CASCADE").await;
    fixture
        .exec("CREATE TABLE orders (id INT PRIMARY KEY, status TEXT)")
        .await;
    fixture
        .exec("INSERT INTO orders VALUES (1, 'open'), (2, 'open')")
        .await;
    let manager = manager(&fixture);

    let mut client = connect(&manager);
    client.handshake_with("counter", "user:counter").await;
    client
        .subscribe(
            "by-status",
            "SELECT status, COUNT(*) FROM orders GROUP BY status",
        )
        .await;

    // The one seeded group arrives as a keyed upsert.
    let seed = take_aggregates(&mut client, "by-status", 1).await.remove(0);
    assert!(!seed.is_full_result, "a grouped seed is a keyed upsert");
    let open_key = seed.group_key.expect("a grouped seed carries its key");
    assert_eq!(seed.result_json.as_deref(), Some("2"));

    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    apply(
        &fixture,
        &mut source,
        &manager,
        "INSERT INTO orders VALUES (4, 'open')",
    )
    .await;
    let grew = take_aggregates(&mut client, "by-status", 1).await.remove(0);
    assert_eq!(
        grew.group_key.as_deref(),
        Some(open_key.as_slice()),
        "the delta addresses its group by the seed's own key"
    );
    assert_eq!(grew.result_json.as_deref(), Some("3"));
    assert!(!grew.is_full_result, "a grouped delta is an upsert");

    // A group born on the change stream appears under a new key, and
    // emptying it again is a keyed removal.
    apply(
        &fixture,
        &mut source,
        &manager,
        "INSERT INTO orders VALUES (100, 'done')",
    )
    .await;
    let born = take_aggregates(&mut client, "by-status", 1).await.remove(0);
    let done_key = born.group_key.clone().expect("a new group carries its key");
    assert_ne!(done_key.as_slice(), open_key.as_slice());
    assert_eq!(born.result_json.as_deref(), Some("1"));
    assert!(!born.is_full_result);
    apply(
        &fixture,
        &mut source,
        &manager,
        "DELETE FROM orders WHERE id = 100",
    )
    .await;
    let removed = take_aggregates(&mut client, "by-status", 1).await.remove(0);
    assert_eq!(
        removed.group_key.as_deref(),
        Some(done_key.as_slice()),
        "the removal addresses the emptied group"
    );
    assert_eq!(removed.result_json, None, "an emptied group is a removal");
    assert!(!removed.is_full_result);
}

/// The R82 done-when, second half: a seed past the group budget demotes at
/// install, the client keeps answering with full results (one row-shaped
/// frame carrying every group, produced by the whole re-read the demotion
/// asked for), and the log carries the transition the client is never told
/// about.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_demoted_subscription_answers_whole_and_logs_the_transition() {
    crate::logging::with_capture("a_demoted_subscription_answers_whole_and_logs_the_transition", |logs| async move {
            let fixture = Fixture::acquire().await;
            fixture.exec("DROP TABLE IF EXISTS orders CASCADE").await;
            fixture
                .exec("CREATE TABLE orders (id INT PRIMARY KEY, status TEXT)")
                .await;
            // One row per group, past the fold budget of 1024 groups.
            fixture
                .exec("INSERT INTO orders SELECT g, 'status-' || g FROM generate_series(1, 1025) AS g")
                .await;
            let manager = manager(&fixture);

            let mut client = connect(&manager);
            client.handshake_with("wide", "user:wide").await;
            client
                .subscribe(
                    "by-status",
                    "SELECT status, COUNT(*) FROM orders GROUP BY status",
                )
                .await;

            // The demoted first answer is one whole row-shaped frame, not keyed
            // upserts: the fold is gone, the whole re-read is the tier now.
            let whole = take_aggregates(&mut client, "by-status", 1).await.remove(0);
            assert!(
                whole.is_full_result,
                "a demoted subscription answers with full results"
            );
            assert_eq!(whole.group_key, None, "a whole answer addresses no group");
            let rows: serde_json::Value = serde_json::from_str(
                whole
                    .result_json
                    .as_deref()
                    .expect("a whole answer has a body"),
            )
            .expect("the whole answer is JSON rows");
            assert_eq!(
                rows.as_array().map(Vec::len),
                Some(1025),
                "every group is in the whole answer"
            );

            let named = logs
                .lines()
                .into_iter()
                .any(|line| line["message"] == "subscription changed maintenance tier");
            assert!(named, "the log names the transition");
    })
    .await;
}

/// A server transport whose sends wait while its gate is shut, standing in for a client that stopped reading its socket.
struct Gated {
    inner: LoopbackTransport,
    open: tokio::sync::watch::Receiver<bool>,
}

impl Gated {
    async fn wait_open(&mut self) {
        let _ = self.open.wait_for(|open| *open).await;
    }
}

impl Transport for Gated {
    type Error = LoopbackError;

    async fn send_control(&mut self, message: ControlMessage) -> Result<(), Self::Error> {
        self.wait_open().await;
        self.inner.send_control(message).await
    }

    async fn send_bulk(&mut self, message: BulkMessage) -> Result<(), Self::Error> {
        self.wait_open().await;
        self.inner.send_bulk(message).await
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, Self::Error> {
        self.inner.recv().await
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}

/// A client subscribed only to a computed query that stops reading its socket is closed at its send budget, so the aggregate updates queued for it stay bounded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_reader_of_aggregates_is_closed_at_its_send_budget() {
    const CHANGES: i64 = 40;
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS orders CASCADE").await;
    fixture
        .exec("CREATE TABLE orders (id INT PRIMARY KEY, status TEXT)")
        .await;
    fixture.exec("INSERT INTO orders VALUES (1, 'open')").await;
    // Room for a handful of aggregate updates, far fewer than the changes below.
    let manager = manager_with_session(&fixture, SessionConfig::new().with_send_queue_bytes(1_000));

    let (server_end, client_end) = loopback();
    let (gate, open) = tokio::sync::watch::channel(true);
    let session = Arc::clone(&manager);
    let served = tokio::spawn(async move {
        session
            .serve(Gated {
                inner: server_end,
                open,
            })
            .await
    });
    let mut client = Client::new(client_end);
    client.handshake_with("stalled", "user:stalled").await;
    client
        .subscribe(
            "by-status",
            "SELECT status, COUNT(*) FROM orders GROUP BY status",
        )
        .await;
    take_aggregates(&mut client, "by-status", 1).await;

    gate.send(false).expect("shut the gate");
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    for id in 2..2 + CHANGES {
        apply(
            &fixture,
            &mut source,
            &manager,
            &format!("INSERT INTO orders VALUES ({id}, 'open')"),
        )
        .await;
    }
    gate.send(true).expect("open the gate");

    let mut delivered = 0;
    let reason = loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), client.next_control())
            .await
            .expect("the connection closes once its queued updates pass the budget");
        match frame {
            ControlMessage::AggregateUpdate(_) => delivered += 1,
            ControlMessage::FatalError(fatal) => break fatal.reason,
            other => panic!("unexpected frame {other:?}"),
        }
    };
    assert_eq!(reason, FatalErrorReason::SendQueueExceeded);
    assert!(
        delivered < CHANGES,
        "the budget held back updates, {delivered} of {CHANGES} delivered"
    );
    let _ = served.await;
}
