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

use connetto_core::messages::{
    AggregateChange, BulkMessage, ControlMessage, FatalErrorReason, SqliteValue,
};
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
    manager_with_ddl(fixture, session, PG_DDL)
}

fn manager_with_ddl(fixture: &Fixture, session: SessionConfig, ddl: &str) -> Arc<Manager> {
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default()
            .with_identified(TierLimits::identified().with_read_timeout(Duration::from_secs(30))),
        AbuseConfig::default(),
    ));
    let pool = fixture.admin().clone();
    ManagerBuilder::new(
        Materializer::builder(ddl)
            .with_write_catalog(RuntimeWritableCatalog::default())
            .with_read_connector(PgReadConnector::with_session_setup(pool.clone()))
            .build()
            .expect("build materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), ddl).expect("snapshot source"),
        // A grouped count is global, so the policy never sees it.
        RosterAuth::granting_nobody(),
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(pool.clone()),
        pg_write_target::<ConnettoDefaults>(pool, ddl).expect("build write target"),
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

    let columns = client.expect_aggregate_columns("by-status").await;
    assert_eq!(columns.columns.len(), 2);
    // The one seeded group arrives as a keyed upsert.
    let seed = take_aggregates(&mut client, "by-status", 1).await.remove(0);
    let AggregateChange::Upsert { key: open_key, row } = seed.change else {
        panic!("expected keyed seed");
    };
    assert_eq!(
        row,
        vec![
            SqliteValue::Text("open".to_owned()),
            SqliteValue::Integer(2)
        ]
    );

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
        grew.change,
        AggregateChange::Upsert {
            key: open_key.clone(),
            row: vec![
                SqliteValue::Text("open".to_owned()),
                SqliteValue::Integer(3)
            ],
        }
    );

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
    let AggregateChange::Upsert { key: done_key, row } = born.change else {
        panic!("expected keyed birth");
    };
    assert_ne!(done_key, open_key);
    assert_eq!(
        row,
        vec![
            SqliteValue::Text("done".to_owned()),
            SqliteValue::Integer(1)
        ]
    );
    apply(
        &fixture,
        &mut source,
        &manager,
        "DELETE FROM orders WHERE id = 100",
    )
    .await;
    let removed = take_aggregates(&mut client, "by-status", 1).await.remove(0);
    assert_eq!(removed.change, AggregateChange::Remove { key: done_key });
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

            let columns = client.expect_aggregate_columns("by-status").await;
            assert_eq!(columns.columns.len(), 2);
            // The demoted first answer is one whole row-shaped frame, not keyed
            // upserts: the fold is gone, the whole re-read is the tier now.
            let whole = take_aggregates(&mut client, "by-status", 1).await.remove(0);
            let AggregateChange::Replace { rows } = whole.change else {
                panic!("expected whole answer");
            };
            assert_eq!(rows.len(), 1025, "every group is in the whole answer");
            assert!(rows.iter().all(|row| matches!(row.as_slice(), [SqliteValue::Text(_), SqliteValue::Integer(1)])));

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
    client.expect_aggregate_columns("by-status").await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_groups_keep_their_cells_and_distinct_keys_on_the_wire() {
    for (column_type, values, expected) in [
        (
            "UUID",
            "'00010203-0405-0607-0809-0a0b0c0d0e0f'",
            vec![(0_u8..16).collect::<Vec<_>>()],
        ),
        (
            "BYTEA",
            "'\\x80'), (2, '\\x81'",
            vec![vec![0x80], vec![0x81]],
        ),
    ] {
        let fixture = Fixture::acquire().await;
        fixture.exec("DROP TABLE IF EXISTS orders CASCADE").await;
        let ddl = format!("CREATE TABLE orders (id INT PRIMARY KEY, category {column_type});");
        fixture.exec(&ddl).await;
        fixture
            .exec(&format!("INSERT INTO orders VALUES (1, {values})"))
            .await;
        let manager = manager_with_ddl(&fixture, SessionConfig::default(), &ddl);
        let mut client = connect(&manager);
        client.handshake_with("binary", "user:binary").await;
        client
            .subscribe(
                "groups",
                "SELECT category, COUNT(*) FROM orders GROUP BY category",
            )
            .await;
        let columns = client.expect_aggregate_columns("groups").await;
        assert_eq!(columns.columns.len(), 2);
        assert_eq!(
            columns.columns[0],
            connetto_core::messages::ColumnType::Blob
        );
        let answers = take_aggregates(&mut client, "groups", expected.len()).await;
        let mut keys = Vec::new();
        let mut actual = Vec::new();
        for answer in answers {
            let AggregateChange::Upsert { key, row } = answer.change else {
                panic!("expected keyed binary group");
            };
            assert!(!keys.contains(&key));
            keys.push(key);
            let [SqliteValue::Blob(bytes), SqliteValue::Integer(1)] = row.as_slice() else {
                panic!("expected binary group and count");
            };
            actual.push(bytes.clone());
        }
        actual.sort();
        assert_eq!(actual, expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_grouped_answer_declares_columns_before_a_group_is_born() {
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS orders CASCADE").await;
    fixture.exec(PG_DDL).await;
    let manager = manager(&fixture);
    let mut client = connect(&manager);
    client.handshake_with("empty", "user:empty").await;
    client
        .subscribe(
            "groups",
            "SELECT status, COUNT(*) FROM orders GROUP BY status",
        )
        .await;
    let columns = client.expect_aggregate_columns("groups").await;
    assert_eq!(columns.columns.len(), 2);
    assert_eq!(
        columns.columns[0],
        connetto_core::messages::ColumnType::Text
    );
    assert!(matches!(client.barrier(41).await, ControlMessage::Pong(pong) if pong.nonce == 41));
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("source");
    apply(
        &fixture,
        &mut source,
        &manager,
        "INSERT INTO orders VALUES (1, 'new')",
    )
    .await;
    let update = take_aggregates(&mut client, "groups", 1).await.remove(0);
    let AggregateChange::Upsert { row, .. } = update.change else {
        panic!("expected new group");
    };
    assert_eq!(
        row,
        vec![SqliteValue::Text("new".to_owned()), SqliteValue::Integer(1)]
    );
}
