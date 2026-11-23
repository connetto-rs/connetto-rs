//! Phase 4b acceptance test: re-execution and aggregate delivery.
//!
//! Docker-free: a MIN aggregate subscription is bootstrapped through a fake
//! connector (canned scalar responses), an in-process fold delivers a lower
//! value without touching the connector, and deleting the current extreme
//! forces a re-execution that consults the connector. Each step asserts the
//! `AggregateUpdate` the client receives. The real-connector bootstrap is
//! covered by the Docker-gated test in `pg_async.rs`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use connetto_core::PROTOCOL_VERSION;
use connetto_core::messages::{
    AggregateChange, ControlMessage, Handshake, SqliteValue, Subscribe, SubscriptionSpec,
};
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_server::{
    ConnettoReadSetup, ManagerBuilder, Materializer, PageSpec, RuntimeWritableCatalog,
    SessionManager, SnapshotEstimate, SnapshotPage, SnapshotSource, loopback, pg_write_target,
};
use connetto_test_harness::{ConnettoDefaults, Fixture, RosterAuth, WITHHELD_ID};
use subql::backend::{Postgres, ScalarFamily, Value as PgValue};
use subql::reexec::{AsyncConnector, FailureClass, ReadQuery, RowPage, Snapshot as ConnectorRead};
use subql::{CdcSource, PgCommitPosition, PgLsn, PgSnapshotFence, PgSqliteEmuSource, SourceItem};

const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);";

/// A connector that answers `execute_scalar` from a queue of canned integers, each with the fence of the moment it stands for.
/// Backed by an `Arc` so it can be cloned for the materializer and the session.
#[derive(Clone)]
struct QueuedConnector {
    responses: Arc<Mutex<VecDeque<Canned<i64>>>>,
}

/// A canned answer and the fence of the moment it stands for.
type Canned<V> = (V, Option<PgSnapshotFence>);

/// A read taken before every emulated change, whose commits all start past WAL position 1.
fn before_every_change() -> Option<PgSnapshotFence> {
    PgSnapshotFence::parse("1:1:", PgLsn(1))
}

/// A read taken after every emulated change: it sees the emulator's small transaction ids and ends past its positions.
fn after_every_change() -> Option<PgSnapshotFence> {
    PgSnapshotFence::parse("1000000:1000000:", PgLsn(1 << 40))
}

impl QueuedConnector {
    fn new(responses: impl IntoIterator<Item = (i64, Option<PgSnapshotFence>)>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        }
    }
}

#[expect(
    clippy::manual_async_fn,
    reason = "the trait names the future type so the Send bound is stated on the signature"
)]
impl AsyncConnector for QueuedConnector {
    type AuthContext = ConnettoReadSetup;
    type Error = std::io::Error;
    type Checkpoint = PgCommitPosition;
    type Backend = Postgres;

    fn failure_class(&self, _error: &std::io::Error) -> FailureClass {
        FailureClass::Transient
    }

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _setup: &ConnettoReadSetup,
    ) -> impl core::future::Future<
        Output = Result<(PgValue<Postgres>, Option<PgSnapshotFence>), std::io::Error>,
    > + Send {
        let next = self.responses.lock().expect("queue poisoned").pop_front();
        async move {
            next.map(|(n, fence)| (PgValue::Int(n), fence))
                .ok_or_else(|| std::io::Error::other("no more canned responses"))
        }
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _setup: &ConnettoReadSetup,
    ) -> impl core::future::Future<
        Output = Result<ConnectorRead<RowPage<Postgres>, PgCommitPosition>, std::io::Error>,
    > + Send {
        async { Err(std::io::Error::other("read_page not used in reexec tests")) }
    }
}

/// Aggregate subscriptions never snapshot, so this is never invoked.
struct NoSnapshot;

impl SnapshotSource for NoSnapshot {
    type Error = std::convert::Infallible;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn estimate(
        &self,
        _select_sql: &str,
        _binds: &[connetto_core::messages::SqliteValue],
        _caller: &connetto_core::Principal,
    ) -> Result<SnapshotEstimate, Self::Error> {
        Ok(SnapshotEstimate {
            rows: 0.0,
            width: 0,
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn snapshot_page(
        &self,
        _select_sql: &str,
        _binds: &[connetto_core::messages::SqliteValue],
        _auth: &connetto_core::Principal,
        _page: &PageSpec,
    ) -> Result<SnapshotPage, Self::Error> {
        Ok(SnapshotPage {
            patchset: Vec::new(),
            cursor: connetto_core::Cursor::new(Vec::new()),
            next: None,
            filled: false,
            widest_row: 0,
            rows: 0,
            bytes: 0,
        })
    }
}

type Manager = SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults, QueuedConnector>;

async fn next_control<T: Transport>(transport: &mut T) -> ControlMessage {
    match transport.recv().await.expect("recv frame") {
        Some(IncomingFrame::Control(msg)) => msg,
        other => panic!("expected control frame, got {other:?}"),
    }
}

pub(super) fn integer(update: &connetto_core::messages::AggregateUpdate) -> i64 {
    let AggregateChange::Replace { rows } = &update.change else {
        panic!("expected whole answer");
    };
    let [row] = rows.as_slice() else {
        panic!("expected one row")
    };
    let [SqliteValue::Integer(value)] = row.as_slice() else {
        panic!("expected integer")
    };
    *value
}

fn aggregate_value(msg: ControlMessage) -> i64 {
    match msg {
        ControlMessage::AggregateUpdate(update) => {
            assert_eq!(update.sub_id, "cheapest");
            integer(&update)
        }
        other => panic!("expected aggregate update, got {other:?}"),
    }
}

async fn drive(source: &mut PgSqliteEmuSource, manager: &Manager, sql: &str) {
    source.execute_sql(sql).expect("execute dml");
    while let Some(item) = source.next_item().await.expect("poll source") {
        let SourceItem::Event(event) = item else {
            continue;
        };
        manager
            .dispatch_event(&event)
            .await
            .expect("dispatch event");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reexec_bootstraps_folds_and_retriggers() {
    let fixture = Fixture::acquire().await;
    // Bootstrap answers 10 before any change, the re-execution after the delete answers 20 and holds both changes.
    let connector = QueuedConnector::new([(10, before_every_change()), (20, after_every_change())]);
    let materializer = Materializer::builder(PG_DDL)
        .with_write_catalog(RuntimeWritableCatalog::default())
        .with_read_connector(connector.clone())
        .build()
        .expect("build materializer");
    let target = pg_write_target::<ConnettoDefaults>(fixture.admin().clone(), PG_DDL)
        .expect("build write target");
    let manager = ManagerBuilder::new(
        materializer,
        NoSnapshot,
        // Aggregate results never go through the policy.
        RosterAuth::granting_nobody().withholding(WITHHELD_ID),
        Arc::new(TestGrantChecker),
        connector,
        target,
    )
    .build();

    let (server_transport, mut client) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));

    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, "aggregator")
                .with_grant(connetto_core::messages::Grant::new("user:aggregator")),
        ))
        .await
        .expect("send handshake");
    // The server leads every connection with a challenge a person ignores (R91).
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    let ControlMessage::HandshakeAck(_) = next_control(&mut client).await else {
        panic!("expected handshake ack");
    };

    // Subscribe to MIN(amount): bootstrap runs the query through the connector.
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "cheapest".to_owned(),
            spec: SubscriptionSpec::new("SELECT MIN(amount) FROM orders"),
        }))
        .await
        .expect("send subscribe");
    let ControlMessage::AggregateColumns(columns) = next_control(&mut client).await else {
        panic!("expected columns before first answer");
    };
    assert_eq!(columns.sub_id, "cheapest");
    assert_eq!(columns.columns.len(), 1);
    assert_eq!(aggregate_value(next_control(&mut client).await), 10);

    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");

    // A lower value folds in-process: the connector is not consulted.
    drive(
        &mut source,
        &manager,
        "INSERT INTO orders (id, amount) VALUES (1, 5)",
    )
    .await;
    assert_eq!(aggregate_value(next_control(&mut client).await), 5);

    // Deleting the current extreme forces a re-execution through the connector.
    drive(&mut source, &manager, "DELETE FROM orders WHERE id = 1").await;
    assert_eq!(aggregate_value(next_control(&mut client).await), 20);

    client.close().await.expect("close client");
    server.await.expect("join server").expect("session ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maximum_timestamptz_uses_replica_text() {
    const DDL: &str = "CREATE TABLE aggregate_stamps (id INT PRIMARY KEY, observed TIMESTAMPTZ);";
    const SEED: &str = "INSERT INTO aggregate_stamps VALUES (1, '2026-09-23 15:42:07.957522+02'), (2, '2026-09-22 13:42:07.957522+00')";
    // The same rows in the text the replica stores an instant as.
    const REPLICA_SEED: &str = "INSERT INTO aggregate_stamps VALUES (1, '2026-09-23 13:42:07.957522+00:00'), (2, '2026-09-22 13:42:07.957522+00:00')";
    let fixture = Fixture::acquire().await;
    fixture
        .exec("DROP TABLE IF EXISTS aggregate_stamps CASCADE")
        .await;
    fixture.exec(DDL).await;
    fixture.exec(SEED).await;
    // Two connectors over one pool, one for the engine and one for the
    // session, reading through the same connections.
    let connector = connetto_server::PgReadConnector::with_session_setup(fixture.admin().clone());
    let materializer = Materializer::builder(DDL)
        .with_write_catalog(RuntimeWritableCatalog::default())
        .with_read_connector(connetto_server::PgReadConnector::with_session_setup(
            fixture.admin().clone(),
        ))
        .build()
        .expect("materializer");
    let manager = ManagerBuilder::new(
        materializer,
        NoSnapshot,
        RosterAuth::granting_nobody(),
        Arc::new(TestGrantChecker),
        connector,
        pg_write_target::<ConnettoDefaults>(fixture.admin().clone(), DDL).expect("write target"),
    )
    .build();
    let mut client =
        super::loopback_session::connect(|transport| Arc::clone(&manager).serve(transport));
    client.handshake_with("stamps", "user:stamps").await;
    client
        .subscribe("latest", "SELECT MAX(observed) FROM aggregate_stamps")
        .await;
    let columns = client.expect_aggregate_columns("latest").await;
    assert_eq!(columns.columns.len(), 1);
    let ControlMessage::AggregateUpdate(answer) = client.next_control().await else {
        panic!("expected timestamp answer");
    };
    assert_eq!(answer.sub_id, "latest");
    assert_eq!(
        answer.change,
        AggregateChange::Replace {
            rows: vec![vec![SqliteValue::Text(
                "2026-09-23 13:42:07.957522+00:00".to_owned()
            )]],
        }
    );
    let mut source = PgSqliteEmuSource::open_in_memory(DDL).expect("source");
    source.execute_sql(REPLICA_SEED).expect("seed source");
    source.drain().expect("drain source");
    fixture
        .exec("DELETE FROM aggregate_stamps WHERE id = 1")
        .await;
    source
        .execute_sql("DELETE FROM aggregate_stamps WHERE id = 1")
        .expect("delete extreme");
    let committed = fixture.commit_now().await;
    while let Some(item) = source.next_item().await.expect("source item") {
        let SourceItem::Event(event) = item else {
            continue;
        };
        manager
            .dispatch_event(&connetto_test_harness::committed_at(event, committed))
            .await
            .expect("dispatch");
    }
    let ControlMessage::AggregateUpdate(answer) = client.next_control().await else {
        panic!("expected reexecuted timestamp");
    };
    assert_eq!(answer.sub_id, "latest");
    assert_eq!(
        answer.change,
        AggregateChange::Replace {
            rows: vec![vec![SqliteValue::Text(
                "2026-09-22 13:42:07.957522+00:00".to_owned()
            )]],
        }
    );
}
