//! Oplog append failure tests for the kept-query-core dispatch path.
//!
//! An event the engine has applied but the oplog refused rides the manager's
//! held progress across the retry. These drive `ingest_with_reconnect` with a
//! scripted source that replays what a slot never acknowledged past, and an
//! oplog whose first append fails, as a cut log connection does.
//!
//! The fixture starts its own Postgres, so these need Docker.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use connetto_core::messages::ControlMessage;
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::IncomingFrame;
use connetto_server::{
    AbuseConfig, ChangeRecord, Commits, InMemoryOplog, ManagerBuilder, Materializer, Oplog,
    PgReadConnector, PgSnapshotSource, ReconnectPolicy, RequestGuard, ResumePoint,
    RuntimeWritableCatalog, SessionManager, ThrottleConfig, TierLimits, Unseen, UnseenIds,
    pg_write_target,
};
use connetto_test_harness::{Client, ConnettoDefaults, Fixture, RosterAuth, committed_at};
use diesel::prelude::*;
use diesel::sql_query;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::Pool;
use subql::{
    CdcSource, PgChangeEvent, PgCommit, PgCommitPosition, PgLsn, PgSqliteEmuSource, PgXid,
    SourceItem,
};

const DDL: &str = "CREATE TABLE counts (id INT PRIMARY KEY, n INT)";
const SQLITE_DDL: &str = "CREATE TABLE counts (id INTEGER PRIMARY KEY, n INTEGER);";

/// The position before every row the log can hold, for a from-the-origin read.
const fn origin() -> PgCommitPosition {
    PgCommitPosition::new(PgLsn(0), PgXid(0), 0)
}

type Manager =
    SessionManager<PgSnapshotSource, RosterAuth, ConnettoDefaults, PgReadConnector, FlakyAppend>;

fn connect_to(manager: &Arc<Manager>) -> Client {
    super::loopback_session::connect(|end| Arc::clone(manager).serve(end))
}

fn guard(seed: Duration, triggered: Duration) -> Arc<RequestGuard<String>> {
    Arc::new(RequestGuard::new(
        ThrottleConfig::default()
            .with_identified(TierLimits::identified().with_read_timeout(seed))
            .with_reexec_timeout(triggered),
        AbuseConfig::default(),
    ))
}

fn manager_with_log(
    log: FlakyAppend,
    connector_pool: Pool<AsyncPgConnection>,
    guard: Arc<RequestGuard<String>>,
    snapshot_pool: Pool<AsyncPgConnection>,
    write_pool: Pool<AsyncPgConnection>,
) -> Arc<Manager> {
    ManagerBuilder::new(
        Materializer::builder(DDL)
            .with_write_catalog(RuntimeWritableCatalog::default())
            .with_read_connector(PgReadConnector::with_session_setup(connector_pool.clone()))
            .build()
            .expect("build materializer"),
        PgSnapshotSource::from_ddl(snapshot_pool, DDL).expect("snapshot source"),
        RosterAuth::granting("reader").and("watcher"),
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(connector_pool),
        pg_write_target::<ConnettoDefaults>(write_pool, DDL).expect("build write target"),
    )
    .with_guard(guard)
    .with_oplog(log)
    .build()
}

/// The error a log whose connection was cut refuses the append with.
#[derive(Debug)]
struct AppendRefused;

impl core::fmt::Display for AppendRefused {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("the change log refused the append")
    }
}

/// An in-memory change log whose first append is refused, as a cut connection
/// does, and which keeps every record it accepts.
#[derive(Clone)]
struct FlakyAppend {
    inner: Arc<InMemoryOplog>,
    refusals: Arc<AtomicUsize>,
}

impl FlakyAppend {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryOplog::default()),
            refusals: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Oplog for FlakyAppend {
    type Error = AppendRefused;

    async fn append(&self, record: &ChangeRecord) -> Result<(), AppendRefused> {
        // Only the first append is refused, so the retry's append goes through.
        if self.refusals.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(AppendRefused);
        }
        self.inner
            .append(record)
            .await
            .map_err(|never| match never {})
    }

    async fn entries_since(
        &self,
        after: PgCommitPosition,
    ) -> Result<Vec<ChangeRecord>, AppendRefused> {
        self.inner
            .entries_since(after)
            .await
            .map_err(|never| match never {})
    }

    async fn entries_of(&self, unseen: &Unseen) -> Result<Vec<ChangeRecord>, AppendRefused> {
        self.inner
            .entries_of(unseen)
            .await
            .map_err(|never| match never {})
    }

    async fn commits_of(
        &self,
        unseen: &UnseenIds,
        through: PgLsn,
    ) -> Result<Commits, AppendRefused> {
        self.inner
            .commits_of(unseen, through)
            .await
            .map_err(|never| match never {})
    }

    async fn min_position(&self) -> Result<Option<PgCommitPosition>, AppendRefused> {
        self.inner
            .min_position()
            .await
            .map_err(|never| match never {})
    }

    async fn current_position(&self) -> Result<Option<PgCommitPosition>, AppendRefused> {
        self.inner
            .current_position()
            .await
            .map_err(|never| match never {})
    }

    async fn record_commit(&self, commit: PgCommit) -> Result<(), AppendRefused> {
        self.inner
            .record_commit(commit)
            .await
            .map_err(|never| match never {})
    }

    async fn last_commit(&self) -> Result<Option<PgCommit>, AppendRefused> {
        self.inner
            .last_commit()
            .await
            .map_err(|never| match never {})
    }

    async fn forget_through(&self, through: PgCommitPosition) -> Result<(), AppendRefused> {
        self.inner
            .forget_through(through)
            .await
            .map_err(|never| match never {})
    }
}

/// A change source that replays exactly the items it was handed, then ends
/// cleanly, as a slot replaying what it was never acknowledged past would.
struct Replay(VecDeque<SourceItem<PgChangeEvent, PgCommit>>);

impl CdcSource for Replay {
    type Commit = PgCommit;
    type Event = PgChangeEvent;
    type Error = io::Error;

    fn next_item(
        &mut self,
    ) -> impl Future<Output = Result<Option<SourceItem<PgChangeEvent, PgCommit>>, io::Error>> + Send
    {
        let next = self.0.pop_front();
        async move { Ok(next) }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn ack(&mut self, _upto: PgCommitPosition) -> Result<(), io::Error> {
        Ok(())
    }
}

/// The insert event the emulator stamps for `sql`.
fn emu_event(source: &mut PgSqliteEmuSource, sql: &str) -> PgChangeEvent {
    source.execute_sql(sql).expect("execute dml");
    source
        .drain()
        .expect("drain emu")
        .into_iter()
        .find_map(SourceItem::into_event)
        .expect("the event")
}

/// Nothing more reaches a client for half a second.
async fn assert_quiet(client: &mut Client) {
    let extra = tokio::time::timeout(Duration::from_millis(500), client.next_control()).await;
    assert!(extra.is_err(), "unexpected further control {extra:?}");
}

/// The ids the live patch put into the replica.
fn replica_ids(live: &connetto_core::messages::LivePatch) -> Vec<i64> {
    let applier = Materializer::builder(DDL).build().expect("build applier");
    let mut replica = SqliteConnection::establish(":memory:").expect("open sqlite");
    sql_query(SQLITE_DDL)
        .execute(&mut replica)
        .expect("create the replica table");
    applier
        .apply_diffset(&live.patchset_zstd, &mut replica)
        .expect("apply the live patch");
    counts::table
        .select(counts::id)
        .order(counts::id)
        .load::<i64>(&mut replica)
        .expect("read the replica")
}

diesel::table! {
    /// Row from the counts test fixture.
    counts (id) {
        /// Row identifier, the primary key.
        id -> diesel::sql_types::BigInt,
        /// The value the count reads.
        n -> diesel::sql_types::BigInt,
    }
}

/// An append the engine has already applied is held, and the retry after the
/// refusal appends the record the event still owes, so the log ends with the
/// event once and its answers move once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_append_is_retried_with_the_held_record() {
    crate::logging::with_capture(
        "a_refused_append_is_retried_with_the_held_record",
        |logs| async move {
            let fixture = Fixture::acquire().await;
            fixture.exec("DROP TABLE IF EXISTS counts CASCADE").await;
            fixture
                .exec("CREATE TABLE counts (id INT PRIMARY KEY, n INT)")
                .await;

            let pool = fixture.admin().clone();
            let log = FlakyAppend::new();
            let manager = manager_with_log(
                log.clone(),
                pool.clone(),
                guard(Duration::from_secs(30), Duration::from_secs(30)),
                pool.clone(),
                pool,
            );

            let mut computed = connect_to(&manager);
            computed.handshake_with("computed", "user:reader").await;
            computed
                .subscribe("total", "SELECT COUNT(*) FROM counts")
                .await;
            let ControlMessage::AggregateUpdate(seed) = computed.next_control().await else {
                panic!("expected the count seed");
            };
            assert_eq!(seed.result_json.as_deref(), Some("0"));

            let mut rows = connect_to(&manager);
            rows.handshake_with("rows", "user:watcher").await;
            rows.subscribe("live", "SELECT * FROM counts").await;
            rows.expect_snapshot("live").await;

            // The row commits before the stream plays it, so the re-read and the seed
            // agree on what the database holds.
            fixture
                .exec("INSERT INTO counts (id, n) VALUES (10, 10)")
                .await;
            let at = fixture.commit_now().await;
            let mut source = PgSqliteEmuSource::open_in_memory(DDL).expect("open emu source");
            let event = committed_at(
                emu_event(&mut source, "INSERT INTO counts (id, n) VALUES (10, 10)"),
                at,
            );

            // Both attempts replay the same event, as the slot does once its
            // acknowledgement never passed the commit.
            manager
                .ingest_with_reconnect(
                    |_| {
                        let event = event.clone();
                        async move {
                            Ok::<_, io::Error>(Replay(
                                vec![SourceItem::Event(event)].into(),
                            ))
                        }
                    },
                    &ReconnectPolicy::default(),
                    |_| {},
                )
                .await
                .expect("a refused append ends the attempt, and the held event is replayed, not refused");

            // The log holds the event exactly once, at its position.
            let records = log
                .entries_since(origin())
                .await
                .expect("read the log");
            assert_eq!(
                records.len(),
                1,
                "the retry appends the held record, so the log holds the event once"
            );
            assert_eq!(records[0].position(), event.position());

            // The answer moved once, to the committed row, and nothing follows.
            let got = computed.recv().await;
            let Some(IncomingFrame::Control(ControlMessage::AggregateUpdate(moved))) = got else {
                panic!(
                    "expected the count movement, got {got:?}; server log: {:?}",
                    logs.lines()
                );
            };
            assert_eq!(moved.result_json.as_deref(), Some("1"));
            assert_quiet(&mut computed).await;

            // The row itself landed once.
            let live = rows.wait_for_live(Duration::from_secs(5)).await;
            assert_eq!(live.sub_id, "live");
            assert_eq!(replica_ids(&live), vec![10]);
            let again = rows.try_live(Duration::from_millis(500)).await;
            assert!(again.is_none(), "the patch must not be sent twice: {again:?}");
        }
    )
    .await;
}

/// A dispatch that fails before its event is acknowledged can meet a stream
/// that resumes past it. The held progress is the wrong event's, so it is
/// dropped, and the new event applies and delivers on its own merits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_that_resumes_past_a_held_event_drops_what_it_owed() {
    let fixture = Arc::new(Fixture::acquire().await);
    fixture.exec("DROP TABLE IF EXISTS counts CASCADE").await;
    fixture
        .exec("CREATE TABLE counts (id INT PRIMARY KEY, n INT)")
        .await;

    let pool = fixture.admin().clone();
    let log = FlakyAppend::new();
    let manager = manager_with_log(
        log.clone(),
        pool.clone(),
        guard(Duration::from_secs(30), Duration::from_secs(30)),
        pool.clone(),
        pool.clone(),
    );

    let mut computed = connect_to(&manager);
    computed.handshake_with("computed", "user:reader").await;
    computed
        .subscribe("total", "SELECT COUNT(*) FROM counts")
        .await;
    let ControlMessage::AggregateUpdate(seed) = computed.next_control().await else {
        panic!("expected the count seed");
    };
    assert_eq!(seed.result_json.as_deref(), Some("0"));

    let mut rows = connect_to(&manager);
    rows.handshake_with("rows", "user:watcher").await;
    rows.subscribe("live", "SELECT * FROM counts").await;
    rows.expect_snapshot("live").await;

    // The first event's row is not in Postgres, but its fold still lands in the
    // engine's store. Only its row patch and oplog record are owed and dropped.
    let at_first = fixture.commit_now().await;
    let mut source = PgSqliteEmuSource::open_in_memory(DDL).expect("open emu source");
    let first = committed_at(
        emu_event(&mut source, "INSERT INTO counts (id, n) VALUES (10, 10)"),
        at_first,
    );
    let second = emu_event(&mut source, "INSERT INTO counts (id, n) VALUES (20, 20)");
    // The resumed attempt stamps its event after the second row commits, so the
    // log assertion reads the position from the drive.
    let second_position: Arc<Mutex<Option<PgCommitPosition>>> = Arc::new(Mutex::new(None));
    let attempts = Arc::new(AtomicUsize::new(0));
    manager
        .ingest_with_reconnect(
            |_resume: ResumePoint| {
                let first_attempt = attempts.fetch_add(1, Ordering::Relaxed) == 0;
                let event = if first_attempt {
                    first.clone()
                } else {
                    second.clone()
                };
                let admin = pool.clone();
                let fx = Arc::clone(&fixture);
                let stamped_position = Arc::clone(&second_position);
                async move {
                    let event = if first_attempt {
                        event
                    } else {
                        // The stream's second attempt resumes past the first
                        // event, with the second row committed meanwhile.
                        connetto_test_harness::exec(
                            &admin,
                            "INSERT INTO counts (id, n) VALUES (20, 20)",
                        )
                        .await;
                        let at = fx.commit_now().await;
                        let stamped = committed_at(event, at);
                        *stamped_position.lock() = Some(stamped.position());
                        stamped
                    };
                    Ok::<_, io::Error>(Replay(vec![SourceItem::Event(event)].into()))
                }
            },
            &ReconnectPolicy::default(),
            |_| {},
        )
        .await
        .expect("a refused append ends the attempt, and the resumed stream is ingested");
    let second_position = second_position
        .lock()
        .expect("the resumed attempt stamped its event");
    let live = rows.wait_for_live(Duration::from_secs(5)).await;
    assert_eq!(live.sub_id, "live");
    assert_eq!(
        replica_ids(&live),
        vec![20],
        "the first event's patch must not ride the second event's position"
    );
    let again = rows.try_live(Duration::from_millis(500)).await;
    assert!(
        again.is_none(),
        "only the resumed event's patch may land: {again:?}"
    );

    // The count moved once, to 2, the two events each folded once. The first
    // event's individual movement rode its held progress and was dropped, so
    // its fold is inside the answer but not its own frame.
    let ControlMessage::AggregateUpdate(moved) = computed.next_control().await else {
        panic!("expected the count movement");
    };
    assert_eq!(moved.result_json.as_deref(), Some("2"));
    assert_quiet(&mut computed).await;

    // The log holds the second event at its position, and nothing for the first.
    let records = log.entries_since(origin()).await.expect("read the log");
    assert_eq!(
        records.len(),
        1,
        "the first event's refused append left no record, so the second is the log's only one"
    );
    assert_eq!(records[0].position(), second_position);
}
