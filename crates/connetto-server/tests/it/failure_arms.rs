//! The failure arms the ingest path must refuse rather than kill.
//!
//! A value the wire cannot carry and a row a subscriber saw and no longer
//! sees are each answered by ending one subscription alone with the one
//! fixed refusal or by withdrawing the row, and the change stream keeps
//! flowing for everyone else. These tests pin those answers against real
//! Postgres, including the catchup of an event the replica cannot hold, and
//! count reconnects so a failure that takes the shared stream down is named.
//! The no-column flip runs on the shipped authorization path, with the
//! policy translated to the authorization service, the store the upkeep
//! brings level before every event, and the membership term whose narrowing
//! withdraws the rows a moved grant no longer reaches.
//!
//! Needs Docker. Every fixture starts its own Postgres and a real pgoutput
//! slot, and the flip tests start the authorization service beside it.

#![expect(
    clippy::too_many_lines,
    reason = "each test walks one failure from the committed event to the wire answer in order, and a split would hide the sequence"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use connetto_core::Cursor;
use connetto_core::auth::{DEFAULT_USER_SETTING, Principal};
use connetto_core::messages::{BulkMessage, ControlMessage, SUBSCRIPTION_REFUSED};
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::IncomingFrame;
use connetto_server::openfga::{Counted, FgaAuth, ModelSubject, Translated};
use connetto_server::{
    AbuseConfig, FailedAuthorization, ManagerBuilder, Materializer, Oplog, OplogConfig, PgOplog,
    PgReadConnector, PgSnapshotSource, Position, ReconnectEvent, ReconnectPolicy, RequestGuard,
    ResumePoint, RuntimeWritableCatalog, SessionManager, ThrottleConfig, TierLimits,
    TimelineHistory, loopback, pg_write_target,
};
use connetto_test_harness::fanout::caller_mapping;
use connetto_test_harness::{
    Client, ConnettoDefaults, Fixture, HarnessAuth, OPLOG_TABLE, PUBLICATION, SLOT, Server,
    ServerConfig, pool_for, spawn_server, with_user,
};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::Pool;
use openfga_client::client::OpenFgaServiceClient;
use sqlite_diff_rs::{ParsedDiffSet, PatchsetOp, Value as WireValue};
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::visibility::VisibilityPolicy;
use subql::visibility::openfga::OpenFgaPolicy;
use subql::{ParserDB, PgCommitPosition, PgLsn, PgStreamingCdcSource, PgStreamingConfig, PgXid};
use tracing::Instrument;

use crate::owner_policy::OwnerPolicy;

diesel::table! {
    /// One visible doc, the table the client subscribes.
    docs (id) {
        /// Doc identifier, the primary key.
        id -> Integer,
        /// Doc body.
        body -> Text,
    }
}

diesel::table! {
    /// A viewer named for one doc, the grant the policy reads.
    members (doc_id, viewer) {
        /// The doc the viewer is named for.
        doc_id -> Integer,
        /// The viewer's user id.
        viewer -> Text,
    }
}

type Manager<A> = SessionManager<PgSnapshotSource, A, ConnettoDefaults, PgReadConnector, PgOplog>;

fn connect_to<A>(manager: &Arc<Manager<A>>) -> Client
where
    A: VisibilityPolicy<Watcher = Arc<Principal>, Backend = Postgres> + Send + Sync + 'static,
    <A as VisibilityPolicy>::Error: std::fmt::Display + FailedAuthorization,
{
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

fn guard() -> Arc<RequestGuard<String>> {
    Arc::new(RequestGuard::new(
        ThrottleConfig::default()
            .with_identified(TierLimits::identified().with_read_timeout(Duration::from_secs(30)))
            .with_reexec_timeout(Duration::from_secs(30)),
        AbuseConfig::default(),
    ))
}

/// A manager over the deployment's own wiring, the oplog the caller carries,
/// so a test reads the records the stream kept out of the same table the
/// stream wrote them to.
fn full_manager<A>(
    pool: Pool<AsyncPgConnection>,
    schema_ddl: &str,
    auth: A,
    oplog: PgOplog,
) -> Arc<Manager<A>>
where
    A: VisibilityPolicy<Watcher = Arc<Principal>, Backend = Postgres> + Send + Sync + 'static,
    <A as VisibilityPolicy>::Error: std::fmt::Display + FailedAuthorization,
{
    ManagerBuilder::new(
        Materializer::builder(schema_ddl)
            .with_write_catalog(RuntimeWritableCatalog::default())
            .with_read_connector(PgReadConnector::with_session_setup(pool.clone()))
            .build()
            .expect("build materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), schema_ddl).expect("snapshot source"),
        auth,
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(pool.clone()),
        pg_write_target::<ConnettoDefaults>(pool, schema_ddl).expect("build write target"),
    )
    .with_guard(guard())
    .with_oplog(oplog)
    .build()
}

/// The error text every [`ReconnectEvent`] carries, for the observations the
/// assertions print when they fail.
fn event_error_text<'a>(event: &ReconnectEvent<'a>) -> &'a str {
    match event {
        ReconnectEvent::Retrying { error, .. }
        | ReconnectEvent::GaveUp { error, .. }
        | ReconnectEvent::AuthRetrying { error, .. }
        | ReconnectEvent::ReadRetrying { error, .. } => error,
    }
}

/// One shared stream with a capped retry policy, counting every reconnect so a
/// replay loop on the same event is observable.
fn start_ingest<A>(
    manager: &Arc<Manager<A>>,
    stream_url: String,
    schema_ddl: &str,
    retries: &Arc<AtomicU32>,
    in_place: &Arc<AtomicU32>,
) -> tokio::task::JoinHandle<()>
where
    A: VisibilityPolicy<Watcher = Arc<Principal>, Backend = Postgres> + Send + Sync + 'static,
    <A as VisibilityPolicy>::Error: std::fmt::Display + FailedAuthorization,
{
    let session = Arc::clone(manager);
    let ddl = schema_ddl.to_owned();
    let retries = Arc::clone(retries);
    let in_place = Arc::clone(in_place);
    tokio::spawn(async move {
        let connect = |resume: ResumePoint| {
            let url = stream_url.clone();
            let ddl = ddl.clone();
            async move {
                let catalog = ParserDB::parse::<PostgreSqlDialect>(&ddl)
                    .map_err(|err| format!("catalog: {err:?}"))?;
                let config = PgStreamingConfig::new(url, SLOT, PUBLICATION).start(resume.get());
                PgStreamingCdcSource::connect(config, catalog)
                    .await
                    .map_err(|err| err.to_string())
            }
        };
        let policy = ReconnectPolicy::new()
            .with_initial_backoff(Duration::from_millis(50))
            .with_max_backoff(Duration::from_millis(200))
            .with_max_attempts(Some(4));
        let _ = session
            .ingest_with_reconnect(connect, &policy, |event| match event {
                ReconnectEvent::Retrying { attempt, .. } => {
                    retries.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "stream retried (attempt {attempt}): {}",
                        event_error_text(&event)
                    );
                }
                ReconnectEvent::GaveUp { attempts, error } => {
                    eprintln!("stream gave up after {attempts} attempts: {error}");
                }
                ReconnectEvent::ReadRetrying { .. } | ReconnectEvent::AuthRetrying { .. } => {
                    in_place.fetch_add(1, Ordering::Relaxed);
                }
            })
            .await;
    })
}

fn client_replica(sqlite_ddl: &str) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:").expect("open sqlite");
    // DDL the query DSL cannot express; batched so a replica of two tables
    // bootstraps in one call.
    conn.batch_execute(sqlite_ddl)
        .expect("create replica tables");
    conn
}

/// The primary keys the replica currently holds for `table`, ordered.
fn replica_rows(replica: &mut SqliteConnection, table: &str) -> Vec<i64> {
    #[derive(QueryableByName)]
    struct Id {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
    }
    let rows: Vec<Id> = sql_query(format!("SELECT id FROM {table} ORDER BY id"))
        .load(replica)
        .expect("read replica rows");
    rows.into_iter().map(|row| row.id).collect()
}

/// Apply every live patch that arrives until the stream goes idle.
async fn drain_live(client: &mut Client, applier: &Materializer, replica: &mut SqliteConnection) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "live patches kept arriving past the deadline, the stream never went idle"
        );
        let Some(patch) = client.try_live(Duration::from_millis(500)).await else {
            break;
        };
        applier
            .apply_diffset(&patch.patchset_zstd, replica)
            .expect("apply live patch");
    }
}

/// An update that moves the primary key and takes the row out of the watcher's
/// reach in the same statement. The deployment needs the replica to lose the
/// old row under its old key, and the stream to keep flowing after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_after_a_primary_key_change_uses_the_old_key() {
    const PG: &str =
        "CREATE TABLE notes (id INT PRIMARY KEY, owner TEXT NOT NULL, body TEXT NOT NULL)";
    const SQLITE: &str =
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, body TEXT NOT NULL)";

    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS notes CASCADE").await;
    fixture.exec(PG).await;
    fixture
        .exec("INSERT INTO notes (id, owner, body) VALUES (1, 'alice', 'hello')")
        .await;
    fixture.start_replication(&["notes"]).await;

    let pool = fixture.admin().clone();
    let oplog = PgOplog::new(pool.clone(), OPLOG_TABLE, OplogConfig::default());
    oplog.ensure_schema().await.expect("provision the oplog");
    let manager = full_manager(pool.clone(), PG, OwnerPolicy, oplog);
    let mut client = connect_to(&manager);
    client.handshake_with("alice", "user:alice").await;
    client.subscribe("notes", "SELECT * FROM notes").await;

    let applier = Materializer::builder(PG).build().expect("build applier");
    let mut replica = client_replica(SQLITE);
    for patch in client.expect_snapshot("notes").await {
        applier
            .apply_diffset(&patch.patchset_zstd, &mut replica)
            .expect("apply snapshot");
    }
    assert_eq!(
        replica_rows(&mut replica, "notes"),
        vec![1],
        "snapshot holds the row"
    );

    let retries = Arc::new(AtomicU32::new(0));
    let in_place = Arc::new(AtomicU32::new(0));
    let ingest = start_ingest(
        &manager,
        fixture.admin_url().to_owned(),
        PG,
        &retries,
        &in_place,
    );

    // The old image keeps id 1 and owner alice, the new one takes both away.
    fixture
        .exec("UPDATE notes SET id = 2, owner = 'bob', body = 'moved' WHERE id = 1")
        .await;
    drain_live(&mut client, &applier, &mut replica).await;

    fixture
        .exec("INSERT INTO notes (id, owner, body) VALUES (4, 'alice', 'again')")
        .await;
    let live = client.try_live(Duration::from_secs(5)).await;
    if let Some(patch) = live.as_ref() {
        applier
            .apply_diffset(&patch.patchset_zstd, &mut replica)
            .expect("apply live patch");
    }

    let rows = replica_rows(&mut replica, "notes");
    assert!(
        live.is_some(),
        "a later visible insert kept arriving on the stream; it did not, and the stream \
         retried {} times so far",
        retries.load(Ordering::Relaxed)
    );
    assert_eq!(
        rows,
        vec![4],
        "the replica lost the old row under its old key 1 and the re-homed row id 2 was \
         never visible, so only the later insert 4 remains; it holds {rows:?} instead"
    );
    assert_eq!(
        retries.load(Ordering::Relaxed),
        0,
        "a clean withdrawal must not need a reconnect"
    );
    assert!(
        !ingest.is_finished(),
        "the ingest loop ended when the stream should keep running"
    );
}

/// The tables the unencodable probes share, the one the value lands in and
/// the one a later change must still reach. The owner leads the columns, the
/// position the policy reads it from.
const INSTANTS_PG: &str = "CREATE TABLE instants (id INT PRIMARY KEY, owner TEXT NOT NULL, \
                             at TIMESTAMPTZ NOT NULL); \
                          CREATE TABLE notes (id INT PRIMARY KEY, owner TEXT NOT NULL, \
                             body TEXT NOT NULL)";
const INSTANTS_SQLITE: &str = "CREATE TABLE instants (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                                at TEXT NOT NULL); \
                               CREATE TABLE notes (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                                body TEXT NOT NULL)";

/// A value Postgres can hold but the SQLite wire cannot carry, the
/// `timestamptz` infinity. The update's new image is that value, so the
/// dispatch cannot build the patch the matched subscriber is owed, and the
/// deployment needs that subscriber refused with `SUBSCRIPTION_REFUSED`
/// while a caller whose verdict skips the hidden row is told nothing and
/// keeps flowing and the stream never restarts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_from_a_wire_unencodable_old_value_must_not_kill_the_stream() {
    let fixture = Fixture::acquire().await;
    fixture
        .exec("DROP TABLE IF EXISTS instants, notes CASCADE")
        .await;
    fixture.exec(INSTANTS_PG).await;
    fixture
        .exec("INSERT INTO instants (id, owner, at) VALUES (1, 'alice', '2026-01-01 00:00:00+00')")
        .await;
    fixture
        .exec("INSERT INTO notes (id, owner, body) VALUES (1, 'alice', 'hello')")
        .await;
    fixture.start_replication(&["instants", "notes"]).await;

    let pool = fixture.admin().clone();
    let oplog = PgOplog::new(pool.clone(), OPLOG_TABLE, OplogConfig::default());
    oplog.ensure_schema().await.expect("provision the oplog");
    let manager = full_manager(pool.clone(), INSTANTS_PG, OwnerPolicy, oplog);
    let mut client = connect_to(&manager);
    client.handshake_with("alice", "user:alice").await;
    client.subscribe("instants", "SELECT * FROM instants").await;
    client.subscribe("notes", "SELECT * FROM notes").await;

    let applier = Materializer::builder(INSTANTS_PG)
        .build()
        .expect("build applier");
    let mut replica = client_replica(INSTANTS_SQLITE);
    for label in ["instants", "notes"] {
        for patch in client.expect_snapshot(label).await {
            applier
                .apply_diffset(&patch.patchset_zstd, &mut replica)
                .expect("apply snapshot");
        }
    }
    assert_eq!(
        replica_rows(&mut replica, "notes"),
        vec![1],
        "the snapshot holds the note"
    );

    // bob sees only his own rows, so the hidden row tells him nothing, and the
    // event that refuses alice must keep his subscription flowing.
    let mut bob = connect_to(&manager);
    bob.handshake_with("bob", "user:bob").await;
    bob.subscribe("instants", "SELECT * FROM instants").await;
    let mut bob_replica = client_replica(INSTANTS_SQLITE);
    for patch in bob.expect_snapshot("instants").await {
        applier
            .apply_diffset(&patch.patchset_zstd, &mut bob_replica)
            .expect("apply bob's snapshot");
    }
    assert_eq!(
        replica_rows(&mut bob_replica, "instants"),
        vec![1],
        "the snapshot serves the raw query to every caller"
    );

    let retries = Arc::new(AtomicU32::new(0));
    let in_place = Arc::new(AtomicU32::new(0));
    let ingest = start_ingest(
        &manager,
        fixture.admin_url().to_owned(),
        INSTANTS_PG,
        &retries,
        &in_place,
    );

    // The row moves to the value the codec cannot carry. The event matches
    // the instants subscription alone, so the refusal names it.
    fixture
        .exec("UPDATE instants SET at = 'infinity' WHERE id = 1")
        .await;
    let refused = tokio::time::timeout(Duration::from_secs(10), client.next_control()).await;
    match refused {
        Ok(ControlMessage::NonFatalError(fatal)) => {
            assert_eq!(
                fatal.related_to.as_deref(),
                Some("instants"),
                "the refusal names the subscription the event matched"
            );
            assert_eq!(fatal.detail, SUBSCRIPTION_REFUSED);
        }
        other => panic!("the matched subscription was refused, the client met {other:?}"),
    }

    // bob's verdict skips the hidden row, so the event tells him nothing.
    let silence = tokio::time::timeout(Duration::from_millis(500), bob.recv()).await;
    assert!(
        silence.is_err(),
        "a caller whose verdict skips the hidden row is told nothing, and bob met {silence:?}"
    );

    fixture
        .exec("INSERT INTO notes (id, owner, body) VALUES (2, 'alice', 'again')")
        .await;
    let live = client.try_live(Duration::from_secs(5)).await;
    if let Some(patch) = live.as_ref() {
        applier
            .apply_diffset(&patch.patchset_zstd, &mut replica)
            .expect("apply live patch");
    }

    assert!(
        live.is_some(),
        "a later visible insert on the other table kept arriving on the stream; it did not, and \
         the stream retried {} times so far",
        retries.load(Ordering::Relaxed)
    );
    assert_eq!(
        replica_rows(&mut replica, "notes"),
        vec![1, 2],
        "the notes subscription caught the later insert"
    );
    assert_eq!(
        replica_rows(&mut replica, "instants"),
        vec![1],
        "the refused subscription receives nothing more, its row left to the replica"
    );

    // bob's own row arrives on the subscription his verdict never reached.
    fixture
        .exec("INSERT INTO instants (id, owner, at) VALUES (2, 'bob', '2026-01-02 00:00:00+00')")
        .await;
    let bob_live = bob.try_live(Duration::from_secs(5)).await;
    if let Some(patch) = bob_live.as_ref() {
        applier
            .apply_diffset(&patch.patchset_zstd, &mut bob_replica)
            .expect("apply bob's live patch");
    }
    assert!(
        bob_live.is_some(),
        "bob's own row kept arriving on a stream his subscription never left; it did not"
    );
    assert_eq!(
        replica_rows(&mut bob_replica, "instants"),
        vec![1, 2],
        "bob's replica holds the snapshot row and the row the stream delivered"
    );
    assert_eq!(
        retries.load(Ordering::Relaxed),
        0,
        "a value the fold cannot carry must not take the shared stream down"
    );
    assert!(
        !ingest.is_finished(),
        "the ingest loop ended when the stream should keep running"
    );
}

/// A client offline across a value the wire cannot carry resumes after the
/// event. The oplog kept the event with no payload, so the catchup matches
/// the record's event and refuses the subscription it matched while the
/// client's other subscription catches up normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unencodable_event_must_refuse_the_subscriber_on_catchup() {
    let fixture = Fixture::acquire().await;
    fixture
        .exec("DROP TABLE IF EXISTS instants, notes CASCADE")
        .await;
    fixture.exec(INSTANTS_PG).await;
    fixture.start_replication(&["instants", "notes"]).await;

    let pool = fixture.admin().clone();
    let oplog = PgOplog::new(pool.clone(), OPLOG_TABLE, OplogConfig::default());
    oplog.ensure_schema().await.expect("provision the oplog");
    let manager = full_manager(pool.clone(), INSTANTS_PG, OwnerPolicy, oplog);

    let mut client = connect_to(&manager);
    client.handshake_with("alice", "user:alice").await;
    client.subscribe("instants", "SELECT * FROM instants").await;
    client.expect_snapshot("instants").await;
    client.subscribe("notes", "SELECT * FROM notes").await;
    client.expect_snapshot("notes").await;
    // The client goes offline here with two empty snapshots, before the rows
    // its resume will catch up over exist.
    client.close().await;

    let retries = Arc::new(AtomicU32::new(0));
    let in_place = Arc::new(AtomicU32::new(0));
    let ingest = start_ingest(
        &manager,
        fixture.admin_url().to_owned(),
        INSTANTS_PG,
        &retries,
        &in_place,
    );

    // The rows the stream records into the oplog as live changes. The slot
    // predates them, so the stream reaches them.
    fixture
        .exec("INSERT INTO instants (id, owner, at) VALUES (1, 'alice', '2026-01-01 00:00:00+00')")
        .await;
    fixture
        .exec("INSERT INTO notes (id, owner, body) VALUES (1, 'alice', 'hello')")
        .await;

    // The cursor a resuming client presents is where the log stood once the
    // stream had kept both seeded rows.
    let log = PgOplog::new(pool.clone(), OPLOG_TABLE, OplogConfig::default());
    let ceiling = logged_past(
        &log,
        PgCommitPosition::new(PgLsn(0), PgXid(0), 0),
        &["instants", "notes"],
    )
    .await;
    let cursor = Cursor::new(
        Position {
            system: TimelineHistory::default().system(),
            timeline: 1,
            at: ceiling,
        }
        .to_cursor_bytes(),
    );

    // The event the replica cannot hold, then one it can.
    fixture
        .exec("UPDATE instants SET at = 'infinity' WHERE id = 1")
        .await;
    fixture
        .exec("INSERT INTO notes (id, owner, body) VALUES (2, 'alice', 'again')")
        .await;
    logged_past(&log, ceiling, &["instants", "notes"]).await;

    let mut resumed = connect_to(&manager);
    // The client presents no held keys, so the subject coverage it declares
    // for the cursor is the empty set, which is what the identity grant
    // resolved to.
    resumed
        .handshake_resuming("alice-2", "user:alice", cursor, Some(&[]))
        .await;
    resumed
        .subscribe("instants", "SELECT * FROM instants")
        .await;
    resumed.subscribe("notes", "SELECT * FROM notes").await;

    // The catchup owes the client the note's later row and the refusal of
    // the subscription the unencodable event matched. The replica starts
    // empty, since the client's snapshots were.
    let applier = Materializer::builder(INSTANTS_PG)
        .build()
        .expect("build applier");
    let mut replica = client_replica(INSTANTS_SQLITE);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut got_refusal = false;
    let mut got_note = false;
    let mut got_end = false;
    while !(got_refusal && got_note && got_end) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, resumed.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the catchup stayed silent past the deadline, still waiting for \
                     refusal={got_refusal} note={got_note} end={got_end}"
                )
            })
            .expect("the connection stayed open");
        match frame {
            IncomingFrame::Control(ControlMessage::NonFatalError(fatal)) => {
                assert_eq!(
                    fatal.related_to.as_deref(),
                    Some("instants"),
                    "the refusal names the subscription the marker matched"
                );
                assert_eq!(fatal.detail, SUBSCRIPTION_REFUSED);
                got_refusal = true;
            }
            IncomingFrame::Bulk(BulkMessage::LivePatch(patch)) => {
                applier
                    .apply_diffset(&patch.patchset_zstd, &mut replica)
                    .expect("apply the catchup patch");
                got_note = true;
            }
            IncomingFrame::Control(ControlMessage::SnapshotEnd(end)) => {
                assert_eq!(end.sub_id, "notes", "only the notes catchup completes");
                got_end = true;
            }
            IncomingFrame::Control(ControlMessage::FullResyncRequired(_)) => {
                panic!("the resumed cursor inside the window must not resync");
            }
            IncomingFrame::Control(ControlMessage::FatalError(fatal)) => {
                panic!("the connection closed with {fatal:?} while the catchup owed frames");
            }
            _ => {}
        }
    }
    assert_eq!(
        replica_rows(&mut replica, "notes"),
        vec![2],
        "the other subscription caught up across the marker"
    );
    assert!(
        !ingest.is_finished(),
        "a value the replica cannot store must not take the shared stream down, and it \
         retried {} times so far",
        retries.load(Ordering::Relaxed)
    );
}

/// Wait until the log holds a record of every table in `tables` past `after`,
/// and answer the position of the last record it holds.
async fn logged_past(log: &PgOplog, after: PgCommitPosition, tables: &[&str]) -> PgCommitPosition {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let records = log.entries_since(after).await.expect("read the log");
        if tables
            .iter()
            .all(|table| records.iter().any(|record| record.table() == *table))
        {
            return records.last().expect("a record per table").position();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the log never recorded {tables:?} past {after:?} within the deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The guarded table and the membership table that grants it.
const FLIP_DDL: &str = "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT NOT NULL); \
                        CREATE TABLE members (doc_id INT NOT NULL, viewer TEXT NOT NULL, \
                        PRIMARY KEY (doc_id, viewer));";
/// The replica's own copy of the two tables.
const FLIP_SQLITE: &str = "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL); \
                           CREATE TABLE members (doc_id INTEGER NOT NULL, viewer TEXT NOT NULL, \
                           PRIMARY KEY (doc_id, viewer));";
/// The deployment's read rule on the guarded table, one statement each so the
/// fixture can run them in order.
const FLIP_POLICY_STATEMENTS: [&str; 2] = [
    "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
    "CREATE POLICY docs_members ON docs FOR ALL USING (\
       id IN (SELECT doc_id FROM members WHERE viewer = \
       current_setting('app.user_id', true)))",
];
/// The client's own query, in the replica's dialect: the membership term.
const FLIP_QUERY: &str = "SELECT * FROM docs WHERE id IN (SELECT doc_id FROM members \
                          WHERE viewer = current_app_user())";
/// The non-superuser role the snapshot reads run as, under the row policies.
const FLIP_READER: &str = "nfv_reader";
/// The subscription label and the membership mirror's.
const DOCS_SUB: &str = "docs";
const MEMBERSHIP_LABEL: &str = "connetto-membership:members";

/// The flip's statements, in the order the transaction commits them.
enum FlipStatement {
    /// The no-op update of the doc, the statement that changes no column of
    /// the subscribed table.
    Update,
    /// The membership row leaving.
    MembershipLeave,
}

const FLIP_UPDATE_FIRST: [FlipStatement; 2] =
    [FlipStatement::Update, FlipStatement::MembershipLeave];
const FLIP_DELETE_FIRST: [FlipStatement; 2] =
    [FlipStatement::MembershipLeave, FlipStatement::Update];
const FLIP_DELETE_ALONE: [FlipStatement; 1] = [FlipStatement::MembershipLeave];

/// The stack a flip test drives: the tables under the policy, the reader
/// role, the seed rows, the shipped executor with its store upkeep, and the
/// server answering through it.
async fn flip_stack(fixture: &Fixture) -> Server {
    let mut statements: Vec<&str> = vec!["DROP TABLE IF EXISTS docs, members CASCADE"];
    for statement in FLIP_DDL.split(';') {
        if !statement.trim().is_empty() {
            statements.push(statement);
        }
    }
    statements.extend(FLIP_POLICY_STATEMENTS);
    statements.push("INSERT INTO docs (id, body) VALUES (1, 'hello')");
    statements.push("INSERT INTO members (doc_id, viewer) VALUES (1, 'alice')");
    let reader_role = format!(
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{FLIP_READER}') \
         THEN CREATE ROLE {FLIP_READER} LOGIN PASSWORD '{FLIP_READER}'; END IF; END $$"
    );
    let reader_usage = format!("GRANT USAGE ON SCHEMA public TO {FLIP_READER}");
    let reader_select = format!("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {FLIP_READER}");
    statements.push(&reader_role);
    statements.push(&reader_usage);
    statements.push(&reader_select);
    fixture.setup(&statements).await;

    let reader = pool_for(&with_user(fixture.admin_url(), FLIP_READER, FLIP_READER)).await;
    let engine_ddl = connetto_schema::with_registry::<connetto_core::bots::TextIds>(FLIP_DDL);

    // The composition the shipped binary builds, over one fresh store.
    let (channel, store) = fixture.fga_store().await;
    let translated = Translated::of::<String, connetto_core::bots::TextIds>(
        FLIP_DDL,
        &FLIP_POLICY_STATEMENTS.join(";\n"),
        DEFAULT_USER_SETTING,
    )
    .expect("the membership policy translates");
    let mut setup = OpenFgaServiceClient::new(channel.clone());
    let model = translated
        .install_model(&mut setup, &store)
        .await
        .expect("the service accepted the rules");
    let loader = OpenFgaPolicy::<_, _, ModelSubject<String, String>, Postgres>::new(
        translated.shapes_arc(),
        setup,
        store.clone(),
    )
    .expect("the index carries what the questions need")
    .authorization_model_id(model.id().to_owned());
    translated
        .load_into(&fixture.admin().clone(), &loader)
        .await
        .expect("the facts load");
    let naming = translated.naming();
    let (shapes, translator, reach) = translated.into_parts();
    let delegate = OpenFgaPolicy::new(
        Arc::clone(&shapes),
        OpenFgaServiceClient::new(Counted::new(channel)),
        store,
    )
    .expect("the index carries what the questions need")
    .authorization_model_id(model.id().to_owned());
    let fga = FgaAuth::new(Arc::clone(&shapes), delegate, naming);
    let upkeep = fga.upkeep(reach, translator.clone(), fixture.admin().clone());

    let snapshot = PgSnapshotSource::from_ddl(reader.clone(), &engine_ddl)
        .expect("snapshot source")
        .with_publication(PUBLICATION);
    let config = ServerConfig::new(engine_ddl, fixture.admin_url())
        .with_policies(FLIP_POLICY_STATEMENTS.join(";\n"))
        .with_translation(translator, Some(caller_mapping()))
        .with_replication(["docs", "members"]);
    spawn_server(
        config,
        snapshot,
        HarnessAuth::fga(fga, upkeep),
        fixture.admin().clone(),
        fixture.admin().clone(),
    )
    .await
}

/// Commit the flip the way the deployment commits it, one transaction the
/// writer role holds, the statements in the given order.
async fn flip_transaction(pool: &Pool<AsyncPgConnection>, statements: &[FlipStatement]) {
    let mut conn = pool.get().await.expect("a write connection");
    conn.transaction(async |conn| -> Result<(), diesel::result::Error> {
        for statement in statements {
            match statement {
                FlipStatement::Update => {
                    let update = diesel::update(docs::table)
                        .filter(docs::id.eq(1))
                        .set(docs::body.eq("hello"));
                    diesel_async::RunQueryDsl::execute(&update, conn).await?;
                }
                FlipStatement::MembershipLeave => {
                    let leave = diesel::delete(members::table)
                        .filter(members::doc_id.eq(1))
                        .filter(members::viewer.eq("alice"));
                    diesel_async::RunQueryDsl::execute(&leave, conn).await?;
                }
            }
        }
        Ok(())
    })
    .await
    .expect("the flip commits");
}

/// The term subscription's frames: the docs snapshot, the membership mirror's
/// announce and snapshot, applied to the replica the client would keep.
async fn subscribe_terms(
    client: &mut Client,
    applier: &Materializer,
    replica: &mut SqliteConnection,
) {
    client.subscribe(DOCS_SUB, FLIP_QUERY).await;
    for patch in client.expect_snapshot(DOCS_SUB).await {
        applier
            .apply_diffset(&patch.patchset_zstd, replica)
            .expect("apply the docs snapshot");
    }
    let ControlMessage::MembershipOpened(opened) = client.next_control().await else {
        panic!("the membership mirror announces itself before its snapshot");
    };
    assert_eq!(
        opened.sub_id, MEMBERSHIP_LABEL,
        "the mirror names the membership table"
    );
    for patch in client.expect_snapshot(MEMBERSHIP_LABEL).await {
        applier
            .apply_diffset(&patch.patchset_zstd, replica)
            .expect("apply the membership snapshot");
    }
}

/// The ids the replica currently holds in the subscribed table, ordered.
fn replica_doc_ids(replica: &mut SqliteConnection) -> Vec<i32> {
    docs::table
        .select(docs::id)
        .order(docs::id)
        .load(replica)
        .expect("read the replica docs")
}

/// The doc ids the replica currently holds in the membership mirror, ordered.
fn replica_membership_ids(replica: &mut SqliteConnection) -> Vec<i32> {
    members::table
        .select(members::doc_id)
        .order(members::doc_id)
        .load(replica)
        .expect("read the replica mirror")
}

/// The operations one live patch carries, in order: an indirect delete is
/// the membership move-out's signature, a plain delete the row verdict's.
fn patch_ops(payload_zstd: &[u8]) -> Vec<String> {
    let bytes = zstd::decode_all(payload_zstd).expect("decompress the live patch");
    let ParsedDiffSet::Patchset(set) = ParsedDiffSet::parse(&bytes).expect("parse the patchset")
    else {
        panic!("the live patch is not a patchset");
    };
    set.iter()
        .map(|op| {
            let kind = match op {
                PatchsetOp::Insert { .. } => "insert",
                PatchsetOp::Update { .. } => "update",
                PatchsetOp::Delete { .. } => "delete",
            };
            let key = match op {
                PatchsetOp::Insert { values, .. } => values.first().map(WireValue::to_string),
                PatchsetOp::Update { pk, .. } | PatchsetOp::Delete { pk, .. } => {
                    pk.first().map(WireValue::to_string)
                }
            };
            let mut description = format!("{kind} {}", op.table().name());
            if let Some(key) = key {
                description.push(' ');
                description.push_str(&key);
            }
            if op.indirect() {
                description.push_str(" (indirect)");
            }
            description
        })
        .collect()
}

/// Apply every live patch that arrives, waiting up to `first` for the first
/// and draining until the stream goes quiet, recording the docs operations.
async fn drain_live_ops(
    client: &mut Client,
    applier: &Materializer,
    replica: &mut SqliteConnection,
    docs_ops: &mut Vec<String>,
    first: Duration,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut patch = client.try_live(first).await;
    while let Some(live) = patch {
        assert!(
            tokio::time::Instant::now() < deadline,
            "live patches kept arriving past the deadline, the stream never went quiet"
        );
        applier
            .apply_diffset(&live.patchset_zstd, replica)
            .expect("apply the live patch");
        if live.sub_id == DOCS_SUB {
            docs_ops.extend(patch_ops(&live.patchset_zstd));
        }
        patch = client.try_live(Duration::from_millis(500)).await;
    }
}

/// The flip scenario on the shipped path: the replica holds the doc, one
/// transaction flips the visibility with no column change of the subscribed
/// table, and the deployment needs the replica to lose the doc, the mirror
/// to follow, and a later visible insert to keep arriving on a stream that
/// never restarts.
async fn flip_scenario(label: &str, statements: &[FlipStatement]) {
    let fixture = Fixture::acquire().await;
    let server = flip_stack(&fixture).await;
    let writer = fixture.admin();
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;

    let applier = Materializer::builder(FLIP_DDL)
        .build()
        .expect("build the applier");
    let mut replica = client_replica(FLIP_SQLITE);
    subscribe_terms(&mut client, &applier, &mut replica).await;
    assert_eq!(
        replica_doc_ids(&mut replica),
        vec![1],
        "the snapshot holds the doc"
    );
    assert_eq!(
        replica_membership_ids(&mut replica),
        vec![1],
        "the mirror holds the caller's own membership row"
    );

    flip_transaction(writer, statements).await;
    let mut docs_ops = Vec::new();
    drain_live_ops(
        &mut client,
        &applier,
        &mut replica,
        &mut docs_ops,
        Duration::from_secs(5),
    )
    .await;

    // A later visible insert, the row and the share it rides on.
    fixture
        .exec("INSERT INTO docs (id, body) VALUES (2, 'world')")
        .await;
    fixture
        .exec("INSERT INTO members (doc_id, viewer) VALUES (2, 'alice')")
        .await;
    drain_live_ops(
        &mut client,
        &applier,
        &mut replica,
        &mut docs_ops,
        Duration::from_secs(5),
    )
    .await;

    eprintln!("{label}: the docs ops through the flip and the later insert: {docs_ops:?}");
    let rows = replica_doc_ids(&mut replica);
    assert_eq!(
        rows,
        vec![2],
        "the replica lost the doc the transaction took away and the later visible insert \
         arrived; it holds {rows:?} instead"
    );
    assert_eq!(
        replica_membership_ids(&mut replica),
        vec![2],
        "the mirror followed the membership, the caller's row left and the new one arrived"
    );
    assert!(
        docs_ops.iter().any(|op| op == "delete docs 1 (indirect)"),
        "the withdrawal rode the membership move-out's indirect delete, and the docs \
         ops were {docs_ops:?}"
    );
    assert!(
        docs_ops
            .iter()
            .filter(|op| op.starts_with("delete docs"))
            .count()
            <= 1,
        "each withdrawal of the doc reached the wire at most once, and the docs ops \
         were {docs_ops:?}"
    );
    assert_eq!(
        server.ingest_restarts(),
        0,
        "the flip took the shared stream down; it restarted {} times",
        server.ingest_restarts()
    );
}

/// An update that changes no column of the subscribed table while a
/// membership row leaves in the same transaction, the update first. The
/// deployment needs the server to withdraw the row anyway, on the shipped
/// authorization path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_no_column_visibility_flip_must_withdraw_the_row() {
    flip_scenario("the no-column flip, the update first", &FLIP_UPDATE_FIRST).await;
}

/// The same flip, the membership leaving before the no-op update, which the
/// row path answers against a store that already moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_no_column_flip_withdraws_the_row_when_the_membership_leaves_first() {
    flip_scenario(
        "the no-column flip, the membership first",
        &FLIP_DELETE_FIRST,
    )
    .await;
}

/// The control: the membership leaving on its own, no statement of the
/// subscribed table in the transaction at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_membership_leave_alone_withdraws_the_row() {
    flip_scenario("the membership leave alone", &FLIP_DELETE_ALONE).await;
}
