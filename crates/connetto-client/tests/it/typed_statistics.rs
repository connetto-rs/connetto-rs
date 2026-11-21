//! R117: server-computed statistics decode into the application's own types.
//!
//! A `uuid` group key, a `bytea` group key whose two values differ only in
//! invalid UTF-8, and the maximum of a `timestamptz` column each arrive as
//! `rosetta_uuid::Uuid`, `Vec<u8>` and `rosetta_utc::TimestampUTC` on the
//! first subscribe, after a reconnect, and after an offline restart. Every
//! resting cell holds the same bytes, with the same storage class, as the
//! replica row of the column it comes from.
//!
//! Needs Docker: the fixture starts its own Postgres.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use connetto_client::{
    ClientBuilder, ConnettoClient, DataDir, Grant, LiveGroups, LiveQuery, LiveValue,
    MemoryRefreshStore, ReconnectPolicy,
};
use connetto_core::test_support::TestGrantChecker;
use connetto_server::{
    LoopbackTransport, ManagerBuilder, Materializer, PgReadConnector, PgSnapshotSource,
    RuntimeWritableCatalog, SessionManager, loopback, pg_write_target,
};
use connetto_test_harness::{ConnettoDefaults, Fixture, RosterAuth, committed_at};
use diesel::prelude::*;
use diesel::sql_types::{Binary, Integer, Text};
use rosetta_utc::TimestampUTC;
use rosetta_uuid::Uuid;
use subql::{CdcSource, PgSqliteEmuSource, SourceItem};
use tokio::sync::Mutex;
use tokio::time::Instant;

const PG_DDL: &str = "-- connetto:major 1\nCREATE TABLE readings (id INT PRIMARY KEY, sensor UUID \
                      NOT NULL, tag BYTEA NOT NULL, observed TIMESTAMPTZ NOT NULL);";
// The emulator refuses a `UUID` column without a chosen representation, and
// a `BYTEA` one carries the same sixteen bytes on the change stream.
const EMU_DDL: &str = "-- connetto:major 1\nCREATE TABLE readings (id INT PRIMARY KEY, sensor \
                       BYTEA NOT NULL, tag BYTEA NOT NULL, observed TIMESTAMPTZ NOT NULL);";
const SQLITE_DDL: &str = "CREATE TABLE IF NOT EXISTS readings (id INTEGER PRIMARY KEY, sensor \
                          BLOB NOT NULL, tag BLOB NOT NULL, observed TEXT NOT NULL);";

const FIRST_SENSOR: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
const SECOND_SENSOR: &str = "ffeeddcc-bbaa-9988-7766-554433221100";

diesel::table! {
    readings (id) {
        id -> BigInt,
        sensor -> rosetta_uuid::sql_types::Uuid,
        tag -> Binary,
        observed -> rosetta_utc::diesel_impls::TimestampUTC,
    }
}

/// One replica row, decoded through the same application types the
/// statistics use.
#[derive(Debug, Clone, PartialEq, Queryable)]
struct Reading {
    id: i64,
    sensor: Uuid,
    tag: Vec<u8>,
    observed: TimestampUTC,
}

/// One stored cell read as raw bytes beside its SQLite storage class.
#[derive(Debug, PartialEq, Eq, QueryableByName)]
struct StoredCell {
    #[diesel(sql_type = Binary)]
    bytes: Vec<u8>,
    #[diesel(sql_type = Text)]
    class: String,
}

/// A recorded query text's row id.
#[derive(QueryableByName)]
struct QueryId {
    #[diesel(sql_type = Integer)]
    id: i32,
}

/// A manager whose computed subscriptions read through connetto's connector.
type Manager = SessionManager<PgSnapshotSource, RosterAuth, ConnettoDefaults, PgReadConnector>;

fn manager(fixture: &Fixture) -> Arc<Manager> {
    let pool = fixture.admin().clone();
    ManagerBuilder::new(
        Materializer::builder(PG_DDL)
            .with_write_catalog(RuntimeWritableCatalog::default())
            .with_read_connector(PgReadConnector::with_session_setup(pool.clone()))
            .build()
            .expect("build materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), PG_DDL).expect("snapshot source"),
        RosterAuth::granting("token"),
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(pool.clone()),
        pg_write_target::<ConnettoDefaults>(pool, PG_DDL).expect("build write target"),
    )
    .build()
}

/// The latest serve task, so the test can end the live session.
type ServeSlot = Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>;

/// A transport factory serving each dial on `manager`, failing while
/// `offline` is set so the outage lasts exactly as long as the test wants.
fn session_factory(
    manager: Arc<Manager>,
    slot: ServeSlot,
    offline: Arc<AtomicBool>,
) -> impl FnMut() -> std::pin::Pin<Box<dyn Future<Output = Result<LoopbackTransport, String>> + Send>>
{
    move || {
        let manager = Arc::clone(&manager);
        let slot = Arc::clone(&slot);
        let offline = Arc::clone(&offline);
        Box::pin(async move {
            // Relaxed: a test toggle, no ordering dependency.
            if offline.load(Ordering::Relaxed) {
                return Err("offline".to_owned());
            }
            let (server_end, client_end) = loopback();
            let handle = tokio::spawn(async move {
                let _ = manager.serve(server_end).await;
            });
            *slot.lock().await = Some(handle);
            Ok(client_end)
        })
    }
}

/// Run the statement `statement` writes in Postgres's spelling on the
/// fixture and in the emulator's on the emulated source, then dispatch every
/// event the source produced, so a later re-read and the change stream agree.
async fn apply(
    fixture: &Fixture,
    source: &mut PgSqliteEmuSource,
    manager: &Arc<Manager>,
    statement: fn(&Spelling) -> String,
) {
    fixture.exec(&statement(&POSTGRES)).await;
    source
        .execute_sql(&statement(&EMULATOR))
        .expect("execute dml");
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

/// How one engine spells a sensor and a tag literal.
struct Spelling {
    sensor: fn(&str) -> String,
    tag: fn(&str) -> String,
}

/// The fixture's `uuid` and `bytea` literals.
const POSTGRES: Spelling = Spelling {
    sensor: postgres_sensor,
    tag: postgres_tag,
};

/// The emulator runs its statements on SQLite, where both columns are blobs.
const EMULATOR: Spelling = Spelling {
    sensor: emulator_sensor,
    tag: emulator_tag,
};

fn postgres_sensor(uuid: &str) -> String {
    format!("'{uuid}'")
}

fn postgres_tag(hex: &str) -> String {
    format!("'\\x{hex}'")
}

fn emulator_sensor(uuid: &str) -> String {
    format!("X'{}'", uuid.replace('-', ""))
}

fn emulator_tag(hex: &str) -> String {
    format!("X'{hex}'")
}

/// The three seeded readings.
fn seed(spelling: &Spelling) -> String {
    let first = (spelling.sensor)(FIRST_SENSOR);
    let second = (spelling.sensor)(SECOND_SENSOR);
    let low = (spelling.tag)("80");
    let high = (spelling.tag)("81");
    format!(
        "INSERT INTO readings VALUES \
         (1, {first}, {low}, '2026-09-23 15:42:07.957522+02'), \
         (2, {first}, {high}, '2026-09-22 13:42:07.957522+00'), \
         (3, {second}, {low}, '2026-09-21 00:00:00+00')"
    )
}

/// The reading that lands while the client is offline.
fn late_reading(spelling: &Spelling) -> String {
    format!(
        "INSERT INTO readings VALUES (4, {}, {}, '2026-09-24 08:00:00+00')",
        (spelling.sensor)(SECOND_SENSOR),
        (spelling.tag)("81"),
    )
}

/// The three statistics and the replica rows they are compared against.
struct Watched {
    sensors: LiveGroups<Uuid, i64>,
    tags: LiveGroups<Vec<u8>, i64>,
    latest: LiveValue<Option<TimestampUTC>>,
    rows: LiveQuery<Reading>,
}

/// What every handle of [`Watched`] must show at one stage.
struct Expected {
    sensors: HashMap<Uuid, i64>,
    tags: HashMap<Vec<u8>, i64>,
    latest: TimestampUTC,
    latest_id: i64,
    rows: usize,
}

async fn watch(client: &ConnettoClient<LoopbackTransport>) -> Watched {
    Watched {
        sensors: client
            .watch_groups(
                readings::table
                    .group_by(readings::sensor)
                    .select((readings::sensor, diesel::dsl::count_star())),
            )
            .await
            .expect("watch sensor groups"),
        tags: client
            .watch_groups(
                readings::table
                    .group_by(readings::tag)
                    .select((readings::tag, diesel::dsl::count_star())),
            )
            .await
            .expect("watch tag groups"),
        latest: client
            .watch_value(readings::table.select(diesel::dsl::max(readings::observed)))
            .await
            .expect("watch latest"),
        rows: client
            .watch(readings::table.order(readings::id))
            .await
            .expect("watch rows"),
    }
}

/// Await `watched` reaching `expected`, bounded so a wedged path fails
/// rather than hangs.
async fn settle(watched: &mut Watched, expected: &Expected, stage: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let timed_out = |what: &str| format!("{stage}: {what} timed out");
    while watched.sensors.map() != expected.sensors {
        tokio::time::timeout_at(deadline, watched.sensors.changed())
            .await
            .unwrap_or_else(|_| panic!("{}", timed_out("sensor groups")))
            .expect("driver alive");
    }
    while watched.tags.map() != expected.tags {
        tokio::time::timeout_at(deadline, watched.tags.changed())
            .await
            .unwrap_or_else(|_| panic!("{}", timed_out("tag groups")))
            .expect("driver alive");
    }
    while watched.latest.value() != Some(Some(expected.latest)) {
        tokio::time::timeout_at(deadline, watched.latest.changed())
            .await
            .unwrap_or_else(|_| panic!("{}", timed_out("latest observation")))
            .expect("driver alive");
    }
    while watched.rows.rows().len() != expected.rows {
        tokio::time::timeout_at(deadline, watched.rows.changed())
            .await
            .unwrap_or_else(|_| panic!("{}", timed_out("replica rows")))
            .expect("driver alive");
    }
}

/// Assert `watched` shows `expected` at once, with no server to wait on.
fn assert_shows(watched: &Watched, expected: &Expected, stage: &str) {
    assert_eq!(watched.sensors.map(), expected.sensors, "{stage}: sensors");
    assert_eq!(watched.tags.map(), expected.tags, "{stage}: tags");
    assert_eq!(
        watched.latest.value(),
        Some(Some(expected.latest)),
        "{stage}: latest observation"
    );
    assert_eq!(watched.rows.rows().len(), expected.rows, "{stage}: rows");
}

/// The cells of the first data column of the statistic whose recorded text
/// contains `needle`, in byte order.
fn resting_cells(conn: &mut SqliteConnection, needle: &str) -> Vec<StoredCell> {
    let ids: Vec<QueryId> =
        diesel::sql_query("SELECT id FROM _connetto_query WHERE instr(sql, ?) > 0")
            .bind::<Text, _>(needle)
            .load(conn)
            .expect("read recorded queries");
    let [QueryId { id }] = ids.as_slice() else {
        panic!("expected one recorded query containing {needle}");
    };
    diesel::sql_query(format!(
        "SELECT c0 AS bytes, typeof(c0) AS class FROM _connetto_stat_{id} ORDER BY c0"
    ))
    .load(conn)
    .expect("read resting cells")
}

/// The distinct cells of the replica column `column`, in byte order.
fn replica_cells(conn: &mut SqliteConnection, column: &str) -> Vec<StoredCell> {
    diesel::sql_query(format!(
        "SELECT DISTINCT {column} AS bytes, typeof({column}) AS class FROM readings ORDER BY {column}"
    ))
    .load(conn)
    .expect("read replica cells")
}

/// The replica's `observed` cell of row `id`.
fn replica_observed(conn: &mut SqliteConnection, id: i64) -> Vec<StoredCell> {
    diesel::sql_query(
        "SELECT observed AS bytes, typeof(observed) AS class FROM readings WHERE id = ?",
    )
    .bind::<diesel::sql_types::BigInt, _>(id)
    .load(conn)
    .expect("read replica observation")
}

/// Assert every resting statistic holds exactly the replica's bytes for the
/// column it comes from.
async fn assert_byte_identical(
    client: &ConnettoClient<LoopbackTransport>,
    expected: &Expected,
    stage: &str,
) {
    let latest_id = expected.latest_id;
    let (sensors, tags, latest) = client
        .with_conn(move |conn| {
            let conn = conn.conn();
            (
                (
                    resting_cells(conn, "GROUP BY `readings`.`sensor`"),
                    replica_cells(conn, "sensor"),
                ),
                (
                    resting_cells(conn, "GROUP BY `readings`.`tag`"),
                    replica_cells(conn, "tag"),
                ),
                (
                    resting_cells(conn, "max(`readings`.`observed`)"),
                    replica_observed(conn, latest_id),
                ),
            )
        })
        .await
        .expect("the client is not locked");
    assert_eq!(
        sensors.0, sensors.1,
        "{stage}: sensor keys rest as the replica stores them"
    );
    assert_eq!(
        tags.0, tags.1,
        "{stage}: tag keys rest as the replica stores them"
    );
    assert_eq!(
        latest.0, latest.1,
        "{stage}: the maximum rests as the replica stores it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_statistics_decode_on_subscribe_reconnect_and_restart() {
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS readings CASCADE").await;
    fixture
        .exec(
            "CREATE TABLE readings (id INT PRIMARY KEY, sensor UUID NOT NULL, tag BYTEA NOT NULL, \
             observed TIMESTAMPTZ NOT NULL)",
        )
        .await;
    fixture.exec(&seed(&POSTGRES)).await;
    let mut source = PgSqliteEmuSource::open_in_memory(EMU_DDL).expect("open emu source");
    source
        .execute_sql(&seed(&EMULATOR))
        .expect("seed emu source");
    source.drain().expect("drain emu source");

    let first: Uuid = FIRST_SENSOR.parse().expect("first sensor");
    let second: Uuid = SECOND_SENSOR.parse().expect("second sensor");
    let seeded = Expected {
        sensors: HashMap::from([(first, 2), (second, 1)]),
        tags: HashMap::from([(vec![0x80], 2), (vec![0x81], 1)]),
        latest: "2026-09-23T13:42:07.957522Z".parse().expect("timestamp"),
        latest_id: 1,
        rows: 3,
    };

    let manager = manager(&fixture);
    let slot: ServeSlot = Arc::new(Mutex::new(None));
    let offline = Arc::new(AtomicBool::new(false));
    let dir = tempfile::tempdir().expect("temp dir");

    let (client, pump) = ClientBuilder::new(
        super::support::bundle(SQLITE_DDL),
        session_factory(
            Arc::clone(&manager),
            Arc::clone(&slot),
            Arc::clone(&offline),
        ),
    )
    .with_reconnect(
        ReconnectPolicy::new()
            .with_initial_backoff(Duration::from_millis(1))
            .with_max_backoff(Duration::from_millis(20)),
    )
    .with_sleeper(|duration| tokio::time::sleep(duration))
    .signed_in(credential())
    .durable(
        DataDir::new(dir.path().to_path_buf()),
        super::support::key_store(&credential()).await,
        MemoryRefreshStore::default(),
    )
    .connect_with_pump()
    .await
    .expect("client connect");
    let pump = tokio::spawn(pump);

    let mut watched = watch(client.client()).await;
    settle(&mut watched, &seeded, "first subscribe").await;
    assert_eq!(
        watched.rows.rows()[0],
        Reading {
            id: 1,
            sensor: first,
            tag: vec![0x80],
            observed: seeded.latest,
        },
        "the replica row decodes into the same types",
    );
    assert_byte_identical(client.client(), &seeded, "first subscribe").await;

    // The session ends, and a reading lands while the client cannot dial.
    offline.store(true, Ordering::Relaxed);
    end_session(&slot).await;
    apply(&fixture, &mut source, &manager, late_reading).await;
    offline.store(false, Ordering::Relaxed);

    let reconnected = Expected {
        sensors: HashMap::from([(first, 2), (second, 2)]),
        tags: HashMap::from([(vec![0x80], 2), (vec![0x81], 2)]),
        latest: "2026-09-24T08:00:00Z".parse().expect("timestamp"),
        latest_id: 4,
        rows: 4,
    };
    settle(&mut watched, &reconnected, "after reconnect").await;
    assert_byte_identical(client.client(), &reconnected, "after reconnect").await;

    drop(watched);
    client.close().await;
    pump.await.expect("first pump ends");
    end_session(&slot).await;

    restart_offline(dir.path(), &reconnected).await;
}

/// The credential every open of the test's replica presents.
fn credential() -> connetto_client::HeldCredential {
    super::support::held_grant(Grant::new("user:token#typed"), "token")
}

/// End the live server session, so the client sees its transport close.
async fn end_session(slot: &ServeSlot) {
    if let Some(handle) = slot.lock().await.take() {
        handle.abort();
        let _ = handle.await;
    }
}

/// Reopen the replica in `dir` with no server reachable and assert every
/// statistic shows `expected` from its resting table.
async fn restart_offline(dir: &std::path::Path, expected: &Expected) {
    let conn = ClientBuilder::new(
        super::support::bundle(SQLITE_DDL),
        super::support::NeverDial::<LoopbackTransport>::default(),
    )
    .signed_in(credential())
    .durable(
        DataDir::new(dir.to_path_buf()),
        super::support::key_store(&credential()).await,
        MemoryRefreshStore::default(),
    )
    .open_driven()
    .await
    .expect("reopen offline");
    assert!(!conn.is_connected(), "the restart reaches no server");
    let (client, pump) = ConnettoClient::with_pump(conn);
    let pump = tokio::spawn(pump);
    let watched = watch(&client).await;
    assert_shows(&watched, expected, "after an offline restart");
    assert_byte_identical(&client, expected, "after an offline restart").await;
    drop(watched);
    drop(client);
    pump.await.expect("second pump ends");
}
