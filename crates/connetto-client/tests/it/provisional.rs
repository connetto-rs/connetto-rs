//! The provisional bookkeeping of peer-applied rows.
//!
//! Provenance rows are written straight into `_connetto_provisional`, so what
//! is proved here is what the bookkeeping does with them: the eviction passes
//! spare the rows that are still provisional, a server write on a key
//! confirms it with one event per apply, the reads stand the rows back from
//! the table, and a provenance row never outlives the replica row it
//! describes.

use std::fmt::Write as _;
use std::time::Duration;

use connetto_client::provisional::{ProvisionalRow, ProvisionalState, encode_key};
use connetto_client::{
    ClientBuilder, ClientError, ClientEvent, ConnettoConnection, DataDir, Gate, GateAskOutcome,
    KeyValue, MemoryRefreshStore, Moment, NativeClientBuilder, ReconnectPolicy, SyncSchema,
    SystemClock,
};

use connetto_core::Cursor;
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, LivePatch, SnapshotBegin,
    SnapshotEnd, SnapshotPatch, SubscriptionPriority,
};
use connetto_core::traits::IncomingFrame;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_diff_rs::{DiffOps, Insert, PatchSet, PatchsetFormat, SimpleTable, Update, Value};

use super::support::{self, Script};

const DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT); \
                   CREATE TABLE parts (name TEXT, size BLOB, label TEXT, \
                   PRIMARY KEY (name, size)); \
                   CREATE TABLE reals (id REAL PRIMARY KEY, label TEXT);";

const AUTHOR: &str = "r77-peer-author";
const ACCOUNT: &str = "peer@account";
const RECEIVED_AT: i64 = 1_700_000_000;
const SEQ: i64 = 7;

diesel::table! {
    /// The synced orders table, read off the replica.
    orders (id) {
        /// Primary key.
        id -> BigInt,
        /// Free-text payload.
        status -> Nullable<Text>,
    }
}

diesel::table! {
    /// The synced reals table, a real primary key.
    reals (id) {
        /// Primary key.
        id -> Double,
        /// Free-text payload.
        label -> Nullable<Text>,
    }
}

/// One row of the provenance table, read raw because `_connetto_provisional`
/// is an internal table the typed schema does not name.
#[derive(diesel::QueryableByName, Debug, Clone, PartialEq)]
struct ProvenanceRecord {
    #[diesel(sql_type = diesel::sql_types::Text)]
    tbl: String,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    pk: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    author: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    seq: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    account: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    received_at: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    state: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Binary>)]
    displaced_author: Option<Vec<u8>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    displaced_seq: Option<i64>,
}

/// The replica's provenance rows as stored, keyed order.
fn stored_provenance<T>(conn: &mut ConnettoConnection<T>) -> Vec<ProvenanceRecord>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::sql_query(
        "SELECT tbl, pk, author, seq, account, received_at, state, \
         displaced_author, displaced_seq FROM _connetto_provisional ORDER BY tbl, pk",
    )
    .load(conn.conn())
    .expect("read the provenance table")
}

/// Write one provenance row straight into the replica.
fn record_provenance<T>(
    conn: &mut ConnettoConnection<T>,
    table: &str,
    key: &[KeyValue],
    state: ProvisionalState,
    displaced: Option<(&str, i64)>,
) where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    let author: Vec<u8> = AUTHOR.bytes().collect();
    let (displaced_author, displaced_seq) = match displaced {
        Some((other, seq)) => (Some(other.bytes().collect::<Vec<u8>>()), Some(seq)),
        None => (None, None),
    };
    conn.conn()
        .batch_execute(&format!(
            "INSERT INTO _connetto_provisional \
             (tbl, pk, author, seq, account, received_at, state) \
             VALUES ('{table}', X'{pk}', X'{author}', {SEQ}, '{ACCOUNT}', {RECEIVED_AT}, {state})",
            pk = hex(&encode_key(key)),
            author = hex(&author),
            state = state.db(),
        ))
        .expect("insert the provenance row");
    conn.conn()
        .batch_execute(&format!(
            "UPDATE _connetto_provisional SET displaced_author = {author}, displaced_seq = {seq} \
             WHERE tbl = '{table}'",
            author = displaced_author
                .map_or_else(|| "NULL".to_owned(), |bytes| format!("X'{}'", hex(&bytes))),
            seq = displaced_seq.map_or_else(|| "NULL".to_owned(), |seq| seq.to_string()),
        ))
        .expect("set the displaced record");
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            write!(out, "{byte:02X}").expect("write into a String");
            out
        })
}

/// Write one provenance row straight into the replica with the raw stored
/// `state` value and `pk` bytes, so a test can name a state or a key the
/// table does not define.
fn raw_record<T>(conn: &mut ConnettoConnection<T>, table: &str, pk: &str, state: i64)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    let author: Vec<u8> = AUTHOR.bytes().collect();
    conn.conn()
        .batch_execute(&format!(
            "INSERT INTO _connetto_provisional \
             (tbl, pk, author, seq, account, received_at, state) \
             VALUES ('{table}', X'{pk}', X'{author}', {SEQ}, '{ACCOUNT}', {RECEIVED_AT}, {state})",
            author = hex(&author),
        ))
        .expect("insert the provenance row");
}

/// A compressed insert patchset over `orders`.
fn orders_payload(rows: &[(i64, &str)]) -> Vec<u8> {
    let mut patch = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for (id, status) in rows {
        let insert = Insert::<_, String, Vec<u8>>::from(SimpleTable::new(
            "orders",
            &[("id", ""), ("status", "")],
            &[0],
        ))
        .set(0, Value::<String, Vec<u8>>::Integer(*id))
        .expect("set the key")
        .set(1, Value::<String, Vec<u8>>::Text(status.to_string()))
        .expect("set the status");
        patch = patch.insert(insert);
    }
    zstd::encode_all(patch.build().as_slice(), 3).expect("compress the patch")
}

/// A compressed insert patchset over `reals`.
fn reals_payload(rows: &[(f64, &str)]) -> Vec<u8> {
    let mut patch = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for (id, label) in rows {
        let insert = Insert::<_, String, Vec<u8>>::from(SimpleTable::new(
            "reals",
            &[("id", ""), ("label", "")],
            &[0],
        ))
        .set(0, Value::<String, Vec<u8>>::Real(*id))
        .expect("set the key")
        .set(1, Value::<String, Vec<u8>>::Text(label.to_string()))
        .expect("set the label");
        patch = patch.insert(insert);
    }
    zstd::encode_all(patch.build().as_slice(), 3).expect("compress the patch")
}

/// The `reals` table's ids, key order.
fn real_ids<T>(conn: &mut ConnettoConnection<T>) -> Vec<f64>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    reals::table
        .select(reals::id)
        .order(reals::id.asc())
        .load(conn.conn())
        .expect("read the reals table")
}

/// A compressed one-row update patchset over `table`, the row's new column
/// values, the patchset form carrying no old values.
fn update_payload(table: &str, columns: &[&str], new: Vec<Value<String, Vec<u8>>>) -> Vec<u8> {
    let schema = SimpleTable::new(
        table,
        &columns.iter().map(|name| (*name, "")).collect::<Vec<_>>(),
        &[0],
    );
    let mut update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(schema);
    for (position, value) in new.into_iter().enumerate() {
        update = update.set(position, value).expect("set the update");
    }
    zstd::encode_all(
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .update(update)
            .build()
            .as_slice(),
        3,
    )
    .expect("compress the patch")
}

fn cursor_byte(byte: u8) -> Cursor {
    Cursor::new(vec![0, 0, 0, 0, 0, 0, 0, byte])
}

/// Open a connected replica whose scripted server answers the handshake and
/// nothing else until a test queues frames.
async fn connected() -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    let conn = ClientBuilder::new(support::bundle(DDL), support::Once::new(script.clone()))
        .signed_in(support::held("r77"))
        .connect_driven()
        .await
        .expect("connect");
    (conn, script)
}

/// A connected replica over the shared-store schema, its `vocabulary` table
/// living in the shared tier.
async fn connected_shared() -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    let schema = SyncSchema::new(
        connetto_schema::translate_shared::<String, connetto_core::bots::TextIds>(
            "-- connetto:major 1\nCREATE TABLE vocabulary (id INT PRIMARY KEY, label TEXT NOT NULL); \
             CREATE TABLE notes (id INT PRIMARY KEY, vocabulary_id INT NOT NULL, \
              owner TEXT NOT NULL); \
             ALTER TABLE vocabulary ENABLE ROW LEVEL SECURITY; \
             ALTER TABLE notes ENABLE ROW LEVEL SECURITY;",
            "CREATE POLICY vocabulary_read ON vocabulary FOR SELECT USING (id > 0); \
             CREATE POLICY notes_read ON notes FOR SELECT USING \
             (owner = current_setting('app.user_id', true));",
            &["vocabulary"],
        )
        .expect("translate the shared vocabulary"),
    );
    let conn = ClientBuilder::new(schema, support::Once::new(script.clone()))
        .signed_in(support::held("r77"))
        .connect_driven()
        .await
        .expect("connect");
    (conn, script)
}

fn send_snapshot(server: &Script, sub_id: &str, payload: Vec<u8>, cursor: u8) {
    server.say(IncomingFrame::Control(ControlMessage::SnapshotBegin(
        SnapshotBegin {
            sub_id: sub_id.to_owned(),
            priority: SubscriptionPriority::default(),
        },
    )));
    server.say(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(
        SnapshotPatch::new(sub_id.to_owned(), payload),
    )));
    server.say(IncomingFrame::Control(ControlMessage::SnapshotEnd(
        SnapshotEnd {
            sub_id: sub_id.to_owned(),
            cursor: cursor_byte(cursor),
        },
    )));
}

/// Pump until `pred` matches, refusing to outlive the wire.
async fn pump_to<T>(
    conn: &mut ConnettoConnection<T>,
    pred: impl Fn(&ClientEvent) -> bool,
) -> ClientEvent
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    loop {
        let event = conn.pump_one().await.expect("pump");
        assert!(!matches!(event, ClientEvent::Closed), "closed early");
        if pred(&event) {
            return event;
        }
    }
}

/// Drain events for a bounded quiet window, so a test can say what did and
/// did not arrive when the absence is the point.
async fn drain_quiet<T>(conn: &mut ConnettoConnection<T>, window: Duration) -> Vec<ClientEvent>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    let mut seen = Vec::new();
    loop {
        match conn
            .pump_one_or(tokio::time::sleep(window))
            .await
            .expect("pump")
        {
            Some(event) => seen.push(event),
            None => return seen,
        }
    }
}

fn order_ids<T>(conn: &mut ConnettoConnection<T>) -> Vec<i64>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    orders::table
        .select(orders::id)
        .order(orders::id.asc())
        .load(conn.conn())
        .expect("read the replica")
}

/// A subscription over `orders` holding only `id = 1`, and a row at 2 that a
/// frame still in flight past an ended subscription delivered, so no
/// subscription holds it and it is fair game for eviction.
async fn orders_fixture() -> (ConnettoConnection<Script>, Script) {
    let (mut conn, server) = connected().await;
    conn.subscribe("a", "SELECT * FROM orders WHERE id = 1")
        .await
        .expect("subscribe");
    send_snapshot(&server, "a", orders_payload(&[(1, "x")]), 1);
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "a"),
    )
    .await;
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "ended".to_owned(),
        cursor_byte(1),
        orders_payload(&[(2, "y")]),
    ))));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::LivePatch { .. })
    })
    .await;
    (conn, server)
}

/// A live patch writing a provisional key clears the provenance in the same
/// apply and raises one `ProvisionalConfirmed` carrying the row.
#[tokio::test]
async fn a_live_patch_on_a_provisional_key_confirms_it() {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "a".to_owned(),
        cursor_byte(2),
        update_payload(
            "orders",
            &["id", "status"],
            vec![Value::Integer(2), Value::Text("s".to_owned())],
        ),
    ))));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::LivePatch { .. })
    })
    .await;
    let ClientEvent::ProvisionalConfirmed { rows } = pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::ProvisionalConfirmed { .. })
    })
    .await
    else {
        panic!("the confirm never arrived");
    };
    let author: Vec<u8> = AUTHOR.bytes().collect();
    assert_eq!(
        rows,
        vec![ProvisionalRow {
            table: "orders".to_owned(),
            key: vec![KeyValue::Int(2)],
            author,
            seq: SEQ,
            account: ACCOUNT.to_owned(),
            received_at: RECEIVED_AT,
            state: ProvisionalState::Provisional,
            displaced_author: None,
            displaced_seq: None,
        }],
        "the confirmed row is the provenance as stored"
    );
    assert_eq!(stored_provenance(&mut conn), Vec::new());
}

/// A live patch writing another key confirms nothing and leaves the standing
/// provenance in place.
#[tokio::test]
async fn a_live_patch_on_another_key_leaves_provenance_intact() {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "a".to_owned(),
        cursor_byte(2),
        orders_payload(&[(3, "z")]),
    ))));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::LivePatch { .. })
    })
    .await;
    let quiet = drain_quiet(&mut conn, Duration::from_millis(200)).await;
    assert!(
        !quiet
            .iter()
            .any(|event| matches!(event, ClientEvent::ProvisionalConfirmed { .. })),
        "a write to another key confirms nothing"
    );
    assert_eq!(
        stored_provenance(&mut conn).len(),
        1,
        "the standing provenance is untouched"
    );
}

/// A snapshot chunk writing a provisional key confirms it the same way a live
/// patch does.
#[tokio::test]
async fn a_snapshot_write_on_a_provisional_key_confirms_it() {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.subscribe("b", "SELECT * FROM orders")
        .await
        .expect("subscribe b");
    send_snapshot(&server, "b", orders_payload(&[(4, "w")]), 3);
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "b"),
    )
    .await;
    // The second snapshot's cursor moves past the first's rows, so the key 2
    // row lands as an update against what the first snapshot stored.
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "b".to_owned(),
        cursor_byte(4),
        update_payload(
            "orders",
            &["id", "status"],
            vec![Value::Integer(2), Value::Text("s".to_owned())],
        ),
    ))));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::LivePatch { .. })
    })
    .await;
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::ProvisionalConfirmed { .. })
    })
    .await;
    assert_eq!(stored_provenance(&mut conn), Vec::new());
}

/// A truncated table resync takes the provisional row with the rest of the
/// table, and its provenance goes with it rather than outliving the row.
#[tokio::test]
async fn a_truncate_resync_drops_the_provenance_with_the_row() {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "a".to_owned(),
            reason: FullResyncReason::TableTruncated {
                table: "orders".to_owned(),
            },
        },
    )));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::FullResync { .. })
    })
    .await;
    send_snapshot(&server, "a", orders_payload(&[(1, "x"), (3, "z")]), 5);
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "a"),
    )
    .await;
    assert_eq!(
        order_ids(&mut conn),
        vec![1, 3],
        "the truncated table comes back as the server now holds it"
    );
    assert!(
        stored_provenance(&mut conn).is_empty(),
        "the provenance of a row the truncate took does not linger"
    );
}

/// The reads stand the rows back from the table, decoded to their typed keys,
/// and answer nothing for a key that holds no provenance.
#[tokio::test]
async fn reads_return_what_is_stored() {
    let (mut conn, _server) = connected().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        Some(("displaced-author", 9)),
    );
    record_provenance(
        &mut conn,
        "parts",
        &[KeyValue::Text("gone".to_owned()), KeyValue::Blob(vec![2])],
        ProvisionalState::Adjudicated,
        None,
    );
    let author: Vec<u8> = AUTHOR.bytes().collect();
    let displaced: Vec<u8> = "displaced-author".bytes().collect();
    let orders_row = ProvisionalRow {
        table: "orders".to_owned(),
        key: vec![KeyValue::Int(2)],
        author: author.clone(),
        seq: SEQ,
        account: ACCOUNT.to_owned(),
        received_at: RECEIVED_AT,
        state: ProvisionalState::Provisional,
        displaced_author: Some(displaced.clone()),
        displaced_seq: Some(9),
    };
    let parts_row = ProvisionalRow {
        table: "parts".to_owned(),
        key: vec![KeyValue::Text("gone".to_owned()), KeyValue::Blob(vec![2])],
        author,
        seq: SEQ,
        account: ACCOUNT.to_owned(),
        received_at: RECEIVED_AT,
        state: ProvisionalState::Adjudicated,
        displaced_author: None,
        displaced_seq: None,
    };
    assert_eq!(
        conn.provenance("orders", &[KeyValue::Int(2)])
            .expect("read the provenance"),
        Some(orders_row.clone())
    );
    assert_eq!(
        conn.provenance(
            "parts",
            &[KeyValue::Text("gone".to_owned()), KeyValue::Blob(vec![2])]
        )
        .expect("read the provenance"),
        Some(parts_row.clone())
    );
    assert_eq!(
        conn.provenance("orders", &[KeyValue::Int(99)])
            .expect("read the provenance"),
        None
    );
    assert_eq!(
        conn.provisional_rows().expect("read the provenance rows"),
        vec![orders_row, parts_row],
        "keyed order across tables"
    );
    let _ = displaced;
}

/// The shared tier keeps the same rule: a shared table's provisional row is
/// spared by the tidy pass and confirmed by a server write on the shared
/// route.
#[tokio::test]
async fn a_shared_table_provisional_row_survives_tidy_and_confirms() {
    let (mut conn, server) = connected_shared().await;
    conn.subscribe("a", "SELECT * FROM vocabulary WHERE id = 1")
        .await
        .expect("subscribe");
    send_snapshot(
        &server,
        "a",
        {
            let mut patch = PatchSet::<SimpleTable, String, Vec<u8>>::new();
            for (id, label) in [(1i64, "word-1"), (2, "word-2")] {
                let insert = Insert::<_, String, Vec<u8>>::from(SimpleTable::new(
                    "vocabulary",
                    &[("id", ""), ("label", "")],
                    &[0],
                ))
                .set(0, Value::<String, Vec<u8>>::Integer(id))
                .expect("set the key")
                .set(1, Value::<String, Vec<u8>>::Text(label.to_owned()))
                .expect("set the label");
                patch = patch.insert(insert);
            }
            zstd::encode_all(patch.build().as_slice(), 3).expect("compress")
        },
        1,
    );
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "a"),
    )
    .await;
    let physical = conn
        .policy_tables()
        .physical("vocabulary")
        .unwrap_or("vocabulary")
        .to_owned();
    record_provenance(
        &mut conn,
        &physical,
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.tidy().expect("tidy");
    let words: Vec<(i64, String)> = vocabulary::table
        .select((vocabulary::id, vocabulary::label))
        .order(vocabulary::id.asc())
        .load(conn.conn())
        .expect("read the shared store");
    assert_eq!(
        words,
        vec![(1, "word-1".to_owned()), (2, "word-2".to_owned())],
        "the shared row the filter does not name stays while its provenance is provisional"
    );
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "a".to_owned(),
        cursor_byte(2),
        update_payload(
            "vocabulary",
            &["id", "label"],
            vec![Value::Integer(2), Value::Text("word-2s".to_owned())],
        ),
    ))));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::LivePatch { .. })
    })
    .await;
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::ProvisionalConfirmed { .. })
    })
    .await;
    assert_eq!(stored_provenance(&mut conn), Vec::new());
}

/// The bound every prompt and event in a gated test has to arrive within.
const BOUND: Duration = Duration::from_secs(5);

/// The live client stands the provenance back from the table, naming the
/// table as the application writes it, and answers nothing for an absent key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_client_answers_provenance_from_the_table() {
    let script = Script::default();
    script.say(support::handshake_ack());
    let (running, pump) = ClientBuilder::new(support::bundle(DDL), support::Once::new(script))
        .signed_in(support::held("r77"))
        .connect_with_pump()
        .await
        .expect("the build connects");
    tokio::spawn(pump);
    let client = running.client().clone();
    client
        .with_conn(|conn| {
            record_provenance(
                conn,
                "orders",
                &[KeyValue::Int(2)],
                ProvisionalState::Provisional,
                Some(("r77-local-author", 3)),
            );
        })
        .await
        .expect("seed the provenance");
    let author: Vec<u8> = AUTHOR.bytes().collect();
    let row = ProvisionalRow {
        table: "orders".to_owned(),
        key: vec![KeyValue::Int(2)],
        author: author.clone(),
        seq: SEQ,
        account: ACCOUNT.to_owned(),
        received_at: RECEIVED_AT,
        state: ProvisionalState::Provisional,
        displaced_author: Some("r77-local-author".bytes().collect()),
        displaced_seq: Some(3),
    };
    for table in ["orders", "ORDERS"] {
        assert_eq!(
            client
                .provenance(table, &[KeyValue::Int(2)])
                .await
                .expect("read the provenance"),
            Some(row.clone()),
            "the table is named as the application writes it"
        );
    }
    assert_eq!(
        client
            .provenance("orders", &[KeyValue::Int(99)])
            .await
            .expect("read an absent key"),
        None,
        "an absent key holds no provenance"
    );
    assert_eq!(
        client
            .provisional_rows()
            .await
            .expect("list the provenance"),
        vec![row],
        "the listing stands the stored row back"
    );
}

/// A locked gate refuses the provenance reads rather than answer them stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_locked_gate_refuses_provenance_reads() {
    let script = Script::default();
    script.say(support::handshake_ack());
    let mechanism = support::LockedMechanism::new();
    let connecting = tokio::spawn({
        let mechanism = mechanism.clone();
        async move {
            let dir = tempfile::tempdir().expect("a temp dir");
            let credential = support::held("r77");
            let key_store = support::key_store(&credential).await;
            ClientBuilder::new(support::bundle(DDL), support::Once::new(script))
                .with_reconnect(ReconnectPolicy::default())
                .signed_in(credential)
                .durable(
                    DataDir::new(dir.path().to_path_buf()),
                    key_store,
                    MemoryRefreshStore::default(),
                )
                .with_gate(Gate::default().with_recheck(Some(Duration::ZERO)))
                .with_gate_mechanism(mechanism)
                .connect_with_pump()
                .await
        }
    });
    mechanism.until_asked(1).await;
    mechanism.resolve(GateAskOutcome::Approved);
    let (running, pump) = tokio::time::timeout(BOUND, connecting)
        .await
        .expect("the build returns once the launch prompt resolves")
        .expect("the connect task")
        .expect("the approved build connects");
    tokio::spawn(pump);
    let client = running.client().clone();
    let mut events = client.events();
    client.away(Moment::now(&SystemClock)).await;
    client.back(Moment::now(&SystemClock)).await;
    let mut locked = false;
    let until = tokio::time::Instant::now() + BOUND;
    while !locked {
        match tokio::time::timeout_at(until, events.recv()).await {
            Ok(Ok(ClientEvent::Locked)) => locked = true,
            Ok(Ok(_) | Err(_)) => {}
            Err(_) => break,
        }
    }
    assert!(
        locked,
        "the zero grace re-checks on the return, so the gate locks"
    );
    assert!(
        client
            .provenance("orders", &[KeyValue::Int(2)])
            .await
            .is_err_and(|err| matches!(err, ClientError::Locked)),
        "a locked gate refuses the provenance read"
    );
    assert!(
        client
            .provisional_rows()
            .await
            .is_err_and(|err| matches!(err, ClientError::Locked)),
        "a locked gate refuses the provenance listing"
    );
}

/// The native client stands the provenance back from the table, the same
/// rows the live client would.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_native_client_answers_provenance_from_the_table() {
    let script = Script::default();
    script.say(support::handshake_ack());
    let client = NativeClientBuilder::new("ws://127.0.0.1:1", support::bundle(DDL))
        .with_dialer(support::Once::new(script))
        .connect()
        .await
        .expect("the build connects");
    client
        .client()
        .with_conn(|conn| {
            record_provenance(
                conn,
                "orders",
                &[KeyValue::Int(2)],
                ProvisionalState::Provisional,
                None,
            );
        })
        .await
        .expect("seed the provenance");
    let author: Vec<u8> = AUTHOR.bytes().collect();
    let row = ProvisionalRow {
        table: "orders".to_owned(),
        key: vec![KeyValue::Int(2)],
        author,
        seq: SEQ,
        account: ACCOUNT.to_owned(),
        received_at: RECEIVED_AT,
        state: ProvisionalState::Provisional,
        displaced_author: None,
        displaced_seq: None,
    };
    assert_eq!(
        client
            .provenance("orders", &[KeyValue::Int(2)])
            .await
            .expect("read the provenance"),
        Some(row.clone()),
        "the table is named as the application writes it"
    );
    assert_eq!(
        client
            .provisional_rows()
            .await
            .expect("list the provenance"),
        vec![row],
        "the listing stands the stored row back"
    );
}

/// A stored state the table no longer defines refuses the reads rather than
/// stand a row back from nothing.
#[tokio::test]
async fn a_record_with_no_defined_state_fails_the_reads() {
    let (mut conn, _server) = connected().await;
    raw_record(
        &mut conn,
        "orders",
        &hex(&encode_key(&[KeyValue::Int(2)])),
        2,
    );
    let err = conn
        .provenance("orders", &[KeyValue::Int(2)])
        .expect_err("the stored state is undefined");
    assert!(
        err.to_string()
            .contains("holds no state the table defines: 2"),
        "the refusal names the state: {err}"
    );
    let err = conn
        .provisional_rows()
        .expect_err("the listing reads the same row");
    assert!(
        err.to_string()
            .contains("holds no state the table defines: 2"),
        "the listing refuses too: {err}"
    );
}

/// Mark every stored record adjudicated, its verdict in.
fn adjudicate_all<T>(conn: &mut ConnettoConnection<T>)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    conn.conn()
        .batch_execute(&format!(
            "UPDATE _connetto_provisional SET state = {}",
            ProvisionalState::Adjudicated.db()
        ))
        .expect("adjudicate the records");
}

/// A record whose key the table's layout cannot express names no row, so it
/// stays while it awaits its verdict, as a provisional delete does, and once
/// adjudicated the pass sweeps it.
#[tokio::test]
async fn a_key_the_table_cannot_express_goes_once_adjudicated() {
    let (mut conn, _server) = orders_fixture().await;
    raw_record(
        &mut conn,
        "orders",
        &hex(&encode_key(&[KeyValue::Int(9), KeyValue::Int(9)])),
        ProvisionalState::Provisional.db(),
    );
    conn.tidy().expect("tidy");
    assert_eq!(order_ids(&mut conn), vec![1]);
    assert_eq!(stored_provenance(&mut conn).len(), 1);

    adjudicate_all(&mut conn);
    conn.tidy().expect("tidy after the verdict");
    assert_eq!(
        stored_provenance(&mut conn),
        Vec::new(),
        "the mismatched record names no row"
    );
}

/// A record whose key the encoding cannot decode cannot spare its row, so the
/// row is evicted and the record is left alone by the passes.
#[tokio::test]
async fn an_undecodable_key_is_left_alone_by_the_passes() {
    let (mut conn, _server) = orders_fixture().await;
    raw_record(
        &mut conn,
        "orders",
        "09",
        ProvisionalState::Provisional.db(),
    );
    conn.tidy().expect("tidy");
    assert_eq!(order_ids(&mut conn), vec![1]);
    assert_eq!(
        stored_provenance(&mut conn).len(),
        1,
        "the undecodable record is not touched by the passes"
    );
}

/// An adjudicated record on an infinite `REAL` key is probed like any other:
/// the pass finds its row still stored and keeps the record beside it.
#[tokio::test]
async fn an_adjudicated_infinite_key_is_probed_like_any_other() {
    let (mut conn, server) = connected().await;
    conn.subscribe("a", "SELECT * FROM reals")
        .await
        .expect("subscribe");
    send_snapshot(
        &server,
        "a",
        reals_payload(&[(1.0, "x"), (f64::INFINITY, "y")]),
        1,
    );
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "a"),
    )
    .await;
    raw_record(
        &mut conn,
        "reals",
        &hex(&encode_key(&[KeyValue::Real(f64::INFINITY)])),
        ProvisionalState::Adjudicated.db(),
    );
    conn.tidy().expect("the pass probes an infinite key");
    assert_eq!(real_ids(&mut conn), vec![1.0, f64::INFINITY]);
    assert_eq!(
        stored_provenance(&mut conn).len(),
        1,
        "the record's row is stored, so the record stays"
    );
}

/// A record whose key column is null spares no row, since no row has a null
/// key column, so the uncovered row goes. The record stays until its verdict,
/// and once adjudicated the probe finds no row under it and sweeps it.
#[tokio::test]
async fn a_null_key_column_spares_nothing_and_goes_once_adjudicated() {
    let (mut conn, _server) = orders_fixture().await;
    raw_record(
        &mut conn,
        "orders",
        "00",
        ProvisionalState::Provisional.db(),
    );
    conn.tidy().expect("tidy");
    assert_eq!(order_ids(&mut conn), vec![1]);
    assert_eq!(stored_provenance(&mut conn).len(), 1);

    adjudicate_all(&mut conn);
    conn.tidy().expect("tidy after the verdict");
    assert_eq!(
        stored_provenance(&mut conn),
        Vec::new(),
        "the probe finds no row under a null key, so the record goes"
    );
}

/// A payload the replica cannot apply is refused before the cursor advances,
/// and the refusal leaves the rows and the provenance as they were.
#[tokio::test]
async fn a_payload_the_replica_cannot_apply_rolls_back() {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "a".to_owned(),
        cursor_byte(2),
        zstd::encode_all(b"not a patchset".as_slice(), 3).expect("compress"),
    ))));
    let err = conn
        .pump_one()
        .await
        .expect_err("the payload is not a patchset");
    assert!(
        err.to_string().contains("Failed to apply changeset"),
        "the refusal is the applier's: {err}"
    );
    assert_eq!(order_ids(&mut conn), vec![1, 2]);
    assert_eq!(stored_provenance(&mut conn).len(), 1);
}

/// A clear the provenance table refuses rolls the server write back with it,
/// so the write and the confirmation stand or fall together.
#[tokio::test]
async fn a_refused_clear_rolls_back_the_server_write() {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.conn()
        .batch_execute(
            "CREATE TEMP TRIGGER refuse_clear BEFORE DELETE ON _connetto_provisional \
             BEGIN SELECT RAISE(ABORT, 'the clear is refused'); END",
        )
        .expect("arm the refusal");
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "a".to_owned(),
        cursor_byte(2),
        update_payload(
            "orders",
            &["id", "status"],
            vec![Value::Integer(2), Value::Text("s".to_owned())],
        ),
    ))));
    let err = conn.pump_one().await.expect_err("the clear is refused");
    assert!(
        err.to_string().contains("the clear is refused"),
        "the refusal names the trigger: {err}"
    );
    let status: Option<String> = orders::table
        .find(2)
        .select(orders::status)
        .first(conn.conn())
        .expect("read the row");
    assert_eq!(
        status.as_deref(),
        Some("y"),
        "the server write rolled back with the clear"
    );
    assert_eq!(stored_provenance(&mut conn).len(), 1);
}

/// A confirm over a record whose state the table no longer defines rolls the
/// server write back, so a stale record cannot stand a confirmed row.
#[tokio::test]
async fn a_record_with_no_defined_state_fails_the_confirm() {
    let (mut conn, server) = orders_fixture().await;
    raw_record(
        &mut conn,
        "orders",
        &hex(&encode_key(&[KeyValue::Int(2)])),
        2,
    );
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "a".to_owned(),
        cursor_byte(2),
        update_payload(
            "orders",
            &["id", "status"],
            vec![Value::Integer(2), Value::Text("s".to_owned())],
        ),
    ))));
    let err = conn
        .pump_one()
        .await
        .expect_err("the stored state is undefined");
    assert!(
        err.to_string()
            .contains("holds no state the table defines: 2"),
        "the refusal names the state: {err}"
    );
    let status: Option<String> = orders::table
        .find(2)
        .select(orders::status)
        .first(conn.conn())
        .expect("read the row");
    let status = status.expect("the row is still there");
    assert_eq!(status.as_str(), "y");
    assert_eq!(stored_provenance(&mut conn).len(), 1);
}

/// A drop the provenance table refuses rolls the truncate back with it, so
/// the rows and their records are gone together or not at all.
#[tokio::test]
async fn a_refused_drop_rolls_back_the_truncate() {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.conn()
        .batch_execute(
            "CREATE TEMP TRIGGER refuse_drop BEFORE DELETE ON _connetto_provisional \
             BEGIN SELECT RAISE(ABORT, 'the drop is refused'); END",
        )
        .expect("arm the refusal");
    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "a".to_owned(),
            reason: FullResyncReason::TableTruncated {
                table: "orders".to_owned(),
            },
        },
    )));
    let err = conn.pump_one().await.expect_err("the drop is refused");
    assert!(
        err.to_string().contains("the drop is refused"),
        "the refusal names the trigger: {err}"
    );
    assert_eq!(order_ids(&mut conn), vec![1, 2]);
    assert_eq!(stored_provenance(&mut conn).len(), 1);
}

/// The orders fixture with a provisional record at 2 in a provenance table
/// renamed away, so the replica can no longer read it.
async fn unreadable_provenance_fixture() -> (ConnettoConnection<Script>, Script) {
    let (mut conn, server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.conn()
        .batch_execute("ALTER TABLE _connetto_provisional RENAME TO provisional_away")
        .expect("hide the provenance");
    (conn, server)
}

/// A tidy pass that cannot read the provenance fails, so it never evicts a
/// provisional row it could not see was spared.
#[tokio::test]
async fn an_unreadable_provenance_fails_tidy_before_evicting() {
    let (mut conn, _server) = unreadable_provenance_fixture().await;
    let err = conn.tidy().expect_err("the provenance cannot be read");
    assert!(
        err.to_string().contains("_connetto_provisional"),
        "the failure names the table: {err}"
    );
    assert_eq!(order_ids(&mut conn), vec![1, 2]);
}

/// The subscription-end eviction fails the same way, and the subscription
/// stays, so its rows and the provisional row are all still there.
#[tokio::test]
async fn an_unreadable_provenance_fails_the_subscription_end_eviction() {
    let (mut conn, _server) = unreadable_provenance_fixture().await;
    let err = conn
        .unsubscribe("a")
        .await
        .expect_err("the provenance cannot be read");
    assert!(
        err.to_string().contains("_connetto_provisional"),
        "the failure names the table: {err}"
    );
    assert_eq!(order_ids(&mut conn), vec![1, 2]);
}

/// The reads report an unreadable provenance as an error, never as a key
/// that holds no record.
#[tokio::test]
async fn an_unreadable_provenance_fails_the_reads() {
    let (mut conn, _server) = unreadable_provenance_fixture().await;
    assert!(
        conn.provenance("orders", &[KeyValue::Int(2)]).is_err(),
        "a key read cannot answer that the key holds no record"
    );
    assert!(
        conn.provisional_rows().is_err(),
        "the listing cannot answer that no record exists"
    );
}

/// A server write over an unreadable provenance rolls back, so the row is
/// never written without its confirmation.
#[tokio::test]
async fn an_unreadable_provenance_rolls_back_the_server_write() {
    let (mut conn, server) = unreadable_provenance_fixture().await;
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "a".to_owned(),
        cursor_byte(2),
        update_payload(
            "orders",
            &["id", "status"],
            vec![Value::Integer(2), Value::Text("s".to_owned())],
        ),
    ))));
    let err = conn
        .pump_one()
        .await
        .expect_err("the provenance cannot be read");
    assert!(
        err.to_string().contains("_connetto_provisional"),
        "the failure names the table: {err}"
    );
    let status: Option<String> = orders::table
        .find(2)
        .select(orders::status)
        .first(conn.conn())
        .expect("read the row");
    assert_eq!(status.as_deref(), Some("y"), "the server write rolled back");
}

/// A truncate over an unreadable provenance rolls back, so no record
/// outlives the rows it describes.
#[tokio::test]
async fn an_unreadable_provenance_rolls_back_the_truncate() {
    let (mut conn, server) = unreadable_provenance_fixture().await;
    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "a".to_owned(),
            reason: FullResyncReason::TableTruncated {
                table: "orders".to_owned(),
            },
        },
    )));
    let err = conn
        .pump_one()
        .await
        .expect_err("the provenance cannot be read");
    assert!(
        err.to_string().contains("_connetto_provisional"),
        "the failure names the table: {err}"
    );
    assert_eq!(order_ids(&mut conn), vec![1, 2]);
}

diesel::table! {
    /// The shared vocabulary table, read off the shared store.
    vocabulary (id) {
        /// Vocabulary key.
        id -> BigInt,
        /// Vocabulary label.
        label -> Text,
    }
}
