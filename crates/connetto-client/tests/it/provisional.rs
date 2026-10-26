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
use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection, KeyValue, SyncSchema};
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
                   PRIMARY KEY (name, size));";

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
    /// The synced parts table, composite text-plus-blob key.
    parts (name, size) {
        /// First key column.
        name -> Text,
        /// Second key column.
        size -> Binary,
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
            pk = hex(&encode_key(key).expect("encode the key")),
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

/// A compressed insert patchset over `parts`.
fn parts_payload(rows: &[(&str, &[u8], &str)]) -> Vec<u8> {
    let mut patch = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for (name, size, label) in rows {
        let insert = Insert::<_, String, Vec<u8>>::from(SimpleTable::new(
            "parts",
            &[("name", ""), ("size", ""), ("label", "")],
            &[0, 1],
        ))
        .set(0, Value::<String, Vec<u8>>::Text(name.to_string()))
        .expect("set the name")
        .set(1, Value::<String, Vec<u8>>::Blob(size.to_vec()))
        .expect("set the size")
        .set(2, Value::<String, Vec<u8>>::Text(label.to_string()))
        .expect("set the label");
        patch = patch.insert(insert);
    }
    zstd::encode_all(patch.build().as_slice(), 3).expect("compress the patch")
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

/// A snapshot plus subscription over `orders` covering only `id = 1`, so the
/// row at 2 is uncovered by every subscription and fair game for eviction.
async fn orders_fixture() -> (ConnettoConnection<Script>, Script) {
    let (mut conn, server) = connected().await;
    conn.subscribe("a", "SELECT * FROM orders WHERE id = 1")
        .await
        .expect("subscribe");
    send_snapshot(&server, "a", orders_payload(&[(1, "x"), (2, "y")]), 1);
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "a"),
    )
    .await;
    (conn, server)
}

/// An uncovered provisional row survives the tidy pass, whose keep list takes
/// its key from the provenance table.
#[tokio::test]
async fn an_uncovered_provisional_row_survives_tidy() {
    let (mut conn, _server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.tidy().expect("tidy");
    assert_eq!(
        order_ids(&mut conn),
        vec![1, 2],
        "the row the subscription does not cover stays while its provenance is provisional"
    );
    assert_eq!(
        stored_provenance(&mut conn).len(),
        1,
        "the provenance row stays beside the row it describes"
    );
}

/// Once the provenance stops saying provisional, the row is ordinary coverage
/// again and the pass takes it, with the provenance of the taken row removed.
#[tokio::test]
async fn a_provisional_row_is_evicted_once_it_is_no_longer_provisional() {
    let (mut conn, _server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.conn()
        .batch_execute(&format!(
            "UPDATE _connetto_provisional SET state = {state} WHERE tbl = 'orders'",
            state = ProvisionalState::Adjudicated.db()
        ))
        .expect("mark the provenance adjudicated");
    conn.tidy().expect("tidy");
    assert_eq!(
        order_ids(&mut conn),
        vec![1],
        "an adjudicated row is no longer spared"
    );
    assert!(
        stored_provenance(&mut conn).is_empty(),
        "the provenance of a row the pass took does not linger"
    );
}

/// A provisional row whose provenance is gone is no longer spared either.
#[tokio::test]
async fn a_provisional_row_is_evicted_once_its_provenance_is_gone() {
    let (mut conn, _server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.conn()
        .batch_execute("DELETE FROM _connetto_provisional")
        .expect("drop the provenance");
    conn.tidy().expect("tidy");
    assert_eq!(
        order_ids(&mut conn),
        vec![1],
        "without its provenance the uncovered row is ordinary coverage"
    );
}

/// The subscription-end eviction pass spares the provisional row and takes
/// the ended subscription's own row.
#[tokio::test]
async fn a_provisional_row_survives_the_subscription_end_eviction() {
    let (mut conn, _server) = orders_fixture().await;
    record_provenance(
        &mut conn,
        "orders",
        &[KeyValue::Int(2)],
        ProvisionalState::Provisional,
        None,
    );
    conn.unsubscribe("a").await.expect("unsubscribe");
    assert_eq!(
        order_ids(&mut conn),
        vec![2],
        "the ended subscription takes its own row and spares the provisional one"
    );
    assert_eq!(stored_provenance(&mut conn).len(), 1);
}

/// The spare clause is the same `(col IS literal)` shape the pending spare
/// builds, which is what a composite key with a text and a blob column checks.
#[tokio::test]
async fn a_composite_key_provisional_row_survives_tidy() {
    let (mut conn, server) = connected().await;
    conn.subscribe("p", "SELECT * FROM parts WHERE name = 'keep'")
        .await
        .expect("subscribe");
    send_snapshot(
        &server,
        "p",
        parts_payload(&[("keep", &[1], "k"), ("gone", &[2], "g")]),
        1,
    );
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "p"),
    )
    .await;
    record_provenance(
        &mut conn,
        "parts",
        &[KeyValue::Text("gone".to_owned()), KeyValue::Blob(vec![2])],
        ProvisionalState::Provisional,
        None,
    );
    conn.tidy().expect("tidy");
    let kept: Vec<(String, Vec<u8>)> = parts::table
        .select((parts::name, parts::size))
        .order(parts::name.asc())
        .load(conn.conn())
        .expect("read the replica");
    assert_eq!(
        kept,
        vec![("gone".to_owned(), vec![2]), ("keep".to_owned(), vec![1])],
        "the composite-keyed row the filter does not name stays"
    );
    assert_eq!(stored_provenance(&mut conn).len(), 1);
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

/// A full resync clears every row whatever a subscription still claims, and
/// the provenance of the cleared rows goes with them.
#[tokio::test]
async fn a_full_resync_clears_provenance_with_the_rows() {
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
            reason: FullResyncReason::CursorBeyondHistory,
        },
    )));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::FullResync { .. })
    })
    .await;
    send_snapshot(&server, "a", orders_payload(&[(1, "x")]), 6);
    pump_to(
        &mut conn,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "a"),
    )
    .await;
    assert_eq!(order_ids(&mut conn), vec![1]);
    assert_eq!(stored_provenance(&mut conn), Vec::new());
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

diesel::table! {
    /// The shared vocabulary table, read off the shared store.
    vocabulary (id) {
        /// Vocabulary key.
        id -> BigInt,
        /// Vocabulary label.
        label -> Text,
    }
}
