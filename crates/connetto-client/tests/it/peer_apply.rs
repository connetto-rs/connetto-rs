//! The peer apply (R77 decisions 3 to 6, 8 and 13).
//!
//! Two replicas in one process: an author device whose writes are captured
//! under its own pending queue, and a receiver that applies the author's
//! entries through the policy views with the author bound. The author's
//! changesets are read straight out of its durable queue, so every body is
//! the bytes a real capture carries.

use std::time::Duration;

use connetto_client::peer::{PeerApplyOutcome, PeerAuthor, PeerRefusal};
use connetto_client::provisional::{ProvisionalRow, ProvisionalState};
use connetto_client::{
    ClientBuilder, ClientEvent, ConnettoConnection, Grant, KeyValue, SyncSchema,
};
use connetto_core::auth::CapabilitySubject;
use connetto_core::exchange::{ManifestRow, RetractCause, Retraction};
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, LivePatch, SnapshotBegin,
    SnapshotEnd, SnapshotPatch, SubscriptionPriority,
};
use connetto_core::traits::IncomingFrame;
use connetto_core::{Cursor, Major};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sha2::{Digest, Sha256};
use sqlite_diff_rs::{ChangeSet, DiffOps, Insert, PatchSet, SimpleTable, Value};

use super::support::{self, Script};

/// The author replica's schema: the same tables, no policies, so its own
/// writes capture whatever it chooses, a `notes` table the receiver's build
/// does not declare, and the `vocabulary` a shared receiver keeps in its store.
const PLAIN_DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                          status TEXT NOT NULL); \
                          CREATE TABLE boards (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
                          CREATE TABLE shares (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
                          CREATE TABLE pairs (a INTEGER NOT NULL, b INTEGER NOT NULL, \
                          label TEXT NOT NULL, PRIMARY KEY (b, a)); \
                          CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL); \
                          CREATE TABLE vocabulary (id INTEGER PRIMARY KEY, label TEXT NOT NULL);";

/// The receiver replica's source: the same tables behind translated
/// policies, so a peer apply is judged as the author.
const POLICY_SCHEMA: &str = "-- connetto:major 1\n \
                             CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                             status TEXT NOT NULL); \
                             CREATE TABLE boards (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
                             CREATE TABLE shares (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
                             CREATE TABLE pairs (a INTEGER NOT NULL, b INTEGER NOT NULL, \
                             label TEXT NOT NULL, PRIMARY KEY (b, a)); \
                             ALTER TABLE orders ENABLE ROW LEVEL SECURITY; \
                             ALTER TABLE boards ENABLE ROW LEVEL SECURITY; \
                             ALTER TABLE shares ENABLE ROW LEVEL SECURITY; \
                             ALTER TABLE pairs ENABLE ROW LEVEL SECURITY;";

/// Every person reads `orders` but writes only rows they own, so the receiver
/// sees an author's rows. `boards` admits every person, so two authors can
/// collide on one row, and the generated bot cap still holds a bot to its
/// list. `shares` takes a row labelled with a share key the caller holds.
/// `pairs` is open, its key declared in the other order than its columns.
const POLICIES: &str = "CREATE POLICY orders_read ON orders FOR SELECT USING (true); \
                        CREATE POLICY orders_insert ON orders FOR INSERT \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_update ON orders FOR UPDATE \
                        USING (owner = current_setting('app.user_id', true)) \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_delete ON orders FOR DELETE \
                        USING (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY boards_open ON boards FOR ALL USING (true) WITH CHECK (true); \
                        CREATE POLICY pairs_open ON pairs FOR ALL USING (true) WITH CHECK (true); \
                        CREATE POLICY shares_read ON shares FOR SELECT USING (true); \
                        CREATE POLICY shares_insert ON shares FOR INSERT WITH CHECK \
                        (label = ANY (string_to_array(current_setting('app.subjects', true), ',')));";

diesel::table! {
    /// The synced boards table, open to every author.
    boards (id) {
        /// Primary key.
        id -> BigInt,
        /// Free-text payload.
        label -> Text,
    }
}

diesel::table! {
    /// The synced orders table, read off the replica.
    orders (id) {
        /// Primary key.
        id -> BigInt,
        /// The row's owner.
        owner -> Text,
        /// Free-text payload.
        status -> Text,
    }
}

diesel::table! {
    /// The stored peer entries, read raw because the typed schema does not
    /// name connetto's internals.
    _connetto_peer_entry (author, seq) {
        /// The author's key id.
        author -> Binary,
        /// The author's queue sequence.
        seq -> BigInt,
        /// The major the entry was captured under.
        major -> BigInt,
        /// The author's certificate account.
        account -> Text,
        /// The wall clock the entry was received at.
        received_at -> BigInt,
        /// The replica-wide append position.
        position -> BigInt,
    }
}

fn author(key: &str, account: &str, bot_list: Option<&str>) -> PeerAuthor {
    PeerAuthor {
        key_id: key.as_bytes().to_vec(),
        account: account.to_owned(),
        bot_list: bot_list.map(str::to_owned),
    }
}

/// The manifest row of `body` at `seq`, captured under the schema's major 1.
fn manifest(seq: u64, body: &[u8]) -> ManifestRow {
    let digest: [u8; 32] = Sha256::digest(body).into();
    ManifestRow {
        seq,
        major: Major::new(1).expect("major 1"),
        origin: None,
        digest,
    }
}

/// One row of the provenance table, as the typed read stands it.
fn provenance_of<'a>(
    rows: &'a [ProvisionalRow],
    table: &str,
    key: &[KeyValue],
) -> Option<&'a ProvisionalRow> {
    rows.iter()
        .find(|row| row.table == table && row.key == *key)
}

/// An author replica over the policy-free schema.
async fn plain_author() -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    let conn = ClientBuilder::new(
        support::bundle(PLAIN_DDL),
        support::Once::new(script.clone()),
    )
    .signed_in(support::held("r77"))
    .connect_driven()
    .await
    .expect("connect");
    (conn, script)
}

/// A receiver replica over the policy schema.
async fn receiver() -> (ConnettoConnection<Script>, Script) {
    receiver_holding(None).await
}

/// A receiver replica over the policy schema, holding the share key
/// `(grant, subject)` when one is given.
async fn receiver_holding(key: Option<(&str, &str)>) -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(POLICY_SCHEMA, POLICIES)
            .expect("translate the policy schema"),
    );
    let mut builder = ClientBuilder::new(schema, support::Once::new(script.clone()));
    if let Some((grant, subject)) = key {
        builder = builder
            .with_share_keys([(Grant::new(grant), CapabilitySubject::<String>::new(subject))]);
    }
    let conn = builder
        .signed_in(support::held("r77"))
        .connect_driven()
        .await
        .expect("connect");
    (conn, script)
}

/// Insert one order through the connection's capture.
fn insert_order<T>(conn: &mut ConnettoConnection<T>, id: i64, owner: &str, status: &str)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::insert_into(orders::table)
        .values((
            orders::id.eq(id),
            orders::owner.eq(owner),
            orders::status.eq(status),
        ))
        .execute(conn.conn())
        .expect("insert the order");
}

/// Update one order's owner and status through the connection's capture.
fn update_order<T>(conn: &mut ConnettoConnection<T>, id: i64, owner: &str, status: &str)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::update(orders::table.filter(orders::id.eq(id)))
        .set((orders::owner.eq(owner), orders::status.eq(status)))
        .execute(conn.conn())
        .expect("update the order");
}

/// Delete one order through the connection's capture.
fn delete_order<T>(conn: &mut ConnettoConnection<T>, id: i64)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::delete(orders::table.filter(orders::id.eq(id)))
        .execute(conn.conn())
        .expect("delete the order");
}

/// Write one board's label through the connection's capture, inserting it
/// when `insert` and updating it otherwise.
fn write_board<T>(conn: &mut ConnettoConnection<T>, id: i64, label: &str, insert: bool)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    if insert {
        diesel::insert_into(boards::table)
            .values((boards::id.eq(id), boards::label.eq(label)))
            .execute(conn.conn())
            .expect("insert the board");
    } else {
        diesel::update(boards::table.filter(boards::id.eq(id)))
            .set(boards::label.eq(label))
            .execute(conn.conn())
            .expect("update the board");
    }
}

/// The replica's boards, id order.
fn board_rows<T>(conn: &mut ConnettoConnection<T>) -> Vec<(i64, String)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    boards::table
        .select((boards::id, boards::label))
        .order(boards::id.asc())
        .load(conn.conn())
        .expect("read the boards")
}

/// The author's queue entry `seq`, read raw because `_connetto_pending` is
/// an internal table the typed schema does not name.
fn pending_changeset<T>(conn: &mut ConnettoConnection<T>, seq: i64) -> Vec<u8>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    #[derive(diesel::QueryableByName)]
    struct Body {
        #[diesel(sql_type = diesel::sql_types::Binary)]
        changeset: Vec<u8>,
    }
    let row: Body = diesel::sql_query(format!(
        "SELECT changeset FROM _connetto_pending WHERE writer = 'number:1' AND seq = {seq}"
    ))
    .get_result(conn.conn())
    .expect("read the pending entry");
    row.changeset
}

/// How many entries the receiver's own queue still holds.
fn pending_count<T>(conn: &mut ConnettoConnection<T>) -> i64
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    let row: Count = diesel::sql_query("SELECT COUNT(*) AS count FROM _connetto_pending")
        .get_result(conn.conn())
        .expect("count the pending entries");
    row.count
}

/// The replica's orders, id order.
fn order_rows<T>(conn: &mut ConnettoConnection<T>) -> Vec<(i64, String, String)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    orders::table
        .select((orders::id, orders::owner, orders::status))
        .order(orders::id.asc())
        .load(conn.conn())
        .expect("read the orders")
}

/// An uncompressed insert patchset over `orders`.
fn orders_patch(rows: &[(i64, &str, &str)]) -> Vec<u8> {
    let mut patch = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for (id, owner, status) in rows {
        let insert = Insert::<_, String, Vec<u8>>::from(SimpleTable::new(
            "orders",
            &[("id", ""), ("owner", ""), ("status", "")],
            &[0],
        ))
        .set(0, Value::<String, Vec<u8>>::Integer(*id))
        .expect("set the key")
        .set(1, Value::<String, Vec<u8>>::Text(owner.to_string()))
        .expect("set the owner")
        .set(2, Value::<String, Vec<u8>>::Text(status.to_string()))
        .expect("set the status");
        patch = patch.insert(insert);
    }
    patch.build()
}

/// A compressed insert patchset over `orders`.
fn orders_payload(rows: &[(i64, &str, &str)]) -> Vec<u8> {
    zstd::encode_all(orders_patch(rows).as_slice(), 3).expect("compress the patch")
}

fn cursor_byte(byte: u8) -> Cursor {
    Cursor::new(vec![0, 0, 0, 0, 0, 0, 0, byte])
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

/// A policy-passing author write lands, records its provenance, and raises
/// `ProvisionalApplied`.
#[tokio::test]
async fn a_policy_passing_author_write_lands() {
    let [body] = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    match &out {
        PeerApplyOutcome::Applied { rows } => {
            assert_eq!(rows.len(), 1);
            let record = &rows[0];
            assert_eq!(record.table, "orders");
            assert_eq!(record.key, vec![KeyValue::Int(1)]);
            assert_eq!(record.author, "author-a".as_bytes());
            assert_eq!(record.seq, 7);
            assert_eq!(record.account, "peer-anna");
            assert_eq!(record.state, ProvisionalState::Provisional);
        }
        other => panic!("the entry should apply, it {other:?}"),
    }
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
    let record = recv
        .provenance("orders", &[KeyValue::Int(1)])
        .expect("read the provenance")
        .expect("the row holds provenance");
    assert_eq!(record.author, "author-a".as_bytes());
    assert_eq!(record.seq, 7);
    let event = pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::ProvisionalApplied { .. })
    })
    .await;
    match event {
        ClientEvent::ProvisionalApplied { author, seq, rows } => {
            assert_eq!(author, "author-a".as_bytes());
            assert_eq!(seq, 7);
            assert_eq!(rows.len(), 1);
        }
        other => panic!("the wrong event surfaced: {other:?}"),
    }
}

/// A write the author's identity may not make is refused whole: nothing of
/// the entry lands and `PeerWriteRefused` names its rows.
#[tokio::test]
async fn a_write_the_author_may_not_make_is_refused_whole() {
    let [body] = captured([&|conn| {
        insert_order(conn, 1, "peer-anna", "open");
        insert_order(conn, 2, "mallory", "open");
    }])
    .await;

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    match &out {
        PeerApplyOutcome::Refused { cause, rows } => {
            assert_eq!(*cause, PeerRefusal::Policy);
            assert_eq!(rows.len(), 2, "both ops are named: {rows:?}");
            for id in [1, 2] {
                assert!(
                    rows.contains(&("orders".to_owned(), vec![KeyValue::Int(id)])),
                    "order {id} is named: {rows:?}"
                );
            }
        }
        other => panic!("the entry should refuse, it {other:?}"),
    }
    // Neither op landed, the first one's among them.
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(
        recv.provisional_rows().expect("read the records"),
        Vec::new()
    );
    let event = pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::PeerWriteRefused { .. })
    })
    .await;
    match event {
        ClientEvent::PeerWriteRefused { author, seq, .. } => {
            assert_eq!(author, "author-a".as_bytes());
            assert_eq!(seq, 7);
        }
        other => panic!("the wrong event surfaced: {other:?}"),
    }
}

/// A bot author is held to the bot list its certificate carries, and a
/// certificate that carries none fails closed.
#[tokio::test]
async fn a_bot_author_is_judged_by_its_certificate_bot_list() {
    let [body] = captured([&|conn| write_board(conn, 1, "from a bot", true)]).await;
    let row = || manifest(7, &body);

    let (mut recv, _) = receiver().await;
    let allowed = author("bot-a", "bot-a", Some("boards:insert,boards:read"));
    let out = recv
        .apply_peer_entry(&allowed, &row(), &body)
        .expect("apply the entry");
    assert!(
        matches!(out, PeerApplyOutcome::Applied { .. }),
        "the list grants the insert"
    );
    assert_eq!(board_rows(&mut recv), vec![(1, "from a bot".to_owned())]);

    for list in [Some("boards:read"), None] {
        let (mut recv, _) = receiver().await;
        let out = recv
            .apply_peer_entry(&author("bot-a", "bot-a", list), &row(), &body)
            .expect("apply the entry");
        assert!(
            matches!(
                out,
                PeerApplyOutcome::Refused {
                    cause: PeerRefusal::Policy,
                    ..
                }
            ),
            "{list:?} refuses the insert"
        );
        assert_eq!(board_rows(&mut recv), Vec::new());
    }
}

/// A key the receiver's own unsent write holds keeps the receiver's value,
/// the author is recorded displaced, and the rest of the entry applies.
/// `queued` says whether the receiver's write was pushed into its queue
/// first or still sits in the outbox, which the apply queues itself.
async fn receiver_owned_key_keeps_the_receiver_value(queued: bool) {
    let (mut recv, _) = receiver().await;
    insert_order(&mut recv, 2, "r77", "mine");
    if queued {
        recv.flush().await.expect("queue the receiver's write");
    }

    let [body] = captured([&|conn| {
        insert_order(conn, 2, "peer-anna", "theirs");
        insert_order(conn, 3, "peer-anna", "theirs");
    }])
    .await;

    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    assert!(matches!(out, PeerApplyOutcome::Applied { .. }));
    assert_eq!(
        order_rows(&mut recv),
        vec![
            (2, "r77".to_owned(), "mine".to_owned()),
            (3, "peer-anna".to_owned(), "theirs".to_owned()),
        ]
    );
    let rows = recv.provisional_rows().expect("read the records");
    let held =
        provenance_of(&rows, "orders", &[KeyValue::Int(2)]).expect("the held key holds a record");
    assert_eq!(held.seq, 1, "the record names the receiver's queued write");
    assert_eq!(held.displaced_author, Some("author-a".as_bytes().to_vec()));
    assert_eq!(held.displaced_seq, Some(7));
    assert_eq!(held.state, ProvisionalState::Provisional);
    let landed =
        provenance_of(&rows, "orders", &[KeyValue::Int(3)]).expect("the other key holds a record");
    assert_eq!(landed.author, "author-a".as_bytes());
    assert_eq!(landed.displaced_author, None);
    assert_eq!(
        pending_count(&mut recv),
        1,
        "the receiver's write is queued once, and the author's ops are not"
    );
}

#[tokio::test]
async fn a_receiver_owned_key_keeps_the_receiver_value() {
    receiver_owned_key_keeps_the_receiver_value(true).await;
}

#[tokio::test]
async fn a_receiver_write_still_in_the_outbox_keeps_its_key() {
    receiver_owned_key_keeps_the_receiver_value(false).await;
}

/// A second author over a first author's provisional row records the first
/// as displaced, and retracting the second restores the first's value and
/// provenance from the first's stored entry.
#[tokio::test]
async fn a_second_author_displaces_the_first_and_a_retraction_restores_it() {
    let [first, second] = captured([&|conn| write_board(conn, 1, "anna's", true), &|conn| {
        write_board(conn, 1, "bob's", false);
    }])
    .await;

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &first), &first)
        .expect("apply the first entry");
    recv.apply_peer_entry(&b, &manifest(8, &second), &second)
        .expect("apply the second entry");
    assert_eq!(board_rows(&mut recv), vec![(1, "bob's".to_owned())]);
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "boards", &[KeyValue::Int(1)]).expect("a record");
    assert_eq!(record.author, "author-b".as_bytes());
    assert_eq!(record.seq, 8);
    assert_eq!(
        record.displaced_author,
        Some("author-a".as_bytes().to_vec())
    );
    assert_eq!(record.displaced_seq, Some(7));

    recv.retract_peer_entry(
        &b,
        &Retraction {
            seq: 8,
            cause: RetractCause::Conflict,
        },
    )
    .expect("retract the second entry");
    assert_eq!(
        board_rows(&mut recv),
        vec![(1, "anna's".to_owned())],
        "the inversion restores the first author's value"
    );
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "boards", &[KeyValue::Int(1)])
        .expect("the first author's provenance is restored");
    assert_eq!(record.author, "author-a".as_bytes());
    assert_eq!(record.seq, 7);
    assert_eq!(record.account, "peer-anna");
    assert_eq!(record.state, ProvisionalState::Provisional);
    assert_eq!(record.displaced_author, None);
    // The retraction took its entry and left the first author's.
    let entries = stored_entries(&mut recv);
    assert_eq!(entries, vec![("author-a".as_bytes().to_vec(), 7)]);
    let event = pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { .. })
    })
    .await;
    match event {
        ClientEvent::ProvisionalRetracted {
            author,
            seq,
            cause,
            rows,
        } => {
            assert_eq!(author, "author-b".as_bytes());
            assert_eq!(seq, 8);
            assert_eq!(cause, RetractCause::Conflict);
            assert_eq!(rows, vec![("boards".to_owned(), vec![KeyValue::Int(1)])]);
        }
        other => panic!("the wrong event surfaced: {other:?}"),
    }
}

/// Retraction inverts a stored delete from the changeset's old values,
/// raises `ProvisionalRetracted` with its cause, and the provisional delete's
/// record outlives the row it describes.
#[tokio::test]
async fn a_retraction_inverts_a_delete_and_names_its_cause() {
    let [inserted, deleted] = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "open"),
        &|conn| delete_order(conn, 1),
    ])
    .await;

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &inserted), &inserted)
        .expect("apply the insert");
    recv.apply_peer_entry(&a, &manifest(9, &deleted), &deleted)
        .expect("apply the delete");
    assert_eq!(order_rows(&mut recv), Vec::new());
    // The delete's record stays, so a retraction can restore the row.
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the delete's record");
    assert_eq!(record.author, "author-a".as_bytes());
    assert_eq!(record.seq, 9);

    recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 9,
            cause: RetractCause::Abandoned,
        },
    )
    .expect("retract the delete");
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())],
        "the inversion restores the deleted row"
    );
    let rows = recv.provisional_rows().expect("read the records");
    assert!(
        provenance_of(&rows, "orders", &[KeyValue::Int(1)]).is_none(),
        "the inverted key keeps no record"
    );
    let entries = stored_entries(&mut recv);
    assert_eq!(entries, vec![("author-a".as_bytes().to_vec(), 7)]);
    let event = pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { .. })
    })
    .await;
    match event {
        ClientEvent::ProvisionalRetracted {
            author, seq, cause, ..
        } => {
            assert_eq!(author, "author-a".as_bytes());
            assert_eq!(seq, 9);
            assert_eq!(cause, RetractCause::Abandoned);
        }
        other => panic!("the wrong event surfaced: {other:?}"),
    }
}

/// The displaced author's retraction inverts nothing, no record naming it
/// being left, and the holder's retraction then stands its inversion back,
/// the displaced entry being gone the key keeps no record; a retraction of
/// an entry the receiver never stored raises nothing.
#[tokio::test]
async fn a_retraction_with_the_displaced_entry_gone_keeps_no_record() {
    let [first, second] = captured([&|conn| write_board(conn, 1, "anna's", true), &|conn| {
        write_board(conn, 1, "bob's", false);
    }])
    .await;

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &first), &first)
        .expect("apply the first entry");
    recv.apply_peer_entry(&b, &manifest(8, &second), &second)
        .expect("apply the second entry");
    // The key's record names the second entry, the first author displaced,
    // so the first author's retraction inverts nothing and raises nothing.
    recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 7,
            cause: RetractCause::Abandoned,
        },
    )
    .expect("retract the first entry");
    let quiet = drain_quiet(&mut recv, Duration::from_millis(50)).await;
    assert!(
        !quiet
            .iter()
            .any(|event| matches!(event, ClientEvent::ProvisionalRetracted { .. })),
        "the displaced author's retraction inverts nothing"
    );
    // The holder's retraction inverts the update, the row standing back to
    // the first author's value, and the displaced entry being gone the key
    // keeps no record.
    recv.retract_peer_entry(
        &b,
        &Retraction {
            seq: 8,
            cause: RetractCause::Abandoned,
        },
    )
    .expect("retract the second entry");
    assert_eq!(
        board_rows(&mut recv),
        vec![(1, "anna's".to_owned())],
        "the inversion stands the first author's row back"
    );
    let rows = recv.provisional_rows().expect("read the records");
    assert!(
        provenance_of(&rows, "boards", &[KeyValue::Int(1)]).is_none(),
        "the inverted key keeps no record"
    );
    let event = pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { .. })
    })
    .await;
    match event {
        ClientEvent::ProvisionalRetracted {
            author,
            seq,
            cause,
            rows,
        } => {
            assert_eq!(author, "author-b".as_bytes());
            assert_eq!(seq, 8);
            assert_eq!(cause, RetractCause::Abandoned);
            assert_eq!(rows, vec![("boards".to_owned(), vec![KeyValue::Int(1)])]);
        }
        other => panic!("the wrong event surfaced: {other:?}"),
    }
    // A retraction of an entry the receiver never stored does nothing and
    // raises nothing.
    recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 7,
            cause: RetractCause::Abandoned,
        },
    )
    .expect("retract the first entry again");
    let quiet = drain_quiet(&mut recv, Duration::from_millis(50)).await;
    assert!(
        !quiet
            .iter()
            .any(|event| matches!(event, ClientEvent::ProvisionalRetracted { .. })),
        "a missing entry raises nothing"
    );
}

/// A body whose digest disagrees with the manifest row is refused, and an
/// entry captured under another major is skipped without an event.
#[tokio::test]
async fn a_digest_mismatch_is_refused_and_another_major_is_skipped() {
    let [body] = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let a = author("author-a", "peer-anna", Some("*"));

    let (mut recv, _) = receiver().await;
    let mut row = manifest(7, &body);
    row.digest[0] ^= 0xFF;
    let out = recv
        .apply_peer_entry(&a, &row, &body)
        .expect("apply the entry");
    assert!(matches!(
        out,
        PeerApplyOutcome::Refused {
            cause: PeerRefusal::DigestMismatch,
            ..
        }
    ));
    assert_eq!(order_rows(&mut recv), Vec::new());
    let entries = stored_entries(&mut recv);
    assert_eq!(entries, Vec::new());

    let row = ManifestRow {
        major: Major::new(2).expect("major 2"),
        ..manifest(7, &body)
    };
    let out = recv
        .apply_peer_entry(&a, &row, &body)
        .expect("apply the entry");
    assert!(matches!(out, PeerApplyOutcome::Skipped));
    assert_eq!(order_rows(&mut recv), Vec::new());
    let quiet = drain_quiet(&mut recv, Duration::from_millis(50)).await;
    assert!(
        !quiet.iter().any(|event| {
            matches!(
                event,
                ClientEvent::ProvisionalApplied { .. } | ClientEvent::PeerWriteRefused { .. }
            )
        }),
        "a skipped entry raises nothing"
    );
}

/// The outbox never records a peer write, so a push queues nothing as the
/// receiver's own.
#[tokio::test]
async fn a_peer_write_never_enters_the_receiver_queue() {
    let [body] = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    assert_eq!(
        recv.push().await.expect("push what the outbox holds"),
        None,
        "the outbox held nothing to push"
    );
    assert_eq!(pending_count(&mut recv), 0);
    assert_eq!(recv.unsynced().len(), 0);
}

/// Adjudication at a frontier moves the frontier's records to adjudicated
/// and deletes the entries at or below it, leaving a newer entry alone.
#[tokio::test]
async fn adjudication_at_a_frontier_moves_records_and_drops_entries() {
    let [first, second] = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "open"),
        &|conn| update_order(conn, 1, "peer-anna", "closed"),
    ])
    .await;

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &first), &first)
        .expect("apply the first entry");
    recv.apply_peer_entry(&a, &manifest(8, &second), &second)
        .expect("apply the second entry");

    // A frontier below the newest entry adjudicates it only.
    recv.adjudicate_peer_frontier(&a.key_id, 7)
        .expect("adjudicate at 7");
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the record");
    assert_eq!(record.author, "author-a".as_bytes());
    assert_eq!(
        record.seq, 8,
        "the newer entry's record still awaits a verdict"
    );
    assert_eq!(record.state, ProvisionalState::Provisional);
    let entries = stored_entries(&mut recv);
    assert_eq!(entries, vec![("author-a".as_bytes().to_vec(), 8)]);

    recv.adjudicate_peer_frontier(&a.key_id, 8)
        .expect("adjudicate at 8");
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the record");
    assert_eq!(record.state, ProvisionalState::Adjudicated);
    let entries = stored_entries(&mut recv);
    assert_eq!(entries, Vec::new());
    // The rows stay: adjudication hands them to ordinary coverage.
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "closed".to_owned())]
    );
}

/// Serve subscription `a` a snapshot of `rows` ending at `cursor`, and pump
/// until its end lands.
async fn deliver_snapshot(
    recv: &mut ConnettoConnection<Script>,
    server: &Script,
    rows: &[(i64, &str, &str)],
    cursor: u8,
) {
    server.say(IncomingFrame::Control(ControlMessage::SnapshotBegin(
        SnapshotBegin {
            sub_id: "a".to_owned(),
            priority: SubscriptionPriority::default(),
        },
    )));
    server.say(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(
        SnapshotPatch::new("a".to_owned(), orders_payload(rows)),
    )));
    server.say(IncomingFrame::Control(ControlMessage::SnapshotEnd(
        SnapshotEnd {
            sub_id: "a".to_owned(),
            cursor: cursor_byte(cursor),
        },
    )));
    pump_to(
        recv,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "a"),
    )
    .await;
}

/// A full resync keeps the rows while its replacement streams. At the
/// replacement's end the pass takes what nothing holds, and the re-apply
/// brings the peer entries back with provenance, ahead of the receiver's
/// own unsent writes.
#[tokio::test]
async fn a_resync_reapplies_stored_entries_ahead_of_the_pending_writes() {
    let (mut recv, server) = receiver().await;
    recv.subscribe("a", "SELECT * FROM orders")
        .await
        .expect("subscribe");
    deliver_snapshot(&mut recv, &server, &[(9, "r77", "kept")], 1).await;

    // The receiver's own unsent write, held ahead of the peer apply.
    insert_order(&mut recv, 2, "r77", "mine");
    recv.flush().await.expect("queue the receiver's write");

    let [first, second] = captured([
        &|conn| {
            insert_order(conn, 1, "peer-anna", "open");
            insert_order(conn, 2, "peer-anna", "theirs");
        },
        &|conn| insert_order(conn, 3, "peer-bob", "open"),
    ])
    .await;

    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &first), &first)
        .expect("apply the first entry");
    recv.apply_peer_entry(&b, &manifest(8, &second), &second)
        .expect("apply the second entry");
    let positions: Vec<i64> = _connetto_peer_entry::table
        .select(_connetto_peer_entry::position)
        .load(recv.conn())
        .expect("read the positions");
    assert_eq!(positions, vec![1, 2]);

    // The rows stay on screen while the replacement streams.
    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "a".to_owned(),
            reason: FullResyncReason::CursorBeyondHistory,
        },
    )));
    pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::FullResync { .. })
    })
    .await;
    assert_eq!(
        order_rows(&mut recv),
        vec![
            (1, "peer-anna".to_owned(), "open".to_owned()),
            (2, "r77".to_owned(), "mine".to_owned()),
            (3, "peer-bob".to_owned(), "open".to_owned()),
            (9, "r77".to_owned(), "kept".to_owned()),
        ]
    );

    // The replacement snapshot, empty, then the re-apply at its end.
    deliver_snapshot(&mut recv, &server, &[], 2).await;
    pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::ProvisionalApplied { .. })
    })
    .await;

    // The peer rows are back with provenance, the receiver's own write back
    // on top of the key the peer entry also claimed.
    assert_eq!(
        order_rows(&mut recv),
        vec![
            (1, "peer-anna".to_owned(), "open".to_owned()),
            (2, "r77".to_owned(), "mine".to_owned()),
            (3, "peer-bob".to_owned(), "open".to_owned()),
        ]
    );
    let rows = recv.provisional_rows().expect("read the records");
    let one =
        provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the first entry's record");
    assert_eq!(one.author, "author-a".as_bytes());
    assert_eq!(one.seq, 7);
    let two = provenance_of(&rows, "orders", &[KeyValue::Int(2)]).expect("the held key's record");
    assert_eq!(two.displaced_author, Some("author-a".as_bytes().to_vec()));
    assert_eq!(two.displaced_seq, Some(7));
    let three =
        provenance_of(&rows, "orders", &[KeyValue::Int(3)]).expect("the second entry's record");
    assert_eq!(three.author, "author-b".as_bytes());
    assert_eq!(three.seq, 8);
    // The re-apply kept each entry's position.
    let entries: Vec<(Vec<u8>, i64, i64)> = _connetto_peer_entry::table
        .select((
            _connetto_peer_entry::author,
            _connetto_peer_entry::seq,
            _connetto_peer_entry::position,
        ))
        .order(_connetto_peer_entry::position.asc())
        .load(recv.conn())
        .expect("read the entries");
    assert_eq!(
        entries,
        vec![
            ("author-a".as_bytes().to_vec(), 7, 1),
            ("author-b".as_bytes().to_vec(), 8, 2),
        ]
    );
    // The receiver's own un-acked write never left its queue.
    assert_eq!(pending_count(&mut recv), 1);
}

/// One write an author replica makes before a flush.
type AuthorWrite<'a> = &'a dyn Fn(&mut ConnettoConnection<Script>);

/// The bodies an author replica captures for `writes`, one flushed queue
/// entry per write, in order.
async fn captured<const N: usize>(writes: [AuthorWrite<'_>; N]) -> [Vec<u8>; N] {
    let (mut author_replica, _) = plain_author().await;
    let mut bodies = Vec::with_capacity(N);
    for (seq, write) in (1..).zip(writes) {
        write(&mut author_replica);
        author_replica
            .flush()
            .await
            .expect("flush the author's write");
        bodies.push(pending_changeset(&mut author_replica, seq));
    }
    bodies.try_into().expect("one body per write")
}

/// The receiver's stored peer entries, by author and sequence.
fn stored_entries<T>(conn: &mut ConnettoConnection<T>) -> Vec<(Vec<u8>, i64)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    _connetto_peer_entry::table
        .select((_connetto_peer_entry::author, _connetto_peer_entry::seq))
        .order(_connetto_peer_entry::position.asc())
        .load(conn.conn())
        .expect("read the entries")
}

/// Overwrite every stored entry's body with `body`.
fn rewrite_stored_bodies<T>(conn: &mut ConnettoConnection<T>, body: &[u8])
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::sql_query("UPDATE _connetto_peer_entry SET changeset = ?")
        .bind::<diesel::sql_types::Binary, _>(body)
        .execute(conn.conn())
        .expect("rewrite the stored bodies");
}

/// Retracting a provisional insert takes the row away with its record and
/// its entry, and names the row in `ProvisionalRetracted`.
#[tokio::test]
async fn a_retraction_of_an_insert_takes_the_row_away() {
    let bodies = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the insert");
    recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 7,
            cause: RetractCause::Abandoned,
        },
    )
    .expect("retract the insert");
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(
        recv.provisional_rows().expect("read the records"),
        Vec::new()
    );
    assert_eq!(stored_entries(&mut recv), Vec::new());
    let event = pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { .. })
    })
    .await;
    let ClientEvent::ProvisionalRetracted { rows, .. } = event else {
        unreachable!("pump_to returns the event it matched");
    };
    assert_eq!(rows, vec![("orders".to_owned(), vec![KeyValue::Int(1)])]);
}

/// An author may neither update nor delete a row another person owns, and
/// the owner's row and record stand as they were.
#[tokio::test]
async fn an_update_or_delete_of_another_owners_row_is_refused() {
    let updated = captured(
        [&|conn| insert_order(conn, 1, "peer-bob", "open"), &|conn| {
            update_order(conn, 1, "peer-bob", "taken");
        }],
    )
    .await;
    // The delete is captured off the row as the receiver holds it, so only
    // the policy stands between it and the row.
    let deleted = captured(
        [&|conn| insert_order(conn, 1, "peer-bob", "open"), &|conn| {
            delete_order(conn, 1);
        }],
    )
    .await;
    let (mut recv, _) = receiver().await;
    let anna = author("author-a", "peer-anna", Some("*"));
    let bob = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(&bob, &manifest(4, &updated[0]), &updated[0])
        .expect("apply bob's insert");
    for (seq, body) in [(7, &updated[1]), (8, &deleted[1])] {
        let out = recv
            .apply_peer_entry(&anna, &manifest(seq, body), body)
            .expect("apply anna's entry");
        assert!(
            matches!(
                out,
                PeerApplyOutcome::Refused {
                    cause: PeerRefusal::Policy,
                    ..
                }
            ),
            "anna's entry {seq} is refused, it {out:?}"
        );
        assert_eq!(
            order_rows(&mut recv),
            vec![(1, "peer-bob".to_owned(), "open".to_owned())]
        );
        let rows = recv.provisional_rows().expect("read the records");
        let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("bob's record");
        assert_eq!(record.author, "author-b".as_bytes());
    }
    assert_eq!(
        stored_entries(&mut recv),
        vec![("author-b".as_bytes().to_vec(), 4)]
    );
}

/// A delete of a row the receiver never held is refused, since nothing it
/// holds stands behind it.
#[tokio::test]
async fn a_delete_of_a_row_the_receiver_never_held_is_refused() {
    let bodies = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "open"),
        &|conn| delete_order(conn, 1),
    ])
    .await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(8, &bodies[1]), &bodies[1])
        .expect("apply the delete");
    assert!(
        matches!(
            out,
            PeerApplyOutcome::Refused {
                cause: PeerRefusal::Policy,
                ..
            }
        ),
        "the delete is refused, it {out:?}"
    );
    assert_eq!(stored_entries(&mut recv), Vec::new());
}

/// An entry naming a table this build does not declare fails the apply, and
/// nothing of it lands.
#[tokio::test]
async fn an_entry_naming_a_table_the_receiver_lacks_fails_whole() {
    let bodies = captured([&|conn| {
        insert_order(conn, 1, "peer-anna", "open");
        conn.conn()
            .batch_execute("INSERT INTO notes (id, body) VALUES (1, 'x')")
            .expect("insert the note");
    }])
    .await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let err = recv
        .apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect_err("the receiver has no notes table");
    assert!(
        err.to_string().contains("notes"),
        "the failure names it: {err}"
    );
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(
        recv.provisional_rows().expect("read the records"),
        Vec::new()
    );
    assert_eq!(stored_entries(&mut recv), Vec::new());
}

/// A body that is not a captured changeset fails the apply before anything
/// runs, whether it does not parse or is a patchset.
#[tokio::test]
async fn a_body_that_is_no_captured_changeset_fails_the_apply() {
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let patch = orders_patch(&[(1, "peer-anna", "open")]);
    for (body, says) in [
        (b"not a changeset".to_vec(), "parsing a peer entry"),
        (patch, "must be a captured changeset"),
    ] {
        let err = recv
            .apply_peer_entry(&a, &manifest(7, &body), &body)
            .expect_err("the body is no changeset");
        assert!(err.to_string().contains(says), "{says}: {err}");
    }
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(stored_entries(&mut recv), Vec::new());
}

/// A stored entry whose body is no captured changeset fails its retraction,
/// leaving the row, its record and the entry in place.
#[tokio::test]
async fn a_stored_body_that_is_no_captured_changeset_fails_the_retraction() {
    let bodies = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the insert");
    let retraction = Retraction {
        seq: 7,
        cause: RetractCause::Abandoned,
    };
    for (body, says) in [
        (b"not a changeset".to_vec(), "parsing a stored peer entry"),
        (
            orders_patch(&[(1, "peer-anna", "open")]),
            "must be a captured changeset",
        ),
    ] {
        rewrite_stored_bodies(&mut recv, &body);
        let err = recv
            .retract_peer_entry(&a, &retraction)
            .expect_err("the stored body is no changeset");
        assert!(err.to_string().contains(says), "{says}: {err}");
        assert_eq!(
            order_rows(&mut recv),
            vec![(1, "peer-anna".to_owned(), "open".to_owned())]
        );
        assert_eq!(recv.provisional_rows().expect("read the records").len(), 1);
        assert_eq!(
            stored_entries(&mut recv),
            vec![("author-a".as_bytes().to_vec(), 7)]
        );
    }
}

/// An entry whose bookkeeping cannot be written fails the apply and rolls
/// its rows back, and the same entry applies once the store takes writes.
#[tokio::test]
async fn a_failed_entry_write_rolls_the_apply_back() {
    let bodies = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, _) = receiver().await;
    recv.conn()
        .batch_execute(
            "CREATE TEMP TRIGGER refuse_entry BEFORE INSERT ON main._connetto_peer_entry \
             BEGIN SELECT RAISE(ABORT, 'the entry store is full'); END;",
        )
        .expect("refuse the entry write");
    let a = author("author-a", "peer-anna", Some("*"));
    let row = manifest(7, &bodies[0]);
    let err = recv
        .apply_peer_entry(&a, &row, &bodies[0])
        .expect_err("the entry cannot be stored");
    assert!(
        err.to_string().contains("the entry store is full"),
        "the failure is the store's: {err}"
    );
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(
        recv.provisional_rows().expect("read the records"),
        Vec::new()
    );

    recv.conn()
        .batch_execute("DROP TRIGGER refuse_entry")
        .expect("take entry writes again");
    let out = recv
        .apply_peer_entry(&a, &row, &bodies[0])
        .expect("apply the entry");
    assert!(matches!(out, PeerApplyOutcome::Applied { .. }), "{out:?}");
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
}

/// A retraction or an adjudication whose entry cannot be deleted fails and
/// leaves the row, its provisional record and the entry as they were.
#[tokio::test]
async fn a_verdict_whose_entry_cannot_go_changes_nothing() {
    let bodies = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the insert");
    recv.conn()
        .batch_execute(
            "CREATE TEMP TRIGGER keep_entry BEFORE DELETE ON main._connetto_peer_entry \
             BEGIN SELECT RAISE(ABORT, 'the entries are pinned'); END;",
        )
        .expect("pin the entries");
    let retracted = recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 7,
            cause: RetractCause::Abandoned,
        },
    );
    let adjudicated = recv.adjudicate_peer_frontier(&a.key_id, 7);
    for (verdict, result) in [("retraction", retracted), ("adjudication", adjudicated)] {
        let err = result.expect_err("the entry cannot go");
        assert!(
            err.to_string().contains("the entries are pinned"),
            "the {verdict} fails on the store: {err}"
        );
    }
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the record");
    assert_eq!(record.state, ProvisionalState::Provisional);
    assert_eq!(
        stored_entries(&mut recv),
        vec![("author-a".as_bytes().to_vec(), 7)]
    );
}

/// A provisional holding the store refuses to write or delete fails the
/// apply, the retraction, the adjudication and a server write's
/// confirmation alike, each leaving the rows, the records and the entries
/// as they were.
#[tokio::test]
async fn a_holding_the_store_refuses_fails_every_peer_step() {
    let [order, board] = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "open"),
        &|conn| write_board(conn, 1, "anna's", true),
    ])
    .await;
    let (mut recv, server) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &order), &order)
        .expect("apply the insert");
    recv.conn()
        .batch_execute(
            "CREATE TEMP TRIGGER keep_holdings_in BEFORE INSERT ON main._connetto_holding \
             BEGIN SELECT RAISE(ABORT, 'the holdings are pinned'); END; \
             CREATE TEMP TRIGGER keep_holdings_out BEFORE DELETE ON main._connetto_holding \
             BEGIN SELECT RAISE(ABORT, 'the holdings are pinned'); END;",
        )
        .expect("pin the holdings");
    let applied = recv
        .apply_peer_entry(&a, &manifest(8, &board), &board)
        .map(|_| ());
    let retracted = recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 7,
            cause: RetractCause::Abandoned,
        },
    );
    let adjudicated = recv.adjudicate_peer_frontier(&a.key_id, 7);
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "ended".to_owned(),
        cursor_byte(1),
        orders_payload(&[(1, "peer-anna", "open")]),
    ))));
    // The first apply's notice is queued ahead of the patch.
    let confirmed = loop {
        match recv.pump_one().await {
            Ok(ClientEvent::LivePatch { .. }) => break Ok(()),
            Ok(ClientEvent::Closed) => panic!("closed before the patch"),
            Ok(_) => {}
            Err(err) => break Err(err),
        }
    };
    for (step, result) in [
        ("apply", applied),
        ("retraction", retracted),
        ("adjudication", adjudicated),
        ("confirmation", confirmed),
    ] {
        let err = result.expect_err("the holding cannot change");
        assert!(
            err.to_string().contains("the holdings are pinned"),
            "the {step} fails on the store: {err}"
        );
    }
    assert_eq!(board_rows(&mut recv), Vec::new());
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the record");
    assert_eq!(record.state, ProvisionalState::Provisional);
    assert_eq!(
        stored_entries(&mut recv),
        vec![("author-a".as_bytes().to_vec(), 7)]
    );
}

/// A provisional record the store refuses to write fails the apply, the
/// retraction and the adjudication alike, and each leaves the rows, the
/// records and the entries as they were.
#[tokio::test]
async fn a_record_the_store_refuses_fails_the_apply_and_the_verdicts() {
    let bodies = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "open"),
        &|conn| write_board(conn, 1, "anna's", true),
    ])
    .await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the insert");
    recv.conn()
        .batch_execute(
            "CREATE TEMP TRIGGER keep_records_in BEFORE INSERT ON main._connetto_provisional \
             BEGIN SELECT RAISE(ABORT, 'the records are pinned'); END; \
             CREATE TEMP TRIGGER keep_records_as BEFORE UPDATE ON main._connetto_provisional \
             BEGIN SELECT RAISE(ABORT, 'the records are pinned'); END; \
             CREATE TEMP TRIGGER keep_records_out BEFORE DELETE ON main._connetto_provisional \
             BEGIN SELECT RAISE(ABORT, 'the records are pinned'); END;",
        )
        .expect("pin the records");
    let applied = recv
        .apply_peer_entry(&a, &manifest(8, &bodies[1]), &bodies[1])
        .map(|_| ());
    let retracted = recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 7,
            cause: RetractCause::Abandoned,
        },
    );
    let adjudicated = recv.adjudicate_peer_frontier(&a.key_id, 7);
    for (step, result) in [
        ("apply", applied),
        ("retraction", retracted),
        ("adjudication", adjudicated),
    ] {
        let err = result.expect_err("the record cannot be written");
        assert!(
            err.to_string().contains("the records are pinned"),
            "the {step} fails on the store: {err}"
        );
    }
    assert_eq!(board_rows(&mut recv), Vec::new());
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the record");
    assert_eq!(record.state, ProvisionalState::Provisional);
    assert_eq!(
        stored_entries(&mut recv),
        vec![("author-a".as_bytes().to_vec(), 7)]
    );
}

/// Retracting an update puts back the columns it changed and leaves the
/// ones it did not touch.
#[tokio::test]
async fn a_retraction_of_an_update_restores_the_changed_columns() {
    let bodies = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "open"),
        &|conn| update_order(conn, 1, "peer-anna", "closed"),
    ])
    .await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    for (seq, body) in [(7, &bodies[0]), (8, &bodies[1])] {
        recv.apply_peer_entry(&a, &manifest(seq, body), body)
            .expect("apply the entry");
    }
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "closed".to_owned())]
    );
    recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 8,
            cause: RetractCause::Conflict,
        },
    )
    .expect("retract the update");
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
    assert_eq!(
        stored_entries(&mut recv),
        vec![("author-a".as_bytes().to_vec(), 7)]
    );
}

/// Bookkeeping the replica cannot read fails each peer step, never taken
/// for an entry or a record that is absent: a retraction does not pass for
/// one of an entry never stored, and the rows stay as they were.
#[tokio::test]
async fn unreadable_bookkeeping_fails_every_peer_step() {
    let bodies = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "open"),
        &|conn| write_board(conn, 1, "anna's", true),
    ])
    .await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the insert");
    let retraction = Retraction {
        seq: 7,
        cause: RetractCause::Abandoned,
    };
    let second = manifest(8, &bodies[1]);
    for table in ["_connetto_peer_entry", "_connetto_provisional"] {
        recv.conn()
            .batch_execute(&format!("ALTER TABLE {table} RENAME TO _connetto_hidden"))
            .expect("hide the bookkeeping");
        let applied = recv.apply_peer_entry(&a, &second, &bodies[1]).map(|_| ());
        let retracted = recv.retract_peer_entry(&a, &retraction);
        let adjudicated = recv.adjudicate_peer_frontier(&a.key_id, 7);
        for (step, result) in [
            ("apply", applied),
            ("retraction", retracted),
            ("adjudication", adjudicated),
        ] {
            let err = result.expect_err("the bookkeeping cannot be read");
            assert!(
                err.to_string().contains(table),
                "the {step} names {table}: {err}"
            );
        }
        recv.conn()
            .batch_execute(&format!("ALTER TABLE _connetto_hidden RENAME TO {table}"))
            .expect("restore the bookkeeping");
        assert_eq!(board_rows(&mut recv), Vec::new());
        assert_eq!(
            order_rows(&mut recv),
            vec![(1, "peer-anna".to_owned(), "open".to_owned())]
        );
    }
    assert_eq!(
        stored_entries(&mut recv),
        vec![("author-a".as_bytes().to_vec(), 7)]
    );
}

/// The replica's `pairs`, as `(a, b, label)` in `a` order.
fn pair_rows<T>(conn: &mut ConnettoConnection<T>) -> Vec<(i64, i64, String)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    #[derive(diesel::QueryableByName)]
    struct Pair {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        a: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        b: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        label: String,
    }
    diesel::sql_query("SELECT a, b, label FROM pairs ORDER BY a")
        .load::<Pair>(conn.conn())
        .expect("read the pairs")
        .into_iter()
        .map(|pair| (pair.a, pair.b, pair.label))
        .collect()
}

/// A key declared in another order than its columns is recorded in key
/// order, so the typed read finds it, and the receiver's own unsent write on
/// such a key still keeps its value.
#[tokio::test]
async fn a_key_declared_out_of_column_order_is_held_in_key_order() {
    let bodies = captured([&|conn| {
        conn.conn()
            .batch_execute(
                "INSERT INTO pairs (a, b, label) VALUES (1, 2, 'theirs'), (3, 4, 'theirs')",
            )
            .expect("insert the pairs");
    }])
    .await;
    let (mut recv, _) = receiver().await;
    recv.conn()
        .batch_execute("INSERT INTO pairs (a, b, label) VALUES (1, 2, 'mine')")
        .expect("the receiver's own write");
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the entry");
    assert!(matches!(out, PeerApplyOutcome::Applied { .. }), "{out:?}");
    assert_eq!(
        pair_rows(&mut recv),
        vec![(1, 2, "mine".to_owned()), (3, 4, "theirs".to_owned())]
    );
    let landed = recv
        .provenance("pairs", &[KeyValue::Int(4), KeyValue::Int(3)])
        .expect("read the provenance")
        .expect("the landed row holds a record under its key order");
    assert_eq!(landed.author, "author-a".as_bytes());
    let held = recv
        .provenance("pairs", &[KeyValue::Int(2), KeyValue::Int(1)])
        .expect("read the provenance")
        .expect("the held row holds a record under its key order");
    assert_eq!(held.displaced_author, Some("author-a".as_bytes().to_vec()));
    recv.tidy().expect("tidy");
    assert_eq!(
        pair_rows(&mut recv),
        vec![(1, 2, "mine".to_owned()), (3, 4, "theirs".to_owned())],
        "the provisional row's holding names its key in key order, so the pass keeps it"
    );
}

/// An entry with an op that names its row by no key is refused whole, the
/// keyed ops beside it landing nothing either.
#[tokio::test]
async fn an_entry_with_an_unkeyed_op_is_refused_whole() {
    let keyed = SimpleTable::new("orders", &[("id", ""), ("owner", ""), ("status", "")], &[0]);
    let unkeyed = SimpleTable::new("boards", &[("id", ""), ("label", "")], &[]);
    let body = ChangeSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(keyed)
                .set(0, Value::Integer(1))
                .expect("id")
                .set(1, Value::Text("peer-anna".to_owned()))
                .expect("owner")
                .set(2, Value::Text("open".to_owned()))
                .expect("status"),
        )
        .insert(
            Insert::from(unkeyed)
                .set(0, Value::Integer(1))
                .expect("id")
                .set(1, Value::Text("anna's".to_owned()))
                .expect("label"),
        )
        .build();
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    assert_eq!(
        out,
        PeerApplyOutcome::Refused {
            cause: PeerRefusal::Unkeyed,
            rows: vec![("orders".to_owned(), vec![KeyValue::Int(1)])],
        }
    );
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(board_rows(&mut recv), Vec::new());
    assert_eq!(stored_entries(&mut recv), Vec::new());
}

/// A retraction whose undo the store skips, as a guard may silently, fails
/// whole, leaving the row, its record and the entry as they were and raising
/// nothing, and the same retraction goes through once the store takes it.
#[tokio::test]
async fn a_refused_undo_fails_the_retraction_whole() {
    let [body] = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the insert");
    let backing: String = diesel::select(diesel::dsl::sql::<diesel::sql_types::Text>(
        "(SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'orders_%')",
    ))
    .get_result(recv.conn())
    .expect("name the backing table");
    recv.conn()
        .batch_execute(&format!(
            "CREATE TEMP TRIGGER keep_orders BEFORE DELETE ON main.{backing} \
             BEGIN SELECT RAISE(IGNORE); END;"
        ))
        .expect("keep the order");
    let retraction = Retraction {
        seq: 7,
        cause: RetractCause::Abandoned,
    };
    let err = recv
        .retract_peer_entry(&a, &retraction)
        .expect_err("the undo is refused");
    assert!(
        err.to_string().contains("refused putting back"),
        "the failure names the refused undo: {err}"
    );
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the record");
    assert_eq!(record.author, "author-a".as_bytes());
    assert_eq!(record.state, ProvisionalState::Provisional);
    assert_eq!(
        stored_entries(&mut recv),
        vec![("author-a".as_bytes().to_vec(), 7)]
    );
    let quiet = drain_quiet(&mut recv, Duration::from_millis(50)).await;
    assert!(
        !quiet
            .iter()
            .any(|event| matches!(event, ClientEvent::ProvisionalRetracted { .. })),
        "a failed retraction raises nothing"
    );

    recv.conn()
        .batch_execute("DROP TRIGGER keep_orders")
        .expect("take the undo again");
    recv.retract_peer_entry(&a, &retraction)
        .expect("retract the insert");
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(stored_entries(&mut recv), Vec::new());
}

/// The ids of the replica's `shares` rows, comma separated, `None` when it
/// holds none.
fn share_ids<T>(conn: &mut ConnettoConnection<T>) -> Option<String>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::select(diesel::dsl::sql::<
        diesel::sql_types::Nullable<diesel::sql_types::Text>,
    >("(SELECT group_concat(id) FROM shares)"))
    .get_result(conn.conn())
    .expect("read the shares")
}

/// An author is judged holding no share keys, never the receiver's, and the
/// receiver's own key admits its writes again once the apply is over.
#[tokio::test]
async fn an_author_is_judged_without_the_receivers_share_keys() {
    let bodies = captured([&|conn| {
        conn.conn()
            .batch_execute("INSERT INTO shares (id, label) VALUES (1, 'key:a')")
            .expect("insert the share");
    }])
    .await;
    let (mut recv, _) = receiver_holding(Some(("grant-a", "key:a"))).await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the entry");
    assert!(
        matches!(
            out,
            PeerApplyOutcome::Refused {
                cause: PeerRefusal::Policy,
                ..
            }
        ),
        "the author holds no key:a, it {out:?}"
    );
    assert_eq!(share_ids(&mut recv), None);
    recv.conn()
        .batch_execute("INSERT INTO shares (id, label) VALUES (2, 'key:a')")
        .expect("the receiver's own key admits its write");
    assert_eq!(share_ids(&mut recv), Some("2".to_owned()));
}

/// A stored entry under a major the build cannot express fails the resync's
/// re-apply and names the major.
#[tokio::test]
async fn a_stored_entry_under_no_expressible_major_fails_the_reapply() {
    let bodies = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, server) = receiver().await;
    recv.subscribe("a", "SELECT * FROM orders")
        .await
        .expect("subscribe");
    deliver_snapshot(&mut recv, &server, &[], 1).await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &bodies[0]), &bodies[0])
        .expect("apply the insert");
    recv.conn()
        .batch_execute("UPDATE _connetto_peer_entry SET major = 0")
        .expect("store a major no build expresses");

    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "a".to_owned(),
            reason: FullResyncReason::CursorBeyondHistory,
        },
    )));
    pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::FullResync { .. })
    })
    .await;
    server.say(IncomingFrame::Control(ControlMessage::SnapshotBegin(
        SnapshotBegin {
            sub_id: "a".to_owned(),
            priority: SubscriptionPriority::default(),
        },
    )));
    server.say(IncomingFrame::Control(ControlMessage::SnapshotEnd(
        SnapshotEnd {
            sub_id: "a".to_owned(),
            cursor: cursor_byte(2),
        },
    )));
    let err = loop {
        match recv.pump_one().await {
            Ok(ClientEvent::Closed) => panic!("closed before the re-apply"),
            Ok(_) => {}
            Err(err) => break err,
        }
    };
    assert!(
        err.to_string().contains("major"),
        "the failure names the major: {err}"
    );
}

/// Serve a live patch of `rows` over `orders`, addressed to `sub_id`, and
/// pump until it lands.
async fn live_orders(
    recv: &mut ConnettoConnection<Script>,
    server: &Script,
    sub_id: &str,
    rows: &[(i64, &str, &str)],
    cursor: u8,
) {
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        sub_id.to_owned(),
        cursor_byte(cursor),
        orders_payload(rows),
    ))));
    pump_to(recv, |event| matches!(event, ClientEvent::LivePatch { .. })).await;
}

/// A provisional row no subscription holds stays through every pass until
/// its verdict, and the pass after the verdict takes it, nothing else
/// holding it (R77 decision 14).
#[tokio::test]
async fn a_provisional_row_stays_until_its_verdict() {
    let [body] = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the insert");
    recv.tidy().expect("tidy");
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())],
        "no subscription holds the row, its provisional record does"
    );
    recv.adjudicate_peer_frontier(&a.key_id, 7)
        .expect("adjudicate");
    recv.tidy().expect("tidy");
    assert_eq!(
        order_rows(&mut recv),
        Vec::new(),
        "the verdict releases the row to ordinary coverage, and nothing covers it"
    );
}

/// A server write confirms a provisional row and releases its holding, so a
/// write no live subscription takes leaves the row to the next pass.
#[tokio::test]
async fn a_confirmed_row_stays_only_while_a_subscription_holds_it() {
    let [body] = captured([&|conn| insert_order(conn, 1, "peer-anna", "open")]).await;
    let (mut recv, server) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the insert");
    live_orders(&mut recv, &server, "ended", &[(1, "peer-anna", "open")], 1).await;
    assert_eq!(
        recv.provisional_rows().expect("read the records"),
        Vec::new()
    );
    recv.tidy().expect("tidy");
    assert_eq!(
        order_rows(&mut recv),
        Vec::new(),
        "the confirmation released the row, and the ended subscription took nothing"
    );
}

/// A cursor beyond history resyncs every subscription, and the rows its
/// replacement does not deliver back go, but a provisional row awaits its
/// verdict, as an unsent write does, and stays with its record (R11 Part B,
/// O10, R77 decision 13).
#[tokio::test]
async fn a_beyond_history_resync_keeps_a_provisional_row() {
    let [body] = captured([&|conn| insert_order(conn, 2, "peer-anna", "open")]).await;
    let (mut recv, server) = receiver().await;
    recv.subscribe("a", "SELECT * FROM orders")
        .await
        .expect("subscribe");
    deliver_snapshot(&mut recv, &server, &[(1, "r77", "x")], 1).await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the insert");
    live_orders(&mut recv, &server, "ended", &[(3, "r77", "z")], 2).await;
    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "a".to_owned(),
            reason: FullResyncReason::CursorBeyondHistory,
        },
    )));
    pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::FullResync { .. })
    })
    .await;
    deliver_snapshot(&mut recv, &server, &[(1, "r77", "x")], 3).await;
    assert_eq!(
        order_rows(&mut recv),
        vec![
            (1, "r77".to_owned(), "x".to_owned()),
            (2, "peer-anna".to_owned(), "open".to_owned()),
        ],
        "the unheld row 3 goes and the provisional row 2 stays"
    );
    assert_eq!(recv.provisional_rows().expect("read the records").len(), 1);
}

/// A provisional row outlives the subscription that delivered it, since its
/// holding is no subscription's.
#[tokio::test]
async fn a_provisional_row_outlives_its_subscription() {
    let [_, update] = captured([
        &|conn| insert_order(conn, 9, "peer-anna", "kept"),
        &|conn| update_order(conn, 9, "peer-anna", "moved"),
    ])
    .await;
    let (mut recv, server) = receiver().await;
    recv.subscribe("a", "SELECT * FROM orders")
        .await
        .expect("subscribe");
    deliver_snapshot(
        &mut recv,
        &server,
        &[(9, "peer-anna", "kept"), (8, "r77", "x")],
        1,
    )
    .await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &update), &update)
        .expect("apply the update");
    recv.unsubscribe("a").await.expect("unsubscribe");
    assert_eq!(
        order_rows(&mut recv),
        vec![(9, "peer-anna".to_owned(), "moved".to_owned())],
        "the ended subscription's row 8 goes, the provisional row 9 stays"
    );
}

/// A retraction that leaves a row with no record releases its holding, and
/// one that stands a displaced author's record back keeps it.
#[tokio::test]
async fn a_retraction_releases_only_a_key_left_without_a_record() {
    let [first, second] = captured([&|conn| write_board(conn, 1, "anna's", true), &|conn| {
        write_board(conn, 1, "bob's", false);
    }])
    .await;
    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &first), &first)
        .expect("apply the first entry");
    recv.apply_peer_entry(&b, &manifest(8, &second), &second)
        .expect("apply the second entry");
    let retract = |seq| Retraction {
        seq,
        cause: RetractCause::Abandoned,
    };
    recv.retract_peer_entry(&b, &retract(8))
        .expect("retract the second entry");
    recv.tidy().expect("tidy");
    assert_eq!(
        board_rows(&mut recv),
        vec![(1, "anna's".to_owned())],
        "the first author's record stands back and keeps the row"
    );
    recv.adjudicate_peer_frontier(&a.key_id, 6)
        .expect("a frontier below the entry settles nothing");
    let [again] = captured([&|conn| write_board(conn, 1, "bob's", true)]).await;
    recv.apply_peer_entry(&b, &manifest(9, &again), &again)
        .expect("apply a third entry over the first");
    recv.adjudicate_peer_frontier(&a.key_id, 7)
        .expect("the first author's entry goes");
    recv.retract_peer_entry(&b, &retract(9))
        .expect("retract the third entry");
    recv.tidy().expect("tidy");
    assert_eq!(
        board_rows(&mut recv),
        Vec::new(),
        "with the displaced entry gone the key keeps no record and no holding"
    );
}

/// A shared receiver: `vocabulary` lives in the store, open to every
/// person, and `orders` in `main` under its owner policy.
async fn shared_receiver() -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    let schema = SyncSchema::new(
        connetto_schema::translate_shared::<String, connetto_core::bots::TextIds>(
            "-- connetto:major 1\n \
             CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
             status TEXT NOT NULL); \
             CREATE TABLE vocabulary (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
             ALTER TABLE orders ENABLE ROW LEVEL SECURITY; \
             ALTER TABLE vocabulary ENABLE ROW LEVEL SECURITY;",
            "CREATE POLICY orders_read ON orders FOR SELECT USING (true); \
             CREATE POLICY orders_insert ON orders FOR INSERT \
             WITH CHECK (owner = current_setting('app.user_id', true)); \
             CREATE POLICY vocabulary_open ON vocabulary FOR ALL USING (true) WITH CHECK (true);",
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

/// The replica's vocabulary, id order.
fn vocabulary_rows<T>(conn: &mut ConnettoConnection<T>) -> Vec<(i64, String)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    #[derive(diesel::QueryableByName)]
    struct Word {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        label: String,
    }
    diesel::sql_query("SELECT id, label FROM vocabulary ORDER BY id")
        .load::<Word>(conn.conn())
        .expect("read the vocabulary")
        .into_iter()
        .map(|word| (word.id, word.label))
        .collect()
}

/// An entry touching a shared table and a private one lands whole, the
/// shared row in the store, both held until their verdict, and none of it
/// enters the receiver's own queue (R77 decision 15).
#[tokio::test]
async fn a_peer_entry_reaches_a_shared_table_through_the_store() {
    let [body] = captured([&|conn| {
        insert_order(conn, 1, "peer-anna", "open");
        conn.conn()
            .batch_execute("INSERT INTO vocabulary (id, label) VALUES (5, 'peer-word')")
            .expect("insert the word");
    }])
    .await;
    let (mut recv, _) = shared_receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    assert!(matches!(out, PeerApplyOutcome::Applied { .. }), "{out:?}");
    assert_eq!(
        vocabulary_rows(&mut recv),
        vec![(5, "peer-word".to_owned())]
    );
    assert_eq!(
        order_rows(&mut recv),
        vec![(1, "peer-anna".to_owned(), "open".to_owned())]
    );
    let records = recv.provisional_rows().expect("read the records");
    assert!(provenance_of(&records, "vocabulary", &[KeyValue::Int(5)]).is_some());
    assert!(provenance_of(&records, "orders", &[KeyValue::Int(1)]).is_some());
    assert_eq!(pending_count(&mut recv), 0);
    assert_eq!(
        recv.push().await.expect("push what the outbox holds"),
        None,
        "the outbox recorded none of the author's rows"
    );
    recv.tidy().expect("tidy");
    assert_eq!(
        vocabulary_rows(&mut recv),
        vec![(5, "peer-word".to_owned())]
    );

    recv.adjudicate_peer_frontier(&a.key_id, 7)
        .expect("adjudicate");
    recv.tidy().expect("tidy");
    assert_eq!(vocabulary_rows(&mut recv), Vec::new());
    assert_eq!(order_rows(&mut recv), Vec::new());
}

/// A retraction undoes a shared row in the store.
#[tokio::test]
async fn a_retraction_undoes_a_shared_row_in_the_store() {
    let [body] = captured([&|conn| {
        conn.conn()
            .batch_execute("INSERT INTO vocabulary (id, label) VALUES (5, 'peer-word')")
            .expect("insert the word");
    }])
    .await;
    let (mut recv, _) = shared_receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    recv.retract_peer_entry(
        &a,
        &Retraction {
            seq: 7,
            cause: RetractCause::Conflict,
        },
    )
    .expect("retract the entry");
    assert_eq!(vocabulary_rows(&mut recv), Vec::new());
    assert_eq!(
        recv.provisional_rows().expect("read the records"),
        Vec::new()
    );
}
