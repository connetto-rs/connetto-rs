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
use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection, KeyValue, SyncSchema};
use connetto_core::exchange::{ManifestRow, RetractCause, Retraction};
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, SnapshotBegin, SnapshotEnd,
    SnapshotPatch, SubscriptionPriority,
};
use connetto_core::traits::IncomingFrame;
use connetto_core::{Cursor, Major};
use diesel::prelude::*;
use sha2::{Digest, Sha256};
use sqlite_diff_rs::{DiffOps, Insert, PatchSet, SimpleTable, Value};

use super::support::{self, Script};

/// The author replica's schema: the same tables, no policies, so its own
/// writes capture whatever it chooses.
const PLAIN_DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                          status TEXT NOT NULL); \
                          CREATE TABLE boards (id INTEGER PRIMARY KEY, label TEXT NOT NULL);";

/// The receiver replica's source: the same tables behind translated
/// policies, so a peer apply is judged as the author.
const POLICY_SCHEMA: &str = "-- connetto:major 1\n \
                             CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                             status TEXT NOT NULL); \
                             CREATE TABLE boards (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
                             ALTER TABLE orders ENABLE ROW LEVEL SECURITY; \
                             ALTER TABLE boards ENABLE ROW LEVEL SECURITY;";

/// Every person reads `orders` but writes only rows they own, so the receiver
/// sees an author's rows. `boards` admits every person, so two authors can
/// collide on one row, and the generated bot cap still holds a bot to its
/// list.
const POLICIES: &str = "CREATE POLICY orders_read ON orders FOR SELECT USING (true); \
                        CREATE POLICY orders_insert ON orders FOR INSERT \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_update ON orders FOR UPDATE \
                        USING (owner = current_setting('app.user_id', true)) \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_delete ON orders FOR DELETE \
                        USING (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY boards_open ON boards FOR ALL USING (true) WITH CHECK (true);";

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

fn manifest(seq: u64, major: Major, body: &[u8]) -> ManifestRow {
    let digest: [u8; 32] = Sha256::digest(body).into();
    ManifestRow {
        seq,
        major,
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
    let script = Script::default();
    script.say(support::handshake_ack());
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(POLICY_SCHEMA, POLICIES)
            .expect("translate the policy schema"),
    );
    let conn = ClientBuilder::new(schema, support::Once::new(script.clone()))
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

/// A compressed insert patchset over `orders`.
fn orders_payload(rows: &[(i64, &str, &str)]) -> Vec<u8> {
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
    zstd::encode_all(patch.build().as_slice(), 3).expect("compress the patch")
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
    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 1, "peer-anna", "open");
    author_replica
        .flush()
        .await
        .expect("flush the author's write");
    let body = pending_changeset(&mut author_replica, 1);

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(
            &a,
            &manifest(7, Major::new(1).expect("major 1"), &body),
            &body,
        )
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
    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 1, "peer-anna", "open");
    insert_order(&mut author_replica, 2, "mallory", "open");
    author_replica
        .flush()
        .await
        .expect("flush the author's writes");
    let body = pending_changeset(&mut author_replica, 1);

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(
            &a,
            &manifest(7, Major::new(1).expect("major 1"), &body),
            &body,
        )
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
    let (mut author_replica, _) = plain_author().await;
    write_board(&mut author_replica, 1, "from a bot", true);
    author_replica
        .flush()
        .await
        .expect("flush the author's write");
    let body = pending_changeset(&mut author_replica, 1);
    let row = || manifest(7, Major::new(1).expect("major 1"), &body);

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

    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 2, "peer-anna", "theirs");
    insert_order(&mut author_replica, 3, "peer-anna", "theirs");
    author_replica
        .flush()
        .await
        .expect("flush the author's writes");
    let body = pending_changeset(&mut author_replica, 1);

    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(
            &a,
            &manifest(7, Major::new(1).expect("major 1"), &body),
            &body,
        )
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
    let (mut author_replica, _) = plain_author().await;
    write_board(&mut author_replica, 1, "anna's", true);
    author_replica.flush().await.expect("flush the first write");
    let first = pending_changeset(&mut author_replica, 1);
    write_board(&mut author_replica, 1, "bob's", false);
    author_replica
        .flush()
        .await
        .expect("flush the second write");
    let second = pending_changeset(&mut author_replica, 2);

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(
        &a,
        &manifest(7, Major::new(1).expect("major 1"), &first),
        &first,
    )
    .expect("apply the first entry");
    recv.apply_peer_entry(
        &b,
        &manifest(8, Major::new(1).expect("major 1"), &second),
        &second,
    )
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
    let entries: Vec<(Vec<u8>, i64)> = _connetto_peer_entry::table
        .select((_connetto_peer_entry::author, _connetto_peer_entry::seq))
        .load(recv.conn())
        .expect("read the entries");
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
    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 1, "peer-anna", "open");
    author_replica.flush().await.expect("flush the insert");
    let inserted = pending_changeset(&mut author_replica, 1);
    delete_order(&mut author_replica, 1);
    author_replica.flush().await.expect("flush the delete");
    let deleted = pending_changeset(&mut author_replica, 2);

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(
        &a,
        &manifest(7, Major::new(1).expect("major 1"), &inserted),
        &inserted,
    )
    .expect("apply the insert");
    recv.apply_peer_entry(
        &a,
        &manifest(9, Major::new(1).expect("major 1"), &deleted),
        &deleted,
    )
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
    let entries: Vec<(Vec<u8>, i64)> = _connetto_peer_entry::table
        .select((_connetto_peer_entry::author, _connetto_peer_entry::seq))
        .load(recv.conn())
        .expect("read the entries");
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
    let (mut author_replica, _) = plain_author().await;
    write_board(&mut author_replica, 1, "anna's", true);
    author_replica.flush().await.expect("flush the first write");
    let first = pending_changeset(&mut author_replica, 1);
    write_board(&mut author_replica, 1, "bob's", false);
    author_replica
        .flush()
        .await
        .expect("flush the second write");
    let second = pending_changeset(&mut author_replica, 2);

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(
        &a,
        &manifest(7, Major::new(1).expect("major 1"), &first),
        &first,
    )
    .expect("apply the first entry");
    recv.apply_peer_entry(
        &b,
        &manifest(8, Major::new(1).expect("major 1"), &second),
        &second,
    )
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
    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 1, "peer-anna", "open");
    author_replica
        .flush()
        .await
        .expect("flush the author's write");
    let body = pending_changeset(&mut author_replica, 1);
    let a = author("author-a", "peer-anna", Some("*"));

    let (mut recv, _) = receiver().await;
    let mut row = manifest(7, Major::new(1).expect("major 1"), &body);
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
    let entries: Vec<(Vec<u8>, i64)> = _connetto_peer_entry::table
        .select((_connetto_peer_entry::author, _connetto_peer_entry::seq))
        .load(recv.conn())
        .expect("read the entries");
    assert_eq!(entries, Vec::new());

    let row = manifest(7, Major::new(2).expect("major 2"), &body);
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
    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 1, "peer-anna", "open");
    author_replica
        .flush()
        .await
        .expect("flush the author's write");
    let body = pending_changeset(&mut author_replica, 1);

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(
        &a,
        &manifest(7, Major::new(1).expect("major 1"), &body),
        &body,
    )
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
    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 1, "peer-anna", "open");
    author_replica.flush().await.expect("flush the first write");
    let first = pending_changeset(&mut author_replica, 1);
    update_order(&mut author_replica, 1, "peer-anna", "closed");
    author_replica
        .flush()
        .await
        .expect("flush the second write");
    let second = pending_changeset(&mut author_replica, 2);

    let (mut recv, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    recv.apply_peer_entry(
        &a,
        &manifest(7, Major::new(1).expect("major 1"), &first),
        &first,
    )
    .expect("apply the first entry");
    recv.apply_peer_entry(
        &a,
        &manifest(8, Major::new(1).expect("major 1"), &second),
        &second,
    )
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
    let entries: Vec<(Vec<u8>, i64)> = _connetto_peer_entry::table
        .select((_connetto_peer_entry::author, _connetto_peer_entry::seq))
        .load(recv.conn())
        .expect("read the entries");
    assert_eq!(entries, vec![("author-a".as_bytes().to_vec(), 8)]);

    recv.adjudicate_peer_frontier(&a.key_id, 8)
        .expect("adjudicate at 8");
    let rows = recv.provisional_rows().expect("read the records");
    let record = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the record");
    assert_eq!(record.state, ProvisionalState::Adjudicated);
    let entries: Vec<(Vec<u8>, i64)> = _connetto_peer_entry::table
        .select((_connetto_peer_entry::author, _connetto_peer_entry::seq))
        .load(recv.conn())
        .expect("read the entries");
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

/// A full resync drops the peer rows and their records, and the re-apply at
/// the replacement snapshot's end brings them back with provenance, ahead of
/// the receiver's own unsent writes.
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

    let (mut author_replica, _) = plain_author().await;
    insert_order(&mut author_replica, 1, "peer-anna", "open");
    insert_order(&mut author_replica, 2, "peer-anna", "theirs");
    author_replica.flush().await.expect("flush the first write");
    let first = pending_changeset(&mut author_replica, 1);
    insert_order(&mut author_replica, 3, "peer-bob", "open");
    author_replica
        .flush()
        .await
        .expect("flush the second write");
    let second = pending_changeset(&mut author_replica, 2);

    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    recv.apply_peer_entry(
        &a,
        &manifest(7, Major::new(1).expect("major 1"), &first),
        &first,
    )
    .expect("apply the first entry");
    recv.apply_peer_entry(
        &b,
        &manifest(8, Major::new(1).expect("major 1"), &second),
        &second,
    )
    .expect("apply the second entry");
    let positions: Vec<i64> = _connetto_peer_entry::table
        .select(_connetto_peer_entry::position)
        .load(recv.conn())
        .expect("read the positions");
    assert_eq!(positions, vec![1, 2]);

    // The resync clear takes the rows and the records.
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
    assert_eq!(order_rows(&mut recv), Vec::new());
    assert_eq!(
        recv.provisional_rows().expect("read the records"),
        Vec::new()
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
