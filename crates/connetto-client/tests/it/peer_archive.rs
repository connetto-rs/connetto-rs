//! The peer state in the device archive (R77 decision 16).
//!
//! The archive carries the stored peer entries and the per-author cursors in
//! both export scopes. The import re-applies the archived entries through the
//! peer apply and merges the cursors per author in its own transaction, with
//! the receiver's own unsent write holding its key over an archived entry.

use std::time::Duration;

use connetto_client::peer::{PeerApplyOutcome, PeerAuthor};
use connetto_client::provisional::{ProvisionalRow, ProvisionalState};
use connetto_client::{
    ClientBuilder, ClientEvent, ConnettoConnection, ExportScope, ImportChoices, ImportOutcome,
    KeyValue, SyncSchema,
};
use connetto_core::Major;
use connetto_core::exchange::ManifestRow;
use diesel::prelude::*;
use sha2::{Digest, Sha256};

use super::support::{self, Script};

/// The author replica's schema, no policies, so its own writes capture
/// whatever it chooses.
const PLAIN_DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                          status TEXT NOT NULL); \
                          CREATE TABLE boards (id INTEGER PRIMARY KEY, label TEXT NOT NULL);";

/// The receiver replica's source, the same table behind translated policies,
/// so a peer apply is judged as the author.
const POLICY_SCHEMA: &str = "-- connetto:major 1\n \
                             CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                             status TEXT NOT NULL); \
                             CREATE TABLE boards (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
                             ALTER TABLE orders ENABLE ROW LEVEL SECURITY; \
                             ALTER TABLE boards ENABLE ROW LEVEL SECURITY;";

/// Everyone reads `orders`, but a person writes only the rows they own.
/// `boards` admits every person, so two authors can write one row.
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
    /// The synced boards table, open to every author.
    boards (id) {
        /// Primary key.
        id -> BigInt,
        /// Free-text payload.
        label -> Text,
    }
}

diesel::table! {
    /// The stored peer entries, read through the typed schema.
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

diesel::table! {
    /// The per-author frontier the receiver verified.
    _connetto_peer_cursor (author) {
        /// The author's key id.
        author -> Binary,
        /// The author's certificate account.
        account -> Text,
        /// The sequence the receiver verified.
        seq -> BigInt,
        /// The digest of the changeset at the sequence.
        head -> Binary,
        /// The last verified offer bytes.
        offer -> Binary,
    }
}

/// The cursor heads and offers the tests seed, one per situation.
const HEAD1: [u8; 32] = [1; 32];
const HEAD2: [u8; 32] = [2; 32];
const HEAD3: [u8; 32] = [3; 32];
const HEAD_A: [u8; 32] = [0xAA; 32];
const HEAD_B: [u8; 32] = [0xBB; 32];

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

type AuthorWrite<'a> = &'a dyn Fn(&mut ConnettoConnection<Script>);

/// One captured body per write, read off the author's durable queue.
async fn captured<const N: usize>(writes: [AuthorWrite<'_>; N]) -> [Vec<u8>; N] {
    // The internal queue table, read raw because the typed schema does
    // not name connetto's internals.
    #[derive(diesel::QueryableByName)]
    struct Body {
        #[diesel(sql_type = diesel::sql_types::Binary)]
        changeset: Vec<u8>,
    }
    let (mut author_replica, _) = plain_author().await;
    let mut bodies = Vec::with_capacity(N);
    for (seq, write) in (1..).zip(writes) {
        write(&mut author_replica);
        author_replica
            .flush()
            .await
            .expect("flush the author's write");
        let row: Body = diesel::sql_query(format!(
            "SELECT changeset FROM _connetto_pending WHERE writer = 'number:1' AND seq = {seq}"
        ))
        .get_result(author_replica.conn())
        .expect("read the captured changeset");
        bodies.push(row.changeset);
    }
    bodies.try_into().expect("one body per write")
}

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

fn seed_cursor<T>(
    conn: &mut ConnettoConnection<T>,
    author_id: &[u8],
    account: &str,
    seq: i64,
    head: &[u8; 32],
    offer: &[u8],
) where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::insert_into(_connetto_peer_cursor::table)
        .values((
            _connetto_peer_cursor::author.eq(author_id),
            _connetto_peer_cursor::account.eq(account),
            _connetto_peer_cursor::seq.eq(seq),
            _connetto_peer_cursor::head.eq(head),
            _connetto_peer_cursor::offer.eq(offer),
        ))
        .execute(conn.conn())
        .expect("seed the cursor");
}

fn cursor_row<T>(
    conn: &mut ConnettoConnection<T>,
    author_id: &[u8],
) -> Option<(i64, Vec<u8>, Vec<u8>)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    _connetto_peer_cursor::table
        .filter(_connetto_peer_cursor::author.eq(author_id))
        .select((
            _connetto_peer_cursor::seq,
            _connetto_peer_cursor::head,
            _connetto_peer_cursor::offer,
        ))
        .first::<(i64, Vec<u8>, Vec<u8>)>(conn.conn())
        .optional()
        .expect("read the cursor")
}

/// Every notice the replica queued, drained without waiting on the wire.
async fn drain_events<T>(conn: &mut ConnettoConnection<T>) -> Vec<ClientEvent>
where
    T: connetto_core::traits::Transport + Unpin,
    T::Error: core::fmt::Display,
{
    let mut out = Vec::new();
    loop {
        tokio::select! {
            event = conn.pump_one() => {
                match event {
                    Ok(event) => out.push(event),
                    Err(_) => break,
                }
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => break,
        }
    }
    out
}

fn provenance_of<'a>(
    rows: &'a [ProvisionalRow],
    table: &str,
    key: &[KeyValue],
) -> Option<&'a ProvisionalRow> {
    rows.iter()
        .find(|row| row.table == table && row.key == *key)
}

fn export<T>(conn: &mut ConnettoConnection<T>, scope: ExportScope) -> Vec<u8>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    conn.export_local_data(scope, Vec::new())
        .expect("export the replica")
}

async fn restore(bytes: &[u8]) -> (ConnettoConnection<Script>, ImportOutcome) {
    let (mut conn, _) = receiver().await;
    let plan = conn
        .import_local_data(std::io::Cursor::new(bytes))
        .expect("plan the import");
    let outcome = conn
        .apply_import(&plan, &ImportChoices::keeping_the_file())
        .expect("apply the import");
    (conn, outcome)
}

/// The archive carries the stored entries and the cursor in both scopes, and
/// the import re-applies the entries with their provenance, the cursor row
/// coming over with its offer.
#[tokio::test]
async fn an_import_reapplies_the_archived_entries_in_both_scopes() {
    let [b1, b2] = captured([&|conn| insert_order(conn, 1, "peer-anna", "a"), &|conn| {
        insert_order(conn, 2, "peer-anna", "b");
    }])
    .await;

    let (mut r1, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    for (seq, body) in [(1, b1.as_slice()), (2, b2.as_slice())] {
        assert!(matches!(
            r1.apply_peer_entry(&a, &manifest(seq, body), body)
                .expect("apply the entry"),
            PeerApplyOutcome::Applied { .. }
        ));
    }
    seed_cursor(&mut r1, b"author-a", "peer-anna", 2, &HEAD2, b"offer-two");

    let full = export(&mut r1, ExportScope::Everything);
    let unsynced = export(&mut r1, ExportScope::Unsynced);

    for bytes in [full.as_slice(), unsynced.as_slice()] {
        let (mut r2, outcome) = restore(bytes).await;
        assert_eq!(outcome.writes_restored, 0);
        assert_eq!(
            stored_entries(&mut r2),
            vec![
                ("author-a".as_bytes().to_vec(), 1),
                ("author-a".as_bytes().to_vec(), 2)
            ]
        );
        assert_eq!(
            cursor_row(&mut r2, b"author-a"),
            Some((2, HEAD2.to_vec(), b"offer-two".to_vec()))
        );
        assert_eq!(
            order_rows(&mut r2),
            vec![
                (1, "peer-anna".to_owned(), "a".to_owned()),
                (2, "peer-anna".to_owned(), "b".to_owned()),
            ]
        );
        let rows = r2.provisional_rows().expect("read the records");
        let one = provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the first record");
        assert_eq!(one.author, "author-a".as_bytes());
        assert_eq!(one.seq, 1);
        assert_eq!(one.state, ProvisionalState::Provisional);
        let two = provenance_of(&rows, "orders", &[KeyValue::Int(2)]).expect("the second record");
        assert_eq!(two.author, "author-a".as_bytes());
        assert_eq!(two.seq, 2);
        let events = drain_events(&mut r2).await;
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, ClientEvent::PeerChainInvalid { .. })),
            "nothing contradicts, so nothing raises"
        );
    }
}

/// Two authors writing one key interleave in the archive, and the import
/// replays them in the order the receiver first applied them, so the row
/// ends on the last write and the stored positions keep that order for a
/// later re-apply.
#[tokio::test]
async fn an_import_replays_interleaved_authors_in_their_archived_order() {
    let [a1, b1, a2] = captured([
        &|conn| write_board(conn, 1, "anna one", true),
        &|conn| write_board(conn, 1, "bob one", false),
        &|conn| write_board(conn, 1, "anna two", false),
    ])
    .await;

    let (mut r1, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    let b = author("author-b", "peer-bob", Some("*"));
    for (who, seq, body) in [(&a, 1, &a1), (&b, 1, &b1), (&a, 2, &a2)] {
        assert!(matches!(
            r1.apply_peer_entry(who, &manifest(seq, body), body)
                .expect("apply the entry"),
            PeerApplyOutcome::Applied { .. }
        ));
    }
    // An earlier receipt than the import's own clock, so a restore that
    // stamped its records afresh would show.
    for table in ["_connetto_peer_entry", "_connetto_provisional"] {
        diesel::sql_query(format!("UPDATE {table} SET received_at = 1000"))
            .execute(r1.conn())
            .expect("age the receipt");
    }
    let source_rows = board_rows(&mut r1);
    assert_eq!(source_rows, vec![(1, "anna two".to_owned())]);
    let source_records = r1.provisional_rows().expect("read the records");
    let source_entries = stored_entries(&mut r1);
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, _) = restore(&bytes).await;

    assert_eq!(
        board_rows(&mut r2),
        source_rows,
        "the last write in archived order wins"
    );
    assert_eq!(
        r2.provisional_rows().expect("read the records"),
        source_records,
        "the records keep their author, displacement and receipt time"
    );
    assert_eq!(
        stored_entries(&mut r2),
        source_entries,
        "the positions keep the archived order"
    );
}

/// An author whose entries were all adjudicated keeps no stored entry, and
/// the import still carries its cursor over, so the restored device does not
/// ask the author again for what it already took.
#[tokio::test]
async fn an_archived_cursor_with_no_entries_still_comes_over() {
    let (mut r1, _) = receiver().await;
    seed_cursor(&mut r1, b"author-b", "peer-bob", 3, &HEAD3, b"offer-three");
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, _) = restore(&bytes).await;

    assert_eq!(
        cursor_row(&mut r2, b"author-b"),
        Some((3, HEAD3.to_vec(), b"offer-three".to_vec()))
    );
    assert_eq!(stored_entries(&mut r2), Vec::new());
}

/// The archived cursor sits ahead of the receiver's, so the import applies
/// only what is above the receiver's frontier and moves the cursor over.
#[tokio::test]
async fn an_archived_cursor_ahead_of_the_receiver_applies_only_what_is_new() {
    let [b1, b2] = captured([&|conn| insert_order(conn, 1, "peer-anna", "a"), &|conn| {
        insert_order(conn, 2, "peer-anna", "b");
    }])
    .await;

    let (mut r1, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    r1.apply_peer_entry(&a, &manifest(1, b1.as_slice()), b1.as_slice())
        .expect("apply the first entry");
    r1.apply_peer_entry(&a, &manifest(2, b2.as_slice()), b2.as_slice())
        .expect("apply the second entry");
    seed_cursor(&mut r1, b"author-a", "peer-anna", 2, &HEAD2, b"offer-two");
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, _) = receiver().await;
    r2.apply_peer_entry(&a, &manifest(1, b1.as_slice()), b1.as_slice())
        .expect("apply the first entry itself");
    seed_cursor(&mut r2, b"author-a", "peer-anna", 1, &HEAD1, b"offer-one");

    let plan = r2
        .import_local_data(std::io::Cursor::new(&bytes))
        .expect("plan the import");
    r2.apply_import(&plan, &ImportChoices::keeping_the_file())
        .expect("apply the import");

    assert_eq!(
        cursor_row(&mut r2, b"author-a"),
        Some((2, HEAD2.to_vec(), b"offer-two".to_vec()))
    );
    assert_eq!(
        stored_entries(&mut r2),
        vec![
            ("author-a".as_bytes().to_vec(), 1),
            ("author-a".as_bytes().to_vec(), 2)
        ]
    );
    assert_eq!(
        order_rows(&mut r2),
        vec![
            (1, "peer-anna".to_owned(), "a".to_owned()),
            (2, "peer-anna".to_owned(), "b".to_owned()),
        ]
    );
    let events = drain_events(&mut r2).await;
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, ClientEvent::PeerChainInvalid { .. })),
        "the archived cursor is ahead, so nothing contradicts"
    );
}

/// The receiver's cursor sits ahead of the archived one, so the import
/// keeps the receiver's frontier and re-applies nothing.
#[tokio::test]
async fn a_receiver_ahead_of_the_archived_cursor_keeps_its_frontier() {
    let [b1, b2, b3] = captured([
        &|conn| insert_order(conn, 1, "peer-anna", "a"),
        &|conn| insert_order(conn, 2, "peer-anna", "b"),
        &|conn| insert_order(conn, 3, "peer-anna", "c"),
    ])
    .await;

    let (mut r1, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    for (seq, body) in [(1, b1.as_slice()), (2, b2.as_slice())] {
        r1.apply_peer_entry(&a, &manifest(seq, body), body)
            .expect("apply the entry");
    }
    seed_cursor(&mut r1, b"author-a", "peer-anna", 2, &HEAD2, b"offer-two");
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, _) = receiver().await;
    for (seq, body) in [(1, b1.as_slice()), (2, b2.as_slice()), (3, b3.as_slice())] {
        r2.apply_peer_entry(&a, &manifest(seq, body), body)
            .expect("apply the entry");
    }
    seed_cursor(&mut r2, b"author-a", "peer-anna", 3, &HEAD3, b"offer-three");
    let before = r2.provisional_rows().expect("read the records");

    let plan = r2
        .import_local_data(std::io::Cursor::new(&bytes))
        .expect("plan the import");
    r2.apply_import(&plan, &ImportChoices::keeping_the_file())
        .expect("apply the import");

    assert_eq!(
        cursor_row(&mut r2, b"author-a"),
        Some((3, HEAD3.to_vec(), b"offer-three".to_vec()))
    );
    assert_eq!(
        stored_entries(&mut r2),
        vec![
            ("author-a".as_bytes().to_vec(), 1),
            ("author-a".as_bytes().to_vec(), 2),
            ("author-a".as_bytes().to_vec(), 3),
        ]
    );
    // Nothing was re-applied, so the records stand as the receiver made them.
    let after = r2.provisional_rows().expect("read the records");
    assert_eq!(before.len(), after.len());
    for (row_before, row_after) in before.iter().zip(after.iter()) {
        assert_eq!(row_before.received_at, row_after.received_at);
        assert_eq!(row_before.author, row_after.author);
        assert_eq!(row_before.seq, row_after.seq);
    }
    let events = drain_events(&mut r2).await;
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, ClientEvent::PeerChainInvalid { .. })),
        "the archived cursor is behind, so nothing contradicts"
    );
}

/// The cursors agree on the sequence but not on its head, so the import
/// keeps the receiver's frontier, re-applies nothing, and raises the chain
/// invalid for the author.
#[tokio::test]
async fn an_archived_cursor_that_contradicts_the_receiver_raises_chain_invalid() {
    let [_, b2] = captured([&|conn| insert_order(conn, 1, "peer-anna", "a"), &|conn| {
        insert_order(conn, 2, "peer-anna", "b");
    }])
    .await;

    let (mut r1, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    r1.apply_peer_entry(&a, &manifest(2, b2.as_slice()), b2.as_slice())
        .expect("apply the entry");
    seed_cursor(&mut r1, b"author-a", "peer-anna", 2, &HEAD_A, b"offer-a");
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, _) = receiver().await;
    r2.apply_peer_entry(&a, &manifest(2, b2.as_slice()), b2.as_slice())
        .expect("apply the entry itself");
    seed_cursor(&mut r2, b"author-a", "peer-anna", 2, &HEAD_B, b"offer-b");
    let before = r2.provisional_rows().expect("read the records");

    let plan = r2
        .import_local_data(std::io::Cursor::new(&bytes))
        .expect("plan the import");
    r2.apply_import(&plan, &ImportChoices::keeping_the_file())
        .expect("apply the import");

    assert_eq!(
        cursor_row(&mut r2, b"author-a"),
        Some((2, HEAD_B.to_vec(), b"offer-b".to_vec()))
    );
    assert_eq!(
        stored_entries(&mut r2),
        vec![("author-a".as_bytes().to_vec(), 2)],
        "the entry at the cursor is never re-applied"
    );
    let after = r2.provisional_rows().expect("read the records");
    assert_eq!(before.len(), after.len());
    assert_eq!(before[0].received_at, after[0].received_at);
    let events = drain_events(&mut r2).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(event, ClientEvent::PeerChainInvalid { author } if author == b"author-a")
            })
            .count(),
        1,
        "the contradiction raises once, naming the author"
    );
}

/// The receiver's own unsent write holds its key over the archived entry, so
/// the import lands the own value on the key and re-queues the write, the
/// archived record naming the own write over the displaced author.
#[tokio::test]
async fn an_own_unsent_write_holds_its_key_over_the_archived_entry() {
    let [body] = captured([&|conn| {
        insert_order(conn, 1, "peer-anna", "theirs");
        insert_order(conn, 2, "peer-anna", "theirs");
    }])
    .await;

    let (mut r1, _) = receiver().await;
    insert_order(&mut r1, 1, "r77", "mine");
    r1.flush().await.expect("queue the receiver's write");

    let a = author("author-a", "peer-anna", Some("*"));
    r1.apply_peer_entry(&a, &manifest(7, body.as_slice()), body.as_slice())
        .expect("apply the entry");
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, outcome) = restore(&bytes).await;
    assert_eq!(outcome.writes_restored, 1, "the own write is queued again");
    assert_eq!(
        order_rows(&mut r2),
        vec![
            (1, "r77".to_owned(), "mine".to_owned()),
            (2, "peer-anna".to_owned(), "theirs".to_owned()),
        ]
    );
    let rows = r2.provisional_rows().expect("read the records");
    let held =
        provenance_of(&rows, "orders", &[KeyValue::Int(1)]).expect("the held key holds a record");
    assert_eq!(held.author, "number:1".as_bytes());
    assert_eq!(held.seq, 1, "the record names the receiver's queued write");
    assert_eq!(held.displaced_author, Some("author-a".as_bytes().to_vec()));
    assert_eq!(held.displaced_seq, Some(7));
    assert_eq!(held.state, ProvisionalState::Provisional);
    let landed =
        provenance_of(&rows, "orders", &[KeyValue::Int(2)]).expect("the other key holds a record");
    assert_eq!(landed.author, "author-a".as_bytes());
    assert_eq!(landed.displaced_author, None);
    assert_eq!(
        stored_entries(&mut r2),
        vec![("author-a".as_bytes().to_vec(), 7)],
        "the stored entry stays, so a replacement re-apply covers it"
    );
    assert!(
        cursor_row(&mut r2, b"author-a").is_none(),
        "the export seeded no cursor, so the import inserts none"
    );
    let events = drain_events(&mut r2).await;
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, ClientEvent::PeerChainInvalid { .. })),
        "nothing contradicts, so nothing raises"
    );
}

/// Store `body` as `author`'s entry `seq` on `conn` directly, as an
/// exporting device that applied it under different policies would hold it.
fn store_raw_entry<T>(
    conn: &mut ConnettoConnection<T>,
    author: &PeerAuthor,
    seq: i64,
    major: i64,
    body: &[u8],
) where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::sql_query(
        "INSERT INTO _connetto_peer_entry \
         (author, seq, major, changeset, account, bot_list, received_at, position) \
         VALUES (?, ?, ?, ?, ?, NULL, 1000, \
         (SELECT coalesce(max(position), 0) + 1 FROM _connetto_peer_entry))",
    )
    .bind::<diesel::sql_types::Binary, _>(&author.key_id)
    .bind::<diesel::sql_types::BigInt, _>(seq)
    .bind::<diesel::sql_types::BigInt, _>(major)
    .bind::<diesel::sql_types::Binary, _>(body)
    .bind::<diesel::sql_types::Text, _>(&author.account)
    .execute(conn.conn())
    .expect("store the entry");
}

/// An archived entry the importing replica's policies refuse lands nothing,
/// and the import says so with `PeerWriteRefused`, while the plan of a build
/// that exchanges with peers leaves no entry out.
#[tokio::test]
async fn an_archived_entry_the_policies_refuse_is_reported_and_left_out() {
    let [foreign] = captured([&|conn| insert_order(conn, 3, "peer-bob", "x")]).await;
    let (mut r1, _) = receiver().await;
    let anna = author("author-a", "peer-anna", Some("*"));
    store_raw_entry(&mut r1, &anna, 1, 1, &foreign);
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, _) = receiver().await;
    let plan = r2
        .import_local_data(std::io::Cursor::new(&bytes))
        .expect("plan the import");
    assert_eq!(plan.peer_entries_left_out(), 0);
    r2.apply_import(&plan, &ImportChoices::keeping_the_file())
        .expect("apply the import");

    assert_eq!(order_rows(&mut r2), Vec::new());
    assert_eq!(stored_entries(&mut r2), Vec::new());
    let events = drain_events(&mut r2).await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            ClientEvent::PeerWriteRefused { author, seq: 1, rows }
                if author == b"author-a" && rows == &[("orders".to_owned(), vec![KeyValue::Int(3)])]
        )),
        "the refusal surfaces, got {events:?}"
    );
}

/// Archived entries of an author the file holds no cursor for still stop at
/// the receiver's own cursor, so only what lies above it lands and the cursor
/// stays the receiver's.
#[tokio::test]
async fn archived_entries_without_a_cursor_land_only_above_the_receivers() {
    let [b1, b2] = captured([&|conn| insert_order(conn, 1, "peer-anna", "a"), &|conn| {
        insert_order(conn, 2, "peer-anna", "b");
    }])
    .await;
    let (mut r1, _) = receiver().await;
    let a = author("author-a", "peer-anna", Some("*"));
    for (seq, body) in [(1, &b1), (2, &b2)] {
        r1.apply_peer_entry(&a, &manifest(seq, body), body)
            .expect("apply the entry");
    }
    let bytes = export(&mut r1, ExportScope::Unsynced);

    let (mut r2, _) = receiver().await;
    seed_cursor(&mut r2, b"author-a", "peer-anna", 1, &HEAD1, b"offer-one");
    let plan = r2
        .import_local_data(std::io::Cursor::new(&bytes))
        .expect("plan the import");
    r2.apply_import(&plan, &ImportChoices::keeping_the_file())
        .expect("apply the import");

    assert_eq!(
        order_rows(&mut r2),
        vec![(2, "peer-anna".to_owned(), "b".to_owned())]
    );
    assert_eq!(
        cursor_row(&mut r2, b"author-a"),
        Some((1, HEAD1.to_vec(), b"offer-one".to_vec()))
    );
}

/// A peer record the file carries that no replica could hold, an entry under
/// a major no build expresses or a cursor below sequence zero, refuses the
/// whole import and lands nothing of it.
#[tokio::test]
async fn an_impossible_peer_record_refuses_the_whole_import() {
    let [body] = captured([&|conn| insert_order(conn, 1, "peer-anna", "a")]).await;
    let a = author("author-a", "peer-anna", Some("*"));
    let (mut bad_major, _) = receiver().await;
    store_raw_entry(&mut bad_major, &a, 1, -1, &body);
    let (mut bad_cursor, _) = receiver().await;
    seed_cursor(
        &mut bad_cursor,
        b"author-a",
        "peer-anna",
        -1,
        &HEAD1,
        b"offer",
    );

    for source in [&mut bad_major, &mut bad_cursor] {
        let bytes = export(source, ExportScope::Unsynced);
        let (mut r2, _) = receiver().await;
        let plan = r2
            .import_local_data(std::io::Cursor::new(&bytes))
            .expect("plan the import");
        assert!(matches!(
            r2.apply_import(&plan, &ImportChoices::keeping_the_file()),
            Err(connetto_client::ClientError::Import(_))
        ));
        assert_eq!(order_rows(&mut r2), Vec::new());
        assert_eq!(cursor_row(&mut r2, b"author-a"), None);
    }
}

/// A stored cursor whose head is not a chain hash cannot be exported, rather
/// than written into a file no import could verify.
#[tokio::test]
async fn a_cursor_with_a_corrupt_head_refuses_the_export() {
    let (mut r1, _) = receiver().await;
    diesel::sql_query(
        "INSERT INTO _connetto_peer_cursor (author, account, seq, head, offer) \
         VALUES (x'aa', 'peer-anna', 1, x'0102', x'00')",
    )
    .execute(r1.conn())
    .expect("seed a corrupt cursor");
    assert!(matches!(
        r1.export_local_data(ExportScope::Unsynced, Vec::new()),
        Err(connetto_client::ClientError::Session(_))
    ));
}
