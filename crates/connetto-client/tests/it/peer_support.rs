//! Shared helpers for the peer tests.
//!
//! `peer_apply`, `peer_archive` and `provisional` drive replicas over the
//! same scripted socket and read the peer bookkeeping off the same tables.
//! These helpers carry the pieces the files share. Each file keeps its own
//! schema constants, since the replicas it builds declare different tables.

use std::time::Duration;

use connetto_client::peer::PeerAuthor;
use connetto_client::provisional::ProvisionalRow;
use connetto_client::{
    ClientBuilder, ClientEvent, ConnettoConnection, Grant, KeyValue, SyncSchema,
};
use connetto_core::auth::CapabilitySubject;
use connetto_core::exchange::ManifestRow;
use connetto_core::{Cursor, Major};
use diesel::prelude::*;
use sha2::{Digest, Sha256};

use super::support::{self, Script};

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

/// A peer author certificate for `key` and `account`, its bot list when one
/// is given.
pub fn author(key: &str, account: &str, bot_list: Option<&str>) -> PeerAuthor {
    PeerAuthor {
        key_id: key.as_bytes().to_vec(),
        account: account.to_owned(),
        bot_list: bot_list.map(str::to_owned),
    }
}

/// The manifest row of `body` at `seq`, captured under the schema's major 1.
pub fn manifest(seq: u64, body: &[u8]) -> ManifestRow {
    let digest: [u8; 32] = Sha256::digest(body).into();
    ManifestRow {
        seq,
        major: Major::new(1).expect("major 1"),
        origin: None,
        digest,
    }
}

/// A connected replica over the policy-free `ddl`, its scripted server
/// answering the handshake.
pub async fn plain_author(ddl: &str) -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    let conn = ClientBuilder::new(support::bundle(ddl), support::Once::new(script.clone()))
        .signed_in(support::held("r77"))
        .connect_driven()
        .await
        .expect("connect");
    (conn, script)
}

/// A receiver replica over the policy schema `schema`, its policies
/// `policies`.
pub async fn receiver(schema: &str, policies: &str) -> (ConnettoConnection<Script>, Script) {
    receiver_holding(schema, policies, None).await
}

/// A receiver replica over the policy schema, holding the share key
/// `(grant, subject)` when one is given.
pub async fn receiver_holding(
    schema: &str,
    policies: &str,
    key: Option<(&str, &str)>,
) -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(schema, policies)
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
pub fn insert_order<T>(conn: &mut ConnettoConnection<T>, id: i64, owner: &str, status: &str)
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

/// The replica's orders, id order.
pub fn order_rows<T>(conn: &mut ConnettoConnection<T>) -> Vec<(i64, String, String)>
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

/// Write one board's label through the connection's capture, inserting it
/// when `insert` and updating it otherwise.
pub fn write_board<T>(conn: &mut ConnettoConnection<T>, id: i64, label: &str, insert: bool)
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
pub fn board_rows<T>(conn: &mut ConnettoConnection<T>) -> Vec<(i64, String)>
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

/// The queue entry `seq` of the writer number 1, read raw because
/// `_connetto_pending` is an internal table the typed schema does not name.
pub fn pending_changeset<T>(conn: &mut ConnettoConnection<T>, seq: i64) -> Vec<u8>
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

/// One write a test applies to an author replica before a flush.
pub type AuthorWrite<'a> = &'a dyn Fn(&mut ConnettoConnection<Script>);

/// The bodies an author replica over `ddl` captures for `writes`, one
/// flushed queue entry per write, in order.
pub async fn captured<const N: usize>(ddl: &str, writes: [AuthorWrite<'_>; N]) -> [Vec<u8>; N] {
    let (mut author_replica, _) = plain_author(ddl).await;
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
pub fn stored_entries<T>(conn: &mut ConnettoConnection<T>) -> Vec<(Vec<u8>, i64)>
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

/// One row of the provenance table, as the typed read stands it.
pub fn provenance_of<'a>(
    rows: &'a [ProvisionalRow],
    table: &str,
    key: &[KeyValue],
) -> Option<&'a ProvisionalRow> {
    rows.iter()
        .find(|row| row.table == table && row.key == *key)
}

/// A cursor of seven zero bytes and `byte`.
pub fn cursor_byte(byte: u8) -> Cursor {
    Cursor::new(vec![0, 0, 0, 0, 0, 0, 0, byte])
}

/// Pump until `pred` matches, refusing to outlive the wire.
pub async fn pump_to<T>(
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
pub async fn drain_quiet<T>(conn: &mut ConnettoConnection<T>, window: Duration) -> Vec<ClientEvent>
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
