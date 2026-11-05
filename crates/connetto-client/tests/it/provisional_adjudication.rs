//! The server's ruling on the receiver's provisional rows (R77 decision 11).
//!
//! Each replica here applies the author's entries over a link and holds them
//! provisionally, then asks over a server connection. What is proved is the
//! client half of the ruling, the asks it plans at attach, the single ask it
//! carries per author, the verdicts it applies to the rows, and the drops
//! that end the session's ask of an author.

use std::time::Duration;

use connetto_client::peer::{HeldState, PeerAuthor};
use connetto_client::provisional::{ProvisionalState, encode_key};
use connetto_client::{ClientEvent, ConnettoConnection, KeyValue};
use connetto_core::exchange::{RetractCause, Verdict};
use connetto_core::messages::{
    ControlMessage, MutationRejectReason, NonFatalError, PROVISIONAL_ASK_REFUSED,
    ProvisionalStatus, ProvisionalVerdicts, RateLimited,
};
use connetto_core::traits::IncomingFrame;
use diesel::prelude::*;

use super::peer_support::{captured, manifest, pump_to, receiver};
use super::support::{self, Script};

/// The author replica's schema, the receiver's `orders` table with no
/// policies, so its own writes capture whatever it chooses.
const PLAIN_DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                          status TEXT NOT NULL);";

/// The receiver replica's source, the same table behind translated policies.
const DDL: &str = "-- connetto:major 1\n \
                   CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                   status TEXT NOT NULL); \
                   ALTER TABLE orders ENABLE ROW LEVEL SECURITY;";

/// Every person reads `orders` but writes only rows they own, so the
/// receiver sees an author's rows as that author.
const POLICIES: &str = "CREATE POLICY orders_read ON orders FOR SELECT USING (true); \
                        CREATE POLICY orders_insert ON orders FOR INSERT \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_update ON orders FOR UPDATE \
                        USING (owner = current_setting('app.user_id', true)) \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_delete ON orders FOR DELETE \
                        USING (owner = current_setting('app.user_id', true));";

const AUTHOR_A: [u8; 32] = [7; 32];
const AUTHOR_B: [u8; 32] = [8; 32];
const AUTHOR_C: [u8; 32] = [9; 32];
const OFFER_A: &[u8] = b"signed-offer-a";
const OFFER_C: &[u8] = b"signed-offer-c";

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
        /// What the entry is held as, a `HeldState`.
        state -> BigInt,
        /// The major the entry was captured under.
        major -> BigInt,
        /// The entry's body, absent for a digest-only entry.
        changeset -> Nullable<Binary>,
        /// The author's certificate account.
        account -> Text,
        /// The author's bot list, when the author carries one.
        bot_list -> Nullable<Text>,
        /// The wall clock the entry was received at.
        received_at -> BigInt,
        /// The replica-wide append position.
        position -> BigInt,
    }
}

diesel::table! {
    /// The author's stored cursor, head and offer, read raw.
    _connetto_peer_cursor (author) {
        /// The author's key id.
        author -> Binary,
        /// The author's certificate account.
        account -> Text,
        /// The last verified sequence.
        seq -> BigInt,
        /// The chain head at it.
        head -> Binary,
        /// The author's last verified signed offer, as `MessagePack`.
        offer -> Binary,
        /// When the author's certificate ends, in Unix seconds.
        expires_at -> BigInt,
    }
}

diesel::table! {
    /// The peer-applied rows' provenance, read raw.
    _connetto_provisional (tbl, pk) {
        /// The replica table holding the row.
        tbl -> Text,
        /// The row's primary key, encoded.
        pk -> Binary,
        /// The author's key id.
        author -> Binary,
        /// The author's queue sequence the row came from.
        seq -> BigInt,
        /// The author's certificate account.
        account -> Text,
        /// The wall clock the row was received at.
        received_at -> BigInt,
        /// Whether the row still awaits a verdict.
        state -> BigInt,
        /// The author the receiver's own write displaced.
        displaced_author -> Nullable<Binary>,
        /// That author's queue sequence.
        displaced_seq -> Nullable<BigInt>,
    }
}

/// The 32-byte device author the entries apply under.
fn author_of(key: [u8; 32]) -> PeerAuthor {
    PeerAuthor {
        key_id: key.to_vec(),
        account: "peer-anna".to_owned(),
        bot_list: Some("*".to_owned()),
    }
}

/// Insert one order through the connection's capture.
fn insert_order(conn: &mut ConnettoConnection<Script>, id: i64) {
    diesel::insert_into(orders::table)
        .values((
            orders::id.eq(id),
            orders::owner.eq("peer-anna"),
            orders::status.eq("open"),
        ))
        .execute(conn.conn())
        .expect("insert the order");
}

/// Store `author`'s cursor row with `offer`, the offer the ask will carry.
fn seed_cursor<T>(conn: &mut ConnettoConnection<T>, author: &[u8; 32], seq: i64, offer: &[u8])
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::insert_into(_connetto_peer_cursor::table)
        .values((
            _connetto_peer_cursor::author.eq(author.to_vec()),
            _connetto_peer_cursor::account.eq("peer-anna"),
            _connetto_peer_cursor::seq.eq(seq),
            _connetto_peer_cursor::head.eq(vec![0u8; 32]),
            _connetto_peer_cursor::offer.eq(offer.to_vec()),
            _connetto_peer_cursor::expires_at.eq(4_102_444_800_i64),
        ))
        .execute(conn.conn())
        .expect("seed the cursor");
}

/// Write `count` provisional records naming `author`, at sequences 1 to
/// `count`, no replica row or stored entry beside them, the keys standing at
/// `key_offset` past any of the replica's own rows.
fn bulk_records<T>(conn: &mut ConnettoConnection<T>, author: &[u8; 32], count: i64, key_offset: i64)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    for seq in 1..=count {
        diesel::insert_into(_connetto_provisional::table)
            .values((
                _connetto_provisional::tbl.eq("orders"),
                _connetto_provisional::pk.eq(encode_key(&[KeyValue::Int(seq + key_offset)])),
                _connetto_provisional::author.eq(author.to_vec()),
                _connetto_provisional::seq.eq(seq),
                _connetto_provisional::account.eq("peer-anna"),
                _connetto_provisional::received_at.eq(1_700_000_000),
                _connetto_provisional::state.eq(ProvisionalState::Provisional.db()),
            ))
            .execute(conn.conn())
            .expect("seed the record");
    }
}

/// The records naming `author`, sequence and stored state, seq order.
fn record_states<T>(conn: &mut ConnettoConnection<T>, author: &[u8; 32]) -> Vec<(i64, i64)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    _connetto_provisional::table
        .filter(_connetto_provisional::author.eq(author.to_vec()))
        .select((_connetto_provisional::seq, _connetto_provisional::state))
        .order(_connetto_provisional::seq.asc())
        .load(conn.conn())
        .expect("read the records")
}

/// The stored entry sequences of `author`, seq order.
fn entry_seqs<T>(conn: &mut ConnettoConnection<T>, author: &[u8; 32]) -> Vec<i64>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    entry_states(conn, author)
        .into_iter()
        .map(|(seq, _)| seq)
        .collect()
}

/// The stored entries of `author`, sequence and state, seq order.
fn entry_states<T>(conn: &mut ConnettoConnection<T>, author: &[u8; 32]) -> Vec<(i64, i64)>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    _connetto_peer_entry::table
        .filter(_connetto_peer_entry::author.eq(author.to_vec()))
        .select((_connetto_peer_entry::seq, _connetto_peer_entry::state))
        .order(_connetto_peer_entry::seq.asc())
        .load(conn.conn())
        .expect("read the entries")
}

/// The replica's orders, id order.
fn order_ids<T>(conn: &mut ConnettoConnection<T>) -> Vec<i64>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    orders::table
        .select(orders::id)
        .order(orders::id.asc())
        .load(conn.conn())
        .expect("read the orders")
}

/// A receiver replica with the author's entries applied provisionally at
/// sequences 1 to `bodies.len()`, the author's offer stored, its wired
/// script dropped.
async fn provisioned(bodies: &[Vec<u8>]) -> ConnettoConnection<Script> {
    let (mut conn, _) = receiver(DDL, POLICIES).await;
    let author = author_of(AUTHOR_A);
    for (seq, body) in bodies.iter().enumerate() {
        let seq = u64::try_from(seq).expect("a test sequence fits a u64") + 1;
        conn.apply_peer_entry(&author, &manifest(seq, body), body)
            .expect("apply the entry");
    }
    seed_cursor(
        &mut conn,
        &AUTHOR_A,
        i64::try_from(bodies.len()).expect("a count fits an i64"),
        OFFER_A,
    );
    conn
}

/// Re-attach `conn` over a fresh scripted wire that answers the handshake.
async fn attach(mut conn: ConnettoConnection<Script>) -> (ConnettoConnection<Script>, Script) {
    let script = Script::default();
    script.say(support::handshake_ack());
    conn.attach(script.clone()).await.expect("attach");
    (conn, script)
}

/// The provisional asks the client sent, in the order it sent them.
fn asks(script: &Script) -> Vec<ProvisionalStatus> {
    script.sent(|message| match message {
        ControlMessage::ProvisionalStatus(ask) => Some(ask.clone()),
        _ => None,
    })
}

/// Queue one ruling on `script`, the frames a ruling of `verdicts` carries.
fn rule(script: &Script, request_id: &str, verdicts: Vec<(u64, Verdict)>) {
    script.say(IncomingFrame::Control(ControlMessage::ProvisionalVerdicts(
        ProvisionalVerdicts {
            request_id: request_id.to_owned(),
            verdicts,
        },
    )));
}

const APPLIED: &str = "a provisional ruling was applied";
const NO_OPEN_ASK: &str = "a provisional ruling arrived with no open ask";

fn applied(event: &ClientEvent) -> bool {
    matches!(event, ClientEvent::NonFatal { detail, .. } if detail == APPLIED)
}

fn no_open_ask(event: &ClientEvent) -> bool {
    matches!(event, ClientEvent::NonFatal { detail, .. } if detail == NO_OPEN_ASK)
}

/// The ask carries the author's lowest sequences up to the bound and the
/// stored offer, and the next author's ask goes out only once the first
/// ruling stands. An author with no stored offer is never asked.
#[tokio::test]
async fn attach_asks_each_author_it_holds_an_offer_from() {
    let (mut conn, _) = receiver(DDL, POLICIES).await;
    let author = author_of(AUTHOR_A);
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let w2 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 2);
    let w3 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 3);
    let [b1, b2, b3] = captured(PLAIN_DDL, [&w1, &w2, &w3]).await;
    conn.apply_peer_entry(&author, &manifest(1, &b1), &b1)
        .expect("apply sequence 1");
    conn.apply_peer_entry(&author, &manifest(2, &b2), &b2)
        .expect("apply sequence 2");
    conn.apply_peer_entry(&author, &manifest(3, &b3), &b3)
        .expect("apply sequence 3");
    seed_cursor(&mut conn, &AUTHOR_A, 3, OFFER_A);
    // AUTHOR_B's rows hold no stored offer, so nothing of the author is
    // ever asked.
    bulk_records(&mut conn, &AUTHOR_B, 3, 1_000);
    // AUTHOR_C is asked once the ruling for AUTHOR_A stands, one at a time.
    bulk_records(&mut conn, &AUTHOR_C, 3, 2_000);
    seed_cursor(&mut conn, &AUTHOR_C, 3, OFFER_C);

    conn.disconnect();
    let (mut conn, script) = attach(conn).await;

    let [first] = asks(&script).try_into().expect("one ask at a time");
    assert_eq!(first.author, AUTHOR_A);
    assert_eq!(first.seqs, vec![1, 2, 3]);
    assert_eq!(first.offer, OFFER_A);

    rule(
        &script,
        &first.request_id,
        vec![
            (1, Verdict::Pending),
            (2, Verdict::Pending),
            (3, Verdict::Pending),
        ],
    );
    pump_to(&mut conn, applied).await;

    let asks = asks(&script);
    let second = &asks[1];
    assert_eq!(second.author, AUTHOR_C);
    assert_eq!(second.seqs, vec![1, 2, 3]);
    assert_eq!(second.offer, OFFER_C);
    assert!(
        !asks.iter().any(|ask| ask.author == AUTHOR_B),
        "an author with no stored offer is never asked"
    );
}

/// `Applied` and `Settled` adjudicate the entry the way the frontier does,
/// the records move and the entries are retained, the rows standing.
/// `Pending` keeps.
#[tokio::test]
async fn an_applied_ruling_adjudicates_the_entry_and_keeps_its_rows() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let w2 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 2);
    let w3 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 3);
    let bodies = captured(PLAIN_DDL, [&w1, &w2, &w3]).await;
    let mut conn = provisioned(&bodies).await;
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");

    rule(
        &script,
        &ask.request_id,
        vec![
            (1, Verdict::Applied),
            (2, Verdict::Settled),
            (3, Verdict::Pending),
        ],
    );
    pump_to(&mut conn, applied).await;

    assert_eq!(
        record_states(&mut conn, &AUTHOR_A),
        vec![
            (1, ProvisionalState::Adjudicated.db()),
            (2, ProvisionalState::Adjudicated.db()),
            (3, ProvisionalState::Provisional.db()),
        ]
    );
    assert_eq!(
        entry_states(&mut conn, &AUTHOR_A),
        vec![
            (1, HeldState::Retained.db()),
            (2, HeldState::Retained.db()),
            (3, HeldState::Provisional.db()),
        ]
    );
    assert_eq!(order_ids(&mut conn), vec![1, 2, 3]);
}

/// Insert one order another person owns, which the receiver's policies
/// refuse from the author.
fn insert_foreign(conn: &mut ConnettoConnection<Script>, id: i64) {
    diesel::insert_into(orders::table)
        .values((
            orders::id.eq(id),
            orders::owner.eq("mallory"),
            orders::status.eq("open"),
        ))
        .execute(conn.conn())
        .expect("insert the order");
}

/// The row of `body` at `seq` captured under the major 2 another build
/// declares.
fn other_build(seq: u64, body: &[u8]) -> connetto_core::exchange::ManifestRow {
    connetto_core::exchange::ManifestRow {
        major: connetto_core::Major::new(2).expect("major 2"),
        ..manifest(seq, body)
    }
}

/// An ask covers the courier-only and digest-only entries beside the
/// provisional ones, and never a retained one (R78 held-row table).
#[tokio::test]
async fn an_ask_covers_every_held_entry_but_the_retained() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let w2 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 2);
    let m3 = |conn: &mut ConnettoConnection<Script>| insert_foreign(conn, 3);
    let w4 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 4);
    let [b1, b2, b3, b4] = captured(PLAIN_DDL, [&w1, &w2, &m3, &w4]).await;
    let (mut conn, _) = receiver(DDL, POLICIES).await;
    let author = author_of(AUTHOR_A);
    seed_cursor(&mut conn, &AUTHOR_A, 4, OFFER_A);
    conn.apply_peer_entry(&author, &manifest(1, &b1), &b1)
        .expect("apply sequence 1");
    conn.adjudicate_peer_frontier(&AUTHOR_A, 1)
        .expect("retain sequence 1");
    conn.apply_peer_entry(&author, &manifest(2, &b2), &b2)
        .expect("apply sequence 2");
    conn.apply_peer_entry(&author, &manifest(3, &b3), &b3)
        .expect("hold sequence 3 courier-only");
    conn.apply_peer_entry(&author, &other_build(4, &b4), &b4)
        .expect("hold sequence 4 digest-only");
    assert_eq!(
        entry_states(&mut conn, &AUTHOR_A),
        vec![
            (1, HeldState::Retained.db()),
            (2, HeldState::Provisional.db()),
            (3, HeldState::CourierOnly.db()),
            (4, HeldState::DigestOnly.db()),
        ]
    );

    conn.disconnect();
    let (_conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");
    assert_eq!(ask.author, AUTHOR_A);
    assert_eq!(ask.seqs, vec![2, 3, 4]);
}

/// A ruling retains a courier-only or digest-only entry it settles and
/// drops one it refuses or abandons, with nothing to roll back.
#[tokio::test]
async fn a_ruling_retains_or_drops_the_unapplied_entries() {
    let m1 = |conn: &mut ConnettoConnection<Script>| insert_foreign(conn, 1);
    let w2 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 2);
    let m3 = |conn: &mut ConnettoConnection<Script>| insert_foreign(conn, 3);
    let w4 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 4);
    let [b1, b2, b3, b4] = captured(PLAIN_DDL, [&m1, &w2, &m3, &w4]).await;
    let (mut conn, _) = receiver(DDL, POLICIES).await;
    let author = author_of(AUTHOR_A);
    conn.apply_peer_entry(&author, &manifest(1, &b1), &b1)
        .expect("hold sequence 1 courier-only");
    conn.apply_peer_entry(&author, &other_build(2, &b2), &b2)
        .expect("hold sequence 2 digest-only");
    conn.apply_peer_entry(&author, &manifest(3, &b3), &b3)
        .expect("hold sequence 3 courier-only");
    conn.apply_peer_entry(&author, &other_build(4, &b4), &b4)
        .expect("hold sequence 4 digest-only");
    seed_cursor(&mut conn, &AUTHOR_A, 4, OFFER_A);
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");
    assert_eq!(ask.seqs, vec![1, 2, 3, 4]);

    rule(
        &script,
        &ask.request_id,
        vec![
            (1, Verdict::Applied),
            (2, Verdict::Settled),
            (3, Verdict::Refused(RetractCause::Conflict)),
            (4, Verdict::Abandoned),
        ],
    );
    pump_to(&mut conn, applied).await;
    assert_eq!(
        entry_states(&mut conn, &AUTHOR_A),
        vec![(1, HeldState::Retained.db()), (2, HeldState::Retained.db()),]
    );
    assert_eq!(order_ids(&mut conn), Vec::<i64>::new());
}

/// A ruling that reaches an entry the author's frontier retained while the
/// ask was out changes nothing of it, a refusal included, since a retained
/// entry settled for good (R78 held-row table).
#[tokio::test]
async fn a_ruling_for_a_retained_entry_changes_nothing() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let w2 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 2);
    let bodies = captured(PLAIN_DDL, [&w1, &w2]).await;
    let mut conn = provisioned(&bodies).await;
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");
    assert_eq!(ask.seqs, vec![1, 2]);
    conn.adjudicate_peer_frontier(&AUTHOR_A, 1)
        .expect("the frontier retains sequence 1");

    rule(
        &script,
        &ask.request_id,
        vec![
            (1, Verdict::Refused(RetractCause::Conflict)),
            (2, Verdict::Applied),
        ],
    );
    pump_to(&mut conn, applied).await;
    assert_eq!(
        entry_states(&mut conn, &AUTHOR_A),
        vec![(1, HeldState::Retained.db()), (2, HeldState::Retained.db()),]
    );
    assert_eq!(order_ids(&mut conn), vec![1, 2], "no row rolled back");
}

/// `Refused` and `Abandoned` retract the entry through the retraction, with
/// the cause, the rows rolling back.
#[tokio::test]
async fn a_refused_ruling_retracts_the_entry_with_its_cause() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let w2 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 2);
    let w3 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 3);
    let bodies = captured(PLAIN_DDL, [&w1, &w2, &w3]).await;
    let mut conn = provisioned(&bodies).await;
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");

    rule(
        &script,
        &ask.request_id,
        vec![
            (
                1,
                Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized)),
            ),
            (2, Verdict::Refused(RetractCause::Conflict)),
            (3, Verdict::Abandoned),
        ],
    );

    // The ruling's own result stands first, then each retraction it raised.
    pump_to(&mut conn, applied).await;

    let ClientEvent::ProvisionalRetracted {
        author,
        seq,
        cause,
        rows,
    } = pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { seq: 1, .. })
    })
    .await
    else {
        unreachable!("pump_to matched the retraction");
    };
    assert_eq!(author, AUTHOR_A);
    assert_eq!(seq, 1);
    assert_eq!(
        cause,
        RetractCause::Rejected(MutationRejectReason::Unauthorized)
    );
    assert_eq!(rows, vec![("orders".to_owned(), vec![KeyValue::Int(1)])]);

    let seen = pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { seq: 2, .. })
    })
    .await;
    assert!(matches!(
        seen,
        ClientEvent::ProvisionalRetracted {
            cause: RetractCause::Conflict,
            ..
        }
    ));
    let seen = pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { seq: 3, .. })
    })
    .await;
    assert!(matches!(
        seen,
        ClientEvent::ProvisionalRetracted {
            cause: RetractCause::Abandoned,
            ..
        }
    ));

    assert_eq!(record_states(&mut conn, &AUTHOR_A), [] as [(i64, i64); 0]);
    assert_eq!(entry_seqs(&mut conn, &AUTHOR_A), [] as [i64; 0]);
    assert_eq!(order_ids(&mut conn), [] as [i64; 0]);
}

/// A ruling that quotes no open ask stands ignored, the rows and records
/// standing as they stood.
#[tokio::test]
async fn a_ruling_for_no_open_ask_stands_ignored() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let bodies = captured(PLAIN_DDL, [&w1]).await;
    let mut conn = provisioned(&bodies).await;
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");

    rule(&script, "never-sent", vec![(1, Verdict::Abandoned)]);
    pump_to(&mut conn, no_open_ask).await;

    assert_eq!(
        record_states(&mut conn, &AUTHOR_A),
        vec![(1, ProvisionalState::Provisional.db())]
    );
    assert_eq!(order_ids(&mut conn), vec![1]);

    // The ask itself still stands, so its own ruling applies.
    rule(&script, &ask.request_id, vec![(1, Verdict::Pending)]);
    pump_to(&mut conn, applied).await;
    assert_eq!(
        record_states(&mut conn, &AUTHOR_A),
        vec![(1, ProvisionalState::Provisional.db())]
    );
}

/// An author with more sequences than the manifest bound is asked about in
/// ascending chunks of the bound, the next sent once the last is ruled.
#[tokio::test]
async fn an_overbound_author_is_asked_in_chunks_one_at_a_time() {
    let bound = i64::try_from(connetto_core::exchange::MAX_MANIFEST_ROWS)
        .expect("the manifest bound fits an i64");
    let count = bound + 1;
    let (mut conn, _) = receiver(DDL, POLICIES).await;
    bulk_records(&mut conn, &AUTHOR_A, count, 0);
    seed_cursor(&mut conn, &AUTHOR_A, count, OFFER_A);

    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [first] = asks(&script).try_into().expect("one ask at a time");
    assert_eq!(first.author, AUTHOR_A);
    assert_eq!(first.seqs.len(), connetto_core::exchange::MAX_MANIFEST_ROWS);
    assert_eq!(first.seqs.first().copied(), Some(1));
    assert_eq!(
        first.seqs.last().copied(),
        Some(u64::try_from(bound).expect("the bound fits a u64"))
    );

    let pending = first
        .seqs
        .iter()
        .map(|seq| (*seq, Verdict::Pending))
        .collect();
    rule(&script, &first.request_id, pending);
    pump_to(&mut conn, applied).await;

    let [_, second] = asks(&script).try_into().expect("the next chunk follows");
    assert_eq!(second.author, AUTHOR_A);
    assert_ne!(second.request_id, first.request_id);
    assert_eq!(
        second.seqs,
        vec![u64::try_from(count).expect("the count fits a u64")]
    );
}

/// A ruling that overruns its bound drops the ask without dropping the
/// connection, and the next session re-asks the author.
#[tokio::test]
async fn a_lapsed_ask_drops_and_the_next_session_re_asks() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let bodies = captured(PLAIN_DDL, [&w1]).await;
    let mut conn = provisioned(&bodies).await;
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");

    // The ruling overruns its bound before any pump checks it.
    tokio::time::sleep(Duration::from_secs(31)).await;
    conn.pump_one().await.expect("pump");

    // A ruling that arrives after the drop is one for no open ask.
    rule(&script, &ask.request_id, vec![(1, Verdict::Abandoned)]);
    pump_to(&mut conn, no_open_ask).await;
    assert_eq!(
        record_states(&mut conn, &AUTHOR_A),
        vec![(1, ProvisionalState::Provisional.db())]
    );

    // The next session re-asks the author, whose rows still stand.
    conn.disconnect();
    let (_, script) = attach(conn).await;
    let [again] = asks(&script).try_into().expect("the re-ask");
    assert_eq!(again.author, AUTHOR_A);
    assert_eq!(again.seqs, vec![1]);
}

/// A deferral that names the open ask drops it for the session, the next
/// session re-asking the author.
#[tokio::test]
async fn a_deferral_naming_the_ask_drops_it_for_the_session() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let bodies = captured(PLAIN_DDL, [&w1]).await;
    let mut conn = provisioned(&bodies).await;
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");

    script.say(IncomingFrame::Control(ControlMessage::RateLimited(
        RateLimited {
            related_to: Some(ask.request_id.clone()),
            retry_after_ms: 0,
        },
    )));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::RateLimited { related_to, .. }
                if related_to.as_deref() == Some(ask.request_id.as_str()))
    })
    .await;

    rule(&script, &ask.request_id, vec![(1, Verdict::Abandoned)]);
    pump_to(&mut conn, no_open_ask).await;

    conn.disconnect();
    let (_, script) = attach(conn).await;
    let [again] = asks(&script).try_into().expect("the re-ask");
    assert_eq!(again.author, AUTHOR_A);
}

/// A refusal that names the open ask drops it for the session, the next
/// session re-asking the author.
#[tokio::test]
async fn a_refusal_naming_the_ask_drops_it_for_the_session() {
    let w1 = |conn: &mut ConnettoConnection<Script>| insert_order(conn, 1);
    let bodies = captured(PLAIN_DDL, [&w1]).await;
    let mut conn = provisioned(&bodies).await;
    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [ask] = asks(&script).try_into().expect("one ask");

    script.say(IncomingFrame::Control(ControlMessage::NonFatalError(
        NonFatalError {
            related_to: Some(ask.request_id.clone()),
            detail: PROVISIONAL_ASK_REFUSED.to_owned(),
        },
    )));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::NonFatal { detail, .. }
                if detail == PROVISIONAL_ASK_REFUSED)
    })
    .await;

    rule(&script, &ask.request_id, vec![(1, Verdict::Abandoned)]);
    pump_to(&mut conn, no_open_ask).await;

    conn.disconnect();
    let (_, script) = attach(conn).await;
    let [again] = asks(&script).try_into().expect("the re-ask");
    assert_eq!(again.author, AUTHOR_A);
}

/// A refusal that drops one author's ask hands the session on to the next
/// author's ask, so one refused author never holds the others back.
#[tokio::test]
async fn a_dropped_ask_hands_the_session_on_to_the_next_author() {
    let (mut conn, _) = receiver(DDL, POLICIES).await;
    bulk_records(&mut conn, &AUTHOR_A, 1, 0);
    seed_cursor(&mut conn, &AUTHOR_A, 1, OFFER_A);
    bulk_records(&mut conn, &AUTHOR_C, 1, 100);
    seed_cursor(&mut conn, &AUTHOR_C, 1, OFFER_C);

    conn.disconnect();
    let (mut conn, script) = attach(conn).await;
    let [first] = asks(&script).try_into().expect("one ask at a time");
    assert_eq!(first.author, AUTHOR_A);

    script.say(IncomingFrame::Control(ControlMessage::NonFatalError(
        NonFatalError {
            related_to: Some(first.request_id.clone()),
            detail: PROVISIONAL_ASK_REFUSED.to_owned(),
        },
    )));
    pump_to(&mut conn, |event| {
        matches!(event, ClientEvent::NonFatal { detail, .. }
                if detail == PROVISIONAL_ASK_REFUSED)
    })
    .await;
    rule(&script, &first.request_id, vec![(1, Verdict::Pending)]);
    pump_to(&mut conn, no_open_ask).await;

    let [_, second] = asks(&script)
        .try_into()
        .expect("the next author's ask follows");
    assert_eq!(second.author, AUTHOR_C);
}
