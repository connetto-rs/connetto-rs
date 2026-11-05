//! The receiver's courier hold (R78 decisions 3, 17 and 19).
//!
//! A receiver keeps every entry a verified range carried until a verdict
//! reaches it: an applied one provisional, an own-build body its policies
//! refuse courier-only, another build's entry digest-only, and once
//! adjudicated each is retained for a courier to bring back to a restored
//! server. Each test here is one row of the plan's held-row table.

use connetto_client::peer::{HeldState, PeerApplyOutcome, PeerRefusal};
use connetto_client::{ClientEvent, ConnettoConnection};
use connetto_core::Major;
use connetto_core::exchange::{ManifestRow, RetractCause, Retraction};
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, SnapshotBegin, SnapshotEnd,
    SnapshotPatch, SubscriptionPriority,
};
use connetto_core::traits::IncomingFrame;
use diesel::prelude::*;
use sqlite_diff_rs::{PatchSet, SimpleTable};

use super::peer_support::{
    _connetto_peer_entry, author, captured, cursor_byte, held_entries, hold_cursor, insert_order,
    manifest, order_rows, pump_to, receiver,
};
use super::support::Script;

/// The author replica's schema, no policies, so its writes capture whatever
/// it chooses.
const PLAIN_DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                          status TEXT NOT NULL);";

/// The receiver replica's source, the same table behind translated policies.
const POLICY_SCHEMA: &str = "-- connetto:major 1\n \
                             CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                             status TEXT NOT NULL); \
                             ALTER TABLE orders ENABLE ROW LEVEL SECURITY;";

/// Every person reads `orders` but writes only rows they own.
const POLICIES: &str = "CREATE POLICY orders_read ON orders FOR SELECT USING (true); \
                        CREATE POLICY orders_insert ON orders FOR INSERT \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_update ON orders FOR UPDATE \
                        USING (owner = current_setting('app.user_id', true)) \
                        WITH CHECK (owner = current_setting('app.user_id', true)); \
                        CREATE POLICY orders_delete ON orders FOR DELETE \
                        USING (owner = current_setting('app.user_id', true));";

/// The key `author-a` names.
fn anna() -> Vec<u8> {
    "author-a".as_bytes().to_vec()
}

/// The stored body and digest of `anna`'s entry `seq`.
fn body_and_digest<T>(conn: &mut ConnettoConnection<T>, seq: i64) -> (Option<Vec<u8>>, Vec<u8>)
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    _connetto_peer_entry::table
        .filter(_connetto_peer_entry::author.eq(anna()))
        .filter(_connetto_peer_entry::seq.eq(seq))
        .select((
            _connetto_peer_entry::changeset,
            _connetto_peer_entry::digest,
        ))
        .first(conn.conn())
        .expect("read the entry")
}

/// The row of `body` at `seq` captured under the major 2 another build
/// declares.
fn other_build(seq: u64, body: &[u8]) -> ManifestRow {
    ManifestRow {
        major: Major::new(2).expect("major 2"),
        ..manifest(seq, body)
    }
}

/// An own-build body the receiver's policies refuse stays held courier-only,
/// its body and digest kept and nothing of it applied, beside the refusal
/// the application hears.
#[tokio::test]
async fn a_refused_entry_is_held_courier_only_with_its_body() {
    let [body] = captured(
        PLAIN_DDL,
        [&|conn| insert_order(conn, 1, "mallory", "open")],
    )
    .await;
    let (mut recv, _) = receiver(POLICY_SCHEMA, POLICIES).await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &manifest(7, &body), &body)
        .expect("apply the entry");
    assert!(matches!(
        out,
        PeerApplyOutcome::Refused {
            cause: PeerRefusal::Policy,
            ..
        }
    ));
    assert_eq!(order_rows(&mut recv), Vec::new(), "nothing applied");
    assert_eq!(
        held_entries(&mut recv),
        vec![(anna(), 7, HeldState::CourierOnly)]
    );
    assert_eq!(
        body_and_digest(&mut recv, 7),
        (Some(body.clone()), manifest(7, &body).digest.to_vec())
    );
    assert!(
        recv.provisional_rows()
            .expect("read the records")
            .is_empty(),
        "a refused entry writes no record"
    );
}

/// An entry of another build stays held digest-only, its manifest row kept
/// and its body dropped.
#[tokio::test]
async fn an_entry_of_another_build_is_held_digest_only() {
    let [body] = captured(
        PLAIN_DDL,
        [&|conn| insert_order(conn, 1, "peer-anna", "open")],
    )
    .await;
    let (mut recv, _) = receiver(POLICY_SCHEMA, POLICIES).await;
    let a = author("author-a", "peer-anna", Some("*"));
    let out = recv
        .apply_peer_entry(&a, &other_build(7, &body), &body)
        .expect("apply the entry");
    assert!(matches!(out, PeerApplyOutcome::Skipped));
    assert_eq!(order_rows(&mut recv), Vec::new(), "nothing applied");
    assert_eq!(
        held_entries(&mut recv),
        vec![(anna(), 7, HeldState::DigestOnly)]
    );
    assert_eq!(
        body_and_digest(&mut recv, 7),
        (None, manifest(7, &body).digest.to_vec())
    );
}

/// A receiver holding the author's entries 7 provisional, 8 courier-only,
/// 9 digest-only and 10 provisional, the provisional ones on order 1.
async fn holding_every_kind() -> ConnettoConnection<Script> {
    let [p7, o8, d9, p10] = captured(
        PLAIN_DDL,
        [
            &|conn| insert_order(conn, 1, "peer-anna", "open"),
            &|conn| insert_order(conn, 2, "mallory", "open"),
            &|conn| insert_order(conn, 3, "peer-anna", "open"),
            &|conn| insert_order(conn, 4, "peer-anna", "open"),
        ],
    )
    .await;
    let (mut recv, _) = receiver(POLICY_SCHEMA, POLICIES).await;
    hold_cursor(&mut recv, &anna());
    let a = author("author-a", "peer-anna", Some("*"));
    for (row, body) in [
        (manifest(7, &p7), &p7),
        (manifest(8, &o8), &o8),
        (other_build(9, &d9), &d9),
        (manifest(10, &p10), &p10),
    ] {
        recv.apply_peer_entry(&a, &row, body)
            .expect("apply the entry");
    }
    assert_eq!(
        held_entries(&mut recv),
        vec![
            (anna(), 7, HeldState::Provisional),
            (anna(), 8, HeldState::CourierOnly),
            (anna(), 9, HeldState::DigestOnly),
            (anna(), 10, HeldState::Provisional),
        ]
    );
    recv
}

/// The author's frontier retains every held entry at or below it,
/// provisional, courier-only and digest-only alike, the provisional ones'
/// records settling and their rows standing, and leaves the newer entry
/// provisional.
#[tokio::test]
async fn a_frontier_retains_every_held_entry_at_or_below_it() {
    let mut recv = holding_every_kind().await;
    recv.adjudicate_peer_frontier(&anna(), 9)
        .expect("adjudicate at 9");
    assert_eq!(
        held_entries(&mut recv),
        vec![
            (anna(), 7, HeldState::Retained),
            (anna(), 8, HeldState::Retained),
            (anna(), 9, HeldState::Retained),
            (anna(), 10, HeldState::Provisional),
        ]
    );
    let records = recv.provisional_rows().expect("read the records");
    assert_eq!(
        records
            .iter()
            .map(|record| (record.seq, record.state))
            .collect::<Vec<_>>(),
        vec![
            (
                7,
                connetto_client::provisional::ProvisionalState::Adjudicated
            ),
            (
                10,
                connetto_client::provisional::ProvisionalState::Provisional
            ),
        ]
    );
    assert_eq!(
        order_rows(&mut recv)
            .into_iter()
            .map(|(id, ..)| id)
            .collect::<Vec<_>>(),
        vec![1, 4]
    );
}

/// The author's retraction takes a courier-only and a digest-only entry
/// with nothing to invert, and passes a retained one by.
#[tokio::test]
async fn a_retraction_takes_unapplied_entries_and_passes_retained_ones() {
    let mut recv = holding_every_kind().await;
    recv.adjudicate_peer_frontier(&anna(), 7)
        .expect("adjudicate at 7");
    let a = author("author-a", "peer-anna", Some("*"));
    for seq in [7, 8, 9] {
        recv.retract_peer_entry(
            &a,
            &Retraction {
                seq,
                cause: RetractCause::Conflict,
            },
        )
        .expect("retract the entry");
    }
    assert_eq!(
        held_entries(&mut recv),
        vec![
            (anna(), 7, HeldState::Retained),
            (anna(), 10, HeldState::Provisional),
        ]
    );
    assert_eq!(
        order_rows(&mut recv)
            .into_iter()
            .map(|(id, ..)| id)
            .collect::<Vec<_>>(),
        vec![1, 4],
        "the retained entry's row stands"
    );
}

/// A full resync's re-apply takes the provisional entries alone: a retained
/// entry's row is the server's to send, and a courier-only or digest-only
/// entry never applies.
#[tokio::test]
async fn a_resync_reapplies_only_the_provisional_entries() {
    let [t7, o8, d9, p10] = captured(
        PLAIN_DDL,
        [
            &|conn| insert_order(conn, 1, "peer-anna", "open"),
            &|conn| insert_order(conn, 2, "mallory", "open"),
            &|conn| insert_order(conn, 3, "peer-anna", "open"),
            &|conn| insert_order(conn, 4, "peer-anna", "open"),
        ],
    )
    .await;
    let (mut recv, server) = receiver(POLICY_SCHEMA, POLICIES).await;
    recv.subscribe("a", "SELECT * FROM orders")
        .await
        .expect("subscribe");
    deliver_empty_snapshot(&mut recv, &server, 1).await;
    hold_cursor(&mut recv, &anna());
    let a = author("author-a", "peer-anna", Some("*"));
    for (row, body) in [
        (manifest(7, &t7), &t7),
        (manifest(8, &o8), &o8),
        (other_build(9, &d9), &d9),
        (manifest(10, &p10), &p10),
    ] {
        recv.apply_peer_entry(&a, &row, body)
            .expect("apply the entry");
    }
    recv.adjudicate_peer_frontier(&anna(), 7)
        .expect("adjudicate at 7");

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
    deliver_empty_snapshot(&mut recv, &server, 2).await;
    pump_to(&mut recv, |event| {
        matches!(event, ClientEvent::ProvisionalApplied { seq: 10, .. })
    })
    .await;
    assert_eq!(
        order_rows(&mut recv)
            .into_iter()
            .map(|(id, ..)| id)
            .collect::<Vec<_>>(),
        vec![4],
        "only the provisional entry came back"
    );
    assert_eq!(
        held_entries(&mut recv),
        vec![
            (anna(), 7, HeldState::Retained),
            (anna(), 8, HeldState::CourierOnly),
            (anna(), 9, HeldState::DigestOnly),
            (anna(), 10, HeldState::Provisional),
        ],
        "the re-apply moves no entry"
    );
}

/// Serve subscription `a` an empty snapshot ending at `cursor`, and pump
/// until its end lands.
async fn deliver_empty_snapshot(
    recv: &mut ConnettoConnection<Script>,
    server: &Script,
    cursor: u8,
) {
    server.say(IncomingFrame::Control(ControlMessage::SnapshotBegin(
        SnapshotBegin {
            sub_id: "a".to_owned(),
            priority: SubscriptionPriority::default(),
        },
    )));
    // An empty replacement, so whatever comes back is the re-apply's.
    let patch = PatchSet::<SimpleTable, String, Vec<u8>>::new().build();
    server.say(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(
        SnapshotPatch::new(
            "a".to_owned(),
            zstd::encode_all(patch.as_slice(), 3).expect("compress the page"),
        ),
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
