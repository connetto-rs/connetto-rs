use connetto_core::SchemaVersion;
use connetto_core::chain::{GENESIS, chain_walk};
use connetto_core::exchange::{
    Frontier, ManifestRow, Offer, RetractCause, Retraction, SignedOffer,
};
use connetto_core::messages::{ControlMessage, MutationApplied, WriterRef};
use diesel::connection::SimpleConnection as _;
use diesel::prelude::*;
use sha2::Digest as _;

use crate::peer::PeerAuthor;

use crate::drain::tests::{Conn, major, queue, replica};

const KEY: [u8; 32] = [4; 32];
const WRITER: WriterRef = WriterRef::Device(KEY);

/// A major 2 replica whose bound device writer queued rows 1 to `count`.
fn author(count: i32) -> (Conn, Vec<u64>) {
    let mut conn = replica();
    let seqs = (1..=count)
        .map(|id| queue(&mut conn, WRITER, id, major(2)))
        .collect();
    conn.bound_writer = Some(WRITER);
    (conn, seqs)
}

fn own_build() -> (connetto_core::Major, SchemaVersion) {
    (
        major(2),
        SchemaVersion::from_hash(b"the major 2 bundle".to_vec()),
    )
}

fn hash_at(conn: &Conn, seq: u64) -> [u8; 32] {
    conn.writers[&WRITER].pending[&seq].hash
}

fn walk(previous: [u8; 32], rows: &[ManifestRow]) -> [u8; 32] {
    chain_walk(
        &previous,
        rows.iter()
            .map(|row| (row.seq, Some(row.major), row.origin, row.digest)),
    )
}

/// R77 decision 17: a served range chains from the hash before it to the
/// queue's hash at its end, so a receiver holding the signed head verifies
/// it, and every entry of the requester's build carries its body.
#[test]
fn a_served_range_chains_to_the_queued_head() {
    let (mut conn, seqs) = author(3);
    let served = conn
        .serve_range(KEY, 1, 3, &own_build())
        .expect("read the range")
        .expect("the range is held");
    assert_eq!(served.previous, GENESIS);
    assert_eq!(
        served.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
        seqs
    );
    assert_eq!(walk(served.previous, &served.rows), hash_at(&conn, seqs[2]));
    assert_eq!(
        served.bodies, seqs,
        "the requester's build takes every body"
    );

    let tail = conn
        .serve_range(KEY, 2, 3, &own_build())
        .expect("read the range")
        .expect("the range is held");
    assert_eq!(tail.previous, hash_at(&conn, seqs[0]));
    assert_eq!(walk(tail.previous, &tail.rows), hash_at(&conn, seqs[2]));

    for row in &served.rows {
        let body = conn
            .serve_body(KEY, row.seq)
            .expect("read the body")
            .expect("the entry is queued");
        let digest: [u8; 32] = sha2::Sha256::digest(&body).into();
        assert_eq!(digest, row.digest, "the body is what its row digests");
    }
}

/// R77 decision 17: an entry of another build crosses as its row alone.
#[test]
fn a_range_under_another_build_carries_no_body() {
    let (mut conn, _seqs) = author(2);
    let other = (
        major(2),
        SchemaVersion::from_hash(b"another bundle".to_vec()),
    );
    let served = conn
        .serve_range(KEY, 1, 2, &other)
        .expect("read the range")
        .expect("the range is held");
    assert_eq!(served.rows.len(), 2);
    assert_eq!(served.bodies, [] as [u64; 0]);
}

/// R77 decisions 9 and 18: once the server answered the first entry, a
/// range after it chains from the kept settled head, and a range starting
/// at it is no longer held.
#[tokio::test]
async fn a_settled_entry_leaves_its_head_and_takes_its_range() {
    let (mut conn, seqs) = author(3);
    let first = hash_at(&conn, seqs[0]);
    conn.handle_control(ControlMessage::MutationApplied(MutationApplied {
        client_seq: seqs[0],
    }))
    .await
    .expect("the ack lands");
    let served = conn
        .serve_range(KEY, 2, 2, &own_build())
        .expect("read the range")
        .expect("the range after the settled entry is held");
    assert_eq!(served.previous, first, "the settled head is the anchor");
    assert_eq!(
        conn.serve_range(KEY, 1, 2, &own_build())
            .expect("read the range"),
        None,
        "a range starting at a settled entry is no longer held"
    );
    assert_eq!(conn.serve_body(KEY, seqs[0]).expect("read the body"), None);

    conn.handle_control(ControlMessage::MutationApplied(MutationApplied {
        client_seq: seqs[1],
    }))
    .await
    .expect("the ack lands");
    assert_eq!(
        conn.serve_range(KEY, 2, 3, &own_build())
            .expect("read the range"),
        None,
        "a range whose anchor settled away from it is no longer held"
    );
}

/// R77 decision 18: a range past the queue, an empty or reversed range, and
/// a retired writer's range are not held.
#[test]
fn ranges_the_writer_does_not_hold_are_not_available() {
    let (mut conn, _seqs) = author(2);
    for (from, to) in [(1, 3), (0, 1), (2, 1)] {
        assert_eq!(
            conn.serve_range(KEY, from, to, &own_build())
                .expect("read the range"),
            None,
            "{from}..={to}"
        );
    }
    conn.retire_device(crate::Carry::Named)
        .expect("retire the writer");
    assert_eq!(
        conn.serve_range(KEY, 1, 2, &own_build())
            .expect("read the range"),
        None
    );
    assert_eq!(
        conn.serve_range([5; 32], 1, 1, &own_build())
            .expect("read the range"),
        None,
        "a writer that never queued holds nothing"
    );
}

/// R77 slice 6b. An author never verified starts at sequence 0 on
/// [`GENESIS`], and a taken entry of another build moves the cursor alone.
#[test]
fn a_cursor_starts_fresh_and_moves_with_a_skipped_row() {
    let mut conn = replica();
    let author = crate::peer::PeerAuthor {
        key_id: vec![9; 32],
        account: "anna".to_owned(),
        bot_list: None,
    };
    assert_eq!(
        conn.peer_cursor(&author.key_id).expect("read the cursor"),
        super::FRESH_CURSOR
    );
    let offer = connetto_core::exchange::SignedOffer {
        offer: connetto_core::exchange::Offer {
            builds: vec![own_build()],
            settled: None,
            newest: Some(Frontier {
                seq: 1,
                head: [1; 32],
            }),
            retractions: Vec::new(),
        },
        signature: vec![1, 2, 3],
    };
    conn.adopt_peer_offer(&author, &offer, &super::FRESH_CURSOR)
        .expect("adopt the offer");
    let row = ManifestRow {
        seq: 1,
        major: major(1),
        origin: None,
        digest: [3; 32],
    };
    let cursor = Frontier {
        seq: 1,
        head: [1; 32],
    };
    conn.take_peer_row(&author, &row, None, &cursor)
        .expect("take the row");
    assert_eq!(
        conn.peer_cursor(&author.key_id).expect("read the cursor"),
        cursor
    );
}

/// The author of `KEY` as a receiver binds it.
fn writer_author() -> PeerAuthor {
    PeerAuthor {
        key_id: KEY.to_vec(),
        account: "anna".to_owned(),
        bot_list: None,
    }
}

/// An offer of `KEY` settled at `settled`, newest at `newest`, retracting
/// `retracted`.
fn signed(settled: Option<Frontier>, newest: Option<Frontier>, retracted: &[u64]) -> SignedOffer {
    SignedOffer {
        offer: Offer {
            builds: vec![own_build()],
            settled,
            newest,
            retractions: retracted
                .iter()
                .map(|seq| Retraction {
                    seq: *seq,
                    cause: RetractCause::Conflict,
                })
                .collect(),
        },
        signature: vec![1, 2, 3],
    }
}

/// A receiver holding every entry of `anna` applied with its cursor, and
/// the frontier at each entry.
fn receiver_of(anna: &mut Conn, seqs: &[u64]) -> (Conn, Vec<Frontier>) {
    let served = anna
        .serve_range(KEY, 1, seqs.len() as u64, &own_build())
        .expect("read the range")
        .expect("the range is held");
    let mut ben = replica();
    let newest = Frontier {
        seq: *seqs.last().expect("a queued entry"),
        head: walk(served.previous, &served.rows),
    };
    ben.adopt_peer_offer(
        &writer_author(),
        &signed(None, Some(newest), &[]),
        &super::FRESH_CURSOR,
    )
    .expect("adopt the offer");
    let mut frontiers = Vec::new();
    for (index, row) in served.rows.iter().enumerate() {
        let cursor = Frontier {
            seq: row.seq,
            head: walk(served.previous, &served.rows[..=index]),
        };
        let body = anna
            .serve_body(KEY, row.seq)
            .expect("read the body")
            .expect("the entry is queued");
        ben.take_peer_row(&writer_author(), row, Some(&body), &cursor)
            .expect("take the entry");
        frontiers.push(cursor);
    }
    (ben, frontiers)
}

/// The ids of the replica's `items` rows, comma-joined in order.
fn items(conn: &mut Conn) -> String {
    diesel::select(diesel::dsl::sql::<diesel::sql_types::Text>(
        "(SELECT coalesce(group_concat(id, ','), '') FROM (SELECT id FROM items ORDER BY id))",
    ))
    .get_result(&mut conn.db)
    .expect("read the items")
}

/// The stored peer entries of the replica.
fn stored(conn: &mut Conn) -> i64 {
    diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
        "(SELECT count(*) FROM _connetto_peer_entry)",
    ))
    .get_result(&mut conn.db)
    .expect("count the entries")
}

/// R77 state table, offer received with a frontier and retractions: an
/// adopted offer takes back the entry it retracts and adjudicates the rest
/// at or below its settled frontier, and the cursor lands where it says.
#[test]
fn an_adopted_offer_retracts_and_adjudicates_applied_entries() {
    let (mut anna, seqs) = author(2);
    let (mut ben, frontiers) = receiver_of(&mut anna, &seqs);
    assert_eq!(items(&mut ben), "1,2", "both entries applied");
    assert_eq!(stored(&mut ben), 2);

    ben.adopt_peer_offer(
        &writer_author(),
        &signed(Some(frontiers[1].clone()), None, &[seqs[0]]),
        &frontiers[1],
    )
    .expect("adopt the settling offer");
    assert_eq!(items(&mut ben), "2", "the retracted entry is taken back");
    assert_eq!(stored(&mut ben), 0, "the settled entry is adjudicated");
    assert_eq!(
        ben.peer_cursor(&KEY).expect("read the cursor"),
        frontiers[1]
    );
}

/// Refuse every statement `when` names inside its transaction, as a full
/// disk or a lost lock refuses a write.
fn refuse(conn: &mut Conn, when: &str) {
    conn.db
        .batch_execute(&format!(
            "CREATE TEMP TRIGGER refuse {when} BEGIN SELECT RAISE(ABORT, 'refused'); END"
        ))
        .expect("install the refusal");
}

/// R77 slice 6b: a cursor, a retraction or an adjudication the replica
/// cannot write, and a cursor past storage, fail the step that needed them,
/// so the driver retries it.
#[test]
fn exchange_writes_the_replica_refuses_are_errors() {
    let (mut anna, seqs) = author(2);
    let (mut ben, frontiers) = receiver_of(&mut anna, &seqs);
    let past = Frontier {
        seq: u64::MAX,
        head: [0; 32],
    };
    assert!(
        ben.adopt_peer_offer(&writer_author(), &signed(None, None, &[]), &past)
            .is_err()
    );
    let row = ManifestRow {
        seq: 3,
        major: major(1),
        origin: None,
        digest: [3; 32],
    };
    assert!(
        ben.take_peer_row(&writer_author(), &row, None, &past)
            .is_err()
    );

    refuse(&mut ben, "BEFORE DELETE ON _connetto_peer_entry");
    assert!(
        ben.adopt_peer_offer(
            &writer_author(),
            &signed(None, None, &[seqs[0]]),
            &frontiers[1]
        )
        .is_err(),
        "a retraction that cannot delete its entry fails"
    );
    assert!(
        ben.adopt_peer_offer(
            &writer_author(),
            &signed(Some(frontiers[0].clone()), None, &[]),
            &frontiers[1]
        )
        .is_err(),
        "an adjudication that cannot delete its entry fails"
    );

    let mut fresh = replica();
    refuse(&mut fresh, "BEFORE INSERT ON _connetto_peer_cursor");
    assert!(
        fresh
            .adopt_peer_offer(
                &writer_author(),
                &signed(None, None, &[]),
                &super::FRESH_CURSOR
            )
            .is_err(),
        "an offer whose cursor cannot be stored fails"
    );
    let mut stale = replica();
    stale
        .adopt_peer_offer(
            &writer_author(),
            &signed(None, None, &[]),
            &super::FRESH_CURSOR,
        )
        .expect("adopt the offer");
    refuse(&mut stale, "BEFORE UPDATE ON _connetto_peer_cursor");
    assert!(
        stale
            .take_peer_row(&writer_author(), &row, None, &frontiers[0])
            .is_err(),
        "a cursor that cannot move fails the row"
    );
}

/// R77 slice 6b: a corrupt or unreadable cursor, settled frontier or held
/// build is an error, never a range served or a cursor trusted.
#[test]
fn corrupt_exchange_bookkeeping_is_an_error() {
    let key = format!("x'{}'", "04".repeat(32));
    for corruption in [
        format!(
            "INSERT INTO _connetto_peer_cursor (author, account, seq, head, offer) \
             VALUES ({key}, 'anna', -1, zeroblob(32), x'')"
        ),
        format!(
            "INSERT INTO _connetto_peer_cursor (author, account, seq, head, offer) \
             VALUES ({key}, 'anna', 1, zeroblob(31), x'')"
        ),
        "DROP TABLE _connetto_peer_cursor".to_owned(),
    ] {
        let mut conn = replica();
        conn.db
            .batch_execute(&corruption)
            .expect("corrupt the cursor");
        assert!(conn.peer_cursor(&KEY).is_err(), "{corruption} is refused");
    }
    for corruption in [
        "UPDATE _connetto_writers SET settled_seq = -1, settled_head = zeroblob(32)".to_owned(),
        format!(
            "{}; INSERT INTO _connetto_held_batch (major, version) VALUES (0, x'00')",
            crate::majors::HELD_DDL
        ),
    ] {
        let (mut conn, _seqs) = author(1);
        conn.db
            .batch_execute(&corruption)
            .expect("corrupt the bookkeeping");
        assert!(
            conn.serve_range(KEY, 1, 1, &own_build()).is_err(),
            "{corruption} is refused"
        );
    }
}
