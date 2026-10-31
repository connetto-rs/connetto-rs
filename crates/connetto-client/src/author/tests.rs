use connetto_core::SchemaVersion;
use connetto_core::exchange::{Frontier, RetractCause, Retraction};
use connetto_core::messages::{
    ControlMessage, MutationApplied, MutationConflict, MutationReject, MutationRejectReason,
    WriterRef,
};
use tokio::sync::mpsc;

use super::OfferDraft;
use crate::Carry;
use crate::drain::tests::{Conn, hold_major_one, major, old_version, queue, replica};
use crate::enrolment::Command;

const KEY: [u8; 32] = [4; 32];
const WRITER: WriterRef = WriterRef::Device(KEY);

/// A major 2 replica whose bound device writer queued rows 1 to `count`,
/// their sequences, and the enrolment task's end of the steering.
fn device_replica(count: i32) -> (Conn, Vec<u64>, mpsc::UnboundedReceiver<Command>) {
    let mut conn = replica();
    let (steering, steered) = mpsc::unbounded_channel();
    conn.steering = Some(steering);
    let seqs = (1..=count)
        .map(|id| queue(&mut conn, WRITER, id, major(2)))
        .collect();
    conn.bound_writer = Some(WRITER);
    (conn, seqs, steered)
}

/// The chain hash of the device writer's queued `seq`.
fn hash_at(conn: &Conn, seq: u64) -> [u8; 32] {
    conn.writers[&WRITER].pending[&seq].hash
}

/// The device writer's offer as the replica states it.
fn offer(conn: &mut Conn) -> OfferDraft {
    conn.offer_draft(KEY)
        .expect("read the offer")
        .expect("the key is not retired")
}

/// How many offer refreshes the steering carried since the last call.
fn refreshes(steered: &mut mpsc::UnboundedReceiver<Command>) -> usize {
    let mut count = 0;
    while let Ok(command) = steered.try_recv() {
        if matches!(command, Command::OfferChanged) {
            count += 1;
        }
    }
    count
}

/// R77 decision 9: an acknowledged entry leaves the queue with its hash
/// kept as the settled frontier, so the offer names what its range chains
/// from while the newest entry stays queued.
#[tokio::test]
async fn an_applied_entry_settles_the_frontier_at_its_hash() {
    let (mut conn, seqs, _steered) = device_replica(2);
    let (first, second) = (hash_at(&conn, seqs[0]), hash_at(&conn, seqs[1]));
    let fresh = offer(&mut conn);
    assert_eq!(fresh.settled, None, "nothing is settled yet");
    assert_eq!(
        fresh.newest,
        Some(Frontier {
            seq: seqs[1],
            head: second,
        })
    );

    conn.handle_control(ControlMessage::MutationApplied(MutationApplied {
        client_seq: seqs[0],
    }))
    .await
    .expect("the ack lands");
    let settled = offer(&mut conn);
    assert_eq!(
        settled.settled,
        Some(Frontier {
            seq: seqs[0],
            head: first,
        }),
        "the acknowledged entry's hash is kept"
    );
    assert_eq!(
        settled.newest, fresh.newest,
        "the queued entry is still the newest"
    );
    assert_eq!(settled.retractions, []);

    conn.handle_control(ControlMessage::MutationApplied(MutationApplied {
        client_seq: seqs[1],
    }))
    .await
    .expect("the ack lands");
    let drained = offer(&mut conn);
    assert_eq!(
        drained.settled,
        Some(Frontier {
            seq: seqs[1],
            head: second,
        })
    );
    assert_eq!(
        drained.newest, None,
        "an empty queue offers no newest entry"
    );
}

/// R77 decisions 7 and 9: a refused and a conflicted entry are settled and
/// logged as retractions with their causes, and an entry the server could
/// not settle is neither.
#[tokio::test]
async fn refused_and_conflicted_entries_settle_into_the_retraction_log() {
    let (mut conn, seqs, _steered) = device_replica(4);
    conn.handle_control(ControlMessage::MutationReject(MutationReject {
        client_seq: seqs[0],
        reason: MutationRejectReason::Unauthorized,
    }))
    .await
    .expect("the refusal lands");
    conn.handle_control(ControlMessage::MutationConflict(MutationConflict {
        client_seq: seqs[1],
        table: "items".to_owned(),
        server_row: None,
    }))
    .await
    .expect("the conflict lands");
    conn.handle_control(ControlMessage::MutationReject(MutationReject {
        client_seq: seqs[2],
        reason: MutationRejectReason::Indeterminate,
    }))
    .await
    .expect("the deferral lands");

    let offer = offer(&mut conn);
    assert_eq!(
        offer.retractions,
        [
            Retraction {
                seq: seqs[0],
                cause: RetractCause::Rejected(MutationRejectReason::Unauthorized),
            },
            Retraction {
                seq: seqs[1],
                cause: RetractCause::Conflict,
            },
        ]
    );
    assert_eq!(
        offer.settled.map(|settled| settled.seq),
        Some(seqs[1]),
        "the deferred entry is not settled"
    );
}

/// R77 decision 8: the offer names this build and every major a rebuild
/// left held, oldest first.
#[test]
fn the_offer_names_this_build_and_every_held_major() {
    let (mut conn, _seqs, _steered) = device_replica(0);
    let current = (
        major(2),
        SchemaVersion::from_hash(b"the major 2 bundle".to_vec()),
    );
    assert_eq!(offer(&mut conn).builds, core::slice::from_ref(&current));
    hold_major_one(&mut conn);
    assert_eq!(
        offer(&mut conn).builds,
        [(major(1), old_version()), current]
    );
}

/// R77 state table, own device writer retired: a retired key offers
/// nothing.
#[test]
fn a_retired_device_writer_offers_nothing() {
    let (mut conn, _seqs, _steered) = device_replica(1);
    conn.retire_device(Carry::Named).expect("retire the writer");
    assert_eq!(conn.offer_draft(KEY).expect("read the offer"), None);
}

/// R77 state table: a local write, a settled entry and a refusal of the
/// device writer each ask for a fresh offer, and a number writer's write
/// asks for none.
#[tokio::test]
async fn every_change_to_the_device_writers_offer_asks_for_a_fresh_one() {
    let (mut conn, seqs, mut steered) = device_replica(3);
    assert_eq!(refreshes(&mut steered), 3, "one per queued device write");

    queue(&mut conn, WriterRef::Number(7), 10, major(2));
    assert_eq!(refreshes(&mut steered), 0, "a number writer offers nothing");

    conn.handle_control(ControlMessage::MutationApplied(MutationApplied {
        client_seq: seqs[0],
    }))
    .await
    .expect("the ack lands");
    assert_eq!(refreshes(&mut steered), 1, "the settled frontier moved");

    conn.handle_control(ControlMessage::MutationReject(MutationReject {
        client_seq: seqs[1],
        reason: MutationRejectReason::Unauthorized,
    }))
    .await
    .expect("the refusal lands");
    assert_eq!(refreshes(&mut steered), 1, "the retraction log grew");

    conn.handle_control(ControlMessage::MutationReject(MutationReject {
        client_seq: seqs[2],
        reason: MutationRejectReason::Indeterminate,
    }))
    .await
    .expect("the deferral lands");
    assert_eq!(refreshes(&mut steered), 0, "a deferral changes no offer");
}
