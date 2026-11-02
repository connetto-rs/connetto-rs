//! The state machine's rules, one test per row of the plan's table and per
//! rule of the contract (R77 slice 6b).

use std::time::{Duration, Instant};

use connetto_core::chain::{GENESIS, entry_hash};
use connetto_core::exchange::{EndOutcome, Frontier, ManifestRow, Offer, SignedOffer};
use connetto_core::schema::{Major, SchemaVersion};
use sha2::{Digest, Sha256};

use crate::machine::{Action, Exchange, FRAME_BOUND, RETRY_CEILING, RETRY_FIRST};
use crate::{Message, Transfer};

/// A writer chain's rows and bodies, the head at each row, and the head at
/// the end.
type Chain = (Vec<ManifestRow>, Vec<Vec<u8>>, Vec<[u8; 32]>, [u8; 32]);

fn major() -> Major {
    Major::new(2).expect("a major")
}

fn build() -> (Major, SchemaVersion) {
    (major(), SchemaVersion::from_hash(vec![7; 32]))
}

/// The writer chain of `count` entries at `major`, with its rows, bodies and
/// head.
fn chain(count: u64, major: Major) -> (Vec<ManifestRow>, Vec<Vec<u8>>, [u8; 32]) {
    let mut head = GENESIS;
    let mut rows = Vec::new();
    let mut bodies = Vec::new();
    for seq in 1..=count {
        let body = format!("entry {seq}");
        bodies.push(body.clone().into_bytes());
        head = entry_hash(&head, seq, Some(major), None, body.as_bytes());
        rows.push(ManifestRow {
            seq,
            major,
            origin: None,
            digest: Sha256::digest(body.as_bytes()).into(),
        });
    }
    (rows, bodies, head)
}

/// The writer chain of one entry per major in `majors`, in sequence order,
/// with its rows, bodies, the head at each row and the head at the end.
fn chain_with(majors: &[Major]) -> Chain {
    let mut head = GENESIS;
    let mut rows = Vec::new();
    let mut bodies = Vec::new();
    let mut heads = Vec::new();
    for (major, seq) in majors.iter().zip(1..=u64::MAX) {
        let body = format!("entry {seq}");
        bodies.push(body.clone().into_bytes());
        head = entry_hash(&head, seq, Some(*major), None, body.as_bytes());
        heads.push(head);
        rows.push(ManifestRow {
            seq,
            major: *major,
            origin: None,
            digest: Sha256::digest(body.as_bytes()).into(),
        });
    }
    (rows, bodies, heads, head)
}

/// A signed offer over the author's builds, settled frontier and newest
/// frontier.
fn offer_with(
    builds: Vec<(Major, SchemaVersion)>,
    settled: Option<Frontier>,
    newest: Option<(u64, [u8; 32])>,
) -> SignedOffer {
    SignedOffer {
        offer: Offer {
            builds,
            settled,
            newest: newest.map(|(seq, head)| Frontier { seq, head }),
            retractions: Vec::new(),
        },
        signature: vec![1],
    }
}

/// An author with a fresh chain of `count` entries under the one build, its
/// signed offer, rows and bodies.
fn author(count: u64) -> (SignedOffer, Vec<ManifestRow>, Vec<Vec<u8>>) {
    let (rows, bodies, head) = chain(count, major());
    let signed = offer_with(vec![build()], None, Some((count, head)));
    (signed, rows, bodies)
}

/// A request message under the one build.
fn request(request_id: u64, from: u64, to: u64) -> Transfer {
    Transfer::Request {
        request_id,
        from,
        to,
        build: build(),
    }
}

/// Hand request 1's body of each of `rows` to `receiver` from "a", each
/// applying at once, and return every action in order.
fn apply_bodies(
    receiver: &mut Exchange<&'static str>,
    rows: &[ManifestRow],
    bodies: &[Vec<u8>],
    now: Instant,
) -> Vec<Action<&'static str>> {
    let mut applied = Vec::new();
    for (row, body) in rows.iter().zip(bodies) {
        let actions = receiver.message(
            &"a",
            Transfer::Body {
                request_id: 1,
                seq: row.seq,
                changeset: body.clone(),
            },
            now,
        );
        assert!(
            matches!(actions.first(), Some(Action::Apply { .. })),
            "body {} applies, got {actions:?}",
            row.seq
        );
        applied.extend(actions);
    }
    applied
}

/// The apply actions among `actions`.
fn applies(actions: &[Action<&'static str>]) -> usize {
    actions
        .iter()
        .filter(|action| matches!(action, Action::Apply { .. }))
        .count()
}

/// A receiver linked to author "a", its offer adopted and its first request
/// outstanding for the author's whole chain.
struct Ctx {
    receiver: Exchange<&'static str>,
    rows: Vec<ManifestRow>,
    bodies: Vec<Vec<u8>>,
}

fn ctx(count: u64, now: Instant) -> Ctx {
    let (offer, rows, bodies) = author(count);
    let mut receiver = Exchange::<&str>::new(Some(build()));
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = receiver.offer(&"a", offer, true, now);
    assert_eq!(actions.len(), 2);
    Ctx {
        receiver,
        rows,
        bodies,
    }
}

/// Drive the receiver's outstanding request to a clean end, manifest frames
/// of `size` rows, a body for every row, and `End { Complete }`.
fn serve_all(
    receiver: &mut Exchange<&'static str>,
    request_id: u64,
    rows: &[ManifestRow],
    bodies: &[Vec<u8>],
    size: usize,
    now: Instant,
) -> Vec<Action<&'static str>> {
    let mut actions = Vec::new();
    let total = rows.len();
    let mut start = 0;
    while start < total {
        let end = (start + size).min(total);
        actions.extend(receiver.message(
            &"a",
            Transfer::Manifest {
                request_id,
                previous: GENESIS,
                rows: rows[start..end].to_vec(),
                last: end == total,
            },
            now,
        ));
        start = end;
    }
    for (row, body) in rows.iter().zip(bodies) {
        actions.extend(receiver.message(
            &"a",
            Transfer::Body {
                request_id,
                seq: row.seq,
                changeset: body.clone(),
            },
            now,
        ));
    }
    actions.extend(receiver.message(
        &"a",
        Transfer::End {
            request_id,
            outcome: EndOutcome::Complete,
        },
        now,
    ));
    actions
}

/// The own offer is sent at link up when it is held, and nothing is sent
/// when it is not.
#[test]
fn a_link_up_sends_the_held_offer() {
    let offer = author(1).0;
    let mut a = Exchange::<&str>::new(Some(build()));
    assert_eq!(
        a.linked(
            "b",
            Frontier {
                seq: 0,
                head: GENESIS
            }
        ),
        Vec::new()
    );
    let actions = a.set_own_offer(offer.clone());
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "b",
            message: Message::Offer(offer.clone()),
        }]
    );
    assert_eq!(
        a.linked(
            "c",
            Frontier {
                seq: 0,
                head: GENESIS
            }
        ),
        vec![Action::Send {
            peer: "c",
            message: Message::Offer(offer)
        }]
    );
    assert!(a.own_offer().is_some());
}

/// A peer already linked is left as it is, its state and cursor untouched.
#[test]
fn relinking_a_linked_peer_is_ignored() {
    let offer = author(1).0;
    let mut a = Exchange::<&str>::new(Some(build()));
    a.set_own_offer(offer);
    a.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = a.linked(
        "b",
        Frontier {
            seq: 1,
            head: [9; 32],
        },
    );
    assert_eq!(actions, Vec::new());
}

/// A fresh offer reaches every idle peer at once.
#[test]
fn a_fresh_offer_reaches_every_idle_peer() {
    let mut a = Exchange::<&str>::new(Some(build()));
    a.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    a.linked(
        "c",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = a.set_own_offer(author(1).0);
    assert_eq!(actions.len(), 2);
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Send { peer: "b", .. }))
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Send { peer: "c", .. }))
    );
}

/// Clearing the offer sends nothing, and a link coming up after it sends
/// nothing.
#[test]
fn clearing_the_offer_sends_nothing_and_silences_new_links() {
    let mut a = Exchange::<&str>::new(Some(build()));
    a.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    assert_ne!(a.set_own_offer(author(1).0), Vec::new());
    a.clear_own_offer();
    assert!(a.own_offer().is_none());
    assert_eq!(
        a.linked(
            "c",
            Frontier {
                seq: 0,
                head: GENESIS
            }
        ),
        Vec::new()
    );
}

/// Retire stops a serving stream with `End { NotAvailable }`, refuses every
/// later request, and a fresh offer un-retires the writer.
#[test]
fn retire_stops_the_stream_and_refuses_requests() {
    let now = Instant::now();
    let mut a = Exchange::<&str>::new(Some(build()));
    a.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    assert!(matches!(
        a.message(&"b", request(1, 1, 3), now)[0],
        Action::Serve { .. }
    ));
    let actions = a.retire();
    assert_eq!(
        actions,
        vec![
            Action::StopServing { peer: "b" },
            Action::Send {
                peer: "b",
                message: Message::Transfer(Transfer::End {
                    request_id: 1,
                    outcome: EndOutcome::NotAvailable,
                }),
            },
        ]
    );
    let actions = a.message(&"b", request(2, 1, 3), now);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "b",
            message: Message::Transfer(Transfer::End {
                request_id: 2,
                outcome: EndOutcome::NotAvailable,
            }),
        }]
    );
    a.set_own_offer(author(1).0);
    let actions = a.message(&"b", request(3, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
}

/// A valid offer in R1 adopts the frontier and requests the range above the
/// cursor under the own build, with a frame deadline at the bound.
#[test]
fn a_valid_offer_requests_the_range_above_the_cursor() {
    let now = Instant::now();
    let mut receiver = Exchange::<&str>::new(Some(build()));
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let (offer, _, _) = author(3);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request {
                request_id: 1,
                from: 1,
                to: 3,
                ..
            }),
            ..
        }
    ));
    assert_eq!(receiver.next_deadline(), Some(now + FRAME_BOUND));
}

/// A valid offer that names nothing above the cursor enters R2, with no
/// request and no deadline.
#[test]
fn an_offer_with_nothing_new_enters_r2() {
    let now = Instant::now();
    let mut receiver = Exchange::<&str>::new(Some(build()));
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let offer = offer_with(vec![build()], None, None);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert_eq!(actions.len(), 1);
    assert_eq!(receiver.next_deadline(), None);
}

/// An offer whose settled frontier is above the cursor moves the cursor up,
/// and the request starts there.
#[test]
fn an_offer_moves_the_cursor_up_to_settled() {
    let now = Instant::now();
    let mut receiver = Exchange::<&str>::new(Some(build()));
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let settled = Frontier {
        seq: 2,
        head: [3; 32],
    };
    let offer = offer_with(vec![build()], Some(settled), Some((3, [4; 32])));
    let actions = receiver.offer(&"a", offer, true, now);
    let Action::Adopt { cursor, .. } = &actions[0] else {
        panic!("the offer is adopted");
    };
    assert_eq!(
        cursor,
        &Frontier {
            seq: 2,
            head: [3; 32]
        }
    );
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request { from: 3, to: 3, .. }),
            ..
        }
    ));
}

/// A newest frontier at the cursor's sequence with another head raises
/// `PeerChainInvalid` and leaves the state unchanged (decision 20).
#[test]
fn a_forked_newest_at_the_cursor_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let actions = serve_all(&mut receiver, 1, &rows, &bodies, 8, now);
    assert_eq!(applies(&actions), 3);
    let forked = offer_with(vec![build()], None, Some((3, [9; 32])));
    let actions = receiver.offer(&"a", forked, true, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
    // R2 is unchanged, so a valid offer is still acted on.
    let (offer, _, _) = author(4);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request { from: 4, to: 4, .. }),
            ..
        }
    ));
}

/// A settled frontier at the cursor's sequence with another head raises
/// `PeerChainInvalid` and leaves the state unchanged (decision 20).
#[test]
fn a_forked_settled_at_the_cursor_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let actions = serve_all(&mut receiver, 1, &rows, &bodies, 8, now);
    assert_eq!(applies(&actions), 3);
    let forked = offer_with(
        vec![build()],
        Some(Frontier {
            seq: 3,
            head: [9; 32],
        }),
        None,
    );
    let actions = receiver.offer(&"a", forked, true, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
    assert_eq!(receiver.next_deadline(), None);
}

/// An invalid offer raises `PeerChainInvalid` and changes no state, in R1
/// and in R2 alike.
#[test]
fn an_invalid_offer_raises_chain_invalid_and_keeps_the_state() {
    let now = Instant::now();
    let mut receiver = Exchange::<&str>::new(Some(build()));
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = receiver.offer(&"a", author(1).0, false, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
    // R1 is unchanged, so a valid offer is still acted on.
    let actions = receiver.offer(&"a", author(3).0, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(actions[1], Action::Send { .. }));
    // the range completes into R2
    let Ctx { rows, bodies, .. } = ctx(3, now);
    let actions = serve_all(&mut receiver, 1, &rows, &bodies, 8, now);
    assert_eq!(applies(&actions), 3);
    // and an invalid offer in R2 still changes nothing
    let actions = receiver.offer(&"a", author(3).0, false, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
    assert_eq!(receiver.next_deadline(), None);
}

/// An invalid offer while a request is outstanding aborts it, with no retry
/// until a new offer.
#[test]
fn an_invalid_offer_in_r3_aborts_without_a_retry() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let actions = receiver.offer(&"a", author(3).0, false, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
    assert_eq!(receiver.next_deadline(), None);
    assert_eq!(
        receiver.timeout(now + Duration::from_secs(3600)),
        Vec::new()
    );
}

/// A valid offer in R3 is kept, and acted on at the end of the request, a
/// new request for its range.
#[test]
fn a_valid_offer_in_r3_is_kept_and_acted_on_at_the_end() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let (offer, rows, bodies) = author(5);
    let actions = receiver.offer(&"a", offer, true, now);
    assert_eq!(actions, Vec::new());
    let actions = serve_all(&mut receiver, 1, &rows[..3], &bodies[..3], 8, now);
    assert_eq!(applies(&actions), 3);
    // the end acts on the kept offer, a new request for its range
    assert!(matches!(actions[3], Action::Adopt { .. }));
    assert!(matches!(
        actions[4],
        Action::Send {
            message: Message::Transfer(Transfer::Request {
                request_id: 2,
                from: 4,
                to: 5,
                ..
            }),
            ..
        }
    ));
    assert_eq!(receiver.next_deadline(), Some(now + FRAME_BOUND));
}

/// Of the offers kept during a request, the latest wins at its end.
#[test]
fn the_latest_offer_in_r3_wins_at_the_end() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let (offer4, _, _) = author(4);
    let (offer5, rows, bodies) = author(5);
    let actions = receiver.offer(&"a", offer4, true, now);
    assert_eq!(actions, Vec::new());
    let actions = receiver.offer(&"a", offer5, true, now);
    assert_eq!(actions, Vec::new());
    let actions = serve_all(&mut receiver, 1, &rows[..3], &bodies[..3], 8, now);
    assert_eq!(applies(&actions), 3);
    // the end acts on the latest kept offer
    assert!(matches!(actions[3], Action::Adopt { .. }));
    assert!(matches!(
        actions[4],
        Action::Send {
            message: Message::Transfer(Transfer::Request { to: 5, .. }),
            ..
        }
    ));
}

/// A kept offer that forks at the cursor, advanced by the applied rows,
/// raises `PeerChainInvalid` at the end and is not adopted.
#[test]
fn a_kept_offer_forked_at_the_cursor_raises_chain_invalid_at_the_end() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let forked = offer_with(vec![build()], None, Some((3, [9; 32])));
    let actions = receiver.offer(&"a", forked, true, now);
    assert_eq!(actions, Vec::new());
    let (_, rows, bodies) = author(3);
    let actions = serve_all(&mut receiver, 1, &rows, &bodies, 8, now);
    assert_eq!(applies(&actions), 3);
    // the end acts on the kept offer, which forks at the advanced cursor
    assert_eq!(
        actions.last().cloned(),
        Some(Action::ChainInvalid { peer: "a" })
    );
    assert_eq!(receiver.next_deadline(), None);
}

/// A manifest, body or end naming another request id is ignored, and a stale
/// end for a finished request is ignored.
#[test]
fn stale_request_ids_are_ignored() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let manifest = Transfer::Manifest {
        request_id: 9,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    let body = Transfer::Body {
        request_id: 9,
        seq: 1,
        changeset: vec![1],
    };
    assert_eq!(receiver.message(&"a", body, now), Vec::new());
    let end = Transfer::End {
        request_id: 9,
        outcome: EndOutcome::Complete,
    };
    assert_eq!(receiver.message(&"a", end, now), Vec::new());
    // the request still runs
    let actions = serve_all(&mut receiver, 1, &rows, &bodies, 8, now);
    assert_eq!(applies(&actions), 3);
    // a stale end for the finished request is ignored
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Busy,
        },
        now,
    );
    assert_eq!(actions, Vec::new());
}

/// A range split across manifest frames verifies the chain from the cursor's
/// head to the signed head, and the walk waits for the first body.
#[test]
fn a_manifest_split_across_frames_verifies_the_chain() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(4, now);
    let frame1 = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows[..2].to_vec(),
        last: false,
    };
    assert_eq!(receiver.message(&"a", frame1, now), Vec::new());
    let frame2 = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows[2..].to_vec(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", frame2, now), Vec::new());
    apply_bodies(&mut receiver, &rows, &bodies, now);
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Complete,
        },
        now,
    );
    assert_eq!(actions, Vec::new());
}

/// A manifest that carries more rows than the range requested aborts with
/// `PeerChainInvalid` and no retry.
#[test]
fn a_manifest_above_the_range_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let (_, rows, _) = author(4);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    let actions = receiver.message(&"a", manifest, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
    assert_eq!(receiver.next_deadline(), None);
}

/// A manifest frame whose `previous` is not the cursor's head at request
/// time aborts with `PeerChainInvalid`.
#[test]
fn a_manifest_with_a_wrong_previous_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver, rows, ..
    } = ctx(3, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: [9; 32],
        rows,
        last: true,
    };
    let actions = receiver.message(&"a", manifest, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A manifest whose rows break the sequence from `from` aborts with
/// `PeerChainInvalid`.
#[test]
fn a_manifest_gap_in_the_sequence_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver, rows, ..
    } = ctx(3, now);
    let mut rows = rows;
    rows.remove(1);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    let actions = receiver.message(&"a", manifest, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A manifest whose last frame does not end at the range's `to` aborts with
/// `PeerChainInvalid`.
#[test]
fn a_manifest_ending_below_the_range_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver, rows, ..
    } = ctx(3, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows[..2].to_vec(),
        last: true,
    };
    let actions = receiver.message(&"a", manifest, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A manifest whose walk does not reach the offer's signed head aborts with
/// `PeerChainInvalid`, though it ends at `to`.
#[test]
fn a_manifest_ending_below_the_signed_head_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver, rows, ..
    } = ctx(3, now);
    let mut rows = rows;
    rows[2].digest = [9; 32];
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    let actions = receiver.message(&"a", manifest, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A manifest after the range's last frame aborts with `PeerChainInvalid`.
#[test]
fn a_manifest_after_the_last_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver, rows, ..
    } = ctx(2, now);
    let last = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    assert_eq!(receiver.message(&"a", last, now), Vec::new());
    let again = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: Vec::new(),
        last: true,
    };
    let actions = receiver.message(&"a", again, now);
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// Each accepted manifest frame refreshes the frame deadline.
#[test]
fn each_frame_refreshes_the_frame_deadline() {
    let now = Instant::now();
    let Ctx {
        mut receiver, rows, ..
    } = ctx(4, now);
    assert_eq!(receiver.next_deadline(), Some(now + FRAME_BOUND));
    let late = now + Duration::from_secs(20);
    let frame1 = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows[..2].to_vec(),
        last: false,
    };
    assert_eq!(receiver.message(&"a", frame1, late), Vec::new());
    assert_eq!(receiver.next_deadline(), Some(late + FRAME_BOUND));
    // the original deadline is gone
    assert_eq!(receiver.timeout(now + FRAME_BOUND), Vec::new());
    // the refreshed one aborts the request and schedules the retry
    let actions = receiver.timeout(late + FRAME_BOUND);
    assert_eq!(actions, Vec::new());
    assert_eq!(
        receiver.next_deadline(),
        Some(late + FRAME_BOUND + RETRY_FIRST)
    );
}

/// A body before the manifest is complete aborts with `PeerChainInvalid`.
#[test]
fn a_body_before_the_manifest_is_complete_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let frame = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows[..2].to_vec(),
        last: false,
    };
    assert_eq!(receiver.message(&"a", frame, now), Vec::new());
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 1,
            changeset: bodies[0].clone(),
        },
        now,
    );
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A body whose digest does not match its row aborts with
/// `PeerChainInvalid`.
#[test]
fn a_body_with_a_bad_digest_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver, rows, ..
    } = ctx(2, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 1,
            changeset: vec![255],
        },
        now,
    );
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A body for a row the walk does not wait on aborts with
/// `PeerChainInvalid`.
#[test]
fn an_out_of_order_body_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(2, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 2,
            changeset: bodies[1].clone(),
        },
        now,
    );
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A body when every row is already processed aborts with
/// `PeerChainInvalid`.
#[test]
fn a_body_when_no_row_waits_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(1, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 1,
            changeset: bodies[0].clone(),
        },
        now,
    );
    assert!(matches!(actions[0], Action::Apply { .. }));
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 1,
            changeset: bodies[0].clone(),
        },
        now,
    );
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
}

/// A row at a major the request's build does not apply stores its cursor
/// alone, before the build's first apply.
#[test]
fn a_row_at_another_build_skips_before_its_applies() {
    let now = Instant::now();
    let major_old = Major::new(1).expect("a major");
    let (rows, bodies, heads, head) = chain_with(&[major_old, major()]);
    let builds = vec![(major_old, SchemaVersion::from_hash(vec![1; 32])), build()];
    let offer = offer_with(builds, None, Some((2, head)));
    let mut receiver = Exchange::<&str>::new(Some(build()));
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request { from: 1, to: 2, .. }),
            ..
        }
    ));
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    let actions = receiver.message(&"a", manifest, now);
    assert_eq!(
        actions,
        vec![Action::Skip {
            peer: "a",
            row: rows[0].clone(),
            cursor: Frontier {
                seq: 1,
                head: heads[0]
            },
        }]
    );
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 2,
            changeset: bodies[1].clone(),
        },
        now,
    );
    let Action::Apply { cursor, .. } = &actions[0] else {
        panic!("the row applies");
    };
    assert_eq!(
        cursor,
        &Frontier {
            seq: 2,
            head: heads[1]
        }
    );
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Complete,
        },
        now,
    );
    assert_eq!(actions, Vec::new());
}

/// A row at another build stores its cursor alone, between the build's
/// applies.
#[test]
fn a_row_at_another_build_skips_between_applies() {
    let now = Instant::now();
    let major_old = Major::new(1).expect("a major");
    let (rows, bodies, heads, head) = chain_with(&[major(), major_old, major()]);
    let builds = vec![(major_old, SchemaVersion::from_hash(vec![1; 32])), build()];
    let offer = offer_with(builds, None, Some((3, head)));
    let mut receiver = Exchange::<&str>::new(Some(build()));
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request { from: 1, to: 3, .. }),
            ..
        }
    ));
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 1,
            changeset: bodies[0].clone(),
        },
        now,
    );
    assert_eq!(
        actions,
        vec![
            Action::Apply {
                peer: "a",
                row: rows[0].clone(),
                body: bodies[0].clone(),
                cursor: Frontier {
                    seq: 1,
                    head: heads[0]
                },
            },
            Action::Skip {
                peer: "a",
                row: rows[1].clone(),
                cursor: Frontier {
                    seq: 2,
                    head: heads[1]
                },
            },
        ]
    );
    let actions = receiver.message(
        &"a",
        Transfer::Body {
            request_id: 1,
            seq: 3,
            changeset: bodies[2].clone(),
        },
        now,
    );
    let Action::Apply { cursor, .. } = &actions[0] else {
        panic!("the row applies");
    };
    assert_eq!(
        cursor,
        &Frontier {
            seq: 3,
            head: heads[2]
        }
    );
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Complete,
        },
        now,
    );
    assert_eq!(actions, Vec::new());
}

/// A `Complete` end with rows left unprocessed aborts with
/// `PeerChainInvalid` and no retry.
#[test]
fn a_complete_end_with_rows_left_raises_chain_invalid() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    apply_bodies(&mut receiver, &rows[..2], &bodies, now);
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Complete,
        },
        now,
    );
    assert_eq!(actions, vec![Action::ChainInvalid { peer: "a" }]);
    assert_eq!(receiver.next_deadline(), None);
}

/// A `Busy` end with no kept offer returns to R2, and the author's next
/// offer is still acted on.
#[test]
fn a_busy_end_returns_to_r2() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Busy,
        },
        now,
    );
    assert_eq!(actions, Vec::new());
    assert_eq!(receiver.next_deadline(), None);
    let (offer, _, _) = author(3);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(actions[1], Action::Send { .. }));
}

/// A `Busy` end acts on the kept offer when there is one.
#[test]
fn a_busy_end_acts_on_a_kept_offer() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let (offer, _, _) = author(5);
    let actions = receiver.offer(&"a", offer, true, now);
    assert_eq!(actions, Vec::new());
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Busy,
        },
        now,
    );
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request { from: 1, to: 5, .. }),
            ..
        }
    ));
}

/// A `NotAvailable` end with no kept offer returns to R2, the request
/// dropped until the author's next offer (decision 18).
#[test]
fn a_not_available_end_returns_to_r2() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::NotAvailable,
        },
        now,
    );
    assert_eq!(actions, Vec::new());
    assert_eq!(receiver.next_deadline(), None);
    let (offer, _, _) = author(3);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(actions[1], Action::Send { .. }));
}

/// A frame deadline aborts the request and re-requests from the kept offer
/// after the first backoff, a fresh id and a fresh frame deadline.
#[test]
fn a_frame_timeout_retries_after_the_first_backoff() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let deadline = now + FRAME_BOUND;
    assert_eq!(receiver.next_deadline(), Some(deadline));
    let actions = receiver.timeout(deadline);
    assert_eq!(actions, Vec::new());
    assert_eq!(receiver.next_deadline(), Some(deadline + RETRY_FIRST));
    let actions = receiver.timeout(deadline + RETRY_FIRST);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "a",
            message: Message::Transfer(Transfer::Request {
                request_id: 2,
                from: 1,
                to: 3,
                build: build(),
            }),
        }]
    );
    assert_eq!(
        receiver.next_deadline(),
        Some(deadline + RETRY_FIRST + FRAME_BOUND)
    );
}

/// The retry backoff doubles at every stall, up to the ceiling (decision
/// 22).
#[test]
fn the_retry_backoff_doubles_up_to_the_ceiling() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let mut stall = now + FRAME_BOUND;
    receiver.timeout(stall);
    let mut retry = stall + RETRY_FIRST;
    let mut gaps = vec![RETRY_FIRST];
    for _ in 0..6 {
        receiver.timeout(retry);
        stall = retry + FRAME_BOUND;
        receiver.timeout(stall);
        let next = receiver.next_deadline().expect("a retry is scheduled");
        gaps.push(next - stall);
        retry = next;
    }
    assert_eq!(
        gaps[1..6],
        [
            RETRY_FIRST * 2,
            RETRY_FIRST * 4,
            RETRY_FIRST * 8,
            RETRY_FIRST * 16,
            RETRY_FIRST * 32
        ]
    );
    assert_eq!(gaps[6], RETRY_CEILING);
}

/// A completed range resets the backoff to the first one.
#[test]
fn a_completed_range_resets_the_backoff() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let stall = now + FRAME_BOUND;
    receiver.timeout(stall);
    assert_eq!(receiver.next_deadline(), Some(stall + RETRY_FIRST));
    let serve_now = stall + RETRY_FIRST;
    receiver.timeout(serve_now);
    let actions = serve_all(&mut receiver, 2, &rows, &bodies, 8, serve_now);
    assert_eq!(applies(&actions), 3);
    assert_eq!(receiver.next_deadline(), None);
    // a fresh offer extending the cursor opens a new request, and a stall
    // retries at the first backoff, not the doubled one
    let (offer, _, _) = author(4);
    let actions = receiver.offer(&"a", offer, true, serve_now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request { from: 4, to: 4, .. }),
            ..
        }
    ));
    let deadline = serve_now + FRAME_BOUND;
    assert_eq!(receiver.next_deadline(), Some(deadline));
    receiver.timeout(deadline);
    assert_eq!(receiver.next_deadline(), Some(deadline + RETRY_FIRST));
}

/// A fresh offer in R1 acts on at once and drops the pending retry.
#[test]
fn a_new_offer_in_r1_resets_the_retry() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let stall = now + FRAME_BOUND;
    receiver.timeout(stall);
    assert_eq!(receiver.next_deadline(), Some(stall + RETRY_FIRST));
    let (offer, _, _) = author(5);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert!(matches!(
        actions[1],
        Action::Send {
            message: Message::Transfer(Transfer::Request { from: 1, to: 5, .. }),
            ..
        }
    ));
    assert_eq!(receiver.next_deadline(), Some(now + FRAME_BOUND));
}

/// A frame timeout after some rows applied re-requests from the advanced
/// cursor, the processed rows standing (decision 22).
#[test]
fn a_frame_timeout_retries_from_the_advanced_cursor() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    apply_bodies(&mut receiver, &rows[..2], &bodies, now);
    let stall = now + FRAME_BOUND;
    let actions = receiver.timeout(stall);
    assert_eq!(actions, Vec::new());
    let actions = receiver.timeout(stall + RETRY_FIRST);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "a",
            message: Message::Transfer(Transfer::Request {
                request_id: 2,
                from: 3,
                to: 3,
                build: build(),
            }),
        }]
    );
}

/// A failed commit puts the cursor back where the store holds it, aborts
/// the request and re-requests from there after the backoff, so an entry
/// whose apply failed is requested again (decision 22).
#[test]
fn a_failed_commit_retries_from_the_stored_cursor() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    let applied = apply_bodies(&mut receiver, &rows[..2], &bodies, now);
    let Some(Action::Apply { cursor: first, .. }) = applied.first().cloned() else {
        panic!("the first body applies, got {applied:?}");
    };
    // The second row's commit failed, so the store stands at the first.
    receiver.commit_failed(&"a", first, now);
    assert_eq!(receiver.next_deadline(), Some(now + RETRY_FIRST));
    assert_eq!(
        receiver.timeout(now + RETRY_FIRST),
        vec![Action::Send {
            peer: "a",
            message: Message::Transfer(Transfer::Request {
                request_id: 2,
                from: 2,
                to: 3,
                build: build(),
            }),
        }]
    );

    receiver.unlinked(&"a");
    receiver.commit_failed(&"a", first_frontier(), now);
    assert_eq!(
        receiver.next_deadline(),
        None,
        "an unlinked peer is ignored"
    );
}

/// The frontier before any entry.
fn first_frontier() -> Frontier {
    Frontier {
        seq: 0,
        head: GENESIS,
    }
}

/// A failed adopt puts the cursor back, aborts the request, and the retry
/// adopts the same offer again before it requests the range, so the
/// offer's cursor row and retractions are not left to the author's next
/// offer. A receiver with no build re-adopts and requests nothing.
#[test]
fn a_failed_adopt_is_adopted_again_before_the_retry_requests() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(2, now);
    receiver.adopt_failed(&"a", first_frontier(), now);
    assert_eq!(receiver.next_deadline(), Some(now + RETRY_FIRST));
    let actions = receiver.timeout(now + RETRY_FIRST);
    assert!(
        matches!(
            actions.as_slice(),
            [
                Action::Adopt { cursor, .. },
                Action::Send {
                    message: Message::Transfer(Transfer::Request {
                        from: 1,
                        to: 2,
                        ..
                    }),
                    ..
                },
            ] if *cursor == first_frontier()
        ),
        "the retry adopts, then requests, got {actions:?}"
    );

    let (offer, _, _) = author(2);
    let mut bare = Exchange::<&str>::new(None);
    bare.linked("a", first_frontier());
    let _ = bare.offer(&"a", offer, true, now);
    bare.adopt_failed(&"a", first_frontier(), now);
    let actions = bare.timeout(now + RETRY_FIRST);
    assert!(
        matches!(actions.as_slice(), [Action::Adopt { .. }]),
        "a receiver without a build only adopts, got {actions:?}"
    );
    bare.adopt_failed(&"b", first_frontier(), now);
}

/// A request from an idle peer is streamed, and the sender returns to idle
/// when the stream ends.
#[test]
fn a_request_is_served_from_idle() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = sender.message(&"b", request(1, 1, 3), now);
    assert_eq!(
        actions,
        vec![Action::Serve {
            peer: "b",
            request_id: 1,
            from: 1,
            to: 3,
            build: build(),
        }]
    );
    assert_eq!(sender.served(&"b", 1), Vec::new());
    let actions = sender.message(&"b", request(2, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
}

/// A request starting at or below the first entry is answered
/// `NotAvailable`, and the sender stays idle.
#[test]
fn a_request_below_the_first_entry_is_refused() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = sender.message(&"b", request(1, 0, 3), now);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "b",
            message: Message::Transfer(Transfer::End {
                request_id: 1,
                outcome: EndOutcome::NotAvailable,
            }),
        }]
    );
    let actions = sender.message(&"b", request(2, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
}

/// A request whose range is inverted is answered `NotAvailable`.
#[test]
fn a_request_with_an_inverted_range_is_refused() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = sender.message(&"b", request(1, 4, 3), now);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "b",
            message: Message::Transfer(Transfer::End {
                request_id: 1,
                outcome: EndOutcome::NotAvailable,
            }),
        }]
    );
}

/// A second request while a range is streaming is answered `Busy` with its
/// own id.
#[test]
fn a_second_request_while_serving_is_answered_busy() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = sender.message(&"b", request(1, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
    let actions = sender.message(&"b", request(2, 1, 2), now);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "b",
            message: Message::Transfer(Transfer::End {
                request_id: 2,
                outcome: EndOutcome::Busy,
            }),
        }]
    );
}

/// A peer answered `Busy` receives the own offer again when the stream ends
/// (decision 22).
#[test]
fn serving_end_re_offers_to_a_busy_peer() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let offer = author(1).0;
    sender.set_own_offer(offer.clone());
    let actions = sender.message(&"b", request(1, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
    let actions = sender.message(&"b", request(2, 1, 2), now);
    assert!(matches!(
        actions[0],
        Action::Send {
            message: Message::Transfer(Transfer::End {
                outcome: EndOutcome::Busy,
                ..
            }),
            ..
        }
    ));
    let actions = sender.served(&"b", 1);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "b",
            message: Message::Offer(offer),
        }]
    );
}

/// An own offer set while a range is streaming is sent when the stream
/// ends.
#[test]
fn a_deferred_offer_is_sent_at_serving_end() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let offer = author(1).0;
    let actions = sender.message(&"b", request(1, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
    let actions = sender.set_own_offer(offer.clone());
    assert_eq!(actions, Vec::new());
    let actions = sender.served(&"b", 1);
    assert_eq!(
        actions,
        vec![Action::Send {
            peer: "b",
            message: Message::Offer(offer),
        }]
    );
}

/// A stream that ends with nothing that waited sends nothing.
#[test]
fn serving_end_without_a_reason_sends_nothing() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let offer = author(1).0;
    sender.set_own_offer(offer);
    let actions = sender.message(&"b", request(1, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
    let actions = sender.served(&"b", 1);
    assert_eq!(actions, Vec::new());
    // and a stream with no own offer at all sends nothing either
    let actions = sender.message(&"b", request(2, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
    assert_eq!(sender.served(&"b", 2), Vec::new());
}

/// A stale id at the end of a stream is ignored, the stream standing until
/// its own id.
#[test]
fn a_stale_served_id_is_ignored() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = sender.message(&"b", request(1, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
    assert_eq!(sender.served(&"b", 2), Vec::new());
    let actions = sender.message(&"b", request(3, 1, 2), now);
    assert!(matches!(
        actions[0],
        Action::Send {
            message: Message::Transfer(Transfer::End { request_id: 3, .. }),
            ..
        }
    ));
    assert_eq!(sender.served(&"b", 1), Vec::new());
    let actions = sender.message(&"b", request(4, 1, 2), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
}

/// Unlinking a serving peer stops the stream, with nothing behind it, and
/// the peer is gone.
#[test]
fn unlinked_stops_the_stream() {
    let now = Instant::now();
    let mut sender = Exchange::<&str>::new(Some(build()));
    sender.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let actions = sender.message(&"b", request(1, 1, 3), now);
    assert!(matches!(actions[0], Action::Serve { .. }));
    let actions = sender.unlinked(&"b");
    assert_eq!(actions, vec![Action::StopServing { peer: "b" }]);
    let actions = sender.message(&"b", request(2, 1, 2), now);
    assert_eq!(actions, Vec::new());
    assert_eq!(sender.served(&"b", 1), Vec::new());
}

/// Unlinking drops the request, the kept offers and the pending retry, and
/// the peer is gone.
#[test]
fn unlinked_drops_the_request_the_offers_and_the_retry() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let (kept, _, _) = author(4);
    receiver.offer(&"a", kept, true, now);
    receiver.unlinked(&"a");
    assert_eq!(receiver.next_deadline(), None);
    let actions = receiver.offer(&"a", author(3).0, true, now);
    assert_eq!(actions, Vec::new());
    let actions = receiver.message(
        &"a",
        Transfer::End {
            request_id: 1,
            outcome: EndOutcome::Complete,
        },
        now,
    );
    assert_eq!(actions, Vec::new());
}

/// A device without a build adopts offers and serves requests, but never
/// requests and never retries.
#[test]
fn a_device_without_a_build_never_requests() {
    let now = Instant::now();
    let mut receiver = Exchange::<&str>::new(None);
    receiver.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let (offer, _, _) = author(2);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert_eq!(actions.len(), 1);
    assert_eq!(receiver.next_deadline(), None);
    assert_eq!(
        receiver.timeout(now + Duration::from_secs(3600)),
        Vec::new()
    );
    let actions = receiver.message(&"a", request(1, 1, 2), now);
    assert_eq!(
        actions,
        vec![Action::Serve {
            peer: "a",
            request_id: 1,
            from: 1,
            to: 2,
            build: build(),
        }]
    );
}

/// The next deadline is the earliest frame deadline and retry across every
/// peer.
#[test]
fn the_next_deadline_is_the_earliest_across_peers() {
    let now = Instant::now();
    let mut a = Exchange::<&str>::new(Some(build()));
    a.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    a.linked(
        "b",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let late = now + Duration::from_secs(10);
    let actions = a.offer(&"a", author(3).0, true, now);
    assert!(matches!(actions[1], Action::Send { .. }));
    let actions = a.offer(&"b", author(3).0, true, late);
    assert!(matches!(actions[1], Action::Send { .. }));
    assert_eq!(a.next_deadline(), Some(now + FRAME_BOUND));
}

/// An offer or a message from a peer that is not linked is ignored, and a
/// manifest with no outstanding request is ignored.
#[test]
fn a_message_from_an_unlinked_peer_is_ignored() {
    let now = Instant::now();
    let mut a = Exchange::<&str>::new(Some(build()));
    let (offer, rows, bodies) = author(3);
    assert_eq!(a.offer(&"a", offer, true, now), Vec::new());
    assert_eq!(a.message(&"a", request(1, 1, 3), now), Vec::new());
    a.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    assert_eq!(a.message(&"a", manifest, now), Vec::new());
    let end = Transfer::End {
        request_id: 1,
        outcome: EndOutcome::Complete,
    };
    assert_eq!(a.message(&"a", end, now), Vec::new());
    let _ = bodies;
}

/// Unlinking a peer whose link never came up, or already went down, sends
/// nothing.
#[test]
fn unlinked_for_an_unknown_peer_is_a_noop() {
    let mut a = Exchange::<&str>::new(Some(build()));
    a.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    assert_eq!(a.unlinked(&"b"), Vec::new());
    assert_eq!(a.unlinked(&"a"), Vec::new());
    assert_eq!(a.unlinked(&"a"), Vec::new());
}

/// A commit failure with no request outstanding moves the cursor back and
/// schedules nothing.
#[test]
fn a_failed_commit_with_no_request_only_moves_the_cursor() {
    let now = Instant::now();
    let mut a = Exchange::<&str>::new(Some(build()));
    a.linked("a", first_frontier());
    a.commit_failed(&"a", first_frontier(), now);
    assert_eq!(a.next_deadline(), None);
}

/// A manifest, body or end with no request outstanding is ignored.
#[test]
fn frames_with_no_request_outstanding_are_ignored() {
    let now = Instant::now();
    let mut a = Exchange::<&str>::new(Some(build()));
    a.linked(
        "a",
        Frontier {
            seq: 0,
            head: GENESIS,
        },
    );
    let (offer, rows, bodies) = author(3);
    let _ = offer;
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows,
        last: true,
    };
    assert_eq!(a.message(&"a", manifest, now), Vec::new());
    let body = Transfer::Body {
        request_id: 1,
        seq: 1,
        changeset: bodies.into_iter().next().expect("a body"),
    };
    assert_eq!(a.message(&"a", body, now), Vec::new());
    let end = Transfer::End {
        request_id: 1,
        outcome: EndOutcome::Complete,
    };
    assert_eq!(a.message(&"a", end, now), Vec::new());
}

/// A frame deadline and a pending retry that are not yet due send nothing.
#[test]
fn a_timeout_before_a_due_deadline_does_nothing() {
    let now = Instant::now();
    let Ctx { mut receiver, .. } = ctx(3, now);
    let deadline = now + FRAME_BOUND;
    assert_eq!(receiver.timeout(now + Duration::from_secs(1)), Vec::new());
    receiver.timeout(deadline);
    assert_eq!(
        receiver.timeout(deadline + Duration::from_secs(2)),
        Vec::new()
    );
    assert_eq!(receiver.next_deadline(), Some(deadline + RETRY_FIRST));
}

/// A stalled request whose rows all applied leaves nothing behind the
/// cursor, so the due retry sends nothing and no deadline stays pending.
#[test]
fn a_retry_with_nothing_behind_sends_nothing() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    let deadline = now + FRAME_BOUND;
    // The peer streams the whole range, but its end is lost.
    let manifest = Transfer::Manifest {
        request_id: 1,
        previous: GENESIS,
        rows: rows.clone(),
        last: true,
    };
    assert_eq!(receiver.message(&"a", manifest, now), Vec::new());
    apply_bodies(&mut receiver, &rows, &bodies, now);
    assert_eq!(receiver.timeout(deadline), Vec::new());
    assert_eq!(receiver.next_deadline(), Some(deadline + RETRY_FIRST));
    assert_eq!(receiver.timeout(deadline + RETRY_FIRST), Vec::new());
    assert_eq!(receiver.next_deadline(), None);
}

/// A valid offer whose newest sits at the cursor adopts without a request.
#[test]
fn a_valid_offer_with_nothing_behind_the_cursor_adopts_only() {
    let now = Instant::now();
    let Ctx {
        mut receiver,
        rows,
        bodies,
    } = ctx(3, now);
    serve_all(&mut receiver, 1, &rows, &bodies, 8, now);
    let (offer, _, _) = author(3);
    let actions = receiver.offer(&"a", offer, true, now);
    assert!(matches!(actions[0], Action::Adopt { .. }));
    assert_eq!(actions.len(), 1);
    assert_eq!(receiver.next_deadline(), None);
}
