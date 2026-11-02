//! The media links' rules, one test per row of R79's table "Every event in
//! every situation" the machine answers, and the wire's bounds.

use std::time::{Duration, Instant};

use crate::media::{
    Ask, MAX_FETCH_LEN, MAX_PIECE_BYTES, MAX_SERVING, Media, MediaAction, MediaLinks, Priority,
};
use crate::{BoundsError, MAX_MANIFEST_ROWS};

const FILE: [u8; 32] = [7; 32];

fn want() -> Ask {
    Ask::Want { file_id: FILE }
}

fn fetch(start: u64) -> Ask {
    Ask::Fetch {
        file_id: FILE,
        start,
        len: 1024,
    }
}

/// The request id of the one `Send` in `actions`, checking it carries `ask`.
fn sent(actions: &[MediaAction<&'static str, u32>], peer: &str, ask: &Ask) -> u64 {
    let [MediaAction::Send { peer: to, message }] = actions else {
        panic!("expected one send, got {actions:?}");
    };
    assert_eq!(*to, peer);
    assert_eq!(message.ask().as_ref(), Some(ask), "{message:?}");
    message.request_id()
}

fn linked(peers: &[&'static str]) -> MediaLinks<&'static str, u32> {
    let mut links = MediaLinks::new();
    for peer in peers {
        links.linked(peer);
    }
    links
}

fn manifest(request_id: u64, last: bool) -> Media {
    Media::Manifest {
        request_id,
        chunks: vec![([1; 32], 1024)],
        last,
    }
}

fn piece(request_id: u64, last: bool) -> Media {
    Media::Piece {
        request_id,
        encoded: vec![3; 16],
        last,
    }
}

#[test]
fn an_ask_on_an_idle_link_sends_at_once() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let actions = links.ask(&"bob", want(), 1, Priority::OnDemand, now);
    sent(&actions, "bob", &want());
}

#[test]
fn an_ask_on_an_unlinked_peer_is_lost() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let actions = links.ask(&"carol", want(), 1, Priority::OnDemand, now);
    assert_eq!(
        actions,
        vec![MediaAction::Lost {
            peer: "carol",
            tag: 1
        }]
    );
}

#[test]
fn request_ids_differ_across_requests() {
    let now = Instant::now();
    let mut links = linked(&["bob", "carol"]);
    let first = sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, now),
        "bob",
        &want(),
    );
    let second = sent(
        &links.ask(&"carol", want(), 2, Priority::OnDemand, now),
        "carol",
        &want(),
    );
    assert_ne!(first, second);
}

#[test]
fn an_on_demand_ask_goes_ahead_of_every_queued_prefetch() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let id = sent(
        &links.ask(&"bob", want(), 1, Priority::Prefetch, now),
        "bob",
        &want(),
    );
    assert_eq!(
        links.ask(&"bob", fetch(0), 2, Priority::Prefetch, now),
        Vec::new()
    );
    assert_eq!(
        links.ask(&"bob", fetch(1024), 3, Priority::Prefetch, now),
        Vec::new()
    );
    assert_eq!(
        links.ask(&"bob", fetch(2048), 4, Priority::OnDemand, now),
        Vec::new()
    );
    let replied = links.received(&"bob", manifest(id, true), now);
    assert_eq!(
        replied,
        vec![MediaAction::Reply {
            peer: "bob",
            tag: 1,
            message: manifest(id, true),
            done: true,
        }]
    );
    sent(&links.proceed(&"bob", None, now), "bob", &fetch(2048));
}

#[test]
fn a_continuation_goes_ahead_of_the_queue() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let id = sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, now),
        "bob",
        &want(),
    );
    assert_eq!(
        links.ask(&"bob", fetch(4096), 2, Priority::OnDemand, now),
        Vec::new()
    );
    links.received(&"bob", manifest(id, true), now);
    let next = links.proceed(&"bob", Some((fetch(0), 1)), now);
    sent(&next, "bob", &fetch(0));
}

#[test]
fn an_answered_link_sends_nothing_until_it_proceeds() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let id = sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, now),
        "bob",
        &want(),
    );
    links.received(&"bob", manifest(id, true), now);
    assert_eq!(
        links.ask(&"bob", fetch(0), 2, Priority::OnDemand, now),
        Vec::new()
    );
    sent(&links.proceed(&"bob", None, now), "bob", &fetch(0));
}

#[test]
fn a_manifest_before_its_last_keeps_the_request_and_renews_its_bound() {
    let start = Instant::now();
    let mut links = linked(&["bob"]);
    let id = sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, start),
        "bob",
        &want(),
    );
    let later = start + Duration::from_secs(20);
    let actions = links.received(&"bob", manifest(id, false), later);
    assert_eq!(
        actions,
        vec![MediaAction::Reply {
            peer: "bob",
            tag: 1,
            message: manifest(id, false),
            done: false,
        }]
    );
    assert_eq!(links.next_deadline(), Some(later + crate::FRAME_BOUND));
    let again = links.received(&"bob", manifest(id, true), later + Duration::from_secs(25));
    assert!(matches!(
        again.as_slice(),
        [MediaAction::Reply { done: true, .. }]
    ));
}

#[test]
fn pieces_keep_the_request_until_the_last() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let id = sent(
        &links.ask(&"bob", fetch(0), 1, Priority::OnDemand, now),
        "bob",
        &fetch(0),
    );
    assert!(matches!(
        links.received(&"bob", piece(id, false), now).as_slice(),
        [MediaAction::Reply { done: false, .. }]
    ));
    assert!(matches!(
        links.received(&"bob", piece(id, true), now).as_slice(),
        [MediaAction::Reply { done: true, .. }]
    ));
    assert_eq!(links.next_deadline(), None);
}

#[test]
fn refused_and_busy_end_the_request() {
    let answers: [fn(u64) -> Media; 2] = [
        |id| Media::Refused { request_id: id },
        |id| Media::Busy { request_id: id },
    ];
    for answer in answers {
        let now = Instant::now();
        let mut links = linked(&["bob"]);
        let id = sent(
            &links.ask(&"bob", want(), 1, Priority::OnDemand, now),
            "bob",
            &want(),
        );
        assert!(matches!(
            links.received(&"bob", answer(id), now).as_slice(),
            [MediaAction::Reply {
                tag: 1,
                done: true,
                ..
            }]
        ));
        assert_eq!(links.next_deadline(), None);
    }
}

#[test]
fn a_reply_quoting_another_request_is_ignored() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let id = sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, now),
        "bob",
        &want(),
    );
    assert_eq!(
        links.received(&"bob", manifest(id + 1, true), now),
        Vec::new()
    );
    assert_eq!(
        links.received(&"carol", manifest(id, true), now),
        Vec::new()
    );
    links.received(&"bob", manifest(id, true), now);
    assert_eq!(links.received(&"bob", piece(id, true), now), Vec::new());
}

#[test]
fn a_reply_with_no_request_outstanding_is_ignored() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    assert_eq!(links.received(&"bob", manifest(0, true), now), Vec::new());
}

#[test]
fn thirty_seconds_without_the_next_frame_lapse_the_request() {
    let start = Instant::now();
    let mut links = linked(&["bob"]);
    sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, start),
        "bob",
        &want(),
    );
    assert_eq!(
        links.ask(&"bob", fetch(0), 2, Priority::OnDemand, start),
        Vec::new()
    );
    let just_before = start + crate::FRAME_BOUND.saturating_sub(Duration::from_millis(1));
    assert_eq!(links.timeout(just_before), Vec::new());
    let lapsed = links.timeout(start + crate::FRAME_BOUND);
    assert_eq!(
        lapsed,
        vec![MediaAction::Lapsed {
            peer: "bob",
            tag: 1
        }]
    );
    sent(
        &links.proceed(&"bob", None, start + crate::FRAME_BOUND),
        "bob",
        &fetch(0),
    );
}

#[test]
fn a_lost_link_loses_the_outstanding_and_queued_asks_and_stops_serving() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, now),
        "bob",
        &want(),
    );
    links.ask(&"bob", fetch(0), 2, Priority::Prefetch, now);
    links.received(
        &"bob",
        Media::Want {
            request_id: 40,
            file_id: FILE,
        },
        now,
    );
    let actions = links.unlinked(&"bob");
    assert_eq!(
        actions,
        vec![
            MediaAction::Lost {
                peer: "bob",
                tag: 1
            },
            MediaAction::Lost {
                peer: "bob",
                tag: 2
            },
            MediaAction::StopServing { peer: "bob" },
        ]
    );
    assert_eq!(links.next_deadline(), None);
    assert!(links.linked_peers().next().is_none());
}

#[test]
fn a_request_on_an_idle_link_is_served() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let actions = links.received(
        &"bob",
        Media::Want {
            request_id: 9,
            file_id: FILE,
        },
        now,
    );
    assert_eq!(
        actions,
        vec![MediaAction::Serve {
            peer: "bob",
            request_id: 9,
            ask: want(),
        }]
    );
}

#[test]
fn a_second_request_from_a_served_peer_is_busy() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    links.received(
        &"bob",
        Media::Want {
            request_id: 9,
            file_id: FILE,
        },
        now,
    );
    let actions = links.received(
        &"bob",
        Media::Fetch {
            request_id: 10,
            file_id: FILE,
            start: 0,
            len: 1024,
        },
        now,
    );
    assert_eq!(
        actions,
        vec![MediaAction::Send {
            peer: "bob",
            message: Media::Busy { request_id: 10 },
        }]
    );
    links.served(&"bob", 9);
    assert!(matches!(
        links
            .received(
                &"bob",
                Media::Want {
                    request_id: 11,
                    file_id: FILE
                },
                now
            )
            .as_slice(),
        [MediaAction::Serve { request_id: 11, .. }]
    ));
}

#[test]
fn a_holder_serves_at_most_two_peers_at_once() {
    assert_eq!(MAX_SERVING, 2);
    let now = Instant::now();
    let mut links = linked(&["bob", "carol", "dave"]);
    for (peer, id) in [("bob", 1), ("carol", 2)] {
        assert!(matches!(
            links
                .received(
                    &peer,
                    Media::Want {
                        request_id: id,
                        file_id: FILE
                    },
                    now
                )
                .as_slice(),
            [MediaAction::Serve { .. }]
        ));
    }
    let busy = links.received(
        &"dave",
        Media::Want {
            request_id: 3,
            file_id: FILE,
        },
        now,
    );
    assert_eq!(
        busy,
        vec![MediaAction::Send {
            peer: "dave",
            message: Media::Busy { request_id: 3 },
        }]
    );
    links.served(&"carol", 2);
    assert!(matches!(
        links
            .received(
                &"dave",
                Media::Want {
                    request_id: 4,
                    file_id: FILE
                },
                now
            )
            .as_slice(),
        [MediaAction::Serve { .. }]
    ));
}

#[test]
fn a_stale_served_id_changes_nothing() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    links.received(
        &"bob",
        Media::Want {
            request_id: 9,
            file_id: FILE,
        },
        now,
    );
    links.served(&"bob", 8);
    let busy = links.received(
        &"bob",
        Media::Want {
            request_id: 10,
            file_id: FILE,
        },
        now,
    );
    assert!(matches!(busy.as_slice(), [MediaAction::Send { .. }]));
}

#[test]
fn a_request_from_an_unlinked_peer_is_ignored() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    assert_eq!(
        links.received(
            &"carol",
            Media::Want {
                request_id: 9,
                file_id: FILE
            },
            now
        ),
        Vec::new()
    );
}

#[test]
fn candidates_follow_link_up_order_and_a_relink_moves_to_the_end() {
    let mut links = linked(&["bob", "carol", "dave"]);
    assert_eq!(
        links.linked_peers().copied().collect::<Vec<_>>(),
        ["bob", "carol", "dave"]
    );
    links.unlinked(&"bob");
    links.linked(&"bob");
    assert_eq!(
        links.linked_peers().copied().collect::<Vec<_>>(),
        ["carol", "dave", "bob"]
    );
}

#[test]
fn a_continuation_for_an_unlinked_peer_is_lost_and_an_unknown_unlink_is_quiet() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    assert_eq!(
        links.proceed(&"carol", Some((want(), 5)), now),
        vec![MediaAction::Lost {
            peer: "carol",
            tag: 5
        }]
    );
    assert_eq!(links.proceed(&"carol", None, now), Vec::new());
    assert_eq!(links.unlinked(&"carol"), Vec::new());
}

#[test]
fn a_continuation_handed_over_while_a_request_waits_goes_first_after_it() {
    let now = Instant::now();
    let mut links = linked(&["bob"]);
    let id = sent(
        &links.ask(&"bob", want(), 1, Priority::OnDemand, now),
        "bob",
        &want(),
    );
    assert_eq!(
        links.ask(&"bob", fetch(4096), 2, Priority::OnDemand, now),
        Vec::new()
    );
    assert_eq!(links.proceed(&"bob", Some((fetch(0), 3)), now), Vec::new());
    links.received(&"bob", manifest(id, true), now);
    sent(&links.proceed(&"bob", None, now), "bob", &fetch(0));
}

#[test]
fn the_wire_bounds_hold() {
    let rows = vec![([0; 32], 1); MAX_MANIFEST_ROWS + 1];
    let over = Media::Manifest {
        request_id: 1,
        chunks: rows,
        last: true,
    };
    assert_eq!(over.within_bounds(), Err(BoundsError::TooManyRows));
    let big = Media::Piece {
        request_id: 1,
        encoded: vec![0; MAX_PIECE_BYTES + 1],
        last: true,
    };
    assert_eq!(big.within_bounds(), Err(BoundsError::PieceTooLong));
    for len in [0, MAX_FETCH_LEN + 1] {
        let range = Media::Fetch {
            request_id: 1,
            file_id: FILE,
            start: 0,
            len,
        };
        assert_eq!(range.within_bounds(), Err(BoundsError::RangeOutOfBounds));
    }
    let wrapping = Media::Fetch {
        request_id: 1,
        file_id: FILE,
        start: u64::MAX,
        len: 2,
    };
    assert_eq!(wrapping.within_bounds(), Err(BoundsError::RangeOutOfBounds));
    let fine = Media::Fetch {
        request_id: 1,
        file_id: FILE,
        start: 0,
        len: MAX_FETCH_LEN,
    };
    assert_eq!(fine.within_bounds(), Ok(()));
}
