//! Placement's ranking and verdicts, and the leaving handover run between
//! devices over [`MediaLinks`] in every delivery order (R79 decision 16).

use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use super::{Candidate, Mine, Peer, Placement, Verdict, View, decide, hash_term, rank, takers};
use crate::media::{Media, MediaAction, MediaLinks};

const FILE: [u8; 32] = [7; 32];
const NO_RATES: &BTreeMap<[u8; 32], u64> = &BTreeMap::new();

fn candidate(key: u8, account: &'static str, room: u64) -> Candidate<'static> {
    Candidate {
        key: [key; 32],
        account,
        room,
        rates: NO_RATES,
    }
}

fn keys(ranked: &[Candidate<'_>]) -> Vec<u8> {
    ranked.iter().map(|candidate| candidate.key[0]).collect()
}

#[test]
fn equal_rooms_rank_by_the_hash_and_more_room_ranks_higher() {
    let devices: Vec<Candidate<'static>> = (1..=6)
        .map(|key| {
            candidate(
                key,
                ["a", "b", "c", "d", "e", "f"][usize::from(key - 1)],
                100,
            )
        })
        .collect();
    let mut by_hash: Vec<u8> = (1..=6).collect();
    by_hash.sort_by_key(|key| core::cmp::Reverse(hash_term(&FILE, &[*key; 32])));
    assert_eq!(keys(&rank(&FILE, &devices)), by_hash);
    let last = *by_hash.last().expect("six devices");
    let mut grown = devices.clone();
    grown[usize::from(last - 1)].room = 1 << 40;
    assert_eq!(
        keys(&rank(&FILE, &grown))[0],
        last,
        "a far larger room outweighs any hash"
    );
}

#[test]
fn an_accounts_best_device_ranks_before_any_second_device() {
    let devices = [
        candidate(1, "alice", 1 << 30),
        candidate(2, "alice", 1 << 30),
        candidate(3, "bob", 1),
    ];
    let ranked = keys(&rank(&FILE, &devices));
    assert_eq!(ranked.len(), 3);
    assert_eq!(
        ranked[1], 3,
        "bob's only device comes before alice's second"
    );
}

#[test]
fn a_taker_needs_room_for_the_file() {
    let devices = [candidate(1, "a", 99), candidate(2, "b", 100)];
    assert_eq!(keys(&takers(&FILE, 100, &devices, &[])), vec![2]);
}

#[test]
fn a_taker_far_slower_to_every_source_than_the_median_is_left_out() {
    let source = [9; 32];
    let rates = |rate: u64| BTreeMap::from([(source, rate)]);
    let (fast, slow, quick) = (rates(1_000), rates(249), rates(1_000));
    let devices = [
        Candidate {
            rates: &fast,
            ..candidate(1, "a", 100)
        },
        Candidate {
            rates: &slow,
            ..candidate(2, "b", 100)
        },
        Candidate {
            rates: &quick,
            ..candidate(3, "c", 100)
        },
        candidate(4, "d", 100),
    ];
    let left: Vec<u8> = {
        let mut left = keys(&takers(&FILE, 1, &devices, &[source]));
        left.sort_unstable();
        left
    };
    assert_eq!(left, vec![1, 3, 4], "the unmeasured device stays");
    let quarter = rates(250);
    let devices = [
        devices[0],
        Candidate {
            rates: &quarter,
            ..devices[1]
        },
    ];
    assert_eq!(
        takers(&FILE, 1, &devices, &[source]).len(),
        2,
        "a rate at a quarter of the median stays"
    );
    assert_eq!(
        takers(&FILE, 1, &devices, &[[8; 32]]).len(),
        2,
        "rates to a device holding nothing do not count"
    );
    let crawl = rates(1);
    let devices = [
        Candidate {
            rates: &crawl,
            ..candidate(1, "a", 100)
        },
        candidate(2, "b", 100),
    ];
    assert_eq!(
        takers(&FILE, 1, &devices, &[source]).len(),
        2,
        "a lone measured rate has no median to fall under"
    );
}

/// One placement of `FILE` with a count of `copies`, this device keyed 1.
fn placement<'a>(copies: usize, mine: Mine<'a>, peers: Vec<Peer<'a>>) -> Placement<'a> {
    Placement {
        file: FILE,
        size: 10,
        copies,
        me: candidate(1, "me", 100),
        mine,
        peers,
        authors: Vec::new(),
    }
}

fn peer(key: u8, room: u64, view: View, seen: Option<u64>) -> Peer<'static> {
    Peer {
        candidate: candidate(key, ["", "a", "b", "c", "d"][usize::from(key.min(4))], room),
        admitted: true,
        view,
        seen,
    }
}

#[test]
fn a_device_pulls_when_it_ranks_among_the_copies_missing() {
    assert_eq!(decide(&placement(1, Mine::Absent, vec![])), Verdict::Pull);
    assert_eq!(
        decide(&placement(
            1,
            Mine::Absent,
            vec![peer(2, 100, View::Held, None)]
        )),
        Verdict::Nothing,
        "the count holds"
    );
    assert_eq!(
        decide(&placement(
            1,
            Mine::Absent,
            vec![peer(2, 1 << 40, View::Absent, None)]
        )),
        Verdict::Nothing,
        "a far roomier device pulls first"
    );
    assert_eq!(
        decide(&placement(
            2,
            Mine::Absent,
            vec![
                peer(2, 1 << 40, View::Absent, None),
                peer(3, 100, View::Leaving, None),
            ]
        )),
        Verdict::Pull,
        "a leaving holder leaves two copies missing"
    );
    let refused = Peer {
        admitted: false,
        ..peer(2, 1 << 40, View::Absent, None)
    };
    assert_eq!(
        decide(&placement(1, Mine::Absent, vec![refused])),
        Verdict::Pull,
        "a device the gate refuses never pulls"
    );
}

#[test]
fn a_holder_below_the_best_copies_leaves_as_surplus() {
    assert_eq!(
        decide(&placement(
            1,
            Mine::Held,
            vec![peer(2, 1 << 40, View::Held, None)]
        )),
        Verdict::Surplus
    );
    assert_eq!(
        decide(&placement(
            1,
            Mine::Held,
            vec![peer(2, 1, View::Held, None)]
        )),
        Verdict::Nothing
    );
    assert_eq!(
        decide(&placement(
            1,
            Mine::Held,
            vec![peer(2, 1 << 40, View::Leaving, None)]
        )),
        Verdict::Nothing,
        "a leaving holder does not count"
    );
}

#[test]
fn a_leaving_copy_goes_only_on_a_holder_acknowledging_this_attempt() {
    let marks = BTreeMap::from([([2; 32], 4)]);
    let leaving = Mine::Leaving {
        pressed: false,
        marks: &marks,
    };
    for (view, seen, verdict) in [
        (View::Held, Some(4), Verdict::Release),
        (View::Held, Some(3), Verdict::Nothing),
        (View::Held, None, Verdict::Nothing),
        (View::Leaving, Some(9), Verdict::Nothing),
    ] {
        assert_eq!(
            decide(&placement(1, leaving, vec![peer(2, 100, view, seen)])),
            verdict,
            "{view:?} {seen:?}"
        );
    }
    assert_eq!(
        decide(&placement(
            1,
            leaving,
            vec![peer(3, 100, View::Held, Some(9))]
        )),
        Verdict::Nothing,
        "a holder this attempt never told counts nothing"
    );
}

#[test]
fn a_pressed_copy_with_no_taker_is_stranded_and_never_withdraws() {
    let marks = BTreeMap::new();
    let pressed = Mine::Leaving {
        pressed: true,
        marks: &marks,
    };
    assert_eq!(decide(&placement(1, pressed, vec![])), Verdict::Stranded);
    assert_eq!(
        decide(&placement(
            1,
            pressed,
            vec![peer(2, 100, View::Absent, None)]
        )),
        Verdict::Nothing,
        "a device with room can take it over"
    );
    assert_eq!(
        decide(&placement(1, pressed, vec![peer(2, 1, View::Absent, None)])),
        Verdict::Stranded,
        "a device without room for it cannot"
    );
}

#[test]
fn when_every_holder_leaves_the_hash_best_withdraws() {
    let marks = BTreeMap::new();
    let leaving = Mine::Leaving {
        pressed: false,
        marks: &marks,
    };
    let other = peer(2, 100, View::Leaving, None);
    let mine_first = hash_term(&FILE, &[1; 32]) > hash_term(&FILE, &[2; 32]);
    assert_eq!(
        decide(&placement(1, leaving, vec![other])),
        if mine_first {
            Verdict::Withdraw
        } else {
            Verdict::Nothing
        }
    );
    assert_eq!(decide(&placement(1, leaving, vec![])), Verdict::Withdraw);
}

/// What a simulated device holds of `FILE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Holding {
    Absent,
    Held,
    Leaving { pressed: bool },
}

/// One device of the handover runs, its driver reduced to the rules
/// decision 16 states: numbered reports, the attempt's marks, the reply to a
/// newly marked file and the verdicts acted on at once.
struct Device {
    key: [u8; 32],
    room: u64,
    holding: Holding,
    marks: BTreeMap<[u8; 32], u64>,
    links: MediaLinks<[u8; 32], u32>,
    /// The number of the next report it sends.
    next_report: u64,
    /// Re-marks the file leaving under pressure once it withdrew.
    pressed_after_withdraw: bool,
}

/// Devices with a FIFO channel each way between every linked pair.
struct World {
    copies: usize,
    devices: Vec<Device>,
    channels: BTreeMap<(usize, usize), VecDeque<Media>>,
}

impl World {
    /// `holdings` and fresh `rooms`, every pair linked, each device's
    /// opening `Holdings` claiming the room `claims` names, queued both
    /// ways.
    fn new(copies: usize, holdings: &[Holding], rooms: &[u64], claims: &[u64]) -> Self {
        let mut world = World {
            copies,
            devices: Vec::new(),
            channels: BTreeMap::new(),
        };
        for (index, (holding, room)) in holdings.iter().zip(rooms).enumerate() {
            world.devices.push(Device {
                key: [u8::try_from(index + 1).expect("few devices"); 32],
                room: *room,
                holding: *holding,
                marks: BTreeMap::new(),
                links: MediaLinks::new(),
                next_report: 0,
                pressed_after_withdraw: false,
            });
        }
        let count = world.devices.len();
        for from in 0..count {
            for to in (0..count).filter(|to| *to != from) {
                let key = world.devices[to].key;
                world.devices[from].links.linked(&key);
            }
        }
        for (from, &claimed) in claims.iter().enumerate() {
            for to in (0..count).filter(|to| *to != from) {
                let holding = world.devices[from].holding;
                let held = if holding == Holding::Absent {
                    vec![]
                } else {
                    vec![FILE]
                };
                let leaving = if matches!(holding, Holding::Leaving { .. }) {
                    vec![FILE]
                } else {
                    vec![]
                };
                world.send(
                    from,
                    to,
                    Media::Holdings {
                        room: claimed,
                        rates: vec![],
                        held,
                        leaving,
                        number: 0,
                        seen: None,
                        last: true,
                    },
                );
            }
        }
        world
    }

    fn linked(&self, from: usize) -> Vec<usize> {
        (0..self.devices.len())
            .filter(|to| *to != from && self.channels.contains_key(&(from, *to)))
            .collect()
    }

    /// Numbers `report` from `from` to `to` and queues it, recording the
    /// attempt's mark when it carries `from`'s leaving file.
    fn send(&mut self, from: usize, to: usize, mut report: Media) {
        let to_key = self.devices[to].key;
        let device = &mut self.devices[from];
        let number = device.next_report;
        device.next_report += 1;
        report.stamp(number, device.links.seen(&to_key));
        let marks = match &report {
            Media::Holdings { leaving, .. } | Media::Held { leaving, .. } => {
                leaving.contains(&FILE)
            }
            _ => false,
        };
        if marks && matches!(device.holding, Holding::Leaving { .. }) {
            device.marks.insert(to_key, number);
        }
        self.channels
            .entry((from, to))
            .or_default()
            .push_back(report);
    }

    fn broadcast(&mut self, from: usize, report: &Media) {
        for to in self.linked(from) {
            self.send(from, to, report.clone());
        }
    }

    fn change(staying: bool, leaving: bool, removed: bool) -> Media {
        let file = |on: bool| if on { vec![FILE] } else { vec![] };
        Media::Held {
            added: vec![],
            removed: file(removed),
            staying: file(staying),
            leaving: file(leaving),
            number: 0,
            seen: None,
        }
    }

    /// Marks `index`'s copy leaving.
    fn mark(&mut self, index: usize, pressed: bool) {
        let device = &mut self.devices[index];
        device.holding = Holding::Leaving { pressed };
        device.marks.clear();
        self.broadcast(index, &Self::change(false, true, false));
    }

    /// Acts on `index`'s verdicts until it has none.
    fn settle(&mut self, index: usize) {
        loop {
            match self.verdict(index) {
                Verdict::Surplus => self.mark(index, false),
                Verdict::Release => {
                    let device = &mut self.devices[index];
                    device.holding = Holding::Absent;
                    device.marks.clear();
                    self.broadcast(index, &Self::change(false, false, true));
                }
                Verdict::Withdraw => {
                    let device = &mut self.devices[index];
                    device.holding = Holding::Held;
                    device.marks.clear();
                    let pressed = core::mem::take(&mut device.pressed_after_withdraw);
                    self.broadcast(index, &Self::change(true, false, false));
                    if pressed {
                        self.mark(index, true);
                    }
                }
                Verdict::Nothing | Verdict::Pull | Verdict::Stranded => return,
            }
        }
    }

    fn verdict(&self, index: usize) -> Verdict {
        let device = &self.devices[index];
        let mine = match device.holding {
            Holding::Absent => Mine::Absent,
            Holding::Held => Mine::Held,
            Holding::Leaving { pressed } => Mine::Leaving {
                pressed,
                marks: &device.marks,
            },
        };
        let peers = self
            .linked(index)
            .into_iter()
            .filter_map(|other| {
                let key = self.devices[other].key;
                let report = device.links.report(&key)?;
                let view = if report.is_leaving(&FILE) {
                    View::Leaving
                } else if report.holds(&FILE) {
                    View::Held
                } else {
                    View::Absent
                };
                Some(Peer {
                    candidate: Candidate {
                        key,
                        account: "same",
                        room: report.room(),
                        rates: report.rates(),
                    },
                    admitted: true,
                    view,
                    seen: report.seen(),
                })
            })
            .collect();
        let verdict = decide(&Placement {
            file: FILE,
            size: 1,
            copies: self.copies,
            me: Candidate {
                key: device.key,
                account: "same",
                room: device.room,
                rates: NO_RATES,
            },
            mine,
            peers,
            authors: Vec::new(),
        });
        // Pulls are not modelled, the runs only hand copies on.
        if verdict == Verdict::Pull {
            Verdict::Nothing
        } else {
            verdict
        }
    }

    /// Delivers the next report from `from` to `to`, the receiver answering
    /// a newly marked file it holds at once, then acting on its verdicts.
    fn deliver(&mut self, from: usize, to: usize) {
        let report = self
            .channels
            .get_mut(&(from, to))
            .and_then(VecDeque::pop_front)
            .expect("a queued report");
        let from_key = self.devices[from].key;
        let actions = self.devices[to]
            .links
            .received(&from_key, report, Instant::now());
        let answer = actions.iter().any(|action| {
            matches!(action, MediaAction::Reported { marked, .. } if marked.contains(&FILE))
        }) && self.devices[to].holding != Holding::Absent;
        if answer {
            self.send(to, from, Self::change(false, false, false));
        }
        self.settle(to);
    }

    /// Takes the link between `a` and `b` down, with what was in flight.
    fn cut(&mut self, a: usize, b: usize) {
        for (from, to) in [(a, b), (b, a)] {
            self.channels.remove(&(from, to));
            let key = self.devices[to].key;
            self.devices[from].links.unlinked(&key);
            self.devices[from].marks.remove(&key);
        }
        self.settle(a);
        self.settle(b);
    }

    fn ready(&self) -> Vec<(usize, usize)> {
        self.channels
            .iter()
            .filter(|(_, queue)| !queue.is_empty())
            .map(|(pair, _)| *pair)
            .collect()
    }

    fn holders(&self) -> usize {
        self.devices
            .iter()
            .filter(|device| device.holding != Holding::Absent)
            .count()
    }
}

/// A step a run takes: deliver one report, or cut a link.
#[derive(Clone, Copy, Debug)]
enum Step {
    Deliver(usize, usize),
    Cut(usize, usize),
}

/// Runs every order of deliveries, and of the `cuts` among them, from the
/// world `make` builds, checking `always` after every step and `finally`
/// once nothing is left to deliver.
fn every_order(
    make: impl Fn() -> World,
    cuts: &[(usize, usize)],
    always: impl Fn(&World),
    finally: impl Fn(&World),
) -> usize {
    let mut runs = 0;
    let mut pending: Vec<Vec<usize>> = vec![Vec::new()];
    while let Some(mut path) = pending.pop() {
        runs += 1;
        let mut world = make();
        for index in 0..world.devices.len() {
            world.settle(index);
        }
        let mut cut: Vec<(usize, usize)> = cuts.to_vec();
        let mut depth = 0;
        loop {
            always(&world);
            let steps: Vec<Step> = world
                .ready()
                .into_iter()
                .map(|(from, to)| Step::Deliver(from, to))
                .chain(cut.iter().map(|(a, b)| Step::Cut(*a, *b)))
                .collect();
            if steps.is_empty() {
                finally(&world);
                break;
            }
            if depth == path.len() {
                for other in 1..steps.len() {
                    let mut branch = path.clone();
                    branch.push(other);
                    pending.push(branch);
                }
                path.push(0);
            }
            match steps[path[depth]] {
                Step::Deliver(from, to) => world.deliver(from, to),
                Step::Cut(a, b) => {
                    cut.retain(|pair| *pair != (a, b));
                    world.cut(a, b);
                }
            }
            depth += 1;
        }
    }
    runs
}

/// Runs `runs` orders drawn from fixed seeds, for a world too large to
/// run in every order, checking as [`every_order`] does.
fn seeded_orders(
    runs: u64,
    make: impl Fn() -> World,
    cuts: &[(usize, usize)],
    always: impl Fn(&World),
    finally: impl Fn(&World),
) {
    for seed in 1..=runs {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut world = make();
        for index in 0..world.devices.len() {
            world.settle(index);
        }
        let mut cut: Vec<(usize, usize)> = cuts.to_vec();
        loop {
            always(&world);
            let steps: Vec<Step> = world
                .ready()
                .into_iter()
                .map(|(from, to)| Step::Deliver(from, to))
                .chain(cut.iter().map(|(a, b)| Step::Cut(*a, *b)))
                .collect();
            if steps.is_empty() {
                finally(&world);
                break;
            }
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let pick = usize::try_from(state % steps.len() as u64).expect("few steps");
            match steps[pick] {
                Step::Deliver(from, to) => world.deliver(from, to),
                Step::Cut(a, b) => {
                    cut.retain(|pair| *pair != (a, b));
                    world.cut(a, b);
                }
            }
        }
    }
}

fn some_copy_kept(world: &World) {
    assert!(world.holders() >= 1, "every copy was released");
}

#[test]
fn two_holders_ranking_each_other_first_keep_exactly_one_copy_in_every_order() {
    // Each claims far more room than it has, so each ranks the other first
    // and leaves as surplus.
    let runs = every_order(
        || World::new(1, &[Holding::Held; 2], &[1; 2], &[1 << 40; 2]),
        &[],
        some_copy_kept,
        |world| {
            assert_eq!(world.holders(), 1);
            assert!(
                world
                    .devices
                    .iter()
                    .all(|device| !matches!(device.holding, Holding::Leaving { .. })),
                "the copy kept is not left leaving"
            );
        },
    );
    assert!(runs > 1, "{runs}");
}

#[test]
fn a_withdrawal_followed_by_a_pressed_mark_never_loses_the_copy() {
    // A leaves, B leaves too, the hash-best withdraws and, under pressure,
    // marks the copy leaving again in a new attempt that counts nothing the
    // first attempt was acknowledged with.
    let runs = every_order(
        || {
            let mut world = World::new(1, &[Holding::Held; 2], &[1; 2], &[1 << 40; 2]);
            for device in &mut world.devices {
                device.pressed_after_withdraw = true;
            }
            world
        },
        &[],
        some_copy_kept,
        some_copy_kept,
    );
    assert!(runs > 1, "{runs}");
}

#[test]
fn a_third_holder_whose_link_falls_never_counts() {
    seeded_orders(
        20_000,
        || World::new(1, &[Holding::Held; 3], &[1; 3], &[1 << 40; 3]),
        &[(0, 2), (1, 2)],
        some_copy_kept,
        some_copy_kept,
    );
}

#[test]
fn an_idle_holder_answers_a_mark_and_the_leaver_releases() {
    let rooms = [1, 1 << 40];
    let mut world = World::new(1, &[Holding::Held; 2], &rooms, &rooms);
    world.deliver(1, 0);
    assert_eq!(
        world.devices[0].holding,
        Holding::Leaving { pressed: false },
        "the device with less room leaves as surplus"
    );
    world.deliver(0, 1);
    world.deliver(0, 1);
    assert_eq!(
        world.ready(),
        vec![(1, 0)],
        "the idle holder answers the mark with a report"
    );
    assert_eq!(
        world.devices[0].holding,
        Holding::Leaving { pressed: false }
    );
    world.deliver(1, 0);
    assert_eq!(world.devices[0].holding, Holding::Absent);
    assert_eq!(world.devices[1].holding, Holding::Held);
}

#[test]
fn a_first_holdings_crossing_a_mark_acknowledges_nothing() {
    let mut world = World::new(
        1,
        &[Holding::Leaving { pressed: true }, Holding::Held],
        &[1; 2],
        &[1; 2],
    );
    world.deliver(1, 0);
    assert_eq!(
        world.devices[0].holding,
        Holding::Leaving { pressed: true },
        "a Holdings sent before reading the mark is no acknowledgement"
    );
    world.deliver(0, 1);
    world.deliver(1, 0);
    assert_eq!(world.devices[0].holding, Holding::Absent);
}

#[test]
fn a_withdrawal_lets_the_remaining_leaver_release() {
    let mut world = World::new(
        1,
        &[
            Holding::Leaving { pressed: true },
            Holding::Leaving { pressed: false },
        ],
        &[1; 2],
        &[1; 2],
    );
    world.deliver(0, 1);
    assert_eq!(
        world.devices[1].holding,
        Holding::Held,
        "the only surplus leaver withdraws"
    );
    while let Some(&(from, to)) = world.ready().first() {
        world.deliver(from, to);
    }
    assert_eq!(world.devices[0].holding, Holding::Absent);
    assert_eq!(world.devices[1].holding, Holding::Held);
}
