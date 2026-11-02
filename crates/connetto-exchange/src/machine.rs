//! The exchange's state machine, both roles at once (R77 slice 6b).
//!
//! One [`Exchange`] sits behind one device and keeps one record per linked
//! peer, running the receiver's situations (R0 to R3) and the sender's
//! (S0 to S2) of the plan's table "Every event in every situation". It reads
//! no database, opens no socket and starts no timer. The driver feeds it
//! link events, messages and monotonic instants, and carries out the
//! [`Action`]s it returns, having checked each offer's signature against the
//! key the link proved.

use std::fmt::Debug;
use std::hash::Hash;
use std::time::{Duration, Instant};

use connetto_core::chain::chain_walk;
use connetto_core::exchange::{EndOutcome, Frontier, ManifestRow, SignedOffer};
use connetto_core::schema::{Major, SchemaVersion};
use indexmap::IndexMap;
use sha2::{Digest, Sha256};

use crate::captured_under;
use crate::message::{Message, Transfer};

/// The bound on the wait for the next frame of an outstanding range
/// (decision 10).
pub const FRAME_BOUND: Duration = Duration::from_secs(30);

/// The first backoff of a stalled request's retry (decision 22).
pub const RETRY_FIRST: Duration = Duration::from_secs(5);

/// The ceiling of the backoff a stalled request's retry doubles into
/// (decision 22).
pub const RETRY_CEILING: Duration = Duration::from_secs(300);

/// What the machine asks its driver to do, in the order to do it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action<P> {
    /// Send `message` on the link to `peer`.
    Send {
        /// The link's peer.
        peer: P,
        /// The message to send.
        message: Message,
    },
    /// Adopt a verified offer from `peer`. The driver retracts each of its
    /// retractions, then adjudicates its settled frontier, then stores the
    /// offer and `cursor` (moved up to `settled` when that was higher), each
    /// step idempotent so a crash between them replays at the author's next
    /// offer.
    Adopt {
        /// The offer's author.
        peer: P,
        /// The verified offer.
        offer: SignedOffer,
        /// The cursor to store with it.
        cursor: Frontier,
    },
    /// Apply `body` as `peer`'s entry `row` and store `cursor`, the row's
    /// sequence and the chain hash at the row, in the same transaction. A
    /// policy refusal still stores the cursor, the driver's business.
    Apply {
        /// The entry's author.
        peer: P,
        /// The entry's manifest row.
        row: ManifestRow,
        /// The entry's changeset.
        body: Vec<u8>,
        /// The cursor to store with the entry.
        cursor: Frontier,
    },
    /// `row` is an entry of another build, so `cursor` is stored alone.
    Skip {
        /// The entry's author.
        peer: P,
        /// The entry's manifest row.
        row: ManifestRow,
        /// The cursor to store with the skip.
        cursor: Frontier,
    },
    /// Raise `PeerChainInvalid` for author `peer`.
    ChainInvalid {
        /// The offending author.
        peer: P,
    },
    /// Stream `from..=to` to `peer` under `request_id`, bodies only for
    /// entries captured under `build`, ending with `End { Complete }`, or
    /// answer `End { NotAvailable }` when the range is no longer held, then
    /// call [`Exchange::served`].
    Serve {
        /// The requesting peer.
        peer: P,
        /// The id every reply quotes.
        request_id: u64,
        /// The first sequence requested.
        from: u64,
        /// The last sequence requested.
        to: u64,
        /// The requester's build, whose entries cross with their bodies.
        build: (Major, SchemaVersion),
    },
    /// Stop the stream to `peer` now, followed by a [`Action::Send`] of
    /// `End { NotAvailable }` on retire and by nothing on unlink.
    StopServing {
        /// The stream's peer.
        peer: P,
    },
}

/// The receiver's side of one link, per author, holding the stored cursor,
/// the outstanding request, the last verified offer and the retry backoff.
struct Receiver {
    cursor: Frontier,
    request: Option<Request>,
    last_offer: Option<SignedOffer>,
    /// Whether the last offer's `Adopt` failed to commit, so the retry adopts
    /// it again before it requests.
    adopt_owed: bool,
    backoff: Duration,
    retry_at: Option<Instant>,
}

/// One outstanding request of the receiver, the range, its verified offer
/// and the manifest walked so far.
struct Request {
    id: u64,
    to: u64,
    previous: [u8; 32],
    offer: SignedOffer,
    build: (Major, SchemaVersion),
    rows: Vec<ManifestRow>,
    heads: Vec<[u8; 32]>,
    expected: u64,
    complete: bool,
    walk: usize,
    kept: Option<SignedOffer>,
    deadline: Instant,
}

/// The sender's side of one link.
enum Sender {
    /// S1, serving no request.
    Idle,
    /// S2, streaming a request.
    Serving {
        id: u64,
        answered_busy: bool,
        offer_waited: bool,
    },
}

/// The machine's record of one linked peer, the receiver and the sender
/// sides at once.
struct Peer {
    receiver: Receiver,
    sender: Sender,
}

/// The exchange's state machine for one device, generic over the peer
/// identity, one device writer per peer.
pub struct Exchange<P> {
    build: Option<(Major, SchemaVersion)>,
    own_offer: Option<SignedOffer>,
    retired: bool,
    next_id: u64,
    peers: IndexMap<P, Peer>,
}

impl<P: Clone + Eq + Hash + Debug> Exchange<P> {
    /// Build the exchange of a device whose own build is `build`, the
    /// `(major, version)` named in every request it issues. With `None` the
    /// device has no device writer, never requests, and still serves requests
    /// and adopts offers.
    #[must_use]
    pub fn new(build: Option<(Major, SchemaVersion)>) -> Self {
        Self {
            build,
            own_offer: None,
            retired: false,
            next_id: 0,
            peers: IndexMap::new(),
        }
    }

    /// The own offer held, `None` when none is held or the writer retired.
    #[must_use]
    pub fn own_offer(&self) -> Option<&SignedOffer> {
        self.own_offer.as_ref()
    }

    /// Hold a freshly signed own offer. It is sent at once to every linked
    /// peer that serves no request and held for every peer that does, until
    /// its stream ends. Holding an offer also clears a retired writer.
    pub fn set_own_offer(&mut self, offer: SignedOffer) -> Vec<Action<P>> {
        let own_offer = offer.clone();
        self.own_offer = Some(offer);
        self.retired = false;
        let mut actions = Vec::new();
        for (peer, state) in &mut self.peers {
            match &mut state.sender {
                Sender::Idle => {
                    actions.push(Action::Send {
                        peer: peer.clone(),
                        message: Message::Offer(own_offer.clone()),
                    });
                }
                Sender::Serving { offer_waited, .. } => *offer_waited = true,
            }
        }
        actions
    }

    /// Drop the own offer, when it went stale and a new signature is coming.
    /// Nothing is sent, and a link coming up sends nothing until a fresh
    /// offer is held.
    pub fn clear_own_offer(&mut self) {
        self.own_offer = None;
    }

    /// The own device writer retired. The offer is dropped, every serving
    /// stream stops with `End { NotAvailable }` behind it, and every later
    /// request is answered `End { NotAvailable }`.
    pub fn retire(&mut self) -> Vec<Action<P>> {
        self.retired = true;
        self.own_offer = None;
        let mut actions = Vec::new();
        for (peer, state) in &mut self.peers {
            if let Sender::Serving { id, .. } = &mut state.sender {
                let id = *id;
                state.sender = Sender::Idle;
                actions.push(Action::StopServing { peer: peer.clone() });
                actions.push(Action::Send {
                    peer: peer.clone(),
                    message: Message::Transfer(Transfer::End {
                        request_id: id,
                        outcome: EndOutcome::NotAvailable,
                    }),
                });
            }
        }
        actions
    }

    /// The driver finished streaming `request_id` to `peer`, its `End`
    /// already sent. The peer returns to serving no request, and the own
    /// offer is sent when one is held and it waited during the stream or the
    /// peer was answered `Busy` during it (decision 22). A stale id is
    /// ignored.
    pub fn served(&mut self, peer: &P, request_id: u64) -> Vec<Action<P>> {
        let Some(state) = self.peers.get_mut(peer) else {
            return Vec::new();
        };
        let mut reoffer = false;
        if let Sender::Serving {
            id,
            answered_busy,
            offer_waited,
        } = &mut state.sender
            && *id == request_id
        {
            reoffer = self.own_offer.is_some() && (*answered_busy || *offer_waited);
            state.sender = Sender::Idle;
        }
        if reoffer && let Some(offer) = self.own_offer.clone() {
            return vec![Action::Send {
                peer: peer.clone(),
                message: Message::Offer(offer),
            }];
        }
        Vec::new()
    }

    /// The link to `peer` came up. The receiver starts from `cursor`, the
    /// stored cursor for this author, the genesis frontier
    /// (`connetto_core::chain::GENESIS`) when none is stored, and the own
    /// offer is sent when one is held. A peer already linked is left as it
    /// is.
    pub fn linked(&mut self, peer: P, cursor: Frontier) -> Vec<Action<P>> {
        if self.peers.contains_key(&peer) {
            return Vec::new();
        }
        self.peers.insert(
            peer.clone(),
            Peer {
                receiver: Receiver {
                    cursor,
                    request: None,
                    last_offer: None,
                    adopt_owed: false,
                    backoff: RETRY_FIRST,
                    retry_at: None,
                },
                sender: Sender::Idle,
            },
        );
        if let Some(offer) = self.own_offer.clone() {
            vec![Action::Send {
                peer,
                message: Message::Offer(offer),
            }]
        } else {
            Vec::new()
        }
    }

    /// The link to `peer` went down, any state to unlinked. The stream stops
    /// with a stop action when it was serving, and the request, the kept
    /// offers and the pending retry are dropped.
    pub fn unlinked(&mut self, peer: &P) -> Vec<Action<P>> {
        let Some(state) = self.peers.shift_remove(peer) else {
            return Vec::new();
        };
        if let Sender::Serving { .. } = state.sender {
            vec![Action::StopServing { peer: peer.clone() }]
        } else {
            Vec::new()
        }
    }

    /// An `Offer` message from `peer`, `valid` being the driver's signature
    /// check against the key the link proved. A valid offer is adopted and,
    /// when it sits above the cursor and the device holds a build, a request
    /// goes out for its range. An invalid offer raises `PeerChainInvalid`,
    /// aborting the request when one is outstanding. A valid offer arriving
    /// while a request is outstanding is kept and acted on at the request's
    /// end.
    pub fn offer(
        &mut self,
        peer: &P,
        signed: SignedOffer,
        valid: bool,
        now: Instant,
    ) -> Vec<Action<P>> {
        let Some(state) = self.peers.get_mut(peer) else {
            return Vec::new();
        };
        if !valid {
            state.receiver.request = None;
            return vec![Action::ChainInvalid { peer: peer.clone() }];
        }
        if let Some(request) = &mut state.receiver.request {
            request.kept = Some(signed);
            return Vec::new();
        }
        act_on_valid_offer(
            &mut state.receiver,
            self.build.as_ref(),
            &mut self.next_id,
            peer.clone(),
            signed,
            now,
        )
    }

    /// A [`Transfer`] from `peer`, a request on the sender side and a
    /// manifest, body or end on the receiver side. An offer routes through
    /// [`Exchange::offer`], with the driver's signature verdict.
    pub fn message(&mut self, peer: &P, transfer: Transfer, now: Instant) -> Vec<Action<P>> {
        let Some(state) = self.peers.get_mut(peer) else {
            return Vec::new();
        };
        match transfer {
            Transfer::Request {
                request_id,
                from,
                to,
                build,
            } => handle_request(
                state,
                peer.clone(),
                self.retired,
                request_id,
                from,
                to,
                build,
            ),
            Transfer::Manifest {
                request_id,
                previous,
                rows,
                last,
            } => handle_manifest(state, peer.clone(), request_id, previous, rows, last, now),
            Transfer::Body {
                request_id,
                seq,
                changeset,
            } => handle_body(state, peer.clone(), request_id, seq, changeset),
            Transfer::End {
                request_id,
                outcome,
            } => handle_end(
                state,
                peer.clone(),
                self.build.as_ref(),
                &mut self.next_id,
                request_id,
                &outcome,
                now,
            ),
        }
    }

    /// The driver's `Apply` or `Skip` for `peer` failed to commit, so
    /// `stored`, the cursor the driver's store holds, is the truth again. An
    /// outstanding request aborts and is re-issued from `stored` after the
    /// backoff.
    pub fn commit_failed(&mut self, peer: &P, stored: Frontier, now: Instant) {
        let Some(state) = self.peers.get_mut(peer) else {
            return;
        };
        state.receiver.cursor = stored;
        if state.receiver.request.is_some() {
            abort_with_retry(&mut state.receiver, now);
        }
    }

    /// The driver's `Adopt` for `peer` failed to commit, so `stored` is the
    /// truth again and the offer is owed. Any outstanding request aborts,
    /// and after the backoff the retry adopts the offer again before it
    /// requests whatever still sits behind the cursor.
    pub fn adopt_failed(&mut self, peer: &P, stored: Frontier, now: Instant) {
        let Some(state) = self.peers.get_mut(peer) else {
            return;
        };
        state.receiver.cursor = stored;
        state.receiver.adopt_owed = true;
        abort_with_retry(&mut state.receiver, now);
    }

    /// The earliest instant [`Exchange::timeout`] has work, a request's
    /// frame deadline or a scheduled retry, or `None` when both sit idle.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.peers
            .values()
            .flat_map(|state| {
                [
                    state
                        .receiver
                        .request
                        .as_ref()
                        .map(|request| request.deadline),
                    state.receiver.retry_at,
                ]
            })
            .flatten()
            .min()
    }

    /// Run every frame deadline and retry due at or before `now`. A stalled
    /// request aborts and schedules its retry; a due retry re-issues the
    /// kept offer's request when the offer's range still sits behind the
    /// cursor.
    pub fn timeout(&mut self, now: Instant) -> Vec<Action<P>> {
        let mut actions = Vec::new();
        for (peer, state) in &mut self.peers {
            if let Some(request) = state.receiver.request.as_ref() {
                if request.deadline <= now {
                    abort_with_retry(&mut state.receiver, now);
                }
            } else if state.receiver.retry_at.is_some_and(|at| at <= now) {
                state.receiver.retry_at = None;
                actions.extend(retry_request(
                    state,
                    peer.clone(),
                    self.build.as_ref(),
                    &mut self.next_id,
                    now,
                ));
            }
        }
        actions
    }
}

/// Adopt a verified offer in R1 or R2. The fork check at the cursor (decision
/// 20) refuses a head that contradicts it. On the pass, the cursor moves up
/// to the offer's settled frontier, the offer becomes the last verified one,
/// the backoff resets, and a request goes out for the range above the cursor
/// when the device holds a build.
fn act_on_valid_offer<P: Clone>(
    receiver: &mut Receiver,
    build: Option<&(Major, SchemaVersion)>,
    next_id: &mut u64,
    peer: P,
    signed: SignedOffer,
    now: Instant,
) -> Vec<Action<P>> {
    for frontier in [signed.offer.settled.as_ref(), signed.offer.newest.as_ref()] {
        if let Some(frontier) = frontier
            && frontier.seq == receiver.cursor.seq
            && receiver.cursor.seq > 0
            && frontier.head != receiver.cursor.head
        {
            return vec![Action::ChainInvalid { peer }];
        }
    }
    let mut cursor = receiver.cursor.clone();
    if let Some(settled) = &signed.offer.settled
        && settled.seq > cursor.seq
    {
        cursor = settled.clone();
    }
    let newest = signed
        .offer
        .newest
        .as_ref()
        .filter(|newest| newest.seq > cursor.seq)
        .map(|newest| newest.seq);
    receiver.cursor = cursor.clone();
    receiver.last_offer = Some(signed.clone());
    receiver.adopt_owed = false;
    receiver.backoff = RETRY_FIRST;
    receiver.retry_at = None;
    let mut actions = vec![Action::Adopt {
        peer: peer.clone(),
        offer: signed.clone(),
        cursor: cursor.clone(),
    }];
    if let Some(build) = build
        && let Some(newest) = newest
    {
        actions.extend(begin_request(
            receiver,
            next_id,
            peer,
            signed,
            build.clone(),
            newest,
            now,
        ));
    }
    actions
}

/// Open a request for `newest` from the current cursor, with a fresh id and
/// a frame deadline at `now`.
fn begin_request<P>(
    receiver: &mut Receiver,
    next_id: &mut u64,
    peer: P,
    offer: SignedOffer,
    build: (Major, SchemaVersion),
    newest: u64,
    now: Instant,
) -> Vec<Action<P>> {
    *next_id = next_id.wrapping_add(1);
    let id = *next_id;
    let from = receiver.cursor.seq + 1;
    receiver.request = Some(Request {
        id,
        to: newest,
        previous: receiver.cursor.head,
        offer,
        build: build.clone(),
        rows: Vec::new(),
        heads: Vec::new(),
        expected: from,
        complete: false,
        walk: 0,
        kept: None,
        deadline: now + FRAME_BOUND,
    });
    receiver.retry_at = None;
    vec![Action::Send {
        peer,
        message: Message::Transfer(Transfer::Request {
            request_id: id,
            from,
            to: newest,
            build,
        }),
    }]
}

/// Re-issue the kept offer's request from the current cursor, a new id and a
/// fresh frame deadline, when the offer's range still sits behind the
/// cursor.
fn retry_request<P: Clone>(
    state: &mut Peer,
    peer: P,
    build: Option<&(Major, SchemaVersion)>,
    next_id: &mut u64,
    now: Instant,
) -> Vec<Action<P>> {
    // A retry is set only after an offer was taken, by a stalled request
    // or by a failed adopt.
    let offer = state
        .receiver
        .last_offer
        .clone()
        .expect("a due retry is set only after an offer was taken");
    let mut actions = Vec::new();
    if state.receiver.adopt_owed {
        state.receiver.adopt_owed = false;
        if let Some(settled) = &offer.offer.settled
            && settled.seq > state.receiver.cursor.seq
        {
            state.receiver.cursor = settled.clone();
        }
        actions.push(Action::Adopt {
            peer: peer.clone(),
            offer: offer.clone(),
            cursor: state.receiver.cursor.clone(),
        });
    }
    if let Some(build) = build
        && let Some(newest) = offer.offer.newest.clone()
        && newest.seq > state.receiver.cursor.seq
    {
        actions.extend(begin_request(
            &mut state.receiver,
            next_id,
            peer,
            offer,
            build.clone(),
            newest.seq,
            now,
        ));
    }
    actions
}

/// Abort the outstanding request, keep what has been processed, and schedule
/// a retry at the current backoff, doubling it for the next stall (decision
/// 22).
fn abort_with_retry(receiver: &mut Receiver, now: Instant) {
    receiver.request = None;
    receiver.retry_at = Some(now + receiver.backoff);
    receiver.backoff = receiver.backoff.saturating_mul(2).min(RETRY_CEILING);
}

/// The sender's answer to a request. A retired writer and a range at or
/// below the first entry are answered `NotAvailable`, a request while serving
/// is answered `Busy` and remembered, and the rest is streamed.
fn handle_request<P>(
    state: &mut Peer,
    peer: P,
    retired: bool,
    request_id: u64,
    from: u64,
    to: u64,
    build: (Major, SchemaVersion),
) -> Vec<Action<P>> {
    if retired || from == 0 || to < from {
        return vec![Action::Send {
            peer,
            message: Message::Transfer(Transfer::End {
                request_id,
                outcome: EndOutcome::NotAvailable,
            }),
        }];
    }
    match &mut state.sender {
        Sender::Idle => {
            state.sender = Sender::Serving {
                id: request_id,
                answered_busy: false,
                offer_waited: false,
            };
            vec![Action::Serve {
                peer,
                request_id,
                from,
                to,
                build,
            }]
        }
        Sender::Serving { answered_busy, .. } => {
            *answered_busy = true;
            vec![Action::Send {
                peer,
                message: Message::Transfer(Transfer::End {
                    request_id,
                    outcome: EndOutcome::Busy,
                }),
            }]
        }
    }
}

/// The receiver's answer to a manifest frame, for the outstanding request
/// alone. Every frame's `previous` must be the cursor's head at request time,
/// its rows must continue contiguously from `from`, and the range may not
/// carry more rows than `to` minus `from` plus one. After the last frame the
/// rows must end at `to` and the walk must reach the offer's signed head, and
/// the walk past the skipped rows begins.
fn handle_manifest<P: Clone>(
    state: &mut Peer,
    peer: P,
    request_id: u64,
    previous: [u8; 32],
    rows: Vec<ManifestRow>,
    last: bool,
    now: Instant,
) -> Vec<Action<P>> {
    let Some(request) = state.receiver.request.as_ref() else {
        return Vec::new();
    };
    if request.id != request_id {
        return Vec::new();
    }
    let mut next = request.expected;
    let mut invalid = request.complete || previous != request.previous;
    if !invalid {
        for row in &rows {
            if row.seq != next || next > request.to {
                invalid = true;
                break;
            }
            next = next.saturating_add(1);
        }
    }
    if invalid {
        state.receiver.request = None;
        return vec![Action::ChainInvalid { peer }];
    }
    let request = state.receiver.request.as_mut().expect("validated above");
    for row in rows {
        let head = chain_walk(
            &request.head(),
            std::iter::once((row.seq, Some(row.major), row.origin, row.digest)),
        );
        request.heads.push(head);
        request.rows.push(row);
        request.expected = request.expected.saturating_add(1);
    }
    request.deadline = now + FRAME_BOUND;
    if !last {
        return Vec::new();
    }
    request.complete = true;
    let ends_at_to = request.expected == request.to.saturating_add(1);
    let matches_head = request.offer.offer.newest.as_ref().is_some_and(|newest| {
        request
            .heads
            .last()
            .is_some_and(|head| *head == newest.head)
    });
    if !ends_at_to || !matches_head {
        state.receiver.request = None;
        return vec![Action::ChainInvalid { peer }];
    }
    advance_walk(request, &mut state.receiver.cursor, peer)
}

/// The walk past the rows the request's build does not apply. Each such row
/// stores its cursor alone, and the walk stops at the first row the build
/// applies, which waits for its body.
fn advance_walk<P: Clone>(request: &mut Request, cursor: &mut Frontier, peer: P) -> Vec<Action<P>> {
    let mut actions = Vec::new();
    while let Some(row) = request.rows.get(request.walk).cloned() {
        if captured_under(&request.offer.offer.builds, row.major, &request.build) {
            break;
        }
        let next = Frontier {
            seq: row.seq,
            head: request.heads[request.walk],
        };
        request.walk += 1;
        *cursor = next.clone();
        actions.push(Action::Skip {
            peer: peer.clone(),
            row,
            cursor: next,
        });
    }
    actions
}

/// The receiver's answer to a body, the outstanding request's waiting row
/// alone. Its digest must match the row's, and the row applies with its
/// cursor before the walk continues.
fn handle_body<P: Clone>(
    state: &mut Peer,
    peer: P,
    request_id: u64,
    seq: u64,
    changeset: Vec<u8>,
) -> Vec<Action<P>> {
    let Some(request) = state.receiver.request.as_ref() else {
        return Vec::new();
    };
    if request.id != request_id {
        return Vec::new();
    }
    let waiting = request.complete
        && request.walk < request.rows.len()
        && request.rows[request.walk].seq == seq;
    if !waiting {
        state.receiver.request = None;
        return vec![Action::ChainInvalid { peer }];
    }
    let digest: [u8; 32] = Sha256::digest(&changeset).into();
    if digest != request.rows[request.walk].digest {
        state.receiver.request = None;
        return vec![Action::ChainInvalid { peer }];
    }
    let (row, cursor) = (
        request.rows[request.walk].clone(),
        Frontier {
            seq,
            head: request.heads[request.walk],
        },
    );
    let request = state
        .receiver
        .request
        .as_mut()
        .expect("the row was validated against the outstanding request");
    request.walk += 1;
    state.receiver.cursor = cursor.clone();
    let mut actions = vec![Action::Apply {
        peer: peer.clone(),
        row,
        body: changeset,
        cursor,
    }];
    actions.extend(advance_walk(request, &mut state.receiver.cursor, peer));
    actions
}

/// The receiver's answer to the end of the outstanding request. A
/// `Complete` with rows left aborts with `PeerChainInvalid`. Otherwise the
/// request drops, the backoff resets on a completed range, and the kept offer
/// is acted on when there is one.
fn handle_end<P: Clone>(
    state: &mut Peer,
    peer: P,
    build: Option<&(Major, SchemaVersion)>,
    next_id: &mut u64,
    request_id: u64,
    outcome: &EndOutcome,
    now: Instant,
) -> Vec<Action<P>> {
    let Some(request) = state.receiver.request.take() else {
        return Vec::new();
    };
    if request.id != request_id {
        state.receiver.request = Some(request);
        return Vec::new();
    }
    let kept = request.kept;
    let processed = request.complete && request.walk == request.rows.len();
    match outcome {
        EndOutcome::Complete => {
            if !processed {
                return vec![Action::ChainInvalid { peer }];
            }
            state.receiver.backoff = RETRY_FIRST;
            state.receiver.retry_at = None;
            match kept {
                Some(kept) => {
                    act_on_valid_offer(&mut state.receiver, build, next_id, peer, kept, now)
                }
                None => Vec::new(),
            }
        }
        EndOutcome::Busy | EndOutcome::NotAvailable => match kept {
            Some(kept) => act_on_valid_offer(&mut state.receiver, build, next_id, peer, kept, now),
            None => Vec::new(),
        },
    }
}

/// The chain hash through the rows accepted so far, `previous` when none.
impl Request {
    fn head(&self) -> [u8; 32] {
        self.heads.last().copied().unwrap_or(self.previous)
    }
}

#[cfg(test)]
mod tests;
