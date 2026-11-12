//! The media messages and the per-peer links that carry them (R79).
//!
//! [`MediaLinks`] keeps one record per linked peer, the receiver's
//! situations (Q0 to Q3) and the holder's (H0 to H2) of R79's table "Every
//! event in every situation", the holder report each peer last sent
//! (decision 13), and the rate this device measured on its own fetches from
//! each peer. It reads no store, opens no socket and starts no timer. The
//! driver feeds it link events, messages and monotonic instants, carries out
//! the [`MediaAction`]s it returns, judges each request with its gate and
//! checks every piece against the file id.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::hash::Hash;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::FRAME_BOUND;
use crate::message::{BoundsError, MAX_MANIFEST_ROWS};

/// The most encoded bytes one [`Media::Piece`] carries.
pub const MAX_PIECE_BYTES: usize = 1024 * 1024;

/// The longest byte range one [`Media::Fetch`] asks for, the largest chunk.
pub const MAX_FETCH_LEN: u64 = 16 * 1024 * 1024;

/// The peers a holder serves at once, each serve holding one decrypted chunk.
pub const MAX_SERVING: usize = 2;

/// The most entries one list of a holder report carries.
pub const MAX_REPORT_ENTRIES: usize = 8192;

/// The shortest fetch that samples a peer's rate, since a shorter one's time
/// is mostly the round trip.
pub const MIN_RATE_SAMPLE: u64 = 256 * 1024;

/// The age at which a rate sample weighs half as much as when it was taken.
pub const RATE_HALF_LIFE: Duration = Duration::from_secs(30);

/// One media message between two linked devices, carried on the link's
/// media queue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Media {
    /// A request for a file's manifest.
    Want {
        /// The id every reply quotes.
        request_id: u64,
        /// The file asked for.
        file_id: [u8; 32],
    },
    /// A run of the holder's manifest rows, `(chunk hash, length)` in file
    /// order, at most [`MAX_MANIFEST_ROWS`].
    Manifest {
        /// The id the request named.
        request_id: u64,
        /// The rows of this run.
        chunks: Vec<([u8; 32], u64)>,
        /// Whether this is the manifest's last run.
        last: bool,
    },
    /// A request for the verified-streaming encoding of `len` bytes of a
    /// file from `start`.
    Fetch {
        /// The id every reply quotes.
        request_id: u64,
        /// The file asked for.
        file_id: [u8; 32],
        /// The first byte asked for.
        start: u64,
        /// The bytes asked for, at most [`MAX_FETCH_LEN`].
        len: u64,
    },
    /// The next part of a fetch's encoding, at most [`MAX_PIECE_BYTES`].
    Piece {
        /// The id the request named.
        request_id: u64,
        /// The encoded bytes, cut anywhere in the encoding.
        #[serde(with = "serde_bytes")]
        encoded: Vec<u8>,
        /// Whether this is the fetch's last piece.
        last: bool,
    },
    /// The holder lacks the file or its gate refuses the requester, the two
    /// answered alike.
    Refused {
        /// The id the request named.
        request_id: u64,
    },
    /// The holder serves this peer or two others already.
    Busy {
        /// The id the request named.
        request_id: u64,
    },
    /// One run of a holder's full report, sent when the link comes up, its
    /// runs replacing the previous report once the last one arrives.
    Holdings {
        /// The bytes the sender has left for copies held for others.
        room: u64,
        /// The sender's measured rate from each linked device, by key id,
        /// in bytes per second.
        rates: Vec<([u8; 32], u64)>,
        /// Files the sender holds in custody.
        held: Vec<[u8; 32]>,
        /// Held files the sender wants another device to take over.
        leaving: Vec<[u8; 32]>,
        /// Whether this is the report's last run.
        last: bool,
    },
    /// A change to the sender's custody since its last report.
    Held {
        /// Files that entered custody.
        added: Vec<[u8; 32]>,
        /// Files that left custody.
        removed: Vec<[u8; 32]>,
        /// Held files the sender now wants another device to take over.
        leaving: Vec<[u8; 32]>,
    },
    /// The sender's room and rates, replacing the ones it reported before.
    Room {
        /// The bytes the sender has left for copies held for others.
        room: u64,
        /// The sender's measured rate from each linked device, by key id,
        /// in bytes per second.
        rates: Vec<([u8; 32], u64)>,
    },
}

impl Media {
    /// The request id the message names or quotes, `None` for a holder
    /// report.
    #[must_use]
    pub fn request_id(&self) -> Option<u64> {
        match self {
            Media::Want { request_id, .. }
            | Media::Manifest { request_id, .. }
            | Media::Fetch { request_id, .. }
            | Media::Piece { request_id, .. }
            | Media::Refused { request_id }
            | Media::Busy { request_id } => Some(*request_id),
            Media::Holdings { .. } | Media::Held { .. } | Media::Room { .. } => None,
        }
    }

    /// The request this message makes, `None` for a reply.
    #[must_use]
    pub fn ask(&self) -> Option<Ask> {
        match *self {
            Media::Want { file_id, .. } => Some(Ask::Want { file_id }),
            Media::Fetch {
                file_id,
                start,
                len,
                ..
            } => Some(Ask::Fetch {
                file_id,
                start,
                len,
            }),
            _ => None,
        }
    }

    /// The message name and article, for diagnostics that must not print a
    /// payload.
    #[must_use]
    pub fn named(&self) -> &'static str {
        match self {
            Media::Want { .. } => "a Media::Want",
            Media::Manifest { .. } => "a Media::Manifest",
            Media::Fetch { .. } => "a Media::Fetch",
            Media::Piece { .. } => "a Media::Piece",
            Media::Refused { .. } => "a Media::Refused",
            Media::Busy { .. } => "a Media::Busy",
            Media::Holdings { .. } => "a Media::Holdings",
            Media::Held { .. } => "a Media::Held",
            Media::Room { .. } => "a Media::Room",
        }
    }

    /// Whether the message keeps the wire's bounds.
    ///
    /// # Errors
    ///
    /// [`BoundsError::TooManyRows`] for a manifest run above
    /// [`MAX_MANIFEST_ROWS`], [`BoundsError::PieceTooLong`] for a piece above
    /// [`MAX_PIECE_BYTES`], [`BoundsError::RangeOutOfBounds`] for a fetch of
    /// no bytes, of more than [`MAX_FETCH_LEN`], or past the largest offset,
    /// and [`BoundsError::TooManyEntries`] for a report list above
    /// [`MAX_REPORT_ENTRIES`].
    pub fn within_bounds(&self) -> Result<(), BoundsError> {
        match self {
            Media::Manifest { chunks, .. } if chunks.len() > MAX_MANIFEST_ROWS => {
                Err(BoundsError::TooManyRows)
            }
            Media::Piece { encoded, .. } if encoded.len() > MAX_PIECE_BYTES => {
                Err(BoundsError::PieceTooLong)
            }
            Media::Fetch { start, len, .. }
                if *len == 0 || *len > MAX_FETCH_LEN || start.checked_add(*len).is_none() =>
            {
                Err(BoundsError::RangeOutOfBounds)
            }
            Media::Holdings {
                rates,
                held,
                leaving,
                ..
            } if [rates.len(), held.len(), leaving.len()]
                .into_iter()
                .any(|len| len > MAX_REPORT_ENTRIES) =>
            {
                Err(BoundsError::TooManyEntries)
            }
            Media::Held {
                added,
                removed,
                leaving,
            } if [added.len(), removed.len(), leaving.len()]
                .into_iter()
                .any(|len| len > MAX_REPORT_ENTRIES) =>
            {
                Err(BoundsError::TooManyEntries)
            }
            Media::Room { rates, .. } if rates.len() > MAX_REPORT_ENTRIES => {
                Err(BoundsError::TooManyEntries)
            }
            _ => Ok(()),
        }
    }
}

/// A request a receiver makes, before it has an id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ask {
    /// A file's manifest.
    Want {
        /// The file asked for.
        file_id: [u8; 32],
    },
    /// The encoding of `len` bytes of a file from `start`.
    Fetch {
        /// The file asked for.
        file_id: [u8; 32],
        /// The first byte asked for.
        start: u64,
        /// The bytes asked for.
        len: u64,
    },
}

impl Ask {
    fn message(&self, request_id: u64) -> Media {
        match *self {
            Ask::Want { file_id } => Media::Want {
                request_id,
                file_id,
            },
            Ask::Fetch {
                file_id,
                start,
                len,
            } => Media::Fetch {
                request_id,
                file_id,
                start,
                len,
            },
        }
    }
}

/// What one linked peer last reported of its custody, room and rates.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    room: u64,
    rates: BTreeMap<[u8; 32], u64>,
    held: BTreeSet<[u8; 32]>,
    leaving: BTreeSet<[u8; 32]>,
}

impl Report {
    /// The bytes the peer has left for copies held for others.
    #[must_use]
    pub fn room(&self) -> u64 {
        self.room
    }

    /// The peer's measured rate from the device with key id `key`, in bytes
    /// per second, `None` when the peer never fetched from it.
    #[must_use]
    pub fn rate_to(&self, key: &[u8; 32]) -> Option<u64> {
        self.rates.get(key).copied()
    }

    /// Whether the peer holds `file` in custody.
    #[must_use]
    pub fn holds(&self, file: &[u8; 32]) -> bool {
        self.held.contains(file)
    }

    /// Whether the peer wants another device to take `file` over.
    #[must_use]
    pub fn is_leaving(&self, file: &[u8; 32]) -> bool {
        self.leaving.contains(file)
    }

    fn replace_room(&mut self, room: u64, rates: Vec<([u8; 32], u64)>) {
        self.room = room;
        self.rates = rates.into_iter().collect();
    }

    fn change(&mut self, added: Vec<[u8; 32]>, removed: &[[u8; 32]], leaving: Vec<[u8; 32]>) {
        self.held.extend(added);
        for file in removed {
            self.held.remove(file);
            self.leaving.remove(file);
        }
        self.leaving
            .extend(leaving.into_iter().filter(|file| self.held.contains(file)));
    }
}

/// A peer's rate as this device measured it, smoothed over its samples.
#[derive(Clone, Copy, Debug)]
struct Rate {
    bytes_per_second: f64,
    at: Instant,
}

impl Rate {
    /// Fold in a sample taken at `at`, the older rate weighing half per
    /// [`RATE_HALF_LIFE`] of age.
    fn sampled(previous: Option<Self>, sample: f64, at: Instant) -> Self {
        let Some(previous) = previous else {
            return Self {
                bytes_per_second: sample,
                at,
            };
        };
        let age = at.saturating_duration_since(previous.at).as_secs_f64();
        let kept = 0.5_f64.powf(age / RATE_HALF_LIFE.as_secs_f64());
        Self {
            bytes_per_second: previous.bytes_per_second * kept + sample * (1.0 - kept),
            at,
        }
    }
}

/// Where a queued ask waits on a busy link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    /// A fetch the application waits on, ahead of every prefetch.
    OnDemand,
    /// A thumbnail fetched unasked, behind every on-demand ask.
    Prefetch,
}

/// What the links ask their driver to do, in the order to do it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MediaAction<P, T> {
    /// Send `message` on the link to `peer`.
    Send {
        /// The link's peer.
        peer: P,
        /// The message to send.
        message: Media,
    },
    /// A reply to the ask tagged `tag`. When `done`, the link waits for
    /// [`MediaLinks::proceed`] before it sends anything more.
    Reply {
        /// The peer that answered.
        peer: P,
        /// The ask's tag.
        tag: T,
        /// The reply as it crossed.
        message: Media,
        /// Whether the reply ends the request.
        done: bool,
    },
    /// The ask tagged `tag` saw no frame for [`FRAME_BOUND`], and the link
    /// waits for [`MediaLinks::proceed`].
    Lapsed {
        /// The silent peer.
        peer: P,
        /// The ask's tag.
        tag: T,
    },
    /// The ask tagged `tag` will never be answered, its peer unlinked.
    Lost {
        /// The peer the ask named.
        peer: P,
        /// The ask's tag.
        tag: T,
    },
    /// Judge `ask` from `peer` with the gate and stream the answer under
    /// `request_id`, then call [`MediaLinks::served`].
    Serve {
        /// The requesting peer.
        peer: P,
        /// The id every reply quotes.
        request_id: u64,
        /// The request.
        ask: Ask,
    },
    /// Stop the stream to `peer`, whose link fell.
    StopServing {
        /// The stream's peer.
        peer: P,
    },
    /// `peer`'s report changed, read with [`MediaLinks::report`].
    Reported {
        /// The reporting peer.
        peer: P,
    },
}

/// The receiver's side of one link.
enum Asking<T> {
    /// Q1, nothing outstanding.
    Idle,
    /// Q2 or Q3, one request outstanding. `sample` is when a fetch long
    /// enough to sample the rate was sent and how many bytes it asked for.
    Waiting {
        id: u64,
        tag: T,
        deadline: Instant,
        sample: Option<(Instant, u64)>,
    },
    /// The last request ended and the driver has not proceeded yet.
    Answered,
}

/// One linked peer, both roles at once.
struct Link<T> {
    asking: Asking<T>,
    on_demand: VecDeque<(Ask, T)>,
    prefetch: VecDeque<(Ask, T)>,
    serving: Option<u64>,
    /// The peer's last complete report.
    report: Option<Report>,
    /// A report whose last run has not arrived yet.
    incoming: Option<Report>,
}

/// The media side of every link of one device, generic over the peer
/// identity and the tag the driver gives each ask.
pub struct MediaLinks<P, T> {
    peers: IndexMap<P, Link<T>>,
    rates: HashMap<P, Rate>,
    next_id: u64,
}

impl<P, T> Default for MediaLinks<P, T> {
    fn default() -> Self {
        Self {
            peers: IndexMap::new(),
            rates: HashMap::new(),
            next_id: 0,
        }
    }
}

impl<P: Clone + Eq + Hash, T: Clone> MediaLinks<P, T> {
    /// No peer linked.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The linked peers, in the order their links came up, which is the
    /// order a receiver tries holders after the row's author.
    pub fn linked_peers(&self) -> impl Iterator<Item = &P> {
        self.peers.keys()
    }

    /// `peer` linked, behind every peer linked before it.
    pub fn linked(&mut self, peer: &P) {
        self.peers.shift_remove(peer);
        self.peers.insert(
            peer.clone(),
            Link {
                asking: Asking::Idle,
                on_demand: VecDeque::new(),
                prefetch: VecDeque::new(),
                serving: None,
                report: None,
                incoming: None,
            },
        );
    }

    /// `peer`'s link fell. Every ask on it is lost, the outstanding one first
    /// and the queued ones in order, and a stream to it stops.
    pub fn unlinked(&mut self, peer: &P) -> Vec<MediaAction<P, T>> {
        let Some(link) = self.peers.shift_remove(peer) else {
            return Vec::new();
        };
        let mut actions = Vec::new();
        if let Asking::Waiting { tag, .. } = link.asking {
            actions.push(MediaAction::Lost {
                peer: peer.clone(),
                tag,
            });
        }
        for (_, tag) in link.on_demand.into_iter().chain(link.prefetch) {
            actions.push(MediaAction::Lost {
                peer: peer.clone(),
                tag,
            });
        }
        if link.serving.is_some() {
            actions.push(MediaAction::StopServing { peer: peer.clone() });
        }
        actions
    }

    /// Ask `peer` for `ask`, tagged `tag`, sent at once on an idle link and
    /// queued by `priority` otherwise.
    pub fn ask(
        &mut self,
        peer: &P,
        ask: Ask,
        tag: T,
        priority: Priority,
        now: Instant,
    ) -> Vec<MediaAction<P, T>> {
        let Some(link) = self.peers.get_mut(peer) else {
            return vec![MediaAction::Lost {
                peer: peer.clone(),
                tag,
            }];
        };
        if matches!(link.asking, Asking::Idle) {
            return vec![Self::send(&mut self.next_id, peer, link, &ask, tag, now)];
        }
        match priority {
            Priority::OnDemand => link.on_demand.push_back((ask, tag)),
            Priority::Prefetch => link.prefetch.push_back((ask, tag)),
        }
        Vec::new()
    }

    /// The driver handled the reply, lapse or loss that ended `peer`'s last
    /// request. `continuation` is sent first, else the next on-demand ask,
    /// else the next prefetch, else the link turns idle.
    pub fn proceed(
        &mut self,
        peer: &P,
        continuation: Option<(Ask, T)>,
        now: Instant,
    ) -> Vec<MediaAction<P, T>> {
        let Some(link) = self.peers.get_mut(peer) else {
            return continuation
                .map(|(_, tag)| MediaAction::Lost {
                    peer: peer.clone(),
                    tag,
                })
                .into_iter()
                .collect();
        };
        if let Asking::Waiting { .. } = link.asking {
            if let Some(next) = continuation {
                link.on_demand.push_front(next);
            }
            return Vec::new();
        }
        let next = continuation
            .or_else(|| link.on_demand.pop_front())
            .or_else(|| link.prefetch.pop_front());
        if let Some((ask, tag)) = next {
            vec![Self::send(&mut self.next_id, peer, link, &ask, tag, now)]
        } else {
            link.asking = Asking::Idle;
            Vec::new()
        }
    }

    /// `message` crossed from `peer`.
    pub fn received(&mut self, peer: &P, message: Media, now: Instant) -> Vec<MediaAction<P, T>> {
        let serving = self.serving_count();
        let Some(link) = self.peers.get_mut(peer) else {
            return Vec::new();
        };
        let Some(request_id) = message.request_id() else {
            return if Self::take_report(link, message) {
                vec![MediaAction::Reported { peer: peer.clone() }]
            } else {
                Vec::new()
            };
        };
        if let Some(ask) = message.ask() {
            if link.serving.is_some() || serving >= MAX_SERVING {
                return vec![MediaAction::Send {
                    peer: peer.clone(),
                    message: Media::Busy { request_id },
                }];
            }
            link.serving = Some(request_id);
            return vec![MediaAction::Serve {
                peer: peer.clone(),
                request_id,
                ask,
            }];
        }
        let Asking::Waiting {
            id,
            tag,
            deadline,
            sample,
        } = &mut link.asking
        else {
            return Vec::new();
        };
        if *id != request_id {
            return Vec::new();
        }
        let done = match &message {
            Media::Manifest { last, .. } | Media::Piece { last, .. } => *last,
            _ => true,
        };
        // Only a fetch answered to its last piece measures the link, since a
        // refusal or a lapse moved no bytes.
        let measured = match (&message, *sample) {
            (Media::Piece { last: true, .. }, Some((sent, len))) => Some((sent, len)),
            _ => None,
        };
        let tag = tag.clone();
        if done {
            link.asking = Asking::Answered;
        } else {
            *deadline = now + FRAME_BOUND;
        }
        if let Some((sent, len)) = measured {
            let took = now.saturating_duration_since(sent).as_secs_f64();
            if took > 0.0 {
                // A sampled fetch is at most MAX_FETCH_LEN, far inside f64's
                // exact integers.
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "len is at most MAX_FETCH_LEN, exact in an f64"
                )]
                let sample = len as f64 / took;
                let rate = Rate::sampled(self.rates.get(peer).copied(), sample, now);
                self.rates.insert(peer.clone(), rate);
            }
        }
        vec![MediaAction::Reply {
            peer: peer.clone(),
            tag,
            message,
            done,
        }]
    }

    /// `peer`'s last complete report, `None` before its first.
    #[must_use]
    pub fn report(&self, peer: &P) -> Option<&Report> {
        self.peers.get(peer)?.report.as_ref()
    }

    /// The rate this device measured on its fetches from `peer`, in bytes
    /// per second, `None` when it never fetched enough from it.
    #[must_use]
    pub fn rate(&self, peer: &P) -> Option<u64> {
        let rate = self.rates.get(peer)?.bytes_per_second;
        debug_assert!(
            rate.is_finite() && rate >= 0.0,
            "a rate is a positive average of positive samples"
        );
        // Rounding to whole bytes per second is the intended loss, and a
        // rate above u64::MAX saturates, which no link reaches.
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "rounded, finite and non-negative as asserted"
        )]
        Some(rate.round() as u64)
    }

    /// Fold a holder report into `link`, answering whether its complete
    /// report changed.
    fn take_report(link: &mut Link<T>, message: Media) -> bool {
        match message {
            Media::Holdings {
                room,
                rates,
                held,
                leaving,
                last,
            } => {
                let incoming = link.incoming.get_or_insert_with(Report::default);
                incoming.room = room;
                incoming.rates.extend(rates);
                incoming.held.extend(held);
                incoming.leaving.extend(leaving);
                if !last {
                    return false;
                }
                let mut report = link.incoming.take().unwrap_or_default();
                let held = &report.held;
                report.leaving.retain(|file| held.contains(file));
                link.report = Some(report);
                true
            }
            Media::Held {
                added,
                removed,
                leaving,
            } => {
                if let Some(incoming) = link.incoming.as_mut() {
                    incoming.change(added, &removed, leaving);
                    false
                } else if let Some(report) = link.report.as_mut() {
                    report.change(added, &removed, leaving);
                    true
                } else {
                    false
                }
            }
            Media::Room { room, rates } => {
                if let Some(incoming) = link.incoming.as_mut() {
                    incoming.replace_room(room, rates);
                    false
                } else if let Some(report) = link.report.as_mut() {
                    report.replace_room(room, rates);
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// The driver finished streaming `request_id` to `peer`. A stale id
    /// changes nothing.
    pub fn served(&mut self, peer: &P, request_id: u64) {
        if let Some(link) = self.peers.get_mut(peer)
            && link.serving == Some(request_id)
        {
            link.serving = None;
        }
    }

    /// The earliest instant an outstanding request lapses.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.peers
            .values()
            .filter_map(|link| match link.asking {
                Asking::Waiting { deadline, .. } => Some(deadline),
                _ => None,
            })
            .min()
    }

    /// Lapse every request whose bound passed by `now`.
    pub fn timeout(&mut self, now: Instant) -> Vec<MediaAction<P, T>> {
        let mut actions = Vec::new();
        for (peer, link) in &mut self.peers {
            if let Asking::Waiting { tag, deadline, .. } = &link.asking
                && *deadline <= now
            {
                actions.push(MediaAction::Lapsed {
                    peer: peer.clone(),
                    tag: tag.clone(),
                });
                link.asking = Asking::Answered;
            }
        }
        actions
    }

    fn serving_count(&self) -> usize {
        self.peers
            .values()
            .filter(|link| link.serving.is_some())
            .count()
    }

    fn send(
        next_id: &mut u64,
        peer: &P,
        link: &mut Link<T>,
        ask: &Ask,
        tag: T,
        now: Instant,
    ) -> MediaAction<P, T> {
        let id = *next_id;
        *next_id = next_id.wrapping_add(1);
        let sample = match *ask {
            Ask::Fetch { len, .. } if len >= MIN_RATE_SAMPLE => Some((now, len)),
            _ => None,
        };
        link.asking = Asking::Waiting {
            id,
            tag,
            deadline: now + FRAME_BOUND,
            sample,
        };
        MediaAction::Send {
            peer: peer.clone(),
            message: ask.message(id),
        }
    }
}

#[cfg(test)]
mod tests;
