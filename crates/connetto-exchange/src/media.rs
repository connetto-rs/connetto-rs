//! The media messages and the per-peer links that carry them (R79).
//!
//! [`MediaLinks`] keeps one record per linked peer, the receiver's
//! situations (Q0 to Q3) and the holder's (H0 to H2) of R79's table "Every
//! event in every situation". It reads no store, opens no socket and starts
//! no timer. The driver feeds it link events, messages and monotonic
//! instants, carries out the [`MediaAction`]s it returns, judges each request
//! with its gate and checks every piece against the file id.

use std::collections::VecDeque;
use std::hash::Hash;
use std::time::Instant;

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
}

impl Media {
    /// The request id the message names or quotes.
    #[must_use]
    pub fn request_id(&self) -> u64 {
        match self {
            Media::Want { request_id, .. }
            | Media::Manifest { request_id, .. }
            | Media::Fetch { request_id, .. }
            | Media::Piece { request_id, .. }
            | Media::Refused { request_id }
            | Media::Busy { request_id } => *request_id,
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
        }
    }

    /// Whether the message keeps the wire's bounds.
    ///
    /// # Errors
    ///
    /// [`BoundsError::TooManyRows`] for a manifest run above
    /// [`MAX_MANIFEST_ROWS`], [`BoundsError::PieceTooLong`] for a piece above
    /// [`MAX_PIECE_BYTES`], and [`BoundsError::RangeOutOfBounds`] for a fetch
    /// of no bytes, of more than [`MAX_FETCH_LEN`], or past the largest offset.
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
}

/// The receiver's side of one link.
enum Asking<T> {
    /// Q1, nothing outstanding.
    Idle,
    /// Q2 or Q3, one request outstanding.
    Waiting { id: u64, tag: T, deadline: Instant },
    /// The last request ended and the driver has not proceeded yet.
    Answered,
}

/// One linked peer, both roles at once.
struct Link<T> {
    asking: Asking<T>,
    on_demand: VecDeque<(Ask, T)>,
    prefetch: VecDeque<(Ask, T)>,
    serving: Option<u64>,
}

/// The media side of every link of one device, generic over the peer
/// identity and the tag the driver gives each ask.
pub struct MediaLinks<P, T> {
    peers: IndexMap<P, Link<T>>,
    next_id: u64,
}

impl<P, T> Default for MediaLinks<P, T> {
    fn default() -> Self {
        Self {
            peers: IndexMap::new(),
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
        let request_id = message.request_id();
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
        let Asking::Waiting { id, tag, deadline } = &mut link.asking else {
            return Vec::new();
        };
        if *id != request_id {
            return Vec::new();
        }
        let done = match &message {
            Media::Manifest { last, .. } | Media::Piece { last, .. } => *last,
            _ => true,
        };
        let tag = tag.clone();
        if done {
            link.asking = Asking::Answered;
        } else {
            *deadline = now + FRAME_BOUND;
        }
        vec![MediaAction::Reply {
            peer: peer.clone(),
            tag,
            message,
            done,
        }]
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
        link.asking = Asking::Waiting {
            id,
            tag,
            deadline: now + FRAME_BOUND,
        };
        MediaAction::Send {
            peer: peer.clone(),
            message: ask.message(id),
        }
    }
}

#[cfg(test)]
mod tests;
