//! The device-to-device exchange's wire vocabulary (R77), the types every
//! exchange frame and the server's adjudication answer carry.

use serde::{Deserialize, Serialize};

use crate::messages::{MutationRejectReason, Provenance, WriterRef};
use crate::schema::{Major, SchemaVersion};

/// A chain frontier, the sequence a device has settled and the hash at it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier {
    /// The settled sequence.
    pub seq: u64,
    /// The hash of the change at that sequence.
    pub head: [u8; 32],
}

/// The writer and sequence an entry was first written under, as it crosses
/// to a peer.
///
/// A [`Provenance`] also carries a number writer's secret, which names that
/// writer to the server. A peer must never hold it, so the exchange carries
/// this type, which has no place for one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Origin {
    /// The writer the entry was first written under.
    pub writer: WriterRef,
    /// Its sequence there.
    pub seq: u64,
}

/// The writer and sequence alone, the secret left behind.
impl From<Provenance> for Origin {
    fn from(provenance: Provenance) -> Self {
        Self {
            writer: provenance.writer,
            seq: provenance.seq,
        }
    }
}

/// One row of a manifest, a sequence the answering device carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRow {
    /// The sequence the row covers.
    pub seq: u64,
    /// The schema major the entry was captured under.
    pub major: Major,
    /// The entry's original writer, when it carries one.
    pub origin: Option<Origin>,
    /// The hash of the change at that sequence.
    pub digest: [u8; 32],
}

/// Why a settled sequence is pulled back from the peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetractCause {
    /// The entry was refused, for the mutation's own reason.
    Rejected(MutationRejectReason),
    /// Two devices settled different changes at the sequence.
    Conflict,
    /// The entry was never settled anywhere.
    Abandoned,
}

/// A settled sequence the offering device is pulling back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retraction {
    /// The sequence being pulled back.
    pub seq: u64,
    /// Why it is being pulled back.
    pub cause: RetractCause,
}

/// How a manifest request ended on the answering device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndOutcome {
    /// The range ran to its end and its bodies followed in order.
    Complete,
    /// The answering device is busy and will not serve the request now.
    Busy,
    /// The answering device no longer holds the range, because the server
    /// answered part of it or its writer retired, so the requester waits
    /// for its next offer.
    NotAvailable,
}

/// What a device states about its own writer's chain to a linked peer, the
/// builds its entries were captured under, its settled and newest
/// frontiers and its retraction log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offer {
    /// The author's builds, as `(major, schema version)` pairs.
    pub builds: Vec<(Major, SchemaVersion)>,
    /// The author's acked frontier, `None` when it has settled nothing.
    pub settled: Option<Frontier>,
    /// The author's newest entry, `None` when it holds no queue.
    pub newest: Option<Frontier>,
    /// The author's retraction log.
    pub retractions: Vec<Retraction>,
}

impl Offer {
    /// The higher of the offer's signed `settled` and `newest` frontiers
    /// (R78 decision 52), the head the courier's chain is verified to and
    /// the bound its range runs to, `None` when the offer carries neither.
    #[must_use]
    pub fn frontier(&self) -> Option<&Frontier> {
        self.newest
            .as_ref()
            .filter(|newest| {
                self.settled
                    .as_ref()
                    .is_none_or(|settled| newest.seq > settled.seq)
            })
            .or(self.settled.as_ref())
    }
}

/// An [`Offer`] with the device key's signature over
/// [`offer_message`](crate::chain::offer_message), which rebuilds the
/// signed bytes from the fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedOffer {
    /// What the author states.
    pub offer: Offer,
    /// The device key's signature over it.
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}

/// The maximum rows in a manifest frame, provisional ask or courier audit observation.
pub const MAX_MANIFEST_ROWS: usize = 8192;

/// How the server ruled on one asked sequence of a verified provisional ask
/// (R77 decision 11), the writer row's `frontier` being `f`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// A record of the sequence stands with outcome `Applied`.
    Applied,
    /// No record of the sequence stands at or below the frontier.
    Settled,
    /// A record of the sequence stands with a refusal, the cause the
    /// receiver retracts on.
    Refused(RetractCause),
    /// No record of the sequence stands above the frontier of a writer that
    /// can no longer deliver, a retired one or one whose enrolment is
    /// revoked, absent or past its expiry plus tolerance.
    Abandoned,
    /// No record of the sequence stands above the frontier of a writer that
    /// can still deliver, so the receiver waits for the next connection.
    Pending,
}

/// Why the server refused a courier range before settling any of it (R78).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CourierRefusal {
    /// The deployment does not accept courier ranges.
    Disabled,
    /// The connection bound no device writer, so it is no certified bearer.
    Uncertified,
    /// The offer does not decode, the rows exceed [`MAX_MANIFEST_ROWS`],
    /// start at sequence 0, skip or repeat a sequence or run past the offer's
    /// newest, or the range declares more bodies than rows.
    Malformed,
    /// The author's enrolment is absent, revoked, or past its certificate's
    /// expiry plus `DEVICE_SKEW`.
    NotEnrolled,
    /// The author's account is banned, so the entries stay deliverable once
    /// the ban lifts.
    AuthorBanned,
    /// The author's writer is retired.
    WriterRetired,
    /// The offer's signature does not verify against the author's key, or
    /// the rows above the writer's frontier do not chain from its head to
    /// the offer's frontier.
    Unverified,
    /// The rows above the writer's frontier do not start right after it.
    SequenceGap {
        /// The sequence after the writer's frontier.
        expected: u64,
        /// The first sequence the range carries above the frontier.
        got: u64,
    },
}

/// How the server answered one row of a courier range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CourierEntryOutcome {
    /// The sequence is settled, by an earlier path or by this range, with
    /// the verdict a provisional ask would answer, never
    /// [`Verdict::Pending`] or [`Verdict::Abandoned`].
    Ruled(Verdict),
    /// The row carries no body, or an original only its number writer can
    /// settle, so it and every row after it wait for the author's own
    /// device.
    NotCouriered,
    /// Nothing settled at the row, so the range is offered again from it
    /// after the wait.
    Retry {
        /// How long to wait before offering again, 0 when the server names
        /// no wait.
        retry_after_ms: u64,
    },
}

/// The server's answer to one courier range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CourierOutcome {
    /// Nothing settled.
    Refused(CourierRefusal),
    /// One outcome per row answered, in sequence order. The list stops after
    /// a [`CourierEntryOutcome::NotCouriered`] or a
    /// [`CourierEntryOutcome::Retry`], and after a sequence the server holds
    /// under another entry, ruled refused `Forked`, whose chain the rest of
    /// the range no longer continues.
    Entries(Vec<(u64, CourierEntryOutcome)>),
}

#[cfg(test)]
mod tests {
    use super::{Frontier, Offer};

    fn frontier(seq: u64, head: u8) -> Frontier {
        Frontier {
            seq,
            head: [head; 32],
        }
    }

    fn offer(settled: Option<Frontier>, newest: Option<Frontier>) -> Offer {
        Offer {
            builds: Vec::new(),
            settled,
            newest,
            retractions: Vec::new(),
        }
    }

    /// The frontier is the higher of the two signed frontiers, the newest
    /// while it runs ahead, the settled when it does not, and neither when
    /// the offer carries neither.
    #[test]
    fn the_frontier_is_the_higher_signed_one() {
        assert!(offer(None, None).frontier().is_none());
        assert_eq!(
            offer(None, Some(frontier(4, 1))).frontier(),
            Some(&frontier(4, 1))
        );
        assert_eq!(
            offer(Some(frontier(4, 2)), None).frontier(),
            Some(&frontier(4, 2))
        );
        assert_eq!(
            offer(Some(frontier(2, 1)), Some(frontier(4, 2))).frontier(),
            Some(&frontier(4, 2)),
            "the newest runs ahead"
        );
        assert_eq!(
            offer(Some(frontier(4, 1)), Some(frontier(2, 2))).frontier(),
            Some(&frontier(4, 1)),
            "the settled runs past the newest"
        );
    }
}
