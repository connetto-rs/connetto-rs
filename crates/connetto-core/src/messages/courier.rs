//! A courier relaying another author's signed entries to the server (R78
//! decisions 5 and 28 to 32).
//!
//! The courier opens a range on the control plane with the author's stored
//! offer, its manifest rows and the number of bodies that follow, sends that
//! many [`CourierBody`](crate::messages::bulk::CourierBody) frames on the bulk
//! plane in sequence order, and the server answers once, after the last
//! counted body. One range is open per connection at a time. The author's
//! next sessions hear what was couriered in [`CourierNotice`] frames, each
//! acked by a [`CourierNoticeAck`] that lets the server delete its rows.

use serde::{Deserialize, Serialize};

use crate::exchange::{CourierOutcome, ManifestRow, Verdict};
use crate::messages::WriterRef;

/// A courier range for one author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierRange {
    /// The id the bodies and the answer quote.
    pub request_id: String,
    /// The author's device key id.
    pub author: [u8; 32],
    /// The author's last verified signed offer, the `MessagePack` of a
    /// [`SignedOffer`](crate::exchange::SignedOffer).
    #[serde(with = "serde_bytes")]
    pub offer: Vec<u8>,
    /// The manifest rows the courier holds, in sequence order, from the first
    /// it holds to the offer's frontier.
    pub rows: Vec<ManifestRow>,
    /// How many bodies follow on the bulk plane.
    pub bodies: u32,
}

/// The server's one answer to a courier range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierAnswer {
    /// The id of the range answered.
    pub request_id: String,
    /// The refusal or the outcome of each row answered.
    pub outcome: CourierOutcome,
}

/// One entry a courier settled for the session's account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierNoticeRow {
    /// The courier's account, as the deployment renders its user id.
    pub courier: String,
    /// The author's writer the entry settled under.
    pub writer: WriterRef,
    /// The entry's sequence under that writer.
    pub seq: u64,
    /// How the entry settled, `Applied` or `Refused` with its cause.
    pub verdict: Verdict,
}

/// The entries couriers settled for the session's account since it last
/// acked, sent after the handshake (R78 decisions 12, 34 and 39).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierNotice {
    /// The id the ack quotes.
    pub notice_id: String,
    /// At most [`MAX_MANIFEST_ROWS`](crate::exchange::MAX_MANIFEST_ROWS)
    /// rows.
    pub notices: Vec<CourierNoticeRow>,
}

/// The client's receipt of one [`CourierNotice`], which deletes its rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierNoticeAck {
    /// The id of the notice acked.
    pub notice_id: String,
}

/// Observes committed courier audit receipts visible to the authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierAuditRequest {
    /// A nonempty identifier of at most [`Self::MAX_REQUEST_ID_BYTES`] UTF-8 bytes.
    pub request_id: String,
    /// The author's device key identifier.
    pub author: [u8; 32],
    /// Positive `BIGINT` sequences in strictly increasing order, at most [`crate::exchange::MAX_MANIFEST_ROWS`].
    pub seqs: Vec<u64>,
    /// Positive milliseconds remaining on the caller's original monotonic deadline.
    pub remaining_ms: u64,
}

impl CourierAuditRequest {
    /// The maximum retained correlation identifier size in UTF-8 bytes.
    pub const MAX_REQUEST_ID_BYTES: usize = 128;

    /// Whether the request satisfies the wire bounds before authorization.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        !self.request_id.is_empty()
            && self.request_id.len() <= Self::MAX_REQUEST_ID_BYTES
            && self.remaining_ms > 0
            && self.seqs.len() <= crate::exchange::MAX_MANIFEST_ROWS
            && self.seqs.first().is_some_and(|seq| *seq > 0)
            && self
                .seqs
                .last()
                .is_some_and(|seq| i64::try_from(*seq).is_ok())
            && self.seqs.windows(2).all(|pair| pair[0] < pair[1])
    }
}

/// The terminal outcome of a courier audit observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CourierAuditOutcome {
    /// Every requested receipt was committed and visible to the caller.
    Confirmed,
    /// The observation expired without confirming every receipt.
    Expired,
    /// The request was refused without disclosing audit evidence.
    Refused,
    /// An observation failure prevented confirmation.
    Failed,
}

/// The correlated answer to a courier audit observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierAuditAnswer {
    /// The identifier of the observation answered.
    pub request_id: String,
    /// Only `Confirmed` witnesses persistence, other outcomes prove no absence.
    pub outcome: CourierAuditOutcome,
}

/// Cancels a courier audit observation without acknowledging persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CourierAuditCancel {
    /// The identifier of the observation to release.
    pub request_id: String,
}
