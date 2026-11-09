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
