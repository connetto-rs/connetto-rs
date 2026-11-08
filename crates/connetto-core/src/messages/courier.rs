//! A courier relaying another author's signed entries to the server (R78
//! decisions 5 and 28 to 32).
//!
//! The courier opens a range on the control plane with the author's stored
//! offer, its manifest rows and the number of bodies that follow, sends that
//! many [`CourierBody`](crate::messages::bulk::CourierBody) frames on the bulk
//! plane in sequence order, and the server answers once, after the last
//! counted body. One range is open per connection at a time.

use serde::{Deserialize, Serialize};

use crate::exchange::{CourierOutcome, ManifestRow};

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
    /// it holds to the offer's newest.
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
