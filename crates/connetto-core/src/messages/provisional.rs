//! The receiver's question about an author's provisional rows and the
//! server's ruling (R77 decision 11).
//!
//! A receiver that applied an author's entries over a link holds them
//! provisionally, and the author's frontier and retraction log reach it only
//! over a link. On every server connection the receiver asks, per author it
//! holds provisional rows from, how the server stood to the author's
//! sequences, `offer` being the author's last verified signed offer, whose
//! signature is the right to ask. The server rules each sequence from its
//! settled records and answers one [`ProvisionalVerdicts`] per ask.

use serde::{Deserialize, Serialize};

use crate::exchange::Verdict;

/// The one detail text an ask the server cannot honour carries.
///
/// An ask over the bound, an offer that does not decode and a signature that
/// does not verify read identically here, so a caller cannot learn which one
/// stood. The cause goes to the structured log instead.
pub const PROVISIONAL_ASK_REFUSED: &str = "provisional ask refused";

/// The receiver's ask about one author's sequences, sent on every server
/// connection for each author its provisional rows name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionalStatus {
    /// The id the answer quotes.
    pub request_id: String,
    /// The author's device key id, the writer its rows were settled under.
    pub author: [u8; 32],
    /// The author's last verified signed offer, the cursor's stored bytes,
    /// the `MessagePack` of a [`SignedOffer`](crate::exchange::SignedOffer).
    pub offer: Vec<u8>,
    /// The sequences asked about, at most one manifest frame's rows.
    pub seqs: Vec<u64>,
}

/// The server's ruling on one ask, one verdict per asked sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionalVerdicts {
    /// The id of the ask the ruling answers.
    pub request_id: String,
    /// The sequences asked and their rulings, in the ask's order.
    pub verdicts: Vec<(u64, Verdict)>,
}
