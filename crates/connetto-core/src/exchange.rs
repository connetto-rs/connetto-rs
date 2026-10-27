//! The device-to-device exchange's wire vocabulary (R77), the types every
//! exchange frame and the server's adjudication answer carry.

use serde::{Deserialize, Serialize};

use crate::messages::{MutationRejectReason, Provenance, WriterRef};
use crate::schema::Major;

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
}
