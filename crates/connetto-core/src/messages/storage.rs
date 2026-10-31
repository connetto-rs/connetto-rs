//! What an account's data takes on the device, and the relay pair a tab asks
//! its worker for it with (R11 slice 7).

use serde::{Deserialize, Serialize};

/// The room one SQLite database takes, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreBytes {
    /// The database's size, its pages times the page size.
    pub file: u64,
    /// The part of `file` on the freelist, which a trim returns to the
    /// filesystem. What the rows take is `file - free`.
    pub free: u64,
}

/// The device's shared store, which every identity on it reads its public
/// tables from (R11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreUse {
    /// The room the store takes.
    pub bytes: StoreBytes,
    /// How many identities use the store, this one included. Which ones is
    /// never said.
    pub identities: u64,
}

/// What an account's data takes on the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageUse {
    /// The account's own replica.
    pub replica: StoreBytes,
    /// The shared store, absent when the build shares no table or turns the
    /// store off.
    pub shared: Option<SharedStoreUse>,
}

/// A tab asks its relay's worker what the account's data takes.
///
/// Relay only, like [`TabShareKeyChange`](super::TabShareKeyChange). A real
/// server neither sends nor answers it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabStorageUse {
    /// Tab-chosen correlation token, echoed by the answer.
    pub request_id: String,
}

/// How a worker answered a [`TabStorageUse`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TabStorageOutcome {
    /// What the worker's replica and the shared store take.
    Use(StorageUse),
    /// The worker's gate is locked, so nothing was read.
    Locked,
    /// The worker's replica refused the read.
    Refused(String),
}

/// A worker answers a [`TabStorageUse`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabStorageAnswer {
    /// Correlation token from the request this answers.
    pub request_id: String,
    /// The answer.
    pub outcome: TabStorageOutcome,
}
