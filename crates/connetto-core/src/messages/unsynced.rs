//! The local work that has not reached the server, as an application follows
//! it live, and the relay pair a tab dismisses a refusal with.

use serde::{Deserialize, Serialize};

use super::{PriorRefusal, WriterRef};
use crate::schema::Major;

/// A primary-key column value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KeyValue {
    /// SQL NULL.
    Null,
    /// Integer key column.
    Int(i64),
    /// Real key column.
    Real(f64),
    /// Text key column.
    Text(String),
    /// Blob key column.
    Blob(Vec<u8>),
}

/// One queued write awaiting its final answer, named by the writer and the
/// sequence it is the writer's, with the application schema major it was
/// captured under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsyncedWrite {
    /// The writer the entry is queued under.
    pub writer: WriterRef,
    /// The entry's sequence in that writer's queue.
    pub seq: u64,
    /// The major the write was captured under, absent for a build declaring
    /// none. Only that major's server can apply it (R31).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub major: Option<Major>,
}

/// What a write did to one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowOp {
    /// The row was inserted.
    Insert,
    /// The row was updated.
    Update,
    /// The row was deleted.
    Delete,
}

/// One row a write touched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WrittenRow {
    /// The table, named as the application writes it.
    pub table: String,
    /// The row's primary-key column values, in key order.
    pub key: Vec<KeyValue>,
    /// What the write did to the row.
    pub op: RowOp,
}

/// Where a write stands on its way to the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteState {
    /// Kept on the device and not sent on the current connection.
    Queued,
    /// Sent on the current connection, its answer not in yet.
    Sent,
    /// The server could not settle it yet, so it is sent again later.
    Deferred,
    /// The server refused it and the device rolled its rows back. It stays
    /// until the application dismisses it.
    Refused(PriorRefusal),
}

/// A write that has not reached the server, or one the server refused.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackedWrite {
    /// The write.
    pub write: UnsyncedWrite,
    /// Where it stands.
    pub state: WriteState,
    /// The rows it touched, in the order it touched them.
    pub rows: Vec<WrittenRow>,
}

/// Where a staged file stands on its way to the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileState {
    /// Waiting to upload.
    Queued,
    /// The upload was refused for the reason given, and waits for a retry.
    Refused(String),
}

/// A staged file whose bytes have not reached the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsyncedFile {
    /// The file's identity, in hex.
    pub file_id: String,
    /// The major an older build held the file for, absent for this build's
    /// own uploads (R31).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub major: Option<Major>,
    /// Where it stands.
    pub state: FileState,
}

/// Every write and file of this device that has not reached the server, and
/// every refused write the application has not dismissed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UnsyncedWork {
    /// The writes, oldest first per writer.
    pub writes: Vec<TrackedWrite>,
    /// The staged files, in identity order.
    pub files: Vec<UnsyncedFile>,
}

/// A tab asks its relay's worker to dismiss a refused write.
///
/// Relay only, like [`TabStorageUse`](super::TabStorageUse). A real server
/// neither sends nor answers it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabDismissRefusal {
    /// Tab-chosen correlation token, echoed by the answer.
    pub request_id: String,
    /// The refused write's writer.
    pub writer: WriterRef,
    /// The refused write's sequence.
    pub seq: u64,
}

/// How a worker answered a [`TabDismissRefusal`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TabDismissOutcome {
    /// The refusal is dismissed, or there was none to dismiss.
    Dismissed,
    /// The worker's gate is locked, so nothing was written.
    Locked,
    /// The worker's replica refused the write.
    Refused(String),
}

/// A worker answers a [`TabDismissRefusal`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabDismissAnswer {
    /// Correlation token from the request this answers.
    pub request_id: String,
    /// The answer.
    pub outcome: TabDismissOutcome,
}
