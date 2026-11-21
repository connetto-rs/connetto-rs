//! Server-computed answer delivery.
//!
//! `subql` maintains aggregates and computed row sets server-side (Q5.1). The
//! server tells the client the declared type of every column of an answer
//! once per registration with [`AggregateColumns`], then moves the answer with
//! [`AggregateUpdate`]s. Every cell is a [`SqliteValue`] in the form the
//! replica stores the column it comes from, so the client rests it as it rests
//! rows and decodes it through the same diesel `FromSql` impls (R117).

use serde::{Deserialize, Serialize};

use super::subscription::SqliteValue;

/// The declared type of one column of a computed answer, which a client
/// declares the column of its `STRICT` resting table with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnType {
    /// `INTEGER`.
    Integer,
    /// `REAL`.
    Real,
    /// `TEXT`.
    Text,
    /// `BLOB`.
    Blob,
    /// `ANY`, an output no catalog column stands behind, whose cells keep
    /// whatever storage class their value family is stored as.
    Any,
}

impl ColumnType {
    /// The SQLite type name a `STRICT` table declares the column with.
    #[must_use]
    pub const fn sql_name(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
            Self::Any => "ANY",
        }
    }
}

/// The columns of one computed subscription's answer, in select order.
///
/// Sent once at each registration, ahead of the subscription's first
/// [`AggregateUpdate`] and also when its answer starts empty, so the client
/// can create the table the answer rests in before any value arrives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateColumns {
    /// Which subscription the columns describe.
    pub sub_id: String,
    /// One declared type per select-list item, in select order.
    pub columns: Vec<ColumnType>,
}

/// One movement of a computed subscription's answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateUpdate {
    /// Which subscription this update belongs to.
    pub sub_id: String,
    /// What moved.
    pub change: AggregateChange,
}

/// What one [`AggregateUpdate`] does to the answer. Every row holds one cell
/// per column [`AggregateColumns`] declared, in select order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateChange {
    /// Write the row under `key`: a fold's group, or a keyed computed row.
    Upsert {
        /// The opaque identity `subql` encodes the group or row key as. The
        /// client stores it verbatim as the row's key.
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
        /// The row, in select order.
        row: Vec<SqliteValue>,
    },
    /// Remove the row under `key`, a group or keyed row that left the answer.
    Remove {
        /// The identity of the row that left.
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
    },
    /// Replace the whole answer with `rows`, in answer order. A scalar
    /// aggregate's answer is one row of one cell, and no row at all when a
    /// `HAVING` holds for none.
    Replace {
        /// The answer's rows, in answer order.
        rows: Vec<Vec<SqliteValue>>,
    },
}
