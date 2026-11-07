//! Subscription registration, cancellation, and snapshot delimiters.
//!
//! The subscription language is a SQLite-dialect `SELECT`, the same dialect the
//! client runs against its local replica, with `?` placeholders accompanied by
//! typed [`BindValue`]s in placeholder order. The server substitutes the binds
//! into the parsed statement, reverse-translates it to Postgres, and hands that
//! to `subql` for parsing (Q4.1). Row-level and aggregate subscriptions share
//! this envelope. The server classifies each subscription from its SQL, so the
//! envelope carries no kind discriminant.

use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;

/// One value bound to a `?` placeholder in a subscription query, in placeholder
/// order. The five SQLite storage classes, which is exactly what a diesel query
/// rendered against the client's replica can produce.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BindValue {
    /// SQL `NULL`.
    Null,
    /// An `INTEGER` value.
    Integer(i64),
    /// A `REAL` value.
    Real(f64),
    /// A `TEXT` value.
    Text(String),
    /// A `BLOB` value.
    Blob(#[serde(with = "serde_bytes")] Vec<u8>),
}

/// The observation contract handed to `subql` at registration time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionSpec {
    /// Full `SELECT` in the client's SQLite dialect, either a row projection or
    /// a single scalar aggregate. The server reverse-translates it to Postgres,
    /// classifies it from the SQL, and rejects untranslatable or unsupported
    /// syntax at registration time.
    pub query: String,
    /// Values for the `?` placeholders in `query`, in placeholder order. Empty
    /// when the query has no placeholders.
    #[serde(default)]
    pub binds: Vec<BindValue>,
}

impl SubscriptionSpec {
    /// Build a subscription. The server decides from the query whether it is a
    /// row or an aggregate subscription.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            binds: Vec::new(),
        }
    }

    /// Attach the values for the query's `?` placeholders, in placeholder order.
    #[must_use]
    pub fn with_binds(mut self, binds: Vec<BindValue>) -> Self {
        self.binds = binds;
        self
    }
}

/// Client registers a new subscription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscribe {
    /// Client-chosen id, unique per session. Correlates snapshot and update messages.
    pub sub_id: String,
    /// What the client wants to observe.
    pub spec: SubscriptionSpec,
}

/// Client cancels a subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unsubscribe {
    /// Id of the subscription being cancelled. Server tolerates unknown ids
    /// silently (idempotent).
    pub sub_id: String,
}

/// Server announces a membership subscription it opened on the client's
/// behalf (R27 decisions 3, 4 and 7).
///
/// A subscription whose filter narrows through a membership table needs that
/// table's rows on the replica, both for the offline answer and for the
/// policy view a translated schema reads, so the server opens one standing
/// subscription over the caller's own membership rows and tells the client
/// with this frame, ahead of its `SnapshotBegin`. The client keeps the table
/// out of the application-facing changed-tables signal, unconditionally. The
/// subscription is the server's: it is counted against the caller's
/// allowance, reopened when a term subscription registers again, and torn
/// down with the last term subscription that needs it, so the client never
/// re-declares it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipOpened {
    /// The hidden subscription's label, server-chosen and stable per
    /// membership table: `connetto-membership:<member_table>`.
    pub sub_id: String,
    /// The membership table it reads, by its catalog name, which is the
    /// table the client hides from the changed-tables signal.
    pub member_table: String,
}

/// Server marks the start of an initial snapshot for a subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBegin {
    /// Subscription this snapshot belongs to.
    pub sub_id: String,
}

/// Completes an initial snapshot or row catch-up and pins its resume point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEnd {
    /// Subscription whose delivery completed.
    pub sub_id: String,
    /// Snapshot read position or row catch-up ceiling, including an empty replay.
    pub cursor: Cursor,
}
