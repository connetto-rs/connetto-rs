//! Live queries named by serializable descriptors (R116).
//!
//! A typed Diesel query cannot cross a process boundary such as Tauri's
//! `invoke`. A descriptor names the query by its name and arguments and builds
//! the same typed query on either side, so a registry on the far side runs
//! only the names it holds and the rows stay typed at compile time.

use connetto_core::traits::{MaybeSend, Transport};

use crate::{LiveHandle, LiveQuery, Watchable};

/// A serializable descriptor of a typed live query.
pub trait LiveSpec: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static {
    /// The name a registry holds this descriptor under, unique within an
    /// application.
    const NAME: &'static str;
    /// The element of the answer, a row for a row query, the value of a
    /// scalar, and a `(key, value)` pair for a group.
    type Row;
    /// The live handle the query produces, whose snapshot crosses a process
    /// boundary.
    type Handle: LiveHandle<Snapshot: serde::Serialize + serde::de::DeserializeOwned>;

    /// Build the typed query this descriptor names.
    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<Self, T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display;
}

/// The opt-in key of a row query, which lets a consumer follow each row by
/// identity across snapshots.
pub trait KeyedSpec: LiveSpec<Handle = LiveQuery<<Self as LiveSpec>::Row>>
where
    Self::Row: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
{
    /// The row's identity, stable while the row exists.
    type Key: serde::Serialize + serde::de::DeserializeOwned + Eq + core::hash::Hash + Clone;

    /// The identity of `row`.
    fn key(row: &Self::Row) -> Self::Key;
}
