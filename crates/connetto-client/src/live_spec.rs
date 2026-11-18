//! Live queries named by serializable descriptors (R116).
//!
//! A typed Diesel query cannot cross a process boundary such as Tauri's
//! `invoke`. A descriptor names the query by its name and arguments and builds
//! the same typed query on either side, so a registry on the far side runs
//! only the names it holds and the rows stay typed at compile time.

use connetto_core::traits::{MaybeSend, Transport};

use crate::{
    ClientError, ConnettoClient, LiveHandle, LiveQuery, LiveUnsynced, UnsyncedWork, Watchable,
};

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
    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + 'static
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

/// The descriptor of this device's unsynced work, its writes and staged
/// files that have not reached the server and its undismissed refusals, as
/// [`ConnettoClient::unsynced_live`] follows it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Unsynced;

impl LiveSpec for Unsynced {
    const NAME: &'static str = "connetto_unsynced";
    type Row = UnsyncedWork;
    type Handle = LiveUnsynced;

    fn query<T>(&self) -> impl Watchable<T, UnsyncedWork, Handle = LiveUnsynced> + 'static
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        UnsyncedQuery
    }
}

/// What [`Unsynced`] builds, subscribing through
/// [`ConnettoClient::unsynced_live`].
struct UnsyncedQuery;

impl<T> Watchable<T, UnsyncedWork> for UnsyncedQuery
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    type Handle = LiveUnsynced;

    fn live<'a>(
        self,
        client: &'a ConnettoClient<T>,
    ) -> impl Future<Output = Result<LiveUnsynced, ClientError>> + MaybeSend + 'a
    where
        Self: 'a,
    {
        client.unsynced_live()
    }
}
