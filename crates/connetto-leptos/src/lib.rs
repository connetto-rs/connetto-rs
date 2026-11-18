//! Leptos adapter for connetto live queries.
//!
//! [`use_live`] follows a [`LiveSpec`] from a [`LiveSource`] through
//! [`follow`], started with [`spawn_local_scoped_with_cancellation`], so the
//! reactive owner's cleanup aborts the loop and drops its feed.
//!
//! [`keyed`] turns a row snapshot into one signal per row, for a `For` that
//! keys on the row's identity and keeps the row's DOM node across an edit.

use core::hash::Hash;
use std::collections::HashMap;

use connetto_client::{LiveSnapshot, LiveSource, LiveSpec, follow};
use reactive_graph::computed::Memo;
use reactive_graph::signal::{ArcRwSignal, ReadSignal, signal};
use reactive_graph::spawn_local_scoped_with_cancellation;
use reactive_graph::traits::{Set, With, WithUntracked};
use reactive_graph::wrappers::read::Signal;

/// A live query bound to a reactive owner, its snapshot and the error that
/// ended it.
///
/// [`value`](Self::value) holds the type's `Default` until the first snapshot
/// lands, and keeps its last value once [`error`](Self::error) is set.
pub struct UseLive<Snapshot: Send + Sync + 'static> {
    value: ReadSignal<Snapshot>,
    error: ReadSignal<Option<String>>,
}

impl<Snapshot: Send + Sync + 'static> Clone for UseLive<Snapshot> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Snapshot: Send + Sync + 'static> Copy for UseLive<Snapshot> {}

impl<Snapshot: Send + Sync + 'static> UseLive<Snapshot> {
    /// The live snapshot.
    #[must_use]
    pub const fn value(&self) -> ReadSignal<Snapshot> {
        self.value
    }

    /// The error that ended the subscription, if one did.
    #[must_use]
    pub const fn error(&self) -> ReadSignal<Option<String>> {
        self.error
    }
}

/// Follow `spec` from `source` until the current reactive owner is cleaned up.
///
/// A component calls it once, so it remounts to follow another spec.
pub fn use_live<Source, Spec>(source: &Source, spec: Spec) -> UseLive<LiveSnapshot<Spec>>
where
    Source: LiveSource,
    Spec: LiveSpec,
    LiveSnapshot<Spec>: Default + Send + Sync,
{
    let (value, set_value) = signal(LiveSnapshot::<Spec>::default());
    let (error, set_error) = signal(None::<String>);
    spawn_local_scoped_with_cancellation(follow(
        source.clone(),
        spec,
        move |snapshot| set_value.set(snapshot),
        move |failure| set_error.set(Some(failure.to_string())),
    ));
    UseLive { value, error }
}

/// One signal per row of `rows`, in snapshot order, each beside its `key`.
///
/// A row whose key was in the previous snapshot keeps its signal, which is set
/// only when the row changed, so a `For` keyed on the key alone keeps the
/// row's DOM node and updates its cells in place. `key` must tell every row of
/// one snapshot apart, as a primary key does.
pub fn keyed<Row, Key>(
    rows: impl Into<Signal<Vec<Row>>>,
    key: impl Fn(&Row) -> Key + Send + Sync + 'static,
) -> Memo<Vec<(Key, ArcRwSignal<Row>)>>
where
    Row: Clone + PartialEq + Send + Sync + 'static,
    Key: Clone + Eq + Hash + Send + Sync + 'static,
{
    let rows = rows.into();
    Memo::new(move |previous: Option<&Vec<(Key, ArcRwSignal<Row>)>>| {
        let mut kept: HashMap<Key, ArcRwSignal<Row>> =
            previous.into_iter().flatten().cloned().collect();
        rows.with(|rows| {
            rows.iter()
                .map(|row| {
                    let id = key(row);
                    let cell = match kept.remove(&id) {
                        Some(cell) => {
                            if cell.with_untracked(|held| held != row) {
                                cell.set(row.clone());
                            }
                            cell
                        }
                        None => ArcRwSignal::new(row.clone()),
                    };
                    (id, cell)
                })
                .collect()
        })
    })
}
