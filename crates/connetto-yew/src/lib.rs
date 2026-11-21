//! Yew adapter for connetto live queries.
//!
//! [`use_live`] follows a [`LiveSpec`] from a [`LiveSource`] through
//! [`follow`], and aborts that loop from the effect's cleanup, since Yew's
//! [`spawn_local`] is detached and an unmount alone would not drop the feed.

use connetto_client::{LiveSnapshot, LiveSource, LiveSpec, follow};
use futures_util::future::{AbortHandle, Abortable};
use yew::platform::spawn_local;
use yew::prelude::*;

/// A live query bound to a component, its snapshot and the error that ended it.
///
/// [`value`](Self::value) is the type's `Default` until the first snapshot
/// lands, and keeps its last value once [`error`](Self::error) is set.
pub struct UseLive<Snapshot> {
    value: UseStateHandle<Snapshot>,
    error: UseStateHandle<Option<String>>,
}

impl<Snapshot: Clone> UseLive<Snapshot> {
    /// The current live snapshot.
    #[must_use]
    pub fn value(&self) -> Snapshot {
        (*self.value).clone()
    }

    /// The error that ended the subscription, if one did.
    #[must_use]
    pub fn error(&self) -> Option<String> {
        (*self.error).clone()
    }
}

/// Follow `spec` from `source` for as long as this component is mounted.
///
/// Both are captured on first render, so a component remounts to follow
/// another spec.
#[hook]
pub fn use_live<Source, Spec>(source: &Source, spec: Spec) -> UseLive<LiveSnapshot<Spec>>
where
    Source: LiveSource,
    Spec: LiveSpec,
    LiveSnapshot<Spec>: Default + Clone + 'static,
{
    let value = use_state(LiveSnapshot::<Spec>::default);
    let error = use_state(|| None::<String>);
    {
        let value = value.clone();
        let error = error.clone();
        let source = source.clone();
        use_effect_with((), move |()| {
            let (abort, registration) = AbortHandle::new_pair();
            let driver = follow(
                source,
                spec,
                move |snapshot| value.set(snapshot),
                move |failure| error.set(Some(failure.to_string())),
            );
            spawn_local(async move {
                let _ = Abortable::new(driver, registration).await;
            });
            move || abort.abort()
        });
    }
    UseLive { value, error }
}
