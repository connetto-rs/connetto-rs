//! Dioxus adapter for connetto live queries.
//!
//! [`use_live`] follows a [`LiveSpec`] from a [`LiveSource`] through
//! [`follow`] in a component-scoped task, which Dioxus drops on unmount, and
//! with it the feed.
//!
//! A desktop sign-in ends in the system browser, which keeps the front. The
//! application asks for its window back once its setup completes, with
//! `dioxus::desktop::window().set_focus()` where the setup task finishes, as
//! `examples/dioxus-demo` does, and GNOME on Wayland answers that with
//! its notification that the app is ready. connetto knows no window, so this
//! adapter offers no hook for it, except that `use_hello_owner` raises the
//! window when it lends it, since Windows shows Hello only over a window in
//! front.

use connetto_client::{LiveSnapshot, LiveSource, LiveSpec, follow};
use dioxus_core::{spawn, use_hook};
use dioxus_hooks::use_signal;
use dioxus_signals::{ReadSignal, WritableExt};

#[cfg(feature = "desktop")]
mod away;
#[cfg(feature = "desktop")]
pub use away::use_away_input;
#[cfg(all(feature = "hello", target_os = "windows"))]
mod hello;
#[cfg(all(feature = "hello", target_os = "windows"))]
pub use hello::use_hello_owner;

/// A live query bound to a component: the snapshot signal and an error slot.
///
/// Reading [`value`](Self::value) inside a component subscribes that
/// component to the signal, so only components that read it re-render when
/// the snapshot moves.
pub struct UseLive<S: 'static> {
    value: ReadSignal<S>,
    error: ReadSignal<Option<String>>,
}

impl<S> Clone for UseLive<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for UseLive<S> {}

impl<S: 'static> UseLive<S> {
    /// The live snapshot: the type's `Default` until the first refresh lands
    /// (an empty `Vec` for rows, `None` for a scalar).
    #[must_use]
    pub fn value(&self) -> ReadSignal<S> {
        self.value
    }

    /// The error slot: `Some` when subscribing failed or the driving client
    /// stopped. The snapshot signal keeps its last value in that case.
    #[must_use]
    pub fn error(&self) -> ReadSignal<Option<String>> {
        self.error
    }
}

/// Follow `spec` from `source` for as long as this component is mounted.
///
/// Both are captured on first render, so a component remounts to follow
/// another spec.
pub fn use_live<Source, Spec>(source: &Source, spec: Spec) -> UseLive<LiveSnapshot<Spec>>
where
    Source: LiveSource,
    Spec: LiveSpec,
    LiveSnapshot<Spec>: Default + 'static,
{
    let mut value = use_signal(LiveSnapshot::<Spec>::default);
    let mut error = use_signal(|| None::<String>);
    let source = source.clone();
    use_hook(move || {
        spawn(follow(
            source,
            spec,
            move |snapshot| value.set(snapshot),
            move |failure| error.set(Some(failure.to_string())),
        ))
    });
    UseLive {
        value: value.into(),
        error: error.into(),
    }
}
