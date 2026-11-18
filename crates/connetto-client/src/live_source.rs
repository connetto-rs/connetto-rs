//! Where a live query is followed from, and the one loop every framework hook
//! runs over it (R116).

use core::future::Future;

use connetto_core::traits::{MaybeSend, Transport};

use crate::{ClientError, ConnettoClient, LiveHandle, LiveSpec, Watchable};

/// The snapshot a feed following `Spec` yields.
pub type LiveSnapshot<Spec> = <<Spec as LiveSpec>::Handle as LiveHandle>::Snapshot;

/// A handle a component follows a [`LiveSpec`] through, wherever the client runs.
pub trait LiveSource: Clone + 'static {
    /// The feed following `Spec`, whose drop ends the subscription.
    type Feed<Spec: LiveSpec>: LiveHandle<Snapshot = LiveSnapshot<Spec>>;

    /// Subscribe to `spec`.
    ///
    /// # Errors
    ///
    /// The [`ClientError`] the subscription was refused or failed with.
    fn subscribe<Spec: LiveSpec>(
        &self,
        spec: Spec,
    ) -> impl Future<Output = Result<Self::Feed<Spec>, ClientError>> + MaybeSend + '_;
}

impl<T> LiveSource for ConnettoClient<T>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    type Feed<Spec: LiveSpec> = Spec::Handle;

    fn subscribe<Spec: LiveSpec>(
        &self,
        spec: Spec,
    ) -> impl Future<Output = Result<Spec::Handle, ClientError>> + MaybeSend + '_ {
        spec.query::<T>().live(self)
    }
}

/// Follow `spec` from `source`, passing each snapshot to `write` and the error
/// that ends the feed to `fail`, until the feed fails or this future drops.
pub async fn follow<Source, Spec>(
    source: Source,
    spec: Spec,
    mut write: impl FnMut(LiveSnapshot<Spec>),
    fail: impl FnOnce(ClientError),
) where
    Source: LiveSource,
    Spec: LiveSpec,
{
    let mut feed = match source.subscribe(spec).await {
        Ok(feed) => feed,
        Err(failure) => return fail(failure),
    };
    loop {
        write(feed.snapshot());
        if let Err(failure) = feed.changed().await {
            return fail(failure);
        }
    }
}
