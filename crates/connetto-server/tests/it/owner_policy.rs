//! A visibility policy for the tests whose tables name the owner in the row.

use std::sync::Arc;

use connetto_core::auth::Principal;
use subql::backend::{Postgres, Value};
use subql::visibility::{RowView, RowWrite, Verdict, VisibilityPolicy};

/// Allows reads of the row to the watcher the row's owner names, allows every
/// write. The owner is read off the row itself, the way a row-view policy
/// sees it.
pub struct OwnerPolicy;

impl VisibilityPolicy for OwnerPolicy {
    type Watcher = Arc<Principal>;
    type Error = std::convert::Infallible;
    type Backend = Postgres;

    fn may_see<R>(
        &self,
        row: &R,
        watchers: &[Self::Watcher],
        verdicts: &mut [Verdict],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        let owner = match row.value_at(1) {
            Ok(Value::String(owner)) => owner,
            _ => String::new(),
        };
        async move {
            for (watcher, verdict) in watchers.iter().zip(verdicts.iter_mut()) {
                let admitted = watcher
                    .identity()
                    .is_some_and(|identity| identity.user_id == owner);
                if admitted {
                    *verdict = Verdict::Allow;
                }
            }
            Ok(())
        }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_write<R>(
        &self,
        _write: RowWrite<'_, R>,
        _watcher: &Self::Watcher,
    ) -> Result<Verdict, Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Ok(Verdict::Allow)
    }
}
