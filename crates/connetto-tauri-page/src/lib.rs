//! The live source a Tauri webview follows connetto live queries through
//! (R116).
//!
//! The client runs in the native core, where `tauri-plugin-connetto` serves
//! the specs the application registered. [`TauriPage`] is a
//! [`LiveSource`], so a framework hook takes it where a browser page takes the
//! client itself. Each subscription sends the spec's descriptor with the
//! plugin's `subscribe` command, receives the answers through a Tauri channel
//! as MessagePack bytes, rebuilds each snapshot from them, and acknowledges
//! every message it applied. Dropping the feed, or the pending subscribe,
//! sends the `stop` command.

mod js;
#[cfg(test)]
mod tests;

use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use connetto_client::live_wire::{
    FeedBody, FeedMessage, KeyedRow, SubscribeRequest, decode, encode, rows_array,
};
use connetto_client::{ClientError, LiveHandle, LiveSnapshot, LiveSource, LiveSpec};
use futures_channel::mpsc;
use futures_util::StreamExt;
use serde_bytes::ByteBuf;

/// The ids of the page's subscriptions, which only grow within one page load
/// whatever number of sources the page holds, since the plugin tells a
/// webview's subscriptions apart by id alone.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The plugin's commands and the channels its messages arrive through.
pub(crate) trait Core: Send + Sync + 'static {
    /// A channel delivering each message's bytes to `inbox`, with the name the
    /// subscribe command gives it and what keeps it open until dropped.
    fn channel(&self, inbox: mpsc::UnboundedSender<Vec<u8>>) -> (String, Box<dyn Send>);

    /// Send the subscribe command with its encoded `request`, failing with
    /// the plugin's refusal.
    fn subscribe(
        &self,
        request: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

    /// Send the stop command for subscription `id`.
    fn stop(&self, id: u64);

    /// Acknowledge message `seq` of subscription `id`.
    fn ack(&self, id: u64, seq: u64);
}

/// The live source of a Tauri webview, reaching the client in the native core
/// through `tauri-plugin-connetto`.
#[derive(Clone)]
pub struct TauriPage {
    core: Arc<dyn Core>,
}

impl TauriPage {
    /// The source over the page's `window.__TAURI__`, which the application
    /// exposes with `app.withGlobalTauri`.
    #[must_use]
    pub fn new() -> Self {
        Self::over(js::Js)
    }

    pub(crate) fn over(core: impl Core) -> Self {
        Self {
            core: Arc::new(core),
        }
    }
}

impl Default for TauriPage {
    fn default() -> Self {
        Self::new()
    }
}

/// One subscription's hold on the plugin, which stops it when dropped.
struct Subscription {
    core: Arc<dyn Core>,
    id: u64,
    /// Whether the plugin may hold the subscription, which a refused
    /// subscribe rules out.
    held: bool,
    _channel: Box<dyn Send>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.held {
            self.core.stop(self.id);
        }
    }
}

/// The answer a feed holds, as the plugin sent it.
enum Base {
    /// The encoded snapshot of a spec without a key.
    Whole(Vec<u8>),
    /// The rows of a keyed spec, each beside its key.
    Keyed(Vec<KeyedRow>),
}

impl Base {
    fn snapshot<Spec: LiveSpec>(&self) -> Result<LiveSnapshot<Spec>, ClientError> {
        match self {
            Self::Whole(bytes) => decode(bytes),
            Self::Keyed(rows) => decode(&rows_array(rows)),
        }
    }
}

/// A spec followed through the plugin, whose drop stops the subscription.
pub struct Feed<Spec> {
    subscription: Subscription,
    inbox: mpsc::UnboundedReceiver<Vec<u8>>,
    base: Base,
    sub_id: String,
    spec: PhantomData<fn() -> Spec>,
}

/// The next message of a subscription.
async fn receive(inbox: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Result<FeedMessage, ClientError> {
    let bytes = inbox
        .next()
        .await
        .ok_or_else(|| ClientError::Transport("the core closed the channel".to_owned()))?;
    decode(&bytes)
}

impl<Spec: LiveSpec> LiveHandle for Feed<Spec> {
    type Snapshot = LiveSnapshot<Spec>;

    fn snapshot(&self) -> Self::Snapshot {
        self.base
            .snapshot::<Spec>()
            .expect("the base decoded when it arrived")
    }

    async fn changed(&mut self) -> Result<(), ClientError> {
        let message = receive(&mut self.inbox).await?;
        match message.body {
            FeedBody::Whole(bytes) => self.base = Base::Whole(bytes.into_vec()),
            FeedBody::Keyed(rows) => self.base = Base::Keyed(rows),
            FeedBody::Diff(diff) => {
                let Base::Keyed(rows) = &mut self.base else {
                    return Err(ClientError::Protocol(
                        "a keyed diff to an answer without a key".to_owned(),
                    ));
                };
                diff.apply(rows)?;
            }
            FeedBody::Failed(failure) => return Err(ClientError::Remote(failure)),
        }
        self.base.snapshot::<Spec>()?;
        self.subscription
            .core
            .ack(self.subscription.id, message.seq);
        Ok(())
    }

    fn sub_id(&self) -> &str {
        &self.sub_id
    }
}

impl LiveSource for TauriPage {
    type Feed<Spec: LiveSpec> = Feed<Spec>;

    async fn subscribe<Spec: LiveSpec>(&self, spec: Spec) -> Result<Feed<Spec>, ClientError> {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, mut inbox) = mpsc::unbounded();
        let (channel, open) = self.core.channel(sender);
        let request = encode(&SubscribeRequest {
            id,
            name: Spec::NAME.to_owned(),
            descriptor: ByteBuf::from(encode(&spec)?),
            channel,
        })?;
        let mut subscription = Subscription {
            core: Arc::clone(&self.core),
            id,
            held: true,
            _channel: open,
        };
        if let Err(refusal) = self.core.subscribe(request).await {
            subscription.held = false;
            return Err(ClientError::Remote(refusal));
        }
        let message = receive(&mut inbox).await?;
        let base = match message.body {
            FeedBody::Whole(bytes) => Base::Whole(bytes.into_vec()),
            FeedBody::Keyed(rows) => Base::Keyed(rows),
            FeedBody::Diff(_) => {
                return Err(ClientError::Protocol(
                    "a keyed diff before the first answer".to_owned(),
                ));
            }
            FeedBody::Failed(failure) => return Err(ClientError::Remote(failure)),
        };
        base.snapshot::<Spec>()?;
        self.core.ack(id, message.seq);
        Ok(Feed {
            subscription,
            inbox,
            base,
            sub_id: id.to_string(),
            spec: PhantomData,
        })
    }
}
