//! The live specs a webview may follow, filled by the application at setup.

use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use connetto_client::live_wire::{KeyedRow, decode, encode};
use connetto_client::{ClientError, KeyedSpec, LiveHandle, LiveSource, LiveSpec};

use crate::Refusal;

/// A boxed future the plugin's tasks hold.
pub(crate) type Boxed<'a, Output> = Pin<Box<dyn Future<Output = Output> + Send + 'a>>;

/// A subscription on its way to its first answer.
pub(crate) type Starting = Boxed<'static, Result<Box<dyn Answering>, ClientError>>;

/// What starts a subscription from a spec's name and its encoding.
pub(crate) type Start = Box<dyn Fn(&str, &[u8]) -> Result<Starting, Refusal> + Send + Sync>;

/// A subscription's answer as it crosses to the webview.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    /// The encoded snapshot of a spec without a key.
    Whole(Vec<u8>),
    /// The rows of a keyed spec, each beside its key.
    Keyed(Vec<KeyedRow>),
}

/// A followed subscription, whatever its spec.
pub(crate) trait Answering: Send {
    /// The current answer.
    fn answer(&self) -> Result<Answer, ClientError>;

    /// Wait until the answer changes.
    fn changed(&mut self) -> Boxed<'_, Result<(), ClientError>>;
}

/// A feed answered whole.
struct Whole<Feed>(Feed);

impl<Feed> Answering for Whole<Feed>
where
    Feed: LiveHandle<Snapshot: serde::Serialize> + Send,
{
    fn answer(&self) -> Result<Answer, ClientError> {
        encode(&self.0.snapshot()).map(Answer::Whole)
    }

    fn changed(&mut self) -> Boxed<'_, Result<(), ClientError>> {
        Box::pin(self.0.changed())
    }
}

/// A feed of a keyed spec, answered row by row.
struct Keyed<Spec, Feed> {
    feed: Feed,
    spec: PhantomData<fn() -> Spec>,
}

impl<Spec, Feed> Answering for Keyed<Spec, Feed>
where
    Spec: KeyedSpec,
    Spec::Row: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
    Feed: LiveHandle<Snapshot = Vec<Spec::Row>> + Send,
{
    fn answer(&self) -> Result<Answer, ClientError> {
        self.feed
            .snapshot()
            .iter()
            .map(|row| Ok(KeyedRow::new(encode(&Spec::key(row))?, encode(row)?)))
            .collect::<Result<_, ClientError>>()
            .map(Answer::Keyed)
    }

    fn changed(&mut self) -> Boxed<'_, Result<(), ClientError>> {
        Box::pin(self.feed.changed())
    }
}

/// What starts one spec on a source from the spec's encoding.
type Starter<Source> = Box<dyn Fn(Source, &[u8]) -> Result<Starting, Refusal> + Send + Sync>;

/// The live specs a webview may follow, each by its
/// [`NAME`](LiveSpec::NAME).
///
/// A webview names a spec, and the plugin builds the spec's query natively, so
/// a script in the webview reaches only the queries registered here.
pub struct Registry<Source> {
    starters: HashMap<&'static str, Starter<Source>>,
}

impl<Source> Default for Registry<Source> {
    fn default() -> Self {
        Self {
            starters: HashMap::new(),
        }
    }
}

impl<Source> Registry<Source>
where
    Source: LiveSource + Send + Sync,
{
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Let a webview follow `Spec`, each change crossing as its whole snapshot.
    ///
    /// # Panics
    ///
    /// When another spec is registered under the same name.
    #[must_use]
    pub fn register<Spec>(self) -> Self
    where
        Spec: LiveSpec,
        Source::Feed<Spec>: Send + 'static,
    {
        self.insert::<Spec>(Box::new(|source, descriptor| {
            let spec: Spec = decode(descriptor)?;
            Ok(Box::pin(async move {
                let feed = source.subscribe(spec).await?;
                Ok(Box::new(Whole(feed)) as Box<dyn Answering>)
            }))
        }))
    }

    /// Let a webview follow `Spec`, each change crossing as the rows added,
    /// changed and removed since the webview's last acknowledged answer.
    ///
    /// # Panics
    ///
    /// When another spec is registered under the same name.
    #[must_use]
    pub fn register_keyed<Spec>(self) -> Self
    where
        Spec: KeyedSpec,
        Spec::Row: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
        Source::Feed<Spec>: LiveHandle<Snapshot = Vec<Spec::Row>> + Send + 'static,
    {
        self.insert::<Spec>(Box::new(|source, descriptor| {
            let spec: Spec = decode(descriptor)?;
            Ok(Box::pin(async move {
                let feed = source.subscribe(spec).await?;
                Ok(Box::new(Keyed::<Spec, _> {
                    feed,
                    spec: PhantomData,
                }) as Box<dyn Answering>)
            }))
        }))
    }

    fn insert<Spec: LiveSpec>(mut self, starter: Starter<Source>) -> Self {
        let earlier = self.starters.insert(Spec::NAME, starter);
        assert!(
            earlier.is_none(),
            "two live specs are registered as {}",
            Spec::NAME
        );
        self
    }

    /// The starter of every registered spec on whatever source `slot` holds.
    pub(crate) fn start_from(self, slot: SourceSlot<Source>) -> Start {
        Box::new(move |name, descriptor| {
            let starter = self
                .starters
                .get(name)
                .ok_or_else(|| Refusal::Unregistered(name.to_owned()))?;
            match slot.get() {
                Some(source) => starter(source, descriptor),
                None => Ok(Box::pin(core::future::ready(Err(
                    ClientError::NotConnected,
                )))),
            }
        })
    }
}

/// The source the plugin follows specs from, which the application sets once
/// its client runs and replaces when the session changes.
pub struct SourceSlot<Source> {
    source: Arc<RwLock<Option<Source>>>,
}

impl<Source> Clone for SourceSlot<Source> {
    fn clone(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
        }
    }
}

impl<Source> Default for SourceSlot<Source> {
    fn default() -> Self {
        Self {
            source: Arc::new(RwLock::new(None)),
        }
    }
}

impl<Source: Clone> SourceSlot<Source> {
    /// Follow later subscriptions from `source`, or refuse them with
    /// [`ClientError::NotConnected`] while it is `None`. Running
    /// subscriptions keep their source and end with it.
    pub fn set(&self, source: Option<Source>) {
        *self.source.write().unwrap_or_else(PoisonError::into_inner) = source;
    }

    fn get(&self) -> Option<Source> {
        self.source
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
