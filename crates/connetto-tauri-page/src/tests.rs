//! The page source over a scripted core in place of the page's
//! `window.__TAURI__`.

use core::future::Future;
use core::pin::Pin;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use connetto_client::live_wire::{
    FeedBody, FeedMessage, KeyedDiff, KeyedRow, SubscribeRequest, decode, encode,
};
use connetto_client::{ClientError, LiveHandle, LiveQuery, LiveSource, LiveSpec, Watchable};
use connetto_core::traits::{MaybeSend, Transport};
use diesel::prelude::*;
use futures_channel::mpsc;
use futures_util::FutureExt;
use serde_bytes::ByteBuf;

use crate::{Core, TauriPage};

diesel::table! {
    /// The table the test spec names.
    items (id) {
        /// The item's identifier
        id -> Integer,
        /// The item's label
        label -> Text,
    }
}

#[derive(Queryable, Selectable, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = items)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Item {
    id: i32,
    label: String,
}

fn item(id: i32, label: &str) -> Item {
    Item {
        id,
        label: label.to_owned(),
    }
}

/// Every item by id.
#[derive(serde::Serialize, serde::Deserialize)]
struct Items;

impl LiveSpec for Items {
    const NAME: &'static str = "items";
    type Row = Item;
    type Handle = LiveQuery<Item>;

    fn query<T>(&self) -> impl Watchable<T, Item, Handle = LiveQuery<Item>> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        items::table.order(items::id)
    }
}

/// A command the page sent.
#[derive(Debug, PartialEq, Eq)]
enum Sent {
    Subscribe(String),
    Stop,
    Ack(u64),
}

/// What the scripted core saw and holds.
#[derive(Default)]
struct Held {
    /// The answer to the next subscribe, the refusal or the bodies sent at
    /// once.
    script: Option<Result<Vec<FeedBody>, String>>,
    channels: HashMap<String, mpsc::UnboundedSender<Vec<u8>>>,
    /// The channel each subscription names.
    subscribed: HashMap<u64, String>,
    sent: Vec<(u64, Sent)>,
    open_channels: usize,
}

/// A core whose answers the test scripts.
#[derive(Clone, Default)]
struct Scripted {
    held: Arc<Mutex<Held>>,
}

impl Scripted {
    fn held(&self) -> MutexGuard<'_, Held> {
        self.held
            .lock()
            .expect("no test thread panics holding the core")
    }

    /// Answer the next subscribe with `script`.
    fn script(&self, script: Result<Vec<FeedBody>, String>) {
        self.held().script = Some(script);
    }

    /// Send message `seq` carrying `body` to subscription `id`.
    fn send(&self, id: u64, seq: u64, body: FeedBody) {
        let held = self.held();
        let channel = &held.subscribed[&id];
        held.channels[channel]
            .unbounded_send(encode(&FeedMessage { seq, body }).expect("messages encode"))
            .expect("the page keeps its channel open");
    }

    /// What the page sent for subscription `id`.
    fn sent(&self, id: u64) -> Vec<Sent> {
        let mut held = self.held();
        let (theirs, rest) = core::mem::take(&mut held.sent)
            .into_iter()
            .partition(|(sent_id, _)| *sent_id == id);
        held.sent = rest;
        theirs.into_iter().map(|(_, sent)| sent).collect()
    }

    fn open_channels(&self) -> usize {
        self.held().open_channels
    }
}

/// A channel the scripted core counts while it is open.
struct Open(Scripted);

impl Drop for Open {
    fn drop(&mut self) {
        self.0.held().open_channels -= 1;
    }
}

impl Core for Scripted {
    fn channel(&self, inbox: mpsc::UnboundedSender<Vec<u8>>) -> (String, Box<dyn Send>) {
        let mut held = self.held();
        let name = format!("__CHANNEL__:{}", held.channels.len());
        held.channels.insert(name.clone(), inbox);
        held.open_channels += 1;
        (name, Box::new(Open(self.clone())))
    }

    fn subscribe(
        &self,
        request: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let request: SubscribeRequest = decode(&request).expect("the page encodes its request");
        let script = {
            let mut held = self.held();
            held.sent
                .push((request.id, Sent::Subscribe(request.name.clone())));
            held.subscribed.insert(request.id, request.channel);
            held.script
                .take()
                .expect("the test scripts every subscribe")
        };
        let answered = script.map(|bodies| {
            for (seq, body) in (1..).zip(bodies) {
                self.send(request.id, seq, body);
            }
        });
        Box::pin(core::future::ready(answered))
    }

    fn stop(&self, id: u64) {
        self.held().sent.push((id, Sent::Stop));
    }

    fn ack(&self, id: u64, seq: u64) {
        self.held().sent.push((id, Sent::Ack(seq)));
    }
}

fn keyed(items: &[Item]) -> Vec<KeyedRow> {
    items
        .iter()
        .map(|row| {
            KeyedRow::new(
                encode(&row.id).expect("keys encode"),
                encode(row).expect("rows encode"),
            )
        })
        .collect()
}

fn whole(items: &[Item]) -> FeedBody {
    FeedBody::Whole(ByteBuf::from(encode(items).expect("snapshots encode")))
}

/// The subscription id of `feed`.
fn id<Feed: LiveHandle>(feed: &Feed) -> u64 {
    feed.sub_id().parse().expect("ids are numbers")
}

#[tokio::test]
async fn the_first_answer_resolves_the_subscribe_and_is_acknowledged() {
    let core = Scripted::default();
    core.script(Ok(vec![whole(&[item(1, "a")])]));

    let feed = TauriPage::over(core.clone())
        .subscribe(Items)
        .await
        .expect("the core answers");

    assert_eq!(feed.snapshot(), vec![item(1, "a")]);
    assert_eq!(
        core.sent(id(&feed)),
        vec![Sent::Subscribe("items".to_owned()), Sent::Ack(1)]
    );
}

#[tokio::test]
async fn keyed_diffs_rebuild_the_answer_in_order() {
    let core = Scripted::default();
    let first = [item(1, "a"), item(2, "b"), item(3, "c")];
    let next = [item(3, "c"), item(1, "A"), item(4, "d")];
    core.script(Ok(vec![FeedBody::Keyed(keyed(&first))]));
    let mut feed = TauriPage::over(core.clone())
        .subscribe(Items)
        .await
        .expect("the core answers");
    let id = id(&feed);
    core.sent(id);

    core.send(
        id,
        2,
        FeedBody::Diff(KeyedDiff::between(&keyed(&first), &keyed(&next)).expect("they differ")),
    );
    feed.changed().await.expect("the diff applies");

    assert_eq!(feed.snapshot(), next);
    assert_eq!(core.sent(id), vec![Sent::Ack(2)]);
}

#[tokio::test]
async fn a_whole_answer_replaces_the_last() {
    let core = Scripted::default();
    core.script(Ok(vec![whole(&[item(1, "a")])]));
    let mut feed = TauriPage::over(core.clone())
        .subscribe(Items)
        .await
        .expect("the core answers");
    let id = id(&feed);
    core.sent(id);

    core.send(id, 2, whole(&[item(2, "b")]));
    feed.changed().await.expect("the answer decodes");

    assert_eq!(feed.snapshot(), vec![item(2, "b")]);
    assert_eq!(core.sent(id), vec![Sent::Ack(2)]);
}

#[tokio::test]
async fn a_refused_subscribe_fails_and_sends_no_stop() {
    let core = Scripted::default();
    core.script(Err("no live spec is registered as items".to_owned()));

    let refused = TauriPage::over(core.clone()).subscribe(Items).await.err();

    assert!(
        matches!(&refused, Some(ClientError::Remote(refusal)) if refusal.contains("registered")),
        "{refused:?}"
    );
    let sent: Vec<_> = core.held().sent.drain(..).map(|(_, sent)| sent).collect();
    assert_eq!(sent, vec![Sent::Subscribe("items".to_owned())]);
    assert_eq!(core.open_channels(), 0, "the channel closes");
}

#[tokio::test]
async fn an_error_before_the_first_answer_fails_the_subscribe_and_stops_it() {
    let core = Scripted::default();
    core.script(Ok(vec![FeedBody::Failed("not connected".to_owned())]));

    let failed = TauriPage::over(core.clone()).subscribe(Items).await.err();

    assert!(
        matches!(&failed, Some(ClientError::Remote(failure)) if failure == "not connected"),
        "{failed:?}"
    );
    let sent: Vec<_> = core.held().sent.drain(..).map(|(_, sent)| sent).collect();
    assert_eq!(sent, vec![Sent::Subscribe("items".to_owned()), Sent::Stop]);
}

#[tokio::test]
async fn a_diff_before_the_first_answer_is_a_protocol_error() {
    let core = Scripted::default();
    let diff = KeyedDiff::between(&[], &keyed(&[item(1, "a")])).expect("they differ");
    core.script(Ok(vec![FeedBody::Diff(diff)]));

    let failed = TauriPage::over(core.clone()).subscribe(Items).await.err();

    assert!(
        matches!(failed, Some(ClientError::Protocol(_))),
        "{failed:?}"
    );
}

#[tokio::test]
async fn an_error_while_following_fails_changed_unacknowledged() {
    let core = Scripted::default();
    core.script(Ok(vec![whole(&[item(1, "a")])]));
    let mut feed = TauriPage::over(core.clone())
        .subscribe(Items)
        .await
        .expect("the core answers");
    let id = id(&feed);
    core.sent(id);

    core.send(id, 2, FeedBody::Failed("the client stopped".to_owned()));
    let failed = feed.changed().await;

    assert!(
        matches!(&failed, Err(ClientError::Remote(failure)) if failure == "the client stopped"),
        "{failed:?}"
    );
    assert_eq!(core.sent(id), Vec::new());
}

#[tokio::test]
async fn an_answer_of_another_shape_fails_changed_unacknowledged() {
    let core = Scripted::default();
    core.script(Ok(vec![whole(&[item(1, "a")])]));
    let mut feed = TauriPage::over(core.clone())
        .subscribe(Items)
        .await
        .expect("the core answers");
    let id = id(&feed);
    core.sent(id);

    core.send(
        id,
        2,
        FeedBody::Whole(ByteBuf::from(encode("text").expect("strings encode"))),
    );

    assert!(feed.changed().await.is_err());
    assert_eq!(core.sent(id), Vec::new());
}

#[tokio::test]
async fn dropping_the_feed_stops_the_subscription_and_closes_its_channel() {
    let core = Scripted::default();
    core.script(Ok(vec![whole(&[item(1, "a")])]));
    let feed = TauriPage::over(core.clone())
        .subscribe(Items)
        .await
        .expect("the core answers");
    let id = id(&feed);
    core.sent(id);

    drop(feed);

    assert_eq!(core.sent(id), vec![Sent::Stop]);
    assert_eq!(core.open_channels(), 0);
}

#[test]
fn dropping_a_pending_subscribe_stops_it() {
    let core = Scripted::default();
    core.script(Ok(Vec::new()));
    let source = TauriPage::over(core.clone());
    let mut pending = Box::pin(source.subscribe(Items));

    assert!(
        (&mut pending).now_or_never().is_none(),
        "no answer arrived yet"
    );
    drop(pending);

    let sent: Vec<_> = core.held().sent.drain(..).map(|(_, sent)| sent).collect();
    assert_eq!(sent, vec![Sent::Subscribe("items".to_owned()), Sent::Stop]);
    assert_eq!(core.open_channels(), 0);
}
