//! The hub and its forwarders over a scripted source, with the webview's
//! channel replaced by a queue.

use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Waker};
use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use connetto_client::live_wire::{
    FeedBody, FeedMessage, KeyedRow, SubscribeRequest, decode, encode,
};
use connetto_client::{
    ClientError, KeyedSpec, LiveHandle, LiveQuery, LiveSnapshot, LiveSource, LiveSpec, Watchable,
};
use connetto_core::traits::{MaybeSend, Transport};
use diesel::prelude::*;
use serde_bytes::ByteBuf;
use tauri::ipc::InvokeBody;
use tokio::sync::{mpsc, watch};

use crate::forward::forward;
use crate::hub::Hub;
use crate::registry::Start;
use crate::{Refusal, Registry, SourceSlot, request_bytes};

/// The bound on every wait, which only detects a hang.
const BOUND: Duration = Duration::from_secs(5);

diesel::table! {
    /// The table the test specs name.
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

/// Every item by id, followed row by row.
#[derive(serde::Serialize, serde::Deserialize)]
struct Keyed;

impl LiveSpec for Keyed {
    const NAME: &'static str = "keyed";
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

impl KeyedSpec for Keyed {
    type Key = i32;

    fn key(row: &Item) -> i32 {
        row.id
    }
}

/// Every item by id, answered whole.
#[derive(serde::Serialize, serde::Deserialize)]
struct Whole;

impl LiveSpec for Whole {
    const NAME: &'static str = "whole";
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

/// One spec's published answer, encoded.
type Answer = watch::Sender<Vec<u8>>;

/// A source whose answers the test publishes, one per spec name.
#[derive(Clone, Default)]
struct Scripted {
    answers: Arc<Mutex<HashMap<&'static str, Answer>>>,
}

impl Scripted {
    fn answer<Spec: LiveSpec>(&self) -> watch::Sender<Vec<u8>> {
        self.answers
            .lock()
            .expect("no test thread panics holding the answers")
            .entry(Spec::NAME)
            .or_insert_with(|| watch::channel(Vec::new()).0)
            .clone()
    }

    /// Answer every follower of `Spec` with `rows`.
    fn publish<Spec: LiveSpec<Row = Item>>(&self, rows: &[Item]) {
        self.answer::<Spec>()
            .send_replace(encode(rows).expect("items encode"));
    }

    /// End every feed of `Spec`, failing its followers.
    fn vanish<Spec: LiveSpec>(&self) {
        self.answers
            .lock()
            .expect("no test thread panics holding the answers")
            .remove(Spec::NAME);
    }

    /// Wait until no feed of `Spec` remains.
    async fn unfollowed<Spec: LiveSpec>(&self) {
        let answer = self.answer::<Spec>();
        tokio::time::timeout(BOUND, answer.closed())
            .await
            .expect("the feed ends");
    }
}

/// A feed of [`Scripted`].
struct Feed<Spec> {
    answer: watch::Receiver<Vec<u8>>,
    spec: PhantomData<fn() -> Spec>,
}

impl<Spec: LiveSpec> LiveHandle for Feed<Spec> {
    type Snapshot = LiveSnapshot<Spec>;

    fn snapshot(&self) -> Self::Snapshot {
        decode(&self.answer.borrow()).expect("the test publishes this spec's snapshots")
    }

    async fn changed(&mut self) -> Result<(), ClientError> {
        self.answer
            .changed()
            .await
            .map_err(|_| ClientError::Transport("the scripted answer vanished".to_owned()))
    }

    fn sub_id(&self) -> &str {
        Spec::NAME
    }
}

impl LiveSource for Scripted {
    type Feed<Spec: LiveSpec> = Feed<Spec>;

    fn subscribe<Spec: LiveSpec>(
        &self,
        _spec: Spec,
    ) -> impl Future<Output = Result<Feed<Spec>, ClientError>> + MaybeSend + '_ {
        let answer = self.answer::<Spec>().subscribe();
        async move {
            Ok(Feed {
                answer,
                spec: PhantomData,
            })
        }
    }
}

/// The starter of the test specs over `source`, or over an empty slot when it
/// is `None`.
fn start(source: Option<Scripted>) -> Start {
    let slot = SourceSlot::default();
    slot.set(source);
    Registry::new()
        .register_keyed::<Keyed>()
        .register::<Whole>()
        .start_from(slot)
}

fn hub(source: Option<Scripted>) -> Hub {
    Hub::new(start(source))
}

/// The page side of one subscription, which receives its messages and keeps
/// the keyed answer they build.
struct Page {
    messages: mpsc::UnboundedReceiver<FeedMessage>,
    rows: Vec<KeyedRow>,
}

impl Page {
    /// Subscribe to `Spec` as the webview `page` in `window`.
    fn subscribe<Spec: LiveSpec>(
        hub: &Hub,
        page: &str,
        window: &str,
        id: u64,
        spec: &Spec,
    ) -> Result<Self, Refusal> {
        let (outlet, messages) = mpsc::unbounded_channel();
        hub.subscribe(
            page,
            window,
            &SubscribeRequest {
                id,
                name: Spec::NAME.to_owned(),
                descriptor: ByteBuf::from(encode(spec).expect("specs encode")),
                channel: String::from("__CHANNEL__:1"),
            },
            Box::new(move |message| {
                let _ = outlet.send(message);
            }),
        )?;
        Ok(Self {
            messages,
            rows: Vec::new(),
        })
    }

    async fn next(&mut self) -> FeedMessage {
        tokio::time::timeout(BOUND, self.messages.recv())
            .await
            .expect("a message arrives")
            .expect("the outlet lives while the forwarder runs")
    }

    /// The next message, applied to the keyed answer, and the items it holds.
    async fn apply(&mut self) -> (u64, Vec<Item>) {
        let message = self.next().await;
        match message.body {
            FeedBody::Keyed(rows) => self.rows = rows,
            FeedBody::Diff(diff) => diff.apply(&mut self.rows).expect("diffs apply"),
            other => panic!("expected a keyed answer, got {other:?}"),
        }
        let items = self
            .rows
            .iter()
            .map(|entry| decode(&entry.row).expect("rows decode"))
            .collect();
        (message.seq, items)
    }

    /// Wait until the forwarder ends, which drops the outlet.
    async fn ended(&mut self) {
        let last = tokio::time::timeout(BOUND, self.messages.recv())
            .await
            .expect("the forwarder ends");
        assert_eq!(last, None, "no message follows the end");
    }
}

#[tokio::test]
async fn a_keyed_change_crosses_as_the_diff_from_the_acknowledged_answer() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a"), item(2, "b")]);
    let hub = hub(Some(source.clone()));
    let mut page = Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");

    assert_eq!(page.apply().await, (1, vec![item(1, "a"), item(2, "b")]));
    hub.ack("main", 1, 1);
    source.publish::<Keyed>(&[item(1, "a"), item(2, "B"), item(3, "c")]);

    let message = page.next().await;
    let FeedBody::Diff(diff) = &message.body else {
        panic!("expected a diff, got {message:?}");
    };
    assert_eq!(
        diff.upserted
            .iter()
            .map(|entry| decode::<Item>(&entry.row).expect("rows decode"))
            .collect::<Vec<_>>(),
        vec![item(2, "B"), item(3, "c")],
        "the unchanged row stays behind"
    );
}

#[tokio::test]
async fn an_ack_naming_another_message_releases_nothing() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));
    let mut page = Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");
    assert_eq!(page.apply().await, (1, vec![item(1, "a")]));

    source.publish::<Keyed>(&[item(2, "b")]);
    hub.ack("main", 1, 7);
    source.publish::<Keyed>(&[item(3, "c")]);
    hub.ack("main", 1, 1);

    assert_eq!(
        page.apply().await,
        (2, vec![item(3, "c")]),
        "the stray ack sent nothing, so the next message is the second"
    );
}

#[tokio::test]
async fn a_change_reverted_while_in_flight_sends_nothing() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));
    let mut page = Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");
    assert_eq!(page.apply().await, (1, vec![item(1, "a")]));
    hub.ack("main", 1, 1);

    source.publish::<Keyed>(&[item(1, "b")]);
    assert_eq!(page.apply().await, (2, vec![item(1, "b")]));
    source.publish::<Keyed>(&[item(1, "a")]);
    source.publish::<Keyed>(&[item(1, "b")]);
    hub.ack("main", 1, 2);
    source.publish::<Keyed>(&[item(1, "c")]);

    assert_eq!(
        page.apply().await,
        (3, vec![item(1, "c")]),
        "the answer the page holds crossed no second time"
    );
}

#[tokio::test]
async fn a_spec_without_a_key_crosses_whole() {
    let source = Scripted::default();
    source.publish::<Whole>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));
    let mut page = Page::subscribe(&hub, "main", "main", 1, &Whole).expect("whole is registered");

    let first = page.next().await;
    hub.ack("main", 1, 1);
    source.publish::<Whole>(&[item(1, "a"), item(2, "b")]);
    let second = page.next().await;

    for (message, items) in [
        (first, vec![item(1, "a")]),
        (second, vec![item(1, "a"), item(2, "b")]),
    ] {
        let FeedBody::Whole(bytes) = message.body else {
            panic!("expected a whole answer, got {message:?}");
        };
        assert_eq!(
            decode::<Vec<Item>>(&bytes).expect("snapshots decode"),
            items
        );
    }
}

#[tokio::test]
async fn an_unregistered_spec_is_refused() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Other;

    impl LiveSpec for Other {
        const NAME: &'static str = "other";
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

    let hub = hub(Some(Scripted::default()));
    let refusal = Page::subscribe(&hub, "main", "main", 1, &Other).err();
    assert!(
        matches!(&refusal, Some(Refusal::Unregistered(name)) if name == "other"),
        "{refusal:?}"
    );
}

#[tokio::test]
async fn a_subscription_without_a_source_fails_as_not_connected() {
    let hub = hub(None);
    let mut page = Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");

    let message = page.next().await;
    assert_eq!(
        message.body,
        FeedBody::Failed(ClientError::NotConnected.to_string())
    );
    page.ended().await;
}

#[tokio::test]
async fn a_feed_that_fails_sends_its_error_and_ends() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));
    let mut page = Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");
    assert_eq!(page.apply().await, (1, vec![item(1, "a")]));

    source.vanish::<Keyed>();

    let message = page.next().await;
    assert!(
        matches!(&message.body, FeedBody::Failed(error) if error.contains("vanished")),
        "{message:?}"
    );
    page.ended().await;
}

#[tokio::test]
async fn stop_ends_the_feed() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));
    let mut page = Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");
    assert_eq!(page.apply().await, (1, vec![item(1, "a")]));

    hub.stop("main", "main", 1);

    source.unfollowed::<Keyed>().await;
    page.ended().await;
}

#[tokio::test]
async fn a_stop_arriving_before_its_subscribe_keeps_it_from_starting() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));

    hub.stop("main", "main", 1);
    let mut stopped =
        Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");
    let mut next = Page::subscribe(&hub, "main", "main", 2, &Keyed).expect("keyed is registered");

    stopped.ended().await;
    assert_eq!(next.apply().await, (1, vec![item(1, "a")]));
}

#[tokio::test]
async fn a_page_load_ends_that_webviews_subscriptions_only() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    source.publish::<Whole>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));
    let mut reloaded =
        Page::subscribe(&hub, "main", "main", 1, &Keyed).expect("keyed is registered");
    let mut kept = Page::subscribe(&hub, "side", "main", 1, &Whole).expect("whole is registered");
    reloaded.next().await;
    kept.next().await;

    hub.page_started("main");

    reloaded.ended().await;
    hub.ack("side", 1, 1);
    source.publish::<Whole>(&[item(2, "b")]);
    assert_eq!(kept.next().await.seq, 2);
}

#[tokio::test]
async fn a_closed_window_ends_its_webviews_subscriptions_only() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    source.publish::<Whole>(&[item(1, "a")]);
    let hub = hub(Some(source.clone()));
    let mut closed =
        Page::subscribe(&hub, "first", "first", 1, &Keyed).expect("keyed is registered");
    let mut kept =
        Page::subscribe(&hub, "second", "second", 1, &Whole).expect("whole is registered");
    closed.next().await;
    kept.next().await;

    hub.window_destroyed("first");

    closed.ended().await;
    hub.ack("second", 1, 1);
    source.publish::<Whole>(&[item(2, "b")]);
    assert_eq!(kept.next().await.seq, 2);
}

/// Poll `forwarding` until it waits, so it has handled every change and
/// acknowledgement already sent to it.
fn settle(forwarding: &mut Pin<Box<impl Future<Output = ()>>>) {
    let polled = forwarding
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(polled.is_pending(), "the forwarder still follows");
}

#[test]
fn a_page_that_stops_acknowledging_holds_one_message() {
    let source = Scripted::default();
    source.publish::<Keyed>(&[item(1, "a")]);
    let starting = start(Some(source.clone()))(Keyed::NAME, &encode(&Keyed).expect("specs encode"))
        .expect("keyed is registered");
    let (outlet, mut messages) = mpsc::unbounded_channel();
    let (acks, acknowledged) = mpsc::unbounded_channel();
    let mut forwarding = Box::pin(forward(
        starting,
        Box::new(move |message| {
            let _ = outlet.send(message);
        }),
        acknowledged,
        1,
        "main".to_owned(),
    ));
    settle(&mut forwarding);
    assert_eq!(messages.try_recv().map(|message| message.seq), Ok(1));

    for label in ["b", "c", "d"] {
        source.publish::<Keyed>(&[item(1, label)]);
        settle(&mut forwarding);
        assert!(
            messages.try_recv().is_err(),
            "nothing crosses before the ack"
        );
    }
    acks.send(1).expect("the forwarder follows");
    settle(&mut forwarding);

    let message = messages
        .try_recv()
        .expect("the ack releases the latest answer");
    assert_eq!(message.seq, 2);
    let FeedBody::Diff(diff) = message.body else {
        panic!("expected a diff, got {message:?}");
    };
    assert_eq!(
        diff.upserted
            .iter()
            .map(|entry| decode::<Item>(&entry.row).expect("rows decode"))
            .collect::<Vec<_>>(),
        vec![item(1, "d")]
    );
    assert!(messages.try_recv().is_err(), "one message is in flight");
}

#[test]
fn a_request_arrives_as_bytes_or_as_androids_array_of_numbers() {
    let request = SubscribeRequest {
        id: 1,
        name: Keyed::NAME.to_owned(),
        descriptor: ByteBuf::from(encode(&Keyed).expect("specs encode")),
        channel: String::from("__CHANNEL__:1"),
    };
    let bytes = encode(&request).expect("requests encode");
    let numbers = serde_json::to_value(&bytes).expect("bytes are numbers");

    for body in [InvokeBody::Raw(bytes), InvokeBody::Json(numbers)] {
        let read = request_bytes(&body).expect("both shapes carry bytes");
        assert_eq!(
            decode::<SubscribeRequest>(&read).expect("the request decodes"),
            request
        );
    }
    assert!(matches!(
        request_bytes(&InvokeBody::Json(serde_json::json!({ "id": 1 }))),
        Err(Refusal::NotBytes)
    ));
}

/// The plugin's commands invoked over a mock webview's IPC, its channel
/// messages caught by the app's channel interceptor.
mod ipc {
    use std::collections::BTreeMap;
    use std::sync::mpsc as std_mpsc;

    use tauri::ipc::{CallbackFn, InvokeResponseBody, RuntimeAuthority};
    use tauri::test::{
        INVOKE_KEY, MockRuntime, get_ipc_response, mock_builder, mock_context, noop_assets,
    };
    use tauri::utils::acl::ExecutionContext;
    use tauri::utils::acl::resolved::{Resolved, ResolvedCommand, ResolvedCommandReference};
    use tauri::webview::InvokeRequest;
    use tauri::{App, WebviewUrl, WebviewWindow, WebviewWindowBuilder};

    use super::*;

    /// The channel id the page's request names.
    const CHANNEL: u32 = 7;

    /// A mock app running the plugin over `source`, the window `main`, and
    /// the messages its channels carry, each beside its channel id.
    struct Running {
        _app: App<MockRuntime>,
        window: WebviewWindow<MockRuntime>,
        messages: std_mpsc::Receiver<(u32, FeedMessage)>,
    }

    fn run(source: &Scripted) -> Running {
        let mut context = mock_context(noop_assets());
        let mut allowed = Resolved::default();
        for command in ["subscribe", "stop", "ack"] {
            allowed.allowed_commands.insert(
                format!("plugin:connetto|{command}"),
                vec![ResolvedCommand::new(
                    ExecutionContext::Local,
                    ResolvedCommandReference::new("test".to_owned(), "connetto:default".to_owned()),
                    vec![glob::Pattern::new("*").expect("a pattern")],
                    Vec::new(),
                    None,
                )],
            );
        }
        *context.runtime_authority_mut() = RuntimeAuthority::new(BTreeMap::new(), allowed);
        let (plugin, slot) = crate::init(
            Registry::new()
                .register_keyed::<Keyed>()
                .register::<Whole>(),
        );
        slot.set(Some(source.clone()));
        let (sender, messages) = std_mpsc::channel();
        let app = mock_builder()
            .plugin(plugin)
            .channel_interceptor(move |_webview, callback, _index, body| {
                if let InvokeResponseBody::Raw(bytes) = body {
                    let message = decode(bytes).expect("the plugin sends feed messages");
                    let _ = sender.send((callback.0, message));
                }
                true
            })
            .build(context)
            .expect("the mock app builds");
        let window = WebviewWindowBuilder::new(&app, "main", WebviewUrl::default())
            .build()
            .expect("the mock window builds");
        Running {
            _app: app,
            window,
            messages,
        }
    }

    impl Running {
        fn invoke(&self, command: &str, body: InvokeBody) -> Result<(), serde_json::Value> {
            get_ipc_response(
                &self.window,
                InvokeRequest {
                    cmd: format!("plugin:connetto|{command}"),
                    callback: CallbackFn(0),
                    error: CallbackFn(1),
                    url: if cfg!(windows) {
                        "http://tauri.localhost"
                    } else {
                        "tauri://localhost"
                    }
                    .parse()
                    .expect("a url"),
                    body,
                    headers: tauri::http::HeaderMap::default(),
                    invoke_key: INVOKE_KEY.to_owned(),
                },
            )
            .map(drop)
        }

        fn subscribe<Spec: LiveSpec>(&self, id: u64, spec: &Spec) -> Result<(), serde_json::Value> {
            let request = SubscribeRequest {
                id,
                name: Spec::NAME.to_owned(),
                descriptor: ByteBuf::from(encode(spec).expect("specs encode")),
                channel: format!("__CHANNEL__:{CHANNEL}"),
            };
            self.invoke(
                "subscribe",
                InvokeBody::Raw(encode(&request).expect("requests encode")),
            )
        }

        fn next(&self) -> FeedMessage {
            let (channel, message) = self
                .messages
                .recv_timeout(BOUND)
                .expect("a message arrives");
            assert_eq!(channel, CHANNEL, "the message goes to the page's channel");
            message
        }
    }

    #[test]
    fn the_commands_follow_a_spec_over_the_pages_channel() {
        let source = Scripted::default();
        source.publish::<Keyed>(&[item(1, "a")]);
        let running = run(&source);

        running.subscribe(1, &Keyed).expect("keyed is registered");
        assert!(matches!(running.next().body, FeedBody::Keyed(rows) if rows.len() == 1));
        running
            .invoke(
                "ack",
                InvokeBody::Json(serde_json::json!({ "id": 1, "seq": 1 })),
            )
            .expect("ack is allowed");
        source.publish::<Keyed>(&[item(1, "a"), item(2, "b")]);
        let message = running.next();
        assert_eq!(message.seq, 2);
        assert!(matches!(message.body, FeedBody::Diff(_)), "{message:?}");

        running
            .invoke("stop", InvokeBody::Json(serde_json::json!({ "id": 1 })))
            .expect("stop is allowed");
        tauri::async_runtime::block_on(source.unfollowed::<Keyed>());
    }

    #[test]
    fn subscribing_to_an_unregistered_spec_is_refused_over_ipc() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Other;

        impl LiveSpec for Other {
            const NAME: &'static str = "other";
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

        let running = run(&Scripted::default());

        assert_eq!(
            running.subscribe(1, &Other),
            Err(serde_json::Value::String(
                Refusal::Unregistered("other".to_owned()).to_string()
            ))
        );
    }
}
