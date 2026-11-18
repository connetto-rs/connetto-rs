//! The Leptos hook in a browser page, over an in-memory client connected to
//! a scripted server (R116).

#![cfg(target_arch = "wasm32")]

use core::future::Future;
use core::pin::pin;

use connetto_client::{
    ClientBuilder, ClientEvent, ConnettoClient, Grant, HeldCredential, LiveGroups, LiveQuery,
    LiveSpec, LiveValue, SyncSchema, Watchable,
};
use connetto_core::messages::{
    AggregateUpdate, ControlMessage, HandshakeAck, Subscribe, WriterGrant,
};
use connetto_core::schema::SchemaBundle;
use connetto_core::traits::{IncomingFrame, MaybeSend, Transport};
use connetto_core::{Cursor, LoopbackError, LoopbackTransport, loopback};
use connetto_leptos::{keyed, use_live};
use diesel::prelude::*;
use futures_channel::mpsc;
use futures_util::StreamExt;
use futures_util::future::{Either, select};
use js_sys::{Array, Promise};
use leptos::mount::mount_to;
use leptos::prelude::*;
// diesel's prelude carries a `view!` of its own.
use leptos::view;
use send_wrapper::SendWrapper;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{Element, HtmlElement, MutationObserver, MutationObserverInit};

wasm_bindgen_test_configure!(run_in_browser);

const DDL: &str =
    "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL, quantity INTEGER NOT NULL)";

/// The bound on every wait in milliseconds, which only detects a hang.
const BOUND_MS: i32 = 10_000;

diesel::table! {
    /// Synced test table.
    items (id) {
        /// Item identifier, the primary key
        id -> Integer,
        /// Item label
        label -> Text,
        /// How many of the item there are
        quantity -> Integer,
    }
}

#[derive(Queryable, Selectable, Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = items)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Item {
    id: i32,
    label: String,
    quantity: i32,
}

/// Every item, by id.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct AllItems;

impl LiveSpec for AllItems {
    const NAME: &'static str = "all_items";
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

/// How many items there are.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ItemCount;

impl LiveSpec for ItemCount {
    const NAME: &'static str = "item_count";
    type Row = i64;
    type Handle = LiveValue<i64>;

    fn query<T>(&self) -> impl Watchable<T, i64, Handle = LiveValue<i64>> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        items::table.count()
    }
}

/// How many items there are of each quantity.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ItemsPerQuantity;

impl LiveSpec for ItemsPerQuantity {
    const NAME: &'static str = "items_per_quantity";
    type Row = (i32, i64);
    type Handle = LiveGroups<i32, i64>;

    fn query<T>(&self) -> impl Watchable<T, (i32, i64), Handle = LiveGroups<i32, i64>> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        items::table
            .group_by(items::quantity)
            .select((items::quantity, diesel::dsl::count_star()))
    }
}

/// The server end of a client's connection, which answers the handshake,
/// reports each subscription and pushes what the test says.
struct Server {
    subscribed: mpsc::UnboundedReceiver<Subscribe>,
    say: mpsc::UnboundedSender<ControlMessage>,
}

impl Server {
    /// The next `count` subscriptions the client opened.
    async fn subscriptions(&mut self, count: usize) -> Vec<Subscribe> {
        let mut subscribed = Vec::with_capacity(count);
        let heard = bounded(async {
            while subscribed.len() < count {
                let next = self.subscribed.next().await;
                subscribed.push(next.expect("the server is running"));
            }
        })
        .await;
        assert!(
            heard.is_some(),
            "{count} subscriptions within the bound, heard {}",
            subscribed.len()
        );
        subscribed
    }

    /// Push `message` to the client.
    fn say(&self, message: ControlMessage) {
        self.say
            .unbounded_send(message)
            .expect("the server is running");
    }
}

/// What the server does next.
enum Next {
    Heard(Result<Option<IncomingFrame>, LoopbackError>),
    Say(Option<ControlMessage>),
}

/// The scripted server's loop over its end of the pair.
async fn serve(
    mut wire: LoopbackTransport,
    subscribed: mpsc::UnboundedSender<Subscribe>,
    mut said: mpsc::UnboundedReceiver<ControlMessage>,
) {
    loop {
        let next = match select(pin!(wire.recv()), said.next()).await {
            Either::Left((frame, _)) => Next::Heard(frame),
            Either::Right((message, _)) => Next::Say(message),
        };
        let answer = match next {
            Next::Heard(Ok(Some(IncomingFrame::Control(ControlMessage::Handshake(_))))) => {
                ControlMessage::HandshakeAck(HandshakeAck {
                    connection_id: "leptos".to_owned(),
                    session_token: "leptos".to_owned(),
                    resume_token: "leptos".to_owned(),
                    current_cursor: Cursor::new(Vec::new()),
                    schema_version: None,
                    sunset_at_secs: None,
                    courier: false,
                    initial_credits: 64,
                    writer: Some(WriterGrant {
                        number: 1,
                        frontier: 0,
                    }),
                })
            }
            Next::Heard(Ok(Some(IncomingFrame::Control(ControlMessage::Subscribe(subscribe))))) => {
                let _ = subscribed.unbounded_send(subscribe);
                continue;
            }
            Next::Heard(Ok(Some(_))) => continue,
            Next::Say(Some(message)) => message,
            Next::Heard(_) | Next::Say(None) => return,
        };
        if wire.send_control(answer).await.is_err() {
            return;
        }
    }
}

/// A running in-memory client connected to a scripted server.
async fn connected_client() -> (ConnettoClient<LoopbackTransport>, Server) {
    let schema = SyncSchema::new(
        SchemaBundle::new(
            "-- connetto:major 1",
            "",
            DDL,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares major 1"),
    );
    let credential = HeldCredential::new(Grant::new("user:browser"), "browser")
        .expect("a string identity serializes");
    let (wire, client_end) = loopback();
    let (subscribed_sender, subscribed) = mpsc::unbounded();
    let (say, said) = mpsc::unbounded();
    spawn_local(serve(wire, subscribed_sender, said));
    let mut end = Some(client_end);
    let connection = ClientBuilder::new(schema, move || {
        core::future::ready(end.take().ok_or("the pair is dialed once"))
    })
    .signed_in(credential)
    .connect_driven()
    .await
    .expect("connect to the scripted server");
    let (client, pump) = ConnettoClient::with_pump(connection);
    spawn_local(pump);
    (client, Server { subscribed, say })
}

/// The id of the one subscription whose query `pick` accepts.
fn sub_id_of(subscribed: &[Subscribe], pick: impl Fn(&str) -> bool) -> String {
    let mut picked = subscribed
        .iter()
        .filter(|subscribe| pick(&subscribe.spec.query));
    let id = picked
        .next()
        .expect("a subscription matches")
        .sub_id
        .clone();
    assert!(picked.next().is_none(), "one subscription matches");
    id
}

/// The server's push of the scalar `value`.
fn scalar(sub_id: &str, value: i64) -> ControlMessage {
    ControlMessage::AggregateUpdate(AggregateUpdate {
        sub_id: sub_id.to_owned(),
        group_key: None,
        group_values_json: None,
        result_json: Some(value.to_string()),
        is_full_result: true,
    })
}

/// The server's push of `count` items of `quantity`, the only group.
fn group(sub_id: &str, quantity: i32, count: i64) -> ControlMessage {
    ControlMessage::AggregateUpdate(AggregateUpdate {
        sub_id: sub_id.to_owned(),
        group_key: Some(quantity.to_be_bytes().to_vec()),
        group_values_json: Some(format!("[{quantity}]")),
        result_json: Some(count.to_string()),
        is_full_result: true,
    })
}

/// Write `item`, or overwrite the item with its id.
async fn put(client: &ConnettoClient<LoopbackTransport>, item: Item) {
    client
        .with_conn(move |conn| {
            diesel::replace_into(items::table)
                .values((
                    items::id.eq(item.id),
                    items::label.eq(item.label),
                    items::quantity.eq(item.quantity),
                ))
                .execute(conn.conn())
        })
        .await
        .expect("gate not locked")
        .expect("write the item");
}

fn item(id: i32, label: &str, quantity: i32) -> Item {
    Item {
        id,
        label: label.to_owned(),
        quantity,
    }
}

/// A fresh element in the page body to mount into.
fn host() -> HtmlElement {
    let document = web_sys::window()
        .and_then(|window| window.document())
        .expect("a page document");
    let host: HtmlElement = document
        .create_element("div")
        .expect("create the host")
        .unchecked_into();
    document
        .body()
        .expect("a page body")
        .append_child(&host)
        .expect("attach the host");
    host
}

/// `work`'s output, or `None` once the bound passes.
async fn bounded<Output>(work: impl Future<Output = Output>) -> Option<Output> {
    let expired = Promise::new(&mut |resolve, _| {
        web_sys::window()
            .expect("a window")
            .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, BOUND_MS)
            .expect("arm the bound");
    });
    match select(pin!(work), JsFuture::from(expired)).await {
        Either::Left((output, _)) => Some(output),
        Either::Right(_) => None,
    }
}

/// The next change under `element`, as the DOM reports it.
fn next_mutation(element: &Element) -> JsFuture {
    JsFuture::from(Promise::new(&mut |resolve, _| {
        let callback = Closure::once_into_js(move |_: Array, observer: MutationObserver| {
            observer.disconnect();
            let _ = resolve.call0(&JsValue::NULL);
        });
        let observer =
            MutationObserver::new(callback.unchecked_ref()).expect("create the observer");
        let options = MutationObserverInit::new();
        options.set_subtree(true);
        options.set_child_list(true);
        options.set_character_data(true);
        observer
            .observe_with_options(element, &options)
            .expect("observe the host");
    }))
}

/// Wait until the text under `element` reads `expected`.
async fn until_text(element: &Element, expected: &str) {
    let reached = bounded(async {
        while element.text_content().unwrap_or_default() != expected {
            next_mutation(element).await.expect("the observer resolves");
        }
    })
    .await;
    assert!(
        reached.is_some(),
        "the page reads {expected:?} within the bound, it reads {:?}",
        element.text_content().unwrap_or_default()
    );
}

/// The hook shows a row query, a scalar and a group from their first
/// snapshot, and follows a later write into each.
#[wasm_bindgen_test]
async fn use_live_follows_rows_scalars_and_groups() {
    let (client, mut server) = connected_client().await;
    put(&client, item(1, "first", 2)).await;
    let host = host();
    let source = client.clone();
    let _mounted = mount_to(host.clone(), move || {
        let rows = use_live(&source, AllItems);
        let count = use_live(&source, ItemCount);
        let groups = use_live(&source, ItemsPerQuantity);
        view! {
            <p>{move || rows.value().with(|rows| {
                rows.iter().map(|row| row.label.as_str()).collect::<Vec<_>>().join(",")
            })}</p>
            <p>{move || count.value().get().map_or("none".to_owned(), |n| n.to_string())}</p>
            <p>{move || groups.value().with(|groups| {
                let mut pairs: Vec<_> = groups.iter().map(|(q, n)| format!("{q}x{n}")).collect();
                pairs.sort();
                pairs.join(",")
            })}</p>
        }
    });

    let subscribed = server.subscriptions(3).await;
    let count_sub = sub_id_of(&subscribed, |query| {
        query.contains("COUNT") && !query.contains("GROUP BY")
    });
    let groups_sub = sub_id_of(&subscribed, |query| query.contains("GROUP BY"));
    server.say(scalar(&count_sub, 1));
    server.say(group(&groups_sub, 2, 1));
    until_text(&host, "first12x1").await;

    put(&client, item(2, "second", 2)).await;
    server.say(scalar(&count_sub, 2));
    server.say(group(&groups_sub, 2, 2));
    until_text(&host, "first,second22x2").await;
}

/// Removing the component that called the hook releases its watch.
#[wasm_bindgen_test]
async fn unmounting_the_component_releases_its_watch() {
    let (client, _server) = connected_client().await;
    put(&client, item(1, "first", 2)).await;
    let host = host();
    let (shown, set_shown) = signal(true);
    let source = SendWrapper::new(client.clone());
    let _mounted = mount_to(host.clone(), move || {
        move || {
            shown.get().then(|| {
                let rows = use_live(&*source, AllItems);
                view! { <p>{move || rows.value().with(Vec::len)}</p> }
            })
        }
    });
    until_text(&host, "1").await;

    let mut events = client.events();
    set_shown.set(false);
    let released = bounded(async {
        while !matches!(
            events.recv().await.expect("the client is running"),
            ClientEvent::WatchReleased { .. }
        ) {}
    })
    .await;
    assert!(released.is_some(), "the watch is released within the bound");
}

/// A keyed list keeps the DOM node of a row whose cell changed and updates
/// the cell in place.
#[wasm_bindgen_test]
async fn a_keyed_row_keeps_its_node_when_a_cell_changes() {
    let (client, _server) = connected_client().await;
    put(&client, item(1, "first", 2)).await;
    put(&client, item(2, "second", 3)).await;
    let host = host();
    let source = client.clone();
    let _mounted = mount_to(host.clone(), move || {
        let rows = use_live(&source, AllItems);
        let rows = keyed(rows.value(), |row: &Item| row.id);
        view! {
            <ul>
                <For
                    each=move || rows.get()
                    key=|(id, _)| *id
                    children=|(_, row)| view! { <li>{move || row.with(|row| row.label.clone())}</li> }
                />
            </ul>
        }
    });
    until_text(&host, "firstsecond").await;
    let before = host
        .query_selector("li")
        .expect("a valid selector")
        .expect("the first row's node");

    put(&client, item(1, "edited", 2)).await;
    until_text(&host, "editedsecond").await;
    let after = host
        .query_selector("li")
        .expect("a valid selector")
        .expect("the first row's node");
    assert!(
        after.is_same_node(Some(&before)),
        "the edited row keeps its node"
    );
}
