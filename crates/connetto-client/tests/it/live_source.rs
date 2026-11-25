//! The loop every framework hook runs over a live source (R116).

use connetto_client::{
    ClientBuilder, ClientError, ClientEvent, ConnettoClient, DataDir, LiveQuery, LiveSpec,
    MemoryRefreshStore, SyncSchema, Watchable, follow,
};
use connetto_core::schema::SchemaBundle;
use connetto_core::traits::{MaybeSend, Transport};
use diesel::prelude::*;
use tempfile::{TempDir, tempdir};
use tokio::sync::{mpsc, oneshot};

use super::support::{NeverDial, Script};

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)";
/// A device-private tier, so the test can add a table the drain refuses.
const TIER_DDL: &str = "CREATE TABLE drafts (id INTEGER PRIMARY KEY, body TEXT)";

/// The bound on every wait, which only detects a hang.
const BOUND: core::time::Duration = core::time::Duration::from_secs(5);

diesel::table! {
    /// Synced test table.
    items (id) {
        /// Item identifier, the primary key
        id -> Integer,
        /// Optional item label
        label -> Nullable<Text>,
    }
}

#[derive(Queryable, Selectable, Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = items)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Item {
    id: i32,
    label: Option<String>,
}

/// Every item, by id.
#[derive(serde::Serialize, serde::Deserialize)]
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

/// A running client that never reaches a server, with its pump spawned.
async fn offline_client(dir: &TempDir) -> ConnettoClient<Script> {
    let credential = super::support::held("tester");
    let store = super::support::key_store(&credential).await;
    let schema = SyncSchema::new(
        SchemaBundle::new(
            "-- connetto:major 1",
            "",
            connetto_core::ReplicaCopy::new(
                DDL,
                Vec::<(String, String)>::new(),
                Vec::<String>::new(),
            ),
            Some(TIER_DDL),
        )
        .expect("the source declares major 1"),
    );
    let conn = ClientBuilder::new(schema, NeverDial::<Script>::default())
        .signed_in(credential)
        .durable(
            DataDir::new(dir.path().to_path_buf()),
            store,
            MemoryRefreshStore::default(),
        )
        .open_driven()
        .await
        .expect("open with no server");
    let (client, pump) = ConnettoClient::with_pump(conn);
    tokio::spawn(pump);
    client
}

/// Follow every item on its own task, its snapshots and its ending error on
/// the returned channels.
fn spawn_follow(
    client: &ConnettoClient<Script>,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::UnboundedReceiver<Vec<Item>>,
    oneshot::Receiver<ClientError>,
) {
    let (snapshot_sender, snapshots) = mpsc::unbounded_channel();
    let (failure_sender, failure) = oneshot::channel();
    let following = tokio::spawn(follow(
        client.clone(),
        AllItems,
        move |rows| {
            snapshot_sender
                .send(rows)
                .expect("the test holds the receiver");
        },
        move |error| {
            failure_sender
                .send(error)
                .expect("the test holds the receiver");
        },
    ));
    (following, snapshots, failure)
}

async fn next_snapshot(snapshots: &mut mpsc::UnboundedReceiver<Vec<Item>>) -> Vec<Item> {
    tokio::time::timeout(BOUND, snapshots.recv())
        .await
        .expect("a snapshot within the bound")
        .expect("the loop holds the sender")
}

/// The loop writes the first snapshot and each change, then the error that
/// ends the feed, and returns.
#[tokio::test]
async fn follow_writes_each_snapshot_then_the_error_that_ends_the_feed() {
    let dir = tempdir().expect("temp dir");
    let client = offline_client(&dir).await;
    let (following, mut snapshots, failure) = spawn_follow(&client);

    assert_eq!(next_snapshot(&mut snapshots).await, Vec::new());
    client
        .with_conn(|conn| {
            diesel::insert_into(items::table)
                .values((items::id.eq(1), items::label.eq("first")))
                .execute(conn.conn())
        })
        .await
        .expect("gate not locked")
        .expect("insert");
    assert_eq!(
        next_snapshot(&mut snapshots).await,
        vec![Item {
            id: 1,
            label: Some("first".to_owned()),
        }]
    );

    // A keyless tier table stops the pump at its next drain, as a fault does.
    client
        .with_conn(|conn| {
            diesel::connection::SimpleConnection::batch_execute(
                conn.conn(),
                "CREATE TABLE connetto_local.loose (label TEXT)",
            )
        })
        .await
        .expect("gate not locked")
        .expect("the application creates its own table");
    let ended = tokio::time::timeout(BOUND, failure)
        .await
        .expect("the ending error within the bound")
        .expect("the loop writes the ending error");
    assert!(
        matches!(ended, ClientError::Stopped(_)),
        "the stopped pump ends the feed: {ended:?}"
    );
    tokio::time::timeout(BOUND, following)
        .await
        .expect("the loop returns within the bound")
        .expect("the loop does not panic");
    assert!(
        snapshots.recv().await.is_none(),
        "nothing is written after the ending error"
    );
}

/// Dropping the loop, as a framework does when its component goes, releases
/// the watch it followed.
#[tokio::test]
async fn dropping_the_loop_releases_its_watch() {
    let dir = tempdir().expect("temp dir");
    let client = offline_client(&dir).await;
    let mut events = client.events();
    let (following, mut snapshots, _failure) = spawn_follow(&client);
    next_snapshot(&mut snapshots).await;
    let declared: Vec<String> = client
        .with_conn(|conn| conn.declared_subscriptions().expect("declared"))
        .await
        .expect("gate not locked")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let [watch] = declared.as_slice() else {
        panic!("the loop declares one watch: {declared:?}");
    };

    following.abort();
    let released = tokio::time::timeout(BOUND, async {
        loop {
            if let ClientEvent::WatchReleased { sub_id } = events.recv().await.expect("events") {
                return sub_id;
            }
        }
    })
    .await
    .expect("a watch released within the bound");
    assert_eq!(&released, watch);
}
