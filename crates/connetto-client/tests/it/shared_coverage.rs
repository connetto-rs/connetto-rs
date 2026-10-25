//! One identity's passes over a shared table keep what another identity on
//! the device still covers or has unsent.

use std::time::Duration;

use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection, DataDir, SyncSchema};
use connetto_core::Cursor;
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, LivePatch, SnapshotPatch,
    SubscriptionSpec,
};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::IncomingFrame;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_diff_rs::{DiffOps, Indirect, Insert, PatchDelete, PatchSet, SimpleTable, Value};

const SCHEMA: &str = "CREATE TABLE tags (id INT PRIMARY KEY, label TEXT NOT NULL);";

fn schema() -> SyncSchema {
    SyncSchema::new(
        connetto_schema::translate_shared::<String, connetto_core::bots::TextIds>(
            SCHEMA,
            "",
            &["tags"],
        )
        .expect("translate the shared table"),
    )
}

fn table() -> SimpleTable {
    SimpleTable::new("tags", &[("id", ""), ("label", "")], &[0])
}

/// The compressed patchset inserting a `tags` row for each id.
fn rows(ids: &[i64]) -> Vec<u8> {
    let mut patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for id in ids {
        let insert = Insert::<_, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(*id))
            .unwrap()
            .set(1, Value::Text(format!("tag-{id}")))
            .unwrap();
        patchset = patchset.insert(insert);
    }
    zstd::encode_all(patchset.build().as_slice(), 3).unwrap()
}

/// The compressed patchset of a departure: indirect deletes of each id.
fn departures(ids: &[i64]) -> Vec<u8> {
    let mut patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for id in ids {
        patchset = patchset.delete(PatchDelete::new(table(), vec![(*id).into()]).indirect(true));
    }
    zstd::encode_all(patchset.build().as_slice(), 3).unwrap()
}

fn snapshot(sub: &str, ids: &[i64]) -> IncomingFrame {
    IncomingFrame::Bulk(BulkMessage::SnapshotPatch(SnapshotPatch::new(
        sub.to_owned(),
        rows(ids),
    )))
}

/// `owner`'s connection over the shared device directory, delivering
/// `frames` once connected.
async fn connect(
    directory: &std::path::Path,
    keys: super::support::SharedKeys,
    owner: &str,
    frames: Vec<IncomingFrame>,
) -> ConnettoConnection<FakeTransport> {
    ClientBuilder::new(
        schema(),
        super::support::Once::new(FakeTransport::accepting_then_delivering(frames)),
    )
    .signed_in(super::support::held(owner))
    .durable(
        DataDir::new(directory.to_path_buf()),
        keys,
        connetto_client::MemoryRefreshStore::default(),
    )
    .connect_driven()
    .await
    .expect("connect the account")
}

/// Subscribe `sub` to `query` and pump until its snapshot lands.
async fn subscribe(client: &mut ConnettoConnection<FakeTransport>, sub: &str, query: &str) {
    client
        .subscribe_spec(sub, SubscriptionSpec::new(query))
        .await
        .expect("subscribe");
    for _ in 0..8 {
        if matches!(
            client.pump_one().await.expect("pump"),
            ClientEvent::SnapshotApplied { .. }
        ) {
            return;
        }
    }
    panic!("the snapshot was not applied");
}

/// Pump until the scripted server hangs up.
async fn drain(client: &mut ConnettoConnection<FakeTransport>) {
    while !matches!(client.pump_one().await.expect("pump"), ClientEvent::Closed) {}
}

#[derive(QueryableByName)]
struct Id {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
}

fn ids(client: &mut ConnettoConnection<FakeTransport>) -> Vec<i64> {
    diesel::sql_query("SELECT id FROM tags ORDER BY id")
        .load::<Id>(client.conn())
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect()
}

#[tokio::test]
async fn a_tidy_keeps_the_rows_another_identity_covers_until_it_unsubscribes() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags WHERE id = 1").await;
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 2").await;
    bob.tidy().unwrap();
    assert_eq!(ids(&mut bob), [1, 2], "alice's row survives bob's tidy");
    alice.unsubscribe("a").await.unwrap();
    assert_eq!(
        ids(&mut alice),
        [2],
        "alice's eviction drops her row and keeps bob's"
    );
}

#[tokio::test]
async fn a_released_watch_stops_covering_once_its_grace_runs_out() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    alice
        .subscribe_spec_with_grace(
            "a",
            SubscriptionSpec::new("SELECT * FROM tags WHERE id = 1"),
            Duration::ZERO,
        )
        .await
        .unwrap();
    while !matches!(
        alice.pump_one().await.unwrap(),
        ClientEvent::SnapshotApplied { .. }
    ) {}
    alice.release_subscription("a").unwrap();
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 2").await;
    bob.tidy().unwrap();
    assert_eq!(ids(&mut bob), [2], "a lapsed watch keeps nothing");
}

#[tokio::test]
async fn a_history_resync_keeps_the_rows_another_identity_covers() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags WHERE id = 1").await;
    let mut bob = connect(
        directory.path(),
        keys,
        "bob",
        vec![
            snapshot("b", &[2]),
            IncomingFrame::Control(ControlMessage::FullResyncRequired(FullResyncRequired {
                sub_id: "b".to_owned(),
                reason: FullResyncReason::CursorBeyondHistory,
            })),
        ],
    )
    .await;
    subscribe(&mut bob, "b", "SELECT * FROM tags").await;
    drain(&mut bob).await;
    assert_eq!(
        ids(&mut bob),
        [1],
        "bob's clear empties his rows and keeps alice's"
    );
}

#[tokio::test]
async fn a_tidy_keeps_a_shared_row_another_identity_has_not_uploaded() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys.clone(), "alice", Vec::new()).await;
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (3, 'unsent')")
        .unwrap();
    assert!(alice.push().await.unwrap().is_some(), "the write is queued");
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 2").await;
    bob.tidy().unwrap();
    assert_eq!(ids(&mut bob), [2, 3], "alice's unsent row survives");
}

#[tokio::test]
async fn a_departure_another_identity_still_covers_is_withheld() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags WHERE id = 1").await;
    let mut bob = connect(
        directory.path(),
        keys,
        "bob",
        vec![
            snapshot("b", &[1, 2]),
            IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
                "b".to_owned(),
                Cursor::new(vec![1]),
                departures(&[1, 2]),
            ))),
        ],
    )
    .await;
    subscribe(&mut bob, "b", "SELECT * FROM tags").await;
    drain(&mut bob).await;
    assert_eq!(
        ids(&mut bob),
        [1],
        "the row alice covers stays, the other leaves"
    );
}

#[tokio::test]
async fn a_tidy_keeps_every_row_of_a_table_another_identity_wants_whole() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1, 3])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 2").await;
    bob.tidy().unwrap();
    assert_eq!(ids(&mut bob), [1, 2, 3], "alice wants the table whole");
}

#[tokio::test]
async fn a_departure_from_a_table_another_identity_wants_whole_is_withheld() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    let mut bob = connect(
        directory.path(),
        keys,
        "bob",
        vec![
            snapshot("b", &[1]),
            IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
                "b".to_owned(),
                Cursor::new(vec![1]),
                departures(&[1]),
            ))),
        ],
    )
    .await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 1").await;
    drain(&mut bob).await;
    assert_eq!(ids(&mut bob), [1], "alice still wants the row");
}
