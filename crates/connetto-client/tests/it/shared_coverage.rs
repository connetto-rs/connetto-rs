//! One identity's passes over a shared table keep what another identity on
//! the device still covers or has unsent.

use std::time::Duration;

use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection, DataDir, SyncSchema};
use connetto_core::Cursor;
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, LivePatch, MutationApplied,
    MutationReject, MutationRejectReason, SnapshotEnd, SnapshotPatch, SubscriptionSpec,
};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::IncomingFrame;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_diff_rs::{DiffOps, Indirect, Insert, PatchDelete, PatchSet, SimpleTable, Value};

const SCHEMA: &str = "-- connetto:major 1
CREATE TABLE tags (id INT PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE marks (name TEXT, code BYTEA, weight DOUBLE PRECISION, PRIMARY KEY (name, code, weight));";

pub(super) fn schema() -> SyncSchema {
    SyncSchema::new(
        connetto_schema::translate_shared::<String, connetto_core::bots::TextIds>(
            SCHEMA,
            "",
            &["tags", "marks"],
        )
        .expect("translate the shared tables"),
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

pub(super) fn snapshot(sub: &str, ids: &[i64]) -> IncomingFrame {
    IncomingFrame::Bulk(BulkMessage::SnapshotPatch(SnapshotPatch::new(
        sub.to_owned(),
        rows(ids),
    )))
}

/// `owner`'s connection over the shared device directory, delivering
/// `frames` once connected.
pub(super) async fn connect(
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
pub(super) async fn subscribe(
    client: &mut ConnettoConnection<FakeTransport>,
    sub: &str,
    query: &str,
) {
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

pub(super) fn ids(client: &mut ConnettoConnection<FakeTransport>) -> Vec<i64> {
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
            IncomingFrame::Control(ControlMessage::SnapshotEnd(SnapshotEnd {
                sub_id: "b".to_owned(),
                cursor: Cursor::new(vec![2]),
            })),
        ],
    )
    .await;
    subscribe(&mut bob, "b", "SELECT * FROM tags").await;
    drain(&mut bob).await;
    assert_eq!(
        ids(&mut bob),
        [1],
        "bob's empty replacement takes his rows and keeps alice's"
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

/// Make the shared store refuse `op` on `table`, as a full disk or a damaged
/// store would.
fn refuse(client: &mut ConnettoConnection<FakeTransport>, op: &str, table: &str) {
    client
        .conn()
        .batch_execute(&format!(
            "CREATE TEMP TRIGGER refuse_{op}_{table} BEFORE {op} ON connetto_shared.{table} \
             BEGIN SELECT RAISE(ABORT, 'the store refuses'); END"
        ))
        .unwrap();
}

/// Let the shared store accept `op` on `table` again.
fn accept(client: &mut ConnettoConnection<FakeTransport>, op: &str, table: &str) {
    client
        .conn()
        .batch_execute(&format!("DROP TRIGGER temp.refuse_{op}_{table}"))
        .unwrap();
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

fn subscriptions(client: &mut ConnettoConnection<FakeTransport>) -> i64 {
    diesel::sql_query("SELECT count(*) AS n FROM _connetto_subscription")
        .get_result::<Count>(client.conn())
        .unwrap()
        .n
}

/// Ending a subscription releases its holdings with the record, so a store
/// that refuses the release keeps the subscription.
#[tokio::test]
async fn an_unsubscribe_whose_holdings_cannot_be_released_keeps_the_subscription() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys, "alice", vec![snapshot("a", &[1])]).await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    refuse(&mut alice, "DELETE", "_connetto_shared_holding");
    alice
        .unsubscribe("a")
        .await
        .expect_err("the store refuses the release");
    assert_eq!(subscriptions(&mut alice), 1, "the subscription stays");
}

/// A push whose unsent holdings the store refuses still uploads the write
/// once, and the next push queues nothing more of it.
#[tokio::test]
async fn a_write_whose_unsent_holding_cannot_be_recorded_is_queued_once() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut bob = connect(directory.path(), keys, "bob", Vec::new()).await;
    bob.conn()
        .batch_execute("INSERT INTO tags VALUES (5, 'unsent')")
        .unwrap();
    refuse(&mut bob, "INSERT", "_connetto_shared_holding");
    let refused = bob.push().await;
    accept(&mut bob, "INSERT", "_connetto_shared_holding");
    let retried = bob.push().await.expect("the store accepts again");
    assert_eq!(
        bob.unsynced().len(),
        1,
        "the write is queued exactly once ({refused:?}, then {retried:?})"
    );
}

/// A pass that cannot read what the other identities hold evicts nothing.
#[tokio::test]
async fn a_tidy_that_cannot_read_the_other_identities_evicts_nothing() {
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
    let mut bob = connect(directory.path(), keys, "bob", Vec::new()).await;
    bob.conn()
        .batch_execute("DROP TABLE connetto_shared._connetto_shared_holding")
        .unwrap();
    bob.tidy()
        .expect_err("the pass cannot read what others hold");
    assert_eq!(ids(&mut bob), [1], "alice's row stays");
}

/// An aggregate rests apart from the rows it counts, so it keeps none of
/// them for its identity.
#[tokio::test]
async fn an_aggregate_subscription_covers_no_shared_row() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys.clone(), "alice", Vec::new()).await;
    alice
        .subscribe_spec("count", SubscriptionSpec::new("SELECT count(*) FROM tags"))
        .await
        .expect("subscribe the aggregate");
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[1])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags").await;
    bob.unsubscribe("b").await.unwrap();
    assert!(ids(&mut alice).is_empty(), "nothing keeps the row");
}

/// `owner`'s replica over the shared device directory, opened with no server.
pub(super) async fn open(
    directory: &std::path::Path,
    keys: super::support::SharedKeys,
    owner: &str,
) -> Result<ConnettoConnection<FakeTransport>, connetto_client::ClientError> {
    ClientBuilder::new(
        schema(),
        super::support::NeverDial::<FakeTransport>::default(),
    )
    .signed_in(super::support::held(owner))
    .durable(
        DataDir::new(directory.to_path_buf()),
        keys,
        connetto_client::MemoryRefreshStore::default(),
    )
    .open_driven()
    .await
}

/// An identity the store cannot record, its presence or the holdings of its
/// unsent writes, does not open, so no identity runs unseen by the others'
/// passes.
#[tokio::test]
async fn an_identity_the_store_cannot_record_does_not_open() {
    for (table, op) in [
        ("_connetto_identity", "INSERT"),
        ("_connetto_shared_holding", "DELETE"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let keys = super::support::SharedKeys::default();
        {
            let mut bob = open(directory.path(), keys.clone(), "bob")
                .await
                .expect("bob opens");
            bob.conn()
                .batch_execute("INSERT INTO tags VALUES (5, 'unsent')")
                .unwrap();
            bob.push().await.expect("queue the write").expect("a seq");
        }
        let mut alice = open(directory.path(), keys.clone(), "alice")
            .await
            .expect("alice opens");
        alice
            .conn()
            .batch_execute(&format!(
                "CREATE TRIGGER connetto_shared.refuse BEFORE {op} ON {table} \
                 BEGIN SELECT RAISE(ABORT, 'the store refuses'); END"
            ))
            .unwrap();
        let refused = open(directory.path(), keys, "bob")
            .await
            .err()
            .expect("bob cannot record himself, so he does not open");
        assert!(
            refused.to_string().contains("the store refuses"),
            "the {table} write is what refused, got {refused}"
        );
    }
}

/// Ending a subscription offline releases its holdings with the stop
/// moment, so a store that refuses the release keeps the subscription
/// running.
#[tokio::test]
async fn an_offline_unsubscribe_whose_holdings_cannot_be_released_keeps_the_subscription() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys, "alice", vec![snapshot("a", &[1])]).await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    drain(&mut alice).await;
    refuse(&mut alice, "DELETE", "_connetto_shared_holding");
    alice
        .unsubscribe("a")
        .await
        .expect_err("the store refuses the release");
    let running = diesel::sql_query(
        "SELECT count(*) AS n FROM _connetto_subscription WHERE stopped_at IS NULL",
    )
    .get_result::<Count>(alice.conn())
    .unwrap()
    .n;
    assert_eq!(running, 1, "the subscription has not stopped");
}

/// A departure that cannot read what the other identities hold removes
/// nothing.
#[tokio::test]
async fn a_departure_that_cannot_read_the_other_identities_removes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
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
    bob.conn()
        .batch_execute("DROP TABLE connetto_shared._connetto_shared_holding")
        .unwrap();
    bob.pump_one()
        .await
        .expect_err("the departure cannot read what others hold");
    assert_eq!(ids(&mut bob), [1, 2], "both rows stay");
}

/// Attach a server sending `frames` and pump until it hangs up or a step
/// fails, saying whether one failed.
pub(super) async fn serve(
    client: &mut ConnettoConnection<FakeTransport>,
    frames: Vec<ControlMessage>,
) -> bool {
    let attached = client
        .attach(FakeTransport::accepting_then_delivering(
            frames.into_iter().map(IncomingFrame::Control),
        ))
        .await;
    if attached.is_err() {
        return true;
    }
    loop {
        match client.pump_one().await {
            Ok(ClientEvent::Closed) => return false,
            Err(_) => return true,
            Ok(_) => {}
        }
    }
}

/// The server's answer to a write stands when the store cannot rewrite the
/// write's unsent holdings afterwards, which then over-keep until the next
/// rewrite.
#[tokio::test]
async fn a_settled_write_stays_settled_when_its_unsent_holding_cannot_be_rewritten() {
    for applied in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let keys = super::support::SharedKeys::default();
        let mut bob = open(directory.path(), keys, "bob")
            .await
            .expect("bob opens");
        bob.conn()
            .batch_execute("INSERT INTO tags VALUES (5, 'sent')")
            .unwrap();
        let client_seq = bob.push().await.expect("queue the write").expect("a seq");
        refuse(&mut bob, "DELETE", "_connetto_shared_holding");
        let answer = if applied {
            ControlMessage::MutationApplied(MutationApplied { client_seq })
        } else {
            ControlMessage::MutationReject(MutationReject {
                client_seq,
                reason: MutationRejectReason::Unauthorized,
            })
        };
        assert!(
            serve(&mut bob, vec![answer]).await,
            "applied {applied}: the unsent rewrite fails"
        );
        assert!(
            bob.unsynced().is_empty(),
            "applied {applied}: nothing waits"
        );
        assert_eq!(
            ids(&mut bob),
            if applied { vec![5] } else { Vec::new() },
            "applied {applied}: the row follows the answer"
        );
    }
}
