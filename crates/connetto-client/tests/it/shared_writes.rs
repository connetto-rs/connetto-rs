//! Local writes to shared public tables upload and roll back across schemas.

use connetto_client::{
    ClientBuilder, ClientEvent, ConnettoConnection, DataDir, ExportScope, ImportChoices, SyncSchema,
};
use connetto_core::Cursor;
use connetto_core::messages::{
    ControlMessage, FullResyncReason, FullResyncRequired, MutationReject, MutationRejectReason,
    SnapshotBegin, SnapshotEnd, SubscriptionPriority,
};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::IncomingFrame;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_diff_rs::ParsedDiffSet;

const SCHEMA: &str = "-- connetto:major 1
CREATE TABLE tags (id INT PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE colours (id INT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE notes (id INT PRIMARY KEY, body TEXT NOT NULL);";

fn schema() -> SyncSchema {
    SyncSchema::new(
        connetto_schema::translate_shared::<String, connetto_core::bots::TextIds>(
            SCHEMA,
            "",
            &["tags", "colours"],
        )
        .expect("translate the shared tables"),
    )
}

async fn open(
    directory: &std::path::Path,
    keys: super::support::SharedKeys,
    owner: &str,
) -> ConnettoConnection<FakeTransport> {
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
    .expect("open account replica offline")
}

/// Attach a server refusing `client_seq` and pump until it hangs up.
async fn refuse(
    client: &mut ConnettoConnection<FakeTransport>,
    client_seq: u64,
) -> Vec<ClientEvent> {
    client
        .attach(FakeTransport::accepting_then_delivering([
            IncomingFrame::Control(ControlMessage::MutationReject(MutationReject {
                client_seq,
                reason: MutationRejectReason::Unauthorized,
            })),
        ]))
        .await
        .expect("attach the refusing server");
    let mut seen = Vec::new();
    loop {
        match client.pump_one().await.expect("pump") {
            ClientEvent::Closed => return seen,
            event => seen.push(event),
        }
    }
}

#[derive(QueryableByName, Debug, PartialEq)]
struct Row {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    text: String,
}

#[derive(QueryableByName)]
struct Pending {
    #[diesel(sql_type = diesel::sql_types::Binary)]
    changeset: Vec<u8>,
}

fn rows(client: &mut ConnettoConnection<FakeTransport>, query: &str) -> Vec<Row> {
    diesel::sql_query(query)
        .load(client.conn())
        .expect("read rows")
}

fn tags(client: &mut ConnettoConnection<FakeTransport>) -> Vec<Row> {
    rows(client, "SELECT id, label AS text FROM tags ORDER BY id")
}

fn rejected_tables(events: &[ClientEvent]) -> Vec<String> {
    events
        .iter()
        .find_map(|event| match event {
            ClientEvent::MutationRejected { rows, .. } => {
                Some(rows.iter().map(|row| row.table.clone()).collect())
            }
            _ => None,
        })
        .expect("the refusal is reported")
}

#[tokio::test]
async fn a_rejected_write_to_a_public_table_is_undone_in_the_shared_store() {
    let directory = tempfile::tempdir().unwrap();
    let mut alice = open(
        directory.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (1, 'red')")
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), Some(1));
    let events = refuse(&mut alice, 1).await;
    assert_eq!(rejected_tables(&events), ["tags"]);
    assert!(tags(&mut alice).is_empty(), "the shared row is rolled back");
}

#[tokio::test]
async fn a_rejected_write_spanning_public_and_private_tables_is_undone_in_both() {
    let directory = tempfile::tempdir().unwrap();
    let mut alice = open(
        directory.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    alice
        .conn()
        .batch_execute(
            "BEGIN; INSERT INTO tags VALUES (1, 'red'); INSERT INTO notes VALUES (1, 'mine'); COMMIT;",
        )
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), Some(1));
    let events = refuse(&mut alice, 1).await;
    let mut tables = rejected_tables(&events);
    tables.sort();
    assert_eq!(tables, ["notes", "tags"]);
    assert!(tags(&mut alice).is_empty(), "the shared row is rolled back");
    assert!(
        rows(&mut alice, "SELECT id, body AS text FROM notes").is_empty(),
        "the private row is rolled back"
    );
}

#[tokio::test]
async fn a_write_reaches_the_queue_in_the_order_it_first_touched_each_table() {
    let directory = tempfile::tempdir().unwrap();
    let mut alice = open(
        directory.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    alice
        .conn()
        .batch_execute(
            "BEGIN;
             INSERT INTO tags VALUES (1, 'red');
             INSERT INTO notes VALUES (1, 'mine');
             INSERT INTO colours VALUES (1, 'blue');
             INSERT INTO tags VALUES (2, 'green');
             COMMIT;",
        )
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), Some(1));
    let pending: Vec<Pending> = diesel::sql_query("SELECT changeset FROM _connetto_pending")
        .load(alice.conn())
        .unwrap();
    assert_eq!(pending.len(), 1, "both captures drain into one entry");
    let parsed = ParsedDiffSet::parse(&pending[0].changeset).unwrap();
    let order: Vec<&str> = parsed
        .table_schemas()
        .into_iter()
        .map(|table| table.name().as_str())
        .collect();
    // The queue records the backing tables the translation writes through.
    assert_eq!(order, ["tags_rls", "notes_rls", "colours_rls"]);
}

#[tokio::test]
async fn a_second_identitys_later_write_survives_the_first_identitys_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = open(directory.path(), keys.clone(), "alice").await;
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (1, 'red')")
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), Some(1));
    let mut bob = open(directory.path(), keys, "bob").await;
    bob.conn()
        .batch_execute("UPDATE tags SET label = 'crimson' WHERE id = 1")
        .unwrap();
    assert_eq!(bob.push().await.unwrap(), Some(1), "bob's write is his own");
    refuse(&mut alice, 1).await;
    assert_eq!(
        tags(&mut alice),
        [Row {
            id: 1,
            text: "crimson".to_owned()
        }],
        "the row bob changed afterwards is left as he left it"
    );
}

#[tokio::test]
async fn a_queued_public_write_is_reapplied_to_the_shared_store_after_a_full_resync() {
    let directory = tempfile::tempdir().unwrap();
    let mut alice = open(
        directory.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (1, 'red')")
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), Some(1));
    let sub = "tags".to_owned();
    alice
        .attach(FakeTransport::accepting_then_delivering([
            IncomingFrame::Control(ControlMessage::FullResyncRequired(FullResyncRequired {
                sub_id: sub.clone(),
                reason: FullResyncReason::CursorOutsideRetention,
            })),
            IncomingFrame::Control(ControlMessage::SnapshotBegin(SnapshotBegin {
                sub_id: sub.clone(),
                priority: SubscriptionPriority::default(),
            })),
            IncomingFrame::Control(ControlMessage::SnapshotEnd(SnapshotEnd {
                sub_id: sub.clone(),
                cursor: Cursor::new(Vec::new()),
            })),
        ]))
        .await
        .expect("attach the resyncing server");
    alice
        .subscribe(&sub, "SELECT id, label FROM tags")
        .await
        .expect("subscribe");
    loop {
        if matches!(alice.pump_one().await.expect("pump"), ClientEvent::Closed) {
            break;
        }
    }
    assert_eq!(
        tags(&mut alice),
        [Row {
            id: 1,
            text: "red".to_owned()
        }],
        "the unsynced write survives the resync in the shared store"
    );
}

#[tokio::test]
async fn an_imported_public_write_lands_in_the_shared_store_and_the_queue() {
    let source = tempfile::tempdir().unwrap();
    let mut alice = open(
        source.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (1, 'red')")
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), Some(1));
    let archive = alice
        .export_local_data(ExportScope::Everything, Vec::new())
        .expect("export");
    let destination = tempfile::tempdir().unwrap();
    let mut restored = open(
        destination.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    let plan = restored
        .import_local_data(std::io::Cursor::new(archive))
        .expect("read the archive");
    let outcome = restored
        .apply_import(&plan, &ImportChoices::keeping_the_file())
        .expect("restore");
    assert_eq!(outcome.writes_restored, 1);
    assert_eq!(
        tags(&mut restored),
        [Row {
            id: 1,
            text: "red".to_owned()
        }],
        "the restored write is applied to the shared store"
    );
    assert_eq!(restored.unsynced().len(), 1, "and queued to go up again");
}

#[tokio::test]
async fn a_private_write_with_sharing_on_queues_only_its_own_table() {
    let directory = tempfile::tempdir().unwrap();
    let mut alice = open(
        directory.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    alice
        .conn()
        .batch_execute("INSERT INTO notes VALUES (1, 'mine')")
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), Some(1));
    let pending: Vec<Pending> = diesel::sql_query("SELECT changeset FROM _connetto_pending")
        .load(alice.conn())
        .unwrap();
    let parsed = ParsedDiffSet::parse(&pending[0].changeset).unwrap();
    let tables: Vec<&str> = parsed
        .table_schemas()
        .into_iter()
        .map(|table| table.name().as_str())
        .collect();
    assert_eq!(tables, ["notes_rls"]);
}

#[tokio::test]
async fn a_replica_routes_its_shared_tables_to_the_store_in_any_case() {
    let directory = tempfile::tempdir().unwrap();
    let alice = open(
        directory.path(),
        super::support::SharedKeys::default(),
        "alice",
    )
    .await;
    let shared = alice.shared_tables();
    assert_eq!(
        connetto_client::replica_schema(shared, "TAGS_rls"),
        "connetto_shared"
    );
    assert_eq!(connetto_client::replica_schema(shared, "notes_rls"), "main");
}
