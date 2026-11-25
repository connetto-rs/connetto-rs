use core::time::Duration;

use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection, DataDir, SyncSchema};
use connetto_core::Cursor;
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, LivePatch, MutationApplied,
    MutationReject, MutationRejectReason, SnapshotBegin, SnapshotEnd, SnapshotPatch, WritesSettled,
};
use connetto_core::traits::IncomingFrame;
use diesel::prelude::*;
use sqlite_diff_rs::{DiffOps, Insert, PatchSet, PatchsetFormat, SimpleTable, Update, Value};

use super::support::{self, Script};

const SCHEMA: &str = "-- connetto:major 1
CREATE TABLE actions (id INT PRIMARY KEY);
CREATE TABLE effects (id INT PRIMARY KEY, label TEXT NOT NULL, stamp TEXT NOT NULL);
CREATE FUNCTION derive_effects() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO effects VALUES (NEW.id + 30, 'provisional', 'unstamped');
    UPDATE effects SET label = 'derived' WHERE id = 10;
    DELETE FROM effects WHERE id = 20;
    RETURN NEW;
END $$;
CREATE TRIGGER actions_derive AFTER INSERT ON actions
FOR EACH ROW EXECUTE FUNCTION derive_effects();
CREATE FUNCTION stamp_effect() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.stamp := 'stamped';
    RETURN NEW;
END $$;
CREATE TRIGGER effects_stamp BEFORE INSERT OR UPDATE ON effects
FOR EACH ROW EXECUTE FUNCTION stamp_effect();";

diesel::table! {
    actions (id) {
        id -> BigInt,
    }
}

diesel::table! {
    effects (id) {
        id -> BigInt,
        label -> Text,
        stamp -> Text,
    }
}

fn schema() -> SyncSchema {
    SyncSchema::new(
        connetto_translate::translate_shared::<String, connetto_core::bots::TextIds>(
            connetto_translate::Documents::new(
                SCHEMA,
                "",
                connetto_test_harness::FIXTURE_ROLES,
                connetto_test_harness::FIXTURE_READER,
            ),
            &["effects"],
        )
        .expect("translate shared effects"),
    )
}

async fn open(
    directory: &std::path::Path,
    keys: support::SharedKeys,
    owner: &str,
) -> (ConnettoConnection<Script>, Script) {
    let server = Script::default();
    server.say(support::handshake_ack());
    let connection = ClientBuilder::new(schema(), support::Once::new(server.clone()))
        .signed_in(support::held(owner))
        .durable(
            DataDir::new(directory.to_path_buf()),
            keys,
            connetto_client::MemoryRefreshStore::default(),
        )
        .connect_driven()
        .await
        .expect("open shared identity");
    (connection, server)
}

async fn pump_until(
    connection: &mut ConnettoConnection<Script>,
    matches: impl Fn(&ClientEvent) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = connection.pump_one().await.expect("pump overlay frame");
            if matches(&event) {
                return;
            }
            assert!(!matches!(event, ClientEvent::Closed));
        }
    })
    .await
    .expect("OVERLAY-REGRESSION-1 frame deadline");
}

fn table() -> SimpleTable {
    SimpleTable::new("effects", &[("id", ""), ("label", ""), ("stamp", "")], &[0])
}

async fn seed(connection: &mut ConnettoConnection<Script>, server: &Script) {
    connection
        .subscribe("effects", "SELECT * FROM effects")
        .await
        .unwrap();
    snapshot(connection, server, 1).await;
}

async fn snapshot(connection: &mut ConnettoConnection<Script>, server: &Script, position: u8) {
    let mut patch = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for (identifier, label) in [(10, "waiting"), (20, "retained")] {
        patch = patch.insert(
            Insert::from(table())
                .set(0, Value::Integer(identifier))
                .unwrap()
                .set(1, Value::Text(label.to_owned()))
                .unwrap()
                .set(2, Value::Text("server-stamp".to_owned()))
                .unwrap(),
        );
    }
    server.say(IncomingFrame::Control(ControlMessage::SnapshotBegin(
        SnapshotBegin {
            sub_id: "effects".to_owned(),
        },
    )));
    server.say(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(
        SnapshotPatch::new(
            "effects".to_owned(),
            zstd::encode_all(patch.build().as_slice(), 3).unwrap(),
        ),
    )));
    server.say(IncomingFrame::Control(ControlMessage::SnapshotEnd(
        SnapshotEnd {
            sub_id: "effects".to_owned(),
            cursor: Cursor::new(vec![position]),
        },
    )));
    pump_until(
        connection,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "effects"),
    )
    .await;
}

fn rows(connection: &mut ConnettoConnection<Script>) -> Vec<(i64, String, String)> {
    effects::table
        .order(effects::id)
        .load(connection.conn())
        .unwrap()
}

fn baseline() -> Vec<(i64, String, String)> {
    vec![
        (10, "waiting".to_owned(), "server-stamp".to_owned()),
        (20, "retained".to_owned(), "server-stamp".to_owned()),
    ]
}

fn provisional() -> Vec<(i64, String, String)> {
    vec![
        (10, "derived".to_owned(), "stamped".to_owned()),
        (31, "provisional".to_owned(), "stamped".to_owned()),
    ]
}

async fn derive(connection: &mut ConnettoConnection<Script>) -> u64 {
    diesel::insert_into(actions::table)
        .values(actions::id.eq(1))
        .execute(connection.conn())
        .unwrap();
    let sequence = connection.push().await.unwrap().unwrap();
    let physical = connection
        .policy_tables()
        .physical("actions")
        .unwrap()
        .to_owned();
    assert_eq!(
        support::queued_ops(connection.conn(), sequence),
        [format!("insert {physical} [Integer(1)]")]
    );
    sequence
}

async fn settle(connection: &mut ConnettoConnection<Script>, server: &Script, sequence: u64) {
    let writer = connection.bound_writer().unwrap();
    connection.track_relay_notices();
    server.say(IncomingFrame::Control(ControlMessage::WritesSettled(
        WritesSettled {
            writer,
            through_seq: sequence,
            before_patch: false,
        },
    )));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = connection
                .pump_one_or(core::future::pending())
                .await
                .expect("pump settlement frame");
            let settled = connection.take_relay_notices().into_iter().any(|notice| {
                matches!(notice, connetto_client::RelayNotice::WritesSettled {
                    writer: settled_writer,
                    through_seq,
                    before_patch: false,
                } if settled_writer == writer && through_seq == sequence)
            });
            if settled {
                assert!(event.is_none(), "settlement has no public event");
                return;
            }
            assert!(!matches!(event, Some(ClientEvent::Closed)));
        }
    })
    .await
    .expect("OVERLAY-TEST-4 settlement deadline");
}

async fn server_update(
    connection: &mut ConnettoConnection<Script>,
    server: &Script,
    label: &str,
    position: u8,
) {
    let patch = PatchSet::<SimpleTable, String, Vec<u8>>::new().update(
        Update::<_, PatchsetFormat, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(10))
            .unwrap()
            .set(1, Value::Text(label.to_owned()))
            .unwrap(),
    );
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "effects".to_owned(),
        Cursor::new(vec![position]),
        zstd::encode_all(patch.build().as_slice(), 3).unwrap(),
    ))));
    pump_until(
        connection,
        |event| matches!(event, ClientEvent::LivePatch { sub_id, .. } if sub_id == "effects"),
    )
    .await;
}

#[tokio::test]
async fn derived_shared_insert_update_delete_are_identity_local_and_before_triggers_fire() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    let sequence = derive(&mut alice).await;
    assert_eq!(rows(&mut alice), provisional());
    assert_eq!(rows(&mut bob), baseline());
    server_update(&mut bob, &bob_server, "server-copy", 2).await;
    assert_eq!(rows(&mut alice), provisional());
    let mut server_rows = baseline();
    server_rows[0].1 = "server-copy".to_owned();
    assert_eq!(rows(&mut bob), server_rows);
    settle(&mut alice, &alice_server, sequence).await;
    assert_eq!(rows(&mut alice), server_rows);
    assert_eq!(alice.push().await.unwrap(), None);
    assert_eq!(bob.push().await.unwrap(), None);
}

#[tokio::test]
async fn refused_shared_derivation_drops_only_the_authors_overlay() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    let sequence = derive(&mut alice).await;
    server_update(&mut bob, &bob_server, "server-copy", 2).await;
    alice_server.say(IncomingFrame::Control(ControlMessage::MutationReject(
        MutationReject {
            client_seq: sequence,
            reason: MutationRejectReason::Unauthorized,
        },
    )));
    pump_until(&mut alice, |event| matches!(event, ClientEvent::MutationRejected { client_seq, .. } if *client_seq == sequence)).await;
    assert_eq!(rows(&mut alice), rows(&mut bob));
    assert_eq!(rows(&mut alice)[0].1, "server-copy");
    assert_eq!(
        actions::table
            .count()
            .get_result::<i64>(alice.conn())
            .unwrap(),
        0
    );
    assert_eq!(alice.push().await.unwrap(), None);
}

#[tokio::test]
async fn application_update_of_overlay_uploads_sparse_logical_change_until_its_own_mark() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    let derived_sequence = derive(&mut alice).await;
    diesel::update(effects::table.filter(effects::id.eq(10)))
        .set((
            effects::label.eq("application"),
            effects::stamp.eq("unstamped"),
        ))
        .execute(alice.conn())
        .unwrap();
    let application_sequence = alice.push().await.unwrap().unwrap();
    let physical = alice
        .policy_tables()
        .physical("effects")
        .unwrap()
        .to_owned();
    assert_eq!(
        support::queued_ops(alice.conn(), application_sequence),
        [format!(
            "update {physical} [(Some(Integer(10)), None), (Some(Text(\"derived\")), Some(Text(\"application\"))), (None, None)]"
        ),]
    );
    server_update(&mut bob, &bob_server, "server-copy", 2).await;
    settle(&mut alice, &alice_server, derived_sequence).await;
    assert_eq!(
        rows(&mut alice),
        vec![
            (10, "application".to_owned(), "stamped".to_owned()),
            (20, "retained".to_owned(), "server-stamp".to_owned()),
        ]
    );
    assert_eq!(rows(&mut bob)[0].1, "server-copy");
    server_update(&mut bob, &bob_server, "accepted-application", 3).await;
    assert_eq!(rows(&mut alice)[0].1, "application");
    settle(&mut alice, &alice_server, application_sequence).await;
    assert_eq!(rows(&mut alice), rows(&mut bob));
    assert_eq!(rows(&mut alice)[0].1, "accepted-application");
    assert_eq!(alice.push().await.unwrap(), None);
}

async fn refuse(connection: &mut ConnettoConnection<Script>, server: &Script, sequence: u64) {
    server.say(IncomingFrame::Control(ControlMessage::MutationReject(
        MutationReject {
            client_seq: sequence,
            reason: MutationRejectReason::Unauthorized,
        },
    )));
    pump_until(connection, |event| matches!(event, ClientEvent::MutationRejected { client_seq, .. } if *client_seq == sequence)).await;
}

#[tokio::test]
async fn refused_overlay_application_update_preserves_backing_before_or_after_its_mark() {
    for mark_before_refusal in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let keys = support::SharedKeys::default();
        let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
        let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
        seed(&mut bob, &bob_server).await;
        let derived_sequence = derive(&mut alice).await;
        diesel::update(effects::table.filter(effects::id.eq(10)))
            .set(effects::label.eq("application"))
            .execute(alice.conn())
            .unwrap();
        let application_sequence = alice.push().await.unwrap().unwrap();
        let upload = support::queued_ops(alice.conn(), application_sequence);
        server_update(&mut bob, &bob_server, "application", 2).await;
        settle(&mut alice, &alice_server, derived_sequence).await;
        let backing = rows(&mut bob);
        if mark_before_refusal {
            settle(&mut alice, &alice_server, application_sequence).await;
        }
        assert_eq!(
            support::queued_ops(alice.conn(), application_sequence),
            upload
        );
        refuse(&mut alice, &alice_server, application_sequence).await;
        assert_eq!(rows(&mut bob), backing);
        assert_eq!(rows(&mut alice), backing);
        assert_eq!(alice.push().await.unwrap(), None);
    }
}

#[tokio::test]
async fn refusing_one_entry_with_backing_and_overlay_writes_restores_the_original_backing() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    diesel::update(effects::table.filter(effects::id.eq(10)))
        .set(effects::label.eq("backing-local"))
        .execute(alice.conn())
        .unwrap();
    diesel::insert_into(actions::table)
        .values(actions::id.eq(1))
        .execute(alice.conn())
        .unwrap();
    diesel::update(effects::table.filter(effects::id.eq(10)))
        .set(effects::label.eq("application"))
        .execute(alice.conn())
        .unwrap();
    let sequence = alice.push().await.unwrap().unwrap();
    assert_eq!(rows(&mut bob)[0].1, "backing-local");
    assert_eq!(rows(&mut alice)[0].1, "application");
    let physical_tables = ["effects", "actions"]
        .map(|logical| alice.policy_tables().physical(logical).unwrap().to_owned());
    assert_eq!(
        support::captured_tables(alice.conn(), sequence),
        physical_tables
    );
    refuse(&mut alice, &alice_server, sequence).await;
    assert_eq!(rows(&mut bob), baseline());
    assert_eq!(rows(&mut alice), baseline());
    assert_eq!(
        actions::table
            .count()
            .get_result::<i64>(alice.conn())
            .unwrap(),
        0
    );
    assert_eq!(alice.push().await.unwrap(), None);
    assert_eq!(bob.push().await.unwrap(), None);
}

#[tokio::test]
async fn moved_overlay_keys_and_revived_tombstones_keep_their_own_settlement_tags() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    let derived_sequence = derive(&mut alice).await;
    diesel::update(effects::table.filter(effects::id.eq(10)))
        .set(effects::id.eq(11))
        .execute(alice.conn())
        .unwrap();
    let moved_sequence = alice.push().await.unwrap().unwrap();
    let physical = alice
        .policy_tables()
        .physical("effects")
        .unwrap()
        .to_owned();
    let mut moved_operations = support::queued_ops(alice.conn(), moved_sequence);
    moved_operations.sort_unstable();
    assert_eq!(
        moved_operations,
        [
            format!("delete {physical} [Integer(10), Text(\"derived\"), Text(\"stamped\")]"),
            format!("insert {physical} [Integer(11), Text(\"derived\"), Text(\"stamped\")]"),
        ]
    );
    diesel::insert_into(effects::table)
        .values((
            effects::id.eq(20),
            effects::label.eq("revived"),
            effects::stamp.eq("unstamped"),
        ))
        .execute(alice.conn())
        .unwrap();
    let revived_sequence = alice.push().await.unwrap().unwrap();
    assert_eq!(
        support::queued_ops(alice.conn(), revived_sequence),
        [format!(
            "insert {physical} [Integer(20), Text(\"revived\"), Text(\"stamped\")]"
        ),]
    );
    assert_eq!(rows(&mut bob), baseline());
    settle(&mut alice, &alice_server, derived_sequence).await;
    assert_eq!(
        rows(&mut alice),
        vec![
            (11, "derived".to_owned(), "stamped".to_owned()),
            (20, "revived".to_owned(), "stamped".to_owned()),
        ]
    );
    settle(&mut alice, &alice_server, moved_sequence).await;
    assert_eq!(
        rows(&mut alice),
        vec![
            (10, "waiting".to_owned(), "server-stamp".to_owned()),
            (20, "revived".to_owned(), "stamped".to_owned()),
        ]
    );
    settle(&mut alice, &alice_server, revived_sequence).await;
    assert_eq!(rows(&mut alice), baseline());
    assert_eq!(rows(&mut bob), baseline());
    assert_eq!(alice.push().await.unwrap(), None);
}

#[tokio::test]
async fn unchanged_overlay_update_with_empty_push_preserves_the_original_settlement_tag() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    let sequence = derive(&mut alice).await;
    diesel::update(effects::table.filter(effects::id.eq(10)))
        .set(effects::label.eq("derived"))
        .execute(alice.conn())
        .unwrap();
    assert_eq!(alice.push().await.unwrap(), None);
    assert_eq!(rows(&mut alice), provisional());
    assert_eq!(rows(&mut bob), baseline());
    settle(&mut alice, &alice_server, sequence).await;
    assert_eq!(rows(&mut alice), baseline());
    assert_eq!(alice.push().await.unwrap(), None);
}

#[tokio::test]
async fn rolled_back_overlay_batch_restores_rows_and_the_original_settlement_tag() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    let sequence = derive(&mut alice).await;
    let result = alice
        .conn()
        .transaction::<(), diesel::result::Error, _>(|connection| {
            diesel::insert_into(actions::table)
                .values(actions::id.eq(2))
                .execute(connection)?;
            diesel::update(effects::table.filter(effects::id.eq(10)))
                .set(effects::label.eq("cancelled"))
                .execute(connection)?;
            diesel::delete(effects::table.filter(effects::id.eq(31))).execute(connection)?;
            Err(diesel::result::Error::RollbackTransaction)
        });
    assert!(matches!(
        result,
        Err(diesel::result::Error::RollbackTransaction)
    ));
    assert_eq!(alice.push().await.unwrap(), None);
    assert_eq!(rows(&mut alice), provisional());
    assert_eq!(rows(&mut bob), baseline());
    settle(&mut alice, &alice_server, sequence).await;
    assert_eq!(rows(&mut alice), baseline());
    assert_eq!(alice.push().await.unwrap(), None);
}

#[tokio::test]
async fn cancelled_primary_batch_restores_previous_overlay_rows_and_settlement_tags() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut bob, &bob_server).await;
    let derived_sequence = derive(&mut alice).await;
    diesel::update(effects::table.filter(effects::id.eq(10)))
        .set(effects::label.eq("previous-application"))
        .execute(alice.conn())
        .unwrap();
    let application_sequence = alice.push().await.unwrap().unwrap();
    let previous_overlay = rows(&mut alice);
    alice
        .conn()
        .transaction::<(), diesel::result::Error, _>(|connection| {
            diesel::insert_into(actions::table)
                .values(actions::id.eq(2))
                .execute(connection)?;
            diesel::delete(actions::table.filter(actions::id.eq(2))).execute(connection)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        rows(&mut alice),
        vec![
            (10, "derived".to_owned(), "stamped".to_owned()),
            (31, "provisional".to_owned(), "stamped".to_owned()),
            (32, "provisional".to_owned(), "stamped".to_owned()),
        ]
    );
    assert_eq!(alice.push().await.unwrap(), None);
    assert_eq!(rows(&mut alice), previous_overlay);
    assert_eq!(rows(&mut bob), baseline());
    settle(&mut alice, &alice_server, derived_sequence).await;
    assert_eq!(
        rows(&mut alice),
        vec![
            (10, "previous-application".to_owned(), "stamped".to_owned()),
            (20, "retained".to_owned(), "server-stamp".to_owned()),
        ]
    );
    settle(&mut alice, &alice_server, application_sequence).await;
    assert_eq!(rows(&mut alice), baseline());
    assert_eq!(alice.push().await.unwrap(), None);
}

async fn acknowledge(connection: &mut ConnettoConnection<Script>, server: &Script, sequence: u64) {
    server.say(IncomingFrame::Control(ControlMessage::MutationApplied(
        MutationApplied {
            client_seq: sequence,
        },
    )));
    pump_until(connection, |event| matches!(event, ClientEvent::MutationApplied { client_seq, .. } if *client_seq == sequence)).await;
}

fn handshake_at(frontier: u64) -> IncomingFrame {
    let IncomingFrame::Control(ControlMessage::HandshakeAck(mut acknowledgement)) =
        support::handshake_ack()
    else {
        panic!("handshake fixture");
    };
    acknowledgement.writer.as_mut().unwrap().frontier = frontier;
    IncomingFrame::Control(ControlMessage::HandshakeAck(acknowledgement))
}

#[tokio::test]
async fn attach_drops_acknowledged_shared_only_overlay_with_or_without_reopening() {
    for reopen in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let keys = support::SharedKeys::default();
        let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
        let (mut bob, bob_server) = open(directory.path(), keys.clone(), "bob").await;
        seed(&mut bob, &bob_server).await;
        let sequence = derive(&mut alice).await;
        acknowledge(&mut alice, &alice_server, sequence).await;
        assert_eq!(rows(&mut alice), provisional());
        assert_eq!(rows(&mut bob), baseline());
        if reopen {
            drop(alice);
            alice = ClientBuilder::new(schema(), support::NeverDial::<Script>::default())
                .signed_in(support::held("alice"))
                .durable(
                    DataDir::new(directory.path().to_path_buf()),
                    keys,
                    connetto_client::MemoryRefreshStore::default(),
                )
                .open_driven()
                .await
                .unwrap();
        } else {
            alice_server.hang_up();
            pump_until(&mut alice, |event| matches!(event, ClientEvent::Closed)).await;
        }
        let replacement = Script::default();
        replacement.say(handshake_at(sequence));
        alice.attach(replacement).await.unwrap();
        assert_eq!(rows(&mut alice), baseline());
        assert_eq!(rows(&mut bob), baseline());
        assert_eq!(
            actions::table
                .count()
                .get_result::<i64>(alice.conn())
                .unwrap(),
            1
        );
        assert_eq!(alice.push().await.unwrap(), None);
    }
}

#[tokio::test]
async fn full_resync_drops_acknowledged_shared_only_overlay_without_a_settlement_mark() {
    let directory = tempfile::tempdir().unwrap();
    let keys = support::SharedKeys::default();
    let (mut alice, alice_server) = open(directory.path(), keys.clone(), "alice").await;
    let (mut bob, bob_server) = open(directory.path(), keys, "bob").await;
    seed(&mut alice, &alice_server).await;
    seed(&mut bob, &bob_server).await;
    let sequence = derive(&mut alice).await;
    acknowledge(&mut alice, &alice_server, sequence).await;
    assert_eq!(rows(&mut alice), provisional());
    assert_eq!(rows(&mut bob), baseline());
    alice_server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "effects".to_owned(),
            reason: FullResyncReason::CursorBeyondHistory,
        },
    )));
    snapshot(&mut alice, &alice_server, 2).await;
    assert_eq!(rows(&mut alice), baseline());
    assert_eq!(rows(&mut bob), baseline());
    // The history gap evicts acknowledged rows absent from replacement snapshots.
    assert_eq!(
        actions::table
            .count()
            .get_result::<i64>(alice.conn())
            .unwrap(),
        0
    );
    assert_eq!(alice.push().await.unwrap(), None);
}

diesel::table! {
    tags (id) {
        id -> BigInt,
        label -> Text,
    }
}
diesel::table! {
    connetto_shared._connetto_shared_holding (identity, holder, tbl, key) {
        identity -> Text,
        holder -> Text,
        tbl -> Text,
        key -> Text,
    }
}
diesel::table! {
    _connetto_outbox (seq) {
        seq -> BigInt,
    }
}
diesel::table! {
    _connetto_pending (writer, seq) {
        writer -> Text,
        seq -> BigInt,
    }
}
diesel::table! {
    _connetto_derived (writer, seq) {
        writer -> Text,
        seq -> BigInt,
    }
}
diesel::table! {
    _connetto_overlay_tags (tbl, key) {
        tbl -> Text,
        key -> Text,
    }
}
diesel::table! {
    _connetto_rollback (writer, seq) {
        writer -> Text,
        seq -> BigInt,
    }
}
diesel::table! {
    _connetto_derived_marks (writer) {
        writer -> Text,
        through_seq -> BigInt,
    }
}

fn holdings(connection: &mut ConnettoConnection<Script>) -> Vec<(String, String, String, String)> {
    _connetto_shared_holding::table
        .order((
            _connetto_shared_holding::identity,
            _connetto_shared_holding::holder,
            _connetto_shared_holding::tbl,
            _connetto_shared_holding::key,
        ))
        .load::<(String, String, String, String)>(connection.conn())
        .unwrap()
}

fn assert_unqueued(
    connection: &mut ConnettoConnection<Script>,
    expected_holdings: &[(String, String, String, String)],
) {
    assert_eq!(
        _connetto_outbox::table
            .count()
            .get_result::<i64>(connection.conn())
            .unwrap(),
        1
    );
    assert_eq!(
        _connetto_pending::table
            .count()
            .get_result::<i64>(connection.conn())
            .unwrap(),
        0
    );
    assert_eq!(
        _connetto_derived::table
            .count()
            .get_result::<i64>(connection.conn())
            .unwrap(),
        0
    );
    assert_eq!(
        _connetto_overlay_tags::table
            .count()
            .get_result::<i64>(connection.conn())
            .unwrap(),
        0
    );
    assert_eq!(
        _connetto_rollback::table
            .count()
            .get_result::<i64>(connection.conn())
            .unwrap(),
        0
    );
    assert_eq!(holdings(connection), expected_holdings);
    assert_eq!(
        tags::table
            .order(tags::id)
            .load::<(i64, String)>(connection.conn())
            .unwrap(),
        [(1, "acknowledged".to_owned()), (2, "unsent".to_owned())]
    );
}

#[tokio::test]
async fn attach_without_held_effects_keeps_unsent_writes_and_holdings_on_repeated_marks() {
    use diesel::connection::SimpleConnection;

    let directory = tempfile::tempdir().unwrap();
    let server = Script::default();
    server.say(support::handshake_ack());
    let mut alice = ClientBuilder::new(
        super::shared_coverage::schema(),
        support::Once::new(server.clone()),
    )
    .signed_in(support::held("alice"))
    .durable(
        DataDir::new(directory.path().to_path_buf()),
        support::SharedKeys::default(),
        connetto_client::MemoryRefreshStore::default(),
    )
    .connect_driven()
    .await
    .unwrap();
    diesel::insert_into(tags::table)
        .values((tags::id.eq(1), tags::label.eq("acknowledged")))
        .execute(alice.conn())
        .unwrap();
    let sequence = alice.push().await.unwrap().unwrap();
    assert!(sequence > 0);
    acknowledge(&mut alice, &server, sequence).await;
    server.hang_up();
    pump_until(&mut alice, |event| matches!(event, ClientEvent::Closed)).await;
    diesel::insert_into(tags::table)
        .values((tags::id.eq(2), tags::label.eq("unsent")))
        .execute(alice.conn())
        .unwrap();

    let expected_holdings = holdings(&mut alice);
    assert!(
        expected_holdings
            .iter()
            .any(|(_, holder, _, _)| holder == "connetto-unsent")
    );
    assert_unqueued(&mut alice, &expected_holdings);
    alice
        .conn()
        .batch_execute(
            "CREATE TEMP TRIGGER refuse_holding_delete BEFORE DELETE \
             ON connetto_shared._connetto_shared_holding \
             BEGIN SELECT RAISE(ABORT, 'holdings must stay untouched'); END; \
             CREATE TEMP TRIGGER refuse_holding_insert BEFORE INSERT \
             ON connetto_shared._connetto_shared_holding \
             BEGIN SELECT RAISE(ABORT, 'holdings must stay untouched'); END; \
             CREATE TEMP TRIGGER refuse_holding_update BEFORE UPDATE \
             ON connetto_shared._connetto_shared_holding \
             BEGIN SELECT RAISE(ABORT, 'holdings must stay untouched'); END",
        )
        .unwrap();
    let replacement = Script::default();
    replacement.say(handshake_at(sequence));
    alice.attach(replacement.clone()).await.unwrap();
    assert_unqueued(&mut alice, &expected_holdings);
    for frontier in [sequence, sequence - 1, sequence] {
        settle(&mut alice, &replacement, frontier).await;
        assert_unqueued(&mut alice, &expected_holdings);
        assert_eq!(
            _connetto_derived_marks::table
                .select(_connetto_derived_marks::through_seq)
                .load::<i64>(alice.conn())
                .unwrap(),
            Vec::<i64>::new()
        );
    }
}

#[cfg(feature = "peer")]
mod peer_overlays {
    use super::super::peer_support::{author, captured, manifest};
    use super::*;
    use connetto_client::peer::PeerApplyOutcome;

    const AUTHOR_SCHEMA: &str =
        "CREATE TABLE effects (id INTEGER PRIMARY KEY, label TEXT NOT NULL, stamp TEXT NOT NULL);";

    const POLICIES: &str = "
CREATE POLICY effects_read ON effects FOR SELECT USING (true);
CREATE POLICY effects_insert ON effects FOR INSERT WITH CHECK (true);
CREATE POLICY effects_update ON effects FOR UPDATE
USING (current_setting('app.user_id', true) = 'alice' OR label IN ('waiting', 'provisional'))
WITH CHECK (label <> 'forbidden');
CREATE POLICY effects_delete ON effects FOR DELETE USING (true);";

    struct Fixture {
        _directory: tempfile::TempDir,
        receiver: ConnettoConnection<Script>,
        observer: ConnettoConnection<Script>,
        server: Script,
        sequence: u64,
    }

    async fn open_policy(
        directory: &std::path::Path,
        keys: support::SharedKeys,
        owner: &str,
    ) -> (ConnettoConnection<Script>, Script) {
        let definition = format!("{SCHEMA}\nALTER TABLE effects ENABLE ROW LEVEL SECURITY;");
        let schema = SyncSchema::new(
            connetto_translate::translate_shared::<String, connetto_core::bots::TextIds>(
                connetto_translate::Documents::new(
                    &definition,
                    POLICIES,
                    connetto_test_harness::FIXTURE_ROLES,
                    connetto_test_harness::FIXTURE_READER,
                ),
                &["effects"],
            )
            .unwrap(),
        );
        let server = Script::default();
        server.say(support::handshake_ack());
        let connection = ClientBuilder::new(schema, support::Once::new(server.clone()))
            .signed_in(support::held(owner))
            .durable(
                DataDir::new(directory.to_path_buf()),
                keys,
                connetto_client::MemoryRefreshStore::default(),
            )
            .connect_driven()
            .await
            .unwrap();
        (connection, server)
    }

    async fn fixture() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let keys = support::SharedKeys::default();
        let (mut receiver, server) = open_policy(directory.path(), keys.clone(), "alice").await;
        let (mut observer, observer_server) = open_policy(directory.path(), keys, "bob").await;
        seed(&mut observer, &observer_server).await;
        let sequence = derive(&mut receiver).await;
        assert_eq!(rows(&mut receiver), provisional());
        assert_eq!(rows(&mut observer), baseline());
        Fixture {
            _directory: directory,
            receiver,
            observer,
            server,
            sequence,
        }
    }

    async fn update_body(identifier: i64, label: &str) -> Vec<u8> {
        let [_, body] = captured(
            AUTHOR_SCHEMA,
            [
                &|connection| {
                    diesel::insert_into(effects::table)
                        .values((
                            effects::id.eq(identifier),
                            effects::label.eq("waiting"),
                            effects::stamp.eq("server-stamp"),
                        ))
                        .execute(connection.conn())
                        .unwrap();
                },
                &|connection| {
                    diesel::update(effects::table.find(identifier))
                        .set(effects::label.eq(label))
                        .execute(connection.conn())
                        .unwrap();
                },
            ],
        )
        .await;
        body
    }

    fn apply(connection: &mut ConnettoConnection<Script>, body: &[u8]) -> PeerApplyOutcome {
        connection
            .apply_peer_entry(
                &author("overlay-peer", "peer-writer", Some("*")),
                &manifest(7, body),
                body,
            )
            .unwrap()
    }

    async fn assert_no_upload(fixture: &mut Fixture) {
        assert_eq!(fixture.receiver.push().await.unwrap(), None);
        assert_eq!(fixture.observer.push().await.unwrap(), None);
    }

    #[tokio::test]
    async fn peer_update_judges_shared_old_row_and_preserves_overlay_until_own_mark() {
        let mut fixture = fixture().await;
        let body = update_body(10, "peer-new").await;
        let outcome = apply(&mut fixture.receiver, &body);
        assert!(
            matches!(outcome, PeerApplyOutcome::Applied { .. }),
            "{outcome:?}"
        );
        let mut expected = baseline();
        expected[0] = (10, "peer-new".to_owned(), "stamped".to_owned());
        assert_eq!(rows(&mut fixture.receiver), provisional());
        assert_eq!(rows(&mut fixture.observer), expected);
        assert_no_upload(&mut fixture).await;
        settle(&mut fixture.receiver, &fixture.server, fixture.sequence).await;
        assert_eq!(rows(&mut fixture.receiver), expected);
        assert_eq!(rows(&mut fixture.observer), expected);
        assert_no_upload(&mut fixture).await;
    }

    #[tokio::test]
    async fn peer_update_of_overlay_only_key_is_refused() {
        let mut fixture = fixture().await;
        let body = update_body(31, "peer-new").await;
        assert!(matches!(
            apply(&mut fixture.receiver, &body),
            PeerApplyOutcome::Refused { .. }
        ));
        assert_eq!(rows(&mut fixture.receiver), provisional());
        assert_eq!(rows(&mut fixture.observer), baseline());
        assert_no_upload(&mut fixture).await;
        settle(&mut fixture.receiver, &fixture.server, fixture.sequence).await;
        assert_eq!(rows(&mut fixture.receiver), baseline());
        assert_eq!(rows(&mut fixture.observer), baseline());
        assert_no_upload(&mut fixture).await;
    }

    #[tokio::test]
    async fn peer_insert_onto_overlay_tombstone_preserves_it_until_own_mark() {
        let mut fixture = fixture().await;
        let [body] = captured(
            AUTHOR_SCHEMA,
            [&|connection| {
                diesel::insert_into(effects::table)
                    .values((
                        effects::id.eq(20),
                        effects::label.eq("peer-insert"),
                        effects::stamp.eq("stamped"),
                    ))
                    .execute(connection.conn())
                    .unwrap();
            }],
        )
        .await;
        let outcome = apply(&mut fixture.receiver, &body);
        assert!(
            matches!(outcome, PeerApplyOutcome::Applied { .. }),
            "{outcome:?}"
        );
        let mut expected = baseline();
        expected[1] = (20, "peer-insert".to_owned(), "stamped".to_owned());
        assert_eq!(rows(&mut fixture.receiver), provisional());
        assert_eq!(rows(&mut fixture.observer), expected);
        assert_no_upload(&mut fixture).await;
        settle(&mut fixture.receiver, &fixture.server, fixture.sequence).await;
        assert_eq!(rows(&mut fixture.receiver), expected);
        assert_eq!(rows(&mut fixture.observer), expected);
        assert_no_upload(&mut fixture).await;
    }

    #[tokio::test]
    async fn policy_denied_peer_update_preserves_overlay_and_shared_row() {
        let mut fixture = fixture().await;
        let body = update_body(10, "forbidden").await;
        assert!(matches!(
            apply(&mut fixture.receiver, &body),
            PeerApplyOutcome::Refused { .. }
        ));
        assert_eq!(rows(&mut fixture.receiver), provisional());
        assert_eq!(rows(&mut fixture.observer), baseline());
        assert_no_upload(&mut fixture).await;
        settle(&mut fixture.receiver, &fixture.server, fixture.sequence).await;
        assert_eq!(rows(&mut fixture.receiver), baseline());
        assert_eq!(rows(&mut fixture.observer), baseline());
        assert_no_upload(&mut fixture).await;
    }

    #[tokio::test]
    async fn denied_peer_update_matching_shared_afterimage_is_refused() {
        let mut fixture = fixture().await;
        server_update(&mut fixture.receiver, &fixture.server, "forbidden", 2).await;
        let mut expected = baseline();
        expected[0].1 = "forbidden".to_owned();
        let body = update_body(10, "forbidden").await;
        assert!(matches!(
            apply(&mut fixture.receiver, &body),
            PeerApplyOutcome::Refused { .. }
        ));
        assert_eq!(rows(&mut fixture.receiver), provisional());
        assert_eq!(rows(&mut fixture.observer), expected);
        assert_no_upload(&mut fixture).await;
        settle(&mut fixture.receiver, &fixture.server, fixture.sequence).await;
        assert_eq!(rows(&mut fixture.receiver), expected);
        assert_eq!(rows(&mut fixture.observer), expected);
        assert_no_upload(&mut fixture).await;
    }
}
