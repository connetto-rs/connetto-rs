use core::time::Duration;

use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection};
use connetto_core::Cursor;
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, LivePatch, MutationApplied,
    MutationConflict, MutationReject, MutationRejectReason, SnapshotBegin, SnapshotEnd,
    SnapshotPatch, WritesSettled,
};
use connetto_core::traits::IncomingFrame;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_diff_rs::{
    DiffOps, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update, Value,
};

use super::support::{self, Script};

const DDL: &str = "
CREATE TABLE actions (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE effects (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
CREATE TRIGGER derive_action AFTER INSERT ON actions
WHEN connetto_write_exempt() = 0
BEGIN
    SELECT connetto_derived_write(1);
    INSERT INTO effects VALUES (NEW.id + 30, 'provisional');
    SELECT connetto_derived_write(1);
    UPDATE effects SET label = 'accepted' WHERE id = 10;
    SELECT connetto_derived_write(-1);
    DELETE FROM effects WHERE id = 20;
    SELECT connetto_derived_write(-1);
END;
CREATE TRIGGER failing_action BEFORE INSERT ON actions
WHEN NEW.id = -1 AND connetto_write_exempt() = 0
BEGIN
    SELECT connetto_derived_write(1);
    INSERT INTO effects VALUES (99, 'failed');
    SELECT RAISE(ABORT, 'derived failure');
END;";

diesel::table! {
    actions (id) {
        id -> BigInt,
        label -> Text,
    }
}

diesel::table! {
    effects (id) {
        id -> BigInt,
        label -> Text,
    }
}

async fn connected() -> (ConnettoConnection<Script>, Script) {
    connected_with(DDL).await
}

async fn connected_with(schema: &str) -> (ConnettoConnection<Script>, Script) {
    let server = Script::default();
    server.say(support::handshake_ack());
    let mut connection =
        ClientBuilder::new(support::bundle(schema), support::Once::new(server.clone()))
            .signed_in(support::held("derived-writer"))
            .connect_driven()
            .await
            .expect("connect");
    seed(&mut connection);
    (connection, server)
}

fn handshake_at(frontier: u64) -> IncomingFrame {
    let IncomingFrame::Control(ControlMessage::HandshakeAck(mut acknowledgement)) =
        support::handshake_ack()
    else {
        panic!("handshake fixture");
    };
    acknowledgement
        .writer
        .as_mut()
        .expect("writer grant")
        .frontier = frontier;
    IncomingFrame::Control(ControlMessage::HandshakeAck(acknowledgement))
}

fn seed(connection: &mut ConnettoConnection<Script>) {
    connection
        .transact_with_bookkeeping(
            |replica| {
                diesel::insert_into(effects::table)
                    .values(&[
                        (effects::id.eq(10), effects::label.eq("waiting")),
                        (effects::id.eq(20), effects::label.eq("retained")),
                    ])
                    .execute(replica)
            },
            |_| Ok::<_, diesel::result::Error>(()),
        )
        .expect("seed effects");
}

fn effect_rows(connection: &mut ConnettoConnection<Script>) -> Vec<(i64, String)> {
    effects::table
        .order(effects::id)
        .load(connection.conn())
        .expect("read effects")
}

fn action_ids(connection: &mut ConnettoConnection<Script>) -> Vec<i64> {
    actions::table
        .select(actions::id)
        .order(actions::id)
        .load(connection.conn())
        .expect("read actions")
}

fn baseline() -> Vec<(i64, String)> {
    vec![(10, "waiting".to_owned()), (20, "retained".to_owned())]
}

fn provisional() -> Vec<(i64, String)> {
    vec![(10, "accepted".to_owned()), (31, "provisional".to_owned())]
}

fn insert_action(connection: &mut ConnettoConnection<Script>, identifier: i64) {
    diesel::insert_into(actions::table)
        .values((actions::id.eq(identifier), actions::label.eq("local")))
        .execute(connection)
        .expect("insert action");
}

async fn queued(connection: &mut ConnettoConnection<Script>) -> u64 {
    insert_action(connection, 1);
    let sequence = connection
        .push()
        .await
        .expect("push")
        .expect("queued action");
    assert_eq!(effect_rows(connection), provisional());
    sequence
}

async fn pump_until(
    connection: &mut ConnettoConnection<Script>,
    matches: impl Fn(&ClientEvent) -> bool,
) -> ClientEvent {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = connection.pump_one().await.expect("pump derived frame");
            if matches(&event) {
                return event;
            }
            assert!(
                !matches!(event, ClientEvent::Closed),
                "closed before derived frame"
            );
        }
    })
    .await
    .expect("DERIVED-SLICES frame deadline")
}

async fn acknowledge(connection: &mut ConnettoConnection<Script>, server: &Script, sequence: u64) {
    server.say(IncomingFrame::Control(ControlMessage::MutationApplied(
        MutationApplied {
            client_seq: sequence,
        },
    )));
    pump_until(connection, |event| {
        matches!(event,
        ClientEvent::MutationApplied { client_seq, .. }
            if *client_seq == sequence)
    })
    .await;
}

async fn settle(
    connection: &mut ConnettoConnection<Script>,
    server: &Script,
    sequence: u64,
    before_patch: bool,
) {
    connection.track_relay_notices();
    let writer = connection.bound_writer().expect("bound writer");
    server.say(IncomingFrame::Control(ControlMessage::WritesSettled(
        WritesSettled {
            writer,
            through_seq: sequence,
            before_patch,
        },
    )));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = connection
                .pump_one_or(core::future::pending())
                .await
                .expect("pump settlement");
            if connection.take_relay_notices().contains(
                &connetto_client::RelayNotice::WritesSettled {
                    writer,
                    through_seq: sequence,
                    before_patch,
                },
            ) {
                assert!(event.is_none(), "settlement has no application event");
                break;
            }
        }
    })
    .await
    .expect("DERIVED-SETTLEMENT-1 marker deadline");
}

fn effects_table() -> SimpleTable {
    SimpleTable::new("effects", &[("id", ""), ("label", "")], &[0])
}

fn snapshot_payload(rows: &[(i64, &str)]) -> Vec<u8> {
    let mut patch = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for (identifier, label) in rows {
        patch = patch.insert(
            Insert::from(effects_table())
                .set(0, Value::Integer(*identifier))
                .expect("set key")
                .set(1, Value::Text((*label).to_owned()))
                .expect("set label"),
        );
    }
    zstd::encode_all(patch.build().as_slice(), 3).expect("compress snapshot")
}

fn deterministic_payload() -> Vec<u8> {
    let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(effects_table())
        .set(0, Value::Integer(10))
        .expect("set key")
        .set(1, Value::Text("accepted".to_owned()))
        .expect("set label");
    let patch = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .update(update)
        .delete(PatchDelete::new(effects_table(), vec![Value::Integer(20)]))
        .insert(
            Insert::from(effects_table())
                .set(0, Value::Integer(100))
                .expect("set server key")
                .set(1, Value::Text("server".to_owned()))
                .expect("set server label"),
        );
    zstd::encode_all(patch.build().as_slice(), 3).expect("compress patch")
}

fn send_snapshot(server: &Script, rows: &[(i64, &str)], position: u8) {
    server.say(IncomingFrame::Control(ControlMessage::SnapshotBegin(
        SnapshotBegin {
            sub_id: "effects".to_owned(),
        },
    )));
    server.say(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(
        SnapshotPatch::new("effects".to_owned(), snapshot_payload(rows)),
    )));
    server.say(IncomingFrame::Control(ControlMessage::SnapshotEnd(
        SnapshotEnd {
            sub_id: "effects".to_owned(),
            cursor: Cursor::new(vec![position]),
        },
    )));
}

async fn subscribe_effects(connection: &mut ConnettoConnection<Script>, server: &Script) {
    connection
        .subscribe("effects", "SELECT * FROM effects")
        .await
        .expect("subscribe effects");
    send_snapshot(server, &[(10, "waiting"), (20, "retained")], 1);
    pump_until(
        connection,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "effects"),
    )
    .await;
}

#[tokio::test]
async fn derived_insert_update_delete_are_local_but_absent_from_upload() {
    let (mut connection, _) = connected().await;
    let sequence = queued(&mut connection).await;
    assert_eq!(
        support::queued_ops(connection.conn(), sequence),
        vec![r#"insert actions [Integer(1), Text("local")]"#]
    );
    assert_eq!(connection.push().await.expect("second push"), None);
}

#[tokio::test]
async fn refusal_inverts_derived_insert_update_delete() {
    let (mut connection, server) = connected().await;
    let sequence = queued(&mut connection).await;
    server.say(IncomingFrame::Control(ControlMessage::MutationReject(
        MutationReject {
            client_seq: sequence,
            reason: MutationRejectReason::Unauthorized,
        },
    )));
    pump_until(&mut connection, |event| matches!(event, ClientEvent::MutationRejected { client_seq, .. } if *client_seq == sequence)).await;
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), Vec::<i64>::new());
    assert_eq!(connection.push().await.expect("push after inverse"), None);
}

#[tokio::test]
async fn conflict_inverts_derived_insert_update_delete() {
    let (mut connection, server) = connected().await;
    let sequence = queued(&mut connection).await;
    server.say(IncomingFrame::Control(ControlMessage::MutationConflict(
        MutationConflict {
            client_seq: sequence,
            table: "actions".to_owned(),
            server_row: None,
        },
    )));
    pump_until(&mut connection, |event| matches!(event, ClientEvent::MutationConflict { client_seq, .. } if *client_seq == sequence)).await;
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), Vec::<i64>::new());
}

#[tokio::test]
async fn marker_before_refusal_or_conflict_keeps_the_primary_write_pending() {
    for conflict in [false, true] {
        let (mut connection, server) = connected().await;
        let sequence = queued(&mut connection).await;
        settle(&mut connection, &server, sequence, false).await;
        assert_eq!(effect_rows(&mut connection), baseline());
        assert_eq!(action_ids(&mut connection), vec![1]);
        let answer = if conflict {
            ControlMessage::MutationConflict(MutationConflict {
                client_seq: sequence,
                table: "actions".to_owned(),
                server_row: None,
            })
        } else {
            ControlMessage::MutationReject(MutationReject {
                client_seq: sequence,
                reason: MutationRejectReason::Unauthorized,
            })
        };
        server.say(IncomingFrame::Control(answer));
        pump_until(&mut connection, |event| {
            matches!(
                event,
                ClientEvent::MutationRejected { .. } | ClientEvent::MutationConflict { .. }
            )
        })
        .await;
        assert_eq!(action_ids(&mut connection), Vec::<i64>::new());
        assert_eq!(effect_rows(&mut connection), baseline());
    }
}

#[tokio::test]
async fn failed_derived_statement_resets_capture_for_execute_load_and_batch() {
    for boundary in 0..3 {
        let (mut connection, _) = connected().await;
        assert!(
            diesel::insert_into(actions::table)
                .values((actions::id.eq(-1), actions::label.eq("fail")))
                .execute(&mut connection)
                .is_err()
        );
        match boundary {
            0 => {
                insert_action(&mut connection, 1);
            }
            1 => {
                let identifiers: Vec<i64> = actions::table
                    .select(actions::id)
                    .load(&mut connection)
                    .expect("load resets marks");
                assert_eq!(identifiers, Vec::<i64>::new());
                insert_action(&mut connection, 1);
            }
            _ => connection
                .batch_execute("INSERT INTO actions VALUES (1, 'local')")
                .expect("batch resets marks"),
        }
        let sequence = connection
            .push()
            .await
            .expect("push after failure")
            .expect("queued action");
        assert_eq!(
            support::queued_ops(connection.conn(), sequence),
            vec![r#"insert actions [Integer(1), Text("local")]"#]
        );
        assert_eq!(effect_rows(&mut connection), provisional());
    }
}

#[tokio::test]
async fn rolled_back_transaction_discards_derived_capture() {
    let (mut connection, server) = connected().await;
    let result = connection.transaction::<(), diesel::result::Error, _>(|transaction| {
        diesel::insert_into(actions::table)
            .values((actions::id.eq(2), actions::label.eq("rolled back")))
            .execute(transaction)?;
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(result.is_err());
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), Vec::<i64>::new());
    assert_eq!(connection.push().await.expect("push rollback"), None);
    let sequence = queued(&mut connection).await;
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(effect_rows(&mut connection), baseline());
}

#[tokio::test]
async fn standalone_marker_drops_derived_rows_and_is_idempotent_before_ack() {
    let (mut connection, server) = connected().await;
    let sequence = queued(&mut connection).await;
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), vec![1]);
    settle(&mut connection, &server, sequence, false).await;
    acknowledge(&mut connection, &server, sequence).await;
    assert_eq!(effect_rows(&mut connection), baseline());
}

fn queued_bytes(connection: &mut ConnettoConnection<Script>, sequence: u64) -> Vec<u8> {
    diesel::select(
        diesel::dsl::sql::<diesel::sql_types::Binary>(
            "(SELECT changeset FROM _connetto_pending WHERE seq = ",
        )
        .bind::<diesel::sql_types::BigInt, _>(i64::try_from(sequence).expect("sequence"))
        .sql(" LIMIT 1)"),
    )
    .get_result(connection.conn())
    .expect("queued bytes")
}

#[tokio::test]
async fn derived_continuation_preserves_application_values_with_distinct_or_equal_endpoints() {
    for endpoint in ["last", "local"] {
        let schema = DDL.replace("SET label = 'accepted'", "SET label = NEW.label");
        let (mut connection, server) = connected_with(&schema).await;
        insert_action(&mut connection, 1);
        let first = connection
            .push()
            .await
            .expect("first derivation")
            .expect("entry");
        diesel::update(effects::table.filter(effects::id.eq(10)))
            .set(effects::label.eq("application"))
            .execute(connection.conn())
            .expect("application value");
        let application = connection
            .push()
            .await
            .expect("queue application value")
            .expect("entry");
        acknowledge(&mut connection, &server, application).await;
        diesel::insert_into(actions::table)
            .values((actions::id.eq(2), actions::label.eq(endpoint)))
            .execute(connection.conn())
            .expect("second derived value");
        let last = connection
            .push()
            .await
            .expect("push second derivation")
            .expect("entry");
        settle(&mut connection, &server, first, false).await;
        settle(&mut connection, &server, last, false).await;
        assert_eq!(
            effect_rows(&mut connection),
            vec![(10, "application".to_owned()), (20, "retained".to_owned()),]
        );
    }
}

#[tokio::test]
async fn refused_unmarked_continuations_restore_the_base_without_changing_upload_bytes() {
    for replay in 0..3 {
        let (mut connection, mut server) = connected().await;
        if replay == 2 {
            subscribe_effects(&mut connection, &server).await;
        }
        let first = queued(&mut connection).await;
        diesel::update(effects::table.filter(effects::id.eq_any([10, 31])))
            .set(effects::label.eq("application"))
            .execute(connection.conn())
            .expect("application writes");
        let second = connection
            .push()
            .await
            .expect("push continuation")
            .expect("entry");
        let uploaded = queued_bytes(&mut connection, second);
        settle(&mut connection, &server, first, false).await;
        assert_eq!(queued_bytes(&mut connection, second), uploaded);
        if replay == 1 {
            let archive = connection
                .export_local_data(connetto_client::ExportScope::Unsynced, Vec::new())
                .expect("export rollback overrides");
            let mut restored =
                ClientBuilder::new(support::bundle(DDL), support::Once::new(Script::default()))
                    .signed_in(support::held("derived-writer"))
                    .open_driven()
                    .expect("destination");
            seed(&mut restored);
            let plan = restored
                .import_local_data(std::io::Cursor::new(archive))
                .expect("read archive");
            restored
                .apply_import(&plan, &connetto_client::ImportChoices::keeping_the_file())
                .expect("restore continuations");
            server = Script::default();
            let IncomingFrame::Control(ControlMessage::HandshakeAck(mut acknowledgement)) =
                handshake_at(0)
            else {
                panic!("handshake");
            };
            acknowledgement.writer.as_mut().expect("grant").number = 9;
            server.say(IncomingFrame::Control(ControlMessage::HandshakeAck(
                acknowledgement,
            )));
            restored
                .attach(server.clone())
                .await
                .expect("rekey imported overrides");
            connection = restored;
        }
        if replay == 2 {
            server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
                FullResyncRequired {
                    sub_id: "effects".to_owned(),
                    reason: FullResyncReason::CursorBeyondHistory,
                },
            )));
            settle(&mut connection, &server, first, true).await;
            send_snapshot(&server, &[(10, "waiting"), (20, "retained")], 2);
            pump_until(&mut connection, |event| {
                matches!(event, ClientEvent::SnapshotEnd { .. })
            })
            .await;
            assert_eq!(
                effect_rows(&mut connection),
                vec![
                    (10, "application".to_owned()),
                    (20, "retained".to_owned()),
                    (31, "application".to_owned()),
                ]
            );
        }
        server.say(IncomingFrame::Control(ControlMessage::MutationReject(
            MutationReject {
                client_seq: second,
                reason: MutationRejectReason::Unauthorized,
            },
        )));
        pump_until(&mut connection, |event| {
            matches!(event, ClientEvent::MutationRejected { .. })
        })
        .await;
        assert_eq!(effect_rows(&mut connection), baseline());
        let remaining: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
            "(SELECT COUNT(*) FROM _connetto_rollback)",
        ))
        .get_result(connection.conn())
        .expect("override count");
        assert_eq!(remaining, 0);
    }
}

#[tokio::test]
async fn unqueued_continuation_override_is_captured_and_removed_on_applied_or_conflict() {
    for applied in [false, true] {
        let (mut connection, server) = connected().await;
        let first = queued(&mut connection).await;
        diesel::update(effects::table.filter(effects::id.eq_any([10, 31])))
            .set(effects::label.eq("application"))
            .execute(connection.conn())
            .expect("unqueued write");
        settle(&mut connection, &server, first, false).await;
        let pending: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
            "(SELECT COUNT(*) FROM _connetto_pending)",
        ))
        .get_result(connection.conn())
        .expect("pending count");
        assert_eq!(pending, 1);
        assert_eq!(
            connection.push().await.expect("push continuation"),
            Some(first + 1)
        );
        assert_ne!(queued_bytes(&mut connection, first + 1), Vec::<u8>::new());
        if applied {
            acknowledge(&mut connection, &server, first + 1).await;
            assert_eq!(
                effect_rows(&mut connection),
                vec![
                    (10, "application".to_owned()),
                    (20, "retained".to_owned()),
                    (31, "application".to_owned()),
                ]
            );
        } else {
            server.say(IncomingFrame::Control(ControlMessage::MutationConflict(
                MutationConflict {
                    client_seq: first + 1,
                    table: "effects".to_owned(),
                    server_row: None,
                },
            )));
            pump_until(&mut connection, |event| {
                matches!(event, ClientEvent::MutationConflict { .. })
            })
            .await;
            assert_eq!(effect_rows(&mut connection), baseline());
        }
        let remaining: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
            "(SELECT COUNT(*) FROM _connetto_rollback)",
        ))
        .get_result(connection.conn())
        .expect("override count");
        assert_eq!(remaining, 0);
    }
}

#[tokio::test]
async fn a_marker_inverts_multiple_entries_in_reverse_write_order() {
    let schema = DDL.replace("SET label = 'accepted'", "SET label = NEW.label");
    let (mut connection, server) = connected_with(&schema).await;
    let mut sequences = Vec::new();
    for (identifier, label) in [(1, "first"), (2, "second")] {
        diesel::insert_into(actions::table)
            .values((actions::id.eq(identifier), actions::label.eq(label)))
            .execute(connection.conn())
            .expect("write derived value");
        sequences.push(
            connection
                .push()
                .await
                .expect("push")
                .expect("queued entry"),
        );
    }
    settle(
        &mut connection,
        &server,
        *sequences.last().expect("last write"),
        false,
    )
    .await;
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), vec![1, 2]);
}

#[tokio::test]
async fn before_patch_inverse_precedes_deterministic_server_update_and_delete() {
    let (mut connection, server) = connected().await;
    subscribe_effects(&mut connection, &server).await;
    let sequence = queued(&mut connection).await;
    acknowledge(&mut connection, &server, sequence).await;
    assert_eq!(effect_rows(&mut connection), provisional());
    settle(&mut connection, &server, sequence, true).await;
    assert_eq!(effect_rows(&mut connection), provisional());
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "effects".to_owned(),
        Cursor::new(vec![2]),
        deterministic_payload(),
    ))));
    pump_until(
        &mut connection,
        |event| matches!(event, ClientEvent::LivePatch { sub_id, .. } if sub_id == "effects"),
    )
    .await;
    assert_eq!(
        effect_rows(&mut connection),
        vec![(10, "accepted".to_owned()), (100, "server".to_owned())]
    );
    settle(&mut connection, &server, sequence, false).await;
    acknowledge(&mut connection, &server, sequence).await;
    assert_eq!(
        effect_rows(&mut connection),
        vec![(10, "accepted".to_owned()), (100, "server".to_owned())]
    );
    assert_eq!(connection.push().await.expect("push server patch"), None);
}

#[tokio::test]
async fn failed_server_patch_rolls_back_its_derived_inverse() {
    let (mut connection, server) = connected().await;
    subscribe_effects(&mut connection, &server).await;
    let sequence = queued(&mut connection).await;
    settle(&mut connection, &server, sequence, true).await;
    connection
        .conn()
        .batch_execute(
            "CREATE TEMP TRIGGER refuse_cursor BEFORE INSERT ON _connetto_meta
         BEGIN SELECT RAISE(ABORT, 'cursor refused'); END;",
        )
        .expect("refuse cursor persistence");
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "effects".to_owned(),
        Cursor::new(vec![2]),
        deterministic_payload(),
    ))));
    let failed = tokio::time::timeout(Duration::from_secs(5), connection.pump_one())
        .await
        .expect("DERIVED-SLICES invalid patch deadline");
    assert!(failed.is_err());
    assert_eq!(effect_rows(&mut connection), provisional());
    let through: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
        "COALESCE((SELECT through_seq FROM _connetto_derived_marks WHERE writer = 'number:1'), 0)",
    ))
    .get_result(connection.conn())
    .expect("rolled back mark");
    assert_eq!(through, 0);
    connection
        .conn()
        .batch_execute("DROP TRIGGER refuse_cursor")
        .expect("allow cursor");
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "effects".to_owned(),
        Cursor::new(vec![2]),
        deterministic_payload(),
    ))));
    pump_until(&mut connection, |event| {
        matches!(event, ClientEvent::LivePatch { .. })
    })
    .await;
    let through: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
        "(SELECT through_seq FROM _connetto_derived_marks WHERE writer = 'number:1')",
    ))
    .get_result(connection.conn())
    .expect("committed mark");
    assert_eq!(through, i64::try_from(sequence).expect("sequence"));
}

#[tokio::test]
async fn raw_connection_resets_derived_capture_after_trigger_failure() {
    let (mut connection, server) = connected().await;
    assert!(
        connection
            .conn()
            .batch_execute("INSERT INTO actions VALUES (-1, 'fails')")
            .is_err()
    );
    connection
        .conn()
        .batch_execute("INSERT INTO actions VALUES (1, 'local')")
        .expect("write after failure");
    let sequence = connection
        .push()
        .await
        .expect("push")
        .expect("unmarked action");
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(action_ids(&mut connection), vec![1]);
    assert_eq!(effect_rows(&mut connection), baseline());
}

#[tokio::test]
async fn dropped_raw_cursor_resets_an_open_derived_run() {
    #[derive(diesel::QueryableByName)]
    struct Depth {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        depth: i32,
    }
    let (mut connection, server) = connected().await;
    {
        let mut cursor =
            diesel::sql_query("SELECT connetto_derived_write(1) AS depth UNION ALL SELECT 2")
                .load_iter::<Depth, diesel::connection::DefaultLoadingMode>(connection.conn())
                .expect("start cursor");
        assert_eq!(cursor.next().expect("first row").expect("depth").depth, 1);
    }
    connection
        .conn()
        .batch_execute("INSERT INTO actions VALUES (1, 'local')")
        .expect("write after cursor drop");
    let sequence = connection
        .push()
        .await
        .expect("push")
        .expect("unmarked action");
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(action_ids(&mut connection), vec![1]);
    assert_eq!(effect_rows(&mut connection), baseline());
}

#[tokio::test]
async fn archived_pending_writes_restore_and_reexport_derived_changesets() {
    let (mut source, _) = connected().await;
    queued(&mut source).await;
    let mut archive = source
        .export_local_data(connetto_client::ExportScope::Unsynced, Vec::new())
        .expect("export queued derived writes");
    for _ in 0..2 {
        let mut restored =
            ClientBuilder::new(support::bundle(DDL), support::Once::new(Script::default()))
                .signed_in(support::held("derived-writer"))
                .open_driven()
                .expect("open destination");
        seed(&mut restored);
        let plan = restored
            .import_local_data(std::io::Cursor::new(archive))
            .expect("read pending archive");
        assert_eq!(plan.queued_writes(), 1);
        restored
            .apply_import(&plan, &connetto_client::ImportChoices::keeping_the_file())
            .expect("restore pending entry");
        assert_eq!(effect_rows(&mut restored), provisional());
        assert_eq!(action_ids(&mut restored), vec![1]);
        archive = restored
            .export_local_data(connetto_client::ExportScope::Unsynced, Vec::new())
            .expect("reexport restored derived writes");
    }
}

#[tokio::test]
async fn archive_roundtrip_preserves_original_writer_high_water() {
    let (mut source, server) = connected().await;
    let sequence = queued(&mut source).await;
    settle(&mut source, &server, sequence, false).await;
    acknowledge(&mut source, &server, sequence).await;
    let archive = source
        .export_local_data(connetto_client::ExportScope::Unsynced, Vec::new())
        .expect("export settled mark");
    let mut restored =
        ClientBuilder::new(support::bundle(DDL), support::Once::new(Script::default()))
            .signed_in(support::held("derived-writer"))
            .open_driven()
            .expect("open destination");
    let plan = restored
        .import_local_data(std::io::Cursor::new(archive))
        .expect("read archive");
    restored
        .apply_import(&plan, &connetto_client::ImportChoices::keeping_the_file())
        .expect("restore mark");
    let mark: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
        "(SELECT through_seq FROM _connetto_derived_marks WHERE writer = 'number:1')",
    ))
    .get_result(restored.conn())
    .expect("original writer mark");
    assert_eq!(mark, i64::try_from(sequence).expect("sequence"));
    assert_eq!(
        restored.unsynced(),
        Vec::<connetto_client::UnsyncedWrite>::new()
    );
}

#[tokio::test]
async fn acknowledgement_keeps_derived_rows_until_the_marker() {
    let (mut connection, server) = connected().await;
    let sequence = queued(&mut connection).await;
    acknowledge(&mut connection, &server, sequence).await;
    assert_eq!(effect_rows(&mut connection), provisional());
    assert_eq!(action_ids(&mut connection), vec![1]);
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(effect_rows(&mut connection), baseline());
}

#[tokio::test]
async fn attach_drops_acknowledged_derived_rows_before_catchup() {
    let (mut connection, server) = connected().await;
    let sequence = queued(&mut connection).await;
    acknowledge(&mut connection, &server, sequence).await;
    server.hang_up();
    pump_until(&mut connection, |event| {
        matches!(event, ClientEvent::Closed)
    })
    .await;
    let replacement = Script::default();
    replacement.say(handshake_at(sequence));
    connection.attach(replacement).await.expect("reattach");
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), vec![1]);
}

#[tokio::test]
async fn resync_reapplies_pending_derived_insert_update_delete() {
    let (mut connection, server) = connected().await;
    subscribe_effects(&mut connection, &server).await;
    let sequence = queued(&mut connection).await;
    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "effects".to_owned(),
            reason: FullResyncReason::CursorBeyondHistory,
        },
    )));
    send_snapshot(&server, &[(10, "waiting"), (20, "retained")], 2);
    pump_until(
        &mut connection,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "effects"),
    )
    .await;
    assert_eq!(effect_rows(&mut connection), provisional());
    assert_eq!(action_ids(&mut connection), vec![1]);
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(effect_rows(&mut connection), baseline());
}

#[tokio::test]
async fn resync_drops_acknowledged_derived_rows_without_reapplying_them() {
    let (mut connection, server) = connected().await;
    subscribe_effects(&mut connection, &server).await;
    let sequence = queued(&mut connection).await;
    acknowledge(&mut connection, &server, sequence).await;
    server.say(IncomingFrame::Control(ControlMessage::FullResyncRequired(
        FullResyncRequired {
            sub_id: "effects".to_owned(),
            reason: FullResyncReason::CursorBeyondHistory,
        },
    )));
    settle(&mut connection, &server, sequence, true).await;
    send_snapshot(&server, &[(10, "accepted"), (100, "server")], 2);
    pump_until(
        &mut connection,
        |event| matches!(event, ClientEvent::SnapshotEnd { sub_id, .. } if sub_id == "effects"),
    )
    .await;
    assert_eq!(
        effect_rows(&mut connection),
        vec![(10, "accepted".to_owned()), (100, "server".to_owned())]
    );
}

#[cfg(feature = "peer")]
#[tokio::test]
async fn peer_landing_inverts_its_derived_writes_without_uploading_them() {
    use super::peer_support::{author, manifest, pending_changeset};
    use connetto_client::peer::PeerApplyOutcome;

    let (mut sender, _) = connected().await;
    let sequence = queued(&mut sender).await;
    let body = pending_changeset(&mut sender, i64::try_from(sequence).expect("sequence fits"));
    let (mut receiver, _) = connected().await;
    let outcome = receiver
        .apply_peer_entry(
            &author("derived-author", "derived-writer", None),
            &manifest(sequence, &body),
            &body,
        )
        .expect("land peer entry");
    assert!(matches!(outcome, PeerApplyOutcome::Applied { .. }));
    assert_eq!(action_ids(&mut receiver), vec![1]);
    assert_eq!(effect_rows(&mut receiver), baseline());
    assert_eq!(receiver.push().await.expect("push peer landing"), None);
}

#[cfg(feature = "native-auth")]
async fn durable(directory: &tempfile::TempDir) -> ConnettoConnection<Script> {
    let credential = support::held("derived-writer");
    let store = support::key_store(&credential).await;
    ClientBuilder::new(
        support::bundle(DDL),
        support::NeverDial::<Script>::default(),
    )
    .signed_in(credential)
    .durable(
        connetto_client::DataDir::new(directory.path().to_path_buf()),
        store,
        connetto_client::MemoryRefreshStore::default(),
    )
    .open_driven()
    .await
    .expect("open durable replica")
}

#[cfg(feature = "native-auth")]
#[tokio::test]
async fn old_replica_migrates_and_preserves_queued_writes() {
    let directory = tempfile::tempdir().expect("replica directory");
    let mut connection = durable(&directory).await;
    seed(&mut connection);
    diesel::insert_into(effects::table)
        .values((effects::id.eq(40), effects::label.eq("old pending")))
        .execute(&mut connection)
        .expect("old write");
    let old_sequence = connection
        .push()
        .await
        .expect("old push")
        .expect("old entry");
    drop(connection);
    let path = directory
        .path()
        .join(support::held("derived-writer").replica_name());
    let mut replica = SqliteConnection::establish(path.to_str().expect("replica path"))
        .expect("open old replica");
    connetto_client::cipher::unlock(&mut replica, &connetto_core::test_support::replica_key())
        .expect("unlock old replica");
    replica
        .batch_execute(
            "ALTER TABLE _connetto_outbox DROP COLUMN marked; DROP TABLE _connetto_derived;",
        )
        .expect("restore old bookkeeping schema");
    drop(replica);
    let mut connection = durable(&directory).await;
    assert_eq!(
        support::queued_ops(connection.conn(), old_sequence),
        vec![r#"insert effects [Integer(40), Text("old pending")]"#]
    );
    let server = Script::default();
    server.say(support::handshake_ack());
    connection.track_relay_notices();
    connection
        .attach(server.clone())
        .await
        .expect("attach migrated replica");
    assert_eq!(
        connection.take_relay_notices(),
        vec![connetto_client::RelayNotice::MutationRekey(
            connetto_client::MutationRekey {
                previous_writer: connetto_core::messages::WriterRef::Number(0),
                previous_seq: old_sequence,
                writer: connetto_core::messages::WriterRef::Number(1),
                client_seq: old_sequence,
            }
        )]
    );
    insert_action(&mut connection, 1);
    let sequence = connection
        .push()
        .await
        .expect("migrated push")
        .expect("new entry");
    assert!(sequence > old_sequence);
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(
        effect_rows(&mut connection),
        vec![
            (10, "waiting".to_owned()),
            (20, "retained".to_owned()),
            (40, "old pending".to_owned())
        ]
    );
}

#[tokio::test]
async fn settling_inverse_preserves_a_newer_server_value() {
    let (mut connection, server) = connected().await;
    let sequence = queued(&mut connection).await;
    connection
        .transact_with_bookkeeping(
            |replica| {
                diesel::update(effects::table.filter(effects::id.eq(10)))
                    .set(effects::label.eq("newer server value"))
                    .execute(replica)
            },
            |_| Ok::<_, diesel::result::Error>(()),
        )
        .expect("apply newer server value");
    settle(&mut connection, &server, sequence, false).await;
    assert_eq!(
        effect_rows(&mut connection),
        vec![
            (10, "newer server value".to_owned()),
            (20, "retained".to_owned()),
        ]
    );
}

#[cfg(feature = "native-auth")]
#[tokio::test]
async fn restarted_replica_drops_acknowledged_derived_rows_on_attach() {
    let directory = tempfile::tempdir().expect("replica directory");
    let mut connection = durable(&directory).await;
    seed(&mut connection);
    let server = Script::default();
    server.say(support::handshake_ack());
    connection.attach(server.clone()).await.expect("attach");
    let sequence = queued(&mut connection).await;
    acknowledge(&mut connection, &server, sequence).await;
    assert_eq!(effect_rows(&mut connection), provisional());
    drop(connection);
    let mut connection = durable(&directory).await;
    let server = Script::default();
    server.say(handshake_at(sequence));
    connection
        .attach(server)
        .await
        .expect("attach restarted replica");
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), vec![1]);
}

#[cfg(feature = "native-auth")]
#[tokio::test]
async fn restarted_replica_inverts_unacknowledged_derived_rows_on_attach() {
    let directory = tempfile::tempdir().expect("replica directory");
    let mut connection = durable(&directory).await;
    seed(&mut connection);
    let server = Script::default();
    server.say(support::handshake_ack());
    connection.attach(server).await.expect("attach");
    let sequence = queued(&mut connection).await;
    drop(connection);
    let mut connection = durable(&directory).await;
    assert_eq!(effect_rows(&mut connection), provisional());
    let server = Script::default();
    server.say(handshake_at(sequence));
    connection
        .attach(server.clone())
        .await
        .expect("attach restarted replica");
    assert_eq!(effect_rows(&mut connection), baseline());
    acknowledge(&mut connection, &server, sequence).await;
    assert_eq!(effect_rows(&mut connection), baseline());
    assert_eq!(action_ids(&mut connection), vec![1]);
}
