//! A nullable column added to a served table, against the old major's catalog.
//!
//! Step 3 of R107. The reads connetto builds are catalog-shaped. The stored
//! `SELECT` names the catalog's columns rather than a wildcard, so the
//! snapshot's inner query and the engine's re-execution carry the catalog's
//! column list, and the row a conflict re-read returns projects the
//! catalog's columns. A server built from an older schema keeps serving its
//! table once the live table gains a column. The client never receives the
//! added column, old-major writes still land, and an upload naming the
//! added column is refused with the schema mismatch.
//!
//! Needs Docker: the fixture starts its own Postgres.

use connetto_core::messages::{BulkMessage, ControlMessage, MutationRejectReason};
use connetto_core::traits::IncomingFrame;
use connetto_server::{PgSnapshotSource, RlsAuth};
use connetto_test_harness::{
    Client, Fixture, HarnessAuth, PUBLICATION, RowValue, Server, ServerConfig, drop_slot,
    insert_changeset, spawn_server,
};
use diesel::QueryDsl;
use diesel_async::RunQueryDsl;
use sqlite_diff_rs::{
    ChangeSet, ChangesetFormat, DiffOps, DynTable as _, Insert, ParsedDiffSet, PatchSet,
    PatchsetOp, SimpleTable, TableSchema, Update, Value,
};

use super::writer_shared::{note_table, seed_notes, update_changeset, writable_catalog};

/// The old major's DDL the server is built from.
const PG_DDL: &str = "-- connetto:major 1\n\
                      CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, edited_at TEXT);";
/// The expand: a nullable column on the served table.
const EXPAND: &str = "ALTER TABLE notes ADD COLUMN extra TEXT";
/// The old major's shape of `notes`.
const ARITY: usize = 3;

diesel::table! {
    /// The served table after the expand added a nullable column.
    notes (id) {
        /// Note identifier, the primary key.
        id -> diesel::sql_types::Integer,
        /// Note text.
        body -> diesel::sql_types::Text,
        /// Timestamp of the last edit.
        edited_at -> diesel::sql_types::Text,
        /// The column the expand adds under the running server.
        extra -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
    }
}

/// The notes table the old-major server serves, reading it from the old
/// major's DDL and replicating it for the change stream.
async fn stack(fixture: &Fixture) -> Server {
    let admin = fixture.admin().clone();
    let snapshot = PgSnapshotSource::from_ddl(admin.clone(), PG_DDL)
        .expect("snapshot source")
        .with_publication(PUBLICATION);
    let auth = HarnessAuth::rls(RlsAuth::from_ddl(admin.clone(), PG_DDL).expect("rls auth"));
    let config = ServerConfig::new(PG_DDL, fixture.admin_url())
        .with_writable(writable_catalog())
        .with_replication(["notes"]);
    spawn_server(config, snapshot, auth, admin.clone(), admin).await
}

/// A patchset's one insert op, checked to serve exactly the catalog's columns.
fn insert_serving_catalog(op: &PatchsetOp<TableSchema<String>, String, Vec<u8>>) {
    let PatchsetOp::Insert { values, .. } = &op else {
        panic!("expected an insert, got {op:?}");
    };
    assert_eq!(
        op.table().number_of_columns(),
        ARITY,
        "the read serves the catalog's columns, not the live table's"
    );
    assert_eq!(
        values.len(),
        ARITY,
        "a delivered row carries the catalog's cells, no added column"
    );
}

/// The one row the expanded shape of `notes` holds in its added column, per
/// primary key, read back through the admin pool.
async fn extras(fixture: &Fixture) -> Vec<Option<String>> {
    let mut conn = fixture.admin().get().await.expect("an admin connection");
    notes::table
        .select(notes::extra)
        .order(notes::id)
        .load(&mut *conn)
        .await
        .expect("read the added column")
}

/// The table as a post-expand client spells it, naming the added column.
fn expanded_table() -> SimpleTable {
    SimpleTable::new(
        "notes",
        &[("id", ""), ("body", ""), ("edited_at", ""), ("extra", "")],
        &[0],
    )
}

/// An insert naming the added column, the way a post-expand client spells it.
fn expanded_insert_changeset() -> Vec<u8> {
    insert_changeset(
        "notes",
        &["id", "body", "edited_at", "extra"],
        &[0],
        vec![
            Value::Integer(3),
            Value::Text("new".to_owned()),
            Value::Text("t1".to_owned()),
            Value::Text("added".to_owned()),
        ],
    )
}

/// An update naming the added column, its old image carrying the column's
/// absent value.
fn expanded_update_changeset() -> Vec<u8> {
    let update = Update::<_, ChangesetFormat, String, Vec<u8>>::from(expanded_table())
        .set(0, Value::Integer(1), Value::Integer(1))
        .expect("set id")
        .set(
            1,
            Value::Text("updated".to_owned()),
            Value::Text("stale".to_owned()),
        )
        .expect("set body")
        .set(
            2,
            Value::Text("t1".to_owned()),
            Value::Text("t2".to_owned()),
        )
        .expect("set edited_at")
        .set(3, Value::Null, Value::Text("added".to_owned()))
        .expect("set extra");
    ChangeSet::<SimpleTable, String, Vec<u8>>::new()
        .update(update)
        .build()
}

/// A patchset that inserts one full row. A patchset carries no prior image,
/// so no conflict is planned for it.
fn patchset_insert(id: i64, body: &str, edited_at: &str) -> Vec<u8> {
    let insert = Insert::<_, String, Vec<u8>>::from(note_table())
        .set(0, Value::Integer(id))
        .expect("set id")
        .set(1, Value::Text(body.to_owned()))
        .expect("set body")
        .set(2, Value::Text(edited_at.to_owned()))
        .expect("set edited_at");
    PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(insert)
        .build()
}

/// The client's answer to a conflicting write: the conflicting row, which a
/// client applies over its own, so its JSON must name only the catalog's
/// columns.
async fn conflict_row(client: &mut Client, seq: u64) -> serde_json::Value {
    let ControlMessage::MutationConflict(conflict) = client.next_control().await else {
        panic!("expected a mutation conflict");
    };
    assert_eq!(conflict.client_seq, seq);
    assert_eq!(conflict.table, "notes");
    let row = conflict
        .server_row
        .expect("the conflicting row still exists");
    serde_json::from_str(&row.row_json).expect("the row decodes as a JSON object")
}

/// The `notes` rows the snapshot of `sub_id` delivered, page by page.
async fn snapshot_rows(client: &mut Client, sub_id: &str) -> Vec<Vec<RowValue>> {
    let ControlMessage::SnapshotBegin(begin) = client.next_control().await else {
        panic!("expected a snapshot begin");
    };
    assert_eq!(begin.sub_id, sub_id);
    let mut rows = Vec::new();
    loop {
        match client.recv().await {
            Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(patch))) => {
                assert_eq!(patch.sub_id, sub_id);
                let raw = zstd::decode_all(patch.patchset_zstd.as_slice())
                    .expect("decompress the patchset");
                let ParsedDiffSet::Patchset(set) = ParsedDiffSet::parse(&raw).expect("parse")
                else {
                    panic!("the snapshot page is a patchset");
                };
                for op in set.iter() {
                    insert_serving_catalog(&op);
                    let PatchsetOp::Insert { values, .. } = &op else {
                        panic!("expected an insert, got {op:?}");
                    };
                    rows.push(values.to_vec());
                }
                client.ack_credits(1).await;
            }
            Some(IncomingFrame::Control(ControlMessage::SnapshotEnd(end))) => {
                assert_eq!(end.sub_id, sub_id);
                return rows;
            }
            other => panic!("expected a page or the end of the read, got {other:?}"),
        }
    }
}

/// The one row the table holds at the moment the snapshot starts, so a live
/// change the test makes after it is the only delta the subscription may see.
async fn seed_second_row(fixture: &Fixture) {
    fixture
        .exec("INSERT INTO notes (id, body, edited_at) VALUES (2, 'world', 't1')")
        .await;
}

/// The snapshot read connetto builds serves the catalog's shape past an
/// added column. The stored `SELECT` names the catalog's columns, so the
/// inner read never carries the column the live table gained, and the
/// client's query keeps the columns it asked for.
#[tokio::test]
async fn snapshot_reads_serve_the_catalog_shape_past_an_added_column() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    seed_second_row(&fixture).await;
    fixture.setup(&[EXPAND]).await;

    let server = stack(&fixture).await;
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;
    client.subscribe("rows", "SELECT id, body FROM notes").await;
    let mut rows = snapshot_rows(&mut client, "rows").await;
    rows.sort_by_key(|row| match row[0] {
        Value::Integer(first) => first,
        _ => i64::MAX,
    });
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Text("hello".to_owned()),
                Value::Text("t0".to_owned()),
            ],
            vec![
                Value::Integer(2),
                Value::Text("world".to_owned()),
                Value::Text("t1".to_owned()),
            ],
        ],
        "the snapshot serves the old major's shape"
    );
    client.close().await;
    drop_slot(fixture.admin()).await;
}

/// A stale write's conflict re-reads the current row, and the settled
/// conflict re-answers it with a fresh re-read. Neither names the column the
/// expand added under the running server, and the write path still lands the
/// old major's shape.
#[tokio::test]
async fn conflict_rereads_never_name_an_added_column() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;

    let server = stack(&fixture).await;
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;

    // Row 1 moves under the old major before the expand.
    client
        .upload(1, update_changeset(1, "hello", "updated", "t0", "t1"))
        .await;
    let ControlMessage::MutationApplied(ack) = client.next_control().await else {
        panic!("the write applies");
    };
    assert_eq!(ack.client_seq, 1);

    // The expand lands under the running server.
    fixture.exec(EXPAND).await;

    // A stale update meets a conflict that re-reads row 1.
    client
        .upload(2, update_changeset(1, "updated", "stale", "t0", "t2"))
        .await;
    let row = conflict_row(&mut client, 2).await;
    assert!(
        row.get("extra").is_none(),
        "the re-read must not hand the client the added column: {row}"
    );
    assert_eq!(row["body"], "updated");
    assert_eq!(row["edited_at"], "t1");

    // Re-sending the settled conflict re-answers it from a fresh re-read of
    // the same row.
    client
        .upload(2, update_changeset(1, "updated", "stale", "t0", "t2"))
        .await;
    let row = conflict_row(&mut client, 2).await;
    assert!(
        row.get("extra").is_none(),
        "the settled re-answer must not name the added column: {row}"
    );
    assert_eq!(row["body"], "updated");

    // A write on the right basis still lands, naming only the catalog's
    // columns, so the added column keeps its database default.
    client
        .upload(3, update_changeset(1, "updated", "done", "t1", "t3"))
        .await;
    let ControlMessage::MutationApplied(ack) = client.next_control().await else {
        panic!("the write applies");
    };
    assert_eq!(ack.client_seq, 3);
    assert_eq!(
        extras(&fixture).await,
        vec![None],
        "the write leaves the added column at its default"
    );
    client.close().await;
    drop_slot(fixture.admin()).await;
}

/// An upload that names the column the expand added speaks a shape the old
/// major's catalog does not know, so the write is refused as a schema
/// mismatch and lands nothing.
#[tokio::test]
async fn an_upload_naming_the_added_column_is_refused_as_a_schema_mismatch() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.setup(&[EXPAND]).await;

    let server = stack(&fixture).await;
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;

    client.upload(1, expanded_insert_changeset()).await;
    let ControlMessage::MutationReject(reject) = client.next_control().await else {
        panic!("expected a refusal of the upload that names the added column");
    };
    assert_eq!(reject.client_seq, 1);
    assert_eq!(
        reject.reason,
        MutationRejectReason::SchemaMismatch,
        "a shape the catalog does not know is a schema mismatch"
    );

    client.upload(2, expanded_update_changeset()).await;
    let ControlMessage::MutationReject(reject) = client.next_control().await else {
        panic!("expected a refusal of the update that names the added column");
    };
    assert_eq!(reject.client_seq, 2);
    assert_eq!(
        reject.reason,
        MutationRejectReason::SchemaMismatch,
        "a shape the catalog does not know is a schema mismatch"
    );

    // Neither op landed, and the refused entry does not hold the frontier.
    assert_eq!(extras(&fixture).await, vec![None]);
    client
        .upload(3, update_changeset(1, "hello", "after", "t0", "t1"))
        .await;
    let ControlMessage::MutationApplied(ack) = client.next_control().await else {
        panic!("the write applies");
    };
    assert_eq!(ack.client_seq, 3);
    client.close().await;
    drop_slot(fixture.admin()).await;
}

/// A patchset's insert carries no prior image, so no conflict is planned,
/// and it lands in the catalog's shape the way a changeset's insert does.
#[tokio::test]
async fn a_patchset_insert_lands_in_the_catalog_shape() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.setup(&[EXPAND]).await;

    let server = stack(&fixture).await;
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;

    client.upload(1, patchset_insert(2, "new", "t1")).await;
    let ControlMessage::MutationApplied(ack) = client.next_control().await else {
        panic!("the patchset's insert applies");
    };
    assert_eq!(ack.client_seq, 1);
    assert_eq!(
        extras(&fixture).await,
        vec![None, None],
        "the insert leaves the added column at its default"
    );
    client.close().await;
    drop_slot(fixture.admin()).await;
}

/// A column the live table loses under the running server breaks the
/// catalog-shaped re-read. The presence probe still answers on the surviving
/// key and version, and the row read fails on the column the catalog names,
/// so the write is refused and the server keeps serving.
#[tokio::test]
async fn a_write_past_a_lost_column_is_refused_with_a_constraint_error() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;

    let server = stack(&fixture).await;
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;

    // The live table loses a column the catalog still names.
    fixture.exec("ALTER TABLE notes DROP COLUMN body").await;

    // The basis matches no row, so the probe re-reads the current one.
    client
        .upload(1, update_changeset(1, "stale", "stale", "t9", "t2"))
        .await;
    let ControlMessage::MutationReject(reject) = client.next_control().await else {
        panic!("expected the refusal of the write past the lost column");
    };
    assert_eq!(reject.client_seq, 1);
    assert!(
        matches!(reject.reason, MutationRejectReason::Constraint { .. }),
        "a failed re-read is a constraint error: {reject:?}"
    );

    // The refusal is graceful and repeatable, not a wedged server.
    client
        .upload(2, update_changeset(1, "stale", "stale", "t9", "t2"))
        .await;
    let ControlMessage::MutationReject(reject) = client.next_control().await else {
        panic!("expected the repeatable refusal of the write past the lost column");
    };
    assert_eq!(reject.client_seq, 2);
    assert!(
        matches!(reject.reason, MutationRejectReason::Constraint { .. }),
        "the repeatable refusal is a constraint error: {reject:?}"
    );
    client.close().await;
    drop_slot(fixture.admin()).await;
}
