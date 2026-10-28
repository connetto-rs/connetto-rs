//! The outbox records every application write to a synced table inside the
//! transaction that makes it, and nothing connetto writes itself, and `push`
//! builds the upload from what it recorded.

use connetto_client::{ClientBuilder, ClientError, ConnettoConnection};
use connetto_core::test_support::FakeTransport;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, weight REAL, data BLOB)";

/// One recorded write.
#[derive(Debug, PartialEq, Eq, diesel::QueryableByName)]
struct Recorded {
    #[diesel(sql_type = diesel::sql_types::Text)]
    tbl: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    op: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    old: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    new: Option<String>,
}

fn recorded(conn: &mut ConnettoConnection<FakeTransport>) -> Vec<Recorded> {
    diesel::sql_query("SELECT tbl, op, old, new FROM _connetto_outbox ORDER BY seq")
        .load(conn.conn())
        .expect("read the outbox")
}

/// One blob cell stored beside its outbox row.
#[derive(Debug, PartialEq, Eq, diesel::QueryableByName)]
struct Blob {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    seq: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    col: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    side: String,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    value: Vec<u8>,
}

fn blobs(conn: &mut ConnettoConnection<FakeTransport>) -> Vec<Blob> {
    diesel::sql_query(
        "SELECT seq, col, side, value FROM _connetto_outbox_blob ORDER BY seq, col, side",
    )
    .load(conn.conn())
    .expect("read the outbox's blobs")
}

fn blob(seq: i64, col: &str, side: &str, value: &[u8]) -> Blob {
    Blob {
        seq,
        col: col.to_owned(),
        side: side.to_owned(),
        value: value.to_vec(),
    }
}

/// The operations of queued entry `seq`, sorted, since a changeset orders a
/// table's rows by hash.
fn sorted_ops(conn: &mut ConnettoConnection<FakeTransport>, seq: u64) -> Vec<String> {
    let mut ops = super::support::queued_ops(conn.conn(), seq);
    ops.sort();
    ops
}

fn open(ddl: &str) -> ConnettoConnection<FakeTransport> {
    ClientBuilder::new(
        super::support::bundle(ddl),
        super::support::NeverDial::<FakeTransport>::default(),
    )
    .open_driven()
    .expect("open the replica")
}

fn row(tbl: &str, op: &str, old: Option<&str>, new: Option<&str>) -> Recorded {
    Recorded {
        tbl: tbl.to_owned(),
        op: op.to_owned(),
        old: old.map(str::to_owned),
        new: new.map(str::to_owned),
    }
}

/// Each value keeps its storage class: a real round trips through its 17
/// digits, text with a quote stays text, and a blob is stored raw beside its
/// row with a marker in the JSON. An update records the key and the cells it
/// changed, an insert the whole new row and a delete the whole old one.
#[tokio::test]
async fn each_write_is_recorded_with_its_values() {
    let mut conn = open(DDL);
    conn.batch_execute(
        "INSERT INTO items VALUES (1, 'it''s \"here\"', 0.1, x'00ff'); \
         UPDATE items SET label = NULL, weight = 2 WHERE id = 1; \
         DELETE FROM items WHERE id = 1",
    )
    .expect("write a row three times");

    let inserted =
        r#"{"id":1,"label":"it's \"here\"","weight":{"r":"0.10000000000000001"},"data":{"b":0}}"#;
    let before = r#"{"id":1,"label":"it's \"here\"","weight":{"r":"0.10000000000000001"}}"#;
    let after = r#"{"id":1,"label":null,"weight":{"r":"2.0"}}"#;
    let deleted = r#"{"id":1,"label":null,"weight":{"r":"2.0"},"data":{"b":0}}"#;
    assert_eq!(
        recorded(&mut conn),
        vec![
            row("items", "I", None, Some(inserted)),
            row("items", "U", Some(before), Some(after)),
            row("items", "D", Some(deleted), None),
        ]
    );
    assert_eq!(
        blobs(&mut conn),
        vec![
            blob(1, "data", "n", &[0x00, 0xff]),
            blob(3, "data", "o", &[0x00, 0xff]),
        ]
    );
}

/// A photo is stored once at its own size, and editing the row's caption
/// afterwards records no photo bytes and uploads the caption alone.
#[tokio::test]
async fn a_blob_is_stored_once_and_a_caption_edit_carries_none_of_it() {
    let mut conn = open(DDL);
    conn.batch_execute("INSERT INTO items (id, label, data) VALUES (1, 'cat', zeroblob(12000))")
        .expect("insert a photo");
    let stored: Vec<Blob> = blobs(&mut conn);
    assert_eq!(stored.len(), 1, "one stored copy of the photo");
    assert_eq!(stored[0].value.len(), 12000, "stored raw at its own size");
    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        blobs(&mut conn),
        Vec::new(),
        "the push took the photo's copy"
    );

    conn.batch_execute("UPDATE items SET label = 'cat on a mat' WHERE id = 1")
        .expect("edit the caption");
    assert_eq!(
        recorded(&mut conn),
        vec![row(
            "items",
            "U",
            Some(r#"{"id":1,"label":"cat"}"#),
            Some(r#"{"id":1,"label":"cat on a mat"}"#)
        )]
    );
    assert_eq!(blobs(&mut conn), Vec::new(), "no photo bytes recorded");
    assert_eq!(conn.push().await.expect("push"), Some(2));
    assert_eq!(
        sorted_ops(&mut conn, 2),
        vec![
            r#"update items [(Some(Integer(1)), None), (Some(Text("cat")), Some(Text("cat on a mat"))), (None, None), (None, None)]"#
        ]
    );
}

/// An update that moves a row's key records both whole rows, since the
/// upload carries it as a delete and an insert.
#[tokio::test]
async fn a_key_change_records_both_whole_rows() {
    let mut conn = open(DDL);
    conn.transact_with_bookkeeping(
        |db| db.batch_execute("INSERT INTO items VALUES (1, 'kept', 1.5, NULL)"),
        |_| Ok::<_, diesel::result::Error>(()),
    )
    .expect("a row the server sent");
    conn.batch_execute("UPDATE items SET id = 2 WHERE id = 1")
        .expect("move the key");

    assert_eq!(
        recorded(&mut conn),
        vec![row(
            "items",
            "U",
            Some(r#"{"id":1,"label":"kept","weight":{"r":"1.5"},"data":null}"#),
            Some(r#"{"id":2,"label":"kept","weight":{"r":"1.5"},"data":null}"#)
        )]
    );
    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        sorted_ops(&mut conn, 1),
        vec![
            r#"delete items [Integer(1), Text("kept"), Real(1.5), Null]"#,
            r#"insert items [Integer(2), Text("kept"), Real(1.5), Null]"#,
        ]
    );
}

/// A change a column's collation calls equal, or one only of storage class,
/// is still a change the server has to receive.
#[tokio::test]
async fn a_change_only_case_or_storage_class_tells_apart_is_recorded() {
    let mut conn =
        open("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT COLLATE NOCASE, any)");
    conn.transact_with_bookkeeping(
        |db| db.batch_execute("INSERT INTO items VALUES (1, 'cat', 1)"),
        |_| Ok::<_, diesel::result::Error>(()),
    )
    .expect("a row the server sent");
    conn.batch_execute("UPDATE items SET label = 'CAT', any = 1.0 WHERE id = 1")
        .expect("change case and storage class");

    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        sorted_ops(&mut conn, 1),
        vec![
            r#"update items [(Some(Integer(1)), None), (Some(Text("cat")), Some(Text("CAT"))), (Some(Integer(1)), Some(Real(1.0)))]"#
        ]
    );
}

/// A write the application rolls back leaves nothing behind, because the
/// record is made inside the same transaction.
#[tokio::test]
async fn a_rolled_back_write_leaves_no_record() {
    let mut conn = open(DDL);
    let rolled_back = conn
        .conn()
        .transaction::<(), diesel::result::Error, _>(|db| {
            db.batch_execute("INSERT INTO items (id) VALUES (1)")?;
            Err(diesel::result::Error::RollbackTransaction)
        });
    assert!(rolled_back.is_err(), "the transaction rolled back");

    assert_eq!(recorded(&mut conn), Vec::new());
}

/// connetto's own writes raise the write exemption and are never recorded.
#[tokio::test]
async fn bookkeeping_writes_are_not_recorded() {
    let mut conn = open(DDL);
    conn.transact_with_bookkeeping(
        |db| db.batch_execute("INSERT INTO items (id) VALUES (1)"),
        |_| Ok::<_, diesel::result::Error>(()),
    )
    .expect("bookkeeping write");

    assert_eq!(recorded(&mut conn), Vec::new());
}

/// `push` deletes the records of what it queued, and of writes that cancel
/// out and queue nothing.
#[tokio::test]
async fn push_clears_what_it_covered() {
    let mut conn = open(DDL);
    conn.batch_execute("INSERT INTO items (id) VALUES (1)")
        .expect("write");
    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        recorded(&mut conn),
        Vec::new(),
        "the queued write's record went"
    );

    conn.batch_execute("INSERT INTO items (id) VALUES (2); DELETE FROM items WHERE id = 2")
        .expect("write and undo");
    assert_eq!(conn.push().await.expect("push"), None, "nothing to send");
    assert_eq!(
        recorded(&mut conn),
        Vec::new(),
        "the cancelled writes' records went"
    );
}

/// A key change, a row inserted and deleted again, two updates of one row,
/// a delete and insert of one key, updates of two columns one after the
/// other, and an update then a delete all reach the upload merged per row as
/// SQLite's session merges them.
#[tokio::test]
async fn merged_writes_upload_as_the_session_merges_them() {
    let mut conn = open(DDL);
    conn.batch_execute(
        "INSERT INTO items (id, label) VALUES (1, 'a'), (5, 'e'); \
         UPDATE items SET id = 2 WHERE id = 1; \
         INSERT INTO items (id) VALUES (3); DELETE FROM items WHERE id = 3; \
         UPDATE items SET label = 'b' WHERE id = 2; UPDATE items SET label = 'c' WHERE id = 2",
    )
    .expect("first round of writes");
    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        sorted_ops(&mut conn, 1),
        vec![
            r#"insert items [Integer(2), Text("c"), Null, Null]"#,
            r#"insert items [Integer(5), Text("e"), Null, Null]"#,
        ]
    );

    conn.batch_execute(
        "UPDATE items SET id = 6 WHERE id = 5; \
         DELETE FROM items WHERE id = 2; INSERT INTO items (id, label) VALUES (2, 'again'); \
         UPDATE items SET weight = 1.5 WHERE id = 6; UPDATE items SET weight = NULL WHERE id = 6",
    )
    .expect("second round over pushed rows");
    assert_eq!(conn.push().await.expect("push"), Some(2));
    assert_eq!(
        sorted_ops(&mut conn, 2),
        vec![
            r#"delete items [Integer(5), Text("e"), Null, Null]"#,
            r#"insert items [Integer(6), Text("e"), Null, Null]"#,
            r#"update items [(Some(Integer(2)), None), (Some(Text("c")), Some(Text("again"))), (None, None), (None, None)]"#,
        ]
    );

    conn.batch_execute(
        "UPDATE items SET label = 'x' WHERE id = 6; UPDATE items SET weight = 3.5 WHERE id = 6; \
         UPDATE items SET label = 'y' WHERE id = 2; DELETE FROM items WHERE id = 2",
    )
    .expect("third round over pushed rows");
    assert_eq!(conn.push().await.expect("push"), Some(3));
    assert_eq!(
        sorted_ops(&mut conn, 3),
        vec![
            r#"delete items [Integer(2), Text("again"), Null, Null]"#,
            r#"update items [(Some(Integer(6)), None), (Some(Text("e")), Some(Text("x"))), (Some(Null), Some(Real(3.5))), (None, None)]"#,
        ]
    );
}

/// A table created with `change_schema` is recorded from its first write.
#[tokio::test]
async fn a_table_made_with_change_schema_is_recorded_from_its_first_write() {
    let mut conn = open(DDL);
    conn.change_schema("CREATE TABLE later (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create a synced table at run time");
    conn.batch_execute("INSERT INTO later VALUES (1, 'kept')")
        .expect("write to it");

    assert_eq!(
        recorded(&mut conn),
        vec![row("later", "I", None, Some(r#"{"id":1,"body":"kept"}"#))]
    );
    assert_eq!(conn.push().await.expect("push"), Some(1));
}

/// DDL on a synced table run on the connection itself would leave the
/// triggers behind the table, so SQLite refuses it at the statement, while an
/// index, which changes no row, is the application's to make.
#[tokio::test]
async fn table_ddl_on_the_connection_is_refused() {
    let mut conn = open(DDL);
    for ddl in [
        "CREATE TABLE later (id INTEGER PRIMARY KEY, body TEXT)",
        "ALTER TABLE items ADD COLUMN extra TEXT",
        "ALTER TABLE items RENAME TO goods",
        "DROP TABLE items",
    ] {
        let refused = conn
            .batch_execute(ddl)
            .expect_err("table DDL on the connection is refused");
        assert!(
            refused.to_string().contains("not authorized"),
            "{ddl} refused by the authorizer: {refused}"
        );
    }
    conn.batch_execute("CREATE INDEX items_label ON items (label)")
        .expect("an index is the application's to make");
}

/// A column added between two writes reaches the upload with the value each
/// write gave it, the default for the row written before it existed.
#[tokio::test]
async fn a_column_added_between_writes_reaches_the_upload() {
    let mut conn = open(DDL);
    conn.batch_execute("INSERT INTO items (id, label) VALUES (1, 'before')")
        .expect("write before the change");
    conn.change_schema("ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'plain'")
        .expect("add a column");
    conn.batch_execute("INSERT INTO items (id, label, extra) VALUES (2, 'after', 'given')")
        .expect("write after the change");

    assert_eq!(
        recorded(&mut conn).last(),
        Some(&row(
            "items",
            "I",
            None,
            Some(r#"{"id":2,"label":"after","weight":null,"data":null,"extra":"given"}"#)
        ))
    );
    assert_eq!(conn.push().await.expect("push"), Some(1));
}

/// A dropped column no longer named by a trigger, and a renamed table
/// recorded once under its new name rather than also by the triggers that
/// followed it from the old one.
#[tokio::test]
async fn a_dropped_column_and_a_renamed_table_keep_recording() {
    let mut conn = open(DDL);
    conn.change_schema("ALTER TABLE items DROP COLUMN data; ALTER TABLE items RENAME TO goods")
        .expect("drop a column and rename the table");
    conn.batch_execute("INSERT INTO goods (id, label) VALUES (1, 'renamed')")
        .expect("write to the renamed table");

    assert_eq!(
        recorded(&mut conn),
        vec![row(
            "goods",
            "I",
            None,
            Some(r#"{"id":1,"label":"renamed","weight":null}"#)
        )]
    );
    assert_eq!(conn.push().await.expect("push"), Some(1));
}

/// DDL that fails, or that leaves a table connetto cannot record, changes
/// nothing, and the triggers dropped for it come back.
#[tokio::test]
async fn a_refused_schema_change_changes_nothing() {
    let mut conn = open(DDL);
    match conn
        .change_schema("ALTER TABLE items ADD COLUMN extra TEXT; CREATE TABLE loose (body TEXT)")
    {
        Err(ClientError::WritesNotRecorded { tables }) => assert!(
            tables.iter().any(|refusal| refusal.contains("loose")),
            "names the unkeyed table: {tables:?}"
        ),
        Err(other) => panic!("expected a key refusal, got {other}"),
        Ok(()) => panic!("expected a key refusal, got success"),
    }
    conn.batch_execute("INSERT INTO items (id) VALUES (1)")
        .expect("the table kept its old shape");

    assert_eq!(
        recorded(&mut conn),
        vec![row(
            "items",
            "I",
            None,
            Some(r#"{"id":1,"label":null,"weight":null,"data":null}"#)
        )]
    );
}

/// A row connetto rewrites after a local write and before the push, as a
/// server frame does, uploads the values the application wrote.
#[tokio::test]
async fn a_row_rewritten_by_connetto_before_the_push_uploads_the_written_values() {
    let mut conn = open(DDL);
    conn.batch_execute("INSERT INTO items (id, label) VALUES (1, 'local'), (2, 'kept')")
        .expect("local writes");
    conn.transact_with_bookkeeping(
        |db| db.batch_execute("UPDATE items SET label = 'server' WHERE id = 1"),
        |_| Ok::<_, diesel::result::Error>(()),
    )
    .expect("an exempt rewrite, as a frame applies one");

    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        sorted_ops(&mut conn, 1),
        vec![
            r#"insert items [Integer(1), Text("local"), Null, Null]"#,
            r#"insert items [Integer(2), Text("kept"), Null, Null]"#,
        ]
    );
}

/// The queue entry and the deletion of the records it covers commit
/// together, so a push whose clearing fails queues nothing and keeps every
/// record for the next push, cancelled writes included.
#[tokio::test]
async fn a_push_that_cannot_clear_the_outbox_queues_nothing() {
    let mut conn = open(DDL);
    conn.batch_execute(
        "CREATE TEMP TRIGGER refuse_clearing BEFORE DELETE ON main._connetto_outbox \
         BEGIN SELECT RAISE(ABORT, 'clearing refused'); END",
    )
    .expect("make clearing the outbox fail");

    conn.batch_execute("INSERT INTO items (id) VALUES (1); DELETE FROM items WHERE id = 1")
        .expect("write and undo");
    let refused = conn
        .push()
        .await
        .expect_err("the cancelled writes cannot be cleared");
    assert!(
        refused.to_string().contains("clearing refused"),
        "{refused}"
    );
    assert_eq!(
        recorded(&mut conn).len(),
        2,
        "the cancelled writes keep their records"
    );

    conn.batch_execute("INSERT INTO items (id) VALUES (2)")
        .expect("write");
    let refused = conn.push().await.expect_err("the write cannot be cleared");
    assert!(
        refused.to_string().contains("clearing refused"),
        "{refused}"
    );
    assert!(conn.unsynced().is_empty(), "nothing was queued");
    assert_eq!(recorded(&mut conn).len(), 3, "every record is kept");

    conn.batch_execute("DROP TRIGGER temp.refuse_clearing")
        .expect("let clearing succeed");
    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(conn.unsynced().len(), 1, "the write is queued once");
    assert_eq!(recorded(&mut conn), Vec::new(), "and its records are gone");
}

/// DDL that fails partway changes nothing, and the triggers the schema change
/// dropped before it come back with the rollback.
#[tokio::test]
async fn a_failing_schema_change_keeps_recording() {
    let mut conn = open(DDL);
    conn.change_schema(
        "ALTER TABLE items ADD COLUMN extra TEXT; CREATE TABLE items (id INTEGER PRIMARY KEY)",
    )
    .expect_err("the second statement names a table that exists");
    conn.batch_execute("INSERT INTO items (id) VALUES (1)")
        .expect("write");

    assert_eq!(
        recorded(&mut conn),
        vec![row(
            "items",
            "I",
            None,
            Some(r#"{"id":1,"label":null,"weight":null,"data":null}"#)
        )]
    );
}

/// A schema change whose triggers cannot be created, here because a trigger
/// already holds the name connetto gives one, changes nothing, and the
/// tables recorded before it stay recorded.
#[tokio::test]
async fn a_schema_change_that_cannot_create_its_triggers_changes_nothing() {
    let mut conn = open(DDL);
    conn.batch_execute(
        "CREATE TEMP TRIGGER \"_connetto_outbox_I_later\" AFTER INSERT ON main.items \
         BEGIN SELECT 1; END",
    )
    .expect("a trigger holding connetto's name for the new table");
    conn.change_schema("CREATE TABLE later (id INTEGER PRIMARY KEY)")
        .expect_err("the new table's trigger cannot be created");
    assert!(
        conn.batch_execute("INSERT INTO later (id) VALUES (1)")
            .is_err(),
        "the table was not created"
    );
    conn.batch_execute("INSERT INTO items (id) VALUES (1)")
        .expect("write");

    assert_eq!(
        recorded(&mut conn),
        vec![row(
            "items",
            "I",
            None,
            Some(r#"{"id":1,"label":null,"weight":null,"data":null}"#)
        )]
    );
}

/// How many objects named `names` the main and temp schemas hold.
fn schema_objects(conn: &mut ConnettoConnection<FakeTransport>, names: &str) -> i64 {
    diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
        "(SELECT count(*) FROM (SELECT name FROM sqlite_master \
          UNION ALL SELECT name FROM sqlite_temp_master) WHERE name IN ({names}))"
    )))
    .get_result(conn.conn())
    .expect("read the catalog")
}

/// A drop asks SQLite's authorizer to delete the object it drops, and gives
/// up without an error when refused, so a view, a temporary table, a
/// temporary view and a virtual table the application drops on the
/// connection are really gone.
#[tokio::test]
async fn the_objects_the_connection_may_drop_really_go() {
    let mut conn = open(DDL);
    conn.batch_execute(
        "CREATE VIEW labels AS SELECT label FROM items; \
         CREATE TEMP TABLE scratch (x); \
         CREATE TEMP VIEW recent AS SELECT id FROM items; \
         CREATE VIRTUAL TABLE temp.search USING fts5(body)",
    )
    .expect("create one object of each kind");
    let names = "'labels', 'scratch', 'recent', 'search'";
    assert_eq!(schema_objects(&mut conn, names), 4);

    conn.batch_execute("DROP VIEW labels; DROP TABLE scratch; DROP VIEW recent; DROP TABLE search")
        .expect("drop each");
    assert_eq!(
        schema_objects(&mut conn, names),
        0,
        "every drop took effect"
    );
}

/// An update to a row keyed by a real number is an update of that row, not
/// a delete and an insert, since the key compares by its stored bits.
#[tokio::test]
async fn a_row_keyed_by_a_real_number_uploads_as_an_update() {
    let mut conn = open("CREATE TABLE readings (at REAL PRIMARY KEY, label TEXT)");
    conn.transact_with_bookkeeping(
        |db| db.batch_execute("INSERT INTO readings VALUES (1.5, 'cold')"),
        |_| Ok::<_, diesel::result::Error>(()),
    )
    .expect("a row the server sent");
    conn.batch_execute("UPDATE readings SET label = 'warm' WHERE at = 1.5")
        .expect("edit the label");

    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        sorted_ops(&mut conn, 1),
        vec![
            r#"update readings [(Some(Real(1.5)), None), (Some(Text("cold")), Some(Text("warm")))]"#
        ]
    );
}

/// A delete uploads the whole old row, a blob cell included, since the
/// server reads it for the delete's conflict and policy checks.
#[tokio::test]
async fn a_deleted_rows_blob_reaches_the_upload() {
    let mut conn = open(DDL);
    conn.transact_with_bookkeeping(
        |db| db.batch_execute("INSERT INTO items VALUES (1, 'cat', 0.5, x'00ff')"),
        |_| Ok::<_, diesel::result::Error>(()),
    )
    .expect("a row the server sent");
    conn.batch_execute("DELETE FROM items WHERE id = 1")
        .expect("delete it");

    assert_eq!(conn.push().await.expect("push"), Some(1));
    assert_eq!(
        sorted_ops(&mut conn, 1),
        vec![r#"delete items [Integer(1), Text("cat"), Real(0.5), Blob([0, 255])]"#]
    );
}

/// Rows recorded before columns were added upload each new column at its
/// default, of every storage class, `NULL` for a column with none.
#[tokio::test]
async fn rows_recorded_before_new_columns_upload_their_defaults() {
    let mut conn = open(DDL);
    conn.batch_execute("INSERT INTO items (id) VALUES (1), (2)")
        .expect("write before the change");
    conn.change_schema(
        "ALTER TABLE items ADD COLUMN plain TEXT; \
         ALTER TABLE items ADD COLUMN thumb BLOB DEFAULT x'01'; \
         ALTER TABLE items ADD COLUMN gone DEFAULT NULL; \
         ALTER TABLE items ADD COLUMN tally INTEGER DEFAULT 7; \
         ALTER TABLE items ADD COLUMN ratio REAL DEFAULT 2.5",
    )
    .expect("add a column of each kind");

    assert_eq!(conn.push().await.expect("push"), Some(1));
    let added = "Null, Blob([1]), Null, Integer(7), Real(2.5)";
    assert_eq!(
        sorted_ops(&mut conn, 1),
        vec![
            format!("insert items [Integer(1), Null, Null, Null, {added}]"),
            format!("insert items [Integer(2), Null, Null, Null, {added}]"),
        ]
    );
}

/// An outbox row that does not decode stops the push, which queues nothing
/// rather than uploading values nobody wrote.
#[tokio::test]
async fn a_malformed_outbox_row_stops_the_push() {
    let mut conn = open(DDL);
    let planted: [(Option<&str>, Option<&str>); 3] = [
        (None, Some(r#"{"id":true}"#)),
        (None, Some(r#"{"id":{"q":1}}"#)),
        (None, None),
    ];
    for (old, new) in planted {
        diesel::sql_query(
            "INSERT INTO _connetto_outbox (tbl, op, old, new) VALUES ('items', 'I', ?, ?)",
        )
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(old)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(new)
        .execute(conn.conn())
        .expect("plant a malformed row");
        let refused = conn.push().await.expect_err("the row does not decode");
        assert!(
            refused.to_string().contains("outbox"),
            "names the outbox: {refused}"
        );
        assert!(conn.unsynced().is_empty(), "nothing was queued");
        conn.batch_execute("DELETE FROM _connetto_outbox")
            .expect("clear the planted row");
    }
}
