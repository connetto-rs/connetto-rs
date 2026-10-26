//! The outbox records every application write to a synced table inside the
//! transaction that makes it, and nothing connetto writes itself. Debug
//! builds check at each `push` that the outbox amounts to the same upload as
//! the capture session, so every push here also runs that check.

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
/// digits, a blob travels as hex, and text with a quote stays text.
#[tokio::test]
async fn each_write_is_recorded_with_its_values() {
    let mut conn = open(DDL);
    conn.batch_execute(
        "INSERT INTO items VALUES (1, 'it''s \"here\"', 0.1, x'00ff'); \
         UPDATE items SET label = NULL, weight = 2 WHERE id = 1; \
         DELETE FROM items WHERE id = 1",
    )
    .expect("write a row three times");

    let inserted = r#"{"id":1,"label":"it's \"here\"","weight":{"r":"0.10000000000000001"},"data":{"b":"00FF"}}"#;
    let updated = r#"{"id":1,"label":null,"weight":{"r":"2.0"},"data":{"b":"00FF"}}"#;
    assert_eq!(
        recorded(&mut conn),
        vec![
            row("items", "I", None, Some(inserted)),
            row("items", "U", Some(inserted), Some(updated)),
            row("items", "D", Some(updated), None),
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

/// A key change, a row inserted and deleted again, and two updates of one
/// row all reach the upload the way the capture session merges them.
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

    conn.batch_execute(
        "UPDATE items SET id = 6 WHERE id = 5; \
         DELETE FROM items WHERE id = 2; INSERT INTO items (id, label) VALUES (2, 'again'); \
         UPDATE items SET weight = 1.5 WHERE id = 6; UPDATE items SET weight = NULL WHERE id = 6",
    )
    .expect("second round over pushed rows");
    assert_eq!(conn.push().await.expect("push"), Some(2));
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
/// server frame does, leaves the outbox holding the written values and the
/// capture session the rewritten ones, and the push still goes out.
#[tokio::test]
async fn a_row_rewritten_by_connetto_before_the_push_still_pushes() {
    let mut conn = open(DDL);
    conn.batch_execute("INSERT INTO items (id, label) VALUES (1, 'local'), (2, 'kept')")
        .expect("local writes");
    conn.transact_with_bookkeeping(
        |db| db.batch_execute("UPDATE items SET label = 'server' WHERE id = 1"),
        |_| Ok::<_, diesel::result::Error>(()),
    )
    .expect("an exempt rewrite, as a frame applies one");

    assert_eq!(conn.push().await.expect("push"), Some(1));
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
