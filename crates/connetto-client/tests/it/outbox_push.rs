//! `push` builds the upload from the outbox, so a write is safe from the
//! moment its transaction commits: it survives a crash before the push, it
//! reaches an export, a schema change cannot strand it, and a push sends
//! whatever an earlier step queued without sending.

use connetto_client::{
    ClientBuilder, ClientError, ConnettoConnection, DataDir, ExportScope, MemoryRefreshStore,
};
use connetto_core::messages::ControlMessage;
use diesel::connection::SimpleConnection;
use tempfile::{TempDir, tempdir};

use super::support::{Script, handshake_ack, queued_ops};

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)";

/// A replica in `dir`, opened with no server under the suite's identity.
async fn open_in(dir: &TempDir) -> ConnettoConnection<Script> {
    let credential = super::support::held("tester");
    let store = super::support::key_store(&credential).await;
    ClientBuilder::new(
        super::support::bundle(DDL),
        super::support::NeverDial::<Script>::default(),
    )
    .signed_in(credential)
    .durable(
        DataDir::new(dir.path().to_path_buf()),
        store,
        MemoryRefreshStore::default(),
    )
    .open_driven()
    .await
    .expect("open with no server")
}

fn outbox_rows(conn: &mut ConnettoConnection<Script>) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let count: Count = diesel::RunQueryDsl::get_result(
        diesel::sql_query("SELECT count(*) AS n FROM _connetto_outbox"),
        conn.conn(),
    )
    .expect("count the outbox");
    count.n
}

/// A write committed and never pushed outlives the process, and the next
/// run's push uploads it.
#[tokio::test]
async fn a_write_committed_before_a_crash_is_pushed_by_the_next_run() {
    let dir = tempdir().expect("temp dir");
    let mut conn = open_in(&dir).await;
    conn.batch_execute("INSERT INTO items VALUES (1, 'before the crash')")
        .expect("write");
    drop(conn);

    let mut conn = open_in(&dir).await;
    let seq = conn
        .push()
        .await
        .expect("push")
        .expect("the write is queued");
    assert_eq!(
        queued_ops(conn.conn(), seq),
        vec![r#"insert items [Integer(1), Text("before the crash")]"#]
    );
}

/// Dropping or renaming a table that holds writes not yet pushed would lose
/// them, so the schema change is refused with nothing changed, and goes
/// through once a push has taken them.
#[tokio::test]
async fn a_schema_change_that_would_strand_unpushed_writes_is_refused() {
    let dir = tempdir().expect("temp dir");
    let mut conn = open_in(&dir).await;
    conn.batch_execute("INSERT INTO items VALUES (1, 'unpushed')")
        .expect("write");

    for ddl in ["DROP TABLE items", "ALTER TABLE items RENAME TO goods"] {
        match conn.change_schema(ddl) {
            Err(refusal @ ClientError::UnpushedWrites { .. }) => assert!(
                refusal.to_string().contains("items") && refusal.to_string().contains("push"),
                "names the table and the way out: {refusal}"
            ),
            Err(other) => panic!("{ddl}: expected an unpushed-writes refusal, got {other}"),
            Ok(()) => panic!("{ddl}: expected an unpushed-writes refusal, got success"),
        }
    }
    conn.batch_execute("INSERT INTO items VALUES (2, 'still here')")
        .expect("the table is still there and still recorded");
    assert_eq!(outbox_rows(&mut conn), 2, "both writes are still recorded");

    conn.push().await.expect("push").expect("queued");
    conn.change_schema("DROP TABLE items")
        .expect("with nothing unpushed the table may go");
}

/// An export taken before any push carries the write, queued under its
/// writer like any pushed one.
#[tokio::test]
async fn an_export_carries_a_write_not_yet_pushed() {
    let dir = tempdir().expect("temp dir");
    let mut conn = open_in(&dir).await;
    conn.batch_execute("INSERT INTO items VALUES (1, 'unpushed')")
        .expect("write");

    let archive = conn
        .export_local_data(ExportScope::Everything, Vec::new())
        .expect("export");
    assert_eq!(conn.unsynced().len(), 1, "the export queued the write");
    assert_eq!(outbox_rows(&mut conn), 0, "and took it out of the outbox");
    let plan = conn
        .import_local_data(std::io::Cursor::new(archive))
        .expect("the archive reads back");
    assert_eq!(plan.queued_writes(), 1, "the archive carries the write");
}

/// An export while connected queues the outbox without sending it, and the
/// next push sends that entry ahead of its own, so the server never sees a
/// gap in the writer's sequence.
#[tokio::test]
async fn a_push_after_a_connected_export_sends_the_exported_entry_first() {
    let dir = tempdir().expect("temp dir");
    let mut conn = open_in(&dir).await;
    let script = Script::default();
    script.say(handshake_ack());
    conn.attach(script.clone()).await.expect("attach");

    conn.batch_execute("INSERT INTO items VALUES (1, 'exported')")
        .expect("first write");
    conn.export_local_data(ExportScope::Everything, Vec::new())
        .expect("export");
    conn.batch_execute("INSERT INTO items VALUES (2, 'pushed')")
        .expect("second write");
    let pushed = conn.push().await.expect("push").expect("queued");

    let sent = script.sent(|message| match message {
        ControlMessage::MutationHeader(header) => Some(header.client_seq),
        _ => None,
    });
    assert_eq!(
        sent,
        vec![pushed - 1, pushed],
        "both entries went up in order"
    );
}
