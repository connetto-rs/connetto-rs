//! A table whose live shape no longer matches the catalog, through a running
//! server.
//!
//! The drift guard refuses such a table, so its subscriptions end with
//! `SUBSCRIPTION_REFUSED_SCHEMA`, a new subscription to it is refused the same
//! way, and a write to it is rejected as a schema mismatch. The refusal holds
//! until the process ends, whichever connect of the stream finds the drift.
//!
//! Needs Docker, because the fixture starts its own Postgres.

use std::time::Duration;

use connetto_core::messages::{
    BulkMessage, ControlMessage, MutationRejectReason, NonFatalError, SUBSCRIPTION_REFUSED_SCHEMA,
};
use connetto_core::traits::IncomingFrame;
use connetto_server::{PgSnapshotSource, RlsAuth, RuntimeWritableCatalog};
use connetto_test_harness::{
    Client, Fixture, HarnessAuth, PUBLICATION, Server, ServerConfig, drop_slot, engine_ddl,
    insert_changeset, spawn_server,
};
use sqlite_diff_rs::Value;

const PG_DDL: &str = "-- connetto:major 1\n\
                      CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, extra TEXT);\
                      CREATE TABLE keep (id INT PRIMARY KEY, body TEXT);";
/// How long a frame that must arrive is given.
const DELIVERY: Duration = Duration::from_secs(30);
/// Long enough that only the stream's `Relation` message can find a drift.
const NEVER: Duration = Duration::from_secs(3600);

/// The two tables, one row each.
async fn tables(fixture: &Fixture) {
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS notes CASCADE",
            "DROP TABLE IF EXISTS keep CASCADE",
            PG_DDL,
            "INSERT INTO notes (id, body, extra) VALUES (1, 'a', 'x')",
            "INSERT INTO keep (id, body) VALUES (1, 'a')",
        ])
        .await;
}

/// A server over the catalog `PG_DDL` declares, re-reading every live shape
/// `every`.
async fn serve(fixture: &Fixture, every: Duration) -> Server {
    let admin = fixture.admin().clone();
    let engine_ddl = engine_ddl(PG_DDL);
    let snapshot = PgSnapshotSource::from_ddl(admin.clone(), &engine_ddl)
        .expect("snapshot source")
        .with_publication(PUBLICATION);
    let auth = HarnessAuth::rls(RlsAuth::from_ddl(admin.clone(), &engine_ddl).expect("rls auth"));
    let config = ServerConfig::new(PG_DDL, fixture.admin_url())
        .with_writable(
            RuntimeWritableCatalog::builder()
                .writable("notes")
                .writable("keep")
                .build(),
        )
        .with_replication(["notes", "keep"])
        .with_drift_reconciliation(every);
    spawn_server(config, snapshot, auth, admin.clone(), admin).await
}

async fn connect(server: &Server) -> Client {
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;
    client
}

/// The next refusal the client receives, past any snapshot or live frame.
async fn next_refusal(client: &mut Client) -> NonFatalError {
    tokio::time::timeout(DELIVERY, async {
        loop {
            match client.recv().await {
                Some(IncomingFrame::Control(ControlMessage::NonFatalError(refusal))) => {
                    return refusal;
                }
                Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(_))) => {
                    client.ack_credits(1).await;
                }
                Some(
                    IncomingFrame::Control(
                        ControlMessage::SnapshotBegin(_)
                        | ControlMessage::SnapshotEnd(_)
                        | ControlMessage::RevocationUpdate(_),
                    )
                    | IncomingFrame::Bulk(BulkMessage::LivePatch(_)),
                ) => {}
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
    })
    .await
    .expect("the refusal arrives")
}

/// Assert `sub_id` is refused because its table no longer matches the catalog.
async fn expect_schema_refusal(client: &mut Client, sub_id: &str) {
    let refusal = next_refusal(client).await;
    assert_eq!(refusal.related_to.as_deref(), Some(sub_id));
    assert_eq!(refusal.detail, SUBSCRIPTION_REFUSED_SCHEMA);
}

/// Insert one `notes` row as client sequence `seq`, answering its rejection.
async fn write_note(client: &mut Client, seq: u64, id: i64) -> MutationRejectReason {
    client
        .upload(
            seq,
            insert_changeset(
                "notes",
                &["id", "body", "extra"],
                &[0],
                vec![
                    Value::Integer(id),
                    Value::Text("b".to_owned()),
                    Value::Text("y".to_owned()),
                ],
            ),
        )
        .await;
    let ControlMessage::MutationReject(reject) = client.next_control().await else {
        panic!("the write to a refused table is rejected");
    };
    assert_eq!(reject.client_seq, seq);
    reject.reason
}

#[tokio::test]
async fn a_dropped_column_refuses_the_table_live_for_reads_and_writes() {
    let fixture = Fixture::acquire().await;
    tables(&fixture).await;
    let server = serve(&fixture, NEVER).await;
    let mut client = connect(&server).await;
    client.subscribe("n", "SELECT * FROM notes").await;
    client.expect_snapshot("n").await;

    // The `Relation` message ahead of the insert carries the new shape.
    fixture.exec("ALTER TABLE notes DROP COLUMN extra").await;
    fixture
        .exec("INSERT INTO notes (id, body) VALUES (2, 'b')")
        .await;
    expect_schema_refusal(&mut client, "n").await;

    client.subscribe("again", "SELECT * FROM notes").await;
    expect_schema_refusal(&mut client, "again").await;
    assert_eq!(
        write_note(&mut client, 1, 3).await,
        MutationRejectReason::SchemaMismatch
    );

    client.subscribe("k", "SELECT * FROM keep").await;
    assert!(
        !client.expect_snapshot("k").await.is_empty(),
        "a table that still matches is served"
    );

    drop(client);
    drop(server);
    drop_slot(fixture.admin()).await;
}

#[tokio::test]
async fn a_column_dropped_before_boot_refuses_the_table_at_the_first_connect() {
    let fixture = Fixture::acquire().await;
    tables(&fixture).await;
    fixture.exec("ALTER TABLE notes DROP COLUMN extra").await;
    let server = serve(&fixture, NEVER).await;
    let mut client = connect(&server).await;

    // A subscribe ahead of the first connect is served and then ended by it.
    client.subscribe("n", "SELECT * FROM notes").await;
    expect_schema_refusal(&mut client, "n").await;
    assert_eq!(
        write_note(&mut client, 1, 3).await,
        MutationRejectReason::SchemaMismatch
    );

    drop(client);
    drop(server);
    drop_slot(fixture.admin()).await;
}

#[tokio::test]
async fn reconciliation_finds_a_dropped_column_no_change_reports() {
    let fixture = Fixture::acquire().await;
    tables(&fixture).await;
    let server = serve(&fixture, Duration::from_secs(1)).await;
    let mut client = connect(&server).await;
    client.subscribe("n", "SELECT * FROM notes").await;
    client.expect_snapshot("n").await;

    // No row of `notes` changes, so no `Relation` message ever carries this.
    fixture.exec("ALTER TABLE notes DROP COLUMN extra").await;
    expect_schema_refusal(&mut client, "n").await;

    drop(client);
    drop(server);
    drop_slot(fixture.admin()).await;
}

#[tokio::test]
async fn a_refusal_outlives_a_reconnect_and_ends_with_the_process() {
    let fixture = Fixture::acquire().await;
    tables(&fixture).await;
    let server = serve(&fixture, NEVER).await;
    let mut client = connect(&server).await;
    client.subscribe("k", "SELECT * FROM keep").await;
    client.expect_snapshot("k").await;
    client.subscribe("n", "SELECT * FROM notes").await;
    client.expect_snapshot("n").await;
    fixture.exec("ALTER TABLE notes DROP COLUMN extra").await;
    fixture
        .exec("INSERT INTO notes (id, body) VALUES (2, 'b')")
        .await;
    expect_schema_refusal(&mut client, "n").await;

    // The shape matches again. The terminate returns once the walsender has
    // exited, so the next live patch can only come over a new connect, whose
    // guard accepts the table.
    fixture.exec("ALTER TABLE notes ADD COLUMN extra TEXT").await;
    fixture
        .exec(
            "SELECT pg_terminate_backend(active_pid, 30000) FROM pg_replication_slots \
             WHERE slot_name = 'connetto_slot' AND active_pid IS NOT NULL",
        )
        .await;
    fixture
        .exec("INSERT INTO keep (id, body) VALUES (2, 'b')")
        .await;
    client.wait_for_live(DELIVERY).await;
    assert!(server.ingest_restarts() > 0, "the stream reconnected");

    client.subscribe("again", "SELECT * FROM notes").await;
    expect_schema_refusal(&mut client, "again").await;
    assert_eq!(
        write_note(&mut client, 1, 3).await,
        MutationRejectReason::SchemaMismatch
    );
    drop(client);
    drop(server);

    let server = serve(&fixture, NEVER).await;
    let mut client = connect(&server).await;
    client.subscribe("n", "SELECT * FROM notes").await;
    assert!(
        !client.expect_snapshot("n").await.is_empty(),
        "a restarted process judges the table afresh and serves it"
    );

    drop(client);
    drop(server);
    drop_slot(fixture.admin()).await;
}
