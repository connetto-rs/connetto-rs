//! R94 proof 3: the native builder against the in-process harness server.
//!
//! An anonymous build connects and reads the row the server synced, a
//! held-credential build reads only the row its own identity owns, and a
//! durable build reopens its encrypted replica from the same directory and
//! key store, with no server in the loop the second time.
//!
//! Needs Docker, since the fixture starts its own Postgres.

use std::sync::Arc;
use std::time::Duration;

use connetto_client::{
    Custody, Grant, HeldCredential, MemoryKeyStore, MemoryRefreshStore, NativeClientBuilder,
    NoGate, SyncSchema, TransportFactory,
};
use connetto_core::messages::{ControlMessage, HandshakeAck, Pong};
use connetto_core::traits::{IncomingFrame, MaybeSend, ReplicaKeyStore, Transport};
use connetto_core::{Cursor, ReplicaKey};
use connetto_server::{LoopbackTransport, PgSnapshotSource, RlsAuth, SessionConfig, loopback};
use connetto_test_harness::{
    Fixture, HarnessAuth, RosterAuth, Server, ServerConfig, engine_ddl, pool_for, spawn_server,
    with_user,
};
use diesel::prelude::*;
use tempfile::tempdir;

const PG_DDL: &str = "-- connetto:major 1
CREATE TABLE orders (id INT PRIMARY KEY, owner TEXT, body TEXT);";
const POLICIES: &str = "ALTER TABLE orders ENABLE ROW LEVEL SECURITY; \
     CREATE POLICY orders_p ON orders \
     USING (owner = current_setting('app.user_id', true)) \
     WITH CHECK (owner = current_setting('app.user_id', true));";

diesel::table! {
    /// Orders table, primary key id.
    orders (id) {
        /// Order identifier, the primary key.
        id -> Integer,
        /// The row's owner identity.
        owner -> Nullable<Text>,
        /// The row's body.
        body -> Nullable<Text>,
    }
}

#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = orders)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Order {
    id: i32,
    owner: Option<String>,
    body: Option<String>,
}

/// Create one server session when the native builder dials.
fn server_dialer(server: &Server) -> impl TransportFactory<Transport = LoopbackTransport> + use<> {
    let mut manager = Some(Arc::clone(server.manager()));
    move || {
        let transport = manager
            .take()
            .map(|manager| {
                let (server_transport, client) = loopback();
                tokio::spawn(async move {
                    let _ = manager.serve(server_transport).await;
                });
                client
            })
            .ok_or(super::support::DialExhausted);
        std::future::ready(transport)
    }
}

/// A transport that answers the handshake and the keepalives, then goes quiet
/// without closing, so the client idles on it the way it would on a live
/// socket with nothing to say.
fn quiet_transport() -> LoopbackTransport {
    let (mut server, client_end) = loopback();
    tokio::spawn(async move {
        let Ok(Some(IncomingFrame::Control(ControlMessage::Handshake(_)))) = server.recv().await
        else {
            return;
        };
        server
            .send_control(ControlMessage::HandshakeAck(HandshakeAck {
                connection_id: "quiet".to_owned(),
                session_token: "quiet".to_owned(),
                resume_token: "quiet".to_owned(),
                current_cursor: Cursor::from(Vec::new()),
                schema_version: None,
                sunset_at_secs: None,
                courier: false,
                initial_credits: 64,
                writer: Some(connetto_core::messages::WriterGrant {
                    number: 1,
                    frontier: 0,
                }),
            }))
            .await
            .expect("ack the handshake");
        loop {
            match server.recv().await {
                Ok(Some(IncomingFrame::Control(ControlMessage::Ping(ping)))) => {
                    server
                        .send_control(ControlMessage::Pong(Pong { nonce: ping.nonce }))
                        .await
                        .expect("pong");
                }
                Ok(Some(_)) => {}
                _ => return,
            }
        }
    });
    client_end
}

/// A key store shared between the two durable builds, the way the application
/// shares its platform key store between its runs.
#[derive(Clone)]
struct SharedKeyStore(Arc<MemoryKeyStore>);

impl SharedKeyStore {
    fn new() -> Self {
        Self(Arc::new(MemoryKeyStore::default()))
    }
}

impl ReplicaKeyStore for SharedKeyStore {
    type Error = connetto_client::ClientError;

    fn load(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<ReplicaKey>, Self::Error>> + MaybeSend {
        self.0.load(name)
    }

    fn store(
        &self,
        name: &str,
        key: &ReplicaKey,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        self.0.store(name, key)
    }

    fn clear(&self, name: &str) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        self.0.clear(name)
    }

    fn protection(&self) -> Custody {
        self.0.protection()
    }
}

/// Wait for a live query's first rows, bounded.
async fn until_first_rows<R: Clone + PartialEq>(live: &mut connetto_client::LiveQuery<R>) {
    if live.rows().is_empty() {
        tokio::time::timeout(Duration::from_secs(10), live.changed())
            .await
            .expect("the first rows arrive in time")
            .expect("the client is alive");
    }
}

/// An anonymous build connects and reads the row the server synced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_anonymous_connect_reads_a_synced_row() {
    let fixture = Fixture::acquire().await;
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS orders CASCADE",
            "DROP TABLE IF EXISTS _connetto_writers, _connetto_settled CASCADE",
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_writer') \
             THEN CREATE ROLE app_writer LOGIN PASSWORD 'app_writer'; END IF; END $$",
            PG_DDL,
            "GRANT USAGE ON SCHEMA public TO app_writer",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON orders TO app_writer",
        ])
        .await;
    connetto_test_harness::provision_writers(fixture.admin()).await;
    fixture
        .exec(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, \
             _connetto_session_writers TO app_writer",
        )
        .await;
    let writer_pool = pool_for(&with_user(fixture.admin_url(), "app_writer", "app_writer")).await;
    let snapshot =
        PgSnapshotSource::from_ddl(writer_pool.clone(), PG_DDL).expect("snapshot source");
    let server = spawn_server(
        ServerConfig::new(PG_DDL, fixture.admin_url()).with_replication(["orders"]),
        snapshot,
        HarnessAuth::roster(RosterAuth::granting_nobody()),
        writer_pool,
        fixture.admin().clone(),
    )
    .await;
    fixture
        .exec("INSERT INTO orders VALUES (1, 'anon', 'hello')")
        .await;

    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            connetto_schema::Documents::new(
                PG_DDL,
                "",
                connetto_test_harness::FIXTURE_ROLES,
                connetto_test_harness::FIXTURE_READER,
            ),
        )
        .expect("translate the sources"),
    );
    let client = NativeClientBuilder::new("ws://127.0.0.1:1", schema)
        .with_dialer(server_dialer(&server))
        .connect()
        .await
        .expect("an anonymous build connects");
    let mut live = client
        .client()
        .watch::<_, Order>(orders::table.order(orders::id))
        .await
        .expect("watch the synced table");
    until_first_rows(&mut live).await;
    assert_eq!(
        live.rows(),
        vec![Order {
            id: 1,
            owner: Some("anon".to_owned()),
            body: Some("hello".to_owned())
        }],
        "the row the server synced is what the anonymous build reads"
    );
}

/// A held-credential build reads the row its own identity owns, and none of
/// the rows another identity owns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_held_credential_reads_the_callers_own_row() {
    let fixture = Fixture::acquire().await;
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS orders CASCADE",
            "DROP TABLE IF EXISTS _connetto_writers, _connetto_settled CASCADE",
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_writer') \
             THEN CREATE ROLE app_writer LOGIN PASSWORD 'app_writer'; END IF; END $$",
            PG_DDL,
            POLICIES,
            "GRANT USAGE ON SCHEMA public TO app_writer",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON orders TO app_writer",
        ])
        .await;
    connetto_test_harness::provision_writers(fixture.admin()).await;
    fixture
        .exec(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, \
             _connetto_session_writers TO app_writer",
        )
        .await;
    let writer_pool = pool_for(&with_user(fixture.admin_url(), "app_writer", "app_writer")).await;
    // The one catalog the server parts read, the way the harness appends it
    // inside `spawn_server` (R77 decision 11).
    let engine_ddl = engine_ddl(PG_DDL);
    let snapshot =
        PgSnapshotSource::from_ddl(writer_pool.clone(), &engine_ddl).expect("snapshot source");
    let auth =
        HarnessAuth::rls(RlsAuth::from_ddl(writer_pool.clone(), &engine_ddl).expect("rls auth"));
    let server = spawn_server(
        ServerConfig::new(PG_DDL, fixture.admin_url())
            .with_replication(["orders"])
            .with_policies(POLICIES)
            .with_session(
                SessionConfig::new().with_schema_version(Some(
                    connetto_schema::translate::<String, connetto_core::bots::TextIds>(
                        connetto_schema::Documents::new(
                            PG_DDL,
                            POLICIES,
                            connetto_test_harness::FIXTURE_ROLES,
                            connetto_test_harness::FIXTURE_READER,
                        ),
                    )
                    .expect("translate")
                    .version(),
                )),
            ),
        snapshot,
        auth,
        writer_pool,
        fixture.admin().clone(),
    )
    .await;
    fixture
        .exec("INSERT INTO orders VALUES (1, 'alice', 'mine'), (2, 'bob', 'hidden')")
        .await;

    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            connetto_schema::Documents::new(
                PG_DDL,
                POLICIES,
                connetto_test_harness::FIXTURE_ROLES,
                connetto_test_harness::FIXTURE_READER,
            ),
        )
        .expect("translate the sources"),
    );
    let client = NativeClientBuilder::new("ws://127.0.0.1:1", schema)
        .with_dialer(server_dialer(&server))
        .signed_in(
            HeldCredential::new(Grant::new("user:alice"), "alice").expect("the credential holds"),
        )
        .connect()
        .await
        .expect("the signed-in build connects");
    let mut live = client
        .client()
        .watch::<_, Order>(orders::table.order(orders::id))
        .await
        .expect("watch the synced table");

    until_first_rows(&mut live).await;
    assert_eq!(
        live.rows(),
        vec![Order {
            id: 1,
            owner: Some("alice".to_owned()),
            body: Some("mine".to_owned())
        }],
        "the row the caller's identity owns is the only row it reads"
    );
}

async fn insert_courier_receipt(fixture: &Fixture, author: &[u8; 32]) {
    use connetto_core::SessionId;
    use connetto_server::audit::{AuthEvent, AuthOp, ConnettoAuditSchema};
    use connetto_server::defaults::{_connetto_writers, ConnettoAudit};

    let mut conn = fixture.admin().get().await.expect("admin connection");
    let insert = diesel::insert_into(_connetto_writers::table)
        .values((
            _connetto_writers::user_id.eq("alice"),
            _connetto_writers::device_key.eq(author.as_slice()),
            _connetto_writers::frontier.eq(0_i64),
        ))
        .returning(_connetto_writers::writer);
    let writer: i64 = diesel_async::RunQueryDsl::get_result(insert, &mut *conn)
        .await
        .expect("insert author writer");
    let mut receipt = AuthEvent::new(
        SessionId::from_uuid(uuid::Uuid::new_v4()),
        Some("alice".to_owned()),
        AuthOp::WriteCouriered,
    )
    .about_courier("bob".to_owned());
    receipt.writer = Some(writer);
    receipt.seq = Some(1);
    assert_eq!(
        diesel_async::RunQueryDsl::execute(ConnettoAudit::audit_insert(receipt), &mut *conn)
            .await
            .expect("commit courier receipt"),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builder_observes_only_committed_courier_receipts_visible_to_its_identity() {
    use connetto_client::CourierAuditError;

    const AUTHOR: [u8; 32] = [41; 32];
    const WITHIN: Duration = Duration::from_secs(10);

    tokio::time::timeout(Duration::from_secs(60), async {
        let fixture = Fixture::acquire().await;
        fixture
            .setup(&[
                "DROP TABLE IF EXISTS orders CASCADE",
                "DROP TABLE IF EXISTS _connetto_writers, _connetto_settled CASCADE",
                PG_DDL,
            ])
            .await;
        connetto_test_harness::provision_writers(fixture.admin()).await;
        connetto_test_harness::provision_audit(fixture.admin()).await;
        insert_courier_receipt(&fixture, &AUTHOR).await;
        let schema = SyncSchema::new(
            connetto_schema::translate::<String, connetto_core::bots::TextIds>(
                connetto_schema::Documents::new(
                    PG_DDL,
                    "",
                    connetto_test_harness::FIXTURE_ROLES,
                    connetto_test_harness::FIXTURE_READER,
                ),
            )
            .expect("translate the sources"),
        );
        let snapshot = PgSnapshotSource::from_ddl(fixture.admin().clone(), &engine_ddl(PG_DDL))
            .expect("snapshot source");
        let server = spawn_server(
            ServerConfig::new(PG_DDL, fixture.admin_url()).with_replication(["orders"]),
            snapshot,
            HarnessAuth::roster(RosterAuth::granting("alice").and("bob").and("mallory")),
            fixture.admin().clone(),
            fixture.admin().clone(),
        )
        .await;

        for account in ["alice", "bob", "mallory"] {
            let client = Box::pin(
                NativeClientBuilder::new("ws://127.0.0.1:1", schema.clone())
                    .with_dialer(server_dialer(&server))
                    .signed_in(
                        HeldCredential::new(Grant::new(format!("user:{account}")), account)
                            .expect("the credential holds"),
                    )
                    .connect(),
            )
            .await
            .expect("the signed-in observer connects");
            if account == "mallory" {
                assert!(matches!(
                    client
                        .observe_courier_audit(&AUTHOR, &[1], Duration::from_secs(1))
                        .await,
                    Err(CourierAuditError::Expired)
                ));
            } else {
                client
                    .observe_courier_audit(&AUTHOR, &[1], WITHIN)
                    .await
                    .unwrap_or_else(|error| panic!("{account} receipt 1 failed with {error}"));
                assert!(matches!(
                    client
                        .observe_courier_audit(&AUTHOR, &[1, 2], Duration::from_secs(1))
                        .await,
                    Err(CourierAuditError::Expired)
                ));
            }
            client.close().await;
        }
    })
    .await
    .expect("NATIVE-AUDIT-COVERAGE-522 completes within its bound");
}

const NOTES_DDL: &str = "-- connetto:major 1
CREATE TABLE notes (id INT PRIMARY KEY, label TEXT);";

diesel::table! {
    /// Notes table, primary key id.
    notes (id) {
        /// Note identifier, the primary key.
        id -> Integer,
        /// The note's label.
        label -> Nullable<Text>,
    }
}

#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = notes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct Note {
    id: i32,
    label: Option<String>,
}

/// A durable build writes a row, and a second build over the same directory
/// and key store reads it back with no server in the loop, reporting the
/// custody its key store carries.
#[tokio::test]
async fn builder_durable_replica_reopens_offline_and_reports_its_custody() {
    let dir = tempdir().expect("a temp dir");
    let store = SharedKeyStore::new();
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            connetto_schema::Documents::new(
                NOTES_DDL,
                "",
                connetto_test_harness::FIXTURE_ROLES,
                connetto_test_harness::FIXTURE_READER,
            ),
        )
        .expect("translate the sources"),
    );

    let client = Box::pin(
        NativeClientBuilder::new("ws://127.0.0.1:1", schema.clone())
            .with_dialer(super::support::Once::new(quiet_transport()))
            .signed_in(
                HeldCredential::new(Grant::new("user:bob"), "bob").expect("the credential holds"),
            )
            .durable(dir.path(), store.clone(), MemoryRefreshStore::default())
            .connect(),
    )
    .await
    .expect("a durable build connects");
    client
        .client()
        .with_conn(|conn| {
            diesel::insert_into(notes::table)
                .values((notes::id.eq(1), notes::label.eq("kept")))
                .execute(conn.conn())
                .expect("write the row")
        })
        .await
        .expect("the client is not locked");
    assert_eq!(
        client.custody(),
        Custody::Unverified(NoGate::Unsupported),
        "the key store's custody is the custody the build reports"
    );
    drop(client);

    // The second build dials a quiet socket and reopens the encrypted
    // replica from the same directory and the same key store. The row it
    // reads back is the one the first build wrote.
    let client = Box::pin(
        NativeClientBuilder::new("ws://127.0.0.1:1", schema)
            .with_dialer(super::support::Once::new(quiet_transport()))
            .signed_in(
                HeldCredential::new(Grant::new("user:bob"), "bob").expect("the credential holds"),
            )
            .durable(dir.path(), store, MemoryRefreshStore::default())
            .connect(),
    )
    .await
    .expect("the second build reopens the same replica");
    let rows: Vec<Note> = client
        .client()
        .with_conn(|conn| {
            notes::table
                .select(Note::as_select())
                .load(conn.conn())
                .expect("read the rows")
        })
        .await
        .expect("the client is not locked");
    assert_eq!(
        rows,
        vec![Note {
            id: 1,
            label: Some("kept".to_owned())
        }],
        "the row came back from the reopened replica with no server in the loop"
    );
}
