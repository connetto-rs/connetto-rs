//! The handshake's schema-version gate, driven end to end by the bundle step.
//!
//! The server derives the version it declares from the translated bundle, the
//! way the production binary does. A client that presents the same bundle
//! connects, and a client that presents a bundle over the same sources but a
//! different replica DDL is refused `ClientError::SchemaOutdated`, because the
//! two sides hashed different things.
//!
//! Needs Docker, since the fixture starts its own Postgres.

use connetto_client::{ClientBuilder, ClientError, SyncSchema};
use connetto_core::{SchemaBundle, SchemaVersion};
use connetto_server::{LoopbackTransport, PgSnapshotSource, RlsAuth, SessionConfig};
use connetto_test_harness::{
    Fixture, HarnessAuth, ServerConfig, engine_ddl, pool_for, spawn_server, with_user,
};

const PG_DDL: &str = "-- connetto:major 1
CREATE TABLE orders (id INT PRIMARY KEY, owner TEXT, body TEXT);";
const POLICIES: &str = "ALTER TABLE orders ENABLE ROW LEVEL SECURITY; \
     CREATE POLICY orders_p ON orders \
     USING (owner = current_setting('app.user_id', true)) \
     WITH CHECK (owner = current_setting('app.user_id', true));";

/// The bundle version a side declares, derived the way the production binary
/// derives it, from the same sources both sides hold.
fn derived_version() -> SchemaVersion {
    connetto_schema::translate::<String, connetto_core::bots::TextIds>(
        connetto_schema::Documents::new(
            PG_DDL,
            POLICIES,
            connetto_test_harness::FIXTURE_ROLES,
            connetto_test_harness::FIXTURE_READER,
        ),
    )
    .expect("translate the sources")
    .version()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_handshake_gate_hashes_the_same_bundle_on_both_sides() {
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
            .with_session(SessionConfig::new().with_schema_version(Some(derived_version()))),
        snapshot,
        auth,
        writer_pool,
        fixture.admin().clone(),
    )
    .await;

    let mut conn = ClientBuilder::new(
        SyncSchema::new(
            connetto_schema::translate::<String, connetto_core::bots::TextIds>(
                connetto_schema::Documents::new(
                    PG_DDL,
                    POLICIES,
                    connetto_test_harness::FIXTURE_ROLES,
                    connetto_test_harness::FIXTURE_READER,
                ),
            )
            .expect("translate the sources"),
        ),
        super::support::NeverDial::<LoopbackTransport>::default(),
    )
    .signed_in(super::support::held("alice"))
    .open_driven()
    .expect("open matching replica");
    let connected = conn.attach(server.attach()).await;
    assert!(
        connected.is_ok(),
        "the matching bundle connects: {:?}",
        connected.err()
    );
    drop(connected);
    drop(conn);

    // A client over the same sources but a different replica DDL hashed a
    // different bundle, and is told to reload rather than mis-parse.
    let stale_bundle = SchemaBundle::new(
        PG_DDL,
        POLICIES,
        "CREATE TABLE orders_rls (id INTEGER PRIMARY KEY, owner TEXT, body TEXT);",
        Vec::<(String, String)>::new(),
        Vec::<String>::new(),
        None::<&str>,
    )
    .expect("the source declares major 1");
    let stale = stale_bundle.version();
    assert_ne!(
        stale,
        derived_version(),
        "a different replica DDL changes the hash"
    );
    let mut conn = ClientBuilder::new(
        SyncSchema::new(stale_bundle),
        super::support::NeverDial::<LoopbackTransport>::default(),
    )
    .signed_in(super::support::held("alice"))
    .open_driven()
    .expect("open stale replica");
    match conn.attach(server.attach()).await {
        Err(ClientError::SchemaOutdated { client, server }) => {
            assert_eq!(
                client,
                Some(stale),
                "the client reports the version it baked"
            );
            assert_eq!(
                server,
                derived_version(),
                "the server reports the version it derived"
            );
        }
        Err(other) => panic!("expected SchemaOutdated, got {other:?}"),
        Ok(()) => panic!("a stale client connected to a versioned server"),
    }
}
