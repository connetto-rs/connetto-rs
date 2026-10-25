//! A subscription naming a strict subset of a table's columns.
//!
//! The replica holds the application's whole table, so the server widens such a
//! subscription to whole rows, and every row it brings matches the server's,
//! at the snapshot and after a change to a column the list leaves out.
//!
//! Needs Docker, since the fixture starts its own Postgres.

use std::time::Duration;

use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection, SyncSchema};
use connetto_core::traits::Transport;
use connetto_server::{PgSnapshotSource, RlsAuth, SessionConfig};
use connetto_test_harness::{
    Fixture, HarnessAuth, Server, ServerConfig, pool_for, spawn_server, with_user,
};
use diesel::prelude::*;

/// `body` is nullable in `notes` and required in `pages`, the two ways a
/// column a subscription leaves out can be declared.
const PG_DDL: &str = "-- connetto:major 1
CREATE TABLE notes (id INT PRIMARY KEY, owner TEXT NOT NULL, title TEXT NOT NULL, body TEXT);
CREATE TABLE pages (id INT PRIMARY KEY, owner TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL);";

const POLICIES: &str = "ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes USING (owner = current_setting('app.user_id', true));
ALTER TABLE pages ENABLE ROW LEVEL SECURITY;
CREATE POLICY pages_p ON pages USING (owner = current_setting('app.user_id', true));";

/// How long a frame that should arrive is given.
const DELIVERY: Duration = Duration::from_secs(30);

diesel::table! {
    /// The replica's copy of the server's notes.
    notes (id) {
        /// Row key.
        id -> Integer,
        /// Whose note it is.
        owner -> Text,
        /// The column the subscription names.
        title -> Text,
        /// The column the subscription leaves out.
        body -> Nullable<Text>,
    }
}

diesel::table! {
    /// The replica's copy of the server's pages.
    pages (id) {
        /// Row key.
        id -> Integer,
        /// Whose page it is.
        owner -> Text,
        /// The column the subscription names.
        title -> Text,
        /// The required column the subscription leaves out.
        body -> Text,
    }
}
fn schema() -> connetto_core::schema::SchemaBundle {
    connetto_schema::translate::<String, connetto_core::bots::TextIds>(PG_DDL, POLICIES)
        .expect("translate the sources")
}

async fn server(fixture: &Fixture) -> Server {
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS notes, pages CASCADE",
            "DROP TABLE IF EXISTS _connetto_writers, _connetto_settled CASCADE",
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_writer') \
             THEN CREATE ROLE app_writer LOGIN PASSWORD 'app_writer'; END IF; END $$",
            PG_DDL,
            POLICIES,
            "GRANT USAGE ON SCHEMA public TO app_writer",
            "GRANT SELECT ON notes, pages TO app_writer",
        ])
        .await;
    connetto_test_harness::provision_writers(fixture.admin()).await;
    fixture
        .exec(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled \
             TO app_writer",
        )
        .await;
    fixture
        .exec(
            "INSERT INTO notes (id, owner, title, body) VALUES (1, 'alice', 'kept', 'server body'); \
             INSERT INTO pages (id, owner, title, body) VALUES (1, 'alice', 'kept', 'server body')",
        )
        .await;
    let reader = pool_for(&with_user(fixture.admin_url(), "app_writer", "app_writer")).await;
    let snapshot = PgSnapshotSource::from_ddl(reader.clone(), PG_DDL).expect("snapshot source");
    let auth = HarnessAuth::rls(RlsAuth::from_ddl(reader.clone(), PG_DDL).expect("rls auth"));
    spawn_server(
        ServerConfig::new(PG_DDL, fixture.admin_url())
            .with_replication(["notes", "pages"])
            .with_session(SessionConfig::new().with_schema_version(Some(schema().version()))),
        snapshot,
        auth,
        reader,
        fixture.admin().clone(),
    )
    .await
}

/// Pump until the snapshot of the subscription named `sub_id` ends, failing on
/// a refusal, on an apply error, or after [`DELIVERY`].
async fn snapshot_lands<T>(conn: &mut ConnettoConnection<T>, sub_id: &str)
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    let deadline = tokio::time::Instant::now() + DELIVERY;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let event = tokio::time::timeout(remaining, conn.pump_one())
            .await
            .expect("the snapshot did not arrive in time")
            .expect("the snapshot applies");
        match event {
            ClientEvent::NonFatal {
                related_to: Some(id),
                detail,
            } if id == sub_id => panic!("the column subset was refused: {detail}"),
            ClientEvent::SnapshotEnd { .. } => return,
            _ => {}
        }
    }
}

async fn alice(server: &Server) -> ConnettoConnection<impl Transport<Error: core::fmt::Display>> {
    ClientBuilder::new(
        SyncSchema::new(schema()),
        super::support::Once::new(server.attach()),
    )
    .signed_in(super::support::held("alice"))
    .connect_driven()
    .await
    .expect("connect")
}

/// A left-out nullable column holds the server's value, at the snapshot and
/// after a change to that column alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_left_out_column_matches_the_server() {
    let fixture = Fixture::acquire().await;
    let server = server(&fixture).await;
    let mut alice = alice(&server).await;

    alice
        .subscribe("titles", "SELECT id, owner, title FROM notes")
        .await
        .expect("send the subscription");
    snapshot_lands(&mut alice, "titles").await;
    let body: Option<String> = notes::table
        .find(1)
        .select(notes::body)
        .first(alice.conn())
        .expect("the snapshot row is in the replica");
    assert_eq!(
        body.as_deref(),
        Some("server body"),
        "the replica holds a body the server row does not"
    );

    fixture
        .exec("UPDATE notes SET body = 'changed' WHERE id = 1")
        .await;
    let deadline = tokio::time::Instant::now() + DELIVERY;
    loop {
        let body: Option<String> = notes::table
            .find(1)
            .select(notes::body)
            .first(alice.conn())
            .expect("the row stays in the replica");
        if body.as_deref() == Some("changed") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a change to the left-out column never reached the replica, which holds {body:?}"
        );
        let _ = tokio::time::timeout(Duration::from_millis(500), alice.pump_one()).await;
    }
}

/// A left-out required column lands with its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_left_out_required_column_lands_with_its_row() {
    let fixture = Fixture::acquire().await;
    let server = server(&fixture).await;
    let mut alice = alice(&server).await;

    alice
        .subscribe("pages", "SELECT id, owner, title FROM pages")
        .await
        .expect("send the subscription");
    snapshot_lands(&mut alice, "pages").await;
    let body: Option<String> = pages::table
        .find(1)
        .select(pages::body)
        .first(alice.conn())
        .optional()
        .expect("read the replica");
    assert_eq!(
        body.as_deref(),
        Some("server body"),
        "the replica's row differs from the server's"
    );
}
