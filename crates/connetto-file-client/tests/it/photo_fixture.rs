//! The photo and file-server fixture the Docker-gated proofs share.

use core::future::Future;
use core::time::Duration;
use std::marker::PhantomData;

use connetto_client::live::ConnettoClient;
use connetto_client::reconnect::ReconnectPolicy;
use connetto_client::{ClientBuilder, Gate, Grant, HeldCredential, MemoryRefreshStore, SyncSchema};
use connetto_core::schema::SchemaBundle;
use connetto_core::transport::LoopbackTransport;
use connetto_file_client::{ContentClient, FsStore, ReqwestHttp};
use connetto_file_core::{FileId, MimeClass};
use connetto_file_server::{
    AppPools, Config, DefaultFileSchema, FsStore as ServerStore, TicketSigner, TicketVerifier,
};
use connetto_server::{PgSnapshotSource, RlsAuth, RuntimeWritableCatalog};
use connetto_test_harness::{
    Fixture, HarnessAuth, Server, ServerConfig, pool_for, spawn_server, with_user,
};
use diesel::prelude::*;

/// The application's own table, as Postgres holds it.
pub const PG_DDL: &str = "-- connetto:major 1\nCREATE TABLE photos (id INT PRIMARY KEY, owner TEXT, \
                      content_id BYTEA, content_state TEXT, edited_at TEXT);";

/// The same table as the replica holds it.
pub const SQLITE_DDL: &str = "CREATE TABLE photos (id INTEGER PRIMARY KEY, owner TEXT, \
                          content_id BLOB, content_state TEXT, edited_at TEXT)";

/// The photograph this case carries the whole way.
pub const PHOTO: &[u8] = b"JPEG-ish bytes standing in for a photograph taken in the field, \
                       long enough that a ranged request over it means something";

diesel::table! {
    /// The application's photo entries, one shape for the replica and the server alike.
    photos (id) {
        /// Entry identifier.
        id -> diesel::sql_types::Integer,
        /// Identity that owns the entry.
        owner -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
        /// BLAKE3 identity of the photo's bytes.
        content_id -> diesel::sql_types::Nullable<diesel::sql_types::Binary>,
        /// Availability of the bytes, as the server last said.
        content_state -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
        /// Optimistic concurrency column.
        edited_at -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
    }
}

/// The deployment statements the proofs need beside the file server's own DDL.
pub const DEPLOYMENT: &[&str] = &[
    "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_writer') \
     THEN CREATE ROLE app_writer LOGIN PASSWORD 'app_writer'; END IF; END $$",
    "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'file_reader') \
     THEN CREATE ROLE file_reader LOGIN PASSWORD 'file_reader' NOINHERIT; END IF; END $$",
    "CREATE TABLE photos (id INT PRIMARY KEY, owner TEXT, content_id BYTEA, \
     content_state TEXT, edited_at TEXT)",
    "ALTER TABLE photos REPLICA IDENTITY FULL",
    "GRANT USAGE ON SCHEMA public TO app_writer",
    "GRANT USAGE ON SCHEMA public TO file_reader",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON photos TO app_writer",
    "GRANT SELECT, UPDATE ON photos TO file_reader",
    "GRANT SELECT ON _cfs_manifests TO file_reader",
    "GRANT SELECT ON _cfs_manifest_chunks TO file_reader",
    "CREATE OR REPLACE FUNCTION connetto_visible_files(p_file_ids BYTEA[]) \
     RETURNS BYTEA[] LANGUAGE sql SECURITY INVOKER SET search_path TO '' AS $$ \
       SELECT ARRAY(SELECT f FROM UNNEST(p_file_ids) AS f \
         WHERE EXISTS (SELECT 1 FROM public.photos p WHERE p.content_id = f)) \
     $$",
    "GRANT EXECUTE ON FUNCTION connetto_visible_files TO file_reader",
    "GRANT EXECUTE ON FUNCTION connetto_visible_files TO app_writer",
    "CREATE OR REPLACE FUNCTION connetto_set_content_state(p_file_id BYTEA, \
     p_new_state TEXT, p_caller TEXT) RETURNS BYTEA LANGUAGE plpgsql \
     SECURITY DEFINER SET search_path TO '' AS $$ BEGIN \
       UPDATE public.photos SET content_state = p_new_state WHERE content_id = p_file_id; \
       RETURN p_file_id; END; $$",
    "GRANT EXECUTE ON FUNCTION connetto_set_content_state TO file_reader",
];

/// One device of alice's at `path`, presenting `grant` and dialing through
/// `dial`, redialing under the default policy while the dial fails.
pub async fn device<F, Fut>(
    path: &std::path::Path,
    grant: &str,
    dial: F,
) -> ConnettoClient<LoopbackTransport>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<LoopbackTransport, &'static str>> + Send + 'static,
{
    let credential =
        HeldCredential::new(Grant::new(grant), "alice").expect("a string identity serializes");
    let (running, pump) = ClientBuilder::new(
        SyncSchema::new(
            SchemaBundle::new(
                PG_DDL,
                "",
                SQLITE_DDL,
                Vec::<(String, String)>::new(),
                Vec::<String>::new(),
                None::<&str>,
            )
            .expect("the source declares major 1"),
        ),
        dial,
    )
    .with_reconnect(ReconnectPolicy::default())
    .signed_in(credential)
    .durable(
        super::support::At(path.to_path_buf()),
        super::support::FixedKey,
        MemoryRefreshStore::default(),
    )
    .with_gate(Gate::off())
    .connect_with_pump()
    .await
    .expect("open the device");
    tokio::spawn(pump);
    running.client().clone()
}

/// Installs the file server's own schema, the application's table, the deployment
/// contracts, and the roles each side connects as.
pub async fn provision(fixture: &Fixture) {
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS photos CASCADE",
            "DROP TABLE IF EXISTS _connetto_writers, _connetto_settled CASCADE",
        ])
        .await;
    for statement in split_statements(connetto_file_server::DEPLOYMENT_DDL) {
        fixture.exec(&statement).await;
    }
    for statement in DEPLOYMENT {
        fixture.exec(statement).await;
    }
    connetto_test_harness::provision_writers(fixture.admin()).await;
    fixture
        .exec(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled \
             TO app_writer",
        )
        .await;
}

/// Starts the file server on its own socket and returns the base URL, the sync
/// server's signer, and the task serving it.
pub async fn start_file_server(
    fixture: &Fixture,
    chunk_root: &std::path::Path,
) -> (String, TicketSigner, tokio::task::JoinHandle<()>) {
    let reader_pool = pool_for(&with_user(
        fixture.admin_url(),
        "file_reader",
        "file_reader",
    ))
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the file server");
    let base_url = format!("http://{}", listener.local_addr().expect("address"));
    let (signer, public_key) = TicketSigner::generate(&base_url, Duration::from_secs(600), 1 << 20)
        .expect("generate a ticket key");
    let router = connetto_file_server::serve(Config::<DefaultFileSchema> {
        pools: AppPools {
            admin: fixture.admin().clone(),
            reader: reader_pool,
        },
        store: connetto_file_server::AnyStore::Fs(
            ServerStore::new(chunk_root.join("server")).expect("server chunk store"),
        ),
        verifier: TicketVerifier::new(public_key),
        grace: Duration::from_secs(600),
        caller_settings: connetto_file_server::CallerSettings::default(),
        quotas: connetto_file_server::QuotaSettings::default(),
        ceilings: connetto_file_server::CeilingCache::default(),
        _schema: PhantomData,
    })
    .await
    .expect("the deployment contracts are in place");
    let serving = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (base_url, signer, serving)
}

/// A harness server with the file server's signer wired into its session loop.
pub async fn spawn_sync_server(fixture: &Fixture, signer: TicketSigner) -> Server {
    let writer_pool = pool_for(&with_user(fixture.admin_url(), "app_writer", "app_writer")).await;
    let snapshot =
        PgSnapshotSource::from_ddl(writer_pool.clone(), PG_DDL).expect("snapshot source");
    let auth = HarnessAuth::rls(RlsAuth::from_ddl(writer_pool.clone(), PG_DDL).expect("rls auth"));
    spawn_server(
        ServerConfig::new(PG_DDL, fixture.admin_url())
            .with_writable(
                RuntimeWritableCatalog::builder()
                    .versioned("photos", "edited_at")
                    .build(),
            )
            .with_replication(["photos"])
            .with_content_signer(signer),
        snapshot,
        auth,
        writer_pool,
        fixture.admin().clone(),
    )
    .await
}

/// Walks the outbox until it sends, or gives up loudly.
pub async fn wait_for_upload(
    content: &ContentClient<LoopbackTransport, FsStore, ReqwestHttp>,
) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let sent = content.flush_outbox().await.expect("walk the outbox");
        if sent > 0 {
            return sent;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the outbox never drained"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Reads `content_state` out of Postgres until it equals `wanted`, one
/// connection for the whole wait so the server's pool does not starve.
pub async fn wait_for_pg_state(fixture: &Fixture, file_id: &[u8; 32], wanted: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut conn = fixture.admin().get().await.expect("admin connection");
    loop {
        let rows: Vec<Option<String>> = diesel_async::RunQueryDsl::load(
            photos::table
                .filter(photos::content_id.eq(file_id.to_vec()))
                .select(photos::content_state),
            &mut *conn,
        )
        .await
        .expect("read the state");
        if rows.into_iter().flatten().any(|state| state == wanted) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "content_state never became {wanted}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Attaches a content client to the device's dedicated chunk directory.
pub async fn attach_content(
    a: ConnettoClient<LoopbackTransport>,
    chunk_dir: &std::path::Path,
) -> ContentClient<LoopbackTransport, FsStore, ReqwestHttp> {
    ContentClient::attach(
        a,
        FsStore::new(chunk_dir.join("device-a")),
        [3; 32],
        ReqwestHttp::new(),
    )
    .await
    .expect("attach content handling to the device")
}

/// Stages the test photograph as photo row 1 and returns the file identity.
pub async fn stage_photo(
    a_content: &ContentClient<LoopbackTransport, FsStore, ReqwestHttp>,
) -> FileId {
    let (file_id, ()) = a_content
        .stage(PHOTO, MimeClass::Jpeg, |conn, file_id| {
            diesel::insert_into(photos::table)
                .values((
                    photos::id.eq(1),
                    photos::owner.eq("alice"),
                    photos::content_id.eq(file_id.as_bytes().to_vec()),
                    photos::edited_at.eq("t0"),
                ))
                .execute(conn)
                .map(|_| ())
        })
        .await
        .expect("stage the photo");
    file_id
}

/// Splits DDL with no dollar-quoted bodies on semicolons.
pub fn split_statements(ddl: &str) -> Vec<String> {
    ddl.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(str::to_owned)
        .collect()
}
