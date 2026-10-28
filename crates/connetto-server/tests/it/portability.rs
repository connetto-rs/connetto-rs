//! The person's data download (R61 slice 2), each test over its own fixture
//! through the one construction path.

use std::io;
use std::pin::Pin;

use connetto_core::messages::BindValue;
use connetto_server::builder::{
    BuildError, Database, OidcProvider, OpenFga, ServerBuilder, ServerSchema, TokenKeys,
};
use connetto_server::portability::{
    DataRequestError, DataRequester, PortabilityBind, PortabilityError, PortabilityList,
    PortabilityQuery,
};
use connetto_server::{
    OidcProviderConfig, RuntimeWritableCatalog, SessionId, ThrottleConfig, TierLimits,
};
use connetto_test_harness::{
    Fixture, MOCK_OAUTH_CLIENT_ID, MOCK_OAUTH_CLIENT_SECRET, MOCK_OAUTH_PROVIDER, MockOauth,
    OPLOG_TABLE, PUBLICATION, SLOT, isolated_session_keyring, provision_cap_policies,
};
use diesel::sqlite::SqliteConnection;
use diesel::{Connection as _, ExpressionMethods, QueryableByName};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl as _, SimpleAsyncConnection as _};
use futures_util::FutureExt as _;
use serde_json::json;
use tempfile::{NamedTempFile, TempDir};
use tokio::io::AsyncWrite;
use uuid::Uuid;

use super::e2e::{PG_SERIAL, exec, reset_audit_table, signing_keys, with_user_url};
use super::lifecycle::admin_pool;

/// The schema document declares no RLS, the policies document carries it.
const PG_DDL: &str = "-- connetto:major 1\n\
                      CREATE TABLE records (id INT PRIMARY KEY, owner TEXT, body TEXT, secret TEXT, \
                                            score DOUBLE PRECISION, avatar BYTEA);\
                      CREATE TABLE mod_notes (id INT PRIMARY KEY, about TEXT, kind TEXT, body TEXT);\
                      CREATE TABLE stamps (at TIMESTAMPTZ PRIMARY KEY, owner TEXT, body TEXT);";
/// A record is visible to its owner or a named share key; a note only when it
/// is kind `visible`, which the owner-declared query overrides.
const POLICIES: &str = "ALTER TABLE records ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY records_p ON records USING (\
         owner = current_setting('app.user_id', true) \
         OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));\n\
     ALTER TABLE mod_notes ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY notes_p ON mod_notes USING (\
         about = current_setting('app.user_id', true) AND kind = 'visible');\n\
     ALTER TABLE stamps ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY stamps_p ON stamps USING (owner = current_setting('app.user_id', true));";

diesel::table! {
    records (id) {
        id -> Integer,
        owner -> Nullable<Text>,
        body -> Nullable<Text>,
        secret -> Nullable<Text>,
    }
}

diesel::table! {
    mod_notes (id) {
        id -> Integer,
        about -> Nullable<Text>,
        kind -> Nullable<Text>,
        body -> Nullable<Text>,
    }
}

diesel::table! {
    connetto_sessions (session_id) {
        session_id -> diesel::sql_types::Uuid,
        user_id -> Text,
        current_refresh_hash -> Binary,
        idle_deadline -> Timestamptz,
        absolute_deadline -> Timestamptz,
        revoked -> Bool,
        created_at -> Timestamptz,
        provider -> Text,
        issuer -> Text,
    }
}

/// The download's registered queries: the person's records minus the secret
/// column, and the person's notes through an owner-declared query.
fn record_list() -> PortabilityList {
    PortabilityList::new("r61-fixture")
        .query(PortabilityQuery::new(
            "SELECT id, owner, body FROM records",
            "record rows",
        ))
        .query(
            PortabilityQuery::new(
                "SELECT * FROM mod_notes WHERE about = ?",
                "notes of the person",
            )
            .binds(vec![PortabilityBind::Person])
            .read_as_owner(),
        )
}

/// Reset the portability fixture the way e2e resets its own.
async fn reset_portability_fixture(pool: &Pool<AsyncPgConnection>, fixture: &Fixture) {
    exec(pool, "DROP TABLE IF EXISTS records CASCADE").await;
    exec(pool, "DROP TABLE IF EXISTS mod_notes CASCADE").await;
    exec(pool, "DROP TABLE IF EXISTS stamps CASCADE").await;
    exec(pool, "DROP TABLE IF EXISTS _connetto_settled").await;
    exec(pool, "DROP TABLE IF EXISTS _connetto_writers").await;
    exec(pool, "DROP TABLE IF EXISTS _connetto_session_writers").await;
    connetto_test_harness::provision_writers(pool).await;
    reset_audit_table(pool).await;
    let mut conn = pool.get().await.expect("a connection");
    conn.batch_execute(PG_DDL).await.expect("the tables create");
    conn.batch_execute(POLICIES)
        .await
        .expect("the policies apply");
    drop(conn);
    provision_cap_policies(pool, PG_DDL, POLICIES).await;
    exec(
        pool,
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_reader') \
         THEN CREATE ROLE app_reader LOGIN PASSWORD 'app_reader'; END IF; END $$",
    )
    .await;
    exec(pool, "GRANT USAGE ON SCHEMA public TO app_reader").await;
    exec(
        pool,
        "GRANT SELECT ON records, mod_notes, stamps TO app_reader",
    )
    .await;
    exec(
        pool,
        "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, \
         _connetto_session_writers TO app_reader",
    )
    .await;
    let mut conn = pool.get().await.expect("a connection");
    diesel::insert_into(records::table)
        .values(vec![
            (
                records::id.eq(1),
                records::owner.eq("alice"),
                records::body.eq("alice visible"),
                records::secret.eq("alice secret"),
            ),
            (
                records::id.eq(2),
                records::owner.eq("bob"),
                records::body.eq("bob visible"),
                records::secret.eq("bob secret"),
            ),
            (
                records::id.eq(3),
                records::owner.eq("key:shared"),
                records::body.eq("shared"),
                records::secret.eq("shared secret"),
            ),
        ])
        .execute(&mut conn)
        .await
        .expect("seed records");
    diesel::insert_into(mod_notes::table)
        .values(vec![
            (
                mod_notes::id.eq(1),
                mod_notes::about.eq("alice"),
                mod_notes::kind.eq("visible"),
                mod_notes::body.eq("a visible note"),
            ),
            (
                mod_notes::id.eq(2),
                mod_notes::about.eq("alice"),
                mod_notes::kind.eq("moderation"),
                mod_notes::body.eq("a moderation note"),
            ),
            (
                mod_notes::id.eq(3),
                mod_notes::about.eq("bob"),
                mod_notes::kind.eq("visible"),
                mod_notes::body.eq("b visible note"),
            ),
        ])
        .execute(&mut conn)
        .await
        .expect("seed notes");
    fixture
        .start_replication(&["records", "mod_notes", "stamps"])
        .await;
}

/// The one-construction-path server over the portability fixture.
async fn portability_builder(
    fixture: &Fixture,
    list: PortabilityList,
) -> (ServerBuilder, MockOauth, TempDir) {
    let url = fixture.admin_url().to_owned();
    let reader = with_user_url(&url, "app_reader", "app_reader");
    let idp = MockOauth::start().await;
    let keys = TempDir::new().expect("a key dir");
    let (private, public) = signing_keys(&keys);
    let token = TokenKeys::from_pem(
        std::fs::read(&private).expect("read the private half"),
        std::fs::read(&public).expect("read the public half"),
    );
    let (_channel, store) = fixture.fga_store().await;
    let provider = OidcProvider::Generic(
        OidcProviderConfig::new(
            MOCK_OAUTH_PROVIDER,
            MOCK_OAUTH_CLIENT_ID,
            idp.issuer().to_owned(),
            "http://127.0.0.1:0/auth/callback",
        )
        .with_client_secret(Some(MOCK_OAUTH_CLIENT_SECRET.to_owned())),
    );
    let builder = ServerBuilder::new(
        Database::new(url, reader),
        ServerSchema::new(PG_DDL, POLICIES),
        token,
        OpenFga::new(fixture.fga_url().await.to_owned(), store),
    )
    .slot(SLOT)
    .publication(PUBLICATION)
    .oplog_table(OPLOG_TABLE)
    .oidc_providers(vec![provider])
    .writable(
        RuntimeWritableCatalog::builder()
            .writable("records")
            .build(),
    )
    .portability(list);
    (builder, idp, keys)
}

/// One live session for `user_id` on a fresh id.
async fn live_session(pool: &Pool<AsyncPgConnection>, user_id: &str) -> SessionId {
    let session_id = SessionId::from_uuid(Uuid::new_v4());
    let now = chrono::Utc::now();
    let mut conn = pool.get().await.expect("a connection");
    diesel::insert_into(connetto_sessions::table)
        .values((
            connetto_sessions::session_id.eq(session_id),
            connetto_sessions::user_id.eq(user_id),
            connetto_sessions::current_refresh_hash.eq(b"refresh-secret"),
            connetto_sessions::idle_deadline.eq(now + chrono::Duration::hours(1)),
            connetto_sessions::absolute_deadline.eq(now + chrono::Duration::days(30)),
            connetto_sessions::revoked.eq(false),
            connetto_sessions::created_at.eq(now),
            connetto_sessions::provider.eq(MOCK_OAUTH_PROVIDER),
            connetto_sessions::issuer.eq("https://issuer.example"),
        ))
        .execute(&mut conn)
        .await
        .expect("the session lands");
    session_id
}

/// The download's sink, kept so the receipt's byte count can be checked.
#[derive(Default)]
struct BytesSink {
    bytes: Vec<u8>,
}

impl AsyncWrite for BytesSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.bytes.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// Stage the archive bytes and open them as SQLite, the file kept as long as
/// the connection.
fn archive_db(data: &[u8]) -> (SqliteConnection, NamedTempFile) {
    let file = NamedTempFile::new().expect("a temp file for the archive");
    std::fs::write(file.path(), data).expect("stage the archive");
    let conn = SqliteConnection::establish(file.path().to_str().expect("a UTF-8 path"))
        .expect("the archive SQLite opens");
    (conn, file)
}

/// The archive's columns for `table`, raw because the tables are the deployment's.
fn archive_columns(db: &mut SqliteConnection, table: &str) -> Vec<String> {
    #[derive(QueryableByName)]
    struct TableInfo {
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
    }
    // A pragma table function, which no `table!` schema can name.
    let rows: Vec<TableInfo> = diesel::RunQueryDsl::load(
        diesel::sql_query("SELECT name FROM pragma_table_info(?) ORDER BY cid")
            .bind::<diesel::sql_types::Text, _>(table),
        db,
    )
    .expect("the pragma reads");
    rows.into_iter().map(|row| row.name).collect()
}

#[derive(Debug, PartialEq, Eq, QueryableByName)]
struct RecordRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    owner: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    body: String,
}

#[derive(Debug, PartialEq, Eq, QueryableByName)]
struct NoteRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    about: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    kind: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    body: String,
}

/// The one audit row a finished download leaves.
#[derive(Debug, PartialEq, Eq, QueryableByName)]
struct AuditRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    session: Option<uuid::Uuid>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    user_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    op: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    query_list_hash: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    archive_bytes: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    acted_by: Option<String>,
}

/// The archive's `data.sqlite` and `manifest.json` entries.
fn unzip(bytes: &[u8]) -> (Vec<u8>, serde_json::Value) {
    let mut archive = zip::ZipArchive::new(io::Cursor::new(bytes)).expect("the archive is a zip");
    let mut data = None;
    let mut manifest = None;
    for index in 0..archive.len() {
        let mut file = archive.by_index(index).expect("an entry");
        let name = file.name().to_owned();
        let mut content = Vec::new();
        io::Read::read_to_end(&mut file, &mut content).expect("read the entry");
        match name.as_str() {
            "data.sqlite" => data = Some(content),
            "manifest.json" => {
                manifest = Some(serde_json::from_slice(&content).expect("the manifest is JSON"));
            }
            other => panic!("unexpected archive entry {other:?}"),
        }
    }
    (
        data.expect("the archive holds data.sqlite"),
        manifest.expect("the archive holds manifest.json"),
    )
}

/// What alice's download carries, whichever requester asked for it.
fn assert_alice_archive(data: &[u8]) {
    let (mut db, _file) = archive_db(data);
    assert_eq!(
        archive_columns(&mut db, "records"),
        vec!["id", "owner", "body"],
        "the secret column is not a queried column and stays out of the table"
    );
    // The archive tables are the deployment's, so no `table!` names them.
    let records: Vec<RecordRow> = diesel::RunQueryDsl::load(
        diesel::sql_query("SELECT id, owner, body FROM records ORDER BY id"),
        &mut db,
    )
    .expect("read the records");
    assert_eq!(
        records,
        vec![RecordRow {
            id: 1,
            owner: "alice".to_owned(),
            body: "alice visible".to_owned(),
        }],
        "bob's row is not alice's, and the share key's row is a subject the \
         download's caller does not hold"
    );
    let notes: Vec<NoteRow> = diesel::RunQueryDsl::load(
        diesel::sql_query("SELECT id, about, kind, body FROM mod_notes ORDER BY id"),
        &mut db,
    )
    .expect("read the notes");
    assert_eq!(
        notes,
        vec![
            NoteRow {
                id: 1,
                about: "alice".to_owned(),
                kind: "visible".to_owned(),
                body: "a visible note".to_owned(),
            },
            NoteRow {
                id: 2,
                about: "alice".to_owned(),
                kind: "moderation".to_owned(),
                body: "a moderation note".to_owned(),
            },
        ],
        "the moderation note only the owner-declared query reaches is present, \
         and bob's note is not alice's"
    );
}

/// The manifest the archive carries.
fn assert_manifest(manifest: &serde_json::Value, query_list_hash: &str) {
    assert_eq!(manifest["person"], "alice");
    assert_eq!(manifest["deployment"], "r61-fixture");
    let system = manifest["postgres_system_identifier"]
        .as_str()
        .expect("the manifest names the cluster");
    assert!(
        system.parse::<u64>().is_ok(),
        "a system identifier: {system}"
    );
    let generated_at = manifest["generated_at"].as_str().expect("a moment");
    assert!(
        chrono::DateTime::parse_from_rfc3339(generated_at).is_ok(),
        "an RFC 3339 moment: {generated_at}"
    );
    let snapshot = manifest["snapshot"].as_str().expect("a snapshot");
    assert_eq!(
        snapshot.split(':').count(),
        3,
        "a pg_snapshot names xmin, xmax and the in-progress list: {snapshot}"
    );
    let queries = manifest["queries"]
        .as_array()
        .expect("the manifest names its queries");
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0]["sql"], "SELECT id, owner, body FROM records");
    assert_eq!(queries[0]["table"], "records");
    assert_eq!(queries[0]["columns"], json!(["id", "owner", "body"]));
    assert_eq!(queries[0]["description"], "record rows");
    assert_eq!(queries[0]["read_as_owner"], false);
    assert_eq!(queries[1]["sql"], "SELECT * FROM mod_notes WHERE about = ?");
    assert_eq!(queries[1]["table"], "mod_notes");
    assert_eq!(
        queries[1]["columns"],
        json!(["id", "about", "kind", "body"])
    );
    assert_eq!(queries[1]["description"], "notes of the person");
    assert_eq!(queries[1]["read_as_owner"], true);
    assert_eq!(manifest["query_list_hash"], query_list_hash);
}

/// The download's audit rows, ordered.
async fn audit_rows(pool: &Pool<AsyncPgConnection>) -> Vec<AuditRow> {
    let mut conn = pool.get().await.expect("a connection");
    diesel::sql_query(
        "SELECT session, user_id, CAST(op AS TEXT) AS op, query_list_hash, archive_bytes, acted_by \
         FROM auth_events ORDER BY at",
    )
    .load(&mut conn)
    .await
    .expect("read auth_events")
}

#[tokio::test]
async fn a_download_carries_only_the_rows_the_queries_reach() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let mut sink = BytesSink::default();
    let receipt = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut sink,
        )
        .await
        .expect("the download finishes");
    assert_eq!(
        receipt.archive_bytes,
        u64::try_from(sink.bytes.len()).expect("the sink fits"),
        "the receipt counts what the sink received"
    );

    let (data, manifest) = unzip(&sink.bytes);
    assert_alice_archive(&data);
    assert_manifest(&manifest, &receipt.query_list_hash);
    assert_eq!(
        audit_rows(&pool).await,
        vec![AuditRow {
            session: Some(*alice_session.as_uuid()),
            user_id: Some("alice".to_owned()),
            op: "data_downloaded".to_owned(),
            query_list_hash: Some(receipt.query_list_hash.clone()),
            archive_bytes: Some(i64::try_from(sink.bytes.len()).expect("the size fits")),
            acted_by: None,
        }],
        "one download leaves one audit row"
    );
}

#[tokio::test]
async fn a_download_refuses_a_session_the_store_no_longer_holds() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let revoked = live_session(&pool, "alice").await;
    let mut conn = pool.get().await.expect("a connection");
    diesel::update(connetto_sessions::table)
        .filter(connetto_sessions::session_id.eq(revoked))
        .set(connetto_sessions::revoked.eq(true))
        .execute(&mut conn)
        .await
        .expect("the revocation lands");
    let unknown = SessionId::from_uuid(Uuid::new_v4());

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let mut sink = BytesSink::default();
    for requester in [
        DataRequester::Session(revoked),
        DataRequester::Session(unknown),
    ] {
        let refused = parts
            .handle
            .download_data("alice".to_owned(), &requester, &mut sink)
            .await;
        assert!(
            matches!(refused, Err(DataRequestError::SessionNotLive)),
            "a session the store does not hold live cannot download, got {refused:?}"
        );
    }
    assert!(sink.bytes.is_empty(), "no archive was written");
}

#[tokio::test]
async fn an_operator_download_leaves_the_same_archive_and_names_who_acted() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let mut sink = BytesSink::default();
    let receipt = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Operator("staff:marco".to_owned()),
            &mut sink,
        )
        .await
        .expect("the operator's download finishes");

    let (data, manifest) = unzip(&sink.bytes);
    assert_alice_archive(&data);
    assert_manifest(&manifest, &receipt.query_list_hash);
    assert_eq!(
        audit_rows(&pool).await,
        vec![AuditRow {
            session: None,
            user_id: Some("alice".to_owned()),
            op: "data_downloaded".to_owned(),
            query_list_hash: Some(receipt.query_list_hash.clone()),
            archive_bytes: Some(i64::try_from(sink.bytes.len()).expect("the size fits")),
            acted_by: Some("staff:marco".to_owned()),
        }],
        "the operator's row names who acted and no session"
    );
}

#[tokio::test]
async fn a_download_fails_when_the_audit_table_is_gone() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    exec(&pool, "DROP TABLE auth_events").await;
    let mut sink = BytesSink::default();
    let err = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut sink,
        )
        .await
        .expect_err("without the audit table the record cannot be written");
    assert!(
        matches!(err, DataRequestError::Audit(_)),
        "the failure is the audit row's, not the read's: {err}"
    );
}

/// A session whose liveness the store cannot answer is not taken as live, so
/// the download fails before reading anything.
#[tokio::test]
async fn a_download_fails_when_the_session_store_cannot_answer() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    exec(&pool, "DROP TABLE connetto_sessions CASCADE").await;
    let mut sink = BytesSink::default();
    let err = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut sink,
        )
        .await
        .expect_err("without the session store no session is live");
    assert!(
        matches!(err, DataRequestError::Liveness(_)),
        "the failure is the store's, not a refusal of the session: {err}"
    );
    assert!(sink.bytes.is_empty(), "no archive was sent");
    assert_eq!(audit_rows(&pool).await, Vec::new(), "nothing is recorded");
}

/// The manifest names the cluster the rows came from, so a download whose
/// cluster record cannot be read fails rather than ship an unattributed one.
#[tokio::test]
async fn a_download_fails_when_the_cluster_record_cannot_be_read() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    exec(&pool, "DROP TABLE connetto_epoch").await;
    let mut sink = BytesSink::default();
    let err = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Operator("staff:marco".to_owned()),
            &mut sink,
        )
        .await
        .expect_err("without the cluster record the manifest cannot name it");
    assert!(
        matches!(err, DataRequestError::Epoch(_)),
        "the failure is the cluster record's: {err}"
    );
    assert!(sink.bytes.is_empty(), "no archive was sent");
    assert_eq!(audit_rows(&pool).await, Vec::new(), "nothing is recorded");
}

/// An owner-declared query that fails when Postgres runs it fails the whole
/// download, though the person's own queries read fine before it.
#[tokio::test]
async fn an_owner_query_failing_at_read_time_fails_the_download() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let alice_session = live_session(&pool, "alice").await;
    let list = PortabilityList::new("r61-fixture")
        .query(PortabilityQuery::new(
            "SELECT id, owner, body FROM records",
            "record rows",
        ))
        .query(
            PortabilityQuery::new(
                "SELECT * FROM mod_notes WHERE id / ? = 1",
                "notes divided by zero",
            )
            .binds(vec![PortabilityBind::Value(BindValue::Integer(0))])
            .read_as_owner(),
        );

    let (builder, _idp, _keys) = portability_builder(&fixture, list).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let mut sink = BytesSink::default();
    let err = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut sink,
        )
        .await
        .expect_err("dividing by zero fails the owner read");
    assert!(
        matches!(err, DataRequestError::Read(_)),
        "the failure is the read's: {err}"
    );
    assert!(sink.bytes.is_empty(), "no archive was sent");
    assert_eq!(audit_rows(&pool).await, Vec::new(), "nothing is recorded");
}

#[tokio::test]
async fn a_download_refuses_when_no_query_is_registered() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let alice_session = live_session(&pool, "alice").await;
    let session_requester = DataRequester::Session(alice_session);

    let (builder, _idp, _keys) =
        portability_builder(&fixture, PortabilityList::new("r61-fixture")).await;
    let mut sink = BytesSink::default();
    let parts = builder.build().await.expect("the deployment assembles");
    let refused = parts
        .handle
        .download_data("alice".to_owned(), &session_requester, &mut sink)
        .await;
    assert!(
        matches!(refused, Err(DataRequestError::NoQueries)),
        "got {refused:?}"
    );
    assert!(sink.bytes.is_empty(), "no archive was written");
}

#[tokio::test]
async fn a_bad_query_refuses_the_build_naming_it() {
    let builder = |list: PortabilityList| {
        let keys = TempDir::new().expect("a key dir");
        let (private, public) = signing_keys(&keys);
        ServerBuilder::new(
            Database::new(
                "postgres://connetto:connetto@127.0.0.1:1/connetto",
                "postgres://connetto:connetto@127.0.0.1:1/connetto",
            ),
            ServerSchema::new(PG_DDL, ""),
            TokenKeys::from_pem(
                std::fs::read(&private).expect("read the private half"),
                std::fs::read(&public).expect("read the public half"),
            ),
            OpenFga::new("http://127.0.0.1:1", "store"),
        )
        .oidc_providers(vec![OidcProvider::Generic(OidcProviderConfig::new(
            MOCK_OAUTH_PROVIDER,
            MOCK_OAUTH_CLIENT_ID,
            "http://127.0.0.1:1/issuer",
            "http://127.0.0.1:1/auth/callback",
        ))])
        .portability(list)
    };
    // The refusal comes before build's first await, so one poll settles it.
    let refused = |builder: ServerBuilder, what: &str| -> BuildError {
        match builder.build().now_or_never().expect(what) {
            Ok(_) => panic!("{what}"),
            Err(err) => err,
        }
    };
    let unknown_table = builder(PortabilityList::new("d").query(PortabilityQuery::new(
        "SELECT * FROM nowhere",
        "the missing table",
    )));
    let err = refused(unknown_table, "an unknown table refuses");
    assert!(
        matches!(&err, BuildError::Portability { name, .. } if name == "the missing table"),
        "the refusal names the query: {err}"
    );
    let no_key = builder(PortabilityList::new("d").query(PortabilityQuery::new(
        "SELECT body FROM records",
        "the keyless list",
    )));
    let err = refused(no_key, "a column list without the key refuses");
    assert!(
        matches!(&err, BuildError::Portability { name, .. } if name == "the keyless list"),
        "the refusal names the query: {err}"
    );
    let duplicate = builder(
        PortabilityList::new("d")
            .query(PortabilityQuery::new(
                "SELECT id, owner, body FROM records",
                "first",
            ))
            .query(PortabilityQuery::new("SELECT * FROM records", "second")),
    );
    let err = refused(duplicate, "two queries on one table refuse");
    assert!(
        matches!(&err, BuildError::Portability { name, .. } if name == "second"),
        "the refusal names the second query: {err}"
    );
    let count = builder(PortabilityList::new("d").query(PortabilityQuery::new(
        "SELECT COUNT(*) FROM records",
        "the count",
    )));
    let err = refused(count, "an aggregate reads no rows and refuses");
    assert!(
        matches!(
            &err,
            BuildError::Portability { name, source: PortabilityError::Projection }
                if name == "the count"
        ),
        "the refusal names the aggregate: {err}"
    );
    let two = builder(PortabilityList::new("d").query(PortabilityQuery::new(
        "SELECT * FROM records; SELECT * FROM mod_notes",
        "two statements",
    )));
    let err = refused(two, "a query holding two statements refuses");
    assert!(
        matches!(
            &err,
            BuildError::Portability { name, source: PortabilityError::Translate(_) }
                if name == "two statements"
        ),
        "the refusal names the query: {err}"
    );
}

/// The ids [`seed_many_records`] gives alice.
fn many_alice_ids() -> impl Iterator<Item = i32> {
    (10..=3209).filter(|n| n % 3 != 0)
}

/// The body [`seed_many_records`] gives row `n`, padded so the archive spans
/// several of the stream's chunks.
fn many_body(n: i32) -> String {
    format!("row {n} {}", "x".repeat(64))
}

/// Seed 3200 more records. Every third row is bob's, so each page skips rows
/// the policy hides, and the ids run past 1000 so a key compared as text
/// would resume wrongly.
async fn seed_many_records(pool: &Pool<AsyncPgConnection>) {
    exec(
        pool,
        "INSERT INTO records (id, owner, body) \
         SELECT n, CASE WHEN n % 3 = 0 THEN 'bob' ELSE 'alice' END, \
                'row ' || n || ' ' || repeat('x', 64) \
         FROM generate_series(10, 3209) AS n",
    )
    .await;
}

/// A person's rows past one read's page all arrive once, each page resuming
/// after the last key of the one before.
#[tokio::test]
async fn a_download_pages_through_more_rows_than_one_read_holds() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    seed_many_records(&pool).await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let mut sink = BytesSink::default();
    parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut sink,
        )
        .await
        .expect("the download finishes");

    let (data, _manifest) = unzip(&sink.bytes);
    let (mut db, _file) = archive_db(&data);
    let records: Vec<RecordRow> = diesel::RunQueryDsl::load(
        diesel::sql_query("SELECT id, owner, body FROM records ORDER BY id"),
        &mut db,
    )
    .expect("read the records");
    let expected: Vec<RecordRow> = std::iter::once(RecordRow {
        id: 1,
        owner: "alice".to_owned(),
        body: "alice visible".to_owned(),
    })
    .chain(many_alice_ids().map(|n| RecordRow {
        id: n,
        owner: "alice".to_owned(),
        body: many_body(n),
    }))
    .collect();
    assert!(
        expected.len() > 2 * 1024,
        "the person's rows fill more than two pages"
    );
    assert_eq!(
        records, expected,
        "every one of alice's rows arrives once and none of bob's"
    );
}

/// The download's sink when the client went away before the first byte.
struct BrokenSink;

impl AsyncWrite for BrokenSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::task::Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// A sink that breaks fails the download, and a download that never reached
/// the person is not recorded as one.
#[tokio::test]
async fn a_download_whose_sink_breaks_fails_unrecorded() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    seed_many_records(&pool).await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let err = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut BrokenSink,
        )
        .await
        .expect_err("a broken sink cannot take the archive");
    assert!(
        matches!(&err, DataRequestError::Io(io) if io.kind() == io::ErrorKind::BrokenPipe),
        "the failure is the sink's: {err}"
    );
    assert_eq!(
        audit_rows(&pool).await,
        Vec::new(),
        "a download the sink never took is not recorded"
    );
}

/// A table keyed by a timestamp pages like an integer-keyed one, each page
/// resuming past the last instant the one before delivered.
#[tokio::test]
async fn a_download_pages_through_a_table_keyed_by_a_timestamp() {
    #[derive(QueryableByName)]
    struct Body {
        #[diesel(sql_type = diesel::sql_types::Text)]
        body: String,
    }

    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    // Every third instant is bob's, so the pages skip rows the policy hides.
    exec(
        &pool,
        "INSERT INTO stamps (at, owner, body) \
         SELECT timestamptz '2026-01-01 00:00:00+00' + n * interval '1 minute', \
                CASE WHEN n % 3 = 0 THEN 'bob' ELSE 'alice' END, 'stamp ' || n \
         FROM generate_series(1, 3200) AS n",
    )
    .await;
    let list = PortabilityList::new("r61-fixture").query(PortabilityQuery::new(
        "SELECT at, owner, body FROM stamps",
        "stamps",
    ));

    let (builder, _idp, _keys) = portability_builder(&fixture, list).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let mut sink = BytesSink::default();
    parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Operator("staff:marco".to_owned()),
            &mut sink,
        )
        .await
        .expect("the download finishes");

    let (data, _manifest) = unzip(&sink.bytes);
    let (mut db, _file) = archive_db(&data);
    let bodies: Vec<Body> = diesel::RunQueryDsl::load(
        diesel::sql_query("SELECT body FROM stamps ORDER BY at"),
        &mut db,
    )
    .expect("read the stamps");
    let expected: Vec<String> = (1..=3200)
        .filter(|n| n % 3 != 0)
        .map(|n| format!("stamp {n}"))
        .collect();
    assert!(
        expected.len() > 2 * 1024,
        "alice's rows fill more than two pages"
    );
    assert_eq!(
        bodies.into_iter().map(|row| row.body).collect::<Vec<_>>(),
        expected,
        "every one of alice's stamps arrives once and none of bob's"
    );
}

/// A fixed bind narrows the rows beside the policy, and each value keeps its
/// type in the archive: a NULL stays NULL, a double a real, bytes a blob.
#[tokio::test]
async fn a_download_binds_fixed_values_and_keeps_each_value_typed() {
    #[derive(Debug, PartialEq, QueryableByName)]
    struct TypedRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        id: i32,
        #[diesel(sql_type = diesel::sql_types::Text)]
        body_type: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        score_type: String,
        #[diesel(sql_type = diesel::sql_types::Double)]
        score: f64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        avatar_type: String,
        #[diesel(sql_type = diesel::sql_types::Binary)]
        avatar: Vec<u8>,
    }

    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    exec(
        &pool,
        "INSERT INTO records (id, owner, body, score, avatar) VALUES \
         (10, 'alice', NULL, 2.5, '\\x00ff10'::bytea), \
         (11, 'alice', 'low', 0.5, '\\x01'::bytea), \
         (12, 'bob', 'bob scored', 9.5, '\\x02'::bytea)",
    )
    .await;
    let alice_session = live_session(&pool, "alice").await;
    let list = PortabilityList::new("r61-fixture").query(
        PortabilityQuery::new(
            "SELECT id, owner, body, score, avatar FROM records WHERE score >= ?",
            "scored records",
        )
        .binds(vec![PortabilityBind::Value(BindValue::Real(1.0))]),
    );

    let (builder, _idp, _keys) = portability_builder(&fixture, list).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let mut sink = BytesSink::default();
    parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut sink,
        )
        .await
        .expect("the download finishes");

    let (data, _manifest) = unzip(&sink.bytes);
    let (mut db, _file) = archive_db(&data);
    let rows: Vec<TypedRow> = diesel::RunQueryDsl::load(
        diesel::sql_query(
            "SELECT id, typeof(body) AS body_type, typeof(score) AS score_type, score, \
             typeof(avatar) AS avatar_type, avatar FROM records ORDER BY id",
        ),
        &mut db,
    )
    .expect("read the records");
    assert_eq!(
        rows,
        vec![TypedRow {
            id: 10,
            body_type: "null".to_owned(),
            score_type: "real".to_owned(),
            score: 2.5,
            avatar_type: "blob".to_owned(),
            avatar: vec![0x00, 0xff, 0x10],
        }],
        "the bind keeps alice's scored row and drops her low and unscored ones, \
         the policy drops bob's, and each value keeps its type"
    );
}

/// A row wider than the identified tier's ceiling fails the download naming
/// the query, and nothing is recorded or sent.
#[tokio::test]
async fn a_row_over_the_ceiling_fails_the_download() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder
        .throttle(
            ThrottleConfig::default().with_identified(TierLimits::identified().with_row_ceiling(8)),
        )
        .build()
        .await
        .expect("the deployment assembles");
    let mut sink = BytesSink::default();
    let err = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Session(alice_session),
            &mut sink,
        )
        .await
        .expect_err("alice's record is wider than 8 bytes");
    assert!(
        matches!(
            &err,
            DataRequestError::RowTooWide { query, ceiling: 8, .. } if query == "record rows"
        ),
        "the failure names the query and the ceiling: {err}"
    );
    assert!(sink.bytes.is_empty(), "no archive was sent");
    assert_eq!(
        audit_rows(&pool).await,
        Vec::new(),
        "a download that failed is not recorded as one"
    );
}
