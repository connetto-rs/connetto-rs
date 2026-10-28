//! The person's data download (R61 slices 2 and 3), each test over its own
//! fixture through the one construction path.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use connetto_core::device_cert::KeyId;
use connetto_core::messages::{BindValue, ControlMessage, FatalError, FatalErrorReason};
use connetto_server::builder::{
    BuildError, ContentSettings, Database, OidcProvider, OpenFga, ServerBuilder, ServerSchema,
    StoreSpec, TokenKeys,
};
use connetto_server::device_cert::DeviceCertConfig;
use connetto_server::portability::{
    DataRequestError, DataRequester, PersonEnded, PortabilityBind, PortabilityError,
    PortabilityFile, PortabilityList, PortabilityQuery,
};
use connetto_server::{
    OidcProviderConfig, RuntimeWritableCatalog, SessionId, SessionsError, ThrottleConfig,
    TierLimits,
};
use connetto_test_harness::{
    Fixture, MOCK_OAUTH_CLIENT_ID, MOCK_OAUTH_CLIENT_SECRET, MOCK_OAUTH_PROVIDER, MockOauth,
    OPLOG_TABLE, PUBLICATION, SLOT, isolated_session_keyring, provision_cap_policies,
};
use diesel::sqlite::SqliteConnection;
use diesel::{Connection as _, ExpressionMethods, QueryDsl as _, QueryableByName};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl as _, SimpleAsyncConnection as _};
use futures_util::FutureExt as _;
use rcgen::PublicKeyData as _;
use serde_json::json;
use tempfile::{NamedTempFile, TempDir};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use uuid::Uuid;

use super::builder_coverage::apply_content_deployment;
use super::device_identity::{challenge_nonce, enrol_request};
use super::e2e::{PG_SERIAL, exec, mint_tokens, reset_audit_table, signing_keys, with_user_url};
use super::enrolment::{device_key, rooted_issuer};
use super::lifecycle::{admin_pool, next_control, wait_ready};

/// The schema document declares no RLS, the policies document carries it.
const PG_DDL: &str = "-- connetto:major 1\n\
                      CREATE TABLE records (id INT PRIMARY KEY, owner TEXT, body TEXT, secret TEXT, \
                                            score DOUBLE PRECISION, avatar BYTEA);\
                      CREATE TABLE mod_notes (id INT PRIMARY KEY, about TEXT, kind TEXT, body TEXT);\
                      CREATE TABLE stamps (at TIMESTAMPTZ PRIMARY KEY, owner TEXT, body TEXT);\
                      CREATE TABLE media (id INT PRIMARY KEY, owner TEXT, content_id BYTEA, title TEXT);";
/// A record is visible to its owner or a named share key, a note only when it
/// is kind `visible`, which the owner-declared query overrides, and a stamp
/// or a media row to its owner.
const POLICIES: &str = "ALTER TABLE records ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY records_p ON records USING (\
         owner = current_setting('app.user_id', true) \
         OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));\n\
     ALTER TABLE mod_notes ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY notes_p ON mod_notes USING (\
         about = current_setting('app.user_id', true) AND kind = 'visible');\n\
     ALTER TABLE stamps ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY stamps_p ON stamps USING (owner = current_setting('app.user_id', true));\n\
     ALTER TABLE media ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY media_p ON media USING (owner = current_setting('app.user_id', true));";

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
    media (id) {
        id -> Integer,
        owner -> Text,
        content_id -> Binary,
        title -> Nullable<Text>,
    }
}

diesel::table! {
    _cfs_manifests (file_id, uploaded_by) {
        file_id -> Binary,
        total_len -> BigInt,
        accepted_bytes -> BigInt,
        committed -> Bool,
        uploaded_by -> Text,
    }
}

diesel::table! {
    _cfs_manifest_chunks (file_id, uploaded_by, position) {
        file_id -> Binary,
        uploaded_by -> Text,
        position -> Integer,
        chunk_hash -> Binary,
        chunk_len -> BigInt,
        stored -> Bool,
    }
}

diesel::table! {
    _cfs_chunk_registry (chunk_hash) {
        chunk_hash -> Binary,
        state -> Text,
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

diesel::table! {
    connetto_device_enrolments (key_id) {
        key_id -> Binary,
        user_id -> Text,
        session_id -> Uuid,
        enrolled_at -> Timestamptz,
        last_seen -> Timestamptz,
        revoked_at -> Nullable<Timestamptz>,
        public_key -> Binary,
        attestation -> Text,
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
    exec(pool, "DROP TABLE IF EXISTS media CASCADE").await;
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
        "GRANT SELECT ON records, mod_notes, stamps, media TO app_reader",
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
        .start_replication(&["records", "mod_notes", "stamps", "media"])
        .await;
}

/// The one-construction-path server over the portability fixture.
async fn portability_builder(
    fixture: &Fixture,
    list: PortabilityList,
) -> (ServerBuilder, MockOauth, TempDir) {
    // The tests on this path never drive the OAuth dance over HTTP, so the
    // callback's port is a placeholder the server never reaches.
    portability_builder_base(fixture, list, "http://127.0.0.1:0").await
}

/// [`portability_builder`] under a real auth base, for the tests that serve
/// HTTP and drive the login dance through the callback.
async fn portability_builder_base(
    fixture: &Fixture,
    list: PortabilityList,
    base: &str,
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
            format!("{base}/auth/callback"),
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

/// Every entry of the archive by name.
fn unzip(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut archive = zip::ZipArchive::new(io::Cursor::new(bytes)).expect("the archive is a zip");
    (0..archive.len())
        .map(|index| {
            let mut file = archive.by_index(index).expect("an entry");
            let mut content = Vec::new();
            io::Read::read_to_end(&mut file, &mut content).expect("read the entry");
            (file.name().to_owned(), content)
        })
        .collect()
}

/// The archive's `data.sqlite` and `manifest.json`, with no other entry.
fn unzip_rows(bytes: &[u8]) -> (Vec<u8>, serde_json::Value) {
    let mut entries = unzip(bytes);
    let data = entries
        .remove("data.sqlite")
        .expect("the archive holds data.sqlite");
    let manifest = entries
        .remove("manifest.json")
        .expect("the archive holds manifest.json");
    assert!(
        entries.is_empty(),
        "no files were named: {:?}",
        entries.keys()
    );
    (
        data,
        serde_json::from_slice(&manifest).expect("the manifest is JSON"),
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

/// The sessions a `session_revoked` audit row names, ordered.
async fn revoked_sessions(pool: &Pool<AsyncPgConnection>) -> Vec<uuid::Uuid> {
    let mut rows = audit_rows(pool).await;
    rows.retain(|row| row.op == "session_revoked");
    let mut sessions: Vec<uuid::Uuid> = rows
        .into_iter()
        .map(|row| row.session.expect("a session revocation names its session"))
        .collect();
    sessions.sort();
    sessions
}

/// Whether the named session is revoked.
async fn session_revoked(pool: &Pool<AsyncPgConnection>, session: SessionId) -> bool {
    let mut conn = pool.get().await.expect("a connection");
    diesel::select(diesel::dsl::exists(
        connetto_sessions::table
            .filter(connetto_sessions::session_id.eq(session))
            .filter(connetto_sessions::revoked.eq(true)),
    ))
    .get_result(&mut conn)
    .await
    .expect("read the session")
}

/// Whether the named key's enrolment is revoked.
async fn device_revoked(pool: &Pool<AsyncPgConnection>, key: KeyId) -> bool {
    let mut conn = pool.get().await.expect("a connection");
    diesel::select(diesel::dsl::exists(
        connetto_device_enrolments::table
            .filter(connetto_device_enrolments::key_id.eq(key.as_bytes().to_vec()))
            .filter(connetto_device_enrolments::revoked_at.is_not_null()),
    ))
    .get_result(&mut conn)
    .await
    .expect("read the enrolment")
}

/// Enrol a fresh device key under `subject`'s login, answering the login's
/// user id and the enrolled key.
async fn enrol_device(port: u16, subject: &str) -> (String, KeyId) {
    let base = format!("http://127.0.0.1:{port}");
    let (access, user_id, _refresh) = mint_tokens(&base, subject).await;
    let mut client = super::lifecycle::live_session(&format!("127.0.0.1:{port}"), &access).await;
    let nonce = challenge_nonce(&mut client).await;
    let key = device_key();
    match enrol_request(&mut client, &key, nonce, None).await {
        ControlMessage::EnrolGrant(_) => {}
        other => panic!("the enrolment answered {other:?}"),
    }
    (
        user_id,
        KeyId::of_public_key(&key.subject_public_key_info()),
    )
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

    let (data, manifest) = unzip_rows(&sink.bytes);
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

    let (data, manifest) = unzip_rows(&sink.bytes);
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

/// A server over unreachable collaborators, refusing at registration before
/// it would connect to any.
fn unconnected_builder(list: PortabilityList, content: Option<ContentSettings>) -> ServerBuilder {
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
    .content(content)
    .portability(list)
}

/// The build's refusal, which comes before its first await, so one poll
/// settles it.
fn refused(builder: ServerBuilder, what: &str) -> BuildError {
    match builder.build().now_or_never().expect(what) {
        Ok(_) => panic!("{what}"),
        Err(err) => err,
    }
}

#[tokio::test]
async fn a_bad_query_refuses_the_build_naming_it() {
    let builder = unconnected_builder;
    let unknown_table = builder(
        PortabilityList::new("d").query(PortabilityQuery::new(
            "SELECT * FROM nowhere",
            "the missing table",
        )),
        None,
    );
    let err = refused(unknown_table, "an unknown table refuses");
    assert!(
        matches!(&err, BuildError::Portability { name, .. } if name == "the missing table"),
        "the refusal names the query: {err}"
    );
    let no_key = builder(
        PortabilityList::new("d").query(PortabilityQuery::new(
            "SELECT body FROM records",
            "the keyless list",
        )),
        None,
    );
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
        None,
    );
    let err = refused(duplicate, "two queries on one table refuse");
    assert!(
        matches!(&err, BuildError::Portability { name, .. } if name == "second"),
        "the refusal names the second query: {err}"
    );
    let count = builder(
        PortabilityList::new("d").query(PortabilityQuery::new(
            "SELECT COUNT(*) FROM records",
            "the count",
        )),
        None,
    );
    let err = refused(count, "an aggregate reads no rows and refuses");
    assert!(
        matches!(
            &err,
            BuildError::Portability { name, source: PortabilityError::Projection }
                if name == "the count"
        ),
        "the refusal names the aggregate: {err}"
    );
    let two = builder(
        PortabilityList::new("d").query(PortabilityQuery::new(
            "SELECT * FROM records; SELECT * FROM mod_notes",
            "two statements",
        )),
        None,
    );
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

    let (data, _manifest) = unzip_rows(&sink.bytes);
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

    let (data, _manifest) = unzip_rows(&sink.bytes);
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

    let (data, _manifest) = unzip_rows(&sink.bytes);
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

#[tokio::test]
async fn a_bad_file_column_refuses_the_build_naming_it() {
    let store = TempDir::new().expect("a store directory");
    let media_files = |file: PortabilityFile| {
        PortabilityList::new("d").query(
            PortabilityQuery::new("SELECT id, owner, content_id, title FROM media", "photos")
                .file(file),
        )
    };
    let err = refused(
        unconnected_builder(media_files(PortabilityFile::new("content_id")), None),
        "files without a content store refuse",
    );
    assert!(
        matches!(&err, BuildError::Portability { name, source: PortabilityError::NoContentStore }
            if name == "photos"),
        "the refusal names the query: {err}"
    );
    let err = refused(
        unconnected_builder(
            media_files(PortabilityFile::new("owner").named_by("title")),
            Some(content_settings(store.path())),
        ),
        "a text column cannot hold a file id",
    );
    assert!(
        matches!(&err, BuildError::Portability {
            source: PortabilityError::FileColumnType { column, .. }, ..
        } if column == "owner"),
        "the refusal names the column: {err}"
    );
    let err = refused(
        unconnected_builder(
            media_files(PortabilityFile::new("content_id").named_by("caption")),
            Some(content_settings(store.path())),
        ),
        "a name column the query does not read refuses",
    );
    assert!(
        matches!(&err, BuildError::Portability {
            source: PortabilityError::FileColumn { column }, ..
        } if column == "caption"),
        "the refusal names the column: {err}"
    );
}

/// File settings over a file-system store at `store`, with the sweep idle
/// for the length of a test.
fn content_settings(store: &Path) -> ContentSettings {
    ContentSettings {
        base_url: "http://127.0.0.1:8099".to_owned(),
        ttl: Duration::from_secs(60),
        read_ceiling: 1 << 20,
        grace: Duration::from_secs(3600),
        cadence: Duration::from_secs(3600),
        quota_identity: 0,
        storage_ceiling: 0,
        bandwidth_ceiling: 0,
        bandwidth_window_days: 30,
        warn_fraction: 0.8,
        ceiling_refresh: Duration::from_secs(10),
        owner_pool_size: 2,
        store: StoreSpec::Fs(store.to_path_buf()),
        key: ring::signature::Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new())
            .expect("a ticket key")
            .as_ref()
            .to_vec(),
    }
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// Where the file-system store keeps the chunk `hash`.
fn chunk_path(store: &Path, hash: &[u8]) -> std::path::PathBuf {
    let hash = hex(hash);
    store.join(&hash[..2]).join(&hash[2..4]).join(&hash)
}

/// A file committed by `uploader` whose bytes are `chunks`, each written
/// where the file-system store keeps it and registered stored. Returns its
/// id, the BLAKE3 hash of the whole file.
async fn seed_file(
    pool: &Pool<AsyncPgConnection>,
    store: &Path,
    uploader: &str,
    chunks: &[&[u8]],
) -> Vec<u8> {
    let mut whole = blake3::Hasher::new();
    for chunk in chunks {
        whole.update(chunk);
    }
    let id = whole.finalize().as_bytes().to_vec();
    let total = i64::try_from(chunks.iter().map(|chunk| chunk.len()).sum::<usize>())
        .expect("the size fits");
    let mut conn = pool.get().await.expect("a connection");
    diesel::insert_into(_cfs_manifests::table)
        .values((
            _cfs_manifests::file_id.eq(&id),
            _cfs_manifests::total_len.eq(total),
            _cfs_manifests::accepted_bytes.eq(total),
            _cfs_manifests::committed.eq(true),
            _cfs_manifests::uploaded_by.eq(uploader),
        ))
        .execute(&mut conn)
        .await
        .expect("the manifest lands");
    for (position, chunk) in chunks.iter().enumerate() {
        let hash = blake3::hash(chunk).as_bytes().to_vec();
        let path = chunk_path(store, &hash);
        std::fs::create_dir_all(path.parent().expect("the chunk parents"))
            .expect("make the chunk parents");
        std::fs::write(&path, chunk).expect("store the chunk");
        diesel::insert_into(_cfs_chunk_registry::table)
            .values((
                _cfs_chunk_registry::chunk_hash.eq(&hash),
                _cfs_chunk_registry::state.eq("stored"),
            ))
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .expect("the chunk registers");
        diesel::insert_into(_cfs_manifest_chunks::table)
            .values((
                _cfs_manifest_chunks::file_id.eq(&id),
                _cfs_manifest_chunks::uploaded_by.eq(uploader),
                _cfs_manifest_chunks::position.eq(i32::try_from(position).expect("a position")),
                _cfs_manifest_chunks::chunk_hash.eq(&hash),
                _cfs_manifest_chunks::chunk_len
                    .eq(i64::try_from(chunk.len()).expect("the length fits")),
                _cfs_manifest_chunks::stored.eq(true),
            ))
            .execute(&mut conn)
            .await
            .expect("the chunk row lands");
    }
    id
}

/// One media row.
async fn seed_media(
    pool: &Pool<AsyncPgConnection>,
    id: i32,
    owner: &str,
    file: &[u8],
    title: Option<&str>,
) {
    let mut conn = pool.get().await.expect("a connection");
    diesel::insert_into(media::table)
        .values((
            media::id.eq(id),
            media::owner.eq(owner),
            media::content_id.eq(file),
            media::title.eq(title),
        ))
        .execute(&mut conn)
        .await
        .expect("the media row lands");
}

/// The download of a person's photos and the files they name.
fn photo_list() -> PortabilityList {
    PortabilityList::new("r61-fixture").query(
        PortabilityQuery::new("SELECT id, owner, content_id, title FROM media", "photos")
            .file(PortabilityFile::new("content_id").named_by("title")),
    )
}

#[tokio::test]
async fn a_download_carries_each_file_its_rows_name_once() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    apply_content_deployment(&pool).await;
    let store = TempDir::new().expect("a store directory");
    let beach = seed_file(&pool, store.path(), "alice", &[b"sand and ", b"sea"]).await;
    let other_beach = seed_file(&pool, store.path(), "alice", &[b"another beach"]).await;
    let untitled = seed_file(&pool, store.path(), "alice", &[b"no title"]).await;
    let climbing = seed_file(&pool, store.path(), "alice", &[b"up and out"]).await;
    let lost = seed_file(&pool, store.path(), "alice", &[b"soon unreadable"]).await;
    let bobs = seed_file(&pool, store.path(), "bob", &[b"bob's photo"]).await;
    let never = vec![7_u8; 32];
    seed_media(&pool, 1, "alice", &beach, Some("beach.jpg")).await;
    seed_media(&pool, 2, "alice", &beach, Some("copy of beach.jpg")).await;
    seed_media(&pool, 3, "alice", &other_beach, Some("Beach.jpg")).await;
    seed_media(&pool, 4, "alice", &never, Some("draft.png")).await;
    seed_media(&pool, 5, "alice", &untitled, None).await;
    seed_media(&pool, 6, "alice", &climbing, Some("../up.txt")).await;
    seed_media(&pool, 7, "alice", &lost, Some("gone.txt")).await;
    seed_media(&pool, 8, "bob", &bobs, Some("bob.jpg")).await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, photo_list()).await;
    let parts = builder
        .content(Some(content_settings(store.path())))
        .build()
        .await
        .expect("the deployment assembles");
    // The lost file's only chunk leaves the store after the boot reconcile.
    std::fs::remove_file(chunk_path(
        store.path(),
        blake3::hash(b"soon unreadable").as_bytes(),
    ))
    .expect("lose the chunk");
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

    let mut entries = unzip(&sink.bytes);
    let manifest: serde_json::Value = serde_json::from_slice(
        &entries
            .remove("manifest.json")
            .expect("the archive holds manifest.json"),
    )
    .expect("the manifest is JSON");
    let data = entries
        .remove("data.sqlite")
        .expect("the archive holds data.sqlite");
    let beach_short = format!("files/Beach ({}).jpg", &hex(&other_beach)[..8]);
    assert_eq!(
        entries,
        BTreeMap::from([
            ("files/beach.jpg".to_owned(), b"sand and sea".to_vec()),
            (beach_short.clone(), b"another beach".to_vec()),
            (format!("files/{}", hex(&untitled)), b"no title".to_vec()),
            ("files/_up.txt".to_owned(), b"up and out".to_vec()),
        ]),
        "each file once under its first row's title, a clashing title gaining \
         the head of its id, an untitled one under its id, a title that climbs \
         out kept inside, and nothing of bob's"
    );
    assert_eq!(
        manifest["files"],
        json!([
            {"file_id": hex(&beach), "path": "beach.jpg"},
            {"file_id": hex(&other_beach), "path": beach_short.trim_start_matches("files/")},
            {"file_id": hex(&untitled), "path": hex(&untitled)},
            {"file_id": hex(&climbing), "path": "_up.txt"},
        ])
    );
    assert_eq!(
        manifest["missing_files"],
        json!([
            {"file_id": hex(&never), "reason": "not_uploaded"},
            {"file_id": hex(&lost), "reason": "unreadable"},
        ])
    );
    assert_eq!(manifest["queries"][0]["file_column"], "content_id");
    assert_eq!(manifest["queries"][0]["name_column"], "title");
    assert_eq!(manifest["query_list_hash"], receipt.query_list_hash);

    let rows = archive_media(&data);
    assert_eq!(
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7],
        "every row of alice's stays in the table, the ones whose file is missing too"
    );
    assert_eq!(rows[1].1, beach, "the row keeps the id the manifest maps");
}

/// The archive's media rows, by id, with the file id each holds.
fn archive_media(data: &[u8]) -> Vec<(i32, Vec<u8>)> {
    #[derive(QueryableByName)]
    struct MediaRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        id: i32,
        #[diesel(sql_type = diesel::sql_types::Binary)]
        content_id: Vec<u8>,
    }
    let (mut db, _file) = archive_db(data);
    let rows: Vec<MediaRow> = diesel::RunQueryDsl::load(
        diesel::sql_query("SELECT id, content_id FROM media ORDER BY id"),
        &mut db,
    )
    .expect("read the media rows");
    rows.into_iter()
        .map(|row| (row.id, row.content_id))
        .collect()
}

/// A row naming no file adds none, a cell that is no file id and a file
/// whose first chunk is not the one its manifest names are listed as not
/// produced, and an empty file is carried as an empty entry.
#[tokio::test]
async fn a_download_lists_what_it_cannot_produce_and_carries_an_empty_file() {
    #[derive(QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        id: i32,
    }

    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    apply_content_deployment(&pool).await;
    let store = TempDir::new().expect("a store directory");
    let empty = seed_file(&pool, store.path(), "alice", &[]).await;
    let tampered = seed_file(&pool, store.path(), "alice", &[b"original bytes"]).await;
    let short = [1_u8, 2, 3];
    seed_media(&pool, 1, "alice", &empty, Some("empty.txt")).await;
    seed_media(&pool, 2, "alice", &tampered, Some("tampered.txt")).await;
    seed_media(&pool, 3, "alice", &short, Some("short.txt")).await;
    exec(
        &pool,
        "INSERT INTO media (id, owner, content_id, title) VALUES (4, 'alice', NULL, 'none')",
    )
    .await;
    let alice_session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, photo_list()).await;
    let parts = builder
        .content(Some(content_settings(store.path())))
        .build()
        .await
        .expect("the deployment assembles");
    // The tampered file's only chunk changes on disk after the boot reconcile.
    std::fs::write(
        chunk_path(store.path(), blake3::hash(b"original bytes").as_bytes()),
        b"tampered bytes",
    )
    .expect("tamper with the chunk");
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

    let mut entries = unzip(&sink.bytes);
    let manifest: serde_json::Value = serde_json::from_slice(
        &entries
            .remove("manifest.json")
            .expect("the archive holds manifest.json"),
    )
    .expect("the manifest is JSON");
    let data = entries
        .remove("data.sqlite")
        .expect("the archive holds data.sqlite");
    assert_eq!(
        entries,
        BTreeMap::from([("files/empty.txt".to_owned(), Vec::new())]),
        "the empty file is the only one produced"
    );
    assert_eq!(
        manifest["files"],
        json!([{"file_id": hex(&empty), "path": "empty.txt"}])
    );
    let mut missing: Vec<(String, String)> = manifest["missing_files"]
        .as_array()
        .expect("the manifest lists the missing files")
        .iter()
        .map(|file| {
            (
                file["file_id"].as_str().expect("an id").to_owned(),
                file["reason"].as_str().expect("a reason").to_owned(),
            )
        })
        .collect();
    missing.sort_unstable();
    let mut expected = vec![
        (hex(&short), "not_a_file_id".to_owned()),
        (hex(&tampered), "corrupt".to_owned()),
    ];
    expected.sort_unstable();
    assert_eq!(
        missing, expected,
        "the short cell and the tampered file are listed, the row naming no file is not"
    );

    let (mut db, _file) = archive_db(&data);
    let ids: Vec<IdRow> = diesel::RunQueryDsl::load(
        diesel::sql_query("SELECT id FROM media ORDER BY id"),
        &mut db,
    )
    .expect("read the media rows");
    assert_eq!(
        ids.into_iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "every row of alice's stays in the table, whatever its file"
    );
}

#[tokio::test]
async fn a_chunk_lost_inside_a_file_fails_the_download() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    apply_content_deployment(&pool).await;
    let store = TempDir::new().expect("a store directory");
    let torn = seed_file(
        &pool,
        store.path(),
        "alice",
        &[b"the first half, ", b"the second"],
    )
    .await;
    seed_media(&pool, 1, "alice", &torn, Some("torn.txt")).await;

    let (builder, _idp, _keys) = portability_builder(&fixture, photo_list()).await;
    let parts = builder
        .content(Some(content_settings(store.path())))
        .build()
        .await
        .expect("the deployment assembles");
    std::fs::remove_file(chunk_path(
        store.path(),
        blake3::hash(b"the second").as_bytes(),
    ))
    .expect("lose the second chunk");
    let mut sink = BytesSink::default();
    let refused = parts
        .handle
        .download_data(
            "alice".to_owned(),
            &DataRequester::Operator("staff:marco".to_owned()),
            &mut sink,
        )
        .await;
    assert!(
        matches!(&refused, Err(DataRequestError::FileBytes { file, .. }) if *file == hex(&torn)),
        "a file whose entry began cannot be taken back, so the download fails: {refused:?}"
    );
    assert_eq!(
        audit_rows(&pool).await,
        Vec::new(),
        "a failed download leaves no audit row"
    );
}

/// The sign-out an erasure runs first revokes the person's live sessions,
/// leaves the other people's live, records one row per revocation, and a
/// second call revokes nothing.
#[tokio::test]
async fn end_person_revokes_the_persons_sessions_and_not_the_others() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let alice_one = live_session(&pool, "alice").await;
    let alice_two = live_session(&pool, "alice").await;
    let bob = live_session(&pool, "bob").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let ended = parts
        .handle
        .end_person(&"alice".to_owned())
        .await
        .expect("the sign-out finishes");
    assert_eq!(
        ended,
        PersonEnded {
            sessions: 2,
            devices: 0
        },
        "both of alice's live sessions are revoked and nothing else is touched"
    );
    assert!(
        session_revoked(&pool, alice_one).await,
        "alice's first session is revoked"
    );
    assert!(
        session_revoked(&pool, alice_two).await,
        "alice's second session is revoked"
    );
    assert!(
        !session_revoked(&pool, bob).await,
        "bob's session stays live"
    );

    // The audit writes are spawned, so they land shortly after the call.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut rows = revoked_sessions(&pool).await;
    while rows.len() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        rows = revoked_sessions(&pool).await;
    }
    // Spawned writes land in no fixed order, so the rows compare as a set.
    rows.sort_unstable();
    let mut expected = [*alice_one.as_uuid(), *alice_two.as_uuid()];
    expected.sort_unstable();
    assert_eq!(
        rows, expected,
        "one session_revoked row per revoked session, no other"
    );

    let again = parts
        .handle
        .end_person(&"alice".to_owned())
        .await
        .expect("the second sign-out finishes");
    assert_eq!(
        again,
        PersonEnded {
            sessions: 0,
            devices: 0
        },
        "the second sign-out revokes nothing"
    );
}

/// The sign-out closes the person's live connection with `SessionRevoked`, the
/// revocation hook's close, before the next handshake would refuse it.
#[tokio::test]
async fn end_person_closes_the_persons_live_connection() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let base = format!("http://127.0.0.1:{port}");
    let (builder, _idp, _keys) = portability_builder_base(&fixture, record_list(), &base).await;
    let parts = builder.build().await.expect("the deployment assembles");
    let stream = tokio::spawn(parts.change_stream);
    let http = tokio::spawn(async move { axum::serve(listener, parts.router).await });
    wait_ready(&base).await;

    let (access, user_id, _refresh) = mint_tokens(&base, "alice").await;
    let mut client = super::lifecycle::live_session(&format!("127.0.0.1:{port}"), &access).await;
    let ended = parts
        .handle
        .end_person(&user_id)
        .await
        .expect("the sign-out finishes");
    assert_eq!(ended.sessions, 1, "the login's session is the one revoked");

    let last = loop {
        if let fatal @ ControlMessage::FatalError(_) = next_control(&mut client).await {
            break fatal;
        }
    };
    match last {
        ControlMessage::FatalError(FatalError {
            reason: FatalErrorReason::SessionRevoked,
        }) => {}
        other => panic!("the session's last frame says {other:?}"),
    }
    stream.abort();
    http.abort();
}

/// The sign-out revokes the person's enrolled device keys, leaves the other
/// people's, and counts what it revoked.
#[tokio::test]
async fn end_person_revokes_the_persons_device_keys() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    connetto_test_harness::stack::provision_enrolment_tables(&fixture).await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let base = format!("http://127.0.0.1:{port}");
    let (builder, _idp, _keys) = portability_builder_base(&fixture, record_list(), &base).await;
    let (_, _, issuer) = rooted_issuer();
    let parts = builder
        .device_identity(Some(DeviceCertConfig::new(issuer)))
        .build()
        .await
        .expect("the deployment with device identity assembles");
    let stream = tokio::spawn(parts.change_stream);
    let http = tokio::spawn(async move { axum::serve(listener, parts.router).await });
    wait_ready(&base).await;

    let (alice_id, alice_key) = enrol_device(port, "alice").await;
    let (_bob_id, bob_key) = enrol_device(port, "bob").await;

    let ended = parts
        .handle
        .end_person(&alice_id)
        .await
        .expect("the sign-out finishes");
    assert_eq!(
        ended,
        PersonEnded {
            sessions: 1,
            devices: 1
        },
        "alice's session and key are revoked, bob's are not"
    );
    assert!(
        device_revoked(&pool, alice_key).await,
        "alice's enrolment ends revoked"
    );
    assert!(
        !device_revoked(&pool, bob_key).await,
        "bob's enrolment stays"
    );
    stream.abort();
    http.abort();
}

/// A sign-out whose session store cannot answer fails rather than report
/// that the person had no session.
#[tokio::test]
async fn end_person_fails_when_the_session_store_cannot_answer() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let parts = builder.build().await.expect("the deployment assembles");
    exec(&pool, "DROP TABLE connetto_sessions CASCADE").await;
    let err = parts
        .handle
        .end_person(&"alice".to_owned())
        .await
        .expect_err("without the session store nothing can be revoked");
    assert!(
        matches!(err, SessionsError::Auth(_)),
        "the failure is the store's: {err}"
    );
}

/// A sign-out whose enrolment table cannot be read fails rather than report
/// that the person enrolled no device.
#[tokio::test]
async fn end_person_fails_when_the_enrolments_cannot_be_read() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_portability_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    connetto_test_harness::stack::provision_enrolment_tables(&fixture).await;
    let session = live_session(&pool, "alice").await;

    let (builder, _idp, _keys) = portability_builder(&fixture, record_list()).await;
    let (_, _, issuer) = rooted_issuer();
    let parts = builder
        .device_identity(Some(DeviceCertConfig::new(issuer)))
        .build()
        .await
        .expect("the deployment with device identity assembles");
    exec(&pool, "DROP TABLE connetto_device_enrolments CASCADE").await;
    let err = parts
        .handle
        .end_person(&"alice".to_owned())
        .await
        .expect_err("without the enrolments the devices cannot be listed");
    assert!(
        matches!(err, SessionsError::Devices(_)),
        "the failure is the enrolment table's: {err}"
    );
    assert!(
        session_revoked(&pool, session).await,
        "the sessions, revoked first, stay revoked"
    );
}
