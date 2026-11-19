//! Tickets bound to the login session that minted them (R109): every
//! ticketed request asks whether the ticket's session is live, a session that
//! ended answers like an invisible file, a store that cannot answer fails the
//! request retryably, and a ticket minted with no login is never asked about.

use std::sync::Arc;

use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use connetto_core::ContentCaller;
use connetto_file_core::{ChunkStore, FileId, Manifest, MemStore, MimeClass, process_file};
use connetto_file_server::TicketSigner;
use connetto_file_server::ticket::{TicketPayload, Verb};
use tower::ServiceExt;

use crate::fixture::{
    Pg, Sessions, build_router_with_sessions, connect_admin, fs_store, identified,
    insert_committed_manifest, keyed, register_file_ownership, session_of,
};

/// The bytes every test here uploads or serves.
const DATA: &[u8] = b"a photograph whose tickets end with the login";

/// A ticket for `verb` on `file_id`, minted for `caller`.
fn ticket(signer: &TicketSigner, file_id: &FileId, verb: Verb, caller: ContentCaller) -> String {
    signer
        .mint(&TicketPayload {
            file_id: *file_id.as_bytes(),
            verb,
            ceiling: u64::try_from(DATA.len()).expect("small") + 1024,
            expiry: chrono::Utc::now().timestamp() + 3600,
            caller,
            carry: None,
        })
        .expect("the ticket mints")
}

async fn send(app: &axum::Router, request: Request<axum::body::Body>) -> Response {
    app.clone()
        .oneshot(request)
        .await
        .expect("the router answers")
}

fn read(file_id: &FileId, token: &str) -> Request<axum::body::Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/files/{file_id}?t={token}"))
        .body(axum::body::Body::empty())
        .expect("a request")
}

fn intent(manifest: &Manifest, token: &str) -> Request<axum::body::Body> {
    let chunks: Vec<serde_json::Value> = manifest
        .chunks()
        .iter()
        .map(|chunk| serde_json::json!({ "hash": format!("{}", chunk.hash), "len": chunk.len }))
        .collect();
    let body = serde_json::json!({ "total_len": DATA.len(), "chunks": chunks });
    Request::builder()
        .method("POST")
        .uri(format!("/files/{}/intent?t={token}", manifest.file_id()))
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            serde_json::to_vec(&body).expect("json"),
        ))
        .expect("a request")
}

/// Every chunk `PUT` of `manifest`, answering the first status that is not
/// `204`, or `204` when all were stored.
async fn put_chunks(
    app: &axum::Router,
    mem: &MemStore,
    manifest: &Manifest,
    token: &str,
) -> StatusCode {
    for chunk in manifest.chunks() {
        let bytes = mem.read_chunk(&chunk.hash).await.expect("the chunk");
        let status = send(
            app,
            Request::builder()
                .method("PUT")
                .uri(format!("/chunks/{}?t={token}", chunk.hash))
                .header("content-type", "application/octet-stream")
                .body(axum::body::Body::from(bytes))
                .expect("a request"),
        )
        .await
        .status();
        if status != StatusCode::NO_CONTENT {
            return status;
        }
    }
    StatusCode::NO_CONTENT
}

fn commit(manifest: &Manifest, token: &str) -> Request<axum::body::Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/files/{}/commit?t={token}", manifest.file_id()))
        .body(axum::body::Body::empty())
        .expect("a request")
}

/// `DATA` committed for alice, its one chunk in the store under `dir`.
async fn committed_for_alice(pg: &Pg, dir: &tempfile::TempDir) -> FileId {
    let mem = MemStore::new();
    let manifest = process_file(DATA, MimeClass::Generic, &mem)
        .await
        .expect("the file splits");
    let store = connetto_file_server::FsStore::new(dir.path()).expect("a store");
    for chunk in manifest.chunks() {
        let bytes = mem.read_chunk(&chunk.hash).await.expect("the chunk");
        store
            .write_chunk(&chunk.hash, &bytes)
            .await
            .expect("store the chunk");
    }
    let file_id = manifest.file_id();
    let mut conn = connect_admin(&pg.url_admin).await;
    insert_committed_manifest(&mut conn, &file_id, &identified("alice"), manifest.chunks()).await;
    register_file_ownership(&mut conn, &file_id, "alice").await;
    file_id
}

#[tokio::test]
async fn a_read_ticket_ends_with_its_login_session() {
    let pg = Pg::start().await;
    let dir = tempfile::TempDir::new().expect("a temp dir");
    let file_id = committed_for_alice(&pg, &dir).await;
    let sessions = Arc::new(Sessions::default());
    let (app, signer) =
        build_router_with_sessions(&pg, fs_store(&dir), Arc::clone(&sessions)).await;
    let token = ticket(&signer, &file_id, Verb::Read, identified("alice"));

    let served = send(&app, read(&file_id, &token)).await;
    assert_eq!(served.status(), StatusCode::OK, "the live session reads");
    assert_eq!(
        served
            .headers()
            .get(header::CACHE_CONTROL)
            .expect("a read names its caching"),
        "private, max-age=31536000, immutable",
        "only the reader's own cache may keep bytes served under a ticket"
    );

    sessions.end(session_of("alice"));
    assert_eq!(
        send(&app, read(&file_id, &token)).await.status(),
        StatusCode::NOT_FOUND,
        "the same ticket reads like an invisible file once its session ended"
    );
}

#[tokio::test]
async fn a_ticket_minted_with_no_login_is_never_asked_about() {
    const SUBJECT: &str = "key:k1";

    let pg = Pg::start_with_subjects().await;
    let dir = tempfile::TempDir::new().expect("a temp dir");
    let mem = MemStore::new();
    let manifest = process_file(DATA, MimeClass::Generic, &mem)
        .await
        .expect("the file splits");
    let store = connetto_file_server::FsStore::new(dir.path()).expect("a store");
    for chunk in manifest.chunks() {
        let bytes = mem.read_chunk(&chunk.hash).await.expect("the chunk");
        store
            .write_chunk(&chunk.hash, &bytes)
            .await
            .expect("store the chunk");
    }
    let file_id = manifest.file_id();
    let mut conn = connect_admin(&pg.url_admin).await;
    insert_committed_manifest(&mut conn, &file_id, &keyed([SUBJECT]), manifest.chunks()).await;
    register_file_ownership(&mut conn, &file_id, SUBJECT).await;
    drop(conn);
    let sessions = Arc::new(Sessions::default());
    sessions.fail();
    let (app, signer) =
        build_router_with_sessions(&pg, fs_store(&dir), Arc::clone(&sessions)).await;
    let token = ticket(&signer, &file_id, Verb::Read, keyed([SUBJECT]));

    assert_eq!(
        send(&app, read(&file_id, &token)).await.status(),
        StatusCode::OK,
        "a share key's ticket reads while the session store is down"
    );
    assert_eq!(sessions.asked(), 0, "and the store was never asked");
}

#[tokio::test]
async fn a_store_that_cannot_answer_fails_the_request_retryably() {
    let pg = Pg::start().await;
    let dir = tempfile::TempDir::new().expect("a temp dir");
    let file_id = committed_for_alice(&pg, &dir).await;
    let sessions = Arc::new(Sessions::default());
    sessions.fail();
    let (app, signer) =
        build_router_with_sessions(&pg, fs_store(&dir), Arc::clone(&sessions)).await;

    let token = ticket(&signer, &file_id, Verb::Read, identified("alice"));
    assert_eq!(
        send(&app, read(&file_id, &token)).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a read the store cannot answer for fails like a database failure"
    );

    let mem = MemStore::new();
    let manifest = process_file(b"another photograph", MimeClass::Generic, &mem)
        .await
        .expect("the file splits");
    let token = ticket(
        &signer,
        &manifest.file_id(),
        Verb::Write,
        identified("alice"),
    );
    assert_eq!(
        send(&app, intent(&manifest, &token)).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "and so does an upload"
    );
}

#[tokio::test]
async fn every_upload_stage_ends_with_its_login_session() {
    let pg = Pg::start().await;
    let dir = tempfile::TempDir::new().expect("a temp dir");
    let sessions = Arc::new(Sessions::default());
    let (app, signer) =
        build_router_with_sessions(&pg, fs_store(&dir), Arc::clone(&sessions)).await;
    let mem = MemStore::new();
    let manifest = process_file(DATA, MimeClass::Generic, &mem)
        .await
        .expect("the file splits");
    let file_id = manifest.file_id();

    // Each uploader's session ends just before one stage.
    let at_intent = ticket(&signer, &file_id, Verb::Write, identified("ann"));
    sessions.end(session_of("ann"));
    assert_eq!(
        send(&app, intent(&manifest, &at_intent)).await.status(),
        StatusCode::NOT_FOUND,
        "the intent of an ended session reads like an invisible file"
    );

    let at_chunk = ticket(&signer, &file_id, Verb::Write, identified("ben"));
    assert_eq!(
        send(&app, intent(&manifest, &at_chunk)).await.status(),
        StatusCode::OK,
        "the live session declares"
    );
    sessions.end(session_of("ben"));
    assert_eq!(
        put_chunks(&app, &mem, &manifest, &at_chunk).await,
        StatusCode::NOT_FOUND,
        "a chunk of an ended session is refused"
    );

    let at_commit = ticket(&signer, &file_id, Verb::Write, identified("cat"));
    assert_eq!(
        send(&app, intent(&manifest, &at_commit)).await.status(),
        StatusCode::OK,
        "the live session declares"
    );
    assert_eq!(
        put_chunks(&app, &mem, &manifest, &at_commit).await,
        StatusCode::NO_CONTENT,
        "and stores its chunks"
    );
    sessions.end(session_of("cat"));
    assert_eq!(
        send(&app, commit(&manifest, &at_commit)).await.status(),
        StatusCode::NOT_FOUND,
        "the commit of an ended session is refused"
    );

    // The same upload goes through under the next login's ticket.
    let next_login = ContentCaller::new(
        Some("cat".to_owned()),
        Some(connetto_core::SessionId::from_token_hash(
            "cat's next login",
        )),
        None,
        connetto_core::BOT_LIST_WILDCARD.to_owned(),
    );
    let next = ticket(&signer, &file_id, Verb::Write, next_login);
    assert_eq!(
        send(&app, intent(&manifest, &next)).await.status(),
        StatusCode::OK,
        "the next login declares"
    );
    assert_eq!(
        put_chunks(&app, &mem, &manifest, &next).await,
        StatusCode::NO_CONTENT,
        "stores"
    );
    assert_eq!(
        send(&app, commit(&manifest, &next)).await.status(),
        StatusCode::OK,
        "and commits"
    );
}
