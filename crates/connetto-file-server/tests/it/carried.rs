//! A carried upload: a device of another account uploads a file for its
//! owner (R79 decision 17).

use std::sync::Arc;

use parking_lot::Mutex;

use axum::http::StatusCode;
use connetto_core::{ContentCarry, SessionId};
use connetto_file_core::{ChunkStore, FileId, MemStore, MimeClass, process_file};
use connetto_file_server::ticket::{TicketPayload, Verb};
use connetto_file_server::{
    AppPools, CarriedCommit, Config, DefaultFileSchema, TicketSigner, serve,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use tower::ServiceExt;

use crate::fixture::{Pg, connect_admin, fs_store, identified, make_signer};

diesel::table! {
    /// The manifest columns a carried upload writes, named here because the
    /// crate keeps its own schema private.
    _cfs_manifests (file_id, uploaded_by) {
        file_id -> Bytea,
        uploaded_by -> Text,
        carried_by -> Nullable<Text>,
        committed -> Bool,
    }
}

/// A router whose carried commits land in the returned list.
async fn router_hearing_carries(
    pg: &Pg,
    dir: &tempfile::TempDir,
) -> (axum::Router, TicketSigner, Arc<Mutex<Vec<CarriedCommit>>>) {
    let (signer, verifier) = make_signer();
    let heard = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&heard);
    let router = serve(Config::<DefaultFileSchema> {
        pools: AppPools {
            admin: pg.admin_pool().await,
            reader: pg.reader_pool().await,
        },
        store: fs_store(dir),
        verifier,
        grace: std::time::Duration::from_secs(3600),
        caller_settings: connetto_file_server::CallerSettings::default(),
        quotas: connetto_file_server::QuotaSettings::default(),
        ceilings: connetto_file_server::CeilingCache::default(),
        on_carried: Some(Arc::new(move |commit| {
            sink.lock().push(commit);
        })),
        _schema: std::marker::PhantomData,
    })
    .await
    .expect("the fixture passes preflight");
    (router, signer, heard)
}

async fn send(app: &axum::Router, request: axum::http::Request<axum::body::Body>) -> StatusCode {
    app.clone()
        .oneshot(request)
        .await
        .expect("the router answers")
        .status()
}

#[tokio::test]
async fn a_carried_upload_is_recorded_for_its_owner_and_heard_once() {
    let pg = Pg::start().await;
    let dir = tempfile::TempDir::new().expect("a temp dir");
    let (app, signer, heard) = router_hearing_carries(&pg, &dir).await;

    let data = b"a photograph bob carries for alice";
    let mem = MemStore::new();
    let manifest = process_file(data, MimeClass::Generic, &mem)
        .await
        .expect("the file splits");
    let file_id: FileId = manifest.file_id();
    let file_hex = format!("{file_id}");
    let session = SessionId::from_token_hash("bob's session");
    let carry = ContentCarry {
        owner: "alice".to_owned(),
        session,
    };
    let ticket = signer
        .mint(&TicketPayload {
            file_id: *file_id.as_bytes(),
            verb: Verb::Write,
            ceiling: u64::try_from(data.len()).expect("small") + 1024,
            expiry: chrono::Utc::now().timestamp() + 3600,
            caller: identified("bob"),
            carry: Some(carry.clone()),
        })
        .expect("the ticket mints");

    let chunks: Vec<serde_json::Value> = manifest
        .chunks()
        .iter()
        .map(|c| serde_json::json!({ "hash": format!("{}", c.hash), "len": c.len }))
        .collect();
    let intent = serde_json::json!({ "total_len": data.len(), "chunks": chunks });
    let status = send(
        &app,
        axum::http::Request::builder()
            .method("POST")
            .uri(format!("/files/{file_hex}/intent?t={ticket}"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                serde_json::to_vec(&intent).expect("json"),
            ))
            .expect("a request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "intent");
    for chunk in manifest.chunks() {
        let bytes = mem.read_chunk(&chunk.hash).await.expect("the chunk");
        let status = send(
            &app,
            axum::http::Request::builder()
                .method("PUT")
                .uri(format!("/chunks/{}?t={ticket}", chunk.hash))
                .header("content-type", "application/octet-stream")
                .body(axum::body::Body::from(bytes))
                .expect("a request"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "chunk PUT");
    }
    let commit = || {
        axum::http::Request::builder()
            .method("POST")
            .uri(format!("/files/{file_hex}/commit?t={ticket}"))
            .body(axum::body::Body::empty())
            .expect("a request")
    };
    assert_eq!(send(&app, commit()).await, StatusCode::OK, "commit");
    assert_eq!(
        send(&app, commit()).await,
        StatusCode::OK,
        "a retried commit is idempotent"
    );

    let mut conn = connect_admin(&pg.url_admin).await;
    let rows: Vec<(String, Option<String>, bool)> = _cfs_manifests::table
        .filter(_cfs_manifests::file_id.eq(file_id.as_bytes().to_vec()))
        .select((
            _cfs_manifests::uploaded_by,
            _cfs_manifests::carried_by,
            _cfs_manifests::committed,
        ))
        .load(&mut conn)
        .await
        .expect("read the manifest");
    let carrier_key = identified("bob")
        .storage_key(',')
        .expect("an identity owns rows");
    assert_eq!(
        rows,
        [(carry.storage_key(), Some(carrier_key), true)],
        "the owner's manifest, naming its carrier, and no manifest of bob's"
    );
    assert_eq!(
        *heard.lock(),
        [CarriedCommit {
            file_id: *file_id.as_bytes(),
            owner: "alice".to_owned(),
            carrier: "bob".to_owned(),
            session,
        }],
        "the durable commit is heard once, the retry not at all"
    );
}
