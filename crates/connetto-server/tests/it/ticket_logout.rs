//! Content tickets end with the login session that minted them (R109),
//! proved against the assembled deployment: the auth store, the session
//! loop's mint and the file routes the builder mounts beside the login
//! endpoints.

use std::time::Duration;

use connetto_core::messages::{
    CONTENT_TICKET_REFUSED, ContentTicketRequest, ContentVerb, ControlMessage, NonFatalError,
};
use connetto_core::traits::Transport;
use connetto_file_core::{ChunkStore, Manifest, MemStore, MimeClass, process_file};
use connetto_server::WebSocketTransport;
use connetto_server::builder::{ContentSettings, StoreSpec};
use connetto_test_harness::server_fixture::{PG_SERIAL, exec, mint_tokens, reset_fixture};
use connetto_test_harness::{Fixture, isolated_session_keyring};
use openidconnect::reqwest;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};

use super::builder_coverage::apply_content_deployment;
use super::lifecycle::{admin_pool, builder_over, live_session_as, next_control, wait_ready};

/// The photograph every case uploads and reads.
const DATA: &[u8] = b"a photograph whose tickets end with the login that minted them";

/// The URL of a ticket `client` asks for, or the refusal detail it was answered.
async fn ticket(
    client: &mut WebSocketTransport<TcpStream>,
    manifest: &Manifest,
    verb: ContentVerb,
) -> Result<String, String> {
    client
        .send_control(ControlMessage::ContentTicketRequest(ContentTicketRequest {
            request_id: "r109".to_owned(),
            file_id: *manifest.file_id().as_bytes(),
            verb,
            for_owner: None,
        }))
        .await
        .expect("send the ticket request");
    loop {
        match next_control(client).await {
            ControlMessage::ContentTicketGrant(grant) => return Ok(grant.url),
            ControlMessage::NonFatalError(NonFatalError { detail, .. }) => return Err(detail),
            _ => {}
        }
    }
}

/// The byte length `manifest` declares.
fn total_len(manifest: &Manifest) -> u64 {
    manifest.chunks().iter().map(|chunk| chunk.len).sum()
}

/// The write verb for `manifest`.
fn write_verb(manifest: &Manifest) -> ContentVerb {
    ContentVerb::Write {
        declared_len: total_len(manifest),
    }
}

/// The status the intent at `url` answers for `manifest`.
async fn intent(agent: &reqwest::Client, url: &str, manifest: &Manifest) -> u16 {
    let chunks: Vec<serde_json::Value> = manifest
        .chunks()
        .iter()
        .map(|chunk| json!({ "hash": format!("{}", chunk.hash), "len": chunk.len }))
        .collect();
    agent
        .post(url)
        .json(&json!({ "total_len": total_len(manifest), "chunks": chunks }))
        .send()
        .await
        .expect("POST the intent")
        .status()
        .as_u16()
}

/// Upload `manifest`'s chunks and commit it under the write ticket whose
/// intent URL is `url`, which already declared it.
async fn finish_upload(
    agent: &reqwest::Client,
    base: &str,
    url: &str,
    mem: &MemStore,
    manifest: &Manifest,
) {
    let token = url.split_once("?t=").expect("a ticket query").1;
    for chunk in manifest.chunks() {
        let bytes = mem.read_chunk(&chunk.hash).await.expect("the chunk");
        let status = agent
            .put(format!("{base}/chunks/{}?t={token}", chunk.hash))
            .header("content-type", "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .expect("PUT the chunk")
            .status();
        assert_eq!(status.as_u16(), 204, "the chunk is stored");
    }
    let status = agent
        .post(format!(
            "{base}/files/{}/commit?t={token}",
            manifest.file_id()
        ))
        .send()
        .await
        .expect("POST the commit")
        .status();
    assert_eq!(status.as_u16(), 200, "the upload commits");
}

/// The status and caching a read at `url` answers.
async fn read(agent: &reqwest::Client, url: &str) -> (u16, Option<String>) {
    let response = agent.get(url).send().await.expect("GET the file");
    let caching = response
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .map(|value| value.to_str().expect("ASCII").to_owned());
    (response.status().as_u16(), caching)
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "two logins, the logout and the idled session run as one ordered exchange against one deployment"
)]
async fn a_logout_ends_its_tickets_and_spares_another_login() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    apply_content_deployment(&pool).await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let base = format!("http://127.0.0.1:{port}");
    let addr = format!("127.0.0.1:{port}");
    let dir = TempDir::new().expect("a store directory");
    let key = ring::signature::Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new())
        .expect("a ticket key")
        .as_ref()
        .to_vec();
    let settings = ContentSettings {
        base_url: base.clone(),
        ttl: Duration::from_secs(600),
        read_ceiling: 1 << 20,
        grace: Duration::from_secs(600),
        cadence: Duration::from_secs(600),
        quota_identity: 0,
        storage_ceiling: 0,
        bandwidth_ceiling: 0,
        bandwidth_window_days: 30,
        warn_fraction: 0.8,
        ceiling_refresh: Duration::from_secs(10),
        owner_pool_size: 2,
        store: StoreSpec::Fs(dir.path().to_path_buf()),
        key,
    };
    let (builder, _idp, _keys) = builder_over(&fixture, port).await;
    let parts = builder
        .content(Some(settings))
        .build()
        .await
        .expect("the deployment assembles");
    let stream = tokio::spawn(parts.change_stream);
    let http = tokio::spawn(async move { axum::serve(listener, parts.router).await });
    wait_ready(&base).await;
    let agent = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("an HTTP client");

    // One person, two logins, each on its own device.
    let (first_access, _, first_refresh) = mint_tokens(&base, "e2e-user").await;
    let (second_access, _, _) = mint_tokens(&base, "e2e-user").await;
    let mut first = live_session_as(&addr, &first_access, "first-device").await;
    let mut second = live_session_as(&addr, &second_access, "second-device").await;

    let mem = MemStore::new();
    let photo = process_file(DATA, MimeClass::Generic, &mem)
        .await
        .expect("the photograph splits");
    let other = process_file(b"a second photograph", MimeClass::Generic, &mem)
        .await
        .expect("the second photograph splits");
    for manifest in [&photo, &other] {
        exec(
            &pool,
            &format!(
                "INSERT INTO photos (content_id) VALUES (decode('{}', 'hex'))",
                manifest.file_id()
            ),
        )
        .await;
    }

    let upload = ticket(&mut first, &photo, write_verb(&photo))
        .await
        .expect("the first login may upload");
    assert_eq!(intent(&agent, &upload, &photo).await, 200, "it declares");
    finish_upload(&agent, &base, &upload, &mem, &photo).await;
    let unused_write = ticket(&mut first, &other, write_verb(&other))
        .await
        .expect("the first login may upload the second photograph");
    let first_read = ticket(&mut first, &photo, ContentVerb::Read)
        .await
        .expect("the first login may read");
    let second_read = ticket(&mut second, &photo, ContentVerb::Read)
        .await
        .expect("the second login may read");
    assert_eq!(
        read(&agent, &first_read).await,
        (200, Some("private, max-age=31536000, immutable".to_owned())),
        "the first login reads, cached for the reader alone"
    );

    let logout = agent
        .post(format!("{base}/auth/logout"))
        .json(&json!({ "refresh_token": first_refresh }))
        .send()
        .await
        .expect("POST /auth/logout");
    assert!(logout.status().is_success(), "logout: {}", logout.status());

    assert_eq!(
        read(&agent, &first_read).await.0,
        404,
        "the logged-out session's ticket reads like an invisible file"
    );
    assert_eq!(
        intent(&agent, &unused_write, &other).await,
        404,
        "and its write ticket is refused at the intent"
    );
    assert_eq!(
        read(&agent, &second_read).await.0,
        200,
        "while the other login of the same person keeps reading"
    );
    let next_write = ticket(&mut second, &other, write_verb(&other))
        .await
        .expect("the live login may upload the refused photograph");
    assert_eq!(
        intent(&agent, &next_write, &other).await,
        200,
        "and the refused upload goes up under it"
    );

    // The second session idles past its window in the store while its
    // connection stays open, so nothing told the connection.
    exec(
        &pool,
        "UPDATE connetto_sessions SET idle_deadline = now() - interval '1 minute' \
         WHERE NOT revoked",
    )
    .await;
    assert_eq!(
        ticket(&mut second, &photo, ContentVerb::Read).await,
        Err(CONTENT_TICKET_REFUSED.to_owned()),
        "an open connection whose session ended is refused a ticket"
    );
    assert_eq!(
        read(&agent, &second_read).await.0,
        404,
        "and the ticket it held stops reading"
    );

    stream.abort();
    http.abort();
}
