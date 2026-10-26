//! The phase's acceptance: the offline photo case, end to end and native.
//!
//! One device writes an entry and its bytes with nothing listening. It
//! reconnects, and both arrive: the row over the sync path, the bytes over the
//! content channel under a websocket-minted write ticket. A second device sees
//! `content_state` flip on its own replica and fetches the bytes by ranged
//! `GET` under a read ticket.
//!
//! Needs Docker: the fixture starts its own Postgres, and the file server runs
//! beside it on a real socket.

use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::sync::Arc;

use super::photo_fixture::{
    PHOTO, attach_content, device, photos, provision, spawn_sync_server, stage_photo,
    start_file_server, wait_for_pg_state, wait_for_upload,
};
use connetto_client::live::ConnettoClient;
use connetto_client::reconnect::TokioSleeper;
use connetto_core::transport::LoopbackTransport;
use connetto_file_client::{ContentClient, FsStore, ReqwestHttp, Resolved};
use connetto_file_core::FileId;
use connetto_file_server::{DefaultFileSchema, FsStore as ServerStore};
use connetto_test_harness::{Fixture, Server};
use diesel::prelude::*;
use tempfile::tempdir;

/// The whole case, in one test because each half is the other's premise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker: the fixture starts its own Postgres"]
async fn a_photo_written_offline_arrives_and_a_second_device_fetches_it() {
    let fixture = Fixture::acquire().await;
    provision(&fixture).await;
    let chunk_dir = tempdir().expect("temp dir");
    let (base_url, signer, files) = start_file_server(&fixture, chunk_dir.path()).await;

    // The sync server, with that signer wired into its session loop.
    let server = Arc::new(spawn_sync_server(&fixture, signer).await);

    // Device A: replica opened with no transport, so the entry and its bytes
    // are written while the gate is closed.
    let replica_dir = tempdir().expect("temp dir");
    let (a, gate) = offline_device_a(replica_dir.path(), &server).await;
    let a_content = attach_content(a, chunk_dir.path()).await;
    let file_id = stage_offline_photo(&a_content).await;

    // Reconnect: the row goes up as an ordinary mutation, the bytes go up
    // through the outbox walk, and the walk has to wait for the row: a write
    // ticket is refused for a file the deployment cannot yet see, and what
    // makes it visible is the mutation landing.
    gate.store(true, Ordering::Relaxed);
    let uploaded = wait_for_upload(&a_content).await;
    assert_eq!(uploaded, 1, "the outbox walk sent the one file waiting");

    // Postgres now holds the row, the manifest, and the flipped state.
    let state = wait_for_state(&fixture, file_id.as_bytes()).await;
    assert_eq!(
        state.as_deref(),
        Some("available"),
        "the commit called the deployment's setter, which is the availability signal"
    );

    // Device B, a second device, sees the row and the flipped state on its
    // own replica through an ordinary subscription.
    let (b, b_content) = setup_device_b(replica_dir.path(), &server, chunk_dir.path()).await;
    b.pin("photos", "SELECT * FROM photos")
        .await
        .expect("device B declares a durable interest in the photos");
    let seen = wait_for_replica_state(&b, file_id.as_bytes()).await;
    assert_eq!(
        seen.as_deref(),
        Some("available"),
        "device B learns the bytes are fetchable from the row, not from the content channel"
    );
    assert_device_b_fetches_photo(&b_content, &base_url, file_id).await;

    files.abort();
}

/// R70 decision 13: a file the server's boot reconcile marked lost comes back
/// from the device that still holds it, with no call from the application
/// beyond registering where `lost` shows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker: the fixture starts its own Postgres"]
async fn a_device_holding_a_lost_photo_uploads_it_again() {
    let fixture = Fixture::acquire().await;
    provision(&fixture).await;
    let chunk_dir = tempdir().expect("temp dir");
    let (_, signer, files) = start_file_server(&fixture, chunk_dir.path()).await;
    let server = Arc::new(spawn_sync_server(&fixture, signer).await);
    let replica_dir = tempdir().expect("temp dir");
    let (a, gate) = offline_device_a(replica_dir.path(), &server).await;
    let a_content = attach_content(a.clone(), chunk_dir.path())
        .await
        .heal_lost(
            "SELECT content_id FROM photos WHERE content_state = 'lost'",
            "content_id",
        )
        .await
        .expect("the query returns the column it names");
    let file_id = stage_offline_photo(&a_content).await;
    gate.store(true, Ordering::Relaxed);
    a.pin("photos", "SELECT * FROM photos")
        .await
        .expect("device A follows its own photos, which is how it hears of the loss");
    wait_for_upload(&a_content).await;
    assert_eq!(
        wait_for_state(&fixture, file_id.as_bytes())
            .await
            .as_deref(),
        Some("available")
    );

    // What a restore leaves after the sweep removed the bytes past the backup.
    let server_chunks = chunk_dir.path().join("server");
    std::fs::remove_dir_all(&server_chunks).expect("lose the server's bytes");
    let reconciled = connetto_file_server::reconcile_store::<DefaultFileSchema>(
        fixture.admin(),
        &connetto_file_server::AnyStore::Fs(ServerStore::new(&server_chunks).expect("store")),
        &connetto_file_server::CallerSettings::default(),
        Duration::from_secs(600),
    )
    .await
    .expect("the boot reconcile runs");
    assert_eq!(reconciled.lost, vec![file_id]);

    let driver = {
        let a_content = Arc::new(a_content);
        let driving = Arc::clone(&a_content);
        tokio::spawn(async move { driving.drive_outbox(TokioSleeper).await });
        a_content
    };
    wait_for_pg_state(&fixture, file_id.as_bytes(), "available").await;
    assert_eq!(
        driver.bytes(file_id).await.expect("read").as_deref(),
        Some(PHOTO),
        "the photo is whole again"
    );
    files.abort();
}

/// Stages the test photograph on device A and asserts the bytes resolve
/// from the local chunk store.
async fn stage_offline_photo(
    a_content: &ContentClient<LoopbackTransport, FsStore, ReqwestHttp>,
) -> FileId {
    let file_id = stage_photo(a_content).await;
    assert!(
        matches!(
            a_content.resolve(file_id).await.expect("resolve offline"),
            Resolved::Local { .. }
        ),
        "the bytes are readable on the device that authored them, with no server"
    );
    file_id
}

/// Connects device B to the sync server and attaches a fresh content client.
///
/// Returns both so the test can pin a subscription and then poll replica state.
async fn setup_device_b(
    replica_root: &std::path::Path,
    server: &Arc<Server>,
    chunk_dir: &std::path::Path,
) -> (
    ConnettoClient<LoopbackTransport>,
    ContentClient<LoopbackTransport, FsStore, ReqwestHttp>,
) {
    let b = connect_device_b(replica_root, server).await;
    let b_content = ContentClient::attach(
        b.clone(),
        FsStore::new(chunk_dir.join("device-b")),
        [4; 32],
        ReqwestHttp::new(),
    )
    .await
    .expect("attach content handling to device B");
    (b, b_content)
}

/// Resolves the photograph on device B via a signed URL, performs a ranged
/// GET and checks the partial-content status and the first sixteen bytes,
/// then downloads the whole file and checks it is byte-identical to the
/// original.
async fn assert_device_b_fetches_photo(
    b_content: &ContentClient<LoopbackTransport, FsStore, ReqwestHttp>,
    base_url: &str,
    file_id: FileId,
) {
    let resolved = b_content
        .resolve(file_id)
        .await
        .expect("resolve on device B");
    let Resolved::Remote { url } = resolved else {
        panic!("device B holds no local bytes, so the answer is a signed URL, got {resolved:?}");
    };
    assert!(
        url.starts_with(base_url),
        "the granted URL addresses the file server, got {url}"
    );
    let ranged = reqwest::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-15")
        .send()
        .await
        .expect("ranged request");
    assert_eq!(
        ranged.status().as_u16(),
        206,
        "a range is served as a partial response"
    );
    let head = ranged.bytes().await.expect("ranged body");
    assert_eq!(
        head.as_ref(),
        &PHOTO[..16],
        "the range is the first sixteen bytes of the photograph"
    );
    let whole = b_content
        .bytes(file_id)
        .await
        .expect("download the whole file")
        .expect("the server holds it");
    assert_eq!(whole, PHOTO, "device B reads the photograph device A wrote");
}

/// Opens device A's replica with no transport at all and drives it with the
/// reconnect pump behind a gate.
///
/// The gate is what makes the offline half genuine: the factory refuses to
/// produce a transport until the test opens it, so the device is offline in
/// exactly the way a device with no network is, and the reconnect is the
/// production reconnect path rather than a hand-placed attach.
async fn offline_device_a(
    replica_root: &std::path::Path,
    server: &Arc<Server>,
) -> (ConnettoClient<LoopbackTransport>, Arc<AtomicBool>) {
    let gate = Arc::new(AtomicBool::new(false));
    let factory_gate = Arc::clone(&gate);
    let factory_server = Arc::clone(server);
    let client = device(
        &replica_root.join("a.sqlite"),
        "user:alice#writer",
        move || {
            let gate = Arc::clone(&factory_gate);
            let server = Arc::clone(&factory_server);
            async move {
                if gate.load(Ordering::Relaxed) {
                    Ok(server.attach())
                } else {
                    Err("device A has no server yet")
                }
            }
        },
    )
    .await;
    let connected = client
        .with_conn(|conn| conn.is_connected())
        .await
        .expect("the gate is off");
    assert!(
        !connected,
        "device A is offline, which is the premise of the whole case"
    );
    (client, gate)
}

/// Connects the second device, which was never offline and holds no bytes.
async fn connect_device_b(
    replica_root: &std::path::Path,
    server: &Arc<Server>,
) -> ConnettoClient<LoopbackTransport> {
    let server = Arc::clone(server);
    device(
        &replica_root.join("b.sqlite"),
        "user:alice#reader",
        move || {
            let server = Arc::clone(&server);
            async move { Ok(server.attach()) }
        },
    )
    .await
}

/// Reads `content_state` out of Postgres until the commit has flipped it.
async fn wait_for_state(fixture: &Fixture, file_id: &[u8; 32]) -> Option<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    // One connection for the whole wait: taking a fresh one per turn starves
    // the pool the server is also using.
    let mut conn = fixture.admin().get().await.expect("admin connection");
    loop {
        let query = diesel::sql_query("SELECT content_state FROM photos WHERE content_id = $1")
            .bind::<diesel::sql_types::Bytea, _>(file_id.to_vec());
        let rows: Vec<Option<String>> =
            diesel_async::RunQueryDsl::load::<StateRow>(query, &mut *conn)
                .await
                .expect("read the state")
                .into_iter()
                .map(|row| row.content_state)
                .collect();
        if let Some(state) = rows.into_iter().next().flatten() {
            return Some(state);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "content_state never flipped in Postgres"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// One `content_state` cell.
#[derive(diesel::QueryableByName)]
struct StateRow {
    /// Availability of the bytes.
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    content_state: Option<String>,
}

/// Reads `content_state` off a device's own replica until sync delivers it.
async fn wait_for_replica_state(
    client: &ConnettoClient<LoopbackTransport>,
    file_id: &[u8; 32],
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let wanted = file_id.to_vec();
    loop {
        let state = client
            .with_conn(|conn| {
                diesel::RunQueryDsl::load::<Option<String>>(
                    photos::table
                        .filter(photos::content_id.eq(wanted.clone()))
                        .select(photos::content_state),
                    conn.conn(),
                )
                .expect("read the replica")
            })
            .await
            .expect("gate not locked");
        if let Some(state) = state.into_iter().next().flatten() {
            return Some(state);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the state never reached the second device's replica"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
