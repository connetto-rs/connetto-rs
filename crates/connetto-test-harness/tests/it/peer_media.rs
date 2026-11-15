//! R79 slice 8 proof, the camp shape in one process. Three signed-in native
//! clients of different accounts run against the demo deployment the real
//! `connetto-server` binary serves, each with a content client over its
//! replica, and the peer link carries a photo while the server is away.
//!
//! Alice and bob sit in a team the photo is posted to, and carl does not.
//! The server goes away. Alice takes a photo and its thumbnail offline. Bob
//! links her, applies the row, and gets the thumbnail at once and the full
//! image when he opens it. Carl links her and gets nothing. An operator
//! removes alice from the team while all stay offline. When the server is
//! back it refuses her photo, and the retraction sweeps the cached chunks off
//! bob's device.
//!
//! Needs Docker, since the fixture starts its own Postgres and authorization
//! service and the login walks a containerised provider.
#![expect(
    clippy::large_futures,
    reason = "the phase drives three full client lifecycles, its futures are large by design"
)]

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use connetto_client::{ClientEvent, NativeClient, NativeTransport};
use connetto_demo_support::native::{photo_content, stage_photo};
use connetto_file_client::{
    ContentClient, ContentEvent, ContentHandle, FileId, FsStore, MimeClass, ReqwestHttp, Resolved,
};
use connetto_server::authn::identity::deterministic_uuid;
use connetto_test_harness::stack::{
    DEMO_DEPLOYMENT, demo_device_ca, ensure_server_bin, provision, provision_enrolment_tables,
    spawn_server, wait_until_closed,
};
use connetto_test_harness::{MockOauth, isolated_session_keyring};
use diesel::prelude::*;
use tempfile::tempdir;
use tokio::sync::broadcast;

use crate::demo::*;

/// The mark a re-executed child carries, absent when it is the driver.
const PHASE: &str = "R79_PEER_MEDIA_PHASE";

/// The keyring service carl signs in under.
const APP_CARL: &str = "r79-peer-media-carl";

/// The team the photo is posted to.
pub(crate) const TEAM: &str = "T1";

/// The key the content stores encrypt under, one per device in the proof.
pub(crate) const ROOT_KEY: [u8; 32] = [79; 32];

diesel::table! {
    /// The photo rows, the view the client writes through.
    photos (id) {
        /// Primary key the replica mints when the insert omits it.
        id -> Binary,
        /// The order the photo hangs off.
        order_id -> Binary,
        /// Who owns the parent order.
        owner_id -> Text,
        /// The full image.
        content_id -> Binary,
        /// `NULL` until the file server commit writes `available`.
        content_state -> Nullable<Text>,
        /// The thumbnail.
        thumb_id -> Nullable<Binary>,
        /// The team the photo is posted to.
        team_id -> Nullable<Text>,
    }
}

pub(crate) type Content = ContentClient<NativeTransport, FsStore, ReqwestHttp>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_photo_crosses_the_camp_and_its_retraction_sweeps_it() {
    if std::env::var(PHASE).is_ok() {
        run_camp().await;
        return;
    }
    // The builder's `.keyring()` store is detected, and a headless desktop's
    // Secret Service dismisses its unlock prompt, so the camp runs in a child
    // whose keyring auto-detection settles on sealed files.
    let dir = tempdir().expect("a scratch dir");
    let credentials = dir.path().join("credentials");
    let state = dir.path().join("state");
    std::fs::create_dir_all(&credentials).expect("the credentials dir");
    std::fs::create_dir_all(&state).expect("the state dir");
    std::fs::write(credentials.join("connetto.wrap-key"), [1_u8; 32]).expect("the wrap key");
    let output = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "peer_media::a_photo_crosses_the_camp_phase",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("STATE_DIRECTORY")
        .env("CREDENTIALS_DIRECTORY", &credentials)
        .env("STATE_DIRECTORY", &state)
        .env(PHASE, "run")
        .output()
        .expect("spawn the phase");
    assert!(
        output.status.success(),
        "the camp failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The camp itself, the phase the driver re-executes in a child process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the camp driver runs it in a child process"]
async fn a_photo_crosses_the_camp_phase() {
    if std::env::var(PHASE).is_err() {
        return;
    }
    run_camp().await;
}

/// The durable content client attached by the builder.
fn content_for(
    client: &NativeClient<NativeTransport, ContentHandle<NativeTransport>>,
) -> std::sync::Arc<Content> {
    match client.content().expect("the build attached content") {
        ContentHandle::Durable(content) => std::sync::Arc::clone(content),
        ContentHandle::InMemory(_) => panic!("the camp uses durable content"),
    }
}

/// Whether `content` holds a manifest for `file_id`, read without fetching.
pub(crate) async fn holds(content: &Content, file_id: FileId) -> bool {
    content
        .outboard(file_id)
        .await
        .expect("read the bookkeeping")
        .is_some()
}

/// The chunk files under `dir`'s store.
fn chunk_files(dir: &Path) -> usize {
    fn walk(path: &Path) -> usize {
        std::fs::read_dir(path).map_or(0, |entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| {
                    let path = entry.path();
                    if path.is_dir() { walk(&path) } else { 1 }
                })
                .sum()
        })
    }
    std::fs::read_dir(dir)
        .expect("the data directory exists")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with("-content"))
        .map(|entry| walk(&entry.path()))
        .sum()
}

/// Waits for `content` to announce `file_id` fetched, bounded.
pub(crate) async fn fetched(events: &mut broadcast::Receiver<ContentEvent>, file_id: FileId) {
    tokio::time::timeout(BOUND, async {
        loop {
            if let Ok(ContentEvent::Fetched { file_id: got }) = events.recv().await
                && got == file_id
            {
                return;
            }
        }
    })
    .await
    .expect("the file arrives within the bound");
}

/// Alice's order id, the parent her photo names.
pub(crate) async fn order_of<C>(client: &NativeClient<NativeTransport, C>) -> Vec<u8> {
    client
        .client()
        .with_conn(|conn| {
            orders::table
                .select(orders::id)
                .first::<Vec<u8>>(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("alice holds her order")
}

/// The thumbnail id the photo row of `full_id` names.
async fn thumb_of<C>(client: &NativeClient<NativeTransport, C>, full_id: FileId) -> FileId {
    let thumb = client
        .client()
        .with_conn(move |conn| {
            photos::table
                .filter(photos::content_id.eq(full_id.as_bytes().as_slice()))
                .select(photos::thumb_id)
                .first::<Option<Vec<u8>>>(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("alice holds her photo row")
        .expect("the row names a thumbnail");
    FileId::from_bytes(thumb.try_into().expect("a 32-byte file id"))
}

/// A PNG of `width` by `height` pixels that does not compress, `seed`
/// choosing the stream.
fn photo_png(seed: u32, width: u32, height: u32) -> Vec<u8> {
    let pixels = photo_bytes(
        seed,
        3 * usize::try_from(width * height).expect("pixel count"),
    );
    let image = image::RgbImage::from_raw(width, height, pixels).expect("one byte per channel");
    let mut png = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut png, image::ImageFormat::Png)
        .expect("encode the photo");
    png.into_inner()
}

/// Deterministic bytes that do not compress, `seed` choosing the stream.
pub(crate) fn photo_bytes(seed: u32, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len + 4);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// The scenario, one assertion per step.
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per assertion, in the order the photo crosses the camp"
)]
async fn run_camp() {
    let _keyring = isolated_session_keyring();

    let provisioned = provision(&DEMO_DEPLOYMENT, "peer-media", None)
        .await
        .expect("provision");
    provision_enrolment_tables(&provisioned.fixture).await;
    let device_ca = demo_device_ca(SystemTime::now()).expect("the demo CA");
    let root = std::fs::read(&device_ca.root).expect("the demo root");
    let idp = MockOauth::start().await;

    let server_bin = ensure_server_bin().await.expect("the server binary");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let bind = listener.local_addr().expect("address").to_string();
    drop(listener);
    let base = format!("http://{bind}");

    let mut envs = provisioned.server_env(&DEMO_DEPLOYMENT, &bind, &base);
    envs.push((
        "CONNETTO_DEVICE_ROOT".to_owned(),
        device_ca.root.display().to_string(),
    ));
    envs.push((
        "CONNETTO_DEVICE_ISSUER_DIR".to_owned(),
        device_ca.issuer.display().to_string(),
    ));
    envs.extend(idp.env_pairs(PROVIDER, &format!("{base}/auth/callback")));
    let server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server one");

    let dirs = [
        tempdir().expect("alice dir"),
        tempdir().expect("bob dir"),
        tempdir().expect("carl dir"),
    ];
    let [dir_a, dir_b, dir_c] = &dirs;
    let peer_listener: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let addr: SocketAddr = bind.parse().expect("address");

    let issuer = idp.issuer().to_owned();
    let alice_id = deterministic_uuid(&issuer, "alice").to_string();
    let bob_id = deterministic_uuid(&issuer, "bob").to_string();
    seed_roster(&provisioned.fixture, &alice_id, &bob_id).await;

    for (app, subject, dir) in [
        (APP_ALICE, "alice", dir_a.path()),
        (APP_BOB, "bob", dir_b.path()),
    ] {
        prime_watches(
            addr,
            fresh_login(&base, app, subject),
            dir,
            &root,
            peer_listener,
        )
        .await;
    }
    let alice = connect_client_with_content(
        addr,
        last_used(&base, APP_ALICE),
        dir_a.path(),
        &root,
        peer_listener,
        photo_content(),
    )
    .await;
    let bob = connect_client_with_content(
        addr,
        last_used(&base, APP_BOB),
        dir_b.path(),
        &root,
        peer_listener,
        photo_content(),
    )
    .await;
    let carl = connect_client_with_content(
        addr,
        fresh_login(&base, APP_CARL, "carl"),
        dir_c.path(),
        &root,
        peer_listener,
        photo_content(),
    )
    .await;
    assert_eq!(
        wait_session(&alice).await,
        alice_id,
        "alice's id matches the seed"
    );
    assert_eq!(
        wait_session(&bob).await,
        bob_id,
        "bob's id matches the seed"
    );
    let alice_watches = watch_tables(&alice).await;
    let bob_watches = watch_tables(&bob).await;
    for client in [&alice, &bob, &carl] {
        wait_for_certificate(client, BOUND)
            .await
            .expect("each device enrols");
    }
    let all = [
        ("T1".to_owned(), alice_id.clone()),
        ("T1".to_owned(), bob_id.clone()),
        ("T2".to_owned(), alice_id.clone()),
        ("T2".to_owned(), bob_id.clone()),
    ];
    assert!(wait_roster(&alice, &all).await, "alice synced the roster");
    assert!(wait_roster(&bob, &all).await, "bob synced the roster");

    let alice_content = content_for(&alice);
    let bob_content = content_for(&bob);
    let carl_content = content_for(&carl);

    // The server goes away for all three.
    drop(server);
    wait_until_closed(&bind, BOUND).await;

    // Alice takes the photo offline, in the team, through the demo's staging,
    // which makes the thumbnail and posts the row naming both files.
    let full = photo_png(1, 1024, 1024);
    let (full_id, _) = stage_photo(&alice_content, &full, MimeClass::Png, &alice_id, Some(TEAM))
        .await
        .expect("stage the photo");
    let thumb_id = thumb_of(&alice, full_id).await;

    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener");
    let mut bob_files = bob_content.events();

    // Bob links alice and applies her order and photo, and the
    // thumbnail comes with the apply, before he asks for anything.
    bob.link_peer(alice_addr).await.expect("bob links alice");
    fetched(&mut bob_files, thumb_id).await;
    match bob_content
        .resolve(thumb_id)
        .await
        .expect("bob shows the thumbnail")
    {
        Resolved::Local { bytes, .. } => {
            let thumbnail = image::load_from_memory(&bytes).expect("a decodable thumbnail");
            assert_eq!(
                (thumbnail.width(), thumbnail.height()),
                (256, 256),
                "the thumbnail alice made at stage"
            );
        }
        other => panic!("the thumbnail is on bob's device, not {other:?}"),
    }
    assert!(
        !holds(&bob_content, full_id).await,
        "the full image waits until bob opens it"
    );

    // He opens it and the full image comes from alice.
    match bob_content
        .resolve(full_id)
        .await
        .expect("bob opens the photo")
    {
        Resolved::Local { bytes, .. } => {
            assert!(bytes == full, "bob gets the photo alice took");
        }
        other => panic!("the photo reaches bob in camp, not {other:?}"),
    }

    // Carl, outside the team, links alice and gets nothing.
    carl.link_peer(alice_addr).await.expect("carl links alice");
    for file_id in [full_id, thumb_id] {
        assert_eq!(
            carl_content.resolve(file_id).await.expect("carl asks"),
            Resolved::Unavailable,
            "alice refuses a requester outside the team"
        );
        assert!(!holds(&carl_content, file_id).await);
    }

    // The provisional row spares both files through a tidy.
    bob_content.tidy_content().await.expect("bob tidies");
    assert!(
        holds(&bob_content, full_id).await,
        "the full image is spared"
    );
    assert!(
        holds(&bob_content, thumb_id).await,
        "the thumbnail is spared"
    );
    assert!(
        chunk_files(dir_b.path()) > 0,
        "bob holds the photo's chunks"
    );

    // The operator removes alice from the team while all stay offline.
    provisioned
        .fixture
        .exec(&format!(
            "DELETE FROM team_members WHERE team_id = '{TEAM}' AND member_id = '{alice_id}'"
        ))
        .await;

    drop(alice_watches);
    drop(bob_watches);
    drop((alice_content, bob_content, carl_content));
    for client in [alice, bob, carl] {
        client.close().await;
    }

    // The server comes back, bob first.
    let _server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server two");
    let bob2 = connect_client_with_content(
        addr,
        last_used(&base, APP_BOB),
        dir_b.path(),
        &root,
        peer_listener,
        photo_content(),
    )
    .await;
    let mut bob_events = bob2.client().events();
    wait_for_certificate(&bob2, BOUND)
        .await
        .expect("bob holds his device");
    let bob_content = content_for(&bob2);
    assert!(
        holds(&bob_content, full_id).await,
        "bob still holds the photo before the verdict"
    );

    // Alice reconnects and pushes her queue, and the server refuses the
    // photo she posted to a team she has left.
    let alice2 = connect_client_with_content(
        addr,
        last_used(&base, APP_ALICE),
        dir_a.path(),
        &root,
        peer_listener,
        photo_content(),
    )
    .await;
    wait_for_certificate(&alice2, BOUND)
        .await
        .expect("alice holds her device");
    let alice_addr = wait_peer_address(&alice2, BOUND)
        .await
        .expect("alice serves her peer listener again");

    bob2.link_peer(alice_addr)
        .await
        .expect("bob re-links alice");
    let retracted = next_event(&mut bob_events, BOUND, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { rows, .. }
            if rows.iter().any(|(table, _)| table.contains("photos")))
    })
    .await;
    assert!(retracted.is_some(), "the photo's retraction reaches bob");
    // The pass removes the manifests before their chunks, so the driver
    // keeps running until the chunk files are gone too. Dropping it
    // earlier would cut the pass short and leave chunks behind.
    let until = Instant::now() + BOUND;
    loop {
        let left = chunk_files(dir_b.path());
        if left == 0 && !holds(&bob_content, full_id).await && !holds(&bob_content, thumb_id).await
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "the retraction left the photo on bob's device, {left} chunk files"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(bob_content);
    bob2.close().await;
    alice2.close().await;
}
