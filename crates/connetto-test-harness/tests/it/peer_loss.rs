//! R79 slice 14 proof, the Done when's last clause. Four signed-in native
//! clients of different accounts run against the demo deployment the real
//! `connetto-server` binary serves with its courier path on, each with a
//! content client replicating the photo column to three copies.
//!
//! The server goes away. Alice takes a photo offline, posted to the team bob,
//! carl and dave share with her, and each of the three links her and takes
//! the photo into custody. Alice's device then goes away for good. The
//! server comes back, bob couriers alice's rows, and his settled copy goes up
//! at once, credited to alice and recorded as carried by bob. Once the state
//! column reads `available`, every holder releases its copy.
//!
//! Needs Docker, since the fixture starts its own Postgres and authorization
//! service and the login walks a containerised provider.
#![expect(
    clippy::large_futures,
    reason = "the phase drives four full client lifecycles, its futures are large by design"
)]

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime};

use connetto_client::live::{ConnettoClient, LiveQuery};
use connetto_client::{ClientEvent, NativeClient, NativeTransport};
use connetto_file_client::{ContentClient, ContentEvent, FileId, FsStore, MimeClass, ReqwestHttp};
use connetto_server::audit::AuthOp;
use connetto_server::authn::identity::deterministic_uuid;
use connetto_server::defaults::auth_events;
use connetto_test_harness::stack::{
    DEMO_DEPLOYMENT, demo_device_ca, ensure_server_bin, provision, provision_enrolment_tables,
    spawn_server, wait_until_closed,
};
use connetto_test_harness::{MockOauth, isolated_session_keyring};
use diesel::prelude::*;
use tempfile::tempdir;
use tokio::sync::broadcast;

use crate::demo::*;
use crate::peer_media::{Content, ROOT_KEY, TEAM, holds, order_of, photo_bytes, photos};

/// The mark a re-executed child carries, absent when it is the driver.
const PHASE: &str = "R79_PEER_LOSS_PHASE";

/// The keyring services the two further teammates sign in under.
const APP_CARL: &str = "r79-peer-loss-carl";
const APP_DAVE: &str = "r79-peer-loss-dave";

/// The devices other than the author that keep each replicated photo.
const COPIES: u8 = 3;

diesel::table! {
    /// The custody records of the photos a device holds for others.
    _connetto_content_custody (file_id) {
        /// The held file.
        file_id -> Binary,
        /// `held`, or a leaving mark.
        state -> Text,
    }
}

diesel::table! {
    /// The file server's manifests, one per file and uploader.
    _cfs_manifests (file_id, uploaded_by) {
        /// The file.
        file_id -> Binary,
        /// The account the upload is credited to.
        uploaded_by -> Text,
        /// The carrier's key, set for a carried upload.
        carried_by -> Nullable<Text>,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_authors_photo_reaches_the_server_through_its_teammates() {
    if std::env::var(PHASE).is_ok() {
        run_loss().await;
        return;
    }
    // A headless desktop's Secret Service dismisses its unlock prompt, so the
    // scenario runs in a child whose keyring detection settles on sealed
    // files, as the camp proof does.
    let dir = tempdir().expect("a scratch dir");
    let credentials = dir.path().join("credentials");
    let state = dir.path().join("state");
    std::fs::create_dir_all(&credentials).expect("the credentials dir");
    std::fs::create_dir_all(&state).expect("the state dir");
    std::fs::write(credentials.join("connetto.wrap-key"), [1_u8; 32]).expect("the wrap key");
    let output = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "peer_loss::a_lost_authors_photo_phase",
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
        "the loss scenario failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The scenario itself, the phase the driver re-executes in a child process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the loss driver runs it in a child process"]
async fn a_lost_authors_photo_phase() {
    if std::env::var(PHASE).is_err() {
        return;
    }
    run_loss().await;
}

/// A content client over `client`'s replica and the chunk store in `dir`,
/// replicating the photo column to [`COPIES`] devices until its state column
/// reads `available`.
async fn replicating(client: &ConnettoClient<NativeTransport>, dir: &Path) -> Content {
    ContentClient::attach(
        client.clone(),
        FsStore::new(dir.join("chunks")),
        ROOT_KEY,
        ReqwestHttp::new(),
    )
    .await
    .expect("attach the content client")
    .with_peer_content("photos", "content_id")
    .replicate(COPIES, "content_state")
    .await
    .expect("declare the replicated photo column")
}

/// The custody state of `file_id` on `client`'s replica, `None` for none.
async fn custody_state(client: &NativeClient<NativeTransport>, file_id: FileId) -> Option<String> {
    client
        .client()
        .with_conn(move |conn| {
            _connetto_content_custody::table
                .filter(_connetto_content_custody::file_id.eq(file_id.as_bytes().to_vec()))
                .select(_connetto_content_custody::state)
                .first::<String>(conn.conn())
                .optional()
        })
        .await
        .expect("the replica is open")
        .expect("read the custody record")
}

/// Stages `bytes` as alice's photo on her order, posted to the team, and
/// answers its file id.
async fn take_photo(content: &Content, order_id: Vec<u8>, owner: &str, bytes: &[u8]) -> FileId {
    let owner = owner.to_owned();
    let (file_id, ()) = content
        .stage(bytes, MimeClass::Jpeg, move |conn, file_id| {
            diesel::insert_into(photos::table)
                .values((
                    photos::order_id.eq(order_id),
                    photos::owner_id.eq(owner),
                    photos::content_id.eq(file_id.as_bytes().to_vec()),
                    photos::team_id.eq(Some(TEAM)),
                ))
                .execute(conn)
                .map(|_| ())
        })
        .await
        .expect("stage the photo");
    file_id
}

/// Watches the photos' states on `client`, the subscription that brings the
/// server's `available` to the replica.
async fn watch_photos(
    client: &NativeClient<NativeTransport>,
) -> LiveQuery<(Vec<u8>, Option<String>)> {
    client
        .client()
        .watch(photos::table.select((photos::content_id, photos::content_state)))
        .await
        .expect("the photos follow")
}

/// Waits on `photos` until `file_id`'s state reads `available`, bounded.
async fn until_available(photos: &mut LiveQuery<(Vec<u8>, Option<String>)>, file_id: FileId) {
    tokio::time::timeout(BOUND, async {
        loop {
            if photos.rows().iter().any(|(id, state)| {
                id.as_slice() == file_id.as_bytes() && state.as_deref() == Some("available")
            }) {
                return;
            }
            photos.changed().await.expect("the watch runs");
        }
    })
    .await
    .expect("the photo's state reads available within the bound");
}

/// Waits for `content` to announce `file_id` fetched into custody, bounded,
/// and names what `label`'s device holds and heard when it never does.
async fn pulled(
    label: &str,
    client: &NativeClient<NativeTransport>,
    content: &Content,
    events: &mut broadcast::Receiver<ContentEvent>,
    file_id: FileId,
) {
    let mut heard = Vec::new();
    let arrived = tokio::time::timeout(BOUND, async {
        loop {
            match events.recv().await {
                Ok(ContentEvent::Fetched { file_id: got }) if got == file_id => return,
                Ok(event) => heard.push(format!("{event:?}")),
                Err(err) => heard.push(format!("{err:?}")),
            }
        }
    })
    .await;
    if arrived.is_err() {
        let rows = client.provisional_rows().await.map(|rows| {
            rows.into_iter()
                .map(|row| (row.table, row.state))
                .collect::<Vec<_>>()
        });
        let visible: Vec<(Vec<u8>, Option<String>)> = client
            .client()
            .with_conn(|conn| {
                photos::table
                    .select((photos::content_id, photos::content_state))
                    .load(conn.conn())
            })
            .await
            .expect("the replica is open")
            .expect("read the photos");
        panic!(
            "{label} never pulled the photo, holding it {}, custody {:?}, provisional rows {rows:?}, \
             photos visible {}, having heard {heard:?}",
            holds(content, file_id).await,
            custody_state(client, file_id).await,
            visible.len(),
        );
    }
}

/// Waits for `content` to announce `file_id` uploaded, bounded.
async fn uploaded(events: &mut broadcast::Receiver<ContentEvent>, file_id: FileId) {
    tokio::time::timeout(BOUND, async {
        loop {
            match events.recv().await {
                Ok(ContentEvent::Uploaded { file_id: got }) if got == file_id => return,
                Ok(ContentEvent::UploadRefused {
                    file_id: got,
                    detail,
                }) if got == file_id => {
                    panic!("the carried upload was refused: {detail}");
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the carried upload commits within the bound");
}

/// Releases `file_id` on a reconnected holder once its state reads
/// `available`, the copy leaving custody and the tidy removing it as cache.
async fn released(
    client: &NativeClient<NativeTransport>,
    content: &Content,
    photos: &mut LiveQuery<(Vec<u8>, Option<String>)>,
    file_id: FileId,
) {
    until_available(photos, file_id).await;
    content.tidy_content().await.expect("the holder tidies");
    assert_eq!(
        custody_state(client, file_id).await,
        None,
        "the server holds the photo, so custody lets it go"
    );
    assert!(
        !holds(content, file_id).await,
        "the released copy is cache the tidy removes"
    );
}

/// The scenario, one assertion per step.
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per assertion, in the order the photo travels"
)]
async fn run_loss() {
    let _keyring = isolated_session_keyring();

    let provisioned = provision(&DEMO_DEPLOYMENT, "peer-loss", None)
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
    envs.push(("CONNETTO_COURIER".to_owned(), "on".to_owned()));
    envs.extend(idp.env_pairs(PROVIDER, &format!("{base}/auth/callback")));
    let server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server one");

    let dirs = [
        tempdir().expect("alice dir"),
        tempdir().expect("bob dir"),
        tempdir().expect("carl dir"),
        tempdir().expect("dave dir"),
    ];
    let [dir_a, dir_b, dir_c, dir_d] = &dirs;
    let peer_listener: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let addr: SocketAddr = bind.parse().expect("address");

    let issuer = idp.issuer().to_owned();
    let alice_id = deterministic_uuid(&issuer, "alice").to_string();
    let bob_id = deterministic_uuid(&issuer, "bob").to_string();
    let carl_id = deterministic_uuid(&issuer, "carl").to_string();
    let dave_id = deterministic_uuid(&issuer, "dave").to_string();
    seed_roster(&provisioned.fixture, &alice_id, &bob_id).await;
    for teammate in [&carl_id, &dave_id] {
        provisioned
            .fixture
            .exec(&format!(
                "INSERT INTO team_members (team_id, member_id) VALUES ('{TEAM}', '{teammate}')"
            ))
            .await;
    }

    let devices = [
        (APP_ALICE, "alice", dir_a.path()),
        (APP_BOB, "bob", dir_b.path()),
        (APP_CARL, "carl", dir_c.path()),
        (APP_DAVE, "dave", dir_d.path()),
    ];
    for (app, subject, dir) in devices {
        prime_watches(
            addr,
            fresh_login(&base, app, subject),
            dir,
            &root,
            peer_listener,
        )
        .await;
    }
    let [alice, bob, carl, dave] = {
        let mut connected = Vec::new();
        for (app, _, dir) in devices {
            connected
                .push(connect_client(addr, last_used(&base, app), dir, &root, peer_listener).await);
        }
        <[NativeClient<NativeTransport>; 4]>::try_from(connected)
            .unwrap_or_else(|_| panic!("four devices connect"))
    };
    for (client, id) in [
        (&alice, &alice_id),
        (&bob, &bob_id),
        (&carl, &carl_id),
        (&dave, &dave_id),
    ] {
        assert_eq!(&wait_session(client).await, id, "the id matches the seed");
    }
    let alice_cert = wait_for_certificate(&alice, BOUND)
        .await
        .expect("alice enrols");
    for client in [&bob, &carl, &dave] {
        wait_for_certificate(client, BOUND)
            .await
            .expect("each teammate enrols");
    }
    let alice_key = alice_cert.identity().key().as_bytes().to_vec();
    let mut all: Vec<(String, String)> = [&alice_id, &bob_id, &carl_id, &dave_id]
        .into_iter()
        .map(|id| (TEAM.to_owned(), id.clone()))
        .collect();
    all.extend([&alice_id, &bob_id].map(|id| ("T2".to_owned(), id.clone())));
    for client in [&alice, &bob, &carl, &dave] {
        assert!(
            wait_roster(client, &all).await,
            "every device synced the roster"
        );
    }
    let mut kept_watches = Vec::new();
    for client in [&alice, &bob, &carl, &dave] {
        kept_watches.push(watch_tables(client).await);
    }

    let alice_content = replicating(alice.client(), dir_a.path()).await;
    let holders = [
        replicating(bob.client(), dir_b.path()).await,
        replicating(carl.client(), dir_c.path()).await,
        replicating(dave.client(), dir_d.path()).await,
    ];

    // The server goes away for everyone, and alice takes the photo offline.
    drop(server);
    wait_until_closed(&bind, BOUND).await;
    write_order(&alice, &alice_id, 1).await;
    let order_id = order_of(&alice).await;
    let full = photo_bytes(14, 2 * 1024 * 1024 + 41);
    let file_id = take_photo(&alice_content, order_id, &alice_id, &full).await;

    // Each teammate links alice, applies her row and takes the photo into
    // custody, three holders for three copies.
    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener");
    let ports = [&alice, &bob, &carl, &dave].map(|client| client.media_port().expect("one port"));
    let [alice_port, bob_port, carl_port, dave_port] = ports;
    let mut arrivals = holders.each_ref().map(Content::events);
    let [bob_content, carl_content, dave_content] = &holders;
    tokio::select! {
        () = alice_content.drive_media(alice_port) => panic!("alice's media driver ended"),
        () = bob_content.drive_media(bob_port) => panic!("bob's media driver ended"),
        () = carl_content.drive_media(carl_port) => panic!("carl's media driver ended"),
        () = dave_content.drive_media(dave_port) => panic!("dave's media driver ended"),
        () = async {
            for teammate in [&bob, &carl, &dave] {
                teammate.link_peer(alice_addr).await.expect("a teammate links alice");
            }
            for ((label, teammate), (content, events)) in
                [("bob", &bob), ("carl", &carl), ("dave", &dave)]
                    .into_iter()
                    .zip(holders.iter().zip(&mut arrivals))
            {
                pulled(label, teammate, content, events, file_id).await;
            }
        } => {}
    }
    for teammate in [&bob, &carl, &dave] {
        assert_eq!(
            custody_state(teammate, file_id).await.as_deref(),
            Some("held"),
            "every teammate holds the photo in custody"
        );
    }

    // Alice's device goes away for good, her photo never reaching the server.
    drop(kept_watches);
    drop((alice_content, holders));
    for client in [alice, bob, carl, dave] {
        client.close().await;
    }

    // The server comes back with the courier path on, and bob reconnects.
    let _server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server two");
    let bob2 = connect_client(
        addr,
        last_used(&base, APP_BOB),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut bob_events = bob2.client().events();
    wait_for_certificate(&bob2, BOUND)
        .await
        .expect("bob holds his device");
    let bob_content = replicating(bob2.client(), dir_b.path()).await;
    let mut bob_photos = watch_photos(&bob2).await;
    let mut bob_files = bob_content.events();
    let bob_port = bob2.media_port().expect("bob's port");

    let never = |_: Duration| std::future::pending::<()>();
    tokio::select! {
        () = bob_content.drive_media(bob_port) => panic!("bob's media driver ended"),
        () = bob_content.drive_outbox(never) => panic!("bob's outbox driver ended"),
        () = async {
            // Bob's session learns alice's entry waits on the server, and he
            // couriers it.
            let offered = bob2
                .courier_candidates()
                .await
                .expect("the candidates are read")
                .iter()
                .any(|candidate| candidate.author == alice_key && !candidate.seqs.is_empty());
            if !offered {
                let candidate = next_event(&mut bob_events, BOUND, |event| {
                    matches!(event, ClientEvent::CourierCandidates(candidate)
                        if candidate.author == alice_key && !candidate.seqs.is_empty())
                })
                .await;
                assert!(candidate.is_some(), "alice becomes a courier candidate");
            }
            let report = bob2
                .courier(&alice_key)
                .await
                .expect("the courier call answers");
            assert!(
                !report.settled.is_empty() && report.refused.is_empty(),
                "alice's photo row settles, the report is {report:?}"
            );

            // The courier's copy goes up at once, and the server's setter
            // flips the state, which takes the copy out of bob's custody.
            uploaded(&mut bob_files, file_id).await;
            released(&bob2, &bob_content, &mut bob_photos, file_id).await;
        } => {}
    }

    // The server holds the photo credited to alice and carried by bob.
    let mut conn = provisioned
        .fixture
        .admin()
        .get()
        .await
        .expect("admin connection");
    let manifests: Vec<(String, Option<String>)> = diesel_async::RunQueryDsl::load(
        _cfs_manifests::table
            .filter(_cfs_manifests::file_id.eq(file_id.as_bytes().to_vec()))
            .select((_cfs_manifests::uploaded_by, _cfs_manifests::carried_by)),
        &mut *conn,
    )
    .await
    .expect("read the manifests");
    assert_eq!(manifests.len(), 1, "one upload, {manifests:?}");
    assert_eq!(
        manifests[0],
        (format!("user:{alice_id}"), Some(format!("user:{bob_id}"))),
        "the upload is credited to alice and carried by bob"
    );
    let carried: Vec<(Option<String>, Option<String>)> = diesel_async::RunQueryDsl::load(
        auth_events::table
            .filter(auth_events::op.eq(AuthOp::ContentCarried))
            .select((auth_events::user_id, auth_events::courier_user_id)),
        &mut *conn,
    )
    .await
    .expect("read the carried audit rows");
    assert_eq!(
        carried,
        vec![(Some(alice_id.clone()), Some(bob_id.clone()))],
        "one content_carried row naming alice as owner and bob as carrier"
    );
    drop(conn);

    // Carl and dave come back and let their copies go too.
    for (app, dir) in [(APP_CARL, dir_c.path()), (APP_DAVE, dir_d.path())] {
        let teammate = connect_client(addr, last_used(&base, app), dir, &root, peer_listener).await;
        let content = replicating(teammate.client(), dir).await;
        let mut photos = watch_photos(&teammate).await;
        released(&teammate, &content, &mut photos, file_id).await;
        drop(photos);
        drop(content);
        teammate.close().await;
    }

    drop(bob_photos);
    drop(bob_content);
    bob2.close().await;
}
