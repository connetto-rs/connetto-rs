//! The live pump's courier dial (R78 slice 4c, decision 47): the range its
//! wire owes is dialed by the pump over the reconnect factory, an
//! uncertified bind refusing the call, a refused dial backing off.

use core::fmt::Write as _;
use std::process::Command;
use std::time::Duration;

use axum::Json;
use connetto_client::{
    AccountChoice, Auth, CourierError, FirstThen, KeyringStore, NativeClient, NativeClientBuilder,
    SyncSchema, encode_identity,
};
use connetto_core::exchange::{CourierRefusal, Frontier, Offer, SignedOffer};
#[cfg(feature = "device-identity")]
use connetto_core::messages::Challenge;
use connetto_core::messages::{ControlMessage, HandshakeAck, WriterGrant};
use connetto_core::schema::SchemaBundle;
use connetto_core::traits::RefreshTokenStore as _;
use connetto_core::{Cursor, IncomingFrame, Major, SchemaVersion};
use diesel::connection::SimpleConnection as _;
use serde_json::json;
use tempfile::tempdir;
use tokio::time::timeout;

use super::support::{self, Script};

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);";
/// The bound a pump-driven dial owes on.
const BOUND: Duration = Duration::from_secs(5);
/// The device key the tests courier, its offer standing major 1 so the
/// range rides the courier's dial rather than the major-2 build's main wire.
const AUTHOR: [u8; 32] = [0x07; 32];
/// The certificate end a test cursor carries.
const CERT_END: i64 = 4_102_444_800;
/// The app id the back-off phase signs in under.
const APP_DIAL: &str = "r78-courier-dial";
/// The subject the phase signs in as, the replica both boots open.
const SUBJECT: &str = "dialer";
/// The refresh token the phase's auth service rotates on.
const REFRESH_TOKEN: &str = "courier-dial-refresh";
/// The access token the phase's auth service grants, which the Script wires
/// never check.
const ACCESS_TOKEN: &str = "courier-dial-access";

fn hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes {
        write!(out, "{byte:02x}").expect("the hex appends");
    }
    out
}

/// A test schema bundle standing major 2, one major above the author's offer.
fn bundle2() -> SyncSchema {
    SyncSchema::new(
        SchemaBundle::new(
            "-- connetto:major 2",
            "",
            DDL,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares major 2"),
    )
}

/// The author's offer the tests seed, major 1, newest at the seeded row.
fn offer_bytes() -> Vec<u8> {
    let offer = SignedOffer {
        offer: Offer {
            builds: vec![(
                Major::new(1).expect("a major"),
                SchemaVersion::from_hash(b"-- connetto:major 1".to_vec()),
            )],
            settled: None,
            newest: Some(Frontier {
                seq: 1,
                head: [9; 32],
            }),
            retractions: Vec::new(),
        },
        signature: vec![1, 2, 3],
    };
    rmp_serde::to_vec_named(&offer).expect("the offer encodes")
}

/// The seeded held row and its major-1 offer, the run the dial stands.
fn seed_sql() -> String {
    let author = hex(&AUTHOR);
    format!(
        "INSERT INTO _connetto_peer_entry \
         (author, seq, state, major, origin, digest, changeset, account, bot_list, received_at, position) \
         VALUES (x'{author}', 1, 1, 1, NULL, x'0707070707070707070707070707070707070707070707070707070707070707', x'68656c64', 'courier-author', NULL, 1, 1); \
         INSERT INTO _connetto_peer_cursor (author, account, seq, head, offer, expires_at) \
         VALUES (x'{author}', 'courier-author', 0, zeroblob(32), x'{offer}', {CERT_END})",
        offer = hex(&offer_bytes())
    )
}

/// The handshake answer that grants the courier path and no writer, for the
/// build whose handshake presents the device.
fn courier_ack_device() -> IncomingFrame {
    IncomingFrame::Control(ControlMessage::HandshakeAck(HandshakeAck {
        connection_id: "courier".to_owned(),
        session_token: "courier".to_owned(),
        resume_token: "courier".to_owned(),
        current_cursor: Cursor::from(Vec::new()),
        schema_version: None,
        sunset_at_secs: None,
        courier: true,
        initial_credits: 64,
        writer: None,
    }))
}

/// The handshake answer that grants the courier path, the support one with
/// it on.
fn courier_ack() -> IncomingFrame {
    IncomingFrame::Control(ControlMessage::HandshakeAck(HandshakeAck {
        connection_id: "courier".to_owned(),
        session_token: "courier".to_owned(),
        resume_token: "courier".to_owned(),
        current_cursor: Cursor::from(Vec::new()),
        schema_version: None,
        sunset_at_secs: None,
        courier: true,
        initial_credits: 64,
        writer: Some(WriterGrant {
            number: 1,
            frontier: 0,
        }),
    }))
}

/// An anonymous native build whose main wire is `main`, the dialer handing
/// the courier dial's wire off `second`, its pump spawned.
async fn build(
    main: &Script,
    second: &Script,
) -> (NativeClient<Script>, tokio::task::JoinHandle<()>) {
    main.say(courier_ack());
    second.say(courier_ack());
    let second = second.clone();
    let dialer = FirstThen::new(main.clone(), move || {
        let wire = second.clone();
        async move { Ok::<Script, support::Gone>(wire) }
    });
    let (native, pump) = NativeClientBuilder::new("ws://127.0.0.1:1", bundle2())
        .with_dialer(dialer)
        .connect_with_pump()
        .await
        .expect("the build connects");
    (native, tokio::spawn(pump))
}

/// The pump dials the range its wire owes, and the bind the device cannot
/// stand refuses the call, the dial owing nothing more.
#[tokio::test]
async fn the_live_pump_dials_the_owed_courier_range_and_the_bind_refuses_it() {
    let main = Script::default();
    let second = Script::default();
    let (native, pump) = build(&main, &second).await;
    let client = native.client().clone();
    client
        .with_conn(|conn| conn.conn().batch_execute(&seed_sql()))
        .await
        .expect("the gate is open")
        .expect("the seed lands");
    let err = timeout(BOUND, client.courier(&AUTHOR))
        .await
        .expect("the pump dials the range in time")
        .expect_err("the device stands no bind");
    assert!(
        matches!(err, CourierError::Refused(CourierRefusal::Uncertified)),
        "the uncertified bind refuses the range: {err:?}"
    );
    assert_eq!(
        client
            .with_conn(|conn| conn.courier_drain_timer())
            .await
            .expect("the gate is open"),
        None,
        "the refused dial owes nothing more"
    );
    drop(pump);
}

/// The connection-opening challenge frame, the one every server sends first.
#[cfg(feature = "device-identity")]
fn challenge() -> IncomingFrame {
    IncomingFrame::Control(ControlMessage::Challenge(Challenge { nonce: [0x42; 32] }))
}

/// A fake auth service whose `/auth/refresh` answers the phase's silent
/// sign-ins, holding nothing else.
#[cfg(feature = "device-identity")]
async fn fake_auth() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the auth listener binds");
    let port = listener.local_addr().expect("an address").port();
    let answer = json!({
        "access_token": ACCESS_TOKEN,
        "refresh_token": REFRESH_TOKEN,
        "user_id": SUBJECT,
        "session_expires_at": CERT_END,
    });
    let router = axum::Router::new().route(
        "/auth/refresh",
        axum::routing::post(move || async move { Json(answer.clone()) }),
    );
    (
        format!("http://127.0.0.1:{port}"),
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("the auth router serves");
        }),
    )
}

/// A device build over `dir` whose main wire is `main` answered with `ack`,
/// every dial after it refused, its pump spawned. The build signs in
/// silently under `APP_DIAL`, the way a restarted device's second boot does,
/// so the builder hands the device's writer over.
#[cfg(feature = "device-identity")]
async fn dial_build(
    dir: &std::path::Path,
    main: &Script,
    ack: IncomingFrame,
    base: &str,
    roots: &[Vec<u8>],
) -> (NativeClient<Script>, tokio::task::JoinHandle<()>) {
    main.say(challenge());
    main.say(ack);
    let dialer = FirstThen::new(main.clone(), move || async {
        Err::<Script, support::Gone>(support::Gone::Closed)
    });
    let sign_in = Auth::new(base, "dial")
        .with_account(AccountChoice::Account(
            encode_identity(SUBJECT).expect("the id serializes"),
        ))
        .keyring(APP_DIAL);
    let (native, pump) = Box::pin(
        NativeClientBuilder::new("ws://127.0.0.1:1", bundle2())
            .with_dialer(dialer)
            .signed_in(sign_in)
            .durable(dir)
            .with_deployment_roots(roots.to_vec())
            .connect_with_pump(),
    )
    .await
    .expect("the build connects");
    (native, tokio::spawn(pump))
}

/// A dial the factory refuses backs off under the pump, the range's wait
/// growing while the call keeps its bound. A first boot stands the device's
/// certificate and writer beside its key in the keyring, and a second boot
/// the way a restarted device does loads them, so the range's wire
/// certifies the device, owes the dial, and the factory refuses it.
#[cfg(feature = "device-identity")]
#[test]
fn a_refused_courier_dial_backs_off_under_the_live_pump() {
    let dir = tempdir().expect("a temp dir");
    let credentials = dir.path().join("credentials");
    let state = dir.path().join("state");
    std::fs::create_dir_all(&credentials).expect("the credentials dir");
    std::fs::create_dir_all(&state).expect("the state dir");
    std::fs::write(credentials.join("connetto.wrap-key"), [1u8; 32]).expect("the wrap key");
    // The builder's keyring store is detected, never named, and a desktop's
    // D-Bus Secret Service dismisses headless, so the phase re-runs where
    // detection settles on the sealed files.
    let output = Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "courier_dial::dial_backoff_phase",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("STATE_DIRECTORY")
        .env("CREDENTIALS_DIRECTORY", &credentials)
        .env("STATE_DIRECTORY", &state)
        .output()
        .expect("the phase spawns");
    assert!(
        output.status.success(),
        "the back-off phase failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The phase the driver re-execs, the child's keyring sealed in its state
/// dir. Run alone it does nothing.
#[cfg(feature = "device-identity")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a phase the dial driver runs in a child process"]
async fn dial_backoff_phase() {
    // The silent sign-in's refresh token, beside what a login would have
    // stored, so both boots sign in without a browser.
    let (base, _auth) = fake_auth().await;
    let tokens = KeyringStore::new(APP_DIAL);
    tokens
        .store(
            &encode_identity(SUBJECT).expect("the id serializes"),
            REFRESH_TOKEN,
        )
        .await
        .expect("the token stores");
    let dir = tempdir().expect("a temp dir");
    let (leaf, issuer, key_id) = crate::writer::mint_device_certificate(SUBJECT);
    // The phase verifies no chain, so the parseable issuer cert stands as
    // the root the build ships.
    let roots = vec![issuer.clone()];
    // The first boot opens the device's key and stands the certificate and
    // the device's writer the second boot loads with.
    let main = Script::default();
    let (native, pump) =
        Box::pin(dial_build(dir.path(), &main, courier_ack(), &base, &roots)).await;
    let client = native.client().clone();
    let seed = format!(
        "{seed};
         INSERT INTO _connetto_device_certificate (id, certificate, issuer, lifetime_secs) \
         VALUES (1, x'{leaf_hex}', x'{issuer_hex}', 2592000); \
         INSERT INTO _connetto_writers (id, next_seq, head) \
         VALUES ('device:{key_hex}', 1, x'{head_hex}');",
        seed = seed_sql(),
        leaf_hex = hex(&leaf),
        issuer_hex = hex(&issuer),
        key_hex = hex(&key_id),
        head_hex = "00".repeat(32),
    );
    client
        .with_conn(|conn| conn.conn().batch_execute(&seed))
        .await
        .expect("the gate is open")
        .expect("the device stands");
    drop(pump);
    client.close().await;

    // The second boot loads the device, owes the dial, and the factory
    // refuses it.
    let main = Script::default();
    let (native, pump) = Box::pin(dial_build(
        dir.path(),
        &main,
        courier_ack_device(),
        &base,
        &roots,
    ))
    .await;
    let client = native.client().clone();
    let call = client.clone();
    tokio::spawn(async move {
        let _ = call.courier(&AUTHOR).await;
    });
    timeout(BOUND, async {
        loop {
            let wait = client
                .with_conn(|conn| conn.courier_drain_timer().map(|timer| timer.wait()))
                .await
                .expect("the gate is open");
            if wait == Some(Duration::ZERO) || wait.is_none() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            break;
        }
    })
    .await
    .expect("the failed dial backs off in time");
    drop(pump);
}
