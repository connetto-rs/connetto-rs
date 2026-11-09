//! R78 slice 4a proof, a device's writes reaching the server through a
//! different account's session. Two signed-in native clients of different
//! accounts run against the demo deployment the real `connetto-server`
//! binary serves with its courier path on.
//!
//! The server goes away for both. Alice writes two team notes, and bob links
//! her offline and applies them as her. Alice's device then stays away for
//! good. The server comes back, bob reconnects, his session offers alice as a
//! courier candidate, and `courier(alice)` delivers her notes. The server
//! holds them under alice's account, one `write_couriered` audit row per entry
//! naming bob as the courier, bob's records stand adjudicated, and when alice's
//! device does come back she hears what was couriered and her own queue
//! settles without applying anything twice.
//!
//! Needs Docker, since the fixture starts its own Postgres and authorization
//! service and the login walks a containerised provider.
#![expect(
    clippy::large_futures,
    reason = "the phase drives two full client lifecycles, its futures are large by design"
)]

use core::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};

use connetto_client::{ClientEvent, CourierCandidate, provisional::ProvisionalState};
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

use crate::demo::*;

/// The mark a re-executed child carries, absent when it is the driver.
const PHASE: &str = "R78_COURIER_OFFER_PHASE";

/// The two notes alice writes offline, in the order she writes them.
const NOTES: [&str; 2] = ["the first couriered note", "the second couriered note"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_courier_delivers_a_lost_devices_writes_through_another_account() {
    if std::env::var(PHASE).is_ok() {
        run_courier().await;
        return;
    }
    // A headless desktop's Secret Service dismisses its unlock prompt, so the
    // scenario runs in a child whose keyring detection settles on sealed
    // files, as the peer exchange proof does.
    let dir = tempdir().expect("a scratch dir");
    let credentials = dir.path().join("credentials");
    let state = dir.path().join("state");
    std::fs::create_dir_all(&credentials).expect("the credentials dir");
    std::fs::create_dir_all(&state).expect("the state dir");
    std::fs::write(credentials.join("connetto.wrap-key"), [1_u8; 32]).expect("the wrap key");
    let output = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "courier_offer::a_courier_offer_phase",
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
        "the courier scenario failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The scenario, the phase the driver re-executes in a child process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the courier driver runs it in a child process"]
async fn a_courier_offer_phase() {
    if std::env::var(PHASE).is_err() {
        return;
    }
    run_courier().await;
}

/// One assertion per step, in the order alice's notes travel.
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per assertion, in the order the notes travel"
)]
async fn run_courier() {
    let _keyring = isolated_session_keyring();

    let provisioned = provision(&DEMO_DEPLOYMENT, "courier-offer", None)
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

    let dir_a = tempdir().expect("a dir");
    let dir_b = tempdir().expect("b dir");
    let peer_listener: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let addr: SocketAddr = bind.parse().expect("address");

    let issuer = idp.issuer().to_owned();
    let alice_id = deterministic_uuid(&issuer, "alice").to_string();
    let bob_id = deterministic_uuid(&issuer, "bob").to_string();
    seed_roster(&provisioned.fixture, &alice_id, &bob_id).await;

    prime_watches(
        addr,
        fresh_login(&base, APP_ALICE, "alice"),
        dir_a.path(),
        &root,
        peer_listener,
    )
    .await;
    prime_watches(
        addr,
        fresh_login(&base, APP_BOB, "bob"),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;

    let alice = connect_client(
        addr,
        last_used(&base, APP_ALICE),
        dir_a.path(),
        &root,
        peer_listener,
    )
    .await;
    let bob = connect_client(
        addr,
        last_used(&base, APP_BOB),
        dir_b.path(),
        &root,
        peer_listener,
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
    let alice_cert = wait_for_certificate(&alice, BOUND)
        .await
        .expect("alice enrols");
    wait_for_certificate(&bob, BOUND).await.expect("bob enrols");
    let all = [
        ("T1".to_owned(), alice_id.clone()),
        ("T1".to_owned(), bob_id.clone()),
        ("T2".to_owned(), alice_id.clone()),
        ("T2".to_owned(), bob_id.clone()),
    ];
    assert!(wait_roster(&alice, &all).await, "alice synced the roster");
    assert!(wait_roster(&bob, &all).await, "bob synced the roster");
    let alice_key = alice_cert.identity().key().as_bytes().to_vec();

    // The server goes away for both, and alice writes her notes offline.
    drop(server);
    wait_until_closed(&bind, BOUND).await;
    for note in NOTES {
        write_note(&alice, "T2", &alice_id, note).await;
    }

    // Bob links alice while both stay offline and applies her notes as her.
    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener");
    bob.link_peer(alice_addr).await.expect("bob links alice");
    wait_until(
        BOUND,
        || async { bob.provisional_rows().await.expect("the rows are read") },
        |rows| rows.len() == NOTES.len(),
    )
    .await
    .unwrap_or_else(|rows| panic!("alice's notes never applied on bob, the records hold {rows:?}"));

    // Alice's device goes away for good, its queue never reaching the server.
    drop(alice_watches);
    alice.close().await;
    drop(alice);
    drop(bob_watches);
    bob.close().await;
    drop(bob);

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

    // Bob's session asks about alice's entries, the server holds them
    // pending, and alice becomes a courier candidate from her first entry on.
    let candidate = wait_candidate(&bob2, &alice_key).await;
    assert_eq!(
        candidate.seqs.first(),
        Some(&1),
        "alice's entries are offered from her first, the candidate is {candidate:?}"
    );
    assert_eq!(
        candidate.account, alice_id,
        "the candidate names alice's account"
    );
    assert!(
        candidate.bytes > 0,
        "the candidate counts the bodies bob holds"
    );

    // The application asks bob's device to courier alice's notes.
    let report = bob2
        .courier(&alice_key)
        .await
        .expect("the courier call answers");
    assert_eq!(
        report.settled, candidate.seqs,
        "every offered note settles, the report is {report:?}"
    );
    assert!(
        report.refused.is_empty() && report.kept.is_empty() && report.retry_after.is_none(),
        "nothing is refused, kept or deferred, the report is {report:?}"
    );

    // The answer leaves nothing of alice's to courier, and bob hears it.
    let emptied = next_event(&mut bob_events, BOUND, |event| {
        matches!(
            event,
            ClientEvent::CourierCandidates(candidate)
                if candidate.author == alice_key && candidate.seqs.is_empty()
        )
    })
    .await;
    assert!(
        emptied.is_some(),
        "bob hears alice has nothing left to courier"
    );
    assert!(
        bob2.courier_candidates()
            .await
            .expect("the candidates are read")
            .iter()
            .all(|candidate| candidate.author != alice_key),
        "alice is no longer a candidate"
    );

    // Bob's records of alice's notes stand adjudicated.
    wait_until(
        BOUND,
        || async { bob2.provisional_rows().await.expect("read bob's records") },
        |rows| {
            rows.iter()
                .all(|row| row.state != ProvisionalState::Provisional)
        },
    )
    .await
    .unwrap_or_else(|rows| {
        panic!("the courier answer never adjudicated bob's records, they hold {rows:?}")
    });

    // The server holds both notes under alice's account, each audited as
    // couriered by bob.
    assert_server_notes(&provisioned.fixture, &alice_id).await;
    let mut conn = provisioned
        .fixture
        .admin()
        .get()
        .await
        .expect("admin connection");
    let couriered: Vec<(Option<String>, Option<String>)> = diesel_async::RunQueryDsl::load(
        auth_events::table
            .filter(auth_events::op.eq(AuthOp::WriteCouriered))
            .select((auth_events::user_id, auth_events::courier_user_id)),
        &mut *conn,
    )
    .await
    .expect("read the couriered audit rows");
    assert_eq!(
        couriered,
        vec![(Some(alice_id.clone()), Some(bob_id.clone())); candidate.seqs.len()],
        "one write_couriered row per entry, naming alice as author and bob as courier"
    );
    drop(conn);

    // Alice's device comes back, hears what bob couriered, and settles her
    // own queue from the server's record without a second apply.
    let alice2 = connect_client(
        addr,
        last_used(&base, APP_ALICE),
        dir_a.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut alice_events = alice2.client().events();
    let notices = next_event(&mut alice_events, BOUND, |event| {
        matches!(event, ClientEvent::WritesCouriered { .. })
    })
    .await;
    let Some(ClientEvent::WritesCouriered { notices, .. }) = notices else {
        panic!("alice never heard what was couriered");
    };
    assert_eq!(
        notices.len(),
        candidate.seqs.len(),
        "alice hears every couriered entry"
    );
    assert!(
        notices.iter().all(|notice| notice.courier == bob_id),
        "every notice names bob as the courier, they read {notices:?}"
    );
    assert!(wait_unsynced_empty(&alice2).await, "alice's queue settles");
    assert_server_notes(&provisioned.fixture, &alice_id).await;

    bob2.close().await;
    alice2.close().await;
    drop(bob2);
    drop(alice2);
}

/// Poll `read` until `done` holds of its value, a hundred milliseconds
/// between laps, bounded by `bound`; on overrun the last reading stands as
/// the error.
async fn wait_until<T, F, Fut, P>(bound: Duration, mut read: F, done: P) -> Result<T, T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
    P: Fn(&T) -> bool,
{
    let until = Instant::now() + bound;
    loop {
        let value = read().await;
        if done(&value) {
            return Ok(value);
        }
        if Instant::now() >= until {
            return Err(value);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `courier_candidates` until `author` is a candidate, bounded by
/// `BOUND`.
async fn wait_candidate(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    author: &[u8],
) -> CourierCandidate {
    wait_until(
        BOUND,
        || async {
            client
                .courier_candidates()
                .await
                .expect("the candidates are read")
                .into_iter()
                .find(|candidate| candidate.author == author)
        },
        Option::is_some,
    )
    .await
    .unwrap_or_else(|_| panic!("the author never became a courier candidate"))
    .expect("the candidate stood")
}

/// The server's notes are alice's two, each once.
async fn assert_server_notes(fixture: &connetto_test_harness::Fixture, alice: &str) {
    let mut conn = fixture.admin().get().await.expect("admin connection");
    let rows: Vec<(String, String, String)> = diesel_async::RunQueryDsl::load(
        team_notes::table
            .select((team_notes::team_id, team_notes::author_id, team_notes::body))
            .order(team_notes::body.asc()),
        &mut *conn,
    )
    .await
    .expect("read the notes");
    let expected: Vec<(String, String, String)> = NOTES
        .iter()
        .map(|body| ("T2".to_owned(), alice.to_owned(), (*body).to_owned()))
        .collect();
    assert_eq!(rows, expected, "the server holds alice's notes, each once");
}
