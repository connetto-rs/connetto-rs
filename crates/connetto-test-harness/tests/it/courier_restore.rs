//! R78 slice 6 proof, a courier's re-delivery to a restored server. The
//! deployment takes a backup, alice's device delivers two notes the server
//! acknowledges after the backup, bob's device holds them as retained beside
//! the offer alice's device re-presents with her settled frontier, alice's
//! device goes away for good, the database is restored to the backup, bob
//! logs in again, his session's retained ask answers his rows `Pending`
//! and `courier(alice)` brings them back, which the restored server holds
//! under alice's account once, one `write_couriered` audit row each.
//!
//! Needs Docker, since the fixture starts its own Postgres and authorization
//! service and the login walks a containerised provider.
#![expect(
    clippy::large_futures,
    reason = "the phase drives two full client lifecycles, its futures are large by design"
)]

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use connetto_client::peer::HeldState;
use connetto_client::{
    ClientEvent, CourierCandidate, CourierError, KeyringAuth, NativeClientBuilder, SyncTuning,
    WriterRef, provisional::ProvisionalState,
};
use connetto_core::exchange::Verdict;
use connetto_server::audit::AuthOp;
use connetto_server::authn::identity::deterministic_uuid;
use connetto_server::defaults::{_connetto_writers, auth_events};
use connetto_test_harness::stack::{
    DEMO_DEPLOYMENT, demo_device_ca, ensure_server_bin, provision, provision_enrolment_tables,
    spawn_server, wait_until_closed,
};
use connetto_test_harness::{MockOauth, isolated_session_keyring};
use diesel::prelude::*;
use tempfile::tempdir;

use crate::demo::*;

/// The mark a re-executed child carries, absent when it is the driver.
const PHASE: &str = "R78_COURIER_RESTORE_PHASE";

/// The two notes alice writes offline, in the order she writes them.
const NOTES: [&str; 2] = ["the first restored note", "the second restored note"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_courier_redelivers_to_a_restored_server() {
    if std::env::var(PHASE).is_ok() {
        run_restore().await;
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
            "courier_restore::a_courier_restore_phase",
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
        "the restore scenario failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The scenario, the phase the driver re-executes in a child process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the restore driver runs it in a child process"]
async fn a_courier_restore_phase() {
    if std::env::var(PHASE).is_err() {
        return;
    }
    run_restore().await;
}

/// One assertion per step, in the order alice's notes travel.
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per assertion, in the order the notes travel"
)]
async fn run_restore() {
    let _keyring = isolated_session_keyring();

    let provisioned = provision(&DEMO_DEPLOYMENT, "courier-restore", None)
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

    // The deployment takes its backup, then the server goes away for both.
    provisioned.fixture.dump_backup().await;
    drop(server);
    wait_until_closed(&bind, BOUND).await;

    // Alice writes her notes offline, and bob links her while both stay offline.
    for note in NOTES {
        write_note(&alice, "T2", &alice_id, note).await;
    }
    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener");
    bob.link_peer(alice_addr).await.expect("bob links alice");
    wait_provisional_count(&bob, NOTES.len()).await;

    // The server comes back; alice's redial acknowledges her notes after the
    // backup, and bob's records stand adjudicated beside the offer she re-presents.
    let server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server two");
    assert!(
        wait_unsynced_empty(&alice).await,
        "alice's queue settles on the redial"
    );
    wait_adjudicated(&bob).await;
    assert_server_notes(&provisioned.fixture, &alice_id).await;

    // Alice's device goes away for good, then bob's old client; his rows and
    // offer are durable in his dir.
    drop(alice_watches);
    alice.close().await;
    drop(alice);
    drop(bob_watches);
    bob.close().await;
    drop(bob);

    // The database is restored to the backup, and the server comes back.
    drop(server);
    wait_until_closed(&bind, BOUND).await;
    provisioned.fixture.restore_same_cluster().await;
    let server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server three");

    // Bob logs in again; the restore rewinds the reconnect log, so his session
    // is revoked and the new one asks about his retained rows.
    let bob2 = connect_client(
        addr,
        fresh_login(&base, APP_BOB, "bob"),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut bob_events = bob2.client().events();
    wait_for_certificate(&bob2, BOUND)
        .await
        .expect("bob holds his device");

    // The ask answers Pending, standing the rows as candidates from alice's
    // first entry on.
    let candidate = wait_pending_candidate(&bob2, &mut bob_events, &alice_key).await;
    // Whether alice's two notes queued as one entry or two depends on when her
    // pump flushed, so the candidate offers her entries from her first on.
    assert!(
        !candidate.seqs.is_empty()
            && candidate.seqs.iter().copied().eq(1..=candidate
                .seqs
                .len()
                .try_into()
                .expect("a handful of entries fits a u64")),
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

    // The server holds both notes under alice's account, each audited as
    // couriered by bob.
    assert_server_notes(&provisioned.fixture, &alice_id).await;
    bob2.observe_courier_audit(&alice_key, &report.settled, BOUND)
        .await
        .expect("the couriered audit receipts commit");
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

    // The answer ends the candidacy, and bob hears it.
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

    bob2.close().await;
    drop(bob2);
    drop(server);
}

/// Poll bob's provisional records until he holds `count` of them.
async fn wait_provisional_count(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    count: usize,
) {
    let until = Instant::now() + BOUND;
    loop {
        let rows = client.provisional_rows().await.expect("the rows are read");
        if rows.len() == count {
            return;
        }
        assert!(
            Instant::now() < until,
            "alice's notes never applied on bob, the records hold {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll bob's records until none of them awaits a verdict, the link's offer
/// and his session's asks having adjudicated them.
async fn wait_adjudicated(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
) {
    let until = Instant::now() + BOUND;
    loop {
        let rows = client.provisional_rows().await.expect("the rows are read");
        if rows
            .iter()
            .all(|row| row.state != ProvisionalState::Provisional)
        {
            return;
        }
        assert!(
            Instant::now() < until,
            "bob's records never adjudicated, they hold {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// What bob's event stream says about `author`'s candidacy.
enum Candidacy {
    Pending(CourierCandidate),
    Refused,
    Lagged,
}

/// Wait for `author`'s candidate carrying the ask's `Pending` answer, from the
/// state at hand once `events` is subscribed and then from `CourierCandidates`
/// events, which also fire when only the verdict changes. Panics on the ask
/// refusal that keeps the rows out of candidacy. `BOUND` only detects a run
/// that never gets there.
async fn wait_pending_candidate(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    author: &[u8],
) -> CourierCandidate {
    let pending = |candidate: &CourierCandidate| {
        candidate.author == author && candidate.last == Some(Verdict::Pending)
    };
    let deadline = tokio::time::Instant::now() + BOUND;
    loop {
        if let Some(candidate) = client
            .courier_candidates()
            .await
            .expect("the candidates are read")
            .into_iter()
            .find(|candidate| pending(candidate))
        {
            return candidate;
        }
        let next = tokio::time::timeout_at(deadline, async {
            loop {
                match events.recv().await {
                    Ok(ClientEvent::CourierCandidates(candidate)) if pending(&candidate) => {
                        return Candidacy::Pending(candidate);
                    }
                    Ok(ClientEvent::NonFatal { detail, .. })
                        if detail == "provisional ask refused" =>
                    {
                        return Candidacy::Refused;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        return Candidacy::Lagged;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        panic!("bob's event stream closed before the candidate stood");
                    }
                }
            }
        })
        .await;
        let refused = match next {
            Ok(Candidacy::Pending(candidate)) => return candidate,
            // A lagged stream lost events, so the state is read again.
            Ok(Candidacy::Lagged) => continue,
            Ok(Candidacy::Refused) => true,
            Err(_elapsed) => false,
        };
        let rows = client
            .provisional_rows()
            .await
            .expect("bob's rows are read");
        let candidates = client
            .courier_candidates()
            .await
            .expect("bob's candidates are read");
        assert!(
            !refused,
            "the restored server refused the retained ask, so the rows never \
             become candidates; bob holds {rows:?}, his candidates read {candidates:?}"
        );
        panic!(
            "no candidate for the author carried the Pending answer and no ask was \
             refused, bob holds {rows:?}, his candidates read {candidates:?}"
        );
    }
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

/// The mark a re-executed child carries for the retired-author scenario.
const RETIRED_PHASE: &str = "R78_COURIER_RETIRED_PHASE";

/// Alice's five notes across the two offline rounds, in write order.
const RETIRED_NOTES: [&str; 5] = [
    "the first retired note",
    "the second retired note",
    "the third retired note",
    "the fourth retired note",
    "the fifth retired note",
];

/// The team each note lands on, the shared T2 and alice's own T3.
const RETIRED_TEAMS: [&str; 5] = ["T2", "T3", "T2", "T3", "T2"];

diesel::table! {
    /// The stored peer entries, read raw because the typed schema does not
    /// name connetto's internals.
    _connetto_peer_entry (author, seq) {
        /// The author's key id.
        author -> Binary,
        /// The author's queue sequence.
        seq -> BigInt,
        /// What the entry is held as, a `HeldState`.
        state -> BigInt,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retired_authors_held_copies_leave_nothing_to_courier() {
    if std::env::var(RETIRED_PHASE).is_ok() {
        run_retired().await;
        return;
    }
    // A headless desktop's Secret Service dismisses its unlock prompt, so the
    // scenario runs in a child whose keyring detection settles on sealed
    // files, as the redelivery proof does.
    let dir = tempdir().expect("a scratch dir");
    let credentials = dir.path().join("credentials");
    let state = dir.path().join("state");
    std::fs::create_dir_all(&credentials).expect("the credentials dir");
    std::fs::create_dir_all(&state).expect("the state dir");
    std::fs::write(credentials.join("connetto.wrap-key"), [1_u8; 32]).expect("the wrap key");
    let output = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "courier_restore::a_courier_retired_phase",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("STATE_DIRECTORY")
        .env("CREDENTIALS_DIRECTORY", &credentials)
        .env("STATE_DIRECTORY", &state)
        .env(RETIRED_PHASE, "run")
        .output()
        .expect("spawn the phase");
    assert!(
        output.status.success(),
        "the retired-author scenario failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The scenario, the phase the driver re-executes in a child process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the retired-author driver runs it in a child process"]
async fn a_courier_retired_phase() {
    if std::env::var(RETIRED_PHASE).is_err() {
        return;
    }
    run_retired().await;
}

/// One assertion per step, in the order alice's notes travel to their ruling.
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per assertion, in the order the notes travel"
)]
async fn run_retired() {
    let _keyring = isolated_session_keyring();

    let provisioned = provision(&DEMO_DEPLOYMENT, "courier-retired", None)
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
    provisioned
        .fixture
        .exec(&format!(
            "INSERT INTO team_members (team_id, member_id) VALUES ('T3', '{alice_id}')"
        ))
        .await;

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

    // Step one: alice keeps no acked retention, so a rewind her retained run
    // cannot cover retires her writer instead of re-sending. Her sync wire
    // runs through a relay the test cuts to take her offline alone.
    let (alice_relay, relay) = relay_to(addr).await;
    let alice = connect_zero_retention(
        alice_relay,
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
        ("T3".to_owned(), alice_id.clone()),
    ];
    assert!(wait_roster(&alice, &all).await, "alice synced the roster");
    assert!(wait_roster(&bob, &all).await, "bob synced the roster");
    let alice_key = alice_cert.identity().key().as_bytes().to_vec();
    let writer = WriterRef::Device(
        alice_key
            .as_slice()
            .try_into()
            .expect("the device key is 32 bytes"),
    );

    // Step two: the deployment takes its backup, then the server goes away.
    provisioned.fixture.dump_backup().await;
    drop(server);
    wait_until_closed(&bind, BOUND).await;

    // Step three: n1 and n2 queue offline, one entry per note.
    write_noted(&alice, &writer, &alice_id, 0).await;
    write_noted(&alice, &writer, &alice_id, 1).await;

    // Step four: both notes apply on bob, the policy running as alice.
    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener");
    bob.link_peer(alice_addr).await.expect("bob links alice");
    wait_held(
        &bob,
        &alice_key,
        &[(1, HeldState::Provisional), (2, HeldState::Provisional)],
    )
    .await;
    // n2 stands applied as alice, a T3 member, and bob's view hides it,
    // the read answering as bob, who holds no T3.
    assert_bodies(&bob, &[0]).await;

    // Step five: the acks settle, and the re-presented offer adjudicates the
    // rows retained.
    let server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server two");
    assert!(
        wait_unsynced_empty(&alice).await,
        "alice's queue settles on the redial"
    );
    wait_adjudicated(&bob).await;
    wait_held(
        &bob,
        &alice_key,
        &[(1, HeldState::Retained), (2, HeldState::Retained)],
    )
    .await;
    assert_retired_notes(&provisioned.fixture, &alice_id, &[0, 1]).await;

    // Step six: alice loses the network, her T3 membership goes on the
    // server, so only bob's replica drops it, and she writes n3 to n5.
    relay.abort();
    let _ = relay.await;
    provisioned
        .fixture
        .exec(&format!(
            "DELETE FROM team_members WHERE team_id = 'T3' AND member_id = '{alice_id}'"
        ))
        .await;
    assert!(
        wait_roster(&bob, &all[..4]).await,
        "bob's replica loses alice's T3 membership"
    );
    assert!(
        wait_roster(&alice, &all).await,
        "alice's replica keeps her T3 membership offline"
    );
    drop(server);
    wait_until_closed(&bind, BOUND).await;
    write_noted(&alice, &writer, &alice_id, 2).await;
    write_noted(&alice, &writer, &alice_id, 3).await;
    write_noted(&alice, &writer, &alice_id, 4).await;

    // Step seven: on bob the policy checks n4 as alice, who holds no T3 there.
    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener again");
    bob.link_peer(alice_addr)
        .await
        .expect("bob links alice again");
    wait_held(
        &bob,
        &alice_key,
        &[
            (1, HeldState::Retained),
            (2, HeldState::Retained),
            (3, HeldState::Provisional),
            (4, HeldState::CourierOnly),
            (5, HeldState::Provisional),
        ],
    )
    .await;
    // n2 stands hidden from bob's view, and n4 refused on his replica.
    assert_bodies(&bob, &[0, 2, 4]).await;

    // Step eight: both devices go, the database restores, the server returns.
    drop(alice_watches);
    alice.close().await;
    drop(alice);
    drop(bob_watches);
    bob.close().await;
    drop(bob);
    provisioned.fixture.restore_same_cluster().await;
    let server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server three");

    // Step nine: bob signs in fresh while alice is offline, so the old
    // writer is not retired yet; the retained ask answers Pending and
    // stands T(1), T(2) as marked candidates.
    let bob2 = connect_client(
        addr,
        fresh_login(&base, APP_BOB, "bob"),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut bob_events = bob2.client().events();
    wait_for_certificate(&bob2, BOUND)
        .await
        .expect("bob holds his device");
    let candidate = wait_pending_candidate(&bob2, &mut bob_events, &alice_key).await;
    assert!(
        candidate.seqs.contains(&3) && candidate.seqs.contains(&4) && candidate.seqs.contains(&5),
        "n3, n4 and n5 stand as candidates, the candidate is {candidate:?}"
    );
    bob2.close().await;

    // Step ten: alice signs in fresh; the restored frontier 0 retires the
    // old writer, n3 to n5 settle under the writer the product picks.
    let alice2 = connect_zero_retention(
        addr,
        fresh_login(&base, APP_ALICE, "alice"),
        dir_a.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut alice_events = alice2.client().events();
    let retired = next_event(&mut alice_events, BOUND, |event| {
        matches!(
            event,
            ClientEvent::NonFatal { detail, .. }
                if detail == "the device's writer retired, a fresh key enrolls"
        )
    })
    .await;
    assert!(
        retired.is_some(),
        "the old writer never retired, so the fresh key enrolls over it"
    );
    let _fresh_key = wait_fresh_certificate(&alice2, &alice_key).await;
    let old = wait_writer_retired(&provisioned.fixture, &mut alice_events, &alice_key).await;
    assert_eq!(
        old.frontier, 0,
        "the restored server holds the old key at frontier 0"
    );
    assert_retired_notes(&provisioned.fixture, &alice_id, &[2, 3, 4]).await;
    assert!(
        wait_unsynced_empty(&alice2).await,
        "alice's fresh queue settles behind the applies"
    );

    // Step eleven: bob's resumed session asks again, and the retired writer's Abandoned takes alice's retained entries 1 and 2.
    let bob3 = connect_client(
        addr,
        fresh_login(&base, APP_BOB, "bob"),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut resumed = bob3.client().events();
    wait_for_certificate(&bob3, BOUND)
        .await
        .expect("bob holds his device");
    wait_held_on_events(&bob3, &mut resumed, &alice_key, &[]).await;
    let nothing = bob3.courier(&alice_key).await.expect_err("nothing is held");
    assert!(
        matches!(nothing, CourierError::NothingHeld),
        "bob offers nothing of the retired writer, the call spelled {nothing:?}"
    );
    assert_retired_notes(&provisioned.fixture, &alice_id, &[2, 3, 4]).await;
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
    assert!(
        couriered.is_empty(),
        "no write stands couriered, the audit holds {couriered:?}"
    );

    bob3.close().await;
    drop(bob2);
    drop(server);
}

/// The signed-in demo build with zero acked retention, the tuning set before
/// the sign-in fork the setter leaves behind.
async fn connect_zero_retention(
    addr: SocketAddr,
    sign_in: KeyringAuth,
    dir: &Path,
    root: &[u8],
    peer_listener: SocketAddr,
) -> connetto_client::NativeClient<connetto_client::NativeTransport> {
    NativeClientBuilder::new(format!("ws://{addr}/sync"), bundle())
        .with_tuning(SyncTuning::default().with_acked_retention(Duration::ZERO))
        .signed_in(sign_in)
        .durable(dir)
        .with_deployment_roots(vec![root.to_owned()])
        .with_peer_listener(peer_listener)
        .with_peer_autolink(false)
        .connect()
        .await
        .expect("connects")
}

/// A loopback relay to `server` and the task that runs it. Aborting the task
/// drops the listener and every relayed connection, so the client dialing the
/// relay loses the network while every other client keeps it.
async fn relay_to(server: SocketAddr) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the relay");
    let addr = listener.local_addr().expect("the relay address");
    let task = tokio::spawn(async move {
        let mut links = tokio::task::JoinSet::new();
        while let Ok((mut client, _)) = listener.accept().await {
            links.spawn(async move {
                if let Ok(mut upstream) = tokio::net::TcpStream::connect(server).await {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                }
            });
        }
    });
    (addr, task)
}

/// Alice's note of index `index`, queued as its own entry under `writer`.
async fn write_noted(
    alice: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    writer: &WriterRef,
    author: &str,
    index: usize,
) {
    write_note(alice, RETIRED_TEAMS[index], author, RETIRED_NOTES[index]).await;
    let seq = u64::try_from(index).expect("a note index of five fits a u64");
    wait_unsynced(alice, *writer, seq + 1).await;
}

/// Poll the queue until entry `through` stands under `writer`, bounded by
/// `BOUND`, so each note queues as its own entry.
async fn wait_unsynced(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    writer: WriterRef,
    through: u64,
) {
    let until = Instant::now() + BOUND;
    loop {
        let seqs: Vec<u64> = client
            .client()
            .unsynced()
            .await
            .expect("the queue is read")
            .into_iter()
            .filter(|write| write.writer == writer)
            .map(|write| write.seq)
            .collect();
        if seqs.iter().any(|seq| *seq >= through) {
            return;
        }
        assert!(
            Instant::now() < until,
            "entry {through} never queued under the writer, the queue holds {seqs:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The stored peer entries of `author` the replica holds, sequence order.
async fn held_rows(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    author: &[u8],
) -> Vec<(i64, HeldState)> {
    let rows: Vec<(i64, i64)> = client
        .client()
        .with_conn(|conn| {
            _connetto_peer_entry::table
                .select((_connetto_peer_entry::seq, _connetto_peer_entry::state))
                .filter(_connetto_peer_entry::author.eq(author))
                .order(_connetto_peer_entry::seq.asc())
                .load(conn.conn())
                .expect("the held rows are read")
        })
        .await
        .expect("the read runs");
    rows.into_iter()
        .map(|(seq, state)| (seq, held_state(state)))
        .collect()
}

/// Wait until `author`'s held rows stand `expected`, re-reading them after every event of `events`, bounded by `BOUND`.
async fn wait_held_on_events(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    author: &[u8],
    expected: &[(i64, HeldState)],
) {
    let deadline = tokio::time::Instant::now() + BOUND;
    loop {
        let rows = held_rows(client, author).await;
        if rows == expected {
            return;
        }
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("the event stream closed while the replica held {rows:?}")
            }
            Err(elapsed) => {
                panic!(
                    "the held rows never stood {expected:?} ({elapsed}), the replica holds {rows:?}"
                )
            }
        }
    }
}

/// The held state a stored value names, panicking with the value when it
/// spells none.
fn held_state(state: i64) -> HeldState {
    if state == HeldState::Provisional.db() {
        HeldState::Provisional
    } else if state == HeldState::CourierOnly.db() {
        HeldState::CourierOnly
    } else if state == HeldState::DigestOnly.db() {
        HeldState::DigestOnly
    } else if state == HeldState::Retained.db() {
        HeldState::Retained
    } else {
        panic!("the held row spells no state the table defines: {state}")
    }
}

/// Poll `author`'s held rows until they stand exactly `expected`, bounded by
/// `BOUND`.
async fn wait_held(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    author: &[u8],
    expected: &[(i64, HeldState)],
) {
    let until = Instant::now() + BOUND;
    loop {
        let rows = held_rows(client, author).await;
        if rows.len() == expected.len()
            && rows
                .iter()
                .zip(expected.iter())
                .all(|((seq, state), (want_seq, want_state))| {
                    seq == want_seq && state == want_state
                })
        {
            return;
        }
        assert!(
            Instant::now() < until,
            "the held rows never stood as the plan names them, the replica holds {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The replica's note bodies stand exactly the given notes, each once,
/// polled until the applies and prunes land.
async fn assert_bodies(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    notes: &[usize],
) {
    let mut expected: Vec<String> = notes
        .iter()
        .map(|index| RETIRED_NOTES[*index].to_owned())
        .collect();
    expected.sort();
    let until = Instant::now() + BOUND;
    loop {
        let actual = note_bodies(client).await;
        if actual == expected {
            return;
        }
        assert!(
            Instant::now() < until,
            "the replica's notes never stood the expected rows, it holds {actual:?}, expected {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The server's notes stand exactly the given notes under `alice`, each once,
/// polled until the settles land.
async fn assert_retired_notes(
    fixture: &connetto_test_harness::Fixture,
    alice: &str,
    notes: &[usize],
) {
    let until = Instant::now() + BOUND;
    loop {
        let mut conn = fixture.admin().get().await.expect("admin connection");
        let rows: Vec<(String, String, String)> = diesel_async::RunQueryDsl::load(
            team_notes::table
                .select((team_notes::team_id, team_notes::author_id, team_notes::body))
                .order(team_notes::body.asc()),
            &mut *conn,
        )
        .await
        .expect("read the notes");
        let mut expected: Vec<(String, String, String)> = notes
            .iter()
            .map(|index| {
                (
                    RETIRED_TEAMS[*index].to_owned(),
                    alice.to_owned(),
                    RETIRED_NOTES[*index].to_owned(),
                )
            })
            .collect();
        expected.sort_by(|a, b| a.2.cmp(&b.2));
        if rows == expected {
            return;
        }
        assert!(
            Instant::now() < until,
            "the server never held the named notes under alice, it holds {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the device certificate until it names a key other than `stale`,
/// bounded by `BOUND`.
async fn wait_fresh_certificate(
    client: &connetto_client::NativeClient<connetto_client::NativeTransport>,
    stale: &[u8],
) -> Vec<u8> {
    let until = Instant::now() + BOUND;
    loop {
        if let Some(cert) = client.device_certificate() {
            let key = cert.identity().key().as_bytes().to_vec();
            if key != *stale {
                return key;
            }
        }
        assert!(
            Instant::now() < until,
            "no fresh device key enrolled, the device still holds {stale:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The writer row a device key stands behind, read as admin.
#[derive(Clone, Debug)]
struct WriterFacts {
    /// The frontier it stands at.
    frontier: i64,
    /// Whether it stands stamped retired.
    retired: bool,
}

async fn writer_row(
    fixture: &connetto_test_harness::Fixture,
    device_key: &[u8],
) -> Vec<WriterFacts> {
    let mut conn = fixture.admin().get().await.expect("admin connection");
    diesel_async::RunQueryDsl::load(
        _connetto_writers::table
            .filter(_connetto_writers::device_key.eq(device_key.to_vec()))
            .select((
                _connetto_writers::frontier,
                _connetto_writers::retired_at.is_not_null(),
            )),
        &mut *conn,
    )
    .await
    .expect("read the writer rows")
    .into_iter()
    .map(|(frontier, retired)| WriterFacts { frontier, retired })
    .collect()
}

/// Read the retired writer after the server answers its retirement request.
async fn wait_writer_retired(
    fixture: &connetto_test_harness::Fixture,
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    device_key: &[u8],
) -> WriterFacts {
    next_event(events, BOUND, |event| {
        matches!(
            event,
            ClientEvent::NonFatal { detail, .. }
                if detail == "a writer retirement was answered"
        )
    })
    .await
    .expect("the retirement request is answered");
    let rows = writer_row(fixture, device_key).await;
    let [row]: [WriterFacts; 1] = rows
        .try_into()
        .unwrap_or_else(|rows| panic!("one writer must name the retired device, got {rows:?}"));
    assert!(row.retired, "the answered writer is retired");
    row
}
