//! R78 slice 6 proof, a courier's re-delivery to a restored server. The
//! deployment takes a backup, alice's device delivers two notes the server
//! acknowledges after the backup, bob's device holds them as retained beside
//! the offer alice's device re-presents with her settled frontier, alice's
//! device goes away for good, the database is restored to the backup, bob
//! logs in again, the full resync's retained ask answers his rows `Pending`
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
use std::time::{Duration, Instant, SystemTime};

use connetto_client::{ClientEvent, CourierCandidate, provisional::ProvisionalState};
use connetto_core::exchange::Verdict;
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
    // is revoked and his full resync ends owing the retained ask.
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
    // Alice's two offline notes queue as one entry (the outbox builds one
    // changeset under her writer), so the candidate offers her single entry.
    assert_eq!(
        candidate.seqs,
        [1],
        "alice's entry is offered from her first, the candidate is {candidate:?}"
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
