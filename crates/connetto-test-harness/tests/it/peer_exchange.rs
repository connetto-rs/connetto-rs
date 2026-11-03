//! R77 slice 6d proof, the handheld shape in one process. Two signed-in
//! native clients of different accounts run against the demo deployment the
//! real `connetto-server` binary serves, and the peer link carries what the
//! server cannot while it is away.
//!
//! Both accounts sit in two teams whose roster the server gates the note
//! rows on. The server goes away for both. Alice writes a note the team still
//! settles, a row of a table the team cannot read from the server, and a note
//! the team stops settling. Bob links her offline and applies the writes that
//! pass his policies as her. An operator removes her from the first team
//! while both stay offline. Both come back. The server refuses the note the
//! removal affected, the settled note reaches bob from the server and
//! confirms the provisional apply, and the settled frontier plus the
//! retraction cross the re-linked peer link, adjudicating the uncovered row
//! and rolling the refused one back on both replicas.
//!
//! Needs Docker, since the fixture starts its own Postgres and authorization
//! service and the login walks a containerised provider.
#![expect(
    clippy::large_futures,
    reason = "the phase drives two full client lifecycles, its futures are large by design"
)]

use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};

use connetto_client::{
    ClientEvent,
    provisional::{ProvisionalRow, ProvisionalState},
};
use connetto_core::exchange::RetractCause;
use connetto_core::messages::MutationRejectReason;
use connetto_server::authn::identity::deterministic_uuid;
use connetto_test_harness::stack::{
    DEMO_DEPLOYMENT, demo_device_ca, ensure_server_bin, provision, provision_enrolment_tables,
    spawn_server, wait_until_closed,
};
use connetto_test_harness::{MockOauth, isolated_session_keyring};
use diesel::prelude::*;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::Pool;
use tempfile::tempdir;

use crate::demo::*;

/// The mark a re-executed child carries, absent when it is the driver.
const PHASE: &str = "R77_PEER_EXCHANGE_PHASE";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offline_peer_exchange_confirms_adjudicates_and_retracts() {
    if std::env::var(PHASE).is_ok() {
        run_exchange().await;
        return;
    }
    // The builder's `.keyring()` store is detected, and a headless desktop's
    // Secret Service dismisses its unlock prompt, so the exchange runs in a
    // child whose keyring auto-detection settles on sealed files.
    let dir = tempdir().expect("a scratch dir");
    let credentials = dir.path().join("credentials");
    let state = dir.path().join("state");
    std::fs::create_dir_all(&credentials).expect("the credentials dir");
    std::fs::create_dir_all(&state).expect("the state dir");
    std::fs::write(credentials.join("connetto.wrap-key"), [1_u8; 32]).expect("the wrap key");
    let output = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "peer_exchange::an_offline_peer_exchange_phase",
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
        "the exchange failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The exchange itself, the phase the driver re-executes in a child process
/// whose keyring auto-detection settles on sealed files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the exchange driver runs it in a child process"]
async fn an_offline_peer_exchange_phase() {
    if std::env::var(PHASE).is_err() {
        return;
    }
    run_exchange().await;
}

/// The scenario, one assertion per step in the order the removal crosses the
/// links.
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per assertion, in the order the removal crosses the links"
)]
async fn run_exchange() {
    let _keyring = isolated_session_keyring();

    let provisioned = provision(&DEMO_DEPLOYMENT, "peer-exchange", None)
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

    let dir_a = tempdir().expect("a dir");
    let dir_b = tempdir().expect("b dir");
    let peer_listener: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let addr: SocketAddr = bind.parse().expect("address");

    // The user id is a deterministic UUID v5 over the provider's issuer and
    // the login subject, so the roster is seeded before either first snapshot.
    let issuer = idp.issuer().to_owned();
    let alice_id = deterministic_uuid(&issuer, "alice").to_string();
    let bob_id = deterministic_uuid(&issuer, "bob").to_string();
    seed_roster(&provisioned.fixture, &alice_id, &bob_id).await;

    // Each account signs in once to remember its watches, then the build the
    // phase drives re-announces them on attach and resnapshots the roster.
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
    // The session's id proves the deterministic seed named the rows right.
    assert_eq!(
        wait_session(&alice).await,
        alice_id.clone(),
        "alice's id matches the seed"
    );
    assert_eq!(
        wait_session(&bob).await,
        bob_id.clone(),
        "bob's id matches the seed"
    );

    // The tables each replica follows, the handles held until the phase ends
    // so the subscriptions stay declared.
    let alice_watches = watch_tables(&alice).await;
    let bob_watches = watch_tables(&bob).await;
    let alice_orders = alice
        .client()
        .watch::<_, (String, i64)>(
            orders::table
                .select((orders::owner_id, orders::quantity))
                .order(orders::owner_id.asc()),
        )
        .await
        .expect("the orders follow");
    let alice_cert = wait_for_certificate(&alice, BOUND)
        .await
        .expect("alice enrols");
    let bob_cert = wait_for_certificate(&bob, BOUND).await.expect("bob enrols");
    let all = [
        ("T1".to_owned(), alice_id.clone()),
        ("T1".to_owned(), bob_id.clone()),
        ("T2".to_owned(), alice_id.clone()),
        ("T2".to_owned(), bob_id.clone()),
    ];
    let alice_ok = wait_roster(&alice, &all).await;
    assert!(alice_ok, "alice synced the roster");
    let bob_ok = wait_roster(&bob, &all).await;
    assert!(bob_ok, "bob synced the roster");
    let _ = bob_cert;

    // The server goes away for both, the live sessions among them.
    drop(server);
    wait_until_closed(&bind, BOUND).await;

    // Alice writes the three rows offline, the confirmed note first.
    write_note(&alice, "T2", &alice_id, "the team row").await;
    write_order(&alice, &alice_id, 1).await;
    write_note(&alice, "T1", &alice_id, "the removed row").await;
    let t2_note_id = read_note_id(&alice, "T2", "the team row").await;
    let t1_note_id = read_note_id(&alice, "T1", "the removed row").await;

    // Bob links alice while both stay offline and applies the writes that
    // pass his policies as her.
    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener");
    // The receiver is set before the link, since the offer lands and applies
    // within the link, and its events would otherwise precede the receiver.
    let mut bob_events = bob.client().events();
    let linked = bob.link_peer(alice_addr).await.expect("bob links alice");
    assert_eq!(
        linked.key(),
        alice_cert.identity().key(),
        "the link answers alice's identity"
    );
    // The rows land while the link is still open, so read them from the
    // replica, and correlate the note rows by the ids alice's writes minted.
    let mut rows: Vec<ProvisionalRow> = Vec::new();
    let started = Instant::now();
    while rows.len() < 3 && Instant::now() < started + BOUND {
        rows = bob.provisional_rows().await.expect("the rows are read");
        if rows.len() < 3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(rows.len(), 3, "all three of alice's entries apply");
    let mut tables: Vec<String> = rows.iter().map(|row| row.table.clone()).collect();
    tables.sort();
    assert_eq!(
        tables,
        vec!["orders", "team_notes", "team_notes"],
        "the three entries are the note pair and the order"
    );
    // The notes are correlated to the ids alice's writes minted, so the
    // later confirmations and retractions are checked against the exact keys.
    let t2_key = key_of(&rows, &t2_note_id);
    let t1_key = key_of(&rows, &t1_note_id);
    let alice_key = alice_cert.identity().key().as_bytes().to_vec();
    for row in &rows {
        assert_eq!(row.author, alice_key, "the record names alice's author");
    }
    assert!(
        rows.iter()
            .all(|row| row.state == ProvisionalState::Provisional),
        "the applies all await a verdict"
    );
    // Bob's app hears each applied entry with no server reachable, one event
    // per entry of alice's that the records name.
    hear_applies(&mut bob_events, &rows, &alice_key).await;

    // The eviction pass runs on bob's replica, and the provisional rows
    // survive it while they await a verdict.
    bob.client().tidy().await.expect("the tidy pass runs");
    let rows = bob
        .provisional_rows()
        .await
        .expect("the rows survive the pass");
    assert_eq!(
        rows.len(),
        3,
        "the provisional rows survive the eviction pass"
    );

    // The operator removes alice from T1 while both stay offline.
    provisioned
        .fixture
        .exec(&format!(
            "DELETE FROM team_members WHERE team_id = 'T1' AND member_id = '{alice_id}'"
        ))
        .await;

    // The watches let go and the rebuilt replicas re-announce the remembered
    // subscriptions on their attach, inside the grace.
    drop(alice_orders);
    drop(alice_watches);
    drop(bob_watches);
    alice.close().await;
    drop(alice);
    bob.close().await;
    drop(bob);

    // The server comes back.
    let _server = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server two");
    let remaining = [
        ("T1".to_owned(), bob_id.clone()),
        ("T2".to_owned(), alice_id.clone()),
        ("T2".to_owned(), bob_id.clone()),
    ];

    // Bob reconnects first, his snapshot settling on the removed row gone.
    let bob2 = connect_client(
        addr,
        last_used(&base, APP_BOB),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;
    // The receiver is set before alice's push, so the note that confirms the
    // provisional apply lands on it the moment it reaches bob.
    let mut bob_events = bob2.client().events();
    wait_for_certificate(&bob2, BOUND)
        .await
        .expect("bob holds his device");
    assert!(
        wait_roster(&bob2, &remaining).await,
        "bob's snapshot dropped the removed row"
    );

    // Alice reconnects and pushes the offline queue.
    let alice2 = connect_client(
        addr,
        last_used(&base, APP_ALICE),
        dir_a.path(),
        &root,
        peer_listener,
    )
    .await;
    wait_for_certificate(&alice2, BOUND)
        .await
        .expect("alice holds her device");
    let mut alice_events = alice2.client().events();
    expect_removed_note_refusal(
        &mut alice_events,
        &alice2,
        &remaining,
        &["the team row".to_owned()],
    )
    .await;

    // Bob re-links alice, and her next offer carries the frontier and the
    // retraction.
    let alice_addr = wait_peer_address(&alice2, BOUND)
        .await
        .expect("alice serves her peer listener again");
    let linked = bob2
        .link_peer(alice_addr)
        .await
        .expect("bob re-links alice");
    assert_eq!(
        linked.key(),
        alice_cert.identity().key(),
        "the link answers alice's identity again"
    );
    let mut confirmed = None;
    let mut retracted = None;
    while confirmed.is_none() || retracted.is_none() {
        let event = next_event(&mut bob_events, BOUND, |event| {
            matches!(event, ClientEvent::ProvisionalConfirmed { .. })
                || matches!(event, ClientEvent::ProvisionalRetracted { .. })
        })
        .await;
        match event {
            Some(ClientEvent::ProvisionalConfirmed { rows }) => {
                confirmed = Some(rows);
            }
            Some(ClientEvent::ProvisionalRetracted {
                author,
                cause,
                rows,
                ..
            }) => {
                assert_eq!(
                    author,
                    alice_cert.identity().key().as_bytes().to_vec(),
                    "the retraction names alice"
                );
                retracted = Some((cause, rows));
            }
            None => panic!("the link never carried the confirmation and the retraction"),
            Some(_) => panic!("the link carried an unexpected event"),
        }
    }
    if let Some(rows) = &confirmed {
        let keys: Vec<_> = rows.iter().map(|row| row.key.clone()).collect();
        assert!(
            keys.contains(&t2_key),
            "the server's note confirms the provisional apply, confirmed {keys:?}"
        );
    }
    if let Some((cause, rows)) = &retracted {
        assert!(
            matches!(
                cause,
                RetractCause::Rejected(MutationRejectReason::Unauthorized)
            ),
            "the retraction names the refusal cause, it spelled {cause:?}"
        );
        let keys: Vec<_> = rows.iter().map(|row| row.1.clone()).collect();
        assert!(
            keys.contains(&t1_key),
            "the retraction inverts the refused note, it inverted {keys:?}"
        );
    }

    // The settled frontier adjudicates the orders row on bob's replica, the
    // row hidden from him by the orders policy.
    let until = Instant::now() + BOUND;
    loop {
        let rows = bob2
            .provisional_rows()
            .await
            .expect("read bob's provenance");
        if let Some(order) = rows.iter().find(|row| row.table == "orders")
            && order.state == ProvisionalState::Adjudicated
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "the frontier never adjudicated the orders row, the records hold {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Nothing double-applied. Bob's replica holds the confirmed note alone of
    // the notes, the adjudicated record alone of the records, and the refused
    // note nowhere.
    let rows = bob2
        .provisional_rows()
        .await
        .expect("read bob's final provenance");
    assert_eq!(
        rows.len(),
        1,
        "only the adjudicated record remains on bob, the records hold {rows:?}"
    );
    assert!(
        rows[0].table == "orders" && rows[0].state == ProvisionalState::Adjudicated,
        "the record is the adjudicated orders row"
    );
    assert!(
        alice2
            .provisional_rows()
            .await
            .expect("read alice's final provenance")
            .is_empty(),
        "alice holds no records, the confirmed and the retracted both settled"
    );
    assert_eq!(
        note_bodies(&bob2).await,
        vec!["the team row".to_owned()],
        "bob holds the confirmed note and the refused note nowhere"
    );
    assert!(
        orders_owners(&bob2).await.is_empty(),
        "bob's view hides the adjudicated order row"
    );
    assert_eq!(
        orders_owners(&alice2).await,
        vec![alice_id.clone()],
        "alice holds the order row she wrote"
    );

    // The server holds exactly what settled, the refused note among what it
    // never held.
    assert_postgres_final(provisioned.fixture.admin(), remaining.to_vec(), &alice_id).await;

    bob2.close().await;
    alice2.close().await;
    drop(bob2);
    drop(alice2);
}

/// The Postgres side after the exchange, `alice` the id her rows name.
async fn assert_postgres_final(
    pool: &Pool<AsyncPgConnection>,
    remaining: Vec<(String, String)>,
    alice: &str,
) {
    let mut conn = pool.get().await.expect("admin connection");
    let mut remaining = remaining;
    remaining.sort();
    let rows: Vec<(String, String)> = diesel_async::RunQueryDsl::load(
        team_members::table
            .select((team_members::team_id, team_members::member_id))
            .order((team_members::team_id.asc(), team_members::member_id.asc())),
        &mut *conn,
    )
    .await
    .expect("read the roster");
    assert_eq!(
        rows, remaining,
        "the server holds the roster the operator left"
    );
    let rows: Vec<(String, String, String)> = diesel_async::RunQueryDsl::load(
        team_notes::table
            .select((team_notes::team_id, team_notes::author_id, team_notes::body))
            .order(team_notes::body.asc()),
        &mut *conn,
    )
    .await
    .expect("read the notes");
    assert_eq!(
        rows,
        vec![("T2".to_owned(), alice.to_owned(), "the team row".to_owned())],
        "the server holds the settled note alone"
    );
    let owners: Vec<String> = diesel_async::RunQueryDsl::load(
        orders::table
            .select(orders::owner_id)
            .order(orders::owner_id.asc()),
        &mut *conn,
    )
    .await
    .expect("read the orders");
    assert_eq!(
        owners,
        vec![alice.to_owned()],
        "the server holds the order row"
    );
}
