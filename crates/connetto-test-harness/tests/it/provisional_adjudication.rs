//! R77 slice 7 proof, the server settling the rows of an author the receiver
//! never meets again. Two signed-in native clients of different accounts run
//! against the demo deployment the real `connetto-server` binary serves. The
//! server goes away, the peer link carries alice's writes to bob, and they
//! stand provisional there. The server applies the order and refuses the note
//! the team stopped settling when alice alone comes back, and bob's ask
//! settles both through the verdicts alone, no link to alice. The server goes
//! away again, alice writes a note bob applies over the link and never comes
//! back, the operator revokes her enrolment, and bob's next session hears the
//! note abandoned.
//!
//! Needs Docker, since the fixture starts its own Postgres and authorization
//! service and the login walks a containerised provider.
#![expect(
    clippy::large_futures,
    reason = "the phase drives several client lifecycles, its futures are large by design"
)]

use std::fmt::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};

use connetto_client::{
    ClientEvent, NativeClient, NativeTransport,
    provisional::{ProvisionalRow, ProvisionalState},
};
use connetto_core::exchange::RetractCause;
use connetto_core::messages::MutationRejectReason;
use connetto_server::authn::identity::deterministic_uuid;
use connetto_server::defaults::{_connetto_settled, _connetto_writers};
use connetto_server::writer_schema::SettledOutcome;
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
const PHASE: &str = "R77_PROVISIONAL_ADJUDICATION_PHASE";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_online_adjudication_confirms_and_retracts_through_the_verdicts() {
    if std::env::var(PHASE).is_ok() {
        run_adjudication().await;
        return;
    }
    // The builder's `.keyring()` store is detected, and a headless desktop's
    // Secret Service dismisses its unlock prompt, so the adjudication runs in
    // a child whose keyring auto-detection settles on sealed files.
    let dir = tempdir().expect("a scratch dir");
    let credentials = dir.path().join("credentials");
    let state = dir.path().join("state");
    std::fs::create_dir_all(&credentials).expect("the credentials dir");
    std::fs::create_dir_all(&state).expect("the state dir");
    std::fs::write(credentials.join("connetto.wrap-key"), [1_u8; 32]).expect("the wrap key");
    let output = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "provisional_adjudication::an_online_adjudication_phase",
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
        "the adjudication failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The adjudication itself, the phase the driver re-executes in a child
/// process whose keyring auto-detection settles on sealed files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the adjudication driver runs it in a child process"]
async fn an_online_adjudication_phase() {
    if std::env::var(PHASE).is_err() {
        return;
    }
    run_adjudication().await;
}

/// Poll the replica's provisional records until `table`'s record stands
/// adjudicated, bounded by `BOUND`.
async fn wait_adjudicated(client: &NativeClient<NativeTransport>, table: &str) {
    let until = Instant::now() + BOUND;
    loop {
        let rows = client.provisional_rows().await.expect("read the records");
        if rows
            .iter()
            .any(|row| row.table == table && row.state == ProvisionalState::Adjudicated)
        {
            return;
        }
        assert!(
            Instant::now() < until,
            "the verdict never adjudicated the {table} record, the records hold {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The author's settled records after the sweep of her revoked enrolment, the
/// applied one pruned and the refusal standing, bounded by `BOUND`.
async fn assert_the_refusal_survived_the_sweep(
    pool: &Pool<AsyncPgConnection>,
    alice_key: &[u8],
    applied_seq: i64,
    refused_seq: i64,
) {
    let mut conn = pool.get().await.expect("admin connection");
    let writer_query = _connetto_writers::table
        .filter(_connetto_writers::device_key.eq(alice_key.to_vec()))
        .select(_connetto_writers::writer);
    let writer: i64 = diesel_async::RunQueryDsl::get_result(writer_query, &mut conn)
        .await
        .expect("the author's writer stands, the sweep keeps its row");
    let expected = SettledOutcome::Rejected(MutationRejectReason::Unauthorized).json();
    let until = Instant::now() + BOUND;
    loop {
        let applied_query = _connetto_settled::table
            .filter(_connetto_settled::writer.eq(writer))
            .filter(_connetto_settled::seq.eq(applied_seq))
            .select(_connetto_settled::outcome)
            .limit(1);
        let applied: Option<String> = diesel_async::RunQueryDsl::load(applied_query, &mut conn)
            .await
            .expect("read the applied record")
            .into_iter()
            .next();
        let refused_query = _connetto_settled::table
            .filter(_connetto_settled::writer.eq(writer))
            .filter(_connetto_settled::seq.eq(refused_seq))
            .select(_connetto_settled::outcome)
            .limit(1);
        let refused: Option<String> = diesel_async::RunQueryDsl::load(refused_query, &mut conn)
            .await
            .expect("read the refusal record")
            .into_iter()
            .next();
        if applied.is_none() && refused.as_deref() == Some(expected.as_str()) {
            return;
        }
        assert!(
            Instant::now() < until,
            "the sweep never pruned the applied record while the refusal stood, \
             the records hold {applied:?} and {refused:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The scenario, one assertion per step in the order the verdicts settle the
/// rows.
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per assertion, in the order the verdicts settle the rows"
)]
async fn run_adjudication() {
    let _keyring = isolated_session_keyring();

    let provisioned = provision(&DEMO_DEPLOYMENT, "provisional-adjudication", None)
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

    // Each account signs in once to remember its watches, then the builds the
    // phase drives re-announce them on attach and resnapshot the roster.
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

    // The tables each replica follows, the handles held until the window ends
    // so the subscriptions stay declared.
    let alice_watches = watch_tables(&alice).await;
    let bob_watches = watch_tables(&bob).await;
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
    assert!(wait_roster(&alice, &all).await, "alice synced the roster");
    assert!(wait_roster(&bob, &all).await, "bob synced the roster");
    let _ = bob_cert;

    // The server goes away for both, the live sessions among them.
    drop(server);
    wait_until_closed(&bind, BOUND).await;

    // Alice writes the two rows offline, the order her team cannot read
    // first, the flush between the writes queuing each as its own entry.
    write_order(&alice, &alice_id, 1).await;
    let unsent = alice
        .client()
        .unsynced()
        .await
        .expect("the order is queued");
    assert_eq!(unsent.len(), 1, "the order stands queued alone");
    write_note(&alice, "T1", &alice_id, "the removed row").await;
    let unsent = alice.client().unsynced().await.expect("the note is queued");
    let mut seqs: Vec<u64> = unsent.iter().map(|write| write.seq).collect();
    seqs.sort_unstable();
    assert_eq!(
        seqs,
        vec![1, 2],
        "the entries stand queued one per write, they hold {seqs:?}"
    );
    let t1_note_id = read_note_id(&alice, "T1", "the removed row").await;

    // Bob links alice while both stay offline and applies the writes as her.
    let alice_addr = wait_peer_address(&alice, BOUND)
        .await
        .expect("alice serves her peer listener");
    // The receiver is set before the link, since the offer lands and applies
    // within the link, and its events would otherwise precede the receiver.
    let mut bob_first_events = bob.client().events();
    let linked = bob.link_peer(alice_addr).await.expect("bob links alice");
    assert_eq!(
        linked.key(),
        alice_cert.identity().key(),
        "the link answers alice's identity"
    );
    // The rows land while the link is still open, so read them from the
    // replica, and correlate the note row by the id alice's write minted.
    let mut rows: Vec<ProvisionalRow> = Vec::new();
    let started = Instant::now();
    while rows.len() < 2 && Instant::now() < started + BOUND {
        rows = bob.provisional_rows().await.expect("the rows are read");
        if rows.len() < 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(rows.len(), 2, "both of alice's entries apply");
    let mut tables: Vec<String> = rows.iter().map(|row| row.table.clone()).collect();
    tables.sort();
    assert_eq!(
        tables,
        vec!["orders".to_owned(), "team_notes".to_owned()],
        "the two entries are the note and the order"
    );
    let t1_key = key_of(&rows, &t1_note_id);
    let order_seq = rows
        .iter()
        .find(|row| row.table == "orders")
        .expect("the order entry applied")
        .seq;
    let t1_seq = rows
        .iter()
        .find(|row| row.table == "team_notes")
        .expect("the note entry applied")
        .seq;
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
    hear_applies(&mut bob_first_events, &rows, &alice_key).await;

    // The eviction pass runs on bob's replica, and the provisional rows
    // survive it while they await a verdict.
    bob.client().tidy().await.expect("the tidy pass runs");
    let rows = bob
        .provisional_rows()
        .await
        .expect("the rows survive the pass");
    assert_eq!(
        rows.len(),
        2,
        "the provisional rows survive the eviction pass"
    );

    // The operator removes alice from T1 while both stay offline.
    provisioned
        .fixture
        .exec(&format!(
            "DELETE FROM team_members WHERE team_id = 'T1' AND member_id = '{alice_id}'"
        ))
        .await;

    // The watches let go and the closed builds stand down, alice never to
    // meet bob again over a link.
    drop(alice_watches);
    drop(bob_watches);
    alice.close().await;
    drop(alice);
    bob.close().await;
    drop(bob);

    // The server comes back.
    let server2 = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server two");
    let remaining = [
        ("T1".to_owned(), bob_id.clone()),
        ("T2".to_owned(), alice_id.clone()),
        ("T2".to_owned(), bob_id.clone()),
    ];

    // Alice reconnects alone and pushes the offline queue, the order standing
    // applied and the note refused.
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
    expect_removed_note_refusal(&mut alice_events, &alice2, &remaining, &[]).await;
    alice2.close().await;
    drop(alice2);

    // Bob reconnects with no link to alice, the verdicts his only answer.
    let bob2 = connect_client(
        addr,
        last_used(&base, APP_BOB),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut bob_verdict_events = bob2.client().events();
    wait_for_certificate(&bob2, BOUND)
        .await
        .expect("bob holds his device");
    assert!(
        wait_roster(&bob2, &remaining).await,
        "bob's snapshot dropped the removed row"
    );
    let retracted = next_event(&mut bob_verdict_events, BOUND, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { .. })
    })
    .await
    .expect("the verdicts carried the refusal to bob");
    if let ClientEvent::ProvisionalRetracted {
        author,
        seq,
        cause,
        rows,
    } = &retracted
    {
        assert_eq!(author, &alice_key, "the retraction names alice");
        assert_eq!(seq, &t1_seq, "the retraction names the note's sequence");
        assert!(
            matches!(
                cause,
                RetractCause::Rejected(MutationRejectReason::Unauthorized)
            ),
            "the retraction names the refusal cause, it spelled {cause:?}"
        );
        let keys: Vec<_> = rows.iter().map(|row| row.1.clone()).collect();
        assert_eq!(
            keys,
            vec![t1_key.clone()],
            "the retraction inverts the refused note alone, it inverted {keys:?}"
        );
    }

    // The verdict adjudicates the order record on bob's replica, the row his
    // policy hides from the server, and nothing retracted it.
    wait_adjudicated(&bob2, "orders").await;
    let rows = bob2
        .provisional_rows()
        .await
        .expect("read bob's provenance");
    assert_eq!(
        rows.len(),
        1,
        "only the adjudicated record remains on bob, the records hold {rows:?}"
    );
    assert!(
        rows[0].table == "orders" && rows[0].state == ProvisionalState::Adjudicated,
        "the record is the adjudicated orders row"
    );
    assert_eq!(
        note_bodies(&bob2).await,
        Vec::<String>::new(),
        "bob holds no note, the refused one retracted"
    );
    assert!(
        orders_owners(&bob2).await.is_empty(),
        "bob's view hides the adjudicated order row"
    );

    // The fresh build signs in while the server still stands. The sign-in
    // refreshes its stored session against the server, and a build cannot
    // start signed in while the server is away.
    let alice3 = connect_client(
        addr,
        last_used(&base, APP_ALICE),
        dir_a.path(),
        &root,
        peer_listener,
    )
    .await;
    let alice_addr = wait_peer_address(&alice3, BOUND)
        .await
        .expect("alice serves her peer listener again");
    wait_for_certificate(&alice3, BOUND)
        .await
        .expect("alice holds her device again");

    // The server goes away before the write, so the row never reaches it, and
    // the build stands down before the server returns.
    drop(server2);
    wait_until_closed(&bind, BOUND).await;
    write_note(&alice3, "T2", &alice_id, "the abandoned row").await;
    let unsent = alice3
        .client()
        .unsynced()
        .await
        .expect("the note is queued");
    assert_eq!(unsent.len(), 1, "the note stands queued alone");
    let abandoned_note_id = read_note_id(&alice3, "T2", "the abandoned row").await;

    // Bob applies the note over the link while the server stays away.
    let mut bob_apply_events = bob2.client().events();
    let linked = bob2
        .link_peer(alice_addr)
        .await
        .expect("bob links alice again");
    assert_eq!(
        linked.key(),
        alice_cert.identity().key(),
        "the link answers alice's identity again"
    );
    let mut rows: Vec<ProvisionalRow> = Vec::new();
    let started = Instant::now();
    while rows.iter().find(|row| row.table == "team_notes").is_none()
        && Instant::now() < started + BOUND
    {
        rows = bob2.provisional_rows().await.expect("the rows are read");
        if rows.iter().find(|row| row.table == "team_notes").is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let abandoned = rows
        .iter()
        .find(|row| row.table == "team_notes")
        .expect("the abandoned note applied");
    let abandoned_key = key_of(&rows, &abandoned_note_id);
    let abandoned_seq = abandoned.seq;
    assert_eq!(
        abandoned.author, alice_key,
        "the record names alice's author"
    );
    assert_eq!(
        abandoned.state,
        ProvisionalState::Provisional,
        "the apply awaits a verdict"
    );

    // Bob's app hears the apply with no server reachable.
    let applied = next_event(&mut bob_apply_events, BOUND, |event| {
        matches!(
            event,
            ClientEvent::ProvisionalApplied { author, seq, .. }
                if *author == alice_key && *seq == abandoned_seq
        )
    })
    .await
    .expect("bob's app hears the apply");
    if let ClientEvent::ProvisionalApplied { rows, .. } = &applied {
        let keys: Vec<_> = rows.iter().map(|row| row.key.clone()).collect();
        assert_eq!(
            keys,
            vec![abandoned_key.clone()],
            "the apply names the abandoned note alone, it named {keys:?}"
        );
    }

    // The operator revokes alice's enrolment, the same statement the server's
    // own revocation path runs when it marks a key revoked, which the
    // enrolment tests drive as `manager.revoke_device` in process.
    let mut hex = String::with_capacity(alice_key.len() * 2);
    for byte in &alice_key {
        write!(&mut hex, "{byte:02x}").expect("write the hex");
    }
    provisioned
        .fixture
        .exec(&format!(
            "UPDATE connetto_device_enrolments SET revoked_at = now() \
             WHERE key_id = '\\x{hex}'"
        ))
        .await;

    // The closed builds stand down and the server comes back, its sweep
    // pruned the author's applied records on the way.
    bob2.close().await;
    drop(bob2);
    alice3.close().await;
    drop(alice3);
    let server3 = spawn_server(&server_bin, &envs, &bind)
        .await
        .expect("server three");

    // Bob reconnects, and the verdicts retract the abandoned row.
    let bob3 = connect_client(
        addr,
        last_used(&base, APP_BOB),
        dir_b.path(),
        &root,
        peer_listener,
    )
    .await;
    let mut bob_abandon_events = bob3.client().events();
    wait_for_certificate(&bob3, BOUND)
        .await
        .expect("bob holds his device");
    assert!(
        wait_roster(&bob3, &remaining).await,
        "bob's snapshot stands as the operator left it"
    );
    let retracted = next_event(&mut bob_abandon_events, BOUND, |event| {
        matches!(event, ClientEvent::ProvisionalRetracted { .. })
    })
    .await
    .expect("the verdicts carried the abandonment to bob");
    if let ClientEvent::ProvisionalRetracted {
        author,
        seq,
        cause,
        rows,
    } = &retracted
    {
        assert_eq!(author, &alice_key, "the retraction names alice");
        assert_eq!(
            seq, &abandoned_seq,
            "the retraction names the note's sequence"
        );
        assert!(
            matches!(cause, RetractCause::Abandoned),
            "the retraction names the abandonment, it spelled {cause:?}"
        );
        let keys: Vec<_> = rows.iter().map(|row| row.1.clone()).collect();
        assert_eq!(
            keys,
            vec![abandoned_key.clone()],
            "the retraction inverts the abandoned note alone, it inverted {keys:?}"
        );
    }

    // The abandoned row is gone from bob's replica, and the order record
    // stands adjudicated from the first session's verdicts.
    let rows = bob3
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
    assert_eq!(
        note_bodies(&bob3).await,
        Vec::<String>::new(),
        "bob holds no note, the abandoned one retracted"
    );
    assert!(
        orders_owners(&bob3).await.is_empty(),
        "bob's view hides the adjudicated order row"
    );

    // The sweep pruned the applied record of the revoked enrolment while the
    // refusal stands, so a receiver that never met the author again still
    // learns the refusal from the server's records.
    assert_the_refusal_survived_the_sweep(
        provisioned.fixture.admin(),
        &alice_key,
        order_seq,
        t1_seq,
    )
    .await;

    // The server holds exactly what settled, the abandoned row among what it
    // never held.
    assert_postgres_final(provisioned.fixture.admin(), remaining.to_vec(), &alice_id).await;

    bob3.close().await;
    drop(bob3);
    drop(server3);
}

/// The Postgres side after the adjudication, `alice` the id her rows name.
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
    let notes: Vec<String> =
        diesel_async::RunQueryDsl::load(team_notes::table.select(team_notes::body), &mut *conn)
            .await
            .expect("read the notes");
    assert!(
        notes.is_empty(),
        "the server holds no note, the refused one refused and the abandoned one never pushed"
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
        "the server holds the order row alone"
    );
}
