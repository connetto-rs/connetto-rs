//! Docker-gated tests of one connection's courier stream (R78 decisions 1
//! and 28 to 32), every row of the plan's stream table.
//!
//! A courier opens a range with `CourierRange`, declaring how many
//! `CourierBody` frames follow, and the server settles each body as it lands
//! and answers once, after the last counted body. One range is open per
//! connection, a refused or stopped range drains its remaining bodies, and a
//! body no conforming client sends ends the connection with the entries
//! settled before it standing.
//!
//! Needs Docker: the fixture starts its own Postgres.

use super::courier_frontier::{
    Chain, FAULTED, Faulting, Manager, applied, audited_guard, bind_nonce_message, chain,
    courier_manager, manager_over, note, notes, offer, plain, roster, ruled, settle_directly,
    settled_count, sign, tomorrow, writing_manager,
};
use super::writer_shared::{Enrolments, insert_changeset, seed_notes, writer_for_key};
use connetto_core::PROTOCOL_VERSION;
use connetto_core::device_cert::KeyId;
use connetto_core::exchange::{CourierEntryOutcome, CourierOutcome, CourierRefusal, Verdict};
use connetto_core::messages::{
    BindDevice, EnrolGrant, Grant, Handshake, HandshakeAck, writer_bind_message,
};
use connetto_core::messages::{
    ControlMessage, CourierAnswer, CourierBody, CourierRange, FatalErrorReason, WriterOutcome,
    WriterRequest,
};
use connetto_core::traits::Transport;
use connetto_server::device_cert::DeviceCertConfig;
use connetto_server::{SessionConfig, SessionError, WebSocketTransport};
use connetto_test_harness::{Client, Fixture, RowValue, isolated_session_keyring};
use diesel_async::RunQueryDsl as _;
use rcgen::{KeyPair, PublicKeyData as _};
use tokio::net::{TcpListener, TcpStream};

use super::device_identity::{challenge_nonce, enrol_request, reply};
use super::e2e::{PG_SERIAL, mint_tokens, reset_fixture};
use super::enrolment::{device_key, rooted_issuer};
use super::lifecycle::{admin_pool, builder_over, next_control, served_major, wait_ready};

/// The manager every stream test drives, the courier path switched on.
fn courier_on(fixture: &Fixture, store: &Enrolments) -> Manager {
    let (_captured, guard) = audited_guard();
    writing_manager(
        fixture.admin(),
        guard,
        Some(store.installable()),
        SessionConfig::default().with_courier(true),
    )
}

/// A courier connection signed in as `user` with `key` bound as its device
/// writer, and the task serving it.
async fn courier_session(
    manager: &Manager,
    user: &str,
    key: &KeyPair,
    key_id: [u8; 32],
) -> (Client, tokio::task::JoinHandle<Result<(), SessionError>>) {
    let (mut client, server) = super::courier_frontier::loopback_client(manager);
    let (challenge, _) = client
        .handshake_challenge_writing(
            "courier",
            &[&format!("user:{user}")],
            None,
            WriterRequest::Device,
            None,
        )
        .await;
    let answer = client
        .bind_device(
            "bind",
            key_id,
            sign(key, &bind_nonce_message(&challenge.nonce)),
        )
        .await;
    assert!(matches!(answer.outcome, WriterOutcome::Bound { .. }));
    (client, server)
}

/// The `CourierRange` for `range` from `from` on, under the author's offer
/// of its newest entry, declaring `bodies`.
fn range_frame(
    request_id: &str,
    author: &KeyPair,
    author_id: [u8; 32],
    range: &Chain,
    from: u64,
    bodies: u32,
) -> CourierRange {
    CourierRange {
        request_id: request_id.to_owned(),
        author: author_id,
        offer: offer(author, author_id, range.newest()),
        rows: range
            .rows
            .iter()
            .filter(|row| row.seq >= from)
            .cloned()
            .collect(),
        bodies,
    }
}

/// The `CourierBody` of `range`'s row `seq`, compressed as the wire carries
/// it.
fn body_frame(request_id: &str, range: &Chain, seq: u64) -> CourierBody {
    let body = range
        .bodies
        .iter()
        .find(|body| body.seq == seq)
        .expect("the range holds the body");
    CourierBody {
        request_id: request_id.to_owned(),
        seq,
        changeset_zstd: zstd::encode_all(body.changeset.as_slice(), 0).expect("compress"),
    }
}

/// The range's one answer, the next control frame.
async fn answer(client: &mut Client) -> CourierAnswer {
    match client.next_control().await {
        ControlMessage::CourierAnswer(answer) => answer,
        other => panic!("expected the courier answer, got {other:?}"),
    }
}

/// The rows the answer settled or found settled.
fn answered(answer: CourierAnswer) -> Vec<(u64, CourierEntryOutcome)> {
    match answer.outcome {
        CourierOutcome::Entries(entries) => entries,
        CourierOutcome::Refused(refusal) => panic!("expected answered rows, got {refusal:?}"),
    }
}

/// Nothing answers before the barrier, so no `CourierAnswer` is on its way.
async fn no_answer_yet(client: &mut Client) {
    assert!(
        matches!(client.barrier(77).await, ControlMessage::Pong(_)),
        "no answer before the range's last body"
    );
}

/// The connection ends with a protocol violation naming `detail`.
async fn violated(
    mut client: Client,
    server: tokio::task::JoinHandle<Result<(), SessionError>>,
    detail: &str,
) {
    match client.next_control().await {
        ControlMessage::FatalError(fatal) => match fatal.reason {
            FatalErrorReason::ProtocolViolation { detail: told } => {
                assert_eq!(told, detail);
            }
            other => panic!("expected a protocol violation, got {other:?}"),
        },
        other => panic!("expected the fatal error, got {other:?}"),
    }
    let ended = server.await.expect("join the session");
    assert!(
        matches!(&ended, Err(SessionError::Protocol(told)) if told == detail),
        "the connection ends on the violation, got {ended:?}"
    );
}

/// Three notes of an author's chain from sequence 1.
fn three() -> Chain {
    chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
        plain(insert_changeset(4, "four", "t3")),
    ])
}

/// Four notes of an author's chain from sequence 1.
fn four() -> Chain {
    chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
        plain(insert_changeset(4, "four", "t3")),
        plain(insert_changeset(5, "five", "t4")),
    ])
}

/// A body for `seq` of the range `request_id` whose envelope does not
/// decompress.
fn unreadable(request_id: &str, seq: u64) -> CourierBody {
    CourierBody {
        request_id: request_id.to_owned(),
        seq,
        changeset_zstd: b"not zstd".to_vec(),
    }
}

/// A body for `seq` of the range `request_id` carrying another changeset
/// than its row's digest names.
fn mismatched(request_id: &str, seq: u64) -> CourierBody {
    CourierBody {
        request_id: request_id.to_owned(),
        seq,
        changeset_zstd: zstd::encode_all(insert_changeset(9, "nine", "t9").as_slice(), 0)
            .expect("compress"),
    }
}

/// The fixture, the store with alice's author key and bob's courier key,
/// and the manager with the courier path on.
async fn camp() -> (
    Fixture,
    Enrolments,
    (KeyPair, [u8; 32]),
    (KeyPair, [u8; 32]),
    Manager,
) {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let author = store.enrol("alice", tomorrow()).await;
    let courier = store.enrol("bob", tomorrow()).await;
    let manager = courier_on(&fixture, &store);
    (fixture, store, author, courier, manager)
}

/// A range's bodies settle as they land and the range answers after its
/// last counted body, after which the connection takes another range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_answers_after_its_last_body_and_the_connection_takes_another() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (author, author_id) = store.enrol("alice", tomorrow()).await;
    let (courier, courier_id) = store.enrol("bob", tomorrow()).await;
    let manager = courier_on(&fixture, &store);
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
    let range = three();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 2))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    no_answer_yet(&mut client).await;
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the first body settled as it landed"
    );
    client.courier_body(body_frame("c1", &range, 2)).await;
    let first = answer(&mut client).await;
    assert_eq!(first.request_id, "c1");
    assert_eq!(
        answered(first),
        vec![
            applied(1),
            applied(2),
            (3, CourierEntryOutcome::NotCouriered)
        ],
        "the row the courier sent no body for waits"
    );

    // The connection is free for the next range.
    client
        .courier_range(range_frame("c2", &author, author_id, &range, 3, 1))
        .await;
    client.courier_body(body_frame("c2", &range, 3)).await;
    assert_eq!(answered(answer(&mut client).await), vec![applied(3)]);
    assert_eq!(
        notes(fixture.admin()).await,
        vec![
            note(1, "hello", "t0"),
            note(2, "two", "t1"),
            note(3, "three", "t2"),
            note(4, "four", "t3"),
        ]
    );
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// A range counting no bodies answers at once, its first row above the
/// frontier left without a body answered `NotCouriered`, and the connection
/// stays free.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_counting_no_bodies_answers_at_once() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (author, author_id) = store.enrol("alice", tomorrow()).await;
    let (courier, courier_id) = store.enrol("bob", tomorrow()).await;
    let manager = courier_on(&fixture, &store);
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
    let range = three();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 0))
        .await;
    assert_eq!(
        answered(answer(&mut client).await),
        vec![(1, CourierEntryOutcome::NotCouriered)]
    );
    client
        .courier_range(range_frame("c2", &author, author_id, &range, 1, 0))
        .await;
    assert_eq!(answer(&mut client).await.request_id, "c2");
    assert_eq!(settled_count(fixture.admin()).await, 0);
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// A body for a row at or below the frontier is consumed and applies
/// nothing, the row answered from its record, and the next row settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_for_a_settled_row_is_consumed_and_applies_nothing() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (author, author_id) = store.enrol("alice", tomorrow()).await;
    let (courier, courier_id) = store.enrol("bob", tomorrow()).await;
    let manager = courier_on(&fixture, &store);
    let range = three();
    settle_directly(
        &manager,
        "alice",
        &author,
        author_id,
        vec![range.bodies[0].changeset.clone()],
    )
    .await;
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 2))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    client.courier_body(body_frame("c1", &range, 2)).await;
    assert_eq!(
        answered(answer(&mut client).await),
        vec![
            ruled(1, Verdict::Applied),
            applied(2),
            (3, CourierEntryOutcome::NotCouriered)
        ]
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![
            note(1, "hello", "t0"),
            note(2, "two", "t1"),
            note(3, "three", "t2"),
        ],
        "the settled row applied once"
    );
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// A body for a later row, past one the courier sent no body for, stops the
/// range at the skipped row, and the bodies after the stop are dropped
/// unread, one that does not decompress among them, before the one answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_past_a_row_without_one_stops_the_range_and_the_rest_drain() {
    let (fixture, _store, (author, author_id), (courier, courier_id), manager) = camp().await;
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
    let range = four();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 3))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    client.courier_body(body_frame("c1", &range, 3)).await;
    no_answer_yet(&mut client).await;
    client.courier_body(unreadable("c1", 4)).await;
    assert_eq!(
        answered(answer(&mut client).await),
        vec![applied(1), (2, CourierEntryOutcome::NotCouriered)]
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "nothing past the stop applied"
    );
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// When the last counted body arrives, the next row above the frontier left
/// without a body is answered `NotCouriered`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_body_leaves_the_next_row_not_couriered() {
    let (fixture, _store, (author, author_id), (courier, courier_id), manager) = camp().await;
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
    let range = three();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 1))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    assert_eq!(
        answered(answer(&mut client).await),
        vec![applied(1), (2, CourierEntryOutcome::NotCouriered)]
    );
    assert_eq!(settled_count(fixture.admin()).await, 1);
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// A row that settles nothing stops the range `Retry`, and its remaining
/// bodies drain before the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_that_settles_nothing_stops_the_range_and_the_rest_drain() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (author, author_id) = store.enrol("alice", tomorrow()).await;
    let (courier, courier_id) = store.enrol("bob", tomorrow()).await;
    let (_captured, guard) = audited_guard();
    let manager = manager_over(
        fixture.admin(),
        guard,
        Some(store.installable()),
        SessionConfig::default().with_courier(true),
        Faulting(roster()),
    );
    let (mut client, server) = super::courier_frontier::loopback_client(&manager);
    let (challenge, _) = client
        .handshake_challenge_writing("courier", &["user:bob"], None, WriterRequest::Device, None)
        .await;
    client
        .bind_device(
            "bind",
            courier_id,
            sign(&courier, &bind_nonce_message(&challenge.nonce)),
        )
        .await;
    let range = chain(vec![
        plain(insert_changeset(FAULTED, "faulted", "t1")),
        plain(insert_changeset(3, "three", "t2")),
    ]);

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 2))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    client.courier_body(mismatched("c1", 2)).await;
    assert_eq!(
        answered(answer(&mut client).await),
        vec![(1, CourierEntryOutcome::Retry { retry_after_ms: 0 })]
    );
    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// A range refused `Disabled` on a server with the path off, whose ack says
/// so, drains its bodies, an unreadable one among them, and answers once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_disabled_server_drains_the_range_and_answers_once() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (author, author_id) = store.enrol("alice", tomorrow()).await;
    let (courier, courier_id) = store.enrol("bob", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    let (mut client, server) = super::courier_frontier::loopback_client(&manager);
    let (challenge, ack) = client
        .handshake_challenge_writing("courier", &["user:bob"], None, WriterRequest::Device, None)
        .await;
    assert!(!ack.courier, "the ack tells the path is off");
    client
        .bind_device(
            "bind",
            courier_id,
            sign(&courier, &bind_nonce_message(&challenge.nonce)),
        )
        .await;
    let range = three();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 2))
        .await;
    client.courier_body(unreadable("c1", 1)).await;
    no_answer_yet(&mut client).await;
    client.courier_body(body_frame("c1", &range, 2)).await;
    assert_eq!(
        answer(&mut client).await.outcome,
        CourierOutcome::Refused(CourierRefusal::Disabled)
    );
    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// A connection that bound no device writer is refused `Uncertified`, and
/// its bodies drain before the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncertified_connection_is_refused() {
    let (fixture, _store, (author, author_id), _courier, manager) = camp().await;
    let (mut client, server) = super::courier_frontier::loopback_client(&manager);
    let ack = client.handshake_with("browser", "user:bob").await;
    assert!(ack.courier, "the ack tells the path is on");
    let range = three();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 1))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    assert_eq!(
        answer(&mut client).await.outcome,
        CourierOutcome::Refused(CourierRefusal::Uncertified)
    );
    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// The courier's own mutations and other frames are handled between the
/// bodies of its open range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutations_between_bodies_are_handled_as_ever() {
    let (fixture, _store, (author, author_id), (courier, courier_id), manager) = camp().await;
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
    let range = three();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 2))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    client.upload(1, insert_changeset(7, "bobs", "t7")).await;
    let ControlMessage::MutationApplied(applied_own) = client.next_control().await else {
        panic!("the courier's own write applies between the bodies");
    };
    assert_eq!(applied_own.client_seq, 1);
    client.courier_body(body_frame("c1", &range, 2)).await;
    assert_eq!(
        answered(answer(&mut client).await),
        vec![
            applied(1),
            applied(2),
            (3, CourierEntryOutcome::NotCouriered)
        ]
    );
    assert_ne!(
        writer_for_key(fixture.admin(), author_id).await,
        writer_for_key(fixture.admin(), courier_id).await,
        "each settled under its own writer"
    );
    client.close().await;
    server.await.expect("join").expect("session ok");
}

/// A connection that ends with a range open answers nothing, and the
/// entries its bodies settled stand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_ending_mid_range_keeps_what_settled() {
    let (fixture, _store, (author, author_id), (courier, courier_id), manager) = camp().await;
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
    let range = three();

    client
        .courier_range(range_frame("c1", &author, author_id, &range, 1, 3))
        .await;
    client.courier_body(body_frame("c1", &range, 1)).await;
    no_answer_yet(&mut client).await;
    client.close().await;
    server.await.expect("join").expect("session ok");
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the settled entry stands"
    );
}

/// A body with no range open ends the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_with_no_range_open_ends_the_connection() {
    let (_fixture, _store, _author, (courier, courier_id), manager) = camp().await;
    let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
    client.courier_body(body_frame("c1", &three(), 1)).await;
    violated(client, server, "a courier body with no range open").await;
}

/// A range while one is open ends the connection, whether the open range is
/// settling or draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_range_while_one_is_open_ends_the_connection() {
    let (_fixture, _store, (author, author_id), (courier, courier_id), manager) = camp().await;
    let range = three();
    for draining in [false, true] {
        let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
        let first = if draining {
            // A range of more bodies than rows is refused `Malformed`.
            CourierRange {
                bodies: 2,
                ..range_frame("c1", &author, author_id, &range, 3, 1)
            }
        } else {
            range_frame("c1", &author, author_id, &range, 1, 2)
        };
        client.courier_range(first).await;
        client
            .courier_range(range_frame("c2", &author, author_id, &range, 1, 1))
            .await;
        violated(client, server, "a courier range while another is open").await;
    }
}

/// A body of another range, naming no row of the range, or out of sequence
/// order ends the connection, settling or draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_out_of_its_range_or_order_ends_the_connection() {
    let (_fixture, _store, (author, author_id), (courier, courier_id), manager) = camp().await;
    let range = three();
    let cases: [(u64, CourierBody, &str); 3] = [
        (
            1,
            body_frame("other", &range, 2),
            "a courier body of another range",
        ),
        (
            2,
            CourierBody {
                seq: 1,
                ..body_frame("c1", &range, 1)
            },
            "a courier body naming sequence 1, no row of the range",
        ),
        (
            1,
            body_frame("c1", &range, 1),
            "a courier body for sequence 1 out of order",
        ),
    ];
    for refused in [false, true] {
        for (from, bad, detail) in cases.clone() {
            let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
            let mut open = range_frame("c1", &author, author_id, &range, from, 2);
            if refused {
                open.offer = b"garbage".to_vec();
            }
            client.courier_range(open).await;
            if detail.ends_with("out of order") {
                client.courier_body(body_frame("c1", &range, 2)).await;
            }
            client.courier_body(bad).await;
            violated(client, server, detail).await;
        }
    }
}

/// A body that does not decompress or disagrees with its row's digest ends
/// a settling range's connection, and the entries settled before it stand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broken_body_ends_the_connection_and_what_settled_stands() {
    let (fixture, _store, (author, author_id), (courier, courier_id), manager) = camp().await;
    let range = three();
    for (bad, detail) in [
        (
            unreadable("c1", 2),
            "a courier body for sequence 2 does not decompress",
        ),
        (
            mismatched("c1", 2),
            "a courier body for sequence 2 disagrees with its row's digest",
        ),
    ] {
        let (mut client, server) = courier_session(&manager, "bob", &courier, courier_id).await;
        client
            .courier_range(range_frame("c1", &author, author_id, &range, 1, 3))
            .await;
        client.courier_body(body_frame("c1", &range, 1)).await;
        client.courier_body(bad).await;
        violated(client, server, detail).await;
    }
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the entry settled before the broken body stands"
    );
}

/// One insert into the built server's `orders` table.
fn order(id: i64) -> Vec<u8> {
    connetto_test_harness::insert_changeset(
        "orders",
        &["id", "price", "quantity", "status"],
        &[0],
        vec![
            RowValue::Integer(id),
            RowValue::Null,
            RowValue::Integer(1),
            RowValue::Text("couriered".to_owned()),
        ],
    )
}

/// How many `orders` rows the built server holds.
async fn orders(fixture: &Fixture) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let pool = admin_pool(fixture).await;
    let mut conn = pool.get().await.expect("a connection");
    diesel::sql_query("SELECT count(*) AS n FROM orders")
        .get_result::<Count>(&mut conn)
        .await
        .expect("count the orders")
        .n
}

/// A session over the socket signed in with `token`, writing as a device,
/// with the connection's challenge nonce and the ack.
async fn device_session(
    addr: &str,
    token: &str,
    client_id: &str,
) -> (WebSocketTransport<TcpStream>, [u8; 32], HandshakeAck) {
    let tcp = TcpStream::connect(addr)
        .await
        .expect("connect the sync route");
    let mut client = WebSocketTransport::connect(&format!("ws://{addr}/sync"), tcp)
        .await
        .expect("the WebSocket handshake");
    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, client_id)
                .with_major(served_major())
                .with_grant(Grant::new(token))
                .with_writer(WriterRequest::Device),
        ))
        .await
        .expect("send the handshake");
    let ControlMessage::Challenge(challenge) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    let ControlMessage::HandshakeAck(ack) = next_control(&mut client).await else {
        panic!("expected the handshake ack");
    };
    (client, challenge.nonce, ack)
}

/// Enrol a fresh key on `client`'s session and hand it back with its id.
async fn enrolled(client: &mut WebSocketTransport<TcpStream>) -> (KeyPair, [u8; 32]) {
    let key = device_key();
    let nonce = challenge_nonce(client).await;
    let ControlMessage::EnrolGrant(EnrolGrant { .. }) =
        enrol_request(client, &key, nonce, None).await
    else {
        panic!("the enrolment was refused");
    };
    let id = *KeyId::of_public_key(&key.subject_public_key_info()).as_bytes();
    (key, id)
}

/// Run a built server with device identity and the courier path `on`, its
/// author enrolled and its courier enrolled and bound over the socket, and
/// send the courier a range of two entries with both bodies, answering the
/// ack the courier read and the range's answer.
async fn courier_over_socket(on: bool) -> (HandshakeAck, CourierAnswer, i64) {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    connetto_test_harness::stack::provision_enrolment_tables(&fixture).await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, _idp, _keys) = builder_over(&fixture, port).await;
    let (_, _, issuer) = rooted_issuer();
    let parts = builder
        .device_identity(Some(DeviceCertConfig::new(issuer)))
        .courier(on)
        .build()
        .await
        .expect("the deployment assembles");
    let stream = tokio::spawn(parts.change_stream);
    let http = tokio::spawn(async move { axum::serve(listener, parts.router).await });
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base).await;
    let addr = format!("127.0.0.1:{port}");

    let (author_token, _, _) = mint_tokens(&base, "author").await;
    let (mut author_session, _, _) = device_session(&addr, &author_token, "author").await;
    let (author, author_id) = enrolled(&mut author_session).await;

    let (courier_token, _, _) = mint_tokens(&base, "courier").await;
    let (mut courier, nonce, ack) = device_session(&addr, &courier_token, "courier").await;
    let (courier_key, courier_id) = enrolled(&mut courier).await;
    courier
        .send_control(ControlMessage::BindDevice(BindDevice {
            request_id: "bind".to_owned(),
            key_id: courier_id,
            signature: sign(&courier_key, &writer_bind_message(&nonce)),
        }))
        .await
        .expect("bind the courier's key");
    let ControlMessage::WriterAnswer(bound) = reply(&mut courier).await else {
        panic!("the bind answered something else");
    };
    assert!(matches!(bound.outcome, WriterOutcome::Bound { .. }));

    let before = orders(&fixture).await;
    let range = chain(vec![plain(order(10)), plain(order(11))]);
    courier
        .send_control(ControlMessage::CourierRange(range_frame(
            "c1", &author, author_id, &range, 1, 2,
        )))
        .await
        .expect("open the range");
    for seq in [1, 2] {
        courier
            .send_bulk(connetto_core::messages::BulkMessage::CourierBody(
                body_frame("c1", &range, seq),
            ))
            .await
            .expect("send a body");
    }
    let ControlMessage::CourierAnswer(answer) = reply(&mut courier).await else {
        panic!("expected the courier answer");
    };
    let landed = orders(&fixture).await - before;
    stream.abort();
    http.abort();
    (ack, answer, landed)
}

/// A built server with the courier path on tells the courier so and settles
/// a range a certified courier relays for another account's device, over a
/// real socket.
#[tokio::test]
async fn a_built_server_with_the_path_on_settles_a_range_over_a_socket() {
    let (ack, answer, landed) = Box::pin(courier_over_socket(true)).await;
    assert!(ack.courier, "the ack tells the path is on");
    assert_eq!(answer.request_id, "c1");
    assert_eq!(answered(answer), vec![applied(1), applied(2)]);
    assert_eq!(landed, 2, "both entries landed");
}

/// A built server with the path off, the default, tells the courier so and
/// refuses the range `Disabled` after its bodies, over a real socket.
#[tokio::test]
async fn a_built_server_with_the_path_off_refuses_a_range_over_a_socket() {
    let (ack, answer, landed) = Box::pin(courier_over_socket(false)).await;
    assert!(!ack.courier, "the ack tells the path is off");
    assert_eq!(
        answer.outcome,
        CourierOutcome::Refused(CourierRefusal::Disabled)
    );
    assert_eq!(landed, 0, "nothing landed");
}
