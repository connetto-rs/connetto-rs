//! Courier audit observers through the public builder and connection drivers.

mod transport;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use connetto_client::{
    AccessTokenSource, ClientBuilder, ClientError, ClientEvent, Clock, ConnettoClient,
    CourierAuditError, CourierAuditWait, HeldCredential, ReconnectPolicy, SyncTuning, SystemClock,
    TransportFactory,
};
use connetto_core::messages::{
    BulkMessage, ControlMessage, CourierAuditAnswer, CourierAuditOutcome, CourierAuditRequest, Pong,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{LoopbackTransport, loopback};
use futures_util::FutureExt;
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);";
const BOUND: Duration = Duration::from_secs(10);
const WITHIN: Duration = Duration::from_secs(60);
const AUTHOR: [u8; 32] = [7; 32];
type Observer<'a> = Pin<Box<dyn Future<Output = Result<(), CourierAuditError>> + Send + 'a>>;

struct DialPlan {
    ports: mpsc::Receiver<(u64, LoopbackTransport)>,
    attempts: mpsc::Sender<u64>,
    next: u64,
}

impl TransportFactory for DialPlan {
    type Transport = LoopbackTransport;
    type Error = super::support::DialExhausted;

    async fn connect(&mut self) -> Result<Self::Transport, Self::Error> {
        let id = self.next;
        self.next += 1;
        self.attempts.try_send(id).expect("dial attempt capacity");
        let (reply_id, port) = bounded("dial response", self.ports.recv())
            .await
            .ok_or(super::support::DialExhausted)?;
        assert_eq!(reply_id, id, "the transport belongs to this dial");
        Ok(port)
    }
}

struct Harness {
    client: ConnettoClient<LoopbackTransport>,
    server: LoopbackTransport,
    ports: mpsc::Sender<(u64, LoopbackTransport)>,
    attempts: mpsc::Receiver<u64>,
    pump: JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

async fn bounded<T>(id: &str, work: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, work)
        .await
        .unwrap_or_else(|_| panic!("{id} exceeded its monotonic bound"))
}

async fn accepting() -> (LoopbackTransport, LoopbackTransport) {
    let (mut server, client) = loopback();
    let IncomingFrame::Control(ack) = super::support::handshake_ack() else {
        panic!("handshake acknowledgement is a control frame");
    };
    bounded("queue handshake ack", server.send_control(ack))
        .await
        .expect("queue handshake ack");
    (server, client)
}

async fn running(account: &str) -> Harness {
    running_credential(super::support::held(account)).await
}

async fn running_credential(credential: HeldCredential) -> Harness {
    let (server, port) = accepting().await;
    let (ports, receiver) = mpsc::channel(4);
    let (attempts, mut observed) = mpsc::channel(4);
    ports.try_send((1, port)).expect("initial dial slot");
    let dialer = DialPlan {
        ports: receiver,
        attempts,
        next: 1,
    };
    let (running, pump) = Box::pin(bounded(
        "initial handshake",
        ClientBuilder::new(super::support::bundle(DDL), dialer)
            .with_tuning(SyncTuning::default().with_probe_after(Duration::from_secs(3600)))
            .with_reconnect(
                ReconnectPolicy::new()
                    .with_initial_backoff(Duration::from_millis(1))
                    .with_max_backoff(Duration::from_millis(1)),
            )
            .with_sleeper(tokio::time::sleep)
            .signed_in(credential)
            .connect_with_pump(),
    ))
    .await
    .expect("initial connection");
    assert_eq!(bounded("initial dial 1", observed.recv()).await, Some(1));
    Harness {
        client: running.client().clone(),
        server,
        ports,
        attempts: observed,
        pump: tokio::spawn(pump),
    }
}

fn observe<'a>(
    client: &'a ConnettoClient<LoopbackTransport>,
    seqs: &'a [u64],
    within: Duration,
) -> Observer<'a> {
    Box::pin(client.observe_courier_audit(&AUTHOR, seqs, within))
}

async fn control(server: &mut LoopbackTransport, id: &str) -> ControlMessage {
    bounded(id, async {
        loop {
            match server.recv().await.expect("receive client frame") {
                Some(IncomingFrame::Control(ControlMessage::Handshake(_))) => {}
                Some(IncomingFrame::Control(message)) => return message,
                frame => panic!("unexpected frame for {id}, got {frame:?}"),
            }
        }
    })
    .await
}

async fn request(server: &mut LoopbackTransport, wait: &mut Observer<'_>) -> CourierAuditRequest {
    tokio::select! {
        biased;
        result = wait => panic!("observation completed before its request, got {result:?}"),
        message = control(server, "audit request") => {
            let ControlMessage::CourierAuditRequest(request) = message else {
                panic!("expected an audit request, got {message:?}");
            };
            assert!(request.is_well_formed());
            request
        }
    }
}

async fn answer(server: &mut LoopbackTransport, id: &str, outcome: CourierAuditOutcome) {
    bounded(
        id,
        server.send_control(ControlMessage::CourierAuditAnswer(CourierAuditAnswer {
            request_id: id.to_owned(),
            outcome,
        })),
    )
    .await
    .expect("send correlated audit answer");
}

async fn barrier(
    server: &mut LoopbackTransport,
    events: &mut broadcast::Receiver<ClientEvent>,
    nonce: u64,
) -> Vec<ClientEvent> {
    bounded(
        &format!("send pong {nonce}"),
        server.send_control(ControlMessage::Pong(Pong { nonce })),
    )
    .await
    .expect("send inbound processing barrier");
    bounded("correlated pong barrier", async {
        let mut seen = Vec::new();
        loop {
            let event = events.recv().await.expect("event stream remains open");
            if event == (ClientEvent::Pong { nonce }) {
                return seen;
            }
            seen.push(event);
        }
    })
    .await
}

async fn disconnect(h: &mut Harness) {
    bounded("disconnect server", h.server.close())
        .await
        .expect("disconnect server");
    assert_eq!(
        bounded("reconnect dial 2", h.attempts.recv()).await,
        Some(2)
    );
}

async fn reconnect(h: &mut Harness) {
    let (server, port) = accepting().await;
    h.server = server;
    h.ports
        .try_send((2, port))
        .expect("reconnect dial response 2");
}

#[tokio::test]
async fn manual_wait_expires_without_driver_maintenance() {
    let mut conn = ClientBuilder::new(
        super::support::bundle(DDL),
        super::support::NeverDial::<LoopbackTransport>::default(),
    )
    .signed_in(super::support::held("alice"))
    .open_driven()
    .expect("open manual connection");
    let wake = Arc::new(Notify::new());
    let deadline = SystemClock.monotonic() + Duration::from_secs(1);
    let wait: CourierAuditWait = conn
        .begin_courier_audit(&AUTHOR, &[1], deadline, Arc::clone(&wake))
        .expect("register manual observation");

    let result = bounded("manual audit expiry", wait.wait()).await;
    assert!(matches!(result, Err(CourierAuditError::Expired)));
    assert!(SystemClock.monotonic() >= deadline);
    assert!(
        wake.notified().now_or_never().is_some(),
        "expiry wakes cancellation maintenance"
    );
}

#[tokio::test]
async fn a_settled_manual_receipt_survives_late_polling() {
    let (mut server, transport) = accepting().await;
    let mut conn = ClientBuilder::new(
        super::support::bundle(DDL),
        super::support::Once::new(transport),
    )
    .signed_in(super::support::held("alice"))
    .connect_driven()
    .await
    .expect("connect manual observer");
    let wake = Arc::new(Notify::new());
    let deadline = SystemClock.monotonic() + Duration::from_secs(1);
    let confirmed = conn
        .begin_courier_audit(&AUTHOR, &[1], deadline, Arc::clone(&wake))
        .expect("register confirmed observation");
    let expired = conn
        .begin_courier_audit(&AUTHOR, &[2], deadline, wake)
        .expect("register deadline observation");
    conn.process_courier_audit()
        .await
        .expect("send observations");
    for _ in 0..2 {
        let ControlMessage::CourierAuditRequest(request) =
            control(&mut server, "manual receipt").await
        else {
            panic!("expected audit request");
        };
        if request.seqs == [1] {
            answer(
                &mut server,
                &request.request_id,
                CourierAuditOutcome::Confirmed,
            )
            .await;
        } else {
            assert_eq!(request.seqs, [2]);
        }
    }
    server
        .send_control(ControlMessage::Pong(Pong { nonce: 321 }))
        .await
        .expect("receipt barrier");
    bounded("receipt barrier 321", async {
        loop {
            if conn.pump_one().await.expect("process receipt") == (ClientEvent::Pong { nonce: 321 })
            {
                break;
            }
        }
    })
    .await;
    assert!(
        SystemClock.monotonic() < deadline,
        "receipt settled before its deadline"
    );
    assert!(matches!(
        bounded("deadline observation", expired.wait()).await,
        Err(CourierAuditError::Expired)
    ));
    assert!(
        bounded("already settled receipt", confirmed.wait())
            .await
            .is_ok()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmation_is_correlated_and_never_broadcast() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let mut events = client.events();
    barrier(&mut h.server, &mut events, 100).await;
    let mut wait = observe(&client, &[3, 1, 3, 2], WITHIN);
    let sent = request(&mut h.server, &mut wait).await;
    assert_eq!(sent.author, AUTHOR);
    assert_eq!(sent.seqs, [1, 2, 3]);
    assert!(sent.remaining_ms <= 60_000);
    answer(
        &mut h.server,
        "foreign-receipt",
        CourierAuditOutcome::Confirmed,
    )
    .await;
    assert_eq!(barrier(&mut h.server, &mut events, 101).await, []);
    assert!(wait.as_mut().now_or_never().is_none());
    answer(
        &mut h.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&sent.request_id, wait)
        .await
        .expect("confirmed receipt");
    assert_eq!(barrier(&mut h.server, &mut events, 102).await, []);
    bounded("close confirmed client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_reissues_with_a_fresh_route_and_the_original_bound() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let mut wait = observe(&client, &[11, 12], WITHIN);
    let first = request(&mut h.server, &mut wait).await;
    disconnect(&mut h).await;
    assert!(wait.as_mut().now_or_never().is_none());
    reconnect(&mut h).await;
    let second = request(&mut h.server, &mut wait).await;
    assert_ne!(first.request_id, second.request_id);
    assert_eq!(first.author, second.author);
    assert_eq!(first.seqs, second.seqs);
    assert!(second.remaining_ms <= first.remaining_ms);
    let mut events = client.events();
    answer(
        &mut h.server,
        &first.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    barrier(&mut h.server, &mut events, 201).await;
    assert!(wait.as_mut().now_or_never().is_none());
    answer(
        &mut h.server,
        &second.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&second.request_id, wait)
        .await
        .expect("receipt committed during the outage");
    bounded("close reconnected client", client.close()).await;
}

async fn cancelled(server: &mut LoopbackTransport, expected: &str) {
    let message = control(server, expected).await;
    let ControlMessage::CourierAuditCancel(cancel) = message else {
        panic!("expected cancellation for {expected}, got {message:?}");
    };
    assert_eq!(cancel.request_id, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_inputs_are_rejected_without_poisoning_the_next_observation() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let cases: &[(&[u8], &[u64], Duration)] = &[
        (&[], &[1], WITHIN),
        (&AUTHOR[..31], &[1], WITHIN),
        (&[7; 33], &[1], WITHIN),
        (&AUTHOR, &[], WITHIN),
        (&AUTHOR, &[0], WITHIN),
        (&AUTHOR, &[1, 0, 2], WITHIN),
        (&AUTHOR, &[1_u64 << 63], WITHIN),
        (&AUTHOR, &[u64::MAX], WITHIN),
        (&AUTHOR, &[1], Duration::ZERO),
        (&AUTHOR, &[1], Duration::from_secs(u64::MAX / 1000 + 1)),
        (&AUTHOR, &[1], Duration::MAX),
    ];
    for (index, &(author, seqs, within)) in cases.iter().enumerate() {
        let result = bounded(
            &format!("invalid input {index}"),
            client.observe_courier_audit(author, seqs, within),
        )
        .await;
        assert!(matches!(result, Err(CourierAuditError::InvalidInput)));
    }
    let mut wait = observe(&client, &[1], WITHIN);
    let sent = request(&mut h.server, &mut wait).await;
    assert_eq!(sent.seqs, [1]);
    answer(
        &mut h.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&sent.request_id, wait)
        .await
        .expect("valid observation");
    bounded("close invalid-input client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_capacity_counts_canonical_requests_and_drop_releases_it() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let oversized: Vec<u64> = (1..=8193).collect();
    assert!(matches!(
        bounded(
            "request sequence capacity",
            observe(&client, &oversized, WITHIN)
        )
        .await,
        Err(CourierAuditError::Capacity)
    ));
    let seqs: Vec<u64> = (1..=8192).rev().chain(1..=8192).collect();
    let expected: Vec<u64> = (1..=8192).collect();
    let mut retained = Vec::new();
    for _ in 0..8 {
        let mut wait = observe(&client, &seqs, WITHIN);
        let sent = request(&mut h.server, &mut wait).await;
        assert_eq!(sent.seqs, expected);
        retained.push((sent.request_id, wait));
    }
    assert!(matches!(
        bounded(
            "retained sequence capacity",
            observe(&client, &[8193], WITHIN)
        )
        .await,
        Err(CourierAuditError::Capacity)
    ));
    let (dropped_id, dropped) = retained.pop().expect("eight retained observations");
    drop(dropped);
    cancelled(&mut h.server, &dropped_id).await;
    let mut replacement = observe(&client, &seqs, WITHIN);
    let sent = request(&mut h.server, &mut replacement).await;
    assert_ne!(sent.request_id, dropped_id);
    assert_eq!(sent.seqs, expected);
    retained.push((sent.request_id, replacement));
    for (id, wait) in retained {
        answer(&mut h.server, &id, CourierAuditOutcome::Confirmed).await;
        bounded(&id, wait).await.expect("retained receipt");
    }
    let mut after_answers = observe(&client, &seqs, WITHIN);
    let sent = request(&mut h.server, &mut after_answers).await;
    answer(
        &mut h.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&sent.request_id, after_answers)
        .await
        .expect("answered capacity released");
    bounded("close capacity client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_observers_receive_only_their_typed_out_of_order_answers() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let outcomes = [
        CourierAuditOutcome::Confirmed,
        CourierAuditOutcome::Expired,
        CourierAuditOutcome::Refused,
        CourierAuditOutcome::Failed,
    ];
    let mut pending = Vec::new();
    for outcome in outcomes {
        let mut wait = observe(&client, &[41], WITHIN);
        let sent = request(&mut h.server, &mut wait).await;
        assert!(pending.iter().all(|(id, _, _)| id != &sent.request_id));
        pending.push((sent.request_id, wait, outcome));
    }
    let mut events = client.events();
    let mut nonce = 300;
    while let Some((id, wait, outcome)) = pending.pop() {
        answer(&mut h.server, &id, outcome).await;
        let result = bounded(&id, wait).await;
        assert!(matches!(
            (outcome, result),
            (CourierAuditOutcome::Confirmed, Ok(()))
                | (
                    CourierAuditOutcome::Expired,
                    Err(CourierAuditError::Expired)
                )
                | (
                    CourierAuditOutcome::Refused,
                    Err(CourierAuditError::Refused)
                )
                | (CourierAuditOutcome::Failed, Err(CourierAuditError::Failed))
        ));
        answer(&mut h.server, &id, CourierAuditOutcome::Confirmed).await;
        assert_eq!(barrier(&mut h.server, &mut events, nonce).await, []);
        nonce += 1;
        for (_, other, _) in &mut pending {
            assert!(other.as_mut().now_or_never().is_none());
        }
    }
    bounded("close concurrent client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_drop_and_local_expiry_cancel_while_the_gate_is_locked() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let mut dropped = observe(&client, &[51], WITHIN);
    let drop_request = request(&mut h.server, &mut dropped).await;
    let mut expiring = observe(&client, &[52], Duration::from_secs(1));
    let expiry_request = request(&mut h.server, &mut expiring).await;
    bounded(
        "lock observation gate",
        client.enable_gate(None, Arc::new(super::support::LockedMechanism::new())),
    )
    .await;
    assert!(matches!(
        bounded("locked application access", client.with_conn(|_| ())).await,
        Err(ClientError::Locked)
    ));
    assert!(matches!(
        bounded("locked observation", observe(&client, &[53], WITHIN)).await,
        Err(CourierAuditError::Client(ClientError::Locked))
    ));
    drop(dropped);
    cancelled(&mut h.server, &drop_request.request_id).await;
    assert!(matches!(
        bounded(&expiry_request.request_id, expiring).await,
        Err(CourierAuditError::Expired)
    ));
    cancelled(&mut h.server, &expiry_request.request_id).await;
    bounded("close locked client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_expires_an_observer_whose_caller_is_not_polling() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let mut dormant = observe(&client, &[54], Duration::from_secs(1));
    let first = request(&mut h.server, &mut dormant).await;
    let mut driven = observe(&client, &[55], Duration::from_secs(1));
    let second = request(&mut h.server, &mut driven).await;
    assert!(matches!(
        bounded(&second.request_id, driven).await,
        Err(CourierAuditError::Expired)
    ));
    let messages = [
        control(&mut h.server, "expired observation cancellation").await,
        control(&mut h.server, "expired observation cancellation").await,
    ];
    let mut cancelled = messages.each_ref().map(|message| {
        let ControlMessage::CourierAuditCancel(cancel) = message else {
            panic!("expired observation must cancel its request");
        };
        cancel.request_id.as_str()
    });
    cancelled.sort_unstable();
    let mut expected = [first.request_id.as_str(), second.request_id.as_str()];
    expected.sort_unstable();
    assert_eq!(cancelled, expected);
    answer(
        &mut h.server,
        &first.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    assert!(matches!(
        bounded(&first.request_id, dormant).await,
        Err(CourierAuditError::Expired)
    ));
    let mut next = observe(&client, &[54], WITHIN);
    let sent = request(&mut h.server, &mut next).await;
    answer(
        &mut h.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&sent.request_id, next)
        .await
        .expect("a fresh observer owns its confirmation");
    bounded("close maintenance-expiry client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_expiry_and_drop_are_not_reissued_on_recovery() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let mut preserved = observe(&client, &[65], WITHIN);
    let original = request(&mut h.server, &mut preserved).await;
    let mut expiring = observe(&client, &[61], Duration::from_secs(1));
    let expired_request = request(&mut h.server, &mut expiring).await;
    let mut dropped = observe(&client, &[62], WITHIN);
    request(&mut h.server, &mut dropped).await;
    disconnect(&mut h).await;
    drop(dropped);
    assert!(matches!(
        bounded(&expired_request.request_id, expiring).await,
        Err(CourierAuditError::Expired)
    ));
    assert!(preserved.as_mut().now_or_never().is_none());
    let mut offline = observe(&client, &[63], WITHIN);
    assert!(offline.as_mut().now_or_never().is_none());
    reconnect(&mut h).await;
    let first = request(&mut h.server, &mut offline).await;
    let second = request(&mut h.server, &mut offline).await;
    let (resumed, sent) = if first.seqs == [65] {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(resumed.seqs, [65]);
    assert_ne!(resumed.request_id, original.request_id);
    assert!(resumed.remaining_ms < original.remaining_ms);
    assert_eq!(sent.seqs, [63]);
    assert_ne!(sent.request_id, expired_request.request_id);
    answer(
        &mut h.server,
        &resumed.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&resumed.request_id, preserved)
        .await
        .expect("original bound survived recovery");
    answer(
        &mut h.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&sent.request_id, offline)
        .await
        .expect("offline registration recovered");
    let mut next = observe(&client, &[64], WITHIN);
    let sent = request(&mut h.server, &mut next).await;
    assert_eq!(sent.seqs, [64]);
    answer(
        &mut h.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&sent.request_id, next)
        .await
        .expect("no expired route remained");
    bounded("close offline-expiry client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_releases_all_observers_and_refuses_new_observations() {
    for offline in [false, true] {
        let mut h = running("alice").await;
        let client = h.client.clone();
        let mut first = observe(&client, &[71], WITHIN);
        let first_request = request(&mut h.server, &mut first).await;
        let mut second = observe(&client, &[72], WITHIN);
        let second_request = request(&mut h.server, &mut second).await;
        if offline {
            disconnect(&mut h).await;
        }
        bounded("close observed client", client.close()).await;
        for (id, wait) in [
            (first_request.request_id, first),
            (second_request.request_id, second),
        ] {
            assert!(matches!(
                bounded(&id, wait).await,
                Err(CourierAuditError::SessionEnded)
            ));
        }
        assert!(matches!(
            bounded("observation after close", observe(&client, &[73], WITHIN)).await,
            Err(CourierAuditError::SessionEnded)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_original_deadline_bounds_registration_behind_connection_work() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let (entered, entry) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let holder_client = client.clone();
    let holder = tokio::spawn(async move {
        holder_client
            .with_conn(move |_| {
                entered.send("registration-lock").expect("entry receiver");
                assert_eq!(
                    released
                        .recv_timeout(BOUND)
                        .expect("release connection work"),
                    "registration-lock"
                );
            })
            .await
    });
    assert_eq!(
        bounded("registration-lock", entry)
            .await
            .expect("connection work entered"),
        "registration-lock"
    );
    let result = bounded(
        "registration deadline",
        observe(&client, &[81], Duration::from_millis(100)),
    )
    .await;
    release
        .send("registration-lock")
        .expect("connection work remains held");
    bounded("connection work release", holder)
        .await
        .expect("connection work task")
        .expect("connection work succeeded");
    assert!(matches!(result, Err(CourierAuditError::Expired)));
    let mut next = observe(&client, &[82], WITHIN);
    let sent = request(&mut h.server, &mut next).await;
    assert_eq!(sent.seqs, [82]);
    answer(
        &mut h.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&sent.request_id, next)
        .await
        .expect("lock contention left no request");
    bounded("close registration-bound client", client.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_authentication_does_not_transfer_observers_to_another_account() {
    let credential =
        super::support::held("alice").with_token_source(AccessTokenSource::new(|| async {
            Err(ClientError::Auth("the login was revoked".to_owned()))
        }));
    let mut alice = running_credential(credential).await;
    let client = alice.client.clone();
    let mut events = client.events();
    let mut wait = observe(&client, &[91], WITHIN);
    let sent = request(&mut alice.server, &mut wait).await;
    disconnect(&mut alice).await;
    reconnect(&mut alice).await;
    assert!(matches!(
        bounded(&sent.request_id, wait).await,
        Err(CourierAuditError::SessionEnded)
    ));
    bounded("revoked login outcome", async {
        loop {
            match events.recv().await.expect("authentication event") {
                ClientEvent::AuthenticationRequired => break,
                ClientEvent::Closed => panic!("revocation must request authentication"),
                _ => {}
            }
        }
    })
    .await;
    assert!(matches!(
        bounded(
            "revoked account observation",
            observe(&client, &[91], WITHIN)
        )
        .await,
        Err(CourierAuditError::SessionEnded)
    ));

    let mut bob = running("bob").await;
    let replacement = bob.client.clone();
    let mut replacement_events = replacement.events();
    let mut replacement_wait = observe(&replacement, &[91], WITHIN);
    let replacement_request = request(&mut bob.server, &mut replacement_wait).await;
    assert_ne!(replacement_request.request_id, sent.request_id);
    answer(
        &mut bob.server,
        &sent.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    barrier(&mut bob.server, &mut replacement_events, 901).await;
    assert!(replacement_wait.as_mut().now_or_never().is_none());
    answer(
        &mut bob.server,
        &replacement_request.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&replacement_request.request_id, replacement_wait)
        .await
        .expect("the replacement account owns its observation");
    bounded("close revoked client", client.close()).await;
    bounded("close replacement client", replacement.close()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_terminal_protocol_fault_releases_observers() {
    let mut h = running("alice").await;
    let client = h.client.clone();
    let mut events = client.events();
    let mut wait = observe(&client, &[101], WITHIN);
    let sent = request(&mut h.server, &mut wait).await;
    let id = sent.request_id.clone();
    bounded(
        &id,
        h.server
            .send_control(ControlMessage::CourierAuditRequest(sent)),
    )
    .await
    .expect("send a client-only frame from the server");
    assert!(matches!(
        bounded(&id, wait).await,
        Err(CourierAuditError::SessionEnded)
    ));
    bounded("terminal protocol fault", async {
        loop {
            if let ClientEvent::Stopped { detail } = events.recv().await.expect("fault event") {
                assert!(detail.contains("unexpected control frame from server"));
                break;
            }
        }
    })
    .await;
    assert!(matches!(
        bounded(
            "observation after protocol fault",
            observe(&client, &[102], WITHIN)
        )
        .await,
        Err(CourierAuditError::SessionEnded)
    ));
    bounded("close faulted client", client.close()).await;
}

struct BlockedAuditTransport {
    inner: LoopbackTransport,
    entered: Option<oneshot::Sender<String>>,
    release: Option<oneshot::Receiver<String>>,
}

impl Transport for BlockedAuditTransport {
    type Error = <LoopbackTransport as Transport>::Error;

    async fn send_control(&mut self, message: ControlMessage) -> Result<(), Self::Error> {
        if let ControlMessage::CourierAuditRequest(request) = &message {
            if let Some(entered) = self.entered.take() {
                entered
                    .send(request.request_id.clone())
                    .expect("blocked send observer");
            }
            if let Some(release) = self.release.as_mut() {
                let released = tokio::time::timeout(BOUND * 2, release)
                    .await
                    .expect("blocked send release bound")
                    .expect("blocked send release");
                assert_eq!(released, request.request_id);
                self.release = None;
            }
        }
        self.inner.send_control(message).await
    }

    async fn send_bulk(&mut self, message: BulkMessage) -> Result<(), Self::Error> {
        self.inner.send_bulk(message).await
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, Self::Error> {
        self.inner.recv().await
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_drop_interrupts_a_blocked_request_send() {
    let (mut server, inner) = accepting().await;
    let (entered, request_entered) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut port = Some(BlockedAuditTransport {
        inner,
        entered: Some(entered),
        release: Some(released),
    });
    let (running, pump) = Box::pin(bounded(
        "blocked transport handshake",
        ClientBuilder::new(super::support::bundle(DDL), move || {
            std::future::ready(port.take().ok_or(super::support::DialExhausted))
        })
        .signed_in(super::support::held("alice"))
        .connect_with_pump(),
    ))
    .await
    .expect("blocked transport connection");
    let pump = tokio::spawn(pump);
    let client = running.client().clone();
    let observing = client.clone();
    let observer = tokio::spawn(async move {
        observing
            .observe_courier_audit(&AUTHOR, &[111], WITHIN)
            .await
    });
    let id = bounded("blocked audit request", request_entered)
        .await
        .expect("request entered transport");
    observer.abort();
    assert!(
        bounded(&id, observer)
            .await
            .expect_err("observer was aborted")
            .is_cancelled()
    );
    let cancellation = tokio::time::timeout(BOUND, async {
        loop {
            match server.recv().await.expect("receive cancellation") {
                Some(IncomingFrame::Control(ControlMessage::Handshake(_))) => {}
                frame => return frame,
            }
        }
    })
    .await;
    let _ = release.send(id.clone());
    bounded("close blocked transport client", client.close()).await;
    bounded("blocked transport pump exit", pump)
        .await
        .expect("pump exits cleanly");
    assert!(matches!(
        cancellation,
        Ok(Some(IncomingFrame::Control(ControlMessage::CourierAuditCancel(cancel))))
            if cancel.request_id == id
    ));
}
