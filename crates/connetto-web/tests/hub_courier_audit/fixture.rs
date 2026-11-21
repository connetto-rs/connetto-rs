use core::cell::Cell;
use core::future::{Future, ready};
use core::time::Duration;
use std::rc::Rc;
use std::sync::Arc;

use connetto_client::away::{Clock, SystemClock};
use connetto_client::{
    ClientBuilder, FirstThen, Grant, HeldCredential, ReconnectPolicy, SyncSchema,
};
use connetto_core::messages::{
    BulkMessage, ContentTicketGrant, ContentTicketRequest, ControlMessage, CourierAuditAnswer,
    CourierAuditCancel, CourierAuditOutcome, CourierAuditRequest, Handshake, HandshakeAck, Ping,
    SubscriptionSpec, WriterGrant,
};
use connetto_core::schema::SchemaBundle;
use connetto_core::test_support::FakeTransportError;
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, Major, PROTOCOL_VERSION};
use connetto_file_client::{BrowserHttp, BrowserStore, ContentArchive};
use connetto_file_core::FileId;
use connetto_web::relay::{RelayError, TabId};
use connetto_web::{HubReconnect, MessageTransport, RelayHub};
use futures_util::future::{AbortHandle, Abortable, LocalBoxFuture};
use tokio::sync::{Notify, mpsc};
use wasm_bindgen_futures::spawn_local;
use web_sys::MessagePort;

#[derive(Clone, Copy)]
pub struct Bound(Duration);

impl Bound {
    pub fn new() -> Self {
        Self(SystemClock.monotonic() + Duration::from_secs(15))
    }

    pub fn until(deadline: Duration) -> Self {
        Self(deadline)
    }

    pub async fn run<T>(self, id: &str, future: impl Future<Output = T>) -> T {
        let remaining = self.0.saturating_sub(SystemClock.monotonic());
        assert!(!remaining.is_zero(), "{id} exceeded its monotonic bound");
        tokio::select! {
            biased;
            value = future => value,
            () = connetto_web::workers::sleep(remaining) => panic!("{id} exceeded its monotonic bound"),
        }
    }
}

#[derive(Clone)]
pub struct ReplayGate {
    open: Rc<Cell<bool>>,
    wake: Arc<Notify>,
}

impl ReplayGate {
    fn new(open: bool) -> Self {
        Self {
            open: Rc::new(Cell::new(open)),
            wake: Arc::new(Notify::new()),
        }
    }

    pub fn release(&self) {
        self.open.set(true);
        self.wake.notify_waiters();
    }

    async fn wait(&self, bound: Bound, id: &str) {
        while !self.open.get() {
            let wake = self.wake.notified();
            bound.run(id, wake).await;
        }
    }
}

pub struct Upstream {
    incoming: mpsc::UnboundedReceiver<Option<IncomingFrame>>,
    inject: mpsc::UnboundedSender<Option<IncomingFrame>>,
    sent: mpsc::UnboundedSender<ControlMessage>,
    automatic_handshake: bool,
    replay: ReplayGate,
    bound: Bound,
}

pub struct Peer {
    inject: mpsc::UnboundedSender<Option<IncomingFrame>>,
    sent: mpsc::UnboundedReceiver<ControlMessage>,
    pub replay: ReplayGate,
}

pub fn upstream(bound: Bound, automatic_handshake: bool, hold_replay: bool) -> (Upstream, Peer) {
    let (inject, incoming) = mpsc::unbounded_channel();
    let (sent, received) = mpsc::unbounded_channel();
    let replay = ReplayGate::new(!hold_replay);
    (
        Upstream {
            incoming,
            inject: inject.clone(),
            sent,
            automatic_handshake,
            replay: replay.clone(),
            bound,
        },
        Peer {
            inject,
            sent: received,
            replay,
        },
    )
}

fn acknowledgement() -> ControlMessage {
    ControlMessage::HandshakeAck(HandshakeAck {
        connection_id: "audit-upstream".to_owned(),
        session_token: "00000000-0000-4000-8000-000000000000".to_owned(),
        resume_token: "run:00000000-0000-4000-8000-000000000000".to_owned(),
        current_cursor: Cursor::new(Vec::new()),
        schema_version: None,
        sunset_at_secs: None,
        courier: false,
        initial_credits: 64,
        writer: Some(WriterGrant {
            number: 1,
            frontier: 0,
        }),
    })
}

impl Transport for Upstream {
    type Error = FakeTransportError;

    async fn send_control(&mut self, message: ControlMessage) -> Result<(), Self::Error> {
        let handshake = matches!(message, ControlMessage::Handshake(_));
        let subscription = match &message {
            ControlMessage::Subscribe(sub) => Some(sub.sub_id.clone()),
            _ => None,
        };
        self.sent
            .send(message)
            .map_err(|_| FakeTransportError::Closed)?;
        if handshake && self.automatic_handshake {
            self.inject
                .send(Some(IncomingFrame::Control(acknowledgement())))
                .map_err(|_| FakeTransportError::Closed)?;
        }
        if let Some(id) = subscription {
            self.replay.wait(self.bound, &id).await;
        }
        Ok(())
    }

    fn send_bulk(
        &mut self,
        _message: BulkMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, Self::Error> {
        Ok(self.incoming.recv().await.flatten())
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }
}

impl Peer {
    pub fn grant(&self, request: &ContentTicketRequest) {
        let url = format!(
            "https://content.invalid/files/{}/intent?t={}",
            FileId::from_bytes(request.file_id),
            request.request_id,
        );
        self.inject
            .send(Some(IncomingFrame::Control(
                ControlMessage::ContentTicketGrant(ContentTicketGrant {
                    request_id: request.request_id.clone(),
                    url,
                }),
            )))
            .expect("grant correlated upload ticket");
    }

    pub fn answer(&self, id: &str, outcome: CourierAuditOutcome) {
        self.inject
            .send(Some(IncomingFrame::Control(
                ControlMessage::CourierAuditAnswer(CourierAuditAnswer {
                    request_id: id.to_owned(),
                    outcome,
                }),
            )))
            .expect("inject correlated audit answer");
    }

    pub fn accept(&self) {
        self.inject
            .send(Some(IncomingFrame::Control(acknowledgement())))
            .expect("accept pending upstream handshake");
    }

    pub fn disconnect(&self) {
        self.inject.send(None).expect("end upstream stream");
    }

    pub async fn control(&mut self, bound: Bound, id: &str) -> ControlMessage {
        bound
            .run(id, self.sent.recv())
            .await
            .expect("upstream remains owned")
    }

    pub async fn request(&mut self, bound: Bound, author: u8, seqs: &[u64]) -> CourierAuditRequest {
        loop {
            match self.control(bound, "upstream audit request").await {
                ControlMessage::CourierAuditRequest(request) => {
                    assert_eq!(request.author, [author; 32]);
                    assert_eq!(request.seqs, seqs);
                    return request;
                }
                ControlMessage::CourierAuditCancel(cancel) => {
                    panic!("unexpected cancellation {cancel:?}")
                }
                _ => {}
            }
        }
    }

    pub async fn cancelled(&mut self, bound: Bound, id: &str) {
        loop {
            match self.control(bound, id).await {
                ControlMessage::CourierAuditCancel(cancel) => {
                    assert_eq!(cancel.request_id, id);
                    return;
                }
                ControlMessage::CourierAuditRequest(request) => {
                    panic!("unexpected request {request:?}")
                }
                _ => {}
            }
        }
    }

    pub async fn handshake(&mut self, bound: Bound) {
        loop {
            if let ControlMessage::Handshake(hello) =
                self.control(bound, "recovery handshake").await
            {
                assert_eq!(hello.grants, vec![Grant::new("user:audit")]);
                return;
            }
        }
    }

    pub async fn replaying(&mut self, bound: Bound) {
        loop {
            if let ControlMessage::Subscribe(sub) = self.control(bound, "audit-replay").await {
                assert_eq!(sub.sub_id, "audit-replay");
                return;
            }
        }
    }
}

struct Redials {
    pending: mpsc::UnboundedReceiver<Upstream>,
    bound: Bound,
}

impl connetto_client::TransportFactory for Redials {
    type Transport = Upstream;
    type Error = FakeTransportError;

    async fn connect(&mut self) -> Result<Upstream, Self::Error> {
        self.bound
            .run("audit-redial", self.pending.recv())
            .await
            .ok_or(FakeTransportError::Closed)
    }
}

pub struct Harness {
    pub hub: RelayHub,
    content: bool,
    abort: AbortHandle,
    recovery: mpsc::UnboundedSender<Upstream>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl Harness {
    pub async fn new(bound: Bound, recovery: Option<Upstream>, replay: bool) -> (Self, Peer) {
        Self::build(bound, recovery, replay, None).await
    }

    pub async fn with_content(bound: Bound, content: ContentArchive<BrowserStore>) -> (Self, Peer) {
        Self::build(bound, None, false, Some(content)).await
    }

    async fn build(
        bound: Bound,
        recovery: Option<Upstream>,
        replay: bool,
        content: Option<ContentArchive<BrowserStore>>,
    ) -> (Self, Peer) {
        let (initial, peer) = upstream(bound, true, false);
        let schema = SyncSchema::new(SchemaBundle::new(
            "-- connetto:major 1", "",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT); CREATE TABLE photos (id INTEGER PRIMARY KEY, content_id BLOB NOT NULL)",
            Vec::<(String, String)>::new(), Vec::<String>::new(), None::<&str>,
        ).expect("audit schema"));
        let worker = bound
            .run(
                "initial upstream handshake",
                ClientBuilder::new(
                    schema,
                    FirstThen::new(initial, || ready(Err(FakeTransportError::Closed))),
                )
                .signed_in(
                    HeldCredential::new(Grant::new("user:audit"), "audit").expect("audit identity"),
                )
                .connect_driven(),
            )
            .await
            .expect("connect audit worker");
        let (recovery_tx, pending) = mpsc::unbounded_channel();
        if let Some(recovery) = recovery {
            recovery_tx.send(recovery).expect("queue initial recovery");
        }
        let reconnect = HubReconnect {
            factory: Redials { pending, bound },
            sleeper: connetto_web::workers::sleep,
            policy: ReconnectPolicy::new(),
            upstream: if replay {
                vec![(
                    "audit-replay".to_owned(),
                    SubscriptionSpec::new("SELECT * FROM items"),
                )]
            } else {
                Vec::new()
            },
        };
        let has_content = content.is_some();
        let (hub, pump): (_, LocalBoxFuture<'static, Result<(), RelayError>>) =
            if let Some(content) = content {
                let (hub, pump, _notices) = RelayHub::with_reconnect_and_content(
                    worker,
                    ":memory:",
                    reconnect,
                    content,
                    BrowserHttp::new(),
                )
                .expect("content audit relay");
                (hub, Box::pin(pump))
            } else {
                let (hub, pump, _notices) =
                    RelayHub::with_reconnect(worker, ":memory:", reconnect).expect("audit relay");
                (hub, Box::pin(pump))
            };
        let (abort, registration) = AbortHandle::new_pair();
        spawn_local(async move {
            if let Ok(result) = Abortable::new(pump, registration).await {
                result.expect("audit relay pump");
            }
        });
        (
            Self {
                hub,
                content: has_content,
                abort,
                recovery: recovery_tx,
            },
            peer,
        )
    }

    pub async fn redial(&self, bound: Bound) -> Peer {
        let (transport, mut peer) = upstream(bound, false, false);
        self.recovery
            .send(transport)
            .expect("queue replacement attachment");
        peer.handshake(bound).await;
        peer.accept();
        peer
    }

    pub async fn tab(&self, bound: Bound, name: &str) -> Tab {
        let channel = web_sys::MessageChannel::new().expect("audit message channel");
        let transport = MessageTransport::<MessagePort>::new(channel.port1());
        let id = if self.content {
            self.hub.attach_with_content(transport)
        } else {
            self.hub.attach(transport)
        };
        let mut tab = Tab {
            id,
            wire: MessageTransport::<MessagePort>::new(channel.port2()),
        };
        tab.send(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, name).with_major(Major::new(1).expect("major one")),
        ))
        .await;
        loop {
            if let IncomingFrame::Control(ControlMessage::HandshakeAck(_)) =
                tab.frame(bound, name).await
            {
                return tab;
            }
        }
    }
}

pub struct Tab {
    pub id: TabId,
    pub wire: MessageTransport<MessagePort>,
}

impl Tab {
    pub async fn send(&mut self, message: ControlMessage) {
        self.wire
            .send_control(message)
            .await
            .expect("send tab control");
    }

    pub async fn audit(&mut self, id: &str, author: u8, seqs: Vec<u64>, remaining_ms: u64) {
        self.send(ControlMessage::CourierAuditRequest(CourierAuditRequest {
            request_id: id.to_owned(),
            author: [author; 32],
            seqs,
            remaining_ms,
        }))
        .await;
    }

    pub async fn cancel(&mut self, id: &str) {
        self.send(ControlMessage::CourierAuditCancel(CourierAuditCancel {
            request_id: id.to_owned(),
        }))
        .await;
    }

    pub async fn ping(&mut self, nonce: u64) {
        self.send(ControlMessage::Ping(Ping { nonce })).await;
    }

    pub async fn frame(&mut self, bound: Bound, id: &str) -> IncomingFrame {
        bound
            .run(id, self.wire.recv())
            .await
            .expect("tab transport")
            .expect("tab remains open")
    }

    pub async fn answer(&mut self, bound: Bound, id: &str, expected: CourierAuditOutcome) {
        loop {
            if let IncomingFrame::Control(ControlMessage::CourierAuditAnswer(answer)) =
                self.frame(bound, id).await
            {
                assert_eq!(answer.request_id, id);
                assert_eq!(answer.outcome, expected);
                return;
            }
        }
    }

    pub async fn answer_before_pong(&mut self, bound: Bound, ids: &[&str], nonce: u64) {
        let mut pending: std::collections::HashSet<_> = ids.iter().copied().collect();
        while !pending.is_empty() {
            match self.frame(bound, "stalled audit answers").await {
                IncomingFrame::Control(ControlMessage::CourierAuditAnswer(answer)) => {
                    assert!(
                        pending.remove(answer.request_id.as_str()),
                        "unexpected audit ID {}",
                        answer.request_id
                    );
                    assert_eq!(answer.outcome, CourierAuditOutcome::Expired);
                }
                IncomingFrame::Control(ControlMessage::Pong(pong)) => {
                    assert_ne!(
                        pong.nonce, nonce,
                        "application frame overtook stalled attachment"
                    );
                }
                _ => {}
            }
        }
    }

    pub async fn barrier(&mut self, bound: Bound, nonce: u64) {
        self.ping(nonce).await;
        self.pong(bound, nonce).await;
    }

    pub async fn pong(&mut self, bound: Bound, nonce: u64) {
        loop {
            match self.frame(bound, "tab ping barrier").await {
                IncomingFrame::Control(ControlMessage::Pong(pong)) if pong.nonce == nonce => return,
                IncomingFrame::Control(ControlMessage::CourierAuditAnswer(answer)) => {
                    panic!("unexpected audit answer {answer:?}");
                }
                _ => {}
            }
        }
    }
}
