//! The fakes the hub's tests share: a scripted upstream the hub's reconnect
//! factory serves, the dials and waits they count, and the bounded reads
//! they put on a tab's channel.

// Each fake's consumers are the two hub test targets, and each target
// compiles this module alone, so whichever half of the fakes a target does
// not use is dead code from its side.
#![expect(
    dead_code,
    reason = "shared by the two hub test targets, each of which compiles this module alone and uses its own subset"
)]

use core::future::ready;
use core::time::Duration;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use connetto_client::{ClientBuilder, FirstThen, ReconnectPolicy, SyncSchema};
use connetto_core::messages::{
    BulkMessage, ControlMessage, FatalError, FatalErrorReason, Handshake, HandshakeAck,
    WriterGrant, WriterRequest,
};
use connetto_core::schema::{SchemaBundle, SchemaVersion};
use connetto_core::test_support::FakeTransportError;
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, Major, PROTOCOL_VERSION};
use connetto_web::{HubReconnect, MessageTransport, RelayHub};
use wasm_bindgen_futures::spawn_local;
use web_sys::MessagePort;

/// The major line the test schema source declares.
pub const SOURCE: &str = "-- connetto:major 1";

/// The major [`SOURCE`] declares, which a tab built from it presents.
pub fn major() -> Major {
    Major::declared_in(SOURCE).expect("the source declares major 1")
}

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)";

/// The client schema over [`DDL`].
fn schema() -> SyncSchema {
    SyncSchema::new(
        SchemaBundle::new(
            SOURCE,
            "",
            DDL,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares major 1"),
    )
}

/// How one fake upstream answers its handshake and then behaves.
pub struct Script {
    /// Refuse the handshake with this reason instead of acknowledging it.
    refuse: Option<FatalErrorReason>,
    /// Stay open after the inbox drains rather than reporting end of stream.
    silent: bool,
    /// The major's sunset to carry in the acknowledgement, in seconds, when
    /// the scenario names one.
    sunset: Option<u64>,
}

impl Script {
    pub fn refusing(reason: FatalErrorReason) -> Self {
        Self {
            refuse: Some(reason),
            silent: false,
            sunset: None,
        }
    }

    pub fn accepting(silent: bool) -> Self {
        Self {
            refuse: None,
            silent,
            sunset: None,
        }
    }

    /// Acknowledge carrying the major's sunset and stay open.
    pub fn accepting_with_sunset(silent: bool, sunset: u64) -> Self {
        Self {
            refuse: None,
            silent,
            sunset: Some(sunset),
        }
    }
}

/// A fake upstream that answers the handshake from its script and then ends
/// its stream, or stays open until the test hands it a close reason, as a
/// server that closes a live session with a reason does.
pub struct FakeUpstream {
    script: Script,
    close: tokio::sync::mpsc::UnboundedReceiver<FatalErrorReason>,
    inbox: Mutex<VecDeque<IncomingFrame>>,
}

impl FakeUpstream {
    pub fn scripted(
        script: Script,
    ) -> (Self, tokio::sync::mpsc::UnboundedSender<FatalErrorReason>) {
        let (close, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                script,
                close: rx,
                inbox: Mutex::new(VecDeque::new()),
            },
            close,
        )
    }

    fn answer(&self, writer: Option<WriterGrant>) -> IncomingFrame {
        match &self.script.refuse {
            Some(reason) => {
                IncomingFrame::Control(ControlMessage::FatalError(FatalError::new(reason.clone())))
            }
            None => IncomingFrame::Control(ControlMessage::HandshakeAck(HandshakeAck {
                connection_id: "connection-hub-fake".to_owned(),
                session_token: "00000000-0000-4000-8000-000000000000".to_owned(),
                resume_token: "run:00000000-0000-4000-8000-000000000000".to_owned(),
                current_cursor: Cursor::new(Vec::new()),
                schema_version: None::<SchemaVersion>,
                // The scenario's sunset, when it names one, so the tabs the
                // hub relays the acknowledgement to carry the same instant.
                sunset_at_secs: self.script.sunset,
                initial_credits: 64,
                writer,
            })),
        }
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the trait methods are async and the bodies finish without awaiting"
)]
impl Transport for FakeUpstream {
    type Error = FakeTransportError;

    async fn send_control(&mut self, message: ControlMessage) -> Result<(), FakeTransportError> {
        if let ControlMessage::Handshake(handshake) = &message {
            let writer = match &handshake.writer {
                WriterRequest::Device => None,
                WriterRequest::Issue { .. } | WriterRequest::Number { .. } => Some(WriterGrant {
                    number: 1,
                    frontier: 0,
                }),
            };
            self.inbox.lock().unwrap().push_back(self.answer(writer));
        }
        Ok(())
    }

    async fn send_bulk(&mut self, _message: BulkMessage) -> Result<(), FakeTransportError> {
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, FakeTransportError> {
        loop {
            let pending = {
                let mut inbox = self.inbox.lock().unwrap();
                inbox.pop_front()
            };
            if let Some(frame) = pending {
                return Ok(Some(frame));
            }
            if !self.script.silent {
                return Ok(None);
            }
            let Some(reason) = self.close.recv().await else {
                return Ok(None);
            };
            self.inbox.lock().unwrap().push_back(IncomingFrame::Control(
                ControlMessage::FatalError(FatalError::new(reason)),
            ));
        }
    }

    async fn close(&mut self) -> Result<(), FakeTransportError> {
        Ok(())
    }
}

/// A dial plan the hub's reconnect factory serves in order, with a count of
/// every dial the test can assert. The fakes' close senders stay held here,
/// so a plan fake never reports end of stream on its own.
pub struct Plan {
    scripts: Mutex<VecDeque<Script>>,
    closers: Mutex<Vec<tokio::sync::mpsc::UnboundedSender<FatalErrorReason>>>,
    dials: AtomicUsize,
    dialed: Mutex<Vec<&'static str>>,
}

impl Plan {
    pub fn new(scripts: Vec<Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            closers: Mutex::new(Vec::new()),
            dials: AtomicUsize::new(0),
            dialed: Mutex::new(Vec::new()),
        }
    }

    fn dial(&self) -> FakeUpstream {
        self.dials.fetch_add(1, Ordering::Relaxed);
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Script::accepting(false));
        self.dialed
            .lock()
            .unwrap()
            .push(if script.refuse.is_some() {
                "refusing"
            } else {
                "accepting"
            });
        let (upstream, close) = FakeUpstream::scripted(script);
        self.closers.lock().unwrap().push(close);
        upstream
    }

    pub fn dials(&self) -> usize {
        self.dials.load(Ordering::Relaxed)
    }

    pub fn dialed(&self) -> Vec<&'static str> {
        self.dialed.lock().unwrap().clone()
    }
}

/// The waits the hub asked its sleeper for, in order.
pub struct Waits {
    waits: Mutex<Vec<Duration>>,
}

impl Waits {
    pub fn new() -> Self {
        Self {
            waits: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, wait: Duration) {
        self.waits.lock().unwrap().push(wait);
    }

    pub fn take(&self) -> Vec<Duration> {
        self.waits.lock().unwrap().to_vec()
    }
}

/// A dialer handing out `transport` once and nothing after.
pub fn once<T: Transport + 'static>(
    transport: T,
) -> FirstThen<impl FnMut() -> core::future::Ready<Result<T, FakeTransportError>>> {
    FirstThen::new(transport, || ready(Err(FakeTransportError::Closed)))
}

/// A hub over a scripted upstream with the test's reconnect wiring, and the
/// tab end of a real message channel attached before the pump starts, so the
/// tab is registered before any upstream outcome can end the hub.
pub async fn hub_with_tab(
    worker_script: Script,
    plan: Arc<Plan>,
    waits: Arc<Waits>,
) -> (
    MessageTransport<MessagePort>,
    tokio::sync::mpsc::UnboundedSender<FatalErrorReason>,
) {
    let (worker, worker_close) = FakeUpstream::scripted(worker_script);
    let client = ClientBuilder::new(schema(), once(worker))
        .with_client_id("hub-fake-worker")
        .connect_driven()
        .await
        .expect("worker connect");
    let factory = move || {
        let upstream = plan.dial();
        ready(Ok::<_, FakeTransportError>(upstream))
    };
    let sleeper = move |wait: Duration| {
        waits.record(wait);
        connetto_web::workers::sleep(wait)
    };
    let (hub, pump, _notices) = RelayHub::with_reconnect(
        client,
        ":memory:",
        HubReconnect {
            factory,
            sleeper,
            policy: ReconnectPolicy::new(),
            upstream: vec![],
        },
    )
    .expect("hub meta");
    let channel = web_sys::MessageChannel::new().expect("message channel");
    hub.attach(MessageTransport::<MessagePort>::new(channel.port1()));
    spawn_local(async move {
        pump.await.expect("hub pump");
    });
    (
        MessageTransport::<MessagePort>::new(channel.port2()),
        worker_close,
    )
}

/// One frame from the tab, or a deadline failure when the hub goes quiet.
pub async fn next_frame(tab: &mut MessageTransport<MessagePort>, what: &str) -> IncomingFrame {
    next_frame_within(tab, what, Duration::from_secs(5)).await
}

/// One frame from the tab within `window`, or a deadline failure.
pub async fn next_frame_within(
    tab: &mut MessageTransport<MessagePort>,
    what: &str,
    window: Duration,
) -> IncomingFrame {
    let frame = tokio::select! {
        frame = tab.recv() => frame.expect("the tab's transport"),
        () = connetto_web::workers::sleep(window) => {
            panic!("the hub never {what} within {window:?}")
        }
    };
    match frame {
        Some(frame) => frame,
        None => panic!("the hub closed the tab before it {what}"),
    }
}

/// The refusal the tab draws, skipping whatever the hub sends first.
pub async fn next_fatal(tab: &mut MessageTransport<MessagePort>) -> FatalErrorReason {
    loop {
        let frame = next_frame(tab, "refused the tab").await;
        if let IncomingFrame::Control(ControlMessage::FatalError(fatal)) = frame {
            return fatal.reason;
        }
    }
}

/// The acknowledgement a tab handshake draws, skipping whatever the hub
/// sends first.
pub async fn handshake_ack(tab: &mut MessageTransport<MessagePort>) -> HandshakeAck {
    tab.send_control(ControlMessage::Handshake(
        Handshake::new(PROTOCOL_VERSION, "a-tab").with_major(major()),
    ))
    .await
    .expect("post the tab handshake");
    loop {
        let frame = next_frame(tab, "acknowledged the tab").await;
        if let IncomingFrame::Control(ControlMessage::HandshakeAck(ack)) = frame {
            return ack;
        }
    }
}

/// A tab handshake that lands its ack, skipping whatever the hub sends first.
pub async fn handshake(tab: &mut MessageTransport<MessagePort>) {
    let _ = handshake_ack(tab).await;
}
