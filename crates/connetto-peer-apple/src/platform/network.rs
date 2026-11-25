//! Network framework connections for the peer link (R112 decision 3).
//!
//! A connection or listener is configured before it starts and from then on
//! touched only on its own serial queue. A stream posts its sends, receives
//! and cancel there, and only retained handles' reference counts cross
//! threads.

use std::cell::Cell;
use std::ffi::CStr;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use block2::{Block, RcBlock};
use connetto_core::device_cert::DeviceIdentity;
use connetto_peer::{LinkError, Medium, Node};
use dispatch2::{DispatchData, DispatchQueue, DispatchRetained};
use objc2_network::{
    _nw_content_context_default_stream, _nw_content_context_final_send,
    _nw_parameters_configure_protocol_disable, NWConnection, NWContentContext, NWEndpoint,
    NWError, NWInterface, NWListener, NWObject, NWParameters, NWProtocolOptions, NWRetained,
    nw_connection_state_t, nw_endpoint_type_t, nw_error_domain_t, nw_listener_state_t,
};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

use crate::HOP_WITHIN;

/// The most one receive asks Network for.
const RECEIVE_MAX: u32 = 256 * 1024;
/// The most one write hands Network.
const SEND_CHUNK: usize = 256 * 1024;
/// The bytes in flight past which a write waits for Network to take them.
const SEND_HIGH_WATER: usize = 1024 * 1024;
/// The bound a connection gets to become ready, a TCP dial's connect bound.
const READY_WITHIN: Duration = Duration::from_secs(10);
/// The name prefix of Apple's peer-to-peer Wi-Fi interfaces.
const PEER_TO_PEER_PREFIX: &[u8] = b"awdl";

/// A retained Network object, moved across threads only to reach its queue.
struct Queued<T: NWObject + ?Sized>(NWRetained<T>);

// SAFETY: retaining and releasing a Network object is an atomic reference
// count, and every other call on a started object runs on its serial queue.
unsafe impl<T: NWObject + ?Sized> Send for Queued<T> {}

impl<T: NWObject + ?Sized> Clone for Queued<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: NWObject + ?Sized> Queued<T> {
    /// The object, for work running on its queue.
    fn on_queue(&self) -> &T {
        &self.0
    }
}

/// Reinterpret a block that spells C `bool` as `u8`, which `block2` cannot
/// encode (objc2 #890).
///
/// # Safety
///
/// `From` and `To` differ only where `To` has `bool` and `From` has `u8`,
/// which share the C calling convention on Apple's targets, and a returned
/// `u8` is 0 or 1.
unsafe fn recast<'a, From, To>(block: &'a Block<'static, From>) -> &'a Block<'static, To> {
    let block: *const Block<'static, From> = block;
    // SAFETY: `Block` is a marker over the same heap block, and the caller
    // guarantees the two signatures are ABI-identical.
    unsafe { &*block.cast::<Block<'static, To>>() }
}

/// What a stream's queue shares with its readers and writers.
#[derive(Default)]
struct Shared {
    /// Bytes received and not yet read, from `read_at` on.
    received: Vec<u8>,
    read_at: usize,
    /// Whether a receive is posted and unanswered.
    receiving: bool,
    /// Whether the peer finished sending.
    ended: bool,
    reader: Option<Waker>,
    /// Bytes handed to Network and not yet taken.
    in_flight: usize,
    writer: Option<Waker>,
    /// Why the connection failed, once it did.
    failed: Option<(io::ErrorKind, String)>,
    /// Whether the final send went out.
    shut: bool,
}

impl Shared {
    /// Record the first failure and wake both directions.
    fn fail(&mut self, error: &io::Error) {
        self.failed
            .get_or_insert_with(|| (error.kind(), error.to_string()));
        if let Some(reader) = self.reader.take() {
            reader.wake();
        }
        if let Some(writer) = self.writer.take() {
            writer.wake();
        }
    }

    fn failure(&self) -> Option<io::Error> {
        self.failed
            .as_ref()
            .map(|(kind, text)| io::Error::new(*kind, text.clone()))
    }
}

/// A Network framework connection as the byte stream a link runs over.
pub struct NwStream {
    connection: Queued<NWConnection>,
    queue: DispatchRetained<DispatchQueue>,
    shared: Arc<Mutex<Shared>>,
}

impl NwStream {
    /// Run `work` on the connection's queue.
    fn post(&self, work: impl FnOnce(&NWConnection) + Send + 'static) {
        let connection = self.connection.clone();
        self.queue.exec_async(move || work(connection.on_queue()));
    }
}

/// Post one receive, whose answer lands in `shared` and wakes the reader.
fn receive(connection: &NWConnection, shared: Arc<Mutex<Shared>>) {
    let done = RcBlock::new(
        move |data: *mut DispatchData,
              _context: *mut NWContentContext,
              complete: u8,
              error: *mut NWError| {
            let mut shared = shared.lock();
            shared.receiving = false;
            // SAFETY: Network passes a live dispatch data or NULL for the call.
            if let Some(data) = unsafe { data.as_ref() } {
                let read = std::mem::take(&mut shared.read_at);
                shared.received.drain(..read);
                shared.received.extend_from_slice(&data.to_vec());
            }
            // SAFETY: Network passes a live error or NULL for the call.
            if let Some(error) = unsafe { error.as_ref() } {
                shared.fail(&io_error(error));
            }
            if complete != 0 {
                shared.ended = true;
            }
            if let Some(reader) = shared.reader.take() {
                reader.wake();
            }
        },
    );
    // SAFETY: the block differs from `nw_connection_receive_completion_t`
    // only in its `bool` spelled `u8`.
    connection.receive(1, RECEIVE_MAX, unsafe { recast(&done) });
}

impl AsyncRead for NwStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut shared = self.shared.lock();
        let available = shared.received.len() - shared.read_at;
        if available > 0 {
            let taken = available.min(buf.remaining());
            let at = shared.read_at;
            buf.put_slice(&shared.received[at..at + taken]);
            shared.read_at += taken;
            return Poll::Ready(Ok(()));
        }
        if shared.ended {
            return Poll::Ready(Ok(()));
        }
        if let Some(error) = shared.failure() {
            return Poll::Ready(Err(error));
        }
        shared.reader = Some(cx.waker().clone());
        if !shared.receiving {
            shared.receiving = true;
            drop(shared);
            let shared = Arc::clone(&self.shared);
            self.post(move |connection| receive(connection, shared));
        }
        Poll::Pending
    }
}

impl AsyncWrite for NwStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let taken = buf.len().min(SEND_CHUNK);
        {
            let mut shared = self.shared.lock();
            if let Some(error) = shared.failure() {
                return Poll::Ready(Err(error));
            }
            if shared.in_flight >= SEND_HIGH_WATER {
                shared.writer = Some(cx.waker().clone());
                return Poll::Pending;
            }
            shared.in_flight += taken;
        }
        let bytes = buf[..taken].to_vec();
        let shared = Arc::clone(&self.shared);
        self.post(move |connection| {
            let data = DispatchData::from_bytes(&bytes);
            let done = RcBlock::new(move |error: *mut NWError| {
                let mut shared = shared.lock();
                shared.in_flight -= taken;
                // SAFETY: Network passes a live error or NULL for the call.
                if let Some(error) = unsafe { error.as_ref() } {
                    shared.fail(&io_error(error));
                }
                if let Some(writer) = shared.writer.take() {
                    writer.wake();
                }
            });
            // SAFETY: Network's constant default stream context, valid for the
            // process.
            let context: &NWContentContext = unsafe { _nw_content_context_default_stream };
            connection.send(Some(&data), context, false, &done);
        });
        Poll::Ready(Ok(taken))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut shared = self.shared.lock();
        if let Some(error) = shared.failure() {
            return Poll::Ready(Err(error));
        }
        if shared.in_flight == 0 {
            return Poll::Ready(Ok(()));
        }
        shared.writer = Some(cx.waker().clone());
        Poll::Pending
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.as_mut().poll_flush(cx)?.is_pending() {
            return Poll::Pending;
        }
        let first = !std::mem::replace(&mut self.shared.lock().shut, true);
        if first {
            self.post(|connection| {
                let done = RcBlock::new(|_error: *mut NWError| {});
                // SAFETY: Network's constant final-send context, valid for the
                // process.
                let context: &NWContentContext = unsafe { _nw_content_context_final_send };
                connection.send(None, context, true, &done);
            });
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for NwStream {
    fn drop(&mut self) {
        self.post(NWConnection::cancel);
    }
}

/// The I/O error a Network error stands for, the POSIX code where it has one.
fn io_error(error: &NWError) -> io::Error {
    if error.error_domain() == nw_error_domain_t::posix {
        io::Error::from_raw_os_error(error.error_code())
    } else {
        io::Error::other(format!(
            "Network error domain {} code {}",
            error.error_domain().0,
            error.error_code()
        ))
    }
}

/// The error a state handler names, or one naming `what` without it.
fn state_error(error: *mut NWError, what: &str) -> io::Error {
    // SAFETY: Network passes a live error or NULL for the handler call.
    unsafe { error.as_ref() }.map_or_else(|| io::Error::other(what.to_owned()), io_error)
}

/// TCP for the link, without Network's TLS since the link runs its own, and
/// with peer-to-peer interfaces allowed.
fn parameters() -> NWRetained<NWParameters> {
    let tcp = RcBlock::new(|options: NonNull<NWProtocolOptions>| {
        // SAFETY: Network passes live TCP options for the block's call.
        unsafe { options.as_ref() }.tcp_set_no_delay(true);
    });
    // SAFETY: Network's constant block leaving a protocol out, valid for the
    // process.
    let no_tls = unsafe { _nw_parameters_configure_protocol_disable };
    let parameters = NWParameters::new_secure_tcp(no_tls, &tcp);
    parameters.set_include_peer_to_peer(true);
    parameters
}

/// What a ready connection runs over, peer-to-peer when its path crosses an
/// `awdl` interface.
fn medium_of(connection: &NWConnection) -> Medium {
    let Some(path) = connection.current_path() else {
        return Medium::Network;
    };
    let peer_to_peer = Rc::new(Cell::new(false));
    let seen = Rc::clone(&peer_to_peer);
    let each = RcBlock::new(move |interface: NonNull<NWInterface>| -> u8 {
        // SAFETY: Network passes a live interface for the block's call.
        let interface = unsafe { interface.as_ref() };
        // SAFETY: an interface's name is NUL-terminated and lives as long as
        // the interface.
        let name = unsafe { CStr::from_ptr(interface.name().as_ptr()) };
        let found = name.to_bytes().starts_with(PEER_TO_PEER_PREFIX);
        seen.set(seen.get() || found);
        u8::from(!found)
    });
    // SAFETY: the block differs from `nw_path_enumerate_interfaces_block_t`
    // only in its `bool` spelled `u8`, and it returns 0 or 1.
    path.enumerate_interfaces(unsafe { recast(&each) });
    if peer_to_peer.get() {
        Medium::PeerToPeer
    } else {
        Medium::Network
    }
}

/// Start `connection` on its own queue, answering its stream at once and its
/// medium once it is ready, or why it never was.
fn open(connection: &NWConnection) -> (NwStream, oneshot::Receiver<io::Result<Medium>>) {
    let queue = DispatchQueue::new(Some(c"connetto.peer.connection"), None);
    let shared = Arc::new(Mutex::new(Shared::default()));
    let (ready_tx, ready_rx) = oneshot::channel();
    let ready = Cell::new(Some(ready_tx));
    let failing = Arc::clone(&shared);
    let this = connection.retain();
    let handler = RcBlock::new(move |state: nw_connection_state_t, error: *mut NWError| {
        if state == nw_connection_state_t::ready {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Ok(medium_of(&this)));
            }
        } else if state == nw_connection_state_t::waiting
            || state == nw_connection_state_t::failed
            || state == nw_connection_state_t::cancelled
        {
            let error = state_error(error, "the connection ended");
            failing.lock().fail(&error);
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(error));
            }
        }
    });
    connection.set_state_changed_handler(Some(&handler));
    // SAFETY: the queue is this connection's alone and serial, so the
    // handler's state is only ever touched there.
    unsafe { connection.set_queue(&queue) };
    connection.start();
    let stream = NwStream {
        connection: Queued(connection.retain()),
        queue,
        shared,
    };
    (stream, ready_rx)
}

/// Wait up to the ready bound for a connection's medium.
async fn ready(ready: oneshot::Receiver<io::Result<Medium>>) -> Result<Medium, LinkError> {
    match tokio::time::timeout(READY_WITHIN, ready).await {
        Ok(Ok(Ok(medium))) => Ok(medium),
        Ok(Ok(Err(error))) => Err(LinkError::Unreachable(error)),
        Ok(Err(_)) => Err(LinkError::Unreachable(io::Error::other(
            "the connection ended before it was ready",
        ))),
        Err(_) => Err(LinkError::Timeout),
    }
}

/// Dial `addr` through Network framework and link the peer there, refusing
/// as [`Node::link`] does before any connection opens.
///
/// # Errors
///
/// [`LinkError`] naming the failed phase or the refusal.
pub async fn link(node: &Node, addr: SocketAddr) -> Result<DeviceIdentity, LinkError> {
    if !node.peer_links_on() {
        return Err(LinkError::LinksOff);
    }
    if node.identity().is_none() {
        return Err(LinkError::NotServing);
    }
    let (stream, readiness) = {
        let endpoint = NWEndpoint::new_address(addr);
        let connection = NWConnection::new(&endpoint, &parameters());
        open(&connection)
    };
    let medium = ready(readiness).await?;
    node.link_over(stream, addr, medium).await
}

/// A Network framework listener handing every connection that becomes ready
/// to its node. Dropping it cancels the listener.
pub struct Listener {
    /// Locked only so a shared listener is `Sync`, the drop alone reaching it.
    network: Mutex<Queued<NWListener>>,
    queue: DispatchRetained<DispatchQueue>,
    port: u16,
}

impl Listener {
    /// Listen on `listen`, any free port for port 0, handing each ready
    /// connection to `node`'s accept.
    ///
    /// # Errors
    ///
    /// When Network refuses the listener, it fails, or it is not ready within
    /// [`HOP_WITHIN`].
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime.
    pub async fn start(node: Node, listen: SocketAddr) -> io::Result<Self> {
        let (mut started, readiness) = {
            let parameters = parameters();
            parameters.set_local_endpoint(Some(&NWEndpoint::new_address(listen)));
            let listener = NWListener::new(&parameters)
                .ok_or_else(|| io::Error::other("Network refused the listener"))?;
            let queue = DispatchQueue::new(Some(c"connetto.peer.listener"), None);
            let (ready_tx, ready_rx) = oneshot::channel();
            let ready = Cell::new(Some(ready_tx));
            let this = listener.clone();
            let state = RcBlock::new(move |state: nw_listener_state_t, error: *mut NWError| {
                let answer = if state == nw_listener_state_t::ready {
                    Ok(this.port())
                } else if state == nw_listener_state_t::waiting
                    || state == nw_listener_state_t::failed
                    || state == nw_listener_state_t::cancelled
                {
                    // A listener waits when its address is not up, which a
                    // TCP bind refuses at once.
                    Err(state_error(error, "the listener could not start"))
                } else {
                    return;
                };
                if let Some(ready) = ready.take() {
                    let _ = ready.send(answer);
                }
            });
            let runtime = tokio::runtime::Handle::current();
            let accept = RcBlock::new(move |connection: NonNull<NWConnection>| {
                // SAFETY: Network passes a live connection for the block's call.
                accept(&node, &runtime, unsafe { connection.as_ref() });
            });
            listener.set_state_changed_handler(Some(&state));
            listener.set_new_connection_handler(Some(&accept));
            // SAFETY: the queue is this listener's alone and serial, so its
            // handlers' state is only ever touched there.
            unsafe { listener.set_queue(&queue) };
            listener.start();
            let started = Self {
                network: Mutex::new(Queued(listener)),
                queue,
                port: 0,
            };
            (started, ready_rx)
        };
        started.port = match tokio::time::timeout(HOP_WITHIN, readiness).await {
            Ok(Ok(answer)) => answer?,
            Ok(Err(_)) => return Err(io::Error::other("the listener ended unready")),
            Err(_) => return Err(io::ErrorKind::TimedOut.into()),
        };
        Ok(started)
    }

    /// The port the listener accepts on.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let listener = self.network.get_mut().clone();
        self.queue
            .exec_async(move || listener.on_queue().cancel());
    }
}

/// Start an accepted connection and hand it to `node` once it is ready, on
/// the listener's queue.
fn accept(node: &Node, runtime: &tokio::runtime::Handle, connection: &NWConnection) {
    let endpoint = connection.endpoint();
    let remote = (endpoint.r#type() == nw_endpoint_type_t::address).then(|| endpoint.address());
    let (stream, readiness) = open(connection);
    // An accepted connection always names its address, and one that does not
    // is dropped, which cancels it.
    let Some(remote) = remote else {
        return;
    };
    let node = node.clone();
    runtime.spawn(async move {
        // A connection that loses Network's race fails before it is ready and
        // is dropped here.
        if let Ok(medium) = ready(readiness).await {
            node.accept_over(stream, remote, medium).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::{Duration, Instant};

    use connetto_core::device_cert::AttestationLevel;
    use connetto_peer::test_support::{DAY, Deployment, events, node, whole_second};
    use connetto_peer::{CloseReason, LinkError, Medium, PeerEvent};
    use tokio::sync::mpsc;

    use super::{HOP_WITHIN, Listener, READY_WITHIN, link};

    /// Wait up to ten seconds for the node's `Linked` event.
    async fn linked(events: &mut mpsc::UnboundedReceiver<PeerEvent>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !matches!(events.recv().await, Some(PeerEvent::Linked { .. }) | None) {}
        })
        .await
        .expect("the link comes up within the bound");
    }

    /// Two nodes link through a Network framework listener and dial over the
    /// loopback, each side's link read naming a network.
    #[tokio::test(flavor = "multi_thread")]
    async fn nodes_link_through_network_framework_over_a_network() {
        let now = whole_second();
        let deployment = Deployment::new(41, now);
        let issuer = deployment.add_issuer(now, [1; 16]);
        let alice = deployment.device(
            &issuer,
            "alice",
            now,
            DAY,
            [1; 16],
            AttestationLevel::Unproven,
        );
        let bob = deployment.device(
            &issuer,
            "bob",
            now,
            DAY,
            [2; 16],
            AttestationLevel::Unproven,
        );
        let (a_tx, mut a_rx) = events();
        let (b_tx, mut b_rx) = events();
        let a = node(deployment.root_der(), a_tx);
        let b = node(deployment.root_der(), b_tx);
        a.present(alice.identity());
        b.present(bob.identity());

        let listener = Listener::start(b.clone(), SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("the listener is ready");
        let bob_at = SocketAddr::from((Ipv4Addr::LOCALHOST, listener.port()));
        let peer = link(&a, bob_at).await.expect("the link completes");
        assert_eq!(peer, bob.identity);
        linked(&mut a_rx).await;
        linked(&mut b_rx).await;

        let a_links = a.linked();
        assert_eq!(
            a_links
                .iter()
                .map(|link| (&link.peer, link.remote, link.medium))
                .collect::<Vec<_>>(),
            [(&bob.identity, Some(bob_at), Some(Medium::Network))]
        );
        let b_links = b.linked();
        assert_eq!(
            b_links
                .iter()
                .map(|link| (&link.peer, link.medium))
                .collect::<Vec<_>>(),
            [(&alice.identity, Some(Medium::Network))]
        );
        assert!(
            b_links[0]
                .remote
                .is_some_and(|remote| remote.ip().is_loopback()),
            "the acceptor names the dialer's loopback address"
        );

        // A list Bob keeps after the link reaches Alice over it.
        let (list, signer) = deployment.issuer_list(&issuer, 1, &[], now);
        b.keep_list(list.clone(), signer.clone());
        let received = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match a_rx.recv().await {
                    Some(PeerEvent::ListReceived { list, signer }) => break (list, signer),
                    Some(_) => {}
                    None => panic!("the node's events closed"),
                }
            }
        })
        .await
        .expect("the list arrives within the bound");
        assert_eq!(received, (list, signer));

        a.stop(CloseReason::Closed);
        b.stop(CloseReason::Closed);
    }

    /// A dial to a port nobody listens on fails unreachable, well inside the
    /// ready bound.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dial_nobody_answers_fails_unreachable() {
        let now = whole_second();
        let deployment = Deployment::new(42, now);
        let issuer = deployment.add_issuer(now, [1; 16]);
        let alice = deployment.device(
            &issuer,
            "alice",
            now,
            DAY,
            [1; 16],
            AttestationLevel::Unproven,
        );
        let (a_tx, _a_rx) = events();
        let a = node(deployment.root_der(), a_tx);
        a.present(alice.identity());
        let closed = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .and_then(|socket| socket.local_addr())
            .expect("a free loopback port");

        let began = Instant::now();
        let dialled = link(&a, closed).await;
        assert!(
            matches!(dialled, Err(LinkError::Unreachable(_))),
            "the refused dial is unreachable, got {dialled:?}"
        );
        assert!(
            began.elapsed() < READY_WITHIN,
            "the refusal arrives before the ready bound"
        );
    }

    /// A listener on an address no interface holds fails to start, well
    /// inside the hop bound.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_listener_on_an_address_no_interface_holds_fails() {
        let deployment = Deployment::new(43, whole_second());
        let (b_tx, _b_rx) = events();
        let b = node(deployment.root_der(), b_tx);
        let unheld: SocketAddr = "192.0.2.1:443".parse().expect("a test-net address");

        let began = Instant::now();
        let started = Listener::start(b, unheld).await;
        assert!(
            started.is_err(),
            "the listener refuses an address no interface holds"
        );
        assert!(
            began.elapsed() < HOP_WITHIN,
            "the refusal arrives before the hop bound"
        );
    }
}
