//! One session served over a loopback transport, for the frame-level client.

use connetto_server::{LoopbackTransport, loopback};
use connetto_test_harness::Client;
use tracing::Instrument;

/// Serve one session on a spawned task and hand back the client end.
///
/// `serve` is the manager's own `serve` call, so this works for every manager
/// type a test builds. The task runs under the caller's span, so a record the
/// session emits carries whatever opened the connection.
pub fn connect<F>(serve: impl FnOnce(LoopbackTransport) -> F) -> Client
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let (server_end, client_end) = loopback();
    let session = serve(server_end);
    tokio::spawn(
        async move {
            let _ = session.await;
        }
        .instrument(tracing::Span::current()),
    );
    Client::new(client_end)
}
