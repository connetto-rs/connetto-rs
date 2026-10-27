//! Waiting on a running client's event stream inside a test's bound.

use connetto_client::ClientEvent;
use tokio::sync::broadcast::Receiver;
use tokio::time::Instant;

/// Read events until `wanted` arrives by `deadline`, failing on any event
/// `forbidden` names.
///
/// # Panics
///
/// When the deadline passes first, the stream closes, or a forbidden event
/// arrives.
pub async fn await_event(
    events: &mut Receiver<ClientEvent>,
    deadline: Instant,
    wanted: impl Fn(&ClientEvent) -> bool,
    forbidden: impl Fn(&ClientEvent) -> bool,
) {
    loop {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .expect("the awaited event arrives within the deadline")
            .expect("event stream closed");
        assert!(!forbidden(&event), "unexpected event {event:?}");
        if wanted(&event) {
            return;
        }
    }
}
