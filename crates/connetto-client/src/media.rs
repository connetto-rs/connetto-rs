//! The media port: the peer link's media frames handed to one consumer, the
//! content client that serves and fetches file chunks (R79 decision 11).
//!
//! The enrolment task drives the peer node and forwards every media frame,
//! link and unlink to the port the application took. With no live port, a
//! linked peer's request is answered `Refused` at once, the answer a holder
//! gives for a file it lacks.

use std::sync::Arc;

use connetto_core::device_cert::DeviceIdentity;
use connetto_exchange::media::Media;
use connetto_peer::PeerSendError;
use parking_lot::Mutex;
use tokio::sync::mpsc;

/// What the port's consumer learns from the peer links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaEvent {
    /// A peer linked, with the bot list its certificate carries, `None` for a
    /// certificate without one.
    Linked {
        /// The linked peer.
        peer: DeviceIdentity,
        /// The certificate's bot list.
        bot_list: Option<String>,
    },
    /// A peer's last link fell.
    Unlinked {
        /// The peer that left.
        peer: DeviceIdentity,
    },
    /// A media message crossed from a linked peer.
    Message {
        /// The peer it crossed from.
        peer: DeviceIdentity,
        /// The message as it crossed.
        message: Media,
    },
}

/// The sender the enrolment task forwards to while a port lives.
#[derive(Clone, Default)]
pub(crate) struct MediaSlot(Arc<Mutex<Option<mpsc::UnboundedSender<MediaEvent>>>>);

impl MediaSlot {
    /// A fresh port over `node`, `None` while another port lives.
    pub(crate) fn take(&self, node: connetto_peer::Node) -> Option<MediaPort> {
        let mut slot = self.0.lock();
        if slot.as_ref().is_some_and(|sender| !sender.is_closed()) {
            return None;
        }
        let (sender, events) = mpsc::unbounded_channel();
        *slot = Some(sender);
        Some(MediaPort { node, events })
    }

    /// Hand `event` to the live port, answering it back when none lives.
    #[expect(
        clippy::result_large_err,
        reason = "the refusal hands the event back to the task by value"
    )]
    pub(crate) fn forward(&self, event: MediaEvent) -> Result<(), MediaEvent> {
        match self.0.lock().as_ref() {
            Some(sender) => sender.send(event).map_err(|refused| refused.0),
            None => Err(event),
        }
    }
}

/// One consumer's view of the peer links' media traffic.
pub struct MediaPort {
    node: connetto_peer::Node,
    events: mpsc::UnboundedReceiver<MediaEvent>,
}

impl MediaPort {
    /// The next event, `None` once the client ends.
    pub async fn recv(&mut self) -> Option<MediaEvent> {
        self.events.recv().await
    }

    /// Queue `message` on the media queue of the link to `peer`.
    ///
    /// # Errors
    ///
    /// [`PeerSendError`] with the message handed back when it breaks the
    /// wire's bounds, when the peer holds no live link, or when the link
    /// closes while the send waits.
    pub async fn send(
        &self,
        peer: &DeviceIdentity,
        message: Media,
    ) -> Result<(), PeerSendError<Media>> {
        self.node.send_media(peer, message).await
    }

    /// The peers linked now, each with its certificate's bot list, in no
    /// order, for a consumer that starts after links came up.
    #[must_use]
    pub fn linked(&self) -> Vec<(DeviceIdentity, Option<String>)> {
        self.node
            .linked()
            .into_iter()
            .map(|link| (link.peer, link.bot_list))
            .collect()
    }
}
