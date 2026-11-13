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
        /// The link's number, which every send to the peer names. A peer
        /// already linked reports a new link, which replaced the old one.
        link: u64,
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
        /// The number of the link it crossed on, which can be a link already
        /// replaced.
        link: u64,
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
        Some(MediaPort {
            sender: MediaSender { node },
            events,
        })
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
    sender: MediaSender,
    events: mpsc::UnboundedReceiver<MediaEvent>,
}

impl MediaPort {
    /// A port over a bare `node`, its events fed by the caller, for proofs
    /// that run a consumer without an enrolled client.
    #[cfg(feature = "test-support")]
    #[must_use]
    pub fn over(node: connetto_peer::Node, events: mpsc::UnboundedReceiver<MediaEvent>) -> Self {
        Self {
            sender: MediaSender { node },
            events,
        }
    }

    /// The next event, `None` once the client ends.
    pub async fn recv(&mut self) -> Option<MediaEvent> {
        self.events.recv().await
    }

    /// The sending half, which a consumer clones to send while it waits on
    /// [`recv`](Self::recv).
    #[must_use]
    pub fn sender(&self) -> &MediaSender {
        &self.sender
    }
}

/// The sending half of a [`MediaPort`].
#[derive(Clone)]
pub struct MediaSender {
    node: connetto_peer::Node,
}

impl MediaSender {
    /// Queue `message` on the media queue of `peer`'s link numbered `link`.
    ///
    /// # Errors
    ///
    /// [`PeerSendError`] with the message handed back when it breaks the
    /// wire's bounds, when the peer's live link is not that one, or when the
    /// link closes while the send waits.
    #[expect(
        clippy::result_large_err,
        reason = "the refusal hands the message back to its sender by value"
    )]
    pub async fn send(
        &self,
        peer: &DeviceIdentity,
        link: u64,
        message: Media,
    ) -> Result<(), PeerSendError<Media>> {
        self.node.send_media(peer, link, message).await
    }

    /// The peers with a live link now, in no order, for a consumer that
    /// starts after links came up.
    #[must_use]
    pub fn linked(&self) -> Vec<LinkedPeer> {
        self.node
            .linked()
            .into_iter()
            .filter_map(|link| {
                Some(LinkedPeer {
                    link: link.link?,
                    peer: link.peer,
                    bot_list: link.bot_list,
                })
            })
            .collect()
    }
}

/// One peer with a live link, as [`MediaSender::linked`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkedPeer {
    /// The peer.
    pub peer: DeviceIdentity,
    /// The link's number.
    pub link: u64,
    /// The bot list its certificate carries, `None` for a certificate
    /// without one.
    pub bot_list: Option<String>,
}
