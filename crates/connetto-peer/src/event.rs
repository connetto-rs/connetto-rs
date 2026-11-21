//! What the node tells the device about its links.

use std::time::SystemTime;

use connetto_core::device_cert::DeviceIdentity;
use connetto_exchange::Message;
use connetto_exchange::media::Media;

use crate::CloseReason;

/// A change to the node's links, or a list one of them delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerEvent {
    /// A peer key is now linked.
    Linked {
        /// The peer the link reached.
        peer: DeviceIdentity,
        /// The link's number, unique within the node, which a media send
        /// names so a report never crosses on a later link (R79).
        link: u64,
        /// The peer's `SubjectPublicKeyInfo`, DER, which its offers verify
        /// against.
        public_key: Vec<u8>,
        /// The bot list the peer's certificate carries, `None` for a
        /// certificate without the extension, which readers bind as `NULL`
        /// (R77 decision 12).
        bot_list: Option<String>,
        /// The end of the peer's certificate, which a receiver keeps the
        /// peer's retained entries until (R78 decision 17).
        expires_at: SystemTime,
    },
    /// A live link to a peer was replaced by a new one on the same identity
    /// (a duplicate re-link), so the old link's frames are gone; the peer's
    /// identity, key, bot list and certificate end are re-presented.
    LinkReplaced {
        /// The peer the re-link reached.
        peer: DeviceIdentity,
        /// The new link's number.
        link: u64,
        /// The peer's `SubjectPublicKeyInfo`, DER, which its offers verify
        /// against.
        public_key: Vec<u8>,
        /// The bot list the peer's certificate carries, `None` for a
        /// certificate without the extension.
        bot_list: Option<String>,
        /// The end of the peer's certificate.
        expires_at: SystemTime,
    },
    /// A linked peer presented a renewed certificate on its link, on the
    /// same key (R78 slice 3a).
    Renewed {
        /// The peer that renewed.
        peer: DeviceIdentity,
        /// The end of the renewed certificate.
        expires_at: SystemTime,
    },
    /// A peer key lost its last link.
    Unlinked {
        /// The peer the link fell from.
        peer: DeviceIdentity,
        /// Why the link closed.
        reason: CloseReason,
    },
    /// A link delivered a revocation list the node lacked.
    ListReceived {
        /// The list, DER.
        list: Vec<u8>,
        /// The signer's certificate, DER.
        signer: Vec<u8>,
    },
    /// A link delivered an exchange message (R77).
    Exchange {
        /// The peer the message crossed from.
        peer: DeviceIdentity,
        /// The message as it crossed.
        message: Message,
    },
    /// A link delivered a media message (R79).
    Media {
        /// The peer the message crossed from.
        peer: DeviceIdentity,
        /// The number of the link it crossed on.
        link: u64,
        /// The message as it crossed.
        message: Media,
    },
}
