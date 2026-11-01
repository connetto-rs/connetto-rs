//! The exchange's message and its transfers, in the order one exchange runs
//! (R77).

use connetto_core::exchange::{EndOutcome, ManifestRow, SignedOffer};
use connetto_core::schema::{Major, SchemaVersion};
use serde::{Deserialize, Serialize};

/// The rows one manifest message carries, the bound a manifest over is
/// refused on both ends of the link.
pub const MAX_MANIFEST_ROWS: usize = 8192;

/// One exchange message between two linked devices, the signed offer or a
/// transfer of the peer's writer chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// The device's own signed offer.
    Offer(SignedOffer),
    /// A transfer of the peer's writer chain, its request, manifests,
    /// bodies and end.
    Transfer(Transfer),
}

/// A transfer of one exchange's writer chain, in the order one exchange
/// runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transfer {
    /// A request for the manifest of `from..=to` and the bodies of the
    /// entries captured under `build`.
    Request {
        /// The id every reply quotes.
        request_id: u64,
        /// The first sequence requested.
        from: u64,
        /// The last sequence requested, the newest of the verified offer.
        to: u64,
        /// The requester's own build, whose entries cross with their bodies.
        build: (Major, SchemaVersion),
    },
    /// A manifest message, at most [`MAX_MANIFEST_ROWS`] rows.
    Manifest {
        /// The id the request named.
        request_id: u64,
        /// The hash the range chains from.
        previous: [u8; 32],
        /// The range's rows, in sequence order.
        rows: Vec<ManifestRow>,
        /// Whether this is the range's last manifest message.
        last: bool,
    },
    /// The body of one entry.
    Body {
        /// The id the request named.
        request_id: u64,
        /// The entry's sequence.
        seq: u64,
        /// The entry's changeset, the bytes its manifest row digests.
        #[serde(with = "serde_bytes")]
        changeset: Vec<u8>,
    },
    /// The end of a request's stream, with its outcome.
    End {
        /// The id the request named.
        request_id: u64,
        /// How the request ended.
        outcome: EndOutcome,
    },
}

impl Message {
    /// The message name and article, for diagnostics that must not print a
    /// payload.
    #[must_use]
    pub fn named(&self) -> &'static str {
        match self {
            Message::Offer(_) => "an Exchange::Offer",
            Message::Transfer(transfer) => transfer.named(),
        }
    }

    /// Whether the message keeps the wire's row bound, which only a
    /// manifest can break.
    ///
    /// # Errors
    ///
    /// [`BoundsError::TooManyRows`] when a manifest carries more than
    /// [`MAX_MANIFEST_ROWS`] rows.
    pub fn within_bounds(&self) -> Result<(), BoundsError> {
        if let Message::Transfer(Transfer::Manifest { rows, .. }) = self
            && rows.len() > MAX_MANIFEST_ROWS
        {
            return Err(BoundsError::TooManyRows);
        }
        Ok(())
    }
}

impl Transfer {
    /// The transfer's name and article, for diagnostics that must not print
    /// a payload.
    #[must_use]
    pub fn named(&self) -> &'static str {
        match self {
            Transfer::Request { .. } => "an Exchange::Request",
            Transfer::Manifest { .. } => "an Exchange::Manifest",
            Transfer::Body { .. } => "an Exchange::Body",
            Transfer::End { .. } => "an Exchange::End",
        }
    }
}

/// A message breaks the wire's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BoundsError {
    /// A manifest carries more than [`MAX_MANIFEST_ROWS`] rows.
    #[error("the manifest carries more rows than the bound")]
    TooManyRows,
}

/// The manifest messages of one range, `rows` cut into runs of at most
/// [`MAX_MANIFEST_ROWS`] with the last one marked. An empty range is one
/// empty last message.
pub fn manifest(
    request_id: u64,
    previous: [u8; 32],
    rows: Vec<ManifestRow>,
) -> impl Iterator<Item = Message> {
    let count = rows.len().div_ceil(MAX_MANIFEST_ROWS).max(1);
    let mut rows = rows.into_iter();
    (0..count).map(move |index| {
        Message::Transfer(Transfer::Manifest {
            request_id,
            previous,
            rows: rows.by_ref().take(MAX_MANIFEST_ROWS).collect(),
            last: index + 1 == count,
        })
    })
}

/// Whether an entry captured under `major` by an author holding `builds`
/// is one `build` applies, which decides whether its body crosses
/// (decision 8). Both ends run the same test, the sender over its own
/// builds and the receiver over the builds the author's offer names.
#[must_use]
pub fn captured_under(
    builds: &[(Major, SchemaVersion)],
    major: Major,
    build: &(Major, SchemaVersion),
) -> bool {
    major == build.0
        && builds
            .iter()
            .any(|(held, version)| *held == major && *version == build.1)
}

#[cfg(test)]
mod tests;
