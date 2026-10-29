//! The link's frame protocol, a four-byte big-endian length ahead of a
//! messagepack `PeerFrame`.

use std::fmt;
use std::io;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use connetto_core::exchange::{EndOutcome, Frontier, ManifestRow, Retraction};
use connetto_core::schema::{Major, SchemaVersion};

use crate::error::CloseReason;

/// The frame length ceiling, four megabytes.
pub(crate) const MAX_FRAME_LEN: u32 = 4 * 1024 * 1024;
/// The rows one manifest frame carries, the bound a manifest over is
/// refused on both ends of the link.
pub const MAX_MANIFEST_ROWS: usize = 8192;
/// The frame version this crate speaks.
pub(crate) const PROTOCOL_VERSION: u16 = 2;

/// A peer-link frame with hotspot passphrases redacted from its `Debug` output.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerFrame {
    /// The first frame from each side, naming the version, the dial the side
    /// made, and the revocation list numbers the side keeps.
    Hello {
        /// The frame version the side speaks.
        version: u16,
        /// The dial counter of the side that opened the connection, 0 from
        /// the listener.
        dial: u64,
        /// The kept lists, as `(issuer key id, number)` pairs.
        numbers: Vec<(ByteBuf, u64)>,
    },
    /// A revocation list the peer lacks.
    List {
        /// The list, DER.
        list: ByteBuf,
        /// The signer's certificate, DER.
        signer: ByteBuf,
    },
    /// A deliberate close the sender reports, so both ends name the same
    /// reason.
    Close {
        /// The reason the sender closes with.
        reason: CloseReason,
    },
    /// The renewed chain a grant replaced, which the sender hands to every
    /// live link still holding the old one (decision 10).
    Certificate {
        /// The renewed leaf, DER.
        leaf: ByteBuf,
        /// The leaf's issuer certificate, DER.
        issuer: ByteBuf,
    },
    /// The hotspot details the host hands the joiner in the Bluetooth
    /// exchange, once both have proved their identity (R76 slice 5).
    Offer {
        /// The hosted network's name.
        ssid: String,
        /// The hosted network's passphrase.
        passphrase: String,
        /// The security the network carries, 0 for WPA2 and 1 for WPA3.
        security: u8,
        /// The host's peer port, while the host serves.
        port: Option<u16>,
    },
    /// One of the exchange's five frames (R77).
    Exchange(ExchangeFrame),
    /// A liveness probe.
    Ping,
    /// The answer to a liveness probe.
    Pong,
}

/// The exchange's five frames, in the wire order an exchange runs (R77).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeFrame {
    /// The device's own offer, its builds, its settled and newest frontiers,
    /// its retraction log and the signature over them all.
    Offer {
        /// The author's builds, as `(major, schema version)` pairs.
        builds: Vec<(Major, SchemaVersion)>,
        /// The author's acked frontier, `None` when it has settled nothing.
        settled: Option<Frontier>,
        /// The author's newest entry, `None` when it holds no queue.
        newest: Option<Frontier>,
        /// The author's retraction log.
        retractions: Vec<Retraction>,
        /// The device key's signature over the offer's fields.
        signature: Vec<u8>,
    },
    /// A request for the manifest and bodies from `from` onward.
    Request {
        /// The id the replies quote.
        request_id: u64,
        /// The first sequence requested.
        from: u64,
    },
    /// A manifest frame, at most [`MAX_MANIFEST_ROWS`] rows.
    Manifest {
        /// The id the request named.
        request_id: u64,
        /// The hash the range chains from.
        previous: [u8; 32],
        /// The range's rows, in sequence order.
        rows: Vec<ManifestRow>,
        /// Whether this is the range's last manifest frame.
        last: bool,
    },
    /// A changeset body for one sequence.
    Body {
        /// The id the request named.
        request_id: u64,
        /// The sequence the changeset covers.
        seq: u64,
        /// The changeset bytes.
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

impl ExchangeFrame {
    /// The frame name and article for diagnostics, naming the variant.
    pub(crate) fn named(&self) -> &'static str {
        match self {
            ExchangeFrame::Offer { .. } => "an Exchange::Offer",
            ExchangeFrame::Request { .. } => "an Exchange::Request",
            ExchangeFrame::Manifest { .. } => "an Exchange::Manifest",
            ExchangeFrame::Body { .. } => "an Exchange::Body",
            ExchangeFrame::End { .. } => "an Exchange::End",
        }
    }

    /// Whether the frame keeps the wire's row bound, which only a manifest
    /// can break.
    ///
    /// # Errors
    ///
    /// [`FrameError::TooManyRows`] when a manifest carries more than
    /// [`MAX_MANIFEST_ROWS`] rows.
    pub(crate) fn within_bounds(&self) -> Result<(), FrameError> {
        if let ExchangeFrame::Manifest { rows, .. } = self
            && rows.len() > MAX_MANIFEST_ROWS
        {
            return Err(FrameError::TooManyRows);
        }
        Ok(())
    }
}

impl PeerFrame {
    /// The payload-free frame name and article for diagnostics.
    pub(crate) fn named(&self) -> &'static str {
        match self {
            PeerFrame::Hello { .. } => "a Hello",
            PeerFrame::List { .. } => "a List",
            PeerFrame::Close { .. } => "a Close",
            PeerFrame::Certificate { .. } => "a Certificate",
            PeerFrame::Offer { .. } => "an Offer",
            PeerFrame::Exchange(frame) => frame.named(),
            PeerFrame::Ping => "a Ping",
            PeerFrame::Pong => "a Pong",
        }
    }
}
impl fmt::Debug for PeerFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerFrame::Hello {
                version,
                dial,
                numbers,
            } => f
                .debug_struct("Hello")
                .field("version", version)
                .field("dial", dial)
                .field("numbers", numbers)
                .finish(),
            PeerFrame::List { list, signer } => f
                .debug_struct("List")
                .field("list", list)
                .field("signer", signer)
                .finish(),
            PeerFrame::Close { reason } => f.debug_struct("Close").field("reason", reason).finish(),
            PeerFrame::Certificate { leaf, issuer } => f
                .debug_struct("Certificate")
                .field("leaf", leaf)
                .field("issuer", issuer)
                .finish(),
            PeerFrame::Offer {
                ssid,
                security,
                port,
                ..
            } => f
                .debug_struct("Offer")
                .field("ssid", ssid)
                .field("passphrase", &"<redacted>")
                .field("security", security)
                .field("port", port)
                .finish(),
            PeerFrame::Exchange(frame) => f.debug_tuple("Exchange").field(frame).finish(),
            PeerFrame::Ping => f.debug_struct("Ping").finish(),
            PeerFrame::Pong => f.debug_struct("Pong").finish(),
        }
    }
}

/// A frame decode or I/O failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FrameError {
    /// The link's I/O failed or ended.
    #[error("the link's I/O failed")]
    Io(#[source] io::Error),
    /// The peer's frame exceeds the ceiling.
    #[error("the frame exceeds the ceiling")]
    TooLong,
    /// A manifest carries more rows than the bound.
    #[error("the manifest carries more rows than the bound")]
    TooManyRows,
    /// The frame is not a `PeerFrame`.
    #[error("the frame does not decode")]
    Malformed,
}

/// The wire's row bound, which only an exchange manifest can break, checked
/// on both ends of the link.
fn wire_bounds(frame: &PeerFrame) -> Result<(), FrameError> {
    match frame {
        PeerFrame::Exchange(frame) => frame.within_bounds(),
        _ => Ok(()),
    }
}

/// Read one frame, or `None` when the link ends cleanly.
pub(crate) async fn read_frame<R: AsyncRead + Unpin>(
    rd: &mut R,
) -> Result<Option<PeerFrame>, FrameError> {
    let mut len = [0u8; 4];
    match rd.read_exact(&mut len).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(FrameError::Io(err)),
    }
    let len = u32::from_be_bytes(len);
    if len > MAX_FRAME_LEN {
        return Err(FrameError::TooLong);
    }
    let mut body = vec![0u8; usize::try_from(len).expect("a u32 length fits a usize")];
    rd.read_exact(&mut body).await.map_err(FrameError::Io)?;
    let frame: PeerFrame = rmp_serde::from_slice(&body).map_err(|_| FrameError::Malformed)?;
    wire_bounds(&frame)?;
    Ok(Some(frame))
}

/// Frames read off a stream across calls, keeping a partly arrived frame
/// when a `select!` drops the read, so a frame loop never loses its place.
#[derive(Debug, Default)]
pub(crate) struct FrameReader {
    /// The bytes read and not yet decoded.
    pending: Vec<u8>,
}

impl FrameReader {
    /// The next frame, or `None` when the link ends cleanly between frames.
    /// Dropping the future before it completes loses no bytes.
    pub(crate) async fn next<R: AsyncRead + Unpin>(
        &mut self,
        rd: &mut R,
    ) -> Result<Option<PeerFrame>, FrameError> {
        let mut chunk = [0u8; 16 * 1024];
        loop {
            if let Some(frame) = self.decode()? {
                return Ok(Some(frame));
            }
            let read = rd.read(&mut chunk).await.map_err(FrameError::Io)?;
            if read == 0 {
                return if self.pending.is_empty() {
                    Ok(None)
                } else {
                    Err(FrameError::Io(io::ErrorKind::UnexpectedEof.into()))
                };
            }
            self.pending.extend_from_slice(&chunk[..read]);
        }
    }

    /// Decode one whole frame from the pending bytes, if they hold one.
    fn decode(&mut self) -> Result<Option<PeerFrame>, FrameError> {
        let Some(len) = self.pending.first_chunk::<4>() else {
            return Ok(None);
        };
        let len = u32::from_be_bytes(*len);
        if len > MAX_FRAME_LEN {
            return Err(FrameError::TooLong);
        }
        let end = 4 + usize::try_from(len).expect("a u32 length fits a usize");
        if self.pending.len() < end {
            return Ok(None);
        }
        let frame: PeerFrame =
            rmp_serde::from_slice(&self.pending[4..end]).map_err(|_| FrameError::Malformed)?;
        self.pending.drain(..end);
        wire_bounds(&frame)?;
        Ok(Some(frame))
    }
}

/// Write one frame.
pub(crate) async fn write_frame<W: AsyncWrite + Unpin>(
    wr: &mut W,
    frame: &PeerFrame,
) -> Result<(), FrameError> {
    wire_bounds(frame)?;
    let body = rmp_serde::to_vec_named(frame).map_err(|_| FrameError::Malformed)?;
    if body.len() > usize::try_from(MAX_FRAME_LEN).expect("a 32-bit constant") {
        return Err(FrameError::TooLong);
    }
    let len = u32::try_from(body.len()).expect("checked against the ceiling");
    wr.write_all(&len.to_be_bytes())
        .await
        .map_err(FrameError::Io)?;
    wr.write_all(&body).await.map_err(FrameError::Io)?;
    wr.flush().await.map_err(FrameError::Io)?;
    Ok(())
}
