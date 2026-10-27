//! Who a connection writes as, the writer half of every write's identity (R75).
//!
//! Every write carries `(writer, sequence)`, and the server keeps one applied
//! frontier per writer that every path consults. A native device with an
//! enrolled key writes as that key and proves it once per connection by
//! signing the connection's [`Challenge`](super::Challenge) nonce. Every
//! other client, a browser profile, a build without a device identity or a
//! device before its first enrolment, writes under a number the server issues
//! and the client keeps with its replica, beside the [`WriterSecret`] it drew
//! when it asked for the number and presents wherever it names it.

use core::fmt;

use serde::{Deserialize, Serialize};

/// The domain separator of the message a device key signs to bind its writer.
///
/// Distinct from the bot registration's, so a signature over one connection's
/// nonce answers exactly one of the two questions.
pub const WRITER_BIND_DOMAIN: &[u8] = b"connetto writer bind v1\0";

/// The message a device key signs to bind its writer, the separator followed
/// by the connection's nonce.
#[must_use]
pub fn writer_bind_message(nonce: &[u8; 32]) -> Vec<u8> {
    let mut message = WRITER_BIND_DOMAIN.to_vec();
    message.extend_from_slice(nonce);
    message
}

/// The secret a replica draws when it asks to be issued a number writer, and
/// presents with the number in every later handshake, retirement and carried
/// entry's provenance (R70 decision 22).
///
/// The server stores only its hash. A restore rewinds the writer numbers with
/// the rows, so a number may be issued again to another replica, and the
/// secret is what keeps the replica that held it before from binding to it.
/// Drawn from the platform's entropy by the client, since the core carries
/// no entropy source.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterSecret([u8; 32]);

impl WriterSecret {
    /// The secret's length in bytes.
    pub const LEN: usize = 32;

    /// The secret holding `bytes`.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::LEN]) -> Self {
        Self(bytes)
    }

    /// The secret's bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }
}

/// Never prints the bytes, so a logged request cannot leak the secret.
impl fmt::Debug for WriterSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WriterSecret(..)")
    }
}

/// A writer, named as the wire names one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WriterRef {
    /// A device key, by its key id, the SHA-256 of its `SubjectPublicKeyInfo`.
    Device([u8; 32]),
    /// A number the server issued.
    Number(u64),
}

/// The writer a handshake asks to write as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriterRequest {
    /// Issue a fresh number writer, for a client that holds none. The server
    /// keeps the secret's hash, and a number issued without one can never be
    /// presented again, so only a connection that will not come back asks
    /// for one so.
    Issue {
        /// The secret later handshakes present with the issued number.
        secret: Option<WriterSecret>,
    },
    /// Write as this number, issued to the same account earlier under
    /// `secret`.
    Number {
        /// The number.
        number: u64,
        /// The secret it was issued under.
        secret: WriterSecret,
    },
    /// Write as a device key, bound by a [`BindDevice`] after the
    /// acknowledgement. No write is taken before it.
    Device,
}

/// A number writer no later handshake can present, for a connection that
/// will not come back.
impl Default for WriterRequest {
    fn default() -> Self {
        Self::Issue { secret: None }
    }
}

/// The number writer a handshake confirmed or issued, with its frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterGrant {
    /// The writer's number, the one the client asked for or a fresh one.
    pub number: u64,
    /// The highest sequence the server has settled for it, 0 for none. Every
    /// sequence at or below it is applied or refused, and the next write must
    /// carry the one after it.
    pub frontier: u64,
}

/// A device proves its key to write as it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindDevice {
    /// Client-chosen correlation token, echoed by the answer.
    pub request_id: String,
    /// The device key's id.
    pub key_id: [u8; 32],
    /// The device key's ECDSA P-256 SHA-256 signature over
    /// [`writer_bind_message`] of the connection's nonce, DER encoded.
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}

/// An archive's importer retires the writer the archive was made under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireWriter {
    /// Client-chosen correlation token, echoed by the answer.
    pub request_id: String,
    /// The writer to retire, one of the caller's own account.
    pub writer: WriterRef,
    /// The writer's secret when it is a number, absent for a device.
    pub secret: Option<WriterSecret>,
}

/// How the server answered a [`BindDevice`] or a [`RetireWriter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriterOutcome {
    /// The device key is this connection's writer from here on.
    Bound {
        /// The highest sequence settled for it, 0 for none.
        frontier: u64,
    },
    /// The writer named is retired, now or earlier.
    Retired,
    /// The key is no live enrolment of the caller's account, or the
    /// signature does not verify.
    NotEnrolled,
    /// The writer named is no writer of the caller's account, or a number
    /// presented without the secret it was issued under.
    Unknown,
}

/// The server's answer to a [`BindDevice`] or a [`RetireWriter`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterAnswer {
    /// Correlation token from the request this answers.
    pub request_id: String,
    /// What happened.
    pub outcome: WriterOutcome,
}

/// Where a restored or carried-forward entry came from.
///
/// An import, and a device whose writer retired, re-queue an entry under a
/// fresh writer and sequence, and the server settles the original through
/// this, so the logical write is applied once whichever copy arrives first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// The writer the entry was first written under.
    pub writer: WriterRef,
    /// Its sequence there.
    pub seq: u64,
    /// That writer's secret when it is a number, absent for a device. A
    /// provenance whose secret does not match names no writer, so the entry
    /// applies as an ordinary write.
    pub secret: Option<WriterSecret>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bind_signature_cannot_answer_a_bot_registration() {
        let nonce = [7; 32];
        assert_ne!(
            writer_bind_message(&nonce),
            super::super::registration::bot_registration_message(&nonce)
        );
    }

    #[test]
    fn writers_order_with_a_stable_key_for_queues() {
        let low = WriterRef::Device([0; 32]);
        let high = WriterRef::Device([1; 32]);
        assert!(low < high);
        assert!(WriterRef::Device([9; 32]) < WriterRef::Number(0));
        assert!(WriterRef::Number(1) < WriterRef::Number(2));
    }

    /// A secret never reaches a log through `Debug`, alone or inside the
    /// messages that carry it.
    #[test]
    fn a_writer_secret_never_prints_its_bytes() {
        let secret = WriterSecret::from_bytes([0xab; WriterSecret::LEN]);
        assert_eq!(format!("{secret:?}"), "WriterSecret(..)");
        let request = format!("{:?}", WriterRequest::Number { number: 3, secret });
        let provenance = format!(
            "{:?}",
            Provenance {
                writer: WriterRef::Number(3),
                seq: 1,
                secret: Some(secret),
            }
        );
        for printed in [request, provenance] {
            assert!(
                !printed.contains("171") && !printed.contains("ab"),
                "the bytes stay out of {printed}"
            );
        }
    }
}
