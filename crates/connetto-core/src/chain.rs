//! The hash chain a writer's pending entries form (R75 step 1, R74 decision
//! 12).
//!
//! Each entry hashes the entry before it, its own sequence, its capture
//! major and its provenance, and its changeset, as a Scuttlebutt feed links
//! each message to the previous one. The client keeps the hash with each
//! pending entry, and the server computes the same chain from the entries
//! it settles, so a direct write carries no hash on the wire. A device
//! writer signs the newest entry of a range it hands to a peer or a
//! courier, and the receiver checks the range back to the last entry it
//! verified. Because the head binds every entry's major and provenance, one
//! attestation serves both paths. The device key also signs one offer per
//! hand-off, and a manifest's rows recompute the chain from a verified head.

#[cfg(feature = "device-cert")]
use ring::signature::{ECDSA_P256_SHA256_ASN1, UnparsedPublicKey};
use sha2::{Digest, Sha256};

#[cfg(feature = "device-cert")]
use crate::device_cert::spki_point;
use crate::exchange::{Frontier, Origin, RetractCause, Retraction};
use crate::messages::{MutationRejectReason, WriterRef};
use crate::schema::{Major, SchemaVersion};

/// The hash the first entry of every writer chains from.
pub const GENESIS: [u8; 32] = [0; 32];

/// The domain separator of an entry hash.
const ENTRY_DOMAIN: &[u8] = b"connetto-chain-v2\0";

/// The domain separator of the message a device key signs over a range.
const SIGN_DOMAIN: &[u8] = b"connetto-chain-sign-v1\0";

/// The domain separator of the message a device key signs over an offer.
const OFFER_DOMAIN: &[u8] = b"connetto-exchange-offer-v1\0";

/// The hash of the entry at `seq` carrying `changeset`, chained from
/// `previous`, the hash of the entry at `seq - 1` or [`GENESIS`] for the
/// writer's first entry, and named by its capture `major` and, when the
/// entry was first written under another writer, its `provenance`.
#[must_use]
pub fn entry_hash(
    previous: &[u8; 32],
    seq: u64,
    major: Option<Major>,
    origin: Option<Origin>,
    changeset: &[u8],
) -> [u8; 32] {
    entry_hash_digest(
        previous,
        seq,
        major,
        origin,
        Sha256::digest(changeset).into(),
    )
}

/// The chain recomputed from `previous` through the rows in sequence
/// order, the last row's hash, or `previous` when the list is empty. Each
/// row names one entry's sequence, capture major, provenance and changeset
/// digest.
#[must_use]
pub fn chain_walk(
    previous: &[u8; 32],
    rows: impl IntoIterator<Item = (u64, Option<Major>, Option<Origin>, [u8; 32])>,
) -> [u8; 32] {
    let mut head = *previous;
    for (seq, major, origin, digest) in rows {
        head = entry_hash_digest(&head, seq, major, origin, digest);
    }
    head
}

fn entry_hash_digest(
    previous: &[u8; 32],
    seq: u64,
    major: Option<Major>,
    origin: Option<Origin>,
    digest: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(ENTRY_DOMAIN);
    hasher.update(previous);
    hasher.update(seq.to_be_bytes());
    // `Major` is non-zero, so zero stands for the absent major without a tag.
    hasher.update(major.map_or([0; 4], |major| major.get().to_be_bytes()));
    encode_origin(&mut hasher, origin);
    hasher.update(digest);
    hasher.finalize().into()
}

/// The one canonical shape of a provenance original, the absent one, a
/// device one or a number one.
fn encode_origin(out: &mut Sha256, origin: Option<Origin>) {
    match origin {
        None => out.update([0u8]),
        Some(origin) => match origin.writer {
            WriterRef::Device(key) => {
                out.update([1u8]);
                out.update(key);
                out.update(origin.seq.to_be_bytes());
            }
            WriterRef::Number(number) => {
                out.update([2u8]);
                out.update(number.to_be_bytes());
                out.update(origin.seq.to_be_bytes());
            }
        },
    }
}

/// The message a device key signs to vouch for its chain up to the entry at
/// `seq` whose hash is `head`, for a peer and a courier alike.
///
/// The key id is inside the message, so a signature over one writer's chain
/// cannot be presented as another writer's.
#[must_use]
pub fn range_message(key_id: &[u8; 32], seq: u64, head: &[u8; 32]) -> Vec<u8> {
    let mut message = Vec::with_capacity(SIGN_DOMAIN.len() + 32 + 8 + 32);
    message.extend_from_slice(SIGN_DOMAIN);
    message.extend_from_slice(key_id);
    message.extend_from_slice(&seq.to_be_bytes());
    message.extend_from_slice(head);
    message
}

/// The message a device key signs over an exchange offer: its settled and
/// newest frontiers, its builds and its retractions.
///
/// The key id is inside the message, so a signature over one writer's offer
/// cannot be presented as another writer's.
///
/// # Panics
///
/// When a list's length or a refusal's detail overflows its fixed-width
/// encoding.
#[must_use]
pub fn offer_message(
    key_id: &[u8; 32],
    settled: Option<&Frontier>,
    newest: Option<&Frontier>,
    builds: &[(Major, SchemaVersion)],
    retractions: &[Retraction],
) -> Vec<u8> {
    let mut message = Vec::new();
    message.extend_from_slice(OFFER_DOMAIN);
    message.extend_from_slice(key_id);
    encode_frontier(&mut message, settled);
    encode_frontier(&mut message, newest);
    message.extend_from_slice(
        &u64::try_from(builds.len())
            .expect("an offer's build count fits u64")
            .to_be_bytes(),
    );
    for (major, version) in builds {
        message.extend_from_slice(&major.get().to_be_bytes());
        message.extend_from_slice(
            &u32::try_from(version.hash().len())
                .expect("a schema version hash fits u32")
                .to_be_bytes(),
        );
        message.extend_from_slice(version.hash());
    }
    message.extend_from_slice(
        &u64::try_from(retractions.len())
            .expect("an offer's retraction count fits u64")
            .to_be_bytes(),
    );
    for retraction in retractions {
        message.extend_from_slice(&retraction.seq.to_be_bytes());
        encode_retract_cause(&mut message, &retraction.cause);
    }
    message
}

fn encode_frontier(out: &mut Vec<u8>, frontier: Option<&Frontier>) {
    match frontier {
        None => out.push(0),
        Some(frontier) => {
            out.push(1);
            out.extend_from_slice(&frontier.seq.to_be_bytes());
            out.extend_from_slice(&frontier.head);
        }
    }
}

fn encode_retract_cause(out: &mut Vec<u8>, cause: &RetractCause) {
    match cause {
        RetractCause::Rejected(reason) => {
            out.push(0);
            encode_reject_reason(out, reason);
        }
        RetractCause::Conflict => out.push(1),
        RetractCause::Abandoned => out.push(2),
    }
}

fn encode_reject_reason(out: &mut Vec<u8>, reason: &MutationRejectReason) {
    let (tag, detail) = match reason {
        MutationRejectReason::Unauthorized => (0, None),
        MutationRejectReason::SchemaMismatch => (1, None),
        MutationRejectReason::Constraint { detail } => (2, Some(detail)),
        MutationRejectReason::Malformed { detail } => (3, Some(detail)),
        MutationRejectReason::Other { detail } => (4, Some(detail)),
        MutationRejectReason::Indeterminate => (5, None),
        MutationRejectReason::Forked => (6, None),
    };
    out.push(tag);
    if let Some(detail) = detail {
        out.extend_from_slice(
            &u32::try_from(detail.len())
                .expect("a refusal detail fits u32")
                .to_be_bytes(),
        );
        out.extend_from_slice(detail.as_bytes());
    }
}

/// Whether `signature` is the key's ECDSA P-256 SHA-256 signature over the
/// range at `seq` headed by `head`, the check a peer and a courier run.
///
/// `public_key` is the `SubjectPublicKeyInfo` the certificate and the
/// server's writer row hold.
#[cfg(feature = "device-cert")]
#[must_use]
pub fn verify_range(
    public_key: &[u8],
    key_id: &[u8; 32],
    seq: u64,
    head: &[u8; 32],
    signature: &[u8],
) -> bool {
    verify_signature(public_key, &range_message(key_id, seq, head), signature)
}

/// Whether `signature` is the key's ECDSA P-256 SHA-256 signature over the
/// offer as [`offer_message`] encodes it.
///
/// `public_key` is the `SubjectPublicKeyInfo` the certificate and the
/// server's writer row hold.
#[cfg(feature = "device-cert")]
#[must_use]
pub fn verify_offer(
    public_key: &[u8],
    key_id: &[u8; 32],
    settled: Option<&Frontier>,
    newest: Option<&Frontier>,
    builds: &[(Major, SchemaVersion)],
    retractions: &[Retraction],
    signature: &[u8],
) -> bool {
    verify_signature(
        public_key,
        &offer_message(key_id, settled, newest, builds, retractions),
        signature,
    )
}

#[cfg(feature = "device-cert")]
fn verify_signature(public_key: &[u8], message: &[u8], signature: &[u8]) -> bool {
    let Some(point) = spki_point(public_key) else {
        return false;
    };
    UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, &point)
        .verify(message, signature)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair as _};

    use super::*;
    use crate::exchange::{Frontier, Origin, RetractCause, Retraction};
    use crate::messages::{MutationRejectReason, WriterRef};
    use crate::schema::{Major, SchemaVersion};

    fn major(value: u32) -> Major {
        Major::new(value).expect("the test value is a major")
    }

    fn version(nibble: u8) -> SchemaVersion {
        SchemaVersion::from_hash(vec![nibble; 32])
    }

    #[cfg(feature = "device-cert")]
    fn sign(key: &EcdsaKeyPair, message: &[u8]) -> Vec<u8> {
        key.sign(&SystemRandom::new(), message)
            .expect("the key signs")
            .as_ref()
            .to_vec()
    }

    #[cfg(feature = "device-cert")]
    /// A P-256 key and the `SubjectPublicKeyInfo` its certificate or writer
    /// row would hold.
    fn keypair() -> (EcdsaKeyPair, Vec<u8>) {
        let der =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &SystemRandom::new())
                .expect("the key generates");
        let pair = EcdsaKeyPair::from_pkcs8(
            &ECDSA_P256_SHA256_ASN1_SIGNING,
            der.as_ref(),
            &SystemRandom::new(),
        )
        .expect("the DER parses");
        let mut spki = vec![
            0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06,
            0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
        ];
        spki.extend_from_slice(pair.public_key().as_ref());
        (pair, spki)
    }

    #[test]
    fn every_input_moves_the_hash() {
        let base = entry_hash(&GENESIS, 1, Some(major(2)), None, b"changeset");
        assert_ne!(
            base,
            entry_hash(&[1; 32], 1, Some(major(2)), None, b"changeset"),
            "the predecessor is inside the hash"
        );
        assert_ne!(
            base,
            entry_hash(&GENESIS, 2, Some(major(2)), None, b"changeset"),
            "the sequence is inside the hash"
        );
        assert_ne!(
            base,
            entry_hash(&GENESIS, 1, Some(major(3)), None, b"changeset"),
            "the capture major is inside the hash"
        );
        assert_ne!(
            base,
            entry_hash(&GENESIS, 1, None, None, b"changeset"),
            "an absent major is not a present one"
        );
        for n in 1..=4 {
            let hash = entry_hash(&GENESIS, 1, Some(major(n)), None, b"changeset");
            if n != 2 {
                assert_ne!(base, hash, "every present major hashes apart from the base");
            }
            for m in 1..=4 {
                if m != n {
                    assert_ne!(
                        hash,
                        entry_hash(&GENESIS, 1, Some(major(m)), None, b"changeset"),
                        "every present major hashes apart from the others"
                    );
                }
            }
        }
        assert_ne!(
            base,
            entry_hash(&GENESIS, 1, Some(major(2)), None, b"changesex"),
            "the changeset is inside the hash"
        );
        assert_eq!(
            base,
            entry_hash(&GENESIS, 1, Some(major(2)), None, b"changeset"),
            "the same entry hashes the same way"
        );
    }

    #[test]
    fn a_provenance_is_inside_the_hash() {
        let base = entry_hash(&GENESIS, 1, Some(major(2)), None, b"changeset");
        let device = Origin {
            writer: WriterRef::Device([1; 32]),
            seq: 7,
        };
        let number = Origin {
            writer: WriterRef::Number(9),
            seq: 7,
        };
        assert_ne!(
            base,
            entry_hash(&GENESIS, 1, Some(major(2)), Some(device), b"changeset"),
            "a stripped provenance is not the signed one"
        );
        assert_ne!(
            entry_hash(&GENESIS, 1, Some(major(2)), Some(device), b"changeset"),
            entry_hash(&GENESIS, 1, Some(major(2)), Some(number), b"changeset"),
            "the device and number encodings cannot collide"
        );
        assert_ne!(
            entry_hash(&GENESIS, 1, Some(major(2)), Some(device), b"changeset"),
            entry_hash(
                &GENESIS,
                1,
                Some(major(2)),
                Some(Origin {
                    writer: WriterRef::Device([2; 32]),
                    seq: 7
                }),
                b"changeset"
            ),
            "a substituted writer identity is not the original"
        );
        assert_ne!(
            entry_hash(&GENESIS, 1, Some(major(2)), Some(device), b"changeset"),
            entry_hash(
                &GENESIS,
                1,
                Some(major(2)),
                Some(Origin {
                    writer: WriterRef::Device([1; 32]),
                    seq: 8
                }),
                b"changeset"
            ),
            "a changed original sequence is not the original"
        );
    }

    #[test]
    fn a_chain_commits_to_every_entry_before_its_head() {
        let first = entry_hash(&GENESIS, 1, Some(major(2)), None, b"one");
        let head = entry_hash(&first, 2, Some(major(2)), None, b"two");
        let forked_first = entry_hash(&GENESIS, 1, Some(major(2)), None, b"uno");
        assert_ne!(
            head,
            entry_hash(&forked_first, 2, Some(major(2)), None, b"two")
        );
    }

    #[test]
    fn a_range_message_names_its_writer() {
        let head = entry_hash(&GENESIS, 1, Some(major(2)), None, b"one");
        assert_ne!(
            range_message(&[1; 32], 1, &head),
            range_message(&[2; 32], 1, &head)
        );
        assert_ne!(
            range_message(&[1; 32], 1, &head),
            range_message(&[1; 32], 2, &head)
        );
    }

    #[test]
    #[cfg(feature = "device-cert")]
    fn a_range_signature_verifies_and_fails_on_changes() {
        let (key, spki) = keypair();
        let (_, other_spki) = keypair();
        let head = entry_hash(&GENESIS, 1, Some(major(2)), None, b"one");
        let signature = sign(&key, &range_message(&[9; 32], 1, &head));
        assert!(
            verify_range(&spki, &[9; 32], 1, &head, &signature),
            "the key's signature verifies"
        );
        assert!(
            !verify_range(&other_spki, &[9; 32], 1, &head, &signature),
            "another key does not verify"
        );
        let other_head = entry_hash(&GENESIS, 1, Some(major(2)), None, b"two");
        assert!(
            !verify_range(&spki, &[9; 32], 1, &other_head, &signature),
            "another head does not verify"
        );
        assert!(
            !verify_range(&spki, &[9; 32], 2, &head, &signature),
            "another sequence does not verify"
        );
        let mut flipped = signature.clone();
        flipped[0] ^= 1;
        assert!(
            !verify_range(&spki, &[9; 32], 1, &head, &flipped),
            "a flipped byte does not verify"
        );
    }

    fn offer() -> (
        Frontier,
        Frontier,
        Vec<(Major, SchemaVersion)>,
        Vec<Retraction>,
    ) {
        (
            Frontier {
                seq: 3,
                head: [1; 32],
            },
            Frontier {
                seq: 5,
                head: [2; 32],
            },
            vec![(major(2), version(1)), (major(3), version(2))],
            vec![
                Retraction {
                    seq: 2,
                    cause: RetractCause::Rejected(MutationRejectReason::Forked),
                },
                Retraction {
                    seq: 4,
                    cause: RetractCause::Conflict,
                },
            ],
        )
    }

    #[test]
    fn an_offer_message_covers_every_field() {
        let (settled, newest, builds, retractions) = offer();
        let base = offer_message(
            &[5; 32],
            Some(&settled),
            Some(&newest),
            &builds,
            &retractions,
        );
        assert_ne!(
            base,
            offer_message(
                &[6; 32],
                Some(&settled),
                Some(&newest),
                &builds,
                &retractions
            ),
            "the author key is inside the message"
        );
        assert_ne!(
            base,
            offer_message(&[5; 32], None, Some(&newest), &builds, &retractions),
            "a dropped settled frontier is not the signed one"
        );
        assert_ne!(
            base,
            offer_message(
                &[5; 32],
                Some(&Frontier {
                    seq: 4,
                    head: [1; 32]
                }),
                Some(&newest),
                &builds,
                &retractions
            ),
            "a changed settled frontier is not the signed one"
        );
        assert_ne!(
            base,
            offer_message(&[5; 32], Some(&settled), None, &builds, &retractions),
            "a dropped newest frontier is not the signed one"
        );
        assert_ne!(
            base,
            offer_message(
                &[5; 32],
                Some(&settled),
                Some(&newest),
                &[(major(2), version(3))],
                &retractions
            ),
            "a changed build is not the signed one"
        );
        let mut reordered = retractions.clone();
        reordered.swap(0, 1);
        assert_ne!(
            base,
            offer_message(&[5; 32], Some(&settled), Some(&newest), &builds, &reordered),
            "the retraction order is inside the message"
        );
        assert_ne!(
            base,
            offer_message(
                &[5; 32],
                Some(&settled),
                Some(&newest),
                &builds,
                &retractions[..1]
            ),
            "a dropped retraction is not the signed one"
        );
        let mut abandoned = retractions.clone();
        abandoned[0] = Retraction {
            seq: 2,
            cause: RetractCause::Abandoned,
        };
        assert_ne!(
            base,
            offer_message(&[5; 32], Some(&settled), Some(&newest), &builds, &abandoned),
            "a changed retraction cause is not the signed one"
        );
        let mut detailed = retractions.clone();
        detailed[0] = Retraction {
            seq: 2,
            cause: RetractCause::Rejected(MutationRejectReason::Other {
                detail: "the row is locked".to_owned(),
            }),
        };
        assert_ne!(
            base,
            offer_message(&[5; 32], Some(&settled), Some(&newest), &builds, &detailed),
            "a changed refusal reason is not the signed one"
        );
    }

    /// Every refusal reason signs as its own message, so a peer cannot swap
    /// the reason a retraction names under the author's signature.
    #[test]
    fn every_refusal_reason_signs_as_its_own_message() {
        let (settled, newest, builds, _) = offer();
        let detail = || "the row is locked".to_owned();
        let reasons = [
            MutationRejectReason::Unauthorized,
            MutationRejectReason::SchemaMismatch,
            MutationRejectReason::Constraint { detail: detail() },
            MutationRejectReason::Malformed { detail: detail() },
            MutationRejectReason::Other { detail: detail() },
            MutationRejectReason::Indeterminate,
            MutationRejectReason::Forked,
        ];
        let messages: Vec<Vec<u8>> = reasons
            .iter()
            .map(|reason| {
                let retraction = Retraction {
                    seq: 2,
                    cause: RetractCause::Rejected(reason.clone()),
                };
                offer_message(
                    &[5; 32],
                    Some(&settled),
                    Some(&newest),
                    &builds,
                    &[retraction],
                )
            })
            .collect();
        for (index, message) in messages.iter().enumerate() {
            assert!(
                !messages[index + 1..].contains(message),
                "{:?} signs as a later reason",
                reasons[index]
            );
        }
    }

    #[test]
    #[cfg(feature = "device-cert")]
    fn an_offer_signature_verifies_through_the_public_verifier() {
        let (key, spki) = keypair();
        let (_, other_spki) = keypair();
        let (settled, newest, builds, retractions) = offer();
        let signature = sign(
            &key,
            &offer_message(
                &[5; 32],
                Some(&settled),
                Some(&newest),
                &builds,
                &retractions,
            ),
        );
        assert!(
            verify_offer(
                &spki,
                &[5; 32],
                Some(&settled),
                Some(&newest),
                &builds,
                &retractions,
                &signature
            ),
            "the author's signature verifies"
        );
        assert!(
            !verify_offer(
                &other_spki,
                &[5; 32],
                Some(&settled),
                Some(&newest),
                &builds,
                &retractions,
                &signature
            ),
            "another key does not verify"
        );
        let mut flipped = signature.clone();
        flipped[0] ^= 1;
        assert!(
            !verify_offer(
                &spki,
                &[5; 32],
                Some(&settled),
                Some(&newest),
                &builds,
                &retractions,
                &flipped
            ),
            "a flipped byte does not verify"
        );
    }

    /// A key that is no P-256 `SubjectPublicKeyInfo` verifies nothing, the
    /// signature a real key made over the same message included.
    #[test]
    #[cfg(feature = "device-cert")]
    fn a_malformed_key_verifies_nothing() {
        let (key, _) = keypair();
        let (settled, newest, builds, retractions) = offer();
        let head = entry_hash(&GENESIS, 1, Some(major(2)), None, b"one");
        let range = sign(&key, &range_message(&[9; 32], 1, &head));
        let offered = sign(
            &key,
            &offer_message(
                &[5; 32],
                Some(&settled),
                Some(&newest),
                &builds,
                &retractions,
            ),
        );
        for malformed in [&b""[..], b"not a key", &[0x30; 91]] {
            assert!(
                !verify_range(malformed, &[9; 32], 1, &head, &range),
                "{malformed:?} verifies a range"
            );
            assert!(
                !verify_offer(
                    malformed,
                    &[5; 32],
                    Some(&settled),
                    Some(&newest),
                    &builds,
                    &retractions,
                    &offered
                ),
                "{malformed:?} verifies an offer"
            );
        }
    }

    #[test]
    fn a_chain_walk_recomputes_the_head_the_entries_stood() {
        let entries = [
            (1u64, Some(major(2)), None::<Origin>, b"one".as_slice()),
            (
                2,
                Some(major(2)),
                Some(Origin {
                    writer: WriterRef::Number(7),
                    seq: 1,
                }),
                b"two".as_slice(),
            ),
            (3, None, None, b"three".as_slice()),
        ];
        let mut head = GENESIS;
        for (seq, major, origin, changeset) in entries {
            head = entry_hash(&head, seq, major, origin, changeset);
        }
        let rows = entries.iter().map(|(seq, major, origin, changeset)| {
            (*seq, *major, *origin, Sha256::digest(changeset).into())
        });
        assert_eq!(
            chain_walk(&GENESIS, rows),
            head,
            "the walk returns the head the entries stood"
        );
        assert_eq!(
            chain_walk(&[7; 32], [].into_iter()),
            [7; 32],
            "an empty walk stands the head it took"
        );
    }
}
