//! What a live subscription sends across a process boundary such as a Tauri
//! channel, and how the far side rebuilds each snapshot from it (R116).
//!
//! Values cross as MessagePack. A keyed answer crosses as its rows' bytes,
//! each beside its key's bytes, so the far side follows it without naming the
//! key type, and a change crosses as a [`KeyedDiff`] from the last answer the
//! far side acknowledged.

use std::collections::{HashMap, HashSet};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::ClientError;

/// A far side's request to follow a registered [`LiveSpec`](crate::LiveSpec).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeRequest {
    /// The subscription's id, which the far side mints and which only grows
    /// within one page load.
    pub id: u64,
    /// The spec's [`NAME`](crate::LiveSpec::NAME).
    pub name: String,
    /// The spec itself, encoded.
    pub descriptor: ByteBuf,
    /// The channel the messages go to, as the far side names it.
    pub channel: String,
}

/// One message on a subscription's channel, numbered so the far side can
/// acknowledge it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedMessage {
    /// The message's number, growing within one subscription.
    pub seq: u64,
    /// What the message carries.
    pub body: FeedBody,
}

/// What a [`FeedMessage`] carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedBody {
    /// The whole snapshot of a subscription without a key, encoded.
    Whole(ByteBuf),
    /// The whole answer of a keyed subscription, each row beside its key.
    Keyed(Vec<KeyedRow>),
    /// What changed in a keyed answer since the last one acknowledged.
    Diff(KeyedDiff),
    /// The error that ended the subscription.
    Failed(String),
}

/// One row of a keyed answer, encoded, beside its key's encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedRow {
    /// The row's key.
    pub key: ByteBuf,
    /// The row.
    pub row: ByteBuf,
}

impl KeyedRow {
    /// The row encoded as `row`, under the key encoded as `key`.
    #[must_use]
    pub fn new(key: Vec<u8>, row: Vec<u8>) -> Self {
        Self {
            key: ByteBuf::from(key),
            row: ByteBuf::from(row),
        }
    }
}

/// What changed in a keyed answer since a base answer.
///
/// Applying it removes [`removed`](Self::removed), replaces the rows of
/// [`upserted`](Self::upserted) whose key the base holds and appends the
/// others, then puts the rows in [`order`](Self::order) when it is given.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedDiff {
    /// The rows added or changed, in the new answer's order.
    pub upserted: Vec<KeyedRow>,
    /// The keys of the rows that left, in the base's order.
    pub removed: Vec<ByteBuf>,
    /// Every key of the new answer in order, present only when the removals
    /// and the appended rows alone do not give that order.
    pub order: Option<Vec<ByteBuf>>,
}

impl KeyedDiff {
    /// The diff turning `base` into `next`, or `None` when they are equal.
    ///
    /// A row counts as changed when its bytes differ. Keys must tell the rows
    /// of one answer apart.
    #[must_use]
    pub fn between(base: &[KeyedRow], next: &[KeyedRow]) -> Option<Self> {
        let held: HashMap<&[u8], &[u8]> = base
            .iter()
            .map(|entry| (entry.key.as_slice(), entry.row.as_slice()))
            .collect();
        let kept: HashSet<&[u8]> = next.iter().map(|entry| entry.key.as_slice()).collect();
        let removed: Vec<ByteBuf> = base
            .iter()
            .filter(|entry| !kept.contains(entry.key.as_slice()))
            .map(|entry| entry.key.clone())
            .collect();
        let upserted: Vec<KeyedRow> = next
            .iter()
            .filter(|entry| held.get(entry.key.as_slice()) != Some(&entry.row.as_slice()))
            .cloned()
            .collect();
        let applied = base
            .iter()
            .map(|entry| entry.key.as_slice())
            .filter(|key| kept.contains(key))
            .chain(
                next.iter()
                    .map(|entry| entry.key.as_slice())
                    .filter(|key| !held.contains_key(key)),
            );
        let order = (!applied.eq(next.iter().map(|entry| entry.key.as_slice())))
            .then(|| next.iter().map(|entry| entry.key.clone()).collect());
        (!upserted.is_empty() || !removed.is_empty() || order.is_some()).then_some(Self {
            upserted,
            removed,
            order,
        })
    }

    /// Apply this diff to `base`, the answer it was taken from.
    ///
    /// # Errors
    ///
    /// [`ClientError::Protocol`] when the diff removes a key `base` lacks or
    /// orders other keys than the result holds, so it was taken from another
    /// base. `base` is then left partly changed.
    pub fn apply(self, base: &mut Vec<KeyedRow>) -> Result<(), ClientError> {
        for key in self.removed {
            let at = base
                .iter()
                .position(|entry| entry.key == key)
                .ok_or_else(|| {
                    ClientError::Protocol("a diff removes a key its base lacks".to_owned())
                })?;
            base.remove(at);
        }
        for entry in self.upserted {
            match base.iter_mut().find(|held| held.key == entry.key) {
                Some(held) => held.row = entry.row,
                None => base.push(entry),
            }
        }
        if let Some(order) = self.order {
            let mut rows: HashMap<ByteBuf, ByteBuf> =
                base.drain(..).map(|entry| (entry.key, entry.row)).collect();
            let held = rows.len();
            for key in order {
                let row = rows.remove(&key).ok_or_else(|| {
                    ClientError::Protocol("a diff orders a key its result lacks".to_owned())
                })?;
                base.push(KeyedRow { key, row });
            }
            if base.len() != held {
                return Err(ClientError::Protocol(
                    "a diff's order leaves out a key of its result".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// The rows of `answer` as one MessagePack array, the encoding of the `Vec`
/// of rows they make.
///
/// # Panics
///
/// When `answer` holds 2^32 rows or more, which MessagePack cannot count.
#[must_use]
pub fn rows_array(answer: &[KeyedRow]) -> Vec<u8> {
    let mut array =
        Vec::with_capacity(5 + answer.iter().map(|entry| entry.row.len()).sum::<usize>());
    let len = u32::try_from(answer.len()).expect("an answer holds fewer than 2^32 rows");
    rmp::encode::write_array_len(&mut array, len).expect("writing to a Vec cannot fail");
    for entry in answer {
        array.extend_from_slice(&entry.row);
    }
    array
}

/// `value` as MessagePack, with struct fields by name, as connetto's wire
/// encodes.
///
/// # Errors
///
/// [`ClientError::Protocol`] when `value` does not serialize.
pub fn encode<V: Serialize + ?Sized>(value: &V) -> Result<Vec<u8>, ClientError> {
    rmp_serde::to_vec_named(value).map_err(|error| ClientError::Protocol(error.to_string()))
}

/// The value `bytes` encode.
///
/// # Errors
///
/// [`ClientError::Protocol`] when `bytes` do not decode as a `V`.
pub fn decode<V: DeserializeOwned>(bytes: &[u8]) -> Result<V, ClientError> {
    rmp_serde::from_slice(bytes).map_err(|error| ClientError::Protocol(error.to_string()))
}
