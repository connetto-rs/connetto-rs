//! The author's side of the exchange (R77 decisions 7 and 9): each writer's
//! settled frontier, the device writer's retraction log, and the offer built
//! from them that the device key signs for every linked peer.

use connetto_core::chain::offer_message;
use connetto_core::exchange::{Frontier, RetractCause, Retraction};
use connetto_core::messages::WriterRef;
use connetto_core::{Major, SchemaVersion};
use connetto_peer::ExchangeFrame;
use diesel::prelude::*;

use crate::{ClientError, seq_storage, writer_id};

/// Creates the retraction log beside the writer queues.
pub(crate) const RETRACTION_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_retraction \
     (writer TEXT, seq INTEGER, cause BLOB NOT NULL, PRIMARY KEY (writer, seq))";

/// The columns the settled frontier adds to the writer rows.
pub(crate) const SETTLED_COLUMNS: [&str; 2] = [
    "ALTER TABLE _connetto_writers ADD COLUMN settled_seq INTEGER",
    "ALTER TABLE _connetto_writers ADD COLUMN settled_head BLOB",
];

diesel::table! {
    /// The settled frontier of each writer row.
    #[sql_name = "_connetto_writers"]
    settled (id) {
        /// The writer identity.
        id -> Text,
        /// The last sequence the server answered for good.
        settled_seq -> Nullable<BigInt>,
        /// That entry's chain hash.
        settled_head -> Nullable<Binary>,
    }
}

diesel::table! {
    /// The device writer's entries the server refused, kept until the writer
    /// retires.
    _connetto_retraction (writer, seq) {
        /// The writer identity.
        writer -> Text,
        /// The refused entry's sequence.
        seq -> BigInt,
        /// Why it was refused, as JSON.
        cause -> Binary,
    }
}

/// Record `writer`'s entry `seq`, chained to `head`, as its settled frontier,
/// and `cause` in the retraction log when the server refused a device
/// writer's entry. Runs in the transaction that retires the entry, and the
/// server answers a writer's entries in sequence order, so the frontier only
/// moves forward.
pub(crate) fn settle(
    db: &mut SqliteConnection,
    writer: &WriterRef,
    seq: u64,
    head: &[u8; 32],
    cause: Option<&RetractCause>,
) -> Result<(), ClientError> {
    let id = writer_id(writer);
    let stored = seq_storage(seq)?;
    diesel::update(settled::table.filter(settled::id.eq(&id)))
        .set((
            settled::settled_seq.eq(stored),
            settled::settled_head.eq(head.as_slice()),
        ))
        .execute(db)?;
    if let (WriterRef::Device(_), Some(cause)) = (writer, cause) {
        let cause = serde_json::to_vec(cause).expect("a retraction cause encodes as JSON");
        diesel::insert_into(_connetto_retraction::table)
            .values((
                _connetto_retraction::writer.eq(&id),
                _connetto_retraction::seq.eq(stored),
                _connetto_retraction::cause.eq(cause),
            ))
            .execute(db)?;
    }
    Ok(())
}

/// Forget the retraction log of a writer whose state goes with it.
pub(crate) fn forget(db: &mut SqliteConnection, writer: &WriterRef) -> Result<(), ClientError> {
    diesel::delete(
        _connetto_retraction::table.filter(_connetto_retraction::writer.eq(writer_id(writer))),
    )
    .execute(db)?;
    Ok(())
}

/// `writer`'s settled frontier, `None` before the server answered any entry.
fn settled_of(
    db: &mut SqliteConnection,
    writer: &WriterRef,
) -> Result<Option<Frontier>, ClientError> {
    let row = settled::table
        .filter(settled::id.eq(writer_id(writer)))
        .select((settled::settled_seq, settled::settled_head))
        .first::<(Option<i64>, Option<Vec<u8>>)>(db)
        .optional()?;
    let Some((Some(seq), Some(head))) = row else {
        return Ok(None);
    };
    let corrupt = || ClientError::Session(format!("a writer records settled sequence {seq}"));
    Ok(Some(Frontier {
        seq: u64::try_from(seq).map_err(|_| corrupt())?,
        head: head.try_into().map_err(|_| corrupt())?,
    }))
}

/// `writer`'s retraction log, in sequence order.
fn retractions_of(
    db: &mut SqliteConnection,
    writer: &WriterRef,
) -> Result<Vec<Retraction>, ClientError> {
    _connetto_retraction::table
        .filter(_connetto_retraction::writer.eq(writer_id(writer)))
        .order(_connetto_retraction::seq)
        .select((_connetto_retraction::seq, _connetto_retraction::cause))
        .load::<(i64, Vec<u8>)>(db)?
        .into_iter()
        .map(|(seq, cause)| {
            let corrupt = || ClientError::Session(format!("a retraction records sequence {seq}"));
            Ok(Retraction {
                seq: u64::try_from(seq).map_err(|_| corrupt())?,
                cause: serde_json::from_slice(&cause).map_err(|_| corrupt())?,
            })
        })
        .collect()
}

/// The device writer's state as its offer states it, before the device key
/// signs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OfferDraft {
    /// The device writer's key id.
    pub(crate) key_id: [u8; 32],
    /// The builds whose entries the queue holds, as `(major, version)`.
    pub(crate) builds: Vec<(Major, SchemaVersion)>,
    /// The settled frontier.
    pub(crate) settled: Option<Frontier>,
    /// The newest queued entry.
    pub(crate) newest: Option<Frontier>,
    /// The retraction log.
    pub(crate) retractions: Vec<Retraction>,
}

impl OfferDraft {
    /// Read the offer of device writer `key_id`, given its newest queued
    /// entry and the builds this replica holds.
    pub(crate) fn read(
        db: &mut SqliteConnection,
        key_id: [u8; 32],
        newest: Option<Frontier>,
        builds: Vec<(Major, SchemaVersion)>,
    ) -> Result<Self, ClientError> {
        let writer = WriterRef::Device(key_id);
        Ok(Self {
            key_id,
            builds,
            settled: settled_of(db, &writer)?,
            newest,
            retractions: retractions_of(db, &writer)?,
        })
    }

    /// The bytes the device key signs.
    pub(crate) fn message(&self) -> Vec<u8> {
        offer_message(
            &self.key_id,
            self.settled.as_ref(),
            self.newest.as_ref(),
            &self.builds,
            &self.retractions,
        )
    }

    /// The offer frame carrying the device key's `signature` over
    /// [`message`](Self::message).
    pub(crate) fn signed(self, signature: Vec<u8>) -> ExchangeFrame {
        ExchangeFrame::Offer {
            builds: self.builds,
            settled: self.settled,
            newest: self.newest,
            retractions: self.retractions,
            signature,
        }
    }
}

#[cfg(test)]
mod tests;
