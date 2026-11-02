//! The replica's half of the device-to-device exchange (R77 slice 6b), the
//! device writer's own offer and the ranges it serves, and the cursor, offer
//! and entries it takes from each author.

use connetto_core::chain::GENESIS;
use connetto_core::exchange::{Frontier, ManifestRow, Offer, Origin, SignedOffer};
use connetto_core::messages::WriterRef;
use connetto_core::traits::Transport;
use connetto_core::{Major, SchemaVersion};
use diesel::prelude::*;
use sha2::{Digest as _, Sha256};

use crate::peer::PeerAuthor;
use crate::{ClientError, ConnettoConnection, ExemptWrites, author, majors};

/// Creates the per-author cursor beside the replica's other bookkeeping.
pub(crate) const PEER_CURSOR_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_peer_cursor \
     (author BLOB PRIMARY KEY, account TEXT NOT NULL, seq INTEGER NOT NULL, \
      head BLOB NOT NULL, offer BLOB NOT NULL)";

diesel::table! {
    /// The last verified sequence and head of each author, with the signed
    /// offer it was last verified under (R77).
    _connetto_peer_cursor (author) {
        /// The author's key id.
        author -> Binary,
        /// The author's certificate account.
        account -> Text,
        /// The last verified sequence.
        seq -> BigInt,
        /// The chain hash at it.
        head -> Binary,
        /// The author's last verified signed offer, as `MessagePack`.
        offer -> Binary,
    }
}

use _connetto_peer_cursor::dsl;

/// The cursor an author's link starts from, sequence 0 at [`GENESIS`] when
/// nothing of the author was ever verified.
pub(crate) const FRESH_CURSOR: Frontier = Frontier {
    seq: 0,
    head: GENESIS,
};

/// Store `cursor` as `author`'s, on the row its adopted offer created. An
/// author with no row was never adopted, so taking its entries would move a
/// cursor nothing records and fails instead.
pub(crate) fn store_cursor(
    db: &mut SqliteConnection,
    author: &[u8],
    cursor: &Frontier,
) -> Result<(), ClientError> {
    let updated = diesel::update(dsl::_connetto_peer_cursor.filter(dsl::author.eq(author)))
        .set((
            dsl::seq.eq(stored_seq(cursor.seq)?),
            dsl::head.eq(cursor.head.as_slice()),
        ))
        .execute(db)?;
    if updated == 0 {
        return Err(ClientError::Session(
            "a peer's entry arrived before its offer was adopted".to_owned(),
        ));
    }
    Ok(())
}

fn stored_seq(seq: u64) -> Result<i64, ClientError> {
    i64::try_from(seq)
        .map_err(|_| ClientError::Session(format!("a peer cursor at {seq} overflows storage")))
}

/// `author`'s stored cursor, `None` when nothing of it was ever verified.
///
/// # Errors
///
/// [`ClientError`] when the cursor cannot be read or is corrupt.
pub(crate) fn cursor_of(
    db: &mut SqliteConnection,
    author: &[u8],
) -> Result<Option<Frontier>, ClientError> {
    let row: Option<(i64, Vec<u8>)> = dsl::_connetto_peer_cursor
        .filter(dsl::author.eq(author))
        .select((dsl::seq, dsl::head))
        .first(db)
        .optional()?;
    let Some((seq, head)) = row else {
        return Ok(None);
    };
    let corrupt = || ClientError::Session(format!("a peer cursor records sequence {seq}"));
    Ok(Some(Frontier {
        seq: u64::try_from(seq).map_err(|_| corrupt())?,
        head: head.try_into().map_err(|_| corrupt())?,
    }))
}

/// Every stored cursor, author order, the archive's copy of the peer state
/// (R77 decision 16).
///
/// # Errors
///
/// [`ClientError`] when a cursor cannot be read or its head is corrupt.
pub(crate) fn all_cursors(
    db: &mut SqliteConnection,
) -> Result<Vec<crate::archive::ArchivedPeerCursor>, ClientError> {
    #[derive(diesel::Queryable)]
    struct Row {
        author: Vec<u8>,
        account: String,
        seq: i64,
        head: Vec<u8>,
        offer: Vec<u8>,
    }
    let rows: Vec<Row> = dsl::_connetto_peer_cursor
        .select((dsl::author, dsl::account, dsl::seq, dsl::head, dsl::offer))
        .order(dsl::author.asc())
        .load(db)?;
    rows.into_iter()
        .map(|row| {
            Ok(crate::archive::ArchivedPeerCursor {
                head: row.head.try_into().map_err(|_| {
                    ClientError::Session("a peer cursor records no chain head".to_owned())
                })?,
                author: row.author,
                account: row.account,
                seq: row.seq,
                offer: row.offer,
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Store or replace `author`'s cursor with `cursor` and its verified `offer`
/// (R77 decision 16), the archive's import writing it where the receiver held
/// none.
///
/// # Errors
///
/// [`ClientError`] when the sequence overflows storage or the cursor cannot
/// be written.
pub(crate) fn upsert_cursor(
    db: &mut SqliteConnection,
    author: &[u8],
    account: &str,
    cursor: &Frontier,
    offer: &[u8],
) -> Result<(), ClientError> {
    let seq = stored_seq(cursor.seq)?;
    diesel::insert_into(dsl::_connetto_peer_cursor)
        .values((
            dsl::author.eq(author),
            dsl::account.eq(account),
            dsl::seq.eq(seq),
            dsl::head.eq(cursor.head.as_slice()),
            dsl::offer.eq(offer),
        ))
        .on_conflict(dsl::author)
        .do_update()
        .set((
            dsl::account.eq(account),
            dsl::seq.eq(seq),
            dsl::head.eq(cursor.head.as_slice()),
            dsl::offer.eq(offer),
        ))
        .execute(db)?;
    Ok(())
}

/// A range the device writer serves, with the hash it chains from, its rows,
/// and the sequences whose bodies cross under the requester's build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Served {
    /// The chain hash at the sequence before the range.
    pub(crate) previous: [u8; 32],
    /// The range's rows, in sequence order.
    pub(crate) rows: Vec<ManifestRow>,
    /// The sequences whose bodies cross, in sequence order.
    pub(crate) bodies: Vec<u64>,
}

impl<T> ConnettoConnection<T>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    /// The builds this replica holds entries of, oldest first, each held
    /// major's and then its own.
    fn own_builds(&mut self) -> Result<Vec<(Major, SchemaVersion)>, ClientError> {
        let mut builds = majors::held_builds(&mut self.db)?;
        builds.extend(self.own_build());
        Ok(builds)
    }

    /// This build's own `(major, version)`, the one a request names, `None`
    /// for a build that declares no schema version.
    pub(crate) fn own_build(&self) -> Option<(Major, SchemaVersion)> {
        self.config
            .schema_major
            .zip(self.config.schema_version.clone())
    }

    /// The device writer `key_id`'s offer before the device key signs it:
    /// the builds this replica holds entries of, oldest first, the settled
    /// frontier, the newest queued entry and the retraction log. `None` once
    /// a retirement excluded the key, which offers nothing until a fresh key
    /// enrols (R77 decisions 7 to 9).
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the bookkeeping cannot be read.
    pub(crate) fn own_offer(&mut self, key_id: [u8; 32]) -> Result<Option<Offer>, ClientError> {
        if self.retired_keys.contains(&key_id) {
            return Ok(None);
        }
        let newest = self
            .writers
            .get(&WriterRef::Device(key_id))
            .and_then(|queue| queue.pending.last_key_value())
            .map(|(seq, entry)| Frontier {
                seq: *seq,
                head: entry.hash,
            });
        let builds = self.own_builds()?;
        author::offer(&mut self.db, key_id, newest, builds).map(Some)
    }

    /// The device writer `key_id`'s range `from..=to` as a manifest, with the
    /// sequences whose bodies cross under `build` (R77 decisions 8 and 17).
    /// `None` when the writer no longer holds the whole range, because the
    /// server answered part of it or the writer retired, which the requester
    /// is told as `NotAvailable` (decision 18).
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the bookkeeping cannot be read or an entry's
    /// upload bytes cannot be rebuilt.
    pub(crate) fn serve_range(
        &mut self,
        key_id: [u8; 32],
        from: u64,
        to: u64,
        build: &(Major, SchemaVersion),
    ) -> Result<Option<Served>, ClientError> {
        if from == 0 || to < from || self.retired_keys.contains(&key_id) {
            return Ok(None);
        }
        let writer = WriterRef::Device(key_id);
        let builds = self.own_builds()?;
        let settled = author::settled_of(&mut self.db, &writer)?;
        let Some(queue) = self.writers.get(&writer) else {
            return Ok(None);
        };
        let previous = match from - 1 {
            0 => GENESIS,
            before => match queue.pending.get(&before) {
                Some(entry) => entry.hash,
                None => match settled {
                    Some(settled) if settled.seq == before => settled.head,
                    _ => return Ok(None),
                },
            },
        };
        let mut rows = Vec::new();
        let mut bodies = Vec::new();
        for seq in from..=to {
            let Some(entry) = queue.pending.get(&seq) else {
                return Ok(None);
            };
            // A build without a declared major captures nothing a manifest
            // can name.
            let Some(major) = entry.major else {
                return Ok(None);
            };
            rows.push(ManifestRow {
                seq,
                major,
                origin: entry.provenance.map(Origin::from),
                digest: Sha256::digest(self.chain_bytes(entry)?).into(),
            });
            if connetto_exchange::captured_under(&builds, major, build) {
                bodies.push(seq);
            }
        }
        Ok(Some(Served {
            previous,
            rows,
            bodies,
        }))
    }

    /// The upload bytes of the device writer `key_id`'s entry `seq`, the
    /// body a manifest row digests, `None` once the entry left the queue.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the entry's upload bytes cannot be rebuilt.
    pub(crate) fn serve_body(
        &self,
        key_id: [u8; 32],
        seq: u64,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let Some(entry) = self
            .writers
            .get(&WriterRef::Device(key_id))
            .and_then(|queue| queue.pending.get(&seq))
        else {
            return Ok(None);
        };
        Ok(Some(self.chain_bytes(entry)?.into_owned()))
    }

    /// `author`'s stored cursor, [`FRESH_CURSOR`] when nothing of it was
    /// ever verified.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the cursor cannot be read or is corrupt.
    pub(crate) fn peer_cursor(&mut self, author: &[u8]) -> Result<Frontier, ClientError> {
        let row = dsl::_connetto_peer_cursor
            .filter(dsl::author.eq(author))
            .select((dsl::seq, dsl::head))
            .first::<(i64, Vec<u8>)>(&mut self.db)
            .optional()?;
        let Some((seq, head)) = row else {
            return Ok(FRESH_CURSOR);
        };
        let corrupt = || ClientError::Session(format!("a peer cursor records sequence {seq}"));
        Ok(Frontier {
            seq: u64::try_from(seq).map_err(|_| corrupt())?,
            head: head.try_into().map_err(|_| corrupt())?,
        })
    }

    /// Take `author`'s verified offer (R77), which retracts each entry its log
    /// names, adjudicates the stored entries at or below its settled
    /// frontier, then keeps the offer with `cursor`. Each step is idempotent,
    /// so a crash between them replays at the author's next offer.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when a retraction, the adjudication or the cursor
    /// cannot be written.
    pub(crate) fn adopt_peer_offer(
        &mut self,
        author: &PeerAuthor,
        offer: &SignedOffer,
        cursor: &Frontier,
    ) -> Result<(), ClientError> {
        for retraction in &offer.offer.retractions {
            self.retract_peer_entry(author, retraction)?;
        }
        if let Some(settled) = &offer.offer.settled {
            self.adjudicate_peer_frontier(&author.key_id, settled.seq)?;
        }
        let encoded = rmp_serde::to_vec_named(offer).expect("a signed offer encodes");
        let seq = stored_seq(cursor.seq)?;
        let _exempt = ExemptWrites::raise(&self.write_flags);
        diesel::insert_into(dsl::_connetto_peer_cursor)
            .values((
                dsl::author.eq(&author.key_id),
                dsl::account.eq(&author.account),
                dsl::seq.eq(seq),
                dsl::head.eq(cursor.head.as_slice()),
                dsl::offer.eq(&encoded),
            ))
            .on_conflict(dsl::author)
            .do_update()
            .set((
                dsl::account.eq(&author.account),
                dsl::seq.eq(seq),
                dsl::head.eq(cursor.head.as_slice()),
                dsl::offer.eq(&encoded),
            ))
            .execute(&mut self.db)?;
        Ok(())
    }

    /// Take `author`'s entry `row` with the cursor at it (R77 state table,
    /// rows applied, refused and at another build). `body` applies as the
    /// author with the cursor in the same transaction, a refusal keeps the
    /// cursor alone, and an entry of another build, `body` `None`, moves the
    /// cursor alone.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the entry or the cursor cannot be written.
    pub(crate) fn take_peer_row(
        &mut self,
        author: &PeerAuthor,
        row: &ManifestRow,
        body: Option<&[u8]>,
        cursor: &Frontier,
    ) -> Result<(), ClientError> {
        if let Some(body) = body
            && let crate::peer::PeerApplyOutcome::Applied { .. } =
                self.apply_peer_entry_at(author, row, body, Some(cursor))?
        {
            return Ok(());
        }
        let _exempt = ExemptWrites::raise(&self.write_flags);
        store_cursor(&mut self.db, &author.key_id, cursor)
    }
}

#[cfg(test)]
mod tests;
