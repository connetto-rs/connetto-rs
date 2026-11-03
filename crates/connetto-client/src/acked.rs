//! The entries the server answered for good, kept for a restore (R78
//! decisions 17, 21 and 22).
//!
//! A server restored to an earlier point holds a writer at the frontier of
//! its backup, below what the replica saw settled. The replica keeps every
//! entry the server answered, applied or refused, for the build's retention
//! window, and re-sends the run from the server's frontier byte for byte when
//! it retains the whole of it, a refused entry declaring its refusal. The
//! table stays apart from `_connetto_pending`, whose readers take it for the
//! unsent writes alone.

use connetto_core::Major;
use connetto_core::messages::{PriorRefusal, Provenance, WriterRef};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::{ClientError, PendingEntry, seq_storage, stored_provenance, writer_id};

/// Creates the table beside the writer queues.
pub(crate) const ACKED_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_acked \
     (writer TEXT NOT NULL, seq INTEGER NOT NULL, changeset BLOB NOT NULL, \
      entry_hash BLOB NOT NULL, prov_writer TEXT, prov_seq INTEGER, prov_secret BLOB, \
      major INTEGER, refused BLOB, acked_at INTEGER NOT NULL, PRIMARY KEY (writer, seq))";

diesel::table! {
    /// The entries the server answered for good, kept for a restore.
    _connetto_acked (writer, seq) {
        /// The writer identity.
        writer -> Text,
        /// The entry's sequence under the writer.
        seq -> BigInt,
        /// The captured changeset, as `_connetto_pending` held it.
        changeset -> Binary,
        /// The entry's chain hash.
        entry_hash -> Binary,
        /// The original writer identity.
        prov_writer -> Nullable<Text>,
        /// The original writer's sequence.
        prov_seq -> Nullable<BigInt>,
        /// The secret the original number writer was issued under.
        prov_secret -> Nullable<Binary>,
        /// The application schema major the entry was captured under.
        major -> Nullable<BigInt>,
        /// The refusal the entry settled with, as JSON, absent when applied.
        refused -> Nullable<Binary>,
        /// When the server answered it, in seconds of the replica's clock.
        acked_at -> BigInt,
    }
}

/// One retained entry, as a restore re-sends it.
#[derive(Debug, Clone)]
pub(crate) struct AckedEntry {
    /// The captured changeset.
    pub(crate) changeset: Vec<u8>,
    /// The original writer and sequence, when the entry carries them.
    pub(crate) provenance: Option<Provenance>,
    /// The refusal the entry settled with.
    pub(crate) refused: Option<PriorRefusal>,
}

/// Keep `writer`'s entry `seq`, answered with `refused` or applied, at
/// `now`, then drop every retained entry older than `retention`. Runs in the
/// transaction that retires the entry from `_connetto_pending`.
pub(crate) fn retain(
    db: &mut SqliteConnection,
    writer: &WriterRef,
    seq: u64,
    entry: &PendingEntry,
    refused: Option<&PriorRefusal>,
    now: i64,
    retention: core::time::Duration,
) -> Result<(), ClientError> {
    let (prov_writer, prov_seq, prov_secret) = match &entry.provenance {
        Some(prov) => (
            Some(writer_id(&prov.writer)),
            Some(seq_storage(prov.seq)?),
            prov.secret.map(|secret| secret.as_bytes().to_vec()),
        ),
        None => (None, None, None),
    };
    let refused = refused
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|err| ClientError::Session(format!("a refusal does not encode: {err}")))?;
    diesel::replace_into(_connetto_acked::table)
        .values((
            _connetto_acked::writer.eq(writer_id(writer)),
            _connetto_acked::seq.eq(seq_storage(seq)?),
            _connetto_acked::changeset.eq(&entry.changeset),
            _connetto_acked::entry_hash.eq(entry.hash.as_slice()),
            _connetto_acked::prov_writer.eq(prov_writer),
            _connetto_acked::prov_seq.eq(prov_seq),
            _connetto_acked::prov_secret.eq(prov_secret),
            _connetto_acked::major.eq(entry.major.map(|major| i64::from(major.get()))),
            _connetto_acked::refused.eq(refused),
            _connetto_acked::acked_at.eq(now),
        ))
        .execute(db)?;
    prune(db, now, retention)
}

/// Drop every retained entry answered `retention` or longer before `now`.
pub(crate) fn prune(
    db: &mut SqliteConnection,
    now: i64,
    retention: core::time::Duration,
) -> Result<(), ClientError> {
    let window = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
    diesel::delete(
        _connetto_acked::table.filter(_connetto_acked::acked_at.le(now.saturating_sub(window))),
    )
    .execute(db)?;
    Ok(())
}

/// Record that a restored server refused `writer`'s retained entry `seq`, so
/// a later restore re-sends it declaring that refusal.
pub(crate) fn refuse(
    db: &mut SqliteConnection,
    writer: &WriterRef,
    seq: u64,
    refused: &PriorRefusal,
) -> Result<(), ClientError> {
    let refused = serde_json::to_vec(refused)
        .map_err(|err| ClientError::Session(format!("a refusal does not encode: {err}")))?;
    diesel::update(
        _connetto_acked::table
            .filter(_connetto_acked::writer.eq(writer_id(writer)))
            .filter(_connetto_acked::seq.eq(seq_storage(seq)?)),
    )
    .set(_connetto_acked::refused.eq(refused))
    .execute(db)?;
    Ok(())
}

/// Drop every retained entry of a writer whose state goes with it.
pub(crate) fn forget(db: &mut SqliteConnection, writer: &WriterRef) -> Result<(), ClientError> {
    diesel::delete(_connetto_acked::table.filter(_connetto_acked::writer.eq(writer_id(writer))))
        .execute(db)?;
    Ok(())
}

/// Rewrite every retained entry `major` captured through `rewrite`, into the
/// wire form that major's server reads, as R31's rebuild rewrites the queue.
pub(crate) fn rewrite_major(
    db: &mut SqliteConnection,
    major: i64,
    mut rewrite: impl FnMut(Vec<u8>) -> Result<Vec<u8>, ClientError>,
) -> Result<(), ClientError> {
    let rows: Vec<(String, i64, Vec<u8>)> = _connetto_acked::table
        .filter(_connetto_acked::major.eq(major))
        .select((
            _connetto_acked::writer,
            _connetto_acked::seq,
            _connetto_acked::changeset,
        ))
        .load(db)?;
    for (writer, seq, changeset) in rows {
        diesel::update(
            _connetto_acked::table
                .filter(_connetto_acked::writer.eq(&writer))
                .filter(_connetto_acked::seq.eq(seq)),
        )
        .set(_connetto_acked::changeset.eq(rewrite(changeset)?))
        .execute(db)?;
    }
    Ok(())
}

/// `writer`'s retained entries from `from` through `to` in sequence order,
/// when the replica retains every one of them under `major`, the capture
/// major this connection's wire carries.
pub(crate) fn run(
    db: &mut SqliteConnection,
    writer: &WriterRef,
    from: u64,
    to: u64,
    major: Option<Major>,
) -> Result<Option<Vec<(u64, AckedEntry)>>, ClientError> {
    #[derive(diesel::Queryable)]
    struct Row {
        seq: i64,
        changeset: Vec<u8>,
        prov_writer: Option<String>,
        prov_seq: Option<i64>,
        prov_secret: Option<Vec<u8>>,
        major: Option<i64>,
        refused: Option<Vec<u8>>,
    }
    if from > to {
        return Ok(None);
    }
    let rows: Vec<Row> = _connetto_acked::table
        .filter(_connetto_acked::writer.eq(writer_id(writer)))
        .filter(_connetto_acked::seq.ge(seq_storage(from)?))
        .filter(_connetto_acked::seq.le(seq_storage(to)?))
        .order(_connetto_acked::seq.asc())
        .select((
            _connetto_acked::seq,
            _connetto_acked::changeset,
            _connetto_acked::prov_writer,
            _connetto_acked::prov_seq,
            _connetto_acked::prov_secret,
            _connetto_acked::major,
            _connetto_acked::refused,
        ))
        .load(db)?;
    let mut entries = Vec::with_capacity(rows.len());
    for (expected, row) in (from..=to).zip(rows) {
        let captured = row
            .major
            .and_then(|major| u32::try_from(major).ok())
            .and_then(Major::new);
        // A gap, or an entry only another major's wire carries, leaves the
        // run to the writer's retirement.
        if u64::try_from(row.seq).ok() != Some(expected) || captured != major {
            return Ok(None);
        }
        let refused = row
            .refused
            .map(|bytes| serde_json::from_slice::<PriorRefusal>(&bytes))
            .transpose()
            .map_err(|err| {
                ClientError::Session(format!("a retained refusal is malformed: {err}"))
            })?;
        entries.push((
            expected,
            AckedEntry {
                changeset: row.changeset,
                provenance: stored_provenance(
                    row.prov_writer.as_deref(),
                    row.prov_seq,
                    row.prov_secret,
                ),
                refused,
            },
        ));
    }
    let complete = u64::try_from(entries.len()).ok() == to.checked_sub(from).map(|span| span + 1);
    Ok(complete.then_some(entries))
}
