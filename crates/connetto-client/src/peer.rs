//! The stored peer entries and the author's binding (R77 decisions 3 to 6,
//! R78 decision 19).
//!
//! Every entry a verified range carried is kept until a verdict takes it, in
//! one of four states. A provisional entry applied, so a retraction can invert
//! it and a resync can re-apply it. A courier-only entry is an own-build body
//! the receiver's policies refused, never applied, kept for a courier. A
//! digest-only entry is another build's, its manifest row alone. A retained
//! entry was adjudicated and is kept, body and row, until its author's
//! certificate ends, so a courier can bring it back to a restored server. The
//! entry's position is the replica's append order across every author, which
//! the resync's re-apply runs in.

use diesel::connection::SimpleConnection as _;
use diesel::prelude::*;
use sha2::{Digest as _, Sha256};

use crate::provisional::{ProvisionalRow, ProvisionalState};
use crate::{ClientError, KeyValue};

/// Creates the entry table beside the replica's other bookkeeping.
pub(crate) const PEER_ENTRY_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_peer_entry \
     (author BLOB, seq INTEGER, state INTEGER NOT NULL, major INTEGER NOT NULL, origin BLOB, \
      digest BLOB NOT NULL, changeset BLOB, account TEXT NOT NULL, bot_list TEXT, \
      received_at INTEGER NOT NULL, position INTEGER NOT NULL, PRIMARY KEY (author, seq))";

/// Creates the single row that stands while the resync's asks about the
/// retained rows are owed (R78 decision 49).
pub(crate) const RETAINED_ASK_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_retained_ask \
     (id INTEGER PRIMARY KEY CHECK (id = 1))";

diesel::table! {
    /// The row that stands while the resync's asks about the retained rows
    /// are owed (R78 decision 49).
    _connetto_retained_ask (id) {
        /// The row's only value.
        id -> Integer,
    }
}

diesel::table! {
    /// Every peer entry the receiver holds until a verdict takes it (R77,
    /// R78 decision 19).
    _connetto_peer_entry (author, seq) {
        /// The author's key id.
        author -> Binary,
        /// The author's queue sequence.
        seq -> BigInt,
        /// What the entry is held as, a [`HeldState`](crate::peer::HeldState).
        state -> BigInt,
        /// The major the entry was captured under.
        major -> BigInt,
        /// The entry's original writer and sequence as `MessagePack`, absent
        /// for an entry with no provenance or one stored before the column.
        origin -> Nullable<Binary>,
        /// The hash of the entry's body, its manifest row's digest.
        digest -> Binary,
        /// The entry's body, as received, absent for a digest-only entry.
        changeset -> Nullable<Binary>,
        /// The author's certificate account.
        account -> Text,
        /// The author's certificate bot list, absent when it carried none.
        bot_list -> Nullable<Text>,
        /// The wall clock the entry was received at, in seconds.
        received_at -> BigInt,
        /// The replica-wide append position the entry took.
        position -> BigInt,
    }
}

use _connetto_peer_entry::dsl;

/// What one stored peer entry is held as (R78 decision 19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeldState {
    /// Applied with its records, awaiting a verdict.
    Provisional,
    /// An own-build body the receiver's policies refused, never applied,
    /// awaiting a verdict.
    CourierOnly,
    /// Another build's entry, its manifest row alone, awaiting a verdict.
    DigestOnly,
    /// Adjudicated, kept until the author's certificate ends.
    Retained,
}

impl HeldState {
    /// The stored value.
    #[must_use]
    pub const fn db(self) -> i64 {
        match self {
            Self::Provisional => 0,
            Self::CourierOnly => 1,
            Self::DigestOnly => 2,
            Self::Retained => 3,
        }
    }

    /// The state a stored value names.
    ///
    /// # Errors
    ///
    /// [`ClientError::Session`] for a value no state stores.
    pub(crate) fn from_db(value: i64) -> Result<Self, ClientError> {
        match value {
            0 => Ok(Self::Provisional),
            1 => Ok(Self::CourierOnly),
            2 => Ok(Self::DigestOnly),
            3 => Ok(Self::Retained),
            other => Err(ClientError::Session(format!(
                "a stored peer entry holds the unknown state {other}"
            ))),
        }
    }
}

/// One author a peer entry applies under.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerAuthor {
    /// The author's device key.
    pub key_id: Vec<u8>,
    /// The certificate's account, the value the caller function binds.
    pub account: String,
    /// The certificate's bot list, which fails closed as `None`.
    pub bot_list: Option<String>,
}

/// Why a peer entry's apply was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRefusal {
    /// The author's write fails the receiver's policies as the author.
    Policy,
    /// The body is not the bytes the manifest row digests.
    DigestMismatch,
    /// An op names its row by no key, which no synced table allows, so the
    /// receiver can neither judge nor record it.
    Unkeyed,
}

/// What a peer entry's apply settled.
#[derive(Debug, Clone, PartialEq)]
pub enum PeerApplyOutcome {
    /// The entry's rows landed and its records were written.
    Applied {
        /// Every record the apply wrote, applied rows and the receiver's own
        /// keys it displaced.
        rows: Vec<crate::provisional::ProvisionalRow>,
    },
    /// Nothing of the entry's rows landed. A policy or key refusal keeps the
    /// body courier-only.
    Refused {
        /// Why.
        cause: PeerRefusal,
        /// The entry's rows, physical table and key.
        rows: Vec<(String, Vec<KeyValue>)>,
    },
    /// The entry crosses as its manifest row only, its build's apply not
    /// this receiver's, and is kept digest-only.
    Skipped,
}

/// One provisional entry, as the re-apply and the retraction stand it.
pub(crate) struct Entry {
    /// The author's key id.
    pub(crate) author: Vec<u8>,
    /// The author's queue sequence.
    pub(crate) seq: i64,
    /// The major the entry was captured under.
    pub(crate) major: i64,
    /// The entry's original writer, when it carries one.
    pub(crate) origin: Option<connetto_core::exchange::Origin>,
    /// The hash of the entry's body.
    pub(crate) digest: [u8; 32],
    /// The entry's body, as stored.
    pub(crate) changeset: Vec<u8>,
    /// The author's certificate account.
    pub(crate) account: String,
    /// The author's certificate bot list, which a re-apply binds again.
    pub(crate) bot_list: Option<String>,
    /// The wall clock the entry was received at, in seconds.
    pub(crate) received_at: i64,
}

/// One held entry a verdict can still reach, its author, sequence and
/// state, which is never [`HeldState::Retained`].
pub(crate) struct Held {
    /// The author's key id.
    pub(crate) author: Vec<u8>,
    /// The author's queue sequence.
    pub(crate) seq: i64,
    /// What the entry is held as.
    pub(crate) state: HeldState,
}

/// One held row the courier offers, as its range stands it (R78 slice 4).
pub(crate) struct HeldRow {
    /// The author's queue sequence.
    pub(crate) seq: i64,
    /// The major the entry was captured under.
    pub(crate) major: connetto_core::Major,
    /// The entry's original writer, when it carries one.
    pub(crate) origin: Option<connetto_core::exchange::Origin>,
    /// The hash of the entry's body.
    pub(crate) digest: [u8; 32],
    /// The entry's body, absent for a digest-only entry.
    pub(crate) changeset: Option<Vec<u8>>,
}

#[derive(diesel::Queryable)]
struct StoredEntry {
    author: Vec<u8>,
    seq: i64,
    major: i64,
    origin: Option<Vec<u8>>,
    digest: Vec<u8>,
    changeset: Option<Vec<u8>>,
    account: String,
    bot_list: Option<String>,
    received_at: i64,
}

/// The entry columns the reads select, the position the `ORDER BY` alone
/// uses.
const ENTRY_SELECT: (
    dsl::author,
    dsl::seq,
    dsl::major,
    dsl::origin,
    dsl::digest,
    dsl::changeset,
    dsl::account,
    dsl::bot_list,
    dsl::received_at,
) = (
    dsl::author,
    dsl::seq,
    dsl::major,
    dsl::origin,
    dsl::digest,
    dsl::changeset,
    dsl::account,
    dsl::bot_list,
    dsl::received_at,
);

/// A provisional row as the reads stand it.
///
/// # Errors
///
/// [`ClientError::Session`] when the row holds no body, a digest of another
/// length or an origin that does not decode.
fn into_entry(row: StoredEntry) -> Result<Entry, ClientError> {
    let corrupt = |what: &str| {
        ClientError::Session(format!(
            "the provisional peer entry at sequence {} holds {what}",
            row.seq
        ))
    };
    let digest = <[u8; 32]>::try_from(row.digest.as_slice()).map_err(|_| corrupt("no digest"))?;
    let origin = row
        .origin
        .as_deref()
        .map(rmp_serde::from_slice)
        .transpose()
        .map_err(|_| corrupt("an unreadable origin"))?;
    let changeset = row.changeset.ok_or_else(|| corrupt("no body"))?;
    Ok(Entry {
        author: row.author,
        seq: row.seq,
        major: row.major,
        origin,
        digest,
        changeset,
        account: row.account,
        bot_list: row.bot_list,
        received_at: row.received_at,
    })
}

/// The position the next stored entry takes, rebuilt from the stored ones
/// like the pending queue's.
pub(crate) fn load_next_position(db: &mut SqliteConnection) -> Result<u64, ClientError> {
    let row: Option<i64> = dsl::_connetto_peer_entry
        .select(diesel::dsl::max(dsl::position))
        .first(db)?;
    Ok(row
        .and_then(|position| u64::try_from(position).ok())
        .unwrap_or(1))
}

/// The provisional entry of one author's sequence, if the receiver holds it
/// provisionally.
pub(crate) fn entry_of(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<Option<Entry>, ClientError> {
    let row: Option<StoredEntry> = dsl::_connetto_peer_entry
        .select(ENTRY_SELECT)
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.eq(seq))
        .filter(dsl::state.eq(HeldState::Provisional.db()))
        .first(db)
        .optional()?;
    row.map(into_entry).transpose()
}

/// What one author's sequence is held as, `None` when the receiver holds
/// nothing of it.
pub(crate) fn state_of(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<Option<HeldState>, ClientError> {
    let row: Option<i64> = dsl::_connetto_peer_entry
        .select(dsl::state)
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.eq(seq))
        .first(db)
        .optional()?;
    row.map(HeldState::from_db).transpose()
}

/// The author's held entries at or below a frontier that a verdict can
/// still reach, position order.
pub(crate) fn unsettled_at_or_below(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<Vec<Held>, ClientError> {
    let rows: Vec<(Vec<u8>, i64, i64)> = dsl::_connetto_peer_entry
        .select((dsl::author, dsl::seq, dsl::state))
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.le(seq))
        .filter(dsl::state.ne(HeldState::Retained.db()))
        .order(dsl::position.asc())
        .load(db)?;
    rows.into_iter()
        .map(|(author, seq, state)| {
            Ok(Held {
                author,
                seq,
                state: HeldState::from_db(state)?,
            })
        })
        .collect()
}

/// Every courier-only and digest-only entry, author and sequence, which the
/// session's asks cover beside the provisional records (R78 slice 3a).
pub(crate) fn unapplied(db: &mut SqliteConnection) -> Result<Vec<(Vec<u8>, i64)>, ClientError> {
    Ok(dsl::_connetto_peer_entry
        .select((dsl::author, dsl::seq))
        .filter(dsl::state.eq_any([HeldState::CourierOnly.db(), HeldState::DigestOnly.db()]))
        .load(db)?)
}

/// Every retained entry, author and sequence, which a full resync's end
/// asks about (R78 decision 49).
pub(crate) fn retained_rows(db: &mut SqliteConnection) -> Result<Vec<(Vec<u8>, i64)>, ClientError> {
    Ok(dsl::_connetto_peer_entry
        .select((dsl::author, dsl::seq))
        .filter(dsl::state.eq(HeldState::Retained.db()))
        .load(db)?)
}

/// Whether the resync's asks about the retained rows still stand owed (R78
/// decision 49).
pub(crate) fn retained_ask_owed(db: &mut SqliteConnection) -> Result<bool, ClientError> {
    Ok(diesel::select(diesel::dsl::exists(
        _connetto_retained_ask::table.filter(_connetto_retained_ask::id.eq(1)),
    ))
    .get_result::<bool>(db)?)
}

/// Owe the resync's asks about the retained rows, from a full resync's end
/// (R78 decision 49).
pub(crate) fn owe_retained_ask(db: &mut SqliteConnection) -> Result<(), ClientError> {
    diesel::insert_into(_connetto_retained_ask::table)
        .values(_connetto_retained_ask::id.eq(1))
        .execute(db)?;
    Ok(())
}

/// Answer the owed retained asks, every ruling having landed (R78 decision
/// 49).
pub(crate) fn clear_retained_ask(db: &mut SqliteConnection) -> Result<(), ClientError> {
    diesel::delete(_connetto_retained_ask::table).execute(db)?;
    Ok(())
}

/// The author's held rows at or below a sequence, which the courier's range
/// stands (R78 slice 4), sequence order, a retained row among them only while
/// its mark stands.
///
/// # Errors
///
/// [`ClientError`] when a stored row holds a major the build cannot express,
/// a state no held row stores, an unreadable origin or a digest of another
/// length.
pub(crate) fn held_at_or_below(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
    marked: Option<Vec<i64>>,
) -> Result<Vec<HeldRow>, ClientError> {
    type Row = (i64, i64, i64, Option<Vec<u8>>, Vec<u8>, Option<Vec<u8>>);
    let fields = (
        dsl::seq,
        dsl::major,
        dsl::state,
        dsl::origin,
        dsl::digest,
        dsl::changeset,
    );
    let rows: Vec<Row> = match marked {
        None => dsl::_connetto_peer_entry
            .select(fields)
            .filter(dsl::author.eq(author))
            .filter(dsl::seq.le(seq))
            .filter(dsl::state.ne(HeldState::Retained.db()))
            .order(dsl::seq.asc())
            .load(db)?,
        Some(marked) => dsl::_connetto_peer_entry
            .select(fields)
            .filter(dsl::author.eq(author))
            .filter(dsl::seq.le(seq))
            .filter(
                dsl::state
                    .eq_any([
                        HeldState::Provisional.db(),
                        HeldState::CourierOnly.db(),
                        HeldState::DigestOnly.db(),
                    ])
                    .or(dsl::state
                        .eq(HeldState::Retained.db())
                        .and(dsl::seq.eq_any(marked))),
            )
            .order(dsl::seq.asc())
            .load(db)?,
    };
    rows.into_iter()
        .map(|(seq, major, state, origin, digest, changeset)| {
            let corrupt = |what: &str| {
                ClientError::Session(format!(
                    "the held peer entry at sequence {seq} holds {what}"
                ))
            };
            let major = u32::try_from(major)
                .ok()
                .and_then(connetto_core::Major::new)
                .ok_or_else(|| corrupt("a major the build cannot express"))?;
            HeldState::from_db(state).map_err(|_| corrupt("no held state"))?;
            let origin = origin
                .as_deref()
                .map(rmp_serde::from_slice)
                .transpose()
                .map_err(|_| corrupt("an unreadable origin"))?;
            let digest =
                <[u8; 32]>::try_from(digest.as_slice()).map_err(|_| corrupt("no digest"))?;
            Ok(HeldRow {
                seq,
                major,
                origin,
                digest,
                changeset,
            })
        })
        .collect()
}

/// The author's rows a courier offered, gone with the server's refusal that
/// takes them (R78 slice 4).
///
/// # Errors
///
/// [`ClientError`] when the rows cannot be written.
pub(crate) fn drop_courier_rows(
    db: &mut SqliteConnection,
    author: &[u8],
) -> Result<(), ClientError> {
    diesel::delete(
        dsl::_connetto_peer_entry
            .filter(dsl::author.eq(author))
            .filter(dsl::state.eq_any([
                HeldState::CourierOnly.db(),
                HeldState::DigestOnly.db(),
                HeldState::Retained.db(),
            ])),
    )
    .execute(db)?;
    Ok(())
}

/// Every provisional entry, position order, the resync's re-apply and the
/// archive export running in.
pub(crate) fn all_entries(db: &mut SqliteConnection) -> Result<Vec<Entry>, ClientError> {
    let rows: Vec<StoredEntry> = dsl::_connetto_peer_entry
        .select(ENTRY_SELECT)
        .filter(dsl::state.eq(HeldState::Provisional.db()))
        .order(dsl::position.asc())
        .load(db)?;
    rows.into_iter().map(into_entry).collect()
}

/// Every held entry as the archive carries it, position order (R78
/// decision 20).
///
/// # Errors
///
/// [`ClientError::Session`] when a stored digest is not a hash.
pub(crate) fn all_held(
    db: &mut SqliteConnection,
) -> Result<Vec<crate::archive::ArchivedPeerEntry>, ClientError> {
    #[derive(diesel::Queryable)]
    struct Row {
        author: Vec<u8>,
        seq: i64,
        state: i64,
        major: i64,
        origin: Option<Vec<u8>>,
        digest: Vec<u8>,
        changeset: Option<Vec<u8>>,
        account: String,
        bot_list: Option<String>,
        received_at: i64,
    }
    let rows: Vec<Row> = dsl::_connetto_peer_entry
        .select((
            dsl::author,
            dsl::seq,
            dsl::state,
            dsl::major,
            dsl::origin,
            dsl::digest,
            dsl::changeset,
            dsl::account,
            dsl::bot_list,
            dsl::received_at,
        ))
        .order(dsl::position.asc())
        .load(db)?;
    rows.into_iter()
        .map(|row| {
            Ok(crate::archive::ArchivedPeerEntry {
                digest: <[u8; 32]>::try_from(row.digest.as_slice()).map_err(|_| {
                    ClientError::Session(format!(
                        "the held peer entry at sequence {} holds no digest",
                        row.seq
                    ))
                })?,
                author: row.author,
                seq: row.seq,
                state: row.state,
                major: row.major,
                origin: row.origin,
                changeset: row.changeset,
                account: row.account,
                bot_list: row.bot_list,
                received_at: row.received_at,
            })
        })
        .collect()
}

/// Hold every provisional entry courier-only, the R31 rebuild dropping the
/// records that made them provisional with the synced tables (R78 decision
/// 7). An entry table of an earlier build comes to the held shape first.
///
/// # Errors
///
/// [`ClientError`] when the table cannot be upgraded or written.
pub(crate) fn hold_for_rebuild(db: &mut SqliteConnection) -> Result<(), ClientError> {
    if columns_of(db, "_connetto_peer_entry")?.is_empty() {
        return Ok(());
    }
    upgrade_entries(db)?;
    diesel::update(dsl::_connetto_peer_entry.filter(dsl::state.eq(HeldState::Provisional.db())))
        .set(dsl::state.eq(HeldState::CourierOnly.db()))
        .execute(db)?;
    Ok(())
}

/// The position a stored entry already took, `None` when the entry is not
/// stored yet.
pub(crate) fn existing_position(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<Option<i64>, ClientError> {
    Ok(dsl::_connetto_peer_entry
        .select(dsl::position)
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.eq(seq))
        .first(db)
        .optional()?)
}

/// One entry to store, beside the author it names.
pub(crate) struct NewEntry<'a> {
    /// The entry's manifest row.
    pub(crate) row: &'a connetto_core::exchange::ManifestRow,
    /// What the entry is held as.
    pub(crate) state: HeldState,
    /// The entry's body, absent for a digest-only entry.
    pub(crate) body: Option<&'a [u8]>,
    /// The wall clock the entry was received at, in seconds.
    pub(crate) received_at: i64,
    /// The replica-wide append position it takes.
    pub(crate) position: i64,
}

/// Store an entry of `author` unless the receiver holds its sequence
/// already, reporting whether it was written.
pub(crate) fn store_entry(
    db: &mut SqliteConnection,
    author: &PeerAuthor,
    entry: &NewEntry<'_>,
) -> Result<bool, ClientError> {
    let origin = entry
        .row
        .origin
        .map(|origin| rmp_serde::to_vec_named(&origin).expect("an origin encodes"));
    let written = diesel::insert_into(dsl::_connetto_peer_entry)
        .values((
            dsl::author.eq(&author.key_id),
            dsl::seq.eq(i64::try_from(entry.row.seq).unwrap_or(i64::MAX)),
            dsl::state.eq(entry.state.db()),
            dsl::major.eq(i64::from(entry.row.major.get())),
            dsl::origin.eq(origin),
            dsl::digest.eq(entry.row.digest.as_slice()),
            dsl::changeset.eq(entry.body),
            dsl::account.eq(&author.account),
            dsl::bot_list.eq(author.bot_list.as_deref()),
            dsl::received_at.eq(entry.received_at),
            dsl::position.eq(entry.position),
        ))
        .on_conflict_do_nothing()
        .execute(db)?;
    Ok(written == 1)
}

/// Retain one held entry, the adjudication keeping it for a restore (R78
/// decision 17).
pub(crate) fn retain(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<(), ClientError> {
    diesel::update(dsl::_connetto_peer_entry)
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.eq(seq))
        .set(dsl::state.eq(HeldState::Retained.db()))
        .execute(db)?;
    Ok(())
}

/// Delete one stored entry, the retraction taking it.
pub(crate) fn delete_entry(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<(), ClientError> {
    diesel::delete(dsl::_connetto_peer_entry)
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.eq(seq))
        .execute(db)?;
    Ok(())
}

/// Delete every retained entry whose author's certificate ended
/// [`DEVICE_SKEW`](connetto_core::clock::DEVICE_SKEW) or more before now, by
/// the wall clock the replica keeps its times in, or whose author's cursor is
/// gone, since a courier delivers nothing without the author's offer (R78
/// decision 17).
pub(crate) fn prune_retained(db: &mut SqliteConnection) -> Result<(), ClientError> {
    let skew = i64::try_from(connetto_core::clock::DEVICE_SKEW.as_secs()).unwrap_or(i64::MAX);
    diesel::sql_query(
        "DELETE FROM _connetto_peer_entry WHERE state = ? AND NOT EXISTS \
         (SELECT 1 FROM _connetto_peer_cursor AS cursor \
          WHERE cursor.author = _connetto_peer_entry.author \
          AND cursor.expires_at + ? > CAST(strftime('%s', 'now') AS INTEGER))",
    )
    .bind::<diesel::sql_types::BigInt, _>(HeldState::Retained.db())
    .bind::<diesel::sql_types::BigInt, _>(skew)
    .execute(db)?;
    Ok(())
}

/// The columns `table` holds, empty when the replica holds no such table.
///
/// # Errors
///
/// [`ClientError`] when the table's columns cannot be read.
pub(crate) fn columns_of(
    db: &mut SqliteConnection,
    table: &str,
) -> Result<Vec<String>, ClientError> {
    #[derive(diesel::QueryableByName)]
    struct Column {
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
    }
    let columns: Vec<Column> = diesel::sql_query("SELECT name FROM pragma_table_info(?)")
        .bind::<diesel::sql_types::Text, _>(table)
        .load(db)?;
    Ok(columns.into_iter().map(|column| column.name).collect())
}

/// Bring an entry table of an earlier build to the held shape (R78 slice
/// 3a): every row it holds stays provisional, its digest recomputed from
/// its body and its origin left empty, since the earlier build kept none.
///
/// # Errors
///
/// [`ClientError`] when the table cannot be read or rewritten.
pub(crate) fn upgrade_entries(db: &mut SqliteConnection) -> Result<(), ClientError> {
    #[derive(diesel::QueryableByName)]
    struct Earlier {
        #[diesel(sql_type = diesel::sql_types::Binary)]
        author: Vec<u8>,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        seq: i64,
        #[diesel(sql_type = diesel::sql_types::Binary)]
        changeset: Vec<u8>,
    }
    let columns = columns_of(db, "_connetto_peer_entry")?;
    if columns.is_empty() || columns.iter().any(|column| column == "state") {
        return Ok(());
    }
    db.transaction::<_, ClientError, _>(|db| {
        let earlier: Vec<Earlier> =
            diesel::sql_query("SELECT author, seq, changeset FROM _connetto_peer_entry")
                .load(db)?;
        db.batch_execute(
            "ALTER TABLE _connetto_peer_entry RENAME TO _connetto_peer_entry_earlier",
        )?;
        db.batch_execute(PEER_ENTRY_DDL)?;
        db.batch_execute(
            "INSERT INTO _connetto_peer_entry (author, seq, state, major, digest, changeset, \
             account, bot_list, received_at, position) \
             SELECT author, seq, 0, major, X'', changeset, account, bot_list, received_at, \
             position FROM _connetto_peer_entry_earlier; \
             DROP TABLE _connetto_peer_entry_earlier",
        )?;
        for row in earlier {
            let digest: [u8; 32] = Sha256::digest(&row.changeset).into();
            diesel::update(dsl::_connetto_peer_entry)
                .filter(dsl::author.eq(&row.author))
                .filter(dsl::seq.eq(row.seq))
                .set(dsl::digest.eq(digest.as_slice()))
                .execute(db)?;
        }
        Ok(())
    })
}

/// The receiver's side of one peer apply, what its ops are judged against.
pub(crate) struct OpsContext<'a> {
    /// The receiver's configuration: its table map, its shared tables and
    /// the identity its shared holdings name.
    pub(crate) config: &'a crate::ClientConfig,
    /// The keys the receiver's own unsent writes hold.
    pub(crate) held: &'a crate::HeldKeys,
    /// The entry's author.
    pub(crate) author: &'a PeerAuthor,
    /// The entry's queue sequence.
    pub(crate) seq: i64,
    /// The receiver's own account, which a held key's record names.
    pub(crate) receiver: &'a str,
    /// The wall clock the entry is received at, in seconds.
    pub(crate) now: i64,
}

/// The key the row `op` writes, in key order, the order every provenance
/// record and every server write names it in.
pub(crate) fn key_of(op: &crate::replay::PeerOp) -> Vec<KeyValue> {
    op.primary_key().into_iter().map(KeyValue::from).collect()
}

/// Apply an entry's ops as its author through the policy views, a shared
/// table's in the store, one provisional record and its holding per key, or
/// the refusal when an op names no key or the views refused one, the caller
/// rolling the entry back whole (R77 decisions 14 and 15).
///
/// A key the receiver's own unsent write holds is left as it stands, the
/// author recorded displaced by the write (R77 decision 6).
pub(crate) fn apply_ops<'d>(
    db: &mut SqliteConnection,
    ops: impl IntoIterator<Item = crate::replay::PeerOp<'d>>,
    cx: &OpsContext<'_>,
) -> Result<Result<Vec<ProvisionalRow>, PeerRefusal>, ClientError> {
    let mut rows = Vec::new();
    for op in ops {
        let table_logical = op.table().name().to_owned();
        let table = cx.config.policy_tables.stored(&table_logical);
        let key = key_of(&op);
        if key.is_empty() {
            return Ok(Err(PeerRefusal::Unkeyed));
        }
        let held = cx
            .held
            .get(&(table.clone(), crate::provisional::encode_key(&key)));
        let record = if let Some((writer, pending_seq)) = held {
            ProvisionalRow {
                table,
                key,
                author: writer.as_bytes().to_vec(),
                seq: *pending_seq,
                account: cx.receiver.to_owned(),
                received_at: cx.now,
                state: ProvisionalState::Provisional,
                displaced_author: Some(cx.author.key_id.clone()),
                displaced_seq: Some(cx.seq),
            }
        } else {
            let backing = crate::backing_of(&cx.config.shared_tables, &table);
            let verdict = crate::replay::apply_peer_op(db, &table_logical, backing, &op)?;
            if verdict == crate::replay::PeerVerdict::Refused {
                return Ok(Err(PeerRefusal::Policy));
            }
            let displaced = crate::provisional::row_of_key(db, &table, &key)?
                .filter(|existing| existing.author != cx.author.key_id);
            ProvisionalRow {
                table,
                key,
                author: cx.author.key_id.clone(),
                seq: cx.seq,
                account: cx.author.account.clone(),
                received_at: cx.now,
                state: ProvisionalState::Provisional,
                displaced_seq: displaced.as_ref().map(|existing| existing.seq),
                displaced_author: displaced.map(|existing| existing.author),
            }
        };
        crate::provisional::upsert_record(db, &record)?;
        rows.push(record);
    }
    let keys: Vec<(String, Vec<KeyValue>)> = rows
        .iter()
        .map(|row| (row.table.clone(), row.key.clone()))
        .collect();
    crate::holdings::hold_provisional(db, cx.config, &keys)?;
    Ok(Ok(rows))
}

/// The rows an entry's ops name, each table as the application writes it,
/// deduplicated in payload order, a refusal's report.
pub(crate) fn entry_rows<'d>(
    ops: impl IntoIterator<Item = crate::replay::PeerOp<'d>>,
) -> Vec<(String, Vec<KeyValue>)> {
    let mut seen = std::collections::BTreeSet::new();
    let mut rows = Vec::new();
    for op in ops {
        let key = key_of(&op);
        let table = op.table().name().to_owned();
        if !key.is_empty() && seen.insert((table.clone(), crate::provisional::encode_key(&key))) {
            rows.push((table, key));
        }
    }
    rows
}

/// The values the policy functions answer with before the bind, restored on
/// the guard's drop.
struct RestoreValues {
    caller: Option<(String, String)>,
    subjects: Option<(String, Option<String>)>,
}

/// Binds the policy functions to one author for the life of the guard and
/// restores the receiver's own values on drop (R77 decision 4), the
/// `adopt_relay_identity` mechanism with an RAII restore. The author's
/// `NULL` subjects make every share-key arm deny, and the receiver's
/// restored bot list the wildcard the open registered.
pub(crate) struct AuthorBinding<'s> {
    pub(crate) db: &'s mut SqliteConnection,
    restore: RestoreValues,
}

impl<'s> AuthorBinding<'s> {
    /// Bind the caller, subject and bot-list functions to `author`, the
    /// receiver's own `caller` and `subjects` values restored when the
    /// guard drops.
    pub(crate) fn new(
        db: &'s mut SqliteConnection,
        author: &PeerAuthor,
        caller: Option<&(String, String)>,
        subjects: Option<&(String, Option<String>)>,
    ) -> Result<Self, ClientError> {
        let restore = RestoreValues {
            caller: caller.cloned(),
            subjects: subjects.cloned(),
        };
        if let Some((function, _)) = caller {
            crate::register_caller(db, function, author.account.clone())?;
        }
        if let Some((function, _)) = subjects {
            crate::register_subjects(db, function, None)?;
        }
        crate::register_bot_list(db, author.bot_list.clone())?;
        Ok(Self { db, restore })
    }
}

impl Drop for AuthorBinding<'_> {
    fn drop(&mut self) {
        let RestoreValues { caller, subjects } = &self.restore;
        let restored: Result<(), ClientError> = (|| {
            if let Some((function, identity)) = caller {
                crate::register_caller(self.db, function, identity.clone())?;
            }
            if let Some((function, rendered)) = subjects {
                crate::register_subjects(self.db, function, rendered.clone())?;
            }
            crate::register_bot_list(self.db, Some(connetto_core::BOT_LIST_WILDCARD.to_owned()))?;
            Ok(())
        })();
        if let Err(err) = restored {
            tracing::warn!(%err, "restoring the receiver's policy functions");
        }
    }
}
