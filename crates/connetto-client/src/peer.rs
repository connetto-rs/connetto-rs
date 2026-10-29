//! The stored peer entries and the author's binding (R77 decisions 3 to 6).
//!
//! An applied entry is kept until a verdict takes it, so a retraction can
//! invert it and a resync can re-apply it. The entry's position is the
//! replica's append order across every author, which the resync's re-apply
//! runs in.

use diesel::prelude::*;

use crate::provisional::{ProvisionalRow, ProvisionalState};
use crate::{ClientError, KeyValue};

/// Creates the entry table beside the replica's other bookkeeping.
pub(crate) const PEER_ENTRY_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_peer_entry \
     (author BLOB, seq INTEGER, major INTEGER NOT NULL, changeset BLOB NOT NULL, \
      account TEXT NOT NULL, bot_list TEXT, received_at INTEGER NOT NULL, \
      position INTEGER NOT NULL, PRIMARY KEY (author, seq))";

diesel::table! {
    /// The applied peer entries held until a verdict takes them (R77).
    _connetto_peer_entry (author, seq) {
        /// The author's key id.
        author -> Binary,
        /// The author's queue sequence.
        seq -> BigInt,
        /// The major the entry was captured under.
        major -> BigInt,
        /// The entry's body, as received.
        changeset -> Binary,
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
    /// Nothing of the entry's rows landed.
    Refused {
        /// Why.
        cause: PeerRefusal,
        /// The entry's rows, physical table and key.
        rows: Vec<(String, Vec<KeyValue>)>,
    },
    /// The entry crosses as its manifest row only, its build's apply not
    /// this receiver's.
    Skipped,
}

/// One stored entry, as the re-apply and the retraction stand it.
pub(crate) struct Entry {
    /// The author's key id.
    pub(crate) author: Vec<u8>,
    /// The author's queue sequence.
    pub(crate) seq: i64,
    /// The major the entry was captured under.
    pub(crate) major: i64,
    /// The entry's body, as stored.
    pub(crate) changeset: Vec<u8>,
    /// The author's certificate account.
    pub(crate) account: String,
    /// The author's certificate bot list, which a re-apply binds again.
    pub(crate) bot_list: Option<String>,
    /// The wall clock the entry was received at, in seconds.
    pub(crate) received_at: i64,
}

#[derive(diesel::Queryable)]
struct StoredEntry {
    author: Vec<u8>,
    seq: i64,
    major: i64,
    changeset: Vec<u8>,
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
    dsl::changeset,
    dsl::account,
    dsl::bot_list,
    dsl::received_at,
) = (
    dsl::author,
    dsl::seq,
    dsl::major,
    dsl::changeset,
    dsl::account,
    dsl::bot_list,
    dsl::received_at,
);

fn into_entry(row: StoredEntry) -> Entry {
    Entry {
        author: row.author,
        seq: row.seq,
        major: row.major,
        changeset: row.changeset,
        account: row.account,
        bot_list: row.bot_list,
        received_at: row.received_at,
    }
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

/// The stored entry of one author's sequence, if the receiver holds it.
pub(crate) fn entry_of(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<Option<Entry>, ClientError> {
    let row: Option<StoredEntry> = dsl::_connetto_peer_entry
        .select(ENTRY_SELECT)
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.eq(seq))
        .first(db)
        .optional()?;
    Ok(row.map(into_entry))
}

/// The author's stored entries at or below a frontier, position order.
pub(crate) fn entries_at_or_below(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<Vec<Entry>, ClientError> {
    let rows: Vec<StoredEntry> = dsl::_connetto_peer_entry
        .select(ENTRY_SELECT)
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.le(seq))
        .order(dsl::position.asc())
        .load(db)?;
    Ok(rows.into_iter().map(into_entry).collect())
}

/// Every stored entry, position order, the resync's re-apply running in.
pub(crate) fn all_entries(db: &mut SqliteConnection) -> Result<Vec<Entry>, ClientError> {
    let rows: Vec<StoredEntry> = dsl::_connetto_peer_entry
        .select(ENTRY_SELECT)
        .order(dsl::position.asc())
        .load(db)?;
    Ok(rows.into_iter().map(into_entry).collect())
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

/// Store an entry of `author` at the position its apply took.
pub(crate) fn store_entry(
    db: &mut SqliteConnection,
    author: &PeerAuthor,
    seq: i64,
    major: i64,
    changeset: &[u8],
    received_at: i64,
    position: i64,
) -> Result<(), ClientError> {
    diesel::insert_into(dsl::_connetto_peer_entry)
        .values((
            dsl::author.eq(&author.key_id),
            dsl::seq.eq(seq),
            dsl::major.eq(major),
            dsl::changeset.eq(changeset),
            dsl::account.eq(&author.account),
            dsl::bot_list.eq(author.bot_list.as_deref()),
            dsl::received_at.eq(received_at),
            dsl::position.eq(position),
        ))
        .execute(db)?;
    Ok(())
}

/// Delete one stored entry, the retraction and the adjudication taking it.
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

/// The receiver's side of one peer apply, what its ops are judged against.
pub(crate) struct OpsContext<'a> {
    /// The replica's logical to physical table map.
    pub(crate) tables: &'a crate::PolicyTables,
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

/// Apply an entry's ops as its author through the policy views, one
/// provisional record per key, or `None` when the views refused an op and
/// the caller rolls the entry back whole.
///
/// A key the receiver's own unsent write holds is left as it stands, the
/// author recorded displaced by the write (R77 decision 6).
pub(crate) fn apply_ops<'d>(
    db: &mut SqliteConnection,
    ops: impl IntoIterator<Item = crate::replay::PeerOp<'d>>,
    cx: &OpsContext<'_>,
) -> Result<Option<Vec<ProvisionalRow>>, ClientError> {
    let mut rows = Vec::new();
    for op in ops {
        let table_logical = op.table().name().to_owned();
        let table = cx.tables.stored(&table_logical);
        let key: Vec<KeyValue> = crate::replay::peer_key_values(&op)
            .into_iter()
            .map(KeyValue::from)
            .collect();
        if key.is_empty() {
            continue;
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
            let verdict = crate::replay::apply_peer_op(db, &table_logical, &table, &op)?;
            if matches!(verdict, crate::replay::PeerVerdict::Refused) {
                return Ok(None);
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
    Ok(Some(rows))
}

/// The rows a logical entry body names, each table as the application
/// writes it, deduplicated in payload order, a refusal's report.
pub(crate) fn entry_rows(logical: &[u8]) -> Result<Vec<(String, Vec<KeyValue>)>, ClientError> {
    let mut seen = std::collections::BTreeSet::new();
    let mut rows = Vec::new();
    for (table, key) in crate::op_keys(logical)
        .map_err(|err| ClientError::Apply(format!("reading a peer entry's keys: {err:?}")))?
    {
        if !key.is_empty() && seen.insert((table.clone(), crate::provisional::encode_key(&key))) {
            rows.push((table, key));
        }
    }
    Ok(rows)
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
