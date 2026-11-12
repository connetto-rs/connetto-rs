//! Custody: the copies of replicated columns' files a device holds for
//! another device's rows until the server acknowledges them (R79 decisions
//! 12, 15 and 16).

use core::fmt::Display;
use std::collections::{HashMap, HashSet};

use connetto_client::ConnettoConnection;
use connetto_core::traits::Transport;
use connetto_file_core::FileId;
use diesel::prelude::*;

use super::PeerColumn;
use crate::db;
use crate::error::ContentError;

/// The state the file server's setter writes once it holds a file.
const AVAILABLE: &str = "available";

/// The state the file server's setter writes once it lost a file.
const LOST: &str = "lost";

/// What the replicated columns of the replica say of each file they name,
/// read before the retention pass's transaction.
#[derive(Default)]
pub(in crate::client) struct Named {
    /// The files a provisional row of a courierable author names, with the
    /// first such author.
    provisional: HashMap<FileId, Vec<u8>>,
    /// How many provisional rows name each file, a lapsed author's included.
    provisional_rows: HashMap<FileId, usize>,
}

impl Named {
    /// The first courierable provisional author of a row naming `file_id`.
    pub(in crate::client) fn author_of(&self, file_id: FileId) -> Option<&[u8]> {
        self.provisional.get(&file_id).map(Vec::as_slice)
    }
}

/// Each provisional author's courier reach, read once per pass.
#[derive(Default)]
pub(in crate::client) struct Reach(HashMap<Vec<u8>, bool>);

impl Reach {
    /// Whether `author`'s certificate lapsed, so no courier carries its rows.
    ///
    /// # Errors
    ///
    /// [`ContentError::Client`] when the author's cursor cannot be read.
    pub(in crate::client) fn lapsed<T>(
        &mut self,
        conn: &mut ConnettoConnection<T>,
        author: &[u8],
    ) -> Result<bool, ContentError>
    where
        T: Transport,
        T::Error: Display,
    {
        if let Some(gone) = self.0.get(author) {
            return Ok(*gone);
        }
        let gone = conn.courier_lapsed(author)?;
        self.0.insert(author.to_vec(), gone);
        Ok(gone)
    }
}

/// Reads the provisional rows of every replicated column.
///
/// # Errors
///
/// [`ContentError::Client`] when the provisional records or an author's
/// cursor cannot be read.
pub(in crate::client) fn named<T>(
    conn: &mut ConnettoConnection<T>,
    declared: &[PeerColumn],
    reach: &mut Reach,
) -> Result<Named, ContentError>
where
    T: Transport,
    T::Error: Display,
{
    let mut named = Named::default();
    for declared in declared
        .iter()
        .filter(|declared| declared.replicate.is_some())
    {
        for blob in conn.provisional_blobs(&declared.table, &declared.column)? {
            let Ok(id) = <[u8; 32]>::try_from(blob.value.as_slice()) else {
                continue;
            };
            let file_id = FileId::from_bytes(id);
            *named.provisional_rows.entry(file_id).or_default() += 1;
            if !reach.lapsed(conn, &blob.author)? {
                named.provisional.entry(file_id).or_insert(blob.author);
            }
        }
    }
    Ok(named)
}

/// What placement reads of the replica for one file a replicated column
/// names (R79 decisions 12 and 14).
#[derive(Default)]
pub(in crate::client) struct Placed {
    /// The most copies any replicated column naming it declares.
    pub(in crate::client) copies: usize,
    /// The key ids of the authors of the provisional rows naming it.
    pub(in crate::client) authors: Vec<Vec<u8>>,
    /// Whether a provisional row of an author in courier reach names it, so
    /// a device that lacks it pulls it.
    pub(in crate::client) pulled: bool,
}

/// Reads every file a replicated column names with what placement needs.
///
/// # Errors
///
/// [`ContentError::Client`] when the provisional records or an author's
/// cursor cannot be read, and [`ContentError::Replica`] when a replicated
/// column cannot.
pub(in crate::client) fn placing<T>(
    conn: &mut ConnettoConnection<T>,
    declared: &[PeerColumn],
) -> Result<HashMap<FileId, Placed>, ContentError>
where
    T: Transport,
    T::Error: Display,
{
    let mut placed: HashMap<FileId, Placed> = HashMap::new();
    let mut reach = Reach::default();
    for declared in declared {
        let Some(replicate) = &declared.replicate else {
            continue;
        };
        let copies = usize::from(replicate.copies);
        for file_id in states(conn.conn(), core::slice::from_ref(declared))?.into_keys() {
            let entry = placed.entry(file_id).or_default();
            entry.copies = entry.copies.max(copies);
        }
        for blob in conn.provisional_blobs(&declared.table, &declared.column)? {
            let Ok(id) = <[u8; 32]>::try_from(blob.value.as_slice()) else {
                continue;
            };
            let lapsed = reach.lapsed(conn, &blob.author)?;
            let entry = placed.entry(FileId::from_bytes(id)).or_default();
            entry.copies = entry.copies.max(copies);
            entry.pulled |= !lapsed;
            if !entry.authors.contains(&blob.author) {
                entry.authors.push(blob.author);
            }
        }
    }
    Ok(placed)
}

/// One replicated column's value and state on one row.
#[derive(diesel::QueryableByName)]
struct Row {
    #[diesel(sql_type = diesel::sql_types::Binary)]
    file_id: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    state: Option<String>,
}

/// The states of every row naming each file through a replicated column.
fn states(
    conn: &mut SqliteConnection,
    declared: &[PeerColumn],
) -> Result<HashMap<FileId, Vec<Option<String>>>, ContentError> {
    let mut states: HashMap<FileId, Vec<Option<String>>> = HashMap::new();
    for declared in declared {
        let Some(replicate) = &declared.replicate else {
            continue;
        };
        // The application's table and columns are named at runtime, which the typed DSL cannot address.
        let rows: Vec<Row> = diesel::sql_query(format!(
            "SELECT [{column}] AS file_id, [{state}] AS state FROM \"{table}\" \
             WHERE typeof([{column}]) = 'blob'",
            column = declared.column,
            state = replicate.state,
            table = declared.table.replace('"', "\"\""),
        ))
        .load(conn)?;
        for row in rows {
            if let Ok(id) = <[u8; 32]>::try_from(row.file_id.as_slice()) {
                states
                    .entry(FileId::from_bytes(id))
                    .or_default()
                    .push(row.state);
            }
        }
    }
    Ok(states)
}

/// The retention pass's custody half, run inside its transaction: a copy
/// whose row is gone or whose author lapsed leaves, one a settled row names
/// stays until the state reads `available` and learns when it settled, and a
/// cache copy whose state reads `lost` returns. Every file left in custody
/// joins `kept`, and the answer says whether custody changed.
///
/// # Errors
///
/// [`ContentError::Replica`] when a replicated column or the bookkeeping
/// cannot be read or written.
pub(in crate::client) fn pass(
    conn: &mut SqliteConnection,
    declared: &[PeerColumn],
    named: &Named,
    now: i64,
    kept: &mut HashSet<FileId>,
) -> Result<bool, ContentError> {
    let states = states(conn, declared)?;
    let held: HashSet<FileId> = db::all_manifests(conn)?.into_iter().collect();
    let mut changed = false;
    let custody = db::custody(conn)?;
    for &file_id in &custody {
        let settled_rows = states.get(&file_id).map_or(0, Vec::len)
            > named.provisional_rows.get(&file_id).copied().unwrap_or(0);
        let available = states
            .get(&file_id)
            .is_some_and(|rows| rows.iter().any(|state| state.as_deref() == Some(AVAILABLE)));
        if !held.contains(&file_id) || available {
            db::leave_custody(conn, file_id)?;
            changed = true;
        } else if named.provisional.contains_key(&file_id) {
            kept.insert(file_id);
        } else if settled_rows {
            db::custody_settled(conn, file_id, now)?;
            kept.insert(file_id);
        } else {
            db::leave_custody(conn, file_id)?;
            changed = true;
        }
    }
    let custody: HashSet<FileId> = custody.into_iter().collect();
    for (file_id, rows) in &states {
        if custody.contains(file_id)
            || !held.contains(file_id)
            || db::is_unsent(conn, *file_id)?
            || !rows.iter().any(|state| state.as_deref() == Some(LOST))
        {
            continue;
        }
        db::enter_custody(conn, *file_id, None)?;
        kept.insert(*file_id);
        changed = true;
    }
    Ok(changed)
}

/// The files of this device's own unsent writes a replicated column names,
/// whose holders it counts.
///
/// # Errors
///
/// [`ContentError::Replica`] when a replicated column or the outbox cannot be
/// read.
pub(in crate::client) fn own_replicated(
    conn: &mut SqliteConnection,
    declared: &[PeerColumn],
) -> Result<HashSet<FileId>, ContentError> {
    let named = states(conn, declared)?;
    let mut own = HashSet::new();
    for file_id in db::outbox(conn)? {
        if named.contains_key(&file_id) {
            own.insert(file_id);
        }
    }
    Ok(own)
}

/// The earliest instant, in Unix seconds, at which the author of a
/// provisional row naming a file in custody stops being in courier reach,
/// `None` when no custody copy hangs on a courierable author.
///
/// # Errors
///
/// [`ContentError::Client`] when the provisional records or a cursor cannot
/// be read.
pub(in crate::client) fn next_lapse<T>(
    conn: &mut ConnettoConnection<T>,
    declared: &[PeerColumn],
) -> Result<Option<i64>, ContentError>
where
    T: Transport,
    T::Error: Display,
{
    let named = named(conn, declared, &mut Reach::default())?;
    let custody: HashSet<FileId> = db::custody(conn.conn())?.into_iter().collect();
    let mut authors: Vec<&[u8]> = named
        .provisional
        .iter()
        .filter(|(file_id, _)| custody.contains(file_id))
        .map(|(_, author)| author.as_slice())
        .collect();
    authors.sort_unstable();
    authors.dedup();
    let mut earliest = None;
    for author in authors {
        if let Some(at) = conn.courier_lapse_at(author)? {
            earliest = Some(earliest.map_or(at, |earliest: i64| earliest.min(at)));
        }
    }
    Ok(earliest)
}

/// The wall clock in Unix seconds, which `settled_seen_at` and the cursors'
/// certificate ends are kept in.
pub(in crate::client) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
        })
}
