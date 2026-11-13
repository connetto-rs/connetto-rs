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
    /// Whether a settled row names it and no row's state reads `available`,
    /// so the server may lack it and a copy handed on is kept in custody.
    pub(in crate::client) settled: bool,
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
    let mut rows: HashMap<FileId, (usize, bool)> = HashMap::new();
    let mut provisional_rows: HashMap<FileId, usize> = HashMap::new();
    for declared in declared {
        let Some(replicate) = &declared.replicate else {
            continue;
        };
        let copies = usize::from(replicate.copies);
        for (file_id, states) in states(conn.conn(), core::slice::from_ref(declared))? {
            let entry = placed.entry(file_id).or_default();
            entry.copies = entry.copies.max(copies);
            let (count, available) = rows.entry(file_id).or_default();
            *count += states.len();
            *available |= states
                .iter()
                .any(|state| state.as_deref() == Some(AVAILABLE));
        }
        for blob in conn.provisional_blobs(&declared.table, &declared.column)? {
            let Ok(id) = <[u8; 32]>::try_from(blob.value.as_slice()) else {
                continue;
            };
            let lapsed = reach.lapsed(conn, &blob.author)?;
            *provisional_rows.entry(FileId::from_bytes(id)).or_default() += 1;
            let entry = placed.entry(FileId::from_bytes(id)).or_default();
            entry.copies = entry.copies.max(copies);
            entry.pulled |= !lapsed;
            entry.authors.push(blob.author);
        }
    }
    for (file_id, entry) in &mut placed {
        let (count, available) = rows.get(file_id).copied().unwrap_or_default();
        entry.settled = !available && count > provisional_rows.get(file_id).copied().unwrap_or(0);
        // One author's several rows naming the file count once, in the order
        // their rows came.
        let mut seen = HashSet::new();
        entry.authors.retain(|author| seen.insert(author.clone()));
    }
    Ok(placed)
}

/// Whether a settled row names `file_id` and no row's state reads
/// `available`, so a copy pulled to take a leaving one over is kept in
/// custody with no provisional author.
///
/// # Errors
///
/// [`ContentError::Client`] when the provisional records or an author's
/// cursor cannot be read, and [`ContentError::Replica`] when a replicated
/// column cannot.
pub(in crate::client) fn settled_open<T>(
    conn: &mut ConnettoConnection<T>,
    declared: &[PeerColumn],
    file_id: FileId,
) -> Result<bool, ContentError>
where
    T: Transport,
    T::Error: Display,
{
    Ok(placing(conn, declared)?
        .get(&file_id)
        .is_some_and(|placed| placed.settled))
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

/// Why a file may sit in custody, the one rule the retention pass both
/// releases and takes back by, so no pass undoes the one before.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Hold {
    /// No eligible row keeps the copy.
    Release,
    /// A provisional row of a courierable author names it.
    Provisional,
    /// A settled row names it, and no row reads `available` unless one reads
    /// `lost`, since a wrong `lost` costs an upload and a wrong `available`
    /// the last copy.
    Settled,
}

/// What keeps `file_id` in custody, given the states of every row naming it.
fn hold(file_id: FileId, rows: &[Option<String>], named: &Named) -> Hold {
    let any = |wanted: &str| rows.iter().any(|state| state.as_deref() == Some(wanted));
    if any(AVAILABLE) && !any(LOST) {
        Hold::Release
    } else if named.provisional.contains_key(&file_id) {
        Hold::Provisional
    } else if rows.len() > named.provisional_rows.get(&file_id).copied().unwrap_or(0) {
        Hold::Settled
    } else {
        Hold::Release
    }
}

/// The retention pass's custody half, run inside its transaction: a copy
/// whose row is gone or whose author lapsed leaves, one a settled row names
/// stays until the state reads `available` and learns when it settled, and a
/// cache copy whose state reads `lost` returns when the same rule would keep
/// it. Every file left in custody joins `kept`, and the answer says whether
/// custody changed.
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
        let rows = states.get(&file_id).map_or(&[][..], Vec::as_slice);
        match (held.contains(&file_id), hold(file_id, rows, named)) {
            (true, Hold::Provisional) => {
                kept.insert(file_id);
            }
            (true, Hold::Settled) => {
                db::custody_settled(conn, file_id, now)?;
                kept.insert(file_id);
            }
            _ => {
                db::leave_custody(conn, file_id)?;
                changed = true;
            }
        }
    }
    let custody: HashSet<FileId> = custody.into_iter().collect();
    for (file_id, rows) in &states {
        if custody.contains(file_id)
            || !held.contains(file_id)
            || !rows.iter().any(|state| state.as_deref() == Some(LOST))
            || hold(*file_id, rows, named) == Hold::Release
            || db::is_unsent(conn, *file_id)?
        {
            continue;
        }
        db::enter_custody(conn, *file_id, None, None)?;
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use connetto_file_core::{ChunkHash, ChunkMeta, FileId, Manifest};
    use diesel::connection::SimpleConnection;
    use diesel::prelude::*;

    use super::super::{PeerColumn, Replicate};
    use super::{Named, pass};
    use crate::db;

    diesel::table! {
        photos (id) {
            id -> BigInt,
            content_id -> Binary,
            content_state -> Nullable<Text>,
        }
    }

    /// A replica holding one cached file in custody, named by one row per
    /// entry of `states`.
    fn replica(file_id: FileId, states: &[&str]) -> SqliteConnection {
        let mut conn = SqliteConnection::establish(":memory:").expect("an in-memory replica");
        conn.batch_execute(db::CONTENT_DDL)
            .expect("the bookkeeping");
        conn.batch_execute(
            "CREATE TABLE photos (id INTEGER PRIMARY KEY, content_id BLOB NOT NULL, \
             content_state TEXT)",
        )
        .expect("the photo table");
        for (id, state) in (1..).zip(states) {
            diesel::insert_into(photos::table)
                .values((
                    photos::id.eq(id),
                    photos::content_id.eq(file_id.as_bytes().to_vec()),
                    photos::content_state.eq(*state),
                ))
                .execute(&mut conn)
                .expect("name the file");
        }
        let chunk = ChunkMeta {
            hash: ChunkHash::from_bytes([9; 32]),
            len: 10,
        };
        db::put_manifest(&mut conn, &Manifest::new(file_id, vec![chunk])).expect("the manifest");
        db::enter_custody(&mut conn, file_id, Some(&[7; 32]), None).expect("the custody record");
        conn
    }

    fn declared() -> [PeerColumn; 1] {
        [PeerColumn {
            table: "photos".to_owned(),
            column: "content_id".to_owned(),
            prefetch: false,
            replicate: Some(Replicate {
                copies: 3,
                state: "content_state".to_owned(),
            }),
        }]
    }

    /// Runs the pass four times, answering whether each changed custody.
    fn passes(conn: &mut SqliteConnection, named: &Named) -> Vec<bool> {
        (0..4)
            .map(|_| pass(conn, &declared(), named, 0, &mut HashSet::new()).expect("the pass"))
            .collect()
    }

    #[test]
    fn a_lost_copy_of_a_lapsed_authors_provisional_row_stays_released() {
        let file_id = FileId::from_bytes([1; 32]);
        let mut conn = replica(file_id, &["lost"]);
        let lapsed = Named {
            provisional: HashMap::new(),
            provisional_rows: HashMap::from([(file_id, 1)]),
        };
        assert_eq!(passes(&mut conn, &lapsed), [true, false, false, false]);
        assert_eq!(
            db::custody(&mut conn).expect("custody"),
            Vec::<FileId>::new()
        );
    }

    #[test]
    fn a_lost_row_brings_back_a_copy_another_row_reads_available_for() {
        let file_id = FileId::from_bytes([2; 32]);
        let mut conn = replica(file_id, &["available", "lost"]);
        db::leave_custody(&mut conn, file_id).expect("start as cache");
        assert_eq!(
            passes(&mut conn, &Named::default()),
            [true, false, false, false]
        );
        assert_eq!(db::custody(&mut conn).expect("custody"), [file_id]);
    }

    #[test]
    fn a_lost_copy_of_a_settled_row_returns_and_stays() {
        let file_id = FileId::from_bytes([3; 32]);
        let mut conn = replica(file_id, &["lost"]);
        db::leave_custody(&mut conn, file_id).expect("start as cache");
        assert_eq!(
            passes(&mut conn, &Named::default()),
            [true, false, false, false]
        );
        assert_eq!(db::custody(&mut conn).expect("custody"), [file_id]);
    }
}
