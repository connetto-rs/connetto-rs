//! The provenance of peer-applied rows (R77 decision 2).
//!
//! A row a peer applies lands in the ordinary replica tables, so the
//! application's own SQL sees it, and this table keeps, beside the row, who
//! wrote it and from which entry of the author's queue. The eviction passes
//! spare the rows still awaiting a verdict, a server write on a key clears
//! the row's record and confirms it, and the application stands the records
//! back from the table, since the broadcast event stream can drop events.

use std::collections::HashMap;

use diesel::prelude::*;
use sqlite_diff_rs::Value;

use crate::live::bind_literal_of;
use crate::{ClientError, KeyValue};
use connetto_core::quote_ident;

/// Creates the provenance table beside the replica's other bookkeeping.
pub(crate) const PROVISIONAL_DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_provisional \
     (tbl TEXT, pk BLOB, author BLOB NOT NULL, seq INTEGER NOT NULL, \
      account TEXT NOT NULL, received_at INTEGER NOT NULL, state INTEGER NOT NULL, \
      displaced_author BLOB, displaced_seq INTEGER, \
      PRIMARY KEY (tbl, pk))";

diesel::table! {
    /// The peer-applied rows' provenance, one record beside each row it
    /// describes (R77).
    _connetto_provisional (tbl, pk) {
        /// The replica table holding the row, lowercased physical name.
        tbl -> Text,
        /// The row's key, encoded as [`encode_key`](crate::provisional::encode_key).
        pk -> Binary,
        /// The author's key id.
        author -> Binary,
        /// The author's queue sequence the row came from.
        seq -> BigInt,
        /// The author's certificate account.
        account -> Text,
        /// The wall clock the row was received at, in seconds, for display.
        received_at -> BigInt,
        /// Whether the row still awaits a verdict.
        state -> BigInt,
        /// The author a later local write displaced.
        displaced_author -> Nullable<Binary>,
        /// That author's queue sequence.
        displaced_seq -> Nullable<BigInt>,
    }
}

use _connetto_provisional::dsl;

/// Whether a row's provenance still awaits a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionalState {
    /// The row still awaits a server or peer verdict, so the eviction passes
    /// spare it.
    Provisional,
    /// A verdict settled the key, so the row falls under ordinary coverage.
    Adjudicated,
}

impl ProvisionalState {
    /// The value the table stores.
    #[must_use]
    pub const fn db(self) -> i64 {
        match self {
            Self::Provisional => 0,
            Self::Adjudicated => 1,
        }
    }
}

fn state_of(value: i64) -> Result<ProvisionalState, ClientError> {
    match value {
        0 => Ok(ProvisionalState::Provisional),
        1 => Ok(ProvisionalState::Adjudicated),
        other => Err(ClientError::Session(format!(
            "a provisional record holds no state the table defines: {other}"
        ))),
    }
}

/// One row of the provenance table: who wrote the replica row, from which
/// entry of the author's queue, and what the write displaced.
#[derive(Debug, Clone, PartialEq)]
pub struct ProvisionalRow {
    /// The replica table holding the row.
    pub table: String,
    /// The row's primary-key column values, in key order.
    pub key: Vec<KeyValue>,
    /// The author's key id.
    pub author: Vec<u8>,
    /// The author's queue sequence the row came from.
    pub seq: i64,
    /// The author's certificate account.
    pub account: String,
    /// The wall clock the row was received at, in seconds, for display.
    pub received_at: i64,
    /// Whether the row still awaits a verdict.
    pub state: ProvisionalState,
    /// The author the receiver's own write displaced, with that author's
    /// queue sequence.
    pub displaced_author: Option<Vec<u8>>,
    /// That author's queue sequence.
    pub displaced_seq: Option<i64>,
}

/// The stored row, in the table's column order, for the internal reads.
#[derive(diesel::Queryable)]
struct Stored {
    tbl: String,
    pk: Vec<u8>,
    author: Vec<u8>,
    seq: i64,
    account: String,
    received_at: i64,
    state: i64,
    displaced_author: Option<Vec<u8>>,
    displaced_seq: Option<i64>,
}

const TAG_NULL: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_REAL: u8 = 2;
const TAG_TEXT: u8 = 3;
const TAG_BLOB: u8 = 4;

/// The table's `pk` encoding of a key: one tag byte per column, the integer
/// and real values as eight little-endian bytes, the text and blob values as
/// an eight-byte little-endian length then their bytes.
#[must_use]
pub fn encode_key(key: &[KeyValue]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in key {
        match value {
            KeyValue::Null => out.push(TAG_NULL),
            KeyValue::Int(int) => {
                out.push(TAG_INTEGER);
                out.extend_from_slice(&int.to_le_bytes());
            }
            KeyValue::Real(real) => {
                out.push(TAG_REAL);
                out.extend_from_slice(&real.to_le_bytes());
            }
            KeyValue::Text(text) => {
                out.push(TAG_TEXT);
                push_sized(&mut out, text.as_bytes());
            }
            KeyValue::Blob(blob) => {
                out.push(TAG_BLOB);
                push_sized(&mut out, blob);
            }
        }
    }
    out
}

fn push_sized(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Decode the table's `pk` bytes back into their typed columns.
///
/// # Errors
///
/// [`ClientError::Session`] when the bytes are not a well-formed key.
pub fn decode_key(bytes: &[u8]) -> Result<Vec<KeyValue>, ClientError> {
    let mut out = Vec::new();
    let mut rest = bytes;
    while let Some((&tag, payload)) = rest.split_first() {
        rest = payload;
        let value = match tag {
            TAG_NULL => KeyValue::Null,
            TAG_INTEGER | TAG_REAL => {
                let Some((word, tail)) = rest.split_first_chunk::<8>() else {
                    return Err(short());
                };
                rest = tail;
                if tag == TAG_INTEGER {
                    KeyValue::Int(i64::from_le_bytes(*word))
                } else {
                    KeyValue::Real(f64::from_le_bytes(*word))
                }
            }
            TAG_TEXT | TAG_BLOB => {
                let Some((head, tail)) = rest.split_first_chunk::<8>() else {
                    return Err(short());
                };
                // A length no slice can hold on this target is as short as any
                // other length the bytes cannot cover.
                let len = usize::try_from(u64::from_le_bytes(*head)).unwrap_or(usize::MAX);
                let Some((body, tail)) = tail.split_at_checked(len) else {
                    return Err(short());
                };
                rest = tail;
                if tag == TAG_TEXT {
                    KeyValue::Text(String::from_utf8(body.to_vec()).map_err(|_| {
                        ClientError::Session(
                            "a provisional key holds a text column that is not valid UTF-8"
                                .to_owned(),
                        )
                    })?)
                } else {
                    KeyValue::Blob(body.to_vec())
                }
            }
            _ => {
                return Err(ClientError::Session(
                    "a provisional key carries an undefined column tag".to_owned(),
                ));
            }
        };
        out.push(value);
    }
    Ok(out)
}

fn short() -> ClientError {
    ClientError::Session("a provisional key is truncated".to_owned())
}

/// The wire shape a stored key column takes.
fn value_of(value: &KeyValue) -> Value<String, Vec<u8>> {
    match value {
        KeyValue::Null => Value::Null,
        KeyValue::Int(int) => Value::Integer(*int),
        KeyValue::Real(real) => Value::Real(*real),
        KeyValue::Text(text) => Value::Text(text.clone()),
        KeyValue::Blob(blob) => Value::Blob(blob.clone()),
    }
}

/// The table's primary-key column names, in key order, the empty list for a
/// rowid table that declares none.
fn pk_column_names(db: &mut SqliteConnection, table: &str) -> Result<Vec<String>, ClientError> {
    #[derive(diesel::QueryableByName)]
    struct KeyColumn {
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
    }
    // The table name is the provenance record's own, so the pragma stays a
    // raw statement.
    let quoted = table.replace('\'', "''");
    let rows: Vec<KeyColumn> = diesel::sql_query(format!(
        "SELECT name FROM pragma_table_info('{quoted}') WHERE pk > 0 ORDER BY pk"
    ))
    .load(db)?;
    Ok(rows.into_iter().map(|row| row.name).collect())
}

/// One keep clause for a stored key, the same `(col IS literal AND ...)`
/// shape the pending spare builds, or `None` when the table's key layout
/// cannot express the key, whose row is then provably gone.
fn key_clause(
    db: &mut SqliteConnection,
    table: &str,
    key: &[KeyValue],
) -> Result<Option<String>, ClientError> {
    let names = pk_column_names(db, table)?;
    if key.is_empty() || names.len() != key.len() {
        return Ok(None);
    }
    let wheres: Vec<String> = names
        .iter()
        .zip(key)
        .map(|(name, value)| {
            Ok(format!(
                "{} IS {}",
                quote_ident(name),
                bind_literal_of(&value_of(value))?
            ))
        })
        .collect::<Result<_, ClientError>>()?;
    Ok(Some(format!("({})", wheres.join(" AND "))))
}

/// The eviction passes' keep clauses over the rows still awaiting a
/// verdict: per lowercased physical table, one clause per row, the same
/// shape and keys the pending spare carries.
pub(crate) fn spare(
    db: &mut SqliteConnection,
) -> Result<HashMap<String, Vec<String>>, ClientError> {
    let mut spare: HashMap<String, Vec<String>> = HashMap::new();
    let stored: Vec<(String, Vec<u8>, i64)> = dsl::_connetto_provisional
        .select((dsl::tbl, dsl::pk, dsl::state))
        .load(db)?;
    for record in stored {
        let (tbl, pk, state) = record;
        if state != ProvisionalState::Provisional.db() {
            continue;
        }
        let Ok(key) = decode_key(&pk) else {
            continue;
        };
        let Some(clause) = key_clause(db, &tbl, &key)? else {
            continue;
        };
        spare.entry(tbl).or_default().push(clause);
    }
    Ok(spare)
}

/// The records, keyed order across tables.
fn load_all(db: &mut SqliteConnection) -> Result<Vec<Stored>, ClientError> {
    Ok(dsl::_connetto_provisional
        .order((dsl::tbl.asc(), dsl::pk.asc()))
        .load(db)?)
}

fn row_of(record: Stored) -> Result<ProvisionalRow, ClientError> {
    Ok(ProvisionalRow {
        table: record.tbl,
        key: decode_key(&record.pk)?,
        author: record.author,
        seq: record.seq,
        account: record.account,
        received_at: record.received_at,
        state: state_of(record.state)?,
        displaced_author: record.displaced_author,
        displaced_seq: record.displaced_seq,
    })
}

/// Every record the replica holds, keyed order.
pub(crate) fn rows(db: &mut SqliteConnection) -> Result<Vec<ProvisionalRow>, ClientError> {
    load_all(db)?.into_iter().map(row_of).collect()
}

/// The record of one key, `None` when the key holds none.
pub(crate) fn row_of_key(
    db: &mut SqliteConnection,
    table: &str,
    key: &[KeyValue],
) -> Result<Option<ProvisionalRow>, ClientError> {
    let blob = encode_key(key);
    let record: Option<Stored> = dsl::_connetto_provisional
        .filter(dsl::tbl.eq(table))
        .filter(dsl::pk.eq(&blob))
        .first(db)
        .optional()?;
    record.map(row_of).transpose()
}

/// The records of the keys a server write just wrote, deleted in the
/// caller's transaction and returned as the application stands them. A key
/// named twice is cleared once, its record gone at the second lookup.
pub(crate) fn clear_written(
    db: &mut SqliteConnection,
    keys: &[(String, Vec<KeyValue>)],
) -> Result<Vec<ProvisionalRow>, ClientError> {
    let mut out = Vec::new();
    for (table, key) in keys {
        let blob = encode_key(key);
        let record: Option<Stored> = dsl::_connetto_provisional
            .filter(dsl::tbl.eq(table))
            .filter(dsl::pk.eq(&blob))
            .first(db)
            .optional()?;
        if let Some(record) = record {
            diesel::delete(dsl::_connetto_provisional)
                .filter(dsl::tbl.eq(table))
                .filter(dsl::pk.eq(&blob))
                .execute(db)?;
            out.push(row_of(record)?);
        }
    }
    Ok(out)
}

/// The (table, key) pairs a payload writes, payload order, the tables
/// lowercased as the payload names them.
pub(crate) fn written_keys(bytes: &[u8]) -> Result<Vec<(String, Vec<KeyValue>)>, ClientError> {
    let ops = crate::op_keys(bytes).map_err(|err| {
        ClientError::Apply(format!("parsing a payload to confirm its keys: {err:?}"))
    })?;
    Ok(ops
        .into_iter()
        .map(|(table, key)| (table.to_lowercase(), key))
        .collect())
}

/// Drop the records of `table`'s keys whose rows are gone, whatever took
/// them, so a record never outlives the row it describes.
pub(crate) fn drop_gone(db: &mut SqliteConnection, table: &str) -> Result<(), ClientError> {
    let records: Vec<(Vec<u8>, i64)> = dsl::_connetto_provisional
        .select((dsl::pk, dsl::state))
        .filter(dsl::tbl.eq(table))
        .load(db)?;
    for (pk, state) in records {
        if state == ProvisionalState::Provisional.db() {
            // A provisional delete keeps its record beside the gone row, so
            // a retraction can restore the row from the entry's old values.
            continue;
        }
        let Ok(key) = decode_key(&pk) else {
            continue;
        };
        if !key_alive(db, table, &key)? {
            diesel::delete(dsl::_connetto_provisional)
                .filter(dsl::tbl.eq(table))
                .filter(dsl::pk.eq(&pk))
                .execute(db)?;
        }
    }
    Ok(())
}

fn key_alive(
    db: &mut SqliteConnection,
    table: &str,
    key: &[KeyValue],
) -> Result<bool, ClientError> {
    let Some(clause) = key_clause(db, table, key)? else {
        return Ok(false);
    };
    // The table and the clause come from the provenance record, so the DSL
    // cannot name them and the probe stays raw SQL.
    let probe = format!(
        "EXISTS (SELECT 1 FROM {} WHERE {clause})",
        quote_ident(table)
    );
    Ok(diesel::select(diesel::dsl::sql::<diesel::sql_types::Bool>(&probe)).get_result(db)?)
}

/// Drop every record of `table`, whose rows all went.
pub(crate) fn drop_table(db: &mut SqliteConnection, table: &str) -> Result<(), ClientError> {
    diesel::delete(dsl::_connetto_provisional)
        .filter(dsl::tbl.eq(table))
        .execute(db)?;
    Ok(())
}

/// Store or replace the record of one key, the delete and insert running
/// in the caller's transaction so the replacement is atomic (R77).
pub(crate) fn upsert_record(
    db: &mut SqliteConnection,
    record: &ProvisionalRow,
) -> Result<(), ClientError> {
    let pk = encode_key(&record.key);
    diesel::delete(dsl::_connetto_provisional)
        .filter(dsl::tbl.eq(&record.table))
        .filter(dsl::pk.eq(&pk))
        .execute(db)?;
    diesel::insert_into(dsl::_connetto_provisional)
        .values((
            dsl::tbl.eq(record.table.clone()),
            dsl::pk.eq(&pk),
            dsl::author.eq(&record.author),
            dsl::seq.eq(record.seq),
            dsl::account.eq(record.account.clone()),
            dsl::received_at.eq(record.received_at),
            dsl::state.eq(record.state.db()),
            dsl::displaced_author.eq(&record.displaced_author),
            dsl::displaced_seq.eq(record.displaced_seq),
        ))
        .execute(db)?;
    Ok(())
}

/// Delete the record of one key, the retraction's key whose provenance the
/// receiver can no longer stand back.
pub(crate) fn delete_record(
    db: &mut SqliteConnection,
    table: &str,
    key: &[KeyValue],
) -> Result<(), ClientError> {
    let blob = encode_key(key);
    diesel::delete(dsl::_connetto_provisional)
        .filter(dsl::tbl.eq(table))
        .filter(dsl::pk.eq(&blob))
        .execute(db)?;
    Ok(())
}

/// The records naming one author's entry, whatever their verdict state, in
/// keyed order.
pub(crate) fn records_of_entry(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<Vec<ProvisionalRow>, ClientError> {
    let stored: Vec<Stored> = dsl::_connetto_provisional
        .filter(dsl::author.eq(author))
        .filter(dsl::seq.eq(seq))
        .order((dsl::tbl.asc(), dsl::pk.asc()))
        .load(db)?;
    stored.into_iter().map(row_of).collect()
}

/// Move the records naming one author's entry from provisional to
/// adjudicated, the frontier having settled them (R77).
pub(crate) fn adjudicate_entry(
    db: &mut SqliteConnection,
    author: &[u8],
    seq: i64,
) -> Result<usize, ClientError> {
    diesel::update(
        dsl::_connetto_provisional
            .filter(dsl::author.eq(author))
            .filter(dsl::seq.eq(seq))
            .filter(dsl::state.eq(ProvisionalState::Provisional.db())),
    )
    .set(dsl::state.eq(ProvisionalState::Adjudicated.db()))
    .execute(db)
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::{decode_key, encode_key};
    use crate::KeyValue;

    /// Every column kind round-trips, alone and inside a composite key.
    #[test]
    fn every_key_column_kind_round_trips() {
        let columns = [
            KeyValue::Null,
            KeyValue::Int(-7),
            KeyValue::Real(2.5),
            KeyValue::Text("pk é".to_owned()),
            KeyValue::Blob(vec![0, 255, 1]),
        ];
        for column in &columns {
            let key = vec![column.clone()];
            let bytes = encode_key(&key);
            assert_eq!(decode_key(&bytes).expect("decode"), key);
        }
        let bytes = encode_key(&columns);
        assert_eq!(decode_key(&bytes).expect("decode"), columns);
    }

    /// Keys SQLite tells apart encode apart, so two rows never share a record.
    #[test]
    fn distinct_keys_encode_distinctly() {
        let keys = [
            vec![KeyValue::Int(1)],
            vec![KeyValue::Real(1.0)],
            vec![KeyValue::Text("1".to_owned())],
            vec![KeyValue::Blob(b"1".to_vec())],
            vec![KeyValue::Text("ab".to_owned())],
            vec![
                KeyValue::Text("a".to_owned()),
                KeyValue::Text("b".to_owned()),
            ],
        ];
        let encoded: Vec<Vec<u8>> = keys.iter().map(|key| encode_key(key)).collect();
        for (index, bytes) in encoded.iter().enumerate() {
            assert!(
                !encoded[index + 1..].contains(bytes),
                "{:?} shares its encoding with a later key",
                keys[index]
            );
        }
    }

    /// Bytes outside the encoding are refused, never read as another key.
    #[test]
    fn malformed_key_bytes_are_refused() {
        for bytes in [
            vec![1, 0, 0, 0],                                // an integer cut short
            vec![3, 5, 0, 0, 0],                             // a text length cut short
            vec![4, 5, 0, 0, 0, 0, 0, 0, 0, 1, 2],           // a blob shorter than its length
            vec![4, 255, 255, 255, 255, 255, 255, 255, 255], // a length no slice holds
            vec![3, 2, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xfe],     // text that is not UTF-8
            vec![9],                                         // an undefined tag
        ] {
            assert!(decode_key(&bytes).is_err(), "{bytes:?} decoded");
        }
    }
}
