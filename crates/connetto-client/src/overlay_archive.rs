use std::collections::HashMap;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Binary, Double, Nullable, Text};
use diesel_sqlite_session::SqliteSessionExt;

use crate::archive::Cell;
use crate::archive::rows::{IncomingRow, RowWrite};
use crate::overlay::{_connetto_overlay_tags, _connetto_overlays};
use crate::{ClientError, harden, outbox, quote_ident};

const SCRATCH: &str = "connetto_overlay_export";
const TAGS: &str = "_connetto_overlay_tags";
type Tag = (String, String, Option<String>, Option<i64>);

diesel::allow_tables_to_appear_in_same_query!(_connetto_overlay_tags, crate::_connetto_pending);

/// Export the overlay rows currently owned by one entry.
pub(crate) fn export_entry(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
) -> Result<Vec<u8>, ClientError> {
    let tags = _connetto_overlay_tags::table
        .filter(_connetto_overlay_tags::writer.eq(writer))
        .filter(_connetto_overlay_tags::seq.eq(sequence))
        .select((
            _connetto_overlay_tags::tbl,
            _connetto_overlay_tags::key,
            _connetto_overlay_tags::writer,
            _connetto_overlay_tags::seq,
        ))
        .load::<Tag>(database)?;
    export_tags(database, &tags)
}

/// Export overlay rows whose entries are no longer pending.
pub(crate) fn export_held(database: &mut SqliteConnection) -> Result<Vec<u8>, ClientError> {
    use crate::_connetto_pending;
    let tags = _connetto_overlay_tags::table
        .filter(_connetto_overlay_tags::writer.is_not_null())
        .filter(_connetto_overlay_tags::dirty.eq(false))
        .filter(diesel::dsl::not(diesel::dsl::exists(
            _connetto_pending::table
                .select(_connetto_pending::seq)
                .filter(
                    _connetto_pending::writer
                        .nullable()
                        .eq(_connetto_overlay_tags::writer),
                )
                .filter(
                    _connetto_pending::seq
                        .nullable()
                        .eq(_connetto_overlay_tags::seq),
                ),
        )))
        .select((
            _connetto_overlay_tags::tbl,
            _connetto_overlay_tags::key,
            _connetto_overlay_tags::writer,
            _connetto_overlay_tags::seq,
        ))
        .load::<Tag>(database)?;
    export_tags(database, &tags)
}

fn export_tags(database: &mut SqliteConnection, tags: &[Tag]) -> Result<Vec<u8>, ClientError> {
    if tags.is_empty() {
        return Ok(Vec::new());
    }
    harden::attach_in_window(database, ":memory:", SCRATCH, harden::AttachPermits::Write)?;
    let result = export_attached(database, tags);
    let detached = database
        .batch_execute(&format!("DETACH DATABASE {}", quote_ident(SCRATCH)))
        .map_err(ClientError::from)
        .and_then(|()| harden::seal_attaches(database).map_err(ClientError::from));
    let bytes = result?;
    detached?;
    Ok(bytes)
}

fn export_attached(database: &mut SqliteConnection, tags: &[Tag]) -> Result<Vec<u8>, ClientError> {
    let catalog = crate::schema_ddl_rows(database, "main")?;
    let mut tables: Vec<&str> = tags.iter().map(|tag| tag.0.as_str()).collect();
    tables.push(TAGS);
    tables.sort_unstable();
    tables.dedup();
    for table in &tables {
        if *table == TAGS {
            database.batch_execute(&format!(
                "CREATE TABLE {}.{TAGS} (tbl TEXT NOT NULL, key TEXT NOT NULL, \
                 writer TEXT, seq INTEGER, PRIMARY KEY (tbl, key))",
                quote_ident(SCRATCH),
            ))?;
            continue;
        }
        let definition = catalog
            .iter()
            .find(|row| row.name == *table)
            .and_then(|row| row.sql.as_deref())
            .ok_or_else(|| ClientError::Export(format!("missing overlay table {table}")))?;
        database.batch_execute(&crate::rename_create_table(
            definition,
            &format!("{}.{}", quote_ident(SCRATCH), quote_ident(table)),
        )?)?;
    }
    let mut session = database
        .create_session_on(SCRATCH)
        .map_err(|error| ClientError::Export(error.to_string()))?;
    for table in &tables {
        session
            .attach_by_name(table)
            .map_err(|error| ClientError::Export(error.to_string()))?;
    }
    for (table, key, writer, sequence) in tags {
        let columns = outbox::columns(database, "main", table)?;
        // Runtime overlay names and columns cannot use a static Diesel schema.
        let copied = diesel::sql_query(format!(
            "INSERT INTO {}.{} SELECT source.* FROM main.{} AS source WHERE {} = ?",
            quote_ident(SCRATCH),
            quote_ident(table),
            quote_ident(table),
            outbox::key_clause("source", &columns),
        ))
        .bind::<Text, _>(key)
        .execute(database)?;
        if copied != 1 {
            return Err(ClientError::Export(format!(
                "overlay tag for {table} names {copied} rows"
            )));
        }
        // The destination is an attached scratch schema rather than main.
        diesel::sql_query(format!(
            "INSERT INTO {}.{TAGS} (tbl, key, writer, seq) VALUES (?, ?, ?, ?)",
            quote_ident(SCRATCH),
        ))
        .bind::<Text, _>(table)
        .bind::<Text, _>(key)
        .bind::<Nullable<Text>, _>(writer.as_deref())
        .bind::<Nullable<BigInt>, _>(*sequence)
        .execute(database)?;
    }
    session
        .patchset()
        .map_err(|error| ClientError::Export(error.to_string()))
}

/// Restore entry rows under the receiving writer and sequence.
pub(crate) fn import_entry(
    database: &mut SqliteConnection,
    bytes: &[u8],
    writer: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    import_rows(database, bytes, Some((writer, sequence)))
}

/// Restore held rows under their original ownership.
pub(crate) fn import_held(
    database: &mut SqliteConnection,
    bytes: &[u8],
) -> Result<(), ClientError> {
    import_rows(database, bytes, None)
}

fn import_columns(
    database: &mut SqliteConnection,
) -> Result<HashMap<String, Vec<String>>, ClientError> {
    let tables = _connetto_overlays::table
        .select(_connetto_overlays::overlay)
        .load::<String>(database)?;
    let mut known = HashMap::new();
    for table in tables {
        let columns = outbox::columns(database, "main", &table)?;
        known.insert(
            table.to_lowercase(),
            columns.into_iter().map(|column| column.name).collect(),
        );
    }
    known.insert(
        TAGS.to_owned(),
        ["tbl", "key", "writer", "seq"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
    );
    Ok(known)
}

fn import_rows(
    database: &mut SqliteConnection,
    bytes: &[u8],
    owner: Option<(&str, i64)>,
) -> Result<(), ClientError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let known = import_columns(database)?;
    let rows = crate::archive::rows::read_rows(bytes, &known)?;
    let mut tags = HashMap::new();
    let mut overlays = Vec::new();
    for row in rows {
        if row.table == TAGS {
            let mut values = row.values.into_iter();
            let (
                Some(Cell::Text(table)),
                Some(Cell::Text(key)),
                Some(Cell::Text(writer)),
                Some(Cell::Integer(sequence)),
                None,
            ) = (
                values.next(),
                values.next(),
                values.next(),
                values.next(),
                values.next(),
            )
            else {
                return Err(ClientError::Import("invalid overlay ownership tag".into()));
            };
            if sequence <= 0
                || crate::writer_from_id(&writer).is_none()
                || tags
                    .entry(table)
                    .or_insert_with(HashMap::new)
                    .insert(key, (writer, sequence))
                    .is_some()
            {
                return Err(ClientError::Import("invalid overlay ownership tag".into()));
            }
        } else {
            overlays.push(row);
        }
    }
    let mut restored = Vec::with_capacity(overlays.len());
    for row in overlays {
        let key = row_key(database, &row)?;
        let Some((writer, sequence)) = tags.get_mut(&row.table).and_then(|keys| keys.remove(&key))
        else {
            return Err(ClientError::Import(
                "overlay row has no matching ownership tag".into(),
            ));
        };
        restored.push((row, key, writer, sequence));
    }
    if tags.values().any(|keys| !keys.is_empty()) {
        return Err(ClientError::Import(
            "overlay ownership tag has no matching row".into(),
        ));
    }
    database.transaction(|database| {
        for (row, key, writer, sequence) in &restored {
            crate::archive::rows::write_row(
                database,
                &RowWrite {
                    schema: "main",
                    table: &row.table,
                    columns: &row.columns,
                    key_columns: &row.key_columns,
                    values: &row.values,
                },
            )?;
            let (writer, sequence) = owner.unwrap_or((writer.as_str(), *sequence));
            diesel::insert_into(_connetto_overlay_tags::table)
                .values((
                    _connetto_overlay_tags::tbl.eq(&row.table),
                    _connetto_overlay_tags::key.eq(key),
                    _connetto_overlay_tags::writer.eq(writer),
                    _connetto_overlay_tags::seq.eq(sequence),
                    _connetto_overlay_tags::dirty.eq(false),
                ))
                .on_conflict((_connetto_overlay_tags::tbl, _connetto_overlay_tags::key))
                .do_update()
                .set((
                    _connetto_overlay_tags::writer.eq(writer),
                    _connetto_overlay_tags::seq.eq(sequence),
                    _connetto_overlay_tags::dirty.eq(false),
                ))
                .execute(database)?;
        }
        Ok(())
    })
}

fn row_key(database: &mut SqliteConnection, row: &IncomingRow) -> Result<String, ClientError> {
    #[derive(QueryableByName)]
    struct KeyRow {
        #[diesel(sql_type = Text)]
        key: String,
    }
    let columns = outbox::columns(database, "main", &row.table)?;
    let expected: Vec<&str> = outbox::key_columns(&columns)
        .into_iter()
        .map(|column| column.name.as_str())
        .collect();
    if !expected
        .iter()
        .copied()
        .eq(row.key_columns.iter().map(String::as_str))
    {
        return Err(ClientError::Import(
            "overlay primary key differs from its schema".into(),
        ));
    }
    let projection = columns
        .iter()
        .map(|column| format!("? AS {}", quote_ident(&column.name)))
        .collect::<Vec<_>>()
        .join(", ");
    // SQLite renders typed key values with the same expression used by capture.
    let mut query = diesel::sql_query(format!(
        "SELECT {} AS key FROM (SELECT {projection}) AS source",
        outbox::key_clause("source", &columns)
    ))
    .into_boxed::<diesel::sqlite::Sqlite>();
    for value in &row.values {
        query = match value {
            Cell::Null => query.bind::<Nullable<Text>, _>(None::<&str>),
            Cell::Integer(number) => query.bind::<BigInt, _>(*number),
            Cell::Real(number) => query.bind::<Double, _>(*number),
            Cell::Text(text) => query.bind::<Text, _>(text.as_str()),
            Cell::Blob(bytes) => query.bind::<Binary, _>(bytes.as_slice()),
        };
    }
    Ok(query.get_result::<KeyRow>(database)?.key)
}

#[cfg(test)]
mod tests {
    use super::*;

    diesel::table! {
        sample_overlay (id) {
            id -> BigInt,
            payload -> Binary,
            title -> Text,
            tombstone -> Bool,
        }
    }

    fn database() -> SqliteConnection {
        let mut database = SqliteConnection::establish(":memory:").expect("database");
        database
            .batch_execute(crate::overlay::DDL)
            .expect("metadata");
        database.batch_execute(
            "CREATE TABLE sample_overlay (id INTEGER PRIMARY KEY NOT NULL, payload BLOB NOT NULL, \
             title TEXT NOT NULL, tombstone INTEGER NOT NULL); \
             CREATE TABLE _connetto_pending (writer TEXT NOT NULL, seq INTEGER NOT NULL); \
             INSERT INTO _connetto_pending VALUES ('number:1', 7);"
        ).expect("schema");
        diesel::insert_into(_connetto_overlays::table)
            .values((
                _connetto_overlays::logical.eq("sample"),
                _connetto_overlays::physical.eq("sample_backing"),
                _connetto_overlays::overlay.eq("sample_overlay"),
            ))
            .execute(&mut database)
            .expect("registry");
        database
    }

    fn insert(database: &mut SqliteConnection, identity: i64, sequence: i64, tombstone: bool) {
        diesel::insert_into(sample_overlay::table)
            .values((
                sample_overlay::id.eq(identity),
                sample_overlay::payload.eq(&[0u8, 255, 1][..]),
                sample_overlay::title.eq("before\0after"),
                sample_overlay::tombstone.eq(tombstone),
            ))
            .execute(database)
            .expect("overlay");
        diesel::insert_into(_connetto_overlay_tags::table)
            .values((
                _connetto_overlay_tags::tbl.eq("sample_overlay"),
                _connetto_overlay_tags::key.eq(format!("(\"id\" IS {identity})")),
                _connetto_overlay_tags::writer.eq("number:1"),
                _connetto_overlay_tags::seq.eq(sequence),
            ))
            .execute(database)
            .expect("tag");
    }

    #[test]
    fn entry_round_trip_preserves_typed_rows_and_rekeys_ownership() {
        let mut source = database();
        insert(&mut source, 1, 7, false);
        insert(&mut source, 2, 7, true);
        insert(&mut source, 3, 8, false);
        let bytes = export_entry(&mut source, "number:1", 7).expect("export");
        let mut target = database();
        import_entry(&mut target, &bytes, "number:2", 11).expect("import");
        let rows = sample_overlay::table
            .order(sample_overlay::id)
            .load::<(i64, Vec<u8>, String, bool)>(&mut target)
            .expect("rows");
        assert_eq!(
            rows,
            vec![
                (1, vec![0, 255, 1], "before\0after".into(), false),
                (2, vec![0, 255, 1], "before\0after".into(), true),
            ]
        );
        let owners = _connetto_overlay_tags::table
            .select((_connetto_overlay_tags::writer, _connetto_overlay_tags::seq))
            .load::<(Option<String>, Option<i64>)>(&mut target)
            .expect("owners");
        assert_eq!(owners, vec![(Some("number:2".into()), Some(11)); 2]);
    }

    #[test]
    fn held_round_trip_excludes_pending_rows_and_keeps_original_owner() {
        let mut source = database();
        insert(&mut source, 1, 7, false);
        insert(&mut source, 2, 8, true);
        let bytes = export_held(&mut source).expect("export held");
        let mut target = database();
        import_held(&mut target, &bytes).expect("import held");
        assert_eq!(
            sample_overlay::table
                .select(sample_overlay::id)
                .load::<i64>(&mut target)
                .expect("rows"),
            vec![2]
        );
        assert_eq!(
            _connetto_overlay_tags::table
                .select((_connetto_overlay_tags::writer, _connetto_overlay_tags::seq))
                .first::<(Option<String>, Option<i64>)>(&mut target)
                .expect("owner"),
            (Some("number:1".into()), Some(8))
        );
    }

    #[test]
    fn forged_tag_predicate_is_rejected_without_writing_rows() {
        let mut source = database();
        insert(&mut source, 1, 7, false);
        let mut bytes = export_entry(&mut source, "number:1", 7).expect("export");
        let predicate = b"(\"id\" IS 1)";
        let offset = bytes
            .windows(predicate.len())
            .position(|part| part == predicate)
            .expect("tag predicate");
        bytes[offset..offset + predicate.len()].copy_from_slice(b"(1 OR 1=1 )");
        let mut target = database();
        assert!(import_entry(&mut target, &bytes, "number:2", 11).is_err());
        assert_eq!(
            sample_overlay::table
                .count()
                .get_result::<i64>(&mut target)
                .expect("count"),
            0
        );
        assert_eq!(
            _connetto_overlay_tags::table
                .count()
                .get_result::<i64>(&mut target)
                .expect("count"),
            0
        );
    }

    #[test]
    fn imported_tags_clear_dirty_without_serializing_it() {
        let mut source = SqliteConnection::establish(":memory:").expect("source");
        source.batch_execute(crate::overlay::DDL).expect("metadata");
        source
            .batch_execute(
                "CREATE TABLE sample_overlay (id INTEGER PRIMARY KEY NOT NULL); \
             INSERT INTO sample_overlay VALUES (1); \
             INSERT INTO _connetto_overlays VALUES ('sample', 'backing', 'sample_overlay'); \
             INSERT INTO _connetto_overlay_tags (tbl, key, writer, seq, dirty) \
             VALUES ('sample_overlay', '(\"id\" IS 1)', 'number:1', 1, 1)",
            )
            .expect("source rows");
        let bytes = export_entry(&mut source, "number:1", 1).expect("export");
        import_entry(&mut source, &bytes, "number:2", 2).expect("import");
        assert_eq!(
            _connetto_overlay_tags::table
                .select((
                    _connetto_overlay_tags::writer,
                    _connetto_overlay_tags::seq,
                    _connetto_overlay_tags::dirty
                ))
                .first::<(Option<String>, Option<i64>, bool)>(&mut source)
                .expect("tag"),
            (Some("number:2".into()), Some(2), false)
        );
    }
}
