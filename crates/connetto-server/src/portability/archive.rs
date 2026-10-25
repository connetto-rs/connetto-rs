//! The download's archive: the SQLite file the rows land in, its manifest,
//! and the zip both are streamed in.
//!
//! Synchronous throughout, so it runs on blocking tasks and talks to the
//! async reads over channels.

use std::collections::HashMap;
use std::io::{self, Write};

use connetto_core::quote_ident;
use connetto_core::schema::SchemaBundle;
use diesel::connection::SimpleConnection as _;
use diesel::query_builder::{BoxedSqlQuery, SqlQuery};
use diesel::sql_types::{BigInt, Binary, Double, Integer, Nullable, Text};
use diesel::sqlite::{Sqlite, SqliteConnection};
use diesel::{Connection as _, QueryableByName, RunQueryDsl as _, sql_query};
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use super::{DataRequestError, PortabilityError};

/// One cell as the client replica stores it.
pub(super) type Cell = sqlite_diff_rs::Value<String, Vec<u8>>;

/// The bytes a zip chunk collects before it is handed to the sink.
const CHUNK_BYTES: usize = 64 * 1024;

/// One page of rows for the query at `query`, each row in the archive's
/// column order.
pub(super) struct Batch {
    pub(super) query: usize,
    pub(super) rows: Vec<Vec<Cell>>,
}

/// One archive table: its creation and the insert each row takes.
pub(super) struct TableShape {
    create: String,
    insert: String,
}

/// The client's replica schema, applied to a scratch in-memory database so
/// each archive table keeps the column types and key a replica gives it.
///
/// A replica serves a table under row-level security through a view over a
/// physical table, so the archive reads the physical one, whose columns and
/// key are the application's declared table, and keeps the declared name.
pub(super) struct Replica {
    conn: SqliteConnection,
    physical: HashMap<String, String>,
}

#[derive(QueryableByName)]
struct ColumnInfo {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    declared: String,
    #[diesel(sql_type = Integer)]
    pk: i32,
}

impl Replica {
    /// Apply `bundle`'s replica DDL to a scratch database.
    pub(super) fn open(bundle: &SchemaBundle) -> Result<Self, PortabilityError> {
        let mut conn =
            SqliteConnection::establish(":memory:").map_err(PortabilityError::ReplicaOpen)?;
        conn.batch_execute(bundle.replica_ddl())
            .map_err(PortabilityError::Replica)?;
        Ok(Self {
            conn,
            physical: bundle.policy_tables().iter().cloned().collect(),
        })
    }

    /// The archive table for `table` restricted to `columns`, in their order,
    /// with each column's replica type and the replica's primary key.
    pub(super) fn shape(
        &mut self,
        table: &str,
        columns: &[String],
    ) -> Result<TableShape, PortabilityError> {
        let physical = self.physical.get(table).map_or(table, String::as_str);
        // A pragma table function, which no `table!` schema can name.
        let info: Vec<ColumnInfo> =
            sql_query("SELECT name, type AS declared, pk FROM pragma_table_info(?) ORDER BY cid")
                .bind::<Text, _>(physical)
                .load(&mut self.conn)
                .map_err(PortabilityError::Replica)?;
        let missing = |column: &str| PortabilityError::ReplicaColumn {
            table: table.to_owned(),
            column: column.to_owned(),
        };
        let mut definitions = Vec::with_capacity(columns.len() + 1);
        for column in columns {
            let found = info
                .iter()
                .find(|info| &info.name == column)
                .ok_or_else(|| missing(column))?;
            definitions.push(format!("{} {}", quote_ident(column), found.declared));
        }
        let mut key: Vec<&ColumnInfo> = info.iter().filter(|info| info.pk > 0).collect();
        key.sort_by_key(|info| info.pk);
        if let Some(absent) = key.iter().find(|info| !columns.contains(&info.name)) {
            return Err(missing(&absent.name));
        }
        if !key.is_empty() {
            let names: Vec<String> = key.iter().map(|info| quote_ident(&info.name)).collect();
            definitions.push(format!("PRIMARY KEY ({})", names.join(", ")));
        }
        let names: Vec<String> = columns.iter().map(|column| quote_ident(column)).collect();
        let places = vec!["?"; columns.len()].join(", ");
        Ok(TableShape {
            create: format!(
                "CREATE TABLE {} ({})",
                quote_ident(table),
                definitions.join(", ")
            ),
            insert: format!(
                "INSERT INTO {} ({}) VALUES ({places})",
                quote_ident(table),
                names.join(", ")
            ),
        })
    }
}

/// Create every archive table in a fresh temp file and insert each batch
/// `batches` delivers, until the reads close the channel.
pub(super) fn write_sqlite(
    shapes: &[TableShape],
    mut batches: mpsc::Receiver<Batch>,
) -> Result<NamedTempFile, DataRequestError> {
    let file = NamedTempFile::new().map_err(DataRequestError::Io)?;
    let path = file.path().to_str().ok_or_else(|| {
        DataRequestError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the temp directory's path is not UTF-8",
        ))
    })?;
    let mut conn = SqliteConnection::establish(path).map_err(DataRequestError::ArchiveOpen)?;
    // The archive's tables are the deployment's, known only at run time.
    for shape in shapes {
        sql_query(&shape.create)
            .execute(&mut conn)
            .map_err(DataRequestError::ArchiveWrite)?;
    }
    while let Some(batch) = batches.blocking_recv() {
        let insert = &shapes[batch.query].insert;
        conn.transaction(|c| {
            for row in batch.rows {
                bind_row(sql_query(insert).into_boxed(), row).execute(c)?;
            }
            Ok(())
        })
        .map_err(DataRequestError::ArchiveWrite)?;
    }
    drop(conn);
    Ok(file)
}

/// Bind one row's cells in order, each under its own storage class.
fn bind_row(
    query: BoxedSqlQuery<'_, Sqlite, SqlQuery>,
    row: Vec<Cell>,
) -> BoxedSqlQuery<'_, Sqlite, SqlQuery> {
    row.into_iter().fold(query, |query, cell| match cell {
        Cell::Null => query.bind::<Nullable<Text>, _>(None::<String>),
        Cell::Integer(value) => query.bind::<BigInt, _>(value),
        Cell::Real(value) => query.bind::<Double, _>(value),
        Cell::Text(value) => query.bind::<Text, _>(value),
        Cell::Blob(value) => query.bind::<Binary, _>(value),
    })
}

/// What the archive says about itself.
#[derive(serde::Serialize)]
pub(super) struct Manifest<'a> {
    pub(super) person: &'a str,
    pub(super) deployment: &'a str,
    pub(super) postgres_system_identifier: Option<&'a str>,
    pub(super) generated_at: String,
    pub(super) snapshot: &'a str,
    pub(super) queries: Vec<ManifestQuery<'a>>,
    pub(super) query_list_hash: &'a str,
}

/// One registered query as the manifest names it.
#[derive(serde::Serialize)]
pub(super) struct ManifestQuery<'a> {
    pub(super) sql: &'a str,
    pub(super) table: &'a str,
    pub(super) columns: &'a [String],
    pub(super) description: &'a str,
    pub(super) read_as_owner: bool,
}

/// A writer handing its bytes to the async side in chunks.
struct ChannelWriter {
    chunks: mpsc::Sender<Vec<u8>>,
    buffer: Vec<u8>,
}

impl ChannelWriter {
    fn send(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(CHUNK_BYTES));
        self.chunks
            .blocking_send(chunk)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "the download's sink stopped"))
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() >= CHUNK_BYTES {
            self.send()?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send()
    }
}

/// Stream `data.sqlite` and `manifest.json` as one zip into `chunks`, stored
/// rather than compressed and with zip64 on so no size is too large.
pub(super) fn zip(
    file: &NamedTempFile,
    manifest: &[u8],
    chunks: mpsc::Sender<Vec<u8>>,
) -> Result<(), DataRequestError> {
    let mut zip = ZipWriter::new_stream(ChannelWriter {
        chunks,
        buffer: Vec::with_capacity(CHUNK_BYTES),
    });
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Stored)
        .large_file(true);
    zip.start_file("data.sqlite", options)
        .map_err(DataRequestError::Zip)?;
    let mut data = file.reopen().map_err(DataRequestError::Io)?;
    io::copy(&mut data, &mut zip).map_err(DataRequestError::Io)?;
    zip.start_file("manifest.json", options)
        .map_err(DataRequestError::Zip)?;
    zip.write_all(manifest).map_err(DataRequestError::Io)?;
    let mut writer = zip.finish().map_err(DataRequestError::Zip)?.into_inner();
    writer.flush().map_err(DataRequestError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, QueryableByName)]
    struct Described {
        #[diesel(sql_type = Text)]
        name: String,
        #[diesel(sql_type = Text)]
        declared: String,
        #[diesel(sql_type = Integer)]
        pk: i32,
    }

    fn described(name: &str, declared: &str, pk: i32) -> Described {
        Described {
            name: name.to_owned(),
            declared: declared.to_owned(),
            pk,
        }
    }

    /// An archive table holds the queried columns in the query's order, each
    /// with the replica's type, and keeps the replica's composite key, read
    /// from the physical table a policy view stands over.
    #[test]
    fn an_archive_table_keeps_the_queried_columns_their_types_and_the_key() {
        let bundle = connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            "-- connetto:major 1\nCREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, c BYTEA, \
             d DOUBLE PRECISION, PRIMARY KEY (b, a));",
            "",
        )
        .expect("the schema translates");
        assert!(
            bundle
                .policy_tables()
                .iter()
                .any(|(logical, _)| logical == "t"),
            "the bot list cap puts t behind a view: {:?}",
            bundle.policy_tables()
        );
        let mut replica = Replica::open(&bundle).expect("the replica schema applies");
        let shape = replica
            .shape("t", &["d".to_owned(), "a".to_owned(), "b".to_owned()])
            .expect("the columns carry the key");
        let mut archive = SqliteConnection::establish(":memory:").expect("a scratch archive");
        sql_query(&shape.create)
            .execute(&mut archive)
            .expect("the archive table creates");
        let columns: Vec<Described> =
            sql_query("SELECT name, type AS declared, pk FROM pragma_table_info('t') ORDER BY cid")
                .load(&mut archive)
                .expect("the archive table reads");
        assert_eq!(
            columns,
            vec![
                described("d", "REAL", 0),
                described("a", "INTEGER", 2),
                described("b", "TEXT", 1),
            ]
        );

        let refused = replica.shape("t", &["a".to_owned(), "c".to_owned()]);
        assert!(
            matches!(&refused, Err(PortabilityError::ReplicaColumn { column, .. }) if column == "b"),
            "a list leaving out a key column has no archive table"
        );
    }
}
