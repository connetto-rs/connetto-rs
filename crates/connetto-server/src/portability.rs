//! A person's data requests (R61): who asks, the queries a download runs, the
//! archive it writes, and the records a download and an erasure leave in
//! `auth_events`.
//!
//! Both records are written and awaited, never fired and forgotten, because
//! they exist for disputes, so a request whose record cannot be written fails.

mod archive;

use std::collections::HashSet;
use std::sync::Arc;

use crate::audit::{AuditWriteError, AuthEvent, AuthOp};
use crate::authn::service::AuthError;
use crate::builder::BuildError;
use crate::capability::CallerBinding;
use crate::key_filter::{KeyError, KeyFilter};
use crate::materializer::{CallerMappings, MaterializerError, translate_select, wire_binds};
use crate::snapshot::{BinaryRows, attach_binds, page_sql};
use crate::throttle::ReadLimits;
use connetto_core::SessionId;
use connetto_core::auth::{AuthContext, DEFAULT_USER_SETTING, Principal, Subject, VerifiedSession};
use connetto_core::messages::BindValue;
use diesel::QueryableByName;
use diesel::sql_types::{Bool, Text, Timestamptz};
use diesel_async::pooled_connection::bb8::{Pool, RunError};
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _};
use sqlite_diff_rs::PatchsetOp;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::{ColumnId, ParserDB, QueryProjection, TableId, catalog_helpers};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tokio::sync::mpsc;

use archive::{Batch, Manifest, ManifestQuery, TableShape};

/// Rows one page of a download reads, the last one telling whether another
/// page follows.
const PAGE_ROWS: u32 = 1024;

/// Who asks for a person's data request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataRequester {
    /// A signed-in session, which a download requires to be live.
    Session(SessionId),
    /// Staff acting with no session behind them, as the application names
    /// them, for example `staff:marco` or a ticket id.
    Operator(String),
}

impl DataRequester {
    /// The audit event this requester leaves for `person`.
    pub(crate) fn event<Id>(&self, person: Id, op: AuthOp) -> AuthEvent<Id> {
        match self {
            Self::Session(session) => AuthEvent::new(*session, Some(person), op),
            Self::Operator(acted_by) => AuthEvent::by_operator(Some(person), op, acted_by.clone()),
        }
    }
}

/// One value a registered query binds to a `?` placeholder.
#[derive(Debug, Clone, PartialEq)]
pub enum PortabilityBind {
    /// The person the download is for, bound as text, the rendering the
    /// identity setting carries.
    Person,
    /// A value fixed at registration.
    Value(BindValue),
}

/// One query a download runs, in the form a subscription takes: SQLite
/// dialect with `?` placeholders, one table, whole rows or a column list
/// carrying the primary key.
#[derive(Debug, Clone)]
pub struct PortabilityQuery {
    sql: String,
    description: String,
    binds: Vec<PortabilityBind>,
    read_as_owner: bool,
}

impl PortabilityQuery {
    /// A query reading as the person, named in the manifest by `description`.
    #[must_use]
    pub fn new(sql: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            description: description.into(),
            binds: Vec::new(),
            read_as_owner: false,
        }
    }

    /// The values bound to the query's placeholders, in placeholder order.
    #[must_use]
    pub fn binds(mut self, binds: Vec<PortabilityBind>) -> Self {
        self.binds = binds;
        self
    }

    /// Read as the owner role, past row-level security, for rows about the
    /// person that their own policies hide from them, such as moderation
    /// notes. The query's own filter is then the only scope, so it must name
    /// the person.
    #[must_use]
    pub fn read_as_owner(mut self) -> Self {
        self.read_as_owner = true;
        self
    }
}

/// The deployment's registered download, the queries every archive answers.
#[derive(Debug, Clone)]
pub struct PortabilityList {
    deployment: String,
    queries: Vec<PortabilityQuery>,
}

impl PortabilityList {
    /// An empty list for `deployment`, the name the manifest carries.
    #[must_use]
    pub fn new(deployment: impl Into<String>) -> Self {
        Self {
            deployment: deployment.into(),
            queries: Vec::new(),
        }
    }

    /// Add one query, at most one per table.
    #[must_use]
    pub fn query(mut self, query: PortabilityQuery) -> Self {
        self.queries.push(query);
        self
    }
}

/// What a finished download wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadReceipt {
    /// Bytes the sink received.
    pub archive_bytes: u64,
    /// The hex BLAKE3 hash naming the query list the archive answers.
    pub query_list_hash: String,
}

/// A registered query the build refused.
#[derive(Debug, thiserror::Error)]
pub enum PortabilityError {
    /// The query does not reverse translate.
    #[error("the query does not translate")]
    Translate(#[source] MaterializerError),
    /// The query does not compile against the catalog.
    #[error("the query does not compile")]
    Compile(#[source] subql::RegisterError),
    /// The query reads neither whole rows nor a column list.
    #[error("the query reads neither whole rows nor a column list")]
    Projection,
    /// The table has no primary key, or the column list leaves it out.
    #[error("the query does not carry the primary key of table {table}")]
    NoKey {
        /// The table read.
        table: String,
    },
    /// An earlier query already reads the table.
    #[error("table {table} is read by an earlier query")]
    DuplicateTable {
        /// The table read twice.
        table: String,
    },
    /// The catalog cannot describe the table.
    #[error("the catalog cannot describe the query's table")]
    Catalog(#[source] subql::CatalogError),
    /// The catalog names no table or column at an id the query compiled to.
    #[error("the catalog names nothing at table {table_id}")]
    Unnamed {
        /// The table the query compiled to.
        table_id: TableId,
    },
    /// The replica's schema does not hold the table or a column the query reads.
    #[error("the replica schema has no column {column} in table {table}")]
    ReplicaColumn {
        /// The table read.
        table: String,
        /// The column missing from the replica.
        column: String,
    },
    /// The replica's schema could not be read.
    #[error("the replica schema cannot be read")]
    Replica(#[source] diesel::result::Error),
    /// The scratch database the replica schema is read in did not open.
    #[error("the scratch database for the replica schema did not open")]
    ReplicaOpen(#[source] diesel::ConnectionError),
}

/// A data request was refused or could not be answered.
#[derive(Debug, thiserror::Error)]
pub enum DataRequestError {
    /// The requesting session is not live in the auth store.
    #[error("the requesting session is not live")]
    SessionNotLive,
    /// The session's liveness could not be read.
    #[error("the session's liveness could not be read")]
    Liveness(#[source] AuthError),
    /// The deployment registered no download query.
    #[error("the deployment registered no download query")]
    NoQueries,
    /// A row is wider than the identified tier's row ceiling.
    #[error("a row of {query} is {bytes} bytes, over the {ceiling} byte ceiling")]
    RowTooWide {
        /// The description of the query that read it.
        query: String,
        /// The row's width on the wire.
        bytes: u64,
        /// The ceiling it exceeds.
        ceiling: u64,
    },
    /// No connection could be had.
    #[error("no database connection for the download")]
    Connection(#[source] RunError),
    /// A read against Postgres failed.
    #[error("the download's read failed")]
    Read(#[from] diesel::result::Error),
    /// A row read could not be encoded.
    #[error("a row of {query} could not be encoded")]
    Encode {
        /// The description of the query that read it.
        query: String,
        /// The encoder's refusal.
        #[source]
        source: sqlite_diff_rs::wal2json::ConversionError,
    },
    /// A key value read could not resume the next page.
    #[error("a key of {query} could not resume the next page")]
    Resume {
        /// The description of the query that read it.
        query: String,
        /// The key value that did not decode.
        #[source]
        source: subql::ValueError,
    },
    /// The table's key has a type the page filter cannot bind.
    #[error("the key type {kind} of table {table} cannot resume a page")]
    UnsupportedKey {
        /// The table read.
        table: String,
        /// The key column's scalar kind.
        kind: String,
    },
    /// The cluster's recorded identity could not be read.
    #[error("the cluster's identity could not be read")]
    Epoch(#[source] crate::epoch::EpochError),
    /// The archive's SQLite file could not be opened.
    #[error("the archive's SQLite file could not be opened")]
    ArchiveOpen(#[source] diesel::ConnectionError),
    /// A write to the archive's SQLite file failed.
    #[error("the archive's SQLite write failed")]
    ArchiveWrite(#[source] diesel::result::Error),
    /// The archive writer stopped before the reads ended.
    #[error("the archive writer stopped")]
    WriterStopped,
    /// Staging or streaming the archive failed.
    #[error("the archive could not be staged or streamed")]
    Io(#[source] std::io::Error),
    /// The zip could not be written.
    #[error("the zip could not be written")]
    Zip(#[source] zip::result::ZipError),
    /// The manifest could not be rendered.
    #[error("the manifest could not be rendered")]
    Manifest(#[source] serde_json::Error),
    /// A blocking archive task panicked or was cancelled.
    #[error("an archive task did not finish")]
    Task(#[source] tokio::task::JoinError),
    /// The record could not be written.
    #[error(transparent)]
    Audit(#[from] AuditWriteError),
}

impl DataRequestError {
    fn resume(query: &str, error: KeyError) -> Self {
        match error {
            KeyError::Value(source) => Self::Resume {
                query: query.to_owned(),
                source,
            },
            KeyError::Unsupported { table, kind } => Self::UnsupportedKey { table, kind },
        }
    }
}

/// The reader's recovery state could not be read at startup.
#[derive(Debug, thiserror::Error)]
pub enum ReaderCheckError {
    /// No reader connection could be had.
    #[error("no reader connection to check recovery on")]
    Connection(#[source] RunError),
    /// The check failed.
    #[error("the recovery check failed")]
    Query(#[source] diesel::result::Error),
}

/// One registered query, validated against the catalog.
struct ValidQuery {
    sql: String,
    pg_sql: String,
    description: String,
    binds: Vec<PortabilityBind>,
    read_as_owner: bool,
    table: String,
    table_id: TableId,
    /// The archive's columns, in the query's order.
    columns: Vec<String>,
    /// The catalog ordinal of each archive column.
    ordinals: Vec<usize>,
    key: Vec<ColumnId>,
    key_names: Vec<String>,
}

/// `binds` with the person put in.
fn bind_values(binds: &[PortabilityBind], person: &str) -> Vec<BindValue> {
    binds
        .iter()
        .map(|bind| match bind {
            PortabilityBind::Person => BindValue::Text(person.to_owned()),
            PortabilityBind::Value(value) => value.clone(),
        })
        .collect()
}

/// The registered list once the build accepted it, waiting for its pools.
pub(crate) struct ValidatedList {
    deployment: String,
    queries: Vec<ValidQuery>,
    shapes: Arc<[TableShape]>,
    query_list_hash: String,
    catalog: ParserDB,
}

impl ValidatedList {
    /// Whether any query reads as the owner role, which needs the reader on
    /// the primary (decision 12).
    pub(crate) fn reads_as_owner(&self) -> bool {
        self.queries.iter().any(|query| query.read_as_owner)
    }

    /// The download served from `reader` and `owner` under `limits`.
    pub(crate) fn serve(
        self,
        reader: Pool<AsyncPgConnection>,
        owner: Pool<AsyncPgConnection>,
        limits: ReadLimits,
    ) -> PortabilityState {
        PortabilityState {
            list: self,
            reader,
            owner,
            limits,
        }
    }
}

/// Validate `list` against the deployment's catalog, `engine_ddl`, and its
/// client replica schema, `replica`, before anything connects.
///
/// # Errors
///
/// [`BuildError::Portability`] naming the refused query,
/// [`BuildError::PortabilityReplica`] when the replica schema does not apply,
/// and [`MaterializerError::Catalog`] when the catalog does not parse.
pub(crate) fn validate(
    list: PortabilityList,
    engine_ddl: &str,
    replica: &connetto_core::schema::SchemaBundle,
    caller: &CallerMappings,
) -> Result<ValidatedList, BuildError> {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(engine_ddl)
        .map_err(|err| MaterializerError::Catalog(format!("{err:?}")))?;
    let mut replica = archive::Replica::open(replica).map_err(BuildError::PortabilityReplica)?;
    let query_list_hash = query_list_hash(&list.queries);
    let mut tables = HashSet::new();
    let mut queries = Vec::with_capacity(list.queries.len());
    let mut shapes = Vec::with_capacity(list.queries.len());
    for query in list.queries {
        let refused = |source| BuildError::Portability {
            name: query.description.clone(),
            source,
        };
        let valid = validate_query(&catalog, caller, &query).map_err(refused)?;
        if !tables.insert(valid.table.clone()) {
            return Err(refused(PortabilityError::DuplicateTable {
                table: valid.table,
            }));
        }
        shapes.push(
            replica
                .shape(&valid.table, &valid.columns)
                .map_err(refused)?,
        );
        queries.push(valid);
    }
    Ok(ValidatedList {
        deployment: list.deployment,
        queries,
        shapes: shapes.into(),
        query_list_hash,
        catalog,
    })
}

fn validate_query(
    catalog: &ParserDB,
    caller: &CallerMappings,
    query: &PortabilityQuery,
) -> Result<ValidQuery, PortabilityError> {
    let pg_sql = translate_select(catalog, Some(caller), &query.sql)
        .map_err(PortabilityError::Translate)?
        .to_string();
    // Stand-ins only fix each placeholder's type for the compile, the read binds the real values.
    let standins = wire_binds(&bind_values(&query.binds, ""));
    let compiled = subql::compiler::parser::parse_compile_normalize_and_prefilter_with_binds::<
        Postgres,
        _,
    >(&pg_sql, &PostgreSqlDialect {}, catalog, &standins, None)
    .map_err(PortabilityError::Compile)?;
    let table_id = compiled.table_id;
    let table = catalog_helpers::table_name(catalog, table_id)
        .ok_or(PortabilityError::Unnamed { table_id })?;
    let key = catalog_helpers::primary_key_columns(catalog, table_id)
        .map_err(PortabilityError::Catalog)?;
    if key.is_empty() {
        return Err(PortabilityError::NoKey { table });
    }
    let columns: Vec<ColumnId> = match compiled.projection {
        QueryProjection::Rows => {
            let arity = catalog_helpers::table_arity(catalog, table_id)
                .map_err(PortabilityError::Catalog)?;
            (0..arity)
                .map(|ordinal| {
                    ColumnId::try_from(ordinal).map_err(|_| PortabilityError::Unnamed { table_id })
                })
                .collect::<Result<_, _>>()?
        }
        QueryProjection::Columns {
            columns,
            carries_key: true,
            ..
        } => columns,
        QueryProjection::Columns { .. } => return Err(PortabilityError::NoKey { table }),
        _ => return Err(PortabilityError::Projection),
    };
    let name = |column: ColumnId| {
        catalog_helpers::column_name(catalog, table_id, column)
            .ok_or(PortabilityError::Unnamed { table_id })
    };
    Ok(ValidQuery {
        columns: columns
            .iter()
            .map(|column| name(*column))
            .collect::<Result<_, _>>()?,
        ordinals: columns.iter().map(|column| usize::from(*column)).collect(),
        key_names: key
            .iter()
            .map(|column| name(*column))
            .collect::<Result<_, _>>()?,
        key,
        sql: query.sql.clone(),
        pg_sql,
        description: query.description.clone(),
        binds: query.binds.clone(),
        read_as_owner: query.read_as_owner,
        table,
        table_id,
    })
}

/// The hex BLAKE3 hash of the registered list, in order: each query's SQL,
/// binds, description and owner flag.
fn query_list_hash(queries: &[PortabilityQuery]) -> String {
    #[derive(serde::Serialize)]
    struct Canonical<'a> {
        sql: &'a str,
        binds: Vec<Option<&'a BindValue>>,
        description: &'a str,
        read_as_owner: bool,
    }
    let canonical: Vec<Canonical<'_>> = queries
        .iter()
        .map(|query| Canonical {
            sql: &query.sql,
            // The person is `None`, a value no registered bind can take.
            binds: query
                .binds
                .iter()
                .map(|bind| match bind {
                    PortabilityBind::Person => None,
                    PortabilityBind::Value(value) => Some(value),
                })
                .collect(),
            description: &query.description,
            read_as_owner: query.read_as_owner,
        })
        .collect();
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    blake3::hash(&bytes).to_hex().to_string()
}

/// Refuse an owner query when the reader is a hot standby, whose snapshot the
/// primary cannot import (decision 12).
///
/// # Errors
///
/// [`ReaderCheckError`] when the reader cannot be asked.
pub(crate) async fn reader_in_recovery(
    reader: &Pool<AsyncPgConnection>,
) -> Result<bool, ReaderCheckError> {
    #[derive(QueryableByName)]
    struct Recovery {
        #[diesel(sql_type = Bool)]
        in_recovery: bool,
    }
    let mut conn = reader.get().await.map_err(ReaderCheckError::Connection)?;
    let row: Recovery = diesel::sql_query("SELECT pg_is_in_recovery() AS in_recovery")
        .get_result(&mut conn)
        .await
        .map_err(ReaderCheckError::Query)?;
    Ok(row.in_recovery)
}

/// The download a built server answers.
pub(crate) struct PortabilityState {
    list: ValidatedList,
    reader: Pool<AsyncPgConnection>,
    owner: Pool<AsyncPgConnection>,
    limits: ReadLimits,
}

/// The moment the download's snapshot was taken (decision 7).
#[derive(QueryableByName)]
struct MomentRow {
    #[diesel(sql_type = Timestamptz)]
    generated_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = Text)]
    snapshot: String,
}

#[derive(QueryableByName)]
struct ExportedRow {
    #[diesel(sql_type = Text)]
    snapshot_id: String,
}

impl PortabilityState {
    /// Whether no query is registered.
    pub(crate) fn is_empty(&self) -> bool {
        self.list.queries.is_empty()
    }

    /// The hash naming the registered list.
    pub(crate) fn query_list_hash(&self) -> &str {
        &self.list.query_list_hash
    }

    /// Read every query for `person` under one snapshot, write the archive to
    /// `sink` and return the bytes it received.
    pub(crate) async fn download<W>(
        &self,
        person: &str,
        session: Option<SessionId>,
        sink: &mut W,
    ) -> Result<u64, DataRequestError>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let system = crate::epoch::recorded(&self.owner)
            .await
            .map_err(DataRequestError::Epoch)?;
        let (rows, received) = mpsc::channel(4);
        let shapes = Arc::clone(&self.list.shapes);
        let writer = tokio::task::spawn_blocking(move || archive::write_sqlite(&shapes, received));
        let read = self.read_all(person, session, rows).await;
        // The writer's own failure explains a read that found it gone.
        let file = writer.await.map_err(DataRequestError::Task)??;
        let moment = read?;
        let manifest = Manifest {
            person,
            deployment: &self.list.deployment,
            postgres_system_identifier: system.as_deref(),
            generated_at: moment.generated_at.to_rfc3339(),
            snapshot: &moment.snapshot,
            queries: self
                .list
                .queries
                .iter()
                .map(|query| ManifestQuery {
                    sql: &query.sql,
                    table: &query.table,
                    columns: &query.columns,
                    description: &query.description,
                    read_as_owner: query.read_as_owner,
                })
                .collect(),
            query_list_hash: &self.list.query_list_hash,
        };
        let manifest = serde_json::to_vec_pretty(&manifest).map_err(DataRequestError::Manifest)?;
        let (chunks, mut zipped) = mpsc::channel::<Vec<u8>>(8);
        let packing = tokio::task::spawn_blocking(move || archive::zip(&file, &manifest, chunks));
        let mut bytes = 0_u64;
        while let Some(chunk) = zipped.recv().await {
            sink.write_all(&chunk).await.map_err(DataRequestError::Io)?;
            bytes += chunk.len() as u64;
        }
        packing.await.map_err(DataRequestError::Task)??;
        sink.flush().await.map_err(DataRequestError::Io)?;
        Ok(bytes)
    }

    /// Read every query under one moment, the person's queries on the reader
    /// as the person and the owner queries on the owner pool under the
    /// snapshot the reader exported.
    async fn read_all(
        &self,
        person: &str,
        session: Option<SessionId>,
        rows: mpsc::Sender<Batch>,
    ) -> Result<MomentRow, DataRequestError> {
        let session_id = session.unwrap_or_else(|| SessionId::from_uuid(uuid::Uuid::nil()));
        let mut caller = Principal::<String, String>::unidentified(session_id);
        let accepted = caller.accept(Subject::Identity(VerifiedSession {
            context: AuthContext::new(person),
            session_id,
            bot_list: None,
        }));
        debug_assert!(
            accepted.is_ok(),
            "a fresh principal takes its first identity"
        );
        let binding = CallerBinding::of(&caller, Arc::from(DEFAULT_USER_SETTING));
        let timeout_ms = crate::reexec::statement_timeout_ms(self.limits.timeout);
        let owner_reads = self.list.reads_as_owner();
        let rows = &rows;
        let mut conn = self
            .reader
            .get()
            .await
            .map_err(DataRequestError::Connection)?;
        conn.transaction::<MomentRow, DataRequestError, _>(async move |c| {
            diesel::sql_query("SET TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ")
                .execute(c)
                .await?;
            // The first statement takes the snapshot, so this names exactly it.
            let moment: MomentRow = diesel::sql_query(
                "SELECT now() AS generated_at, pg_current_snapshot()::text AS snapshot",
            )
            .get_result(c)
            .await?;
            diesel::sql_query(format!("SET LOCAL statement_timeout = {timeout_ms}"))
                .execute(c)
                .await?;
            binding.apply(c).await?;
            for (index, query) in self.list.queries.iter().enumerate() {
                if !query.read_as_owner {
                    self.read_query(c, index, person, rows).await?;
                }
            }
            if owner_reads {
                let exported: ExportedRow =
                    diesel::sql_query("SELECT pg_export_snapshot() AS snapshot_id")
                        .get_result(c)
                        .await?;
                self.read_as_owner(&exported.snapshot_id, timeout_ms, person, rows)
                    .await?;
            }
            Ok(moment)
        })
        .await
    }

    /// The owner queries, on an owner connection importing the reader's
    /// snapshot, with no caller bound.
    async fn read_as_owner(
        &self,
        snapshot_id: &str,
        timeout_ms: u32,
        person: &str,
        rows: &mpsc::Sender<Batch>,
    ) -> Result<(), DataRequestError> {
        let mut conn = self
            .owner
            .get()
            .await
            .map_err(DataRequestError::Connection)?;
        // The id is Postgres's own, quoted for the one statement that cannot bind it.
        let import = format!(
            "SET TRANSACTION SNAPSHOT '{}'",
            snapshot_id.replace('\'', "''")
        );
        conn.transaction::<(), DataRequestError, _>(async move |c| {
            diesel::sql_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(c)
                .await?;
            diesel::sql_query(import).execute(c).await?;
            diesel::sql_query(format!("SET LOCAL statement_timeout = {timeout_ms}"))
                .execute(c)
                .await?;
            for (index, query) in self.list.queries.iter().enumerate() {
                if query.read_as_owner {
                    self.read_query(c, index, person, rows).await?;
                }
            }
            Ok(())
        })
        .await
    }

    /// Page through query `index` on the primary key and hand each page's
    /// rows to the archive writer.
    async fn read_query(
        &self,
        conn: &mut AsyncPgConnection,
        index: usize,
        person: &str,
        rows: &mpsc::Sender<Batch>,
    ) -> Result<(), DataRequestError> {
        let query = &self.list.queries[index];
        let catalog = &self.list.catalog;
        let binds = bind_values(&query.binds, person);
        let mut after: Option<Vec<Value<Postgres>>> = None;
        loop {
            let resume = match &after {
                Some(values) => KeyFilter::after(
                    catalog,
                    query.table_id,
                    &query.table,
                    binds.len() + 1,
                    |position, _| Ok(values.get(position).cloned().unwrap_or(Value::Missing)),
                )
                .map_err(|err| DataRequestError::resume(&query.description, err))?,
                None => None,
            };
            let sql = page_sql(
                &query.pg_sql,
                &query.key_names,
                resume.as_ref().map(KeyFilter::predicate),
                PAGE_ROWS + 1,
            );
            // Raw SQL because the query and its table come from the
            // deployment's runtime registration, so no `table!` names them.
            let mut page = attach_binds(
                diesel::sql_query(sql).into_boxed::<diesel::pg::Pg>(),
                binds.clone(),
            );
            if let Some(resume) = &resume {
                page = resume.bind(page);
            }
            let read = BinaryRows::load(conn, page).await?;
            let ceiling = self.limits.row_ceiling;
            if let Some(widest) = read.row_widths().into_iter().max()
                && widest as u64 > ceiling
            {
                return Err(DataRequestError::RowTooWide {
                    query: query.description.clone(),
                    bytes: widest as u64,
                    ceiling,
                });
            }
            let filled = read.row_count() > PAGE_ROWS as usize;
            let mut cells = read.cells();
            cells.truncate(PAGE_ROWS as usize);
            let built = subql::emit::pgbinary_patchset_builder::<Postgres, _>(
                catalog,
                &query.table,
                &read.column_names(),
                &cells,
            )
            .map_err(|source| DataRequestError::Encode {
                query: query.description.clone(),
                source,
            })?;
            let mut batch = Vec::with_capacity(cells.len());
            let mut last = None;
            for op in built.iter() {
                if let PatchsetOp::Insert { values, .. } = op {
                    batch.push(
                        query
                            .ordinals
                            .iter()
                            .map(|ordinal| values[*ordinal].clone())
                            .collect(),
                    );
                    last = Some(values);
                }
            }
            after = match last {
                Some(values) if filled => {
                    let row = crate::pk::row_from_wire(catalog, query.table_id, values);
                    Some(
                        query
                            .key
                            .iter()
                            .map(|column| {
                                row.get(usize::from(*column))
                                    .cloned()
                                    .unwrap_or(Value::Missing)
                            })
                            .collect(),
                    )
                }
                _ => None,
            };
            rows.send(Batch {
                query: index,
                rows: batch,
            })
            .await
            .map_err(|_| DataRequestError::WriterStopped)?;
            if after.is_none() {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notes() -> PortabilityQuery {
        PortabilityQuery::new("SELECT * FROM notes WHERE about = ?", "notes")
            .binds(vec![PortabilityBind::Person])
    }

    /// The hash is what a dispute compares, so it holds still for the same
    /// list and moves with any part of a query that changes what is read.
    #[test]
    fn the_query_list_hash_names_every_part_of_the_list() {
        let base = query_list_hash(&[notes()]);
        assert_eq!(base, query_list_hash(&[notes()]));
        let variants = [
            vec![notes().read_as_owner()],
            vec![
                PortabilityQuery::new("SELECT * FROM notes WHERE about = ?", "other")
                    .binds(vec![PortabilityBind::Person]),
            ],
            vec![notes().binds(vec![PortabilityBind::Value(BindValue::Text(String::new()))])],
            vec![
                PortabilityQuery::new("SELECT * FROM notes WHERE kind = ?", "notes")
                    .binds(vec![PortabilityBind::Person]),
            ],
            vec![
                notes(),
                PortabilityQuery::new("SELECT * FROM records", "records"),
            ],
        ];
        for variant in variants {
            assert_ne!(query_list_hash(&variant), base, "{variant:?}");
        }
    }
}
