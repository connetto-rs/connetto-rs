//! Live queries: run a diesel query once and hold an object that stays fresh.
//!
//! [`ConnettoClient`] wraps a [`ConnettoConnection`] behind a shared async
//! lock and drives it from one background pump task, so applications never
//! hand-write a pump loop. [`ConnettoClient::watch`] takes an ordinary typed
//! diesel query, runs it against the local replica for the immediate answer,
//! renders it to SQLite SQL plus bind values, and registers the matching
//! server subscription. The returned [`LiveQuery`] caches the rows, refreshes
//! them whenever a table the query reads changes (a server patch or a local
//! write alike), and signals each change through an awaitable
//! [`changed`](LiveQuery::changed). Dropping the handle unsubscribes.
//!
//! The pump never parks on the transport while holding the connection lock:
//! it waits with a cancellable pump step
//! ([`ConnettoConnection::pump_one_or`]) that a wake signal interrupts, so
//! creating a live query or running a one-off closure through
//! [`with_conn`](ConnettoClient::with_conn) acquires the lock promptly. The
//! pump task holds only a weak reference to the shared state, so dropping
//! every [`ConnettoClient`] clone ends the pump, closes the transport, and
//! releases the connection: RAII end to end, matching the drop-unsubscribe
//! contract of the handles themselves.

use core::future::Future;
use core::task::Poll;
use core::time::Duration;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, PoisonError, RwLock, RwLockReadGuard, Weak};

use connetto_core::auth::{CapabilityKey, CapabilitySubject};
use connetto_core::messages::{
    AggregateChange, ContentVerb, FatalErrorReason, Grant, SUBSCRIPTION_REFUSED,
    SUBSCRIPTION_REFUSED_SCHEMA, ShareKeyChange, SqliteValue, SubscriptionSpec,
};
use connetto_core::traits::{MaybeSend, Transport};
use connetto_core::{Major, quote_ident};
use diesel::associations::{HasTable, Identifiable};
use diesel::deserialize::{FromSqlRow, StaticallySizedRow};
use diesel::dsl::Find;
use diesel::expression::{QueryMetadata, TypedExpressionType};
use diesel::query_builder::{
    AsChangeset, AsQuery, InsertStatement, IntoUpdateTarget, Query, QueryFragment, UpdateStatement,
};
use diesel::query_dsl::methods::{FindDsl, LoadQuery};
use diesel::query_dsl::{CompatibleType, RunQueryDsl};
use diesel::result::QueryResult;
use diesel::sql_types::SqlType;
use diesel::sqlite::Sqlite;
use diesel::{Insertable, SqliteConnection};
use sqlparser::ast::{
    Expr, GroupByExpr, SelectItem, SetExpr, Statement, TableFactor, visit_relations,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use subql::backend::Value as SubqlValue;
use tokio::sync::{Mutex, Notify, broadcast, watch};

use crate::away::{
    Clock, GateAskFuture, GateAskOutcome, GateController, GateMechanism, GateSink, Moment,
    SystemClock,
};
use crate::dsl::SplitLast;
use crate::reconnect::{NoReconnect, NoSleep, ReconnectPolicy, Sleeper, TransportFactory};
use crate::{
    ClientError, ClientEvent, ConnettoConnection, DrainTimer, KeepaliveTimer, ResendTimer,
    ShareKeyTimer, StorageUse, UnsyncedWork, WriterRef,
};

/// Render a typed diesel query to its SQLite SQL (with `?` placeholders) and
/// the bind values the placeholders stand for, in placeholder order.
///
/// Rendering rides `subql`'s diesel-typed API, the same machinery the
/// engine's own typed registration uses, so the SQL skeleton and bind
/// decoding stay identical on both ends of the wire.
///
/// # Errors
///
/// [`ClientError::Session`] when diesel cannot render the query or a bind
/// uses a type outside the wire's scalar set.
pub fn render_query<Q: QueryFragment<Sqlite>>(
    query: &Q,
) -> Result<(String, Vec<SqliteValue>), ClientError> {
    let (sql, values) = subql::diesel_api::render_typed::<subql::backend::SQLite, Sqlite, _>(query)
        .map_err(|e| ClientError::Session(e.to_string()))?;
    let binds = values
        .into_iter()
        .map(|value| match value {
            SubqlValue::Null => Ok(SqliteValue::Null),
            SubqlValue::Int(i) => Ok(SqliteValue::Integer(i)),
            SubqlValue::Float(f) => Ok(SqliteValue::Real(f)),
            SubqlValue::String(s) => Ok(SqliteValue::Text(s)),
            SubqlValue::Bytes(b) => Ok(SqliteValue::Blob(b)),
            other => Err(ClientError::Session(format!(
                "unsupported bind value shape {other:?}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((sql, binds))
}

/// Which answer path a subscription query rides client-side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryShape {
    /// A row projection: answered from the replica, refreshed on table changes.
    Rows,
    /// A single ungrouped scalar aggregate: answered by server pushes.
    Aggregate,
    /// A grouped aggregate projecting its group columns and one aggregate:
    /// answered by server pushes keyed by group (R84).
    Grouped,
}

/// What the client needs from a rendered subscription query: the tables it
/// reads (for targeted refresh), its shape (for answer-path routing), and
/// whether its select list reads as a keyed map, every item but the last one
/// of its plain `GROUP BY` columns and the last one the value.
struct ParsedSubscription {
    tables: HashSet<String>,
    shape: QueryShape,
    keyed: bool,
}

/// The lowercased names of every table a subscription query reads.
///
/// The same extraction the pump uses to route refreshes, exposed so a relay
/// serving the wire protocol from a worker-held replica can route snapshots
/// and live patches by table.
///
/// # Errors
///
/// [`ClientError::Session`] when the SQL cannot be parsed.
pub fn subscription_tables(sql: &str) -> Result<HashSet<String>, ClientError> {
    Ok(parse_subscription(sql)?.tables)
}

/// Whether a rendered subscription query is answered by server-pushed
/// aggregate frames rather than from the replica: a scalar aggregate or a
/// grouped statistic (R84), whose keyed frames ride the same path.
///
/// The same shape classification the pump uses to route a query to
/// [`ConnettoClient::watch_value`] or [`ConnettoClient::watch_groups`],
/// exposed so a relay serving the wire protocol from a worker-held connection
/// can route a tab `Subscribe` to the aggregate path instead of a row
/// snapshot, matching the direct client.
///
/// This classifies from the rendered SQL by function name, so it recognizes
/// only the built-in scalar aggregate family (`COUNT`, `SUM`, `AVG`, the
/// extremes, and the variance and stddev functions). It cannot recognize a
/// custom aggregate: a text classifier cannot tell a user aggregate
/// `my_agg(x)` from a user scalar function `my_func(x)` without a name
/// registry, which would misclassify the scalar. Custom aggregates are
/// supported only through the typed [`live()`](crate::dsl::Watchable::live)
/// path, which classifies by diesel's `IsAggregate` type marker. A relay
/// forwarding raw tab SQL therefore serves the built-in family only.
///
/// # Errors
///
/// [`ClientError::Session`] when the SQL cannot be parsed.
pub fn subscription_is_aggregate(sql: &str) -> Result<bool, ClientError> {
    Ok(parse_subscription(sql)?.shape != QueryShape::Rows)
}

/// The scalar aggregate functions the server maintains, lowercased.
///
/// This is the built-in family the SQL-text classification recognizes. It is
/// deliberately a fixed set, not a registry of arbitrary user aggregate
/// names: see [`subscription_is_aggregate`] for why a name-based classifier
/// cannot admit custom aggregates without misclassifying scalar functions.
const AGGREGATE_FUNCTIONS: &[&str] = &[
    "avg",
    "count",
    "max",
    "min",
    "stddev_pop",
    "stddev_samp",
    "sum",
    "total",
    "var_pop",
    "var_samp",
];

/// Whether a value watch already knows it faces an aggregate.
#[derive(Clone, Copy)]
enum ShapeSource {
    /// The typed [`live()`](crate::dsl::Watchable::live) dispatch proved the
    /// aggregate shape through diesel's `IsAggregate` marker, so the SQL-text
    /// classification is bypassed. This is the only path that admits a custom
    /// aggregate, whose function name is absent from [`AGGREGATE_FUNCTIONS`].
    Marker,
    /// A dynamic caller with no type-level marker, classified from the
    /// rendered SQL by function name against the built-in family.
    Sql,
}

/// Parse a rendered subscription query into its table set and shape.
fn parse_subscription(sql: &str) -> Result<ParsedSubscription, ClientError> {
    let statements = Parser::parse_sql(&SQLiteDialect {}, sql)
        .map_err(|e| ClientError::Session(e.to_string()))?;
    let mut tables = HashSet::new();
    let _ = visit_relations(&statements, |name| {
        // The ident VALUE, never its Display: a quoted identifier renders
        // with its quote characters, which would never intersect the plain
        // table names the change tracker reports.
        if let Some(ident) = name.0.last().and_then(|part| part.as_ident()) {
            tables.insert(ident.value.to_lowercase());
        }
        core::ops::ControlFlow::<()>::Continue(())
    });
    let (shape, keyed) = match statements.as_slice() {
        [Statement::Query(query)] => query_shape(query),
        _ => (QueryShape::Rows, false),
    };
    Ok(ParsedSubscription {
        tables,
        shape,
        keyed,
    })
}

/// Whether `expr` is a call to one of the built-in aggregate functions.
fn is_aggregate_call(expr: &Expr) -> bool {
    let Expr::Function(function) = expr else {
        return false;
    };
    function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| AGGREGATE_FUNCTIONS.contains(&ident.value.to_lowercase().as_str()))
}

/// The lowercased column name of a plain identifier expression, unqualified
/// or table-qualified, or `None` for anything else.
fn plain_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.to_lowercase()),
        _ => None,
    }
}

/// A query is aggregate-shaped when it projects exactly one ungrouped call to
/// a known scalar aggregate, mirroring the server's classification closely
/// enough to route the client's answer path. It is grouped when a plain-column
/// `GROUP BY` is projected in full alongside exactly one such call, in any
/// order (R84). Anything else, a `GROUP BY` over expressions or one whose
/// columns the projection hides included, stays a row query. The second
/// answer says whether the select list reads as a keyed map: the `GROUP BY`
/// columns in any order and then one item that is not a plain column, the
/// only order a keyed handle splits each answer row by position in.
fn query_shape(query: &sqlparser::ast::Query) -> (QueryShape, bool) {
    let rows = (QueryShape::Rows, false);
    let SetExpr::Select(select) = query.body.as_ref() else {
        return rows;
    };
    let group_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if mods.is_empty() => exprs,
        GroupByExpr::Expressions(..) | GroupByExpr::All(_) => return rows,
    };
    let mut items = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let (SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. }) = item else {
            return rows;
        };
        items.push(expr);
    }
    if group_exprs.is_empty() {
        return match items.as_slice() {
            [expr] if is_aggregate_call(expr) => (QueryShape::Aggregate, false),
            _ => rows,
        };
    }
    let Some(mut wanted) = group_exprs
        .iter()
        .map(plain_column)
        .collect::<Option<Vec<_>>>()
    else {
        return rows;
    };
    wanted.sort_unstable();
    // Grouped: the projection is exactly the group columns plus one
    // aggregate call, in any order.
    let mut aggregates = 0_usize;
    let mut projected: Vec<String> = Vec::new();
    for expr in &items {
        if is_aggregate_call(expr) {
            aggregates += 1;
        } else if let Some(column) = plain_column(expr) {
            projected.push(column);
        } else {
            return rows;
        }
    }
    projected.sort_unstable();
    let shape = if aggregates == 1 && projected == wanted {
        QueryShape::Grouped
    } else {
        QueryShape::Rows
    };
    let keyed = items.split_last().is_some_and(|(last, init)| {
        plain_column(last).is_none()
            && init
                .iter()
                .map(|expr| plain_column(expr))
                .collect::<Option<Vec<_>>>()
                .is_some_and(|mut columns| {
                    columns.sort_unstable();
                    columns == wanted
                })
    });
    (shape, keyed)
}

/// Refuse a scalar value watch whose rendered SQL is not a scalar aggregate,
/// naming the right method, unless a type-level marker already proved the
/// shape.
fn require_scalar_shape(
    shape: ShapeSource,
    parsed: &ParsedSubscription,
) -> Result<(), ClientError> {
    if matches!(shape, ShapeSource::Marker) {
        return Ok(());
    }
    match parsed.shape {
        QueryShape::Aggregate => Ok(()),
        QueryShape::Grouped => Err(ClientError::Session(
            "grouped aggregate: use watch_groups, one scalar cannot carry a keyed result"
                .to_owned(),
        )),
        QueryShape::Rows => Err(ClientError::Session(
            "row query: use watch, a row projection is answered from the replica".to_owned(),
        )),
    }
}

/// Refuse a grouped watch unless the select list reads as a keyed map, and,
/// without a type-level marker, unless the rendered SQL proves the grouped
/// shape. A keyed handle splits each answer row by position, the group
/// columns first and the value last.
fn require_grouped_shape(
    shape: ShapeSource,
    parsed: &ParsedSubscription,
) -> Result<(), ClientError> {
    if matches!(shape, ShapeSource::Sql) {
        match parsed.shape {
            QueryShape::Grouped => {}
            QueryShape::Aggregate => {
                return Err(ClientError::Session(
                    "scalar aggregate: use watch_value, there is no group to key on".to_owned(),
                ));
            }
            QueryShape::Rows => {
                return Err(ClientError::Session(
                    "not a grouped statistic: watch_groups needs a plain-column GROUP BY \
                     projected in full beside exactly one aggregate"
                        .to_owned(),
                ));
            }
        }
    }
    if parsed.keyed {
        Ok(())
    } else {
        Err(ClientError::Session(
            "grouped statistic: select the plain GROUP BY columns first and the aggregate last"
                .to_owned(),
        ))
    }
}

/// The wire subscription specs backing a row handle, by tier. All tables
/// synced: the query itself rides the wire (server-side predicate pushdown).
/// All tables local: no subscription at all. Mixed: one whole-table
/// subscription per synced table, sorted for a deterministic order, so the
/// synced side stays live and covering for the handle's lifetime. The caller
/// maps each spec to a shared, ref-counted wire subscription.
fn wire_subscriptions(
    local: &HashSet<String>,
    tables: &HashSet<String>,
    query_spec: impl FnOnce() -> SubscriptionSpec,
) -> Vec<SubscriptionSpec> {
    let local_count = tables.intersection(local).count();
    if local_count == 0 {
        return vec![query_spec()];
    }
    if local_count == tables.len() {
        return Vec::new();
    }
    let mut synced: Vec<&str> = tables
        .iter()
        .filter(|table| !local.contains(*table))
        .map(String::as_str)
        .collect();
    synced.sort_unstable();
    synced
        .into_iter()
        .map(|table| SubscriptionSpec::new(format!("SELECT * FROM {}", quote_ident(table))))
        .collect()
}

/// The lowercased tables a row subscription's rows come from, its outer
/// query's own `FROM` and `JOIN`s, or `None` for an aggregate, whose answer
/// rests in its own `_connetto_stat_<query_id>` table. A membership term's
/// `IN (SELECT ...)` table is no table of the subscription's rows.
pub(crate) fn row_tables(spec: &SubscriptionSpec) -> Result<Option<HashSet<String>>, ClientError> {
    let sql = inline_binds(&spec.query, &spec.binds)?;
    let parsed = parse_subscription(&sql)?;
    if matches!(parsed.shape, QueryShape::Aggregate | QueryShape::Grouped) {
        return Ok(None);
    }
    let statements = Parser::parse_sql(&SQLiteDialect {}, &sql)
        .map_err(|err| ClientError::Session(err.to_string()))?;
    Ok(Some(match statements.as_slice() {
        [Statement::Query(query)] => match query.body.as_ref() {
            SetExpr::Select(select) => outer_tables(select),
            _ => parsed.tables,
        },
        _ => parsed.tables,
    }))
}

/// The lowercased tables `spec`'s query may read a membership term from
/// (R27). A plain `SELECT` reads a term's table in an `IN (SELECT ...)`, so
/// its own `FROM` and `JOIN`s are left out. Any other query keeps every table
/// it reads, since keeping a membership table too long costs a few rows
/// while dropping one too early empties the term's local answer.
pub(crate) fn membership_tables(spec: &SubscriptionSpec) -> Result<HashSet<String>, ClientError> {
    let sql = inline_binds(&spec.query, &spec.binds)?;
    let mut tables = parse_subscription(&sql)?.tables;
    let statements = Parser::parse_sql(&SQLiteDialect {}, &sql)
        .map_err(|err| ClientError::Session(err.to_string()))?;
    if let [Statement::Query(query)] = statements.as_slice()
        && let SetExpr::Select(select) = query.body.as_ref()
    {
        let outer = outer_tables(select);
        tables.retain(|table| !outer.contains(table));
    }
    Ok(tables)
}

/// The base tables named in a query's own `FROM` and `JOIN`s, excluding any
/// relation that appears only inside a subquery. A membership term's `IN
/// (SELECT ... FROM member_table ...)` names `member_table`, which belongs to
/// refresh routing but not to the set whose row departures this subscription
/// answers for.
fn outer_tables(select: &sqlparser::ast::Select) -> HashSet<String> {
    let mut tables = HashSet::new();
    for from in &select.from {
        collect_relation(&from.relation, &mut tables);
        for join in &from.joins {
            collect_relation(&join.relation, &mut tables);
        }
    }
    tables
}

/// Insert the lowercased base-table name a [`TableFactor`] names, if it names
/// one. A derived table (a subquery in `FROM`) names none and is skipped.
fn collect_relation(relation: &TableFactor, tables: &mut HashSet<String>) {
    if let TableFactor::Table { name, .. } = relation
        && let Some(ident) = name.0.last().and_then(|part| part.as_ident())
    {
        tables.insert(ident.value.to_lowercase());
    }
}

/// One SQL literal for a wire value, so a primary key can be matched inside a
/// statement that has no bind list.
pub(crate) fn bind_literal_of(
    value: &sqlite_diff_rs::Value<String, Vec<u8>>,
) -> Result<String, ClientError> {
    bind_literal(&match value {
        sqlite_diff_rs::Value::Null => SqliteValue::Null,
        sqlite_diff_rs::Value::Integer(int) => SqliteValue::Integer(*int),
        sqlite_diff_rs::Value::Real(real) => SqliteValue::Real(*real),
        sqlite_diff_rs::Value::Text(text) => SqliteValue::Text(text.clone()),
        sqlite_diff_rs::Value::Blob(blob) => SqliteValue::Blob(blob.clone()),
    })
}

/// One SQL literal for a bound value, for local re-execution of a rendered
/// aggregate. Text doubles its quotes, blobs render as `X'..'` hex.
fn bind_literal(bind: &SqliteValue) -> Result<String, ClientError> {
    Ok(match bind {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(value) => value.to_string(),
        SqliteValue::Real(value) if value.is_finite() => format!("{value:?}"),
        SqliteValue::Real(_) => {
            return Err(ClientError::Session(
                "non-finite float bind has no SQL literal".to_owned(),
            ));
        }
        SqliteValue::Text(value) => format!("'{}'", value.replace('\'', "''")),
        SqliteValue::Blob(bytes) => {
            use core::fmt::Write;
            let mut literal = String::with_capacity(bytes.len() * 2 + 3);
            literal.push_str("X'");
            for byte in bytes {
                let _ = write!(literal, "{byte:02X}");
            }
            literal.push('\'');
            literal
        }
    })
}

/// The rendered SQL with every `?` placeholder replaced by its bind value as
/// a literal, skipping quoted regions. Local re-execution has no bind API for
/// a raw SQL string with a dynamic bind list, and the values are the client's
/// own typed data, escaped per storage class.
fn inline_binds(sql: &str, binds: &[SqliteValue]) -> Result<String, ClientError> {
    let mut out = String::with_capacity(sql.len());
    let mut remaining = binds.iter();
    let mut in_quote: Option<char> = None;
    for c in sql.chars() {
        if let Some(quote) = in_quote {
            out.push(c);
            if c == quote {
                in_quote = None;
            }
        } else {
            match c {
                '\'' | '"' | '`' => {
                    in_quote = Some(c);
                    out.push(c);
                }
                '?' => {
                    let bind = remaining.next().ok_or_else(|| {
                        ClientError::Session("more placeholders than binds".to_owned())
                    })?;
                    out.push_str(&bind_literal(bind)?);
                }
                _ => out.push(c),
            }
        }
    }
    if remaining.next().is_some() {
        return Err(ClientError::Session(
            "more binds than placeholders".to_owned(),
        ));
    }
    Ok(out)
}

/// A type the columns of a computed answer load as, from their SQL type
/// `ST`: a value, a group key or a row.
///
/// Implemented for every type diesel's `load` reads from `ST` on `SQLite`,
/// application types and tuples included, so every computed handle accepts
/// exactly what `load` accepts. Generic code names this one bound instead of
/// diesel's loading bounds.
pub trait Loadable<ST>: Sized + 'static {
    /// How many columns a value reads.
    const WIDTH: usize;

    /// Run `sql`, a complete statement with no bind parameters, and load
    /// every row it returns.
    ///
    /// # Errors
    ///
    /// The diesel error when the statement fails or a row does not load.
    fn load_all(conn: &mut SqliteConnection, sql: &str) -> QueryResult<Vec<Self>>;
}

impl<ST, U> Loadable<ST> for U
where
    ST: SqlType + TypedExpressionType + CompatibleType<U, Sqlite> + 'static,
    Sqlite: QueryMetadata<ST>,
    U: StaticallySizedRow<ST, Sqlite>
        + FromSqlRow<<ST as CompatibleType<U, Sqlite>>::SqlType, Sqlite>
        + 'static,
    <ST as CompatibleType<U, Sqlite>>::SqlType: 'static,
{
    const WIDTH: usize = <U as StaticallySizedRow<ST, Sqlite>>::FIELD_COUNT;

    fn load_all(conn: &mut SqliteConnection, sql: &str) -> QueryResult<Vec<Self>> {
        diesel::dsl::sql::<ST>(sql).load::<U>(conn)
    }
}

/// Run a local scalar aggregate, its binds already inlined, and load its
/// one row with the query's own SQL type `ST`.
fn run_local_scalar<ST, V: Loadable<ST>>(
    conn: &mut SqliteConnection,
    inlined: &str,
) -> Result<V, ClientError> {
    V::load_all(conn, inlined)
        .map_err(|e| ClientError::Session(e.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| ClientError::Session("a scalar aggregate returned no row".to_owned()))
}

/// Subscription ids and the wake signal shared with every [`LiveQuery`], so a
/// synchronous `Drop` can queue its unsubscribe for the async pump, and the
/// fault that stopped the pump, if one did. It also holds the subscriptions
/// the server refused and the wake that ends a handle's wait on one.
struct Reaper {
    pending: StdMutex<Vec<String>>,
    wake: Arc<Notify>,
    stopped: OnceLock<String>,
    /// The handles the server has refused, by sub id. They stay marked until
    /// the server re-serves the sub or the handle drops.
    refused: StdMutex<HashSet<String>>,
    /// Wakes every handle waiting in `changed` so one that was refused returns
    /// the refusal instead of outliving its subscription.
    refuse_wake: Arc<Notify>,
}

impl Reaper {
    /// Record that the server refused one handle's subscription, waking the
    /// handles that wait in `changed`.
    fn record_refusal(&self, sub_id: &str) {
        let mut refused = match self.refused.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if refused.insert(sub_id.to_owned()) {
            self.refuse_wake.notify_waiters();
        }
    }

    /// Let a dropped handle take its refusal mark with it.
    fn forget_refusal(&self, sub_id: &str) {
        let mut refused = match self.refused.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        refused.remove(sub_id);
    }

    /// Whether the server refused this handle's subscription.
    fn refusal(&self, sub_id: &str) -> bool {
        match self.refused.lock() {
            Ok(guard) => guard.contains(sub_id),
            Err(poisoned) => poisoned.into_inner().contains(sub_id),
        }
    }
}

/// What [`LiveQuery`] and [`LiveValue`] both are underneath: a subscription
/// id, the wake signal its refreshes arrive on, and the queue that unsubscribes
/// it when the handle goes.
///
/// Dropping this is what queues the unsubscribe, so it is the last field of
/// each handle rather than a duplicated `Drop` on each.
struct LiveHandleCore {
    sub_id: String,
    changed: watch::Receiver<u64>,
    reaper: Arc<Reaper>,
}

impl LiveHandleCore {
    fn new(sub_id: String, changed: watch::Receiver<u64>, reaper: &Arc<Reaper>) -> Self {
        Self {
            sub_id,
            changed,
            reaper: Arc::clone(reaper),
        }
    }

    fn sub_id(&self) -> &str {
        &self.sub_id
    }

    async fn changed(&mut self) -> Result<(), ClientError> {
        if let Some(detail) = self.reaper.stopped.get() {
            return Err(ClientError::Stopped(detail.clone()));
        }
        loop {
            if self.reaper.refusal(&self.sub_id) {
                return Err(ClientError::SubscriptionRefused);
            }
            let refuse_wake = self.reaper.refuse_wake.notified();
            let res = tokio::select! {
                res = self.changed.changed() => res,
                () = refuse_wake => {
                    if self.reaper.refusal(&self.sub_id) {
                        return Err(ClientError::SubscriptionRefused);
                    }
                    // Another handle took the wake, and this one was not refused.
                    continue;
                }
            };
            if self.reaper.refusal(&self.sub_id) {
                return Err(ClientError::SubscriptionRefused);
            }
            return res.map_err(|_| {
                self.reaper.stopped.get().map_or_else(
                    || ClientError::Transport("live query driver stopped".to_owned()),
                    |detail| ClientError::Stopped(detail.clone()),
                )
            });
        }
    }
}

impl Drop for LiveHandleCore {
    fn drop(&mut self) {
        let sub_id = core::mem::take(&mut self.sub_id);
        self.reaper.forget_refusal(&sub_id);
        let mut pending = match self.reaper.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        pending.push(sub_id);
        drop(pending);
        self.reaper.wake.notify_one();
    }
}

/// Driver-side refresh callback of one live query: re-run the captured query
/// against the shared connection and publish fresh rows.
type Refresh<T> = Box<dyn FnMut(&mut ConnettoConnection<T>) -> Result<(), ClientError> + Send>;

/// One live handle's driver-side state: which tables it reads, how to re-run
/// its query and publish fresh results, and the ids of the shared wire
/// subscriptions backing it (empty for a handle served purely from the local
/// tier, one carrying the query itself for a synced query, one whole-table
/// subscription per synced table for a mixed-tier query).
struct LiveEntry<T: Transport> {
    sub_id: String,
    tables: HashSet<String>,
    refresh: Refresh<T>,
    wire_ids: Vec<String>,
}

/// A computed answer and when it was last synced, the pair every computed
/// handle holds: each decoded row under the key it rests under, in key order,
/// which is answer order for a whole answer. The as-of time is the newest
/// stored `updated_at` on the local clock, so the application judges
/// staleness by its age plus the connection-state events it already
/// receives (R83 decision 6).
struct Answer<X> {
    rows: BTreeMap<Vec<u8>, X>,
    as_of: Option<i64>,
}

impl<X> Default for Answer<X> {
    fn default() -> Self {
        Self {
            rows: BTreeMap::new(),
            as_of: None,
        }
    }
}

/// Read a computed handle's answer, through a poisoned lock too.
fn read_answer<X>(lock: &RwLock<Answer<X>>) -> RwLockReadGuard<'_, Answer<X>> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

/// Reads the rested rows of the statistic for a query under its binds, every
/// row or only the one under a key, loaded as `X`.
type LoadAnswer<X> = fn(
    &mut SqliteConnection,
    &str,
    &[SqliteValue],
    Option<&[u8]>,
) -> Result<Vec<crate::aggregates::RestedRow<X>>, ClientError>;

/// Load every column of each rested row as one `X`, a value or a row.
fn load_whole<ST, X: Loadable<ST>>(
    conn: &mut SqliteConnection,
    query: &str,
    binds: &[SqliteValue],
    key: Option<&[u8]>,
) -> Result<Vec<crate::aggregates::RestedRow<X>>, ClientError> {
    crate::aggregates::load::<ST, X>(conn, query, binds, key, crate::aggregates::Cells::All)
}

/// Load each rested row of a grouped statistic as its key `K`, from every
/// column but the last, and its aggregate `V`, from the last.
fn load_groups<KS, VS, K: Loadable<KS>, V: Loadable<VS>>(
    conn: &mut SqliteConnection,
    query: &str,
    binds: &[SqliteValue],
    key: Option<&[u8]>,
) -> Result<Vec<crate::aggregates::RestedRow<(K, V)>>, ClientError> {
    let keys =
        crate::aggregates::load::<KS, K>(conn, query, binds, key, crate::aggregates::Cells::Init)?;
    let values =
        crate::aggregates::load::<VS, V>(conn, query, binds, key, crate::aggregates::Cells::Last)?;
    Ok(keys
        .into_iter()
        .zip(values)
        .map(|((stored, group, at), (_, value, _))| (stored, (group, value), at))
        .collect())
}

/// Build the apply callback of a computed handle whose answer rows `load`
/// reads as `X`, from the statistic of `query` under `binds`.
///
/// A moved row is re-read alone and set or removed under its key, so a push
/// decodes only the group it names. A whole move re-reads every row. An
/// emptied answer rests no row to date itself by, so the last data's as-of
/// stands. Only a changed answer wakes the handle.
fn answer_apply<X>(
    load: LoadAnswer<X>,
    query: String,
    binds: Vec<SqliteValue>,
    state: Arc<RwLock<Answer<X>>>,
    changed: watch::Sender<u64>,
) -> ApplyComputed
where
    X: PartialEq + Send + Sync + 'static,
{
    Box::new(move |conn, moved| {
        let fresh = match moved {
            Moved::All => load(conn, &query, &binds, None)?,
            Moved::Row(key) => load(conn, &query, &binds, Some(key))?,
        };
        let mut slot = state.write().unwrap_or_else(PoisonError::into_inner);
        let moved_any = match moved {
            Moved::All => {
                if let Some(as_of) = fresh.iter().map(|(_, _, at)| *at).max() {
                    slot.as_of = Some(as_of);
                }
                let rows: BTreeMap<Vec<u8>, X> =
                    fresh.into_iter().map(|(key, row, _)| (key, row)).collect();
                let moved_any = slot.rows != rows;
                slot.rows = rows;
                moved_any
            }
            Moved::Row(key) => match fresh.into_iter().next() {
                Some((key, row, at)) => {
                    slot.as_of = Some(at);
                    let moved_any = slot.rows.get(&key) != Some(&row);
                    slot.rows.insert(key, row);
                    moved_any
                }
                None => slot.rows.remove(key).is_some(),
            },
        };
        drop(slot);
        if moved_any {
            changed.send_modify(|generation| *generation += 1);
        }
        Ok(())
    })
}

/// The key a scalar answer's one row rests under, position zero of a whole
/// answer, which a local scalar also files its value under.
const SCALAR_KEY: [u8; 8] = 0_u64.to_be_bytes();

/// How a computed watch treats a query over local tables only, for an
/// answer whose rows are `X`.
enum ComputedTier<X> {
    /// A scalar, answered by exact local re-execution on change, which runs
    /// the inlined statement and loads its one row.
    Scalar(fn(&mut SqliteConnection, &str) -> Result<X, ClientError>),
    /// Refused, since a complete local tier answers the shape through
    /// `watch`. `kind` names the query in the mixed-tier refusal, `local` is
    /// the refusal of a query over local tables only.
    Refused {
        kind: &'static str,
        local: &'static str,
    },
}

/// What moved in a computed answer: the one row stored under a key, which a
/// keyed delta writes or removes, or the whole answer, which a replacement,
/// a resync, a column declaration or a gate opening rebuilds.
#[derive(Clone, Copy)]
enum Moved<'a> {
    /// The row stored under this key.
    Row(&'a [u8]),
    /// Every row.
    All,
}

impl<'a> Moved<'a> {
    /// What one answer movement touched.
    fn of(change: &'a AggregateChange) -> Self {
        match change {
            AggregateChange::Upsert { key, .. } | AggregateChange::Remove { key } => Self::Row(key),
            AggregateChange::Replace { .. } => Self::All,
        }
    }
}

/// Driver-side apply callback of one computed handle: re-read what moved from
/// the statistic's resting table, decode it through diesel and publish it.
/// The resting table is the one home for the last answer, so a live push and
/// a bootstrap take the same path and a handle never sees which tier served
/// it (R84, decision 4). A decode failure leaves the handle as it was.
type ApplyComputed =
    Box<dyn FnMut(&mut SqliteConnection, Moved<'_>) -> Result<(), ClientError> + Send>;

/// One computed handle's driver-side state (a scalar value, a keyed map or
/// a row answer), with the id of the shared wire subscription that feeds it.
struct ComputedEntry {
    sub_id: String,
    wire_id: String,
    apply: ApplyComputed,
}

/// One unsynced handle's driver-side state: the snapshot it reads and the
/// signal that wakes it.
struct UnsyncedEntry {
    sub_id: String,
    work: Arc<RwLock<UnsyncedWork>>,
    changed: watch::Sender<u64>,
}

/// One shared wire subscription: declared once on the wire under `wire_id`,
/// reference counted across every row and computed handle that resolved to
/// the same `spec`. The last computed answer rests in the statistic's
/// resting table (R83), so a late joiner and a restart both bootstrap
/// through those rows.
struct WireSub {
    wire_id: String,
    spec: SubscriptionSpec,
    refs: usize,
}

/// The connection and the live registries, guarded together so a refresh
/// always sees the replica state the pump just produced. `wire` is the
/// ref-counted layer beneath the handles: identical queries share one entry,
/// so the client opens one wire subscription per distinct query.
struct State<T: Transport> {
    conn: ConnettoConnection<T>,
    registry: Vec<LiveEntry<T>>,
    computed: Vec<ComputedEntry>,
    wire: Vec<WireSub>,
    /// The wire subs whose computed frames a locked gate held, each rebuilt
    /// whole once the gate opens.
    held: Vec<String>,
    /// The handles following this device's unsynced work.
    unsynced: Vec<UnsyncedEntry>,
}

/// The pump's exit signal. The flag and the wake form a double-check, so a
/// waiter that registers after the pump fired the wake still sees the flag
/// and returns, and a waiter that registered first is woken by the fire.
struct PumpDone {
    done: AtomicBool,
    wake: Notify,
}

/// Everything the client handles and the pump task share.
struct Shared<T: Transport> {
    state: Mutex<State<T>>,
    wake: Arc<Notify>,
    reaper: Arc<Reaper>,
    events: broadcast::Sender<ClientEvent>,
    next_live: AtomicU64,
    // Distinct id spaces: handle ids (`live-N`) and shared wire ids (`wire-N`).
    next_wire: AtomicU64,
    /// Whether the replica has ever received data from a server, shared with
    /// every live handle. Updated where the cursor is observed rather than
    /// where queries refresh, so it holds for a first sync that delivers no
    /// rows and therefore refreshes nothing.
    ever_synced: Arc<AtomicBool>,
    /// The gate's re-check state machine, whose sink emits its transitions
    /// into the event stream. Outside [`State`] so the pump can drive the
    /// prompt without holding the state lock.
    gate: GateController,
    /// Raised by [`ConnettoClient::close`], read by the pump at the top of
    /// its loop to end without a redial.
    close_requested: Arc<AtomicBool>,
    /// The pump's exit signal, the double-check half of
    /// [`ConnettoClient::close`].
    pump_done: PumpDone,
    /// The watch grace every watcher defaults to, cached from the config at
    /// build so the default site and the pump share one value.
    grace: Duration,
}

impl<T: Transport> Shared<T> {
    /// Take the state lock, interrupting the pump's idle wait.
    ///
    /// Queue first, then wake: the fair mutex hands the pump's released lock
    /// to an already queued caller before the pump can take it back. Waking
    /// before queueing races instead: the pump can consume the wake, finish
    /// its cycle, re-acquire the lock, and park on a fresh wake future all
    /// inside the caller's descheduling window, after which the caller queues
    /// on a lock the idle pump holds forever. CPU-starved CI hit exactly that.
    async fn lock_interrupting(&self) -> tokio::sync::MutexGuard<'_, State<T>> {
        let mut lock = core::pin::pin!(self.state.lock());
        let first = core::future::poll_fn(|cx| Poll::Ready(lock.as_mut().poll(cx))).await;
        match first {
            Poll::Ready(guard) => guard,
            Poll::Pending => {
                self.wake.notify_one();
                lock.await
            }
        }
    }
}

/// The shared surface of every live handle: a current snapshot, an awaitable
/// change signal, the backing subscription id, and drop-unsubscribe. Framework
/// adapters build on this so a single hook serves row and scalar handles
/// alike.
pub trait LiveHandle {
    /// The snapshot type: `Vec<R>` for a row query, `Option<V>` for a scalar.
    type Snapshot;

    /// The current snapshot.
    fn snapshot(&self) -> Self::Snapshot;

    /// Wait until the snapshot changes. Resolves once per real change,
    /// coalescing bursts.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] when the driving [`ConnettoClient`] is gone.
    fn changed(&mut self) -> impl core::future::Future<Output = Result<(), ClientError>> + Send;

    /// The subscription id backing this handle.
    fn sub_id(&self) -> &str;
}

/// A typed diesel query kept fresh by the client's pump.
///
/// Read the current rows with [`rows`](Self::rows) (a cheap clone of the
/// driver-maintained cache) and await [`changed`](Self::changed) to learn when
/// they moved. Dropping the handle queues the server unsubscribe; the pump
/// sends it on its next step.
pub struct LiveQuery<R> {
    handle: LiveHandleCore,
    rows: Arc<RwLock<Vec<R>>>,
    /// Whether this query reads any synced table. False for a query over
    /// device-private tables alone, whose rows never depended on a server.
    reads_synced: bool,
    ever_synced: Arc<AtomicBool>,
}

impl<R: Clone + Send + Sync> LiveHandle for LiveQuery<R> {
    type Snapshot = Vec<R>;

    fn snapshot(&self) -> Vec<R> {
        self.rows()
    }

    fn changed(&mut self) -> impl core::future::Future<Output = Result<(), ClientError>> + Send {
        LiveQuery::changed(self)
    }

    fn sub_id(&self) -> &str {
        LiveQuery::sub_id(self)
    }
}

impl<R: Clone> LiveQuery<R> {
    /// The current rows, as of the latest refresh.
    #[must_use]
    pub fn rows(&self) -> Vec<R> {
        self.rows.read().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |rows| rows.clone(),
        )
    }
}

impl<R> LiveQuery<R> {
    /// The subscription id backing this handle.
    #[must_use]
    pub fn sub_id(&self) -> &str {
        self.handle.sub_id()
    }

    /// Whether these rows come from a replica that has never synced.
    ///
    /// True only while this query reads synced tables and no server has ever
    /// answered. An empty result then means the rows were never fetched, not
    /// that there are none, and the application picks the sentence to show.
    /// Always false for a query over device-private tables alone.
    ///
    /// Turns false for good on the first sync, including one that delivers no
    /// rows, which is what separates a loaded empty set from an unloaded one.
    #[must_use]
    pub fn never_synced(&self) -> bool {
        self.reads_synced && !self.ever_synced.load(Ordering::Relaxed)
    }

    /// Wait until the rows change. Resolves once per refresh that actually
    /// altered the result set, coalescing bursts.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] when the driving [`ConnettoClient`] is gone.
    pub async fn changed(&mut self) -> Result<(), ClientError> {
        self.handle.changed().await
    }
}

/// A server-maintained scalar aggregate kept fresh by the client's pump.
///
/// Created by [`ConnettoClient::watch_value`] for aggregate queries. The
/// replica holds only this client's authorized rows, so it cannot answer a
/// global statistic: the value comes exclusively from server pushes, and
/// [`value`](Self::value) is `None` until the server's bootstrap arrives.
/// Dropping the handle queues the server unsubscribe, exactly like
/// [`LiveQuery`].
pub struct LiveValue<V> {
    handle: LiveHandleCore,
    value: Arc<RwLock<Answer<V>>>,
}

impl<V: Clone + Send + Sync> LiveHandle for LiveValue<V> {
    type Snapshot = Option<V>;

    fn snapshot(&self) -> Option<V> {
        self.value()
    }

    fn changed(&mut self) -> impl core::future::Future<Output = Result<(), ClientError>> + Send {
        LiveValue::changed(self)
    }

    fn sub_id(&self) -> &str {
        LiveValue::sub_id(self)
    }
}

impl<V: Clone> LiveValue<V> {
    /// The current value, or `None` before any value has arrived. On an
    /// offline restart this is the last synced value read from the resting
    /// table, not `None`, which is the whole of R83.
    #[must_use]
    pub fn value(&self) -> Option<V> {
        read_answer(&self.value).rows.values().next().cloned()
    }

    /// When the current value was last synced, in seconds since the epoch on
    /// the local clock, or `None` while no value has arrived. The application
    /// judges staleness from its age together with the connection-state
    /// events it already receives (R83 decision 6).
    #[must_use]
    pub fn as_of_secs(&self) -> Option<i64> {
        read_answer(&self.value).as_of
    }
}

impl<V> LiveValue<V> {
    /// The subscription id backing this handle.
    #[must_use]
    pub fn sub_id(&self) -> &str {
        self.handle.sub_id()
    }

    /// Wait until the value changes, the initial bootstrap included.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] when the driving [`ConnettoClient`] is gone.
    pub async fn changed(&mut self) -> Result<(), ClientError> {
        self.handle.changed().await
    }
}

/// A server-maintained grouped aggregate kept fresh by the client's pump, one
/// entry per group (R84).
///
/// Created by [`ConnettoClient::watch_groups`] for grouped aggregate queries.
/// The replica holds only this client's authorized rows, so it cannot answer
/// a global statistic: the map comes exclusively from server pushes, resting
/// in the statistic's resting table (R83), so an offline restart shows the
/// last synced map. A fold delta upserts one entry, a group's departure
/// removes one, and a server-side demotion to whole answers rebuilds the map
/// without surfacing as a distinct state. Dropping the handle queues the
/// server unsubscribe, exactly like [`LiveQuery`].
pub struct LiveGroups<K, V> {
    handle: LiveHandleCore,
    state: Arc<RwLock<Answer<(K, V)>>>,
}

impl<K, V> LiveHandle for LiveGroups<K, V>
where
    K: Eq + core::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    type Snapshot = HashMap<K, V>;

    fn snapshot(&self) -> HashMap<K, V> {
        self.map()
    }

    fn changed(&mut self) -> impl core::future::Future<Output = Result<(), ClientError>> + Send {
        LiveGroups::changed(self)
    }

    fn sub_id(&self) -> &str {
        LiveGroups::sub_id(self)
    }
}

impl<K: Eq + core::hash::Hash + Clone, V: Clone> LiveGroups<K, V> {
    /// The current map, one entry per group. Empty before any value has
    /// arrived. On an offline restart this is the last synced map read from
    /// the resting table, not empty, which is R83 extended to groups.
    #[must_use]
    pub fn map(&self) -> HashMap<K, V> {
        read_answer(&self.state).rows.values().cloned().collect()
    }

    /// When the map was last synced, in seconds since the epoch on the local
    /// clock, or `None` while nothing has arrived. The application judges
    /// staleness from its age together with the connection-state events it
    /// already receives (R83 decision 6).
    #[must_use]
    pub fn as_of_secs(&self) -> Option<i64> {
        read_answer(&self.state).as_of
    }
}

impl<K, V> LiveGroups<K, V> {
    /// The subscription id backing this handle.
    #[must_use]
    pub fn sub_id(&self) -> &str {
        self.handle.sub_id()
    }

    /// Wait until the map changes, the initial seed included.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] when the driving [`ConnettoClient`] is gone.
    pub async fn changed(&mut self) -> Result<(), ClientError> {
        self.handle.changed().await
    }
}

/// A server-computed row answer kept fresh by the client's pump (R84).
///
/// Created by [`ConnettoClient::watch_rows`] for row-shaped queries the
/// server re-executes rather than syncing (joins, `DISTINCT`, expression
/// projections). A whole answer replaces the rows, and a keyed computed row
/// moves alone. Each answer rests in the statistic's resting table (R83), so
/// an offline restart shows the last synced rows. Dropping the handle queues
/// the server unsubscribe, exactly like [`LiveQuery`].
pub struct LiveRows<R> {
    handle: LiveHandleCore,
    state: Arc<RwLock<Answer<R>>>,
}

impl<R: Clone + Send + Sync> LiveHandle for LiveRows<R> {
    type Snapshot = Vec<R>;

    fn snapshot(&self) -> Vec<R> {
        self.rows()
    }

    fn changed(&mut self) -> impl core::future::Future<Output = Result<(), ClientError>> + Send {
        LiveRows::changed(self)
    }

    fn sub_id(&self) -> &str {
        LiveRows::sub_id(self)
    }
}

impl<R: Clone> LiveRows<R> {
    /// The current answer, in the order the server produced it, or in key
    /// order for keyed computed rows. Empty before any answer has arrived. On
    /// an offline restart this is the last synced answer read from the
    /// resting table, not empty.
    #[must_use]
    pub fn rows(&self) -> Vec<R> {
        read_answer(&self.state).rows.values().cloned().collect()
    }

    /// When the answer was last synced, in seconds since the epoch on the
    /// local clock, or `None` while nothing has arrived (R83 decision 6).
    #[must_use]
    pub fn as_of_secs(&self) -> Option<i64> {
        read_answer(&self.state).as_of
    }
}

impl<R> LiveRows<R> {
    /// The subscription id backing this handle.
    #[must_use]
    pub fn sub_id(&self) -> &str {
        self.handle.sub_id()
    }

    /// Wait until the answer changes, the initial one included.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] when the driving [`ConnettoClient`] is gone.
    pub async fn changed(&mut self) -> Result<(), ClientError> {
        self.handle.changed().await
    }
}

/// This device's unsynced work kept fresh by the client's pump: every write
/// and staged file that has not reached the server, each with where it
/// stands, and every refused write the application has not dismissed.
///
/// Created by [`ConnettoClient::unsynced_live`]. The snapshot is rebuilt at
/// the pump step after a write, a send, a server answer or a dismissal, and
/// held while the gate is locked, like every live handle. Dropping the
/// handle releases nothing on the server.
pub struct LiveUnsynced {
    handle: LiveHandleCore,
    work: Arc<RwLock<UnsyncedWork>>,
}

impl LiveHandle for LiveUnsynced {
    type Snapshot = UnsyncedWork;

    fn snapshot(&self) -> UnsyncedWork {
        self.work()
    }

    fn changed(&mut self) -> impl core::future::Future<Output = Result<(), ClientError>> + Send {
        LiveUnsynced::changed(self)
    }

    fn sub_id(&self) -> &str {
        LiveUnsynced::sub_id(self)
    }
}

impl LiveUnsynced {
    /// The current snapshot, as of the latest rebuild.
    #[must_use]
    pub fn work(&self) -> UnsyncedWork {
        self.work.read().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |work| work.clone(),
        )
    }

    /// The id backing this handle.
    #[must_use]
    pub fn sub_id(&self) -> &str {
        self.handle.sub_id()
    }

    /// Wait until the snapshot changes. Resolves once per rebuild that
    /// altered it, coalescing bursts.
    ///
    /// # Errors
    ///
    /// [`ClientError::Stopped`] or [`ClientError::Transport`] when the
    /// driving [`ConnettoClient`] is gone.
    pub async fn changed(&mut self) -> Result<(), ClientError> {
        self.handle.changed().await
    }
}

/// Liveness token every [`ConnettoClient`] clone holds. Dropping the last one
/// wakes the pump, which then closes the connection gracefully and exits.
struct ClientToken {
    wake: Arc<Notify>,
}

impl Drop for ClientToken {
    fn drop(&mut self) {
        self.wake.notify_one();
    }
}

/// The gate's sink into the client's own event stream.
struct ClientGateSink {
    events: broadcast::Sender<ClientEvent>,
}

impl GateSink for ClientGateSink {
    fn locked(&self) {
        let _ = self.events.send(ClientEvent::Locked);
    }

    fn unlocked(&self) {
        let _ = self.events.send(ClientEvent::Unlocked);
    }

    fn unlock_dismissed(&self) {
        let _ = self.events.send(ClientEvent::UnlockDismissed);
    }
}

/// A shared, background-driven connetto client.
///
/// Wraps a [`ConnettoConnection`] and owns its pump: applications create live
/// queries with [`watch`](Self::watch), run one-off reads and writes with
/// [`with_conn`](Self::with_conn), and observe the raw event stream with
/// [`events`](Self::events). Clones share the one connection. When the last
/// clone drops, the pump closes the connection cleanly (a proper transport
/// close handshake) and ends. A [`LiveQuery`] outliving every client clone
/// keeps its last rows, and its `changed()` reports the driver as stopped.
pub struct ConnettoClient<T: Transport> {
    shared: Arc<Shared<T>>,
    token: Arc<ClientToken>,
}

impl<T: Transport> Clone for ConnettoClient<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            token: Arc::clone(&self.token),
        }
    }
}

impl<T> ConnettoClient<T>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    /// Take ownership of a connected [`ConnettoConnection`] and return the
    /// client together with its pump future, which the caller must drive to
    /// completion (`tokio::spawn` on native, `spawn_local` on wasm).
    ///
    /// The pump exits when the last client clone drops or the transport
    /// closes. The future is the single place the platform's driving mode
    /// touches the client, so every other behavior stays identical across
    /// native and wasm.
    pub fn with_pump(conn: ConnettoConnection<T>) -> (Self, impl Future<Output = ()>) {
        Self::build(conn, None::<ReconnectDriver<NoReconnect<T>, NoSleep>>)
    }

    /// Like [`with_pump`](Self::with_pump), but the pump survives transport
    /// drops: it backs off per `policy`, obtains a fresh connection from
    /// `factory`, resumes the session with the highest applied cursor, and
    /// re-declares every live subscription, without dropping any handle.
    /// The server replays retained changes as ordinary live patches or
    /// orders a full resync. Observers see [`ClientEvent::Reconnecting`] per
    /// attempt and [`ClientEvent::Reconnected`] on success. Exhausting the
    /// policy broadcasts [`ClientEvent::Closed`] and ends the pump.
    ///
    /// A mutation that was fully sent but not yet processed when the
    /// transport died is NOT replayed (acceptance has no reply, so a resend
    /// could double-apply). Writes captured but never sent re-flush after
    /// the resume.
    pub(crate) fn with_reconnect<F, S>(
        conn: ConnettoConnection<T>,
        factory: F,
        sleeper: S,
        policy: ReconnectPolicy,
    ) -> (Self, impl Future<Output = ()>)
    where
        F: TransportFactory<Transport = T> + MaybeSend + 'static,
        S: Sleeper + MaybeSend + 'static,
    {
        Self::build(
            conn,
            Some(ReconnectDriver {
                factory,
                sleeper,
                policy,
            }),
        )
    }

    /// End the pump and close the transport while the clones stay alive.
    ///
    /// The pump exits as the last clone's drop would end it. The transport
    /// closes with the proper close handshake, no reconnect is attempted,
    /// [`ClientEvent::Closed`] goes to every subscriber, and the exit is
    /// signalled back here. Clones that stay alive keep
    /// their handles, whose reads fall back to the offline half of the
    /// contract.
    pub async fn close(&self) {
        self.shared.close_requested.store(true, Ordering::Release);
        self.shared.wake.notify_one();
        loop {
            let notified = self.shared.pump_done.wake.notified();
            if self.shared.pump_done.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Take this identity out of the shared store before a forget wipes its
    /// replica, as [`ConnettoConnection::leave_shared_store`] does.
    #[cfg(feature = "native-auth")]
    pub(crate) async fn leave_shared_store(
        &self,
        discard_unsent: bool,
    ) -> Result<crate::SharedDeparture, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        state.conn.leave_shared_store(discard_unsent)
    }

    /// Swap the replica for an empty in-memory one after [`close`](Self::close),
    /// so no handle keeps the file open while it is deleted. Windows refuses to
    /// delete an open file. Clones that stay alive read the empty replica.
    #[cfg(feature = "native-auth")]
    pub(crate) async fn release_replica(&self) -> Result<(), ClientError> {
        let mut state = self.shared.state.lock().await;
        let emptied = state.conn.emptied()?;
        drop(std::mem::replace(&mut state.conn, emptied));
        Ok(())
    }

    /// Shared constructor body behind the two pump flavors.
    fn build<F, S>(
        mut conn: ConnettoConnection<T>,
        reconnect: Option<ReconnectDriver<F, S>>,
    ) -> (Self, impl Future<Output = ()>)
    where
        F: TransportFactory<Transport = T> + MaybeSend + 'static,
        S: Sleeper + MaybeSend + 'static,
    {
        let wake = Arc::new(Notify::new());
        let (events, _) = broadcast::channel(256);
        let ever_synced = Arc::new(AtomicBool::new(conn.has_ever_synced()));
        // Seeded from what a previous run left declared, so watching the same
        // query again re-claims its record instead of minting a second one for
        // the same text, and so a fresh id can never collide with a stored one.
        let declared = conn.declared_subscriptions().unwrap_or_default();
        let next_wire = declared
            .iter()
            .filter_map(|(id, _)| id.strip_prefix(WIRE_PREFIX))
            .filter_map(|n| n.parse::<u64>().ok())
            .max()
            .map_or(1, |max| max.saturating_add(1));
        let wire: Vec<WireSub> = declared
            .into_iter()
            .map(|(wire_id, spec)| WireSub {
                wire_id,
                spec,
                // Nothing in this run holds them yet. A watch that re-claims
                // one takes the count to one, and one never claimed stays here
                // for R15's grace to retire.
                refs: 0,
            })
            .collect();
        let grace = conn.watch_grace();
        let gate = GateController::new(Arc::new(ClientGateSink {
            events: events.clone(),
        }));
        let shared = Arc::new(Shared {
            ever_synced,
            state: Mutex::new(State {
                conn,
                registry: Vec::new(),
                computed: Vec::new(),
                wire,
                held: Vec::new(),
                unsynced: Vec::new(),
            }),
            wake: Arc::clone(&wake),
            reaper: Arc::new(Reaper {
                pending: StdMutex::new(Vec::new()),
                wake: Arc::clone(&wake),
                stopped: OnceLock::new(),
                refused: StdMutex::new(HashSet::new()),
                refuse_wake: Arc::new(Notify::new()),
            }),
            events,
            next_live: AtomicU64::new(1),
            next_wire: AtomicU64::new(next_wire),
            gate,
            close_requested: Arc::new(AtomicBool::new(false)),
            pump_done: PumpDone {
                done: AtomicBool::new(false),
                wake: Notify::new(),
            },
            grace,
        });
        let token = Arc::new(ClientToken { wake });
        let driver = pump(Arc::clone(&shared), Arc::downgrade(&token), reconnect);
        (Self { shared, token }, driver)
    }

    /// Refuse the work while the gate is locked. The gate is read under the
    /// state lock, so a refusal is decided under the same lock the pump
    /// gates. Called at the top of a watch entry, before its setup.
    async fn refuse_if_locked(&self) -> Result<(), ClientError> {
        let _state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        Ok(())
    }

    /// Run a typed diesel query and keep its result fresh.
    ///
    /// Executes the query against the local replica for the immediate,
    /// offline-capable answer, registers a server subscription rendered from
    /// the same query (SQLite SQL plus bind values, translated server-side),
    /// and returns a [`LiveQuery`] whose rows the pump refreshes whenever a
    /// table the query reads changes. Dropping the handle unsubscribes.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the query cannot be rendered, the initial local
    /// read fails, or the subscribe frame cannot be sent.
    pub async fn watch<Q, R>(&self, query: Q) -> Result<LiveQuery<R>, ClientError>
    where
        Q: QueryFragment<Sqlite> + Clone + Send + 'static,
        Q: for<'query> LoadQuery<'query, SqliteConnection, R>,
        R: Clone + PartialEq + Send + Sync + 'static,
    {
        self.watch_fn(move || query.clone()).await
    }

    /// Run a diesel query and keep its result fresh, choosing how long the
    /// subscription outlives its last handle. See
    /// [`watch_fn_with_grace`](Self::watch_fn_with_grace).
    ///
    /// # Errors
    ///
    /// Same as [`watch`](Self::watch).
    pub async fn watch_with_grace<Q, R>(
        &self,
        query: Q,
        grace: Duration,
    ) -> Result<LiveQuery<R>, ClientError>
    where
        Q: QueryFragment<Sqlite> + Clone + Send + 'static,
        Q: for<'query> LoadQuery<'query, SqliteConnection, R>,
        R: Clone + PartialEq + Send + Sync + 'static,
    {
        self.watch_fn_with_grace(move || query.clone(), grace).await
    }

    /// Run a diesel query produced by a factory closure and keep its result
    /// fresh.
    ///
    /// The row live query for a boxed (`.into_boxed()`) or otherwise
    /// dynamically built query. Such a query carries no compile-time
    /// aggregation marker, so the typed [`live()`](crate::dsl::Watchable::live)
    /// verb cannot dispatch on it, and it is not `Clone`, so
    /// [`watch`](Self::watch), which clones a stored query on every refresh,
    /// cannot re-run it. In place of a query value it takes `build`, a factory
    /// that yields a fresh query instance on each call.
    ///
    /// `build` is invoked once to render the server subscription, once for the
    /// initial local read, and again on every refresh. It MUST be pure and
    /// stable: each call has to build an equivalent query, the same SQL,
    /// tables, and binds. The wire subscription is fixed from the first
    /// render, so a `build` that renders different SQL on a later call would
    /// silently diverge from what refresh reads. A deliberately time-varying
    /// window is a separate replica-retention feature, not this.
    ///
    /// Like [`watch`](Self::watch) it reads the local replica first for the
    /// immediate, offline-capable answer, registers a server subscription
    /// rendered from the query, and returns a [`LiveQuery`] whose rows the pump
    /// refreshes whenever a table the query reads changes. Dropping the handle
    /// unsubscribes. A boxed aggregate query has no row answer here: send it to
    /// [`watch_value`](Self::watch_value) instead.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the query cannot be rendered, is aggregate-shaped,
    /// the initial local read fails, or the subscribe frame cannot be sent.
    pub async fn watch_fn<F, Q, R>(&self, build: F) -> Result<LiveQuery<R>, ClientError>
    where
        F: Fn() -> Q + Send + 'static,
        Q: QueryFragment<Sqlite>,
        Q: for<'query> LoadQuery<'query, SqliteConnection, R>,
        R: Clone + PartialEq + Send + Sync + 'static,
    {
        self.watch_fn_with_grace(build, self.shared.grace).await
    }

    /// Run a diesel query and keep its result fresh, choosing how long the
    /// subscription outlives its last handle.
    ///
    /// The default is [`crate::DEFAULT_GRACE`] and the ceiling is
    /// [`crate::MAX_GRACE`], above
    /// which the request is clamped: wanting to outlive the cap is by
    /// definition a pin, so use [`pin`](Self::pin) for that. A zero grace ends
    /// the subscription the moment the last handle drops, which is what the
    /// behaviour was before graces existed.
    ///
    /// # Errors
    ///
    /// Same as [`watch_fn`](Self::watch_fn).
    pub async fn watch_fn_with_grace<F, Q, R>(
        &self,
        build: F,
        grace: Duration,
    ) -> Result<LiveQuery<R>, ClientError>
    where
        F: Fn() -> Q + Send + 'static,
        Q: QueryFragment<Sqlite>,
        Q: for<'query> LoadQuery<'query, SqliteConnection, R>,
        R: Clone + PartialEq + Send + Sync + 'static,
    {
        let mut state = self.shared.lock_interrupting().await;
        self.register_watch(&mut state, build, grace).await
    }

    /// Register a live query against already-locked state: render it, read the
    /// initial rows, wire the subscription, and return the handle. Shared by
    /// [`watch_fn_with_grace`](Self::watch_fn_with_grace) and the write-and-keep
    /// surface, so an insert and the watch over its row land under one lock.
    async fn register_watch<F, Q, R>(
        &self,
        state: &mut State<T>,
        build: F,
        grace: Duration,
    ) -> Result<LiveQuery<R>, ClientError>
    where
        F: Fn() -> Q + Send + 'static,
        Q: QueryFragment<Sqlite>,
        Q: for<'query> LoadQuery<'query, SqliteConnection, R>,
        R: Clone + PartialEq + Send + Sync + 'static,
    {
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        let (sql, binds) = render_query(&build())?;
        let parsed = parse_subscription(&sql)?;
        if parsed.shape == QueryShape::Aggregate {
            return Err(ClientError::Session(
                "aggregate query: use watch_value, the replica cannot answer a global statistic"
                    .to_owned(),
            ));
        }
        // A grouped statistic over complete local tables is an ordinary row
        // query the replica answers exactly, so only the synced case routes
        // away.
        if parsed.shape == QueryShape::Grouped
            && parsed
                .tables
                .intersection(state.conn.local_tables())
                .count()
                != parsed.tables.len()
        {
            return Err(ClientError::Session(
                "grouped statistic: use watch_groups, the replica cannot answer a global \
                 statistic as rows"
                    .to_owned(),
            ));
        }
        let tables = parsed.tables;
        let seq = self.shared.next_live.fetch_add(1, Ordering::Relaxed);
        let sub_id = format!("live-{seq}");

        let initial: Vec<R> = build()
            .load(state.conn.conn())
            .map_err(|e| ClientError::Session(e.to_string()))?;
        let rows = Arc::new(RwLock::new(initial));
        let (tx, rx) = watch::channel(0_u64);

        let refresh_rows = Arc::clone(&rows);
        let refresh = Box::new(move |conn: &mut ConnettoConnection<T>| {
            let fresh: Vec<R> = build()
                .load(conn.conn())
                .map_err(|e| ClientError::Session(e.to_string()))?;
            let unchanged = refresh_rows.read().is_ok_and(|current| *current == fresh);
            if !unchanged {
                match refresh_rows.write() {
                    Ok(mut rows) => *rows = fresh,
                    Err(poisoned) => *poisoned.into_inner() = fresh,
                }
                tx.send_modify(|generation| *generation += 1);
            }
            Ok(())
        });

        // Tier dispatch. A query over local tier tables alone registers no
        // server subscription: the update hook refreshes it on local writes.
        // A mixed query subscribes each synced table whole, tied to the
        // handle: the requirement is that the synced side stays live and
        // covering while the handle exists, and whole-table subscribe is the
        // disposable v1 mechanism (predicate pushdown is a later refinement).
        let specs = wire_subscriptions(state.conn.local_tables(), &tables, || {
            SubscriptionSpec::new(sql).with_binds(binds)
        });
        let mut wire_ids = Vec::with_capacity(specs.len());
        for spec in specs {
            let wire_id = attach_wire(state, &self.shared, &sub_id, spec, grace).await?;
            wire_ids.push(wire_id);
        }
        let reads_synced = !wire_ids.is_empty();
        state.registry.push(LiveEntry {
            sub_id: sub_id.clone(),
            tables,
            refresh,
            wire_ids,
        });

        Ok(LiveQuery {
            handle: LiveHandleCore::new(sub_id, rx, &self.shared.reaper),
            rows,
            reads_synced,
            ever_synced: Arc::clone(&self.shared.ever_synced),
        })
    }

    /// Insert `values` and keep the inserted row live: the returned
    /// [`LiveQuery`] tracks exactly that row, so it survives eviction while any
    /// handle holds it and reports the row vanishing when it is deleted.
    ///
    /// The table is the value's own, inferred at the type level from the
    /// returned row rather than named. The insert appends a `RETURNING` clause,
    /// reads the primary key from the row's [`Identifiable`] impl, and watches
    /// `table.find(key)`. Single-column primary keys only: a composite key uses
    /// the two-call pattern (`with_conn` write, then `watch(table.find(key))`).
    ///
    /// Until the server acknowledges this write the row is spared eviction by
    /// the pending-write guard, so the write and the watch need not be atomic
    /// even though they share one lock here (R15 step 4).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and [`ClientError`]
    /// when the insert or the watch registration fails.
    pub async fn insert_watched_with_grace<V, R, K>(
        &self,
        values: V,
        grace: Duration,
    ) -> Result<(R, LiveQuery<R>), ClientError>
    where
        V: Insertable<R::Table>,
        R: HasTable + Clone + PartialEq + Send + Sync + 'static,
        for<'a> &'a R: Identifiable<Id = &'a K>,
        K: Clone + Send + Sync + 'static,
        R::Table: FindDsl<K> + Send + 'static,
        Find<R::Table, K>: QueryFragment<Sqlite> + for<'q> LoadQuery<'q, SqliteConnection, R>,
        InsertStatement<R::Table, <V as Insertable<R::Table>>::Values>:
            for<'q> LoadQuery<'q, SqliteConnection, R>,
    {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        let row: R = diesel::insert_into(<R as HasTable>::table())
            .values(values)
            .get_result(state.conn.conn())
            .map_err(|e| ClientError::Session(e.to_string()))?;
        let key: K = (*Identifiable::id(&row)).clone();
        let live = self
            .register_watch(
                &mut state,
                move || <R as HasTable>::table().find(key.clone()),
                grace,
            )
            .await?;
        Ok((row, live))
    }

    /// Insert `values` and keep the inserted row live with the default grace.
    /// See [`insert_watched_with_grace`](Self::insert_watched_with_grace).
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the insert or the watch registration fails.
    pub async fn insert_watched<V, R, K>(&self, values: V) -> Result<(R, LiveQuery<R>), ClientError>
    where
        V: Insertable<R::Table>,
        R: HasTable + Clone + PartialEq + Send + Sync + 'static,
        for<'a> &'a R: Identifiable<Id = &'a K>,
        K: Clone + Send + Sync + 'static,
        R::Table: FindDsl<K> + Send + 'static,
        Find<R::Table, K>: QueryFragment<Sqlite> + for<'q> LoadQuery<'q, SqliteConnection, R>,
        InsertStatement<R::Table, <V as Insertable<R::Table>>::Values>:
            for<'q> LoadQuery<'q, SqliteConnection, R>,
    {
        self.insert_watched_with_grace(values, self.shared.grace)
            .await
    }

    /// Insert `values` and pin the inserted row under `name`, the durable form
    /// of keeping a written row: it survives restarts and being offline until
    /// [`unpin`](Self::unpin). Returns the inserted row.
    ///
    /// The table is inferred from the row and its primary key read from the
    /// row's [`Identifiable`] impl, as in [`insert_watched`](Self::insert_watched).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and [`ClientError`]
    /// when the insert or the pin fails.
    pub async fn insert_pinned<V, R, K>(&self, name: &str, values: V) -> Result<R, ClientError>
    where
        V: Insertable<R::Table>,
        R: HasTable,
        for<'a> &'a R: Identifiable<Id = &'a K>,
        K: Clone,
        R::Table: FindDsl<K>,
        Find<R::Table, K>: QueryFragment<Sqlite>,
        InsertStatement<R::Table, <V as Insertable<R::Table>>::Values>:
            for<'q> LoadQuery<'q, SqliteConnection, R>,
    {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        let row: R = diesel::insert_into(<R as HasTable>::table())
            .values(values)
            .get_result(state.conn.conn())
            .map_err(|e| ClientError::Session(e.to_string()))?;
        let key: K = (*Identifiable::id(&row)).clone();
        let query = <R as HasTable>::table().find(key);
        let (sql, binds) = render_query(&query)?;
        let spec = SubscriptionSpec::new(sql).with_binds(binds);
        let wire_id = attach_wire(
            &mut state,
            &self.shared,
            name,
            spec.clone(),
            self.shared.grace,
        )
        .await?;
        state.conn.pin_subscription(name, &wire_id, &spec)?;
        Ok(row)
    }

    /// Update the rows `target` names and keep the updated row live: the update
    /// twin of [`insert_watched`](Self::insert_watched). `target` is an
    /// identifiable row reference or a `table.find(key)`, `changeset` the diesel
    /// change to apply. Returns the updated row and a [`LiveQuery`] over it.
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, before anything is
    /// written, and [`ClientError`] when the update or the watch registration
    /// fails.
    pub async fn update_watched<Tgt, C, R, K>(
        &self,
        target: Tgt,
        changeset: C,
    ) -> Result<(R, LiveQuery<R>), ClientError>
    where
        Tgt: IntoUpdateTarget,
        C: AsChangeset<Target = <Tgt as HasTable>::Table>,
        R: HasTable + Clone + PartialEq + Send + Sync + 'static,
        for<'a> &'a R: Identifiable<Id = &'a K>,
        K: Clone + Send + Sync + 'static,
        R::Table: FindDsl<K> + Send + 'static,
        Find<R::Table, K>: QueryFragment<Sqlite> + for<'q> LoadQuery<'q, SqliteConnection, R>,
        UpdateStatement<
            <Tgt as HasTable>::Table,
            <Tgt as IntoUpdateTarget>::WhereClause,
            <C as AsChangeset>::Changeset,
        >: AsQuery + for<'q> LoadQuery<'q, SqliteConnection, R>,
    {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        let row: R = diesel::update(target)
            .set(changeset)
            .get_result(state.conn.conn())
            .map_err(|e| ClientError::Session(e.to_string()))?;
        let key: K = (*Identifiable::id(&row)).clone();
        let live = self
            .register_watch(
                &mut state,
                move || <R as HasTable>::table().find(key.clone()),
                self.shared.grace,
            )
            .await?;
        Ok((row, live))
    }

    /// Watch a server-maintained scalar aggregate, decoding each value
    /// through diesel into `V`, exactly as `get_result` would load the query.
    ///
    /// The query must be a single ungrouped aggregate of the built-in family
    /// (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, or the variance and stddev
    /// family), classified from the rendered SQL by function name. It
    /// registers like any subscription, but the answer path is inverted: no
    /// local read happens (the replica holds only this client's authorized
    /// subset), and every value, the bootstrap included, arrives as a server
    /// push the client rests in the replica's storage form. Use an `Option`
    /// value type (for example `Option<f64>`) for aggregates that are `null`
    /// over an empty set, such as `AVG`, `MIN`, and `MAX`.
    ///
    /// This classification recognizes only the built-in family. A custom
    /// aggregate must be driven through the typed
    /// [`live()`](crate::dsl::Watchable::live) path, which classifies by
    /// diesel's `IsAggregate` marker instead.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the query cannot be rendered, is not
    /// aggregate-shaped, or the subscribe frame cannot be sent. A server-side
    /// refusal (for example an aggregate on an RLS table) arrives later as
    /// [`ClientEvent::NonFatal`] on [`events`](Self::events).
    pub async fn watch_value<Q, V>(&self, query: Q) -> Result<LiveValue<V>, ClientError>
    where
        Q: Query + QueryFragment<Sqlite>,
        V: Loadable<Q::SqlType> + Clone + PartialEq + Send + Sync,
    {
        let rendered = render_query(&query)?;
        self.watch_value_core::<Q::SqlType, V>(rendered, ShapeSource::Sql)
            .await
    }

    /// The typed `live()` entry: the aggregate shape is already proven by
    /// diesel's `IsAggregate` marker, so the SQL-text classification is
    /// bypassed and a custom aggregate name is accepted.
    pub(crate) async fn watch_value_typed<Q, V>(
        &self,
        query: Q,
    ) -> Result<LiveValue<V>, ClientError>
    where
        Q: Query + QueryFragment<Sqlite>,
        V: Loadable<Q::SqlType> + Clone + PartialEq + Send + Sync,
    {
        let rendered = render_query(&query)?;
        self.watch_value_core::<Q::SqlType, V>(rendered, ShapeSource::Marker)
            .await
    }

    /// The core behind every value watch, over the query rendered to its SQL
    /// and binds. `shape` says whether the aggregate shape is trusted from a
    /// type-level marker or must be classified from the rendered SQL.
    async fn watch_value_core<ST, V>(
        &self,
        (sql, binds): (String, Vec<SqliteValue>),
        shape: ShapeSource,
    ) -> Result<LiveValue<V>, ClientError>
    where
        V: Loadable<ST> + Clone + PartialEq + Send + Sync,
    {
        self.refuse_if_locked().await?;
        let parsed = parse_subscription(&sql)?;
        require_scalar_shape(shape, &parsed)?;
        let (handle, value) = self
            .watch_computed(
                sql,
                binds,
                parsed,
                load_whole::<ST, V>,
                ComputedTier::Scalar(run_local_scalar::<ST, V>),
            )
            .await?;
        Ok(LiveValue { handle, value })
    }

    /// Watch a grouped aggregate as a live map, one entry per group, each
    /// row loaded through diesel (R84).
    ///
    /// The query must select its plain group columns first, in any order,
    /// and exactly one aggregate last, for example
    /// `SELECT status, COUNT(*) FROM orders GROUP BY status`. `K` loads from
    /// the group columns as one `Queryable` value (the bare type for one
    /// column, a tuple for several) and `V` from the aggregate, each with the
    /// query's own SQL types.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the query cannot be rendered, is not
    /// grouped-aggregate-shaped, reads a local table (a local tier is
    /// complete, so `watch` answers it as rows), or the subscribe frame
    /// cannot be sent. A server-side refusal arrives later as
    /// [`ClientEvent::NonFatal`] on [`events`](Self::events).
    pub async fn watch_groups<Q, K, V>(&self, query: Q) -> Result<LiveGroups<K, V>, ClientError>
    where
        Q: Query + QueryFragment<Sqlite>,
        Q::SqlType: SplitLast,
        K: Loadable<<Q::SqlType as SplitLast>::Init> + Eq + core::hash::Hash + Clone + Send + Sync,
        V: Loadable<<Q::SqlType as SplitLast>::Last> + Clone + PartialEq + Send + Sync,
    {
        let rendered = render_query(&query)?;
        self.watch_groups_core::<<Q::SqlType as SplitLast>::Init, <Q::SqlType as SplitLast>::Last, K, V>(
            rendered,
            ShapeSource::Sql,
        )
        .await
    }

    /// The typed `live()` grouped entry: diesel proved the grouped aggregate
    /// shape, so only the column order a keyed row splits by remains a
    /// runtime check.
    pub(crate) async fn watch_groups_typed<Q, K, V>(
        &self,
        query: Q,
    ) -> Result<LiveGroups<K, V>, ClientError>
    where
        Q: Query + QueryFragment<Sqlite>,
        Q::SqlType: SplitLast,
        K: Loadable<<Q::SqlType as SplitLast>::Init> + Eq + core::hash::Hash + Clone + Send + Sync,
        V: Loadable<<Q::SqlType as SplitLast>::Last> + Clone + PartialEq + Send + Sync,
    {
        let rendered = render_query(&query)?;
        self.watch_groups_core::<<Q::SqlType as SplitLast>::Init, <Q::SqlType as SplitLast>::Last, K, V>(
            rendered,
            ShapeSource::Marker,
        )
        .await
    }

    /// The core behind every grouped watch, over the query rendered to its
    /// SQL and binds, with the key columns' SQL type `KS` and the value's
    /// `VS`.
    async fn watch_groups_core<KS, VS, K, V>(
        &self,
        (sql, binds): (String, Vec<SqliteValue>),
        shape: ShapeSource,
    ) -> Result<LiveGroups<K, V>, ClientError>
    where
        K: Loadable<KS> + Eq + core::hash::Hash + Clone + Send + Sync,
        V: Loadable<VS> + Clone + PartialEq + Send + Sync,
    {
        self.refuse_if_locked().await?;
        let parsed = parse_subscription(&sql)?;
        require_grouped_shape(shape, &parsed)?;
        let (handle, state) = self
            .watch_computed(
                sql,
                binds,
                parsed,
                load_groups::<KS, VS, K, V>,
                ComputedTier::Refused {
                    kind: "aggregate",
                    local: "local grouped query: use watch, a complete local tier answers it as \
                            rows",
                },
            )
            .await?;
        Ok(LiveGroups { handle, state })
    }

    /// Watch a row-shaped query the server computes, loading each answer row
    /// into `R` through diesel, exactly as `load` would load the query (R84).
    ///
    /// This is the explicit surface for queries the server serves by
    /// re-execution rather than by syncing rows (joins, `DISTINCT`,
    /// expression projections): whether a query is re-executed is the
    /// server's tiering decision, invisible to the client's type system, so
    /// this handle is asked for by name and never dispatched to. A whole
    /// answer replaces [`rows`](LiveRows::rows) in the order the server
    /// produced it, and each answer rests in the statistic's resting table so
    /// an offline restart shows the last synced one.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the query cannot be rendered, is
    /// aggregate-shaped (use [`watch_value`](Self::watch_value) or
    /// [`watch_groups`](Self::watch_groups)), reads a local table (a
    /// complete local tier answers it through [`watch`](Self::watch)), or
    /// the subscribe frame cannot be sent. A server-side refusal arrives
    /// later as [`ClientEvent::NonFatal`] on [`events`](Self::events).
    pub async fn watch_rows<Q, R>(&self, query: Q) -> Result<LiveRows<R>, ClientError>
    where
        Q: Query + QueryFragment<Sqlite>,
        R: Loadable<Q::SqlType> + Clone + PartialEq + Send + Sync,
    {
        let (sql, binds) = render_query(&query)?;
        self.refuse_if_locked().await?;
        let parsed = parse_subscription(&sql)?;
        match parsed.shape {
            QueryShape::Rows => {}
            QueryShape::Aggregate => {
                return Err(ClientError::Session(
                    "scalar aggregate: use watch_value, an answer row cannot carry it".to_owned(),
                ));
            }
            QueryShape::Grouped => {
                return Err(ClientError::Session(
                    "grouped statistic: use watch_groups, the keyed map is its shape".to_owned(),
                ));
            }
        }
        let (handle, state) = self
            .watch_computed(
                sql,
                binds,
                parsed,
                load_whole::<Q::SqlType, R>,
                ComputedTier::Refused {
                    kind: "query",
                    local: "local query: use watch, a complete local tier answers it as rows",
                },
            )
            .await?;
        Ok(LiveRows { handle, state })
    }

    /// Register one computed handle whose answer rows `load` reads as `X`,
    /// and bootstrap it.
    ///
    /// A computed answer cannot span the tiers. Over local tables only, a
    /// scalar is answered by exact local re-execution on change, and any
    /// other shape is refused with the tier's own message. Over synced
    /// tables the handle bootstraps from the resting table before
    /// subscribing, so a same-run late joiner and an offline restart resolve
    /// through the rested rows rather than a run-local cache (R83 decision
    /// 3). The bootstrap marks itself seen, so the first `changed()` still
    /// waits for a real change, like any bootstrap.
    async fn watch_computed<X>(
        &self,
        sql: String,
        binds: Vec<SqliteValue>,
        parsed: ParsedSubscription,
        load: LoadAnswer<X>,
        tier: ComputedTier<X>,
    ) -> Result<(LiveHandleCore, Arc<RwLock<Answer<X>>>), ClientError>
    where
        X: Clone + PartialEq + Send + Sync + 'static,
    {
        let seq = self.shared.next_live.fetch_add(1, Ordering::Relaxed);
        let sub_id = format!("live-{seq}");
        let answer = Arc::new(RwLock::new(Answer::<X>::default()));
        let (tx, mut rx) = watch::channel(0_u64);

        let mut state = self.shared.lock_interrupting().await;

        let local_count = parsed
            .tables
            .intersection(state.conn.local_tables())
            .count();
        if local_count > 0 {
            let kind = match &tier {
                ComputedTier::Scalar(_) => "aggregate",
                ComputedTier::Refused { kind, .. } => kind,
            };
            if local_count != parsed.tables.len() {
                return Err(ClientError::Session(format!(
                    "mixed-tier {kind}: a computed answer cannot span local and synced tables"
                )));
            }
            let run_local = match tier {
                ComputedTier::Scalar(run_local) => run_local,
                ComputedTier::Refused { local, .. } => {
                    return Err(ClientError::Session(local.to_owned()));
                }
            };
            let inlined = inline_binds(&sql, &binds)?;
            let publish = Arc::clone(&answer);
            let local = move |conn: &mut ConnettoConnection<T>| -> Result<bool, ClientError> {
                let now = conn.now_secs()?;
                let fresh = run_local(conn.conn(), &inlined)?;
                let mut slot = publish.write().unwrap_or_else(PoisonError::into_inner);
                slot.as_of = Some(now);
                let key = SCALAR_KEY.to_vec();
                let moved_any = slot.rows.get(&key) != Some(&fresh);
                slot.rows.insert(key, fresh);
                Ok(moved_any)
            };
            local(&mut state.conn)?;
            let refresh = Box::new(move |conn: &mut ConnettoConnection<T>| {
                if local(conn)? {
                    tx.send_modify(|generation| *generation += 1);
                }
                Ok(())
            });
            state.registry.push(LiveEntry {
                sub_id: sub_id.clone(),
                tables: parsed.tables,
                refresh,
                wire_ids: Vec::new(),
            });
            return Ok((LiveHandleCore::new(sub_id, rx, &self.shared.reaper), answer));
        }

        let mut apply = answer_apply(load, sql.clone(), binds.clone(), Arc::clone(&answer), tx);
        apply(state.conn.conn(), Moved::All)?;
        rx.borrow_and_update();
        let spec = SubscriptionSpec::new(sql).with_binds(binds);
        // No grace for a computed answer. The grace exists so a re-watch does
        // not re-pay a snapshot, but a computed handle holds no replica rows
        // and its answer rests durably, so keeping the subscription alive
        // across a drop would buy nothing the resting table does not already
        // give.
        let wire_id = attach_wire(&mut state, &self.shared, &sub_id, spec, Duration::ZERO).await?;
        state.computed.push(ComputedEntry {
            sub_id: sub_id.clone(),
            wire_id,
            apply,
        });
        Ok((LiveHandleCore::new(sub_id, rx, &self.shared.reaper), answer))
    }

    /// The (writer, sequence) pairs of the local writes the server has not
    /// confirmed, the ones the pump has not queued yet among them.
    ///
    /// The writes committed since the pump's last flush are queued first, so
    /// a guard reading this right after a write sees it. Non-empty means a
    /// teardown would lose data.
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and a fault the
    /// queueing or the send surfaces. A lost transport does not fail the
    /// read, since the entry is durable and the pump's reconnect replays it.
    pub async fn unsynced(&self) -> Result<Vec<crate::UnsyncedWrite>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        flush_before_read(&mut state.conn).await?;
        Ok(state.conn.unsynced())
    }

    /// Whether the drain wire is up and its held writes have settled (R31).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked.
    pub async fn held_files_ready(&self) -> Result<Option<Major>, ClientError> {
        let state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        Ok(state.conn.held_files_ready())
    }

    /// Whether the draining major still holds files, whatever the wire state
    /// (R31).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and a failure to
    /// read the held rows.
    pub async fn held_files_pending(&self) -> Result<bool, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        state.conn.held_files_pending()
    }

    /// Ask the held major's server for a content ticket over the drain wire
    /// (R31).
    ///
    /// # Errors
    ///
    /// [`ClientError::NotConnected`] without a drain wire, [`ClientError::ShareKeysPending`] while its authority is pending, or [`ClientError::Transport`] when sending fails.
    pub async fn request_held_ticket(
        &self,
        request_id: String,
        file_id: [u8; 32],
        verb: ContentVerb,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state
            .conn
            .request_held_ticket(request_id, file_id, verb)
            .await
    }

    /// A held file settled, so the batch may now be done (R31).
    ///
    /// # Errors
    ///
    /// A failure to read or forget the held rows.
    pub async fn held_file_settled(&self) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.held_file_settled().await
    }

    /// A held file was given up for good, after its held rows were deleted
    /// (R31).
    ///
    /// # Errors
    ///
    /// A failure to read or forget the held rows.
    pub async fn held_file_lost(&self, major: Major) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.held_file_lost(major).await
    }

    /// The local work a deletion would lose, content fields left to the content layer (R31).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and a fault the
    /// queueing or the send surfaces. A lost transport does not fail the
    /// read, since the entry is durable and the pump's reconnect replays it.
    pub async fn pending_work(&self) -> Result<crate::PendingWork, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        flush_before_read(&mut state.conn).await?;
        Ok(crate::PendingWork {
            unsynced: state.conn.unsynced(),
            ..crate::PendingWork::default()
        })
    }

    /// The provenance of a peer-applied row, standing it back from the
    /// table, since the broadcast event stream can drop events (R77).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and the
    /// [`ClientError`] the read surfaces.
    #[cfg(feature = "peer")]
    pub async fn provenance(
        &self,
        table: &str,
        key: &[crate::KeyValue],
    ) -> Result<Option<crate::provisional::ProvisionalRow>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        state.conn.provenance(table, key)
    }

    /// Every provenance row the replica holds, keyed order (R77).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and the
    /// [`ClientError`] the read surfaces.
    #[cfg(feature = "peer")]
    pub async fn provisional_rows(
        &self,
    ) -> Result<Vec<crate::provisional::ProvisionalRow>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        state.conn.provisional_rows()
    }

    /// The session's courier candidates, one per author with a stored offer
    /// (R78 decision 16).
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and the
    /// [`ClientError`] the read surfaces.
    #[cfg(feature = "peer")]
    pub async fn courier_candidates(&self) -> Result<Vec<crate::CourierCandidate>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        state.conn.courier_candidates()
    }

    /// Courier the author's held rows to the server on the main wire (R78
    /// decision 48), the range sent, queued or held for the wire's device
    /// bind by the connection, and the report settled by the pump.
    ///
    /// # Errors
    ///
    /// [`crate::CourierError::Client`] while the gate is locked or the
    /// author's key is not 32 bytes,
    /// [`crate::CourierError::Refused`] with `Uncertified` when no device
    /// bind can come this session, no frame riding and the author standing
    /// a candidate, [`crate::CourierError::TimedOut`] when a held call's
    /// bound runs out, [`crate::CourierError::SessionLost`] when the session
    /// ends before the bind answers, and the [`crate::CourierError`] the
    /// range or its settlement surfaces.
    #[cfg(feature = "peer")]
    pub async fn courier(
        &self,
        author: &[u8],
    ) -> Result<crate::CourierReport, crate::CourierError> {
        let wait: crate::CourierWait;
        {
            let mut state = self.shared.lock_interrupting().await;
            if self.shared.gate.is_locked() {
                return Err(crate::CourierError::Client(ClientError::Locked));
            }
            let author: [u8; 32] = author.try_into().map_err(|_| {
                crate::CourierError::Client(ClientError::Session(
                    "an author key must be 32 bytes".to_owned(),
                ))
            })?;
            wait = state.conn.courier(author).await?;
        }
        self.shared.wake.notify_one();
        tokio::time::timeout(wait.bound, wait.receiver)
            .await
            .map_err(|_| crate::CourierError::TimedOut)?
            .unwrap_or_else(|_| Err(crate::CourierError::SessionLost))
    }

    /// Await committed courier audit receipts within one monotonic bound across reconnects.
    ///
    /// # Errors
    ///
    /// [`crate::CourierAuditError`] distinguishes invalid input, capacity, expiry, refusal, observation failure and session loss.
    pub async fn observe_courier_audit(
        &self,
        author: &[u8],
        seqs: &[u64],
        within: Duration,
    ) -> Result<(), crate::CourierAuditError> {
        let deadline = SystemClock
            .monotonic()
            .checked_add(within)
            .filter(|_| !within.is_zero())
            .ok_or(crate::CourierAuditError::InvalidInput)?;
        let observe = async {
            let wait = {
                let mut state = self.shared.lock_interrupting().await;
                if self.shared.gate.is_locked() {
                    return Err(crate::CourierAuditError::Client(ClientError::Locked));
                }
                if self.shared.close_requested.load(Ordering::Acquire)
                    || self.shared.pump_done.done.load(Ordering::Acquire)
                {
                    return Err(crate::CourierAuditError::SessionEnded);
                }
                state.conn.begin_courier_audit(
                    author,
                    seqs,
                    deadline,
                    Arc::clone(&self.shared.wake),
                )?
            };
            self.shared.wake.notify_one();
            wait.wait_reply().await
        };
        let mut observe = core::pin::pin!(observe);
        loop {
            let remaining = deadline.saturating_sub(SystemClock.monotonic());
            if remaining.is_zero() {
                return Err(crate::CourierAuditError::Expired);
            }
            tokio::select! {
                biased;
                result = &mut observe => return result,
                () = crate::timer_wait(remaining) => {}
            }
        }
    }

    /// Run a closure against the shared connection: one-off diesel reads and
    /// captured writes. Local writes committed here are auto-submitted by the
    /// pump's next flush, and any live query reading the written tables
    /// refreshes.
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, and the closure does not
    /// run. Otherwise whatever the closure's connection work surfaces.
    pub async fn with_conn<F, O>(&self, f: F) -> Result<O, ClientError>
    where
        F: FnOnce(&mut ConnettoConnection<T>) -> O,
    {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        let out = f(&mut state.conn);
        drop(state);
        // A write may have landed: let the pump flush and refresh promptly.
        self.shared.wake.notify_one();
        Ok(out)
    }

    /// Commit a share key to the desired set and refresh local visibility.
    ///
    /// A relay tab's worker holds the keys, so in a tab the call returns once
    /// the worker committed the change and restated the tab's identity.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the client is closed, locked, or cannot persist the change.
    pub async fn add_share_key<K: CapabilityKey>(
        &self,
        grant: &Grant,
        subject: &CapabilitySubject<K>,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        self.allow_relay_request()?;
        if state.conn.is_relay_mirror() {
            let answer = state
                .conn
                .ask_relay_share_key(ShareKeyChange::Add {
                    grant: grant.clone(),
                    subject: subject.key().to_string(),
                })
                .await?;
            drop(state);
            return self.await_relay(answer, "the share-key change").await;
        }
        state.conn.add_share_key(grant, subject).await?;
        refresh_changed(&mut state, &self.shared.events);
        drop(state);
        self.shared.wake.notify_one();
        Ok(())
    }

    /// Remove every grant for a subject and refresh local visibility.
    ///
    /// A relay tab's worker holds the keys, as for
    /// [`add_share_key`](Self::add_share_key).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the client is closed, locked, or cannot persist the change.
    pub async fn remove_share_key<K: CapabilityKey>(
        &self,
        subject: &CapabilitySubject<K>,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        self.allow_relay_request()?;
        if state.conn.is_relay_mirror() {
            let answer = state
                .conn
                .ask_relay_share_key(ShareKeyChange::Remove {
                    subject: subject.key().to_string(),
                })
                .await?;
            drop(state);
            return self.await_relay(answer, "the share-key change").await;
        }
        state.conn.remove_share_key(subject).await?;
        refresh_changed(&mut state, &self.shared.events);
        drop(state);
        self.shared.wake.notify_one();
        Ok(())
    }

    /// What this account's data takes on the device, as
    /// [`ConnettoConnection::storage_use`] reads it (R11 slice 7).
    ///
    /// A relay tab's mirror holds no store of its own, so in a tab the call
    /// asks the worker and returns its answer.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the client is closed, locked, its replica
    /// refuses the read, or a tab's worker does not answer in time.
    pub async fn storage_use(&self) -> Result<StorageUse, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        self.allow_relay_request()?;
        if state.conn.is_relay_mirror() {
            let answer = state.conn.ask_relay_storage_use().await?;
            drop(state);
            return self.await_relay(answer, "the storage read").await;
        }
        state.conn.storage_use()
    }

    /// Follow this device's unsynced work, its writes and staged files that
    /// have not reached the server and its undismissed refusals, as a live
    /// handle.
    ///
    /// A relay tab's mirror holds no outbox, so in a tab the handle follows
    /// the snapshot the worker states.
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked, or
    /// [`ClientError`] if the outbox does not read back.
    pub async fn unsynced_live(&self) -> Result<LiveUnsynced, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        let work = Arc::new(RwLock::new(state.conn.unsynced_work()?));
        let seq = self.shared.next_live.fetch_add(1, Ordering::Relaxed);
        let sub_id = format!("live-{seq}");
        let (changed, receiver) = watch::channel(0_u64);
        state.unsynced.push(UnsyncedEntry {
            sub_id: sub_id.clone(),
            work: Arc::clone(&work),
            changed,
        });
        Ok(LiveUnsynced {
            handle: LiveHandleCore::new(sub_id, receiver, &self.shared.reaper),
            work,
        })
    }

    /// Dismiss `writer`'s refused write `seq`, so the unsynced snapshot no
    /// longer lists it, as [`ConnettoConnection::dismiss_refusal`] does.
    ///
    /// A relay tab's mirror holds no refusals, so in a tab the call asks the
    /// worker and returns its answer.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the client is closed, locked, its replica
    /// refuses the write, or a tab's worker does not answer in time.
    pub async fn dismiss_refusal(&self, writer: WriterRef, seq: u64) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        self.allow_relay_request()?;
        if state.conn.is_relay_mirror() {
            let answer = state.conn.ask_relay_dismiss(writer, seq).await?;
            drop(state);
            return self.await_relay(answer, "the refusal dismissal").await;
        }
        state.conn.dismiss_refusal(&writer, seq)?;
        refresh_changed(&mut state, &self.shared.events);
        Ok(())
    }

    /// Wait for the relay's answer to `request`, which the pump delivers or
    /// ends at its bound, so the pump is woken to arm that bound.
    async fn await_relay<A>(
        &self,
        answer: tokio::sync::oneshot::Receiver<Result<A, ClientError>>,
        request: &str,
    ) -> Result<A, ClientError> {
        self.shared.wake.notify_one();
        answer.await.unwrap_or_else(|_| {
            Err(ClientError::Transport(format!(
                "the relay connection dropped before answering {request}"
            )))
        })
    }

    /// Refuse a request a closed or locked client cannot serve. A relay tab's
    /// request waits on the pump, so a client whose pump ended refuses too.
    fn allow_relay_request(&self) -> Result<(), ClientError> {
        if self.shared.close_requested.load(Ordering::Acquire)
            || self.shared.pump_done.done.load(Ordering::Acquire)
        {
            return Err(ClientError::Stopped("the client is closed".to_owned()));
        }
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        Ok(())
    }

    /// Turn on the away/return re-check.
    ///
    /// From now on a return re-asks the mechanism when the time away exceeds
    /// the grace, and a gated launch starts locked. `recheck` is the grace:
    /// `None` re-checks once per launch (never on a return), `Some(zero)`
    /// re-checks every return, and `Some(d)` re-checks a return whose time
    /// away exceeds `d`.
    pub async fn enable_gate(&self, recheck: Option<Duration>, mechanism: Arc<dyn GateMechanism>) {
        let _state = self.shared.lock_interrupting().await;
        self.shared.gate.enable(recheck, mechanism);
    }

    /// Turn on the re-check open, for a launch whose prompt was approved.
    pub(crate) async fn enable_verified_gate(
        &self,
        recheck: Option<Duration>,
        mechanism: Arc<dyn GateMechanism>,
    ) {
        let _state = self.shared.lock_interrupting().await;
        self.shared.gate.enable_verified(recheck, mechanism);
    }

    /// The application went away at `at`.
    ///
    /// Only a gated, open client keeps the moment, and only when a grace is
    /// set. Anonymous and gate-less clients ignore it, a locked client's
    /// pending prompt stays pending, and no grace means once per launch.
    pub async fn away(&self, at: Moment) {
        let _state = self.shared.lock_interrupting().await;
        self.shared.gate.away(at);
    }

    /// The application came back at `at`.
    ///
    /// A locked client re-asks the mechanism when no prompt is pending. A
    /// gated, open client clears the pending moment and re-checks when the
    /// time away exceeds the grace, or on every return with a zero grace.
    pub async fn back(&self, at: Moment) {
        let _state = self.shared.lock_interrupting().await;
        // Let the pump pick up a started prompt promptly.
        if self.shared.gate.back(at) {
            self.shared.wake.notify_one();
        }
    }

    /// The application asks the gate to unlock, a retry after a dismissed
    /// prompt. Asks the mechanism when the gate is locked and no prompt is
    /// pending, and does nothing when the gate is open or a prompt is pending.
    pub async fn unlock(&self) {
        let _state = self.shared.lock_interrupting().await;
        // Let the pump pick up a started prompt promptly.
        if self.shared.gate.unlock() {
            self.shared.wake.notify_one();
        }
    }

    /// Replays every unacknowledged mutation on the current transport.
    ///
    /// A disconnected client keeps the durable queue unchanged.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when encoding or sending a queued mutation fails.
    pub async fn replay_pending(&self) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.replay_pending().await
    }

    /// Keep a query's rows synced and covered until [`unpin`](Self::unpin),
    /// under an application-chosen name.
    ///
    /// A pin is the durable form of interest: it has no handle and no clock,
    /// so it survives closing and reopening the application and it survives
    /// being offline. Pinning the same name and query twice is a no-op, and
    /// pinning a changed query under an existing name replaces it, which is
    /// the upgrade path. Collisions between application features are the
    /// application's to avoid, since the application chooses the names.
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked.
    /// [`ClientError::Session`] when the replica rejects the record.
    pub async fn pin(&self, name: &str, query: &str) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        let spec = SubscriptionSpec::new(query);
        let wire_id = attach_wire(
            &mut state,
            &self.shared,
            name,
            spec.clone(),
            self.shared.grace,
        )
        .await?;
        state.conn.pin_subscription(name, &wire_id, &spec)
    }

    /// End the pin under `name`. Unknown names are a no-op, so this is safe to
    /// call unconditionally. Its rows stop being covered by the pin and become
    /// evictable unless something else still wants them.
    ///
    /// # Errors
    ///
    /// [`ClientError::Locked`] while the gate is locked.
    /// [`ClientError::Session`] when the replica rejects the write.
    pub async fn unpin(&self, name: &str) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        if self.shared.gate.is_locked() {
            return Err(ClientError::Locked);
        }
        state.conn.unpin_subscription(name)
    }

    /// Every pin, as name and query, in name order.
    ///
    /// # Errors
    ///
    /// [`ClientError::Session`] when the replica cannot be read.
    pub async fn pins(&self) -> Result<Vec<(String, String)>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.pins()
    }

    /// Reclaim replica space now: evict every row no live subscription covers
    /// and return the freed pages to the filesystem.
    ///
    /// The automatic pass already runs when a subscription ends. This is the
    /// application-callable form for a free-up-space affordance, sweeping every
    /// declared table at once. It is a no-op while the transport is down and
    /// spares every pending write, exactly like the automatic pass.
    ///
    /// # Errors
    ///
    /// [`ClientError`] on a replica read or write failure.
    pub async fn tidy(&self) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.tidy()
    }

    /// Send a keepalive probe. The matching [`ClientEvent::Pong`] on the
    /// [`events`](Self::events) stream doubles as a barrier: the server
    /// processes frames in order, so the pong proves every frame sent before
    /// the ping (subscribes and unsubscribes included) was handled. Dropped
    /// handles queue their unsubscribes for the pump, so the ping drains that
    /// queue first: without that, the ping could overtake a queued
    /// unsubscribe and the pong would fence nothing.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] when the ping cannot be sent.
    pub async fn ping(&self, nonce: u64) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        drain_dropped(&mut state, &self.shared.reaper, &self.shared.events).await?;
        state.conn.ping(nonce).await
    }

    /// Ask the server for a content ticket naming one file and one verb,
    /// with `for_owner` naming the account a carried upload credits (R79
    /// decision 17).
    ///
    /// The answer arrives on [`events`](Self::events) as
    /// [`ClientEvent::ContentTicket`] carrying `request_id`, or as a refusal
    /// carrying it in `related_to`. Subscribe before calling: the pump can
    /// deliver the answer before this returns.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transport`] when the request cannot be sent, including
    /// when there is no transport: a ticket is the server's to grant, so
    /// there is nothing to queue offline.
    pub async fn request_content_ticket(
        &self,
        request_id: String,
        file_id: [u8; 32],
        verb: connetto_core::messages::ContentVerb,
        for_owner: Option<String>,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state
            .conn
            .request_content_ticket(request_id, file_id, verb, for_owner)
            .await
    }

    /// Subscribe to the raw [`ClientEvent`] stream the pump produces
    /// (rejections, conflicts, aggregate values, non-fatal errors). Lagging
    /// receivers drop the oldest events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.shared.events.subscribe()
    }

    /// The client's event stream the background tasks publish into (R76),
    /// and the device wipe once the client closed (R105).
    #[cfg(feature = "device-identity")]
    pub(crate) fn event_sender(&self) -> broadcast::Sender<ClientEvent> {
        self.shared.events.clone()
    }

    /// Postfix-free spelling of the [`Watchable`](crate::dsl::Watchable)
    /// verb: `client.live(query)` is `query.live(&client)`, with the handle
    /// type chosen at compile time from the query's shape. The `R` parameter
    /// is turbofish-friendly for row queries: `client.live::<_, Order>(q)`.
    ///
    /// # Errors
    ///
    /// See [`watch`](Self::watch) and [`watch_value`](Self::watch_value).
    pub async fn live<Q, R>(&self, query: Q) -> Result<Q::Handle, ClientError>
    where
        Q: crate::dsl::Watchable<T, R>,
    {
        query.live(self).await
    }
}

/// A configured reconnect driver: the factory, the sleeper, and the policy.
struct ReconnectDriver<F, S> {
    factory: F,
    sleeper: S,
    policy: ReconnectPolicy,
}

/// Whether a pump-step failure means the transport is gone (and a reconnect
/// driver should take over) rather than a local fault.
fn is_disconnect(err: &ClientError) -> bool {
    // A send attempted with no socket is the same situation as one whose socket
    // died: there is a transport to go and find, and the run continues either
    // way. Anything else is a genuine fault and ends the pump.
    matches!(err, ClientError::Transport(_) | ClientError::NotConnected)
}

/// Auto-submit local writes before a queue read. A lost transport does not
/// fail the read, since the entry is already durable, and the pump's
/// reconnect replays it.
async fn flush_before_read<T>(conn: &mut ConnettoConnection<T>) -> Result<(), ClientError>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    match conn.flush().await {
        Ok(_) => Ok(()),
        Err(err) if is_disconnect(&err) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Decrement one reference on the wire subscription `wire_id`. When the last
/// sharer drops, report its id so the grace countdown can start.
///
/// The entry stays in the set at zero references rather than being removed,
/// which is what lets a re-watch inside the grace re-claim it instead of
/// minting a second subscription for the same query and paying a fresh
/// snapshot. It leaves the set only when its grace runs out.
fn release_wire(wire: &mut [WireSub], wire_id: &str, released: &mut Vec<String>) {
    if let Some(entry) = wire.iter_mut().find(|w| w.wire_id == wire_id) {
        entry.refs -= 1;
        if entry.refs == 0 {
            released.push(entry.wire_id.clone());
        }
    }
}

/// The id space for shared wire subscriptions, distinct from handle ids.
const WIRE_PREFIX: &str = "wire-";

/// Attach a handle to the wire subscription for `spec`, sharing an existing
/// one (increment its ref count) or declaring a new one (subscribe once, ref
/// count 1). Returns the wire id. An aggregate handle bootstraps from the
/// resting table rather than from a cached last value here (R83).
///
/// `holder` names the handle id or pin name that holds the watch, and the
/// debug trace pairs it with the wire id.
async fn attach_wire<T>(
    state: &mut State<T>,
    shared: &Shared<T>,
    holder: &str,
    spec: SubscriptionSpec,
    grace: Duration,
) -> Result<String, ClientError>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    let wire_id = if let Some(existing) = state.wire.iter_mut().find(|w| w.spec == spec) {
        let reclaimed = existing.refs == 0;
        existing.refs += 1;
        let wire_id = existing.wire_id.clone();
        if reclaimed {
            // Held again, so the countdown stops. The server still has this
            // subscription, so nothing goes on the wire and no snapshot is
            // paid, which is the whole point of the grace.
            state.conn.hold_subscription(&wire_id)?;
        }
        wire_id
    } else {
        let seq = shared.next_wire.fetch_add(1, Ordering::Relaxed);
        let wire_id = format!("{WIRE_PREFIX}{seq}");
        state
            .conn
            .subscribe_spec_with_grace(&wire_id, spec.clone(), grace)
            .await?;
        state.wire.push(WireSub {
            wire_id: wire_id.clone(),
            spec,
            refs: 1,
        });
        wire_id
    };
    tracing::debug!(holder, wire_id, "watch attached");
    Ok(wire_id)
}

/// Drain the reaper queue: remove each dropped handle's driver-side entry,
/// then cancel the wire subscriptions that backed it. A local tier handle has
/// none, so its retirement is purely local. Entries are removed before any
/// send, so a transport failure can never resurrect a dropped handle. Each
/// watch whose last handle went raises [`ClientEvent::WatchReleased`] once its
/// grace has started.
async fn drain_dropped<T>(
    state: &mut State<T>,
    reaper: &Reaper,
    events: &broadcast::Sender<ClientEvent>,
) -> Result<(), ClientError>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    let pending = {
        let mut queue = match reaper.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        core::mem::take(&mut *queue)
    };
    let mut released: Vec<String> = Vec::new();
    for sub_id in pending {
        if let Some(pos) = state.registry.iter().position(|e| e.sub_id == sub_id) {
            let entry = state.registry.remove(pos);
            for wire_id in entry.wire_ids {
                release_wire(&mut state.wire, &wire_id, &mut released);
            }
        }
        if let Some(pos) = state.computed.iter().position(|e| e.sub_id == sub_id) {
            let entry = state.computed.remove(pos);
            release_wire(&mut state.wire, &entry.wire_id, &mut released);
        }
        state.unsynced.retain(|entry| entry.sub_id != sub_id);
    }
    // The last handle dropping starts a countdown, it does not end the
    // subscription. Navigating away and back inside the grace re-claims the
    // record and pays no fresh snapshot, which is the whole point of the
    // grace. A pin ignores this: it has no handle to drop.
    for sub_id in released {
        state.conn.release_subscription(&sub_id)?;
        let _ = events.send(ClientEvent::WatchReleased { sub_id });
    }
    // Anything whose grace has since run out ends here, which is the only
    // place a watch is ever unsubscribed. Nothing runs on a timer: the pass
    // happens whenever the pump next steps, and an expiry is a comparison.
    //
    // Only when something could actually have expired, which is exactly when
    // some entry is unheld: a countdown starts at the last drop, and a record
    // inherited from a previous run is seeded unheld. With every watch held
    // this costs nothing, which matters because the pump steps per frame and
    // the replica is a real file on a browser's storage.
    if !state.wire.iter().any(|w| w.refs == 0) {
        return Ok(());
    }
    // A subscription this run still holds a handle on is never retired, however
    // the record reads. The record carries the durable claim and the reference
    // count carries the handles, and an unpin arriving while a handle is open
    // would otherwise unsubscribe a live query out from under it.
    //
    // Grace expiry and unpin both surface here as an expired record. The whole
    // pass waits while the transport is down: the record is left in place so the
    // next connected step retires it, because a row evicted offline could not be
    // re-fetched (R15 D3).
    if state.conn.is_connected() {
        for sub_id in state.conn.expired_subscriptions()? {
            if state.wire.iter().any(|w| w.wire_id == sub_id && w.refs > 0) {
                continue;
            }
            // The raw unsubscribe evicts rows scoped to this subscription's
            // tables before the record goes, sparing rows a survivor or a
            // pending write still wants. The retain follows success so a
            // failure leaves the refs == 0 entry in place and the next drain
            // retries it rather than stranding the expired record.
            state.conn.unsubscribe(&sub_id).await?;
            state.wire.retain(|w| w.wire_id != sub_id);
        }
    }
    Ok(())
}

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
type KeySleep = core::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
type KeySleep = core::pin::Pin<Box<dyn Future<Output = ()> + 'static>>;

fn arm_share_key_timer<S: Sleeper + 'static>(
    timer: Option<ShareKeyTimer>,
    armed: &mut Option<(ShareKeyTimer, KeySleep)>,
    sleeper: Option<&mut S>,
) {
    if timer == armed.as_ref().map(|(timer, _)| *timer) {
        return;
    }
    *armed = None;
    let Some(timer) = timer else {
        return;
    };
    let sleep = sleeper.map(|sleeper| Box::pin(sleeper.sleep(timer.wait())) as KeySleep);
    #[cfg(feature = "native-transport")]
    let sleep = sleep.or_else(|| Some(Box::pin(tokio::time::sleep(timer.wait())) as KeySleep));
    *armed = sleep.map(|sleep| (timer, sleep));
}

async fn wait_timer<D, F: Future<Output = ()> + ?Sized>(
    timer: &mut Option<(D, core::pin::Pin<Box<F>>)>,
) {
    if let Some((_, sleep)) = timer {
        sleep.as_mut().await;
    } else {
        core::future::pending::<()>().await;
    }
}

/// The background pump: drains queued unsubscribes, flushes local writes,
/// takes one cancellable pump step, then refreshes every live query whose
/// tables changed. When the last [`ConnettoClient`] clone drops, the pump
/// closes the connection gracefully (transport close handshake) and exits.
///
/// A transport drop ends the pump, unless a reconnect driver is present, in
/// which case the pump recovers: backoff, fresh transport, session resume,
/// re-declared subscriptions. Local faults (session, apply, protocol) stay
/// terminal either way and are announced as [`ClientEvent::Stopped`], as is
/// a refusal the build can never attach under.
#[expect(
    clippy::too_many_lines,
    reason = "one loop arms the resend, keepalive and share-key timers beside the gate prompt"
)]
async fn pump<T, F, S>(
    shared: Arc<Shared<T>>,
    alive: Weak<ClientToken>,
    mut reconnect: Option<ReconnectDriver<F, S>>,
) where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    F: TransportFactory<Transport = T> + MaybeSend + 'static,
    S: Sleeper + MaybeSend + 'static,
{
    let mut needs_recovery = false;
    // A live close of the terminal class names the refusal the redial would only repeat.
    let mut last_close: Option<FatalErrorReason> = None;
    // The sleep for the armed resend, kept across steps so arriving frames do not restart it.
    let mut resend = None;
    // The sleep for the armed keepalive deadline, re-armed when a frame moves it.
    let mut keepalive = None;
    let mut key_timer = None;
    // The drain's owed dial retains its sleep across pump steps.
    let mut drain = None;
    // The courier's owed dial retains its sleep across pump steps.
    let mut courier_drain = None;
    loop {
        if alive.upgrade().is_none() {
            pump_exit(&shared).await;
            return;
        }

        // A close was asked for, so end as the last clone's drop would, with
        // no redial, while the other clones keep their handles.
        if shared.close_requested.load(Ordering::Acquire) {
            pump_exit(&shared).await;
            return;
        }

        // The previous iteration lost the transport. Recover here, outside
        // the state lock, so watch and with_conn callers are only blocked
        // during the brief resume itself, never during backoff sleeps.
        if needs_recovery {
            match drive_recovery(
                &shared,
                &mut reconnect,
                &alive,
                &mut key_timer,
                last_close.take(),
            )
            .await
            {
                Recovered::Live => needs_recovery = false,
                // The top of the loop ends the pump through the close path.
                Recovered::Closing => continue,
                Recovered::Ended(event) => {
                    {
                        let mut state = shared.state.lock().await;
                        state.conn.finish_courier_audit();
                        if let Err(err) = state.conn.flush().await {
                            tracing::warn!(error = %err, "queueing the last writes at exit failed");
                        }
                    }
                    let _ = shared.events.send(event);
                    pump_finished(&shared);
                    return;
                }
                Recovered::Failed(err) => {
                    pump_fail(&shared, err).await;
                    return;
                }
            }
        }

        let mut state = shared.state.lock().await;

        match drain_and_flush(&mut state, &shared, reconnect.is_some()).await {
            Drained::Step => {}
            Drained::Interrupted => continue,
            Drained::Recover => {
                needs_recovery = true;
                continue;
            }
            Drained::Fail(err) => {
                drop(state);
                pump_fail(&shared, err).await;
                return;
            }
        }

        // One cancellable pump step. A wake interrupts the idle wait so lock
        // waiters (watch, with_conn, drops) get in promptly, and the armed
        // resend of deferred writes and the armed keepalive deadline interrupt
        // it when they are due. The keepalive runs while the gate is locked,
        // because the away gate holds the app, not the socket. With no driver
        // there is no sleeper, so deferred writes and keepalive waits hold for
        // the next attach.
        let armed = state.conn.resend_timer();
        if armed != resend.as_ref().map(|(timer, _)| *timer) {
            resend = armed
                .zip(reconnect.as_mut())
                .map(|(timer, driver)| (timer, Box::pin(driver.sleeper.sleep(timer.wait()))));
        }
        let ka_armed = state.conn.keepalive_timer();
        if ka_armed != keepalive.as_ref().map(|(timer, _)| *timer) {
            keepalive = ka_armed
                .zip(reconnect.as_mut())
                .map(|(timer, driver)| (timer, Box::pin(driver.sleeper.sleep(timer.wait()))));
        }
        arm_share_key_timer(
            state.conn.share_key_timer(),
            &mut key_timer,
            reconnect.as_mut().map(|driver| &mut driver.sleeper),
        );
        let owed = state.conn.drain_timer();
        if owed != drain.as_ref().map(|(timer, _)| *timer) {
            drain = owed
                .zip(reconnect.as_mut())
                .map(|(timer, driver)| (timer, Box::pin(driver.sleeper.sleep(timer.wait()))));
        }
        #[cfg(feature = "peer")]
        let courier_owed = state.conn.courier_drain_timer();
        #[cfg(feature = "peer")]
        if courier_owed != courier_drain.as_ref().map(|(timer, _)| *timer) {
            courier_drain = courier_owed
                .zip(reconnect.as_mut())
                .map(|(timer, driver)| (timer, Box::pin(driver.sleeper.sleep(timer.wait()))));
        }
        // Drive the gate's prompt, if one is pending, alongside the frame wait.
        let mut ask = shared.gate.take_ask();
        let mut ask_outcome = None;
        let (stepped, drain_due, courier_due) = step_or_resend_or_keepalive(
            &mut state.conn,
            &shared.wake,
            &mut resend,
            &mut keepalive,
            &mut key_timer,
            &mut drain,
            &mut courier_drain,
            &mut ask,
            &mut ask_outcome,
        )
        .await;
        // A resolved prompt is applied under the state lock, and an unresolved
        // one goes back whatever the step ended in, so a recovery or an exit
        // never strands the gate mid-prompt. Approval resumes access and held
        // refreshes, and a dismissal stays locked for the next return or
        // unlock call.
        shared.gate.apply_outcome(&mut ask, ask_outcome);
        if let Some(ask) = ask {
            shared.gate.restore_ask(ask);
        }
        if let Ok(Some(ClientEvent::ServerClosed { reason })) = &stepped {
            last_close = Some(reason.clone());
        }
        match route_step_result(&mut state, &shared, stepped, reconnect.is_some()) {
            PumpFlow::Proceed => {}
            PumpFlow::Recover => {
                needs_recovery = true;
                continue;
            }
            PumpFlow::Exit => {
                end_unresumed(&mut state, &shared).await;
                return;
            }
            PumpFlow::Fail(err) => {
                drop(state);
                pump_fail(&shared, err).await;
                return;
            }
        }

        // The first cursor is the moment "empty" stops meaning "never
        // fetched", so it is recorded here rather than beside the refresh
        // below, which an empty first sync gives nothing to do.
        if !shared.ever_synced.load(Ordering::Relaxed) && state.conn.has_ever_synced() {
            shared.ever_synced.store(true, Ordering::Relaxed);
        }

        // Refresh live queries whose tables changed, from server patches and
        // local writes alike, while the gate is open. A lock holds the
        // refreshes and the aggregate pushes, which accumulate until the first
        // open refresh drains them, waking each affected handle once.
        if !shared.gate.is_locked() {
            refresh_changed(&mut state, &shared.events);
        }

        // The drain's dial fell due. Dialed outside the state lock, like a
        // reconnect, and handed over under it.
        let mut drain_owed = None;
        if drain_due && let Some((timer, _)) = drain.take() {
            drain_owed = Some(timer);
        }
        #[cfg(feature = "peer")]
        let mut courier_owed = None;
        #[cfg(feature = "peer")]
        if courier_due && let Some((timer, _)) = courier_drain.take() {
            courier_owed = Some(timer);
        }
        #[cfg(feature = "peer")]
        let dial_now = drain_owed.is_some() || courier_owed.is_some();
        #[cfg(not(feature = "peer"))]
        let dial_now = drain_owed.is_some();
        if dial_now {
            drop(state);
        }
        if let Some(timer) = drain_owed {
            dial_owed(&shared, &mut reconnect, OwedWire::Drain, timer).await;
        }
        #[cfg(feature = "peer")]
        if let Some(timer) = courier_owed {
            dial_owed(&shared, &mut reconnect, OwedWire::Courier, timer).await;
        }
        #[cfg(not(feature = "peer"))]
        let _ = courier_due;
    }
}

/// The wire an owed dial dials, its attach and back-off named by kind.
#[derive(Clone, Copy)]
enum OwedWire {
    /// The drain wire its dial has fallen due.
    Drain,
    /// The courier wire its dial has fallen due (R78 decision 47).
    #[cfg(feature = "peer")]
    Courier,
}

impl OwedWire {
    /// The log line the wire's failed attach stands under.
    const fn attach_log(self) -> &'static str {
        match self {
            OwedWire::Drain => "attaching the drain failed",
            #[cfg(feature = "peer")]
            OwedWire::Courier => "attaching the courier wire failed",
        }
    }

    /// The log line the wire's failed dial stands under.
    const fn dial_failed_log(self) -> &'static str {
        match self {
            OwedWire::Drain => "dialing the drain failed, backing off",
            #[cfg(feature = "peer")]
            OwedWire::Courier => "dialing the courier wire failed, backing off",
        }
    }

    /// Attach a fresh `transport` to the wire, or report a failed dial to
    /// its back-off.
    async fn attach_or_back_off<E, T>(
        self,
        conn: &mut ConnettoConnection<T>,
        timer: DrainTimer,
        dialed: Result<T, E>,
    ) where
        T: Transport,
        T::Error: core::fmt::Display,
        E: core::fmt::Display,
    {
        match dialed {
            Ok(transport) => {
                let outcome = match self {
                    OwedWire::Drain => conn.attach_drain(timer, transport).await,
                    #[cfg(feature = "peer")]
                    OwedWire::Courier => conn.attach_courier_drain(timer, transport).await,
                };
                if let Err(err) = outcome {
                    tracing::warn!(error = %err, "{}", self.attach_log());
                }
            }
            Err(err) => {
                tracing::debug!(error = %err, "{}", self.dial_failed_log());
                match self {
                    OwedWire::Drain => conn.drain_dial_failed(timer),
                    #[cfg(feature = "peer")]
                    OwedWire::Courier => conn.courier_drain_dial_failed(timer),
                }
            }
        }
    }
}

/// Dials the wire its dial has fallen due over the reconnect driver's
/// factory, the fresh transport handed to the wire's attach (R31).
async fn dial_owed<T, F, S>(
    shared: &Shared<T>,
    reconnect: &mut Option<ReconnectDriver<F, S>>,
    wire: OwedWire,
    timer: DrainTimer,
) where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    F: TransportFactory<Transport = T> + MaybeSend + 'static,
    S: Sleeper + MaybeSend + 'static,
{
    let Some(driver) = reconnect.as_mut() else {
        return;
    };
    let dialed = driver.factory.connect().await;
    let mut state = shared.state.lock().await;
    wire.attach_or_back_off(&mut state.conn, timer, dialed)
        .await;
}

/// The running client as the enrolment task sees it.
#[cfg(feature = "device-identity")]
struct EnrolLink<T: Transport> {
    shared: Arc<Shared<T>>,
    alive: Weak<ClientToken>,
}

#[cfg(feature = "device-identity")]
impl<T: Transport> Clone for EnrolLink<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            alive: Weak::clone(&self.alive),
        }
    }
}

#[cfg(feature = "device-identity")]
impl<T> crate::enrolment::Link for EnrolLink<T>
where
    T: Transport + Send + 'static,
    T::Error: core::fmt::Display,
{
    fn alive(&self) -> bool {
        self.alive.strong_count() > 0 && !self.shared.pump_done.done.load(Ordering::Acquire)
    }

    fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.shared.events.subscribe()
    }

    fn emit(&self, event: ClientEvent) {
        let _ = self.shared.events.send(event);
    }

    async fn ended(&self) {
        loop {
            let notified = self.shared.pump_done.wake.notified();
            if self.shared.pump_done.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    async fn connection(&self) -> Option<crate::ConnectionNumber> {
        self.shared
            .lock_interrupting()
            .await
            .conn
            .connection_number()
    }

    async fn ask(
        &self,
        request_id: String,
        msg: connetto_core::messages::ControlMessage,
    ) -> Result<tokio::sync::oneshot::Receiver<crate::enrolment::Answer>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.ask_enrolment(request_id, msg).await
    }

    async fn store(&self, held: crate::enrolment::Held) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.store_device_certificate(&held).await
    }

    async fn forget(&self) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.delete_device_certificate()
    }

    async fn store_list(&self, kept: crate::enrolment::KeptList) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.store_revocation_list(&kept)
    }

    #[cfg(feature = "peer")]
    async fn offer(
        &self,
        key_id: [u8; 32],
    ) -> Result<Option<connetto_core::exchange::Offer>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.own_offer(key_id)
    }

    #[cfg(feature = "peer")]
    async fn build(&self) -> Option<(connetto_core::Major, connetto_core::SchemaVersion)> {
        self.shared.lock_interrupting().await.conn.own_build()
    }

    #[cfg(feature = "peer")]
    async fn serve_range(
        &self,
        key_id: [u8; 32],
        from: u64,
        to: u64,
        build: (connetto_core::Major, connetto_core::SchemaVersion),
    ) -> Result<Option<crate::exchange::Served>, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.serve_range(key_id, from, to, &build)
    }

    #[cfg(feature = "peer")]
    async fn serve_body(&self, key_id: [u8; 32], seq: u64) -> Result<Option<Vec<u8>>, ClientError> {
        self.shared
            .lock_interrupting()
            .await
            .conn
            .serve_body(key_id, seq)
    }

    #[cfg(feature = "peer")]
    async fn peer_cursor(
        &self,
        author: Vec<u8>,
    ) -> Result<connetto_core::exchange::Frontier, ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.peer_cursor(&author)
    }

    #[cfg(feature = "peer")]
    async fn adopt(
        &self,
        author: crate::peer::PeerAuthor,
        offer: connetto_core::exchange::SignedOffer,
        cursor: connetto_core::exchange::Frontier,
        expires_at: std::time::SystemTime,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        let adopted = state.conn.adopt_peer_offer(
            &author,
            &offer,
            &cursor,
            crate::exchange::unix_secs(expires_at),
        );
        drop(state);
        // The pump hands on what the adopt raised, offline too.
        self.shared.wake.notify_one();
        adopted
    }

    #[cfg(feature = "peer")]
    async fn renew(
        &self,
        author: Vec<u8>,
        expires_at: std::time::SystemTime,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state
            .conn
            .renew_peer_expiry(&author, crate::exchange::unix_secs(expires_at))
    }

    #[cfg(feature = "peer")]
    async fn take_row(
        &self,
        author: crate::peer::PeerAuthor,
        row: connetto_core::exchange::ManifestRow,
        body: Option<Vec<u8>>,
        cursor: connetto_core::exchange::Frontier,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        let taken = state
            .conn
            .take_peer_row(&author, &row, body.as_deref(), &cursor);
        drop(state);
        // The pump hands on what the apply raised, offline too.
        self.shared.wake.notify_one();
        taken
    }
}

#[cfg(feature = "device-identity")]
impl<T> ConnettoClient<T>
where
    T: Transport + Send + 'static,
    T::Error: core::fmt::Display,
{
    /// The certificate the replica holds, read under the state lock and never
    /// refused by the gate, since it is connetto's own bookkeeping.
    pub(crate) async fn stored_certificate(
        &self,
    ) -> Result<Option<crate::enrolment::Held>, ClientError> {
        self.shared
            .lock_interrupting()
            .await
            .conn
            .device_certificate()
    }

    /// The revocation lists the replica keeps, and the lists the server
    /// pushes from now on, taken before the pump runs so none is missed.
    pub(crate) async fn revocation_inbox(
        &self,
    ) -> Result<(Vec<crate::enrolment::KeptList>, crate::enrolment::ListInbox), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        let kept = state.conn.revocation_lists()?;
        Ok((kept, state.conn.subscribe_revocations()))
    }

    /// The enrolment task beside this client's pump, ending with it.
    pub(crate) fn enrolment(
        &self,
        enroller: crate::enrolment::Enroller,
    ) -> impl Future<Output = ()> + Send + 'static {
        crate::enrolment::run(
            EnrolLink {
                shared: Arc::clone(&self.shared),
                alive: Arc::downgrade(&self.token),
            },
            enroller,
        )
    }

    /// Give the shared connection the device's keys and the enrolment task's
    /// steering, so a device writer can be bound (R75). The bind a
    /// restarted device owed at its handshake arms here, on the live wire, now
    /// that the keys are handed over.
    ///
    /// # Errors
    ///
    /// [`ClientError`] when the deferred bind cannot be signed or sent.
    pub(crate) async fn with_device_writer(
        &self,
        keys: Arc<dyn crate::enrolment::DeviceKeys>,
        steering: tokio::sync::mpsc::UnboundedSender<crate::enrolment::Command>,
    ) -> Result<(), ClientError> {
        let mut state = self.shared.lock_interrupting().await;
        state.conn.with_device_writer(keys, steering).await
    }
}

/// The pump's per-iteration control flow.
enum PumpFlow {
    /// Proceed to the next step.
    Proceed,
    /// The transport is gone. Recover at the top of the next iteration.
    Recover,
    /// The session ended with no driver to resume it.
    Exit,
    /// A fault no reconnect can cure ended the pump.
    Fail(ClientError),
}

/// What the pump does once dropped handles are drained and local writes
/// flushed. A session only ends on a step, so draining never ends the pump.
enum Drained {
    /// Take the pump step.
    Step,
    /// Release the connection lock for the caller that woke the driver.
    Interrupted,
    /// The transport is gone. Recover at the top of the next iteration.
    Recover,
    /// A fault no reconnect can cure ended the pump.
    Fail(ClientError),
}

/// Queue the writes made meanwhile for the next run, then end the pump, when
/// the session ended with no driver to resume it.
async fn end_unresumed<T>(state: &mut State<T>, shared: &Shared<T>)
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    state.conn.finish_courier_audit();
    if let Err(err) = state.conn.flush().await {
        tracing::warn!(error = %err, "queueing the last writes at exit failed");
    }
    pump_finished(shared);
}

/// End the pump on a fault, queueing what can be queued, closing the
/// transport, releasing every live handle with the fault, then announcing it
/// and `Closed`.
async fn pump_fail<T>(shared: &Arc<Shared<T>>, err: ClientError)
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    let detail = err.to_string();
    tracing::warn!(error = %detail, "the client pump stopped on a fault");
    // Set before the senders drop, so a woken handle reads the fault.
    let _ = shared.reaper.stopped.set(detail.clone());
    let mut state = shared.state.lock().await;
    state.conn.finish_courier_audit();
    if let Err(err) = state.conn.flush().await {
        tracing::warn!(error = %err, "queueing the last writes at the fault failed");
    }
    let _ = state.conn.close().await;
    // The closed socket is gone, so nothing later sends on it or reads the
    // connection as live.
    state.conn.disconnect();
    state.registry.clear();
    state.computed.clear();
    drop(state);
    let _ = shared.events.send(ClientEvent::Stopped { detail });
    let _ = shared.events.send(ClientEvent::Closed);
    pump_finished(shared);
}

/// Queue the last writes, close the transport, announce `Closed`, and end
/// the pump, as the last clone's drop would.
async fn pump_exit<T>(shared: &Arc<Shared<T>>)
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    let mut state = shared.state.lock().await;
    state.conn.finish_courier_audit();
    // A write made just before the close is queued before the socket goes.
    if let Err(err) = state.conn.flush().await {
        tracing::warn!(error = %err, "queueing the last writes at close failed");
    }
    let _ = state.conn.close().await;
    drop(state);
    let _ = shared.events.send(ClientEvent::Closed);
    pump_finished(shared);
}

/// How a recovery ended, as the pump acts on it.
enum Recovered {
    /// The transport is live again.
    Live,
    /// A close was asked for while the pump waited to redial.
    Closing,
    /// Recovery cannot proceed, and this terminal event says why.
    Ended(ClientEvent),
    Failed(ClientError),
}

/// Recover a lost transport, outside the state lock.
async fn drive_recovery<T, F, S>(
    shared: &Arc<Shared<T>>,
    reconnect: &mut Option<ReconnectDriver<F, S>>,
    alive: &Weak<ClientToken>,
    key_timer: &mut Option<(ShareKeyTimer, KeySleep)>,
    last_close: Option<FatalErrorReason>,
) -> Recovered
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    F: TransportFactory<Transport = T> + MaybeSend + 'static,
    S: Sleeper + MaybeSend + 'static,
{
    // The loss queued its status for the next pump step, which a redial that
    // keeps failing never reaches, so the app hears it before the backoff.
    {
        let mut state = shared.state.lock().await;
        while let Some(notice) = state.conn.take_notice() {
            let _ = route_step_result(&mut state, shared, Ok(Some(notice)), reconnect.is_some());
        }
    }
    let outcome = match reconnect.as_mut() {
        Some(driver) => recover(shared, driver, alive, key_timer, last_close).await,
        None => Recovery::Exhausted,
    };
    match outcome {
        Recovery::Live => Recovered::Live,
        Recovery::Closing => Recovered::Closing,
        Recovery::ReauthRequired => Recovered::Ended(ClientEvent::AuthenticationRequired),
        Recovery::Mismatched(err) => {
            let detail = err.to_string();
            // Set before the senders drop, so a woken handle reads the fault.
            let _ = shared.reaper.stopped.set(detail.clone());
            let _ = shared.events.send(ClientEvent::Stopped { detail });
            Recovered::Ended(ClientEvent::Closed)
        }
        Recovery::Exhausted => Recovered::Ended(ClientEvent::Closed),
        Recovery::Failed(err) => Recovered::Failed(err),
    }
}

/// Unsubscribe dropped handles, seek a missing socket, and auto-submit local
/// writes, before the pump step.
async fn drain_and_flush<T>(
    state: &mut State<T>,
    shared: &Arc<Shared<T>>,
    has_driver: bool,
) -> Drained
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    let maintenance = tokio::select! {
        biased;
        result = state.conn.process_courier_audit() => result,
        () = shared.wake.notified() => return Drained::Interrupted,
    };
    if let Err(err) = maintenance {
        return if is_disconnect(&err) {
            Drained::Recover
        } else {
            Drained::Fail(err)
        };
    }
    match state
        .conn
        .process_share_keys(!shared.gate.is_locked())
        .await
    {
        Ok(true) if !shared.gate.is_locked() => refresh_changed(state, &shared.events),
        Ok(_) => {}
        Err(err) => {
            if !shared.gate.is_locked() {
                refresh_changed(state, &shared.events);
            }
            return if is_disconnect(&err) {
                Drained::Recover
            } else {
                Drained::Fail(err)
            };
        }
    }
    // Unsubscribes queued by dropped handles. A send failure only marks the
    // transport for recovery, because the server-side subscription dies with
    // the session either way, and the entry is already out of the registry, so
    // nothing re-declares it.
    if let Err(err) = drain_dropped(&mut *state, &shared.reaper, &shared.events).await {
        if is_disconnect(&err) {
            return Drained::Recover;
        }
        return Drained::Fail(err);
    }
    // Auto-submit local writes committed since the last step. With no socket
    // this only queues them durably, which must not wait for a server.
    if let Err(err) = state.conn.flush().await {
        if is_disconnect(&err) {
            return Drained::Recover;
        }
        return Drained::Fail(err);
    }
    // No socket and a driver to find one, so go and find it. With no driver
    // there is nothing to recover to, so the pump falls through and parks,
    // which keeps device-private queries refreshing with no server at all.
    if has_driver && !state.conn.is_connected() {
        return Drained::Recover;
    }
    Drained::Step
}

/// Route one pump step's result. A deliberate close tells the app why and
/// takes the recovery path, a plain close does the same, an aggregate is
/// routed and forwarded, and a lost transport marks recovery.
fn route_step_result<T>(
    state: &mut State<T>,
    shared: &Shared<T>,
    stepped: Result<Option<ClientEvent>, ClientError>,
    has_driver: bool,
) -> PumpFlow
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    match stepped {
        // A deliberate server close tells the app why, then takes the same
        // path a dropped transport takes. The socket is gone either way.
        Ok(Some(event @ ClientEvent::ServerClosed { .. })) => {
            let _ = shared.events.send(event);
            if has_driver {
                PumpFlow::Recover
            } else {
                let _ = shared.events.send(ClientEvent::Closed);
                PumpFlow::Exit
            }
        }
        Ok(Some(ClientEvent::Closed)) => {
            if has_driver {
                PumpFlow::Recover
            } else {
                let _ = shared.events.send(ClientEvent::Closed);
                PumpFlow::Exit
            }
        }
        Ok(Some(event)) => {
            match &event {
                ClientEvent::Aggregate { sub_id, change } => {
                    route_computed(
                        state,
                        &shared.gate,
                        &shared.events,
                        sub_id,
                        Moved::of(change),
                    );
                }
                // New columns may have dropped the resting table, and a resync
                // of a watched answer cleared it, so the handles re-read it
                // whole until the server's next push rebuilds it (R95).
                ClientEvent::AggregateColumns { sub_id, .. }
                | ClientEvent::FullResync { sub_id, .. } => {
                    route_computed(state, &shared.gate, &shared.events, sub_id, Moved::All);
                }
                _ => {}
            }
            if let ClientEvent::NonFatal {
                related_to: Some(wire_id),
                detail,
            } = &event
                && (detail == SUBSCRIPTION_REFUSED || detail == SUBSCRIPTION_REFUSED_SCHEMA)
            {
                note_wire_outcome(state, &shared.reaper, wire_id, true);
            }
            if let ClientEvent::SnapshotEnd { sub_id } = &event {
                // The server re-served the sub, whatever it refused before,
                // so the handles riding it wait for data again.
                note_wire_outcome(state, &shared.reaper, sub_id, false);
            }
            if !apply_relayed_gate(&shared.gate, &event) {
                let _ = shared.events.send(event);
            }
            PumpFlow::Proceed
        }
        Ok(None) => PumpFlow::Proceed,
        Err(err) => {
            if is_disconnect(&err) {
                PumpFlow::Recover
            } else {
                PumpFlow::Fail(err)
            }
        }
    }
}

/// Set or clear the refusal mark of every live handle that rides one wire
/// subscription. A refused sub makes each `changed` return the refusal instead
/// of waiting for a snapshot that never arrives, and a re-served one makes the
/// handles wait for data again.
fn note_wire_outcome<T: Transport>(
    state: &State<T>,
    reaper: &Reaper,
    wire_id: &str,
    refused: bool,
) {
    let note = |reaper: &Reaper, sub_id: &str| {
        if refused {
            reaper.record_refusal(sub_id);
        } else {
            reaper.forget_refusal(sub_id);
        }
    };
    for entry in &state.registry {
        if entry.wire_ids.iter().any(|id| id == wire_id) {
            note(reaper, &entry.sub_id);
        }
    }
    for entry in &state.computed {
        if entry.wire_id == wire_id {
            note(reaper, &entry.sub_id);
        }
    }
}

/// Apply a gate state a relay stated, when this client has no mechanism of
/// its own, and say whether it did. The controller emits the change itself,
/// so the frame's own event is not sent a second time.
fn apply_relayed_gate(gate: &GateController, event: &ClientEvent) -> bool {
    if gate.is_armed() {
        return false;
    }
    match event {
        ClientEvent::Locked => {
            gate.set_locked(true);
            true
        }
        ClientEvent::Unlocked => {
            gate.set_locked(false);
            true
        }
        _ => false,
    }
}

/// The pump's single exit step, which signals a waiting
/// [`ConnettoClient::close`] that the pump has ended.
fn pump_finished<T: Transport>(shared: &Shared<T>) {
    shared.pump_done.done.store(true, Ordering::Release);
    shared.pump_done.wake.notify_waiters();
}

/// One cancellable pump step that also ends when the armed resend is due, the
/// armed keepalive deadline is due, the drain's dial is due, or the gate's
/// prompt resolves, and then sends the deferred writes again and fires the
/// keepalive deadline.
///
/// A due resend or deadline means the cancel won, so the step took no frame
/// and an error from either loses none. A due drain dial is returned for the
/// caller to dial outside the state lock (R31). A resolved prompt is
/// captured in `ask_outcome` for the caller to apply under the state lock.
#[expect(
    clippy::too_many_arguments,
    reason = "The cancellation boundary borrows independently pinned timers and gate state."
)]
async fn step_or_resend_or_keepalive<T, F, G>(
    conn: &mut ConnettoConnection<T>,
    wake: &Notify,
    resend: &mut Option<(ResendTimer, core::pin::Pin<Box<F>>)>,
    keepalive: &mut Option<(KeepaliveTimer, core::pin::Pin<Box<F>>)>,
    key_timer: &mut Option<(ShareKeyTimer, KeySleep)>,
    drain: &mut Option<(DrainTimer, core::pin::Pin<Box<G>>)>,
    courier_drain: &mut Option<(DrainTimer, core::pin::Pin<Box<G>>)>,
    ask: &mut Option<GateAskFuture>,
    ask_outcome: &mut Option<GateAskOutcome>,
) -> (Result<Option<ClientEvent>, ClientError>, bool, bool)
where
    T: Transport,
    T::Error: core::fmt::Display,
    F: Future<Output = ()>,
    G: Future<Output = ()>,
{
    let resend_due = AtomicBool::new(false);
    let ka_due = AtomicBool::new(false);
    let key_due = AtomicBool::new(false);
    let drain_due = AtomicBool::new(false);
    let courier_due = AtomicBool::new(false);
    let cancel = async {
        tokio::select! {
            biased;
            () = wake.notified() => {}
            () = wait_timer(resend) => resend_due.store(true, Ordering::Relaxed),
            () = wait_timer(keepalive) => ka_due.store(true, Ordering::Relaxed),
            () = wait_timer(key_timer) => key_due.store(true, Ordering::Relaxed),
            () = wait_timer(drain) => drain_due.store(true, Ordering::Relaxed),
            () = wait_timer(courier_drain) => courier_due.store(true, Ordering::Relaxed),
            outcome = async {
                match ask.as_mut() {
                    Some(ask) => ask.as_mut().await,
                    None => core::future::pending().await,
                }
            } => *ask_outcome = Some(outcome),
        }
    };
    let stepped = conn.pump_one_or(cancel).await;
    if key_due.load(Ordering::Relaxed) {
        *key_timer = None;
    }
    if resend_due.load(Ordering::Relaxed)
        && let Some((timer, _)) = resend.take()
        && let Err(err) = conn.resend_deferred(timer).await
    {
        return (Err(err), false, false);
    }
    if ka_due.load(Ordering::Relaxed)
        && let Some((timer, _)) = keepalive.take()
        && let Err(err) = conn.keepalive_fire(timer).await
    {
        return (Err(err), false, false);
    }
    (
        stepped,
        drain_due.load(Ordering::Relaxed),
        courier_due.load(Ordering::Relaxed),
    )
}

/// Rebuild the computed answers a lock held, rebuild the unsynced snapshot
/// when it may have moved, then re-run every live query whose tables were
/// touched since the last step.
fn refresh_changed<T: Transport>(state: &mut State<T>, events: &broadcast::Sender<ClientEvent>) {
    for wire_id in core::mem::take(&mut state.held) {
        apply_computed(state, events, &wire_id, Moved::All);
    }
    refresh_unsynced(state, events);
    let changed = state.conn.take_changed_unfiltered();
    if changed.is_empty() {
        return;
    }
    let changed: HashSet<String> = changed.into_iter().map(|t| t.to_lowercase()).collect();
    let State {
        conn,
        registry,
        computed: _,
        wire: _,
        held: _,
        unsynced: _,
    } = state;
    for entry in registry.iter_mut() {
        if entry.tables.is_disjoint(&changed) {
            continue;
        }
        if let Err(err) = (entry.refresh)(conn) {
            let _ = events.send(ClientEvent::NonFatal {
                related_to: Some(entry.sub_id.clone()),
                detail: format!("live query refresh failed: {err}"),
            });
        }
    }
}

/// Rebuild the unsynced snapshot once when it may have moved, and wake each
/// handle following it whose snapshot it changed.
fn refresh_unsynced<T: Transport>(state: &mut State<T>, events: &broadcast::Sender<ClientEvent>) {
    if state.unsynced.is_empty() || !state.conn.take_unsynced_moved() {
        return;
    }
    let fresh = match state.conn.unsynced_work() {
        Ok(fresh) => fresh,
        Err(err) => {
            for entry in &state.unsynced {
                let _ = events.send(ClientEvent::NonFatal {
                    related_to: Some(entry.sub_id.clone()),
                    detail: format!("reading the unsynced work failed: {err}"),
                });
            }
            return;
        }
    };
    for entry in &state.unsynced {
        let unchanged = entry.work.read().is_ok_and(|current| *current == fresh);
        if unchanged {
            continue;
        }
        match entry.work.write() {
            Ok(mut work) => work.clone_from(&fresh),
            Err(poisoned) => poisoned.into_inner().clone_from(&fresh),
        }
        entry.changed.send_modify(|generation| *generation += 1);
    }
}

/// Route one movement of a computed answer to the handles its wire sub feeds,
/// or hold it while the gate is locked, to rebuild whole once it opens.
fn route_computed<T>(
    state: &mut State<T>,
    gate: &GateController,
    events: &broadcast::Sender<ClientEvent>,
    wire_id: &str,
    moved: Moved<'_>,
) where
    T: Transport,
{
    if !state.computed.iter().any(|entry| entry.wire_id == wire_id) {
        return;
    }
    if !gate.is_locked() {
        apply_computed(state, events, wire_id, moved);
    } else if !state.held.iter().any(|held| held == wire_id) {
        state.held.push(wire_id.to_owned());
    }
}

/// Feed one movement of a computed answer to every handle its wire sub
/// feeds, each re-reading what moved through its own types.
///
/// The resting write already happened in the connection's frame-application
/// path, so each handle mirrors the table exactly for both a live push and a
/// bootstrap, and a server-side demotion between keyed deltas and whole
/// answers never surfaces (R84, decision 4). A handle that fails to decode
/// stays as it was and the failure surfaces as a `NonFatal` naming it.
fn apply_computed<T>(
    state: &mut State<T>,
    events: &broadcast::Sender<ClientEvent>,
    wire_id: &str,
    moved: Moved<'_>,
) where
    T: Transport,
{
    let State { conn, computed, .. } = state;
    for entry in computed.iter_mut().filter(|entry| entry.wire_id == wire_id) {
        if let Err(err) = (entry.apply)(conn.conn(), moved) {
            let _ = events.send(ClientEvent::NonFatal {
                related_to: Some(entry.sub_id.clone()),
                detail: format!("live computed answer update failed: {err}"),
            });
        }
    }
}

/// The outcome of a reconnect sequence.
enum Recovery {
    /// The session resumed and every subscription was re-declared.
    Live,
    /// The backoff policy ran out of attempts.
    Exhausted,
    /// The silent refresh could not produce a login grant, so retrying is
    /// futile and the driver routes to interactive re-login instead. The server
    /// never signals this: a grant it refuses leaves the connection open and
    /// says nothing, so the client learns it from its own token source.
    ReauthRequired,
    /// A refusal the build can never attach under, so the pump ends with the
    /// reason named rather than retrying it.
    Mismatched(ClientError),
    /// A close was asked for during a backoff.
    Closing,
    Failed(ClientError),
}

/// How an offline wait ended.
#[derive(Debug, PartialEq, Eq)]
enum Offline<O> {
    /// The awaited backoff or dial completed.
    Due(O),
    /// A close was asked for.
    Closing,
}

/// Keep local work, key deadlines and the gate running during a backoff or
/// dial, and hand on every notice a commit queued meanwhile, a peer apply's
/// among them, so the app hears it with no server reachable.
async fn serve_offline<T, F, S>(
    shared: &Shared<T>,
    pending: F,
    sleeper: &mut S,
    alive: &Weak<ClientToken>,
    key_timer: &mut Option<(ShareKeyTimer, KeySleep)>,
) -> Result<Offline<F::Output>, ClientError>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    F: Future,
    S: Sleeper + 'static,
{
    let mut pending = core::pin::pin!(pending);
    loop {
        if shared.close_requested.load(Ordering::Acquire) || alive.strong_count() == 0 {
            return Ok(Offline::Closing);
        }
        let mut state = shared.state.lock().await;
        tokio::select! {
            biased;
            result = state.conn.process_courier_audit() => result?,
            () = shared.wake.notified() => continue,
        }
        state
            .conn
            .process_share_keys(!shared.gate.is_locked())
            .await?;
        if let Err(err) = state.conn.flush().await {
            tracing::warn!(error = %err, "queueing an offline write failed");
        }
        if !shared.gate.is_locked() {
            refresh_changed(&mut state, &shared.events);
        }
        while let Some(notice) = state.conn.take_notice() {
            let _ = route_step_result(&mut state, shared, Ok(Some(notice)), true);
        }
        arm_share_key_timer(state.conn.share_key_timer(), key_timer, Some(sleeper));
        drop(state);

        let mut ask = shared.gate.take_ask();
        let mut outcome = None;
        let mut key_due = false;
        let ready = tokio::select! {
            biased;
            () = shared.wake.notified() => None,
            () = wait_timer(key_timer) => {
                key_due = true;
                None
            },
            resolved = async {
                match ask.as_mut() {
                    Some(ask) => ask.as_mut().await,
                    None => core::future::pending().await,
                }
            } => {
                outcome = Some(resolved);
                None
            },
            result = pending.as_mut() => Some(result),
        };
        if key_due {
            *key_timer = None;
        }
        shared.gate.apply_outcome(&mut ask, outcome);
        if let Some(ask) = ask {
            shared.gate.restore_ask(ask);
        }
        if let Some(result) = ready {
            return Ok(Offline::Due(result));
        }
    }
}

/// The wait floor a refusal carries, so the driver waits once for the
/// server's stated window instead of probing it.
fn retry_after(err: &ClientError) -> Option<Duration> {
    match err {
        ClientError::Refused {
            reason: FatalErrorReason::RateLimited { retry_after_ms },
        } => Some(Duration::from_millis(*retry_after_ms)),
        _ => None,
    }
}

/// Run the reconnect sequence to completion: backoff, fresh transport,
/// session resume, re-declared subscriptions.
///
/// A live close or a refusal the build can never attach under ends the
/// sequence as [`Recovery::Mismatched`] naming the reason, since the next
/// attach would draw the same answer.
///
/// Sleeps and connect attempts run WITHOUT the state lock. Only the resume
/// handshake and the re-subscribes hold it, so application reads and writes
/// against the replica proceed during an outage.
async fn recover<T, F, S>(
    shared: &Shared<T>,
    driver: &mut ReconnectDriver<F, S>,
    alive: &Weak<ClientToken>,
    key_timer: &mut Option<(ShareKeyTimer, KeySleep)>,
    last_close: Option<FatalErrorReason>,
) -> Recovery
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
    F: TransportFactory<Transport = T>,
    S: Sleeper + 'static,
{
    // The server that closed the live session said the answer once, so a
    // close of the terminal class skips the first redial it would repeat.
    if let Some(reason) = last_close
        && reason.is_build_mismatch()
    {
        return Recovery::Mismatched(crate::refusal_error(&reason));
    }
    let mut episode = driver.policy.start();
    // The wait floor a rate-limited refusal states, consumed by the next wait.
    let mut min_wait: Option<Duration> = None;
    loop {
        let Some(wait) = episode.next_wait() else {
            return Recovery::Exhausted;
        };
        let wait = min_wait.take().map_or(wait, |floor| wait.max(floor));
        let _ = shared.events.send(ClientEvent::Reconnecting {
            attempt: episode.attempt(),
        });
        let sleep = driver.sleeper.sleep(wait);
        match serve_offline(shared, sleep, &mut driver.sleeper, alive, key_timer).await {
            Ok(Offline::Due(())) => {}
            Ok(Offline::Closing) => return Recovery::Closing,
            Err(err) => return Recovery::Failed(err),
        }

        let dial = driver.factory.connect();
        let transport =
            match serve_offline(shared, dial, &mut driver.sleeper, alive, key_timer).await {
                Ok(Offline::Due(Ok(transport))) => transport,
                Ok(Offline::Due(Err(_))) => continue,
                Ok(Offline::Closing) => return Recovery::Closing,
                Err(err) => return Recovery::Failed(err),
            };
        let mut state = shared.state.lock().await;
        match state.conn.attach(transport).await {
            Ok(()) => {}
            // A rejected credential is not a transport blip: stop the backoff
            // loop and let the pump surface a re-login requirement.
            Err(ClientError::Auth(_)) => return Recovery::ReauthRequired,
            // A refusal the build can never attach under is not a server that
            // will come back, so the pump ends with the reason named.
            Err(err) if err.is_build_mismatch() => return Recovery::Mismatched(err),
            Err(err) => {
                min_wait = retry_after(&err);
                continue;
            }
        }
        // Re-declaring every live subscription under its original id is
        // `attach`'s own job now, from the persisted set, which is the same
        // work a first connection does.
        let _ = shared.events.send(ClientEvent::Reconnected);
        return Recovery::Live;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::prelude::*;

    diesel::table! {
        /// Orders, the fixture these render tests build queries against.
        orders (id) {
            /// Order identifier, the primary key.
            id -> BigInt,
            /// How many units the order is for.
            quantity -> BigInt,
        }
    }

    // A bound value renders as a placeholder plus a typed bind, never inline.
    #[test]
    fn render_query_emits_placeholders_and_binds() {
        let query = orders::table.filter(orders::quantity.gt(5_i64));
        let (sql, binds) = render_query(&query).expect("render");
        assert!(
            sql.contains('?'),
            "bind renders as a placeholder, got {sql}"
        );
        assert!(!sql.contains('5'), "the value must not inline, got {sql}");
        assert_eq!(binds, vec![SqliteValue::Integer(5)]);
    }

    // Table extraction yields the bare name whatever the quote style, so it
    // intersects the change tracker's plain table names.
    #[test]
    fn query_tables_unquotes_identifiers() {
        let parsed = parse_subscription(
            "SELECT `orders`.`id` FROM `orders` WHERE (`orders`.`quantity` > ?1)",
        )
        .expect("parse");
        assert_eq!(parsed.tables, HashSet::from(["orders".to_owned()]));
        assert_eq!(parsed.shape, QueryShape::Rows);
    }

    // Shape classification routes scalar aggregates to watch_value, grouped
    // statistics to watch_groups, and rows to watch.
    #[test]
    fn shape_classifies_aggregates_and_rows() {
        let agg = parse_subscription("SELECT COUNT(*) FROM `orders`").expect("parse");
        assert_eq!(agg.shape, QueryShape::Aggregate);
        let rows = parse_subscription("SELECT * FROM orders WHERE quantity > 0").expect("parse");
        assert_eq!(rows.shape, QueryShape::Rows);
        let grouped = parse_subscription("SELECT status, COUNT(*) FROM orders GROUP BY status")
            .expect("parse");
        assert_eq!(grouped.shape, QueryShape::Grouped);
        assert!(grouped.keyed);
    }

    // The grouped shape needs its group columns projected and plain: an
    // expression GROUP BY, a hidden group column, or a second aggregate all
    // stay row queries, which watch then refuses or serves as such. A keyed
    // handle also needs the aggregate last, since it splits each row by
    // position.
    #[test]
    fn grouped_shape_requires_projected_plain_columns_and_one_aggregate() {
        let quoted = parse_subscription(
            "SELECT `orders`.`status`, count(*) FROM `orders` GROUP BY `orders`.`status`",
        )
        .expect("parse");
        assert_eq!(quoted.shape, QueryShape::Grouped);
        assert!(quoted.keyed);
        let composite = parse_subscription(
            "SELECT status, region, COUNT(*) FROM orders GROUP BY region, status",
        )
        .expect("parse");
        assert_eq!(composite.shape, QueryShape::Grouped);
        assert!(composite.keyed, "the key columns may come in any order");
        let leading = parse_subscription("SELECT COUNT(*), status FROM orders GROUP BY status")
            .expect("parse");
        assert_eq!(leading.shape, QueryShape::Grouped);
        assert!(
            !leading.keyed,
            "an aggregate ahead of the key splits wrongly"
        );
        for hidden in [
            "SELECT COUNT(*) FROM orders GROUP BY status",
            "SELECT upper(status), COUNT(*) FROM orders GROUP BY upper(status)",
            "SELECT status, COUNT(*), SUM(quantity) FROM orders GROUP BY status",
            "SELECT status, quantity, COUNT(*) FROM orders GROUP BY status",
        ] {
            let parsed = parse_subscription(hidden).expect("parse");
            assert_eq!(parsed.shape, QueryShape::Rows, "not grouped: {hidden}");
            assert!(!parsed.keyed, "not keyed: {hidden}");
        }
    }

    // The public aggregate-shape classifier mirrors subscription_tables: it is
    // the relay's way to route a tab Subscribe to the aggregate path instead of
    // a row snapshot, matching the client's own routing. A grouped statistic
    // rides the aggregate path too: its frames are aggregate frames.
    #[test]
    fn subscription_is_aggregate_classifies_shape() {
        assert!(subscription_is_aggregate("SELECT COUNT(*) FROM `orders`").expect("parse"));
        assert!(subscription_is_aggregate("SELECT MIN(quantity) FROM orders").expect("parse"));
        assert!(
            !subscription_is_aggregate("SELECT * FROM orders WHERE quantity > 0").expect("parse")
        );
        assert!(
            subscription_is_aggregate("SELECT status, COUNT(*) FROM orders GROUP BY status")
                .expect("parse")
        );
        assert!(subscription_is_aggregate("NOT SQL AT ALL (").is_err());
    }

    // A membership term reads a second table inside `IN (SELECT ...)`. Refresh
    // routing needs both tables, so `parse_subscription` keeps them, and the
    // hidden membership subscription's holdings live as long as some live
    // query reads its table that way (O4), while a table read only in `FROM`
    // keeps no membership holding alive.
    #[test]
    fn membership_tables_names_only_the_subquery_table() {
        let query = "SELECT * FROM docs WHERE project_id IN \
                     (SELECT project_id FROM project_members WHERE user_id = current_app_user())";
        let parsed = parse_subscription(query).expect("parse");
        assert!(
            parsed.tables.contains("project_members"),
            "refresh routing must still watch the membership table, got {:?}",
            parsed.tables
        );
        assert_eq!(
            membership_tables(&SubscriptionSpec::new(query)).expect("tables"),
            HashSet::from(["project_members".to_owned()]),
        );
        assert!(
            membership_tables(&SubscriptionSpec::new("SELECT * FROM project_members"))
                .expect("tables")
                .is_empty(),
            "a table read in FROM is no membership term"
        );
    }

    /// A rate-limited refusal states the wait floor the driver waits out
    /// instead of probing, and no other fault states one.
    #[test]
    fn a_rate_limited_refusal_states_the_wait_floor_and_no_other_fault_does() {
        assert_eq!(
            retry_after(&ClientError::Refused {
                reason: FatalErrorReason::RateLimited {
                    retry_after_ms: 5000,
                },
            }),
            Some(Duration::from_millis(5000))
        );
        assert_eq!(retry_after(&ClientError::Locked), None);
    }
}
