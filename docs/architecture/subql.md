# subql: Required Features & Responsibilities

Tracking document for the features and responsibilities that connetto's architecture decisions assign to `subql`. Each item records whether it has shipped. `subql` has grown from an in-process predicate filter into a CDC subscription runtime, so the loop surface below is shipped and consumed by the [Subscription Materializer](10-subscription-materializer.md), which pins the boundary. The items below track the finer-grained responsibilities and their status.

## Loop surface

`subql` closes the CDC round trip and exposes it as a library the materializer drives.

- **CDC ingestion.** The `CdcSource` trait plus `PgStreamingCdcSource`, `PollingPgCdcSource`, and `SqliteCdcSource`. The source holds the replication connection. The materializer drives the consume loop and acks. The WAL parsers for pgoutput, wal2json v1 and v2, and Maxwell feed one path, and the wal2json v2 path wraps `wal2json_events::MessageV2`, the Debezium envelope. **Shipped.**
- **Event to patchset conversion.** `wal2json_patchset`, `pgoutput_patchset`, and `maxwell_patchset`, plus changeset counterparts, fold events over a source-agnostic catalog into one `sqlite-diff-rs` diffset. **Shipped.**
- **Inbound apply.** `SubscriptionEngine::apply_diffset_bytes` applies uploaded SQLite session bytes to the source through the native diesel adapters. The `diesel-async` counterpart `apply_diffset_bytes_async` (plus `apply_patchset_async` and `apply_changeset_async`) runs the same reconstructed batch over an `AsyncConnection` for Postgres and MySQL, behind the `apply-patchset-postgres-async` and `apply-patchset-mysql-async` features. SQLite apply stays synchronous. **Shipped.**
- **Re-execution engine.** `SubscriptionEngine` registers every query, and triggers come out of the dispatch as `ReExecutionTrigger`. Resolution is `AutoResolvingEngine`, parameterised by `SyncMode` or `AsyncMode`. The async side is `reexec::AsyncMode` parameterising one `AutoResolvingEngine`, with the shipped `PgAsyncDieselConnector` and `MysqlAsyncDieselConnector` built on `diesel-async` (bb8-pooled) behind the `executor-diesel-async-postgres` and `executor-diesel-async-mysql` features. The `AsyncConnector` trait is the async seam, and `SessionSetup` carries the per-registration statements the engine runs inside every read transaction. **Shipped.**

## Predicate Evaluation

### UPDATE transition detection (Q3.4)

**Shipped.** `subql` evaluates both `old_row` and `new_row` for UPDATE and returns the transition per consumer through `ConsumerNotifications`. **enter** surfaces in `inserted`, **exit** in `deleted`, **update** in `updated`, and **no-op** yields nothing. connetto delivers correct INSERT and DELETE events when rows move in and out of a subscription's result set.

### SQL WHERE clause input (Q4.1)

**Shipped.** `subql` accepts SQL `WHERE` clause text directly, with no custom AST. The compiler parses the text and rejects unsupported constructs at registration (`src/compiler/parser`, `RegisterError::UnsupportedSql`). Supported are `=`, `!=`, `<`, `>`, `IN`, `BETWEEN`, `LIKE`, `ILIKE`, `IS NULL`, `AND`, `OR`, `NOT`, and arithmetic.

## Visibility

### The visibility trait

**Shipped**, as `src/visibility.rs`. `VisibilityPolicy` with associated `Watcher`, `Error` and `Backend`. Its `may_see(row, watchers, verdicts)` answers one changed row for every watcher at once, writing one `Verdict` per watcher positionally into a caller-owned buffer sized by `Verdict::reset`, and `may_write(row, watcher, op)` answers one caller for one row. Both are async and both take `R: RowView`. `EventRow` is the shipped `RowView` over a change event bound to one `RowKind`. `subql` ships the trait and nothing underneath it, deliberately, so the executor stays an implementation detail.

### Per-row visibility and permission-record upkeep

**Shipped.** `RowPolicy<DB, P>` answers visibility from the changed row wherever the schema decides the relation, by calling `rls2fga`'s `records_from_row`, and hands the rest to an inner policy that `OpenFgaPolicy<DB, T, W, B>` terminates against a server. One shared `Shapes<DB>` sits behind both, built from a `Translation`. `Shapes::diff` reports what a changed row moved and `write_records` puts it in the store, which is the permission-record upkeep, and it lives here because `subql` is the only place holding both row versions.

A difference whose tuple key states both sides is sent in two calls with the removals first. A table the database leaves open can be told to `Shapes` through `with_unrestricted_tables`, and a statement the model refuses is answered locally as a refusal rather than reported as unanswerable. Asking once per distinct group instead of once per subscriber was searched for and ruled out. The shapes bounded by the row rather than by the audience either truncate silently or read only directly stored tuples without evaluating the model, so there are two ways of answering rather than three, local or one question per watcher batched at the server's cap.

`subql` does not reimplement the mapping semantics. It adapts its own value type to `rls2fga`'s `RowValues` abstraction and calls that crate's evaluator. Two implementations of one mapping in two repositories is the divergence a single policy source exists to prevent, one level down.

### The tables each policy reads

The question of which tables a policy reads is answered from the catalog, not from a `subql` re-export. `sql-traits` already models policies, so `DatabaseLike::policies()` with the policy's using and check expressions answers it from the catalog connetto already parses. `subql` owes nothing here. The connetto side, the startup refusal for a policy that reads a table the publication does not carry, is [08-authorization.md](08-authorization.md).

## Resource Management

### Subscription registry limits (Q4.2)

Memory management for the subscription registry is owned by `subql`. The surface is `with_max_subscriptions`, which takes a cap and an `EvictionPolicy` (`Reject`, `EvictOldest`, `EvictLeastActive`, `EvictColdest`, `EvictBySession`, `EvictByConsumer`), plus `with_custom_eviction` for a caller-supplied evictor. When the registry is full and the policy is `Reject`, registration fails with `RegisterError::RegistryFull`. The engine is uncapped by default and connetto sets none of this, so the registry runs uncapped. The connetto metering that stands in for a cap is [04-subscriptions.md](04-subscriptions.md).

## Aggregate IVM

### MIN/MAX incremental maintenance (Q5.1)

**Shipped (v1).** The `reexec` module maintains single-table scalar MIN and MAX incrementally. Inserts and most updates and deletes fold in memory from the event's row image. A re-execution fires only when the current extreme is removed or displaced, re-querying through the engine's read connector rather than a full scan. This is the `pg_ivm` approach. Streaming-style ordered state per group (Flink or RisingWave) remains overkill. Tie refinement and multi-group extents are future work.

### DISTINCT aggregates (Q5.1b)

**Not shipped, rejected at registration.** `COUNT(DISTINCT col)`, `SUM(DISTINCT col)`, and the like need a per-group frequency map, and the in-process fold does not maintain one. The compiler refuses `COUNT(DISTINCT ...)` and its siblings at registration with `RegisterError::UnsupportedSql`, so the shape is loud rather than silently wrong. The design for a frequency-map fold, shared per distinct column with per-call counters (the RisingWave model), is recorded here for the owner who builds it.

### HAVING evaluation (Q5.2)

The HAVING fast path is **shipped**. `AggHaving` (with `HavingOp`, `HavingFunction`, and `HavingSubject` in `src/compiler/sql_shape.rs`) is the `HAVING` comparison a grouped fold checks per group after each fold, against in-memory accumulator state such as `HAVING COUNT(*) > 10`. The re-execution fallback half is the caller's. Unsupported HAVING constructs go into the per-session re-execution map rather than being claimed in-process, and the goal is to expand the fast solver's coverage over time to reduce that re-execution.

## Re-execution

### Multi-table CDC routing (Q5.3)

**Shipped.** For queries involving `JOIN`s, `subql` tracks the tables each query touches and re-reads it when a change lands on any of them, so a per-session map of unanswerable queries routes efficiently.

### Re-execution rate limiting (Q5.5)

The collapse of duplicate re-execution within a dispatch is internal to the engine. `AsyncMode` keeps a per-subscription read queue and plans its pending reads, so a burst collapses into the reads the engine issues, and there is no named batch entry point. The per-subscription debounce and the global concurrency cap (`with_max_concurrent_reexecutions`) are engine knobs that connetto leaves unset. The goals for them, coalescing rapid bursts into one re-execution and bounding the concurrent reads against Postgres, are owned by the [reliability section](10-subscription-materializer.md#reliability-and-retry).

### Accumulator lifecycle (Q5.4)

Accumulators are in-memory. On a server restart they are rebuilt via re-execution. Persisting accumulator state to avoid the rebuild cost is a future `subql` optimization, not a connetto concern.
