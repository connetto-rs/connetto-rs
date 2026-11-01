# 05: Aggregate Queries

Aggregate queries answer over rows the caller may not hold, and they split by where the data lives. This chapter covers the two answer paths, the server-side delta family and its grouped folds, the client's answer-path classifier, the client resting table, per-viewer re-execution on row-level-secured tables, and the client live handles. Row subscriptions are [04-subscriptions.md](04-subscriptions.md), the server mechanics and the re-execution failure classes are [10-subscription-materializer.md](10-subscription-materializer.md), the client-query mechanism is [13-client-connection.md](13-client-connection.md), and the `subql` capabilities the paths follow are [subql.md](subql.md).

## Two answer paths

Where an aggregate is answered is dictated by where the data lives, and it splits cleanly in two.

**The client-authorized view.** The client holds a local SQLite replica of exactly the rows it is authorized to see, kept current by CDC. Any aggregate scoped to that view, the count of my orders, the average of my amounts, the full statistical family, is computed locally over the replica. This is the primary answer path. It is immediate, available offline, and correct as of the last sync. The server subscription for such a table carries row patches, not aggregate values, so its role is to keep the replica fresh. On reconnect, catch-up reconciles whatever was missed offline.

**Global, cross-client statistics, non-RLS only.** A statistic over rows the client does not hold, a total order count across all users, an anonymized average, cannot be computed on the device. Only these go through the server-side delta accumulator that `subql` maintains in process and pushes as `AggregateUpdate`. `subql` rejects an aggregator on an RLS-protected table at registration (`RegisterError::AggregatorOnRlsTable`), and connetto surfaces the refusal as [`connetto_client::ClientEvent::NonFatal`] with the session intact. A single shared accumulator cannot represent a value that differs per viewer, and a materialized view has the same problem. A materialized view is one computed result, and under RLS `COUNT(*) FROM orders` returns a different value per user, so there is no single server-side value to mirror. The server-side family is therefore global statistics only, by construction.

## The server-side delta family

The delivered server-side delta family is `COUNT`, `COUNT(col)`, `SUM`, `AVG`, `VAR_POP`, `VAR_SAMP`, `STDDEV_POP`, and `STDDEV_SAMP`. The value is seeded once through the connector and folded per CDC event. The `GROUP BY` variants of that family fold per group. The wire carries one keyed upsert per changed group and an emptied group as a keyed removal, and the group key stays opaque bytes end to end.

A subscription whose groups outgrow the group budget, which defaults to 1024 groups and connetto leaves at that default, demotes to whole re-reads under the same identity. The demotion is logged and counted server-side and never told to the client. The tier model, its budget, and its costs are recorded in [R30](https://github.com/connetto-rs/connetto-rs/blob/82983dff19effa9cfb521370a7794d29e51afb51/plans/master-implementation-plan.md#r30-grouped-aggregates-revisited-from-the-research).

`MIN` and `MAX` are served by the re-execution tier. They are seeded under the caller's own read tier and re-read under the shared server bound when a change retires the extreme. A re-execution read is not shared state, so per-viewer statistics are served the same way, and the replica stays the primary path for anything the client already holds.

Multi-table aggregates, aggregates that `JOIN` several tables, have no in-process maintenance and ride the same re-execution tier. `subql` tracks the tables a query touches and re-reads it when a change lands on any of them.

## The client's answer-path classifier

The client's classifier is deliberately broader than the server-side family. `AGGREGATE_FUNCTIONS` in [live.rs](../../crates/connetto-client/src/live.rs) recognizes the delta family plus `MIN`, `MAX`, and SQLite's `TOTAL`. It decides only which path a query rides, never what the server maintains. Anything aggregate-shaped must reach the server path, because running it as a row query against a partial replica would return a wrong answer silently. `MIN` and `MAX` are then served by re-execution.

`TOTAL` has no server-side variant at all. `subql` has no `AggSpec` member for it and no re-execution class, so it rides the registration-refusal path and surfaces as `NonFatal`, loud rather than silently wrong, though that exact fate is untested end to end.

## Resting results

Every server-computed value rests in `_connetto_aggregates` (`AGGREGATE_DDL` in [aggregates.rs](../../crates/connetto-client/src/aggregates.rs)), keyed by query identity. The row is `(query_id, binds, group_key, group_values_json, result_json, updated_at)`, and `query_id` references the query-text row the subscription already persists, because a wire subscription id is minted fresh each run and could not be found again after a restart. No per-subscription DDL is generated. The application reads results through a custom Diesel connection that deserializes `result_json` into `T: serde::DeserializeOwned`, and connetto provides the raw JSON.

The wire carries results as JSON in `AggregateUpdate`, with `group_key`, `group_values_json`, `result_json`, and `is_full_result`. [`connetto_client::ClientEvent::Aggregate`] is the client-side delivery of one such movement.

A value is held in memory on its handle and is `None` until the server's bootstrap push. The handle bootstraps from the resting table and exposes the value's as-of time, so an offline restart shows the last synced value rather than `None`, and an online restart re-bootstraps through the startup re-declaration. A `FullResyncRequired` naming the statistic drops its whole resting answer, and the re-bootstrap that follows rebuilds it.

Aggregate results are connetto-managed client storage, not application schema. The schema symmetry between replica and server applies to row-level data only. Aggregate results have no PostgreSQL counterpart.

## Per-viewer re-execution on RLS tables

The per-viewer statistic over rows the client does not hold is served by re-execution rather than by a shared accumulator. An aggregate registration `subql` refuses on an RLS table is re-registered with per-consumer database reads under the asking viewer's own binding. The read setup appends `set_config` statements for the identity and the packed capability subjects after the budget statement, so every read the engine runs for that subscription executes as that viewer through the deployment's non-superuser reader role.

Each session registers its own `subql` subscription, so results key by query and viewer with no new routing. A change re-executes each affected viewer's read and moves only their answer. An unidentified caller keeps the refusal, since there is nobody to read as, and non-RLS folds keep the shared fast path untouched. The engine answers such a read as whole rows, and the server sends an ungrouped aggregate's one cell as its bare value, the shape the client rests a scalar in, while a grouped one keeps its row objects. The catalog DDL must carry the table's `ENABLE ROW LEVEL SECURITY` marker, which is what `subql` classifies from. The two-viewer proof is [`rls_computed.rs`](../../crates/connetto-server/tests/it/rls_computed.rs).

## The client handles

The grouped handle is `watch_groups`, which returns [`connetto_client::live::LiveGroups`] holding one typed entry per group. The wire delta carries `group_values_json`, the decoded values in `GROUP BY` order beside the opaque canonical key, so the map key is typed without decoding key bytes. A fold delta upserts one entry, a group's departure removes one, and a server-side demotion to whole answers rebuilds the map through the same rested-row path, so the tier change never surfaces. The values rest in `_connetto_aggregates.group_values_json` for the offline rebuild. Grouped `query.live(&client)` dispatches at compile time from diesel's group clause and projection SQL types, deriving scalar or tuple `K` and aggregate `V`. Float, numeric, and JSON map keys have no typed impl. [`connetto_client::live::LiveRows`] is the explicit full-replacement handle for wider re-executed row shapes. The proofs are the three tests in [`grouped_live.rs`](../../crates/connetto-client/tests/it/grouped_live.rs), and the Dioxus desktop demo exercises generic `use_live` with `COUNT(*) GROUP BY quantity`.

The Dioxus and Yew demos render the `COUNT(*)` for each quantity in ascending quantity order, with `none` for an empty group map.

## Re-execution cost and control

A re-execution read is bounded by the per-read budget its registration carries, and a failed read is classified, retried, paused, or given up by the policy in [10-subscription-materializer.md](10-subscription-materializer.md#reliability-and-retry). Scheduling is `subql`'s. Debounce and in-flight collapse live inside the engine's read planning, and connetto sets none of the engine's concurrency or debounce knobs. The tracking item is [subql.md](subql.md#re-execution-rate-limiting-q55).

Accumulator state is in process and owned by `subql`. On a server restart it is rebuilt by re-execution, and persisting it to avoid the rebuild cost is a `subql` optimization, not a connetto concern.
