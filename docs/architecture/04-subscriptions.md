# 04: Subscriptions

A subscription is how a client names the rows it wants and how the server keeps them current. This chapter covers the wire lifecycle of a row subscription, the ordering guarantees the server's run loop carries, the snapshot cursor and the fence it carries, the server-side registry, and the membership term in the subscription language. Aggregate subscriptions have their own chapter, [05-aggregate-queries.md](05-aggregate-queries.md). The CDC path the live updates ride is [03-sync-pipeline.md](03-sync-pipeline.md), the reconnect flow is [06-reconnect.md](06-reconnect.md), and row visibility is decided by [08-authorization.md](08-authorization.md).

## What a subscription is

A subscription is a standing query. The client says which rows it wants and asks to be updated as they change. A subscription has three parts.

- A **subscription ID** (`sub_id`), a client-chosen string, unique within the session, that correlates the snapshot and update messages.
- A **spec**, [`connetto_core::messages::SubscriptionSpec`], the query the client wants to observe.
- A **resume cursor**, [`connetto_core::cursor::Cursor`], opaque bytes the server issues and the client stores as-is and returns verbatim in its next `Handshake`.

The shipped spec carries three fields.

```text
SubscriptionSpec {
  priority: SubscriptionPriority,  // 0 to 3, 0 highest, 1 the default
  query:    String,                // full SELECT in the client's SQLite dialect
  binds:    Vec<BindValue>,        // values for the ? placeholders, in order
}
```

The `query` is a full `SELECT`, and the filter rides in its `WHERE` clause as SQL text. `subql` parses that text and rejects anything it cannot compile at registration. The supported comparison set is `=`, `!=`, `<`, `>`, `IN`, `BETWEEN`, `LIKE`, `ILIKE`, `IS NULL`, `AND`, `OR`, `NOT`, and arithmetic. A custom predicate tree is the alternative a reader would propose, and it was rejected because the language is owned by `subql` and a second AST connetto maintains would have to grow in lockstep with it. Column projection rides inside the `SELECT` as well. A subscription may name a subset of a table's columns, and the server widens it to whole rows before registration and before the snapshot reads, because the replica holds the application's whole table and `subql` reports no change that touches only a column the list leaves out.

The `priority` field travels on the wire and is echoed by `SnapshotBegin`, and it schedules nothing. Snapshots are delivered serially in message-arrival order. [G12](../../plans/master-implementation-plan.md#g12) records the gap.

A subscription selects from one table. A join as an output shape is composed locally from several subscriptions over the tables it reads. A join as a membership predicate, deciding which rows of a second table the client should hold because of a relationship to rows it already holds, is answered by row-level security and by the [membership term](#the-membership-term) below.

## The wire lifecycle

### Registration

The client sends `Subscribe { sub_id, spec }`. The server reverse translates the query from the client's SQLite dialect to Postgres, registers it with the engine under a fresh consumer id, and serves the snapshot or the catch-up.

Two gates can answer `RateLimited` with a wait instead of serving. A reader-share permit is taken before the creation allowance, so a subscribe the share defers has spent nothing and its retry is not refused by count. A per-tier per-minute meter on subscription creation (`subscription` in [guard.rs](../../crates/connetto-server/src/guard.rs)) refuses a caller at its allowance and answers the same frame with the wait.

On any failure, before or after registration, the server answers one `NonFatalError` frame whose `detail` is the fixed [`connetto_core::messages::SUBSCRIPTION_REFUSED`] text and does not add the subscription. The cause never reaches the wire and goes to the structured log, because a detail that varied with the cause would tell a caller which stage refused and so whether a table it guessed exists.

### Snapshot delivery

After registration the server delivers an initial snapshot. `SnapshotBegin { sub_id, priority }` opens it, the rows travel as one or more `SnapshotPatch` bulk frames, each carrying `sub_id` and a Zstd-compressed SQLite patchset, and `SnapshotEnd { sub_id, cursor }` completes it. The `priority` in `SnapshotBegin` is informational and matches the value registered in the spec.

The completion frame is not free of the rows it completes. It shares the session's outbound queue with the patches and costs no credit, so a backlogged client cannot record the resume position over an empty replica and skip the rows it never had.

The route is installed before the read, so live delivery runs throughout the snapshot. A change committed while the snapshot is in flight reaches the client as a `LivePatch` queued behind `SnapshotEnd`. A snapshot read that fails refuses the subscription with the same fixed detail as a registration refusal, rolls the registration back, and leaves the connection and every sibling subscription up.

### The wire order

The client does not buffer updates during snapshot delivery, and it does not need to. Ordering is guaranteed by the shape of the server's run loop. That loop is a single two-armed `tokio::select!`, and the code serving a subscribe, including its first page of rows, is awaited inside the arm that reads from the transport, so the arm draining outbound live patches is not polled meanwhile. Overlapping patches queue in memory and reach the wire in order.

The guarantee is structural, so the loop carries a comment saying that moving either arm onto its own task breaks it silently. The queue side of the guarantee is asserted rather than argued. `snapshot_order_holds_when_the_credit_window_is_closed` in [snapshot_order.rs](../../crates/connetto-server/tests/it/snapshot_order.rs) shuts the credit window by configuration and reads frames in wire order, so a change routing the completion frame around the queue fails a test rather than passing silently.

A read still arriving in pages is the one exception. Its later pages are taken when the client acknowledges the previous one, so a `LivePatch` may land between two pages. The direction is the safe one, because a later page is read after every frame already sent and so can never carry a value older than one the client has applied.

### The cursor and its fence

The position a snapshot reports is read in its own statement before the read transaction opens, so the cursor never counts a change the snapshot cannot see. A position alone would still be wrong. A commit record flushed before that position can belong to a transaction the snapshot cannot see yet, one waiting for a synchronous standby or between `XLogFlush` and `ProcArrayEndTransaction`, and a cursor on that position alone would sit past a change the client never received. The cursor therefore carries the fence of the read's own snapshot beside the position.

The first page reads `pg_current_snapshot()` and `pg_current_wal_insert_lsn()` inside its `REPEATABLE READ` transaction, and [`connetto_server::fence::ReadFence`] wraps the two as subql's `PgSnapshotFence`. The cursor on the wire is the stamped position followed by the encoded fence ([`connetto_server::fence::snapshot_cursor`]). A later page reads at a later moment. A later page's snapshot sees everything the first one saw, so the first page's fence decides for the whole read, and `SnapshotEnd` carries the first page's cursor. A client resuming from it catches up on the entries past the position the fence does not hold and on the missed ones at or below it.

subql's `Seen` judges a change at a position against the fence, and the server acts on each state.

| State | What the fence says | What the server does |
|---|---|---|
| `Held` | The read's rows already reflect the change, so applying it again would count it twice. | The overlap is re-applied rather than discarded. A catch-up walk does not resend the record, and a live dispatch at or below the stored cursor is skipped as one already past. |
| `Missed` | The read's rows lack the change, and a later change may still be one the read holds. | The rows are owed from the oplog. The run loop replays them between two frames, ahead of every live patch still queued, as live patches under the snapshot's own fenced cursor, and a missed transaction dispatched later is delivered even at or below the cursor catch-up left. A missed `Truncate` refuses the subscription. |
| `Beyond` | The change commits at or past the fence's insert position, so the read's rows are all before it and the fence has nothing left to decide. | Nothing is owed, and a dispatch at or below the stored cursor is skipped as a re-delivery. |

Three properties make the table safe. Re-application is safe because patches arrive in commit order, so for any row the last patch applied carries that row's current value, and the replica settles on the same state whether or not the snapshot already held it. A row can briefly show an older value between `SnapshotEnd` and the end of the overlap, and the resume cursor moves backwards for that moment, which replays rather than loses. The replay runs between two frames because the queued live patches all commit later than the missed rows, so sending the missed rows first keeps commit order on the wire. The fence rides with the cursor and stays with the route, so a client that drops before the replay arrives resumes from it and is sent the missed rows again.

A `Truncate` the fence missed cannot be sent as a patch, and a read taken now would miss it again, so the subscription is refused. If the oplog's retention no longer covers the transactions the fence did not see, the snapshot's rows cannot be completed, and the subscription is resnapshotted with `FullResyncReason::CursorOutsideRetention`.

### Paging

A snapshot larger than one delivery arrives as several pages. The read is capped at a per-tier byte budget, and a read past it is served in pages ordered by primary key, each resuming past the last row of the one before, never by offset. The next page is taken when the client acknowledges the last, so the credit window paces the database reads as well as the wire and the server holds one page at a time. `SnapshotBegin` and `SnapshotEnd` still bracket exactly one read, and the position `SnapshotEnd` carries is the first page's, which can only replay rather than skip.

A read the server will not serve is refused rather than truncated. A row above a per-tier ceiling, a table whose typical row is already above it, or a read whose plan needs a sort and runs past its time limit all answer the one fixed refusal on the wire, with the cause in the server's log. A read that fails part way through its pages is replaced once, announced as `FullResyncReason::SnapshotInterrupted`, and refused if the replacement fails too.

### Ongoing updates

After `SnapshotEnd`, the server delivers incremental updates via `LivePatch` bulk frames as the underlying data changes. The CDC fanout path is [03-sync-pipeline.md](03-sync-pipeline.md).

### Cancellation

The client sends `Unsubscribe { sub_id }`. The server removes the subscription, and no further updates are delivered for the id, which the server tolerates when it is unknown. The client may delete the locally cached data for the subscription or retain it for offline use.

### Update

There is no in-place modify. To change a subscription the client sends `Unsubscribe(sub_id)` and then `Subscribe(sub_id, new_spec)`, which serves a new snapshot. The client may retain the old local data until the new snapshot arrives.

## Matching changes to subscriptions

The engine routes an event by the tables it references and answers each covering subscription's compiled predicate against the changed row, in SQL three-valued logic. The transition a row takes through the result set decides the delivery.

| Event | old row | new row | Delivery |
|---|---|---|---|
| `Insert` | absent | matches | insert |
| `Delete` | matches | absent | delete |
| `Update` | matches | matches | update |
| `Update` | matches | does not | delete, the row left the result set |
| `Update` | does not | matches | insert, the row entered the result set |
| `Update` | does not | does not | none |

Each delivery still passes the visibility question before it is sent, against both versions of the row. That check is owned by [08-authorization.md](08-authorization.md).

### The two deletes

The two deletes are different events, and the wire distinguishes them. The `Delete` case removes a row that no longer exists. The departure case removes a row that still exists and merely left this subscription's window. The marker is the patchset op's own session-format indirect flag, set on synthesized departure deletes, so no frame or format changed.

On an event with departures the server encodes a second patchset carrying only the departed rows' table, primary key and `indirect(true)` ([`connetto_server::materializer::Materializer::dispatch`]), identical bytes to every departed subscriber, and splits the consumer list in two rather than fanning out per subscriber. The client releases the addressed subscription's holding of the row and deletes it only when no holding and no unsent write keeps it (`apply_patch` in [lib.rs](../../crates/connetto-client/src/lib.rs)). A direct delete applies unconditionally and releases every holding of the row. The holding rules are [15-replica-retention.md](15-replica-retention.md#departures-and-deletes).

The distinction is load-bearing once two subscriptions share a table, because patches from both apply into the same replica table. A departure delivered as an unmarked delete would remove a row out from under a sibling subscription that still covers it. Nor can the client repair that by predicate-checking alone. On a genuine deletion the server sends a delete to every covering subscription, each would be held back by the others still matching the stale local row, and the row would never be removed at all. Only the marker is correct in both directions.

## The membership term

A subscription's `WHERE` clause may narrow through a membership. The motivating filter is `team_id IN (SELECT team_id FROM team_members WHERE member = current_app_user())`, written in the client's own SQLite dialect. Reverse translation rewrites the caller with the deployment's pairing, the SQLite function `current_app_user` against the identity session setting, so the subquery reaches Postgres as `current_setting` text the classifier reads.

The term is one SQL text with two executors, exactly as policy is. The subquery serves the snapshot on Postgres under row-level security, and `subql`'s compiled term answers the per-row question on the change path. It is seeded at registration from the membership table read as the caller, under the same binding the snapshot uses and inside the same materializer-lock hold as the register, so no dispatch lands between the seed's snapshot and the engine watching. The subscriber is typed at the membership column's own catalog kind, because a mistyped subscriber would admit nobody in silence. A term `rls2fga` cannot classify is refused at registration, and a term whose membership table the publication does not carry is refused the same way, probed live in the seed's own transaction.

A membership change moves rows, and it never resends the subscription. `subql` reports each move as a narrowing, and the server answers it incrementally. A value entering the set is served by the subscription's own `SELECT` with `AND <column> = <value>` conjoined, read as the caller so the rest of the filter and the policy still apply. A value leaving it is served by indirect deletes for the keys the change-path executor now denies, read on the privileged pool, because the policy that made them visible is exactly the membership that ended, and sent only when the same event moved a grant reaching the subscribed table, which is what keeps a never-held key undisclosed. The proof asserts the absence of `FullResyncRequired` in both directions in [membership_term.rs](../../crates/connetto-test-harness/tests/it/membership_term.rs).

The server opens a membership subscription on the client's behalf. The replica needs the caller's own membership rows twice over. The application's query names the membership table locally, and a translated membership policy reads it inside the replica's view, so without those rows the local answer is empty while the server sends the right rows. Registering a term therefore opens one hidden subscription per membership table, labelled `connetto-membership:<table>` and announced with `MembershipOpened` ahead of its own snapshot. It carries the caller's own rows and nothing wider, is counted against the caller's allowance before the term is served so a caller at its ceiling is refused as a unit, reads under the term's own reader-share slot so the share never throttles it once the term is admitted, and is torn down with the last term subscription that needs it. The client keeps the table out of the application-facing changed-tables signal, unconditionally, while the live-query refresh still sees it, which is what re-runs a narrowed query when the membership moves. A membership read that fails refuses the term with it, `subscription refused` on the term's label, and the connection and every sibling subscription stay up.

The term intersects the policy, never widening it. A row the term admits and the policy forbids never arrives, because every move-in is read as the caller and every withdrawal key passes the visibility question. A row the policy admits and the term excludes never arrives, because interest excludes it.

## Reconnect

After reconnect the client re-sends all `Subscribe` messages, and across a client restart the same re-declaration comes from its own persisted subscription set. The server judges the handshake cursor against the database's timeline before serving any of them. A cursor past where its timeline ended answers `FullResyncRequired` with `CursorBeyondHistory` ahead of every snapshot.

Each re-declared row subscription is then judged against the oplog. A catch-up never shows or hides a row that did not change, so a subscription a key change reaches, while the connection's subject coverage is unknown, is resnapshotted with `FullResyncReason::AuthorizationChange`. Otherwise the decision is [`connetto_server::oplog::catchup_decision`]. The gap replays when the resume position sits at or after the oldest retained entry, and resyncs with `FullResyncReason::CursorOutsideRetention` when it does not or when the log is empty, since nothing retained can prove the client has everything.

A catch-up installs the route first, carrying the fence of the snapshot that issued the cursor, then takes a ceiling from the oplog and walks it, sending what the subscription wants past its cursor and the rows its fence missed at or below it. It ends with a `SnapshotEnd` stamped at the ceiling. The full reconnect flow is [06-reconnect.md](06-reconnect.md).

## The server's registry

The registry is in-memory for the session's life and not persisted server-side. A session keeps two maps. `subs` holds the row subscriptions, keyed by the client's label, each keeping the engine's consumer and subscription ids, the reverse-translated query, the membership tables it watches, and the bytes its deliveries have carried, which the growth warning is measured against. `computed_subs` holds the computed reads, one engine id space for every tier, and whether each answer depends on the caller's subject set. Both live in the session state (`RowSub` and `ComputedSub` in [session.rs](../../crates/connetto-server/src/session.rs)). The resume position a subscription holds is an opaque `Cursor`, not a number the caller can read.

The caller may have no identity at all. See [12-identity-session-capability.md](12-identity-session-capability.md).

Registry memory management, the cap and the eviction policy, is owned by `subql`. Its engine is uncapped by default and exposes `with_max_subscriptions` and an `EvictionPolicy`, and connetto sets neither. The tracking item is [subql.md](subql.md#subscription-registry-limits-q42). Connetto meters creation instead, per minute per tier, and the hidden membership subscriptions spend the same allowance, so a caller at its ceiling is refused as a unit rather than served half. The per-tier defaults are recorded in [R19](https://github.com/connetto-rs/connetto-rs/blob/82983dff19effa9cfb521370a7794d29e51afb51/plans/master-implementation-plan.md#r19-request-throttling-tiered-by-identity).

A subscription with no filter is a whole-table claim by definition, and the accumulation its rows leave behind is what the trimming pass in [15-replica-retention.md](15-replica-retention.md#the-trimming-pass) reclaims when coverage lapses.
