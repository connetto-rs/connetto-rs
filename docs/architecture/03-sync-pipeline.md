# 03: Sync Pipeline

How a mutation travels from client to server, and how a server-side change travels back to the clients. This chapter covers the happy path and the main failure modes. Offline and reconnect are in [06](06-reconnect.md), the client connection and the writer model in [13](13-client-connection.md), and the delivery side effects in [10](10-subscription-materializer.md).

## Write path

### 1. Optimistic local apply

The client writes the mutation to its local SQLite immediately, before the server has confirmed it. This keeps the UI responsive.

Nothing tags the row itself. The write is recorded in the outbox inside its own transaction ([13](13-client-connection.md)), and the pending state lives in the queue below, keyed by (writer, sequence). What makes the write optimistic is only that the local apply precedes the server's answer. A rejection or a conflict later inverts the queued changeset (see 6).

### 2. Local mutation queue

The pending writes are stored in the local replica, the `META_DDL` in [lib.rs](../../crates/connetto-client/src/lib.rs). One entry per (writer, sequence), the whole sqlite-diff changeset per entry, so the queue holds exactly what it sends rather than a parallel description of it. Nothing has to be re-encoded on the way out, and the verb is a decoded value rather than a string nothing checks. An entry the import restored or a retired writer's device carried forward carries its provenance, the writer and sequence it was first written under, and every entry carries the schema major it was written under.

One row per writer keeps the next sequence, and the row is written whenever the sequence moves, because the server's exactly-once watermark is per writer and outlives the process. A write made after a restart with an empty queue would otherwise take a number the server had already applied.

The queue is the durability boundary for writes. Anything not yet in the queue is lost on crash.

### 3. Sending

The sender reads the head of the queue and sends a `MutationHeader` control frame followed by its `MutationPatch` bulk frame. It does not dequeue until it receives `MutationApplied`, `MutationReject`, or `MutationConflict`, and it sends one sequence at a time.

The connection writes as a writer. A native device with an enrolled key writes as that device key, the device writer's path, and every other connection writes under a number the server issued. The header names the writer, and if the connection drops before an answer the entry is re-sent after reconnect, and the server is idempotent on a duplicate (writer, sequence), answering from the settled record.

### 4. Server validation and apply

On receiving a `MutationHeader` and its `MutationPatch`:

1. Decompress and parse the patchset. A payload that does not parse is refused with `Malformed`, and one naming tables or columns outside the current schema with `SchemaMismatch`
2. Open one transaction and bind the caller, `SET LOCAL app.user_id` plus the held share keys and the writer the connection writes as, so Postgres row-level-security gates the writes
3. For each version-bearing op, probe the row. `WHERE id = ? AND updated_at = ?` against the op's recorded old `updated_at`. A stale or missing row rolls the transaction back and replies `MutationConflict` carrying the server's current copy when the row still exists
4. Apply the changeset. A row-level-security refusal, a `WITH CHECK` violation or an `UPDATE`/`DELETE` finding no visible row, rolls back and replies `MutationReject` with `Unauthorized`. A constraint violation replies `Constraint`
5. On commit, `MutationApplied` with the sequence, which is what retires the client's pending record

The patchset settles as a whole. One outcome per sequence, so one failing operation fails the whole patchset and the client rolls back all of it. Per-operation results do not exist. [G13](../../plans/master-implementation-plan.md#g13)

### 5. Conflict handling

A conflict occurs when a version-bearing op's recorded `updated_at` does not match the server's current row version, meaning the row was modified by someone else since the client last saw it. The `updated_at` timestamp is the conflict token. The application schema already has it on every entity, `xmin` wraps and is unsuitable, and vector clocks and HLC are overkill for a single-authority PostgreSQL backend.

The server sends `MutationConflict` with the sequence, the table, and the server's copy, absent when the row is gone.

**The client's response is one policy, revert, and the application layers its own merge on top.** The connection inverts the retained changeset and applies the inverse under the write exemption, undoing the optimistic write without recording it for upload, and a row a concurrent server patch already changed is left as the server left it. Both `MutationReject` and `MutationConflict` reach the application carrying the affected rows and, for a conflict, the server's current copy, so an application wanting anything other than server-wins re-writes on top of the revert.

### 6. Rollback on rejection

On `MutationReject`, the client retires the pending record, inverts the retained changeset under the write exemption (reverting the optimistic local write), and surfaces the rejection with its affected rows to the application. `Indeterminate` is the exception. It means the server could not settle the write, so the record and its rows stay and the write is sent again ([10](10-subscription-materializer.md)).

## Read/push path

### 1. CDC source

`subql`'s `CdcSource` holds PostgreSQL's logical replication connection and surfaces typed events to the server. The stack is built on logical replication, so no trigger-based `NOTIFY` path is needed. Each event is a row DML change, and the oplog records the table, the primary-key bytes, the event, and the patch built once for the consumers ([`connetto_server::oplog::ChangeRecord`]).

### 2. Subscription matching

For each change, the CDC fanout engine asks which active subscriptions could be affected. This is an index lookup. Subscriptions are indexed by the tables they reference, and the result is a set of (session id, subscription id, spec) candidates.

`subql` evaluates this in-process, a bitmap prune plus a predicate VM. Predicates it cannot decide against a single row image, JOINs, subqueries, MIN or MAX extreme removal, fall to re-execution against PostgreSQL through the materializer's `Connector`. [10](10-subscription-materializer.md)

### 3. Authorization filter

One question per changed row, asked once for every watcher. `may_see` takes the row and all matched watchers and returns one verdict each, so authorization is evaluated in batch rather than per-subscription serially. Behind the seam, [`connetto_server::openfga::FgaAuth`] is `RowPolicy`, which answers locally from the row's own values, and `OpenFgaPolicy`, which batches the rest to the authorization store. The verdict buffer arrives pre-filled with denials, so a backend that cannot answer denies rather than allows.

The filter consults both versions of the row. The current version first, and the previous one only when the current is absent or invisible to somebody, and connetto acts on the three answers it writes per watcher:

- **Deliver.** The patch this event produced, which for a subscriber that also left the subscription's own window is the marked departure notice
- **Withdraw.** A plain unmarked delete carrying the key alone, so a row that left the caller's reach leaves their device too. A delete's own patch already is that, so only an update pays for a second fold. The caller cannot tell a withdrawal from a deletion, which is what stops the frame disclosing that the row still exists for others
- **Nothing.** No frame at all, which is what stops a deleted row's key reaching a caller who could never see it

A truncate is outside this. It names no row, so no question can be put, and the payload connetto folds for one carries no operations at all. The emptied table reaches the subscriptions as `FullResyncRequired`.

A predicate departure is distinct. A row that stops matching a subscription's own `WHERE` clause while the caller may still see it is announced to exactly the subscribers it left, as a delete marked with the patchset op's indirect flag, and the client applies it only when no surviving subscription still covers the row. The transition decides for every subscriber, so a departure notice survives only for one who may still see the row. [04](04-subscriptions.md) owns the rule.

### 4. Delta packager

One patchset is encoded per CDC event, compressed once, and the identical bytes go to every matched consumer as a `LivePatch` bulk frame carrying that subscription's `sub_id`, the event's cursor, and the compressed patchset. There is no per-session bundling and no (op, pk, values) entry list. The payload is a Zstd-compressed SQLite patchset, exactly what the client applies. An event that also has departures encodes one further payload, the marked departure delete, shared by every subscriber the row left.

### 5. Delivery

The packaged `LivePatch` frame is placed in the session's outbound delivery queue. The delivery queue respects the flow-control window ([02](02-protocol.md#flow-control)). If the window is exhausted, delivery is paused.

The message is sent over the WebSocket connection.

### 6. Client apply

On receiving `LivePatch`:

1. Apply the patchset to local SQLite inside one transaction, under the write exemption so the outbox never records server rows. Frames arrive in commit order on an ordered transport, so there is no gap detection and no reorder buffer on the live path. A gap can only open across a disconnect, and the resume cursor plus catch-up or full resync covers it ([06](06-reconnect.md))
2. Honour the departure marker. An indirect delete applies only when no surviving subscription's predicate still matches the row
3. Persist the frame's cursor as the resume position, atomically with the apply
4. Return flow-control credit to the server

**A resume position is never recorded for data the replica has not applied.** It is a promise that everything up to it is already here, and the next attach asks the server only for what follows, so recording one early loses the rows in between with nothing on either side able to notice. The live path holds it by writing the cursor and the rows in one transaction, step 3 above. The snapshot path has no rows of its own to bind to, because its resume position arrives in `SnapshotEnd` rather than on the patch, and holds it instead by that frame sharing the delivery queue with the rows ([02](02-protocol.md#flow-control)).

The server cursor the client stores is the durability boundary for reads. Loss of this value requires a full re-sync.

## Interaction between write and read paths

When a client's own mutation is successfully applied server-side, it triggers a CDC event. That event flows through the fanout engine and arrives back at the originating client as an ordinary `LivePatch`.

**The echo is not suppressed and needs no recognition.** Applying it is idempotent. The patch carries the values the client already holds, and re-application converges under the same rule as the snapshot overlap. Pending bookkeeping never rides the echo. The dedicated `MutationApplied` reply retires the pending record, and the writer frontier the handshake answers in `HandshakeAck.writer` retires anything settled while the client was away, reporting each as `MutationApplied` so a consumer such as the browser hub learns the outcome whose reply the old socket lost.
