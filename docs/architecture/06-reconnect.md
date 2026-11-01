# 06: Reconnect and Offline

What happens when a client goes offline (network loss, device sleep, app backgrounding) and later reconnects, and what the server keeps so the resume is reliable without a full re-sync when avoidable. This chapter owns the client's persisted resume cursor and session handle, the server's oplog and its retention window, the reconnect flow, pending mutations, tombstones and delete replay, and the failover timeline check. The subscription lifecycle and the snapshot frames are owned by [04-subscriptions.md](04-subscriptions.md), the wire frames and flow control by [02-protocol.md](02-protocol.md), the local mutation queue by [03-sync-pipeline.md](03-sync-pipeline.md), what a caller may see on the catchup path by [08-authorization.md](08-authorization.md), identity and the handle by [12-identity-session-capability.md](12-identity-session-capability.md), the per-event cost of the replay by [17-fan-out.md](17-fan-out.md#catchup), and the deployment's side of failover by [20-deployment.md](20-deployment.md).

## The problem

A client that was connected receives a continuous stream of `LivePatch` bulk frames keyed by the server's commit position. When it reconnects after a gap, it needs to know where it left off (its resume cursor), what changed while it was gone (the server must have kept a log), whether that log is still available (the server retains only a finite window), and how it recovers if the log is gone (full re-sync). Mutations the client queued while offline must be sent and processed in order.

## Client-Side State

The client persists the following in local SQLite across process restarts.

| Item | Description |
|---|---|
| `cursor` | The resume cursor, the position of the last frame the client applied to local SQLite, opaque to the client. The server writes thirty-two bytes, the cluster's system identifier, the timeline, the commit LSN of the change's transaction, its transaction id and the change's ordinal in it, all big-endian so byte order is commit order (see Failover). A cursor a `SnapshotEnd` carries is followed by the snapshot's fence, the WAL insert position read inside the snapshot and the snapshot as `pg_current_snapshot()` prints it ([04-subscriptions.md](04-subscriptions.md)). It sits in the single `_connetto_meta` row and is loaded by `load_cursor_and_subjects` in [lib.rs](../../crates/connetto-client/src/lib.rs). |
| `pending_mutations` | The local mutation queue ([03-sync-pipeline.md](03-sync-pipeline.md)). |
| `subscriptions` | The set of subscriptions to re-declare on reconnect (spec + `sub_id`). |
| `session_token` | The durable session handle. For an identified run the auth store's session id is the handle, an unidentified run gets one minted at handshake, and the `resume_token` credential returned beside it is what proves the handle on the next connect. The client persists the pair outside the local replica, natively where the refresh token lives and worker-only in the browser. Cursors, the watermark and the connection registry key on it ([12-identity-session-capability.md](12-identity-session-capability.md)). |

The cursor is the client's resume cursor, and the replica records it in the same transaction that applies the frame, so a crash between apply and record loses nothing and the two never disagree. The surface a storage implementation exposes for it is [`connetto_core::Store::last_cursor`] and [`connetto_core::Store::set_last_cursor`].

A caller with no identity has an in-memory local copy ([`connetto_client::Replica::in_memory`]) and no persistent cursor. It has no offline resume and always starts from a fresh snapshot ([12-identity-session-capability.md](12-identity-session-capability.md)).

## Server-Side Oplog

The server maintains an **oplog**, an ordered log of [`connetto_server::oplog::ChangeRecord`] entries, one per committed change, that it replays to reconnecting clients. [`connetto_server::oplog::Oplog`] has two implementations, the in-memory log and the Postgres table, and the production target is the Postgres table, which a promoted standby carries with every other table (see Failover). The table is created by `PgOplog::ensure_schema` in [oplog.rs](../../crates/connetto-server/src/oplog.rs).

### Keyed by commit position

An entry is keyed by its commit position, the triple of the commit LSN its transaction's begin names, the transaction's id and the row's ordinal in that transaction. `subql` stamps every Postgres row event with that position, and positions strictly increase in the order changes commit. A change's own WAL record position does not, since an older transaction that commits after a newer one arrives second with the lower record position, and the cursor advance refuses any position below the one it holds. A snapshot's cursor is the position read before its transaction marked `before_commit`, followed by the snapshot's fence, since a commit flushed before that position can still be invisible to the snapshot. `entries_since`, `min_position`, `current_position` and `forget_through` all compare the triple, and `entries_of` finds a fence's running transactions by id, so a client holding a transaction's first row still catches up its second. An entry appended twice, which a source resuming after a failed dispatch delivers, is kept once. The interleaved shape is proven end to end by `interleaved_commit` in [`interleaved_commit.rs`](../../crates/connetto-test-harness/tests/it/interleaved_commit.rs).

### Which transactions the log dropped

A snapshot cursor's fence names the transactions its read could not see, the ones it lists as running and every one from its `xmax` on, and their rows sit below the cursor, so the window reaching the cursor does not prove it still holds them. The ingest therefore records each transaction's commit position by full transaction id in the `oplog_xids` companion table, in the statement that records the commit, widening the 32-bit id a change carries against the server's next id. After every 1,024 pruned entries it forgets the transactions older than the oldest one whose rows the log still holds, moving `xid_horizon` on the one-row `oplog_commit` table to that id in the same statement, so at most about that many recorded transactions trail the window. This is how Postgres keeps its commit log, with a status per transaction and a truncation horizon below which an id is unknown (`TransactionIdInRecentPast` in `xid8funcs.c`). A fenced resume, and the replay a fenced snapshot owes, take Case 2 with `CursorOutsideRetention` when a transaction the fence names lies below the horizon, the range from its `xmax` starts below it, or a known one committed its first row before the oldest retained entry. The index is read after the window, so a transaction pruned meanwhile is still in it or behind the horizon. An unknown id at or above the horizon wrote no row the log holds, and the resume goes on.

### Where the slot resumes

The source yields each transaction's rows and then its commit, which carries the commit's position and where its record ends. The ingest writes that commit into the one-row `oplog_commit` table and only then acknowledges it, and acknowledging a commit is the only thing that moves the replication slot, to exactly that end. So before every connect, a slot whose confirmed position is past the recorded end names changes the log never received, and one at or behind it does not. A reconnect in the same process starts right after the last row or commit the ingest handled (`ResumePoint`), so no engine cursor sees a position twice, and a changed timeline or a declared gap clears it, so the slot's own position decides.

### The event and the patch

The row image is one `event` blob, the serialized change, rather than separate old and new images, because catchup matches and checks the whole event and never reads the two images apart. The verb is a closed set of four, stored as the `connetto_change_op` enum type rather than text, and the read path never consults the column, so the test `pg_oplog_appends_and_reads_back` in [`pg_async.rs`](../../crates/connetto-server/tests/it/pg_async.rs) asserts the column's declared type and that Postgres refuses a verb outside the set.

Rows deleted from the underlying table are retained in the oplog as tombstones, `is_tombstone` true and the retained event a delete, so the server can replay deletes to reconnecting clients.

The catchup path carries the same two-version authorization obligation as the live path, and meets it from this table. The stored event is the whole change, so both row images are there, and that the previous image survives the JSON round trip is asserted by a test rather than assumed. The catchup path routes each record through `subql`'s `transitions` call, the same one the live path uses, so a row this caller may no longer see replays as the plain delete that takes it back, and a deleted row this caller could never see replays as nothing. That catching up leaves the same rows as staying connected is proven by comparing two replicas over one event sequence.

The entry also carries the compressed patch, beside the event. [`connetto_server::materializer::Materializer::dispatch`] folds every logged event once into its patch, whether or not a connected subscription matches it, because the subscriber who needs it may be the one away, and the same bytes go to the matched live consumers and into the `payload` column. Catchup sends those bytes as they are and folds nothing except, on demand, a departure notice or a withdrawal. The event stays, because catchup needs it for two other things. `Materializer::replay_match` decides whether the subscription matches, and `subql`'s `EventRow::current` supplies the post-image for the visibility question. So the entry grows by the patch on both the in-memory and the Postgres backend, and no subscription has to outlive its socket for the patch to exist. `catchup_cost` in [`catchup_cost.rs`](../../crates/connetto-test-harness/tests/it/catchup_cost.rs) asserts that a catchup across other callers' changes and its own compresses nothing.

### Retention window

The oplog is a ring buffer with a configurable retention window, bounded two ways, whichever is hit first, and both are configurable per deployment.

| Bound | Default |
|---|---|
| Maximum number of entries, `OplogConfig::max_entries` | 1M |
| Maximum age, `OplogConfig::max_age` | 72 hours |

Entries outside the window are pruned, and tombstones follow the same rules as any other entry. Pruning is unconditional on the window. No per-client cursor tracking goes into the predicate, and a client outside it gets a full re-sync, the frame [`connetto_core::messages::FullResyncRequired`] on the plane [02-protocol.md](02-protocol.md#message-families) owns. A client whose cursor predates a pruned tombstone does not lose the delete, since it is outside the window and takes the full re-sync, which re-derives every row it holds.

The log's total bytes are not bounded today [G9](../../plans/master-implementation-plan.md#g9). Planned in [the shared frame design](../../plans/master-implementation-plan.md#shared-frames-and-the-oplog-byte-bound), the window gains a byte bound, and the pruning names which bound fired.

It is a memory bound and not an abuse defence. One entry per change event, appended once regardless of who is watching or how many, so a caller cannot enlarge the log by connecting. Only a writer to Postgres adds entries.

## Reconnect Handshake

On reconnect the client presents its resume cursor if it holds one. It persists exactly one cursor and sends it in the handshake ([`connetto_core::messages::handshake::Handshake::with_cursor`] when the client holds one), and the server judges it once. Every re-declared subscription replays from that one position, and the server-side cursor bookkeeping beside it is [17-fan-out.md](17-fan-out.md#per-subscription-cursors).

```text
Handshake {
  client_id,        // the run's client label, a tab-local id in the browser relay
  resume_token,     // the credential proving the run's handle, absent on a first connect
  last_cursor,      // the client's resume cursor, absent if never synced
  cursor_subjects,  // completed subject coverage for last_cursor
  grants,           // zero or more opaque grants
  writer,           // issue a number, or present a number/device
  settled_through,  // prune the server's settled records at or below this
  schema_major,     // the application schema major the build was made against
}
```

The server responds with:

```text
HandshakeAck {
  connection_id,    // per-connection routing label, not identity
  session_token,    // the run's durable handle, in the clear
  resume_token,     // the credential proving that handle next time
  current_cursor,   // the server's current position
  schema_version,   // the schema version in force, or staleness detection off
  initial_credits,  // the flow-control grant
  writer,           // the number and frontier the connection writes as
}
```

The handle is real and everything operational keys on it. The per-subscription cursors resume through it, and the registry serves revocation and supersession by it. `resume_token` proves it, so a handle presented without the credential that proves it starts a fresh run instead of adopting the old one. The exactly-once identity is the writer, not the handle. The connection names it in `Handshake.writer` and the server answers the number and its stored frontier in `HandshakeAck.writer`, so a reconnect that presents the same number resumes from the frontier instead of replaying settled writes.

A grant that fails to resolve does not end the connection. The handshake succeeds on whatever resolved ([12-identity-session-capability.md](12-identity-session-capability.md)).

After `HandshakeAck`, the client re-sends all its `Subscribe` messages.

A silent peer is noticed on both sides. The handshake waits at most [`connetto_client::builder::SyncTuning::answer_within`] (10 s) from the `Handshake` send to the `HandshakeAck`, so a server frozen mid-dial costs one backoff and a redial. Once connected, the client sends `Ping` after [`connetto_client::builder::SyncTuning::probe_after`] (15 s) with no inbound frame and drops the transport after `answer_within` more with none, which shows the application `SyncStatus::Offline` and starts this flow. Every deadline compares real elapsed time, so a timer a throttled tab fires late never drops a link that received a frame. The server closes a session that sent nothing for [`connetto_server::session::SessionConfig::idle_limit`] (60 s) through the exit a dropped socket takes, releasing its registration and subscriptions.

## Reconnect Flow

### Case 1. Client's cursor is within the oplog window

1. The server checks the resume cursor against the oldest retained entry.
2. If the cursor is at or after it, the server can replay.
3. For each re-declared subscription, the server reads the oplog entries since the cursor that match the subscription and the caller's [`connetto_core::auth::Principal`], and sends each one. The format matches the live path, with no special catchup message type.

**Shared catch-up waves.** A resuming subscription attaches its route, reads a ceiling and enrolls with the manager's walker. One [`connetto_server::session::SessionManager`]-owned task starts immediately when idle and processes the resumes gathered during each walk as the next wave. Its cancellation guard exists before spawning and covers every wave until the queue is empty.

The worker reads each fenced member's missed transactions and retains only records that member missed at or below its cursor and ceiling. It pages the ordinary shared gap through [`connetto_server::oplog::Oplog::entries_page`] with at most 1,000 records per page. In-memory pages seek to their lower bound with binary search. A wave of at most four subscriptions matches each through `subql`'s `matches_consumer`, and a larger one matches every reached subscription at once through the read-only `match_rows`, with at most 64 records per materializer-lock hold.

After its first retention read the worker asks the transaction index about each open fenced member, and a member a missed transaction may have left the log for takes the `CursorOutsideRetention` resync before any of its records, as a single resume would.

Each session drains its own matched deliveries under its own credits. A resync outcome releases that session immediately, independently of other members' work. Closing its receiver wakes the worker, removes that caller from pending visibility questions and retries, and interrupts pending backend work when no live recipient needs it.

Transient authorization failures pause the reached members and retry the same record. A permanent policy failure ends those members with `AuthorizationUnanswerable` while other members and queued waves continue.

While waiting for worker deliveries or the cursor lock, the session receives peer input and server output. Application frames and nonterminal server output stay in FIFO order behind catch-up. Peer closure, transport errors and server termination close the catch-up receiver, releasing its membership before any terminal send. The normal run loop consumes held frames before newer frames from the same origin. The held-input budget in [02-protocol.md](02-protocol.md#flow-control) bounds deferred peer traffic independently of delivery credits.

A terminating session opens no further hidden membership subscriptions, including when termination occurs inside a hidden catch-up.

A worker panic releases every active and waiting session with `CatchupFailed`, and a later resume starts a fresh worker. Shutdown interrupts enrolled catch-up work so sessions can consume their queued [`connetto_core::messages::error::FatalErrorReason::ServerShuttingDown`] fatal frames.

Every reconnect-log read a resume makes retries a failure to reach the database under one backoff whose waits add up to at most 30 seconds ([`connetto_server::session::SessionConfig::with_resume_read_budget`]), so a resume that straddles a failover waits rather than ending. A connection's own reads, the handshake's position, and the catchup decision's window and ceiling share one such budget however many subscriptions it resumes, and the walk's reads share a budget of the same size per wave. After each page the walk reads the oldest retained entry once more, and a member whose unread records after its cursor may have been pruned before the walk read them takes Case 2 with `CursorOutsideRetention` instead of a replay with a gap, while the rest of the wave goes on. A read that still fails ends the session without a fatal frame, since no reason names it, for every member of the wave when it is the walk's read, and the session releases its registration and its subscriptions on that exit as on every other.

A change that goes live while a catchup is still replaying moves the subscription's resume position past the entries still being replayed, because the route stands before the replay starts. A replayed entry that trails it leaves the position where the live change put it and is still delivered, and the live patch reaches the client after the replay because only this connection's task sends on its transport.

### Case 2. Client's cursor is outside the oplog window, or zero

1. The client's resume cursor predates the oldest retained oplog entry, or the client has never synced. It cannot catch up incrementally.
2. The server sends `FullResyncRequired { reason: CursorOutsideRetention }`, and the client shows a "re-syncing..." state and clears local data for the affected subscriptions.
3. For each re-declared subscription, the server sends `SnapshotBegin` (control), one or more `SnapshotPatch` bulk frames carrying the matching rows, then `SnapshotEnd(current_lsn)` (control).
4. The client applies the snapshot as a full replacement, not a merge.

A cursor past where its timeline ended, or one the server cannot read, takes this path with `CursorBeyondHistory` instead, whatever the window holds (see Failover).

The notice waits for the read. `FullResyncRequired` is sent only once the fresh snapshot has been read, immediately ahead of its frames, because it is also the instruction to discard local rows. A read that fails instead draws the same single bare refusal as any other cause, so the caller learns nothing about the name it guessed and keeps its rows for a snapshot that never arrived.

[`connetto_core::messages::FullResyncReason::AuthorizationChange`] covers a permission row appearing or disappearing on the Postgres change log. It triggers a per-subscription resync, produced by the owning session's own task so the notice and its replacement stay one ordered pair ([08-authorization.md](08-authorization.md) for the mechanism). Adding that variant was itself a wire change, because [`connetto_core::messages::FullResyncReason`] has no fallback for an unknown value.

### Case 3. The schema changed while offline

An incompatible schema change is a new application schema major ([02-protocol.md](02-protocol.md#the-application-schema-major)), and the client's transition across one is **flush, update, resync, convert the local tier**.

1. **Flush.** Before an in-app update the application reads the drain gate, the pending work connetto reports as un-acked writes plus unsent files, and refuses or warns while it is non-empty. A store can install an update by itself, so the steps below hold whatever the gate did not drain.
2. **Update.** The handshake carries the build's major. A build behind the server is told [`connetto_client::ClientError::UpdateRequired`] before any subscription catchup or pending replay, a build ahead of it [`connetto_client::ClientError::ServerBehind`], and the same major built from another schema still [`connetto_client::ClientError::SchemaOutdated`].
3. **Resync.** The updated build finds a replica recording an older major and rebuilds it in place, in one transaction. Each queued write the old major captured stays in its writer's queue under that major, renamed to the wire form that major's server reads, unsent files move to `_connetto_held_content_*` tables, and `_connetto_held_batch` records the old major and its bundle hash. Every synced table, the cursor, subscriptions and aggregates are dropped, the build's replica DDL is applied, and the new major and hash are recorded. The writer queues and the file stay, so the server's per-writer frontiers, which the deployment's Postgres keeps across majors, still match the chains, and the device-private tier still attaches under its key salt. The first connect is then a fresh full snapshot through Case 2's full-replacement path. A drain connection presents the held major, its hash and each write's writer, and uploads the writes unchanged to the old major's server, which a deployment keeps up until its clients drain. The connection's own wire sends no write queued after an older major's write until that write settles, so writes reach the servers in the order they were made. A held write that server rejects, or a held major that is gone, reaches the application as a typed notice, because the rebuild left nothing to roll back.
4. **Convert the local tier.** The device-private tier records its major in its own `PRAGMA user_version`. When it is behind, connetto opens it alone under the replica key before the attach and runs each boundary in its own transaction. The structural step generated at build time from the exported tier history, then the application's data step, then on the last boundary a check against the build's tier DDL. A step that fails rolls its boundary back and refuses the open.

A replica recording a newer major than the build, or this major from another schema, is refused at open, because no step can make it readable.

### Case 4. The server moved to another major while the transport was gone

Case 3 is the fresh-open story, and the handshake refusal is the only place a build learns the deployment moved. A lost transport meets the same moved server on the way back, and the reconnect path says the same thing there rather than retrying a refusal a retry can never cure.

The native recovery loop ends on the refusals the build can never attach under. `UpdateRequired`, `ServerBehind`, `SchemaOutdated` and a [`connetto_client::ClientError::Refused`] carrying [`connetto_core::messages::error::FatalErrorReason::ProtocolVersionMismatch`] end the pump the way [`connetto_client::ClientError::Auth`] already ends it as `ReauthRequired`, so the app hears [`connetto_client::ClientEvent::Stopped`] naming the reason and [`connetto_client::ClientEvent::Closed`] after it. A server that closed the live session with one of those reasons said so once as [`connetto_client::ClientEvent::ServerClosed`] before the loop began, so the loop skips the first redial of the terminal class. Everything else keeps the retry the policy gives it. `RateLimited` and `ServerShuttingDown` wait for a server that comes back, and after a `RateLimited` refusal the next wait is at least the server's `retry_after_ms`, the field the reason carries so a client waits once rather than probing for the answer. The default policy has no attempt ceiling, and the application bounds it with `with_reconnect`.

The relay hub holds a tab to the worker's major at the tab's own handshake, so a tab that arrives while the hub redials is already checked, and a stale build is refused with the reason named. The case the handshake leaves open is the tab attached before the deploy. When the hub's own attach draws a refusal the hub's build can never attach under, the hub ends the retry there and tells every attached tab the typed fatal it already tells a refused tab, so the platform glue logs the ended hub and the tabs redial a fresh hub whose handshake names the reason to a stale build. A live `ServerClosed` of the terminal class skips the hub's first redial for the same reason, and a `RateLimited` refusal sets the same floor on the hub's next wait. No wire reason names a stale schema, so a hub that draws `SchemaOutdated` tells its tabs `ServerShuttingDown`, and the fresh hub's ack carries the schema version the tab's own client checks.

Proven natively by `mismatch_recovery` in [`mismatch_recovery.rs`](../../crates/connetto-client/tests/it/mismatch_recovery.rs) and by `hub_mismatch` in [`hub_mismatch.rs`](../../crates/connetto-web/tests/hub_mismatch.rs).

## Pending Mutations on Reconnect

After the reconnect handshake and subscription re-declaration:

1. The client sends all pending mutations from the local queue, in `client_seq` order.
2. The server processes them against the current PostgreSQL state.
3. Each mutation may succeed, be rejected, or produce a conflict, the same as the normal write path.

**Ordering note**. Pending mutations are sent after subscription re-declaration, so the server's subscription state is current when the mutations' CDC side effects are emitted.

## Tombstones and Delete Replay

When a client catchups via the oplog, tombstone entries (deletes) must be replayed so the client removes those rows from local SQLite.

The delete is delivered as a `LivePatch` frame with `op = Delete`. The client applies it if the row exists locally, or ignores it if the row is not present (idempotent).

Tombstones enable delete replay for rows the caller holds locally. Per the read-denial principle in [08-authorization.md](08-authorization.md#principles), a tombstone for a row the caller could never see must not be forwarded. An authorization change that would leave a stale local copy is handled by `FullResyncRequired` instead, and a tombstone pruned by the window reaches its client through the full re-sync (see Retention window).

## Failover

One primary and any number of standbys, and connetto reads one change log from whichever is primary. Physical streaming replication carries every table connetto reads and writes byte for byte, the reconnect log, the mutation watermark and the auth store with the application's own tables, so a promoted standby holds everything the old primary had sent it. A cursor is a position in the reconnect log table and stays meaningful there. Several writable databases kept in sync, multi-master, is out of scope, because everything in connetto rests on one ordered change log.

### The recipe

1. **Stream to each standby through a physical slot**, `primary_slot_name` on the standby, with `hot_standby_feedback = on`.
2. **Create connetto's logical slot as a failover slot**, `SELECT pg_create_logical_replication_slot(name, 'pgoutput', false, false, true)`, which needs Postgres 17 or later, and set `sync_replication_slots = on` on the standby, whose `primary_conninfo` must name a database. The standby then holds a copy of the slot that the promotion turns into the primary's, which becomes usable once the consumer has confirmed past the standby's catalog horizon. Without a failover slot the slot is recreated after a promotion, which is the hole [10-subscription-materializer.md](10-subscription-materializer.md) describes and costs every client a full re-download.
3. **Name the standby's physical slot in `synchronized_standby_slots` on the primary**, so the change feed is never sent a change the standby has not received. Without it connetto can deliver changes a failover then loses, which the timeline check below repairs by re-downloading.
4. **Choose synchronous commit where a lost write is unacceptable**, `synchronous_standby_names` with `synchronous_commit = on` or stronger. Asynchronous replication loses whatever the standby had not received when the primary failed, writes the primary had acknowledged, rotated refresh tokens and revocations alike.
5. **Give the servers one address and move it to the promoted standby**, a DNS name, a virtual address or a proxy. connetto does not choose the new primary.

### What clients experience

The server process stays up. Its database connections drop, and its change feed reconnects with backoff until the address answers again. Before the feed reopens the server reads the database's timeline history over a replication connection, and a new timeline closes every live connection with [`connetto_core::messages::error::FatalErrorReason::DatabaseTimelineChanged`], because a live connection never presents its cursor again. A planned switchover that lost nothing closes every connection too, and each resumes by catching up, with no re-download.

Every cursor carries the cluster and the timeline it was issued on, thirty-two bytes with the system identifier and the timeline ahead of the commit position. A reconnecting client whose cursor lies on the current timeline, or on an earlier one at or before the point where that timeline ended, catches up from the reconnect log with no re-download. One past that point was sent changes the promoted database never received, so its subscriptions, hidden membership subscriptions included, get `FullResyncRequired { reason: CursorBeyondHistory }` and a replacement read from the new primary, and those rows leave the device. A nonempty cursor the server cannot read, of any other length or with a fence that does not parse, takes the same resync, and so does one naming another cluster. The synced slot can trail what the old primary confirmed, so the feed may be sent a change twice, and the reconnect log keeps one entry per position.

What a restore to an earlier point does to a client whose queued writes are ahead of the watermark belongs to [20-deployment.md](20-deployment.md). A database restored into another cluster starts again at timeline 1, which this check does not see, and the identifier the cursor carries resyncs it.

`failover` in [failover.rs](../../crates/connetto-server/tests/it/failover.rs) proves the server's side on Postgres 18. It cuts the standby off, commits rows a live client is sent, kills the primary, promotes the standby, moves the address and asserts the three client outcomes above. It leaves `synchronized_standby_slots` unset to produce the lossy case, so steps 3 and 4 of the recipe rest on Postgres's documented behaviour rather than on that test. The client's side of the resync, taking rows that overlapping subscriptions both claim, is tested by `truncate_resync` in [`truncate_resync.rs`](../../crates/connetto-client/tests/it/truncate_resync.rs).
