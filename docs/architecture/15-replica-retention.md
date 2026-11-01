# 15: Replica retention, eviction, and physical trimming

Why the client replica grows, what covers a row, and the passes that remove and physically trim what nothing covers. This chapter is about the client-side replica only. The server-side oplog window a client may fall out of is the [retention window](06-reconnect.md#retention-window), the page codec and the key that opens the replica are owned by [14-at-rest-encryption.md](14-at-rest-encryption.md), the shared public store by [12-identity-session-capability.md](12-identity-session-capability.md#public-tables-may-be-shared-across-identities), and the clock the grace countdown reads by [21-clocks.md](21-clocks.md).

## Not the server-side oplog

The word "retention" also names the server-side oplog ring buffer in [06-reconnect.md](06-reconnect.md#retention-window), a Postgres table bounded by entry count and age that determines how far a client can fall behind before it must re-sync. That table is managed entirely by the server and the client never touches it.

This chapter is about the client-side replica, a SQLite file that holds a local copy of subscribed query results, and the mechanisms that keep it from growing without bound.

## Why the replica grows

The replica holds the union of every subscribed query result the client has ever received. It grows because subscriptions widen or accumulate, not because of a leak. A subscription that covers more rows, a new subscription against an additional table, or a time window that advances each day to include another batch of rows, each causes the replica to grow by the rows it adds. No maintenance pass changes the fact that a broadly-subscribed client holds more data than a narrowly-subscribed one.

The primary control on replica size is therefore subscription shape, not deletion. The trimming mechanisms in this chapter reclaim space freed by eviction, and the callable free-up-space pass reclaims space a deletion freed besides, but they do not substitute for a subscription design that bounds what the client holds.

## What covers a row

Coverage is application-driven through two mechanisms, watches and pins, and through nothing else. There are no schema-declared keep rules and no connetto-side retention policy, because only the application knows why it holds data.

**Watches.** [`connetto_client::live::ConnettoClient::watch`] ties a wire subscription to a handle, and identical queries share one reference-counted `WireSub` entry. On top of that, a watch-backed subscription survives its last drop by a grace period, so navigating away and back does not pay a fresh snapshot. The last drop starts a countdown, and the unsubscribe goes out when it expires. The grace defaults to five minutes ([`connetto_client::DEFAULT_GRACE`]), is capped at ten ([`connetto_client::MAX_GRACE`]), and is per-watch configurable within the cap through [`connetto_client::live::ConnettoClient::watch_with_grace`], [`connetto_client::live::ConnettoClient::watch_fn_with_grace`] and [`connetto_client::ConnettoConnection::subscribe_spec_with_grace`], each clamped to `MAX_GRACE`, with `Duration::ZERO` restoring the immediate unsubscribe. An aggregate watch takes no grace at all, because an aggregate handle holds no replica rows. The cap is deliberate. Wanting to outlive it is by definition a pin, and the cap enforces that boundary mechanically.

Across a restart the countdown runs from the recorded stop moment, so a subscription already past its grace is not re-declared at launch. `replay_subscriptions` in [lib.rs](../../crates/connetto-client/src/lib.rs) re-declares only records still live. A subscription the app died still watching has no recorded stop moment, and it takes a fresh countdown from launch, so the UI has time to re-claim it as screens mount and an abandoned one retires. The anchor is written to the replica when the connection opens rather than into the in-memory seed, because the countdown has to survive this run as well, and it skips pinned records. A pin's grace is zero by design, so anchoring one would end it at the first pump. The accepted asymmetry stands. A query unwatched moments before a crash is protected for less time than one watched through it.

**Pins.** A pin is a durable, named request to keep a query's rows synced and covered until the application removes it. [`connetto_client::live::ConnettoClient::pin`] creates or replaces, [`connetto_client::live::ConnettoClient::unpin`] ends, and [`connetto_client::live::ConnettoClient::pins`] lists. `unpin` ends the subscription at once rather than starting a countdown. The documented use is a dataset downloaded deliberately and cleared explicitly, and a grace tail would keep the server streaming what the application just released. A handle still holding the same query is protected by its reference count instead, which is where handles live. The application chooses the name, so startup pinning is idempotent (same name and same query is a no-op) and re-pinning a changed query under the same name is the upgrade path. Collisions between application features are the application's responsibility. A pin has no clock and no handle. It survives closing and reopening the app, and it survives offline.

Durability is not optional for the offline case. A declaration is recorded in the replica and sent on the first connection, so watching offline works. What persistence buys is the part a running process cannot provide. An application restarting offline re-establishes its watches only because the record outlived the process, and only that record keeps its rows covered until connectivity returns. This is the case that motivated the pin. A dataset downloaded deliberately before going offline, cleared explicitly after coming back.

**Notifications need neither mechanism.** A watch handle is not tied to a page. The application can hold one at application scope, or observe the raw event stream through [`connetto_client::live::ConnettoClient::events`], and turn changes into notifications while the process runs, whatever page is mounted. Startup re-declaration means catch-up is already flowing before any page mounts. Waking a dead process is platform push territory, server-side and outside the replica.

**A local write marks transient interest, and post-ack interest is always explicit.** Every local insert or update marks interest in its rows at capture time, the same way `subql`'s row tracking learns interest from parsed ops. The mark is in-memory bookkeeping over the durable pending queue (`_connetto_pending`). It is set at capture, cleared when the server acknowledges the mutation, and rebuilt from the queue at boot. It is delivery bookkeeping rather than a third retention mechanism, and after the ack the row lives or dies by watches and pins like any other. A typed write surface makes the choice explicit at the write site. The plain diesel write path says "write this, I do not care about it afterwards", and the typed variants ([`connetto_client::live::ConnettoClient::insert_watched`], [`connetto_client::live::ConnettoClient::insert_watched_with_grace`], [`connetto_client::live::ConnettoClient::insert_pinned`], [`connetto_client::live::ConnettoClient::update_watched`]) compose the write with a mark of interest through the existing mechanisms, a watch over the written row or a pin, never through hidden per-row state. The table is inferred from the value's own `Insertable` type rather than passed, a parameter could only disagree with what the type already fixes, and the generated key is read back through a `RETURNING` clause the moment the local insert runs, with the predicate registered as the table's `find` over that key. Batch and free-form writes stay on the documented two-call pattern, safe because the write-time mark covers the gap between write and registration regardless.

## Rotating time-windowed subscriptions

A subscription registered with a predicate such as `WHERE created_at > ?` fixes the bound value at registration time. The bound does not advance as time passes. A client that subscribed in January with a 30-day lookback still holds January rows in December unless it replaces the subscription.

Rotation means tearing down the old subscription and opening a new one with a fresh bound. The new bound excludes rows the old bound held. Those rows receive no delete instruction from the server. The server knows nothing about the client's retention policy and issues no instruction. Ending the old subscription releases its holdings, so a row only it held becomes a candidate for local eviction.

The client is the only party that can determine when a row is held by nothing. A row that falls outside one subscription's new window may still be held by a second overlapping subscription, may be referenced by a pending local write that has not yet been uploaded (protected until the ack, see Local eviction), or may be data the user wants kept for a history pane, which the application expresses as a pin or a held watch. Evicting any of those would lose data the user did not choose to discard, so the eviction step is always separate from the rotation step, and what is kept is owned by the application through watches and pins.

## Local eviction

Eviction removes from the replica every row of a synced table that no holding names and no unsent write of this identity keeps (see What holds a row). Its tables are every keyed application table in `main` and every table the build shares in the store. A pass never reads the device-private tier, which lives in its own schema, nor connetto's `_connetto_` tables, and a rowid-only table, whose rows SQLite records nothing for, has no key a holding could name and is left alone. An acknowledged write to a table no subscription reads therefore goes at the next pass, since nothing holds it once the server has answered.

The passes and their scope.

| Moment | Tables | Offline |
|---|---|---|
| [`connetto_client::live::ConnettoClient::tidy`], and the automatic pass that calls it | every synced table | trims only |
| An unsubscribe, a grace expiry or an unpin that `drain_dropped` retires | the tables whose holdings the record change released | the record is ended and its holdings go, its rows wait |
| Attach, when a record is no longer live | every synced table, since the release happened offline or before a restart | not reachable |
| The `SnapshotEnd` that leaves no resync outstanding | the tables the resync notices released and each resynced subscription's own tables, whose replacement is the server's whole answer for them | not reachable |
| `TableTruncated` | that table, at the notice | not reachable |
| A departure batch | the departed keys alone, in the frame's transaction | not reachable |
| An identity leaving the shared store | every shared table | runs online or not |

Automatic won over app-only triggering because retention exists to bound the replica by default, and the application already expressed its intent by letting the subscription end. The stated cost. Deletes run at moments the application did not schedule, though only ever on rows nothing holds. The callable pass trims the freelist whether or not the pass removed a row, so pages the application freed by deleting rows return to the filesystem too, while an automatic pass trims only when it deleted something.

Two guards on every pass. Rows referenced by a local write the server has not acknowledged are never evicted, whether the write is still in the outbox or already in the pending queue. The keys of every unsent write, the outbox's old and new keys and every queued entry's, are kept by the pass, protection expires by itself on ack, and a row a peer applied provisionally is kept the same way until the server rules on it. And no pass deletes while the transport is down, because a row discarded offline cannot be re-fetched until connectivity returns, which is the empty-screen case. Grace clocks keep running offline, only the deletion waits. A stop moment that a clock stepped back puts ahead of the replica clock counts from now, so a grace never outlasts its length ([21-clocks.md](21-clocks.md)). The callable pass still trims while the transport is down, because reclaiming freelist pages discards nothing and needs no server.

A table in the shared public store keeps its holdings in the store, so every identity's pass reads what every other identity holds, its unsent writes included, as [12-identity-session-capability.md](12-identity-session-capability.md#public-tables-may-be-shared-across-identities) records under "Public tables may be shared across identities".

The residual pass runs by default on a measured threshold. The client counts rows applied per application table since the last pass, one map bump per row from the update hook while a server patch applies, and compares their sum against a configurable threshold ([`connetto_client::builder::SyncTuning::with_residual_threshold`], default [`connetto_client::DEFAULT_RESIDUAL_THRESHOLD`], 10,000 rows). At the crossing a typed [`connetto_client::ClientEvent::TidyDue`] fires once, re-arming when a pass resets the measure, and under the default [`connetto_client::ResidualPass::Automatic`] the connection runs `tidy` itself right after the apply that crossed, inside `ConnettoConnection` so the native client, the browser worker and raw connection users all get it with no wiring. `ResidualPass::Manual` supplants the default. The event still fires and the application calls `tidy` at its own moment. The pass still never evicts while the transport is down, the trim keeps its ratio gate, and the measure (per-table counts plus the freelist share) is queryable at any time through [`connetto_client::ConnettoConnection::residual_pressure`]. Per-table thresholds were refused (the pass sweeps the whole replica whichever table fires, so the precision changes nothing), triggering on the freelist ratio (it only grows after deletes, so pure accumulation never moves it), and the exact unheld count (measuring it costs what the pass costs). Proven by `residual` in [residual.rs](../../crates/connetto-client/tests/it/residual.rs).

A raw `unsubscribe` never strands rows. Connected, `unsubscribe` forgets the record, which releases its holdings in the same transaction, runs the pass over the released tables, then sends the frame. Offline, where the guard above bars deletion, it ends the record, which releases its holdings, and the next connected attach forgets every record no longer live and passes over every synced table. An ended record is never replayed as a subscription. Proven by `a_raw_unsubscribe_reclaims_what_its_subscription_synced` and `an_offline_unsubscribe_defers_eviction_to_the_next_connected_attach` in [retention.rs](../../crates/connetto-client/tests/it/retention.rs).

Eviction must run under the write exemption. `ExemptWrites` in [lib.rs](../../crates/connetto-client/src/lib.rs) raises it, as it does when server patches apply, so the outbox records none of those deletes. A recorded eviction delete would be uploaded as a real client mutation and applied to the Postgres backend, discarding server data the user did not ask to delete.

Local-tier rows are structurally safe from eviction. No `SubscriptionSpec` can ever carry a frontend-tier table, so no frame holds a tier row, and the tier lives in its own schema, which no pass reads. Device-local data survives a retention pass by placement rather than by a runtime guard, because the FK closure rule that enforces the tier boundary at generation time also forecloses cascade paths into the tier.

## What holds a row

The replica records which holder keeps which row, and a row nothing holds goes at the next pass. Every row a server frame writes is held in `_connetto_holding (holder, tbl, key)` by the subscription the frame was addressed to, or by a hidden membership subscription's `connetto-membership:<table>` label, one holding per subscription per row. A frame for a subscription no longer on record holds nothing. A departure releases the addressed subscription's holding, and the row goes in that frame's transaction once no holding and no unsent write keeps it. A delete upstream releases every holding of the row. A resync releases the notified subscription's holdings, its replacement takes back what it still delivers, and the rows nothing took back go when the last outstanding replacement ends, while a `TableTruncated` notice releases every subscription's holding on the table and deletes at once, sparing unsent writes. A subscription's holdings go in the transaction that ends its record, forgets it or finds it past its grace, and a membership label's go once no live query reads its table through a membership term. A new store generation releases the account's holdings, since its replays deliver every row again.

A holding is present or absent, never counted, so a frame applied twice changes nothing and no count can leak or be lost. Its key is the clause the outbox triggers render a written row under, `("a" IS 1 AND "b" IS 'x')`, computed with `quote()` over the stored row, so one text matches a row to its holdings and a key the column's affinity converted still matches. A table the build shares keeps its holdings in the store's `_connetto_shared_holding (identity, holder, tbl, key)`, where an identity's unsent writes hold their rows under `connetto-unsent` from the write's own transaction until the server answers. A row a peer applied is held under `connetto-provisional`, in `main` or under this identity in the store, from the apply's transaction until the server or the author's frontier rules on its record. The subscription releases, a beyond-history resync's and a new store generation's included, never take either reserved holder. A truncate takes the table's provisional holdings with the records it drops, keeping only unsent writes.

Overlap falls out of the same rule. Two subscriptions holding row 7 each keep a holding, and ending either releases only its own, so the row stays while the other holds it, and with no holding left it goes. Dropping a subscription never examines another's query. Recomputing what a subscription covers from its predicate at every pass was refused. A predicate keeps a superset of what a paginated subscription was delivered, keeps a matching row the server never delivered, and cannot answer for a hidden membership subscription that has no declared query, while each pass would re-run one scan per surviving subscription.

The schema is normalised so a shared query is stored once. Three tables in the replica, named with the `_connetto_` prefix that already marks connetto's own bookkeeping there. The query text keyed by its own id and unique on the text, the subscription carrying its id and a reference to that query, and the bind values keyed by subscription and position. Two subscriptions differing only in a bind value therefore share one row of query text.

The subscription list lives in the replica. It must be durable, never synced, per account, destroyed with the account, and untouched by anything that clears synced rows. The replica satisfies all five, and `_connetto_meta` already lives there on the same reasoning. Connetto's own bookkeeping must not depend on a feature the application may not have asked for, while the optional device-private tier would leave the shipped `connetto-client` binary and every caller without a tier with no list at all.

### Departures and deletes

A row-by-row test is wrong for deletions, and the wire has to close the gap.

Table `orders`, one client, two subscriptions. **A** is `status = 'open'` and **B** is `customer = 42`. Row 7 is open and belongs to customer 42, so both want it, and there is **one** copy of it in the replica.

Suppose the server announced a departure as an ordinary, unmarked delete. Two different things would then arrive as **the same frame**, a delete for row 7 addressed to A.

- **The row is deleted in Postgres.** It leaves both subscriptions, so the server sends a delete to A and a delete to B.
- **Its status changes to closed.** It leaves A only, so the server sends a delete to A and an ordinary update to B, which still wants it.

Now apply the naive rule, which is to check whether another subscription's predicate still matches before deleting locally. In the second case it is right. `customer = 42` still matches, the row stays, and B keeps what it is entitled to. In the first case it fails, and it fails twice. A's delete arrives, B's predicate is checked against the row still sitting in the replica, `customer = 42` matches, so the row is kept. Then B's delete arrives, A's predicate is checked against that same untouched row, `status = 'open'` matches, so it is kept again. **Each delete is vetoed by a subscription that is itself being deleted.**

| | Row deleted upstream | Row leaves A, B still wants it |
|---|---|---|
| No departure sent | correctly removed | **wrongly kept for ever**, over-retention |
| Unmarked departure delete | correctly removed | **wrongly removed**, B silently loses it |
| Naive predicate check | **never removed** | correctly kept |
| The marked delete over holdings | correctly removed | correctly kept |

A deletion and a departure from a subscription's window would be indistinguishable on the wire, and a predicate cannot separate them because it is evaluated against a row that is leaving either way. **The delete therefore carries which it is**, as the patchset op's own session-format indirect flag. The server synthesizes a departure delete `indirect(true)` and a genuine delete direct, restoring at fan-out the distinction the per-subscription translation erases. A Postgres `DELETE` event fans out as deletes everywhere, and a Postgres `UPDATE` event fans out as a delete only for the subscriptions the row left. The server sends a departure as a patchset of indirect deletes and nothing else, so the client recognises a departure batch by its shape and applies it through the holdings rather than natively. The flag is native to the patchset format and already round-trips, so catchup and resync inherit it, and the convention is scoped to server-synthesized patchsets, while client-captured changesets keep the flag's native trigger-caused meaning.

One cost, stated rather than buried. A holding is one bookkeeping row per held row per subscription, which on a narrow table is as much storage as the data. A pass is one anti-join per table against an index on the holdings' table and key.

## Physical trimming

### Eviction leaves free pages

SQLite deletion does not shrink the file. Freed pages go onto the freelist and are available for reuse by future inserts, but the file is not truncated. A bulk eviction that removes a large time window produces a freelist of the same size and the file occupies the same disk space as before. Physical trimming returns those pages to the filesystem.

### The auto_vacuum mode

The `INCREMENTAL` auto-vacuum mode keeps freelist bookkeeping up to date at every commit without reclaiming pages at commit time. `FULL` mode reclaims pages at every commit, which would stall the pump on every server patch. `INCREMENTAL` defers reclamation to an explicit `PRAGMA incremental_vacuum` call, which the trimming pass controls.

The critical constraint. `auto_vacuum` is stored in the file, not the connection. Changing from `NONE` on a populated database silently does nothing until a full `VACUUM` rewrites the entire file. The mode must be set before the first table exists, so the deadline is the first release, at which a populated replica cannot change it short of the rewrite.

### The trimming pass

The trimming pass runs after each eviction, and the callable free-up-space pass runs it whether or not the eviction removed a row, so a freelist grown by ordinary deletes of covered rows is reclaimed too. The mechanisms are diesel's public surface, and the SQLite facts behind each call live in diesel's merged pull requests rather than in this repository.

1. Read `SqliteConnection::freelist_count` and `SqliteConnection::page_count`. If the ratio of free pages to total pages is below a threshold, skip the pass. Triggering on ratio rather than a schedule avoids reclaiming a file that has no slack.
2. Run bounded `SqliteConnection::incremental_vacuum`. The page-limit parameter is a latency control. A large freelist does not stall the pump in a single step. The helper drives the pragma to completion, which matters because the pragma frees one page per result row, so a consumer that steps it once reclaims one page regardless of the limit it passed.
3. Run `wal_checkpoint(None, WalCheckpointMode::Truncate)`. Pages reclaimed by `incremental_vacuum` could otherwise reappear inside a grown WAL file, leaving the file occupying the same space at the filesystem level. The `Truncate` mode moves WAL frames into the database file and shrinks the WAL file to zero bytes.
4. Inspect `WalCheckpointOutcome.busy`. A checkpoint blocked by an open reader reports the blockage through the `busy` field and does not fail. The pass records that it did not complete fully and defers to the next maintenance window rather than retrying in a tight loop.

## The create path

connetto creates the replica through `open_inner` in [lib.rs](../../crates/connetto-client/src/lib.rs). The pragma sequence on the `open` path is.

1. `cipher::unlock` (the key pragma, must be the first SQL statement on any encrypted connection)
2. `PRAGMA journal_mode=WAL`
3. `harden::harden_replica_connection` (defensive mode, trusted schema off, three limits, and the attach lockdown), then `PRAGMA foreign_keys = ON`
4. `SqlFunctions` installation
5. Application DDL (`sqlite_ddl`), run without its synced references
6. `META_DDL` (connetto's `_connetto_meta` and `_connetto_pending` tables)

`PRAGMA auto_vacuum = INCREMENTAL` joins this sequence after step 2 and before step 5. After step 2 because the key pragma in step 1 must precede any statement that reads the database header, and WAL is established immediately after it. Before step 5 because both the application DDL and `META_DDL` create tables, and the mode must precede the first `CREATE TABLE`. On the `open_existing` path the database already has its schema and the mode is already stored in the file from the original create, so the pragma applies only on the `open` path.

[14-at-rest-encryption.md](14-at-rest-encryption.md#ordering-constraints) is the authoritative source for the unlock ordering constraint. This chapter records only that `auto_vacuum` joins that sequence and where it lands relative to the DDL steps.

## Browser constraints

OPFS quota is the scarce resource in the browser, so a replica that grows large faces a higher eviction risk under storage pressure, and the bounded `incremental_vacuum` the trimming pass runs matters more there than natively. It reclaims a configurable number of pages per call and never blocks the pump. No full-compaction path exists, in the browser or anywhere else, because every replica is `INCREMENTAL` from creation (see The auto_vacuum mode and The create path), so no `VACUUM` rewrite and no `vacuum_into` output to swap into service ever arise.

## Upstream dependency

The five diesel APIs the trimming pass needs (`SqliteConnection::auto_vacuum` and `set_auto_vacuum`, `page_count` and `freelist_count`, `incremental_vacuum`, and `wal_checkpoint` with its mode and outcome types) are diesel's public surface, and the workspace's diesel reaches every one. Upstream diesel does not hide `diesel::table!`'s undocumented generated items from `missing_docs`, which the root `Cargo.toml` sets to `forbid`, so every column of every table in the workspace is documented and a new table must be too.
