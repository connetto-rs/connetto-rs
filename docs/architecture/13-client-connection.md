# 13: Client Connection Layer

This chapter covers the client-side `diesel` connection and what it makes transparent to the application, the builders that produce a running client, the live-query layer, the worker-tab split in a browser, and the device-private tier beside the replica. The wire the frames ride is [the protocol](02-protocol.md), the write path end to end is [the sync pipeline](03-sync-pipeline.md#write-path), the aggregate problem is [aggregate queries](05-aggregate-queries.md), and what keeps a row on the device is [what covers a row](15-replica-retention.md#what-covers-a-row).

## The connection

[`connetto_client::ConnettoConnection`] wraps the `SqliteConnection` whose outbox records every application write, and implements `diesel`'s `Connection` and `LoadConnection` traits. It is the connection everywhere a real `SQLite` file is held, natively in-process and in the browser's dedicated DB worker over OPFS, the same object with a worker `Transport`. It is `Send`, so it can move between threads, but it is driven by one task at a time and is not `Sync`.

## The browser split

In the browser the elected DB worker owns the single OPFS connection and the upstream `WebSocket`, and a tab holds no connection to the replica. It runs its own unmodified `ConnettoConnection` over an in-memory mirror, built by `WebClientBuilder`'s `attach()`, reaching the worker over the binary wire protocol the server speaks, served worker-side by the `RelayHub` in [relay.rs](../../crates/connetto-web/src/relay.rs). The worker holds one connection to the replica, because the storage pool allows one handle per file and a second open handle trips a `debug_assert`, and the accepted cost is that each tab mirrors its subscribed rows in memory, fine for screen-sized subscriptions.

### Boot identity and failure routing

A spawn, `spawn_db_worker` in [workers.rs](../../crates/connetto-web/src/workers.rs), mints a boot identity, puts it on the worker's URL and leaves it in a global for the blob bootstrap, because a blob worker's location carries no query. The hello channel is one name for the whole origin, so a failure that names nothing is delivered to whoever is waiting, and [R42's worker replacement](https://github.com/connetto-rs/connetto-rs/blob/82983dff19effa9cfb521370a7794d29e51afb51/plans/master-implementation-plan.md#r42-several-accounts-stay-signed-in-at-once) makes that wrong, since a tab waiting for the new worker could adopt the failure of the worker being replaced. Every stage of a boot reports its failure, from the spawning context's `error` event to the worker's own `boot()`, and a stage that reports nothing is a fifteen second wait for the page. `ready` stays untagged, because a ready worker serves every tab, and a worker spawned with no identity stays silent.

### Whose failure a waiter may act on

Every waiter acts only on a failure that names an identity it knows, its own spawn or one it heard announced, because acting on another boot's failure is the bug this replaced. The spawner's waiter acts only on the identity the spawn returned, a reconnect handed none on the last boot it spawned while that boot is in flight, and a waiter that named no identity on the newest `booting:<identity>` it heard, so an untagged failure waits out the deadline. Letting only the spawning tab act makes every other tab wait the full deadline on a broken deployment, and trusting the newest boot still guesses while two boots overlap.

A spawn announces `booting:<identity>` and keeps answering `ask`, with `failed:<identity>:<detail>` once it has failed, because a broadcast is not replayed. A waiter that named nothing keeps only the newest announcement, because a superseded boot's failure can still be in flight. A ready worker posts `ready` for the waiters and `ready:<identity>` beside it, since an outgoing worker can answer after its replacement was announced and an untagged readiness would then bury the replacement's failure. An error thrown after ready is logged as a runtime fault, not replayed.

### The readiness deadline

The wait `await_db_worker_ready_bounded` in [intake.rs](../../crates/connetto-web/src/workers/intake.rs) holds a 15 s base deadline, and a boot silent past it fails with `IntakeError::NotReady`, carrying the last stage named. The worker answers that silence with `progress:<identity>` on the hello channel, or `progress:<identity>:<stage>` naming the step it has just completed, and each `progress:` the waiter acts on moves the deadline to at least 15 s from that message, equal to the worker's own inactivity bound in `subscribe_and_boot`, so a boot that keeps reporting never times out while a silent one still fails, with no total cap, because a slow device beyond a cap would fail while visibly working. The bootstrap posts a plain beat while the module loads, and Rust names each step through `report_boot_progress` in [boot/mod.rs](../../crates/connetto-web/src/workers/boot/mod.rs), so a stalled step cannot hide behind the beat.

The wasm-smoke suites under [examples/wasm-smoke/tests](../../examples/wasm-smoke/tests) prove the topology end to end.

## Building a client

**One construction path, three layers.** [`connetto_client::ClientBuilder`] in [core.rs](../../crates/connetto-client/src/builder/core.rs) is the platform-neutral builder every client goes through, and nothing else constructs a connection. `NativeClientBuilder` in [native.rs](../../crates/connetto-client/src/builder/native.rs) adds the `wss://` dialer, the OS keyring and the provider sign-in, and `WebClientBuilder` in [builder.rs](../../crates/connetto-web/src/builder.rs) adds OPFS, the browser sign-in and the worker's services. All three take a `SyncSchema`, a `SyncTuning`, a reconnect policy, share keys, a sign-in and a `Gate`.

**The schema is one value.** [`connetto_schema::translate`] emits the replica DDL, the policy tables and views, the device-private tier and the schema version together as a `SchemaBundle`, and `SyncSchema::new(bundle)` is the only way a build takes them, so a regenerated DDL cannot travel with a stale version. `SyncSchema` registers `uuidv4()`, the caller function and the subjects function on every connection, the last two answering as nobody for an anonymous build, because a translated view calls them whatever the caller holds.

**Three typed stages.** The base builder names the schema and dialer, `signed_in` names the identity, and `durable(data, replica_keys, share_keys)` names the replica location and secret stores. A native provider build reuses its refresh-token store for share keys, while a held-credential build supplies a `ShareKeyStore` beside its `ReplicaKeyStore`. A file whose replica key is missing is refused with `ClientError::ReplicaKeyMissing`.

**Terminals.** `connect_with_pump()` returns the running client beside its pump, natively `connect()` spawns the pump on `tokio`, `connect_driven()` returns the connected unstarted connection for a caller that drives it frame by frame and fails when its one dial fails since nothing retries it, `open_driven()` opens with no transport, and `ConnettoClient::with_pump(conn)` starts one.

**A running client starts offline.** It opens the replica first, so local reads answer at once, then dials once and attaches on success. A failed dial leaves it offline under its reconnect policy, and while it waits the pump still queues local writes durably, refreshes the live queries they touch, drives a pending gate prompt and honours a close. A close queues the last writes before the socket goes, and `ConnettoClient::unsynced()` is what a teardown guard reads. A plain `ws://` URL to a non-loopback host and an untrusted certificate fail the build.

**Native teardown belongs to the client.** A durable `NativeClient` forgets its device with `forget_device(force)`, which reads the unsynced writes, closes the client, swaps its replica for an empty in-memory one so no handle keeps the file open, revokes a provider session and wipes the replica, its tier, content and key. Windows refuses to delete an open file, which is why the swap comes before the wipe, and clones still alive read the empty replica. `session()` reports the provider sign-in and stored accounts, and a switch builds again with `AccountChoice::Account` or `AccountChoice::New`.

**A failed forget says whether the client still works.** `ForgetError::Refused` means nothing was destroyed and the application keeps the client. `ForgetError::Incomplete` means the client is closed and its `ForgetIncomplete` names each part that failed. Every part past the close runs even when an earlier one failed, so as much as possible is removed, except the share keys and the device key, which go only once the wipe stands.

**The browser has two terminals on one value.** In the worker, `.signed_in(Auth::new(origin, provider)).durable(prefix).boot()` resolves the sign-in, opens the OPFS entry `{prefix}-{replica name}`, connects, and starts the hub and the services. In a page, `.attach(TabTopology)` joins the election, waits for the worker, and builds the mirror, whose schema is `SyncSchema::relay_mirror()` so both tiers live in `main` where every relayed patch applies. After each tab's ack the worker sends `TabIdentity` with the caller and the subscription ids the tab will use.

**Trim settings reach OPFS.** The `opfs_trim` suite in [`opfs_trim.rs`](../../examples/wasm-smoke/tests/opfs_trim.rs) proves a trim pass returns a grown-then-deleted replica to its original size.

## Share keys on a running client

`add_share_key(&grant, &subject)` commits a grant-subject pair and `remove_share_key(&subject)` removes every grant for that subject. Both refresh the local policy views and live queries immediately, while the pump reconciles the whole accepted set over the existing connection. Identical additions and absent removals leave the set unchanged, and locked or closed clients refuse changes.

Durable share-key records stay outside the encrypted replica under the secret store's gate and are reserved from account enumeration. Forgetting an identity clears its record.

While connected, a locked gate defers secret persistence and expiry removes local authorization, with approval saving the deferred set, and a refusal preserves the accepted local set and the server's retry floor.

**In the browser the worker holds the set.** A tab's mirror holds no key, so its `add_share_key` and `remove_share_key` send `TabShareKeyChange` with a request id to the worker. The hub makes the change on the worker's own client, states `TabIdentity` to every tab and answers with `TabShareKeyAnswer` quoting the id, so the asking tab's mirror already answers the new set when its call returns. A locked gate answers as `ClientError::Locked`, a refused store write as `ClientError::Auth`, a refused replica write as `ClientError::Session`, and no answer within 30 seconds as `ClientError::Transport`. A durable worker keeps its set in `IdbKeyStore`.

The replica persists its cursor with `cursor_subjects`, `None` meaning unknown and `Some([])` meaning completed empty coverage. Key changes and authorization resends persist unknown before backing rows change, and a handshake or replacement acknowledgment cannot publish known coverage until every declaration fence and row delivery has completed.

## Live queries and the answer model

The client declares a subscription with one method, `subscribe(sub_id, query)`, where `query` is a `SQLite`-dialect `SELECT`. The server reverse-translates it to Postgres with `pg2sqlite` against its own catalog and classifies it through subql, a row projection, a delta aggregate or a `MIN`/`MAX` re-execution, so the client never declares which path it wants and stays catalog-free. The client reacts to self-describing events, row subscriptions as `SnapshotBegin`, `LivePatch` and `SnapshotEnd` applied to the replica and aggregates as `ClientEvent::Aggregate`. A query the server cannot translate or classify is refused at registration as `ClientEvent::NonFatal` with the session intact. Bind values ride as typed `SubscriptionSpec.binds` beside the `?` placeholders, substituted into the parsed statement before reverse translation.

Local-first is the reason queries run against the replica. The replica holds exactly the rows the client is authorized to see, kept current by CDC, so for anything scoped to the client's own view the replica is the source of truth, and executing the query locally returns immediately, works offline, and is correct as of the last sync. The server subscription keeps the replica fresh instead, and the client recomputes. The one class the replica cannot answer is a global, cross-client statistic, which is what [the server-side accumulator](05-aggregate-queries.md#two-answer-paths) serves.

`watch(query)` takes an ordinary typed `diesel` query, runs it against the replica for the immediate, offline-capable answer, renders it to `SQLite` SQL plus bind values with `diesel`'s own query builder, and registers the server subscription. The returned `LiveQuery` caches its rows, refreshes whenever a table the query reads changes, signals each real change through an awaitable `changed()`, and on the last drop starts a grace countdown, five minutes by default and capped at ten, `watch_with_grace` and `watch_fn_with_grace` choosing per watch and `Duration::ZERO` restoring the immediate unsubscribe, so navigating away and back re-claims the subscription. Dropping the last `ConnettoClient` clone closes the connection cleanly. `subscribe(sub_id, query)` remains the string-level primitive beneath `watch`, pinned by the `loop_emu` test `live_query_stays_fresh_and_unsubscribes_on_drop` in [`loop_emu.rs`](../../crates/connetto-client/tests/it/loop_emu.rs).

Each of the four handles serves one shape of query.

| Method | Handle | Serves |
|---|---|---|
| `watch` | `LiveQuery<R>` | a row projection, answered from the replica |
| `watch_value` | `LiveValue<V>` | a scalar aggregate, answered by server `AggregateUpdate` pushes |
| `watch_groups` | `LiveGroups<K, V>` | a plain-column `GROUP BY` projected beside one aggregate, one map entry per group |
| `watch_rows` | `LiveRows<R>` | a row-shaped query the server re-executes |

`watch_value` inverts the answer path, and no local read happens, because the replica holds only this client's authorized subset and would answer a global statistic wrongly, so every value arrives as a push decoded into the app's type. Dropping a `LiveValue` unsubscribes at the next pump step, and an aggregate takes no grace, because an aggregate handle holds no replica rows. The client classifies the rendered SQL's shape to route between the methods, and a misrouted query fails immediately naming the right method.

Every aggregate the server pushes rests in `_connetto_aggregates`, and the DDL in `AGGREGATE_DDL` in [aggregates.rs](../../crates/connetto-client/src/aggregates.rs) keys the row by query identity, the normalized query text plus the binds verbatim in one canonical blob, since run-local wire subscription ids are minted fresh every run. A scalar rests in the row whose `group_key` is the empty blob, a group beside its `group_values_json`. `LiveValue`, `LiveGroups` and `LiveRows` bootstrap from the rested rows, so an offline restart shows the last synced value, and `LiveValue` exposes the value's as-of time, `as_of_secs`.

The primary app-facing verb is postfix `query.live(&client)`, trait `Watchable` with `client.live(query)` as a delegate. It dispatches at compile time from `diesel`'s select and group clauses, a row projection to `LiveQuery`, a scalar aggregate to `LiveValue`, and a grouped aggregate to `LiveGroups`. `LiveRows` stays explicit because server tiering, not the client type, decides whether an ordinary row shape is re-executed. Values and keys decode through `AggregateWire`, so wrong types do not compile. `LiveHandle` unifies every handle, `snapshot()`, `changed()` and `sub_id()`, which is what lets one hook serve row vectors, scalar options and grouped maps alike. A boxed query erases the clauses and stays on the explicit methods.

Pins are the durable form of interest. `pin(name, query)` creates or replaces, `unpin(name)` ends at once, `pins()` lists, idempotent at startup and offline-safe. The coverage model they implement, and what happens across a restart, is [what covers a row](15-replica-retention.md#what-covers-a-row).

## The diesel SQLite hooks

The hooks are merged on `diesel` `main` ([diesel-rs/diesel#5098](https://github.com/diesel-rs/diesel/pull/5098) for `on_update`, [diesel-rs/diesel#5077](https://github.com/diesel-rs/diesel/pull/5077) for `on_commit`), which connetto follows through `[patch.crates-io]` until a release carries them. The connection wires two of them.

- **`on_update`**, with `SqliteUpdateRouter::on_any` over every change op, records the name of each table whose rows change, local writes and applied server patches alike, into the shared changed-tables set that drives re-queries, surfaced as `Reactive::changed_tables` and as the live-query refresh in the pump.
- **`on_commit`** sets the auto-submit dirty flag when a local write commits. The hook only signals, because `SQLite` forbids using the connection inside it, and `flush()` drains the outbox afterwards.

The `diesel` PR also provides per-op callbacks, `on_rollback` and `find_by_rowid`. None is wired, because table-level granularity is sufficient and nothing needs row images at hook time. Server-pushed aggregate values bypass this path, since they arrive as `ClientEvent::Aggregate` pushes and never touch a `SQLite` table.

## Mutation interception and the native driver

The application runs ordinary `diesel` queries directly on `&mut conn`, and execution delegates to the managed connection, whose outbox records the writes for upload. `conn()` remains an escape hatch to the underlying `SqliteConnection`, beside the driver methods `subscribe()`, `push()`, `pump_one()`, `flush()` and `next_event()`.

A write is applied to local `SQLite` immediately, recorded in `_connetto_outbox` inside the same transaction, and queued in `_connetto_pending` for the next push. This is [the write path](03-sync-pipeline.md#write-path), wired through the connection layer.

Implementing `Connection` requires `diesel`'s `i-implement-a-third-party-backend-and-opt-into-breaking-changes` feature, which unseals `ConnectionSealed`. `establish` is stubbed to error, since the connection is built by a client builder, not from a URL, and the query methods never call it.

Mutation interception is automatic. An open sets the dirty flag when the outbox holds writes a previous run never pushed, and the driver's `flush()` drains the outbox and uploads the mutation, so the app never calls `push()`. `next_event()` flushes pending writes and applies one inbound server frame in a single step.

Every application write to a keyed table in `main` or in the shared store is recorded in `_connetto_outbox` by `TEMP` `AFTER` triggers the client generates from the replica's catalog at every open and at every schema change ([outbox.rs](../../crates/connetto-client/src/outbox.rs)). Each row holds the table, the operation, and the old and new values as JSON objects keyed by column name.

| Operation | What the row holds |
|---|---|
| `INSERT` | the whole new row |
| `DELETE` | the whole old row |
| `UPDATE` | the key and the changed cells, compared as stored bytes so a case change under `NOCASE` or a storage-class change counts, or both whole rows when it moves the key |
| a `REAL` cell | `{"r": ...}` in 17 significant digits |
| a blob cell | the marker `{"b": 0}`, its bytes stored raw in `_connetto_outbox_blob` under the row's sequence, column and side, so a photo costs its size once and a caption edit beside it costs none |

The triggers record nothing while the write exemption is raised, so server frames, rollbacks, replays and bookkeeping never enter the outbox, and a rolled-back transaction takes its rows with it. A second switch, raised by `UnrecordedWrites`, keeps out a write the policy guards still judge ([R77](../../plans/master-implementation-plan.md#r77-the-exchange-and-the-provisional-tier)). A shared table's triggers also hold the written row in the store's `_connetto_shared_holding` under a `connetto-unsent` marker, so another identity's pass keeps it from its commit ([the shared store](12-identity-session-capability.md#public-tables-may-be-shared-across-identities)).

`push` reads the outbox in sequence order, decodes each row against the table's current columns, and feeds them to a `sqlite-diff-rs` changeset builder, which merges them per row as `sqlite3changeset_concat` does and lists the tables in the order they were first written, the order the server applies and checks references. The entry is queued and the rows it covers deleted in one transaction, so a crash between commit and push loses nothing. The upload carries the values the application wrote, so a server frame landing on a row before its push changes the screen and not the upload. While connected, a push sends every unsent entry of the bound writer. An export queues the outbox first.

The triggers name every column, so a table must not change under them. An authorizer refuses `CREATE TABLE`, `ALTER TABLE` and `DROP TABLE` on an application table in `main` or in the shared store, which changes only with a release. DDL on `main` goes through `change_schema`, which drops the outbox triggers, runs the DDL, refuses a table without a key, and recreates the triggers in one transaction, and refuses, with `ClientError::UnpushedWrites`, dropping or renaming a table the outbox holds writes to. A column added after a row was recorded reads as its default, and a dropped column is left out.

Rejection rolls back. Each pushed changeset is retained keyed by `client_seq` until the server settles it, since dropping one would lose the write while its rows stay on the replica. On `MutationReject` or `MutationConflict` the connection inverts the changeset with `invert_changeset` and applies the inverse under the write exemption, undoing the optimistic local write. A row a concurrent server patch already changed is left as the server left it. Both events carry the affected rows as `AffectedRow { table, key }`. The client holds one `SQLite` connection for recording and applying alike, the topology `sqlite-wasm-rs` requires on wasm, adopted on native too.

A `MutationReject` carrying `Indeterminate` is not rolled back. The write stays pending, `ClientEvent::MutationDeferred` reports it, and `ConnettoConnection::resend_timer` arms a jittered resend of every pending write. A `RateLimited` naming a pending write arms the same resend at the wait the server named.

## The aggregate answer paths

The delta aggregate family, `COUNT`, `COUNT(col)`, `SUM`, `AVG`, `VAR_POP`, `VAR_SAMP`, `STDDEV_POP` and `STDDEV_SAMP`, seeds once through the connector and then folds from subql's per-event deltas, arriving as `ClientEvent::Aggregate`. The `MIN`/`MAX` re-execution aggregates serve alongside, seeded under the caller's read tier. An aggregate on an RLS-protected table is refused at registration and surfaces as `ClientEvent::NonFatal` with the session intact.

The `GROUP BY` variants fold per group server-side and arrive as keyed upserts, `group_key` set and `is_full_result` cleared, with an emptied group as a keyed removal. They rest in `_connetto_aggregates` and ride the keyed handle described under [the answer model](#live-queries-and-the-answer-model).

Once a `MutationConflict` rolls the local write back, the server's authoritative row arrives as a normal `LivePatch` and applies under the same write exemption with the server-wins resolver, so the client converges without the conflict message carrying any server state.

## Starting before the server exists

A connection is constructed without a transport and given one later. `open_driven` takes no transport, `attach` supplies one, and the four values a handshake produces are split in two, a `Run` (session handle, resume token, schema version) that outlives a dropped socket and a `Wire` (transport, connection id) that does not. That split lets the next attach resume rather than restart. Anything that genuinely needs a server answers `ClientError::NotConnected`, while local reads and writes are unaffected and writes queue for the first connection.

**Connection state is one event on the stream the application already reads**, `ClientEvent::SyncStatus`, carrying `Connected` or `Offline`, because a value handed back once cannot report a server arriving minutes later. It defaults to `Offline`, because claiming a server is reachable before one has answered is the single answer that is certainly wrong. The relay forwards it to every tab that has completed its own handshake and states the current value to each new tab right after its ack.

**An empty result says which kind of empty it is.** `LiveQuery::never_synced()` is true only while the query reads synced tables and no server has ever answered. It is always false for a query over device-private tables alone. It is driven from the replica's cursor rather than the live query refresh, because a first sync that delivers no rows refreshes nothing.

**A subscription can be declared with no server.** It is recorded in the replica and sent on the first connection, so `watch` works while the transport is down. A pump with no socket parks rather than failing, the same as an idle socket.

**A fault that stops the pump is announced.** A disconnect is the reconnect driver's to recover, while a local fault ends the pump with or without a driver. That end queues the last writes, closes the transport, and emits `ClientEvent::Stopped` followed by `ClientEvent::Closed`, so every wait keyed on `Closed` settles. Every live handle's `changed()` then returns `ClientError::Stopped`, while its last rows stay readable and `with_conn` keeps serving the replica.

## Framework adapters

Both crates wrap `ConnettoClient`'s live-query API as UI framework hooks. `use_live` takes a `Watchable` query and yields `UseLive<Vec<R>>` for a row projection or `UseLive<Option<V>>` for a scalar aggregate, chosen at compile time through `LiveHandle`. `use_live_fn` serves boxed or dynamically built queries, which are not `Clone`, taking a builder closure for the same lifecycle. Both hooks capture their arguments on first render, and re-rendering with a different query has no effect, remount to change it.

Dropping the `LiveHandle` releases the subscription, a row watch starting its grace countdown and an aggregate unsubscribing at the next pump step, and the hook ties the handle's lifetime to the component's. That wiring differs between the two crates, and the difference is why two crates exist.

**`connetto-dioxus`** in [lib.rs](../../crates/connetto-dioxus/src/lib.rs) owns the handle in a component-scoped Dioxus task, and Dioxus cancels scope-bound tasks on unmount, so the drop is implicit. `use_away_input` in [away.rs](../../crates/connetto-dioxus/src/away.rs) feeds the client's one away input from the platform's lifecycle, tao's `Suspended` and `Resumed` on iOS and Android and the window's focus on the desktop. `use_hello_owner` in [hello.rs](../../crates/connetto-dioxus/src/hello.rs) anchors the Windows Hello prompt over the application's desktop window, which Windows requires. The feeders sit behind the crate's `desktop` and `hello` features, the last one Windows-only.

**`connetto-yew`** in [lib.rs](../../crates/connetto-yew/src/lib.rs) wraps the driver future in `Abortable` and returns an effect cleanup that calls `abort()`, because Yew's `spawn_local` is detached and unmounting the component does not cancel it.

## The device-private tier

An author declares the tables the device alone sees by document membership. A build carries two Postgres-dialect source documents, one per tier, the demo's being [schema.sql](../../examples/deployment/schema.sql) for the synced tables and [frontend.sql](../../examples/demo-support/frontend.sql) for the local tier. The local document stays Postgres dialect because its type system carries what the translation needs. The server translates the shared document alone, so a tier table never reaches the server.

The local tier lands in a second attached `SQLite` file beside the replica, sharing its key ([ordering constraints](14-at-rest-encryption.md#ordering-constraints)), and the attach name is an internal constant, never in authored SQL. The placement is load-bearing, because the capture session the client runs is fixed to the replica's main database and the outbox triggers exist only in `main` and the shared store, so writes to the attached tier are physically incapable of being captured. A `REFERENCES` crossing the boundary and a duplicate table name across the documents are generation-time hard errors, because an enforced foreign key from private data into a moving window would block eviction or cascade-destroy private data.

A synced table the deployment keeps write-forbidden is refused on the server by its catalog, the `NotWritable` in [materializer.rs](../../crates/connetto-server/src/materializer.rs), and on the replica by the `RAISE(ABORT)` denial the translation emits for each write command the role's `GRANT`s do not admit. A local write to such a table is uploaded like any other synced write and rejected on arrival.

Live queries dispatch on the tier the replica already knows. A query over local tables alone registers no server subscription, since the local table is complete. A local aggregate re-executes locally on change. A mixed-tier row query subscribes the whole synced table it reads, tied to the handle's lifetime, and a mixed-tier aggregate is a hard error at registration. The tier carries no sync state, so retention and eviction have no path to a tier row. The eviction story is [what covers a row](15-replica-retention.md#what-covers-a-row).

## SQLite hardening

Every replica connection is hardened in one place, [`connetto_client::harden::harden_replica_connection`] in [harden.rs](../../crates/connetto-client/src/harden.rs), called from `open_inner` right after the cipher unlock and the journal mode. Native and browser reach it through the same open, so the two agree on every value by construction. It sets defensive mode on, trusted schema off, three of `SQLite`'s recommended limits (at most 8 function arguments, at most 25,000 instructions per statement, no sorter threads), and the attach lockdown below. The full table with a reason per value is in the plan's [R18](https://github.com/connetto-rs/connetto-rs/blob/82983dff19effa9cfb521370a7794d29e51afb51/plans/master-implementation-plan.md#r18-the-sqlite-hardening-surface).

**The rest of the limit table is deliberately not applied.** Those numbers are written for a process that runs nothing but untrusted SQL, and this connection also runs the application's own `diesel` queries. A four-row batch insert, an eleven-key `IN` lookup, a 60-character `LIKE` pattern and a 1.5 MB value are all refused against the full set, and the trigger depth stays at `SQLite`'s stock 1000, since a twelve-level foreign-key cascade in the tier counts every level against it.

**The attach posture is default-closed with audited windows.** At rest the connection can attach nothing, creation off, write off, and the attached-database ceiling equal to what is already attached, read back from `PRAGMA database_list`. Each connetto-owned attach calls `harden::attach_in_window`, which opens exactly the permits that site needs, attaches, and seals the connection again. The sites are the local tier, the relay's hub meta database, named from the replica's own name so the account's key opens it, and the shared store.

**What this does not promise.** It is a configuration pass, not enforcement, because anything holding the connection can raise the same knobs it lowered. What the lockdown buys is that an attach outside the audited sites fails, so a new attach has to be written deliberately.

Trusted schema off means a function the application registers through `SqlFunctions` and a schema object calls, a column `DEFAULT`, a view, a trigger or a generated column, must carry `SqliteFunctionBehavior::INNOCUOUS`, or SQLite refuses the call as unsafe later than registration. connetto's own caller function already carries the flag.

## References

**Synced tables carry no references on the device.** A replica is a partial copy, and row-level security, paging and eviction all leave a child without its parent, so `SQLite` enforcing the server's `REFERENCES` there could only refuse what the server already accepted. [`connetto_schema::translate`] drops every column `REFERENCES`, table `FOREIGN KEY` and `ALTER TABLE ... ADD ... FOREIGN KEY` clause from the document it hands pg2sqlite, and the bundle keeps the source as written for the server, `without_references` in [lib.rs](../../crates/connetto-schema/src/lib.rs). The removal runs before translation, because pg2sqlite's placement of shared tables refuses a reference between a shared and an unshared table. The server stays the one enforcer of synced references.

**The tier keeps its references, and SQLite enforces them.** `PRAGMA foreign_keys = ON` runs at every open on both backends ([ordering constraints](14-at-rest-encryption.md#ordering-constraints)), so all five actions apply, a cascade runs at any depth, and a `DEFERRABLE INITIALLY DEFERRED` reference is checked at its transaction's commit. A tier reference never reaches a synced table, since `SQLite` references do not cross attached schemas and pg2sqlite refuses a cross-document reference.

**The archive import checks at its commit.** `apply_import_with_bookkeeping` sets `PRAGMA defer_foreign_keys = ON` inside its transaction, because an archive lists tables in creation order and follows no reference order inside a table, so a child created before its parent would otherwise refuse a whole valid import. An orphan at the commit aborts the import.

**In the browser, the worker is the tier's one enforcer.** A tab's mirror carries no references and holds only its subscribed rows. The relay hub replays a tab's tier changeset on the worker connection with checks deferred to the commit, records what the worker actually changed through a session on `connetto_local`, cascades included, and fans it out to every tab reading a touched table. A violation at the commit rejects the mutation back to its author.

**A replica created before the references were stripped keeps them.** Stripping acts only when a table is created, and connetto has no migration for such files. The fix is to delete them and resync.

## A recorded table needs a declared primary key

**The requirement is connetto's rather than SQL's.** Two mechanisms record changes by key and record nothing for a table whose only key is the implicit `rowid`. The outbox triggers, created only on keyed tables, are what upload a synced write, and the relay serves a tab its copy of a table as a session diff, so an unkeyed table is never uploaded and never read by a sibling tab, and nothing notices either loss. A `prefs (name, value)` table is keyed by SQLite's own rules, so connetto refuses rather than warns and every refusal names the fix. The check runs at open over the live schema of the replica and the tier (`check_recorded_tables` in `open_inner` in [lib.rs](../../crates/connetto-client/src/lib.rs)), again in `change_schema`, and before a write travels when `PRAGMA schema_version` moved. `UNIQUE(a, b)` with no primary key is refused too, and so is a key of more than 255 columns, since a change record numbers each key column in one byte.

**An application may declare the tables whose missing key is intentional** through `SyncSchema::with_unrecorded_tables`. A settings singleton, a rebuilt cache and a log ordered by `rowid` have no natural key. The list may name only tables with no declared key, and a listed table that declares one is refused, because it would sync while the application believed it did not. For an unkeyed table "ignored by everything that leaves the device" is already true by the database's own behaviour, so the list has exactly two consumers, the key check and the export's row reader.

**One consequence differs by target.** An unrecorded table stays locally usable on native, since the changed-tables signal never consulted keys, so a live query over it still refreshes. In a browser it does not, because a tab reads through subscriptions the relay serves as a session diff, `snapshot_patchset` in [relay.rs](../../crates/connetto-web/src/relay.rs), so a tab sees an unrecorded table as empty.

**The server needs nothing.** A Postgres table the publication carries is already refused at startup without `REPLICA IDENTITY FULL`, which records nothing for a table with no primary key.

## Schema majors and the update path

**The major is declared once, in the schema source.** A line `-- connetto:major N` in the Postgres schema source is read by `SchemaBundle::new` into `SchemaBundle::major()`, majors start at 1, and a source without the line fails. The line sits inside the bundle hash. The client sends its major in the `Handshake` and the server refuses another with `FatalErrorReason::MajorMismatch { server, client }`. The client reads an older own major as `ClientError::UpdateRequired` and a newer one as `ClientError::ServerBehind`. The bundle-hash check catches the same major built from another schema.

**The replica records the build it was made by.** Its major is `main`'s `PRAGMA user_version`, its bundle hash is the one-row `_connetto_build` table, and `_connetto_build_renames` keeps the physical names a split table was given, because an old major's queued write must leave in the wire form its own major spoke ([majors.rs](../../crates/connetto-client/src/majors.rs)). At open, the open is refused or rebuilt against what is recorded.

| Recorded against the build | Open |
| --- | --- |
| Same major, same hash | Keeps its synced rows. |
| Same major, other hash | Refused with `ClientError::ReplicaSchemaChanged`, naming both hashes. |
| Newer major | Refused with `ClientError::ReplicaNewer`. |
| `user_version` 0 and no `_connetto_build` row | Retains synced rows, queued writes and writer frontiers, then records the build. |
| A major but no `_connetto_build` row | Refused with `ClientError::ReplicaRecordMissing`. |
| Older major | Rebuilt in place. |

**The rebuild keeps the file and the writer queues.** One transaction rewrites each queued write the old major captured into the wire form that major's server reads, records the old major and hash in `_connetto_held_batch`, drops the synced tables, views and triggers with the cursor, subscriptions and aggregates, applies the build's DDL and stamps the new record. `_connetto_writers`, `_connetto_pending`, `_connetto_retire` and `_connetto_retired_keys` stay, since the server keeps each writer's frontier across majors. A write made after the rebuild chains after the old ones in the same writer.

**An older major's writes drain under their own writers, inside the same connection.** The browser storage pool allows one handle per file, so the drain is state on `ConnettoConnection` ([drain.rs](../../crates/connetto-client/src/drain.rs)), and the oldest held major drains first, one writer at a time, settling what the acknowledged frontier covers before sending the rest. [`connetto_client::PendingWork`] lists the unsynced writes and the files held for an older major, and its `is_empty` is the gate an application checks before it offers an in-app update, through `ConnettoClient::pending_work` or, in a browser tab, `request_unsynced`. The gate cannot hold back a store update, so the drain runs after the update as well.

The drain wire's protocol detail, the device-wide shared cache's own build record, the tier's migration across its boundaries and the archive's major ride in the plan's [R31](https://github.com/connetto-rs/connetto-rs/blob/82983dff19effa9cfb521370a7794d29e51afb51/plans/master-implementation-plan.md#r31-application-schema-majors-and-the-update-path) and in the chapters that own those seams.

## The bot runtime and template

Planned in [R103](../../plans/master-implementation-plan.md#r103-the-bot-runtime-and-template), [the bot](12-identity-session-capability.md#bots-lists-and-installations) runs on the same client as a screen, and its runtime and the template ship in a `connetto-bot` crate and an example UI apps never compile.
