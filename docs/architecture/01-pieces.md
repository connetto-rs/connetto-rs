# 01: Pieces Inventory

A catalogue of every component the system has, by area, so nothing that must exist goes unowned. The wire types it names are in [02](02-protocol.md), the delivery and reliability side in [10](10-subscription-materializer.md), the client connection and the writer model in [13](13-client-connection.md), the browser platform in [09](09-wasm.md), and the file stack in [18](18-file-handling.md).

## A. Foundation

| Piece | Description |
|---|---|
| `ControlMessage` and `BulkMessage` | The two wire enums, one per plane. Control carries the typed frames, bulk carries the Zstd-compressed payloads. [`connetto_core::messages::ControlMessage`] and [`connetto_core::messages::BulkMessage`], shared by both sides |
| `MutationHeader` and `MutationPatch` | A client-originated write, split across the two planes. The control frame carries the writer, the sequence and the provenance, the bulk frame the compressed patchset |
| `ChangeRecord` | The server-side record of one committed change. The table, the primary-key bytes, the row event, and the patch built once for the consumers, in [`connetto_server::oplog::ChangeRecord`] |
| `SubscriptionSpec` | How a client declares what it wants. The predicate, its parameters, the subscription id, and a delivery priority |
| `SchemaVersion` | The hash of the translated schema bundle. The server advertises it in the handshake and the client compares it with the one its build baked |
| Core traits | [`connetto_core::traits::Transport`] and [`connetto_core::traits::Store`] (the local SQLite), the seams the rest of the system plugs into, beside the credential and key storage traits in the same module. Authorization is not one of them. Visibility goes through `subql`'s seam |

## B. Transport

| Piece | Description |
|---|---|
| Server session | One struct per connected client. The sink, the client id, the subscription set, the flow-control budget, and the idle limit |
| Client connector | Opens the connection, sends the handshake, drives the read loop, and exposes a typed message channel to higher layers |
| Reconnect loop | Client-side. Exponential back-off, and re-sends the subscriptions and the pending writes after reconnect. Reliability is in [10](10-subscription-materializer.md) |
| Flow control | A per-session send-window. The server back-pressures CDC delivery when the window is exhausted |
| Keepalive | The client sends `Ping` after `probe_after` of inbound silence and drops the transport and recovers after a further `answer_within` of silence, the same bound covering a reconnect's handshake. The server closes a session with no inbound frame for `idle_limit`. The defaults are 15 s, 10 s and 60 s, and the bounds live in [`connetto_client::builder::SyncTuning`] and [`connetto_server::session::SessionConfig`]. The native pump and the browser hub run the one rule on `ConnettoConnection`, also while the away gate is locked |

## C. Subscription Lifecycle

| Piece | Description |
|---|---|
| Subscribe message | The client sends the subscription id and a `SubscriptionSpec` |
| Server subscription registry | Maps the (session id, subscription id) pair to the spec, the auth context, and the last delivered position |
| Initial snapshot sender | On a new subscription, runs the query, applies the auth filter, and streams the result rows to the client |
| Unsubscribe message | The client cancels a subscription by id, and the server removes it from the registry |
| Re-subscribe on reconnect | The client re-declares all subscriptions after reconnect, and the server reconciles with the stored state if available |

## D. Mutation Path (client to server)

| Piece | Description |
|---|---|
| Local mutation queue | The pending writes, persisted in the local replica before sending, so they survive a process restart and offline. The queue's shape is in [03](03-sync-pipeline.md#2-local-mutation-queue), and the writer it keys on in [13](13-client-connection.md) |
| Mutation sender | Reads the head of the queue, sends it, and awaits the outcome before dequeuing. One sequence in flight |
| Server mutation handler | Validates and applies the patchset under the caller's Postgres role, and answers `MutationApplied`, `MutationReject` or `MutationConflict`. Owned by the materializer in [10](10-subscription-materializer.md), with transient failures retried per its policy |
| Conflict detector | Compares the op's recorded `updated_at` to the server's row. A stale or missing row rolls the transaction back and answers `MutationConflict` |
| Conflict resolution | One policy. The client reverts the optimistic write and the application layers its own merge on top. [03](03-sync-pipeline.md#5-conflict-handling) states it |
| Optimistic rollback | On `MutationReject` or `MutationConflict` the client inverts the retained changeset under the write exemption |

## E. CDC Push Path (server to client)

| Piece | Description |
|---|---|
| CDC source | PostgreSQL's logical replication stream, held by `subql`'s `CdcSource`, which produces the typed events. The materializer in [10](10-subscription-materializer.md) drives the consume loop and owns the reconnect with backoff |
| Subscription matcher | For each incoming event, identifies which active subscriptions are potentially affected. Matching is in-process via `subql` (bitmap prune plus predicate VM) and has no retry surface. Its surrounding CDC ingestion and re-execution reach the database through connections the materializer supplies |
| Auth filter | Per-event, through [`connetto_server::openfga::FgaAuth`]. `RowPolicy` answers locally from the row's own values wherever the schema decides, and `OpenFgaPolicy` batches one question per watcher for the rest. Rows that fail are dropped or replaced with a delete, and a transport outage pauses delivery with a typed signal rather than dropping silently |
| Delta packager | Groups the affected rows into a patchset and adds the cursor for the client's resume position. `subql` folds the matched events into `sqlite-diff-rs` patchsets, and the materializer selects each session's authorized subset and invokes the builder |
| Delivery queue | Per-session outbound queue. Respects the flow-control window, and drops or back-pressures when the client is slow. Reliability is in [10](10-subscription-materializer.md) |

## F. Row-level SELECT Subscriptions

| Piece | Description |
|---|---|
| Snapshot query executor | Runs the initial SELECT and pages the results to the client |
| Row identity tracker | Tracks which primary keys have been delivered for a subscription, so incremental adds and removes can be computed |
| Incremental update computation | On a CDC event, determines whether the row enters, leaves, or changes within the subscription's result set |

## G. Aggregate Subscriptions

| Piece | Description |
|---|---|
| Accumulator state | Per-subscription aggregate state (COUNT, SUM, and the variance family), held in memory by `subql` and rebuilt via re-execution on restart |
| Incremental update handler | On a CDC event, `subql` updates the accumulator and emits a signed delta, and the materializer pushes it to the client when the result changes |
| Full re-execution fallback | For unsupported shapes or an invalid accumulator, `subql`'s re-exec engine classifies the query and either triggers a re-execution or auto-resolves it through a caller `Connector`. The materializer runs the query against Postgres over its own pool, coalesces duplicates, and installs the value back into `subql` |

The full discussion is in [05](05-aggregate-queries.md).

## H. Reconnect / Offline

| Piece | Description |
|---|---|
| Client cursor | The client persists its last-known server position, stamped with the database's timeline, and sends it in the reconnect handshake |
| Server oplog | A ring of recent changes keyed by commit position, with a configurable retention window |
| Catchup replayer | On reconnect, if the client's position is within the window, replays the changes since it, filtered by the subscriptions |
| Full re-sync trigger | If the position is outside the window, past where its timeline ended, or unreadable, triggers full snapshot re-delivery for each subscription |
| Tombstone store | Deleted rows are retained in the oplog with a tombstone flag, so deletes can be replayed during catchup |

## I. Schema Distribution

| Piece | Description |
|---|---|
| Schema translation | The build step turns the deployment's Postgres sources into the replica DDL and the policy views, filtered to the tables the client can access, via [`connetto_schema::translate`] and [`connetto_schema::emit`] |
| Schema version envelope | A content hash over the whole bundle, one value, in [`connetto_core::schema::SchemaVersion`] |
| Schema version handshake | The server advertises the version in `HandshakeAck`, and the client never fetches or applies a schema at runtime |
| Stale-build detection | The client compares the advertised version with the one it was baked with. A mismatch means this app build is stale and must reload, since the schema is compiled in and the client runs no DDL. Reload boots a fresh baked template and full-resyncs |

## J. File Sync

| Piece | Description |
|---|---|
| File metadata table | A normal synced table. The file's content identity in a column the application declares, synced through the standard row-sync path |
| Content channel | Separate from the row-sync channel. Plain HTTP under a content ticket the control plane grants |
| Chunker | Content-defined chunking (FastCDC per mime class), each chunk compressed then encrypted, addressed by its plaintext hash, the file identified by BLAKE3 of its bytes |
| Upload path | Client to server. The intent naming the manifest, the needed hashes back, the chunk `PUT`s, and the commit |
| Download path | Server to client. A signed URL by default, local bytes when unsent or pinned |
| Resumability | The intent reply names the chunks the server lacks, so an interrupted upload resends only those. A transfer is bounded by silence, never by elapsed time |
| WASM constraints | No direct filesystem. The worker owns an encrypted OPFS chunk store, with worker-lifetime memory as the fallback. [18](18-file-handling.md) is the normative chapter |

## K. Authorization

| Piece | Description |
|---|---|
| Policy source | The PostgreSQL row-level-security definitions, compiled into a fast in-process policy engine |
| Auth context | Per-session identity and claims, passed to every policy evaluation |
| Read filter | Applied to every row before delivery, at snapshot time and on the CDC push, and fails closed. On the snapshot path it is Postgres RLS inline in the query. On the change path it is `FgaAuth`, with `RowPolicy` answering locally from the row and `OpenFgaPolicy` batching to the server. The verdict buffer arrives pre-filled with denials, so a backend that cannot answer denies rather than allows |
| Write gate | Applied to every mutation before it is executed. Reliability is in [10](10-subscription-materializer.md) |
| Auth batching | Policies are evaluated in batch per CDC event, to avoid per-row round-trips |
| File session token | A short-lived token issued for a specific file. Gates chunk upload and download without per-chunk auth calls |
| Bot registry | Five synced tables under row-level security (`connetto_bots`, `connetto_registration_codes`, `connetto_bot_lists`, `connetto_installations`, `connetto_installation_optional_tables`), the four `AS RESTRICTIVE` cap policies per synced and registry table that hold a bot to its declared tables and verbs, and registration that spends one code as the first frame of a fresh connection, signed over the connection's nonce. The entities are in [12](12-identity-session-capability.md) |

Planned in [R99](../../plans/master-implementation-plan.md#r99-bot-login-and-lifetime), the bot login over the challenge the server opens every connection with. The login is in [11](11-authentication.md), the metering keys in [16](16-server-capacity.md).

## L. WASM / Browser

| Piece | Description |
|---|---|
| Worker isolation | All sync logic runs in a dedicated `Worker` spawned by the Web Locks election winner, and the main thread is never blocked. OPFS sync access handles exist only inside a dedicated worker, so a `SharedWorker` was never an option |
| Browser storage backend | Local SQLite runs in OPFS (Origin Private File System) or an IndexedDB fallback |
| Transport adapter | WebSocket in the browser context, the same `Transport` trait as native |
| File storage adapter | Content chunks stored in OPFS or the browser cache, with no direct filesystem access |
| Common interface boundary | Native and WASM clients expose the same public API, so application code is portable. Android is supported both in the browser and as a native app, and the topology is in [09](09-wasm.md) |

## Crates

One Cargo workspace holds the crates, and each publishes independently so a downstream project depends only on the pieces it needs. connetto's own dependencies are followed on git, and the pins are recorded in the manifest. Every piece above is in scope for the library, and file sync is its own stack of three crates that depend on connetto and never the reverse.

| Crate | Role |
|---|---|
| `connetto-core` | The wire types, the framing and codec, and the traits every side agrees on. Both `connetto-server` and `connetto-client` depend on it, and neither depends on the other |
| `connetto-schema` | The build step that translates a deployment's Postgres sources into the bundle the client and server share |
| `connetto-server` | The server library and binary. Sessions, CDC ingest, the materializer, the auth stack, the mutation handler, and the server builder |
| `connetto-client` | The native client. The Diesel connection, its builder, the background sync, the live queries, and the archives |
| `connetto-dioxus` | The Dioxus hooks that bind live queries to component scope |
| `connetto-yew` | The same hooks for Yew, with the abort-on-unmount lifecycle suited to Yew's task model |
| `connetto-test-harness` | The in-process CDC-loop test harness over a real Postgres, and the browser stack runner. Test infrastructure, not a shipped component |
| `connetto-file-core` | The chunking, the identity, the per-chunk encryption, and the `ChunkStore` and `ChunkInventory` seams. Builds for wasm |
| `connetto-file-server` | The chunk storage on a filesystem or an object store, the two-phase upload, the ranged serving, the sweep, and the content-ticket minting |
| `connetto-file-client` | The encrypted store, the manifests and outbox in the replica, the upload walk, the resolver, and the content pins, natively and in the browser |
| `connetto-auth-session` | The platform browser tab a phone signs in through, returning to the app by redirect |
| `connetto-ca` | The device certificate authority's offline ceremonies |
| `connetto-app-attest` | Apple App Attest behind device enrolment |
| `connetto-peer` | The mutual TLS link between certified devices |
| `connetto-peer-android` | The peer link's Android glue, permissions, hotspots, Bluetooth and discovery |
| `connetto-peer-apple` | The peer link's Apple glue |
| `connetto-architecture` | The architecture chapters as rustdoc, so the docs build checks every symbol a chapter cites |
| `examples/deployment` | The deployment the browser demos share, and the build-time translation behind them |
| `examples/demo-support` | The schema rows, wording and browser pieces the browser demos share |

Planned in [R103](../../plans/master-implementation-plan.md#r103-the-bot-runtime-and-template), the bot runtime and template, and in [R104](../../plans/master-implementation-plan.md#r104-the-demos-for-apps-and-bots) the two demo bots built from it.

The separate workspaces build their own lockfiles:

| Workspace | What it holds |
|---|---|
| `crates/connetto-web` | The browser platform on wasm32. The DB worker, the relay, the storage and the custody |
| `examples/dioxus-demo` | The Dioxus demo for the web, desktop, Android and iOS |
| `examples/leptos-demo` | The Leptos demo for the browser, and the Tauri app's UI |
| `examples/yew-web-demo` | The Yew demo for the browser |
| `examples/wasm-smoke` | The browser smoke suite |
| `examples/webauthn-unlock` | The passkey unlock proof |
