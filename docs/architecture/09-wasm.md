# 09: WASM / Browser

This chapter covers what it takes to run the sync client in a browser. It states the platform constraints, the worker topology the database runs in, the hub the worker runs and how it schedules the tabs' requests, the storage the replica lives in, the transport, the seam the native and browser clients share, and the browser floors the build supports. The connection a tab holds and how the tabs and the worker speak is [the client connection layer](13-client-connection.md), the key handoff between a tab and the worker is [key custody](14-at-rest-encryption.md#key-custody), the worker's custody of the unlock is [authentication's browser and worker topology](11-authentication.md#browser-and-worker-topology), and the file content beside the replica is [client storage](18-file-handling.md#client-storage).

## Core constraints

| Constraint | Native | Browser |
|---|---|---|
| Threading | OS threads | One dedicated `Worker` per origin, the Web Locks election winner, with no shared memory beside the main thread |
| Filesystem | Direct, `std::fs` | OPFS through sync access handles, or the in-memory VFS where OPFS is refused |
| WebSocket | OS socket through `tokio-tungstenite` | The browser `WebSocket` through `web-sys` |
| SQLite | File-backed libsqlite3 | `sqlite-wasm-rs` over the same VFS |
| Blocking I/O | Available | Async only |
| Memory | OS-managed | The WASM linear memory, grown on demand |

All sync logic runs in a background `Worker`, never on the main thread, and the worker is the owner of every sync state. Whichever tab wins a Web Locks election spawns it through `spawn_db_worker` in [workers.rs](../../crates/connetto-web/src/workers.rs), and the tabs and the worker speak over `BroadcastChannel` and `MessagePort` through the `MessageTransport` framing the server wire uses.

Two interfaces pin the topology, one per direction. `createSyncAccessHandle` is `[Exposed=DedicatedWorker]` in the WHATWG File System IDL, so the database cannot live anywhere but a dedicated `Worker`, and a `SharedWorker` cannot host it in any conforming browser. Chrome does not even expose the `Worker` constructor in a `SharedWorker`, so a nested dedicated worker is no route either. `PublicKeyCredential` is `[SecureContext, Exposed = Window]`, so the unlock that produces the replica's key runs in a tab, and the key crosses from tab to worker as a non-extractable `CryptoKey` rather than as bytes. [Key custody](14-at-rest-encryption.md#key-custody) specifies the handoff and its residual exposure, and [authentication](11-authentication.md#browser-and-worker-topology) states the worker-custody invariant the handoff qualifies. Tokens travel in neither direction.

There is no `ServiceWorker` half of the topology. The worker lives as long as the tab that spawned it is open, leadership moves to another tab when that tab closes, and a reopened client reconnects and receives the pending PatchSet through [the reconnect path](06-reconnect.md#pending-mutations-on-reconnect). Authorization deadlines read `performance.now()` from the current global scope, in workers and windows alike.

## Communication with the tabs

A tab holds no connection to the replica. It runs its own [`connetto_client::ConnettoConnection`] over the wire, and the tab-to-worker leg carries the full protocol the server speaks, `ControlMessage` and `BulkMessage` frames over a `MessageTransport` in [frames.rs](../../crates/connetto-web/src/frames.rs). The worker side is the `RelayHub` in [relay.rs](../../crates/connetto-web/src/relay.rs), and `handle_tab_control` dispatches a tab's `Handshake`, `Subscribe` and `Unsubscribe`, `MutationHeader` and its patchset, `AckCredits`, `Ping`, `TabShareKeyChange` and `TabStorageUse`. The hub answers a handshake with the ack, the tab's identity, the gate state and the current sync status right after, so a tab attaching during an outage is not left showing stale rows in silence.

[`connetto_core::messages::SyncStatus`] carries `Connected` or `Offline`. A reconnect attempt in progress is indistinguishable to an application from being offline, and both mean the same thing, that what is being read may be stale. The status rides `ControlMessage::SyncStatus`, so the hub forwards it to the tabs on the same channel as every other control frame. It defaults to `Offline`, because claiming a server is reachable before one has answered is the one answer that is certainly wrong.

## The worker hub

The hub's core is the `HubEvent` select in [relay.rs](../../crates/connetto-web/src/relay.rs), one select that arms every timer, the keepalive and the share-key deadlines among them, beside the gate prompt. The recovery loop serves the requests it would have refused while `main` owns the connection, and the table is the list of situations it handles. A cell says whether the request is served where it arrives, held until the upstream is back, or refused because the hub has ended. Tab work that the replica or the hub can answer stays live offline, so a tab can boot, subscribe, mutate and ask content questions before the worker reaches the server. The internal-lane content frames the replica answers, `Resolve`, `Pin`, `Unpin`, `ListPins`, `RefusedContent`, `RetryRefused`, `RetiredContent`, `ForgetRetiredContent` and `Tidy`, are served at idle and interrupt an attach or replay like the other replica-answered requests. The `Gate` event is the hub's own state, recorded and forwarded the way the hub's own sync status is.

| Request | Connected | Sleeping before retry | Connecting | Attaching | Replaying subscriptions | Closed |
|---|---|---|---|---|---|---|
| `Attached` | Served | Served | Served | Served if nothing is queued, else held | Served if nothing is queued, else held | Refused |
| `Frame` | Served | Served | Served | Interrupts on `TabShareKeyChange` and `TabStorageUse`, else held | Interrupts on `TabShareKeyChange` and `TabStorageUse`, else held | Refused |
| `Gone` | Served | Served | Served | Held | Held | Refused |
| `Kill` | Served | Served | Served | Held | Held | Refused |
| `Unsynced` | Served | Served | Served | Interrupts | Interrupts | Refused |
| `LeaveShared` | Served | Served | Served | Interrupts | Interrupts | Refused |
| `Export` | Served | Served | Served | Interrupts | Interrupts | Refused |
| `Import` | Served | Served | Served | Interrupts | Interrupts | Refused |
| `ForgetRetired` | Served | Served | Served | Interrupts | Interrupts | Refused |
| `RefusedContent` | Served | Served | Served | Interrupts | Interrupts | Refused |
| `RetryRefused` | Served | Served | Served | Interrupts | Interrupts | Refused |
| `Internal` | Served | Served | Served | Interrupts on the replica-answered content frames, else held | Interrupts on the replica-answered content frames, else held | Refused |

An offline stage is a tab completing its full protocol, handshake, subscribe and mutation, against the relay while the upstream is absent. Every `Frame` the relay receives during idle recovery can be answered from local state. A handshake reads the tab's watermark from the replica, a subscribe reads the snapshot from the replica, and a mutation commits to the replica and to `_connetto_pending`, then replays exactly once when the upstream first attaches, under the exactly-once watermark. This is the same contract as the native client's offline write path, and serving frames during idle recovery makes the browser relay a true local peer rather than a proxy that requires a reachable server. The `with_connect_gate` default of `None` does not change any other row of the table.

A frame is held while the attach or the subscription replay owns the connection unless it is one the replica answers, because those phases replay the full upstream state and a concurrent frame would race their ordering. A departure and a kill unsubscribe through the connection the attach owns and nothing waits on them, so they keep their place in the queue. Every request answered from the replica interrupts an attach rather than wait for it, and the interrupted attach resumes on the installed transport afterwards.

One order rule covers every served cell. A request that would be served where it arrives is queued instead while any older request is already queued, and the queue drains in arrival order once the upstream is back, so a kill can never overtake a frame of the tab it kills. Refused means the intake has closed and the hub has ended, so every waiting reply sender drops and its caller reads the hub as gone.

| Background job | Runs while | Held while | One turn is | Interrupted by |
|---|---|---|---|---|
| Outbox upload | Connected | Sleeping, connecting, attaching, replaying | One sendable entry, the one after the last attempt | A served request across its transfer, and a ticket wait that yields |
| Content ticket retry | Connected | Sleeping, connecting, attaching, replaying | One ticket request for the deferred entry | Queued local work, which takes the turn |
| Integrity walk | Connected, sleeping, connecting | Attaching, replaying | Eight chunks of the head file | Every other source, because the walk is one branch of the same wait |
| Orphan sweep | Connected, sleeping, connecting | Attaching, replaying | One listing, then eight deletions | Every other source, and a failure waits for the retry timer |

The upload and the ticket retry need a ticket the server mints, so they cannot run while the upstream is down and the table states that rather than leaving it to be rediscovered. The walk and the sweep read only the replica and the chunk store, so they run through the retry sleep and the connect, which is when a device is most likely to sit for a long time. They stop while the attach and the subscription replay own the connection, because the walk reads the replica through it.

**What a commit invalidates.** A resumable job carries a decision taken over state a later commit can change, so each commit says what it invalidates rather than each job guessing.

An import commit invalidates four things. The scan cursor, because the import can restore the chunk an earlier slice found missing, so the current file is scanned again. The orphan list, because a listed chunk can be exactly what the import wrote, so the list is taken again. The sweep schedule, which is owed a turn again because a failed import leaves chunks nothing names. And the outbox wake, because the restored writes and files are queued. A scan cursor is bound to the file and to a fingerprint of its ordered chunk list, so a manifest replaced under the same identity starts again even without this invalidation, and a retirement is decided by reading the missing chunk again at the end rather than by what a slice saw.

An upload success schedules two things. The retention pass, because an uploaded file is no longer unsent and nothing else releases it. And the next outbox attempt from what the walk left. A failed sweep keeps its turn owed and waits for the upload retry backoff rather than the next cycle, because a sweep lists everything the store holds and retrying that in a loop is worse than waiting. A retirement keeps the file identity in `_connetto_content_retired` until the application acknowledges it, because the dequeue destroys the only record that this device declared the file.

The worker relay runs the content integrity pass at boot and retries its outbox after import, reconnect, mutation acknowledgement, and backoff. Each wake attempts the entry after the one the last wake took, so a file whose ticket keeps being refused cannot starve the rest of the outbox. A ticket wait uses the connection's cancel-safe pump, returns unrelated upstream events to the relay, and yields to queued local relay work while remaining resumable, so a refused or slow mint cannot hold a tab's request.

## Storage

The replica is a `SQLite` file, encrypted with the page codec on both backends. The durable default is OPFS through the sahpool VFS, which allows one handle per file, and that is why the worker holds one database connection and the tabs reach it over the wire rather than over a second handle. `ReplicaStorage::install` in [storage.rs](../../crates/connetto-web/src/storage.rs) installs the OPFS pool when the browser allows it, and the same code runs against the in-memory VFS when it refuses.

When the browser refuses OPFS, the client boots the full topology on an in-memory replica. A Firefox private window refuses `getDirectory` with a `SecurityError`, which is the case the code names. The session gets a working, encrypted client, but the replica, the queued writes and the device-private tier all evaporate when the page reloads. Nothing is persistent, there is no cross-window sharing, and nothing tells the user, which is the gap recorded as [G4](../../plans/master-implementation-plan.md#g4).

The replica's key and the share-key set are the one use of IndexedDB in the topology, and [key custody](14-at-rest-encryption.md#browser-key-store) owns that store. The database never runs on IndexedDB, and no SQLite adapter over it exists.

## Transport

The browser client speaks `web-sys::WebSocket`, and the same [`connetto_core::traits::Transport`] the native client implements backs it, with the same reconnect and keepalive logic and only the socket and the timer different. The relay hub fires the worker connection's keepalive deadline beside its other wakes.

The per-origin connection limit is why the topology is one connection. The elected worker holds the one upstream socket across all the tabs of the origin, and a browser that caps concurrent connections per origin never sees its limit, since the tabs share the worker's connection over the relay.

## File content

Without a filesystem, the file content rides the file crates' own browser storage, OPFS for the chunks beside the replica, and [client storage](18-file-handling.md#client-storage) owns the seam. Chunks are written to storage as they arrive rather than buffered whole, since the WASM linear memory is limited and a full file in memory would exhaust it.

## The platform seam

The native and the browser clients expose the same public API, and the platform-specific code sits behind [`connetto_core::traits::Transport`] and [`connetto_core::traits::Store`] in [traits.rs](../../crates/connetto-core/src/traits.rs), which is the signature of record. The native half implements them with `tokio-tungstenite` and `diesel` over `SQLite`, and the browser half with `web-sys::WebSocket` and `sqlite-wasm-rs` over the worker's VFS. The sync engine depends on the traits alone and holds no direct dependency on a platform I/O primitive. The async futures run on `tokio` natively and on `wasm-bindgen-futures` in the browser. Chunk storage is not among the seams, since the file crates own it.

The consumers are Rust web apps, so the bindings carry no TypeScript surface, and a bundle-size budget belongs to the embedding application, since only it knows what its own bundle can afford. The heavy component is `SQLite` at roughly 300 to 400 KB gzipped. WASM-specific behaviour is tested against the real browser APIs in a headless Chrome build, with no mocked browser environment.

## Supported browsers

The minimum supported browser is the strictest floor of the APIs the build uses, a dedicated `Worker`, `BroadcastChannel`, `navigator.locks`, OPFS and `createSyncAccessHandle`, all in a trustworthy origin, since `navigator.storage` and `navigator.locks` are `[SecureContext]`.

| Browser | Minimum | Binding constraint |
|---|---|---|
| Chrome desktop | 102 | `createSyncAccessHandle` |
| Edge | 102 | `createSyncAccessHandle` |
| Firefox desktop | 111 | OPFS |
| Firefox Android | 111 | OPFS |
| Safari desktop | 15.4 | `BroadcastChannel` and Web Locks |
| Safari iOS | 15.4 | `BroadcastChannel` and Web Locks |
| Chrome for Android | 109 | OPFS on Android |
| Android WebView | 102 | `createSyncAccessHandle` |

Android on the web is supported, on both Chrome for Android and Android WebView, and Android is also served by the native app. A browser below its floor is unsupported.
