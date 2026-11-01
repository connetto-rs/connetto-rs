# 00: Overview

What connetto is and what it does, in one page. The component inventory is in [01](01-pieces.md), the wire is in [02](02-protocol.md), the two paths it owns are in [03](03-sync-pipeline.md), reconnect and offline are in [06](06-reconnect.md), and the delivery side effects and their retries are in [10](10-subscription-materializer.md).

## What connetto does

connetto-rs is a transport and synchronization layer that keeps SQLite-based edge and frontend clients in sync with a PostgreSQL backend. The client runs natively and in the browser from the same codebase, and the immediate consumer is a Dioxus application, which compiles to both targets. One Cargo workspace holds the library crates, and [01](01-pieces.md#crates) lists them.

- Accepts mutations from clients, validates and applies them server-side, and reports the outcome, success, conflict or rejection, back to the originating client
- Watches PostgreSQL for data changes via CDC and pushes the relevant updates to subscribed clients
- Lets clients declare interest in specific queries, row-level or aggregate, and delivers an initial snapshot and the subsequent incremental updates
- Handles reconnect and offline gaps. Clients track their position in the server's change log, catch up on reconnect, and fall back to a full re-sync when the log window is exceeded. [06](06-reconnect.md) owns that path
- Bakes the local replica's schema from the deployment's Postgres sources at build time. The handshake carries the version hash, a mismatch refuses the connection, and an outdated app build reloads rather than fetching a schema. [02](02-protocol.md#the-schema-version) owns the model
- Synchronizes file content on a separate channel from row data, with chunking, hashing and resumability. [18](18-file-handling.md) is the normative chapter
- Enforces authorization on every read and write, derived from PostgreSQL row-level-security policy

The system does not:

- Provide a query language or ORM. Clients write queries against their local SQLite
- Replace PostgreSQL as the source of truth. The server database is authoritative
- Manage UI state or application routing
- Guarantee strong consistency at the client. Clients operate against a local replica that may lag

The transport is one persistent, full-duplex WebSocket per client, and the protocol speaks only on it. An HTTP fallback was declined. A serverless runtime has no place to hold the subscription state, the CDC listener or the session registry, and `wss://` removes the legacy proxy argument. The wire is in [02](02-protocol.md).

## Data flow

```text
+---------------------------------------------------------
| Client (edge or browser)                               |
|                                                        |
|   +-----------+               +-------------------+    |
|   |   Local   |  mutations    |    Sync client    |    |
|   |   SQLite  |---------->    |     (worker or    |    |
|   |  replica  |< row updates  |    background)    |    |
|   +-----------+               +-------------------+    |
+----------------------------|----------------------------
                             |  WebSocket
+---------------------------------------------------------
| Server                                                 |
|                                                        |
|    +-------------+             +-----------------+     |
|    |  PostgreSQL | change events     Session     |     |
|    |  (source of |------------>|   manager, one  |     |
|    |    truth)   |             |    per client   |     |
|    |  CDC / WAL  |             +--------|--------+     |
|    +-------------+                      v              |
|                              +-------------------+     |
|                              |     CDC fanout    |     |
|                              |   matching, auth  |     |
|                              |  filter, delivery |     |
|                              +-------------------+     |
+---------------------------------------------------------
```

**Mutation path**, local to server:

1. Client writes to local SQLite optimistically
2. Client queues the write under its writer and sequence and sends it to the server
3. Server validates and applies it, and acknowledges or rejects
4. The server-side change triggers CDC

**CDC push path**, server to local:

1. PostgreSQL emits a WAL or logical replication event
2. The CDC engine matches the event against the active subscriptions
3. The authorization filter removes the rows the client cannot see
4. The relevant clients receive the update and apply it to local SQLite

**Reliability**. Every cross-process arrow above (client and server, server and PostgreSQL for the CDC source and re-execution, server and OpenFGA, server and the session sink) is retried under the unified policy in [10](10-subscription-materializer.md). The Subscription Materializer is the server-side component that owns those side effects and their retries. `subql`, the in-process engine it wraps, holds no retry surface. The CDC source and re-exec `Connector` it uses are supplied and retried by the materializer.

## Vocabulary

- "Edge client" and "frontend client" are used interchangeably. The architecture is the same for both native and browser targets, with the WASM-specific abstractions handled in [09](09-wasm.md)
- The server owns the subscription state and the authorization, so it takes part in every exchange rather than relaying it
