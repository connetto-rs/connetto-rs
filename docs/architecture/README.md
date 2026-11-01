# Architecture

How connetto works, one chapter per question. Start with the [overview](00-overview.md), then read the chapter that owns your question. The [plan](../../plans/master-implementation-plan.md) holds what is not built yet.

## Chapters

| Chapter | Owns |
|---|---|
| [00 Overview](00-overview.md) | What connetto is, what it does not do, and the vocabulary the other chapters use |
| [01 Pieces](01-pieces.md) | Every component, the crates of the workspace, and the separate workspaces |
| [02 Protocol](02-protocol.md) | The two planes, the framing, the message families, writers and sequencing, flow control and the wire version |
| [03 Sync pipeline](03-sync-pipeline.md) | How a write travels to the server and how a server-side change travels back |
| [04 Subscriptions](04-subscriptions.md) | How a client declares interest in data, the snapshot and its fence, and the life of a subscription |
| [05 Aggregate queries](05-aggregate-queries.md) | Counts, sums and grouped results, and where each is answered |
| [06 Reconnect](06-reconnect.md) | Going offline and coming back, the oplog and its retention window, and the failover recipe |
| [08 Authorization](08-authorization.md) | Which caller may see or write which row, the executors, grant changes and capabilities |
| [09 Browser](09-wasm.md) | The client in a browser, the worker hub, storage, and what the platform offers |
| [10 Subscription materializer](10-subscription-materializer.md) | The boundary between connetto and `subql`, and who retries what |
| [11 Authentication](11-authentication.md) | How a caller proves who it is, from login to an identity bound onto a session, and session lifetimes |
| [12 Identity, session, capability](12-identity-session-capability.md) | The three concepts and what each keys, the threat model, and apps, bots and installations |
| [13 Client connection](13-client-connection.md) | The client-side Diesel connection, reactivity and the framework adapters |
| [14 At-rest encryption](14-at-rest-encryption.md) | The replica page codec, key custody per platform, ordering constraints, and what the encryption defends |
| [15 Replica retention](15-replica-retention.md) | Why the replica grows, holdings, eviction and physical trimming |
| [16 Server capacity](16-server-capacity.md) | What the server holds in flight, its three pools, the share reserved for identified callers, and the write meter |
| [17 Fan-out](17-fan-out.md) | How one change event reaches many subscribers and what stays proportional to their number |
| [18 File handling](18-file-handling.md) | File identity, chunking, the encrypted stores, tickets, availability and the abuse surface |
| [19 Device to device](19-device-to-device.md) | Offline collaboration between devices, certificates, the per-device frontier, the link, the exchange and the provisional tier |
| [20 Deployment](20-deployment.md) | What a deployment holds, what a restore does to connected clients, erasing a person, running the server and the replication slot |
| [21 Clocks](21-clocks.md) | Which clock every timer reads, what each does across a suspend, and the skew between machines |

[subql.md](subql.md) tracks what connetto relies on from `subql`. [architecture-diagram.svg](architecture-diagram.svg) is a picture of the whole.

## Conventions

A chapter describes what is built and links the plan for what is planned. Each fact has one owning chapter, and the others link to it. A public symbol is cited as a rustdoc link with its full path, which the `connetto-architecture` crate checks on every docs build. That crate exists only for the check, and the chapters are read here as Markdown. Anything else is a file link, which the link check in CI follows. Never a line number.
