# 16: Server capacity and admission

How the server bounds the work it will hold in flight for all its clients at once. The server's own Postgres connections are a bounded pool, and a share of the reader pool is held for callers whose handshake resolved an identity, so unidentified traffic can never make the pool unreachable for a signed-in one. [The three pools](#the-three-pools) describes the pools and their configuration. [Reserving for identified callers](#reserving-for-identified-callers) describes the reservation, its refusal, and the meters beside it. [The write path is metered per identity](#the-write-path-is-metered-per-identity) names the meter on the write path and the surface the server never reads. The write path is in [12-identity-session-capability.md](12-identity-session-capability.md), the grant flow and the tier in [08-authorization.md](08-authorization.md), and rate limiting, which this chapter is not, is in [02-protocol.md](02-protocol.md).

## Not rate limiting

Rate limiting bounds what one caller may ask for over time and answers a caller that asks too often. Admission bounds what the server holds in flight at once and answers a caller arriving when there is no room. The two are independent. A caller can be inside every rate limit and still find the server full, and a caller can be over its limit while the server sits idle.

They also fail differently, which is why they are separate mechanisms rather than one. A rate limit has to recognise the caller, so a caller that discards its name escapes it. Admission only has to count, so nothing escapes it.

The queues a session can fill are unbounded [G10](../../plans/master-implementation-plan.md#g10), so a client that writes without pause can fill the process's memory. Nothing in this chapter bounds that.

## The three pools

The server builds three Postgres pools through `build_pool` in [mod.rs](../../crates/connetto-server/src/builder/mod.rs), a bb8 pool over the `diesel_async` engine. The sizes are settings, and the binary reads them from the environment.

| Pool | Purpose | Environment variable | Default |
|---|---|---|---|
| Owner | Connects as the deployment's owning role | `CONNETTO_OWNER_POOL_SIZE` | 10 |
| Re-execution | Connects as the reader role, sized at the owner pool's | (no variable) | the owner pool's size |
| Reader | Connects as a role row-level security applies to | `CONNETTO_READER_POOL_SIZE` and `CONNETTO_READER_RESERVE` (the reserve's share, default 3) | 10 |

The **owner pool** carries the authentication store, audit writes and every read of the facts behind the authorization store, which must see every row ([08-authorization.md](08-authorization.md)). Row-level security does not apply to it, which is the entire reason the reader-role pools exist, and the server refuses to start when it would, a table forcing row-level security or a role that does not own the table.

The **re-execution pool** connects as the reader role and carries the computed reads that run under a caller's binding, the session's scalar and seed first answers and the engine-driven re-reads. It is a pool of their own, sized at the owner pool's, so a re-execution hold never reaches the gated reader pool the reserve splits. It is the reader role so the caller's policies scope the rows a read answers.

The **reader pool** connects as a role that row-level security does apply to, and the binary refuses to start without it. It carries everything a caller's own request touches. Every visibility question, every snapshot read, every mutation apply, and the durable watermark read the handshake performs before it can complete.

The reader pool is therefore the contended resource, and what contends for it is callers rather than connetto's own background work.

## Reserving for identified callers

A share of the reader pool is held for callers whose handshake resolved an identity. Unidentified callers may occupy the total less that share and no more, so a connection remains reachable by an identified caller whatever volume of unidentified traffic has arrived. [`connetto_server::reserve::ReaderGate`] is the mechanism, a permit split held by [`connetto_server::RequestGuard`] and taken around each caller-attributed reader-pool span. An identified caller is never gated. The pool itself is its bound, and the reserve exists so the gate can always say yes to it. The spans are the handshake's watermark read, a row subscription's delivery, a requested share-key replacement's row resends, a mutation's apply, and the library mint call.

The guarantee is arithmetic rather than behavioural. It detects nothing, has no threshold and no window, and keeps no state that decays. It needs no key for the caller, only the tier the handshake already established, which [`connetto_server::throttle::Tier`] carries to every limit call. It is strict, not work-conserving. The reserved share is held back even when no identified caller wants it, so the promise does not weaken to however long in-flight unidentified work takes to drain, which a snapshot transfer makes unbounded in practice. It is one reserve over the whole reader pool, whatever the operation, rather than one per operation class, each of which would need its own number nobody has measured.

An unidentified caller whose share is full queues for 1 second, and a share still full past that is refused. The refusal is a `RateLimited` reason carrying the queue window as its `retry_after_ms`, a fatal error that ends the connection when it reaches the handshake, or a control message that defers the refused mutation, which stays in the client's pending queue. The change path is not what occupies the reader pool. Visibility questions are issued one at a time by the single change-ingest task, so that path holds exactly one connection at any moment whatever the subscription count.

A share key does not confer the reserved standing. Standing here means the deployment can take it away, and a key's withdrawal is deleting the relation behind it while the grant check makes no store call, so a withdrawn key keeps resolving until it expires ([12-identity-session-capability.md](12-identity-session-capability.md)). Lifting a key holder would hand a leaked key a reserved connection for the rest of its lifetime, exactly while the deployment is trying to cut it off.

The meter of record is `meter_key` in [capability.rs](../../crates/connetto-server/src/capability.rs). A grant whose storage key is `user:<id>/keys:<...>` meters under that key, and every other caller meters under the handle its session mints per run.

Planned in [R99](../../plans/master-implementation-plan.md#r99-bot-login-and-lifetime), an installation connection meters the owner its installation row names, and a bot-level connection meters the bot's owner, so a deployment sets budgets per owner.

A ban needs a durable name, and a caller presenting no resume credential is minted a fresh one every connection, so a ban cannot reach a caller that cycles handles. A reservation never asks who is calling, so the number of identities a caller cycles through does not enter into it. What a caller with no identity gets instead is a per-connection tally, three refusal counts within one socket and no window, because the connection is the window. The outcome is that socket closing with no durable record and nothing reported to the application, which ends a runaway loop inside the connection it is happening in and nothing more, since a reconnect starts over.

## The write path is metered per identity

The write path takes its reader-pool span like any other caller-attributed span, and it is metered on top. [`connetto_server::throttle::ThrottleConfig::with_mutation_bytes_per_identity`] in [throttle.rs](../../crates/connetto-server/src/throttle.rs) sets the caller's share of compressed patch bytes per window, 64 MiB per minute by default, and zero switches the meter off. A write over the share is deferred with `RateLimited` and the window's remaining time as its `retry_after_ms`, and the mutation stays in the client's pending queue until the window turns, so it is not lost.

The server never reads or acts on a network address. Volumetric defence belongs to the edge, and defeating the sender is somebody else's job.
