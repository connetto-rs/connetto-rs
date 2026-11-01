# 17: Fan-out

How one change event reaches many subscribers, the unit of computation, what stays proportional to subscriber count and why each of those is acceptable, and the catchup that reuses the bytes the live path already built. The per-client floor this chapter names and optimises is stated below, the framing is in [02-protocol.md](02-protocol.md), the oplog and its retention window by [06-reconnect.md](06-reconnect.md#retention-window), the component that performs the fan-out by [10-subscription-materializer.md](10-subscription-materializer.md), and the client replica that shares the word retention and nothing else by [15-replica-retention.md](15-replica-retention.md#not-the-server-side-oplog).

## The per-client floor

The floor is one socket write per client, of bytes that need not be distinct, computed, encoded, or copied per client, and [08-authorization.md](08-authorization.md) names the property that made sharing pay. This chapter is the account of how close the delivery side gets to it.

## The unit of computation

**The unit is one change event**, not one subscriber and not one subscription. Every computation a change requires happens once per event. What remains per subscriber is a route lookup, a verdict entry and a socket write, and none of the three recomputes anything the event already produced. The reason the unit is the event rather than the subscriber is that a change event is the only thing in the system that happens once. A subscriber is a destination, not a computation.

Per event, exactly once.

| Work | Where |
|---|---|
| One predicate evaluation per distinct interned predicate, with matched consumers resolved from a bitmap | `subql` interns by a hash of normalised SQL and refcounts |
| One patchset build and one Zstd compression | [`connetto_server::materializer::Materializer::dispatch`] |
| One visibility answer naming every watcher of the event | `subql::visibility::VisibilityPolicy::may_see` |
| One oplog append | [`connetto_server::session::SessionManager::dispatch_event`] |

Per subscriber, and nothing beyond this.

| Work | Where |
|---|---|
| One route lookup | the route map |
| One verdict entry, filled by the single per-event call | `may_see`'s one entry per watcher |
| One socket write, each carrying its own copy of the compressed payload | the transport |

## What stays proportional to subscriber count

**The socket write. Inherent.** Bytes must reach each client, and accepting the write is the objective rather than a concession.

**One verdict per watcher. Accepted.** `may_see` fills a vector with one entry per watcher, so the answer is proportional to watcher count even when computing it is not. `subql`'s `RowPolicy` answers locally with no round trip for every relation the schema decides (the two halves of connetto's own policy, identity comparison and held-key comparison, both fall here), and `OpenFgaPolicy` batches the questions per watcher for the rest. For connetto's own policy shape the verdict count is proportional and the round-trip count is zero. The per-watcher cost that remains is one verdict-vector entry and one socket write, both inherent.

**One route lookup per matched consumer. Accepted.** A hash lookup against the route map, which is not the kind of cost this chapter exists to remove.

**One payload copy per subscriber, until the shared payload moves.** `tokio-tungstenite` at 0.24 is where `Message::Binary` takes an owned `Vec<u8>`, so the frame handed to each socket is an owned contiguous buffer.

**What stops being proportional.** Computation, compression, frame encoding, authorization round trips, and once the payload is shared, payload copies.

## The shared frame

The bulk frame today embeds the payload in the encoded envelope, so every send copies the compressed payload into a MessagePack buffer and again into the tagged frame, three copies per subscriber per event. Planned in [the shared frame design](../../plans/master-implementation-plan.md#shared-frames-and-the-oplog-byte-bound), the frame becomes the tag, a short encoded header, and the compressed payload appended untouched, the payload travels as one shared `Arc<[u8]>`, the data frame carries a server handle derived from the question, and one frame per distinct pair of handle and event is built once and cloned per socket.

No part of that shape goes upstream. The predicate identity the handle derives from is already in `subql`'s registration result, and `subql`'s patchset builder returns an owned buffer, so the shared payload wraps without touching `subql`.

## Catchup

A change's patch depends on the change alone, so building it again per record per subscription at catchup would repeat work the live path already did. The oplog stores the compressed patch beside the event, and catchup sends the stored bytes. [`connetto_server::materializer::Materializer::dispatch`] folds each logged event once into its patch, whether or not a connected subscription matches it, because a change arriving while its only reader is offline matches nothing live and that reader is exactly who will ask for it. The catchup path in [session.rs](../../crates/connetto-server/src/session.rs) asks the match and the visibility question first and builds nothing for a record its caller may not see, folding on demand only a departure notice or a withdrawal.

The event stays in the log beside the patch. Catchup needs the structured event twice per record. [`connetto_server::materializer::Materializer::replay_match`] decides whether the subscription matches, and `EventRow::current` supplies the post-image for the visibility question. So the log grows on both backends. The Postgres row keeps its serialized event and gains the patch column, and the in-memory log keeps its event and gains the patch on the heap.

The window is bounded by entry count and age alone ([06-reconnect.md](06-reconnect.md#retention-window)).

### The patch does not wait for a subscriber

Dispatch folds every event it logs into its patch, whether or not a live subscription matches. A subscription is destroyed with its socket, and who receives a logged change is decided at reconnect, by the subscriptions the client declares again.

So catchup needs no subscription kept after its socket closes, and with it no split teardown, no expiry sweeper keyed to the retention window and no registry cap. A closed app's queries kept to decide whom to wake by push belong to [R101](../../plans/master-implementation-plan.md#r101-live-query-lifetimes-and-waking-a-closed-client).

### What catchup does not get

Catchup frames are not shared across clients, and that is correct rather than a gap. Two clients resuming from different positions receive different sequences, so the sharing above has nothing to key on. Catchup gets the copy elimination and not the frame sharing.

And it still pays two costs per record per client, neither addressed here. One predicate match, and one visibility question. The second is free for connetto's own policy shape, both arms answered locally by `RowPolicy` with zero round trips. The first is `subql`'s interned-predicate evaluation and is left alone.

## Per-subscription cursors

The client persists exactly one cursor. `_connetto_meta` is declared `CHECK (id = 1)` and every live patch overwrites it whatever its subscription, so the client maintains a single session-level resume point ([06-reconnect.md](06-reconnect.md#client-side-state)). The handshake carries one cursor, judged once against the database's timeline history into the one position every re-declared subscription replays from. Server side, [`connetto_server::materializer::Materializer::advance_cursor`] writes a per-`(session, subscription)` cursor into `subql`, which does expose `cursor_for` and `cursors_for_session`, and connetto calls neither. The only observable effect of that bookkeeping is `subql`'s non-monotonic rewind error.

## The materializer lock

Hoisting the per-subscriber cursor advance out of the fan-out loop is worth nothing, and is not carried as a live improvement. Only the change-ingest task takes the lock while delivery runs, so the `3 + K` acquisitions per event cannot contend.

One case the structural argument does not cover. The catchup path takes the lock from a session task rather than from the ingest task, once per replayed record to match it, once more per delivered record to advance its cursor, and on demand for a departure notice or a withdrawal. A catchup storm concurrent with live dispatch is the one shape where the lock could contend, and this chapter carries no number for it.

Sharding or removing the mutex is declined, with no measurement of the one shape that could contend.

## What this design declines

- **A pull path.** Push only, over one WebSocket with a credit window. The trigger to revisit is a deployment needing more concurrent readers of one shared query than one server's socket budget allows, and [02-protocol.md](02-protocol.md) already contemplates an HTTP fallback and defers it.
- **Storing the patch instead of the event.** Match and visibility both need the event.
- **Per-subscription resume cursors as a client-visible capability.** The bookkeeping above stays invisible, and this chapter does not add them to the wire.
- **Sharding the materializer mutex.** No evidence, and the one path that could contend has no measurement.
