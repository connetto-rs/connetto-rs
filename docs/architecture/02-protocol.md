# 02: Protocol

How the client and the server talk. The transport, the two planes and their framing, the message families, the grants and the session token, the writer model with its sequencing rules, flow control, rate limiting, and the wire version. The message types are the code, and this chapter links them. Identity and sessions are in [12](12-identity-session-capability.md), the delivery side effects in [10](10-subscription-materializer.md), and the offline side in [06](06-reconnect.md).

## Transport

One persistent, full-duplex connection per client, and WebSocket is the baseline. Both sides send at any time, with no request-response pairing. An HTTP fallback was declined. A serverless runtime has no place to hold the subscription state, the CDC listener or the session registry, and `wss://` removes the legacy proxy argument.

## Framing

The two planes share the connection. The control plane carries the typed frames, and the bulk plane carries the large payloads, the snapshot data, the live patches and the uploaded patchsets. A one-byte tag distinguishes the planes, [`connetto_core::codec::TAG_CONTROL`] and [`connetto_core::codec::TAG_BULK`], and the codec in [`connetto_core::codec`] serializes both planes.

| Plane | Encoding | Content |
|---|---|---|
| Control | MessagePack via `rmp-serde`, never compressed | The typed frames of `ControlMessage` |
| Bulk | MessagePack, with the payload field carrying Zstd bytes the codec moves without touching | The compressed payloads of `BulkMessage` |

Over message-delimited transports such as WebSocket, a frame is the tag byte plus the serialized message. Over byte-stream transports, each message is wrapped in a `u32` big-endian length prefix, the `encode_*_framed` and `decode_*_framed` functions of the codec, and a framed read is refused past [`connetto_core::codec::DEFAULT_MAX_FRAME_LEN`], 64 MiB.

MessagePack is the serialization for both planes. It was chosen over protobuf, FlatBuffers and JSON for the wire. The shape is known at compile time, the encoding is compact, and named structs tolerate an unknown or a missing field, which is what an additive change needs. JSON is reserved for one place, the aggregate results, whose shape is genuinely unknown at compile time and is deserialized client-side into whatever type the caller named.

Planned in [the shared frame design](../../plans/master-implementation-plan.md#shared-frames-and-the-oplog-byte-bound), the bulk frame as a short header with the compressed payload appended, so the payload reaches a socket without a copy. [17](17-fan-out.md) owns the reasoning.

## Message families

[`connetto_core::messages::ControlMessage`] enumerates every control frame, and [`connetto_core::messages::BulkMessage`] enumerates the bulk frames. Direction is enforced at the endpoints, not by the type system, so the same enum represents both halves of the conversation, and a server dispatcher that receives a frame for the other side treats it as a protocol violation. The families below are the ones this chapter and the code discuss.

- Connection. [`connetto_core::messages::Handshake`], [`connetto_core::messages::HandshakeAck`], [`connetto_core::messages::Challenge`], [`connetto_core::messages::Ping`], [`connetto_core::messages::Pong`]
- Bot registration. [`connetto_core::messages::RegisterBot`], [`connetto_core::messages::BotRegistered`], [`connetto_core::messages::ControlMessage::RegistrationRefused`]
- Subscription. [`connetto_core::messages::Subscribe`], [`connetto_core::messages::Unsubscribe`], [`connetto_core::messages::MembershipOpened`]
- Snapshot and catch-up. [`connetto_core::messages::SnapshotBegin`], [`connetto_core::messages::SnapshotEnd`], [`connetto_core::messages::SnapshotPatch`] on the bulk plane
- Live delivery. [`connetto_core::messages::LivePatch`] on the bulk plane, [`connetto_core::messages::AggregateUpdate`], [`connetto_core::messages::FullResyncRequired`]
- Mutation. [`connetto_core::messages::MutationHeader`], [`connetto_core::messages::MutationPatch`] on the bulk plane, [`connetto_core::messages::MutationApplied`], [`connetto_core::messages::MutationReject`], [`connetto_core::messages::MutationConflict`]
- Writer. [`connetto_core::messages::BindDevice`], [`connetto_core::messages::RetireWriter`], [`connetto_core::messages::WriterAnswer`], [`connetto_core::messages::Provenance`]
- Device enrolment. [`connetto_core::messages::EnrolChallengeRequest`], [`connetto_core::messages::EnrolChallenge`], [`connetto_core::messages::EnrolRequest`], [`connetto_core::messages::EnrolGrant`], [`connetto_core::messages::EnrolRefused`], [`connetto_core::messages::RevocationUpdate`], [`connetto_core::messages::DevicesRequest`], [`connetto_core::messages::DevicesList`], [`connetto_core::messages::RevokeDeviceRequest`], [`connetto_core::messages::DeviceRevokedAck`]
- Content tickets. [`connetto_core::messages::ContentTicketRequest`], [`connetto_core::messages::ContentTicketGrant`]
- Share keys. [`connetto_core::messages::ReplaceShareKeys`], [`connetto_core::messages::ShareKeysReplaced`], [`connetto_core::messages::TabShareKeyChange`], [`connetto_core::messages::TabShareKeyAnswer`]
- Relay to tabs. [`connetto_core::messages::SyncStatus`], [`connetto_core::messages::GateState`], [`connetto_core::messages::TabIdentity`], [`connetto_core::messages::TabStorageUse`], [`connetto_core::messages::TabStorageAnswer`], [`connetto_core::messages::HeldWork`]
- Outage and error. `DeliveryPaused` and `DeliveryResumed` on `ControlMessage`, [`connetto_core::messages::FatalError`], [`connetto_core::messages::NonFatalError`], [`connetto_core::messages::RateLimited`]

The relay-to-tab frames are never sent by a real server. The browser worker's hub states them to the tabs it serves, and [09](09-wasm.md) owns the semantics.

`DeliveryPaused` and `DeliveryResumed` carry the outage signal. When the authorization service is unreachable connetto fails closed, delivering no patch, and a caller must be able to tell that from nothing changing. [`connetto_core::messages::PauseCause`] names the cause, the authorization-service outage, the stalled change stream, and the unreachable database. Snapshots are unaffected, because they run under Postgres RLS. [08](08-authorization.md) owns the fail-closed rule.

## The opening challenge

Every connection opens with the server's `Challenge` frame, the random bytes the first frame of the connection signs. The signatures over a connection nonce carry distinct domain separators, so a signature over one connection answers exactly one of the questions it could be asked. A bot registration spends its one-time code as the first frame of a fresh connection, signed over the nonce, and the server settles it with the bot id or refuses it without a reason. The bot entities are in [12](12-identity-session-capability.md), the bot login over the challenge is in [11](11-authentication.md), and a device key proves itself the same way, over its own message, in the writer section below.

## Grants

A grant is a connetto-signed token asserting that the bearer is a named subject, a person like `user:alice` or a key like `key:abc123`. It says nothing about what the subject may do. [08](08-authorization.md) answers that from the authorization model. [`connetto_core::messages::Handshake`] carries zero or more opaque grants, and each resolves independently into an identity, a capability, or a refusal. Two logins on one handshake leave the caller unidentified, so no order of checks decides who is calling.

Every grant is checked by arithmetic. Both kinds are connetto-signed, so checking one is a signature verification against connetto's own public key, with no database lookup. The list carries no routing metadata, nothing sniffs the shape of a string, no order of checks is load-bearing, and an unrecognised string costs arithmetic and nothing more.

The client stores and presents a grant and never interprets what it authorizes, with one exception the format already permits. A grant is an EdDSA JWT and a JWT payload is base64url, signed rather than encrypted, so the client reads the `exp` of a token it already holds with no key and no round trip. It does so in `has_expired` in [`grant_expiry.rs`](../../crates/connetto-client/src/grant_expiry.rs) and skips presenting a key whose time has passed, because otherwise a dead key is re-presented on every reconnect and draws a refusal every time. This is safe because it is advisory only. The server verifies `exp` authoritatively regardless, so a client trusting a forged claim either presents a dead key and is refused as now, or skips a live one and harms only itself. Anything the client cannot read is presented rather than withheld, for the same reason. No other claim may drive any client decision.

A grant that fails to resolve does not end the connection. The session proceeds on whatever resolved, and a caller who presents an expired key beside a valid login is signed in and sees less, which is the ordinary case. The reply says nothing about a failure, no reason and not which grant it was. Not-allowed, no-longer-allowed, and never-existed are indistinguishable, on the same reasoning that a service does not distinguish an authorization failure from a missing resource. The failures are recorded in the server's structured log and nowhere else. A boolean reporting that some grant failed was examined and declined. Revoking a share key deletes an application row and never touches the token, so the grant still checks out and no refusal occurs, and the boolean would have been silent in the one case it existed to catch.

## The session token

The handshake field `resume_token` is the credential a run presents to continue, and the reply carries `session_token`, the handle in the clear, and `resume_token`, the credential for next time. They are two different things. The handle is what the application reads, because a synced row written before anybody signed in is attributed to it, and the credential is the bearer secret proving the handle is this caller's.

The server mints a real durable handle at the handshake, and the client persists it outside the local replica, because an unidentified session's replica is in memory and would not survive a reload. The server refuses a credential it did not sign, so a caller can neither invent a handle nor resume as a visitor whose handle it obtained. An identified run takes its handle from its login grant instead and ignores the field. The session model is in [12](12-identity-session-capability.md).

## The schema version

The `schema_version` in `HandshakeAck` hashes the whole translated bundle. [`connetto_core::SchemaBundle::version`] covers the two Postgres source documents, the translated replica DDL, the policy table pairs and views, and the caller and subjects function names, under a domain tag with every field length-prefixed so no two bundles collide by concatenation. Pair and view order do not count, and the device-private tier does not count, because the server never sees it.

Both sides derive it through one build step. An application's build script runs [`connetto_schema::emit`] over its sources, and `connetto-server` runs [`connetto_schema::translate`] over its own at startup with the pg2sqlite it was built with. Two builds whose sources match but whose translations differ, from two pg2sqlite revisions for instance, therefore present different versions, and the client refuses the handshake as [`connetto_client::ClientError::SchemaOutdated`]. The version rides the ack's one `schema_version` field.

A client always declares a version, because the builders take a `SyncSchema` built from a bundle and give no way to leave it out. A server that declares none still skips the check.

## The application schema major

Every incompatible application schema change is a new major, an integer from 1 up that the Postgres schema source declares on a line of its own, `-- connetto:major N`. [`connetto_core::SchemaBundle::major`] reads it from the source, so the directive sits inside the version hash and cannot drift from the document it describes, and a source without it fails translation.

The client sends its major as `Handshake.schema_major`. The server compares it with its own before checking any grant and refuses a mismatch with `FatalErrorReason::MajorMismatch`, sent before the ack as `ProtocolVersionMismatch` is. A client whose major is older than the server's, or that sent none, surfaces `ClientError::UpdateRequired`, and one whose major is newer surfaces `ClientError::ServerBehind`. A deployment hosting several majors puts a proxy in front of one server per major and routes on that field, so every client keeps one URL. A browser relay hub holds its tabs to the worker's major the same way, so a tab left open across a deploy is refused rather than reading a replica built for another schema.

The major and the version answer different questions. The major is what routing, refusal and the replica's record use. The version still catches the same major built from different sources.

## Writers and sequencing

### Writers

Every write carries a (writer, sequence) pair, and the server keeps one applied frontier per writer that every path consults. The handshake names the writer the connection writes as:

- A native device with an enrolled key writes as that key. The handshake asks for the device, no write is taken before the `BindDevice` proof, and the proof is an ECDSA signature over the connection's `Challenge` nonce. The answer carries the key's frontier.
- Every other client, a browser profile, a build without a device identity, or a device before its first enrolment, writes under a number the server issues. The number is drawn with a [`connetto_core::messages::WriterSecret`], the client keeps both with its replica, and every later handshake presents them together.

`HandshakeAck` answers a number writer with [`connetto_core::messages::WriterGrant`], the number and its frontier. Every sequence at or below the frontier is applied or refused, and the next write must carry the one after. A write arriving at any other sequence closes the connection with `FatalErrorReason::SequenceGap`, and a different entry already settled at the same writer and sequence is answered [`connetto_core::messages::MutationRejectReason::Forked`] and never applies.

Idempotency is per (writer, sequence). A replay reuses the original number, and the server answers it from the settled record. The same account writing under two writers has two independent sequences, and a number already settled under writer A is not a duplicate under writer B. The client keeps one row per writer with its next sequence, and a writer an imported archive was made under retires through `RetireWriter`, so the importer's copies carry the original writer's provenance and the server settles the logical write once whichever copy arrives first. The model's rationale is in [13](13-client-connection.md), and the device side is in [19](19-device-to-device.md).

### Client mutations

- Mutations carry a monotonically increasing sequence per writer, `client_seq` in [`connetto_core::messages::MutationHeader`]
- The server processes writes in sequence order per writer and echoes the sequence in `MutationApplied`, `MutationReject`, and `MutationConflict`
- The client sends one sequence at a time and does not send a new one until the previous one is answered. A window of N in-flight mutations does not apply, because the client sends patchsets rather than individual mutations, and the sequence is what any such window would have ordered

### Server to client ordering

- Row and aggregate updates carry the cursor position
- Frames arrive in commit order on an ordered transport, so the live path applies them in arrival order, with no gap detection and no reorder buffer
- A gap can only open across a disconnect, and the resume cursor plus catch-up or full resync covers it. [06](06-reconnect.md) owns that path
- The client stores its highest applied position and sends it in `Handshake.last_cursor` on reconnect

### Snapshot before updates

- A fresh `Subscribe` receives `SnapshotBegin`, one or more `SnapshotPatch` frames, and `SnapshotEnd`
- An ordinary row catch-up receives its `LivePatch` frames followed by `SnapshotEnd` at the replay ceiling, including when no row changed
- `SnapshotEnd` costs no delivery credit and shares the ordered queue with the rows it completes, so a closed credit window cannot release it ahead of pending rows
- No frame goes out until the snapshot read has succeeded. A subscription the server cannot serve draws exactly one fixed refusal frame, and the cause goes to the structured log
- Live changes after the snapshot or replay ceiling apply on top
- `LivePatch` frames overlapping the snapshot are re-applied, not filtered. Neither the snapshot position nor a change's WAL position orders by visibility, so a discard rule loses data. [04](04-subscriptions.md) owns the reasoning

## Flow control

The client maintains a **receive credit** budget. On connect, the server is granted an initial credit, each bulk frame consumes one credit, and the client sends `AckCredits(n)` to replenish `n` credits after processing `n` of them. No control frame is ever charged, so keepalive and acknowledgements cannot deadlock behind a full window.

The server pauses bulk delivery when credits reach zero and resumes when credits are replenished. A client that stops granting credits makes the server buffer every patch for it without bound. [G10](../../plans/master-implementation-plan.md#g10)

During catch-up, [`connetto_server::session::SessionConfig::with_catchup_input_budget`] limits held peer input to 64 KiB by default, measured as tagged `MessagePack` encoded bytes without an encoded copy. Overflow ends only that connection through the protocol-error path, including when one otherwise-valid frame exceeds the holding budget. The bound excludes decoded-object overhead and transport buffers, and it is independent of bulk delivery credits.

`SnapshotEnd` is the one control frame that waits in the queue. It describes rows the client has not received, so releasing it early is a statement the client would act on wrongly. Everything else on the control plane bypasses the queue entirely. Not-rationed and not-ordered are separate properties, and the queue carries both.

## Rate limiting

Credits bound how much undelivered data a session accumulates. They do not bound what a caller may ask for, so three signals are metered on the sync path. Subscription creation, connections, and refused grants, each per window and each **tiered by whether the handshake resolved an identity**. An authenticated caller is accountable, there is a user to attribute cost to and a session to revoke, and an unidentified one has neither by definition, so its allowance is smaller rather than absent. Everything counts against the durable session handle rather than a per-connection counter, which is what makes a limit survive a reconnect instead of capping one connection.

Over the limit, a subscription draws `RateLimited` and the session stays open, while a connection or a flood of refused grants is closed with `FatalErrorReason::RateLimited`. Nothing is served slowly. connetto refuses rather than queues, because a queue is the cost the caller wanted to impose. [16](16-server-capacity.md) owns the pool and the reader share, and [08](08-authorization.md) the same reasoning applied to bans.

A mutation's compressed bytes are metered per identity. A write takes the reader-share permit, which protects the pool equally against everyone and records nothing about who caused the load, and the meter the write flood needs is a per-identity token bucket. A mutation charges its patch bytes to [`connetto_server::throttle::ThrottleConfig::with_mutation_bytes_per_identity`], default 64 MiB a minute, keyed by the session handle for a caller with no identity so unidentified runs never share one bucket, and over it draws the same `RateLimited` the reader share sends, naming the sequence and the wait until the bytes fit, so the client keeps the write pending and resends once.

**What one read may cost is bounded too.** Credits and counts are the caller's allowance. The cost of a single read is bounded separately. The query is wrapped with a row cap derived from a per-tier byte budget, a per-tier time limit is set on the read's own transaction because a row cap bounds connetto's memory and the wire and not the database's work, and a row above a per-tier ceiling is refused rather than delivered.

**A large read is served rather than refused, which is why an initial snapshot may arrive as several `SnapshotPatch` frames.** A read past its budget is paged by primary key, never by offset, and the next page is taken when the client acknowledges the last, so the credit window paces the database reads as well as the wire. `SnapshotBegin` and `SnapshotEnd` still bracket exactly one read, and the cursor `SnapshotEnd` carries is the first page's, so resuming from it can only re-deliver changes the client already has. A live patch may arrive between two pages. That is safe in one direction only, and it is the right one, because a page is read after every frame already sent and so can never carry a value older than the wire.

A read the server will not serve is refused, never truncated, and every cause reaches the wire as one fixed phrase with the cause in the server's log. A read that fails part way through the pages is replaced once through `FullResyncRequired`, whose `SnapshotInterrupted` reason says so, and refused if the replacement fails too.

**connetto never meters a network address, and never fingerprints a caller.** By the time it could consult an address it has accepted the connection, completed the upgrade and allocated a session, which is the whole cost. That belongs to the edge. Fingerprinting was examined separately and declined on its own merits, not inherited from the address decision. connetto terminates no TLS, so it never sees a `ClientHello` and cannot compute a fingerprint, and one would name a client library rather than a caller in any case. The consequence is accepted rather than hidden. A caller that discards its handle every connection gets a fresh allowance, and the reader share in [16](16-server-capacity.md) reserves a share of the connection pool for identified callers and so bounds unnamed traffic without needing to name it, plus the edge for raw volume.

## The wire version

[`connetto_core::version::PROTOCOL_VERSION`] rides on `Handshake.protocol_version`, and the server closes with `FatalErrorReason::ProtocolVersionMismatch` on a mismatch, before the ack. There is no negotiation. A client on the wrong version is disconnected.

The version bumps on any breaking change to `ControlMessage`, `BulkMessage`, framing, or compression discipline, which is the doctrine the constant carries. Compatible additive changes, a new optional field on an existing struct, do not require a bump, because MessagePack-encoded named structs tolerate unknown and missing fields at the serde layer. Adding a variant to [`connetto_core::messages::FullResyncReason`] or [`connetto_core::messages::FatalErrorReason`] is a breaking change, because neither enum has a forward-compatible fallback for an unknown value.
