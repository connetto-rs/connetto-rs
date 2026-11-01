# 12: Identity, session, and capability

A session, an identity, and a capability are three separate things, and this chapter is the canonical model for them. It says what mints each one, what it survives, what it keys, and what a caller holding one but not the others may do. Where another chapter disagrees with it, this one is right. How a caller proves an identity is [11-authentication.md](11-authentication.md), which rows the identity may reach is [08-authorization.md](08-authorization.md), and what the local replica is named and protected by is [14-at-rest-encryption.md](14-at-rest-encryption.md).

## The three things

| | What it is | Minted by | Survives | Keys |
|---|---|---|---|---|
| **Session** | An opaque durable handle for one client's operational state | Server, at login for an identified run and at handshake for an unidentified one | Reconnects, restarts, leader failover | The exactly-once watermark, subscription cursors, the pending buffer |
| **Identity** | A verified statement of who the caller is | Identity provider, mapped at login | Its refresh window | The Postgres setting the read and write paths bind, and the local replica's name |
| **Capability** | A connetto-signed token asserting the bearer is a named subject | connetto, on an authenticated request | Its expiry, or until its relation is withdrawn | Nothing. The model decides what its subject may do |

A session always exists, an identity may or may not, and a capability may be held zero or many times with or without an identity. All four arrival cases are real. The fourth, a signed-in user holding a key to somebody else's resource, is the one a single-credential shape cannot express.

A caller that authenticated nothing is represented by `Principal::unidentified` rather than by whoever it claimed to be. [`connetto_core::auth::AuthContext`] keeps `user_id` non-optional, because the Postgres setting the read and write paths bind and the exactly-once watermark keyed on `(user_id, session_id)` both demand an identity when one is present. A doc comment is not a guard, so the two seams enforce this at startup.

### Session

The server mints an opaque handle at login for an identified run and at handshake for an unidentified one. The client persists it and presents it on the next connection. The reply carries it in the clear as `session_token`, the string form of the durable `SessionId`, and the authentication credential it pairs with does a different job from this handle ([11-authentication.md#the-connetto-session-credential](11-authentication.md#the-connetto-session-credential)).

`connection_id` is a per-connection routing label minted by the server's per-connection counter at handshake, and it dies with the socket. The counter field is named `next_session`, which counts connections, and the name is the one misnomer the split left behind. `SessionId` is a version-4 UUID, and `session_token` is its string form.

The token persists independently of the local replica. For a caller with no identity the replica is in memory ([A caller with no identity](#a-caller-with-no-identity)), so a token kept inside it would not survive a reload and the session would be lost on every page load.

A handle covers one unbroken run of one caller, and any change of caller mints a new one, so nothing is ever inherited. A handle that outlived a change of caller would hand the watermark, the subscription cursors and the pending buffer to the next person to use the device. Discarding them at sign-in costs nothing, because the local copy changes from in-memory to identity-named at that same moment and a fresh snapshot happens regardless.

A connection that lost its in-memory write counter repairs it from the frontier the handshake answers ([06-reconnect.md](06-reconnect.md#reconnect-handshake)).

### Identity

An identity is optional. `Principal` carries it or does not, alongside whatever capabilities resolved. An identity carries a user id and nothing else. It is the only thing that binds the Postgres setting the read and write paths use, and the only thing that names a durable local replica. Tenant isolation and roles live in the authorization model, not in the identity, and [08-authorization.md](08-authorization.md) owns how the model carries them. An attribute rule that needs a claim takes it as a per-question argument naming the attribute, rather than as a bag carried for the life of every session.

### Capability

A capability is a token connetto minted and signed, asserting one thing. That the bearer is a named subject, for example `key:abc123`. It says nothing about what that subject may do.

| Grant | What the signature proves | Who decides what it may do |
|---|---|---|
| Login token | the bearer is `user:alice` | the authorization model |
| Share key | the bearer is `key:abc123` | the authorization model |

A capability must not carry its own permission. A permission living inside the token would split authorization between the token's contents and the model, which is the divergence the single policy source exists to prevent. The permission is a relation on the subject, `document:readme#viewer@key:abc123`, derived from a Postgres row the application owns.

Withdrawal is immediate. Revoking a share key deletes the relation, a Postgres row change that is the notice the refresh path [08-authorization.md](08-authorization.md) already defines. The token stays cryptographically valid and names a subject with no permissions left. Verification is arithmetic, and [The grant list](#the-grant-list) states the rule in full. A share key still needs an expiry, a second bound beside withdrawal, because a bearer token that never expires is a permanent secret. `AuthConfig` carries `capability_ttl` and a `capability_max_ttl` ceiling, and a mint asking for longer than the ceiling is refused rather than quietly shortened. The grant check reports each key's `exp`, and the session drops the key at that second and resends every subscription it reached, so the expiry holds on an open connection too. Custom handshake authorities may accept grants without an expiry, and a subject named by several accepted grants remains held while any of them is valid.

Minting is a library call the application invokes from its own handler, so the application keeps its routing and rate limits and connetto gains no endpoint beside the five on [`connetto_server::auth_router`] in [http.rs](../../crates/connetto-server/src/authn/http.rs). Creating a share key is itself an action needing authorization, because a caller must not be able to share what it cannot read, and the check goes through the same trait that answers every other authorization question. The call returns the subject id it minted, and the application writes the row that grants the relation to that subject. connetto receives only the key the application extracted, on the handshake or through `add_share_key` on a running client ([13-client-connection.md](13-client-connection.md)).

A link carries the key in its fragment, which never leaves the user agent ([RFC 3986 section 3.5](https://www.rfc-editor.org/rfc/rfc3986#section-3.5)), keeping it out of the HTTP request, server logs and the `Referer` header. On a phone it is a Universal Link or a verified Android App Link, which [RFC 8252 section 7.2](https://www.rfc-editor.org/rfc/rfc8252#section-7.2) prefers over a custom scheme. The page removes the fragment after handing the key to the client, but earlier history entries still hold it, so a link key should be short-lived and revocable.

The subject is the deployment's own type, not a string. `CapabilitySubject<Key = String>` mirrors `AuthContext<Id>`. The key's serde encoding is what the signed token's `sub` claim carries and its `Display` rendering is what a policy compares against. The type implements `CapabilityKey` in [auth.rs](../../crates/connetto-core/src/auth.rs), where the deployment says which Postgres setting the held keys are bound to and how a held set is packed into that one value. Minting is a separate `MintCapabilityKey` beside the issuer in [capability.rs](../../crates/connetto-server/src/capability.rs), because a replica needs the packing but has no business making a bearer secret. `String` implements both, minting `key:` plus a version 4 UUID, and is the default.

A policy can only compare against a value the transaction bound, and a caller may hold several keys, so the set travels as one text value under a second setting beside `app.user_id`, `app.subjects` by default.

```sql
viewer = ANY(string_to_array(current_setting('app.subjects', true), ','))
```

The identity is deliberately not in that list. It stays bound once, at `app.user_id`, so no existing policy changes and there is no second place the identity could disagree with itself. Connetto refuses to sign a key whose rendering contains the separator, the one way a delimited list could grant a neighbouring key's access. A caller holding no key binds the absent marker [08-authorization.md](08-authorization.md) describes rather than leaving the setting unbound, because an unbound custom setting reads as `''` to the next caller on a pooled connection.

A commit is attributed to each key, not to the packed set, because a row owned by the joined list matches no single subject and its uploader could not see it. The content path's `connetto_set_content_state` setter is called once per key the caller holds, so a deployment storing the value as the owner ends with one row per key and the caller sees its own upload under any of them. The setter is called more than once for one commit and must be idempotent, which the `ON CONFLICT DO NOTHING` shape the file chapter recommends satisfies.

A replica answers the same membership arm. pg2sqlite translates the membership test into a delimiter-surrounded `instr` search over the delimited value, so the spelling above reaches the replica unchanged. A deployment declares the setting a held set in its translation (`SessionVariableMapping::holding_set`) and its replica answers it through the subjects function every build registers (`with_share_keys` on the native or browser builder). The grant is what the server reads the key into, and the subject is what the replica compares locally. A holder of neither sees the arm admit nothing, on both ends.

Connetto checks the caller may read the resource the mint call names, and the application writes the permission row on its own connection. Nothing in connetto stops that row naming a different resource. What stops it is the deployment's own policy on the sharing table, a `WITH CHECK` requiring the shared row to be visible, evaluated by Postgres as the sharer. A connetto-owned generic table could only name a row by the `pk` encoding, which no policy can compute, so connetto writing the row was rejected.

A share says which verbs it certifies, and reading is always one of them. The mint call carries a `ShareLevel`, a set drawn from insert, update and delete, and connetto asks the write question once per named verb about the row as it stands, refusing the whole share if any is denied. `IssuedCapability::level` reports back exactly what was certified, which is the thing the permission row must not exceed.

Connetto's answer to that question is `RlsAuth::may_write` in [auth.rs](../../crates/connetto-server/src/auth.rs).

| Verb | How it is certified |
|---|---|
| insert, and the resulting row of an update | allowed. Their only caller applies the write to Postgres immediately afterwards and the database is the gate, so answering here would be a second evaluator that can disagree with it |
| delete on an existing row | a locking read, `SELECT ... FOR UPDATE`, so the table's update rule decides. Bounded by a wait, and an expired wait is an undecided answer rather than a denial |

A delete is refused at the mint when the table writes a rule for a single command, since a locking read speaks only for the update rule. A verb the answerer does not know refuses rather than guessing. An undecided answer surfaces as `ShareError::WriteUndecidable` in [capability.rs](../../crates/connetto-server/src/capability.rs), and the mint certifies nothing.

The application names the verbs rather than connetto inferring them from a two-valued level, because only the application knows what its own relation means. A guess of updating and deleting together would bar a sharer on an append-only or soft-delete table from minting a write share, and updating alone would hand a bearer the right to delete unchecked.

The level is in the reply and not in the token, so what makes it binding is the deployment's own `WITH CHECK` on the sharing table, exactly as it is for the read half.

## A content ticket is not a capability

The file channel serves content bytes over plain HTTP, where an `img` tag can present no grant, so connetto answers the visibility question on the websocket (may this session read the file's metadata row) and a deployment-wired signer from the file crates mints a short-lived URL naming one file, one verb and an expiry. That URL deliberately carries its own permission, and the contradiction with the rule above is resolved by classification. The ticket is not a grant, is never presented at a handshake, and never enters a `Principal`. It is a transport ticket carrying an already-made decision to a channel that cannot ask, its revocation lag is exactly its TTL, and that is the same bounded staleness this system already accepts from decision caching. The signer, its key and the verifying endpoint all live in the file crates ([18-file-handling.md](18-file-handling.md)), so no key material and no token vocabulary enter `connetto-core` beyond the seam trait connetto invokes.

## Bots, lists and installations

A bot is a program that holds a query, is told when it changed, and writes values back under rights a person granted it. It is built from the three things above and from nothing new. The rights are a list the deployment's Postgres holds it to, so the enforcement is a policy the database evaluates rather than a check connetto runs after the fact.

| Entity | What it is |
|---|---|
| Bot | one row in `connetto_bots`, naming the id connetto mints, the owner, the name, the Ed25519 public key, and the current list version |
| Registration code | a one-use bearer stored as a SHA-256 hash in `connetto_registration_codes`, spent to register or to re-register with a changed list |
| Installation | one owner at this deployment the bot serves. A row in `connetto_installations` naming the bot, the owner, and the list version that owner's admin accepted |

A bot is a user. Planned in [R99](../../plans/master-implementation-plan.md#r99-bot-login-and-lifetime), the login, a signature over a server-issued challenge under the registered key. When it connects, the connection's identity is the installation's id for an installation connection and the bot's own id for the bot-level one, bound to `app.user_id`, so `Tier::of`, the ban list, the audit row and every policy treat the connection as any identified caller. The effective list the login resolved is bound as its text to `app.bot_list`, and a caller holding no list binds the `*` wildcard, so Postgres itself holds the caller to its list on every read and write.

The list is a set of table and verb pairs. Each entry names one of the deployment's synced tables and one of read, insert, update or delete, and the optional entries mark the tables an installation need not take. The list travels typed as a `BotList` and is rendered only where a setting binds it, as `table:verb` entries joined by the one separator a policy splits on.

For every synced table the build writes four `AS RESTRICTIVE` policies, one per verb, each admitting only when the caller's `app.bot_list` names that table for that verb or carries the wildcard. A restrictive policy intersects the deployment's own permissive policies, so a bot is admitted to a row only when the deployment's policy admits it and the list names the table for the verb. A person carrying the wildcard is admitted exactly as the deployment's policies admit a person, so the cap changes nothing a person already saw. The file server runs `connetto_visible_files` under the same cap, so a table the list does not name hides its files from the bot.

The registry is five synced tables under row-level security, `connetto_bots`, `connetto_registration_codes`, `connetto_bot_lists`, `connetto_installations` and `connetto_installation_optional_tables`, policy-bearing members of the publication with `REPLICA IDENTITY FULL`. The build appends their DDL to the schema document it translates, so the deployment's Postgres and every replica carry them and the cap reaches them. Clients write the registration codes, the installations and the optional tables through the ordinary sync path, and the server alone writes `connetto_bots` and `connetto_bot_lists`, at registration.

Registration spends one code once. The owner's app mints a base32 code and stores its SHA-256 hash, never the code. The bot presents it as the first frame of a fresh connection with a name, its Ed25519 public key, the list it asks for, and a signature over the connection's nonce under that key. The server checks the list, then the signature, then the code, and only then touches the database, so an outage does not spend the bot's only code. A code that names no bot mints one under its creator at list version 1, and one that names a bot raises the version only when the list differs as a set. Either way the code is spent, and the reply names the bot id and nothing else, a refusal saying only that it was refused.

An installation accepts a version of the list. The admin's app writes a `connetto_installations` row naming the bot, the owner it serves and the list version it accepted, and `connetto_installation_optional_tables` the optional tables it took. The list that installation's session carries is the accepted version's required entries plus the optional entries whose table the installation accepted, which is what lets one bot serve two owners with two different reach.

## The grant list

The handshake carries zero or more grants, opaque to the client. Each is checked independently and yields a subject, or is refused. `Principal` then holds an optional identity plus whichever accepted subjects were capabilities, and what those subjects may do is answered per question by the authorization model rather than resolved at connect time.

The thing that checks a grant is [`connetto_core::HandshakeAuthority`]. It takes one grant and produces a `Subject`, one implementation reads either kind, and the server folds what resolved into one `Principal`. It is not a resolver. `IdentityResolver` ([`connetto_server::IdentityResolver`]) already exists and means something else. It maps the claims a provider asserted to a typed user id in the deployment's own users table, verifying nothing and never seeing a token. `HandshakeAuthority` also signs the resume credential below, because both halves are the server's own signature under one key.

Because a capability is a connetto-signed token like a login token, an unrecognised string costs a signature check and nothing more, and one decode handles both kinds. Which kind of subject a grant names is a claim inside the signed payload, so reading it is reading a checked claim. [02-protocol.md](02-protocol.md#grants) states the same rule, and [11-authentication.md#the-server-verification-seam](11-authentication.md#the-server-verification-seam) reconciles it with the liveness check that survives beside it. A list was chosen over a fixed pair of optional fields, because fixing the arity at two would bolt a third kind of grant onto the message later rather than extend it.

Two logins on one handshake leave the caller unidentified. A run has one identity, and taking whichever grant happened to be checked first would make the order of checks decide the caller. Both are dropped and both are logged, so the outcome fails closed whichever order they arrived in.

### Resolution rule

A grant that fails to check does not end the connection. The handshake succeeds on whatever was accepted, and a caller who presented an expired key beside a valid login is signed in and sees less. An expired key beside a valid login is the ordinary case.

The reply says nothing about the failure. No reason, and not which grant it was. Not allowed, no longer allowed, and never existed are indistinguishable, because a caller who could tell a withdrawn key from a guessed one would hold an oracle over other people's keys. The application cannot tell a user whether to retry or to obtain a new key, only that it is seeing less than it asked for. A login token that expired or whose run was revoked is refused the same silent way, and what tells the client is its own silent refresh failing. No fatal reason names a refused grant, because nothing the handshake learns about a grant can end the connection.

Revocation of a share is quieter still. A capability is withdrawn by deleting the relation that grants it, and the grant check makes no store call, so a revoked key is not refused at all. It resolves, the subject enters the `Principal`, and the rows simply stop matching the policy. The vanishing is active rather than incidental. The change log carries the withdrawn grant row, the server works out which subscriptions its rules reach, and each one is replaced from the server side, so the rows leave the device instead of waiting for the client to re-read. It is also why no wire signal can report revocation. There is no refusal to report.

`HandshakeAck` gains no field about refusals, and neither `session_token` nor `resume_token` says anything about a grant. `session_token` is the run's handle in the clear, and the application has to read it because a synced row written before anybody signed in is attributed to it. `resume_token` is a bearer secret proving that handle is this caller's.

## A caller with no identity

A caller with no identity reads whatever the deployment's policy shows a caller with no identity, and writes when a capability authorizes it and not otherwise.

Its local copy is in memory, always, with no opt-in. `Replica::in_memory()` is SQLite's own `:memory:` and carries no key, because there is nothing at rest to key. There is no third choice.

This is the incognito model, and it dissolves four problems rather than solving them.

- No name, so no naming rule.
- No file, so two callers holding different keys on one shared device cannot read each other's data, and a name-based scheme could not prevent that either because the reading happens locally before any connection opens.
- Nothing at rest, so no key to mint and no purge to sequence.
- Nothing survives a reload, so nothing stale to reconcile.

One guard is in the type. A durable device-private database beside an in-memory replica would be unencrypted, because it inherits the replica's key and an in-memory replica has none. So the replica and that database are described by one value, and the builders that name a file exist only on the encrypted side. The browser worker opens its device-private database as a separate connection, named from the replica's own name (`tier_db_name` in [storage.rs](../../crates/connetto-web/src/storage.rs)), which already carries the identity. An unidentified session gets an in-memory one alongside its in-memory replica, so durable device-private data needs an account. Destroying the replica destroys the tier too, since it shares the key being shredded and would otherwise outlive it.

A session with no identity is online-only. It has no offline resume, because it has no durable identity to resume as and its keys may have changed by the time it returns.

[11-authentication.md#principles](11-authentication.md#principles) says authentication gates sync and never local reads, and that the local copy is readable and writable offline with no valid credential. That holds here within a process lifetime, and what it does not promise is durability across a restart. Durability follows from having an identity, because the copy's name and its key both come from it. The principle is about an identified caller whose credential expired, not about a caller who never had one.

### Signing in

Moving from an unidentified session to an identified one sends any queued writes first and refuses the switch if it cannot. That is the existing unsynced guard applied at the switch, `ConnettoConnection::end_run_for_sign_in` in [lib.rs](../../crates/connetto-client/src/lib.rs), which pushes, refuses with the sequence numbers still queued, and otherwise hands back the outgoing handle.

No adoption primitive exists. Synced rows are discarded and re-snapshotted under the new identity. Queued writes are already sent, because sign-in needs the network, and the device-private database was never inside the replica. It is named on the replica and opened beside it, and an unidentified run's is in memory like everything else it holds.

The switch is not a method that mutates a connection. Signing in changes the local copy from in memory to identity-named, so the caller opens a fresh connection against the identity's own replica. What connetto hands over is the outgoing handle, which is the key the server attributed that run's rows to.

At sign-in the server re-keys that row from the session to the user. connetto surfaces both keys at the switch and performs no merge. Only the application knows which of its tables to re-key and what to do when a row already exists for that user, so connetto makes the merge expressible and stays out of it.

## What each thing keys

The exactly-once identity keys on a server-issued writer number and not on the session or the identity. [11-authentication.md#the-deployments-tables](11-authentication.md#the-deployments-tables) owns the writer table, whose `user_id` is nullable, so an anonymous caller writes under a number with no account at all and a numbered writer can outlive the session that first asked for it.

Routing, flow-control credits, and subscription bookkeeping key on `connection_id` and need neither an identity nor a session.

There is exactly one durable handle per run, and it is a `SessionId`. For an authenticated run the auth store's `SessionId` is the handle. For an unidentified run connetto mints one itself at handshake, so a visit never carries two names, and `Principal::session_id` is non-optional.

An authenticated run needs no answer, because its handle rides inside the login token it presents. An unidentified run has no such token, so connetto hands it a resume credential, a short thing connetto signed under the same key as every grant, and refuses one it did not sign. A caller able to invent a handle could name its own write counter and take over any visit whose handle it obtained, and a signed credential removes both. Stealing one still works, exactly as stealing a browser cookie does, which is the same exposure an authenticated run already has. A registry of minted handles would give the same unforgeability and was rejected, because it costs a lookup on every unidentified handshake and another table a deployment creates. Nothing else is stored for an unidentified run. Its only durable trace is the write-counter row, written lazily on its first write, because the subscription set, the cursors and the buffer are all in process.

The credential expires, because a bearer token no login backs cannot be endless. The bound is configurable and defaults to a fortnight, the life of the case that motivates an unidentified run at all, a shopping cart the visitor comes back to.

One live connection per handle, and the newer connection wins. A handshake presenting a handle that is already live replaces the registry entry, and the old socket is closed with `FatalErrorReason::ConnectionSuperseded`. Two connections must not share a handle, because the handle keys the per-subscription cursor and the pending buffer. Last-wins makes a reconnect racing its own half-dead socket self-heal, at the cost that two deliberately concurrent processes on one stored token evict each other.

The auth store's session record is the authenticated run's own state, holding whether it is live and its retained provider token.

The local replica's name is derived from the identity before any transport opens (`replica_db_name` in [replica.rs](../../crates/connetto-client/src/replica.rs)). Deriving before connecting makes resuming under the wrong identity unrepresentable rather than merely detected. [14-at-rest-encryption.md#how-a-replica-is-named](14-at-rest-encryption.md#how-a-replica-is-named) owns the derivation.

### Public tables may be shared across identities

Because the replica is named from the identity, data visible to everybody is stored once per identity on the same device. For a schema with large shared vocabularies that copy can dominate the replica, so one store per device, holding the public tables a deployment declares, is attached to every identity's replica connection.

Sharing exposes nothing across the boundary encryption draws. The shared store is demand-driven, so its contents show which public rows some identity on the device fetched. Every account under one OS login can already open every other account's replica, as [What at-rest encryption does not cover](#what-at-rest-encryption-does-not-cover) below states, so nothing account B learns is new to B. Against an offline holder of the disk the store is encrypted under a device-scoped key, as each replica is under its own. No signal announces a second identity, since it would warn about exposure the keychain already allows.

Sharing only tables synced in full was rejected as infeasible. A large public table is paged and never held whole on a device.

An identity's reads see rows any identity on the device fetched, which the boundary above already allows, so any table qualifies, a caller-dependent one included. The deployment lists the tables as the last argument of `connetto_schema::emit`, and `connetto_schema::shared_tables` refuses a name the synced schema does not declare while the application compiles. Deriving the list from the schema was rejected because it shares tables nobody chose to share. The bundle carries the validated list outside its version, so a server that names no list accepts the client. A row is kept for as long as some identity holds it. A withdrawal releases only the withdrawn identity's holding, and the row stays while another identity holds it, while that identity's own view still filters it by its own policy.

The build compiles the shared layout into the bundle as four programs, one for each combination of a fresh or existing account file and a fresh or existing shared file. A placed table's backing table lives in `connetto_shared` and its readers are `TEMP` objects every open recreates, so a query joins public and private tables on one connection. The shared file's key is resolved through the same place and key store as the replica's. Server rows for a shared table are replayed into `connetto_shared` inside the transaction of the `main` apply, because the session extension applies to `main` only. A relay serves a late tab's snapshot of a shared table from the store.

A local write to a shared table is recorded by the outbox triggers the identity's own connection holds on the table, in `main`'s outbox beside the private writes, and a push builds one entry of the writing identity's queue from both, with the table sections in the order the transaction first wrote each table. The server applies a mutation in that order and checks references per statement, so a private row may reference a public row written before it. A rejected write is undone in both schemas in one transaction, the shared part replayed under the rollback's rule of leaving any row a later change touched, so another identity's later write to the same row survives. The resync re-apply and an import's restored writes reach the shared store the same way. A relay replays a tab's write as statements in the tab's section order into whichever schema holds each table, refusing it whole on any conflict, which is also how the device-private tier is written.

The store keeps the holdings of its tables in its own holding table, one entry per identity, subscription and row, and what holds a row there follows [15-replica-retention.md](15-replica-retention.md#what-holds-a-row) with the identity added to the holder. A server frame moves the current identity's holdings in the apply's transaction. A delete upstream releases every identity's holding of the row, and a truncate releases every identity's subscription holdings on the table. An identity's unsent writes hold their rows under `connetto-unsent` from the write's own transaction, rewritten when an entry is queued, so a store that refuses either leaves the write unrecorded rather than half-recorded. A pass over a shared table deletes only the rows no identity holds and no unsent write of its own identity keeps, inside a transaction opened `BEGIN IMMEDIATE`, so a write another identity commits first is kept and one after waits for the pass. A resync, a departure or an ended subscription releases only the current identity's holdings, so only an identity's own frames and records judge its rows. The store records which identities use it in `_connetto_identity`, written at open. The store's tables change only with a release, so table DDL on them is refused on every connection.

Forgetting an identity first takes it out of the shared store while its connection is still open. A forced forget undoes the shared writes it discards under the same rollback rule, then the identity's presence and every holding it has in the store go, its unsent writes' included, and every shared row no remaining holding names is evicted, all in one transaction, so a store that refuses any part keeps the identity as it was. The store itself goes once no identity is recorded in it. The key-then-file teardown, the browser's next-boot removal and the failure paths are [14-at-rest-encryption.md#threat-model-boundary](14-at-rest-encryption.md#threat-model-boundary)'s, with the departure that comes first recorded here. A departure that fails is reported as the `shared` part of `ForgetIncomplete`. A logout that keeps the replica keeps its presence and holdings. A revoked device's wipe runs after the client closed on a connection of its own to the replica and the store, undoing the shared writes still queued in the replica first, and a failure there keeps the wipe pending until the next start succeeds.

`ConnettoClient::storage_use` reports what an account's data takes on the device. Its replica's bytes and, when the build shares tables, the shared store's bytes with the number of identities using it, never which ones. Both are page counts, so the read is constant time. A tab's mirror holds no store of its own, so a tab's call asks its worker over the relay, bounded as a share-key change is.

Changing which tables are shared is a schema change, and so a semver-breaking release, whether the list or the bool changes. A replica is never reopened under the other layout within one major, so no rows move between layouts.

A plain bool beside the list controls the store, defaulting to on. A cargo feature was rejected, because features multiply the configurations CI must cover. A const generic was rejected, because it is viral through a connection type that already carries a typed id, and the code it would drop is an attach plus read routing.

### What at-rest encryption does not cover

[14-at-rest-encryption.md](14-at-rest-encryption.md) owns the mechanism, this section owns what it does not reach.

Encryption at rest rests on the operating system's login. Replica keys live in that login's keychain, or in the browser profile's origin storage, so they protect a device's data from whoever lacks the login and do not protect accounts under one login from each other. Separation between different people is the operating system's user account boundary, a per-login keychain on macOS, a per-user credential store on Windows, a per-session kernel keyring on Linux, and separate browser profiles with separate origin storage.

Connetto does not isolate one account from another under a single OS login, and is not trying to. The per-identity replica and its per-identity key exist so that crypto-shredding one account on logout leaves the other readable, which the code states directly in `KeyringKeyStore` in [auth.rs](../../crates/connetto-client/src/auth.rs).

Several accounts can be signed in at once, and switching between them needs no interactive login. A found device can resume any account whose credential is still stored, not only the last one used. Under one OS login, whichever account is running can obtain another account's replica key by asking for it by name, and nothing checks that the caller is that account. A deployment whose accounts under one login belong to different people must separate them at the OS or profile level, since connetto does not.

Gating key release behind user verification was considered and is not needed, since it would answer a question about separating accounts under one login, which connetto leaves to the operating system. User verification is adopted for a different threat. [14-at-rest-encryption.md#the-gate-on-locally-stored-secrets](14-at-rest-encryption.md#the-gate-on-locally-stored-secrets) specifies it as the gate on locally stored secrets against someone holding the device or the browser profile, and WebAuthn's `prf` extension derives the key from a platform authenticator and forces user verification whether or not the caller asks for it, so the browser's gate carries it as a property of the mechanism.

The replica file is named `prefix-sha256(canonical(user_id))` truncated to 128 bits (the hashing step inside `replica_db_name` in [replica.rs](../../crates/connetto-client/src/replica.rs)), unsalted and identical on every device. Anyone with live filesystem access can confirm that a suspected account used this device by hashing a guessed id and testing for the file. The file names alone do not permit enumeration. The account list does. In the browser the plain `AccountStore` index in [auth.rs](../../crates/connetto-web/src/auth.rs) names every account this profile has signed into, plain by design because an account key is an identifier and not a secret. Natively `RefreshTokenStore::accounts()` lists the accounts whose refresh tokens live in the OS keyring, reachable by whoever holds the OS account. The same name correlates one account across devices.

Determinism is the feature here. It is what lets a resumed session find its own replica and its own cursor, and what makes resuming under the wrong identity unrepresentable. Salting would not help against this attacker, who reads the salt too, and neither does encryption under a device-stored key, which the same attacker also reads. Hiding which identities used a device requires a secret the user supplies and the device never stores.

The two leaks pull in opposite directions. Per-identity replicas leak which identities used the device through their filenames. A shared store leaks what was accessed through its contents. Turning sharing off does not buy access-pattern privacy against a local attacker, it only removes one of two disclosures. An application that genuinely needs privacy against someone holding the device needs a user-supplied secret, and that is a different design from either switch position.

## Where authorization happens

This chapter stops at the boundary. [08-authorization.md](08-authorization.md) owns which caller may see or write which row, and the split it records is short. A snapshot is a set-shaped question Postgres RLS answers permanently, a write is a point-shaped question the authorization service answers, and both are compiled from one source by `rls2fga`.
