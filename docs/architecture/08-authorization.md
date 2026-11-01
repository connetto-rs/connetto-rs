# 08: Authorization

This chapter covers which caller may see which row at snapshot time, at change time, on writes and on file access, and what happens when that answer changes. The credential side of the handshake is owned by [11-authentication.md](11-authentication.md), the wire shape of a session, an identity and a capability by [12-identity-session-capability.md](12-identity-session-capability.md), the delivery cost by [17-fan-out.md](17-fan-out.md), and the file content ticket by [18-file-handling.md](18-file-handling.md).

## Principles

1. **Every row delivered has been authorized.** There is no client-trusted filtering.
2. **One policy, one source.** Postgres row-level-security policy text is the source language, and every executor is compiled from that text by the same compiler, so non-divergence is a property of the compiler rather than of either executor.
3. **Authorization is evaluated server-side.** A client never receives rows it may not see, even partially.
4. **Read denials are silent, and silence includes existence.** A row that fails a check is omitted rather than reported, and a deletion the caller could never see must not be forwarded either.
5. **A withdrawal is as much a delivery obligation as a grant.** A caller who loses access must have the rows withdrawn from its local copy.

## The caller

Every check takes a [`connetto_core::auth::Principal`], which carries an optional identity, the capability subjects resolved from the grants the caller presented, and a session handle that is never absent. Every combination of the four reaches the policy, as the table shows.

| Identity | Capabilities | May read | May write |
|---|---|---|---|
| none | none | whatever the policy shows a caller with no identity | nothing |
| none | some | whatever those capabilities grant | where those capabilities grant it |
| some | none | whatever the identity grants | where the identity grants it |
| some | some | the union | the union |

An authorization change does not require a new session, because permissions resolve per question rather than being cached at connect time. The caller carries no tenant, so a deployment that isolates tenants writes the boundary into the policies and connetto answers it as any other visibility question.

### Binding the caller

Every path that runs SQL as a caller goes through one shared binding, `CallerBinding` in [capability.rs](../../crates/connetto-server/src/capability.rs), because the snapshot and the change path are different executors and a caller bound in one but not the other would see a shared row once and never see it update. The binding sets three Postgres settings, each named by the deployment.

- The identity, under `app.user_id` by default. A policy that names its own identity setting sees nothing when the binding is left at the default, because connetto would set a name the policy never reads.
- The share keys, packed by the deployment's own key type, under `app.subjects` by default, the key type naming both the setting and the separator the policy unpacks.
- The caller's bot list, under `app.bot_list`, with a wildcard value for a caller holding no login, so the ticket and the connection hold the caller to one list.

Every caller binds every setting, and a half it does not hold takes an unguessable marker minted once per process, by [`connetto_core::auth::absent_marker`], which no row can carry, so an owner comparison against it is false and the row is hidden. An empty string would not do, because it is a real identity that happens to be blank.

Leaving the setting unbound would not do either. Once any transaction binds a Postgres custom setting, `current_setting(name, true)` answers the empty string for the rest of the session and no `RESET` or `DISCARD ALL` clears it, so on a pooled connection an unbound half reads as the blank identity to the next caller. Absence is therefore a value no caller can be, rather than `NULL`.

## Policy source and executors

Postgres row-level-security policy text is the source of truth. [`rls2fga`](https://github.com/connetto-rs/rls2fga) parses it, classifies each `USING` and `WITH CHECK` expression into its canonical patterns, and generates an OpenFGA authorization model plus the SQL that populates the relationship records. At runtime, point-shaped visibility questions go to OpenFGA rather than to direct SQL evaluation or an in-process compilation.

The executors split by the shape of the question.

| Path | Question shape | Executor |
|---|---|---|
| Snapshot | set filter, one query, many rows | Postgres row-level security, inline in the query |
| Change delivery | point check, twice per row per subscriber | OpenFGA |
| Write | point check, once per operation | OpenFGA |

The change and write executors are [`connetto_server::openfga::FgaAuth`], which composes subql's `RowPolicy` over `OpenFgaPolicy` behind one shared index built at startup, so the boot guard and the change path cannot hold two opinions about the same policies. The visibility trait `subql::visibility::VisibilityPolicy` is defined in subql, which holds the replication connection. It is consulted at the change dispatch [`connetto_server::session::SessionManager::dispatch_event`], the catchup replay `catch_up_row` in [session.rs](../../crates/connetto-server/src/session.rs), the write loop `handle_mutation` in the same file, and the mint [`connetto_server::capability::CapabilityIssuer::issue`]. All four ask one question naming the watchers, and a failure denies one watcher rather than the event.

### Why the snapshot stays on row-level security

A snapshot is a set filter, and row-level security answers it in one round trip using the planner's own indexes with no result cap. OpenFGA's enumeration direction cannot stand in, because `listObjectsMaxResults` defaults to 1000 and `listObjectsDeadline` to 3 seconds, and a truncated snapshot would be silent data loss rather than an error. The split is deliberate rather than interim.

### Why two executors is safe

It is safe only because `rls2fga` compiles both executors from one source. A policy the translator leaves partially unhandled **narrows** rather than widens, so a dropped permissive clause grants nothing and a dropped restrictive clause becomes no access. Rows **vanish** rather than leak, and the startup refusal stops a deployment from discovering that by watching data disappear. A policy that compiles to no access at all has nothing to diverge on, every row is hidden on both executors, and no note names it. That is the gap [G11](../../plans/master-implementation-plan.md#g11).

Two upstream invariants guard the same seam. An exclusion that `rls2fga` emits never subtracts something derived from the object's own row, and the catchup reasoning depends on it. And a translated attribute condition earns a wildcard subject only when the value it compares against is a literal, which is the one way a compiled policy can widen rather than narrow.

### The startup refusals

A policy the translator cannot read stops the server at boot, on a note whose severity is `diverges_from_database` rather than on the narrower unhandled report. A policy reading a table the publication does not carry also stops it, through [`connetto_server::preflight::Artifact::PublishedTable`], because the change-path executor learns about permission changes only from the replication stream and a stale policy would answer confidently and wrongly. `rls2fga` reports the complete set of tables each policy reads, so this is a set comparison that can name the missing table. A third refusal belongs to the replica half of the translation and is stated below.

## The third executor

The client's local SQLite replica carries the policy too, translated from the same Postgres policy text by `pg2sqlite`, which turns each policy-bearing table into a backing table, a view carrying the logical name, triggers, an audit table and a violations view. The two-executors argument extends to a third, since `pg2sqlite` compiles from the same policy text, though it widens the surface on which the executors can disagree.

Triggers in a SQLite file protect nothing from whoever owns the file, and [14-at-rest-encryption.md](14-at-rest-encryption.md#what-is-encrypted) already concedes the unlocked machine. What they buy is three things. A write the server would reject is refused locally before it is queued. A row that reached the replica through a server-side defect does not surface to the application. And the same query returns the same rows locally and remotely. That last promise has an untested seam, because no test runs one policy text through both translators, and [G8](../../plans/master-implementation-plan.md#g8) names the missing test.

The replica's policy is fail-closed on writes. The translation emits check triggers on the backing table that admit a write only while a SQLite function it names returns true, and connetto registers that function, [`connetto_core::schema::WRITE_EXEMPTION_FUNCTION`], on every replica connection and holds it true exactly while it applies the server's patches. The application's writes always see it false, so the policy judges them. Server-authored rows go to the backing table, under the triggers, because the server's patches name the plain table and applying one to the view fails every row as a constraint conflict. The backing-table mechanics and the retention pass are in [03-sync-pipeline.md](03-sync-pipeline.md#3-authorization-filter) and [15-replica-retention.md](15-replica-retention.md).

One build-time coupling follows. A policy naming the caller cannot translate until the deployment says what the caller means locally, so the translation refuses with `SessionVariableMappingNotFound`, naming the pattern and pointing at the option that supplies it, rather than emitting a trigger that silently evaluates the caller as nothing and admits every row. The translation always emits an audit table per policy-bearing table and its name must be supplied through `RlsAuditTableNameRequired`. No policy is rejected for being too complex, because the row-level-security translator raises exactly those two errors, and the deployment's schema build [`connetto_schema::translate`] in [lib.rs](../../crates/connetto-schema/src/lib.rs) supplies both options.

A policy scoped `TO` a named role is refused by this translation at startup, because connetto supplies no session role mapping to it. [`connetto_server::openfga::Translated::of`] alone accepts the same policy, so the refusal comes after the model and its facts are already written to the authorization service, and no test holds the refusal itself. Groups belong in tables, and the grant-change machinery below follows that pattern. [G7](../../plans/master-implementation-plan.md#g7).

## Read authorization

### At snapshot time

The snapshot query runs on a pool whose role is subject to row-level security, with the caller bound, so only rows that pass the policy enter the snapshot. Postgres does not apply policies to a superuser or to the table owner, and the reference binary refuses to start without a separate reader connection, so snapshots never run on the owner pool.

The facts behind the authorization store are read on the owner pool instead, because the reader role sees too little for them. The boot load [`connetto_server::openfga::Translated::load_into`], the boot reconcile [`connetto_server::openfga::Translated::reconcile_store`], and the change path's re-runs each take a [`connetto_server::openfga::FactSource`], named for the owner pool it wraps, because a membership table's own read policy hides every member but the caller, so reader-role reads would be lost from the store.

The server also refuses to start when row-level security would filter the owner pool. Postgres applies a table's policies to its owner under `FORCE ROW LEVEL SECURITY`, so an owner pool connected that way would read the facts, the move-out withdrawals and the bot registry short and lose rows without an error. The preflight artifact `ReadInFull` asks Postgres's own `row_security_active` for every table the policies read and every synced table, as the owner pool's role, and a filtered table refuses the boot naming itself.

### At change time

For each candidate subscription, authorization is checked against both versions of the row, and the pair decides what is delivered.

| Previous visible | Current visible | Delivered |
|---|---|---|
| no | no | nothing |
| no | yes | insert, the row became visible |
| yes | no | delete, the row became invisible |
| yes | yes | update, or insert plus delete if the row identity changed |

The second check is conditional. The previous version only matters where the current version is invisible, so the order is check the current version, deliver and stop if it is visible, and only then check the previous one. A deletion has no current version, so it always checks the previous one, which is exactly what filters a tombstone, and an invisible insertion delivers nothing. The cost is one check per subscriber plus one more for each subscriber who cannot see the current version.

`subql::visibility::transition::transitions` writes `Nothing`, `Deliver` or `Withdraw` per watcher, with `Withdraw` the default and the buffer pre-filled with it, so a call that fails partway leaves every watcher it did not reach withdrawing rather than leaking.

A withdrawal is a plain unmarked delete carrying the key alone, never the event's own patch. The caller cannot tell a withdrawal from a deletion. A frame that said which would disclose that the row still exists for somebody else, and a delete's own patch is already that shape, so only an update pays for a second fold.

Row-level security cannot supply the previous-version answer, because the answerer [`connetto_server::auth::RlsAuth`] answers `may_see` by running `SELECT EXISTS` against the live table and for a deletion answers false for everyone. That is why the change path runs on OpenFGA, which can answer about the row as it was.

The previous version comes from the change log, which requires `REPLICA IDENTITY FULL` on the tables the publication carries, because `DEFAULT` records only the primary key columns and nothing at all when a table has no primary key. The preflight artifact `PreviousImages` checks it at startup and names every table to fix, scoped to the publication rather than the database, because connetto keeps its own bookkeeping tables in the same database and replicates none of them.

A table altered after startup refuses to serve. The condition is permanent for that table, so the change path does not hold the event and retry. `SessionError::ChangeStreamUnusable` escapes the reconnect loop, every connection is closed and the process exits, and the restart meets the startup refusal naming the table.

Reconnect catchup re-answers per client from connetto's own oplog rather than from the change stream, and the oplog stores the whole change event, so the previous image survives the round trip.

### The two-check form does not handle a policy change

The table above covers data changes, and the section below covers authorization changes. With no data change there is no change event on which to run either check.

## When an authorization change arrives

### A rules change

The policy text itself is altered, `rls2fga` re-translates, and the model is replaced. This is a deploy-time event. Every active session is invalidated and clients re-subscribe from scratch.

### A grant change

A row appears or disappears that grants somebody access under unchanged rules. This is the runtime case, and what notices it is the Postgres change log connetto already reads, because a grant is a Postgres row. OpenFGA is never a notice source, because a permission existing only in OpenFGA would make it a second source of truth.

What happens is a per-subscription resync, and never a synthesized deletion. The store upkeep [`connetto_server::openfga::StoreUpkeep`] sees every authorization-bearing row before it is dispatched, applies what it moved, and reports the move, the tables whose read answer depends on the moved fact, and who the fact named. `announce_grant_moves` in [session.rs](../../crates/connetto-server/src/session.rs) matches that against the live routes, each of which records the table its subscription reads, and drops an instruction on the owning session's own queue. That session's task, the only holder of the transport, re-reads the subscription and sends `FullResyncRequired` behind the replacement, in that order, because the resync notice is what makes the client discard the rows it holds.

Manufacturing a tombstone per affected row loses, because the grant row names the grantee but not the objects, so finding them means the capped enumeration direction, and a truncated withdrawal does not announce itself. Resync avoids the question, because a replacement is complete by construction where a diff is not.

A key-shaped grantee is matched against the keys a session holds. A share grants to a key rather than to a person, and the translation renders it as a wildcard tuple whose condition context carries the key, so the fact that moved names one bearer. The upkeep compares that key against the ones each live caller presented at its handshake, and the replacement reaches the bearer's session and no other.

Table-backed groups work through the same machinery. [`connetto_server::reach::GrantReach`] walks the generated rules once at startup and inverts them, so per kind of fact it holds the tables whose read answer depends on it. Only the generated rules connect the membership row that moved to the rows the caller loses. A rule shape the walk does not recognize refuses the boot, because widening would turn a hole in the teardown into extra traffic rather than a startup failure.

The move is read against the batch that carries it. The added and the withdrawn halves of every report feed one key map before any holder is read, because one grant travels as two facts and either can carry the key. A fact whose subject names a model object rather than a caller is resolved against the keys its own batch named, and when none did the fact keeps its wide reading. `a_withdrawn_grant_takes_the_rows_off_the_device` in [revocation.rs](../../crates/connetto-client/tests/it/revocation.rs) withdraws one member's grant and reads the rows back off the real client's replica, while a second member of the same team receives nothing and keeps its copy.

### The promise a deployment can rely on

An authorization change takes effect immediately for writes, within the read cache lifetime for reads, and immediately for both when it triggers a teardown. Writes use the strict consistency preference, because they are low volume and a write accepted against a just-withdrawn capability is exactly the moment a leaked key is being revoked. Reads on the change path keep the fast preference and its caches, because that is what makes the fan-out affordable at all, so a caller who has lost access may see at most one further patch within the lifetime.

OpenFGA ships with its check caches disabled, so a read reflects a withdrawal as soon as the store holds it. Enabling `OPENFGA_CHECK_QUERY_CACHE_ENABLED` puts a lifetime between the two, and `OPENFGA_CHECK_QUERY_CACHE_TTL`, which defaults to 10 seconds, sets how long it lasts. Two constraints bound how precise the promise can be. The preference is per request rather than per item, so a strict question cannot travel in the same batch as cached ones, and there is no way to express that one read must reflect one specific write.

A session revoked through [`connetto_server::authn::AuthService::revoke`] closes its connection with [`connetto_core::messages::error::FatalErrorReason::SessionRevoked`] and records the ending, so a connection the revocation did not reach is closed by the hook the builder installs.

The store must accept a duplicate write, which sets a minimum OpenFGA version. A row-settled fact is applied straight from the change stream without reading the store first, so a row whose fact is re-derived is written again. Every write the upkeep sends carries `on_duplicate` set to `ignore`, and a server predating that field drops it silently, because protobuf ignores unknown fields, and then refuses the duplicate with its own error. That refusal clears by itself, so the event is held and retried and the deliveries behind it pause until the version is fixed.

## Write authorization

The write question goes through the same trait as the read. Two callers ask it, the per-operation loop of the mutation handler once per operation and the mint once per verb a share certifies. [`connetto_server::openfga::FgaAuth`] implements it by delegating to `RowPolicy`, which answers locally from the row's own values wherever the schema decides, and asks OpenFGA for the rest.

Separately from the trait, a mutation applied through the write target runs inside a transaction that binds the caller first, so Postgres row-level security gates the write itself. A policy's `USING` clause doubles as `WITH CHECK`, an insert or update violating it is a hard error, and an update or delete of an invisible row shows up as a zero-row shortfall. That is a second, independent enforcement point.

The builder may construct the row-level-security answerer as an optional second opinion behind the write target, and it answers the write question for the two verbs a share can certify, `update` and `delete`. Both carry an existing row, which is what makes them answerable. Postgres applies a table's update rule to a locking read, so a `SELECT` with `FOR UPDATE` inside the caller-bound transaction returns the row when the caller may change it and nothing when it may not. The transaction commits at once, releasing the lock, and bounds its wait first, because a mint holds a pooled connection while it waits and an unbounded wait would let one stuck writer exhaust the pool.

The delete verb refuses rather than borrowing the update verb's answer where a table writes a rule for a single command. A locking read speaks for the update rule alone, so it answers a delete only when one rule governs every command, which is what connetto's own translation generates. A table carrying an update rule and no delete rule permits no delete at all while the locking read still says yes. So the answerer asks `pg_policies` whether the table writes any command-specific rule and refuses when it does, per question rather than cached, because a cached answer goes stale exactly when a deployment tightens a rule.

Two consequences a deployment can see. The pool the second opinion holds must carry `UPDATE` on the tables it certifies writes over, because Postgres refuses a locking read to a role holding only `SELECT`, and every shipped `roles.sql` already grants it. And a question that cannot be answered refuses the mint through its own reason [`connetto_server::capability::ShareError::WriteUndecidable`], rather than `NotWritable`, because telling an operator a row is unwritable when the truth is that nothing knew names the wrong cause.

## Capabilities

A capability is a connetto-signed token asserting one thing, that the bearer is a named subject, for example `key:abc123`. It says nothing about what that subject may do. The permission is a relation on that subject, derived from a Postgres row the application owns, exactly like every other permission. The login token authenticates `user:alice`, the capability authenticates `key:abc123`, and this chapter answers what each may do in exactly the same way, which is why authorization needs no capability-specific path. The wire shape, the grant list and what the reply says about a refusal are in [12-identity-session-capability.md](12-identity-session-capability.md#capability).

A permission carried in the token would split authorization between the token's contents and the model, the divergence the single policy source exists to prevent. Withdrawal therefore needs nothing new. Revoking a capability is deleting the relation, which is a Postgres row change, which is the notice the grant-change path above already watches for. The token stays cryptographically valid and names a subject with no relations left, so no liveness table is needed, and the capability's expiry is a second bound beside withdrawal.

Minting is a library call and this chapter authorizes it. Creating a capability over a resource is itself an action needing authorization, because a caller must not share what it cannot read. The question is about a row and the caller names only its key, so the mint reads the row as the caller, through a row source, before asking, and hands the values over as the view. Values supplied by the caller are refused outright, because a caller that can state the row's contents can get an answer about a row that does not exist. And the read runs as the caller rather than through a role that sees everything, because otherwise a hidden row and an absent row would answer in different times, turning minting into a probe for rows.

A share names the write verbs it certifies, `update` and `delete`, and reading is checked separately for every share. The caller names which of the two verbs the permission row it is about to write will grant, and the mint asks the write question once per named verb, about the row as it stands, refusing the whole share if any is denied. [`connetto_server::capability::ShareLevel`] is that set, `ShareError::NotWritable` is the refusal, and [`connetto_server::capability::IssuedCapability::level`] reports back exactly what was certified. The application is the only party that knows what its own relation means, which is why it names the verbs rather than connetto inferring them.

The level does not enter the token. Two shares of different levels are indistinguishable to the server, and what makes the level binding is the deployment's own `WITH CHECK` on its sharing table, which is the same policy source deciding both halves. Connetto checks the caller may read the resource the mint call names, but the application writes the permission row itself, on its own connection, so what stops that row naming a different resource is the `WITH CHECK` requiring the shared row to be visible to the sharer.

## File authorization

File metadata is authorized as an ordinary row on the files table, by the same policy as every other row.

The content path binds the caller the same way the data path does, at four places. The ticket mint, where the session checks the file's visibility on the reader pool before it mints anything, and the file server's serve recheck, its needed-hashes answer and its commit dedup check. In each, the caller is bound under the names the deployment's policies read, by [`connetto_file_server::CallerSettings`] and `bind_caller` in [caller.rs](../../crates/connetto-file-server/src/caller.rs), so a caller whose rights come from a share key is answered on its keys and a caller with a login on its login. The ticket carries the caller, so the checks after the mint run on what the ticket says rather than on what the connection says. The ticket model itself is in [18-file-handling.md](18-file-handling.md#the-wire-tickets-and-the-mint).

A ticket outlives the session that minted it for its own lifetime, so a revoked or ended session can still use one until it expires, and the ticket's lifetime is what bounds that. [G5](../../plans/master-implementation-plan.md#g5).

## When the authorization service is unreachable

No patch is delivered and no mutation is accepted while the answer is unknown, the client is told delivery is paused and told why, a fresh connection still takes a snapshot, and delivery resumes on the same connection when the service comes back. The cause the client is told distinguishes the authorization outage from a stalled change stream, by [`connetto_core::messages::PauseCause::AuthServiceUnreachable`] on [`connetto_core::messages::ControlMessage::DeliveryPaused`], because a patch delivered to a caller who may not be allowed to see it cannot be recalled, whereas a stall can be recovered from.

A refused write must not be reported as unauthorized. Rejecting it that way says the caller lacks permission when the truth is that the server cannot tell, and a client that believes itself unauthorized stops retrying and may discard the mutation, turning a transient outage into permanent data loss. [`connetto_core::messages::MutationRejectReason::Indeterminate`] is the answer instead. A client that receives it must retry rather than retire its pending record, and every later write on the connection is answered the same way until the write settles.

Snapshots keep working throughout, because they run on Postgres row-level security by design, so an outage stops live delivery and writes while a fresh connection can still read.

## Audit

High-volume operational events, denials, connection events and the per-row visibility questions, go to structured logging on stdout, and the aggregator is a deployment choice. State changes that matter, permission changes, session invalidations and model changes, are persisted to the `auth_events` table. A rejected grant is a denial and therefore goes to the log, because the wire says nothing about a failed grant and the log line is the only place it is visible.

A refused subscription goes the same way. Every refusal carries the one fixed detail [`connetto_core::messages::error::SUBSCRIPTION_REFUSED`], on the server and through the relay, and no frame precedes it, because a frame sequence that varied with the cause would disclose what the fixed text stops disclosing. The cause is logged at `warn` inside the connection context.

The table is a deployment-facing schema contract. [`connetto_server::audit::ConnettoAuditSchema`] sits beside the store and writer schema contracts, and the `Audit` member of the deployment's [`connetto_server::schema::ConnettoSchema`] names it. The server refuses to start with the audit turned on and the table the member names absent, and the insert is built as a real statement against the application's own declaration, so the compiler settles whether the column types agree.

The event kinds form a closed set, a Postgres enum on the deployment's side and a Rust enum [`connetto_server::audit::AuthOp`] on connetto's, so a value outside the set cannot be written by either end. The set covers session life, authorization, abuse (the ban and its lifting, in [11-authentication.md](11-authentication.md)), the writer frontier's two non-error outcomes, and a person's data requests, the download and the erasure, which are the two rows on which the session column may be empty. Staff answering a request with no session behind them leave the session empty and name themselves in `acted_by`.

`user_id` is the deployment's own identity type and nullable, because a caller may have no identity. `table_name` and `pk` name the shared row for a mint and the grant row that moved for a permission change, with `pk` holding the key as the application's [`connetto_server::audit::ConnettoAuditSchema::row_key`] reads it, so it is readable and joinable. A mint's row names the key it issued in `subject`, which is the string the application's grant row names.

Recording is always on. Ending a person's sessions answers the share keys those sessions minted, read back from their `capability_minted` rows, so a deployment without the table could miss an intruder's keys without anyone noticing. The reference binary has no switch for it and the mint takes its audit hook, so no mint goes unrecorded.

The sink is off the synchronous hot path. The hook fires synchronously and the supplied implementation only queues the event, so a slow or failing sink never delays the caller and never fails the logout, revocation or mint that produced it. One writer task drains the queue, so the rows land in the order the server emitted them, and an insert that fails is logged and dropped. The two data-request rows are the exception. They are written and awaited rather than spawned, and a request whose row cannot be written fails, because the row exists for disputes.

## Cost on the change path

Round trips per event are bounded by the watchers the row does not settle locally, divided by the batch cap, and do not grow with total subscriber count where the schema decides the relation.

`RowPolicy` in subql settles a question locally from the row's own values when it is a direct identity comparison or a held-key comparison, and the rest go to `OpenFgaPolicy`, which packs one question per watcher into `BatchCheck` calls capped at fifty per batch. The change path derives which subjects a row grants from the row itself before it asks, through `records_from_row`, so the answer about the row as it stands never leaves the process.

Every policy connetto writes falls entirely in the local half. A connetto table carries one permissive policy whose `USING` is the caller's identity `OR` the keys the caller holds, and the translation classifies the identity arm as a direct relation and the held-key arm as a request gate, both answered by `RowPolicy` locally, so connetto's own policy shape costs zero round trips at any audience size. A policy that reads another table is the linear case.

A shape bounded by the row rather than by the audience is ruled out, because `ListUsers` truncation is server-side with no page token, so a truncated answer reads as a wrong refusal, and `Expand` detects truncation but returns only directly stored tuples without evaluating the model, so it omits members without knowing it.

A counter test proves the bound. The questions the change path asks pass through [`connetto_server::openfga::Counted`], and [fanout_counters.rs](../../crates/connetto-test-harness/tests/it/fanout_counters.rs) asserts how many round trips one change event costs as the subscriber count grows. The full counter table is in the plan's [measurement section](https://github.com/connetto-rs/connetto-rs/blob/82983dff19effa9cfb521370a7794d29e51afb51/plans/master-implementation-plan.md#r0-the-measurement), and the per-subscriber floor is in [17-fan-out.md](17-fan-out.md#the-unit-of-computation).

Precomputing a materialised permission set is out of the design, because OpenFGA does not offer it and the local tier is already zero-cost for connetto's own policies. If a cross-table policy ever shows a problem in the counters, the smallest correct addition is a local negative filter consulted before calling out, safe in exactly one direction because a probabilistic set has false positives and no false negatives.

For connetto's own policy shape `RowPolicy` answers every watcher locally, so the per-row answer stops depending on the audience and sharing pays. The floor is in [17-fan-out.md](17-fan-out.md#the-unit-of-computation).
