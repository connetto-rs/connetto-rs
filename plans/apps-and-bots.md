# Apps, installations and bots

Design record. Rewritten 2026-09-18 evening from the discussion of that day, which settled the model in three rounds, then reviewed the same night and corrected where the review found contradictions. Every statement carries one of three markers. **Settled (date)** was agreed with the maintainer on that day. **Follows** is a consequence of settled points with no fork left, stated so it is not re-derived. **Open** stops the build of the part it governs until discussed. Superseded shapes are listed at the end so they are not re-derived either. The master plan outranks this record on phases and their status.

## The entities

**Settled (2026-09-18).** Three entities, and the analogy that holds for each.

| Entity | What it is | Analogy |
|---|---|---|
| App | code, one private key, a manifest. A row in `connetto_apps` at every deployment where it is registered | a GitHub App |
| Bot | the app's identity at one deployment. One user, `app.user_id` in every session the app opens there | `dependabot[bot]`, one actor per GitHub |
| Installation | one resource owner at that deployment granting the bot a view of what they own. A row in `connetto_installations` plus the deployment's own sharing rows | a GitHub App installation on an account or organization |

A bot is per deployment so that lab A's grant to app X and lab B's grant to app X are two bots that share a private key and nothing else. What A granted is invisible from B by construction, not by the vendor's discipline.

**Settled (2026-09-18).** An installer is whatever the deployment's owner column names. A person in the demos. An organization in a deployment that models one, written by a person its policy allows to act for it. connetto has no organization of its own and defines no membership, the deployment's policy on `connetto_installations` and on its sharing table decides who may install for whom. Request-to-install and admin-with-limits flows, which GitHub has, are that policy too.

**Settled (2026-09-18).** Install and authorize are two grants, as on GitHub. Install gives the bot access to resources. Authorize would give an app the right to act as a person, which is the UI-app case and is not designed here.

## What a bot is

**Settled (2026-09-18).** A bot is a user. Its session binds `app.user_id` to the bot identity like a person's. Whether it draws a screen is orthogonal. There is no bot crate, the runtime is `connetto-client`'s, the credential is an app credential in `connetto-client::auth`, the reaction primitive is a trait in `connetto-client`, and the deliverable a bot author copies is a template.

**Settled (2026-09-18).** Authorship and ownership are different things. Who acted is the bot's identity, in the audit row of every action, with the session naming which login. Who owns a row is the row's owner column, which the write names and the policy admits, so a bot may write a row the installer owns. Whether it may is the deployment's `WITH CHECK`, through the same sharing arm a deployment writes to let one person edit another's rows. connetto stores no second author on a row. A deployment that wants `created_by` adds the column and the bot fills it.

**Settled (2026-09-18).** An app is an app. Who registered it, who runs it and where the key sits change nothing at runtime.

## Shared view and isolated view

**Settled (2026-09-18).** Inside one deployment a bot may log in once for the whole deployment, the shared view, or once per installation, the isolated view, and both are available at every deployment. The share row chooses. Mechanically, a bot session binds the bot user as its identity (`app.user_id`) and, for an isolated login, the installation's id as its capability subject (`app.subjects`). A share row naming the bot user grants every login of the bot, a share row naming the installation id grants that installation's login and no other.

| The share row names | Who sees the rows | Login shape | Analogy |
|---|---|---|---|
| the bot user | every session of the bot at this deployment, the union of every installer's grant | one login per bot per deployment | a Slack bot user, one per workspace |
| the installation id | only the session that logged in for that installation | one login per installation | a GitHub installation access token |

A cross-user bot, one that matches orders across installers or dedupes photos lab-wide, needs the shared view and gets it from share rows naming the bot user. A per-user bot, the importer or the totaliser, is safer under the isolated view because the session it holds for Alice was never able to see Bob, so a bug cannot write one owner's data onto another's rows, the server refuses it. In neither view does a bot see an owner's rows without that owner's own share row. The view decides only whether grants the bot already received land in one session or in one per installation. Both are one policy language, chapter 08's four-row table already covers identity, key, both and neither, and `CallerBinding` renders both settings today.

**Settled (2026-09-18, review).** The isolated subject is the installation id itself, not a minted key. A bot proves it may act for installation I by an assertion signed with the app's key naming I, and the exchange checks I is an installation of that app, so no bearer secret is minted at install and `MintCapabilityKey::mint` is not involved. **Follows.** connetto renders that subject into `app.subjects` as `inst:<id>`, its own prefix, since the default share key renders as `key:<uuid>` (`capability.rs:48-51`) and a deployment's key type may render otherwise, and an installer who chooses their installation id must never be able to spell a share key with it. Share rows and policies name the `inst:` shape. The installer knows the id because they inserted the row. Since 2026-09-20 both settings are always bound and a half the caller does not hold takes `connetto_core::auth::absent_marker` (chapter 08, corrected the same day), so a shared login binds the marker under `app.subjects` and an isolated login binds `inst:<id>`.

**Follows.** The share table's policy arm for the isolated view compares against `app.subjects`, the P18 `EXISTS` form chapter 12 line 109 gives, and the share table is the deployment's and already policy-bearing. No connetto table enters any policy.

**Follows.** A bot may also hold ordinary share keys a person handed it. Nothing forbids it and the manifest filter applies to the union.

**Follows.** Share rows naming an installation id are key-shaped grants to R7. Since `8735e8d` (2026-09-20, PR #42) a moved grant is announced to the subject it names, so an uninstall under the isolated view resyncs that installation's session and no other. The `Everybody` fan-out the first draft feared is gone.

## The registry

**Settled (2026-09-18, second review).** Three connetto-owned synced tables carrying only what clients write, and two server-only tables carrying what the server writes. The synced three are ordinary policy-bearing tables in the publication with `REPLICA IDENTITY FULL`. The server-only two are not published, no client holds them, and the exchange writes them on the privileged connection the server already holds for slot and publication work. No column privileges exist. `owner` and `installer` are immutable because the policy's `WITH CHECK` half refuses an update that names anyone else, and `key_generation` is set by the trigger, which overwrites whatever a client sent.

| Table | Columns | Default policy in the registry DDL |
|---|---|---|
| `connetto_apps` (synced) | id, owner, public key, key generation | owner equals `app.user_id` for every command. The deployment widens `SELECT` for a catalogue |
| `connetto_app_grants` (synced) | app, owner, table name, verb, accepted | owner equals `app.user_id` for every command, settled from the row itself. `owner` repeats the app's, with a foreign key on `(app, owner)` to `connetto_apps (id, owner)` so it cannot lie, for the reason the demo schema gives on `order_lines` (`schema.sql:21-23`, a policy that reads through the parent costs the change path a round trip). Verb is a `CHECK` over read and write |
| `connetto_installations` (synced) | id, app, installer | installer equals `app.user_id` for every command. `SELECT` also admits the app's bot user for its app's rows, through `connetto_app_bots` |
| `connetto_app_bots` (server-only) | app, bot user id | none, unpublished, written once at the bot's first login |
| `connetto_installation_sessions` (server-only) | installation, session id | none, unpublished, written at an isolated login |

**Follows.** The bot's `SELECT` arm on `connetto_installations` joins an unpublished table, so a change to `connetto_app_bots` never reaches the change path. That is tolerable only because the row is written once, at the bot's first login, before any subscription of that bot exists. Verified 2026-09-22, R91 step 1: all three registry policies as drafted grade at or above the confidence `Translated::of` boots with, the bot arm and the grant-row arm included, so nothing is rewritten. The guard lives at `crates/connetto-server/tests/it/registry_grading.rs` against a later rls2fga tightening.

**Settled (2026-09-18).** Who may publish, install and see which apps are those policies and nothing else. No config switch. A curated deployment writes the `INSERT` policy on `connetto_apps` that says so, a deployment with organizations replaces the installer policy with its membership arm. An operator who wants an app disabled at their deployment bans its bot user, and `refuse_if_banned` refuses every login of the app with no new code.

**Settled (2026-09-18, review).** Publishing and installing are plain inserts the policy admits, and nothing is minted at insert. The one route is the login exchange. **Follows.** The bot user is resolved at the bot's first login through the deployment's `IdentityResolver` (`crates/connetto-server/src/authn/identity.rs`) with `AuthConfig::issuer` as the issuer and the app id as the subject, and written into `connetto_app_bots` by the exchange. `DefaultUuidResolver` is deterministic in issuer and subject (`identity.rs:97`), a deployment resolver tells bots by issuer, must accept an identity with no email or name, and a resolver refusal refuses the login rather than the publish. The bot user row is the deployment's and is never deleted by connetto. Until first login no `connetto_app_bots` row exists and the `SELECT` arm for the bot matches nothing.

**Settled (2026-09-18).** The manifest is the app's rows in `connetto_app_grants`, one per table and verb, tables and verbs only, never columns. A row exists because the developer declared it and is effective when `accepted` is true, which the owner sets when the deployment accepts that grant at registration. **Follows.** The one check connetto adds, the named table exists in the catalog, runs at mutation inspection on inserts to `connetto_app_grants` and refuses the write the way an unauthorized op is refused. A narrowing is a delete and bites at once, a widening is an insert with `accepted` false and changes nothing until accepted. A catalogue asking which apps want to write a table is a plain `WHERE`. Columns are the policy's business, a manifest naming them would be a second authorization language beside the policies. Verbs are read and write only. Capability minting is `CapabilityIssuer`, a library type the application's handler wraps (`capability.rs:392`) with no route of connetto's, so a manifest verb for it would promise an enforcement with no site, and a deployment that lets bots mint reads the grant rows in its own handler. The three synced registry tables are readable by a bot within their policies and exempt from the filter, since no app declares them and the bot's own installations query must run.

**Follows.** An install is the installer's insert of the installation row, whose id they generate, followed by their share rows naming `inst:<id>` or the bot user, all under their own session. A retry after a lost response finds the row present and is refused as a duplicate, one installation per app per installer. A reinstall after an uninstall is a new id, and the share rows are new rows, which the cascade guidance below already forces.

**Settled (2026-09-18).** One route on a library function `app_router(registry)` beside `auth_router`, backed by a library type `AppRegistry`, mounted by the reference binary on the auth listener when a registry is configured. `POST /auth/app-token`, the login. A downstream mounts the router or calls `AppRegistry` from its own handler. Chapter 12's routing-stays-with-the-application argument holds for share minting, whose request shape is the application's, and yields here where the shape is fixed.

## Login

**Settled (2026-09-18).** A bot logs in to a deployment. The login is the assertion exchange at `POST /auth/app-token`, and what it produces is a session the bot keeps across restarts, stored beside its replica the way a device keeps its refresh token.

**Settled (prompt, amended 2026-09-18).** The assertion is an EdDSA JWT signed by the app's private key. `iss` the app id. `sub` the app id for a shared-view login or an installation id for an isolated login, which is RFC 7523's client-authentication shape. `aud` the deployment's `AuthConfig::issuer`, so one key registered at two deployments cannot have an assertion for one accepted at the other. `sid` the session to resume, absent on first login. `jti`, `iat` and a short `exp`. The private key never crosses the wire. The route verifies the signature against `connetto_apps.public_key`, checks an isolated `sub` names an installation of that app, refuses while the app's owner or the installation's installer is banned, refuses a `jti` seen within the `exp` window so a replayed assertion without `sid` cannot open a session per replay, and mints an access grant. **Follows.** The grant is `GrantClaims::User` for the bot user with two claims added to `AccessClaims` (`authn/token.rs:247`), `gen` the key generation the assertion was verified under and `inst` the installation id for an isolated login, so the handshake reads both by arithmetic and builds the `Principal` with no read, identity the bot user, capability `inst:<id>` when `inst` is present.

**Follows.** First login opens a session through the store and the route returns its id. A shared login writes `connetto_app_bots` if no row exists. An isolated login writes `connetto_installation_sessions`, and an isolated login without `sid` for an installation that already has a session replaces it, revoking the old one, since an installation has one session. A shared login without `sid` when one exists opens a second session and the first dies at its ceiling, harmless. A later assertion naming `sid` mints against it if it is live and belongs to that bot and that installation shape. An exchange is the bot's refresh and slides the idle window, so a bot never presents a refresh token, `AuthStoreError::Reuse` never arises, and the ninety-day absolute ceiling stands, an instance opens a fresh session every ninety days and resnapshots once. The `IssuedSession.refresh_token` (`authn/store.rs:68`) minted by `create_session` is discarded. **Follows.** The store gains an operation that slides the idle window without rotating a refresh token, on both stores.

**Follows.** Sessions count logins, not processes. A shared-view bot has one session at the deployment. An isolated-view bot has one per installation it serves. Two processes presenting the same `sid` evict each other per chapter 12, and the template says a reactor that must run once runs in one process.

**Follows.** Rate limiting under R19 keys the exchange on the app id after the signature verifies, and on the address before it. Lifetime is `AuthConfig::access_ttl`, renewed by re-signing, no new knob. Blast radius of one leaked private key is every bot of that app at every deployment where it is registered, bounded by rotation.

**Follows.** Discovery is registration. A vendor learns that lab A exists when lab A registers app X and tells the vendor the URL, or when the vendor holds an account at lab A and registers there. Once logged in with the shared session, the bot learns its installations from a live query on `connetto_installations WHERE app = me`, which the default `SELECT` policy admits for the bot user, and opens an isolated login for each it serves. No callback, no central place.

## The registry cache and the change-log hook

**Follows, new machinery.** The handshake needs the app's current key generation, its accepted grant rows, and for an isolated login the installation's installer, and nothing on the handshake path may read the application database. The session manager keeps a registry cache of all three, seeded at boot from the three synced tables and maintained from the change log inside `ingest`, the same watch that reacts to deletions. This is not R7, which levels a store and announces grant moves. The watch on the three synced tables does four things. A deleted installation row revokes the session `connetto_installation_sessions` names for it, if any, and drops the id from the cache so a held grant with that `inst` is refused at handshake as well. A deleted app row revokes every session of its bot, reading the bot user from `connetto_app_bots` before the cascade removes it, which needs a revoke-by-user operation on both `AuthStore` implementations. A bumped key generation updates the cache and evicts every live session of the bot with a close reason that tells the client to log in again keeping its `sid`, distinct from `SessionRevoked`, whose contract is that the session is gone (`authentication_client.rs:246`), and from `ConnectionSuperseded`. A changed grant row updates the cache and tears down any open subscription of the bot outside the accepted rows with `FullResyncRequired`, as a grant move does.

## Lifetime, as a state table

Every event against every situation for one app at one deployment. "Shared session" is the bot's shared-view login. "Isolated session for I" is its login for installation I. "Away" means no live socket and possibly a held grant.

| Event | Not registered | Registered, bot away | Shared session live | Isolated session for I live |
|---|---|---|---|---|
| Owner inserts an app row (publish) | policy admits or refuses. Grant rows follow as inserts, each checked against the catalog. Nothing is minted | refused as duplicate | n/a | n/a |
| Installer inserts an installation row | refused, no such app | policy on `connetto_installations` decides. Share rows naming `inst:<id>` or the bot user are the installer's next inserts | same, and the shared session's live query on installations shows the new row | same |
| Bot exchanges an assertion | refused | signature against the row's key, `aud` and `jti` checked, `sub` checked against the app or one of its installations, owner and installer ban checked, bot user resolved and written to `connetto_app_bots` if absent, grant minted with `gen` and `inst`, session opened or resumed, idle window slid, isolated session written to `connetto_installation_sessions` | same, the new grant serves the next reconnect | same |
| Bot handshakes | refused, `sid` not live | grant by arithmetic, liveness by the store, `gen`, the accepted rows and the installation from the cache, `Principal` is bot identity plus `inst:<id>` when `inst` is present, ban checked on the bot and on the installer the cache names | newer connection evicts the older | same |
| Bot exchanges without `sid` while a session exists | n/a | shared, a second session opens, the first dies at its ceiling. Isolated, the installation's session is replaced and the old one revoked | same | the old socket receives `SessionRevoked` |
| Installer deletes the installation row (uninstall) | n/a | the hook revokes the session `connetto_installation_sessions` named, if any, and drops the id from the cache. The next isolated exchange for I is refused, no such installation, and a held grant with that `inst` is refused at handshake | nothing happens to the shared session. Rows leave it through R7 when the share rows go | session revoked, `SessionRevoked` sent, socket closed. Rows the bot wrote are untouched |
| Installer deletes their share rows | n/a | rights shrink at the next check | R7 resyncs the affected subscriptions of the subject the row named, rows leave that replica, nobody else is touched (PR #42) | same |
| Owner updates the public key (rotation) | n/a | trigger bumps the generation, the cache follows. A held grant with the old `gen` is refused at handshake, an assertion under the old key fails verification | evicted with the re-login reason, the session stays live for the next login under the new key | same |
| Owner inserts, deletes or accepts a grant row | n/a | insert checked against the catalog, the cache follows | an open subscription outside the accepted rows is torn down with `FullResyncRequired` | same |
| Owner deletes the app row | n/a | cascades by foreign key to every installation, grant row and server-only row, referential actions bypass row security. The hook reads the bot user from the old image and revokes every session of the bot | shared session revoked | isolated session revoked |
| Operator bans the bot user | n/a | every login of the app refused by `refuse_if_banned`, the operator's lever to disable an app at their deployment, lifting restores | evicted, `SessionRevoked` | same |
| Owner is banned | n/a | the exchange refuses every login of the app while the ban stands, lifting restores. No row is deleted | evicted, `SessionRevoked` | same |
| Installer is banned | n/a | the exchange refuses an isolated login for any of the banned installer's installations, `refuse_if_banned` (`session.rs:2849`) also checks the installer the cache names at handshake. No row is deleted, lifting restores. Shared-view grants stay effective as the banned person's share links do (chapter 12, a share is consulted only at use) | nothing changes, the shared session is the bot's | evicted, `SessionRevoked` |
| Installer's account is deleted | n/a | invisible to connetto. The deployment's own cascade on `installer` deletes the installation rows with the person's other rows, which is the uninstall row above | same | same |
| Bot process restarts | n/a | re-signs with `sid`, exchanges, handshakes, resumes cursors and watermark | the restart's connection evicts the dead one | same |
| Store unreachable | refused | the exchange and the handshake refuse, the bot retries with backoff | an open session continues. A change-log revoke waits under `DeliveryPaused` (`session.rs:2229-2245`) and lands when the store is back, so a revoked bot streams until then | same |

## Budgets

**Settled (2026-09-18, amended 2026-09-20).** A bot identity carries no budget of its own. Since `1ec2b57` (2026-09-20) every byte and rate meter charges `meter_key` (`crates/connetto-server/src/capability.rs`), which is the rendered caller's `storage_key` (`connetto_core::auth::ContentCaller`, `auth.rs:287`), `user:<identity>` when an identity is held and `keys:<packed subjects>` otherwise, so a bot session would meter under `user:<bot>` and any signed-in user who may publish would hold a bucket per app they publish and install. A bot session overrides the key by its kind. An isolated session meters under `user:<installer>`, the installer the registry cache names from `inst`, as if the user had acted themselves, and under nothing else, so a vendor's per-user app costs the vendor nothing and a user who installs many apps spends their own allowance. A shared session meters under `app:<owner>`, one bucket keyed on the publisher that every shared-view session of every app they published draws from, separate from the publisher's personal `user:<owner>` bucket, defaulting to one identity's allowance and raised by the deployment for a vendor it has accepted. A publisher never pays personally, a thousand self-published apps share one app bucket, and connetto never has to know whose row a shared write was for. R87's storage quotas follow the same two keys. **Follows.** `Tier::of` answers identified for a bot as for any caller that presented a grant (`throttle.rs`, amended 2026-09-20), the subscription and connection counts key the same two ways, and `AuthThrottle` on the exchange keys on the app id as before.

## The manifest

**Settled (prompt, amended 2026-09-18).** The manifest narrows, never grants. Tables and verbs, as accepted rows of `connetto_app_grants`. Effective rights of any session of the bot are the policy answer, from the share rows through whichever half they name, intersected with the accepted rows, enforced as a filter after the existing check at subscribe and per op on mutation. A content ticket to fetch counts as the table's read, an upload intent and commit as its write.

## Audit

**Follows.** `AuthOp` (`crates/connetto-server/src/audit.rs:55`) gains `BotLogin`, emitted by the exchange when a session opens or resumes after a gap, not on every re-sign, since fifty isolated sessions re-signing each `access_ttl` would write two hundred rows an hour, and `Uninstalled`, `KeyRotated` and `AppDeleted`, emitted by the change-log hook with the session it revoked or evicted. Publish and install are the rows' own existence, since they are plain mutations with no route to emit an op, and a deployment that wants their history keeps it. A bot's own actions carry the bot user and the session as any user's do.

## What exists and is reused

- `connetto_client::ConnettoClient<T>` owns the pump, `Send + Clone`, `start`, `with_reconnect`. One per login.
- Reads are `watch`, `watch_value`, `watch_groups`, `watch_rows`, `live`, behind `LiveHandle`.
- Writes are `with_conn(|c| diesel ...)`, outcomes on `events()` keyed by `client_seq`.
- Files are `connetto-file-client` with `ContentHttp` and `request_content_ticket`. Since PR #41 (2026-09-20) the ticket carries both caller halves through `ContentCaller` and the file server rechecks with both (`crates/connetto-file-server/src/caller.rs`), so an isolated session whose visibility comes from `inst:<id>` gets and serves a ticket with nothing more.
- `IdentityResolver`, `AuthStore`, `CallerBinding` with the absent marker, `meter_key`, `AuthThrottle`, `refuse_if_banned` (a bot can be banned by user id), `Tier::of` (a bot is identified), grant moves announced to the subject they name (`GrantHolder::Subject`, PR #42).
- Test doubles. `FakeTransport`, `LoopbackTransport`, `spawn_server` with `Server::attach()`, `Replica::in_memory()`.

## The reactor, in `connetto-client`

**Settled (prompt).** A reaction is at the query level, a live handle plus being told when it changed with the current snapshot. Bounds `MaybeSend`, `reactor_task` for any target, `spawn_reactor` native only. `insert_awaited` inserts under the state lock, pushes through `ConnettoConnection::push`, awaits that sequence's verdict. The sketch below names a `Watchable` bound that is not verified to exist and is the implementer's to shape.

```rust
pub trait Reactor<T: Transport>: MaybeSend + 'static {
    type Query: Watchable<T, Self::Row>;
    type Row: Clone + MaybeSend + MaybeSync + 'static;
    fn query(&self) -> Self::Query;
    fn react(&mut self, client: &ConnettoClient<T>, snapshot: &[Self::Row]) -> impl Future<Output = Result<(), ClientError>> + MaybeSend;
}
```

**Settled (2026-09-18).** `react` receives the whole snapshot, the same contract a screen has, since `changed` coalesces and the handle holds only the current answer (`live.rs:1127-1141`). `diff_by_key(prev, next, key)` in `connetto-client` returns added, removed and changed rows for a reactor that keeps its previous snapshot, and both demo apps use it. The first call after a restart is the whole answer under any signature.

## The app credential, in `connetto-client::auth`

**Settled (prompt, amended 2026-09-18).** Beside `NativeAuthenticator`. Holds the private key, the app id, the deployment's exchange URL and issuer, the `sub` it logs in as, and the `sid` it keeps. Signs, exchanges, hands grants to the client as an `AccessTokenSource`, re-signs before expiry, persists the `sid` beside the replica, keeps it across the re-login close reason and drops it only when the exchange says the session is not live. **Follows.** `ClientConfig::with_caller` (`crates/connetto-client/src/lib.rs:677`) takes the two-halves caller so the replica's translated policy answers an isolated session the way the server does.

## Seams, tiers, template

Unchanged from the prompt. Seams each with a fake, no seam without one. Scripted tier over `FakeTransport` and `Replica::in_memory()`, local policy tier over pg2sqlite, semantics tier over `spawn_server` against Docker. `Stage` names the corner cases. `examples/bot-template` copied rather than read, with `tests/scripted.rs`, `tests/semantics.rs`, `tests/props.rs`, `benches/reactor.rs`, `mutants.toml`.

## Demos

**Settled (2026-09-18).** The browser stack applies the registry DDL after the file server's `DEPLOYMENT_DDL`, publishes the three tables, adds a `shares` table to the demo schema with policy arms on `orders`, `order_lines`, `photos` and `connetto_visible_files` for both halves, and gives the demo UIs a catalogue, an installed-apps screen, a publish form and an install screen, all live queries and mutations. Two apps from the template under `examples/`. The importer runs isolated, one login per installer, inserts `orders` and `photos` rows the installer owns and uploads bytes. The totaliser runs shared, one login, holds `SUM(order_lines.quantity)` per order across every installer and writes `orders.quantity` (`orders` carries that column on `origin/main`). The stack registers both, installs each under two test users, runs them, proves the importer's session for Alice never sees Bob's rows and the totaliser's does, then rotates a key, narrows a manifest, uninstalls one installer and deletes one app, asserting per the state table.

Guidance the template ships. Share rows for an installation reference `connetto_installations(id) ON DELETE CASCADE` so uninstall removes the grant with the record. A deployment that wants its users' departure to uninstall their apps adds a foreign key from `connetto_installations.installer` to its own users table after the registry DDL has run, the table being in the deployment's database.

## Storage for a headless process

**Settled (2026-09-18).** A bot's replica is `Replica::in_memory()` per login. A restart resnapshots, an offline commit dies with the process, and the template says a bot that must not lose a write treats its source as the durable store and re-derives, which the importer does from the folder and the server. A bot may declare device-local tables like any client, and a bot that holds them needs the file replica, encrypted under the replica key per chapter 14, whose Linux custody across a reboot is R71's unsolved question. The file replica for bots is therefore gated on R71 and arrives with it, under whatever custody R71 chooses, with no change here. The credential file, private key and `sid`, is the operator's and not a replica.

## Where the code lives

No new crate. `connetto-server` gains the registry DDL with its five tables, policies and the generation trigger, the catalog check on grant rows at mutation inspection, the filter exemption for the registry tables, `AppRegistry`, `app_router` with the one route, bot resolution and the two server-only writes at the exchange with `jti` replay refusal, the `gen` and `inst` claims and the `inst:` subject rendering, the idle-slide and revoke-by-user store operations, the registry cache and its change-log hook with the re-login close reason, the ban checks on owner and installer, the filter, the audit ops. `connetto-client` gains the app credential, the two-halves caller, `Reactor`, `diff_by_key`, `insert_awaited`, `testing`. The template is `examples/bot-template`. Chapter 12 gains the three entities, the bot as a user and the two views, chapter 11 the exchange and the session rules, chapter 13 the reactor, chapter 01 a piece row, the plan its phase row.

## Superseded, kept so they are not re-derived

- One bot user per installation. Superseded by one bot per deployment with the installation as the capability half. It would have made sessions count installs rather than logins and would have had no shared view.
- One session per process. Sessions count logins.
- An installation as a capability subject only, with a `connetto_installations` join arm in every policy. The identity half carries authorship and the deployment's own share table carries the arm.
- A minted capability key as the installation subject, and install as a route that mints it. The subject is the installation id and the assertion is the proof. Superseded in the review.
- Bot minting at publish through mutation inspection, and two routes. Publish is a plain insert, the bot is resolved at first login, and the exchange is the one route. Superseded in the review.
- Server-written columns on the synced tables under column privileges. A client changeset carries the whole row, so the privileges would refuse the client's own insert. The two values live in server-only tables. Superseded in the second review.
- A `share` verb in the manifest. connetto serves no mint route, so it could not enforce one.
- Rotation as revocation. Rotation is a generation bump and an eviction, sessions survive.
- A per-installation accepted subset and a per-installer connetto filter. Narrowing per installer is the share rows.
- An app-level list-installations exchange and a callback URL. The bot reads its installations as a live query.
- A config switch for who may register. It is the `INSERT` policy.
- Per-login consent, a bot key as a plain share key, a user-synced bots table, a `first_party` flag.
- A bot acting as a user, binding `app.user_id` to the person. Refused 2026-09-18. Every policy would admit it with no share row, the manifest filter would be the only cap, the audit would name the person, a ban on the bot would ban the person, and the bot could widen its own grant by writing the person's share and installation rows, escaping its manifest in one insert. What is wanted is already covered, the isolated view acts for the person on exactly the rows they shared, a `created_by` column the bot fills makes it look like them where the deployment wants that. GitHub's authorize grant exists because GitHub has no share rows a person can write for an app, connetto does.

## Non-goals

A wasm marketplace of reactors. A SQLite-backed materializer. A bot consuming the materializer in process. A human login for a bot, meaning the provider round trip, a refresh token and a browser. A UI app acting as a person.

## Settled after the review, 2026-09-18

1. A ban refuses and evicts the installer's isolated logins and leaves shared-view grants as share links are left. Deletion is the deployment's cascade on the installation row.
2. The manifest is the child table `connetto_app_grants`, tables and verbs only, columns never.
3. Whole snapshot, `diff_by_key` as a helper.
4. In-memory per login. The file replica for bots is gated on R71 and ships with it.
5. Budgets, superseding the same-day "no per-app limit" answer once the multiplication attack was named. An isolated session is charged to its installer, a shared session to an app budget per owner separate from the owner's personal budget, the bot identity carries no budget. R91 carries a test that fifty isolated logins subscribing at startup fit their installers' allowances, and one that a thousand self-published shared-view apps share one app budget.
6. One route, no minting at insert. The exchange resolves the bot user at first login and writes it, and `app.subjects` carries `inst:<id>` at an isolated login.
7. Second review. The bot user id and the isolated session id live in `connetto_app_bots` and `connetto_installation_sessions`, server-only and unpublished, no column privileges. The isolated subject renders as `inst:<id>`. Grant rows repeat `owner`. Registry tables are exempt from the manifest filter. The cache holds installations. `jti` is checked. Audit ops are `BotLogin` from the exchange and `Uninstalled`, `KeyRotated`, `AppDeleted` from the hook. Banning the bot user is the operator's lever.

## Grounding against main, 2026-10-04

Checked against `main` at `e34b95f`, after R94 (client builders) and R96 (`ServerBuilder`, one port). The step-1 grading guard (`crates/connetto-server/tests/it/registry_grading.rs`) still passes. Citations that moved: `AccessClaims` `authn/token.rs:249` (`GrantClaims::User` `:239`), `DefaultUuidResolver` `identity.rs:91`, `refuse_if_banned` `session.rs:3403` (called `:3054`), the `DeliveryPaused` hold `session.rs:2746-2800`, `ContentCaller::storage_key` `connetto-core/src/auth.rs:293`, `ClientConfig::with_caller` `connetto-client/src/lib.rs:792`, `LiveHandle` coalescing `live.rs:669-691`. Exact: `AuthOp` `audit.rs:55`, `key:<uuid>` `capability.rs:48-52`. `AuthConfig::issuer` stands, set through `ServerBuilder::auth_config`. `schema.sql:21-23` is `examples/deployment/schema.sql`, the shared browser deployment.

**Measured, contradicts the record.** `Translated::policy_tables()` for the installations policy is `["connetto_app_bots", "connetto_installations"]`, and `build_authorization` (`builder/mod.rs:1250-1256`) refuses to boot unless every policy table is published. An unpublished `connetto_app_bots` read by the bot's `SELECT` arm cannot boot.

**Seams chosen, no design fork.** These follow the record and only pick where code goes.

- The exchange is `ServerBuilder::apps(Option<AppsSettings>)` producing an `app_router` merged in `assemble_parts` beside `auth_router`, the `content`/`file_router` pattern. Nothing in the binary but the env translation.
- `AppRegistry` holds the owner pool (the `DbAuthStore`/`PgOplog` pattern), the `TokenAuthority`, the auth store, the deployment's `IdentityResolver` (the builder hands the same `Arc` to the store and the registry) and an `Arc<RegistryCache>` shared with the manager.
- `gen` and `inst` ride in `AccessClaims` and come out of `check_grant` on `VerifiedSession` as an optional app claim, and `resolve_grants` checks them against the cache and pushes `inst:<id>` as a capability subject. The authority stays cache-free.
- The cache is a `ManagerBuilder::with_registry` collaborator, seeded in `wire_manager` after `settle_epoch` and before the change stream starts, maintained in `ingest` beside `keep_store_current`, under the same `DeliveryPaused` hold.
- A bot session's meter key is computed at handshake from the cache (`user:<installer>` or `app:<owner>`) and stored on the session, so a cache entry dropped mid-session never leaves it unkeyed. The content ticket carries that key as its storage key so the file server's R87 quota charges it, with identity and subjects unchanged for attribution and rechecks.
- The rotation eviction is a new `FatalErrorReason::CredentialRotated`. The client needs no new routing, the pump recovers and the app credential re-exchanges with its `sid`.
- The client side of `inst:<id>` is carried by the app credential, a new sealed `NativeSignIn` inside `connetto-client` beside `HeldCredential`, which feeds both halves to the local policy. The isolated subject has no public setter, so no application can spell one.
- The exchange's rate limit keys on the app id after the signature verifies. The pre-signature half keyed on the address is dropped, R19 reads no addresses, and the public key comes from the cache, so an unsigned flood costs arithmetic only.
- `connetto_apps.public_key` is a raw 32-byte Ed25519 key, `BYTEA CHECK (octet_length(public_key) = 32)`.
- The four new `AuthOp` values extend the reference `connetto_auth_op` DDL in chapter 08. Pre-alpha, no migration code.
- `connetto_installation_sessions` and the generation trigger live in a server-only DDL constant the deployment applies, the `EPOCH_DDL` pattern, outside the documents clients translate.

**Settled (2026-10-04).** Four points the grounding opened, decided with the maintainer.

1. `connetto_app_bots` is published. Server-written on the owner pool, no client policy and no reader grant, so no client reads it, and its rows reach the change path, which closes the write-once window the record tolerated. Client bundles carry the empty table. `connetto_installation_sessions` stays unpublished, no policy reads it.
2. An exchange refusal (not registered, banned, bad signature) surfaces to the client as an auth error, so the pump stops with `AuthenticationRequired` and the template's loop decides when to try again. A session that is no longer live re-exchanges without `sid` inside the credential. Network and server errors retry under the reconnect policy.
3. The catalogue, installed-apps, publish and install screens go into both web demos, dioxus-web and yew-web, over `examples/deployment`. The desktop demo is unchanged.
4. A bot subscription whose table the server cannot name is refused by the manifest filter with the ordinary refusal frame.

**The exchange contract (2026-10-04),** fixed so the server route and the client credential build against one text. It follows from the settled points and decision 2 above.

- `POST /auth/app-token` with a JSON body `{"assertion": "<jwt>"}`.
- The assertion is an EdDSA JWT signed by the app's key, header `alg` `EdDSA`. Claims `iss` the app id, `sub` the app id for a shared login or the installation id for an isolated one, `aud` the deployment's `AuthConfig::issuer`, optional `sid` the session to resume, `jti` unique per assertion, `iat`, and `exp` at most five minutes after `iat`.
- `200` with `{"access_token", "session_id", "bot_user", "expires_in"}`. `expires_in` is seconds of the access grant.
- `401` with `{"error": "session_not_live"}` when the assertion verifies but its `sid` is not a live session of this bot in this shape. The credential drops the `sid` and exchanges again without it.
- `401` with `{"error": "refused"}` for every other refusal (unknown app, bad signature, wrong `aud`, expired, replayed `jti`, an isolated `sub` that is not an installation of the app, a banned owner, installer or bot). No reason is disclosed. The credential surfaces it as an auth error.
- `429` with `Retry-After` when the app id is over its exchange limit, `503` when the store is unreachable. The credential surfaces both as transient errors the reconnect policy retries.
- A bot keeps its session alive by exchanging, each exchange slides the idle window. The credential re-exchanges at every handshake, and a long-lived connection renews by exchanging again before the idle window closes.

**Measured while testing the exchange (2026-10-04).** A policy arm that reads a second table through `EXISTS` grades as `Untranslated(MembershipTableGuarded)` in rls2fga when that second table has row-level security of its own, and the server refuses to boot on an untranslated policy. The registry's bot arm passes because `connetto_app_bots` carries no policy. The demos' `shares` table is policy-bearing, since an installer must not read another's share rows, so the arms on `orders`, `order_lines`, `photos` and `connetto_visible_files` as the Demos section words them would not boot. The demo slice grades its shares design under rls2fga before anything else and brings a shape that grades back for discussion if the `EXISTS` form cannot.

**Shares, settled (2026-10-04).** The share arms read `shares` through `SECURITY DEFINER` SQL functions, the shape rls2fga accepts for a guarded membership table, and `shares` carries an owner arm and a grantee read arm so a grantee's replica holds the rows naming it. Measured with the server's `Translated::of`: it grades. pg2sqlite refuses the function call on the client side, so the maintainer launched the upstream fix (`upstream/pg2sqlite-inline-sql-functions.md`, branch `upstream/inline-sql-functions` on `LucaCappelletti94/pg2sqlite`). Settled with that session: an inlined definer body reads the backing table, an invoker body the filtered view. The demo slice waits for that pin.

**Open (2026-10-05), the manifest's form.** Content tickets name no table, so the record's "a ticket counts as its table's read" has nothing to check, and discussing it the maintainer asked for the manifest itself to be a Rust declaration compiled and verified against the schema types, smart-contract style, rather than strings alone. Research on prior art comes first (`plans/research-typed-app-manifest.md`). Until settled, the server's runtime filter over `connetto_app_grants` stands as built, tickets stay unfiltered, and the template and demo bots do not bind to a manifest API.

## Redesign, 2026-10-05

A new session reopened the model with the maintainer under three rules (handoff `plans/handoff-apps-and-bots-redesign.md`): long-term optimality is the only criterion, nothing built is sacred, compile-time guarantees are the ideal. Points below outrank every earlier section they contradict. Earlier sections are rewritten once the conversation ends.

**Settled (2026-10-05).** Two layers, kept apart.

1. **The app is reusable software.** It declares the schema it needs as a trait of properties (a key of a table, a numeric column, a reference from one table to another), never concrete column types. A deployment that wants the app implements the trait for its own tables and deploys the bot. The compiler checks the app's code against the properties and the deployment's implementation against the trait. Macros help write both sides, and they live in synql (24).
2. **The deployed bot is user-like.** Adding it to an owner is a runtime event, decided by the deployment's RLS/FGA rules (point 10).
3. **connetto defines a trait every bot implements**, carrying what connetto needs from a bot, the tables it uses and whether it reads or writes each. The app's schema macro writes that implementation, so the list is derived from source code and nobody types it. Once a deployment implements the app's trait the list names that deployment's concrete tables.
4. **The list has three uses.** The server caps the bot to it, on top of the RLS answer. The consent screen shows it to the admin adding the bot. The bot's replica holds and syncs only those tables. A bot asking for a table outside its list is cheating by definition.
5. **Bots deploy without redeploying the server.** A bot sends its list when it registers. The registered list is the one admins are shown and the one the bot is held to, effective at registration with no separate lab-level approval. A request outside it is refused exactly as a user's request on a table they have no right to, the ordinary refusal and nothing more (no suspension, no eviction).
6. **A different list is a different bot.** When a registered bot sends a different list, every owner it was added to gets "the bot has requested different access", GitHub style. Until that owner's admin accepts, the bot sees nothing of that owner's assets, since it is not the bot they agreed to.
7. **Typed optionality is per installation.** The app's schema may mark a table optional. The admin adding the bot may accept or decline each optional table, and the bot touches an optional table of that installation only after the server confirms it was accepted, a check the code cannot skip. Adding an optional table to a bot therefore pauses nobody. Optionality per deployment (a lab leaving a table unmapped) is not wanted for now, since queries cannot be written over a table that may not exist without special handling in diesel. The two are modelled separately and mean different things.
8. **A signed-in person stands behind every registration.** A deployment user creates a one-time registration code in the deployment's app. The bot starts with that code and registers its public key and its list, and the bot belongs to that user. The deployment's policies decide who may create codes.
9. **Seeing is not acting.** A bot sees and writes under its own identity. Acting as a person, writing so that the result appears as if the person did it, is a different grant and not part of this design. The 2026-09-18 refusal of a bot bound to a person's identity stands.
10. **A bot is added to an owner, as a member, and the deployment's rules decide.** The usual ownership shape: ownables are things owned, owners are ownables that can own, and users, teams, projects and organizations are owners, so ownership chains. Adding a bot to team T inserts a membership row in the deployment's own membership tables, as adding a person would. Only someone with write access to T, an admin of T, may add it, which is the deployment's existing rule on its membership table. The bot then reaches whatever the deployment's rules give that member, everything T owns in the usual shape, even rows the adder cannot see, capped by the list and authored by the bot. connetto walks no ownership and keeps one authorization system for people and bots.
11. **The bot stays when its adder leaves.** Carol adds the importer to T and later leaves T. The bot remains T's member until an admin of T removes it, as a GitHub app stays on an organization.
12. **No share rows, no shared or isolated view, no `inst:<id>` subject for bots.** Bots get access only through membership rows. A share row naming a bot remains possible as ordinary deployment data, connetto does not rely on it. A lab-wide bot is added to the organization. The demo's `shares` table is no longer needed for bots.
13. **connetto ships a standard Postgres RLS/FGA schema**, well structured and ready to use (owners, ownables, users, teams, projects, organizations, memberships, their policies). Any deployment may adopt it and none is held to it.
14. **An installation bills the owner it was added to.** The importer in team T draws on T's budget, as T's members' activity on T's assets would, and a deployment sets budgets per owner. The bot has no budget of its own. Supersedes `user:<installer>` and `app:<owner>`.
15. **The member is the installation.** Each addition of a bot to an owner creates its own member principal, shown and authored as the bot, and the bot logs in once per installation. connetto pauses (6), narrows (7) and bills (14) one installation without knowing the deployment's ownership, and no connection ever sees two installations' rows, so a bot bug cannot copy one team's rows into another's.
16. **Adding is one transaction written by the admin's client.** It inserts connetto's installation record (the bot, the owner it serves, the accepted list version, the optional choices) and the deployment's membership row naming that installation as the member. The deployment's existing rule on its membership table refuses the whole transaction unless the writer administers that owner. connetto knows nothing about teams. **Follows.** An installation record without a membership grants nothing, and a membership without an installation record has no bot to log in, so a half state is inert. Since 14 bills the owner the record names, the record's own policy must admit only an admin of that owner, which the standard schema of 13 writes and a custom deployment writes itself.
17. **Files follow the rows that reference them.** A file is a hash held in a column of a deployment table, and its bytes are reachable exactly when a row referencing it is, through `connetto_visible_files` under RLS. The list carries no file entry.
18. **The list cap is a rule inside Postgres, on every synced table.** connetto sets the installation's list in a session setting beside `app.user_id`, and every synced table carries one generated `AS RESTRICTIVE` policy per verb, `USING (connetto_bot_may('<table>', 'read'))` and the write counterpart, true for every caller holding no list. Postgres ANDs it with the deployment's rules, so subscriptions, writes, `connetto_visible_files` and any later path obey the list without connetto code checking it per path. connetto generates the rules, and the server refuses to start when a synced table lacks one, the R98 preflight shape. Supersedes point 4's "the server caps", which now means this rule, and the built connetto-side manifest filter. **Measure first.** Whether rls2fga grades such a restrictive policy (it parses `RESTRICTIVE`, `classifier/patterns.rs`). **Stated limit.** A deployment function marked `SECURITY DEFINER` bypasses all RLS, this rule included, as it bypasses the deployment's own rules.
19. **No raw-SQL restriction in the bot's client.** A bot's connection is to its own SQLite replica. Raw reads see only what the replica holds, which is only the listed tables. Raw writes are pushed as changes and refused server-side outside the list. Live queries are subscriptions answered under the same rules. The typed schema is the comfortable path, catching typos at compile time, never a security wall.
20. **The client pump always says why it stopped** (taken as plainly better, reported to the maintainer). On `main`, `route_step_result` (`crates/connetto-client/src/live.rs:2957`) ends the pump on any error that is not a disconnect with `pump_finished` and no event, so an application or bot is never told its driver stopped and an awaited write waits forever. The fix emits one terminal event carrying the error before the pump exits, and resolves every pending await with that error. Pre-existing on `main` (`83615f0`), present on every branch cut from it, not introduced by R91. The R91 `testing` stage tests that assert the silence change with it.
21. **Live queries carry a declared lifetime, and the longest one wakes a closed client** (a distinct functionality and its own phase, which the bots phase uses). The developer states per live query how long updates are wanted, in three tiers: while a UI element exists, while the app runs, and even when the app is backgrounded or closed. A third-tier query stays parked at the server while the client is away, and when its answer changes the server wakes the client through the channel it registered, coalesced: a bot's wake URL with a server-signed note, a phone's push token through Apple's or Google's service, a browser's Web Push subscription. The client reconnects, catches up from its cursor through the existing change log, and decides what to do (a bot reacts, an app shows a notification). The grain is the parked query, never the table, so a phone is not woken for every visible edit. **Follows.** A bot that is woken rather than connected wants its replica kept between visits, the encrypted file replica (R71, R93), which supersedes "bots run on `Replica::in_memory()`" for woken bots.
22. **A bot logs in by key at connect, SSH style.** The server sends a fresh random challenge on each new connection, the bot signs it with the key its registration stored, naming the installation it connects for. Nothing can be replayed, since a challenge lives only on its own connection, and no state is shared between server processes. **Follows.** No `/auth/app-token` route, no access token, no session id, no `jti` memory, no idle-window slide for bots, and the exchange's rate limit becomes R19's throttle on failed key logins keyed by bot after the signature verifies. Removing an installation, banning the bot or rotating its key closes the affected connections at once, and resume cursors are kept per installation. A woken bot (21) costs one signature per wake.
23. **Generating a deployment's Rust table types from its schema files belongs to synql** (`earth-metabolome-initiative/synql`, which already generates diesel models per table from SQL documents through `sql-traits` and `diesel-builders`). The maintainer resumes synql after connetto's part is done and adds these functionalities there. connetto's phases therefore do not build a schema-to-Rust generator, and every connetto trait a deployment or an app implements must be implementable by hand today and by synql's generated code later.
24. **The app's schema macro lives in synql**, on its `sql_relations` vocabulary and `diesel-builders`, with the deployment's side that implements it and the generated implementation of connetto's bot trait. connetto's phases define only that bot trait, what connetto needs from a bot (the tables, read or write, optional or required), and the demo bots implement it by hand until synql has the macro.

**Correction to 18, measured against `main` (`docs/architecture/08-authorization.md:110,197`, `17-fan-out.md:65,87`).** Postgres itself evaluates the rule on snapshots (the snapshot runs as the caller on an RLS-subject pool), on mutation apply (the same pool applies client mutations under `WITH CHECK`) and in `connetto_visible_files`. Live change delivery is decided by connetto, `subql` matching the predicate and `VisibilityPolicy::may_see` answered by `RowPolicy` or OpenFGA from rls2fga's translation of the same policy text, and the write pre-check `may_write` likewise. So the cap rule must be understood by that translation too, as one more connetto-known function beside the caller and subjects functions. Since its argument is a table and a verb and never row data, `RowPolicy` answers it locally from the session's list and the change event's relation, which comes from the WAL, not from parsing. **Follows, on the maintainer's sqlparser worry.** The list cap itself never depends on sqlparser parsing a bot's query, on any path. The deployment's own rules on the live path do, through rls2fga and subql, for people and bots alike, which is pre-existing and outside this design.

**Grounding, R98 merged (#126, `ded95a2`, 2026-10-05).** Every deployment-owned table connetto reads through a trait is now a member of one `ConnettoSchema` (`crates/connetto-server/src/schema.rs`), with `type Id` shared by all members, defaults emitted by `connetto_schema!`, and `const TABLES` per member refused at startup when missing. **Follows.** The apps and installations tables connetto reads (registered bots, their lists, installations, registration codes) become one more member of `ConnettoSchema` under the same `Id`, emitted by `connetto_schema!` by default, nameable by a deployment, preflight-checked like the others, instead of a separate registry DDL. The standard RLS/FGA schema of 13 is different in kind: it is the deployment's application data (owners, memberships, policies), which connetto does not read through a trait, so it ships as a ready-to-use SQL document. Phase numbers R99 to R105 remain free, R98 is spent.

**Superseded today, kept so they are not re-derived.** The bot working with the adder's rights and never exceeding them (replaced by 10 and 11). The app declaring reference paths to a scope root and the server adding "belongs to project Y" by following them (replaced by 10). connetto-defined owner kinds as mandatory traits (replaced by 13). `connetto_app_grants` with a lab-level `accepted` flag (replaced by 5 and 6).

**Measured (2026-10-05).** diesel's `view!` (our fork, `51fb453`) generates a type that works in selects, joins, `eq_any` subqueries and boxed queries, while `insert_into`, `update` and `delete` on it fail to compile (`view: Table`, `HasTable`, `IntoUpdateTarget` unsatisfied). A read-only table can therefore be refused writes at compile time through diesel's public API alone. Scratch crate `/tmp/r91-view-spike`.

## Proposed phases, 2026-10-05 (agreed split, R101 first)

**Priority, the maintainer's (2026-10-05).** R101's mechanism and its iPhone and Android experiments come before the bot phases, since an app needs them to be functional. The phone channels fold into R101, R102 is freed, and the bot's wake URL moves to R103.

Each phase's design is brought back to the maintainer before anything is built. Numbers R99 to R105 are free, R98 is spent. R91 keeps its number for the core so the plan's references stay true.

| Phase | What it delivers | Waits on |
|---|---|---|
| R91, the bot registry and the list cap | The `ConnettoSchema` member for bots, registration codes, installations and list versions. Registration: a signed-in user's one-time code, the bot's public key and its list. The generated `AS RESTRICTIVE` rule per synced table and verb with its session setting, understood by rls2fga, `RowPolicy` and pg2sqlite, refused at startup when missing. The admin's one-transaction add, list versions, re-acceptance and per-installation optional tables. Measured first: rls2fga grades the restrictive rule | nothing |
| R99, bot login and lifetime | Key login at connect with a per-connection challenge, the installation as the connection's identity, refusal while the installation has not accepted the bot's current list, closing connections on removal, ban and key rotation, R19's throttle on failed logins, metering on the owner the installation names, the audit ops, every row of a rewritten state table | R91 |
| R100, the standard ownership schema | A ready-to-use Postgres document: ownables, owners, users, teams, projects, organizations, memberships whose member may be a bot installation, their policies. Graded by rls2fga, translated by pg2sqlite, adopted by `examples/deployment`. Optional for any deployment | nothing |
| R101, live-query lifetimes and wake (first) | Opened by the iPhone and Android experiment. The three lifetimes in the client API, parked third-tier queries at the server, coalesced wake, the iPhone and Android channels. Web Push here or in a later phase as the experiment shows | nothing |
| R103, the bot runtime and template | The `connetto-bot` crate: the bot trait (the list), the key credential, the installation loop, reactors with the runtime-kept diff and `insert_awaited`, the bot's wake URL as a channel of R101, the per-installation file replica (R93 absorbed 2026-10-05), the `testing` stage, `examples/bot-template` with its four targets | R91, R99, R101 |
| R104, the demos | `examples/deployment` on the standard schema, add-bot, consent and installed-bots screens in both web demos, the importer added per team and the totaliser per organization, end to end in the browser stack | R100, R103 |

**Outside the phases.** The client pump fix (20) was a bug on `main`, merged as #129 (2026-10-18). The schema macro and the table generator (23, 24) are synql's, after connetto's part.

**What the built R91 code becomes.** Reference only. The token exchange, `jti` set, shared view, `inst:<id>` subject, `connetto_app_grants`, the connetto-side manifest filter and the `user:<installer>`/`app:<owner>` keys are superseded. The registry cache, the change-log hook, the reactor, `diff_by_key`, `insert_awaited`, the `testing` stage and the template's test targets are candidates to carry over into R91, R99 and R103 after each phase's design is agreed.

### R91 design, settled points (2026-10-05)

1. **A list entry names a table and any of four verbs, read, insert, update and delete**, one generated `AS RESTRICTIVE` rule per verb, so a bot is held to exactly the commands it declares and the consent screen can say "creates and edits orders, never deletes them". synql's macro derives the verbs from the app's schema properties.
2. **A bot may also log in as itself, with no installation.** That bot-level connection reads only connetto's own rows about this bot (its installations, the list version each accepted, pending requests), through the same key login and the same Postgres rule, its list being connetto's registry tables. The bot keeps a live query on them, or parks it (R101) and is woken when an installation is added or accepts a new list. It sees nothing of any owner's data.
3. **Every new list needs a fresh one-time code from the bot's owner**, as the first registration did, so a person stands behind every list and a stolen key alone cannot ask any admin for more access. **Follows.** A key rotation goes the same way, a fresh code registers the new key and the old one stops working at once.

### R91 design, agreed 2026-10-05

**Purpose.** A deployment registers bots, admins add them to owners, and every bot is held to its list inside Postgres on every path. Login is R99's, so R91's tests bind the bot's session settings directly through the harness.

**The `ConnettoSchema` member.** `type Bots: ConnettoBotSchema<Id = Self::Id>`, emitted by `connetto_schema!` by default, nameable by a deployment, each table reported in `const TABLES` and refused at startup when missing (R98's shape). Four synced tables, whose policies are the deployment's (R100's standard schema writes them):

| Table | Columns | Written by |
|---|---|---|
| `connetto_registration_codes` | code hash, creator, bot (empty for a first registration), expiry, used at | the creator's client, then the server marks it used |
| `connetto_bots` | id, owner (the code's creator), name, public key (32 bytes), current list version, wake URL (R101) | the server at registration |
| `connetto_bot_lists` | bot, version, table, verb (read, insert, update, delete), optional | the server at registration |
| `connetto_installations` | id (the deployment's `Id`, the member principal), bot, owner it serves, accepted version, accepted optional tables | the admin's client in the add transaction (16), updated on re-acceptance |

**Registration, over the connection.** The bot connects, the server sends its challenge, the bot answers with the code, its public key, its list and its signature over the challenge with that key. The server refuses a used, expired or unknown code, a list naming a table or verb outside the synced catalog, and a signature that does not verify, all with one undisclosing refusal. Otherwise it writes the bot (or, for a code naming an existing bot, the new key or the next list version) and the list rows on the owner pool, and marks the code used. One listener, one protocol, no HTTP route.

**The cap, fail closed (decided 2026-10-06, from the first measurement).** A server-set session setting, `app.bot_list`, holds every connection's effective list as `table:verb` entries joined by commas (for a bot, the accepted version's required entries plus the accepted optional ones), and connetto sets `*` on every person's connection. One generated `AS RESTRICTIVE` policy per synced table and verb (`FOR SELECT` read, `FOR INSERT` insert, `FOR UPDATE` update, `FOR DELETE` delete), each reading `'orders:read' = ANY(string_to_array(current_setting('app.bot_list', true), ',')) OR '*' = ANY(string_to_array(current_setting('app.bot_list', true), ','))`. A connection carrying no list sees and writes nothing, as with connetto's own rule when `app.user_id` is missing, so a forgotten setting hides data from a person rather than lifting a bot's limits. Clients of the database outside connetto set `*` beside `app.user_id`. Registration refuses a table named `*`. connetto ships the generator over the publication's tables. Startup refuses a synced table missing any of its four rules, by name. The bot-level connection's list is connetto's registry tables, read only.

**The two executors.** The rule must hold where connetto decides as well as where Postgres does. connetto declares `app.bot_list` to rls2fga as a set attribute, beside `app.subjects`, so the model grades the rule B, and connetto-schema maps it to a set-holding SQLite function, `current_bot_list()`, so the bot's replica evaluates the same rule (both measured, below). connetto's watcher answers `app_bot_list` from the connection's list, so `RowPolicy` decides the gate locally as it does `app.subjects`. Both settings are connetto's, and the measurement found nothing to change upstream.

**Measured first, before any code.** rls2fga's grade for a policy set carrying the restrictive rules, and pg2sqlite's translation of them. If either refuses, the finding goes upstream and the phase waits on it.

**Proof.** For an unlisted table and for a listed table with an undeclared verb: the snapshot is empty, a mutation is refused at apply and by `may_write`, a live change is not delivered, and `connetto_visible_files` hides a file referenced only from that table. A person's session on the same tables is unaffected. A synced table missing one rule refuses startup naming it. A used, expired or unknown code, a list naming an unknown table and a bad signature are refused identically. A code for an existing bot registers version 2 without touching version 1's acceptances, and a rotation code makes the old key fail. The add transaction is refused whole when the deployment's membership rule refuses.

### R99 design, agreed 2026-10-05

**Purpose.** A registered bot connects, for one installation or as itself, proves its key, and lives under the state table below. Waits on R91.

**Key login.** On a new connection the server sends a random challenge. The bot answers with its bot id, the installation it connects for or none, and its signature over the challenge with its registered key. The server verifies against `connetto_bots.public_key` from the registry cache (seeded at boot and kept current from the change log, the built cache's role), so the handshake reads no table. An installation connection binds `app.user_id` to the installation id, the member principal of 15, and the cap setting to that installation's effective list. A bot-level connection binds the bot id and the registry-only list of R91 point 2. R19's throttle counts failed logins per bot id after the signature verifies and per connection before it.

**Which list applies.** The installation's accepted version, plus the optional entries its admin accepted. When the bot registers a later version, an installation that has not accepted it keeps working only if the new version's required entries are a subset of what it accepted (the change added optional tables or narrowed), otherwise its logins are refused with a distinct "awaiting acceptance" reason and an open connection is closed with it. Narrowing applies at once, by the cache updating the setting on the next statement and closing subscriptions over a table that left the list.

**One connection per installation.** A newer connection for the same installation evicts the older, as chapter 12 does for sessions, so a reactor that must run once runs once. Different installations of one bot may run in different processes.

**Lifetime, every event against every situation.**

| Event | No connection | Installation connection open | Bot-level connection open |
|---|---|---|---|
| Admin adds the bot (16) | next login for it succeeds | n/a | the bot's live query on its installations shows the new row |
| Admin removes it (deletes installation and membership) | login refused, unknown installation | closed with `SessionRevoked` | the row leaves its live query |
| Bot registers a new list | logins refused if required entries grew and not accepted | closed with "awaiting acceptance" in that case, else continues | the request shows as pending |
| Admin accepts the new list | logins succeed under the new list | n/a (was closed) | the acceptance shows |
| Rotation code registers a new key | old key refused | closed, re-login with the new key | closed, re-login with the new key |
| Operator bans the bot | every login refused | every connection closed | closed |
| Operator bans one installation id | that installation's logins refused | closed | unaffected |
| Owner of the bot deletes it | everything refused | every connection closed | closed |
| Membership row deleted, installation row kept | login succeeds and sees nothing | rows leave through R7 | unaffected |
| Store unreachable | logins refused, the bot retries | continues, closures queue under `DeliveryPaused` | same |

**Metering.** An installation connection meters under the owner its installation names (`owner:<id>`, point 14). A bot-level connection meters under the bot's owner (`user:<owner>`), since it reads only the bot's own registry rows. The bot identity holds no bucket.

**Audit.** `AuthOp` gains `BotRegistered`, `BotListRegistered`, `BotKeyRotated`, `BotRemoved` and `BotLoginRefused`. Successful logins are not audited, since a woken bot logs in on every wake. Adding and accepting are the installation row's own changes.

**Proof.** Every row of the table above against a real server. A replayed signature on a second connection is refused. A newer connection evicts the older. An installation connection's traffic lands on `owner:<id>` and never on `user:<bot>`. The cache answers the handshake with the database's registry tables locked.

### R100 design, settled points (2026-10-05, superseded 2026-10-25 by the layered layout in the master plan's R100 section)

1. **Assets descend from `ownables`.** Every asset table's key references `ownables (id, owner_id)`, owners descend from ownables, and users, teams, projects and organizations descend from owners, class-table inheritance as `diesel-builders` and synql handle it. The owner of anything lives in one table and chains are walked there. Creating an asset writes its `ownables` row and its own row. **Measure first.** Each asset table's rule reads `ownables`, which carries its own rule, the exact shape rls2fga refused as `MembershipTableGuarded` on 2026-10-04, and chains need a recursive walk or a maintained closure. Both are graded under rls2fga and translated by pg2sqlite before the shape is built on, and a refusal is an upstream finding.
2. **Memberships carry one of three roles, read, write and admin.** Read sees the owner's assets, write also creates and edits them, admin also manages the owner itself, its members and its bots. Adding a bot (16) needs admin on the owner.

### R100 design, agreed 2026-10-05 (superseded 2026-10-25 by the layered layout in the master plan's R100 section)

**Purpose.** A ready-to-use Postgres document, `connetto-server/sql/standard-ownership.sql` (name indicative), that a deployment may adopt whole and none is held to, plus the policies for R91's registry tables written against it. `examples/deployment` adopts it in R104. Waits on nothing, except where the measurement below sends a finding upstream.

**Tables.** `ownables (id, owner_id → owners)`, `owners (id → ownables)`, `users`, `teams`, `projects` and `organizations` each `(id → owners, …)`, so a team, project or organization is itself owned (a project owned by a team owned by an organization). `principals (id)`, the things that can be members, with `users.id → principals` as well and `connetto_installations.id → principals`, so an installation is a principal and never an owner. `memberships (owner_id → owners, member_id → principals, role read|write|admin)`. Ids are the deployment's `Id`, so a user's id is the connetto identity and an installation's id is what R99 binds as `app.user_id`.

**Rules.** A principal's role on an ownable is the highest role it holds on any owner on that ownable's chain, so an admin of the organization administers every team and project under it, and a member of T reaches T's orders and the orders of projects T owns. Every asset table's rule is "my role on this row's ownable is at least read" for `SELECT`, at least write for `INSERT`, `UPDATE` and `DELETE`. `memberships` is written only by an admin of its owner, which is also the rule that guards adding a bot. Every synced table additionally carries R91's four cap rules.

**Registry policies.** `connetto_registration_codes`: its creator, and any signed-in user may create one (an unadded bot can do nothing). `connetto_bots` and `connetto_bot_lists`: readable by every signed-in user, so an admin can find a bot and read its list on the consent screen. `connetto_installations`: written and read by admins of the owner it serves, and read by its own bot over the bot-level connection.

**Measured first.** The asset rule reads `ownables` and the chain, and `ownables` carries its own rule, which rls2fga refused as `MembershipTableGuarded` in this shape on 2026-10-04. The candidates graded in order: the chain as a recursive CTE inside the rule, a closure table `ownable_ancestors (ownable, owner)` kept by triggers and carrying no rule of its own, and the chain read through a definer function. The first that rls2fga grades and pg2sqlite translates is built, and if none does, the finding goes upstream and R100 waits on it.

**Proof.** The grading test as a guard. Against a real server: a reader of an organization sees orders two levels down, a writer of T edits T's orders and cannot add a member or a bot, an admin of T can, a principal outside the chain sees nothing, an installation added as read cannot insert, and moving a project from team T to team U moves its orders' visibility at once (R7 resync).

### R101, research before design (2026-10-05)

The maintainer wants the lifetime API worked out concretely in a Dioxus app and tried on iOS and Android for ergonomics, preferring Rust lifetimes. How a closed app still receives notifications, from the platform documentation:

- **iOS.** A background notification (`content-available: 1`) wakes the app for 30 seconds, is low priority, is throttled ("don't try to send more than two or three per hour"), and is discarded if the user force-quit the app ([Apple](https://developer.apple.com/documentation/usernotifications/pushing-background-updates-to-your-app)). So a closed iPhone app is reached only by an alert notification, which the system displays. A Notification Service Extension, a separate small process inside the app bundle, receives an alert with `mutable-content` first and has about 30 seconds to rewrite it, for example to decrypt it or fetch a few rows ([Apple](https://developer.apple.com/documentation/usernotifications/modifying-content-in-newly-delivered-notifications)). On iOS "the client wakes and decides" therefore means the extension, not the app.
- **Android.** A Firebase data message is delivered to the app's messaging service even when its process is gone, unless the user force-stopped it, with about 10 seconds to handle it and WorkManager for longer work ([Firebase](https://firebase.google.com/docs/cloud-messaging/android/receive-messages)). The app itself can wake, sync and post a local notification.
- **Web.** A Web Push message wakes the site's service worker while the browser runs, and Chrome requires each push to show a notification.

**Follows for the API.** Two of the three lifetimes are ordinary Rust scope: a query held by a component lives with it, a query held at the app root lives while the app runs, so Dioxus hooks express both through ownership. Only the third outlives the process and cannot be a Rust lifetime, it is a named registration kept by the server. What the server sends on wake (nothing but a name, or enough for an extension to render a notification within 30 seconds) depends on the iOS path, which is why the experiment comes before the API.

### R101 design, agreed 2026-10-05 (opens with an experiment)

**Step 0, the experiment, before any API.** A throwaway Dioxus app on an iPhone and on the Galaxy M52, swiped away, receiving a wake from a minimal server: on iOS an alert with `mutable-content` rewritten by a Rust notification service extension within its 30 seconds, on Android a Firebase data message that wakes the app's service, syncs and posts a local notification. Measured: delivery, latency, what Rust can run inside the iOS extension (memory, time, network), and how scoped live queries read as Dioxus hooks. The maintainer expects upstream findings from it (Dioxus, the mobile tooling), each handled through the upstream procedure.

**Then the design, brought back for agreement.** The client API for the three lifetimes, with the first two as Rust and Dioxus scope and the third as a named registration the server keeps, the parked-query store at the server (a `ConnettoSchema` member), evaluation of parked queries for away clients through the same fan-out, coalescing, and the wake payload the experiment shows each platform needs.

**The bot channel moves to R103** (2026-10-05). There a bot registers a wake URL with its key and list, and the server POSTs a note signed with the server's key, naming the installation (or the bot itself, for its bot-level queries) and the parked query, never data, coalesced per installation, retried with backoff, and a failed delivery costs nothing since the bot catches up whenever it next connects.

### R103 design, settled points (2026-10-05)

1. **A `connetto-bot` crate on top of `connetto-client`** holds the bot trait (the list), the key credential, the installation loop (connect as itself, follow its installations, keep or open one connection with its own replica per installation), the reactors and the wake receiver. UI apps never compile it, and the wake receiver's web server stays out of the client. Supersedes "no bot crate" of 2026-09-18, which held while a bot was one login.
2. **The runtime keeps each reactor's previous answer and hands it the diff.** The last answer is stored beside the installation's replica, and each run receives what was added, changed and removed since, plus the full current answer, exact across restarts and wakes. A run that fails is retried with the same diff, so a reactor's writes must tolerate a repeat (at least once). Supersedes "whole snapshot, `diff_by_key` as a helper" of 2026-09-18.

### R103 design, agreed 2026-10-05

**Purpose.** Writing a bot is implementing one trait and some reactors, and the `connetto-bot` crate runs it for every installation, connected or woken. Waits on R91, R99 and R101. R93 is absorbed (maintainer, 2026-10-05), since R103 creates the runtime R93 edited and woken mode needs its file replica.

**The bot trait.** `name` and `list()`, each entry a table, its verbs and whether it is optional, written by hand until synql's macro generates it (24). The list is what registration sends.

**Registration and key.** The bot binary carries a `register --code <code>` entry point that reads the operator's Ed25519 key file, sends key and list (R91), and is the same path for a new list or a rotated key. The key never leaves the host. The server's public key, returned at registration, is pinned to verify wake notes.

**The installation loop.** A bot-level connection follows the bot's installations (R91 point 2). Each installation gets its own connection and its own replica, the file replica of R93 named from the installation id (R93's decision 1 simplified, since the installation is now the identity), and removal wipes that replica (R93 decision 2). A bot runs connected, holding every installation's connection, or woken, connecting per installation only when a wake note or a restart asks for it. An installation refused for "awaiting acceptance" (R99) is parked and retried when the bot-level connection shows the acceptance.

**Reactors.** A reactor names a query and is registered as a third-tier, parked query of R101 for each installation, so a woken bot is called back when its answer changes. Each run gets the persisted diff and the current answer (point 2) and writes through the installation's client, with `insert_awaited` to wait for the server's verdict on its own write.

**The wake receiver.** An HTTP endpoint the operator exposes, verifying each note against the pinned server key, coalescing, and scheduling that installation's run. A note that never arrives costs nothing, the next connect catches up.

**Testing and template.** The `testing` module gives the corner cases as named stages (a list change awaiting acceptance, removal mid-run, a restart between two runs, a wake for a removed installation, a repeated run after a failure, a refusal outside the list). `examples/bot-template` ships scripted, semantics, property and bench targets and `mutants.toml`, copied rather than read.

**Proof.** Two installations of one bot never see each other's rows on disk or in memory. A restart between two changes yields the exact diff. A wake reconnects only the named installation and its reactor sees the change. A failed run repeats with the same diff. Removal wipes the installation's replica and stops its reactors. A second bot written from the template by someone other than the author needs no change to `connetto-bot`.

### R104 design, agreed 2026-10-05

**Purpose.** The whole model, end to end, in the browser stack, so a reader sees a bot registered, added, held to its list, changed, accepted and removed. Waits on R100 and R103.

**The deployment.** `examples/deployment` adopts R100's standard schema: an organization "lab A" owning two teams, each owning projects, which own `orders`, `order_lines` and `photos` descending from `ownables`. Every synced table carries R91's cap rules.

**Screens, in dioxus-web and yew-web** (the desktop demo unchanged, as decided 2026-10-04). Create a registration code. A catalogue of registered bots with their lists. Add a bot to a team or the organization, with the consent screen naming each table and verb and a toggle per optional table. Installed bots per owner, with a pending "requested different access" to accept, and removal. All live queries and the one add transaction (R91 point 16).

**Two bots from the template, under `examples/`.** The importer, added per team with `orders` insert and update and `photos` read and insert, running connected, importing a folder into orders with their pictures' bytes. The totaliser, added to the organization with `order_lines` read and `orders` read and update, running woken, writing each order's total quantity whenever its lines change. Between them both running modes and both scopes are exercised.

**The scenario, scripted in the browser stack.** Register both with codes, add the importer to team T and the totaliser to lab A, import, watch totals follow, register importer v2 asking for an optional `customers` read and see team T keep working, register a v2 asking for a new required table and see T paused until its admin accepts, remove the importer from T and see its replica wiped, and a hand-written raw query by the importer on an unlisted table refused.

## Proposed phase (superseded 2026-10-05)

**R91: apps, installations and the bot template.** Blocked on nothing. The content-ticket caller fix (PR #41) and the grant-move narrowing (PR #42) both landed 2026-09-20, so an isolated session uploads and an isolated uninstall resyncs one session. The file replica for bots is R71's. Steps in order, each test-first. The registry DDL with its five tables, policies and trigger, and the catalog check on grant rows at mutation inspection, with harness tests that a refused publish is refused, an admitted one syncs to a live query, a grant row naming an unknown table is refused, and an owner cannot reassign their app or a client set its generation. The first test of the phase, before any DDL is written, is the grading: the three registry policies as drafted are handed to rls2fga in a unit test and each must classify at or above the threshold connetto boots with, and the two that join across rows, the bot's `SELECT` arm on `connetto_installations` through `connetto_app_bots` and the grant-row owner policy, are the ones expected to fail first. A policy that grades below is rewritten in the record and in the test until it grades, and the test stays as the guard that a later rls2fga tightening is noticed at build rather than at a deployment's boot refusal. The `gen` and `inst` claims, the idle-slide and revoke-by-user store operations on both stores. `AppRegistry` and `app_router` with the exchange, bot resolution at first login and the two server-only writes, with harness tests for every row of the state table that touches them, including a banned owner's, a banned installer's and a banned bot's logins refused and evicted, a lost-credential re-login replacing an isolated session, and a replayed assertion refused. The registry cache and its change-log hook, proven against real client sessions receiving `SessionRevoked` on uninstall and deletion, the re-login reason on rotation, and staying quiet otherwise. The manifest filter, with its site in `SessionManager` chosen so the existing authorization check has already answered, and proven by four tests against a real bot session: a subscription on a table the accepted rows do not name is refused at subscribe with the refusal frame named in the test, a mutation whose plan touches a table or verb outside the rows is refused per op and the rest of the plan is not applied, a subscription open when its grant row is deleted or un-accepted is torn down with `FullResyncRequired` and does not come back on resubscribe, and the bot's own query on `connetto_installations` passes with no grant row naming it. The two budget keys, proven by an isolated session's bytes landing on `user:<installer>` and not `user:<bot>`, a shared session's on `app:<owner>` and not `user:<owner>`, fifty isolated logins subscribing at startup within their installers' allowances, and a thousand self-published shared apps sharing one app bucket. The app credential, the two-halves caller, `Reactor`, `diff_by_key`, `insert_awaited`, `testing`. The template green in all four commands. The two demo apps and the browser-stack scenario. The four-place sync. Done when the template's targets pass in CI, the importer's session for one installer provably never sees the other's rows while the totaliser's sees both, every row of the state table has a test, and a second bot written from the template by someone other than the author needed no change to `connetto-client`.

### R91 first measurement (2026-10-06)

Graded with connetto's pins, rls2fga `10839a6` and pg2sqlite `95ce881`, over `items (id, owner, label)` carrying connetto's own permissive rule and four `AS RESTRICTIVE` cap rules, one per verb. Throwaway program `crates/connetto-test-harness/examples/r91_cap_grade.rs` in the catchup worktree, uncommitted.

| Cap rule written as | rls2fga, shipped settings | rls2fga, `app.bot_list` declared a set | pg2sqlite, connetto's options | pg2sqlite, plus a `app.bot_list` set mapping |
|---|---|---|---|---|
| `connetto_bot_may('items', 'read')`, the agreed function | D, startup refuses | D, startup refuses | refused, unknown function `connetto_bot_may` | refused, same |
| `current_setting('app.bot_list', true) IS NULL OR 'items:read' = ANY(string_to_array(current_setting('app.bot_list', true), ','))` | D | D | refused, no mapping for `app.bot_list` | refused, the `IS NULL` reads a set-mapped setting as one value |
| `'items:read' = ANY(string_to_array(current_setting('app.bot_list', true), ','))` alone | D | A | refused, no mapping | translated |
| `current_setting('app.bot_list', true) IS NULL` alone | D | D | not run | not run |
| `'items:read' = ANY(...) OR '*' = ANY(...)` on `app.bot_list` | D | B, startup's bar | refused, no mapping | translated |

- The function form needs upstream work in both crates and a SQLite function on every replica.
- What neither crate grades is the absent-list arm, `current_setting(...) IS NULL`, which is how the agreed design leaves people untouched.
- The list-or-wildcard form needs no upstream change, only two connetto settings: `app.bot_list` declared to rls2fga as a set attribute, and a set mapping for it in connetto-schema's pg2sqlite options. It is fail closed: a connection carrying no list sees nothing, so every person's connection carries `*`.
- Not yet checked: rls2fga `main`, the SQLite acceptance of the translated DDL, and `RowPolicy` answering the gate from the request's list.

**The three remaining checks (2026-10-06).**

- **rls2fga `main` (`357b14e`) grades the same.** Five cap clauses, list alone A, list or `*` B, the absent-list form refused (five clauses below the bar), with `app.bot_list` declared a set attribute.
- **SQLite accepts and enforces the translated rule.** pg2sqlite `95ce881` with connetto's options plus a `app.bot_list` set mapping to `current_bot_list()`, applied to an in-memory SQLite database with the functions registered, two rows owned by `alice`, the caller `alice`:

| `current_bot_list()` | rows seen | insert |
|---|---|---|
| `*` (a person) | 2 of 2 | allowed |
| `items:read` | 2 of 2 | refused by the policy trigger |
| `items:read,items:insert` | 2 of 2 | allowed |
| `other:read` | 0 of 2 | refused |
| `NULL` (no list) | 0 of 2 | refused |

- **`RowPolicy` answers the gate locally once connetto supplies the list, read from code.** subql's `RowPolicy` resolves a request-gated relation through `Subject::request_value(parameter, ..)` for any parameter (`src/visibility/policy/evaluate.rs:160-180` at `a49f019`), and an unanswered parameter falls to the delegated question, slower and never wrong. connetto's watcher answers only the subjects parameter today (`crates/connetto-server/src/openfga.rs:320-330`), so R91 adds `app_bot_list` from the connection's list there. Not run.

### R100 first measurement (2026-10-06)

Graded with connetto's pins, rls2fga `10839a6` and pg2sqlite `95ce881`, and the deciding shapes again on rls2fga `main` `357b14e`, which grades them the same. The rule measured is the `orders` one, "my role on this order's ownable chain is at least read" for `SELECT` and at least write for `INSERT`, over `owners`, `ownables (id, owner_id)`, `memberships (owner_id, member_id, role)` and `orders (id → ownables)`. Throwaway program `crates/connetto-test-harness/examples/r100_chain_grade.rs` in the catchup worktree, deleted after the run.

| Candidate | rls2fga | connetto startup | pg2sqlite |
|---|---|---|---|
| 1. Recursive CTE inside the rule, `ownables` and `memberships` without rules | D | refused | translated |
| 1. The same, both with rules | D | refused | translated |
| 2. Closure table `ownable_ancestors (ownable, owner)` joined to `memberships`, role `IN` list, one role, or no role guard | D | refused | translated |
| 2. The same as a nested `EXISTS`, or as `id IN (... owner IN (...))` | D | refused | translated |
| 3. Definer function `role_level(id, me) >= N` | D | refused | refused, unknown function |
| A flattened table `ownable_access (ownable, principal, level)`, one row per granted level, read by a single `EXISTS` | A | accepted | translated |

- rls2fga recognises a membership read from one table that names the row (pattern P4), and no two-hop form, so neither "this order's ancestors" nor "members of those ancestors" can be reached through a second table.
- The flattened table passes everything today, but holds one row per ownable, per principal reaching it, per level. An organization of 1,000 members owning 1,000,000 orders is about a billion rows per level, and adding one member to the organization writes a row for every ownable under it.
- The closure table grows with ownables times chain depth, and a membership change writes one row. It is the shape OpenFGA itself expresses as `member from ancestor`, and the one rls2fga cannot read yet.
- **Decided 2026-10-06: the closure table, once rls2fga reads it.** Request `upstream/rls2fga-membership-through-a-second-table.md` (r91 worktree), merged upstream as rls2fga #77 (2026-10-30). Superseded 2026-10-25 by the layered layout in the master plan's R100 section, which no longer needs it.
