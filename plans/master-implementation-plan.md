# Master implementation plan: identity, session, capability, and the change path

## How to read this

The `Status` table is the one status record of the programme, and every statement of a phase's state or a blocker elsewhere in this file restates it. Phases that are not done or dropped have a section under `Phases`, holding their purpose, the decisions still binding, the remaining steps, the proof and the done-when, and a phase that is not done carries a `Changes state` line beside its status naming the persistent state it changes, the rule being that two unfinished phases sharing a state carry a dependency between them before either starts. Done and dropped phases are rows only, with what they built, their proof and their merged pull requests in the table. `Gaps` holds the defects and risks no phase owns yet, and a phase that takes one links it and the entry goes when the phase lands. `Designs without a phase` holds decided designs nothing builds yet. Everything else, the dates, the measurement history, the resolved upstream requests and what execution changed, lives in `git log` and in the architecture chapters, which hold the decisions of the built system and outrank this plan where the two disagree about a decision.

## Step zero

**Never assume an external system's capabilities, API or performance.** Read the protobuf, the config schema, the vendor documentation or the installed source first. This matters most when a claim argues against a decision already taken, because an unverified assumption then reopens settled ground.

## The gate

The gate is what a session runs in full before any push, and CI runs the same gate on every pull request, except that its clippy legs run `+stable` where the gate prescribes `+nightly`. `.github/workflows/ci.yml` checks each of the eight workspaces in eleven legs, fmt, `cargo +stable clippy --all-targets` and the doc build with `-D warnings`, runs the test shards `server`, `client`, `files` and `rest` under `cargo nextest` with `--test-threads 4` inside a `keyctl session`, and runs the browser suites as four shards of `connetto-browser-stack --shard`. `.github/workflows/coverage.yml` re-runs the three native test shards `server`, `client` and `rest` under `cargo llvm-cov` and uploads them to Codecov. There is no task runner reconciling this prose, the workflows and `.config/nextest.toml`, so the workflows and the config are the executable record of what CI proves.

Eight workspaces, each with its own lockfile, the root, `crates/connetto-web`, the four demos, `examples/wasm-smoke` and `examples/webauthn-unlock`.

The root workspace runs the four proof legs.

```sh
cargo +stable fmt --all -- --check
cargo +nightly clippy --all-targets --all-features -- -D warnings
cargo +stable test --release --all-features
RUSTDOCFLAGS="-D warnings" cargo +stable doc --no-deps --all-features
```

**`cargo +stable` for every build and test, `cargo +nightly` only for clippy.** The rule is load-bearing, because nightly has been observed to ICE on tokio in release, and there is no `rust-toolchain.toml`, so the plus-selector is the only thing enforcing it. The clippy mismatch between the gate and CI is [G14](#g14), and it means a lint that fires only on nightly reaches `main` with CI green.

Tests run `--release`. Debug builds of these workloads are one to two orders of magnitude slower, and a full debug run is a session-eating trap. Use debug only when targeting overflow checks, `debug_assert!` bodies, or a bug that only appears unoptimized, and say so when you do.

**Iteration is not the gate.** Each big crate has ONE integration target, `tests/it`, and a `[profile.testfast]`, release minus `lto` with `codegen-units = 16` and incremental, exists in seven of the eight workspaces, all but `examples/tauri-leptos-demo`. The day-to-day loop runs on the `testfast` profile.

```sh
cargo nextest run --cargo-profile testfast --all-features
cargo nextest run --cargo-profile testfast --all-features -E 'test(retention::)'
cargo +stable test --profile testfast --all-features -p connetto-server --test it openfga_live
```

`.config/nextest.toml` bounds the `docker` group to four threads and serializes the `keyring` and `discovery` groups, and a wedged test is killed at five slow periods instead of hanging the invocation. The `keyctl session` wrapper is optional locally, since every OS-keyring test joins its own fresh session keyring in-process through `connetto_test_harness::isolated_session_keyring`, called first in each such test, and CI still wraps its runs in one out of caution. The gate commands above still run `--release`, unchanged, so the proof profile is untouched.

The browser suites run per suite through `connetto-browser-stack`, a binary of `connetto-test-harness`. It starts the stack and drives one `wasm-pack test --headless --chrome` invocation per suite, and the suite list covers the `tests` targets of `crates/connetto-web`, `examples/wasm-smoke`, `examples/yew-web-demo` and `examples/leptos-demo`, the lib suites of `connetto-web` and `connetto-file-client`, and the dioxus demo's `web` feature. Bare, the binary runs the verified-topology step and then the default suites in series, and `--shard I/N` runs the suites whose position lands on `I`, which is how CI quarters them.

Docker-gated Postgres tests and headless Chrome may both be run freely.

**A silence assertion is bounded by the change it is about.** A live patch carries the position of the change that caused it, and `live_until` returns the moment the replica holds what a test waited for, so that change's settling tail is still in flight when the next assertion starts, and a test records the position it has accounted for on a subscription. The frames a test meets understate the change it waited for, because a membership move reads the live table and can deliver a row before the row's own event is dispatched, whose frame then arrives above every position seen. So before writing the change it wants silence about, a test accounts for everything the database has committed, `Fixture::committed_position` with `pg_current_wal_lsn` as a byte offset, which is exactly the set of changes it asked for and nothing after. A silence assertion refuses any frame past that position and applies what arrives at or below it, so a tail frame that adds a row fails as well, while a wait for the next change refuses to be satisfied by such a tail. The harness client keeps the live patches it meets while waiting for a control frame, so nothing it received is invisible to those comparisons. An assertion on a subscription that has been delivered nothing needs no bound, because there is no settling tail there to confuse with a violation.

## Status

| Phase | State | Blocked on | Scope | Proof | PRs |
|---|---|---|---|---|---|
| [R101](#r101-live-query-lifetimes-and-waking-a-closed-client) | in progress |  | The client API for the three lifetimes, the server's durable engine, the wire declarations, the notification keys and the provider sending |  | [#151](https://github.com/connetto-rs/connetto-rs/pull/151), [#201](https://github.com/connetto-rs/connetto-rs/pull/201) |
| [R99](#r99-bot-login-and-lifetime) | not started |  | Key login at connect, the installation as the connection's identity, the closures on removal and ban, the awaiting-acceptance refusal, the metering and the audit ops |  |  |
| [R103](#r103-the-bot-runtime-and-template) | not started | R99, R101 | The `connetto-bot` crate with the bot trait, the installation loop, the reactors and the wake receiver, and `examples/bot-template` |  |  |
| [R100](#r100-the-standard-ownership-schema) | blocked | sql-rules row-level security checks on `connetto-rs/sql-rules`, then rls2fga translating a declared definer helper into `from parent` relations | A ready-to-use Postgres document with one table per level, the grants per level, the definer helpers and the registry policies written against it |  |  |
| [R104](#r104-the-demos-for-apps-and-bots) | not started | R100, R103 | `examples/deployment` on the standard schema, the screens in both web demos and the two template bots |  |  |
| [R74](#r74-device-identity-enrolment-and-certificates) | in progress | R76 (slice 9d) | The last open proof, the Windows `unproven` enrolment record the slice 9d run produces on hardware |  | [#130](https://github.com/connetto-rs/connetto-rs/pull/130), [#132](https://github.com/connetto-rs/connetto-rs/pull/132), [#138](https://github.com/connetto-rs/connetto-rs/pull/138), [#139](https://github.com/connetto-rs/connetto-rs/pull/139), [#146](https://github.com/connetto-rs/connetto-rs/pull/146), [#161](https://github.com/connetto-rs/connetto-rs/pull/161) |
| [R76](#r76-the-peer-link) | in progress |  | Slices 9d (Windows hosting, joining, beacon and proof) and 9e (the recorded home run) remain, parked while the work goes mobile first |  | [#134](https://github.com/connetto-rs/connetto-rs/pull/134), [#135](https://github.com/connetto-rs/connetto-rs/pull/135), [#136](https://github.com/connetto-rs/connetto-rs/pull/136), [#137](https://github.com/connetto-rs/connetto-rs/pull/137), [#140](https://github.com/connetto-rs/connetto-rs/pull/140), [#141](https://github.com/connetto-rs/connetto-rs/pull/141), [#142](https://github.com/connetto-rs/connetto-rs/pull/142), [#143](https://github.com/connetto-rs/connetto-rs/pull/143), [#148](https://github.com/connetto-rs/connetto-rs/pull/148), [#158](https://github.com/connetto-rs/connetto-rs/pull/158), [#162](https://github.com/connetto-rs/connetto-rs/pull/162), [#163](https://github.com/connetto-rs/connetto-rs/pull/163), [#204](https://github.com/connetto-rs/connetto-rs/pull/204) |
| [R57](#r57-the-demo-feature-gaps-the-export-audit-found) | in progress |  | The application-visible features the export audit found missing from the demos, steps 1 to 5 and 7 found built in the demos on `examples/demo-support` (2026-10-26), step 6 found already fixed (2026-09-12), step 8 merged (2026-10-20), and the human proof pass remaining |  | [#205](https://github.com/connetto-rs/connetto-rs/pull/205), [#216](https://github.com/connetto-rs/connetto-rs/pull/216) |
| [R77](#r77-the-exchange-and-the-provisional-tier) | in progress |  | Slices 7 (online adjudication) and 8 (the handheld proof) remain, the rest of the exchange and the provisional tier is merged |  | [#296](https://github.com/connetto-rs/connetto-rs/pull/296), [#297](https://github.com/connetto-rs/connetto-rs/pull/297), [#300](https://github.com/connetto-rs/connetto-rs/pull/300), [#304](https://github.com/connetto-rs/connetto-rs/pull/304), [#310](https://github.com/connetto-rs/connetto-rs/pull/310), [#342](https://github.com/connetto-rs/connetto-rs/pull/342), [#344](https://github.com/connetto-rs/connetto-rs/pull/344), [#348](https://github.com/connetto-rs/connetto-rs/pull/348), [#351](https://github.com/connetto-rs/connetto-rs/pull/351), [#354](https://github.com/connetto-rs/connetto-rs/pull/354), [#357](https://github.com/connetto-rs/connetto-rs/pull/357), [#365](https://github.com/connetto-rs/connetto-rs/pull/365) |
| [R78](#r78-courier-recovery) | in progress |  | Slices 1a, 1b, 2, 3a, 3b, 4 and 6 remain, the server courier path under the five constraints, deployment-opt-in and default off, the receiver's courier hold, the client's explicit offer and the harness proof, and the author's re-send after a restore is merged |  | [#387](https://github.com/connetto-rs/connetto-rs/pull/387), [#391](https://github.com/connetto-rs/connetto-rs/pull/391) |
| [R79](#r79-media-over-the-peer-link) | in progress | R78 (step 4) | Slice 8, the harness proof, and step 4's opt-in replication |  | [#371](https://github.com/connetto-rs/connetto-rs/pull/371), [#373](https://github.com/connetto-rs/connetto-rs/pull/373), [#376](https://github.com/connetto-rs/connetto-rs/pull/376), [#379](https://github.com/connetto-rs/connetto-rs/pull/379), [#383](https://github.com/connetto-rs/connetto-rs/pull/383), [#385](https://github.com/connetto-rs/connetto-rs/pull/385), [#388](https://github.com/connetto-rs/connetto-rs/pull/388) |
| [R80](#r80-peer-sync-in-every-demo) | not started | R76 (slice 9d), R77, R78, R79 | The camp flow in every native-capable demo on every native platform, the iOS zero-coverage field test, and the grown phone and home runs |  |  |
| [R107](#r107-schema-changes-under-running-servers) | in progress | subql passing pgoutput `Relation` messages to consumers (slice 2) | The expand and contract procedure with the sunset, the table refusal when a running server notices a change, and the two-model boot check |  |  |
| [R108](#r108-caller-kinds-as-database-roles) | not started | sql-traits answering the caller contract, privileges and effective policies for both translators, for slices 2 to 5. R99, for the bot kinds | Caller kinds as database roles, every startup refusal before OpenFGA, four flat kind roles switched per transaction, grants per kind, and a client copy per kind |  |  |
| [R109](#r109-content-tickets-bound-to-their-session) | not started |  | Content tickets carry their session, and every fetch confirms the session is live, so a ticket dies with the session that minted it |  |  |
| [R110](#r110-a-session-bound-by-provider-staleness) | not started |  | Session lifetime bounded by the time since the identity provider last confirmed the user, replacing the fixed idle window and ceiling |  |  |
| [R111](#r111-a-durable-replica-where-opfs-is-refused) | blocked | the JSPI IndexedDB VFS upstream in `sqlite-wasm-rs` | A durable browser replica on IndexedDB through JSPI where OPFS is refused, memory kept only where JSPI is missing |  |  |
| [R21](#r21-one-page-codec-on-both-backends) | blocked | where `sqlcipher-src` and `sqlite3mc-src` live and who maintains them, in [rusqlite discussion #1908](https://github.com/rusqlite/rusqlite/discussions/1908) | One SQLite3MC codec on the native and browser backends, so the cipher pin stops being load-bearing |  |  |
| R0 | done | | The always-on change-path counters in `counters.rs` and the `fanout_load` harness that priced the dispatch loop, with the ceiling measured as one sequential visibility round trip per subscriber per event and the lock wait at zero | [fanout_counters.rs](../crates/connetto-test-harness/tests/it/fanout_counters.rs), [fanout_load.rs](../crates/connetto-test-harness/tests/it/fanout_load.rs) | |
| R1 | done | | The three permissive stand-ins reachable from configuration alone are gone, and the reference binary refuses to start without a reader role and a recognised `CONNETTO_OIDC_PROVIDER` | [e2e.rs](../crates/connetto-server/tests/it/e2e.rs) | |
| R2 | done | | The durable session handle is the operational key. Cursors and the `_connetto_mutations` watermark key on `session_id`, the connection registry serves revocation and supersession, and `TrustingSessionVerifier` is deleted with the verifier a required constructor argument | [session_handle.rs](../crates/connetto-test-harness/tests/it/session_handle.rs), [authn_flow.rs](../crates/connetto-server/tests/it/authn_flow.rs) | |
| R3 | done | | The handshake carries a grant list instead of a single credential, `HandshakeAuthority` checks each grant into a `Principal` with a non-optional handle, a refused grant is logged and silent on the wire, and the type guard keeps an ephemeral replica paired only with an ephemeral tier | [grants.rs](../crates/connetto-server/tests/it/grants.rs), [verified_topology.rs](../crates/connetto-client/tests/it/verified_topology.rs) | |
| R4 | done | | Capabilities as connetto-signed tokens naming subjects that are not people, the `CapabilityIssuer` mint that asks the read question through the same seam, the `app.subjects` binding every transaction applies, the expiry bounds, and the permission as a relation in the model | [capabilities.rs](../crates/connetto-server/tests/it/capabilities.rs), [capability_live.rs](../crates/connetto-test-harness/tests/it/capability_live.rs) | |
| R5a | done | | Every authorization call site asks through subql's `VisibilityPolicy` trait, the Postgres row-level-security implementation sits behind it as `RlsAuth`, the minting path reads its row as the caller, and `AuthPolicy` is deleted | [fanout_counters.rs](../crates/connetto-test-harness/tests/it/fanout_counters.rs) | |
| R5b | done | | The authorization service as the change-path executor. `openfga.rs` translates the deployment's policies, refuses an untranslated one at startup, loads the store from the rule text, holds the event and pauses delivery while the answer is unknown, and the counter test asserts zero round trips on connetto's own policy shape | [openfga_live.rs](../crates/connetto-server/tests/it/openfga_live.rs), [outage.rs](../crates/connetto-test-harness/tests/it/outage.rs), [parity.rs](../crates/connetto-test-harness/tests/it/parity.rs) | |
| R6 | done | | The two-check change form on subql's `transitions`. The current version is asked first and the previous one only on loss, a withdrawal is a plain unmarked delete whose key is read off the changeset, and the startup check refuses a publication table without `REPLICA IDENTITY FULL` | [transitions.rs](../crates/connetto-test-harness/tests/it/transitions.rs), [preflight_previous_image.rs](../crates/connetto-server/tests/it/preflight_previous_image.rs), [oplog_previous_image.rs](../crates/connetto-server/tests/it/oplog_previous_image.rs) | |
| R7 | done | | Revocation teardown. A moved grant fact resyncs exactly the subscriptions that read the tables it reaches, the `GrantReach` index in `reach.rs` maps facts to those tables, and a failed replacement read retries with backoff on the connection's own task | [revocation.rs](../crates/connetto-client/tests/it/revocation.rs), [openfga_live.rs](../crates/connetto-server/tests/it/openfga_live.rs) | |
| R8 | done | | The inert surface removed. Dead configuration fields and unconstructible variants are gone, `SessionManager::shutdown` walks the registry and closes every live connection, and a mid-session close reaches the client as `ClientEvent::ServerClosed` | [wire.rs](../crates/connetto-core/tests/wire.rs), [session_handle.rs](../crates/connetto-test-harness/tests/it/session_handle.rs), [conflict.rs](../examples/wasm-smoke/tests/conflict.rs) | |
| R9 | done | | The permissive policy out of the test suite. `PermissiveAuth` is gone, the harness `RosterAuth` stand-in carries a per-fixture roster and a withheld row, and every fixture that asks the policy asserts the row stays absent | [rls_write_filter.rs](../crates/connetto-server/tests/it/rls_write_filter.rs) | |
| R11 | done |  | A shared public store attached to every identity's replica connection, with the outbox capture and the row holdings in place of recomputed coverage | [shared_store.rs](../crates/connetto-client/tests/it/shared_store.rs), [holdings.rs](../crates/connetto-client/tests/it/holdings.rs), [storage_use.rs](../crates/connetto-client/tests/it/storage_use.rs) | [#179](https://github.com/connetto-rs/connetto-rs/pull/179), [#238](https://github.com/connetto-rs/connetto-rs/pull/238), [#239](https://github.com/connetto-rs/connetto-rs/pull/239), [#259](https://github.com/connetto-rs/connetto-rs/pull/259), [#270](https://github.com/connetto-rs/connetto-rs/pull/270), [#286](https://github.com/connetto-rs/connetto-rs/pull/286), [#298](https://github.com/connetto-rs/connetto-rs/pull/298), [#318](https://github.com/connetto-rs/connetto-rs/pull/318), [#327](https://github.com/connetto-rs/connetto-rs/pull/327), [#334](https://github.com/connetto-rs/connetto-rs/pull/334), [#337](https://github.com/connetto-rs/connetto-rs/pull/337), [#340](https://github.com/connetto-rs/connetto-rs/pull/340), [#349](https://github.com/connetto-rs/connetto-rs/pull/349) |
| R12 | done | | Structured logging through `tracing` in every crate. Native programs write JSON lines to stdout, browser programs to the console, events carry the session handle from the connection context, and a refused grant is named in the log and nowhere else | [logging.rs](../crates/connetto-server/tests/it/logging.rs), [logging.rs](../crates/connetto-web/tests/logging.rs), [grants.rs](../crates/connetto-server/tests/it/grants.rs) | |
| R13 | done | | The deployment-owned `auth_events` contract. The `ConnettoAuditSchema` trait and macro, the `AuthOp` enum, the `AuditHook` seam, and the producers for the three ways a login ends, the share mint and the bans, with state changes in the table and denials in the log | [audit_producers.rs](../crates/connetto-server/tests/it/audit_producers.rs), [audit_table.rs](../crates/connetto-server/tests/it/audit_table.rs) | |
| R14 | dropped | | The dispatch loop's own per-subscriber cost, dropped after measurement, because throughput falls 5 percent between ten and a hundred subscribers at both patch sizes and the loop is not the ceiling | | |
| R15 | done |  | Replica retention, eviction of the uncovered rows, and the trimming pass that returns the freed pages to storage | [retention.rs](../crates/connetto-client/tests/it/retention.rs) |  |
| R16 | done | | The fan-out research and the `17-fan-out.md` architecture. The unit of computation is one change event, and the socket write is the only work charged per subscriber | | |
| R17 | done | | The local tier is named from the identity's replica file with the `-tier` ending, so the name and the key have one scope, the delete-my-data path destroys the tier beside the replica, and the browser storage pool grows per account | [account_switch.rs](../crates/connetto-web/tests/account_switch.rs), [teardown.rs](../crates/connetto-web/tests/teardown.rs) | |
| R18 | done |  | The SQLite hardening surface on replica connections, defensive mode, trusted schema off, attach windows and the asserted limits | [hardening.rs](../crates/connetto-client/tests/it/hardening.rs) |  |
| R19 | done | | Request throttling tiered by identity. Subscription creation, connections and credential refusals meter on the durable handle, an over-limit caller draws the typed refusal or `429`, and the mutation bytes join the per-identity meter beside the upload | [throttle.rs](../crates/connetto-server/tests/it/throttle.rs), [reserve.rs](../crates/connetto-server/tests/it/reserve.rs), [mutation_budget.rs](../crates/connetto-server/tests/it/mutation_budget.rs) | |
| R20 | done | | Start with no reachable server. A connection constructs without a transport, the device persists the account identifier beside the credential, the `Run` and `Wire` split lets a reconnect resume, subscriptions persist in the replica and replay on attach, and the never-synced flag distinguishes an empty dataset from an unreachable one | [offline_start.rs](../crates/connetto-client/tests/it/offline_start.rs), [offline_boot.rs](../crates/connetto-web/tests/offline_boot.rs) | [#129](https://github.com/connetto-rs/connetto-rs/pull/129) |
| R22 | dropped | | The compile-time query set, deleted before starting, because a hand-maintained list of permitted requests defeats the stack, and its pieces went to R38, R19 and R27 | | |
| R23 | done | | User-verified unlock of the locally stored secrets. The browser derives the key-encryption key from a passkey PRF, the ceremony runs in a tab and the key crosses the private port to the worker, a declined unlock refuses the boot, and custody reports three levels with their reasons | [unlock.rs](../examples/wasm-smoke/tests/unlock.rs) | |
| R24 | done |  | A design, the ten positions on how connetto integrates a file-sync stack, chapter 18 carries them |  |  |
| R25 | done |  | A design, device-to-device sync without a server, chapter 19 is the normative record and R74 to R80 derived from it |  |  |
| R26 | done |  | A device export of the synced replica and the device-private tier as a zip of plain SQLite databases, superseded the same day by the R56 changeset format and the R61 portability download | [local_export.rs](../crates/connetto-client/tests/it/local_export.rs), [local_export.rs](../crates/connetto-web/tests/local_export.rs) |  |
| R27 | done |  | A membership term in the subscription language, one filter compiled to two executors, served end to end behind a hidden membership subscription | [membership_term.rs](../crates/connetto-test-harness/tests/it/membership_term.rs) |  |
| R28 | done |  | The row route installed before the snapshot read, and the aggregate bootstrap gap closed by buffering the deltas until the seed | [loop_emu.rs](../crates/connetto-client/tests/it/loop_emu.rs) |  |
| R29 | done |  | Persisted subscriptions and the resync delete built from the surviving subscriptions' coverage | [coverage_resync.rs](../crates/connetto-client/tests/it/coverage_resync.rs) |  |
| R30 | done |  | A design, grouped aggregates revisited, R82 to R85 derived from it |  |  |
| R31 | done |  | Application schema majors checked at the handshake, the in-place replica rebuild with held work draining under the old major, and the local-tier migration | [two_majors.rs](../crates/connetto-client/tests/it/two_majors.rs), [held_drain.rs](../crates/connetto-client/tests/it/held_drain.rs), [tier_majors.rs](../crates/connetto-client/tests/it/tier_majors.rs), [tier_majors.rs](../crates/connetto-web/tests/tier_majors.rs), [held_files.rs](../crates/connetto-file-client/tests/it/held_files.rs) | [#190](https://github.com/connetto-rs/connetto-rs/pull/190), [#191](https://github.com/connetto-rs/connetto-rs/pull/191), [#193](https://github.com/connetto-rs/connetto-rs/pull/193), [#194](https://github.com/connetto-rs/connetto-rs/pull/194), [#195](https://github.com/connetto-rs/connetto-rs/pull/195) |
| R32 | done |  | The replication slot lifecycle, the startup refusal, the lag watch, and the resync epoch when the slot's retained WAL is lost | [slot_watch.rs](../crates/connetto-server/tests/it/slot_watch.rs), [stream_gap.rs](../crates/connetto-server/tests/it/stream_gap.rs), [loop_emu.rs](../crates/connetto-client/tests/it/loop_emu.rs) |  |
| R33 | done |  | A snapshot completion frame that waits for its own data on the server and at the relay, with no resume position persisted ahead of the applied rows | [snapshot_order.rs](../crates/connetto-server/tests/it/snapshot_order.rs), [loop_emu.rs](../crates/connetto-client/tests/it/loop_emu.rs), [credits.rs](../examples/wasm-smoke/tests/credits.rs) |  |
| R34 | done | | The mint names the verbs it certifies. `ShareLevel` asks `may_write` per verb and refuses a denied one by name, the level travels in the reply and never in the token, and `ShareError::NotWritable` names the denied verb | [capabilities.rs](../crates/connetto-server/tests/it/capabilities.rs) | |
| R35 | done | | The over-broad columns narrowed to their values. The deadline columns carry `TIMESTAMPTZ`, the browser tab id is a version 4 `Uuid` the relay validates, the demo `quantity` is required and non-negative, and `client_id_prefix` is deleted | [authn_db.rs](../crates/connetto-server/tests/it/authn_db.rs) | |
| R36 | done |  | Abuse detection over four signals, identity bans with a permanent default, and the enforcement callback that asks the application | [abuse.rs](../crates/connetto-server/tests/it/abuse.rs), [ban_doors.rs](../crates/connetto-server/tests/it/ban_doors.rs) |  |
| R37 | done |  | One configuration style, the five settings types and their setters replacing the plain structs in the server and the client |  |  |
| R38 | done | | A refusal stops disclosing what exists. The subscribe path and the relay send one `SUBSCRIPTION_REFUSED` for every cause, nothing precedes the refusal frame, and the cause goes to the structured log | [snapshot_nonfatal.rs](../crates/connetto-server/tests/it/snapshot_nonfatal.rs), [nonfatal.rs](../examples/wasm-smoke/tests/nonfatal.rs) | |
| R39 | done |  | The reserved pool share, so an identified caller still completes a handshake and a snapshot while every other connection is unidentified | [reserve.rs](../crates/connetto-server/tests/it/reserve.rs) |  |
| R40 | done |  | Replica policy enforcement wired into sync, the type-directed pk codec, the relay hub's split tables and the browser pass | [rls_name_mapping.rs](../crates/connetto-client/tests/it/rls_name_mapping.rs), [rls_sync_path.rs](../crates/connetto-client/tests/it/rls_sync_path.rs) |  |
| R41 | done | | One seam for the two secret stores. `RefreshTokenStore` and `ReplicaKeyStore` live in `connetto-core` and address the account by name on both targets, the browser stores are `IdbKeyStore` and `RefreshStore`, and `connetto_refresh` holds one row per account | [secret_stores.rs](../crates/connetto-client/tests/it/secret_stores.rs), [secret_stores.rs](../crates/connetto-web/tests/secret_stores.rs) | |
| R42 | done | | Several accounts stay signed in at once. The account key is the encoded identity, the credential store lists the stored accounts, a cold boot resumes the last used and can ask the tab which account, and a switch replaces the whole database worker | [account_boot.rs](../crates/connetto-web/tests/account_boot.rs), [browser_auth.rs](../examples/wasm-smoke/tests/browser_auth.rs) | |
| R43 | done | | The browser holds one handle on the local tier. The tier stays attached to the replica connection, the relay's standalone connection and `LocalTier` are gone, and a tab's device-private writes replay as statements against the attachment | [local_tier_one_handle.rs](../crates/connetto-web/tests/local_tier_one_handle.rs), [notes_fanout.rs](../examples/wasm-smoke/tests/notes_fanout.rs) | |
| R44 | done |  | A departure delete synthesized per subscriber and marked indirect, applied only while no surviving subscription covers the row | [loop_emu.rs](../crates/connetto-client/tests/it/loop_emu.rs) |  |
| R45 | done |  | The reconciliation fix bundle, the grace-period unsubscription, the cursor persist, the expired share key and the launch pin | [verified_topology.rs](../crates/connetto-client/tests/it/verified_topology.rs) |  |
| R46 | done |  | The wasm-smoke intermittent hang, the upstream wasm-bindgen finding and the local workaround that passes ten consecutive runs | [authenticated_boot.rs](../examples/wasm-smoke/tests/authenticated_boot.rs) |  |
| R47 | done |  | One helper per job, the client's auth, HTTP and worker helpers consolidated to one definition each |  |  |
| R48 | done | | A `TRUNCATE` empties the client's copy. The `TableTruncated` resync reason clears the table unconditionally, catchup abandons the replay on a truncate it meets, and the pending local writes reapply after every resync | [truncate_resync.rs](../crates/connetto-client/tests/it/truncate_resync.rs), [truncate.rs](../crates/connetto-test-harness/tests/it/truncate.rs) | |
| R49 | done | | The startup refusal for a policy shape whose withdrawal cannot reach the authorization store, the upstream finding, and the deletion of the re-run machinery the refusal made unreachable, with the shape booting again after R86 adopted the upstream repair | [openfga.rs](../crates/connetto-server/src/openfga.rs) | |
| R50 | done | | The row-level-security policy answers the write question from the database. `RlsAuth::may_write` asks the two verbs a share can certify through a locking read, refuses the delete verb where the schema writes a separate rule, and maps a failed question to `ShareError::WriteUndecidable` | [rls_write_question.rs](../crates/connetto-server/tests/it/rls_write_question.rs) | |
| R51 | done | | The native Apple gate on the two keychain items, gated through `AccessPolicy::RequireUserPresence` behind one shared `LAContext` unlock per launch, macOS joins the protected store under the provisioned app, and custody reports `Verified` | [gate_recheck.rs](../crates/connetto-client/tests/it/gate_recheck.rs), [connetto-ios-proof.rs](../crates/connetto-test-harness/src/bin/connetto-ios-proof.rs) | [#112](https://github.com/connetto-rs/connetto-rs/pull/112) |
| R52 | done | | The native Android gate through the unlock-once store option. One approval opens the store until connetto locks it, the `BiometricPrompt` waits for the app's window focus, and a lost store key wipes and resyncs | [gate_recheck.rs](../crates/connetto-client/tests/it/gate_recheck.rs), [connetto-android-proof.rs](../crates/connetto-test-harness/src/bin/connetto-android-proof.rs) | [#112](https://github.com/connetto-rs/connetto-rs/pull/112) |
| R53 | done | | The Windows gate through the `HelloStore`, sealing each secret under a key derived from a Windows Hello passkey PRF. One approval unlocks each launch, and whether the credential survives a fingerprint re-enrolment is measured in R76 slice 9d | [gate.rs](../crates/connetto-client/src/keyring/gate.rs) | [#112](https://github.com/connetto-rs/connetto-rs/pull/112) |
| R54 | done | | Every demo carries every application-visible feature. The multi-account picker and switch, the passkey gate with its custody display, the session expiry warning, the over-limit signal, the several-provider `CONNETTO_OIDC_PROVIDERS` environment, and the parity fills | [browser_auth.rs](../examples/wasm-smoke/tests/browser_auth.rs) | |
| R55 | done | | Containerised test services and the CI that runs them. `testcontainers` behind the service-backed suites, `connetto-browser-stack` driving the browser gates, the `ci.yml` workflow as the gate, and `docs/devstack-local.md` deleted | [ci.yml](../.github/workflows/ci.yml) | |
| R56 | done |  | A device import that restores the device-private tier and the unsent writes from the archive, on the changeset format R26's plain databases gave way to | [local_import.rs](../crates/connetto-client/tests/it/local_import.rs), [apply_behaviour.rs](../crates/connetto-client/tests/it/apply_behaviour.rs), [local_import.rs](../crates/connetto-web/tests/local_import.rs) |  |
| R58 | done |  | A read tier that bounds a large read by a byte budget and a statement timeout and serves it in keyset pages | [read_ceiling.rs](../crates/connetto-server/tests/it/read_ceiling.rs) |  |
| R59 | dropped |  | The subscription ceiling and its paging, absorbed into R58 whose decision 6 made paging the mechanism |  |  |
| R60 | done |  | A `LIMIT n` request that registers as a computed read holding no replica rows, a raw unsubscribe that reclaims, and the default residual pass on a threshold | [subscription_translate.rs](../crates/connetto-server/tests/it/subscription_translate.rs), [retention.rs](../crates/connetto-client/tests/it/retention.rs), [residual.rs](../crates/connetto-client/tests/it/residual.rs) | [#2](https://github.com/connetto-rs/connetto-rs/pull/2) |
| R61 | done |  | A portability download of a person's rows and files from the registered queries, and the erasure recipe in chapter 20 | [portability.rs](../crates/connetto-server/tests/it/portability.rs), [numeric.rs](../crates/connetto-server/tests/it/numeric.rs) | [#288](https://github.com/connetto-rs/connetto-rs/pull/288), [#269](https://github.com/connetto-rs/connetto-rs/pull/269), [#271](https://github.com/connetto-rs/connetto-rs/pull/271), [#284](https://github.com/connetto-rs/connetto-rs/pull/284), [#313](https://github.com/connetto-rs/connetto-rs/pull/313), [#341](https://github.com/connetto-rs/connetto-rs/pull/341) |
| R62 | done |  | A refusal, at open and before every write, of a table with no declared primary key wherever changes are recorded | [key_requirement.rs](../crates/connetto-client/tests/it/key_requirement.rs), [key_requirement.rs](../crates/connetto-web/tests/key_requirement.rs) |  |
| R63 | done |  | The membership term planned and seeded through subql's own `describe_terms`, with the caller bind retired onto the self-seeding caller term | [membership_term.rs](../crates/connetto-test-harness/tests/it/membership_term.rs) |  |
| R64 | done |  | The file core, content-defined chunking, BLAKE3 identity and per-write AEAD with the chunk hash as AAD, agreeing byte for byte between the native and wasm paths |  |  |
| R65 | done |  | The file server, the storage backends, the two-phase upload, ticket serving, the sweep and the byte metering under the deployment's own visibility function |  |  |
| R66 | done |  | The file seam in connetto, `FileStore` deleted, the ticket request and grant on the control plane and the per-identity upload token bucket |  |  |
| R67 | done |  | The native file client, the encrypted chunk store, the manifests and outbox committed with the entry row, the boot integrity pass, the resolver and `tidy_content` |  |  |
| R68 | done |  | The browser file client, worker-owned encrypted OPFS with memory fallback, reference-counted object URLs and the version 3 archive that restores unsent content |  |  |
| R69 | done |  | Files in every demo, the offline and online photo flows on the browser stack and the native photos surface in the desktop demo | [photo_flow.rs](../examples/wasm-smoke/tests/photo_flow.rs), [photo_offline.rs](../examples/wasm-smoke/tests/photo_offline.rs), [photo_visibility.rs](../examples/wasm-smoke/tests/photo_visibility.rs), [demo_photos_flow.rs](../examples/dioxus-demo/tests/demo_photos_flow.rs) | [#28](https://github.com/connetto-rs/connetto-rs/pull/28), [#29](https://github.com/connetto-rs/connetto-rs/pull/29), [#30](https://github.com/connetto-rs/connetto-rs/pull/30), [#31](https://github.com/connetto-rs/connetto-rs/pull/31), [#34](https://github.com/connetto-rs/connetto-rs/pull/34), [#35](https://github.com/connetto-rs/connetto-rs/pull/35) |
| R70 | done |  | The backup and restore story, the `connetto-local-data` archive at version 6 with each queued write carrying its number's secret, the restore procedure in chapter 20 | [restore.rs](../crates/connetto-server/tests/it/restore.rs) | [#59](https://github.com/connetto-rs/connetto-rs/pull/59), [#131](https://github.com/connetto-rs/connetto-rs/pull/131), [#302](https://github.com/connetto-rs/connetto-rs/pull/302), [#306](https://github.com/connetto-rs/connetto-rs/pull/306), [#317](https://github.com/connetto-rs/connetto-rs/pull/317) |
| R71 | done |  | Linux key custody across a reboot, the sealed records, the sandbox keyring, the systemd credential, the wrap key and the desktop proofs | [linux_custody.rs](../crates/connetto-client/tests/it/linux_custody.rs), [desktop-proofs](../scripts/desktop-proofs/) |  |
| R72 | done |  | Clock discipline, the wall-clock rule per timer class, chapter 21 states it and X6 is closed |  | [#211](https://github.com/connetto-rs/connetto-rs/pull/211), [#212](https://github.com/connetto-rs/connetto-rs/pull/212), [#214](https://github.com/connetto-rs/connetto-rs/pull/214), [#215](https://github.com/connetto-rs/connetto-rs/pull/215) |
| R73 | done |  | Failover verification, every cursor carries its timeline and one past where that timeline ended resyncs, X7 closed | [failover.rs](../crates/connetto-server/tests/it/failover.rs) | [#55](https://github.com/connetto-rs/connetto-rs/pull/55) |
| R75 | done |  | The per-device applied frontier, the writer frontiers, the courier dedup on the same frontier and the audit records | [writer_frontier.rs](../crates/connetto-server/tests/it/writer_frontier.rs), [courier_frontier.rs](../crates/connetto-server/tests/it/courier_frontier.rs), [mutation_replay.rs](../crates/connetto-client/tests/it/mutation_replay.rs) | [#196](https://github.com/connetto-rs/connetto-rs/pull/196), [#197](https://github.com/connetto-rs/connetto-rs/pull/197), [#199](https://github.com/connetto-rs/connetto-rs/pull/199), [#200](https://github.com/connetto-rs/connetto-rs/pull/200), [#206](https://github.com/connetto-rs/connetto-rs/pull/206) |
| R81 | done |  | The aggregate read time bound, the seed plans, the re-execution bound and the caller-assigned read ceilings | [read_ceiling.rs](../crates/connetto-server/tests/it/read_ceiling.rs) |  |
| R82 | done |  | Grouped and re-executed delivery, the grouped seed, the keyed deltas and the demotion at the wire | [grouped_wire.rs](../crates/connetto-server/tests/it/grouped_wire.rs) |  |
| R83 | done |  | The client resting table, the aggregate resting state keyed on the query identity | [loop_emu.rs](../crates/connetto-client/tests/it/loop_emu.rs) |  |
| R84 | done |  | Keyed and row-shaped handles, the typed aggregate handles on the client | [grouped_live.rs](../crates/connetto-client/tests/it/grouped_live.rs) |  |
| R85 | done |  | Per-viewer RLS re-execution, the computed subscription's per-viewer identity on the reader pool | [rls_computed.rs](../crates/connetto-server/tests/it/rls_computed.rs) |  |
| R86 | done |  | The re-run path, the boot refusal asks the real question and the composite-join grading is adopted | [openfga_live.rs](../crates/connetto-server/tests/it/openfga_live.rs), [grant_withdrawal.rs](../crates/connetto-test-harness/tests/it/grant_withdrawal.rs) |  |
| R87 | done |  | The per-identity commit quota answered `507` under a per-uploader advisory lock, the cached deployment storage and bandwidth ceilings answered `503` with `Retry-After`, the per-day `_cfs_traffic` ledger, one warning and one error per crossing and the client treating `507` as permanent and `503` as transient | [quotas.rs](../crates/connetto-file-server/tests/it/quotas.rs) | [#46](https://github.com/connetto-rs/connetto-rs/pull/46), [#47](https://github.com/connetto-rs/connetto-rs/pull/47), [#49](https://github.com/connetto-rs/connetto-rs/pull/49) |
| R88 | done |  | The mobile build of a demo, the dioxus demo on Android and iOS through the proof drivers, the emulator and simulator legs on the Tauri demo | [connetto-android-proof.rs](../crates/connetto-test-harness/src/bin/connetto-android-proof.rs), [connetto-ios-proof.rs](../crates/connetto-test-harness/src/bin/connetto-ios-proof.rs) | [#58](https://github.com/connetto-rs/connetto-rs/pull/58), [#60](https://github.com/connetto-rs/connetto-rs/pull/60), [#66](https://github.com/connetto-rs/connetto-rs/pull/66), [#73](https://github.com/connetto-rs/connetto-rs/pull/73), [#165](https://github.com/connetto-rs/connetto-rs/pull/165) |
| R89 | done |  | A failing re-execution read ends its subscription, the `ReadFailure` three classes with the one retry for the codeless error | [reexec_failure.rs](../crates/connetto-server/tests/it/reexec_failure.rs) | [#51](https://github.com/connetto-rs/connetto-rs/pull/51) |
| R90 | done |  | The browser's refresh token in an `HttpOnly` cookie, the websocket keeps its bearer, the cross-site refusal proven |  | [#53](https://github.com/connetto-rs/connetto-rs/pull/53) |
| R91 | done |  | The bot registry and the list cap, the five registry tables, the four `AS RESTRICTIVE` cap policies per synced and registry table and the registration over the connection's challenge | [bot_cap.rs](../crates/connetto-server/tests/it/bot_cap.rs), [bot_registration.rs](../crates/connetto-server/tests/it/bot_registration.rs) | [#152](https://github.com/connetto-rs/connetto-rs/pull/152), [#153](https://github.com/connetto-rs/connetto-rs/pull/153), [#154](https://github.com/connetto-rs/connetto-rs/pull/154) |
| R92 | done |  | Synced tables carry no local references, the DDL strips `REFERENCES` and `FOREIGN KEY` while the tier keeps them and the foreign keys stay enforced | [hardening.rs](../crates/connetto-client/tests/it/hardening.rs) | [#167](https://github.com/connetto-rs/connetto-rs/pull/167), [#168](https://github.com/connetto-rs/connetto-rs/pull/168), [#169](https://github.com/connetto-rs/connetto-rs/pull/169) |
| R93 | dropped |  | Absorbed into R103, its two decisions carried there as amended |  |  |
| R94 | done |  | One client builder per platform, the durable and the anonymous builders with the compile-fail gates | [builder_lifecycle.rs](../crates/connetto-client/tests/it/builder_lifecycle.rs) | [#110](https://github.com/connetto-rs/connetto-rs/pull/110) |
| R95 | done |  | Share keys on a running client, the mid-session add, remove and expiry with no reconnect, natively and in the browser | [share_keys.rs](../crates/connetto-server/tests/it/share_keys.rs) | [#177](https://github.com/connetto-rs/connetto-rs/pull/177), [#180](https://github.com/connetto-rs/connetto-rs/pull/180), [#181](https://github.com/connetto-rs/connetto-rs/pull/181), [#182](https://github.com/connetto-rs/connetto-rs/pull/182), [#219](https://github.com/connetto-rs/connetto-rs/pull/219), [#231](https://github.com/connetto-rs/connetto-rs/pull/231) |
| R96 | done |  | One server builder, the parts mount into the application's own router and the binary and both stacks serve on one port | [lifecycle.rs](../crates/connetto-server/tests/it/lifecycle.rs) | [#122](https://github.com/connetto-rs/connetto-rs/pull/122) |
| R97 | done |  | A desktop login that brings the app window back to the front and closes the loopback tab where the browser allows it |  | [#114](https://github.com/connetto-rs/connetto-rs/pull/114), [#125](https://github.com/connetto-rs/connetto-rs/pull/125) |
| R98 | done |  | One schema contract, `ConnettoSchema` with its members and the startup preflight that refuses a missing member | [builder_coverage.rs](../crates/connetto-server/tests/it/builder_coverage.rs) | [#126](https://github.com/connetto-rs/connetto-rs/pull/126) |
| R105 | done |  | Answering a stolen account, the session record, the session surfaces, the revocation of a second session and the `auth_events` rows |  | [#262](https://github.com/connetto-rs/connetto-rs/pull/262), [#268](https://github.com/connetto-rs/connetto-rs/pull/268), [#274](https://github.com/connetto-rs/connetto-rs/pull/274), [#283](https://github.com/connetto-rs/connetto-rs/pull/283), [#287](https://github.com/connetto-rs/connetto-rs/pull/287), [#289](https://github.com/connetto-rs/connetto-rs/pull/289) |
| R106 | done |  | Database failures classified by their SQLSTATE, the named transient list for reads, writes and the oplog, the `Deterministic` class and the `42501` refusal as `Unauthorized` | [reexec_failure.rs](../crates/connetto-server/tests/it/reexec_failure.rs) | [#343](https://github.com/connetto-rs/connetto-rs/pull/343), [#356](https://github.com/connetto-rs/connetto-rs/pull/356) |

## Gaps

Defects and unowned risks with no phase yet. Each states what the code does and why it matters. A phase that takes one links it and the entry goes when the phase lands.

### G1

**Handshake liveness ignores the idle deadline.** `session_is_live` checks only `revoked` and the absolute deadline, in [`InMemoryAuthStore`](../crates/connetto-server/src/authn/store.rs) and in `live_session` in [schema.rs](../crates/connetto-server/src/authn/schema.rs), while the refresh path checks both. A session past its idle window passes the handshake until its access token expires.

Fixed in one pull request with [G2](#g2), by checking the idle deadline in both stores, proven by a session past its idle window refused at the handshake.

### G2

**The refresh lock map is never pruned.** `AuthService::refresh_locks` in [service.rs](../crates/connetto-server/src/authn/service.rs) gains an entry per session that ever refreshed and keeps it for the life of the process.

Fixed with [G1](#g1), by holding the locks weakly and pruning dead entries on insert.

### G3

**Browser tabs do not receive membership rows.** The relay in [relay.rs](../crates/connetto-web/src/relay.rs) neither forwards `MembershipOpened` nor opens tab-level membership subscriptions, so a tab reading through a membership-shaped policy sees empty results.

Fixed in one pull request in `relay.rs`. The hub forwards `MembershipOpened`, mirrors membership tables into tab snapshots, live routing and resync, proven by a tab reading the demo's membership-shaped `team_notes_p` policy.

### G4

**Without OPFS the web client runs on an in-memory replica.** `ReplicaStorage::install` in [storage.rs](../crates/connetto-web/src/storage.rs) falls back to a memory VFS, so queued writes and the replica are lost on reload and nothing tells the user.

Decided. The client keeps the in-memory replica where OPFS is refused and reports the replica's persistence in its boot report, so the application can warn that a reload loses unsent writes, in one pull request. A durable fallback is [R111](#r111-a-durable-replica-where-opfs-is-refused).

### G5

**A content ticket outlives the session that minted it.** Every fetch rechecks the file's visibility to the ticket's caller (`file_visible_to_caller` in [serve.rs](../crates/connetto-file-server/src/serve.rs)), so a revoked share, a revoked grant or an erased row denies at once. What outlives the session is the caller's identity. After a logout or a revoked session, a cached ticket keeps reading the files that identity can still see until its expiry, `CONNETTO_CONTENT_TICKET_TTL_SECS` (default 3600 s) plus the server skew.

Taken by [R109](#r109-content-tickets-bound-to-their-session).

### G6

**Session lifetime does not follow the provider.** `RefreshLifetimes` runs a 336 h idle window, extended by connetto's own refresh, under a 2160 h absolute ceiling. A user deactivated at the provider keeps a session until one of those lapses, and a continuously active user is signed out at the ceiling.

Taken by [R110](#r110-a-session-bound-by-provider-staleness).

### G7

**Role-scoped policies are refused only by accident.** `rls2fga` turns every `TO` clause into `pg_role` facts nothing in connetto loads or watches, with a `PolicyRoleScope` note of severity `ActionRequired` that `diverges_from_database` excludes, so `Translated::of` accepts the policy. A probe at `rls2fga` 3004d6b86, ahead of the pinned 40437eb and not yet reproduced at the pin, found that without those facts a permissive scope narrows and a restrictive one binds nobody, so an `AS RESTRICTIVE TO app_admin` barrier the reader inherits hides every row in Postgres while OpenFGA admits the caller's rows. Boot fails later, in `build_write_side`, because connetto never calls `pg2sqlite`'s `with_session_user_role` and `pg2sqlite` refuses any role-scoped policy. That refusal is untested, its message names a builder call a deployment author cannot act on, and by reading the code it comes after `build_authorization` has written the model and facts to OpenFGA.

Taken by [R108](#r108-caller-kinds-as-database-roles), whose decision 4 settles each `TO` clause against connetto's caller roles and whose first slice moves every startup refusal before OpenFGA.

### G8

**The two translators disagree, and only `pg2sqlite`'s refusals keep OpenFGA from being wider than Postgres.** Each translator owns its own answer to which policies apply to a caller, what `current_user` means, how a session setting is typed and split, which privileges gate a command, and what to refuse. A probe of one schema in Postgres, the replica and OpenFGA found OpenFGA wider in three cases. It ran `pg2sqlite` 963e08927, the pinned revision, and `rls2fga` 3004d6b86, ahead of the pinned 40437eb, so the `rls2fga` side of every case below awaits reproduction at the pin. `owner = current_user` is read by `rls2fga` as the caller with no note, where Postgres compares the pool role. A policy splitting `app.subjects` on another separator is refused by `pg2sqlite` and accepted by `rls2fga`, which grants rows Postgres hides. The restrictive role scope of [G7](#g7). Narrower or contradictory answers also exist. An `UPDATE` moving a row out of the caller's `SELECT` policy is refused by Postgres, applied by the replica, and allowed by `rls2fga` when `USING` and `WITH CHECK` differ. A `::uuid` cast over an untyped `pg2sqlite` mapping is dropped, hiding every row on the replica. `->>` on a non-string `jsonb` value compares an integer on SQLite. `pg2sqlite` gates writes on any write privilege while Postgres gates per command. connetto declares the caller twice, for `pg2sqlite` in `connetto-schema` and for `rls2fga` in `Translated::of`, in two vocabularies, and runs `build_authorization` before the `pg2sqlite` refusal, which at the probed revisions is the only guard. No test compares the engines on a deployment's policies. The fixes proposed are one caller declaration consumed by both translators, both refusals run before anything is written to OpenFGA, and a differential test with Postgres as the oracle seeded by those probes.

Taken by [R108](#r108-caller-kinds-as-database-roles), which has both translators adopt one set of sql-traits answers and whose proof includes the differential harness.

### G9

**The oplog has no byte bound.** `OplogConfig` bounds entries (1M) and age (72 h). Each entry stores the compressed patch, so wide rows reach gigabytes before either bound prunes, on the heap for `InMemoryOplog`.

Decided. The byte bound ships ahead of the rest of the shared-frame design, in one pull request, with a default budget of 1 GiB and pruning that logs which bound fired.

### G11

**Over-restriction notes reach no operator.** At the pinned `rls2fga`, a policy translated to no access, a self-referential read policy for one, hides every row without a diverging note, and connetto surfaces no other note. Postgres answers such a policy with `42P17` and `pg2sqlite` refuses it, so a deployment carrying one fails boot on the replica side today.

Taken by [R108](#r108-caller-kinds-as-database-roles) for connetto's half, whose first slice refuses before OpenFGA and reports the translators' notes. A diverging note from `rls2fga` for a policy translated to no access is an upstream request.

### G12

**Snapshot priority is not scheduled.** `SubscriptionPriority` travels on the wire and the server delivers snapshots in arrival order.

Decided. `SubscriptionPriority` leaves the wire with a `PROTOCOL_VERSION` bump, in one pull request.

### G14

**The local gate and CI run different clippy.** The gate prescribes `+nightly`, the CI Check job runs `+stable`, so a nightly-only lint can land on `main` with CI green.

Fixed in one pull request, by a CI leg running the gate's clippy on a pinned dated nightly.

### G17

**A snapshot source's cursor contract is unverified.** The resume cursor is opaque to the client, so the client cannot compare two of them, and a stale but non-empty snapshot cursor would regress the resume point. The Postgres source returns the watermark of the commit its read sat at, which is `page_cursor` in [snapshot.rs](../crates/connetto-server/src/snapshot.rs), so production is safe, but no test pins that contract across sources. A regressed cursor widens the catchup or forces a resync.

Fixed in one pull request, by stating the rule on `SnapshotSource::snapshot_page` and a non-regression test of the watermark in `pg_async.rs`.

### G18

**The browser hub's upstream reconnect is never tested against a real server.** The fake-transport browser suites drive `hub_recover` in [relay.rs](../crates/connetto-web/src/relay.rs) through attach refusal, terminal close, rate limiting, resubscription and resync fan-out. No suite drives it across a real server restart, because the shared demo stack cannot be killed mid-run.

Fixed in one pull request, by a browser CI shard with its own restartable stack and a suite that restarts the server mid-run.

## Upstream dependencies

**No phase waits on an upstream merge.** A phase that needs a change in a dependency requests it upstream, then builds against the fork through a `[patch.crates-io]` entry pinned to a revision, and drops the entry once a release carries the change. A review that takes months therefore holds nothing, and an upstream finding lists no phase in its blockers. What follows holds only the requests still open, each checked against its upstream.

1. **R21, the page codec on both backends.** The route lives in [`rusqlite/rusqlite` discussion #1908](https://github.com/rusqlite/rusqlite/discussions/1908), open. The SQLCipher half is agreed and merged on both sides, `libsqlite3-sys` bundling `sqlcipher-src` and `sqlite-wasm-rs` carrying a `sqlcipher` feature, both pinning the same `sqlcipher-src` release. What remains open is where `sqlcipher-src` and `sqlite3mc-src` live and who maintains them, and the optional `bundled-sqlite3mc` feature for `rusqlite`, ready on a branch. The phase starts when the source crates have a home a published `connetto-client` can depend on, since a published crate cannot reach a `[patch.crates-io]` fork.

2. **R100, the standard ownership schema.** The row-level security checks the maintainer ordered first are open in `connetto-rs/sql-rules` as [#18](https://github.com/connetto-rs/sql-rules/pull/18), [#20](https://github.com/connetto-rs/sql-rules/pull/20), [#21](https://github.com/connetto-rs/sql-rules/pull/21), [#24](https://github.com/connetto-rs/sql-rules/pull/24), [#26](https://github.com/connetto-rs/sql-rules/pull/26) and [#29](https://github.com/connetto-rs/sql-rules/pull/29). The `rls2fga` request for translating a declared definer helper into `from parent` relations is not written yet, and R100's `Blocked on` names it.

3. **R107 slice 2.** The slice needs subql to pass pgoutput `Relation` messages on to consumers, and no request is filed yet. R107's `Blocked on` names the change.

4. **R108 slices 2 to 5.** sql-traits answering the deployment's caller contract, merged as [sql-traits #213](https://github.com/connetto-rs/sql-traits/pull/213), the privileges per command and column, and the policies that apply to an acting role, with `pg2sqlite` and `rls2fga` adopting those answers. `rls2fga` #93, #94 and #95 are held and rebuilt on them.

5. **The keyring store pins, R51, R52 and R53, done phases whose patches retire when a release carries the change.** The R51 pin waits on the shared-authentication change, [`apple-native-keyring-store` #26](https://github.com/open-source-cooperative/apple-native-keyring-store/pull/26), which stands on `rust-security-framework`'s typed authentication context, [#263](https://github.com/kornelski/rust-security-framework/pull/263) and [#264](https://github.com/kornelski/rust-security-framework/pull/264), all open, and the keychain crate cannot release while it depends on the unreviewed `security-framework` pull requests, so those decide when the patch goes. The R52 pin carries the unlock-once store option, requested as [`android-native-keyring-store` issue #16](https://github.com/open-source-cooperative/android-native-keyring-store/issues/16), open, and the release behind it waits on [`rust-mobile/ndk-context` #6](https://github.com/rust-mobile/ndk-context/pull/6) and [#7](https://github.com/rust-mobile/ndk-context/pull/7), open. The R53 pin, shared with R74's TPM signing key, waits on [`windows-native-keyring-store` #19](https://github.com/open-source-cooperative/windows-native-keyring-store/pull/19), open.

6. **Merged and pinned, holding no build.** `diesel`'s SQLSTATE stack, #5332 to #5336, and `diesel_async`'s half, [#306](https://github.com/diesel-rs/diesel_async/pull/306) and [#307](https://github.com/diesel-rs/diesel_async/pull/307), are merged on their `main` branches and unreleased, and the root workspace follows both through its `[patch.crates-io]` entries for R106. `diesel_async` `main` also carries #214, the async DSL bound to diesel's marked types, and `sqlite-diff-rs` 0.18.1 carries the adjustment that bound needed. `pg_walstream` #141 and #142, the startup metadata API and `AlignedSnapshot::capture`, are merged on `isdaniel/pg-walstream` `main` and unreleased, and the subql durable engine and replication reader, `LucaCappelletti94/subql` #207 and #208 and `connetto-rs/subql` #223 and #224, are merged on `connetto-rs/subql` `main` and unreleased. None of these gates a build step, and each pin retires when a release carries the change.

# Designs without a phase

Decided designs no phase builds yet. A phase that takes one moves it into its section.

## Shared frames and the oplog byte bound

Decided in R16 part B, which wrote no implementation phase. [G9](#g9) is the defect it answers, and the shared frame cuts the per-subscriber copies the send budget bounds.

**The unit is one change event, not one subscriber and not one subscription.** Every computation a change requires happens once per event. What remains per subscriber is a route lookup, a verdict entry and a socket write, and none of the three recomputes anything the event already produced. The reason the unit is the event rather than the subscriber is that a change event is the only thing in the system that happens once. A subscriber is a destination, not a computation.

The built rows of the per-event and per-subscriber tables are in [17-fan-out.md](../docs/architecture/17-fan-out.md). This design adds the frame row to the per-event table (one frame built, header and body together) and turns the per-subscriber socket write into one carrying shared bytes.

### The frame

A bulk frame becomes the tag, a short encoded header, then the compressed payload appended untouched. Two records already describe that shape. The bulk-plane row in [02-protocol.md](../docs/architecture/02-protocol.md) gives the encoding as Zstd-precompressed opaque bytes, and the module comment on `crates/connetto-core/src/messages/bulk.rs` says the payload is carried so the transport never re-compresses it. The code disagrees with both. `encode_bulk` runs `rmp_serde::to_vec_named` over a `BulkMessage` whose variants embed the payload as a field, so every send copies the payload into a MessagePack buffer and then again into the tagged frame.

The layout, in order, on a message-delimited transport where the frame boundary is the WebSocket frame.

| Field | Size | Content |
|---|---|---|
| Tag | 1 byte | The bulk tag |
| Header length | 4 bytes | Big-endian `u32` |
| Header | Header length | The encoded header |
| Payload | The rest of the frame | The compressed payload, untouched |

The header is a MessagePack-encoded enum mirroring `BulkMessage`'s variants with the payload field removed, so it carries the subscription handle and the cursor on a live patch, the handle alone on a snapshot patch, and the client sequence on a mutation patch. On a byte-stream transport `encode_bulk_framed` wraps the same three parts in the existing outer length, so the length-prefixed pair needs the identical split and nothing else.

**Consequences, all of them accepted.** Three decoder sites change, and all three are today the identical two lines that split the tag and call `decode_bulk`. `WebSocketTransport::recv` in `crates/connetto-core/src/transport.rs`, `BrowserSocket::recv` in `crates/connetto-web/src/lib.rs`, and the single `recv` of the `MessageTransport<S: MessageSink>` that serves both the broadcast transport and the port transport in `crates/connetto-web/src/frames.rs`. `decode_bulk` keeps its signature and performs the split internally, so those sites change only if they stop paying for a copy. `BulkMessage` stops being a plain serde round-trip, so the wire round-trip test is rewritten rather than adjusted. The upload path benefits identically, which was not the motivation.

**No browser client makes this more expensive.** None of the decoders inspects the payload. The one browser participant that reads a bulk payload structurally is the relay. `patch_tables` in `crates/connetto-web/src/relay.rs` decompresses each upstream patch once to learn which tables it touches, then re-frames it per tab. That decompression is per upstream patch and independent of frame layout, and the per-tab re-framing currently copies the payload once per tab, so the relay is a second fan-out with the same shape and the same saving available to it.

#### Copies per subscriber per event

| Stage | Today | After the split and the shared payload | After a shared frame |
|---|---|---|---|
| Clone into `MatchedPatch`, `Materializer::dispatch` | 1 | 0, shared handle | 0 |
| MessagePack encode embedding the payload, `encode_bulk` | 1 | 0, the body is not encoded | 0 |
| Concatenate into the tagged frame, `send_bulk` | 1 | 1 | 0, built once per event |
| **Total** | **3** | **1** | **0** |

### Subscription identity

**A data frame carries a server handle derived from the question, not the name the client chose.** The client keeps its own name and the subscribe reply maps it to the handle. This is ElectricSQL's shape handle and Zero's transformation hash expressed in connetto's terms, and it is what makes two clients' frames capable of being identical at all. A client-chosen name (`sub_id` in `crates/connetto-core/src/messages/subscription.rs`, documented as client-chosen and unique per session) guarantees they never are.

**It needs nothing upstream.** `subql`'s `RegisterResult` already returns `predicate_hash` and `created_new_predicate`, and `Materializer`'s `register_request` in `crates/connetto-server/src/materializer.rs` discards both, keeping only the subscription id. The signal is handed over on every registration and dropped one line later.

**What the client gives up. Nothing on the bulk plane.** The client never routes a bulk frame by the name. The `SnapshotPatch` and `LivePatch` arms of `ConnettoConnection::next_event` both apply the patch to the one replica, and the name only rides out as a label on `ClientEvent`. The relay already discards the upstream name, destructuring `ClientEvent::LivePatch { cursor, patchset_zstd, .. }` and choosing each tab's own subscription by table intersection. The one load-bearing use of the name on the receive path is a control frame, `FullResyncRequired`, which drives `clear_subscription_rows`, and control frames are untouched by this design. That function resolves the resyncing subscription and its siblings from the subscription set persisted in the replica, so the name has to match a stored subscription id.

### Frames shared across clients

**One frame per distinct pair of handle and event, cloned per socket.** A live frame's three parts are then all shared. The handle is derived from the question. The cursor is already per event rather than per client. `Materializer::dispatch` stamps every `MatchedPatch` with the same value taken from the event's checkpoint. The payload is already computed once. So the whole frame, tag included, is identical for every allowed subscriber on that handle, and a subscriber the verdict denied simply does not receive it.

**Nothing in this mechanism waits on R5b.** The shape it needs is R5a's and is already in the tree. `may_see` takes one row and every watcher and returns one verdict each, so the partition over subscribers exists today. R5b makes that partition cheap enough for the sharing to pay. For connetto's own policy shape the round-trip count is zero, so the work saved by sharing one frame dominates. No permission-class identifier has to be invented, which the sequencing record expected to be the blocker.

Sharing applies to the allowed subset, and its value is proportional to how often that subset has more than one member.

#### Reaching zero copies

The dependency bump is necessary and not sufficient. `tungstenite` 0.28 changes `Message::Binary` to take `Bytes`, and that version is already in this workspace's lock file through `dioxus-devtools`, so nothing in the ecosystem blocks it. But `Message::Binary` takes one contiguous region, so a per-subscriber header ahead of a shared body still forces a per-subscriber concatenation that copies the body. **Zero copies therefore needs the handle, the shared frame and the transport bump together**, at which point the one frame is built once per event, held as one `Bytes`, and cloned per socket for a refcount bump.

Sending the header and the body as separate WebSocket continuation fragments would avoid the concatenation without the handle. Declined. `tungstenite` does not expose fragmentation through `Message`, and it would put a cross-frame sequencing rule on the wire to save a copy that the handle removes anyway.

### Payload ownership

**The compressed payload is held by shared reference, as `Arc<[u8]>`, with no new dependency.** The copy table above is where the shared payload stops being one copy fewer and starts being the thing that makes a frame shareable at all. `subql`'s `pgoutput_patchset` returns an owned `Vec<u8>`, so wrapping it costs nothing and changes no `subql` signature. The type flows through `MatchedPatch`, the bulk message variants, and the `Transport` trait.

`Arc<[u8]>` is precedent here rather than a new convention. `ClientEvent::LivePatch` in `crates/connetto-client/src/lib.rs` already carries `patchset_zstd: Arc<[u8]>`, for exactly this reason, so a relay can forward without re-encoding.

It does not foreclose the zero-copy send. `bytes::Bytes::from_owner` accepts any `AsRef<[u8]> + Send + 'static` owner and takes ownership without copying, and `Arc<[u8]>` satisfies that bound. So after the transport bump the frame is converted once per event and cloned per socket, and `connetto-core`'s public types never grow a `bytes` dependency.

### The byte bound

`OplogConfig` gains a byte bound, on by default, and connetto logs which bound pruned. The window is bounded today by entry count and age alone (one million entries or seventy-two hours, [06-reconnect.md](../docs/architecture/06-reconnect.md#retention-window)), and neither notices row width. At the thirty-nine bytes R0 measured for a two-column row a full window of patches is tens of megabytes, and at a few kilobytes per row it is gigabytes. The failure mode of a bound set too small is extra full snapshots rather than lost data, because a resume point that has fallen off the front of the log already draws `FullResyncRequired`, which is a working path. That is precisely why the log line is part of the decision and not a nicety. Extra snapshots look like a client defect and are actually a retention setting. The default value has no measurement behind it and is a starting point, not a finding.

### The cost

| Change | Where |
|---|---|
| Bulk frame becomes tag, header length, header, body | `codec.rs`, `messages/bulk.rs` in `connetto-core` |
| Header type mirroring the bulk variants without the payload | `messages/bulk.rs` |
| Three decoders keep their shape, `decode_bulk` splits internally | `transport.rs`, and `lib.rs` and `frames.rs` in `connetto-web` |
| Length-prefixed pair takes the same split, wire round-trip test rewritten | `codec.rs`, `connetto-core/tests/wire.rs` |
| Payload as `Arc<[u8]>` through `MatchedPatch`, the bulk variants and `Transport` | `materializer.rs`, `connetto-core` |
| `Transport` gains a send taking an already-framed shared buffer | `traits.rs` in `connetto-core`, every transport impl |
| Server handle derived from `predicate_hash`, returned on the subscribe reply, carried in the header | `materializer.rs`, `session.rs`, `messages` |
| One frame built per handle per event, cloned per socket | `session.rs` |
| `tokio-tungstenite` from 0.24, and the frame handed over as `Bytes` | `connetto-core/Cargo.toml`, `transport.rs` |
| `OplogConfig` gains a byte bound, and pruning names the bound that fired | `oplog.rs` |

Nothing goes upstream. The frame split is a breaking framing change, and `PROTOCOL_VERSION` (currently 2, `crates/connetto-core/src/version.rs`) bumps on breaking changes to `ControlMessage`, `BulkMessage`, framing or compression, so the new wire takes a version bump rather than riding on a frozen one.

# Phases

## R101: live-query lifetimes and waking a closed client

**Status.** IN PROGRESS. First of the redesign's phases by the maintainer's priority. Step 0, the device experiment, is done and its findings are decision 2. Step 1, the design, is done and agreed. Step 2, the build, is in progress.

**Blocked on nothing upstream.** The server's durable engine registers a waking query while the stream runs through `pg-walstream`'s `AlignedSnapshot::capture`, merged as `isdaniel/pg-walstream` #142, replacing the stack #136 to #138, and relies on subql's durable engine changes (connetto-rs/subql #207 and #208, #223) and its move to `pg-walstream`'s replication reader (connetto-rs/subql #224). `main` follows all of them through its git pins, and the pins govern only when they retire.

**Changes state.** Client tables, the local declaration store (caller binding, name, typed key, query specification, marker, rendering policy, observed revision and pending operation or retirement intent) and the notification store (notification keys, trusted sender public keys, opaque ownership bindings and replay state). Server tables, a deployment-owned table set holding declarations, verified ownership facts, endpoint bindings and generations, per-instance dirty, notified and acknowledged high-water marks, detector capacity failures, delivery claims and replay checkpoints, and the durable engine's receipt row with its changed-instance and failure tables and a first-receipt flag in connetto's one-row commit table.

### Purpose

An app wants updates while a screen element exists, while it runs, or even when closed, and a closed app must be woken when a query it cares about changes. Bots use the same mechanism in R103.

### Decisions

1. **Three lifetimes per live query.** The first two are Rust and Dioxus scope (a query held by a component, a query held at the app root). The third outlives the process, so it is a named registration the server keeps and evaluates while the client is away, woken per parked query and never per table.
2. **The API comes out of a device experiment**, in a Dioxus app on an iPhone and on the Galaxy M52, swiped away. On iOS a closed app is reached only by an alert notification, rewritten within about 30 seconds by a notification service extension, while background notifications are throttled and dropped for force-quit apps. On Android a data message wakes the app's service for about 10 seconds. The maintainer expects upstream findings, each through the upstream procedure. The reconnect storm the experiment measured, 1,000 apps each missing one change, is addressed by building each catch-up patch once and matching it only against the reconnecting consumer, then sharing one catch-up walk across reconnecting subscriptions.
3. **The iPhone and Android channels belong here.** Web Push here or in a later phase as the experiment shows. The bot's wake URL is R103's.

### Agreed inputs

One query entry point uses the `Element`, `App`, `Kept` and `Waking` markers. Durable declarations have stable names, and `release` ends a durable declaration. `Kept` persists on the client, and `Waking` additionally leaves a registration at the server. The server stores waking registrations in Postgres and restores eligible registrations before replaying the change stream. Notification rendering is chosen per waking declaration, the modes being application code, server-rendered encrypted text and fixed text. A notification key is separate from the biometric-protected replica key, and notification processing does not unlock the replica. `pushups` owns platform registration, token delivery, received-message delivery and background entry points. Connetto owns kept-query authorization, persistence, wake coalescing, server-side sending and payload semantics. The native extension and presentation hooks belong to `pushups`, and Connetto implementation waits for the real API contract rather than supplying parallel native glue.

### The design decisions

1. **Eligibility after login renewal expires.** A login-bound waking registration is eligible only while its login is unrevoked and refreshable. Wake delivery stops at the earlier refresh idle deadline or absolute deadline, a successful refresh slides the idle deadline within the absolute ceiling, and re-enabling delivery after expiry requires a fresh authenticated registration.
2. **Notification scheduling while the app is present.** System wakes are suppressed only while an explicitly foreground client is receiving updates. Background and disconnected clients remain eligible, including a background client whose transport is still open.
3. **The mobile notification rendering boundary.** The server-encrypted iOS mode needs a Rust rendering hook in the notification service extension, and the Android modes need a presentation boundary for the content produced by a data-message handler. Connetto retains encryption, query interpretation, notification keys and server-side sending.
4. **Coalesced alert text.** One device wake combines the changed declarations' fixed or server-rendered text into one alert within provider limits. Every changed declaration name is retained for catch-up, including declarations whose text exceeds the visible alert's budget. The application controls its own `on_wake` output.
5. **Caller coverage.** `Waking` supports anonymous callers, capability-only callers, signed-in callers and signed-in callers holding capabilities. No-login ownership expires with the signed operational resume lease, whose default is 14 days. Capability expiry and current policy remain independent authorization bounds.
6. **Keyed durable instances.** A declaration type supports independently retained keyed instances. `release(Album { id: 42 })` ends that instance, and `release(AssignedToMe)` ends a unit declaration through the same method.
7. **Device-only recipient private keys.** Notification decryption uses device-only recipient private keys with RFC 9180 authenticated HPKE. The server registers each recipient's public encryption key, the notification key remains independent of the replica key, and provider-visible fallback content is non-sensitive.
8. **Delayed release after replacement.** A release targets the observed declaration revision. If another process has replaced that instance before the release reaches the server, the server preserves the replacement and reports a revision conflict, and the client does not retry against the newer revision automatically.
9. **Durable lifetime replacement.** The latest marker defines the durable instance's lifetime. Re-declaring `Waking` as `Kept` retires its server wake registration without releasing the client declaration, and re-declaring `Kept` as `Waking` enables its server registration.
10. **Coalescing duration.** The default per-device coalescing window is one second, configurable by the deployment. The first pending change starts the window, and further writes do not extend its deadline.
11. **Mixed-mode application rendering.** When the replica is readable, the application callback controls the complete combined alert and may keep, replace or suppress it. While the replica is locked, only fixed or server-rendered text is available.
12. **Verified suppression capability.** App-controlled no-alert results require real platform suppression support. On Apple platforms, enabling that result requires verified filtering entitlement approval and SDK suppression support, and missing capability is an explicit error.
13. **Release-time Apple suppression assurance.** Apple suppression requires signed-artifact release checks and real device acceptance. Runtime capability remains explicitly unverified, and the SDK reports `SuppressionRequested` without claiming observed suppression.
14. **Every intervening change survives recovery.** A waking query that changes and returns to its earlier answer during server downtime must still produce a wake. Recovery requires replayable query state or equivalent durable change evidence.
15. **Exact committed-result evidence.** Only actual query-result changes create unread evidence across all requested tiers. Source invalidation may schedule re-evaluation and cannot manufacture unread evidence.

### The server's durable engine

The server hosts one `subql::DurableEngine` beside the live `Materializer`. It admits waking registrations, folds every complete source transaction into them, persists each receipt in the deployment's Postgres before the source is acknowledged, and restores from the stored receipt at boot. Wire declarations, the client API, notification keys and provider sending are later steps. This step exposes a server-side registration call that the protocol step wires.

- `DurableContext.source` names the stream generation, the database system identifier and the slot name. The timeline stays out, because a failover onto a standby keeps every commit position, and the existing `check_before_stream` already verifies that the new timeline's history contains the last ingested commit and reports a gap when it does not. `authority` names the deployment's OpenFGA store.
- The ingest loop buffers a transaction's row events until its commit item, then calls `prepare_transaction(source, after, at, events)` once with the whole commit.
- The receipt persists in one Postgres transaction that compares and replaces the stored revision and writes the checkpoint, operation, changed instances, failures and source progress. Only then does the ingest loop record the commit and acknowledge the slot. A confirmed rollback retries the same preparation, and an unknown outcome reloads the stored receipt and restores it.
- A duplicate commit at or behind the published boundary answers `Ok(None)` and is acknowledged without a receipt.
- Quota exhaustion records the instance's failure in the same receipt and leaves unrelated instances running.
- Boot restores the engine from the stored receipt before the ingest loop starts and resumes the stream from the receipt's boundary. A receipt that is missing while the first-receipt marker is set fails startup instead of seeding from current answers, and a receipt whose context no longer matches, and a feed that resumed past what it delivered, retire every waking query.

One new member of the deployment-owned table set, checked at startup like the others, holds the single current receipt row, its revision, boundary, checkpoint bytes and last operation. Changed-instance evidence and failures persist in their own tables keyed by instance, which the later steps read for catch-up and wakes.

1. **`DurableContext.authority` is the OpenFGA store identity.** Only moving the deployment to another authorization store erases waking queries. A policy or model deploy keeps them, and each declaration is authorized again at restore and at every acknowledgement. The model id stays out of the context.
2. **A declaration that no longer authorizes, or no longer fits the catalog, is refused alone at restore.** subql's `DurableEngine::restore` changes to restore every other instance and report the refused ones with their cause, instead of returning the first error. An envelope, context, revision or boundary mismatch and an undecodable or inconsistent record still fail the whole restore, since those mean the stored receipt is wrong. The restored engine leaves the refused instances out and keeps the stored revision, so its next receipt drops them. connetto records each refusal as a retirement in its own storage before the ingest loop starts.
3. **A first boot is told apart from a lost receipt by a marker beside the commit row.** Persisting the first receipt also sets a flag in connetto's existing one-row commit table, in the same transaction. At boot, no receipt and no flag is a first boot that creates the empty engine at the stored commit position, while no receipt with the flag set fails startup.
4. **A feed gap, or a stored receipt from another cluster, slot or OpenFGA store, retires every waking query and starts a fresh engine.** connetto records each instance in its own instance table as retired with the cause, so the sending step can tell its owner, then creates an empty engine at the resume position and persists its first receipt. Owners register again, the way clients resynchronise after a gap.

**Capturing bootstrap inputs at the published boundary (option 1, taken).** `prepare_register` needs every dependency table's complete rows exactly at the engine's published boundary, and the stream keeps moving while Postgres already holds commits past it. A plain read at the current snapshot would include those commits, and the engine would fold them a second time when the stream delivers them. Option 1 creates a temporary logical slot with `SNAPSHOT 'use'` inside a `REPEATABLE READ` transaction and reads the dependency tables there, and the ingest loop then stops after the last commit below the slot's consistent point, so the boundary matches the snapshot, admits the instance, drops the slot and resumes. It is the only option that keeps writes and runtime registration available. `AlignedSnapshot::capture` and `EventType::commit_lsn` carry it upstream, `capture` takes a plain connection string, runs its blocking setup off the async runtime, resolves each table through the publication's column lists and row filters, and refuses an unpublished table with a `Config` error naming it. It adds a replication-connection requirement to the deployment and a per-registration slot cost.

**Build order.** Each part lands with its tests before the next. Step 1, each source event applied once across retries, is open in the work with its stacked follow-up. Step 2, the storage, is `PgDurableStore` beside `PgOplog`, with `ensure_schema` creating the receipt row table, the instance table, the unread evidence table and the failure table, and adding the first-receipt marker column to the commit table, preflight requiring them like the oplog's. Then boot, restoring before the ingest loop with first boot, lost receipt, refused instances retired, and context mismatch retiring every instance. Then ingest, buffering each transaction's events beside `EventProgress`, preparing at the commit item, persisting through the store in place of `record_commit`, then acknowledging. Then acknowledge and remove, server-side calls taking the owner's current authority, persisted through the same store. Then registration, the server-side call that admits an instance at the published boundary through the aligned capture, using the in-order keepalive position of subql's `pg-walstream` replication reader.

### Steps

1. Done, the design, agreed as the decisions and inputs above.
2. In progress, the build, each source event applied once across retries first, then the server's durable engine, then the wire declarations, the client API, the notification keys and provider sending.

### Done when

The acceptance below passes against real Postgres and a real client. Use real Postgres and a real connetto client with a controlled provider endpoint. Register one waking declaration, disconnect, restart the server, change relevant rows, observe one coalesced wake, reconnect and catch up, release the declaration, and prove a later relevant change produces no wake. The decision 1 contract is exercised with a controlled clock. No production mock or placeholder sender is part of this deliverable.

The decision 14 recovery proof repeats the walk for row, scalar and grouped answers with an insertion and compensating deletion while the server is stopped. The final answer equals its earlier value, and the instance still appears in one coalesced wake and the durable unread set. The exact detector requires a tied-minimum regression. Deleting one of two equal minima leaves the scalar instance clean. A companion query that selects item identity observes the actual row removal and becomes unread.

The required boundaries include a change during an in-flight send, an uncertain provider result, token and recipient-key rotation, stale release after replacement, permission withdrawal, foreground progress loss, login idle and hard deadlines, capability expiry, caller change and policy movement during downtime. A pruned-oplog full resync clears only its proved applied boundary.

The ingest proof injects computed-read, authorization, oplog, dirty-write, checkpoint and source-ack failures independently. Healthy fold results and successful sibling reads survive, each source event applies once per engine lifetime, and every process restart resumes from a complete durable boundary. No passing compiler model substitutes for these runtime checks.

The capacity proof exhausts one running query's total detector-state quota while another query continues. It checks the durable failure and last valid boundary, absence of false changed-answer evidence, preservation of prior unread evidence and source-progress blocking when failure-record persistence fails.

iPhone background execution, extension limits, notification presentation and locked-device key access pass the device acceptance the design names.

---

## R99: bot login and lifetime

**Status.** NOT STARTED. Built after R101 by the maintainer's priority.

**Blocked on nothing** since R91 merged.

**Changes state.** Client tables, none. Server tables, the audit contract's `connetto_auth_op` enum gains `BotRegistered`, `BotListRegistered`, `BotKeyRotated`, `BotRemoved` and `BotLoginRefused` (decision 6), the registry cache answering the handshake from memory. Wire messages, the bot's login answer to `Challenge`, its bot id, the installation or none and its signature (decision 1), and the "awaiting acceptance" refusal and close reason (decision 2). Archive format, none.

### Purpose

A registered bot connects, for one installation or as itself, proves its key, and lives under the state table below. A bot proves itself at connect, SSH style, and never goes through a provider.

### Decisions

1. **Key login at connect.** On a new connection the server sends a fresh random challenge, and the bot answers with its bot id, the installation it connects for or none, and its signature over the challenge with its registered key. The server verifies against `connetto_bots.public_key` from the registry cache, which it seeds at boot and keeps current from the change log, so the handshake reads no table. A challenge lives only on its own connection and no state is shared between server processes, so a signature cannot be replayed. No route, no access token, no session id, no replay memory.
2. **An installation connection binds `app.user_id` to the installation id** and the cap setting to its effective list. The effective list is the installation's accepted version plus the optional entries its admin accepted. When the bot registers a later version whose required entries grew past what an installation accepted, that installation's logins are refused with a distinct "awaiting acceptance" reason and its open connection closes with them. A list that only adds optional entries or narrows does not, and a narrowing applies at once, by the cache updating the setting on the next statement and closing the subscriptions over a table that left the list.
3. **One connection per installation**, a newer one evicting the older, as the one-connection-per-handle rule of chapter 12 does for sessions, so a reactor that must run once runs once. Different installations of one bot may run in different processes.
4. **Removal, a ban on the bot or on one installation, a deleted bot and a key rotation close the affected connections at once.** Deleting an installation closes its connection with `SessionRevoked`, a ban of the bot or of one installation id refuses the logins it names and closes the connections, and deleting the bot refuses everything and closes its connections. A key rotation registers the new key through a fresh one-time code and stops the old one at once, so the connections the old key held close and re-login under the new key. A woken bot costs one signature per wake and catches up from its cursor, kept per installation. R19's throttle counts failed logins per bot id after the signature verifies and per connection before it.
5. **Metering.** An installation connection bills the owner it serves (`owner:<id>`), a bot-level connection the bot's owner. The bot identity holds no bucket.
6. **Audit.** `BotRegistered`, `BotListRegistered`, `BotKeyRotated`, `BotRemoved` and `BotLoginRefused`. Successful logins are not audited, since a woken bot logs in on every wake. Adding and accepting are the installation row's own changes.

### Lifetime, every event against every situation

| Event | No connection | Installation connection open | Bot-level connection open |
|---|---|---|---|
| Admin adds the bot | next login for it succeeds | n/a | the bot's live query on its installations shows the new row |
| Admin removes it (deletes installation and membership) | login refused, unknown installation | closed with `SessionRevoked` | the row leaves its live query |
| Bot registers a new list | logins refused if required entries grew and not accepted | closed with "awaiting acceptance" in that case, else continues | the request shows as pending |
| Admin accepts the new list | logins succeed under the new list | n/a (was closed) | the acceptance shows |
| Rotation code registers a new key | old key refused | closed, re-login with the new key | closed, re-login with the new key |
| Operator bans the bot | every login refused | every connection closed | closed |
| Operator bans one installation id | that installation's logins refused | closed | unaffected |
| Owner of the bot deletes it | everything refused | every connection closed | closed |
| Membership row deleted, installation row kept | login succeeds and sees nothing | rows leave through R7 | unaffected |
| Store unreachable | logins refused, the bot retries | continues, closures queue under `DeliveryPaused` | same |

### Proof

Every row of the table above against a real server. A replayed signature on a second connection is refused, a newer connection evicts the older, installation traffic lands on its owner's bucket and never on the bot's, and the cache answers the handshake with the database's registry tables locked.

### Done when

A bot logs in for any of its installations and as itself, and every event of the state table has its tested outcome.

---

## R103: the bot runtime and template

**Status.** NOT STARTED. R93 is absorbed here, its two decisions carried over as amended, and R71's headless custody, which it needed, is built.

**Blocked on R99 and R101**, R91 having merged.

**Changes state.** Client tables, one encrypted file replica per installation, named from the installation id, with each reactor's previous answer stored beside it (decisions 2 and 3). Server tables, the wake URL on `connetto_bots`, which R91 left empty for this phase. Wire messages, the server's public key on the registration answer, which the bot pins to verify wake notes (decision 4), the notes themselves arriving over HTTP. Archive format, none.

### Purpose

Writing a bot is implementing one trait and some reactors, and the `connetto-bot` crate runs it for every installation, connected or woken. A bot is a program with no screen that holds a query, is told when it changed and writes values back. It runs on the same `ConnettoClient`, the same live handles and the same `with_conn` write path as a screen, and the pieces it needs live in a crate UI apps never compile.

### Decisions

1. **A `connetto-bot` crate on top of `connetto-client`** with the bot trait (name and list, by hand until synql generates it), the `register --code` entry point reading the operator's key file, the installation loop, reactors and the wake receiver. The wake receiver's web server stays out of the client.
2. **One encrypted file replica per installation**, named from the installation id and wiped on removal (R93's decisions as amended), opened for every woken bot and for any bot declaring device-local tables, in memory for a connected bot without them.
3. **Reactors are parked queries per installation.** The runtime persists each one's previous answer beside the replica and passes the diff and the current answer, exact across restarts and wakes, at least once. A run that fails is retried with the same diff, so a reactor's writes tolerate a repeat.
4. **Two running modes**, connected or woken, with the same reactors. The wake URL receives server-signed notes naming an installation and a parked query, never data, verified against the server key pinned at registration. The server coalesces per installation and retries with backoff, and a note that never arrives costs nothing, since the bot catches up whenever it next connects.

### Design

**The bot trait.** `name` and `list()`, each entry a table, its verbs and whether it is optional, written by hand until synql's macro generates it. The list is what registration sends.

**Registration and key.** The bot binary carries a `register --code <code>` entry point that reads the operator's Ed25519 key file, sends key and list (R91), and is the same path for a new list or a rotated key. The key never leaves the host. The server's public key, returned at registration, is pinned to verify wake notes.

**The installation loop.** A bot-level connection follows the bot's installations (R91 point 2). Each installation gets its own connection and its own replica, the file replica of R93 named from the installation id, and removal wipes that replica. A bot runs connected, holding every installation's connection, or woken, connecting per installation only when a wake note or a restart asks for it. An installation refused for "awaiting acceptance" (R99) is parked and retried when the bot-level connection shows the acceptance.

**Reactors.** A reactor names a query and is registered as a third-tier, parked query of R101 for each installation, so a woken bot is called back when its answer changes. Each run gets the persisted diff and the current answer and writes through the installation's client, with `insert_awaited` to wait for the server's verdict on its own write.

**The wake receiver.** An HTTP endpoint the operator exposes, verifying each note against the pinned server key, coalescing, and scheduling that installation's run.

**Testing and template.** The `testing` module gives the corner cases as named stages (a list change awaiting acceptance, removal mid-run, a restart between two runs, a wake for a removed installation, a repeated run after a failure, a refusal outside the list). `examples/bot-template` ships the scripted, semantics, property and bench targets and `mutants.toml`, copied rather than read, so a second bot written from it needs no change to `connetto-bot`.

### Proof

Two installations never see each other's rows on disk or in memory, a restart between two changes yields the exact diff, a wake reconnects only the named installation, a failed run repeats with the same diff, removal wipes the replica. The template's scripted, semantics, property and bench targets pass.

### Done when

A second bot written from the template by someone other than the author needs no change to `connetto-bot`.

---

## R100: the standard ownership schema

**Status.** NOT STARTED. The first design, ownership chained through `ownables` and read through a closure table, is superseded by the layered layout the maintainer chose. Its rules are held, since a review against the Postgres and OpenFGA sources found the layered draft only partly best practice, the maintainer wants best practice alone, and the rules are to be rewritten around definer helpers (below the decisions).

**Blocked on** sql-rules' row-level security checks, which the maintainer ordered first, since rls2fga may later lean on them for its own diagnostics. They are in work as `connetto-rs/sql-rules` #14, #16 and #17 (merged) and #18, #20, #21, #24, #26 and #29 (open). Then on rls2fga translating a declared definer helper into `from parent` relations, a request not yet written, rls2fga refusing to expand a `SECURITY DEFINER` body on purpose, since it sees rows the caller cannot. Then on pg2sqlite translating the definer helper's body to the client side, which refuses a function call in a policy there today, the fix standing on the `upstream/inline-sql-functions` branch with no pull request open. The layered draft's own rules diverge with `MembershipTableGuarded` on every rule reading a grant table, and without row-level security on the grant tables only the `connetto_installations` rules diverge.

**Changes state.** Client tables, none of connetto's, the standard document's tables syncing as a deployment's own. Server tables, none of connetto's, the document offering `organizations`, `teams`, `projects`, a grant table per level and the definer helpers for a deployment to adopt, with the policies of R91's registry tables written against them. Wire messages, none. Archive format, none.

### Purpose

A ready-to-use Postgres document holding ownership, membership and their policies, plus the policies of R91's registry tables written against it. Any deployment may adopt it and none is held to it.

### Decisions

1. **Roles are read, write and admin.** Admin implies write implies read. Adding a member or a bot needs admin.
2. **Registry policies.** Any signed-in user may create a registration code and see registered bots with their lists, installations are visible to the admins of the owner they serve and to their own bot.
3. **One table per level**, `organizations`, `teams (organization_id)`, `projects (team_id, organization_id)`, and assets carrying `project_id, team_id, organization_id`. A composite foreign key to the level above, declared `ON UPDATE CASCADE`, holds the ancestor ids, so moving a project to another team rewrites the team on everything under it in the same statement. Explicit types per level are OpenFGA's recommended model, which keeps recursion for depths that are genuinely unbounded.
4. **Grants per level, carrying their ancestor ids.** An installation is a principal like a user, its id the grant's principal.
5. **One policy per command on every grant table.** A policy on a table may read that table only while the table's own `SELECT` policies hold no subquery, or Postgres raises `42P17` (measured on Postgres 17).
6. **Grant tables carry an own-rows rule**, or every client reads every grant.
7. **The principal column of every grant table is indexed**, since every rule compares it to the caller.
8. **The reader role runs with `jit = off`.** The layered draft cost 1.8 to 2.6 s per read with JIT and 17 to 21 ms without over 20,000 orders, the compilation triggered by the policy-heavy plan's cost estimate.

**Held for a rewrite around definer helpers, not yet designed.** The sources agree on one pair. In Postgres, a `STABLE SECURITY DEFINER` helper with a pinned `search_path`, owned by a role that bypasses row-level security and called as `(SELECT fn())`, resolves the owners the caller holds a role on once per statement, and each rule compares an indexed column to it. In OpenFGA, one type per level, a `parent` link and roles that imply each other (`define admin: [user] or admin from parent`). The rewrite must hold on the client's replica too, where pg2sqlite refuses a function call in a policy, so the helper's body must translate as an inlined body, a `SECURITY DEFINER` body reading the backing table and an invoker body the filtered view, the shape settled for the pg2sqlite fix. The layered draft's separate admin tables, its admin rule reading its own table and its per-row subquery per level are workarounds no source recommends, and the maintainer asked that sql-rules enforce against them.

**Open in the layered draft, to be settled in the helper design.** Whether organization admins appoint co-admins or only the creator does. Whether admins above a level manage its installations, which `connetto_installations`' single `owner` column cannot express without ancestor ids or a hop. Whether co-admins at one level see each other. Who creates teams and projects. The fixed depth, which cannot express an organization inside an organization or a sub-team.

### Steps

1. The helper design, measured on Postgres 17 against the layered draft's behaviour matrix and cost.
2. Graded under sql-rules and rls2fga, and the rls2fga request for declared helpers written and launched upstream.
3. The document and its grading guard. pg2sqlite translated the layered draft without refusal.

### Proof

A reader of an organization sees orders two levels down, a writer of a team edits its orders and cannot add a member or a bot, its admin can, a principal outside the chain sees nothing, an installation added as read cannot insert, and moving a project between teams moves its orders' visibility at once.

### Done when

The document boots a server, grades under rls2fga and sql-rules, translates under pg2sqlite, and the proof passes against it.

---

## R104: the demos for apps and bots

**Status.** NOT STARTED.

**Blocked on R100 and R103.**

**Changes state.** Client tables, none, the demo apps' replicas holding the deployment's own tables. Server tables, none of connetto's, `examples/deployment` adopting R100's document. Wire messages, none. Archive format, none.

### Purpose and decisions

The whole model, end to end, in the browser stack, so a reader sees a bot registered, added, held to its list, changed, accepted and removed. `examples/deployment` adopts R100's standard schema, an organization "lab A" owning two teams, each owning projects, which own `orders`, `order_lines` and `photos`. In dioxus-web and yew-web, the desktop demo unchanged, the screens are, create a registration code, a catalogue of registered bots with their lists, add a bot to a team or the organization with the consent screen naming each table and verb and a toggle per optional table, and installed bots per owner with a pending "requested different access" to accept and removal. All live queries and the one add transaction. Two bots from the template, under `examples/`, the importer added per team with `orders` insert and update and `photos` read and insert, running connected, importing a folder into orders with their pictures' bytes, and the totaliser added to the organization with `order_lines` read and `orders` read and update, running woken, writing each order's total quantity whenever its lines change. Between them both running modes and both scopes are exercised.

The scenario is scripted in the browser stack, register both with codes, add the importer to team T and the totaliser to lab A, import, watch totals follow, register importer v2 asking for an optional `customers` read and see team T keep working, register a v2 asking for a new required table and see T paused until its admin accepts, remove the importer from T and see its replica wiped, and a hand-written raw query by the importer on an unlisted table refused.

### Done when

One scripted browser-stack scenario passes, register, add, import, totals follow, an optional-table list pauses nobody, a required-table list pauses team T until accepted, removal wipes the replica, and a raw query on an unlisted table is refused.
---

## R74: device identity, enrolment, and certificates

**Status.** IN PROGRESS. Steps 1 through 9 are done, with proofs 1, 2 and 7 and proof 6's event half built, and the desktop demo enrolling under the CA its stack mints (decision 30), proven on the CI-shaped emulator, a Galaxy A35 and an iOS simulator. Step 4's attestation verification is built on the server, with the status list and the accepted levels, on Android with the key carrying the fixed challenge and sending its chain under decision 10's OID, and on the Apple client through `connetto-app-attest` (decision 35). What remains is proof 5's `unproven` run on Windows, which rides R76's slice 9d.

**Blocked on nothing.** This phase adds a third secret beside the two stores R94 decision 15 injects by setter, an application-requested certificate lifetime, and an enrolment run after sign-in inside the connection sequence R94 step 3 moves into `connect()`. R94 decisions 11 and 15 make `ClientConfig` and `ConnettoConnection::connect` internal, so the device key's store, its protection report under R94 decision 13, the lifetime setting and the enrolment are built once, on the builders. R96 does not gate it, because the server half (the issuer key checked at startup, its chain and the lifetime ceiling) is server settings types with R37 setters, which R96's builder gathers. Two edges left unblocking. R71, on Linux the device key shares the replica key's custody, which R71 made survive a reboot, so the device key uses R71's stores and needs nothing of its own. R72, certificate validity is the one wall-clock check a peer cannot correct against a server, so step 7 below states the clock rule itself and R72's inventory cites it.

**Changes state.** Client tables, the `_connetto_device_certificate` and `_connetto_revocation_list` tables in the enrolment store. Server tables, `connetto_device_enrolments` and `connetto_device_certificates` with their chain and lifetime columns. Wire messages, the enrolment exchange, `EnrolRequest`, `EnrolGrant`, `RevokeDeviceRequest`, `DeviceRevokedAck` and `EnrolRefused`, the `RevocationUpdate` broadcast, and `DevicesRequest` with its `DevicesList` answer. Archive format, none, the archive holding no enrolment state.

### Purpose

Nothing today lets two devices authenticate each other offline, since clients hold only server-signed credentials. This phase gives a native device a durable identity the server vouches for, a P-256 key created inside the device's security chip and an X.509 certificate the deployment's issuer signs binding that key to the account, plus the signed lost-device notices peers carry to one another.

### Decisions, taken with the maintainer

1. **Lost-device notices travel between peers, and expiry bounds the rest.** The deployment signs notices that devices hand to one another when they meet, so a peer rejects a reported device once any notice reaches it. A group no notice reaches trusts the certificate until it expires.
2. **The root key stays offline, and a replaceable issuer lives on the server.** The online issuer signs certificates and notices, and the offline root signs issuers.
3. **The application is built with the expected deployment root.** A device checks the chain it receives against the root or roots it was built with, and never adopts a root it is handed (RFC 7030 section 4.1.1 contrasts the two).
4. **The server ceiling defaults to 30 days, a certificate lasts 24 hours unless the application asks otherwise, and a connected client can ask again.** The ceiling is server configuration. The application requests a lifetime per case at connect and can request a new certificate with another lifetime while connected.
5. **Validity allows five minutes of clock difference at both ends.**
6. **The device key lives in the security chip.** It is a P-256 key the Secure Enclave, Android Keystore or Windows TPM creates and never releases, and a locked software P-256 key in R71's stores on Linux. P-256 is the one curve all three chips hold. How it signs writes is decision 12.
7. **Chip attestation is verified where a device offers it and recorded in its certificate.** Each deployment decides whether peers or the server refuse unproven devices.
8. **The lost-device list shows a typed descriptor the application defines.** connetto keeps only what it needs to work, and the application decides what makes a device recognisable, a location included.
9. **The deployment is named by a UUID `connetto-ca` generates into the root.** Creating a root mints a random UUID, as SPIFFE advises for trust domain names made without operator input, and writes it as the root's subject common name. A rolled-over root carries the same UUID, so device identities survive a rollover. Server and client read it from the root they already hold, so no setting names it.
10. **connetto's OIDs live under its own IANA Private Enterprise Number.** The attestation extension is `1.3.6.1.4.1.<number>.1`, and any later connetto OID takes the next arc, the way Android places its key attestation extension under Google's number 11129 and Apple places App Attest's under its own arc. The maintainer registers the number, free of charge, and only the attestation step waits for it. IANA assigned connetto the number 67105, so the extension is `1.3.6.1.4.1.67105.1`.
11. **The device key signs once the device has been unlocked since its last restart.** It is created with Apple's after-first-unlock accessibility, which Apple recommends for items background work needs, with no Android unlocked-device requirement and no Windows prompt, so peer exchange and courier keep working with the screen locked. The app exchanges data only once the R51 or R52 prompt has opened its replica in that launch, so the key adds no access the prompt has not granted.
12. **Queued writes form a hash chain whose newest entry the chip signs at each hand-off to a peer.** Each pending entry carries the hash of the entry before it in its writer's queue, as Scuttlebutt feeds link each message to the previous one, and the chip signs only the newest entry each time the queue is handed to a peer. A receiver checks the chain back to its last verified entry, and a courier forwards only a range ending at a signed entry. A connection to the server proves its device writer once, by the chip's signature over the handshake challenge in `BindDevice`, and the server attributes every write on that connection to the key, so no newest-entry signature travels to the server, the once-per-channel proof of possession RFC 8705 gives a certificate-bound token.
13. **Attestation has three levels, set once at enrolment.** `chip-proven` means Android attested the device key at the `TrustedEnvironment` or `StrongBox` level, never `Software`. `app-attested` means Apple's App Attest signed over the certificate request on iPhone or iPad (a Mac cannot, see the design's **Attestation.**), which proves a genuine copy of the app asked and says nothing about where the key lives, since App Attest attests its own separate key. Every other device is `unproven`. Renewals keep the enrolment's level and never attest again, since Apple warns its servers may throttle attestation.
14. **The issuer is replaced every year and the root lasts 10 years.** `connetto-ca` signs a new issuer yearly, valid one year plus the certificate ceiling so its last certificates stay checkable, within NIST SP 800-57's one-to-three-year bound for a private signature key. The root lasts 10 years. A device trusts every root its build carries, so the next root is signed and carried beside the current one well before the current one expires. The server warns at startup when its issuer has less than 60 days left.
15. **`connetto-ca` stores the root key encrypted under an operator passphrase.** The root key is written as PKCS #8 encrypted with PBES2 (scrypt and AES-256-CBC, RFC 8018) and the passphrase is asked at every ceremony, so a copy found on a backup is useless without the passphrase and the operator can keep several copies apart. The issuer key on the server stays a plain file, provisioned like `CONNETTO_CONTENT_KEY`.
16. **A device without a usable key chip gets a software key in its secret store.** A Windows machine without a TPM the app can use, an Intel Mac without Touch ID, or a Mac build without the keychain entitlement creates its P-256 key in software and keeps it unprompted in the plain store beside its secrets (R51 to R53 and R71), so it still enrols and peer-syncs. Its certificate records `unproven`, so a deployment refusing unproven devices shuts it out. Android needs no fallback, since every handheld backs its keystore with an isolated environment (CDD 9.11 H-0-2).
17. **Enrolments and their certificates are two tables the enrolment schema trait names.** One row per device key holds the account, attestation, session, times and descriptor, and one row per issued certificate holds its X.509 serial, its key and its expiry, so a lost-device report lists every unexpired serial of the key by a join. The serial is 16 random bytes, the RFC 5280 identifier a revocation list names a certificate by. The first byte is drawn in 1 to 127, so the serial is positive and minimal, the form RFC 5280 and the DER encoding rules ask of it. A key already enrolled under one account is refused to any other.
18. **A native client enrols from a task beside its pump.** On every attach and when half-life passes on a live connection, the shared client asks for a challenge, signs on the key's own thread and sends the request. The pump routes each `Enrol*` answer to the waiting request by its `request_id` and never to `ClientEvent`, every wait ends after 30 seconds on a monotonic clock and at once on disconnect, and `reissue_certificate` uses the same path. Sync starts without waiting for enrolment.
19. **The client keeps its certificate and lists in connetto's own replica tables.** `_connetto_device_certificate` holds the chain, its validity and the stored lifetime, created like `_connetto_pending`, so it exists for every application, belongs to its account, is encrypted under the replica key and goes with the replica on `forget_device`. Step 5's lists go in `_connetto_revocation_list` beside it.
20. **An Android application hands the enrolment its JNI access through the builder.** `NativeDurable::with_java_access(Arc<dyn JavaAccess>)` exists on Android only, and `connetto-auth-session` offers `java_access()` over `manganis::android::with_activity`, so a Dioxus application writes one line. An Android build with `device-identity` that connects without it fails with a typed error naming the setter, and decision 16 keeps Android without a software fallback.
21. **Only a build on the platform keyring has a device identity.** The enrolment setters exist on keyring builds only, and on any other build `reissue_certificate` answers `CertificateError::NoIdentity` and `device_certificate` answers `None`, so a build whose stores the application names (`NoKeyring`, R71's headless runs) stays a spectator to peer sync. Its `ReplicaKeyStore` holds gated 32-byte keys and cannot keep the software key ungated as decision 11 requires.
22. **Step 5 builds the one list intake, and R76 brings the peer frames.** Every revocation list a client receives goes through one intake that verifies it against the deployment roots, keeps per issuer the highest CRL Number, ignores and logs an equal number with other content, and treats the device's own listed serial as its revocation. Server pushes and enrolment grants use it in step 5, and R76's first link frames call the same intake.
23. **A build with a device identity names its deployment roots.** `NativeDurable::with_deployment_roots` takes the DER roots the application ships, on keyring builds, and a build with `device-identity` that connects without one fails with a typed error naming the setter. Every grant chain and every list is verified against those roots, and one that fails is refused and logged.
24. **A device whose issuer the root revokes drops its certificate and re-enrols its key.** A root-signed list naming the issuer of the device's own certificate withdraws that certificate, since every peer now refuses its chain, but leaves the key, which nothing compromised. The device deletes the certificate, emits `ClientEvent::CertificateWithdrawn`, and enrols the same key under the current issuer on its next connection, which the server grants because the key's enrolment stands.
25. **No offline enrolment and no sub-issuance.** A device enrols only online, against the server's issuer, after signing in, so a device that never met the server stays a spectator to peer sync.
26. **The descriptor's columns map the application's own type.** The application defines its descriptor struct once, in a crate its client and its server both use, and `connetto_schema!` maps that struct field by field to columns, `device_descriptor: AppDevice { name: Text, model: Nullable<Text> }`. The macro destructures the struct, so a field list that disagrees with it does not compile. The server decodes the MessagePack the client sends into that type and refuses an enrolment whose descriptor does not decode with `InvalidRequest`, and the device list encodes each row back.
27. **Each issuer's list number lives in a counter row, a third table beside decision 17's two.** `connetto_device_lists` holds one row per issuer with its last list number, advanced in one atomic upsert that returns the new number, as OpenSSL's `crlnumber` file, EJBCA and smallstep's step-ca keep theirs. Numbers then rise strictly across restarts and across every server instance, whatever their clocks do, which RFC 5280 section 5.2.3's monotonic CRL Number and the clients' replay check need.
28. **The enrolment member hands connetto one async function per statement.** `ConnettoEnrolmentSchema` names its three tables, the deployment's `Id` and its descriptor type, and exposes each statement the store runs as an async function over a borrowed connection, which `connetto_schema!` emits with concrete diesel queries. connetto's generic store composes them inside one transaction and keeps every decision, a revoked key, a key held by another account, a key already revoked.
29. **R74 builds step 7's event, and R76 refuses peers with it.** A client learns its clock is wrong from its own certificate. A certificate the server just granted that the local clock already puts outside its window says the clock is off, the server's issuance serving as the trusted time the way Chromium's bad-clock page compares against network time, and a certificate held at open that is not yet valid by more than the tolerance says the clock is behind. Either moves the client to `ClockOff` and emits `ClientEvent::ClockOutsideWindow { ahead }` once. While `ClockOff`, the hourly look re-evaluates the window and never renews, so a device with a wrong clock no longer re-enrols every hour, and a connection still renews once. R76's peer link reuses the same check to refuse peers.
30. **The demo stack mints the demo's certificate authority.** `connetto-demo-stack` creates a root and an issuer with `connetto-ca` under `target/demo-device-ca/` on its first run and reuses them, signing a new issuer once the current one has under sixty days left. It gives the server `CONNETTO_DEVICE_*`, creates the enrolment tables, and hands the root's path to the program it runs as `CONNETTO_DEMO_BUILD_DEVICE_ROOT`, which the demo's `device-identity` feature embeds at build time, as decision 3 has applications ship their root. A build without the feature shows that it has no device identity.
31. **The Android device key carries a fixed attestation challenge.** Android attests a key once, at creation, with the challenge set through `KeyGenParameterSpec.Builder.setAttestationChallenge`, and returns no attestation chain without one. The device key keeps being created at first open, now with the fixed challenge `connetto device key`, and enrolment sends its attestation chain. The server checks the chain names the request's key and that challenge, while freshness comes from the request itself, which the same key signs over the server's single-use nonce, so a replayed chain is useless without a private key that never leaves the chip.
32. **The server lists the App IDs App Attest vouches for.** `DeviceCertConfig::with_app_attest(app_ids, environment)`, and in the binary `CONNETTO_DEVICE_APP_ATTEST_APP_IDS` (`TEAMID.bundle.id`, comma-separated) with `CONNETTO_DEVICE_APP_ATTEST_ENVIRONMENT` (`production`, the default, or `development`). An attestation that passes Apple's checks for a listed App ID under that environment records `app-attested`, one for any other App ID, or with none configured, records `unproven`, since it proves nothing about the deployment's own app, and one that fails Apple's checks is refused with `InvalidRequest`. The demo stack lists the demo's App ID under `development`.
33. **A deployment names the attestation levels it accepts.** `DeviceCertConfig::with_accepted_attestation`, and in the binary `CONNETTO_DEVICE_ACCEPTED_ATTESTATION`, a comma-separated set of `chip-proven`, `app-attested` and `unproven`, all three by default so nothing is refused until the operator narrows it. An enrolment, or a renewal of an enrolment, whose level is outside the set is refused with `AttestationRequired`, and the client raises `ClientEvent::AttestationRequired` once and asks again only on its next connection, since its level cannot change. Every platform keeps the key in hardware but Linux, while only Android can prove it to the server, so the levels state what was proven, not where the key lives.
34. **The server keeps Google's attestation status list and falls back to `unproven` without it.** It fetches `https://android.googleapis.com/attestation/status` at startup and again when the response's `Cache-Control` age runs out, keeping the last good copy across failures, and checks every certificate of an Android chain against it. An Android attestation checked with no copy, or with one older than 7 days, records `unproven` and logs why, so such a device is refused only where the deployment narrowed its accepted set. `CONNETTO_DEVICE_ANDROID_STATUS` names a URL or a local file instead, for air-gapped deployments and tests.
35. **App Attest goes through `connetto-app-attest`, the one crate allowed `unsafe`.** `DCAppAttestService`'s instance methods stay `unsafe` in objc2, since their first call lazily builds a private controller without a lock and two concurrent first calls crash (objc2 #869, reproduced on macOS 26.6.2), and reading their completion handlers' results takes `unsafe` until objc2 #837. The crate opts out of the workspace's `forbid(unsafe_code)`, holds one process-wide lock around each whole attestation, copies the handlers' results before they return, and exposes a safe blocking `attest`, so every other crate keeps `forbid`. A device where App Attest is unsupported sends no evidence and records `unproven`, while any other App Attest failure fails that enrolment so the next connection retries it, since a transient Apple outage would otherwise record `unproven` for good. Its one limit is documented, other code in the same process calling App Attest outside the lock could still race.
36. **Revoking a revoked key repeats every side effect.** Revocation records `revoked_at` before it publishes, so a publish that fails left the device connected and signed in, and every retry saw the key already revoked and did nothing. A revoke request on a `Revoked` key now publishes again and repeats the close, the push and the auth-store session revocation against the session the enrolment records, and a failed publish drops the cached lists. The table is in the server revocation table under the certificate lifecycle.

### Design

**Certificate profile.** Device certificates follow the SPIFFE X509-SVID profile. The leaf has `cA` false, critical key usage `digitalSignature` only, extended key usage `serverAuth` and `clientAuth`, an empty subject, and exactly one critical URI subject alternative name, `connetto://<deployment-id>/account/<user-id>/device/<key-id>`, where `<deployment-id>` is the root's UUID (decision 9) and `<key-id>` is the SHA-256 of the public key. Attestation evidence is a non-critical extension under connetto's enterprise number (decision 10), holding `chip-proven` or `app-attested` with the platform, or `unproven` (decision 13). `notAfter` is the issue time plus the granted lifetime, and the verifier applies the five-minute tolerance. Issuers are CA certificates the root signs, with `pathLenConstraint` 0 and key usage `keyCertSign` and `cRLSign`, valid one year plus the ceiling, under a 10-year root (decision 14).

**Enrolment, renewal and reissue on the wire.** Request and answer pairs join `ControlMessage`, modelled on `ContentTicketRequest` and `ContentTicketGrant`, each request carrying a `request_id` its answer quotes.

- `EnrolChallengeRequest` is answered by `EnrolChallenge { nonce, expires_in_ms }`, a single-use 32-byte nonce bound to the session and valid for 60 seconds, its validity relative so no two clocks have to agree. The next enrolment request spends it whatever its outcome.
- `EnrolRequest { csr_der, lifetime_secs, attestation, descriptor }` is answered by `EnrolGrant { chain_der, revocation_lists_der }` or `EnrolRefused { reason }`, the reason being `Unidentified`, `OverCeiling { ceiling_secs }`, `Revoked`, `ChallengeExpired`, `InvalidRequest`, `AttestationRequired` or `IssuerUnavailable`. The `attestation` field and `AttestationRequired` arrive with step 4. An enrolment table out of reach answers `IssuerUnavailable` and logs the cause.
- `DevicesRequest` is answered by `DevicesList`, the caller's own enrolments with their first and last enrolment times, revocation time and descriptor, and `RevokeDeviceRequest { key_id }` by `DeviceRevokedAck`, or by `EnrolRefused` with `InvalidRequest` for a key that is not the caller's.
- `RevocationUpdate { lists }` is pushed to every identified session after its handshake and whenever a list changes, each list beside the certificate that signed it, so a device checks it against its roots without holding that issuer's certificate. `EnrolGrant` carries the same pairs, and `FatalErrorReason::DeviceRevoked` joins `SessionRevoked`. A list promises its next by `thisUpdate` plus the lifetime ceiling, the longest any certificate it can name stays valid.

**Proof of possession.** The request is a PKCS #10 request (RFC 2986) the chip key signs, carrying the server nonce as its `challengePassword` attribute. EST binds proof of possession to the TLS session itself (RFC 7030 section 3.5), and connetto-server speaks plain WebSocket behind a TLS edge (R94 decision 14), so the nonce on the authenticated session plays that role. `rcgen` 0.14 writes requests with attributes and signs through its `SigningKey` trait, which the chip key implements.

**Server.** `DeviceCertConfig` holds the issuer key and chain, the retired issuers still signing lists, a default lifetime of 24 hours and a ceiling of 30 days, with R37 setters in the style of `AuthConfig`. A lifetime over the ceiling is refused and never shortened, as `ShareError::TtlTooLong` refuses a share. The issuer key is provisioned like `CONNETTO_CONTENT_KEY` and checked at startup. The deployment tables `connetto_device_enrolments` and `connetto_device_certificates` (decision 17), which `preflight::require` requires, record per enrolment the account, the key id, every issued serial with its expiry, the attestation result, the auth-store session that last enrolled or renewed it, the enrolment, last-seen and revocation times, and the application's descriptor.

**Client.** `NativeDurable` gains `with_certificate_lifetime(Duration)` and `with_device_descriptor(D)`, on the durable stage because peer features need a durable replica (R94 decision 6). `NativeClient` gains `reissue_certificate(lifetime)`, failing with `Offline`, `OverCeiling { ceiling }`, `Revoked` or `Refused`, plus `device_certificate()`, `devices()` and `revoke_device(key_id)`. Renewal runs on any connection past half-life with the stored lifetime. A renewal refused over a ceiling the operator lowered is repeated at the returned ceiling and emits `ClientEvent::CertificateLifetimeCapped`.

**Lost-device notices are X.509 revocation lists**, one per issuer, built by `rcgen` and checked by webpki for the whole chain through rustls's `with_crls`. The peer link wraps webpki in its own client and server verifiers, since rustls names a server only by DNS name or IP address and a device's identity is a URI, and since webpki checks validity at a single instant, so the wrapper also applies the five-minute tolerance.

- Each list carries the CRL Number, which RFC 5280 defines as increasing per issuer. A device keeps per issuer the verified list with the highest number, ignores a lower one, and ignores and logs an equal number with other content.
- A complete list names only revoked unexpired certificates, so its size is bounded by the certificates issued within one ceiling.
- `enforce_revocation_expiration` stays off, since an offline device's lists would otherwise expire and refuse every peer, against decision 1. A certificate from an issuer with no known list is accepted on expiry alone (`allow_unknown_revocation_status`).
- The server pushes lists online. Peers exchange lists as the first frames inside an established link, verify each against the chain to a built-in root, store a newer one at once, and close any live link the new list revokes.
- Lists live in `_connetto_revocation_list` in the replica, keyed by issuer key identifier (decision 19).

**Revoking a device.** Reporting a device lost adds every unexpired serial of its key to the next list, publishes it, closes the device's connections with `DeviceRevoked`, and revokes the auth-store sessions its enrolment row records. Revoking the sessions keeps a stolen device that is still signed in from enrolling a fresh key and undoing the report. The account owner reports from any live session, and the deployment operator through `AuthService`. A device that learns its own revocation deletes its key and certificate, stops its peer link and emits `ClientEvent::DeviceRevoked`. R78's courier refusals and R36's ban machinery consult the enrolment table.

**Describing a device.** The descriptor is any type implementing `DeviceDescriptor` (`Serialize`, `DeserializeOwned`, `Send`, `Sync`, `'static`) in `connetto-core`, a tuple or a struct. The client sends it at enrolment and every renewal, in MessagePack of at most 4 KiB. The server stores it in columns of the deployment's own enrolment table named through an enrolment schema trait, as `DbAuthStore<S>` is generic over `ConnettoStoreSchema` (chapter 11). `NativeClient::devices()` returns `DeviceEntry<D>` values, and the application renders the list. `connetto_client::device` offers optional building blocks, namely the system device name where a platform lets an application read it (Android's `Settings.Global.DEVICE_NAME`, the host name on desktops, nothing on iPhone and iPad, where iOS 16 and later answer `"iPhone"` without a special entitlement), the platform and the model. A location is the application's to include as a client-reported field. The descriptor is untrusted input a stolen device can falsify, so it is shown and never consulted by a security decision, and one that fails to decode or exceeds the cap refuses the enrolment as `InvalidRequest`. The same descriptor serves a session list, which meets the same need.

**Issuer rotation and compromise.** Peers verify any chain to a built-in root, so a new issuer needs no change on devices. A retired issuer stays on the server, signing lists for its own certificates, until its last certificate expires. Root rollover ships the new root beside the old one in an application update, as SPIFFE trust bundles carry several roots during rotation, through `with_deployment_roots`. A compromised issuer is revoked by a list the root signs offline, distributed like any other list. The operator tool `connetto-ca` runs the offline ceremonies, creating the root, an issuer and signing a root list, and never runs on the server. The server takes a retired issuer with `DeviceCertConfig::with_retired_issuer`, which signs a list for its own certificates until it expires, and a root list with `with_root_list`. `connetto-ca revoke-issuer` keeps the root's one complete list in the CA directory, adds the issuer under the next number and signs it again, refusing an issuer another root signed or one already listed.

**Attestation.** Android sends its key attestation chain, which the server validates against Google's attestation roots and revocation list, taking the first attestation extension only, checking the attested key and challenge, and requiring the `TrustedEnvironment` or `StrongBox` level for `chip-proven`. Google signs with two roots, so the set of Google roots is server configuration with both as its default. iOS and iPadOS send an App Attest attestation whose client data hash is the SHA-256 of the request, recorded as `app-attested`. A Mac records `unproven`, since `DCAppAttestService.isSupported` answers `NO` on every Mac, Mac Catalyst and iOS apps on Apple silicon included, by Apple's documentation. Windows records `unproven`, since Microsoft documents application-level key claims only for virtualisation-protected keys and its CA-side TPM attestation only for RSA keys. Linux records `unproven`. Attestation runs at enrolment only (decision 13).

### Certificate lifecycle on a native client, every event in every situation

`NoKey` is a device with no key, never enrolled or with its custody record gone. `Fresh` holds a valid certificate before half-life, `Aging` one past half-life, `Expired` one past `notAfter` plus tolerance by the local clock. `ClockOff` is a local clock that puts the held certificate outside its window, known when a certificate held at open is not yet valid by more than the tolerance, or when a certificate just granted is outside its window by the local clock (decision 29). A revoked device moves to `NoKey` at once, so revocation appears as an event.

| Event | NoKey | Fresh | Aging | Expired | ClockOff |
|---|---|---|---|---|---|
| Connected and signed in | create key, enrol | take lists | renew | renew | renew, keep `ClockOutsideWindow` raised |
| Connection lost | stay a spectator | keep certificate, peer link continues | same as Fresh | peer link stays refused | peer link stays refused |
| Half-life crossed | not applicable | becomes Aging, renews if connected | not applicable | not applicable | not applicable |
| `reissue_certificate` called | enrol with that lifetime, or `Offline` | reissue, or `Offline` | reissue, or `Offline` | reissue, or `Offline` | reissue, or `Offline` |
| Opened with the replica | open or create the key | hold the certificate | hold it, renew when connected | hold it, renew when connected | emit `ClockOutsideWindow { ahead: false }` |
| Grant received | store with the lists it carries, then publish the certificate, start peer link, and a certificate the local clock puts outside its window moves to ClockOff and emits `ClockOutsideWindow` | replace certificate after keeping its lists, restart listeners, the same window check | same as Fresh | same as Fresh | store, leave ClockOff once the window holds the certificate |
| Refused over ceiling | `OverCeiling` to the caller, or a renewal retried at the ceiling with `CertificateLifetimeCapped` | same as NoKey | same | same | same |
| Refused as attestation required | raise `AttestationRequired` once, ask again on the next connection only | keep the certificate until it expires, raise `AttestationRequired` once, ask again on the next connection only | same as Fresh | same as NoKey | same as Fresh |
| Refused as revoked, or `DeviceRevoked` | not applicable | delete key and certificate, emit `DeviceRevoked`, become NoKey | same as Fresh | same as Fresh | same as Fresh |
| Newer list from server or peer | store | store, close revoked peers, own serial listed means revoked | same as Fresh | same as Fresh | same as Fresh |
| Root list naming own issuer | store | store, delete certificate, keep key, emit `CertificateWithdrawn`, re-enrol the same key when connected | same as Fresh | same as Fresh | same as Fresh |
| Peer link attempt (R76) | refuse, spectator | mutual TLS with lists | same as Fresh | refuse, emit `CertificateExpired` | refuse, emit `ClockOutsideWindow` |
| Bluetooth exchange attempt (R76) | refuse | run the exchange | same as Fresh | refuse | refuse |
| Custody record missing at open | stays NoKey | becomes NoKey, the old enrolment expires | same | same | same |
| Wall clock changes, or the hourly look | not applicable | re-evaluate the window | same | same | re-evaluate the window, never renew, leave ClockOff once the window holds the certificate |
| Plain logout | nothing | keep key and certificate for that account's next sign-in | same | same | same |
| `forget_device` | nothing | revoke the enrolment if connected, delete key and certificate | same | same | same |
| Archive import (R75) | enrol a fresh key | retire the archived enrolment, mint a fresh key | same | same | same |
| Account switch (R42) | each account follows its own row | same | same | same | same |

Server side, an enrolment row is `Active` from its grant, `Revoked` from a report until its last serial expires, then purged. Renewing a `Revoked` row is refused as revoked.

**Server revocation, every request in every situation (decision 36).** A key is `Absent` (no enrolment, or not the asking account's), `Active`, or `Revoked`. Publishing builds every list from the enrolment table under fresh numbers, so it can always be run again.

| Request | Absent | Active | Revoked |
|---|---|---|---|
| Owner's `RevokeDeviceRequest` | `EnrolRefused` with `InvalidRequest` | record the revocation, publish, then ack, close the key's session's connections, push the lists to every identified connection and revoke the auth-store session | publish, then the same ack, close, push and session revocation, against the session the enrolment last recorded |
| Operator's `SessionManager::revoke_device` | `RevokeError::NotFound` | as the owner's, answering `true` | as the owner's, answering `false` |
| Publishing fails after the revocation is recorded | not applicable | the revocation stays recorded, the cached lists are dropped so the next handshake or grant rebuilds them from the table, nothing is closed, and the owner gets `IssuerUnavailable` or the operator `RevokeError::Unavailable` | same as Active, and a later request retries everything |
| `EnrolRequest`, first or renewal | enrol | renew | refused as `Revoked` |
| Handshake and `EnrolGrant` lists | the cached lists, rebuilt from the table when none are cached | same | same |

### Steps

Steps 1 through 9 are done. They cover the certificate profile and the `connetto-ca` operator tool, the chip-held device key per platform with decision 16's software fallback, enrolment with renewal past half-life and `reissue_certificate` and the enrolment tables under `preflight::require`, attestation verification and its certificate extension on the server, Android and the Apple client, the revocation lists with their push and peer exchange and device revocation, issuer rotation with retired issuers signing their lists, the clock rule that step 7 states and R72's inventory cites, and the lost-key path on every platform stated in chapter 19. What remains is proof 5's `unproven` run on Windows, which R76's slice 9d provides.

### Proof

1. Enrolment, renewal past half-life, reissue with a new lifetime, refusal over the ceiling and refusal after revocation, Docker-gated against the real server. Built.
2. A list with a lower number never replaces a higher one, and an equal number with other content is ignored. Built.
3. A reported device is refused by a peer that learned the list only from another peer, with the server unreachable. Proven in R76, whose link frames feed step 5's list intake (decision 22), by `a_reported_device_is_refused_by_a_peer_that_learned_it_from_a_peer`.
4. Issuer rotation with one offline peer on each issuer, linking both ways, and a root-signed list making a revoked issuer's certificates refused. Proven in R76.
5. Attestation recorded as `chip-proven` on the A35s, as `app-attested` on the iPhone and the iPad, and as `unproven` on Windows and Linux. The Windows run rides R76's slice 9d.
6. A clock set outside the window refuses every peer and emits `ClockOutsideWindow`. The event half is built, the peer half is proven in R76.
7. A device with a cleared custody record enrols again under a fresh key, and a fresh Linux process reopens the device key from R71's durable stores. Built.

### Done when

A native device enrols while online, holds its chip key and certificate across restarts, renews past half-life, is refused a lifetime over the ceiling, reissues at a new lifetime on request, is refused by peers once any notice of its revocation reaches them, survives an issuer rotation, re-enrols under a fresh key after losing its custody record, and refuses every peer with the reason when its clock is outside the window, all proven by the list above including native runs on the maintainer's devices.

---

## R76: the peer link

**Status.** IN PROGRESS. Slices 1 to 5, 8, 9a, 9b, 9c and 9f are merged, the `connetto-peer` crate, the client's peer link and discovery, Android hosting, joining and the Android central, the text and QR fallback, the on-demand run, and the iPhone, iPad, Linux, macOS and handheld-star legs. Slice 9d, Windows, and 9e, the recorded home run, are parked by choice while the work goes mobile first, and 9e's home run needs every local machine in one place.

**Blocked on** nothing.

**Changes state.** Client tables, none, the client's half being the `connetto-peer` crate's peer link and the platform hosting and discovery (slices 3 to 9). Server tables, none. Wire messages, the peer link's frames below. Archive format, none.

### Purpose

The local data plane R25 decided, discovery by gateway-probe first (a hotspot host is the gateway) with mDNS as the general case, then mutual TLS over plain TCP between certified devices, native targets only. Every native machine takes part, phones, tablets, laptops and desktops alike, so this phase also gives a group with no shared network a way to make one and to hand its details to every member.

### Decisions, taken with the maintainer

1. **Android, Windows and Linux start a hotspot from the application, and Apple devices join.** Android uses `startLocalOnlyHotspot`, Windows a Wi-Fi Direct advertisement in legacy mode, which ordinary devices join as an access point, and Linux NetworkManager's hotspot. macOS has no application API to host (`startIBSSMode` is deprecated since macOS 11), and iPhone and iPad host only through Personal Hotspot switched on in Settings, whose password the user types into the application once so the beacon can hand it out.
2. **A Bluetooth beacon hands out the hotspot details, with a QR code and text as fallback.** Every machine in the set can scan and connect over Bluetooth from Rust through btleplug, which a desktop without a camera needs. With Bluetooth off, the host shows a `WIFI:` QR code and the same details as text.
3. **The beacon is public, and each joining device gets the password through a short exchange in which both sides prove their identity.** The beacon carries the protocol version and the host certificate's fingerprint, as the mDNS TXT record does. A joining device connects to the host over Bluetooth, the two run the same mutual TLS as the Wi-Fi link with the same certificate and revocation-list checks, exchange lists, and only then does the host send `HotspotOffer { ssid, passphrase, security, port }`. A revoked or foreign device is refused.
4. **An on-demand run on two attached phones and a recorded home run prove it.** `connetto-android-proof --peer-serial <second> --every-join` builds and installs once, runs the single-phone proof once, then joins the second phone to the first one's hotspot by the typed form, by the Bluetooth beacon and by the pasted `WIFI:` payload in turn, the runner tapping the system's approval (decision 16) with permissions granted by adb, restoring both phones between joins, recording a failed join and running the next, and writing one `summary.txt` beside the evidence. It runs whenever two phones are at hand and before each peer phase closes. Before each peer phase closes, the maintainer also runs a recorded session at home across every local machine (pippo on Linux, the Mac, the Windows machine, the iPhone, the iPad and the Galaxy M52) as a mesh. Every machine that can host (pippo, the Windows machine, the iPhone and the M52) hosts one round, every other machine joins that round's hotspot, and every pair of machines on it holds a link (decision 14's camp shape), so the four rounds prove every host with every joiner and every pair of systems linked. The Mac never hosts (decision 1) and the house's iPad is the Wi-Fi model, so both only join.
5. **The link lives in its own crate, `connetto-peer`.** It takes the certificate chain, the device key, the deployment roots, the kept lists and the accepted levels as inputs and knows nothing of the replica, and `connetto-client` wires it to the enrolment task behind a `peer` feature. Discovery, hosting and Bluetooth land in the same crate in later slices, so their platform dependencies never reach builds without peers, as `connetto-auth-session` and `connetto-file-client` keep theirs apart.
6. **A client listens as soon as it holds a valid certificate, at an address the builder names.** `NativeDurable::with_peer_listener(SocketAddr)` defaults to `0.0.0.0:0`, every interface on a port the system picks, `NativeClient::peer_address()` answers the bound address, and `NativeClient::link_peer(SocketAddr)` dials. A new grant swaps the certificate the listener presents and keeps its port, so an advertised address stays right.
7. **A link pings every 15 seconds, closes after 45 seconds of silence, and two devices keep one link.** Any frame counts as a sign of life, measured on a monotonic clock, and a silent link closes with `PeerLost`. When a second link to the same device key completes, the link dialled by the device with the lower key ID stays and the other closes with `Duplicate`, so both ends close the same one, and of two links the same device dialled the one with the higher dial counter stays, a number the dialler sends in its `Hello` so both ends compare the same value. The side closing a duplicate sends `Close { Duplicate }` first, and the other side keeps reporting the peer linked for at most the 10-second `Hello` bound while the kept link completes, so two devices dialling each other at once raise no `PeerUnlinked`.
8. **A peer refuses a certificate whose attestation level is outside its own accepted set.** `NativeDurable::with_peer_accepted_attestation` takes the set, all three levels by default as on the server (R74 decision 33), and a refused peer closes with `AttestationRefused`. A set narrowed in an application update then takes effect at once.
9. **A certificate's expiry reaches every live link on time, at both ends.** The device's task wakes at its own `not_after` plus the five-minute tolerance, and when a `ClockOff` certificate's window opens, so the table's standing rows apply when the standing changes. Each link closes at its peer's `not_after` plus the tolerance with `PeerExpired`, which a modified peer that never closes its own links cannot escape. Both are monotonic sleeps up to a wall-clock deadline, checked again against the wall clock when they fire, and a jumped clock is caught by the next look.
10. **A renewed certificate travels to every live link in a `Certificate` frame.** After a grant the device sends `Certificate { leaf, issuer }` on every link, and the far end verifies the chain exactly as at a handshake, requires the same device key the handshake proved, and moves that link's expiry deadline. A chain the verifier refuses closes the link, with `PeerRevoked` for a revoked chain, `PeerExpired` for an expired one, and `Protocol` for any other refusal or another key.
11. **The gateway probe dials the port the join handed over.** A device that joined a hotspot through the Bluetooth `HotspotOffer`, the text fallback or the join API learns the host's peer port with the network and dials the gateway at that port at once, so decision 6's system-picked port stays. Every other network relies on mDNS, whose SRV record carries the port. A failed dial is redialed every 2 seconds while the network stays joined and no link to the gateway stands, for at most 60 seconds after the join, then `ClientEvent::JoinedHostUnreached` reports it. iOS refuses an application's first connection to a local address with `EHOSTUNREACH` while it asks the user for local-network access, so a single dial never links a fresh install.
12. **Discovery reports every peer it finds, and the client dials them itself unless the application turns that off.** While the device serves, discovery advertises and browses, and reports `ClientEvent::PeerFound { address, fingerprint }` and `PeerGone { fingerprint }`. `NativeDurable::with_peer_autolink(bool)`, on by default, lets the client dial what it finds under the table below, which keeps every application from rebuilding the backoff and refusal rules. With autolink off the application dials with `link_peer`, for a picker the user confirms, for linking only while a screen is open, or for not revealing the device to every certified device nearby. Inbound links are accepted either way, since the listener verifies every peer.
13. **Discovery bounds what an unauthenticated network can make it do.** At most 8 dials run at once and a due dial waits its turn, so every row still dials, and at most 256 instances are tracked, a find beyond that evicting the oldest instance not Linked without an event. A dial holds its slot until its outcome arrives, whatever happens to its instance meanwhile, and queued dials start oldest first. The listener runs at most 16 inbound handshakes at once and closes a connection beyond that at accept, and a failed accept waits 100 ms before the next.
14. **A hosted hotspot follows the client and the application, independent of the certificate.** The expected use is a group with no shared network, a camp or a field site, where the host is mostly the group's network provider and every device links to every other through discovery. `host_hotspot` therefore needs no valid certificate, the `HotspotOffer` carries the peer port only while the device serves, and the hotspot stops on `stop_hotspot`, on the client's close, or when the system or the user stops it. An expired host keeps the group's network up, and the Bluetooth exchange still hands out the password only after both sides prove their identity (decision 3).
15. **A device joined to a hotspot binds only its peer sockets to that network.** Android gives a network requested through a `WifiNetworkSpecifier` to the requesting application alone and routes nothing to it unless the application binds, and an A35 supports no two concurrent Wi-Fi connections, measured, so the joined phone also loses its usual Wi-Fi. The link's dial and listen sockets and the mDNS sockets bound for the joined network go through `Network.bindSocket`, and the server connection keeps the default network, mobile data on an A35.
16. **The emi proof joins the second phone through the application.** The runner reads the host's offer from its page, types it into the joiner's form, taps the system's approval through uiautomator, and asserts that the joiner reaches the host at the gateway and both report the link, so `requestNetwork`, the socket binding and the gateway probe run on hardware.
17. **Each system needing foreign code or `unsafe` gets its own small crate, and `connetto-client` owns the rules.** `connetto-client` holds the hotspot API, the backend trait each system implements, and the tables. Android's backend needs Kotlin compiled into the application, since `LocalOnlyHotspotCallback` and `NetworkCallback` are abstract classes, so `connetto-peer-android` ships the plugin through manganis, and the client drives it by class name over `JavaAccess` (R74 decision 20). Apple's and Windows's backends need `unsafe` and arrive as `connetto-peer-apple` and `connetto-peer-windows`, as App Attest lives in `connetto-app-attest`. Linux's NetworkManager backend is safe Rust over zbus and stays in `connetto-peer`.
18. **The beacon is a legacy advertisement carrying the first 8 bytes of the leaf's fingerprint.** A legacy advertisement is the format every joiner receives, iPhone, iPad and the Mac's CoreBluetooth included, and its 31 bytes hold connetto's 128-bit service UUID, the frame version and an 8-byte prefix of the SHA-256 of the presented leaf beside the flags. The prefix lets a joiner tell nearby hosts apart, and the exchange's TLS proves the full identity.
19. **A joiner scans in the background by default and joins on its own only when the application asks.** `set_hotspot_scan(bool)`, on by default, scans whenever the device serves and has joined no hotspot and reports `HostNearby` and `HostGone`. `set_hotspot_autojoin(bool)`, off by default, also joins the strongest host it sees through the exchange, one attempt per host until its beacon changes. Both are settable on the builder and at run time, since whether unattended joining fits depends on where and how the application is used. `join_nearby` and `fetch_offer` stay for a manual join.
20. **Every joiner's central is btleplug, Android's included, once btleplug's Android `init` resolves its classes through the context class loader.** Measured between the two A35s inside the demo with btleplug 0.13.4, scanning saw the advertised UUID and service data within a second, and connecting, service discovery, an MTU of 517, writes with response and notifications all worked, once one call in btleplug's `init` looked up `Adapter` through `LoaderContext` instead of raw `FindClass`, which on a thread manganis attached never sees application classes. That fix is deviceplug/btleplug#495, released in btleplug 0.13.5 with #497's fix of the Java notification queue, and the workspace depends on that release. `connetto-peer-android` bundles btleplug's Java in its Gradle module with R8 keep rules for `com.nonpolynomial.**` and `io.github.gedgygedgy.**`, sets each calling thread's context class loader to the application's, and hands btleplug the virtual machine under its jni 0.22. deviceplug/btleplug#480, a use-after-free in `FnAdapter` on notifications reported at about 1.1% of sessions, stays open, and the emi runs watch for it.
21. **Every backend reports one Bluetooth readiness and prompts only inside a call the user triggered.** `BluetoothState::{Ready, Off, NotPermitted, Unsupported}` is read with `bluetooth_state()` and reported as `ClientEvent::BluetoothChanged`. The background scan and the host's beacon never prompt, wait while the state is not `Ready` and resume when it is, and the hotspot runs throughout. `host_hotspot`, `join_nearby`, `fetch_offer` and `enable_bluetooth()` run the platform's strongest action once inside the call, the system dialog on Android, switching the radio on through `Windows.Devices.Radios` or BlueZ's `Powered`, and Core Bluetooth's power alert or the Bluetooth settings pane on Apple, then go on or answer `BluetoothOff` or `BluetoothNotPermitted`.
22. **`connetto-client` implements the btleplug central for every system, and `connetto-peer-android` only bridges Android's virtual machine to it.** btleplug's Android `init` takes a jni 0.22 environment, connetto and manganis stand on jni 0.21, and jni 0.22 learns the running virtual machine only through the `unsafe` `JavaVM::from_raw`. So `connetto-peer-android` offers a safe `java_vm()` building it from the virtual machine manganis hands out, its one `unsafe` call documented under the crate's own lints as decision 17 places it, and ships btleplug's Java in its Gradle module. `connetto-client` depends on it on Android only and runs btleplug on one dedicated thread, attached, its class loader set to the application's, and btleplug's start first reads `Ready`, never while the client is built. On iPhone and iPad btleplug's start waits for the system's Bluetooth state, which arrives only once the user answers the permission alert.
23. **The QR code carries the standard `WIFI:` fields and the peer port, and the application draws and scans it.** The payload is `WIFI:T:<WPA or SAE>;S:<ssid>;P:<password>;X:<port>;;` with the format's backslash escapes, so a phone's system camera joins the Wi-Fi and skips `X`, which parsers of the format pass over, and an application that scans the code or takes the string learns the port and dials the gateway at once (decision 11). `HotspotOffer::to_wifi_payload` writes it and `HotspotOffer::from_wifi_payload` reads it with a typed refusal, and the application renders and scans the code with its own tools.
24. **The text form is the offer's three lines, and the payload travels as one string.** A person on a machine without a camera types the network and password into the system's own settings, which ask for exactly those, and the port into the application, while a chat message or a scanner carries the payload whole into `from_wifi_payload`.
25. **An iPhone or iPad host advertises connetto's service UUID alone, and a user-facing name comes only after the exchange.** Apple lets an application's advert carry a local name and service UUIDs only, so decision 18's service data is out on iOS, and an iPhone's beacon is the UUID with no prefix. A joiner keys such a host by its Bluetooth address, which iOS rotates about every 15 minutes, so the same iPhone can reappear as a new nearby host and autojoin tries it once more. `HostNearby` carries the prefix where the beacon has one. No beacon carries a name, since the beacon is public (decision 3) and a name in it would tell anyone nearby who hosts. `fetch_offer` and `join_nearby` answer the host identity the handshake proved, so an application can ask "Join Luca's iPhone?" from its own account names before it joins.
26. **An iPhone or iPad host's network name is typed by default and prefilled where the application holds Apple's device-name entitlement.** Personal Hotspot names its network after the device and iOS gives no application its password, so `host_personal_hotspot(ssid, passphrase)` takes both. Since iOS 16 `UIDevice`'s name is a generic "iPhone" unless the application holds `com.apple.developer.device-information.user-assigned-device-name`, which Apple grants per application, so connetto answers the real name through `personal_hotspot_name()` where the running application holds it and `None` elsewhere, and the demo prefills it once Apple grants the demo's request.
27. **An iPhone or iPad host reports an offer no joiner landed on and leaves the retry to the people involved.** iOS tells an application nothing about Personal Hotspot, whether it is on, who joined or whether the typed password is right. Measured on the iPhone 15 Pro Max on iOS 26, `bridge100` at 172.20.10.1 exists exactly while a device is joined to the hotspot, and switching Personal Hotspot on with nobody joined shows no interface. So when an exchange hands the offer out and no device lands on `bridge100` within the join bound, the host emits `ClientEvent::HotspotUnreached`, and the application tells its user, who checks Personal Hotspot and the password, after which the joiner tries again. The beacon keeps advertising.
28. **An Apple joiner asks the system for the network once more when its first join lands nowhere.** iOS can refuse to join a Personal Hotspot switched on seconds earlier, showing its own "unable to join" alert while `applyConfiguration` still answers success, so the device stays on its previous network. When the system has answered and the interface has not moved off the address it stood on before the join after 15 seconds, the joiner applies the configuration a second time, once per join, within the join table's 60-second bound.
29. **The Linux proof drives a headless client in the runner's own process tree.** `connetto-android-proof --linux-peer --every-join` starts itself again as a child that signs in, enrols and serves a line-based command stream, while the runner drives the phone's demo as in decision 4. pippo has no desktop session, and the demo's screens are the Dioxus code the phones already prove.
30. **A Linux host names its network `connetto-` and four random hex digits**, new for each hotspot, beside a fresh random 16-character passphrase held in memory only.
31. **The Linux peripheral is `bluer`**, BlueZ's own binding, in `connetto-peer` for Linux only.
32. **A Linux host tries 5 GHz for 10 seconds, then 2.4 GHz**, and remembers for the life of the process each device whose 5 GHz attempt failed. Intel's Linux driver refuses some 5 GHz access points, and NetworkManager's own give-up takes 25 seconds.
33. **One Wi-Fi device refuses the second of hosting and joining.** A join while the device hosts answers `JoinError::Hosting` and a host while it is joined answers `HotspotError::Joined`, so the application decides and a group never loses its host's network as a side effect.
34. **A Mac joins a network by name only with Location Services access.** macOS 26 hands a process without it scanned networks that carry no name, and refuses to join them with `-3900 tmpErr`, measured at the Mac against the iPhone's Personal Hotspot. So a Mac join answers `JoinError::MissingPermission("Location Services")` until the user allows it, and the application asks once through `connetto_peer_apple::hotspot::request_location_access()`.
35. **A Mac's join scans for the offered network for up to 30 seconds.** An iPhone's Personal Hotspot answers a directed scan only now and then, even with its settings pane open, and a scan soon after another can fail with `tmpErr`, so the join scans every 5 seconds until the name shows or the bound runs out, inside the machine's 60-second join bound.
36. **A Windows host is a Wi-Fi Direct legacy access point with an autonomous group owner.** `WiFiDirectAdvertisementPublisher` with `LegacySettings` enabled and `IsAutonomousGroupOwnerEnabled` set starts a WPA2 access point that ordinary devices join, named as decision 30 names a Linux host. Measured on a borrowed Windows 11 machine (Intel Wireless-AC 9560) from an unpackaged process, the access point came up at `192.168.137.1/24` on a Wi-Fi Direct virtual adapter and the M52 saw it on 5805 MHz while the machine stayed joined to its home network on 5 GHz channel 56, and without the autonomous flag it started but never beaconed.
37. **A Windows host's beacon is connetto's service UUID alone**, decision 25's iPhone shape, so joiners key it by its Bluetooth address. `GattServiceProvider` advertised the UUID from an unpackaged process, measured, and answered `StartedWithoutAllAdvertisementData` when given decision 18's version and prefix as service data, so the UUID and the data do not both fit one legacy advertisement there.
38. **A Windows join is a per-user WLAN profile, deleted on leave.** `WlanSetProfile` with the offer's SSID and passphrase, then `WlanConnect`, and `WlanDeleteProfile` on leave and on the client's close. The joined address, prefix and gateway are read from `GetAdaptersAddresses`, since Windows 11 24H2 refuses `WlanQueryInterface` for the current connection without location consent while profile and connect calls are not gated.
39. **Windows's platform code lives in `connetto-peer-windows`**, over the `windows` crate's WinRT projections and `windows-sys`'s Native Wifi and IP Helper bindings, behind small I/O traits the client's `WindowsHotspotBackend` and `WindowsBluetoothBackend` drive, so every table row is proven against a fake as on Linux. The central is btleplug's own Windows backend, widened in `connetto-client` as decision 22 states, and the readiness switches the radio on through `Windows.Devices.Radios`, which answered `Allowed` to an unpackaged process, measured.
40. **The Windows proof waits for a Windows machine the maintainer can use for radio work.** The machine measured for decisions 36 to 38 is borrowed and is not available for hosting, Bluetooth or the proof. The runner's place, the phone's cable and how the iPhone's steps are taken are settled once such a machine is at hand, since a runner on a machine whose only network is Wi-Fi is cut off from pippo whenever it joins the phone's hotspot. Until then the pull requests of slice 9d are proven by their tables against fakes on Linux and by the Windows check job.
41. **CI gains a Windows check job.** A `windows-latest` job runs clippy and the docs build over the crates with Windows-gated code, no tests, since nothing on main compiled Windows before and the Android and iOS peer code had already shipped broken while unchecked.

### Design

- **The Bluetooth exchange.** The host serves through the platform's server role (`BluetoothGattServer` on Android, `GattServiceProvider` on Windows, BlueZ through `bluer` on Linux, `CBPeripheralManager` in the foreground on iPhone and iPad), Android's and Apple's in platform plugins on the `connetto-auth-session` pattern. The joiner writes to one characteristic and reads notifications on another. Frames are length-prefixed and split to the negotiated packet size, and rustls runs over that stream through `read_tls` and `write_tls`, which take any reader and writer. An A35 supports 16 advertising sets and 1,650 bytes of advertising data, measured.
- **Joining.** iPhone and iPad join through `NEHotspotConfigurationManager`, which the user confirms. The Mac uses `CWInterface.associate`, which may ask for an administrator password. Windows saves a profile and calls `WlanConnect`. Linux calls NetworkManager's `AddAndActivateConnection2` with a volatile profile its user owns, which needs `network-control` and `settings.modify.own`. Android uses the platform's network request with the user's approval in the application, in tests too, with the approval tapped by the runner (decision 16).
- **Hosting permissions.** Android needs `CHANGE_WIFI_STATE`, `NEARBY_WIFI_DEVICES`, `BLUETOOTH_ADVERTISE` and `BLUETOOTH_CONNECT`. Linux needs NetworkManager's `wifi.share.protected` beside the two joining rights. A desktop user holds all three, and outside a desktop session NetworkManager answers `no` or `auth`, measured on pippo, so a refused hotspot maps to a typed error naming the permission. pippo, which runs the proof headless, grants its user the three through the polkit rule `/etc/polkit-1/rules.d/49-connetto-peer-proof.rules`. A host keeps its hotspot credentials in memory only, for the life of the hotspot.
- **Revocation lists on the link.** Each side sends its CRL Numbers per issuer as the first frames inside the established TLS session and then any list the other lacks (R74). A live link whose peer the new list revokes closes with a typed reason.
- **Discovery networks.** emi's eduroam put the two A35s in separate subnets, measured, so they reach each other by direct connection and never by mDNS. Devices on such a network meet through a hotspot.

- **Beacon and exchange.** While the device both hosts a hotspot and serves its peer link, it advertises the beacon of decision 18 and runs a GATT service under connetto's service UUID, with one characteristic the joiner writes and one it reads notifications from. The exchange runs the link's TLS 1.3 with the same verifier over frames split to the negotiated packet size, swaps `Hello` frames and the lists either side lacks into the one intake, and then the host sends `Offer { ssid, passphrase, security, port }`, the hotspot's details with the peer port, and closes. Every exchange is bounded at 30 seconds, at most 4 run at once on a host, and a fifth connection is refused at once. Android needs `BLUETOOTH_ADVERTISE`, `BLUETOOTH_CONNECT` and `BLUETOOTH_SCAN` (with `neverForLocation`), which `connetto-peer-android`'s manifest declares, and Bluetooth `Ready` as decision 21 states it.

The iPhone or iPad host against its state (decisions 25 to 27), beside the beacon table, which it follows for the beacon's own rows with the UUID-only advert.

| Event | Idle | Hosting(offer) |
| --- | --- | --- |
| `host_personal_hotspot(ssid, passphrase)` while the device serves | become Hosting, the beacon per the beacon table | replace the offer, the beacon unchanged |
| `host_personal_hotspot` while the device does not serve | become Hosting with no beacon, the offer for text and QR (decision 14) | replace the offer |
| An exchange hands the offer out | not applicable | start the landing bound of the join bound |
| A device lands on `bridge100` within the landing bound | not applicable | end the bound |
| The landing bound passes with no device on `bridge100` | not applicable | emit `HotspotUnreached`, the beacon keeps advertising |
| Personal Hotspot switched off in Settings, or a wrong password typed | invisible | invisible until a landing bound passes |
| The application moves to the background | nothing | iOS moves the UUID to its overflow area, which Apple scanners filtering on it still read and others do not |
| `stop_hotspot` | nothing | become Idle, the beacon stops |
| Client closed | nothing | become Idle, the beacon stops |

The host's beacon against its state, beside the hotspot tables.

| Event | Off | Advertising |
| --- | --- | --- |
| The device hosts a hotspot and serves | start the GATT service and the advertisement, become Advertising | not applicable |
| The hotspot stops, or the device stops serving | nothing | stop advertising, close the GATT service and every running exchange, become Off |
| A renewal changes the leaf | nothing | advertise again under the new prefix, running exchanges finish under the chain they began with |
| A joiner connects while fewer than 4 exchanges run | not applicable | run the exchange |
| A joiner connects while 4 exchanges run | not applicable | disconnect it at once |
| The joiner's chain is refused | not applicable | end that exchange with no offer, log it |
| The joiner's handshake and hellos complete | not applicable | hand its lists to the intake, send the offer, end the exchange |
| 30 seconds pass in one exchange | not applicable | end that exchange with no offer |
| Bluetooth leaves `Ready` | nothing | stop advertising, close every running exchange, become Off |
| Bluetooth becomes `Ready` while the device hosts and serves | start the GATT service and the advertisement, become Advertising | not applicable |
| `host_hotspot` called while Bluetooth is not `Ready` | the hotspot starts, then decision 21's action runs once unless the prompt is switched off, and the call answers `Hosted { offer, beacon }` with `beacon` holding `BluetoothOff` or `BluetoothNotPermitted` when Bluetooth stays unavailable, the hotspot left running | not applicable |
| Client closed | nothing | stop advertising, close the service |

The joiner's scan and exchange against its state. Scanning runs while scan is on, the device serves, and no hotspot is joined, and autojoin implies scan.

| Event | Idle | Scanning | Exchanging |
| --- | --- | --- | --- |
| Scan or autojoin turned on while the device serves with no joined hotspot | start scanning, become Scanning | nothing | nothing |
| Scan turned off, the device stops serving, or a hotspot is joined | nothing | stop scanning, emit `HostGone` for every reported host, become Idle | finish the exchange, then become Idle |
| A beacon with connetto's UUID is seen | not applicable | a new host, by its prefix where the beacon carries one and else by its Bluetooth address (decision 25), emits `HostNearby`, a known one refreshes its signal | remember it |
| A host's beacon is unseen for 30 seconds | not applicable | emit `HostGone`, forget it | same |
| Autojoin is on and a host not yet tried is seen | not applicable | pick the strongest untried host, become Exchanging | wait |
| `join_nearby(host)` or `fetch_offer(host)` while Bluetooth is not `Ready` | decision 21's action runs once unless the prompt is switched off, then go on as below or answer `BluetoothOff` or `BluetoothNotPermitted` | same | answer `Busy` |
| `join_nearby(host)` or `fetch_offer(host)` | run the exchange, become Exchanging | stop scanning, run the exchange, become Exchanging | answer `Busy` |
| The exchange yields the offer | not applicable | not applicable | `fetch_offer` answers it, `join_nearby` and autojoin hand it to `join_hotspot`, become Idle while joined |
| The host's chain is refused, the exchange times out, or Bluetooth fails | not applicable | not applicable | answer the typed error, mark the host tried until its beacon changes, become Scanning or Idle |
| The join is declined or times out | not applicable | not applicable | mark the host tried until its beacon changes, become Scanning |
| Bluetooth leaves `Ready` | nothing | stop scanning, emit `HostGone` for every reported host, become Idle | fail the exchange with `BluetoothOff` |
| Bluetooth becomes `Ready` while scan is on, the device serves and no hotspot is joined | start scanning, become Scanning | not applicable | not applicable |
| Client closed | nothing | stop scanning | cancel the exchange |

The joiner's central against its state, on a system that runs one (decision 22). A system without a central answers `join_nearby` and `fetch_offer` with `Unsupported` and never leaves Absent.

| Event | Absent | Starting | Running |
| --- | --- | --- | --- |
| Bluetooth reads `Ready` | start btleplug's thread, become Starting | nothing | nothing |
| The start answers the central | not applicable | become Running, then the joiner table's "Bluetooth becomes `Ready`" row applies | not applicable |
| The start fails or passes its 10-second bound | not applicable | log the failure, become Absent, and start again 30 seconds later while Bluetooth still reads `Ready` | not applicable |
| Bluetooth leaves `Ready` | nothing | let the start finish | the joiner table's rows apply, the central stays |
| `join_nearby(host)` or `fetch_offer(host)` while Bluetooth reads `Ready` | start btleplug's thread, become Starting, answer `CentralStarting` | answer `CentralStarting` | the joiner table's rows apply |
| Client closed | nothing | drop the start's answer, which stops its thread | stop the central |

- **Hotspot.** The application calls `NativeClient::host_hotspot()`, answered by a `HotspotOffer { ssid, passphrase, security, port }` or a `HotspotError` (`Unsupported`, `MissingPermission(name)`, `Incompatible`, `NoChannel`, `Disallowed`, `TimedOut`, `Failed`), and `stop_hotspot()`. A joiner calls `join_hotspot(offer)`, answered by the joined network's gateway or a `JoinError` (`Unsupported`, `MissingPermission(name)`, `Declined`, `TimedOut`, `Failed`), and `leave_hotspot()`. `ClientEvent::HotspotStopped` and `HotspotLeft` report what the system ended. Android needs `CHANGE_WIFI_STATE`, `NEARBY_WIFI_DEVICES` (with `neverForLocation`), `ACCESS_WIFI_STATE`, `CHANGE_NETWORK_STATE` and `ACCESS_NETWORK_STATE`, which `connetto-peer-android`'s manifest declares, and the application asks the user for `NEARBY_WIFI_DEVICES` at run time, which the plugin offers to do. A system whose backend has not arrived answers `Unsupported` to every call, Linux, Windows and the Apple systems until slice 8. Every wait is bounded, 30 seconds for a hotspot to start and 60 seconds for a join including the user's approval.

The hosting side against its state, beside the link's and discovery's tables. Nothing in it depends on the device's standing (decision 14).

| Event | Off | Starting | Hosting |
| --- | --- | --- | --- |
| `host_hotspot` | a missing permission answers `MissingPermission`, else request the hotspot, become Starting | wait on the same request | answer the current offer |
| Hotspot started | not applicable | become Hosting, answer the offer, its port present while the device serves | not applicable |
| Hotspot failed | not applicable | become Off, answer the mapped `HotspotError` | not applicable |
| 30 seconds without an answer | not applicable | cancel, become Off, answer `TimedOut` | not applicable |
| System or user stops it | not applicable | become Off, answer `Failed` | become Off, emit `HotspotStopped` |
| `stop_hotspot` | nothing | cancel, become Off, the pending call answers `Failed` | close the reservation, become Off |
| Standing changes | nothing | nothing | nothing, a later `host_hotspot` answers the port as the standing then has it |
| Client closed | nothing | cancel | close the reservation |

The joining side against its state.

| Event | Idle | Requesting | Joined |
| --- | --- | --- | --- |
| `join_hotspot(offer)` | a missing permission answers `MissingPermission`, else request the network for the offer's SSID and passphrase, become Requesting | cancel the earlier request, request this one | release the joined network, request this one |
| Network available | not applicable | become Joined, bind peer sockets for its subnet (decision 15), read its gateway, answer it, and with autolink dial the gateway at the offer's port, discovery taking over after | ignore |
| The dial to the gateway fails | not applicable | not applicable | dial again 2 seconds later while no link to the gateway stands, and 60 seconds after the join stop and emit `JoinedHostUnreached` |
| A link to the gateway stands | not applicable | not applicable | stop dialing |
| Network unavailable, the user declined or the request timed out | not applicable | become Idle, answer `Declined` or `TimedOut` | not applicable |
| 60 seconds without an answer | not applicable | cancel, become Idle, answer `TimedOut` | not applicable |
| Network lost | not applicable | not applicable | stop dialing and binding, become Idle, emit `HotspotLeft`, the links over it close by liveness |
| `leave_hotspot` | nothing | cancel, become Idle, the pending call answers `Declined` | release the network, stop dialing and binding, become Idle |
| Standing changes | nothing | nothing | nothing |
| Client closed | nothing | cancel | stop dialing, release the network |

- **Discovery.** `mdns-sd` advertises `_connetto-peer._tcp.local` under a random instance name drawn at each registration, at the bound port, with a TXT record of `v`, the frame version, and `fp`, the hex SHA-256 of the presented leaf, so the instance and fingerprint change at every renewal and nothing in it names an account. A found instance is dialled at each resolved address, IPv4 first, until one answers. On Android the client holds a `WifiManager.MulticastLock` while it browses, taken through the application's `JavaAccess`.

Discovery against the device's standing and the found instance's state, beside the link's table above. `Serving` is the link's `Serving`, `Idle` is every other standing. An instance is `New` when found, `Linked` while a live link carries its fingerprint, `Waiting` while a retry is pending, and `Barred` after a refusal until its fingerprint changes. Autolink off removes every dial and retry from the table and keeps the rest.

| Event | Idle | Serving, instance New | Serving, Linked | Serving, Waiting | Serving, Barred |
| --- | --- | --- | --- | --- | --- |
| Standing becomes Serving | register the advertisement, start browsing | not applicable | not applicable | not applicable | not applicable |
| Standing leaves Serving | not applicable | unregister, stop browsing, forget every instance | same | same, drop the retry | same |
| Renewal changes the leaf | not applicable | register again under a new name and fingerprint | same | same | same |
| Instance found carrying the device's own fingerprint | not applicable | ignore it | not applicable | not applicable | not applicable |
| Instance found or resolved | not applicable | emit `PeerFound`, dial | emit `PeerFound`, no dial | emit `PeerFound`, keep the retry | emit `PeerFound`, no dial |
| Dial links | not applicable | becomes Linked | not applicable | becomes Linked | not applicable |
| Dial unreachable or timed out | not applicable | becomes Waiting, retry after 5 s | not applicable | retry after twice the last wait, at most 5 min | not applicable |
| Dial refused, or link closed with `PeerRevoked`, `PeerExpired` or `Protocol` | not applicable | becomes Barred | becomes Barred | becomes Barred | not applicable |
| Link closed with `PeerLost`, `Closed` or `Duplicate` | not applicable | not applicable | becomes Waiting, retry after 5 s | not applicable | not applicable |
| Instance found under a new fingerprint for a Barred peer | not applicable | not applicable | not applicable | not applicable | becomes New, dial |
| Instance removed | not applicable | emit `PeerGone`, forget it | emit `PeerGone`, forget it, the link stays | emit `PeerGone`, drop the retry | emit `PeerGone`, forget it |
| Local addresses change | not applicable | browse again | the link stays | drop the retry, become New | stay Barred |
| Inbound link from a found peer | not applicable | becomes Linked | not applicable | becomes Linked, drop the retry | becomes Linked, since the verifier accepted it |
| Instance found while 256 are tracked | not applicable | evict the oldest instance not Linked without an event, then as found | same | same | same |
| A dial is due while 8 dials run | not applicable | queue it, dial when a running dial ends | not applicable | queue the retry the same way | not applicable |
| Inbound connection while 16 handshakes run | close it at accept | close it at accept | same | same | same |
| Autolink turned off at build | browse and report only | no dial | no change | no retry | no change |
- **The link.** TLS 1.3 alone over TCP with rustls and ring, each side presenting its leaf and issuer and signing through the device key, which signs ECDSA P-256 SHA-256 in DER exactly as TLS 1.3's `ecdsa_secp256r1_sha256` needs. Both verifiers wrap webpki over the deployment roots and the kept lists, checking the whole chain against them with unknown status allowed and list expiry ignored (R74), at the local time and, when that fails on validity alone, five minutes either side. They then require the R74 certificate profile, an attestation level in the accepted set, and a device key other than the device's own. The dialled name is ignored, since a device's identity is its URI. TCP connect and the handshake are bounded at 10 seconds each.
- **Frames.** A 4-byte big-endian length, then a MessagePack `PeerFrame` as `connetto-core::codec` encodes the server protocol, at most 4 MiB, a longer one closing the link with `Protocol`. `Hello { version, dial, numbers }` goes first from each side, `dial` the dialler's counter for decision 7 and `numbers` the CRL Number kept per issuer key, then `List` carries each list the other side's numbers lack, `Ping` and `Pong` keep it alive, `Certificate { leaf, issuer }` carries a renewed chain (decision 10), and `Close { reason }` precedes a deliberate close. A received reason other than `Duplicate` is reported as `Closed`, since it names the sender's view. A received list goes to the client's one intake (R74 decision 22), and every list the intake keeps, from the server or from a peer, comes back to the link, which forwards it to every link whose numbers lack it and closes every link it revokes.
- **Events.** `ClientEvent::PeerLinked { peer }` once a link survives decision 7, `PeerUnlinked { peer, reason }` when it closes, `CertificateExpired` on a refused `link_peer`, and `PeerListenFailed` when the listener cannot bind. A refused inbound handshake is logged and raises nothing, since any scanner on the network could cause one.

The link against the device's standing (R74's lifecycle table), the one place a peer event meets a situation. `Serving` is R74's `Fresh` or `Aging`, and `Idle` is `NoKey` or a build without a device identity.

| Event | Idle | Serving | Expired | ClockOff |
| --- | --- | --- | --- | --- |
| Standing becomes `Fresh` or `Aging` | bind the listener | swap the presented certificate, send it as `Certificate` on every link, keep the port and every link | bind the listener | bind the listener |
| Standing becomes `Expired` | not applicable | close every link with `CertificateExpired`, close the listener | not applicable | close the listener |
| Standing becomes `ClockOff` | not applicable | close every link with `ClockOutsideWindow`, close the listener | close nothing more | not applicable |
| Certificate withdrawn or device revoked | not applicable | close every link with `Withdrawn` or `Revoked`, close the listener | become Idle | become Idle |
| `link_peer` | refuse with `NoIdentity` | dial, handshake, `Hello` | refuse with `CertificateExpired` and emit it | refuse with `ClockOutsideWindow`, the event already raised |
| Inbound connection | none arrives | handshake | none arrives | none arrives |
| Handshake verified | not applicable | send `Hello`, emit `PeerLinked` once decision 7 keeps the link | not applicable | not applicable |
| Peer refused by chain, list, window, level or own key | not applicable | close, `link_peer` answers the reason, an inbound one is logged | not applicable | not applicable |
| `Hello` received | not applicable | send each kept list the peer lacks, another version closes with `UnsupportedVersion` | not applicable | not applicable |
| `List` received | not applicable | hand it to the intake | not applicable | not applicable |
| Newer list kept, from the server or a peer | keep it | keep it, forward it to links lacking it, close links it revokes with `PeerRevoked` | keep it | keep it |
| Second link to one device key | not applicable | keep one by decision 7, close the other with `Duplicate` | not applicable | not applicable |
| 15 seconds since the last `Ping` | not applicable | `Ping` the link | not applicable | not applicable |
| 45 seconds without a frame | not applicable | close with `PeerLost` | not applicable | not applicable |
| The peer's certificate passes `not_after` plus five minutes | not applicable | close with `PeerExpired` | not applicable | not applicable |
| `Certificate` received | not applicable | verify the chain as at a handshake for the same key, move the link's deadline, or close with `PeerRevoked`, `PeerExpired` or `Protocol` | not applicable | not applicable |
| `Ping` received | not applicable | `Pong` | not applicable | not applicable |
| Peer closes, I/O fails, or a frame is malformed or too long | not applicable | close with `Closed` or `Protocol`, emit `PeerUnlinked` | not applicable | not applicable |
| Server connection lost | nothing | nothing, links continue | nothing | nothing |
| Listener bind fails | not applicable | emit `PeerListenFailed`, keep dialling, bind again at the next standing change | not applicable | not applicable |
| Client closed | nothing | close every link with `Closed`, close the listener | nothing | nothing |

### Steps

1. Discovery, gateway-probe plus `mdns-sd` under service type `_connetto-peer._tcp.local`, the TXT record carrying protocol version and certificate fingerprint only, discovery treated as fully public with authentication only at the handshake.
2. The link, `rustls` mutual TLS on TCP, both sides verifying against the built-in roots with the revocation lists, refusing expired certificates.
3. Hosting on Android, Windows and Linux, and the typed password entry for a hand-started iPhone or iPad Personal Hotspot.
4. The Bluetooth beacon and exchange on every platform, and joining from every platform.
5. The QR code and text fallback.
6. Platform notes made real, `MulticastLock` for mDNS reception, iOS and macOS local-network permission, Android hotspot and Bluetooth permissions.
7. The on-demand run on two attached phones and the recorded home run.

### Slices

Slices 1 to 8 each merged as one pull request and proved as stated below, and slice 9's 9a, 9b, 9c and 9f legs merged, each ending with an end-to-end join on its own machine.

1. **The `connetto-peer` link on loopback** (step 2, decisions 5, 7 and 8), the crate alone, its verifiers, frames, list forwarding, ping and duplicate rule, taking its certificate, key, roots, lists and clock as inputs, proven by its seventeen loopback tests, which carry R74's proof 4 and proof 6's refusal with a clock set outside the window.
2. **The client's peer link** (R74 step 7's peer half, decision 6), the `peer` feature, the builder setters, the link's table against the enrolment task's standing, the events, and received lists through the one intake, proven against the real server, which carries R74's proof 3, three clients and a server stopped before the reported device dials.
3. **Discovery** (step 1, decisions 12 and 13), `mdns-sd` under `_connetto-peer._tcp.local`, the TXT record carrying the frame version and the leaf fingerprint only, `PeerFound` and `PeerGone`, autolink under the discovery table, and Android's `MulticastLock`, with the gateway probe of decision 11 arriving with slice 4's join API, which knows the gateway and the port.
   **Amended (R76 slice 9f).** An iOS app may send or receive raw multicast only with Apple's managed multicast entitlement, so `mdns-sd` on the iPhone and the iPad never heard another device. Discovery runs behind `connetto-peer`'s `DnsSd` seam, `mdns-sd` stays the default on every other platform, and iOS advertises and browses the declared `_connetto-peer._tcp` through the system Bonjour service in `connetto-peer-apple`.
4. **Android hosting and joining** (step 3's Android part and step 6's Wi-Fi permissions, decisions 11 and 14 to 17), `startLocalOnlyHotspot` and the joining network request through `connetto-peer-android`'s Kotlin plugin, the hotspot tables, the per-socket binding, the gateway probe, the typed refusals naming a missing permission, and the demo hosting, joining and showing its peers, proven on emi with one A35 hosting and the other joining through its application, the runner tapping the approval (decision 16).
5. **The Bluetooth beacon and exchange on Android, without the central** (step 4's Android host part and step 6's Bluetooth permissions, decisions 18, 19 and 21), `BluetoothGattServer` and the legacy advertisement as host, decision 21's readiness on Android, the beacon codec, the framed exchange with rustls over a transport trait, `HotspotOffer` only after both sides proved their identity, and both tables, the joiner's driven through a scanning and connecting backend trait.
6. **The Android central** (step 4's Android joiner part, decision 20), the backend trait over btleplug 0.13.5, with `connetto-peer-android`'s `java_vm` and `use_application_class_loader` and btleplug's Java in its Gradle module (decision 22), proven on emi between the two A35s, one hosting and the other joining through the beacon and exchange.
7. **The text and QR fallback** (step 5, decisions 23 and 24), the `WIFI:` payload, its parser and the offer's three lines, the demo showing the code and the copyable payload and joining from a pasted one, with the reader requiring the closing `;;`, so a payload cut short in a message is refused and never read with a shortened password.
8. **The on-demand run** (step 7's first half, decision 4), `--every-join` on `connetto-android-proof`, every join in one run on the same signed-in phones, with its summary, proven by one run on the two A35s with every join green.
9. **Every other machine** (step 3, 4 and 6 for Windows, Linux, macOS, iPhone and iPad, and step 7's home run), one pull request per system, ordered handhelds first, since the feature is above all for handheld devices where the network is poor or absent. 9a, 9b, 9c and 9f are merged, 9d and 9e are parked.
   - 9a. **iPhone and iPad**, merging both halves in one pull request, since the devices at hand are an iPhone, an iPad and the Mac in one place, and the only end-to-end pair among them is the iPhone hosting and the iPad joining. Joining is the btleplug central over CoreBluetooth, `NEHotspotConfiguration` behind the system's join alert, and the gateway dial under iOS's local-network permission. Hosting is the typed Personal Hotspot and the beacon through `CBPeripheralManager`, which only an iPhone or a cellular iPad can do, and the house's iPad is the Wi-Fi model (`iPad15,7`). Apple lets an application's advert carry only a local name and service UUIDs, so the iPhone's beacon shape is decision 25's. The joined network carries no gateway, so the joined network's gateway is read from the routing table through `sysctl`, the iPhone's listener answers at 172.20.10.1, and iOS refuses an application's first connection to a local address with `EHOSTUNREACH` while it asks for local-network access, which decision 11's redial covers. A Personal Hotspot shows its name to devices outside the owner's account only while its pane is open, so the join asks for the network by name as a hidden one.
   - 9b. **Linux.** Hosting and joining through NetworkManager, the beacon on BlueZ, and the btleplug central, proven by a native client on pippo against a phone in radio range of it, since emi has neither Wi-Fi nor Bluetooth, with decisions 29 to 33.
   - 9c. **macOS.** Joining through CoreWLAN and the btleplug central, the Mac never hosting (decision 1), with decisions 34 and 35, proven by an unattended run of the iPhone's single-phone proof and all three joins.
   - 9d. **Windows**, parked. Hosting through a Wi-Fi Direct advertisement in legacy mode, joining through the WLAN API, the beacon through `GattServiceProvider`, and the btleplug central, with decisions 36 to 41, in four pull requests in this order.
     1. The Windows check job of decision 41 and the btleplug central widened to Windows, so the joiner's Bluetooth half builds and is checked from the start.
     2. `connetto-peer-windows` with hosting and joining (decisions 36 and 38) and `WindowsHotspotBackend`, every table row under a fake of the crate's I/O trait.
     3. The beacon and exchange through `GattServiceProvider` (decision 37) and the readiness through `Windows.Devices.Radios`, as `WindowsBluetoothBackend` under a fake.
     4. The Windows proof leg of decision 40 and its six-join run against a phone, which also records R74 proof 5's Windows `unproven` record, once decision 40's machine is at hand. The same machine measures whether a Windows Hello PRF credential survives a fingerprint re-enrolment, which R53 left unmeasured because its only Windows machine was lent. A credential that does not survive takes the bounded wipe and resync of `start_over` in [windows.rs](../crates/connetto-client/src/keyring/windows.rs).
   - 9e. **The recorded home run**, parked, across every local machine as decision 4's mesh, one round per machine that can host, every other machine joining and every pair linked.
   - 9f. **The handheld star**, the iPhone, the iPad and an Android phone, taken before 9d and 9e, since a field group carries handhelds and these three are the first that must work together offline. Two rounds, the Android phone hosting while the iPhone and the iPad join, then the iPhone hosting while the Android phone and the iPad join, and in each round every device links to every other over the one hotspot. One runner on the Mac drives the iPhone and the iPad through the XCUITest tapper and the Android phone, cabled to the Mac, through adb. It proves what no earlier slice has, an Apple device joining Android's local-only hotspot, an Android joiner reading the iPhone's UUID-only beacon (decision 25), and two joiners on one host at once, and R77 then carries data over this star first. An Android phone under a VPN it cannot bypass cannot dial out over a joined hotspot, so its client reports `JoinedNetworkBlockedByVpn` and stops redialling, while the other devices still reach it by dialling in.

### Done whenTwo native processes with valid certificates discover each other on one network and hold an authenticated link, a device with an expired, revoked or foreign certificate is refused, a device with no shared network learns the hotspot through the Bluetooth exchange or the QR fallback and links, proven by tests on loopback, by the on-demand run on two attached phones, and by the recorded home run's mesh, in which every local machine that can host hosts one round while every other joins and every pair holds a link.

---

## R57: the demo feature gaps the export audit found

**Status.** IN PROGRESS. Steps 1 to 5 and 7 were found built in the demos on `examples/demo-support`, step 6 was found already fixed, and step 8 was built and merged. The remaining work is the human pass of the Proof, of which only the aggregate tracking a write has been watched by hand, on the Tauri app.

**Blocked on nothing.** `R54` claimed every demo carries every application-visible feature. It was true for what it audited and these are what it missed, so this is `R54`'s follow-through rather than a new idea.

**Changes state.** Client tables: none. Server tables: none. Wire messages: none, the step 8 `MutationRejectReason` surfacing riding the client's event stream. Archive format: none, the export pane exercising the archive as it stands.

### Purpose

A capability no demo drives is one nobody has held. That is the lesson `R42` and `R54` already paid for, since auditing the demos against the public API found five library defects a green test suite had hidden. Two of the gaps were worse than an absence, because a demo actively asserted the feature did not exist.

### Steps

The eight steps are all closed. Steps 1 to 5 and 7 were found built in the demos on `examples/demo-support`, a live `COUNT(*)` line in all four UIs, the passkey gate through `Membership::enrol_gate` in the Yew demo beside the Dioxus demo's own gate, the `SyncStatus`, `NonFatal` and `FullResync` status line in every UI, and the device-private tier in the desktop demo. Step 6 was found already fixed, both web demos propagating `await_db_worker_ready()`'s result with `?`. Step 8 was built and merged, `ClientEvent::MutationRejected` carrying the server's `MutationRejectReason` beside the rolled-back rows, the relay forwarding the worker's reason to the tab, and every demo printing it in its status line.

### Proof

Each item is demonstrated in the demo it drives, by a human running it, an aggregate that tracks a write, a gate that enrols, a status line that goes offline and back, a refused subscription that says why, a resync that announces itself, an archive from the desktop demo carrying two databases, and a refused write whose reason reaches the screen. Of the pass, the aggregate tracking a write has been watched by hand on the Tauri app, and the rest remains.

### Done when

Every item above is driven by the demo it names, and no demo comment claims a capability does not exist that the library provides.

### Out of scope

Making the demos runnable in CI, which `R55` settled, since they need a browser, a served bundle and a full stack, and their value is a human clicking them.

---

## R77: the exchange and the provisional tier

**Status.** IN PROGRESS. Slices 1 to 5, 9 and 10 are merged, the chain at version 2 with the signed offer, the provisional bookkeeping, the bot list in the certificate, the peer link frames, the peer apply, a replacement snapshot read as no verdict, and the provisional holding with shared tables. Slices 6, 7 and 8 remain, the exchange driver, online adjudication and the handheld proof. Aimed first at offline data between the iPhone, the iPad and an Android phone over R76 slice 9f's star.

**Blocked on** R75 and R76, both merged for everything this phase uses. R31 is merged, and slices 1, 4 and 5 carry its capture major. The retraction's reason rides on R57 step 8, which surfaces the `MutationRejectReason` the client used to drop.

**Changes state.** Client tables, `_connetto_peer_cursor`, `_connetto_peer_entry`, `_connetto_provisional` and `_connetto_retraction` in `main`, and `settled_seq` and `settled_head` on `_connetto_writers` (decisions 2 and 9). Server tables, none. Wire messages, the chain hash at version 2 covering each entry's capture major and provenance (decision 1), the exchange frames at peer protocol version 2, `ProvisionalStatus` and `ProvisionalVerdicts` on the server wire (decision 11), and the certificate's bot-list extension (decision 12). Archive format, the peer entries and cursors (decision 16).

### Purpose

The peer exchange itself, with every rule the review fixed, per-author-device cursors, the acked frontier against the uncovered-row leak, signed retractions, and the provisional tier with its explicit conflict rule and its own eviction spare clause.

### Decisions

1. **The chain hash covers the capture major and the provenance.** `entry_hash` becomes `connetto-chain-v2` over `previous || seq || major || provenance || SHA-256(changeset)`, and one `range_message` serves the peer and the courier, with no second courier message listing each entry's provenance. A signature over a head then vouches for every entry's major and original writer below it, as a [Scuttlebutt message](https://ssbc.github.io/scuttlebutt-protocol-guide/#message-format) signs its `previous`, `author`, `sequence` and content together. The server hashes the major its connection declared, which is the entry's capture major on the main wire and on R31's drain wire alike, and the provenance from `MutationHeader.provenance`.
2. **Every R77 table lives in `main`, beside `_connetto_pending`.** The provenance, the applied rows and the per-author cursor commit in one transaction, and [SQLite documents](https://www.sqlite.org/lang_attach.html) that under `journal_mode` WAL a transaction spanning attached database files is atomic only within each file. The replica runs WAL. `main` also exists on every build, while the tier exists only when the application declares one. No outbox trigger watches a `_connetto*` table, which keeps these tables device-private as it keeps `_connetto_pending`. This corrects chapter 19, which put the provenance in the device-private tier.
3. **The author's write is judged by replaying it through the policy views as the author.** [`sqlite3changeset_apply`](https://www.sqlite.org/session/sqlite3changeset_apply.html) modifies a row whose old values match without consulting any handler, and it applies to the backing tables, so neither can enforce a policy. The peer apply runs each op as SQL against the logical view, as `replay` already does for the shared tier, with recording off, write exemption off and the author bound. The `INSTEAD OF` guard refuses a `WITH CHECK` violation, and an update or delete that the view's `USING` hides touches no row, which counts as a refusal. Recording is switched off by `UnrecordedWrites`, which raises the second flag the outbox triggers read beside the write exemption, so the policy guards still judge the write and the outbox records none of it.
4. **Identity is rebound on the same connection under a guard.** An `AuthorBinding` guard re-registers `current_app_user()` with the author's certificate `account`, `current_app_subjects()` with `NULL` and `current_bot_list()` with the certificate's bot list (decision 12), and restores the receiver's own values on drop, the `adopt_relay_identity` mechanism with RAII restore. `NULL` subjects make every share-key arm deny, chapter 19's fail-closed rule for rights the receiver cannot answer.
5. **An author's write is applied whole or not at all.** A changeset is one transaction of the author's, so one refused op refuses the entry and none of its rows land. The cursor still advances past it, and the receiver raises `PeerWriteRefused`.
6. **Keys the receiver's own unsent writes hold are filtered before the apply.** The key set is what `pending_spare` already computes. An op on such a key is dropped, the provenance records the author as displaced by the receiver's own write, and the rest of the entry applies. A conflict handler cannot do this, since a matching row applies without invoking it. Any other collision applies, last writer locally in arrival order, with the displaced provisional author recorded. `server_wins` stays with server patches.
7. **One signature per hand-off covers the whole offer.** The device key signs one `offer_message(key_id, settled, newest, builds, retractions)`, so the acked frontier, the retraction log and the queue head cost one chip operation per link, within R74 decision 12's reason for signing per hand-off.
8. **An entry another build captured crosses as a digest.** The offer names the author's builds, its own `(major, version)` and each held major's row from R31's `_connetto_held_batch`. An entry whose `(major, version)` differs from the receiver's build crosses as its manifest row only, so the receiver verifies the chain and advances its cursor and never applies or holds it. A same-major entry under another version is refused the same way, the case R31 refuses at the server.
9. **The settled head is kept.** An acked entry is deleted from `_connetto_pending` today and the hash at the frontier with it. `_connetto_writers` gains `settled_seq` and `settled_head`, written as each acked entry is deleted, so the author can name the hash its offered range chains from. Settlement runs in sequence order, since the server answers a writer's mutations in order.
10. **Every request carries an id and every wait has a bound.** Each exchange request carries a `request_id` every reply quotes, a receiver waits at most 30 s for the next frame of a range on a monotonic clock, and a lapse ends the exchange, never the link, and retries at the next offer.
11. **The server settles the provisional rows of an author the receiver never meets again, with the author's signature as the right to ask.** The frontier and the retraction log reach a receiver only over a link with the author, so without a server path a row the server applied but the receiver's subscriptions do not cover stays spared for ever, and a refused write stays visible as provisional. On every server connection the receiver sends `ProvisionalStatus { request_id, author, offer, seqs }` per author it holds provisional rows from, `offer` being the author's last verified signed offer. The server verifies the signature against the author's key in its writer row, refuses any sequence above the offer's signed `newest`, and answers `ProvisionalVerdicts` per sequence from R75's settled records, `Applied` and `Settled` (at or below the frontier, record pruned) adjudicate, `Refused(cause)` retracts with the cause, `Abandoned` (above the frontier of a retired writer or a revoked enrolment) retracts with cause `Abandoned`, and `Pending` waits for the next connection. A receiver can ask only about entries the author handed it. [Bayou](https://www.cs.cornell.edu/courses/cs6464/2009sp/papers/bayou.pdf) has every replica learn from the primary which tentative writes committed in the same way. Five points were settled with the maintainer on 2026-11-02. The server's pruning at the author's handshake and its revocation sweep delete only `Applied` records, and a rejected or conflicted record stays while its writer row stays, because a refusal at or below the frontier would otherwise answer `Settled` once pruned and the receiver would keep a refused write for ever. An author whose certificate is past its expiry plus the tolerance, with no live enrolment, counts as abandoned like a revoked one, so its entries above the frontier answer `Abandoned`. An author the receiver holds no signed offer from, which only a direct call to `apply_peer_entry` leaves, is not asked, and its rows wait for the author's frontier over a link. One ask carries at most 8192 sequences, the manifest frame's bound, so an author with more is asked about in ascending chunks, and the receiver sends its asks one at a time, the next once the last is answered, lapsed or refused. The server answers an ask before it reads the next message on the session, so it keeps no latch of its own, and a `RateLimited` naming an ask, from the server's general deferral, counts as the 30 s timeout. A key the enrolment store has no record of counts as revoked, as the revocation sweep already treats it, so its sequences above the writer's frontier answer `Abandoned`, and a signature that does not verify, or a sequence above the offer's signed `newest`, answers `NonFatal { related_to: request_id }`, after which the receiver asks again at its next connection.

The server's answer per asked sequence `s` of a verified ask, the writer row's `frontier` being `f`.

| Situation | Verdict |
|---|---|
| a record of `s` with outcome `Applied` | `Applied` |
| a record of `s` with outcome `Rejected(reason)` | `Refused(Rejected(reason))` |
| a record of `s` with outcome `Conflict` | `Refused(Conflict)` |
| no record, `s <= f` | `Settled` |
| no record, `s > f`, writer retired, enrolment revoked, absent, or past expiry plus tolerance | `Abandoned` |
| no record, `s > f`, author still live | `Pending` |

12. **A bot session may enrol a device key, and every certificate carries its bot list.** The maintainer judged that refusing bots hardware could be shortsighted, since small language models may run as bots on field devices. Every certificate carries a non-critical extension under `1.3.6.1.4.1.67105.2`, beside the attestation extension's `.1`, holding the bot list text `bot_list_for` renders for the enrolling session, the wildcard `*` for a person and the list for a bot, the same value the server binds as `app.bot_list`. `AuthorBinding` binds that text, and `NULL` when the extension is absent, so a certificate that does not say fails closed. A changed list reaches peers when the certificate renews, a bounded false allow of the class chapter 19 already accepts for an offline-stale replica.
13. **A resync keeps provisional rows until their verdict, and re-applies the stored entries when the replacement snapshot ends, ahead of the receiver's own unsent writes.** A provisional row is a write the server has not adjudicated, the same kind of write as the receiver's own unsent one, which R15's un-acked rule never lets vanish from its own device and R48 decision 3 puts back with `reapply_pending` once a replacement snapshot ends. A truncate empties its table at once, peer-applied rows and their records with every other row, keeping only the receiver's unsent writes (R11 Part B, O5). Every other resync, an authorization change, a cursor outside retention, an interrupted snapshot or a cursor beyond history, keeps a provisional row and its record in place, as R11 Part B keeps an unsent write (O7, O10). A replacement snapshot that writes a provisional key is no verdict, so during a resync the record stays and no `ProvisionalConfirmed` is raised, while a live server patch on the key still clears the record with `ProvisionalConfirmed`. At the `SnapshotEnd` that ends the resync, the receiver re-applies every entry still in `_connetto_peer_entry`, in the order of its `position`, through the peer apply of decisions 3 to 6, which judges the author's rights again on the resynced replica and rewrites each record, so after a truncate the entries come back and after any other resync the author's values stand over the replacement's. It then re-applies its own pending writes, so its own authorship still wins locally (decision 6). `position` is a durable append position across authors, assigned when an entry is stored and kept across a re-apply, the role a pending entry's position plays for `reapply_pending`, since the wall-clock `received_at` cannot order two entries of one second. An entry the resynced replica now refuses raises `PeerWriteRefused` and stays stored until the author's frontier or the server settles it.
14. **A provisional row is held by the reserved holder `connetto-provisional`, exactly while its provenance record is provisional** (R11 Part B, O7). R11 Part B keeps a row only while a holding names it or an unsent write of this identity keeps it, so the peer apply writes the holding in its own transaction, in `_connetto_holding` for a `main` table and in the store's `_connetto_shared_holding` under this identity for a shared one, and the record's every exit releases it in the transaction that changes the record, a server write on the key (`ProvisionalConfirmed`), the frontier or a verdict moving it to adjudicated, a retraction deleting it, and a truncate dropping the table's records. A retraction that stands a displaced author's record back keeps the holding, since the key is still provisional. A key the receiver's own unsent write holds keeps the holding as well, because its record is provisional too, so one rule covers every record. `holdings::prune`, `release_all` and `release_account` skip the holder as they skip `connetto-unsent`, so a dead subscription, a beyond-history resync and a new store generation leave a provisional row in place, and the resync's re-apply writes the holding again, which an existing one absorbs. The holding's key is the same `quote()` clause every holding uses, rendered from the record's key values, so a provisional delete's record, whose row is gone, still names its key. The pass then keeps a provisional row through its holding alone, with no keep clause of its own.
15. **A peer entry reaches a shared table through the store, as a server patch does.** A build that shares a table keeps the table, its policy view and its holdings in the attached store alone, and `apply_peer_entry` used to address `main` only, so an entry touching a shared table failed with `no such table` and nothing of it landed. The peer apply, its undo and the provisional holding now route a shared table to the store through `shared_route`, the route server patches, rollbacks and undos already take, and the provenance record stays in this identity's own replica beside its entries. Another identity on the device then reads the row before the server rules on it, without its provenance, which is the exposure R11 slice 4 already accepted for an identity's unsent shared writes held under `connetto-unsent`. The identity's departure releases its holdings in the store, and the rows nothing else holds go with them.
16. **An archive carries the peer entries and cursors, and an import re-applies the entries** (decided with the maintainer). Decision 2 puts every peer table in `main`, which the archive does not otherwise carry, so the archive carries the peer state of its own, the stored entries of `_connetto_peer_entry` in their position order and the cursors of `_connetto_peer_cursor` with their last verified offers. The import re-applies the entries through the peer apply of decisions 3 to 6 in its own transaction, so a device restored in camp shows its peer rows again with provenance while still offline, and the `_connetto_provisional` records are the ones that re-apply writes. The entries then stay for the re-apply at a replacement snapshot's end (decision 13), so a snapshot that later replaces a table puts the rows back as it does for every provisional row. An archived cursor merges per author by keeping the further one, which the re-applied entries back, and two cursors at one sequence with different heads name a forked chain, which keeps the replica's own cursor and raises `PeerChainInvalid`. An archived entry at or below the replica's own cursor for its author is not re-applied, since the replica already took it and may have seen it adjudicated since. Both export scopes carry the peer state, which the server does not hold. A build without the `peer` feature, the browser's among them, imports the rest of the archive and reports in the import plan how many peer entries it leaves out, and those rows arrive once the server holds them. The retraction log stays out of the archive. The import retires the archived writer, a retired writer offers nothing and the importing device cannot sign as the archived device key, so a peer holding one of the archived writer's refused entries learns the refusal through slice 7's server path. The archive format's version moves. Rejected: carrying nothing and letting peers send everything again, which loses a restored device's peer rows until the authors are met again or the server answers. Rejected: copying the records, since a copied record names a row the import does not restore and would have to be reconciled with the record the re-apply writes. Rejected: always keeping the replica's own cursor, which asks the authors again for entries the re-apply already wrote. Rejected: re-applying only at the next snapshot's end, which hides the rows until the device is online. Rejected: carrying the retraction log, which no offer could ever send. Rejected: refusing an import into a build without the `peer` feature, which would keep a user from restoring their own unsent writes there.

The `connetto-provisional` holding at every event, for a key in a `main` table and in a shared one (decisions 14 and 15).

| Event | `main` table | Shared table |
|---|---|---|
| Peer apply lands an op, or leaves a key to the receiver's own unsent write | write the holding in `_connetto_holding` | write it in `_connetto_shared_holding` under this identity |
| Peer apply refused, or failed | the transaction rolls back, no holding | same |
| Re-apply at the `SnapshotEnd` ending a resync | write it again, an existing one absorbs it | same |
| Server write on the key (`ProvisionalConfirmed`) | release it with the record | same, in the store |
| Replacement snapshot write during a resync (slice 9) | keep it with the record | same |
| Frontier or verdict adjudicates the record | release it with the record | same |
| Retraction deletes the record | release it | same |
| Retraction stands a displaced author's record back | keep it | same |
| Retraction whose undo is refused | nothing changes | same |
| Truncate drops the table's records | release the table's holdings | release this identity's holdings of the table |
| Subscription ends, `prune` | skip the holder | same |
| Beyond-history resync, `release_all` | skip the holder | same |
| New store generation, `release_account` | skip the holder | n/a, the store is new |
| Identity departs the store | n/a, the replica goes with it | `release_identity` releases it |
| R31 rebuild at a newer major | dropped with the tables | dropped with the store's tables |
| Eviction pass | keeps the row | keeps the row for every identity |

### Decisions for the exchange driver, taken with the maintainer

17. **A request names its whole range and the receiver's build.** `Request { request_id, from, to, build }` asks for `from..=to`, `to` being the `newest.seq` of the offer the receiver verified, and the sender serves exactly that range, so an entry the author writes after signing never reaches a manifest checked against the older head. This is how a [Hypercore request](https://github.com/holepunchto/hypercore/blob/main/lib/messages.js) names the length whose signed tree it wants proven. `build` is the receiver's own `(major, version)`, and the sender sends a body only for an entry captured under it, the others crossing as their manifest row alone (decision 8), since a device with no device writer sends no offer and the sender would otherwise not know the receiver's build. Rejected: serving up to the sender's last sent offer, which bends the state table's rule that an offer arriving during a request waits for its end, and serving everything the sender holds, which sends bodies for nothing.
18. **A range the author no longer holds is answered `NotAvailable`.** An entry the server answered meanwhile has left the author's queue, so the author cannot serve a range starting at or below its settled frontier and answers `End { outcome: NotAvailable }`, kept apart from `Busy` as Hypercore's `noData` keeps `NOT_AVAILABLE` apart from `INVALID_REQUEST`. The receiver drops the request and acts on the author's next offer, which every settlement produces. A device whose writer retired answers every request `NotAvailable`.
19. **The signed offer is one `connetto-core` type.** `Offer { builds, settled, newest, retractions }` and `SignedOffer { offer, signature }` live in `connetto-core::exchange`, the peer message carries `SignedOffer`, the cursor row stores it as MessagePack, and the server decodes the same type in slice 7. The signed bytes are rebuilt from the fields by `offer_message`, as Hypercore rebuilds `treeSignable` from the tree fields it stores beside the signature, so no encoding has to round-trip byte for byte.
20. **A head contradicting a verified one is caught at the cursor.** The receiver keeps one verified `(seq, head)` per author, its cursor, and raises `PeerChainInvalid` when the offer's `settled` or `newest` sits at the cursor's sequence with another head. A fork below the cursor is left to the server's chain check at settlement. Rejected: keeping every verified hash, which adds a column and catches forks the server refuses anyway.
21. **The exchange protocol is a sans-IO crate, `connetto-exchange`.** No Rust crate fits. Hypercore, p2panda, iroh-docs, willow25 and bamboo all sign with ed25519, which the [Secure Enclave](https://developer.apple.com/documentation/security/protecting-keys-with-the-secure-enclave) ("works only with NIST P-256 elliptic curve keys") and [StrongBox](https://android.googlesource.com/platform/hardware/interfaces/+/refs/heads/master/security/keymint/aidl/android/hardware/security/keymint/IKeyMintDevice.aidl) ("StrongBox implementations do not support CURVE_25519") cannot hold. Each would also replace a merged piece of connetto, the chain the server recomputes, the SQLite queue or the mTLS link, and an upstream signature PR would fix none of those. The crate holds the exchange messages and the R0 to R3 and S0 to S2 state machine, fed events and returning actions with no SQLite, runtime or socket, as `quinn-proto` holds QUIC's. `connetto-peer` carries its messages, and the client's enrolment task feeds it and carries out its actions.
22. **A stalled request is retried by both sides.** An author that answered `Busy` sends its current offer again to that peer once its stream ends, and a receiver whose request timed out requests again from its kept offer after a backoff of 5 s doubling to 5 min while the link lives, reset by a completed range or a fresh offer. Either alone leaves a case stalled until the author writes again.

### Design

**The wire.** `PROTOCOL_VERSION` is 2, which the hello, the TXT record and the beacon carry together, so a version 1 and a version 2 device refuse each other at hello. The writer is never sent, since each device offers only its own device writer's chain and the link has proven its key. A device with no device writer offers nothing and still receives. The messages are built in `crates/connetto-exchange/src/message.rs`, carried as `PeerFrame::Exchange` in `crates/connetto-peer/src/frame.rs`, with their shared types in `crates/connetto-core/src/exchange.rs`. The server's `ProvisionalStatus` and `ProvisionalVerdicts` of decision 11 are built with slice 7.

```text
connetto-exchange Message, carried as PeerFrame::Exchange
  Offer(SignedOffer)
  Transfer::Request  { request_id, from, to, build: (Major, SchemaVersion) }
  Transfer::Manifest { request_id, previous: [u8; 32], rows: Vec<ManifestRow>, last }
  Transfer::Body     { request_id, seq, changeset }
  Transfer::End      { request_id, outcome: EndOutcome }

connetto-core exchange
  Offer       { builds, settled: Option<Frontier>, newest: Option<Frontier>, retractions }
  SignedOffer { offer, signature }
  Frontier    { seq, head: [u8; 32] }
  ManifestRow { seq, major, origin: Option<Origin>, digest: [u8; 32] }
  Origin      { writer, seq }
  Retraction  { seq, cause: RetractCause }
  RetractCause = Rejected(MutationRejectReason) | Conflict | Abandoned
  EndOutcome   = Complete | Busy | NotAvailable

the server wire, remaining with slice 7
  ProvisionalStatus   { request_id, author, offer, seqs }
  ProvisionalVerdicts { request_id, verdicts }
  Verdict = Applied | Settled | Refused(RetractCause) | Abandoned | Pending
```

The manifest always runs to the request's `to`, the `newest` of the offer the receiver verified, at most 8192 rows per frame, so the receiver verifies the chain from `previous` to the signed head before any body arrives. Bodies follow in sequence order for the entries captured under the request's build, each checked against its digest. `RetractCause::Conflict` exists because R75's `SettledOutcome::Conflict` applied nothing and carries no `MutationRejectReason`. A row names an entry's original writer as an `Origin`, the writer and sequence of a `Provenance` without the number writer's secret, which names that writer to the server (R70 decision 22) and so must never reach a peer. The type has no place for it, and the chain hash covers the same two fields.

**One exchange, as the receiver sees it.** The link comes up and each side sends its offer, whose signature the receiver checks against the key the link proved. The receiver applies the offer's frontier and retractions and moves its cursor up to `settled` when that is higher, then requests `cursor.seq + 1 ..= newest.seq` under its own build when `newest.seq` is above the cursor (decision 17). It recomputes the chain over the manifest from the cursor's head and compares the last hash with `newest.head`, and a mismatch, or a `settled` or `newest` head contradicting the cursor's head at its sequence (decision 20), raises `PeerChainInvalid` and ends the exchange. Each row then commits in its own transaction. A row at another build advances the cursor alone. Otherwise the body is checked against its digest, keys held by the receiver's own unsent writes are filtered, the remaining ops run through the views under `AuthorBinding`, and the provenance, the stored entry and the cursor are written, or on refusal the cursor alone.

**Client tables, in `main`.**

```sql
CREATE TABLE _connetto_peer_cursor (author BLOB PRIMARY KEY, account TEXT NOT NULL, seq INTEGER NOT NULL, head BLOB NOT NULL, offer BLOB NOT NULL);
CREATE TABLE _connetto_peer_entry (author BLOB, seq INTEGER, major INTEGER NOT NULL, changeset BLOB NOT NULL, account TEXT NOT NULL, bot_list TEXT, received_at INTEGER NOT NULL, position INTEGER NOT NULL, PRIMARY KEY (author, seq));
CREATE TABLE _connetto_provisional (tbl TEXT, pk BLOB, author BLOB NOT NULL, seq INTEGER NOT NULL, account TEXT NOT NULL, received_at INTEGER NOT NULL, state INTEGER NOT NULL, displaced_author BLOB, displaced_seq INTEGER, PRIMARY KEY (tbl, pk));
CREATE TABLE _connetto_retraction (writer TEXT, seq INTEGER, cause BLOB NOT NULL, PRIMARY KEY (writer, seq));
-- _connetto_writers gains settled_seq INTEGER and settled_head BLOB
```

`_connetto_peer_entry` keeps an applied entry until it is adjudicated, because a retraction restores the rows by inverting the author's changeset, as R57's rollback inverts the device's own. A provisional delete keeps its provenance row, so a retraction can restore the row from the changeset's old values. The author's account, its certificate's bot list and `received_at` live once per entry, so decision 13's re-apply binds the bot list the entry was judged under, and a retraction restores a displaced author's record from that author's stored entry, or leaves the key to ordinary coverage when that entry was already adjudicated or retracted. A certificate that carries no bot list stores `NULL`, which fails closed again at the re-apply. `received_at` is the wall clock and serves display only. `offer` is the author's last verified signed offer, which decision 11 presents to the server. `_connetto_retraction` is the author's own log, written when the server rejects or conflicts one of its device writer's entries and kept until the writer retires. R31's rebuild drops `_connetto_provisional` and `_connetto_peer_entry` with the synced tables and keeps the other two.

**The client surface.** Events `ProvisionalApplied`, `ProvisionalConfirmed`, `ProvisionalRetracted { cause }`, `PeerWriteRefused` and `PeerChainInvalid`, each naming the author and the affected rows. Reads `NativeClient::provenance(table, key)` and `provisional_rows()`, from the table, since the broadcast event stream can drop events. There is no builder setting, and a build with the `peer` feature and a device identity exchanges with every linked peer.

**Adjudication.** A server write on a provisional key deletes its provenance in the same transaction and raises `ProvisionalConfirmed`, whether or not it is the author's write. Rows at or below the author's `settled.seq` and absent from the retraction log become adjudicated, their stored entries are deleted, and the spare clause stops covering them. A retraction inverts the stored entry for the rows whose provenance still names it, restores a displaced author's provenance, deletes the entry and raises `ProvisionalRetracted`. Decision 11 settles the rest online.

**Limits stated in chapter 19.** A receiver applies an author's update or delete only to rows it holds, since an `UPDATE` change carries the changed columns and the primary key alone. A peer's change to a row the receiver never held is refused.

### Every event in every situation

Both roles run on every link at once. For one author as the receiver sees it, **R0** not linked, **R1** linked with no offer verified, **R2** offer verified and nothing to request, **R3** a request outstanding. For the sender, **S0** not linked, **S1** linked and serving no request, **S2** serving a request.

| Event | R0 | R1 | R2 | R3 | S0 | S1 | S2 |
|---|---|---|---|---|---|---|---|
| Link up | send own offer, enter R1 | n/a | n/a | n/a | enter S1 | n/a | n/a |
| Link lost | nothing | enter R0 | enter R0 | abort request, the current row's transaction rolls back, enter R0 | nothing | enter S0 | stop streaming, enter S0 |
| Offer received, signature valid | n/a | apply frontier and retractions, move the cursor up to `settled`, request the rest under the own build (R3) or enter R2 | same as R1 | keep it, act on it at `End` | n/a | n/a | n/a |
| Offer received, signature invalid | n/a | `PeerChainInvalid`, stay R1 | same | abort request, same | n/a | n/a | n/a |
| Offer received, `settled` or `newest` contradicts the cursor's head (decision 20) | n/a | `PeerChainInvalid`, stay R1 | same | keep it, checked when acted on at `End` | n/a | n/a | n/a |
| Manifest chain mismatch or fork | n/a | n/a | n/a | `PeerChainInvalid`, abort, enter R1 until a new offer | n/a | n/a | n/a |
| Body digest mismatch | n/a | n/a | n/a | same as a chain mismatch | n/a | n/a | n/a |
| A frame out of order within a request, or `End { Complete }` before every row | n/a | n/a | n/a | same as a chain mismatch | n/a | n/a | n/a |
| A reply quoting another request id, or arriving with no request outstanding | n/a | ignore it | ignore it | ignore it | n/a | n/a | n/a |
| Row at another build | n/a | n/a | n/a | advance the cursor alone | n/a | n/a | n/a |
| Row refused by policy | n/a | n/a | n/a | advance the cursor, `PeerWriteRefused` | n/a | n/a | n/a |
| Row applied | n/a | n/a | n/a | provenance, entry and cursor in one transaction, `ProvisionalApplied` | n/a | n/a | n/a |
| `End { Complete }` | n/a | n/a | n/a | act on a kept offer or enter R2, reset the backoff | n/a | n/a | n/a |
| `End { Busy }` | n/a | n/a | n/a | act on a kept offer or enter R2, the author re-offers when its stream ends (decision 22) | n/a | n/a | n/a |
| `End { NotAvailable }` | n/a | n/a | n/a | drop the request, act on a kept offer or enter R2 until the next offer (decision 18) | n/a | n/a | n/a |
| 30 s without the next frame | n/a | n/a | n/a | abort request, enter R1, request again from the kept offer after the backoff (decision 22) | n/a | n/a | n/a |
| Adopting an offer or applying a row fails to commit | n/a | the cursor returns to the one the replica holds | same | the row's transaction rolls back, the commits queued after it are dropped, the cursor returns to the one the replica holds, abort request, enter R1, request again from it after the backoff | n/a | n/a | n/a |
| Retry backoff elapses | n/a | request again from the kept offer, enter R3 | n/a | n/a | n/a | n/a | n/a |
| Request received | n/a | n/a | n/a | n/a | n/a | stream the range with bodies for the request's build alone, enter S2, or answer `End { NotAvailable }` when the range starts at or below the settled frontier (decisions 17 and 18) | refuse with `End { Busy }`, send the current offer when the stream ends (decision 22) |
| Own stream ends | n/a | n/a | n/a | n/a | n/a | n/a | send the offer that waited, or the current one to a peer answered `Busy`, enter S1 |
| Own new local write | nothing | nothing | nothing | nothing | nothing | sign and send a fresh offer | send it after the stream ends |
| Own entry settled, applied | nothing | nothing | nothing | nothing | nothing | update `settled_*`, fresh offer | after the stream |
| Own entry rejected or conflicted | nothing | nothing | nothing | nothing | write `_connetto_retraction` | same, fresh offer | after the stream |
| Peer certificate renewed | n/a | nothing | nothing | nothing | n/a | nothing | nothing |
| Peer revoked or expired, link closes | as link lost | as link lost | as link lost | as link lost | as link lost | as link lost | as link lost |
| Server patch on a provisional key | delete provenance, `ProvisionalConfirmed` | same | same | same, after the current row's transaction | n/a | n/a | n/a |
| Eviction pass | keeps a row `connetto-provisional` holds | same | same | same | n/a | n/a | n/a |
| Replacement snapshot writes a provisional key during a resync | keep the record, raise nothing | same | same | same, after the current row's transaction | n/a | n/a | n/a |
| Truncate notice for a table | delete its rows and records at once, keep unsent writes and stored entries | same | same | same, after the current row's transaction | n/a | n/a | n/a |
| Replacement snapshot ends (`SnapshotEnd` ending a resync) | re-apply every stored entry through the peer apply, then own pending writes | same | same | same, after the current row's transaction | n/a | n/a | n/a |
| R31 rebuild at a newer major | provisional rows and entries dropped, cursors kept | links close first, as R0 | same | same | n/a | n/a | n/a |
| Archive import | peer tables restored, as R0 | n/a, import runs closed | n/a | n/a | own writer retired, its retraction log kept | n/a | n/a |
| Own device writer retired | nothing | nothing | nothing | nothing | offers stop until the fresh key enrols | offers stop, every request answered `End { NotAvailable }` | stop streaming with `End { NotAvailable }`, enter S1 |
| Server session established | send `ProvisionalStatus` per author with provisional rows | same | same | same | n/a | n/a | n/a |
| `ProvisionalVerdicts` received | adjudicate, retract or keep per verdict | same | same | same, after the current row's transaction | n/a | n/a | n/a |
| 30 s without `ProvisionalVerdicts`, or `RateLimited` or `NonFatal` naming the ask | drop the wait, ask at the next server session | same | same | same | n/a | n/a | n/a |

### Steps

1. The exchange, mTLS session, mutual signed offer of queue bounds plus acked frontier plus retraction log (decision 7), request from cursor plus one, a verified manifest then bodies streamed in order, cursor advanced in the same transaction as the apply.
2. A peer apply suspends capture as `apply_patch` does, or a peer's write re-uploads as the receiver's own, and keeps the policy guards on (decision 3).
3. The conflict rule, the receiver's own unsent write is never displaced (decision 6), any other collision applies last-writer-locally with the displaced author recorded, `server_wins` stays reserved for server patches.
4. Offline authorization, the translated policy evaluated as the author through `AuthorBinding` (decisions 3, 4 and 12), failing closed on share-key and locally unanswerable rights, with the partial-replica false-deny and bounded false-allow limits stated in chapter 19.
5. `_connetto_provisional` in `main` (decision 2), provenance per (table, pk), a `connetto-provisional` holding per provisional record (decision 14), server-sourced writes clearing provenance, typed provenance reads and applied, confirmed and retracted-with-cause events.
6. The acked frontier marks at-or-below provisional rows adjudicated, handing them to ordinary coverage, a signed retraction removes the copy and surfaces its cause, and the server settles the rest online (decision 11).

### Slices

Slices 1 to 5, 9 and 10 are merged, each one pull request, and slices 6 to 8 remain.

1. **The chain at version 2 and the signed offer** (decisions 1 and 7), `connetto-core` with the server's chain recomputation and `courier_deliver` moved to version 2, and a public `verify_range` a peer and the server call. Needs R31's per-entry major. Merged.
2. **The provisional bookkeeping** (decision 2, step 5), `connetto-client` with no wire, `_connetto_provisional`, the eviction keep for provisional rows, the server-write clearing with `ProvisionalConfirmed`, and the reads, proven by tests that write provenance directly and then evict and patch. The other three tables arrive with the slices that write them. Needs nothing from R31. Merged.
3. **The bot list in the certificate** (decision 12), `connetto-core`'s certificate profile, the server's enrolment and renewal filling the extension from `bot_list_for`, and its reading on the client and on a linked peer. Needs nothing from R31. Merged.
4. **The peer link frames** (the wire above), `connetto-peer` at protocol version 2, the frames, a bounded request path through `LinkCommand` and a `PeerEvent` for exchange traffic, proven between two nodes on loopback. Needs R31's `Major`. Merged.
5. **The peer apply** (decisions 3 to 6, 8 and 13), `connetto-client`, from a verified manifest and bodies to applied rows, refusal and retraction, under `UnrecordedWrites`, `_connetto_peer_entry` and `AuthorBinding`, and the re-apply of stored entries when a resync's replacement snapshot ends, proven in process with two replicas and no link. Needs R31. Merged.
6. **The exchange driver**, in four pull requests (decided with the maintainer), each green on its own. The driver runs in the enrolment task, which already receives the link's events and holds the device key, and reaches the replica through the client's lock. An offer names no peer, so one signature serves every linked peer, and writes arriving while a signature is in flight fold into the next offer.
   - **6a, the author's state** (decisions 7 and 9). `settled_seq` and `settled_head` on `_connetto_writers` written as each acknowledged entry leaves the queue, `_connetto_retraction` written when the server rejects or conflicts one of the device writer's entries and kept until the writer retires, and the offer built from them with the build's majors, signed by the device key and sent to every linked peer on link up and on every change the state table names. Proven in process against a bare peer node. Merged.
   - **6b, the exchange both ways** (decisions 8, 10 and 17 to 22, the receiver flow above). The `connetto-exchange` crate with the messages, `SignedOffer` in `connetto-core`, and the state machine of the table above tested row by row without SQLite or sockets. In the client, `_connetto_peer_cursor`, the offer's signature checked against the key the link proved, a request naming its range and build with an id every reply quotes, the manifest's chain checked against the signed head with `PeerChainInvalid` on a mismatch, each row applied with its cursor in one transaction, the 30 s bound per frame with the backoff retry, and the sender serving a range, answering `Busy` or `NotAvailable`, and re-offering after a stream. Proven between two clients on loopback. Merged.
   - **6c, the archive and the rebuild** (decision 16). The peer entries and cursors in the archive, the import's re-apply and its per-author cursor merge, and R31's rebuild keeping the cursors and the retraction log while it drops the entries and the records. Merged.
   - **6d, the harness proof.** Two clients of different accounts against the real server, exchanging offline and reconciling online, as a Docker-gated test in `connetto-test-harness` with both native clients in one process on the loopback peer link, against Postgres, OpenFGA and the mock identity provider. The demo deployment gains a team table both accounts read and write through membership rows of a synced members table its policy reads, so slice 8's handheld run exercises the same shapes. Membership is rows rather than subjects because the receiver judges an author with no subjects bound (decision 3). The proof applies alice's offline row on bob's replica, removes alice from the team while both are offline, and reconnects them. The server refuses alice's row with `Unauthorized`, alice's next offer carries the retraction, and bob rolls the row back with that reason, beside a row the server sends bob, which confirms, a row it never sends him, which the frontier adjudicates, and an eviction that spares every provisional row throughout. The refusal of an author never met again stays slice 7's. Merged.
7. **Online adjudication** (decision 11). Both server messages, proven Docker-gated by a receiver that never meets the author again and still confirms an applied write, retracts a refused one with its reason, and retracts an abandoned one after the author's enrolment is revoked.
8. **The handheld proof.** The demo controls the runner drives and a run on R76 slice 9f's star (iPhone, iPad, Android) per R76 decision 4, which runs before each peer phase closes, and chapter 19's R77 statements marked built.
9. **A replacement snapshot is no verdict** (decision 13, after R11 Part B lands). During a resync a snapshot write on a provisional key keeps its record and raises no `ProvisionalConfirmed`, and the re-apply at `SnapshotEnd` puts the author's values back over the replacement's, proven in process for an authorization change, a cursor beyond history and a truncate. It needs nothing from slices 6 to 8 and may land before them. Merged.
10. **The provisional holding and shared tables** (decisions 14 and 15). The `connetto-provisional` holding written and released with each record, the reserved-holder skip in `prune`, `release_all` and `release_account`, the pass keeping provisional rows through their holdings alone, and the peer apply, its undo and the holding routed to the store for a shared table, proven in process for every release and for a shared table's apply, retraction and adjudication. Merged.

### Done when

Two devices of different accounts exchange offline and reconcile online with nothing lost and nothing double-applied, an offline exchange applies policy-passing writes provisionally, a reconnect confirms covered rows, adjudicates uncovered ones by the frontier, rolls back a refused write on both devices with its reason surfaced, also when the two devices never meet again, and eviction spares provisional rows throughout, proven end to end in the harness and on the handheld star.

---

## R78: courier recovery

**Status.** NOT STARTED. Designed with the maintainer on 2026-11-02, decisions 1 to 25 below, in nine pull requests, slice 5 open as [#387](https://github.com/connetto-rs/connetto-rs/pull/387).

**Blocked on** R77 slice 7, open as [#377](https://github.com/connetto-rs/connetto-rs/pull/377), for slices 1a, 3a, 4 and 6. Slice 1a reads #377's `settled_range`, its `verdict_of` ruling, its pruning that keeps refused records and its `certificate_live_past_skew`. Slice 3a changes the adjudication code #377 adds to the client (`settle_provisional_ask`, `adjudicate_peer_seq`). Slice 4 builds its candidates on #377's `ProvisionalStatus` asks and settles a courier answer the way #377 settles `ProvisionalVerdicts`. Slices 1b, 2, 3b and 5b wait only for the slices before them, and slice 5 waits for nothing. R75 and R31 are merged.

**Changes state.** Client tables, `_connetto_peer_entry` gains `state`, `origin` and `digest` with a nullable `changeset` (decision 19), `_connetto_peer_cursor` gains `expires_at` (decision 17), and `_connetto_acked` in `main` (decision 21). Server tables, `bot_list` on `connetto_device_enrolments` (decision 9), `courier_user_id` and the `write_couriered` label on `auth_events` (decision 10), and a courier notice table in the `Writers` member (decision 12). Wire messages, `CourierRange`, `CourierBody` and `CourierAnswer` (decision 5), `CourierNotice` and `CourierNoticeAck` (decision 12), `HandshakeAck.courier` (decision 1) and `MutationHeader.refused` (decision 23), with `range_message` and `verify_range` deleted (decision 2). Archive format, the courier-held rows with their state (decision 20).

### Purpose

Field survivability, a lost device's work is recoverable from the camp. The server accepts author-signed changesets from any certified bearer, deduplicated by the same frontier every path consults. The boundary is cryptographic, not trusted. The courier authenticates as itself and can only relay verbatim what the author's device key signed, its signed offer covering the chain, whose hashes bind each entry's capture major and provenance original, so a courier cannot strip or substitute either, and its own document rights are never consulted. What a malicious courier can do is withhold, no worse than not couriering, with sequence-gap refusal preventing reordering, and delay, no worse than the author's own delayed reconnect, policy evaluated at apply time. The maintainer's impersonation concern is met by this confinement and rides as five constraints, none optional.

### Steps

1. The server courier path, **deployment-opt-in, default off**, verify the author's signature against its enrolment, apply under the author's identity, refuse sequence gaps, dedup by the frontier.
2. The five constraints, which ride with the path because it is the first place session identity and write author differ, a distinct R13 `auth_events` record per couriered application naming courier and author, per-courier metering under R19's machinery, refusal of revoked enrolments, author notification on next login, and acceptance governed by the account's and enrolment's standing at apply time, since signatures carry no trusted timestamp, with the author's certificate live at delivery, a same-key renewal keeping the author's records and standing.
3. The client side, offering another author's held changesets for courier upload, explicit, never automatic.
4. A courier re-delivering the author's changesets to a server restored to an earlier point, whose rewound frontier accepts them again byte for byte, and the author re-sending its own retained acknowledged entries the same way, demonstrated and written into chapter 20 beside R70's legs (R70 decision 20).

### Decisions, taken with the maintainer

1. **The courier path is a deployment switch, and the handshake tells the client.** `ServerBuilder::courier(true)` and `CONNETTO_COURIER=on` in the binary turn it on, off by default. `HandshakeAck` gains `courier: bool`, so the client's offer answers `CourierUnavailable` without uploading anything, and a courier frame reaching a server with the path off is refused `CourierRefusal::Disabled`. Rejected: the switch alone, under which a client learns it is off only after uploading a range. Rejected: a per-account consent beside the deployment switch.
2. **The courier presents the author's stored `SignedOffer`.** No client ever signs `range_message`. The exchange signs one `SignedOffer` per hand-off over the settled and newest frontiers, the builds and the retraction log (R77 decision 7), and the receiver keeps the author's last verified one in `_connetto_peer_cursor.offer`, so `courier_deliver`'s range signature was one nobody could produce. The server checks the offer with `verify_offer` against the author's key and recomputes the chain from its own head to `offer.newest.head`. `range_message`, `verify_range` and the signature argument of `courier_deliver` are deleted, and R77 decision 1's sentence on one `range_message` serving both paths is corrected in slice 1a. Rejected: a second range signature per hand-off, which doubles the chip operations R77 decision 7 bounds to one.
3. **A receiver keeps every manifest row it verified and every body of its own build.** Today `_connetto_peer_entry` holds only applied entries, so a local policy refusal on the receiver's partial replica, a false deny chapter 19 accepts, or an entry of another build leaves a hole no chain walk can cross, and the table holds no origin, so a carried entry's hash cannot be recomputed at all. Until adjudication the receiver keeps the manifest row (`seq`, `major`, `origin`, `digest`) of every entry it verified, and the body of every entry captured under its build, a refused one in a courier-only state that is never applied locally. The courier sends bodies and digest-only rows up to the signed head, and the server settles bodies in order and stops at the first digest-only row. Rejected: couriering an author only when every body up to the offer's newest applied locally, under which one local false deny stops couriering that author for good.
4. **The server skips the settled prefix.** A range may start at or below the writer's frontier. The server skips those rows without applying or auditing them, answers each with the verdict R77 decision 11's ask would give (`Applied`, `Refused` or `Settled`), and checks the chain from its own head at the frontier, so one round trip suffices and an author reconnecting meanwhile turns nothing into a gap. Rejected: the courier trimming by a prior `ProvisionalVerdicts` with today's `SequenceGap` for anything else, which races the author's own reconnect.
5. **The wire is a control header, bulk bodies and one answer.** `CourierRange { request_id, author, offer, rows }` on the control plane carries the offer's MessagePack bytes and at most `MAX_MANIFEST_ROWS` manifest rows from the first row the courier holds to the offer's newest. One `CourierBody { request_id, seq, changeset_zstd }` per row with a body follows on the bulk plane in sequence order, as a mutation's header and patch travel, and draws on the meter as it arrives. One `CourierAnswer { request_id, outcome }` ends the exchange, a refusal before anything settled or one outcome per row. Rejected: one control frame carrying every body, bounded by the 64 MiB WebSocket frame.
6. **A couriered entry's capture major is the connection's.** A courier holds bodies of its own build alone (R77 decision 8), so the major its connection declared is each body's, as `courier_deliver` takes it today, and the chain hash refuses a relabel. Rejected: a major per entry on the wire, redundant with the hash.
7. **R31's rebuild keeps the courier-held rows.** The rebuild at a newer major keeps every row of `_connetto_peer_entry` with its major and the offers, turns a provisional row into a courier-only one, since its record goes with the synced tables, and the courier delivers rows of an older major on R31's drain wire for that major, as its own held writes drain. Rejected: dropping them as today, under which a camp whose couriers all updated before reaching the server loses the lost device's work.
8. **A number-writer origin stops the range.** A carried entry naming a number writer as its original arises from a browser archive imported into a native device, and the courier never holds the number's secret (R70 decision 22). The server answers that row `NotCouriered` and settles nothing past it, and the author's own device delivers it with the secret. Such an entry is lost with the author's device. Rejected: settling the original by its origin alone, which after a restore re-issued that number to another replica of the account settles the wrong writer and forks the legitimate one. Rejected: applying it plain, as today, which the original writer's own replay applies a second time.
9. **A couriered entry applies under the bot list the author's certificate carries.** `courier_deliver` binds `bot_list: None` today, which `bot_list_for` renders as the person wildcard `*`, so a bot author's couriered write would run with person rights. `connetto_device_enrolments` gains `bot_list TEXT`, written at enrolment, renewal and reissue with the text the certificate's extension carries (R77 decision 12), and the courier path binds it for the author, `NULL` failing closed, with no subjects, as a peer binds an author (R77 decisions 3 and 4). Rejected: the bot's current list from the bot registry at apply time, blocked since no server code resolves a bot's list yet. Rejected: refusing bot authors, which leaves field bots without recovery.
10. **Every couriered entry that settles writes one `write_couriered` audit row naming both accounts.** `AuthOp::WriteCouriered`, applied or refused alike since both fix the sequence, with `user_id` the author, `writer` and `seq` the author's, `path` courier, `session` the courier's session, and a new nullable `courier_user_id` column naming the courier's account. A range refused before anything settles goes to the structured log, as every denial does. A deployment's migration adds the column and the enum label. Rejected: naming the courier by `session` alone, which an erasure or session cleanup leaves pointing at nothing. Rejected: the courier's account in `acted_by`, a column meant for staff acting with no session.
11. **Each courier draws on a courier byte bucket of its own.** `ThrottleConfig::with_courier_bytes(bytes, window)`, a token bucket keyed by the courier's `meter_key`, defaulting to the mutation bucket's 64 MiB a minute, so relaying a teammate's backlog never starves the courier's own writes and an operator sizes it apart. A body arriving past the budget ends the range with `Retry` and the answer carries the wait. Rejected: drawing on the courier's own mutation bucket.
12. **The author's next login hears what was couriered.** A deployment table in the `Writers` member keeps one row per couriered entry that settled, naming the author's account, the courier's account, the writer, the sequence and the outcome. After the handshake of the author account's next identified session the server sends `CourierNotice { notice_id, notices }`, at most `MAX_MANIFEST_ROWS` rows per frame, the client raises `ClientEvent::WritesCouriered` and answers `CourierNoticeAck { notice_id }`, and the server deletes those rows. Rejected: reading the audit rows back at login, a second audit read against audit.rs's contract and bounded by the deployment's audit retention. Rejected: an application hook, which reaches no session unless the application builds it.
13. **A banned author's range is refused unsettled.** `CourierRefusal::AuthorBanned`, nothing settles, so the entries stay deliverable after a lift, as the author's own pending writes would. Rejected: settling each entry `Rejected(Unauthorized)`, which a lift could never bring back.
14. **The author's certificate counts until its expiry plus `DEVICE_SKEW`.** The courier path accepts while `certificate_live_past_skew` holds, the instant at which #377's ask turns `Pending` into `Abandoned`, so the two paths agree. A same-key renewal keeps the writer row, its records and its standing. Rejected: `certificate_live`, under which for five minutes the ask answers `Pending` while the courier is refused.
15. **A revoked key is refused whatever the reason.** Reporting a device lost revokes it, so from that moment no courier delivers its unsent work and peers retract that work as `Abandoned`. `revoke_device`'s documentation and chapter 19 state that a report abandons the device's unsent work, so a team couriers before reporting. Rejected: a destroyed report that keeps couriering open until expiry, since signatures carry no time and anyone holding the key could sign new entries as the author until then.
16. **The courier's offer is a candidates read and a call per author.** `NativeClient::courier_candidates()` reads, per author, the account, the held sequences and bytes and the last verdict, `ClientEvent::CourierCandidates` is raised when a session with the path on finds candidates, and `NativeClient::courier(author)` sends that author's held rows on the current session. The answer settles the courier's own rows as a `ProvisionalVerdicts` does. Nothing runs unless the application calls it. Rejected: a per-sequence selection, which can only shorten the range. Rejected: a builder switch couriering automatically, against step 3.
17. **A write the server acknowledged after a backup comes back from its author or from a courier.** Receivers keep adjudicated entries, bodies, offers and manifest rows, as retained rows, until the author certificate they last saw over a link expires plus `DEVICE_SKEW`, which `_connetto_peer_cursor.expires_at` records, ask about them after a full resync, which every restore method forces, and offer the `Pending` ones as candidates. The author keeps its own acknowledged entries (decisions 21 and 22) and, meeting a rewound frontier they cover, re-sends them through the direct path instead of retiring, retiring only when they leave a gap. This amends chapter 20's restore rule and R75's deletion at acknowledgement. Rejected: receivers retaining while the author still retires, under which the author's acknowledged writes come back only when a courier reaches the server first. Rejected: no retention, under which a write every receiver saw adjudicated before the restore stays lost.
18. **A courier reaching a writer its author already retired is refused `WriterRetired`.** The author's moved entries applied under its fresh writer first, so nothing applies out of order, and the courier drops that author's courier-only, digest-only and retained rows. Rejected: accepting entries below the first sequence the author carried, which land after the author's newer writes so an older value can overwrite a newer one.
19. **One table holds every courier-held row, with a state.** `_connetto_peer_entry` gains `state` (provisional, courier-only, digest-only, retained), `origin` and `digest`, and `changeset` becomes nullable for a digest-only row. R77 decision 13's re-apply, the archive import's re-apply and every other R77 reader take provisional rows alone, and no body is stored twice. Rejected: a separate courier table, which stores an applied entry's body twice until adjudication.
20. **The archive carries the courier-held rows.** Both export scopes carry courier-only, digest-only and retained rows beside the provisional ones, the import re-applies only the provisional ones and restores the others as they are, so a courier restored from its archive still couriers. The archive format's version moves. Rejected: carrying provisional rows alone.
21. **The author keeps its answered entries in a table of their own.** `_connetto_acked (writer, seq, changeset, entry_hash, prov_writer, prov_seq, prov_secret, major, refused, acked_at)` in `main`, mirroring `_connetto_pending`'s columns, holding every entry the server answered for good, applied or refused, with the refusal in `refused`, written in the transaction that deletes it from `_connetto_pending`, so `_connetto_pending` keeps holding exactly the unsent writes its readers (replay, export, the drain, reconcile, the offer) take it for. R31's rebuild keeps the table and rewrites an old major's rows into that major's wire form, as it rewrites the queue. Rejected: an acknowledged flag on `_connetto_pending`, which every present and later reader would have to filter.
22. **One configurable window covers both writer kinds.** `SyncTuning::with_acked_retention(Duration)`, defaulting to 30 days, the default of the certificate lifetime ceiling, pruned by `acked_at`. A device writer and a number writer alike re-send on a rewound frontier their retained entries cover and retire otherwise, a number re-sending only once the restored server confirms it under its secret (R70 decision 22), and a number issued after the backup still answers `WriterUnknown` and retires. Rejected: devices alone, which leaves a browser's acknowledged writes lost. Rejected: the device's certificate life, which never ends for a live device that renews at half-life.
23. **A refused entry is re-sent declaring its refusal** (decided with the maintainer during slice 5). A restored server lost the refusal record too, and the server recomputes the chain from each body, so the entry must go up as the same bytes. `MutationHeader.refused` carries a `PriorRefusal`, `Rejected(reason)` or `Conflict { table }`, and the server settles that sequence with it and applies nothing, so the restored server ends with the record it had and the client rolls nothing back a second time. A writer may always refuse its own write, so the declaration drops nobody else's work. Rejected: re-sending it plain, under which a write the user saw refused can apply on the restored server and come back through the live stream. Rejected: retaining applied entries alone, under which one refusal inside the run retires the writer.
24. **A declared conflict settles as `Conflict` with its table and no key** (decided with the maintainer during slice 5). The client's `MutationConflict` names only the table, so the server stores `Conflict { table, version_column: "", pk_columns: [], pk_values: [] }`, every verdict stays `Refused(Conflict)`, and a replay answers `MutationConflict` with that table and no server row. Rejected: a `Rejected(Other)` record, which peers would read as a rejection.
25. **An older major's retained run is re-sent on the drain wire in a slice of its own, 5b** (decided with the maintainer during slice 5). The drain dials only while a held batch with pending entries exists and never checks for a rewound frontier, so 5b adds a held batch retained rows alone open, the drain's rewind check and its re-send. Until 5b lands, a run holding an older major's entry retires the writer.

### Design

```text
the server wire, control plane unless marked
  CourierRange      { request_id, author: [u8; 32], offer: Vec<u8>, rows: Vec<ManifestRow> }
  CourierBody       { request_id, seq, changeset_zstd }                    bulk plane
  CourierAnswer     { request_id, outcome: CourierOutcome }
  CourierOutcome    = Refused(CourierRefusal) | Entries(Vec<(u64, CourierEntryOutcome)>)
  CourierRefusal    = Disabled | Malformed | NotEnrolled | AuthorBanned | WriterRetired
                    | Unverified | SequenceGap { expected, got }
  CourierEntryOutcome = Ruled(Verdict) | NotCouriered | Retry { retry_after_ms }
  CourierNotice     { notice_id, notices: Vec<CourierNoticeRow> }
  CourierNoticeRow  { courier, writer: WriterRef, seq, verdict: Verdict }
  CourierNoticeAck  { notice_id }
  HandshakeAck      gains courier: bool
```

`ManifestRow` and `Verdict` are R77's types in `connetto-core::exchange`, and `Verdict::Pending` never answers a couriered row. The client waits at most 30 s for `CourierAnswer` on a monotonic clock, R77 decision 10's bound, and holds one range per author open at a time.

```sql
-- client, in main, the two R77 tables as they become
CREATE TABLE _connetto_peer_entry (author BLOB, seq INTEGER, state INTEGER NOT NULL, major INTEGER NOT NULL, origin BLOB, digest BLOB NOT NULL, changeset BLOB, account TEXT NOT NULL, bot_list TEXT, received_at INTEGER NOT NULL, position INTEGER NOT NULL, PRIMARY KEY (author, seq));
CREATE TABLE _connetto_peer_cursor (author BLOB PRIMARY KEY, account TEXT NOT NULL, seq INTEGER NOT NULL, head BLOB NOT NULL, offer BLOB NOT NULL, expires_at INTEGER NOT NULL);
CREATE TABLE _connetto_acked (writer BLOB, seq INTEGER, major INTEGER NOT NULL, origin BLOB, changeset BLOB NOT NULL, hash BLOB NOT NULL, acked_at INTEGER NOT NULL, PRIMARY KEY (writer, seq));
-- server, deployment-owned, courier_user_id taking the deployment's user id type
ALTER TABLE connetto_device_enrolments ADD COLUMN bot_list TEXT;
ALTER TABLE auth_events ADD COLUMN courier_user_id TEXT;
ALTER TYPE connetto_auth_op ADD VALUE 'write_couriered';
-- and one notice row per couriered entry that settled, in a table of the Writers member
```

**One courier range on the server.** The checks run in this order, and the first that answers ends the range. `f` is the writer row's frontier and `h` its head, `GENESIS` and 0 for a writer the courier path creates, as `courier_deliver` creates one today.

| Situation | Answer |
|---|---|
| The courier switch is off | `Refused(Disabled)`, nothing read |
| The offer does not decode, rows exceed the bound or skip a sequence, a body names no row, arrives out of order, or disagrees with its row's digest | `Refused(Malformed)` |
| The enrolment is absent, revoked, or past its expiry plus `DEVICE_SKEW` | `Refused(NotEnrolled)` |
| The author's account is banned | `Refused(AuthorBanned)` |
| The writer is retired | `Refused(WriterRetired)` |
| The offer's signature does not verify, or the chain from `h` through rows `f + 1` to the offer's newest misses `newest.head` | `Refused(Unverified)` |
| The first row is above `f + 1` | `Refused(SequenceGap { expected: f + 1, got })` |
| A row at or below `f` | `Ruled` with #377's verdict for it, nothing applied or audited |
| A row above `f` with a body and no number origin | applied under the author's account, the enrolment row's bot list and no subjects, settled `Applied`, `Rejected` or `Conflict`, one audit row and one notice row, answered `Ruled` |
| A digest-only row or a number origin above `f` | `NotCouriered`, nothing past it settles |
| An entry settles `Indeterminate`, or the courier bucket runs dry | `Retry`, nothing past it settles |

**The courier on the client.** `courier(author)` sends the rows held for the author from the lowest one not known settled to the offer's newest, with every body it holds, and settles the answer row by row in one transaction each, `Applied` and `Settled` adjudicating as a verdict does, `Refused` retracting with its cause, `NotCouriered` and `Retry` keeping the row.

**The author's re-send.** At the handshake for a number and the bind for a device the client compares the frontier the server answers, `f`, with the writer's `settled_seq`, `s`. Below `s` it re-sends `f + 1` to `s` from `_connetto_acked` in order through the direct path when every one is retained, then replays its pending entries, and otherwise retires the writer as chapter 20 states.

### Every event in every situation

For one author as the courier sees it, **C0** no server session, or the session's `courier` flag is off, **C1** a session with the path on and nothing of this author to offer, **C2** the author is a candidate, a held provisional, courier-only or digest-only row not known settled, or after a full resync a retained row the server answers `Pending`, **C3** a `CourierRange` for this author outstanding.

| Event | C0 | C1 | C2 | C3 |
|---|---|---|---|---|
| Server session established, `courier` off | stay | enter C0 | enter C0 | n/a |
| Server session established, `courier` on | #377's asks, then C2 raising `CourierCandidates` when a row is a candidate, else C1 | n/a | n/a | n/a |
| Full resync ends | ask about retained rows too | same | same | same, after the answer |
| `ProvisionalVerdicts` answers a held row | settle it as #377 does | same, enter C2 on a `Pending` row | same, enter C1 when no candidate row is left | same |
| A verified offer over a link adds rows | nothing | enter C2 | stay | stay, the next call sends them |
| `courier(author)` | answer `CourierUnavailable` | answer that nothing is held | send the range and its bodies, enter C3 | answer `CourierBusy` |
| `Ruled(Applied)` or `Ruled(Settled)` for a row | n/a | n/a | n/a | the row becomes retained, a provisional record adjudicated |
| `Ruled(Refused(cause))` for a row | n/a | n/a | n/a | a provisional row retracted with the cause, a courier-only or digest-only row deleted |
| `NotCouriered` | n/a | n/a | n/a | the rows from it stay, enter C1 until a new offer adds rows |
| `Retry` | n/a | n/a | n/a | the rows from it stay, enter C2, the call answers the wait |
| `Refused(Disabled)` | n/a | n/a | n/a | enter C0 |
| `Refused(Malformed)` | n/a | n/a | n/a | log it, enter C2 |
| `Refused(NotEnrolled)`, `Refused(WriterRetired)` or `Refused(Unverified)` | n/a | n/a | n/a | delete the author's courier-only, digest-only and retained rows, provisional rows wait for their verdict, raise `PeerChainInvalid` on `Unverified`, enter C1 |
| `Refused(AuthorBanned)` | n/a | n/a | n/a | keep every row, enter C1 until the next session |
| `Refused(SequenceGap)` | n/a | n/a | n/a | keep every row, enter C1 until a new offer adds rows |
| 30 s without `CourierAnswer` | n/a | n/a | n/a | drop the wait, keep every row, enter C2 |
| Session lost | stay | enter C0 | enter C0 | enter C0, the unanswered rows kept |
| Retention of a retained row runs out | delete it | same | same, enter C1 when no candidate is left | same, after the answer |
| R31 rebuild at a newer major | rows kept under their major (decision 7) | links and session close first, as C0 | same | same |
| Data wipe, destructive logout or revocation of this device | every row goes with the replica | same | same | same |

For one held row of `_connetto_peer_entry`, **P** provisional, applied with its record, **O** courier-only, an own-build body the receiver refused, **D** digest-only, an entry of another build, **T** retained, adjudicated and kept for a restore.

| Event | P | O | D | T |
|---|---|---|---|---|
| Entry verified in a range (R77) | written when it applies | written when the receiver's policy refuses it | written for another build | n/a |
| Author's frontier over a link, a verdict or a courier answer `Applied` or `Settled` | record adjudicated and holding released, enter T | enter T | enter T | nothing |
| Retraction, verdict `Refused` or `Abandoned`, or courier answer `Refused` | inverted and deleted (R77) | deleted | deleted | n/a |
| Server write on the key | `ProvisionalConfirmed` (R77), the row stays P | nothing | nothing | nothing |
| `SnapshotEnd` ending a resync | re-applied (R77 decision 13) | not applied | not applied | not applied |
| Truncate of the table | record dropped, the row stays (R77 decision 13) | nothing | nothing | nothing |
| R31 rebuild at a newer major | enter O under its major | kept under its major | kept | kept |
| Author's certificate renewed over a link | nothing | nothing | nothing | the window moves to the new expiry |
| The last seen expiry plus `DEVICE_SKEW` passes | waits for the server's `Abandoned` | same | same | deleted |
| Courier refused `NotEnrolled`, `WriterRetired` or `Unverified` | waits for its verdict | deleted | deleted | deleted |
| Archive export | carried | carried | carried | carried |
| Archive import | re-applied (R77 decision 16) | restored as it is | restored as it is | restored as it is |
| Eviction pass | keeps the row through its holding | n/a, no row in a table | n/a | n/a |

For one writer of the author's own, its server frontier `f` against its `settled_seq` `s`, **A0** `f` equals `s`, **A1** `f` below `s` with `_connetto_acked` holding every sequence from `f + 1` to `s`, **A2** `f` below `s` with one of them missing, or `WriterUnknown`.

| Event | A0 | A1 | A2 |
|---|---|---|---|
| Handshake grant or device bind answers `f` | replay pending entries from `s + 1` | re-send `f + 1` to `s` from `_connetto_acked` in order, then replay pending entries | retire the writer and move pending entries to a fresh one (chapter 20) |
| The server acknowledges an entry | move it from `_connetto_pending` to `_connetto_acked` in one transaction | same | n/a |
| A re-sent entry answered applied or from the record | n/a | keep its acked row, go on | n/a |
| A re-sent entry answered rejected or conflicted | n/a | keep the refusal on its acked row, so a later restore declares it, and write `_connetto_retraction` for a device writer, no rollback, `ClientEvent::WriteRestored` | n/a |
| A re-sent entry answered `Forked` | n/a | stop, enter A2 | n/a |
| An acked row passes the retention window | deleted | deleted, which can turn a later restore into A2 | deleted |
| Writer retired or revoked | its acked rows deleted | same | same |
| R31 rebuild at a newer major | acked rows kept under their major in its wire form, a run holding one re-sent on the drain wire from slice 5b and retiring until then | same | same |
| Archive export | not carried, the import retires the archived writer | same | same |

### Slices

One pull request each, in this order, each green and useful on its own (decided with the maintainer).

- **1a, the server's courier rules** (decisions 2 to 4, 6, 8, 9, 13 to 15). `courier_deliver` checks the `SignedOffer` with digest rows to the signed head, skips the settled prefix with #377's verdicts, stops at a digest-only row or a number origin, binds the enrolment row's bot list and refuses a banned author, a key past expiry plus `DEVICE_SKEW` and a revoked key, `range_message` and `verify_range` go, and R77 decision 1 is corrected. Proven Docker-gated by calling `courier_deliver` as `courier_frontier.rs` does, for a forged offer, a gap, a revoked key, a banned author, a bot author's list and the author's own later replay answered once. Waits for #377.
- **1b, the courier wire** (decisions 1 and 5). `CourierRange`, `CourierBody` and `CourierAnswer`, the switch and `CONNETTO_COURIER`, and `HandshakeAck.courier`. Proven Docker-gated over a socket with the switch on and off.
- **2, audit, meter and notice** (decisions 10 to 12). `write_couriered` with `courier_user_id`, the courier bucket, the notice table, `CourierNotice` and its ack. Proven Docker-gated, every settled couriered entry audited naming both accounts, a range past the budget answered `Retry`, and the author's next session receiving its notices once.
- **3a, the courier hold** (decisions 3, 17 and 19). The state column, manifest rows, courier-only bodies, retained rows with `expires_at`, and the re-apply and every R77 reader taking provisional rows alone. Proven in process for each row of the held-row table. Waits for #377.
- **3b, the rebuild and the archive** (decisions 7 and 20). R31's rebuild keeping the rows under their major, and the archive carrying them with the format version moved. Proven in process.
- **4, the offer** (decisions 7 and 16). `courier_candidates`, `CourierCandidates`, `courier(author)`, the answer settling the courier's rows, and older-major rows on the drain wire. Proven Docker-gated in `connetto-test-harness`, a device's writes reaching the server through a different account's session. Waits for #377.
- **5, the author's re-send** (decisions 17 and 21 to 24). `_connetto_acked`, `SyncTuning::with_acked_retention`, `MutationHeader.refused` and its server settlement, and the re-send on a rewound frontier for a device and a number writer on the main wire. Proven Docker-gated by `a_restore_takes_a_retained_run_back_and_moves_unknown_numbers`, a same-cluster dump restore, and `a_declared_refusal_settles_without_applying_and_redelivers_from_the_record`.
- **5b, the older major's re-send** (decision 25). A held batch retained rows alone open, the drain's rewind check and the re-send of an older major's run on the drain wire. Proven in process against the drain's fake old-major server.
- **6, the harness proof** (decisions 17 and 18). The Done when end to end, a courier's re-delivery to a restored server and a courier refused after the author retired, with chapters 19 and 20 and the diagram brought to what is built. Waits for every slice before it.

### Done when

A device's writes reach the server through a different account's session with exactly-once held against the author's own later replay, a malicious courier can neither forge nor reorder (gap refusal proven), a revoked enrolment is refused, every couriered application is audited, and a restored server gets back the writes it acknowledged after its backup from their author or from a courier, all Docker-gated.

---

## R79: media over the peer link

**Status.** IN PROGRESS. Steps 1 to 3 are sliced into eight pull requests below, of which slices 1 to 7 are merged, and step 4 waits for R78. Decisions 1 to 10 were taken with the maintainer, and decision 11 was settled on evidence.

**Blocked on** R77, and on R64 to R67 for the chunk machinery, all merged for steps 1 to 3. R77's open slices 7 and 8 block none of them. Step 4 is blocked on R78.

**Changes state.** Client tables, `_connetto_content_outboard` in `main` beside the manifests (decision 4). Server tables, none. Wire messages, the media frames at peer protocol version 3 (decision 1). Archive format, none, since an outboard is derived and peer-fetched content is cache. Demo deployment, `photos.thumb_id` (decision 9).

### Purpose

Sample photos in camp. This is the join point with chapter 18's identity, chunking and manifest, metadata rows travel in the exchange like any rows, so this phase adds content, pull-on-demand by chunk over the established link, a holding device answering after evaluating the requester against its translated policy fail-closed, the receiver verifying against the file id the signed row carries, re-encrypting under its own key, and storing in the never-exported cache class. The application can additionally mark content for replication to present peers, the survivability half of couriering.

### Steps

1. The chunk-request frames on the peer link and the holder-side policy gate.
2. Receiver verification against the file id the signed row carries, re-encryption, cache-class storage, orphan sweep after retraction. **Peer-fetched content has its own spare clause**, `tidy_content` keeps a manifest only while unsent or pinned and R60 made the residual pass automatic, so without this a photo pulled in camp would be deleted at the next crossing, offline and unrefetchable. Peer-fetched chunks are spared while their row is provisional, mirroring the spare clause provisional rows themselves have, and become ordinary cache when the row is adjudicated, at which point the server can serve them again.
3. Thumbnail prefetch (ordinary derived files behind a thumbnail column, instant at their size), and the peer fetch as a stage of the resolver (decision 10).
4. Opt-in replication, the application marks content for push to present peers, the survivability half of R78. Not designed, waiting for R78.

### Decisions, taken with the maintainer

1. **Chunks cross as per-chunk pulls on a media queue of their own, at peer protocol version 3.** `PeerFrame::Media` carries the messages below on a second bounded queue per link, eight frames deep like the exchange's, so a 16 MiB chunk never waits ahead of an offer or a range and an offer never stalls a photo. `PROTOCOL_VERSION` becomes 3 in the hello, the TXT record and the beacon together, because a version 2 end decodes an unknown variant as malformed and closes the link with `CloseReason::Protocol`. A receiver asks for a file's manifest with `Want` and then for each chunk it lacks with `Fetch`, which names the chunk by its byte range of the file, since two holders can chunk one file differently (the author under its mime class, a server-fetched copy under the generic class) while a byte range of `F` is the same on every holder. A holder answers `Manifest` frames of at most 8192 rows, then the range as `Piece` frames of at most 1 MiB of file bytes with their proof hashes (decision 4), and answers `Refused` alike for a file it lacks and a requester its gate refuses, the rule the file server keeps by answering 404 for both (R38). Each side keeps at most one request outstanding per peer, a holder serves at most two peers at once and answers `Busy` beyond that, since each serve holds one decrypted chunk of up to 16 MiB, every reply quotes the request id, and a receiver waits at most 30 s for the next frame on a monotonic clock (R77 decision 10). A lost link resumes at the next chunk the receiver lacks, since every verified chunk is already in its store. Rejected: one request per file, which restarts the whole file on a lost link. Rejected: sharing the exchange queue, which puts a 16 MiB chunk ahead of the next offer.
2. **The serving device judges the requester through declared content columns, failing closed.** The application declares each column holding a file id on its content client, `with_peer_content("photos", "content_id")`, the same declaration in every build of the application. A holder asked for `F` binds the requester's certificate `account`, `NULL` subjects and the certificate's bot list on its own connection for one statement, the `AuthorBinding` mechanism R77 decision 4 built for an author, and runs `SELECT 1 FROM photos WHERE content_id = ? LIMIT 1` through the policy view for each declared column, serving when any answers a row. It refuses when the application declared no column, when the build has no caller mapping and so no view, and on any error, and a certificate with no bot list binds `NULL`, which the views refuse. The gate runs again at every `Fetch`, so a right the holder's replica withdraws ends the transfer at the next chunk. The answer comes from the holder's partial replica, with chapter 19's false-deny and bounded false-allow limits. The same declaration names the rows a receiver routes by (decision 3), the rows the spare clause reads (decision 6) and the thumbnail columns (decision 8). Rejected: query-shaped declarations like pins, which cannot name a row for the routing and the spare clause.
3. **Any linked device holding the file answers, the row's provisional author first.** A receiver resolving `F` asks, in order, the linked authors of the provisional rows whose declared columns name `F`, then every other linked peer in the order its link came up, moving to the next on `Refused`, `Busy`, a lapse or a failed verification. Each holder runs the gate on its own replica, so a teammate who synced the row and its bytes before camp serves them while the author is out of range. Rejected: the author alone, under which a row synced from the server has nobody to ask in camp.
4. **Every piece is verified against the file id by BLAKE3 verified streaming.** `FileId` is the plain BLAKE3 of the file, the root of BLAKE3's own hash tree, so [`bao-tree`](https://github.com/n0-computer/bao-tree) streams any byte range of `F` with the sibling hashes that prove it, with no format change, the upgrade chapter 18 names. [BitTorrent v2](https://www.bittorrent.org/beps/bep_0052.html) checks each 16 KiB block against a file's Merkle root the same way, and [iroh-blobs](https://www.iroh.computer/blog/blake3-hazmat-api) streams BLAKE3 content with this crate. Chunk groups are 16 KiB, so the outboard, the tree's parent hashes, costs 64 bytes per 16 KiB, about 0.4% of the file. A device keeps each file's outboard as an encrypted object in its chunk store, named by `_connetto_content_outboard (file_id BLOB PRIMARY KEY, hash BLOB NOT NULL)`, computed in the native staging and fetching pass that already hashes the file for its identity, and on first serve for a file staged in a browser or before this phase. An outboard leaves with its manifest in the same transaction, and the orphan sweep keeps every outboard the table names. A receiver checks every piece against `F` as it arrives and each completed chunk against the hash its manifest row names, so a forged piece fails within 16 KiB and a lying manifest at its first chunk, and it keeps the outboard its decoding builds, so it can serve the file in turn. Rejected: chunk hashes with the identity checked at commit, under which a lying holder costs up to the whole file's transfer and ranged reads stay unverifiable.
5. **The receiver keeps the holder's chunk boundaries and re-encrypts each chunk under its own key.** A verified chunk is written under its own hash through the receiver's `EncryptingStore` of the generic class, with the receiver's root key, a fresh nonce and the chunk hash as associated data, holding one chunk in memory, and a resumed fetch skips every chunk the store already holds. The manifest rows and the outboard row commit in one transaction once every chunk is in, with no outbox row, which is what makes content cache, never exported and evictable (chapter 18). Rejected: re-chunking the whole file as `fetch_one` does, which holds the file in memory and cannot resume.
6. **Peer-fetched content is spared while a provisional row names it** (the rule in step 2). `tidy_content` keeps a manifest while it is unsent, pinned, or named by a declared column of a row whose `_connetto_provisional` record is still provisional. Adjudication or a server write on the row makes the file ordinary cache, which the next pass removes and the server serves once its author has uploaded it, so a receiver online before that upload shows `Unavailable` until it lands. A file fetched for a row that was never provisional, from a teammate's synced copy, is ordinary cache from the moment it lands. A provisional delete names no file, since its row is gone. Rejected: sparing an adjudicated row's file until a read ticket succeeds, which needs the `content_state` column declared and a ticket round trip per kept file.
7. **A retraction runs the content retention pass.** The content client runs its retention pass on every `ClientEvent::ProvisionalRetracted` and once when its driver starts, since the event broadcast can drop events, so the bytes of a write the server or its author refused leave the receiver at once. The pass is the whole retention pass, so it also removes ordinary cache, which a linked holder can serve again. Rejected: waiting for the next upload or a manual tidy, which keeps a refused write's bytes readable on disk until then.
8. **Thumbnails are prefetched through a flag on the declaration.** `with_peer_content("photos", "thumb_id").prefetch()` marks a thumbnail column. A receiver fetches each prefetch-column file of an author's provisional rows that it lacks from that author, when a peer apply raises `ProvisionalApplied` and when the author's link comes up, the second reading the records since the broadcast can drop events. A prefetch is dropped when its manifest totals more than 1 MiB, so a misdeclared column never pulls a video unasked, and the file stays fetchable on demand. An on-demand fetch to a peer goes before every prefetch queued for it. The application generates a thumbnail and stages it as an ordinary file (chapter 18). Rejected: pins fetching from peers, which keeps every thumbnail until unpinned and gives pins a second source. Rejected: no prefetch, under which the first view of every photo waits a round trip.
9. **The demo deployment's `photos` gains `thumb_id`.** `schema.sql` adds `thumb_id BYTEA`, `content.sql`'s `connetto_visible_files` admits a file either column names, and R80's demos inherit the column. The Done when's proof is a Docker-gated test in `connetto-test-harness` with two native clients in one process on the loopback peer link, as R77 slice 6d's is. Rejected: a fixture table in the test alone, which leaves R80 to add the column.
10. **The peer fetch is a stage of `resolve` after the local answer** (amending R67 decision 8). `resolve` answers locally for any manifest whose chunks are all held, whether unsent, pinned or cached. Online it then asks the server for a signed URL and turns to a linked holder when the server refuses the ticket, which is the case of a provisional row whose author has not uploaded yet. Offline it turns to a linked holder directly. The fetched chunks land in the store and `ChunkStoreSource` answers from them, so the `LocalContentSource` list keeps local sources alone. R67 decision 8 expected the peer to register in that list, but the list is asked only for a held manifest, and its `bytes` takes a `Manifest` a receiver does not have. Rejected: a `LocalContentSource` taking a `FileId`, whose bytes would come back uncached and need a second store write path for the spare clause.

### Decision, settled on evidence

11. **The pull lives in `connetto-file-client` behind a `peer` feature, over a media port `connetto-client` opens.** `connetto-peer` cannot host it, since `connetto-file-client` depends on `connetto-client`, which depends on `connetto-peer` under its own `peer` feature. The messages and a sans-IO machine per peer, with the request ids, the one outstanding request each way, the 30 s bound and the candidate order, live in `connetto-exchange::media` beside the exchange's machine (R77 decision 21). The outboard and the verified range codec live in `connetto-file-core`, which every platform tests under wasm, and the frame and its queue in `connetto-peer`. `connetto-client` gives the file client a port that lists the linked peers with their certificates' accounts and bot lists, sends and receives media messages, runs the gate statement under the requester's binding, and reads the values of a declared column on provisional rows with their authors. The enrolment task, which already receives the link's events, forwards `PeerEvent::Media` to that port.

### Design

```rust
// connetto-exchange::media::Media, carried as PeerFrame::Media on its own queue
Want { request_id: u64, file_id: [u8; 32] }
Manifest { request_id: u64, chunks: Vec<([u8; 32], u64)>, last: bool }
Fetch { request_id: u64, file_id: [u8; 32], start: u64, len: u64 }
Piece { request_id: u64, encoded: Vec<u8>, last: bool }
Refused { request_id: u64 }
Busy { request_id: u64 }
```

```sql
CREATE TABLE _connetto_content_outboard (file_id BLOB PRIMARY KEY, hash BLOB NOT NULL);
```

`Fetch` asks for at most 16 MiB, the largest chunk. `Piece.encoded` is the bao encoding of the next part of the range, at most 1 MiB of file bytes with the parent hashes that prove it, whole 16 KiB groups, so a chunk boundary inside a group costs the edge bytes of that group, verified and discarded.

### Every event in every situation

For one peer as the receiver sees it: **Q0** not linked, **Q1** linked with no media request outstanding, **Q2** a `Want` outstanding, **Q3** a `Fetch` outstanding. As the holder sees it: **H0** not linked, **H1** linked and serving this peer nothing, **H2** serving this peer. A fetch moves to the next candidate (decision 3) wherever a row says so, and when no candidate is left, an on-demand fetch answers `Unavailable` and a prefetch is dropped until the next trigger.

| Event | Q0 | Q1 | Q2 | Q3 | H0 | H1 | H2 |
|---|---|---|---|---|---|---|---|
| Link up | enter Q1, queue the prefetches of this author's provisional rows, resume fetches waiting for a candidate | n/a | n/a | n/a | enter H1 | n/a | n/a |
| Link lost | nothing | enter Q0 | abort, next candidate, enter Q0 | abort, the chunk's verified pieces dropped, earlier chunks kept, next candidate, enter Q0 | nothing | enter H0 | stop serving, enter H0 |
| `resolve` with every chunk of a manifest held | answer locally, no frame | same | same | same | n/a | n/a | n/a |
| `resolve` with no local bytes, offline or with the ticket refused | next candidate | send `Want`, enter Q2 | queue it ahead of every prefetch | same | n/a | n/a | n/a |
| Prefetch due, `ProvisionalApplied` naming a prefetch-column file this device lacks | nothing, link up queues it | send `Want`, enter Q2 | queue it behind every on-demand fetch | same | n/a | n/a | n/a |
| `Manifest`, not last | n/a | ignore it | keep the rows, wait for the next frame | ignore it | n/a | n/a | n/a |
| `Manifest`, last | n/a | ignore it | a prefetch above 1 MiB is dropped, else `Fetch` the first chunk not held (Q3), or commit when every chunk is held (Q1) | ignore it | n/a | n/a | n/a |
| `Manifest` above 8192 rows, or rows after the last | n/a | n/a | abort, next candidate | n/a | n/a | n/a | n/a |
| `Piece` verifies against `F` | n/a | ignore it | ignore it | keep it, and at the last piece check the chunk against its manifest hash, write it, then `Fetch` the next chunk not held, or commit the manifest and outboard rows in one transaction, raise `ContentEvent::Fetched` and enter Q1 | n/a | n/a | n/a |
| `Piece` fails verification, or a chunk disagrees with its manifest hash | n/a | n/a | n/a | abort, drop the chunk's pieces and this holder's manifest, next candidate from `Want` | n/a | n/a | n/a |
| `Refused` | n/a | ignore it | next candidate | next candidate | n/a | n/a | n/a |
| `Busy` | n/a | ignore it | next candidate, and once every candidate answered `Busy`, the first again after 5 s doubling to 5 min (R77 decision 22), an on-demand fetch answering `Unavailable` 30 s after it began or after its last verified piece | same | n/a | n/a | n/a |
| A reply quoting another request id | n/a | ignore it | ignore it | ignore it | n/a | n/a | n/a |
| 30 s without the next frame | n/a | n/a | abort, next candidate, enter Q1 | same | n/a | n/a | n/a |
| A chunk write or the commit fails | n/a | n/a | n/a | abort, the fetch fails with the store's or the replica's error, written chunks left to the orphan sweep, enter Q1 | n/a | n/a | n/a |
| `Want` received | n/a | n/a | n/a | n/a | n/a | gate and manifest held, stream `Manifest` frames and enter H2, else `Refused` | `Busy` |
| `Fetch` received | n/a | n/a | n/a | n/a | n/a | gate again, range within the file and its chunks held, outboard computed when absent, stream `Piece` frames and enter H2, else `Refused`, and `Busy` while two other peers are served | `Busy` |
| Own stream ends | n/a | n/a | n/a | n/a | n/a | n/a | enter H1 |
| Own file evicted or a chunk unreadable mid-stream | n/a | n/a | n/a | n/a | n/a | n/a | stop, `Refused`, enter H1 |
| Peer certificate renewed | nothing | nothing | nothing | nothing | n/a | the next gate binds the renewed bot list | same |
| Peer revoked or expired, link closes | as link lost | as link lost | as link lost | as link lost | as link lost | as link lost | as link lost |
| R31 rebuild at a newer major | n/a | links close first, as link lost | same | same | n/a | same | same |
| Data wipe or destructive logout | n/a | links close first, the store goes with the replica | same | same | n/a | same | same |

A held peer-fetched manifest is **K1** spared, a declared column of a provisional row naming it, **K2** ordinary cache, or **K3** kept by another clause, unsent or pinned.

| Event | K1 | K2 | K3 |
|---|---|---|---|
| Peer fetch commits | enters K1 when a provisional row names it | else enters K2 | n/a |
| Retention pass, after an upload, on `tidy_content`, on `ProvisionalRetracted` or at driver start | kept | manifest and outboard rows deleted, then their unreferenced chunks swept | kept |
| `ProvisionalRetracted` for the naming row | the pass runs, the file moves to K2 and goes unless another row names it | the pass runs | the pass runs |
| Frontier, verdict or server write adjudicates the row | moves to K2, the next pass removes it | nothing | nothing |
| A pin covers it | moves to K3 | moves to K3 | nothing |
| Its pin goes | n/a | n/a | back to K1 or K2 |
| Export | not carried | not carried | unsent carried with its chunks, pinned not |
| Truncate of the naming table or R31's rebuild dropping the records | moves to K2 | nothing | nothing |

### Slices

One pull request each, in this order, each green and useful on its own.

1. **Outboards** (decision 4). `connetto-file-core` gains the outboard and the verified range codec over a `ChunkStore` and a manifest, and `connetto-file-client` the `_connetto_content_outboard` table, the outboard written in the native staging and fetching pass and computed on demand, and its eviction and sweep with its manifest. Proven in process, every range of a held file decoding against its id and a forged byte refused.
2. **The media frames** (decisions 1 and 11). `connetto-exchange::media` with the messages, their bounds and the per-peer machine tested row by row without sockets, and `connetto-peer` at protocol version 3 with `PeerFrame::Media`, its own queue, `Node::send_media` and `PeerEvent::Media`, proven between two nodes on loopback.
3. **The media port and the gate** (decisions 2 and 11). `connetto-client`'s port, the enrolment task forwarding media events to it, the gate statement under the requester's binding, and the provisional values of a declared column with their authors, proven in process for a permitted requester, a refused one, a certificate with no bot list and a build with no caller mapping.
4. **The holder** (decisions 1, 2 and 4). `connetto-file-client`'s `peer` feature, the declarations, and the serving of `Want` and `Fetch` with `Refused` and `Busy`, proven between a client and a bare peer node on loopback.
5. **The receiver** (decisions 3, 5 and 10). The fetch stage of `resolve`, the candidate order, verification, storage and the commit, proven between two clients on loopback, a forged piece and a lying manifest refused and a fetch resumed after a lost link.
6. **The retention** (decisions 6 and 7). The spare clause in `tidy_content` and the retention pass on `ProvisionalRetracted` and at driver start, proven in process for a provisional, an adjudicated and a retracted row's file.
7. **Thumbnails** (decisions 8 and 9). The prefetch flag, its 1 MiB bound and its order behind on-demand fetches, and the demo deployment's `thumb_id`, proven between two clients on loopback.
8. **The harness proof** (decision 9). The Docker-gated test of the Done when's first three clauses, and chapters 18 and 19 brought to what is built.

### Done when

A photo authored offline on one device displays on a peer in camp (thumbnail immediately, full image on tap), a policy-refused requester gets nothing, a retraction sweeps the cached chunks, proven in the harness. With step 4, replicated content survives the author device's loss end to end with R78.

---

---

## R80: peer sync in every demo

**Status.** NOT STARTED.

**Blocked on** R77, R78 and R79, plus R76's slice 9d, which R76 parked until decision 40's machine is at hand.

**Changes state.** Client tables, none, the receiver's file table recording the completed media by its content hash. Server tables, none. Wire messages, the completed media entry on the peer link (step 3). Archive format, none.

### Steps

1. Every native-capable demo carries the camp flow per R54's rule, on every native platform in the set (Android, iOS, iPadOS, macOS, Windows, Linux), meaning hosting or joining a hotspot, discovery, exchange, provisional display with provenance, retraction display and the courier offer.
2. The iOS hotspot zero-coverage field test, the one physical verification the design left open, the zero-coverage start of the iOS Personal Hotspot host stays unverified until the field test, recorded with its result.
3. The on-demand phone run grows to the full camp flow, and the recorded home run across every local machine (R76 decision 4) exercises it before the phase closes.
4. The gate section and CI gain whatever new legs the peer crates added.

### Done when

The camp flow runs through a demo on every local machine in the recorded home run and in the on-demand run on two attached phones, and the field test's answer is recorded in chapter 19.

---

## R107: schema changes under running servers

**Status.** IN PROGRESS. Slice 1, the procedure and the sunset (steps 1, 2 and 7), is in work. Twelve decisions below.

**Blocked on one upstream change, for slice 2.** subql drops pgoutput `Relation` messages in `PgOutputOrder::apply`, and `CdcSource` yields only events and commits, so a consumer cannot see a table's definition change. Slice 2 needs subql to pass them on.

**Changes state.** The handshake acknowledgement gains the major's sunset date, a client event carries it, and a refusal reason names a table refused for a schema change. No stored format changes.

### Purpose

R31 makes an application schema change a major version, and R6 decision 4 holds that a deployment cannot arrive at a changed table by accident. R31 also keeps the old major's server running against the deployment's Postgres while its clients drain, so that server meets the migration of the database it reads. Nothing in a running server learns that a table changed. Every schema consumer derives from the DDL strings the server booted with, and the boot preflight checks names and presence but not columns, types or policies. A change made under a running server is answered by whatever failure it meets first. An added column refuses every row subscriber of its table, a dropped column or table leaves stale values on every client with no notice, a keyed re-read ends the whole change stream, and a narrowed policy keeps delivering rows through the boot-time OpenFGA model until restart. R107 makes a schema change a handled scenario, by a deployment procedure for majors and a server that notices what the procedure does not allow.

### Decisions, taken with the maintainer

1. **A major change is a soft switch.** Old-version clients keep working against the old major's server within reason until that server is turned off, and from then on only the new major remains and clients must update. The old server is not owed a schema it can fully serve once the database is migrated.
2. **Expand and contract, with a sunset.** A major change ships as two migrations. The expand runs while the old major's server is on and only adds. The contract (drops, renames, type changes) runs only after the old major's servers are off. The pattern follows Zero's ordered deploys and the deprecation and sunset headers of web APIs (RFC 8594, RFC 9745).
3. **The expand may add only what old clients and servers can ignore.** That is a new table, index or constraint the old major never reads, and a new column that is nullable or carries a default, so old clients' inserts still succeed. The expand also relaxes an old `NOT NULL` column without a default, so the new major's inserts succeed. Every other change to a table an old major serves waits for the contract.
4. **Policies on a table an old major serves change only at the contract.** An old server's authorization therefore never drifts from the database. A new table's policies belong to the expand.
5. **The old major announces its sunset.** The old major's server is configured with a sunset date and sends it on every handshake acknowledgement. The client raises an event carrying the date so the application can prompt an update, and after the date the server refuses the connection with update-required. The configured sunset is an instant in RFC 3339 UTC, compared against the deployment's disciplined clock (R72), so the instant the operator wrote is the one the deployment enforces with no skew, and at the instant the server closes every live connection with the same update-required reason, so a client that was live learns the sunset the same way as one that returns.
6. **A running server notices a change and refuses the table.** subql passes pgoutput `Relation` messages on, and connetto compares each with its catalog. The boot preflight compares the DDL strings with the live columns, types and policies of every served table. A difference the expand allows is ignored. Any other difference refuses every subscription on that table and refuses new ones, while the other tables keep flowing. pgoutput sends a table's `Relation` message before its first changed row after a definition change, so the table is refused before any re-read of it runs.
7. **The new major's server is built from the contracted schema.** It ignores the old major's leftover columns during the window, and the contract removes only columns it never knew, so it needs no rebuild and its clients see no second schema change.
8. **Each major has its own oplog, slot and publication.** No server replays records folded under another major's schema. The old major's log, slot and publication are dropped when it is turned off, by R32's drop procedure.
9. **The application keeps a renamed or retyped column in step with its successor.** The expand migration adds a trigger copying between the old and the new column, and the contract removes it, the way Zero does. connetto applies no server DDL (R2, R13, R37), and chapter 20 documents the pattern.
10. **A replica identity that stops being `FULL` refuses that table and every table whose policy reads it.** Without the previous image neither the table's withdrawals nor the grant moves it feeds can be computed, so delivering any of them risks a leak. The other tables keep flowing, and the boot preflight refuses the same tables instead of refusing to start. This replaces R6 decision 4's exit.
11. **A table refused for a schema change says so.** Its subscriptions receive a dedicated fixed refusal detail, so the application knows an update is the remedy, and writes to it answer `MutationRejectReason::SchemaMismatch`. The detail reveals only that the table's schema moved, never any data.
12. **The boot check compares two models through sql-traits.** `ParserDB` reads the DDL strings and `pg_diesel`'s `PgDieselDatabase` reads the live catalog, and both implement `DatabaseLike`, so tables, columns, types, nullability, defaults and each policy's command, roles and permissiveness compare through the same traits. Postgres stores a policy expression in its own deparsed form, which adds casts to literals and prints `IN (…)` as `= ANY (ARRAY[…])`, so the source's `USING` and `WITH CHECK` expressions are printed by Postgres too, on temporary copies of the tables in a transaction that is rolled back, and both printouts are parsed and compared exactly. Measured on Postgres 16, 100 policies print in about 5 ms.

### Slices

1. **The procedure and the sunset.** Steps 1, 2 and 7. Blocked on nothing.
2. **Noticing a change.** Steps 3 to 6. Blocked on the subql change above.

### Steps

1. The server reads a sunset date from its configuration and sends it on the handshake acknowledgement. After the date it refuses the handshake with a typed reason the client reports as update-required, the way R31 reports a decommissioned major. The client raises an event carrying the date.
2. Each major's server names its own oplog table, slot and publication (`CONNETTO_OPLOG_TABLE`, `CONNETTO_SLOT`, `CONNETTO_PUBLICATION`), and chapter 20 states one set per major and their drop at retirement.
3. The row fold skips event columns the catalog does not know, snapshots and re-reads name the catalog's columns instead of `SELECT *`, and the write path names only the catalog's columns.
4. subql passes `Relation` messages to consumers as a source item carrying the table and its columns.
5. On a `Relation` message whose columns differ from the catalog, the server reads that table's live definition through `pg_diesel` and applies decision 3. A change the expand does not allow refuses every subscription on the table and every new one.
6. The boot preflight builds `pg_diesel`'s live model of every served table and compares it with `ParserDB`'s per decision 12, applying decision 3 to columns and refusing a table whose policies differ, together with every table whose policy reads it.
7. Chapters 06 and 20 describe the procedure in order. Run the expand, start the new major's server, set the old major's sunset, turn the old major's servers off and drop their stream artifacts, then run the contract.

### Proof

Docker-gated, against two servers of consecutive majors on one database. The expand (a new table and a nullable column) leaves every old-major subscription flowing and old clients' writes landing. The sunset date reaches an old client as an event, and after it the old server refuses with update-required. A column dropped under the old server refuses that table's subscriptions while another table keeps flowing, live and again at the next boot. The contract under the new major's server changes nothing for its clients.

### Done when

A deployment can move between majors by the documented procedure with old clients working through the window, an old client learns its sunset before its server goes away, and a change the procedure does not allow refuses only the tables it touched, whether it arrives while the server runs or before it boots.

---

## R21: one page codec on both backends

**Status.** BLOCKED, designed with the maintainer, ten decisions below.

**Blocked on** where `sqlcipher-src` and `sqlite3mc-src` live and who maintains them, to be decided in [rusqlite discussion #1908](https://github.com/rusqlite/rusqlite/discussions/1908). The SQLCipher half of the route is agreed and merged on both sides, `libsqlite3-sys` bundling `sqlcipher-src` and `sqlite-wasm-rs` carrying a `sqlcipher` feature, both pinning the same `sqlcipher-src` release, and the optional `bundled-sqlite3mc` feature for `rusqlite` is ready on a branch. The route keeps the two source crates rather than one, because `wasm32-unknown-unknown` has no libc and a second source crate must move in step with the first. The source crates have no home a published `connetto-client` can depend on, since a published crate cannot reach a `[patch.crates-io]` fork, so the phase does not start until they do.

The change is the on-disk format of the replica and the tier, the SQLCipher and SQLite3MC split giving way to one SQLite3MC codec on both backends. Server tables and wire messages are untouched, and the archive holds unencrypted rows, so decision 3's tier recovery rides it unchanged.

### Purpose

The saved database is encrypted by **two different libraries**, SQLCipher natively, vendored by `libsqlite3-sys` under `bundled-sqlcipher`, and SQLite3 Multiple Ciphers in the browser, vendored by `sqlite-wasm-rs` under `sqlite3mc`. They produce a compatible file only because `crates/connetto-client/src/cipher.rs` pins `PRAGMA cipher = 'sqlcipher'` and `PRAGMA legacy = 4` on the browser side.

**Correctness therefore rests on a setting nothing obliges a future version bump to preserve**, and the failure mode is bad, if the two drift a file written on one device stops opening on another, and it surfaces on a user's device rather than in a test. Moving native to the browser's library removes the split, so the pin stops being load-bearing. The native side moves to SQLite3 Multiple Ciphers too, so both backends run one codec on one SQLite version, and the alternative, staying on `bundled-sqlcipher`, does not remove the split. See `docs/architecture/14-at-rest-encryption.md`.

### Decisions

1. **The page format is ChaCha20-Poly1305**, SQLite3MC's `chacha20` scheme, which takes the raw 32-byte key through `sqlite3mcExtractRawKey` exactly as its `sqlcipher` scheme does. The browser build has no AES instructions (`AES_HARDWARE_NONE` on wasm), ChaCha20 runs in constant time in software, Poly1305 authenticates in the same pass where SQLCipher v4 adds an HMAC-SHA512 pass, a page reserves 32 bytes instead of 80, and the chunk store already uses XChaCha20-Poly1305. Rejected, SQLCipher v4 on both backends, which survives only as the format of step 0's fallback, since native SQLCipher reads nothing else. Step 0 measures browser page throughput under both schemes, and a result where ChaCha20 is not the faster reopens this decision.
2. **The scheme is declared, never left to a default.** One declaration naming `chacha20` is applied on both backends. With one library at one version the two sides cannot drift apart, and a declaration survives a later SQLite3MC release changing its default where a bare default would not. This replaces the earlier aim of deleting the pin outright.
3. **Files written before R21 stop opening, as a named and recorded break.** No deployment exists. A pre-R21 replica reports `ReplicaUndecryptable` and takes the recovery chapter 14 describes, then re-syncs. A device-local tier is lost unless it is exported before the update and imported after, which works because the R26 archive holds unencrypted rows. No migration code ships. The SQLCipher v4 construction holds until R21 lands.
4. **Native SQLite3MC comes from an upstream `libsqlite3-sys` feature**, a bundled SQLite3MC feature requested from rusqlite through the upstream procedure and carried meanwhile on a fork through `[patch.crates-io]`. Accepted costs, the crate grows toward crates.io's 10 MB cap (0.38.2 publishes at 5.36 MB with two amalgamations), a feature released in 0.39 needs the diesel fork's `<0.39` bound moved, and a published `connetto-client` waits for the release because a patch does not reach downstream builds. Native also stops linking the system `libcrypto`. Rejected, linking a prebuilt library through `SQLITE3_LIB_DIR`, which every application would have to build per target. **Corrected against the tracker.** The tracker was not empty. `rusqlite/rusqlite#1726` asks for exactly this feature, and `gwenn`, who has merged every `rusqlite` pull request since, declined it because `rusqlite` cannot bundle all the SQLite forks, and restated that on the SQLCipher request after wasm support merged. The in-tree shape this decision chose is the one already declined. Step 0's fork passed every check, and its `libsqlite3-sys` packs to 8,641,479 bytes against the 10,485,760-byte cap. **Route amended with the maintainer.** A new source-only crate, started by us and offered to whichever owner the upstream maintainers prefer, carries the untouched SQLite3MC amalgamation. `sqlite-wasm-rs` and `libsqlite3-sys` both take it as a build dependency, so one `Cargo.lock` resolves one SQLite3MC version for both backends. That mirrors how `rusqlite` already runs SQLite3MC in the browser without carrying its source, and no SQLite3MC source crate existed on crates.io when the route was chosen. Rejected the same design session, a source-directory setting in `libsqlite3-sys` which every application would have to configure, and a build-time download as Worldcoin's `walletkit-sqlite` does, which breaks offline builds.
5. **Both backends compile the same SQLite options, asserted by a test**, apart from the options the platform forces (threading, the OS layer, temp storage).
6. **Parity comes from upstream alignment, with runtime parity meanwhile.** The `libsqlite3-sys` request and a second one to `sqlite-wasm-rs` ask both builds for the set of decision 7. Until both release, connetto sets the runtime-settable options identically on both backends (page size at creation per decision 9, and cache size), and chapter 13 lists the remaining compile-time differences for application authors. Foreign-key enforcement is R92's, per decision 8. Rejected, runtime parity with a permanent residue, which leaves `pow()` failing natively and `soundex()` failing in the browser. **Delivery amended with the maintainer.** The SQLite3MC feature compiles `bundled`'s own option set, and each option of decision 7 that `bundled` lacks is requested as its own small `libsqlite3-sys` feature, following `unlock_notify`, `column_metadata`, `preupdate_hook` and `session`. A profile baked into the SQLite3MC feature was rejected because it removes `rusqlite` API, and compiler flags set per application were rejected because a published `connetto-client` cannot impose them. The three omissions of decision 7 (extension loading, deprecated APIs, shared cache) are still open.
7. **The agreed set is the union of today's two builds, with loadable extensions omitted.** The math functions, FTS3, FTS5, SOUNDEX, STAT4, RTREE, the session extension and the introspection virtual tables on both, and `SQLITE_OMIT_LOAD_EXTENSION` on both in line with R18's closed-by-default posture. Rejected, the intersection, which breaks any query using a function one backend has today.
8. **Foreign keys are R92's.** Native enforces them by compile default (`SQLITE_DEFAULT_FOREIGN_KEYS=1`) and the browser does not, while replica DDL carries the server schema's `REFERENCES` clauses over a partial copy. R92 strips those clauses from synced tables and keeps enforcement on at every open on both backends, so SQLite enforces tier references natively. This phase sets nothing about foreign keys. Rejected, enforcement off with tier references re-implemented by generated triggers, which cascade one level deep without `recursive_triggers`, cannot honour deferred constraints, and need a guard table for the import.
9. **One page size on both backends, chosen by measurement.** Step 0 times browser page reads and writes at 4096 and 8192 bytes under ChaCha20, and the faster becomes the size both set at creation. Today native defaults to 4096 and the browser to 8192.
10. **The phase closes on the fork.** Retiring the fork when both upstream releases land is a pin move tracked in the Upstream dependencies section, as the diesel fork already is. Under the amended route of decision 4, retiring the fork waits on three releases, not one, the source crate, a `sqlite-wasm-rs` release consuming it, and a `libsqlite3-sys` release with the feature, followed by the diesel bounds moving.

### Steps

0. **Prove the native build on a fork, and stop if it cannot be made.** A fork of `libsqlite3-sys`, carried through `[patch.crates-io]`, gains a bundled SQLite3MC feature compiling SQLite3MC 2.3.3 on SQLite 3.53.0 with decision 7's option set. It passes when it links under the diesel fork's `libsqlite3-sys` range (`>=0.17.2, <0.39.0` with `bundled_bindings`), carries the session extension and the preupdate hook so `diesel-sqlite-session`'s `sqlite3changeset_apply_v3` resolves, accepts the raw-key `PRAGMA key` form under `chacha20`, lets `cipher::unlock`'s codec probe work on native through the check the browser uses, and re-measures every knob `hardening.rs` asserts (R18) green. The step also takes decision 1's and decision 9's browser measurements, then writes the two upstream requests through the upstream procedure, each after a prior-art search. If the fork cannot pass, the phase stops and reports, and the fallback recorded for that case is two codecs by decision, native on SQLCipher and the browser on SQLite3MC with the `cipher.rs` pin promoted from stopgap to contract, which keeps the SQLCipher v4 format. Rejected now, taking the fallback without trying, since the split is what makes the pin load-bearing. **Result of the fork run.** The fork builds natively and for `aarch64-linux-android`, links no `libcrypto`, and proves the version, option, raw-key `chacha20`, wrong-key, session and codec-probe conditions by test. `PRAGMA cipher_version`, which `cipher::unlock`'s native probe reads today, returns no row under SQLite3MC, so native must move to the browser's `PRAGMA cipher` probe in the change that swaps the codec. The browser measurements of decisions 1 and 9 and the `hardening.rs` re-run are still to take.
1. Replace the native vendoring so both backends run SQLite3 Multiple Ciphers on one SQLite version.
2. **Keep the pin until the split is actually gone**, then replace it with decision 2's single `chacha20` declaration in the same change that removes the second library, never before, because the pin is what holds compatibility together in the meantime.
3. Record the version both backends now pin, since one version was the point. It is the browser's, SQLite 3.53.0 with SQLite3MC 2.3.3 at `sqlite-wasm-rs` 0.5.5, because the diesel fork caps `sqlite-wasm-rs` below 0.6. A test on each backend asserts `sqlite_version()`, the SQLite3MC version and the compile options against one constant, so a bump on one side alone fails. **Target amended with the maintainer.** `sqlite-wasm-rs` 0.6.1 carries SQLite3MC 2.5.1 on SQLite 3.53.4 and is already required by `rusqlite` master, and connetto stays on 0.5.5 only because the diesel fork caps `sqlite-wasm-rs` below 0.6. Both backends target SQLite3MC 2.5.1 on SQLite 3.53.4, which moves that cap.
4. Set the runtime options of decision 6 identically at every open on both backends, and the page size of decision 9 at creation.
5. Record decision 3's break in chapter 14, and the compile-time differences that remain until both upstream releases land in chapter 13.

### Proof

Each of these is a test on both backends unless it names one.

1. A replica and its tier written natively open in the browser and read back identical rows, and the reverse. Removing the `chacha20` declaration on either side fails it.
2. After unlock `PRAGMA cipher` reports `chacha20`, and a connection that skips the declaration cannot open the file.
3. A committed SQLCipher v4 fixture written by the pre-R21 code fails to open as `ReplicaUndecryptable`, purge then resync recovers the replica, and export before plus import after carries a tier across.
4. The raw `x'...'` key opens with no KDF, a wrong key fails at the first schema read, and neither a plaintext marker nor the schema text appears anywhere in the file's bytes.
5. With the codec linked the codec probe passes, and a build on plain `bundled` reports `CodecMissing`.
6. `sqlite_version()`, the SQLite3MC version and `PRAGMA compile_options` match one constant, and until both upstream releases land the chapter 13 list of remaining differences is asserted exactly.
7. Page size at creation and cache size read back identically.
8. Once decision 7's set ships on both sides, a query calling `pow()` and one calling `soundex()` succeed on both.
9. `hardening.rs`, the session capture and `apply_v3` path, and `encrypted_replica` pass unchanged.
10. Natively, the linked test binary carries no `libcrypto`, and the Android cross-build succeeds.

Step 0's browser measurements, ChaCha20 against SQLCipher v4 and 4096 against 8192-byte pages, are recorded here as numbers rather than asserted.

### Done when

One library, one SQLite version and one cipher declaration on both backends on the fork, the browser-only pin gone, the runtime options set identically, the break of decision 3 recorded in chapter 14, and the version and option assertions green on both.

### Codec and pragma API cleanup

The local codec statements, including the device-key attach the shared store uses, are raw SQL for now. Their removal is required once [`diesel-sqlite-cipher`](https://github.com/LucaCappelletti94/diesel-sqlite-cipher) and Diesel's [typed SQLite pragmas](https://github.com/diesel-rs/diesel/pull/5259) are usable together in connetto's dependency pins on native and browser targets. The prerequisite includes borrowed quoted text and secret-safe query diagnostics. Compatible fork pins can satisfy it without a published Diesel release.

| Owner | Required cleanup |
|---|---|
| `diesel-sqlite-cipher` | Replace `cipher.rs`'s `raw_key_literal`, `unlock_statement`, `CIPHER_PRAGMAS` and the raw codec probes with typed codec operations, and remove the local key formatting and the codec-specific pragma SQL. |
| `diesel-sqlite-cipher` | Replace the raw keyed attachment statement inside `harden::attach_keyed_in_window`. Keep connetto's attachment permission window and the resealing on success and failure. |
| Diesel | Replace the raw `journal_mode`, `case_sensitive_like` and `defer_foreign_keys` statements in `connetto-client/src/lib.rs`, and the `schema_cookie` and `harden::attached_count` pragma queries, with typed APIs. Remove the obsolete pragma result structs and formatting. |
| Diesel | Replace `pragma_table_info` in `connetto-client/src/replay.rs`'s `table_shape`, its `defer_foreign_keys` read and writes, and the `defer_foreign_keys` statement of `connetto-web/src/relay.rs`'s local-tier transaction with typed APIs. Remove the obsolete pragma result structs and formatting. |

Connetto retains key custody, store naming, unlock ordering, wrong-key validation, attachment hardening and browser VFS selection. Existing typed Diesel calls stay typed. Deployment DDL emitted by pg2sqlite remains pg2sqlite's responsibility. Remove obsolete helpers and comments throughout the consumers without retaining a parallel raw-SQL path.

The local key statements can expose key material through Diesel instrumentation or debug query formatting. Zeroizing rendered text and binding the attachment key do not provide diagnostic redaction. Adoption must prove that key literals and binds stay absent from debug output, errors and instrumentation, including instrumentation installed at connection establishment.

Completion requires native and headless-browser proof of encrypted replica opens, two identities reading and writing one independently keyed store, rejection of wrong keys without occupying the attachment name, and resealing after failed attachment. Preserve missing-codec rejection, codec compatibility, per-open connection settings and transaction-scoped foreign-key deferral.

## R108: caller kinds as database roles


**Blocked on shared semantics in sql-traits, for slices 2 to 5.** pg2sqlite and rls2fga each work out on their own which policies apply to a role, which privileges gate a command, what `current_user` means and how a session setting is typed and split, and an alignment review measured against PostgreSQL 18 found them disagreeing on each. The fix computes those answers once in sql-traits, as a deployment description (the login role, the acting role of each kind, each session setting's type, separator and whether it may be unset, merged as sql-traits #213), privileges per command and per column, and the policies PostgreSQL applies to an acting role per statement shape, and has both translators adopt them. rls2fga #93, #94 (`TO` settled against caller roles) and #95 (table privileges per role) are held and rebuilt on those answers, so each rule exists once. The two bot kinds take effect once R99 gives bots their logins. Slice 1 is blocked on nothing.

**Changes state.** Client tables: none, but the build's schema bundle becomes one bundle per kind, and a client opens its replica with the bundle of its kind. Server tables: none of connetto's. The deployment adds a third document, its kind roles and their grants, beside the schema and the policies. Wire messages: a refusal reason naming a table and a kind, for a subscription and for a write. Archive format: none.

### Purpose

Every caller reaches Postgres through connetto's one reader role, and a policy tells callers apart only through the settings connetto binds (`app.user_id`, `app.subjects`, `app.bot_list`). A policy written `TO app_admin` therefore means one thing to Postgres, which judges it by the reader role, and another to OpenFGA, which rls2fga gates on `pg_role` memberships nobody loads or watches. Today such a deployment fails to start only by accident: pg2sqlite refuses a role-scoped policy because connetto never names a session role, with a message naming a pg2sqlite builder call, after the server has already written the model and the facts to OpenFGA (`build_write_side`, `builder/mod.rs`). This phase makes the refusal explicit and early, then gives each kind of caller its own database role, so policies can name kinds and table grants can differ per kind.

### Decisions, taken with the maintainer

1. **Four kind roles, flat.** Anonymous callers, signed-in users, a bot acting for the owner that added it, and a bot as itself, by default named `anon`, `authenticated`, `bot_installation` and `bot`, each renameable in the server builder. No kind implies another, as in Hasura's flat model ("Roles in Hasura are defined in a flat, non-hierarchical model"), so a rule for every caller has no `TO` clause. A role per user is rejected, and so is a role bypassing row-level security in the manner of Supabase's `service_role`, whose own guide warns that "A leaked secret key exposes all of your project's data". connetto's callers are devices, and a machine acting through connetto comes as a bot, held to its list.
2. **Finer states stay settings.** Share keys, the bot list, and any later assurance level or attestation stay in the settings connetto binds, as Supabase keeps anonymous sign-ins and MFA as claims under one role.
3. **The switch is per transaction.** The reader role holds every kind role `WITH INHERIT FALSE, SET TRUE`, since Postgres applies a `TO` clause by `has_privs_of_role` and an inherited membership would apply every kind's rules before any switch. `CallerBinding` runs `SET LOCAL ROLE` beside the settings it binds today, so no path runs a caller's statements as the bare reader role.
4. **A `TO` clause is settled against connetto's caller roles**. A policy every caller role reaches applies to every caller, one some reach is scoped to those kinds, and one no caller role reaches, such as a policy for an admin tool that logs in outside connetto, is left out of connetto's model and its client copies and reported, never refused. A role whose membership the schema does not settle refuses startup. Every refusal runs before anything is written to OpenFGA. Until the kind roles exist, the reader role is the only caller role.
5. **The kind roles and their grants are a third document.** connetto translates it beside the schema and the policies, and grants may differ per kind. At startup connetto compares each kind's privileges in the database (`has_table_privilege` per role and command) with the document and refuses to start on any difference. It refuses a kind role that can log in, is `SUPERUSER` or `BYPASSRLS`, belongs to `pg_read_all_data` or `pg_write_all_data`, or is held by the reader role with inheritance. It also refuses a kind role that owns a table the schema declares or inherits the privileges of its owner (`pg_has_role(kind, owner, 'USAGE')`), since PostgreSQL skips an owner's policies unless the table forces row-level security, and sql-traits answers every table as owned by a role other than each acting role. A deployment that shares a role name with another API on the same database, such as Supabase's own `anon`, renames connetto's kinds, since that API's grants would otherwise differ from the document.
6. **connetto's own bookkeeping is never granted to a kind.** The write path writes `_connetto_writers`, `_connetto_settled` and `_connetto_session_writers` after `RESET ROLE`, back under the reader role, in the same transaction as the caller's change.
7. **The registry tables are granted per kind.**

   | Table | `anon` | `authenticated` | `bot_installation` | `bot` |
   |---|---|---|---|---|
   | `connetto_registration_codes` | none | create and read its own | none | none |
   | `connetto_bots`, `connetto_bot_lists` | none | read | read its own bot | read its own rows |
   | `connetto_installations`, `connetto_installation_optional_tables` | none | read and write as admin of the owner served | read its own | read its own |

8. **A command a kind lacks is refused up front.** A subscription to a table the kind cannot read is refused at subscribe with the new reason, a write it cannot make is refused before it is applied and settles as refused, and its client copy omits the table. A `permission denied` that still arrives is classified through R106 as a permanent refusal and never retried.
9. **The client copy is translated once per kind.** pg2sqlite builds each kind's copy from sql-traits' answers for that kind, so the copy keeps the policies that apply to the kind, memberships included, and each table takes the kind's privileges, omitting a table it cannot read and making read-only one it cannot write. Each replica file holds one kind only, since an anonymous caller runs in memory and signing in opens the file named from the identity, so the build emits a bundle per kind and the client picks its own. `current_user` evaluates to the kind role's name, as PostgreSQL defines it after `SET ROLE`.
10. **Each check tells OpenFGA the caller's kind**. The kind belongs to the connection, not to the person, since one share key may be held by an anonymous and by a signed-in caller. So no kind is stored as a fact. Each check carries the caller's kind as a contextual tuple, the pattern of OpenFGA's token-claims guide, which `BatchCheck` accepts per item, and connetto's local answers take the kind as an input in the same way.
11. **connetto checks the kind roles by default, and creating them is opt-in.** By default the deployment creates the roles and applies the grants from the roles-and-grants document, as it already creates the reader role, and startup refuses on any difference, as it already refuses a missing publication or replication slot. A builder setting lets connetto create the roles at startup instead, where its owner role has `CREATEROLE`, which managed Postgres often withholds. A deployment that asks for creation without `CREATEROLE` is refused at startup.

### Slices

1. **Refusals before OpenFGA.** Step 1. Blocked on nothing.
2. **One deployment description.** Step 2. Blocked on sql-traits' deployment description and both translators adopting it.
3. **The kind roles.** Steps 3 to 5. Blocked on sql-traits' privileges and effective policies and both translators adopting them.
4. **The client copy per kind.** Step 6. Needs slice 3.
5. **Refusing what a kind lacks.** Step 7. Needs slices 3 and 4.

### Steps

1. Every startup refusal runs before `build_authorization` writes to OpenFGA, so a refused deployment leaves the store untouched, the replica translation's included. A refusal names the policy and the role it could not settle, never a translator's builder call.
2. connetto builds sql-traits' deployment description once, with the reader role as the only acting role and its three settings declared always bound, and hands the same description to pg2sqlite and rls2fga. A policy for a role outside connetto is then reported and left out on both sides instead of refusing startup through pg2sqlite. Preflight refuses a reader role that owns a table the schema declares or inherits its owner, as decision 5 refuses it for a kind role.
3. The server builder takes the four kind role names and the setting of decision 11, off by default. Preflight checks decision 5's attributes and the grants against the roles-and-grants document, after creating the roles first when the setting is on, and the four kinds become the description's acting roles.
4. `CallerBinding` switches to the caller's kind role, and the write path resets to the reader role for connetto's bookkeeping.
5. connetto translates the roles-and-grants document with the other two, and every check, remote and local, carries the caller's kind per decision 10.
6. `connetto_schema` translates once per kind, the generated bundle carries one entry per kind, and the client opens its replica with the bundle of its kind.
7. The subscription and write refusals of decision 8, and the client's error for a table its copy lacks.

### Proof

Docker-gated. A deployment refused at startup leaves the OpenFGA store untouched. A policy `TO support_staff` for a role outside connetto is reported and serves no connetto caller, in OpenFGA and in the client copy alike, and a `TO` role the schema never creates refuses startup. With the four kind roles and grants that differ per kind, an anonymous caller's write to a table it may only read is refused before the database, its subscription to a table it may not read is refused at subscribe, and its client copy lacks that table, while a signed-in caller reads and writes both. A share key used by an anonymous and by a signed-in caller is answered by each one's kind. A grant applied by hand and missing from the document refuses startup, and so does a kind role or the reader role that owns a synced table or inherits its owner, with and without `FORCE ROW LEVEL SECURITY`. By default a missing kind role refuses startup, and with creation on connetto creates it where its owner role has `CREATEROLE` and refuses where it lacks it. The differential harness that compares PostgreSQL, OpenFGA and the SQLite copy for one policy set, built on the same deployment description, agrees for each kind on connetto's example deployment. The harness is seeded with the alignment review's probes, including a dropped `::uuid` cast over an untyped `pg2sqlite` mapping and `->>` on a non-string `jsonb` value, the two replica-side defects that go to `pg2sqlite` as upstream requests.

### Done when

A deployment can name connetto's kinds in its policies and grant each kind its own table privileges, a policy for a role outside connetto serves none of connetto's callers, every refusal comes before anything is written, and Postgres, OpenFGA and each kind's client copy agree on what each kind may read and write.

## R109: content tickets bound to their session

**Blocked on** nothing.

**Changes state.** Client tables, none. Server tables, none. Wire messages, the content ticket gains the minting session. Archive format, none.

### Purpose

Every fetch already rechecks that the ticket's caller may see the file, so a revoked share or an erased row denies at once. What a ticket outlives is its session, [G5](#g5). After a logout or a revoked session, a cached ticket keeps reading the files that identity can still see until it expires. This phase makes the session's end the ticket's end.

### Decisions, taken with the maintainer

1. **The ticket names its session and every fetch confirms it is live.** The signed payload gains the session that minted it, and the file server consults the session's liveness on every read through a check the server builder wires to `AuthService::session_is_live`, so a logout or a revocation cuts every cached ticket of that session at its next request.

### Open points

1. **A ticket minted by a share key alone**, with no session behind it. Whether it binds to the share key's own liveness or keeps the expiry as its only bound.
2. **The cost of the check.** One store read per fetch, or a short-lived cache of live sessions, and what the cache's lifetime adds to the lag.

### Steps

1. The ticket payload carries the minting session, signed with the rest.
2. The file server's configuration takes a liveness check, the builder wires it to the auth store, and every read refuses a ticket whose session is not live, answering as it answers a file the caller may not see.

### Proof

Docker-gated. A ticket minted in a session reads a file, the session logs out, and the same ticket is refused at once, while a ticket of another live session of the same identity keeps reading.

### Done when

No content read succeeds for a ticket whose session has ended.

---

## R110: a session bound by provider staleness

**Blocked on** nothing.

**Changes state.** Client tables, none. Server tables, the session record gains the time of the provider's last confirmation. Wire messages, none. Archive format, none.

### Purpose

A session lives for a fixed idle window extended by connetto's own refresh, under a fixed absolute ceiling, [G6](#g6). A user the identity provider deactivated keeps a session until one of those lapses, and a user active every day is signed out at the ceiling. This phase bounds a session by how long ago the provider last confirmed the user.

### Decisions, taken with the maintainer

The recommended default bounds staleness. The clock measures the time since the identity provider last confirmed the user, and it resets on every successful provider refresh. It does not measure how old the session is. A continuously connected user therefore never reaches it, for months or for years, and a user the provider has deactivated is cut off at their next reconnect, sooner than any fixed deadline would manage.

What counts as a check, and what a failure means. A successful provider refresh resets the clock. A failure does not end the session and does not touch local data. It means the provider's answer is unknown, and the clock simply keeps running until the bound is reached, at which point an interactive login is required. That login is where the answer becomes unambiguous, because a genuinely removed user fails it and everyone else succeeds and resets the clock. This is forced by the protocol rather than chosen. RFC 6749 defines a single `invalid_grant` code covering a token that is "invalid, expired, revoked, does not match the redirection URI... or was issued to another client", and Google documents its own causes as including a password change, six months of disuse, and exceeding a cap on live tokens. Most are benign, none are distinguishable from the error, so no destructive action may depend on that signal.

Long-lived access is always requested, so the check always exists. Without a retained provider refresh token there is no probe, and the staleness bound silently degenerates into the session-age ceiling it replaced. connetto therefore requests offline access at login even when the application never calls provider APIs, and treats the probe as part of the session mechanism rather than a side effect of whatever scopes were configured. The cost is not uniform. Microsoft lists `offline_access` among the scopes a user consents to, while Google grants the refresh token through a request parameter with no separate consent line.

A failed provider refresh means unknown, never revoked. It ends nothing and destroys nothing, it only stops resetting the clock, because `invalid_grant` cannot distinguish revocation from a password change or six months of disuse. The interactive login at the bound is where the answer becomes unambiguous.

### Open points

1. **The default bound**, asked of the maintainer when the phase starts.

### Steps

1. Every login requests offline access, and the session record keeps the time the provider last confirmed the user.
2. A refresh past the bound refreshes against the provider. A success resets the clock, and a failure leaves it running until the bound, past which the session requires an interactive login.
3. The fixed idle window and absolute ceiling of `RefreshLifetimes` go.

### Proof

Docker-gated, against the mock identity provider. A session the provider keeps confirming outlives the old ceiling. A user the provider deactivates is refused at the next refresh past the bound. A provider outage alone ends no session before the bound.

### Done when

A session ends when the provider has not confirmed the user within the bound, and at no fixed age.

---

## R111: a durable replica where OPFS is refused

**Blocked on** a durable IndexedDB VFS for `sqlite-wasm-rs`, built in Luca's fork for an upstream pull request that answers Spxg/sqlite-wasm-rs #193. Its xSync suspends through JSPI until an IndexedDB transaction opened with `durability: "strict"` completes.

**Changes state.** Client tables, none. Server tables, none. Wire messages, none. Archive format, none. The browser replica gains a second storage backend.

### Purpose

A browser that refuses OPFS, Firefox private windows among them, runs the replica in memory, [G4](#g4), so a reload loses every unsent write. Firefox private windows keep IndexedDB for the life of the private session. A synchronous VFS over IndexedDB loses commits, which for connetto means a reused writer sequence the server refuses as forked, and `sqlite-wasm-vfs` removed the one it had for that reason. JSPI, in Chrome 137 and Firefox 153, lets a VFS wait for IndexedDB before SQLite's commit returns.

### Decisions, taken with the maintainer

1. **The VFS is built upstream first.** It is proven in Luca's `sqlite-wasm-rs` fork before any issue or pull request, and it depends on `rsqlite-vfs` alone so it can become a standalone crate if upstream declines it.
2. **The storage order is OPFS, then the IndexedDB VFS where JSPI is present, then memory**, and the boot report says which one the replica runs on.

### Open points

1. **Loading the fallback.** A wasm module built with JSPI imports fails to instantiate where `WebAssembly.Suspending` or exnref is missing (Safari, Firefox before 153, Chrome before 137), before any Rust runs, so one binary cannot fall back by itself. Either the worker ships two builds and picks one in JavaScript by `typeof WebAssembly.Suspending`, or wasm-bindgen gains suspending imports that degrade to a throwing stub, an upstream change.

### Steps

1. The DB worker builds with JSPI and opens the replica on the IndexedDB VFS when OPFS is refused and JSPI is present.
2. The boot report names the backend, and the memory case keeps its warning.

### Proof

A browser suite in a Firefox private window writes, reloads and finds every acknowledged write and the writer chain intact, and a killed worker after a commit loses nothing the client reported committed.

### Done when

A browser that refuses OPFS but runs JSPI keeps the replica and its queued writes across reloads.

---

# Cross-cutting checklist

**Wire changes.** A phase that breaks the wire bumps `PROTOCOL_VERSION` in the same pull request, by the rule [02-protocol.md](../docs/architecture/02-protocol.md#the-wire-version) states. Nothing is released, so a bump needs no coordination with older clients, and a mismatch stays detectable at the handshake.

**Type-enforced guards, not documentation.** The value handed to `connect` carries a marker for what the run keeps at rest and owns the device-private database beside it, so a durable one paired with an unkeyed replica is not a program, proven by `compile_fail` doctests on `Replica`. `Principal` makes all four arrival cases representable and, being an optional identity beside a set of capabilities, has no fifth state.

---

# Parked, with reasons

These are decided or recorded and belong to **no** phase. They are here so nobody treats their absence as an oversight.

**The unsynced-data warning as a session nears expiry needs no phase.** `expiry_warning` in `crates/connetto-client/src/teardown.rs` already takes the expiry, a lead time and the unsynced sequence numbers, and `session_expires_at` already reaches the client on the auth response. Its caller is the embedding application by design.

---

# What must not be done

Handoff, prompt, upstream and roadmap documents are process artifacts and are never committed. `docs/upstream-*.md` and `docs/handoff-*.md` are untracked and must stay so. This file and the architecture chapters are committed only when the maintainer names them in that moment.

Do not commit at all without an explicit instruction in the moment. Do not push. Do not open a pull request.

Do not treat a phase as done because it compiles. Every phase above has an acceptance section naming an observable result.

Do not implement R6 before R5b. R5b has landed, so this is discharged, and it is kept because the reason was never a cost preference: row-level security cannot answer a question about a row that is no longer in the table.

Do not build the local negative filter before R0 shows it is needed. Its trigger is a measured failure of the counter test with the caches enabled, and not before.
