# 14: At-rest encryption

Every durable replica on a device is ciphertext, and this chapter owns the mechanism that makes it so. It covers what is encrypted, the replica key and its custody, the two secret stores, the user-verification gate in front of them, provisioning, the page codec, the ordering and naming rules, teardown, and the threat model's boundary. How a caller proves an identity is [11-authentication.md](11-authentication.md), what an identity names and what encryption does not cover is [12-identity-session-capability.md](12-identity-session-capability.md), the retention mode is [15-replica-retention.md](15-replica-retention.md), the content store is [18-file-handling.md](18-file-handling.md), and the clocks the re-check reads are [21-clocks.md](21-clocks.md).

## What is encrypted

Every durable replica on a device is ciphertext, and the local tier that attaches to a replica shares its key. The refresh credential lives outside these stores. Natively it is in the OS keyring, and in the browser it is the `HttpOnly` cookie that [11-authentication.md](11-authentication.md#the-connetto-session-credential) owns.

| Store | Native | Browser |
|---|---|---|
| Replica | SQLCipher file, per-replica key | sqlite3mc file in OPFS, per-replica key |
| Local (never-syncing) tier | ATTACHed, inherits replica key | Separate connection, same per-replica key passed explicitly |
| Refresh token | OS keyring, no SQLite involved | none, an `HttpOnly` cookie the browser keeps |
| Account index and last-used marker | OS keyring index record | plain OPFS SQLite database, never secret |

The browser's account index is plain by design, since an account key is an identifier and not a secret. A file an earlier build left under its name reads as not a database, and the boot discards it and asks for a login, while any other failure to open it propagates rather than costing the remembered accounts.

## The key

[`connetto_core::ReplicaKey`] is the shared value type. 32 raw bytes, lowercase hex on the wire, zeroized on drop, and both `Debug` and `Display` redacted, so the material cannot reach a log through a derived formatter.

The device mints the key, on first sight of a replica it is about to create. No key material crosses the wire and the server never holds any. Two devices for the same identity mint different keys and neither can read the other's file. Server escrow is rejected, because the server's auth store has no device concept. Provision-once per device keeps the locally wrapped key the only copy, which is what makes crypto-shredding unconditional.

Losing the cached key loses the replica. Synced tables recover by re-syncing from the server. Device-local tables do not recover at all.

The key survives logout deliberately. It is scoped per device rather than per session, so a returning user resumes the replica instead of re-syncing, and it is destroyed only by an explicit data wipe or by the revocation of the device's own key, which wipes the device. [11-authentication.md](11-authentication.md) owns the revocation itself.

An anonymous boot touches no key record. Its replica, tier and content store are in memory, so the browser worker provisions or loads a key only when the boot is identified. The anonymous content store still runs behind the encrypting decorator, because the archive is built over it on every platform. Its root key is minted for that worker and never stored.

## Key custody

### One trait per secret

Each of the two secrets has one trait in `connetto-core`, implemented by every native and browser store, and every method names the account it addresses.

| Secret | Trait | Native | Browser |
|---|---|---|---|
| refresh token | [`connetto_core::traits::RefreshTokenStore`], boxed futures | [`connetto_client::KeyringStore`], [`connetto_client::MemoryRefreshStore`] | none, an `HttpOnly` cookie |
| replica keys | [`connetto_core::traits::ReplicaKeyStore`], `MaybeSend` futures | [`connetto_client::KeyringKeyStore`], [`connetto_client::MemoryKeyStore`] | `IdbKeyStore` in [auth.rs](../../crates/connetto-web/src/auth.rs) |

Each trait carries an associated `Error`, following [`connetto_core::traits::Transport`]'s, so no shared error type was invented.

The native stores sit on `keyring-core` over the pinned forks `apple-native-keyring-store`, `android-native-keyring-store` and `windows-native-keyring-store`, and on Linux over `linux-keyutils-keyring-store` and the Secret Service over `zbus`.

The account is an argument rather than a field, because one store instance serves every account on both targets. On boot the account is read from the last-used marker ([`connetto_client::auth::remembered_account`], and `remembered_account` in [auth.rs](../../crates/connetto-web/src/auth.rs) in the browser), from an explicit switch target, or is `None` on a first run, which goes straight to an interactive login.

Both stores await, because the browser reaches `IndexedDB` and `SubtleCrypto` through promises that have no synchronous form in a worker, and the Linux desktop reaches the Secret Service only over D-Bus. The refresh-token trait returns boxed `Send` futures, so the native authenticator can hold it as `dyn RefreshTokenStore`. The Apple, Windows and Android stores still answer from a call that returns immediately.

The seam buys one caller. The store tests run the shared exercises from [test_support.rs](../../crates/connetto-core/src/test_support.rs), written against the traits alone, against the native and the browser stores.

### The key store

The store has `load` (return the cached key for `name`, or `None`), `store` (persist a key under `name`), and `clear` (remove the record, which crypto-shreds the replica).

`name` is the same value `replica_db_name` produced for the replica file, so two identities on one device hold separate records and a wipe of one cannot reach the other. A literal name is equally valid.

The production store on native is [`connetto_client::KeyringKeyStore`], in [auth.rs](../../crates/connetto-client/src/auth.rs). Keychain on Apple platforms, the Keystore on Android, Credential Manager under Windows Hello on Windows, and on Linux the store below.

Every durable Linux secret, the replica key, the refresh token and the device-scoped key the shared store uses, survives a reboot.

| Surface | Where the secret lives |
|---|---|
| Desktop session | base64 text in the Secret Service's default collection, unlocked or created through the desktop's own dialog under a bound, as libsecret does |
| Flatpak or Snap | libsecret's sandbox keyring file, opened with the Secret portal's per-application secret |
| Headless host | files under a state directory, sealed under a wrap key the operator supplies as the `connetto.wrap-key` systemd credential, or as a key file the application names when systemd is absent |

A rotated wrap key reseals every record once from the previous key. On KDE, `ksecretd` writes a wallet to disk 5 s after a change and not when it is stopped, so a secret stored in that window before a logout or reboot is lost and reads back empty. With none of these reachable the client refuses with a typed error unless the application explicitly chooses keyutils and accepts losing its keys at reboot. `KeyringStore::backend` and `KeyringKeyStore::backend` report which backend is in use, which store holds the keys, and whether the previous wrap key is still needed.

The test implementation is [`connetto_client::MemoryKeyStore`], an in-memory `HashMap` in [memory_stores.rs](../../crates/connetto-client/src/memory_stores.rs).

### The refresh store

The methods of [`connetto_core::traits::RefreshTokenStore`] are `load` (return the stored token for `account`, or `None`), `store` (persist a token under `account`), `clear` (remove the record for `account`), and `accounts` (return every account the store holds a token for, excluding connetto's own reserved records).

The account key is [`connetto_client::encode_identity`] of the deployment's user id, the serde JSON encoding of the id type. For a `String` id `"alice"`, the key is the seven-character string `"alice"` including the quotes, and [`connetto_client::decode_identity`] reverses it.

The native store cannot ask the platform keyring to list its entries, so [`connetto_client::KeyringStore`] keeps its own index record in the keyring alongside the token entries. An out-of-band keychain edit can leave the index stale, and a stale entry that names an account whose token has since been removed falls through to an interactive login rather than selecting a wrong identity. The index is a record only while it lists an account. Clearing the last account removes the record rather than writing an empty list, so a store whose accounts are all gone leaves nothing in the OS keyring.

After a successful login, both authenticators write [`connetto_client::IDENTITY_RECORD`] (the literal `"connetto-device-identity"`) with that same account key as its value, natively beside the credential and in the browser as a plain record. Reading it with `remembered_account` yields the same string that addresses the account's row, and natively the token. A boot with no marker stored returns `None` and the authenticator goes straight to an interactive login.

### Browser key store

`IdbKeyStore` in [auth.rs](../../crates/connetto-web/src/auth.rs) wraps an `IndexedDB` database named `connetto-key-store`. It has three object stores.

| Store | Contents |
|---|---|
| `kek` | One non-extractable AES-GCM-256 key-encryption key (KEK), stored as a structured-cloneable `CryptoKey`, held only while no passkey is enrolled |
| `wrapped` | Per-identity records, each keyed by the replica name and the enrolled credential id, each holding a 12-byte AES-GCM IV followed by the AES-GCM ciphertext of the raw replica key |
| `credentials` | Enrolled credential identifiers, in the clear, since they are not secret and are needed to scope the assertion |

The KEK is generated once per browser profile, marked non-extractable, and never exported. Script-level reads of the `wrapped` store yield opaque ciphertext, because the KEK bytes are unreachable by script.

The scope of protection is documented on the type. This defends against script-level exfiltration and an off-device copy of the `IndexedDB` contents. It does not defend against a resident attacker who can call `load` directly, and not necessarily against an attacker holding the full browser profile directory, which includes both the `IndexedDB` files and the backing storage for non-extractable keys.

### The gate on locally stored secrets

With the gate on and a platform that supports it, every locally stored secret sits behind a user-verification gate, and the custody the client reports says when it does not. Opening the app presents a fingerprint, a face check or a device passcode once, after which natively both the replica and the stored refresh token become readable, and in the browser the replica key, since the refresh credential is a cookie. Both native secrets are gated, because the refresh token alone can open a session and pull the data down again, and the replica key alone already has it. The two are independent keychain items, gated separately. The server verifies nothing and sees nothing. Session lifetime, revocation and the identity provider's authority are governed by [11-authentication.md](11-authentication.md) and untouched by the gate.

One unlock lasts as long as the process, and a fresh start prompts again. There is no inactivity timeout and no per-operation prompt. The operating system's screen lock owns the unattended machine, since connetto has no notion of a sensitive operation to hang a second prompt on.

An application may ask again after time away. [`connetto_client::Gate`] carries one re-check grace, set with `with_recheck` on the durable stage of either client builder. Absent, a launch unlocks once. Set, the gate asks again when the application returns after being away longer than the grace, and zero means every return. The full event-by-situation table is [The gate's states](#the-gates-states) below.

The client has one away input, `ConnettoClient::away` and `ConnettoClient::back`, each taking a [`connetto_client::Moment`]. [`connetto_dioxus::use_away_input`] feeds it from tao's `Suspended` and `Resumed` on iOS and Android and from window focus on the desktop, the browser tabs report page visibility to the worker, and an application with neither feeds it itself. Time away is the larger of a sleep-counting monotonic delta and a wall-clock delta, measured from the two moments and never with a timer, since a suspended iOS process runs none. [21-clocks.md](21-clocks.md) names each platform's sleep-counting clock.

A re-check that fires pauses the application and keeps the session. Approval emits `ClientEvent::Unlocked` and each held handle wakes once. A dismissal emits `ClientEvent::UnlockDismissed` and stays locked, and the next return or `ConnettoClient::unlock`, typically from a click, asks again. In the browser the prompt runs in the tab that spawned the worker, the only one holding the private port a non-extractable key may cross.

The browser mechanism is a key derived from a passkey. WebAuthn's `prf` extension derives 32 bytes from a credential given an input, the same bytes every time, and the specification forces user verification for it, overriding the request's own preference if necessary, so the gate arrives as a property of the mechanism. The derived value goes through HKDF with a per-purpose label rather than being used as a key directly.

The input is one fixed value, not per identity, producing one key-encryption key that unwraps the per-identity records, so one gesture on a shared profile opens every account's replica rather than the remembered one. Erasing one account without touching another is the job the per-identity records already have.

Each enrolment uses a fresh random WebAuthn `user.id`, so independent origins or browser profiles sharing an authenticator retain separate credentials even when their relying-party domain matches. The stored credential identifier selects the passkey on later launches. Multiple holders are rejected, because every copy lives in the same store and is lost together.

The `kek` store holds the key-encryption key exactly while nobody has enrolled. Enrolling re-wraps every `wrapped` record under the derived key and destroys the stored `kek` record. A profile snapshot taken before enrolment still holds the stored key and therefore the replica key, and enrolment re-wraps but does not re-key it.

`PublicKeyCredential` is `[SecureContext, Exposed = Window]`, so it cannot be called from a worker, while connetto's database and its keys live in a dedicated worker per [09-wasm.md](09-wasm.md). The assertion therefore happens in a tab, and the key crosses into the worker as a key object, not bytes. Web Crypto serializes a `CryptoKey` across security boundaries through structured clone, so the page imports the derived bytes immediately with `extractable: false` and posts the resulting key object. The specification guarantees that key material is not exposed to script except through `exportKey` and `wrapKey`, which a non-extractable key forbids.

The raw bytes exist in page script between the assertion resolving and the import, because the extension returns a buffer and there is no path from a PRF result directly to a key object. Script already resident at that instant obtains exportable material and therefore permanent, portable access to the local data. Script arriving after it can ask the browser to use the key while the page lives, but cannot export, persist, or take it off the device. Conforming user agents are not required to zeroize key material, which may persist to disk, possibly unencrypted, and a key once shared with a destination origin cannot later be restricted or revoked by the source origin.

Natively the account index, the last-used identity and a pending login stay readable before any prompt, since they are not secret and the account choice reads them first.

| Platform | Where the secrets live | Gate | Custody when the gate is impossible |
|---|---|---|---|
| Apple | data protection keychain through `apple-native-keyring-store`'s protected store, the replica SQLCipher over CommonCrypto, each item carrying `RequireUserPresence` | one `LAContext` (`shared-authentication`) evaluated at the first gated read serves every gated item for the life of the process, and the gate's lock resets it | macOS without the `keychain-access-groups` entitlement keeps the secrets ungated in the login keychain, `Unverified(NoGate::Unsupported)` |
| Android | named store of `android-native-keyring-store`, `user-auth-required` with a timeout of zero | the app's `BiometricPrompt` through `KeystorePrompt` in [android.rs](../../crates/connetto-client/src/keyring/android.rs), allowing a strong biometric or the device credential, hosted by `connetto-auth-session` since the crate is pure JNI | no secure lock screen, `Unverified(NoGate::Offerable)`. A release before 11, or a build with no prompt, `Unverified(NoGate::Unsupported)` |
| Windows | `windows-native-keyring-store`'s `HelloStore`, each entry sealed with AES-256-GCM under a key derived from a Windows Hello passkey's PRF through native WebAuthn API 9, the reserved records in the ordinary Credential Manager store | one approval per launch. The prompt needs a live owner window, lent through `KeyringAuth::with_hello_owner` and `use_hello_owner` in [hello.rs](../../crates/connetto-dioxus/src/hello.rs), and Windows disables that window while the prompt is up | no owner window, or no Hello PRF, the secrets stay ungated, `Unverified(Unsupported)` |
| Linux | the key store above | none. The Secret Service dialog is a keyring-password, not user verification, and keyutils has no concept of it | none |

A launch shows one prompt, at the first gated access. A first sign-in only creates the secrets and verifies nothing, so its connect asks once and returns only after the approval, resetting the platform context first because iOS lets the context that created an item read it back without a sheet. A dismissed launch prompt fails the connect. Items written before a gate existed are not migrated.

Removing the screen lock deletes every gated key, and the store reports the key lost rather than replacing it. A durable build wipes the replica and starts a fresh one the server resyncs. A lost or corrupt Hello credential discards the store and reports the loss once, and the durable build gives the same answer. The macOS keychain answers only an app carrying the `keychain-access-groups` entitlement, so the macOS demo ships as a provisioned `.app` (`connetto-macos-app`). Android WebView has no WebAuthn, so a WebView-hosted application's only gate is the Keystore path. `UserConsentVerifier` stays unused. A consent check our own code performs is worth nothing against an attacker holding the files.

[`connetto_client::Gate`]'s `default()` is on, and an application turns it off with `Gate::off()` on the durable stage of the client builder. The gate exists only on that stage, since an in-memory replica has nothing durable to protect. Natively the keyring's own mechanism rides the same setting, and `Gate::off()` writes the secrets without the platform's verification.

Secrets follow the setting. Each platform keeps gated and ungated secrets apart, and a secret an earlier launch stored under the other setting moves to the current storage at its first access. Moving into the gate verifies nothing, so the launch still asks once, and moving out costs one prompt. A copy whose key went with a removed lock reads as a lost key. A write or a delete removes both copies, without a prompt.

The gate is never forced on the user, since dismissing the platform's own prompt is always available. A user who declines gets the stored key without verification, and the application is told so it can warn. Safari has required a user gesture for an assertion since Safari 14 and grants one gesture-free call per navigation, restored after each success and spent on a failure, while Chrome and Firefox do not gate a plain assertion. A first attempt can be automatic everywhere, and a retry after a dismissal needs a real click on Safari, which is why the unlock is a function a tab calls rather than something connetto initiates.

Where no gate is possible, there is no gate. Permanently unsupported surfaces get a key stored locally with no user verification. A PIN was considered and rejected, because the threat is an offline attack on a copied profile and a six-digit PIN is under twenty bits, which no key-derivation function rescues. A passphrase with real entropy would work and is not offered, because the permanently affected population is small and a forgotten-passphrase path is its own design.

The protection level is reported to the application, not merely documented. The two secret-store traits report the protection their items carry through `protection()`, and a builder-built client reports the weaker of the two claims and what the build did, the build's reason winning a tie between two `Unverified` levels. No caller sets a level, since a settable level is the hazard the [`connetto_core::Custody`] module warns about. Read natively from `ConnettoConnection::custody`, in the browser from `custody` in [unlock.rs](../../crates/connetto-web/src/unlock.rs) in the worker or `request_custody` in [intake.rs](../../crates/connetto-web/src/workers/intake.rs) from a tab. The browser answer does not come from a tab's own connection, which holds only its in-memory mirror and would always report no durable key.

### The gate's states

Each cell names what the client does and the situation it ends in. "Unchanged" means no event, no state change and no prompt.

| Situation | Replica | Custody reported | Reached by |
|---|---|---|---|
| anonymous | in memory, no key | `Ephemeral` | a builder that never called `.signed_in()`, or a signed-in builder that never chose a durable replica |
| signed in without a gate | durable, key stored ungated | `Unverified(Unsupported)` where the platform has no gate, `Unverified(Offerable)` where the gate is off, the user has not enrolled, or the device has no passcode or secure lock screen, `Unverified(Declined)` after a declined browser enrolment | the durable replica step with the gate off, or on a platform or device that cannot gate |
| gated and open | durable, key opened through the gate | `Verified` | the gate's prompt approved |
| gated and locked | durable, key behind the gate | `Verified`, since only access is paused | a launch before approval, whether a browser boot or a native first sign-in that has verified nothing yet, or a re-check that fired |

The client keeps at most one pending away moment. A negative delta reads as zero, and the wall reading covers a browser whose `performance.now()` stops in sleep.

| Event | anonymous | signed in without a gate | gated and open | gated and locked |
|---|---|---|---|---|
| launch | open, no prompt, custody `Ephemeral` | open, no prompt | natively, the launch whose sign-in or key read already raised the platform's prompt starts here with no second one | ask the prompt once, and the native connect or the browser worker's boot waits on the answer, so the application never meets the lock at launch |
| went away | ignored | ignored | keep the away moment when a grace is set, replacing an older one | ignored, a pending prompt stays pending |
| came back | ignored | ignored | with no pending away moment, unchanged. With one, clear it and measure time away. Within the grace, unchanged. Beyond it, or with a zero grace, lock the mechanism, emit `Locked`, refuse application access, ask the prompt, and move to gated and locked | with no prompt pending, ask it again. With one pending, unchanged |
| prompt approved | a stray answer is dropped | late browser enrolment. Adopt the derived key, custody `Verified`, move to gated and open | a stray answer is dropped | emit `Unlocked`, resume application access and held live-handle refreshes, move to gated and open |
| prompt dismissed | not reachable | a declined browser enrolment. Custody `Unverified(Declined)` | not reachable | at launch the connect or the boot fails. After a re-check, emit `UnlockDismissed` and stay locked, and the next return or `ConnettoClient::unlock` asks again |
| clock stepped | nothing | nothing | a pending away moment is measured at return by the larger delta, so a step back cannot shorten time away and a step forward costs at most one extra prompt | nothing |
| gate off | nothing durable to gate | this situation | not reachable, the gate is fixed when the builder connects | not reachable |

A platform withdrawing the gate while the client runs, a deleted passkey or a removed screen lock, reaches the client as a prompt that fails, the "prompt dismissed" row.

While locked, the client has refused the application's reads, writes, new watches, pins and unpins with `ClientError::Locked`, holds live-handle refreshes, and has emitted `ClientEvent::Locked`. Live handles keep their last answer and wake once after `Unlocked`. The replica connection, the pump, inbound patches, reconnects and the upload of writes made before the lock keep running until one of them needs a locked secret, and then wait. The browser worker's hub states each lock to every tab (`GateState`), whose clients refuse and hold the same way, and after its handshake ack a tab also learns the worker's sync status and identity (`SyncStatus`, `TabIdentity`), so a tab attaching while locked starts locked. The message families are catalogued in [02-protocol.md](02-protocol.md#message-families).

The native keyring keeps a state of its own per service, since the platform store is process-wide.

| Event | ungated (gate off, or the device cannot) | launch (gated, nothing approved yet) | open | locked (a re-check fired) |
|---|---|---|---|---|
| reserved record read or written | ungated access | ungated access, no prompt | ungated access | ungated access |
| first gated access | ungated access | Apple. A read raises the sheet, a create raises none. Android. The unlock ceremony runs first. Approval moves to open, a dismissal returns the refusal and stays here | not reachable | not reachable |
| gated read or write | ungated access | as above | no prompt, through the shared context or the unlocked store | refused with `ClientError::Locked` and no prompt, so a background token refresh never raises one |
| gated create refused for no passcode or secure lock | not reachable | fall back to ungated, custody `Unverified(Offerable)` | the same | refused with `Locked` |
| gated access refused for a missing entitlement, an Android release before 11, or no prompt or owner window | not reachable | fall back to ungated, on macOS in the login keychain, custody `Unverified(Unsupported)` | not reachable | not reachable |
| a secret found only under the other storage | the gated copy is read, prompting once, and moves to the ungated storage. Dismissed refuses the read, and a copy whose key is gone reads as missing | the ungated copy moves behind the gate with no prompt, and the launch still asks | the same, no prompt | refused with `Locked` |
| mechanism lock | nothing | move to locked (Apple resets the shared context, Android locks the store) | the same | unchanged |
| mechanism ask | approved at once | the prompt, read against a gated secret this process touched, approval moves to open | approved at once, no prompt | the prompt again, approval moves to open, a dismissal stays locked |
| delete, for logout or forgetting the device | both copies go, no prompt | the same | the same | the same |

## Provisioning

`provision_replica_key` is defined in two places, one per target, with the same provision-once semantics. A cached key always wins and is never overwritten, so a second login cannot silently re-key a replica and strand its contents. Only when nothing is cached is a fresh key minted from the device RNG and written through.

- Native. [`connetto_client::replica::provision_replica_key`] in [replica.rs](../../crates/connetto-client/src/replica.rs)
- Browser. `provision_replica_key` in [auth.rs](../../crates/connetto-web/src/auth.rs)

Both are generic over the shared trait and each awaits. They stay one per target rather than moving to `connetto-core` beside the trait, because minting needs an entropy source and `ReplicaKey` deliberately carries none, keeping the browser build free of one.

Call `provision_replica_key` only for a replica that does not yet exist. For one already on disk, call `ReplicaKeyStore::load` and pass the result to `Replica::encrypted_file`. Minting for an existing replica returns a key that decrypts nothing.

## The page codec

connetto does not implement encryption. At each connect it states whether the database holds encrypted pages and hands the key to an off-the-shelf codec. Natively the codec is SQLCipher, vendored by `libsqlite3-sys` under `bundled-sqlcipher`. In the browser it is SQLite3 Multiple Ciphers, vendored by `sqlite-wasm-rs` under `sqlite3mc`. The two are not the same codebase, which forces an explicit pin.

The construction on both sides is SQLCipher version 4. AES-256-CBC per page, with a fresh 16-byte random IV on every page write, plus a 64-byte HMAC-SHA512 over the ciphertext, the IV, and the page number. Both live in the 80 reserved bytes at the end of every page that SQLite accounts for through its page-reserve field. Page 1 carries a 16-byte random per-database salt in the clear. Rewriting a page in place never reuses an IV.

The key is supplied as 32 raw bytes through the `x'...'` form of `PRAGMA key`, documented in [cipher.rs](../../crates/connetto-client/src/cipher.rs), skipping the passphrase KDF because the key is already uniformly random.

The pin is required in the browser. SQLite3 Multiple Ciphers defaults to ChaCha20-Poly1305, and naming `sqlcipher` as the cipher is not enough, because its own `sqlcipher` scheme defaults to a non-legacy variant that places different data in the first bytes of page 1 and cannot read a real SQLCipher file. The two pragmas that establish byte-for-byte compatibility are [`connetto_client::cipher::CIPHER_PRAGMAS`].

```text
PRAGMA cipher = 'sqlcipher'; PRAGMA legacy = 4;
```

The native codec is SQLCipher itself and needs no pinning, so `cipher::unlock` applies them only on wasm.

Planned in [R21](../../plans/master-implementation-plan.md#r21-one-page-codec-on-both-backends), the native side moves to the browser's page codec and the format becomes ChaCha20-Poly1305.

`sqlcipher_export` exists in the SQLCipher amalgamation and not in the sqlite3mc amalgamation, so no plaintext-to-encrypted transform works on both backends.

The browser codec intercepts as a VFS shim, so a database must be opened through a URI that names the codec layer. [`connetto_client::cipher::cipher_url`] composes `file:<name>?vfs=multipleciphers-<vfs>` over the installed VFS (`opfs-sahpool` for OPFS, `memvfs` for the in-memory fallback), so the OPFS-unavailable fallback stays encrypted rather than silently degrading.

## Ordering constraints

`unlock` in [cipher.rs](../../crates/connetto-client/src/cipher.rs) must be the first statement run against a connection. Anything that reads the database header, `PRAGMA journal_mode=WAL` included, fails on an encrypted file before the key is set. Diesel's `establish` only registers SQL functions and never reads the schema, so immediately after `establish` is both safe and the last safe moment. `open_inner` in [lib.rs](../../crates/connetto-client/src/lib.rs), the body `open` and `open_existing` share, applies the unlock before `PRAGMA journal_mode=WAL`, and that ordering is load-bearing.

One further pragma follows `PRAGMA journal_mode=WAL` and precedes the first `CREATE TABLE`, on the create path only. `PRAGMA auto_vacuum = INCREMENTAL`, issued through `SqliteConnection::set_auto_vacuum` in `open_inner`. The mode lives in the file and is fixed at the first table, so it is set when the schema is created and never on a reconnect. [15-replica-retention.md](15-replica-retention.md) is authoritative for why the mode is `INCREMENTAL` and what the trimming pass does with it.

`PRAGMA case_sensitive_like = 1` follows the journal mode on every open, reconnects included. pg2sqlite leads every script it emits with that pragma, as part of declaring that its schema is Postgres's dialect, where `LIKE` is case sensitive. A pragma is connection state, so the copy inside the DDL covers only the first boot that runs the DDL, and `open_existing` runs none. The tier script carries the same pragma, and `with_tier` passes `PRAGMA` lines through unqualified while requalifying every `CREATE TABLE` into the tier schema.

`PRAGMA foreign_keys = ON` follows on every open, reconnects included, through `SqliteConnection::set_foreign_keys_enabled` in `open_inner`, and before the first statement that writes a row. Enforcement is connection state, and the browser build does not enforce by default, so connetto sets it on both. It precedes the replica DDL, which runs with every synced `REFERENCES` and `FOREIGN KEY` clause removed, so it governs the device-local tier's references alone. Two transactions move their checks to commit with `PRAGMA defer_foreign_keys = ON`, which SQLite clears at the commit or the rollback. They are the archive import (`apply_import_with_bookkeeping`) and the relay hub's replay of a tab's tier mutation.

A bare `ATTACH` gives the attached database the connection's derived key. A database it creates takes the main database's key salt, and an existing one is read under the main database's derived key. This holds on both codecs, as the tests in [encrypted_replica.rs](../../crates/connetto-client/tests/it/encrypted_replica.rs) show.

A local tier must therefore be first-booted through the replica connection, which is what `Replica::with_tier` asks for, so both databases share the same key salt. A tier file created by any other connection carries its own salt and will fail to decrypt through the replica connection. A later run says `with_existing_tier` and it works because the salt already matches. The tier is named on the replica rather than attached afterwards, which is what makes a durable tier beside an unkeyed replica unrepresentable.

An attached database can carry a key of its own, given as the `KEY` clause's text `x'<64 hex digits>'`, which both codecs apply to that database alone as a raw key. `attach_keyed_in_window` in [harden.rs](../../crates/connetto-client/src/harden.rs) attaches that way, and it is how every identity's connection reaches the shared public store under the device-scoped key. The same bytes given as a blob literal are taken for a passphrase and run through the KDF into a different key, and `PRAGMA <schema>.key` after the attach is ignored by SQLCipher while sqlite3mc fails it with `SQLITE_NOTADB`. Neither is used.

`sqlite-wasm-rs` allows one connection per database and the sahpool VFS keys its bookkeeping by name, so two live connections to one OPFS file trip a `debug_assert`. The browser local tier is therefore attached to the DB worker's replica connection, exactly as natively, and nothing else opens it ([local_tier_one_handle.rs](../../crates/connetto-web/tests/local_tier_one_handle.rs)). It inherits the replica connection's key and its enforcement of references with the attachment. The shared public store is attached to the same connection, under its own key ([shared_store.rs](../../crates/connetto-web/tests/shared_store.rs)).

## How a replica is named

[`connetto_client::replica::replica_db_name`] in [replica.rs](../../crates/connetto-client/src/replica.rs) derives the replica filename before any transport opens. The derivation runs a SHA-256 over the identity's own serde encoding and encodes the first 128 bits as lowercase hex, producing `{prefix}-{32 hex chars}`. The same identity always selects the same file, and distinct identities produce distinct files.

The derivation does not go through `Display` or any textual representation of the identity. Serde encoding is the canonical byte source, and the result is fixed-length and does not spell the user id in a directory listing.

Deriving the name before connecting is what makes resuming under the wrong identity unrepresentable rather than detected after the fact. An identity mismatch opens a different file and cannot adopt the wrong replica's rows or pending mutations.

The unauthenticated name (for a deployment with no authentication) is the bare prefix, which no derived name can collide with.

## Teardown

Teardown is two orthogonal axes. connetto ships mechanisms and the application decides policy.

Credential teardown. `NativeAuthenticator::logout` in [auth.rs](../../crates/connetto-client/src/auth.rs) revokes the session server-side and clears the stored refresh token. In the browser, `BrowserAuthenticator::logout` in [auth.rs](../../crates/connetto-web/src/auth.rs) revokes the session through the account's cookie, and the server deletes that cookie in the same response. In the browser nothing durable is left to shred, so the local half clears the account's rows from the plain account record store. Natively the last cleared account also removes the keyring index record, as the [refresh store](#the-refresh-store) describes.

Data teardown. [`connetto_client::teardown::wipe_replica`] in [teardown.rs](../../crates/connetto-client/src/teardown.rs) destroys the replica's key-store record and then deletes everything that key opens, in the order the table below gives. The key goes first. If a delete then fails, what remains is inert ciphertext and the wipe's promise still holds. The reverse order would leave a readable file whenever the delete failed. The browser mirror is `wipe_replica` in [storage.rs](../../crates/connetto-web/src/storage.rs).

Every data teardown primitive removes everything the replica key opens, and every one of those things is named by derivation from the replica, so the primitive needs no second parameter to find it. The tier, the hub's state database and the content store are ciphertext under the replica key. A wipe that left any of them behind would hold quota that no sweep can reclaim, because the manifests naming the chunks went with the replica. The one exception is the browser content namespace, which is derived from the replica name and the deployment's seed and so cannot be recomputed by the primitive. The pending-wipe record carries the resolved value.

| Removed | Native `purge_replica`, and through it `wipe_replica` and `forget_device` | Browser `wipe_replica`, run by the next boot from the pending record |
|---|---|---|
| Key-store record | `wipe_replica` and `forget_device` only, `purge_replica` keeps it | Yes, the record named by the replica |
| Replica | `db_path` | The pool entry named by the replica |
| WAL and SHM | `db_path-wal`, `db_path-shm` | Not applicable, the VFS pool holds one entry |
| Device-private tier | `db_path-tier` with its own WAL and SHM, named by `with_tier(ddl)` on an encrypted file replica and by `with_existing_tier()`, and never a caller-chosen path | `tier_db_name(replica)` |
| Relay hub state | Not applicable, the hub is the browser's | `hub_meta_db_name(replica)` |
| Content | The directory `db_path-content`, named by `content_dir(db_path)` in [teardown.rs](../../crates/connetto-client/src/teardown.rs) | The namespace the pending record carries, passed as `Option<&str>`, `None` for a deployment without content |

Order is key, tier, hub state, replica, content in the browser, and key, tier, replica, content natively. Every removal treats absence as success, so a retry after a partial failure is the same call again. Native stops at the first failing delete and reports it, and the retry is `purge_replica` with `force`, which starts the list over. `purge_replica` removes the content directory too, although it keeps the key, because a fresh replica has empty manifest tables and every chunk under the old directory is an orphan. The browser namespace is per account by construction, `sha256(seed, replica_db_name)`, so a wipe of one identity can never reach another's chunks, and an anonymous boot opens no namespace. A content path parameter on `wipe_replica` and `forget_device` is rejected, because two places would have to hold the same value, and leaving native content teardown to the application narrows the [18-file-handling.md](18-file-handling.md) claim that crypto-shredding covers content exactly as it covers rows to shredding alone.

Every destructive primitive blocks on pending local work unless `force` is set. In the browser that guard belongs at the mark, `mark_wipe_pending`, because the connection is still live there and gone by the boot that performs the wipe, so a guard at the wipe would check nothing. The browser guard combines unsynced mutation sequence numbers with the content upload outbox, because acknowledged rows can still have bytes waiting to upload. `purge_replica` in [teardown.rs](../../crates/connetto-client/src/teardown.rs) deletes the file without destroying the key and carries the mutation guard.

[`connetto_client::NativeDurable`] `forget_device` runs both destructive axes under one guard. The unsynced check runs before the credential is destroyed, because once the refresh token is gone the queued writes can no longer be uploaded. A revoke that never reached the server does not abort the data wipe, and a wipe that fails does not keep the credential or the session, so each failure is reported by itself in `ForgetIncomplete`. The browser has no `forget_device`, because its data teardown can only run at the next boot, when the worker that held the connection is gone. Its logout with delete marks the wipe and the next boot finishes it.

The shared store goes with the last identity out. It is ciphertext under its own device key rather than the replica's, so no replica wipe removes it. `forget_device`, and the browser's logout with delete, remove its key record and then its database once no identity has interest or unsent writes in it, natively right after the replica's wipe and in the browser at the next boot before anything opens. A shared file found natively without its key is deleted and a fresh one minted, since nothing could open it again. [12-identity-session-capability.md](12-identity-session-capability.md#public-tables-may-be-shared-across-identities) records the departure that comes first.

A purge that clears the key is unrecoverable. If the file delete then fails, `ReplicaUndecryptable` is reported on the next connect. The recovery is `purge_replica` with `force`, which deletes the now-unreadable file so the next connect can first-boot a fresh one. There is no unsynced guard on that recovery. The pending mutations lived inside the file the key will not open, so they are already lost.

A locked file is never reported as a wrong key. The key probe at open waits up to five seconds for another connection's lock, the case of a client reopened in the same process while its predecessor's last handle is still closing. A lock that outlasts the wait surfaces as `ReplicaBusy`, which a later open clears. Only a read that fails without a lock surfaces as `ReplicaUndecryptable`. In the browser the probe cannot block a worker, so a lock reports `ReplicaBusy` at once.

The browser's replica connection lives inside the relay hub's pump for the worker's whole lifetime, and a wipe cannot run while the connection is live. `mark_wipe_pending` in [storage.rs](../../crates/connetto-web/src/storage.rs) records the replica and its exact persistent content namespace in `connetto-pending-wipes`, and the next worker `boot()` runs `wipe_replica` with that namespace. If the namespace removal then fails, the marker atomically becomes content-only, so later boots retry the orphaned encrypted namespace without deleting a replacement replica. An invalid namespace remains fatal, because selecting fallback storage for a malformed identity is unsafe. The acknowledgment and the content-only transition compare the whole observed record in one read-write transaction, so neither can overwrite a replacement request. The namespace is recorded whenever the boot is identified, whatever store the boot ended up with, so a wipe requested during a session whose `OPFS` was refused still finds the directory an earlier session filled.

## Threat model boundary

[12-identity-session-capability.md](12-identity-session-capability.md#what-at-rest-encryption-does-not-cover) is the canonical statement of what at-rest encryption does not cover, and this chapter does not repeat it.

What it defends, without overclaiming.

Crypto-shredding on logout is the primary job, and it works. Deleting a database file does not erase its contents. The write-ahead log, the journal, free pages, wear-levelling, filesystem snapshots and every backup already taken keep readable copies. Destroying the 32-byte key invalidates all of them at once, including copies nobody controls any more. That is what makes logging out mean something on a device the deployment does not own afterwards, and there is no cheaper mechanism for it.

Copies of the file that leave the device are defended, with backups the case that matters most. Full-disk encryption does not reach them, because a backup is decrypted before it is uploaded. A recovered disk or a discarded drive is the same class.

| Not defended | Why |
|---|---|
| A stolen powered-off device | Current iOS, Android, macOS and Windows encrypt the whole disk by default, so the marginal protection over the platform is small |
| Accounts under one OS login | Every replica key sits in that login's keychain, or in the browser profile's origin storage, so any account running under the login can load any other account's key by name. Separation between different people is the operating system's user boundary ([12-identity-session-capability.md](12-identity-session-capability.md#what-at-rest-encryption-does-not-cover)). The shared public store's device key sits in the same store, so it draws the same boundary |
| An attacker already resident in the process | It can drive the open connection and read decrypted pages regardless of how they were stored |

The stolen-profile case is what the gate exists to close, and for an enrolled profile the claim is made. A key derived from a passkey is not in the profile at all, because the credential and its secret live outside any single browser profile. For an unenrolled profile the stored key is still in the profile, and the claim does not extend there.
