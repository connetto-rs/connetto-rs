# 20: Deployment, backup and restore

**Status**: normative. Paragraphs marked **Built.** describe what exists today. Every normative statement is marked **Decided**, naming its phase in `plans/master-implementation-plan.md`. R70 owns backup and restore and R73 owns failover, and both write here because a standby promoted behind on lag and a server restored to an earlier point put clients ahead of the server in the same way.

---

## What a deployment holds

**Built.** A deployment holds state in three places, which are its Postgres cluster, the stores beside it, and key files. The server binary emits no DDL, so every table below is created by the deployment's migration, and the table says what the server does at startup when one is missing.

**Built (R98, 2026-10-04).** Every table connetto reads through a trait is a member of one `ConnettoSchema`, and the startup check names each member's tables from the `TABLES` the member reports, so a custom member is checked by its own names. Only names are checked, never columns.

| Artifact | Named by | In a Postgres backup | At startup when missing |
|---|---|---|---|
| Application tables | `CONNETTO_PG_DDL` | Yes | The deployment's own concern |
| `connetto_sessions`, `connetto_provider_tokens` | `ConnettoSchema::Auth`, emitted by `connetto_schema!` | Yes | Refused by `preflight::require` (`Artifact::Table`) |
| `_connetto_writers`, `_connetto_settled` and `_connetto_session_writers`, the per-writer applied frontier (R75) and each session's binds (R105) | `ConnettoSchema::Writers`, emitted by `connetto_schema!` | Yes | Refused by `preflight::require` (`Artifact::Table`) |
| `connetto_bans`, only under `CONNETTO_BANS=database` | `ConnettoSchema::Bans` | Yes | Refused by `preflight::require` (`Artifact::Table`) when bans are on |
| The audit table `auth_events` | `ConnettoSchema::Audit` | Yes | Refused by `preflight::require` (`Artifact::Table`), always (R105) |
| `connetto_device_enrolments`, `connetto_device_certificates`, `connetto_device_lists`, only with device identity (R74) | `ConnettoSchema::Enrolments` | Yes | Refused by `preflight::require` (`Artifact::Table`) when device identity is on |
| The bot registry, `connetto_registration_codes`, `connetto_bots`, `connetto_bot_lists`, `connetto_installations`, `connetto_installation_optional_tables` (R91) | `ConnettoSchema::Bots`. The build appends their DDL to the schema document it translates, beside the cap rules, so every replica carries them. The deployment's migration creates them, gives the reader role `SELECT` on them, sets `REPLICA IDENTITY FULL` and adds them to the publication, as for its own synced tables | Yes | Refused by `preflight::require` (`Artifact::Table`, then `Artifact::CappedTable` and `Artifact::PublishedTable`), and a server whose catalogs number a table differently refuses startup (`BuildError::CatalogMismatch`) |
| The generated cap document, the four `AS RESTRICTIVE` bot cap policies per synced and registry table (R91) | Generated from `CONNETTO_PG_DDL` and `CONNETTO_PG_POLICIES` by the build step (`connetto_schema::cap_policies`) and applied by the deployment's migration | Yes | Refused by `require_cap_policies` (`Artifact::CappedTable` per table, naming the missing rule). A session's value in `app.bot_list` carries a bot's declared tables and verbs, and the `*` wildcard for a person |
| The reconnect log, `connetto_oplog`, its last commit, `connetto_oplog_commit`, and its transaction index, `connetto_oplog_xids` | `CONNETTO_OPLOG_TABLE`, with `_commit` and `_xids` appended for the other two | Yes | All three refused by `preflight::require` (`Artifact::Table`), and a log table without the compressed patch or a commit table without `xid_horizon` is refused naming the absent column (`Artifact::TableColumn`) |
| `connetto_epoch`, the cluster the deployment last served from | `connetto_server::epoch::EPOCH_DDL` | Yes, which is what lets a restore into another cluster be seen | Refused by `preflight::require` (`Artifact::Table`) |
| The publication and its previous images | `CONNETTO_PUBLICATION` | Yes | Refused (`Artifact::Publication`, `Artifact::PreviousImages`, and `Artifact::PublishedTable` for every table a policy reads) |
| The logical replication slot | `CONNETTO_SLOT` | **No**, under every method | Refused (`Artifact::ReplicationSlot`) |
| The file server's `_cfs_*` tables and functions | `connetto_file_server::DEPLOYMENT_DDL` | Yes | Refused by `connetto_file_server::preflight` |
| The chunk store | `CONNETTO_CONTENT_STORE`, a `fs:` directory or an `object_store` URL | **No** | Opened by `open_store`, with nothing checked against the manifests |
| The OpenFGA store | `CONNETTO_FGA_STORE` | Only when OpenFGA's own datastore lives in the same cluster | Refused without the id. A new model loads every fact from Postgres (`ModelState::Written`), an installed one reconciles the whole store against Postgres, writing only the difference (`ModelState::Adopted`) |
| The token signing key pair | `CONNETTO_JWT_PRIVATE_KEY_FILE`, `CONNETTO_JWT_PUBLIC_KEY_FILE` | **No** | Refused. The deployment keeps the pair on disk so a token survives a restart |
| The content ticket key | `CONNETTO_CONTENT_KEY` | **No** | Refused when `CONNETTO_CONTENT_URL` is set, which is what makes the key required. It is the deployment's own and survives a restart |
| Device attestation (R74) | `CONNETTO_DEVICE_ACCEPTED_ATTESTATION`, the levels accepted, all three by default, `CONNETTO_DEVICE_APP_ATTEST_APP_IDS` and `CONNETTO_DEVICE_APP_ATTEST_ENVIRONMENT` for Apple's App Attest, and `CONNETTO_DEVICE_ANDROID_STATUS`, a URL or file standing in for Google's attestation status list | **No** | Each refuses startup without `CONNETTO_DEVICE_ISSUER_DIR`. A status list that cannot be fetched is logged, and Android devices record `unproven` until a copy arrives |
| The device certificate issuer, its key and the root it chains to (R74) | `CONNETTO_DEVICE_ISSUER_DIR`, the directory `connetto-ca issuer` wrote, and `CONNETTO_DEVICE_ROOT`, its `root.der`, with `CONNETTO_DEVICE_RETIRED_ISSUER_DIRS` and `CONNETTO_DEVICE_ROOT_LIST` for rotated and revoked issuers | **No** | No issuer directory means no device identity, and any other `CONNETTO_DEVICE_*` setting without it refuses startup. An issuer the root did not sign or an expired issuer refuses startup, and one with under sixty days left is logged |
| The device root key and the root's revocation list (R74) | `root.key.p8e` and `root-list.der` in the operator's `connetto-ca` directory, the key encrypted under a passphrase | **No**, and never on the server | Not read by the server, which ships the root certificate in the applications |
| Provider client secrets | `CONNETTO_OIDC_<PROVIDER>_CLIENT_SECRET` | **No** | Re-issued by the provider |

**The slot is recreated after every restore, whatever the method.** A physical base backup omits `pg_replslot`, and a logical dump carries no slots. A recreated slot starts at the restored cluster's current write position.

**A Postgres backup restores every file manifest and no chunk bytes** (`18-file-handling.md`), so the chunk store is backed up separately, and the next section shows what happens where the two disagree.

---

## What a restore does to connected clients

**Built for rows, logins, authorization and the chunk store.** The row and login facts below are asserted by the Docker-gated tests in `crates/connetto-server/tests/it/restore.rs`, green on 2026-09-23, and the authorization store's by `an_adopted_boot_reconciles_a_store_left_at_another_point` in `crates/connetto-server/tests/it/openfga_live.rs`. In each run a client synced three rows, the deployment took a backup, two more rows reached the client, the database was restored, and one new row was written on the restored server. The same-cluster run drops the slot by hand before restoring, so it covers recreating the slot and not a slot left in place across the restore.

| What a restore rewinds | Point-in-time, onto a new timeline | Dump into a fresh cluster | Dump into the same cluster |
|---|---|---|---|
| A client ahead of the restored server | Resyncs and matches the server | Resyncs and matches the server | Resyncs and matches the server |
| A refresh token from before the restore, rotated or current | Refused with `401` | Refused with `401` | Refused with `401` |
| The restored cluster's system identifier | The backed-up one's | Another | The backed-up one's |
| The replication slot | Absent, so the operator recreates it | Absent | Dropped before the restore and recreated |

**Why each method converges, inferred from the code rather than read from the server's log.** A client's cursor is a write position, the timeline it was issued on and the cluster's system identifier. Within one cluster write positions only grow, so after a same-cluster restore the recreated slot resumes past the rewound reconnect log's end, which is the case the server's resume check (`SessionManager::reconcile_stream`, proven by `stream_gap.rs`) answers by forcing every returning client to resync. A point-in-time restore promotes onto a new timeline that branched below the client's cursor, so R73's timeline check resyncs the client. A dump into a fresh cluster starts again at timeline 1 with the restored write positions below the client's cursor, where the log alone would answer `Catchup` and skip a new row written behind the cursor, so the identifier it carries, which the restored cluster does not share, resyncs it.

**A rewound auth store would revive rotated refresh tokens**, under every method, because `connetto_sessions` holds the hash that was current at the backup, so a token rotated away after the backup would refresh again. **Built (R70).** The server therefore revokes every session whenever the cluster differs from the one `connetto_epoch` recorded, and at boot whenever the slot resumes past the reconnect log, before the listener opens, and each device logs in again. A running server whose feed reconnects to another cluster revokes at once. The recorded cluster is rewritten and the reconnect log trimmed only after the revocation succeeds, so a revocation that fails meets the same restore at the next try. A failover keeps its primary's identifier and its synced slot, so it logs nobody out.

**Built (R70, 2026-10-06): the OpenFGA store follows the database at every boot.** A boot that finds its rules already installed reconciles the whole store against Postgres through `Translated::reconcile_store`, every keyed fact and every whole-shape region, writing only the difference. It reads the facts on the owner pool, since the reader role's view is narrowed by the bot cap and the deployment's own policies (`08-authorization.md`). A membership granted after the backup and removed by the restore is deleted from the store before the server serves, and a store restored on its own comes back equal to the database. The cost is one read of the store and of the facts on every boot.

**A restore leaves the database and the chunk store disagreeing two ways.** A restored manifest can name chunks the sweep removed after the backup, and a chunk uploaded after the backup is named by no registry row, which the sweep, starting from registry rows, never reaches.

**Built (R70, 2026-09-22) for the chunk store.** A boot pass before the listener opens deletes stored chunks no registry row names once past the grace window, and marks lost every manifest naming a chunk whose bytes are gone, telling the application `lost`, so a read refuses before it streams. A device still holding the file uploads it again, restoring it under its original uploaders. A lost file counts toward no uploader's quota until it heals, and its heal meets the deployment ceilings like any upload, because the store no longer holds those bytes. `18-file-handling.md` states the mechanism, and `crates/connetto-file-server/tests/it/restore.rs`, the native `offline_photo.rs` and the browser `examples/wasm-smoke/tests/photo_heal.rs` prove it.

**Built (R73) for the timeline.** A cursor carries the timeline it was issued on beside its write position. One whose position lies beyond where its timeline ended in the current history takes a full resync, and one at or below that point resumes, so a failover inside the replicated window resyncs nobody (`crates/connetto-server/src/timeline.rs`, `06-reconnect.md`).

**Built (R70) for the cluster.** A cursor also carries the cluster's system identifier, read from the `IDENTIFY_SYSTEM` the timeline read already sends, and one from another cluster takes a full resync with `CursorBeyondHistory`, because a database restored into another cluster starts again at timeline 1 and the timeline check does not see it.

**Built (R70, 2026-10-27) for writers.** A restore holds each writer at the frontier of the backup and rewinds the writer number sequence with the rows. The server's frontier never decreases and no code deletes a writer row, so a client that meets a frontier below what it saw settled, at the handshake, at a device bind or in a live `SequenceGap`, or meets `WriterUnknown` for a number it holds, reads it as a restore. It moves that writer's pending entries to a fresh writer without provenance naming the abandoned one, a number taking a new number at the next handshake and a device retiring its key and enrolling a new one. Each number carries a secret only its replica holds (`11-authentication.md`), so a number the restored server issues again to another replica never binds the one that held it before. Writes the server acknowledged after the backup are lost on both sides, and R78's courier is their recovery. `a_restore_moves_rewound_writers_to_fresh_numbers` in `restore.rs` proves it with a same-cluster dump restore.

---

## Changing the schema under a running server

**Decided (R107).** The deployment moves a major in five ordered steps, each of which leaves the old major serving until its own step ends it. The order is what makes every step safe to run while live clients are still attached.

**1. Run the expand**, while the old major's server is on. It adds only what old clients and servers can ignore, a new table, index or constraint the old major never reads, a new nullable column or one with a default so old clients' inserts still succeed, and it relaxes an old `NOT NULL` column without a default so the new major's inserts succeed. A renamed or retyped column keeps its successor in step with an application-owned trigger the expand adds, copying between the two, because connetto applies no server DDL.

**2. Start the new major's server.** Each major's server names its own `CONNETTO_OPLOG_TABLE`, `CONNETTO_SLOT` and `CONNETTO_PUBLICATION`, one set per major. The slot name is unique across the whole cluster (`10-subscription-materializer.md`, the replication slot), which is what makes two majors on one Postgres need two names, and the publication and the oplog table are per-database. The new server is built from the contracted schema it will end at, so it never needs a rebuild and its clients see no second schema change.

**3. Set the old major's sunset** on its servers, `CONNETTO_SUNSET`, an RFC 3339 instant in UTC. Every old client learns it from the acknowledgement and the application prompts an update.

**4. Turn the old major's servers off and drop their stream artifacts**, once its clients are drained. The slot is dropped by SQL, the one artifact no backup holds, and the publication and the oplog tables by the deployment's migration, which owns them the way it owns the slot. Old clients that return after the sunset draw the refusal and stop, and a client that was live at the instant was closed with the same reason.

**5. Run the contract**, the drops, renames and type changes, and the removal of the expand's trigger. Nothing the old major served is changed before this step, and the new major's server never reads what the contract removes.

---

## Erasing a person

**Decided (R61 question 2, step 6 and decisions 18 to 22).** connetto has no erase function. A function erasing over a list of queries could not know the list is complete, and an incomplete erasure fails silently while its name claims success. The deployment erases a person with this recipe instead, walking the inventory above, and records that it did. The recipe reaches further than the download, which carries the registered queries only.

**1. Sign the person out.** **Built (R61, 2026-10-25).** `ServerHandle::end_person(person)` revokes every live session of the person, closing each live connection with `SessionRevoked`, and revokes every device key the person enrolled, which publishes the revocation and closes that device's connection. Deleting the rows alone ends nothing already connected, since a live connection streams until its next handshake is refused and a device key stays valid until its certificate expires.

**2. Delete the application's rows** with ordinary SQL, every table the application holds about the person. Each delete reaches every device holding the row. A device offline at the time receives it from the reconnect log when it returns inside the log's window, and resyncs when it returns after, so no device keeps a copy. This covers server-mediated sync, and the peer copies of `R77` and `R78` follow the same rule once those phases land. The person's own devices are wiped by the teardown calls, `purge_replica`, `wipe_replica` and `forget_device`. Rows a bot wrote under the person's grant are the application's to classify as the person's or the bot owner's.

**3. Delete connetto's rows about the person**, children before the rows they reference.

| Table | What goes |
|---|---|
| `connetto_provider_tokens`, then `connetto_sessions` | The provider tokens of the person's sessions, then every session whose `user_id` is the person. Revoking the grant at the identity provider itself is the application's step, since connetto holds only its copies |
| `connetto_device_certificates`, then `connetto_device_enrolments` | The certificates of the person's enrolments, then every enrolment whose `user_id` is the person, with the application-defined descriptor columns it carries |
| `connetto_bans` | The person's ban. Once the identity is gone it stops nothing (decision 19) |
| The bot registry | For each bot the person owns (`connetto_bots.owner`), its installations' `connetto_installation_optional_tables` rows, its `connetto_installations`, `connetto_bot_lists` and `connetto_registration_codes` rows, then the bot. Then the codes the person created (`creator`) and the installations serving the person (`connetto_installations.owner`, optional tables first). Another person's bot the person installed stays (decision 21) |
| `_cfs_manifests` | The manifests whose `uploaded_by` is the person's id, or a share key the application's own tables bind to the person's account. `_cfs_manifest_chunks` follows by cascade. connetto binds no share key to an account, so an upload under a key bound to no account belongs to the space that key opens and goes when that space's rows go (decision 22) |

**The file bytes go at the next sweep pass.** The sweep reclaims a chunk once no manifest names it and the grace window has passed (`CONNETTO_CONTENT_SWEEP_SECS`, default an hour, and `CONNETTO_CONTENT_SWEEP_GRACE_SECS`, default the upload ticket lifetime, `18-file-handling.md`). A chunk another uploader's manifest also names stays, because it is that uploader's file too.

**Three stores keep something of the person, and the recipe says so rather than promising them away.** `_connetto_writers` and `_connetto_settled` are never deleted with SQL, because they hold the exactly-once frontier and a writer whose row is gone reads as new, so its replayed uploads would apply twice. A device writer's settled rows go through the writer sweep once its enrolment is revoked or gone, and a writer row stays, naming the person when it was the person's number writer. The reconnect log holds row images, the person's deletes among them, until they leave its retention window, at most 72 hours or a million entries by default (`06-reconnect.md`). The audit table keeps the rows naming the person, sign-ins ended and downloads among them, because they are the evidence a dispute about the person's requests turns on, which GDPR Art. 17(3)(e) allows. The deployment sets its own retention for them (decision 18).

**4. Record the erasure.** `ServerHandle::record_erasure(person, requester)` writes the `data_erased` row once the deletes are done, naming the session that asked or the staff member who acted (`08-authorization.md`). It waits for the row and fails when it cannot be written.

**5. Outside the database.** A backup taken before the erasure holds the person's rows and manifests until its own retention ends, so a restore is followed by this recipe again for every person erased since that backup, and the chunk store's backup the same. The application tells every recipient it disclosed the person's data to, as GDPR Art. 17(2) and Art. 19 require.

**The download's queries can be narrowed in time the same way.** A registered query's predicate can express a window such as CCPA's twelve months, so a deployment answering a request bounded in time registers the bound in the query.

---

## Running the server

**Built (R96, 2026-10-04).** The binary is a translation from its environment into one `ServerBuilder`, and the builder is the one assembly path. The binary reads every setting, binds the one listener and serves through the builder's own lifecycle, and owns what a process owns and a library must not own, the logging, the shutdown signal and the exit code. `CONNETTO_BIND` (default `127.0.0.1:8080`) names the one listener that serves the `/sync` WebSocket route, the login endpoints and the file routes. `CONNETTO_AUTH` accepts only `database`, the persisted JWT key files are required so a token survives a restart, and `CONNETTO_CONTENT_KEY` is required when `CONNETTO_CONTENT_URL` is set. The process exits `1` when the change stream cannot answer what a row looked like before it changed, or gives up reconnecting.

**Built (R96, 2026-10-04).** A program embeds the same builder. `ServerBuilder::build` returns the parts, the merged router, the sync routes and the HTTP routes separately, the change-stream future and the shutdown handle, and a program that embeds the server mounts them into its own application. `ServerBuilder::serve` binds them and runs the serving lifecycle without ever ending the process, the change-stream future closes every session when a terminal outcome arrives, and the shutdown signal closes every session with `ServerShuttingDown` on its own drain. `crates/connetto-server/examples/embed.rs` builds the parts and mounts them beside a route of its own, so the application serves the sync, login and file routes from its own listener.

**Built (R98, 2026-10-04). The builder takes the deployment's schema as its one type parameter.** `ServerBuilder` defaults to `ConnettoDefaults`, the tables `connetto_schema!` emits under the names this chapter lists, with a `String` user id and `DefaultUuidResolver` mapping each login to it. `ServerBuilder::deployment_schema::<D>(resolver)` serves over a deployment's own `ConnettoSchema` instead, with any user id type, and takes the `IdentityResolver` typed by that id at the same call, so a schema switch without one does not compile. `identity_resolver` replaces the resolver without switching schema, which is how a deployment maps logins into its own users table. `crates/connetto-server/tests/it/deployment_schema.rs` serves a deployment with its own watermark table and a `uuid::Uuid` user id.

**Built (R107).** `CONNETTO_SUNSET` (unset) names the application schema major's sunset, an RFC 3339 instant in UTC. The server sends the instant in seconds on every handshake acknowledgement, so the clients of the major learn it and the application prompts an update. Once the instant is reached the server refuses every new handshake of the major with the update-required reason and closes every live connection with the same reason, checked against the deployment's own disciplined clock (`21-clocks.md`), so no skew can arise. The value must be an RFC 3339 instant in UTC at or after the Unix epoch, or the process refuses to start and names it.
