//! Browser acceptance for phase E2: the OPFS replica and the device-local tier
//! are both ciphertext at rest, under the key phase E1 resolves.
//!
//! Runs in a dedicated worker, because the OPFS sahpool VFS needs synchronous
//! access handles and only a worker has them. That is also the context the real
//! DB worker runs in, so this exercises the same stack `boot_db_worker` does.
//!
//! Every assertion reads real bytes back out of OPFS through the pool's own
//! export, not a pragma result. The codec is `SQLCipher`, compiled into the
//! SQLite pager, and every database opens through the URL
//! `ReplicaStorage::db_url` hands the connection.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use connetto_client::cipher::{ReplicaKey, UnlockError, unlock};
use connetto_web::storage::ReplicaStorage;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_wasm_rs::vfs::transfer::DbTransfer as _;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A string written into the database that must never appear in the file bytes.
const MARKER: &str = "connetto-plaintext-canary-a7f31c0e";

/// The file magic a plaintext SQLite database starts with. An encrypted file
/// opens with the 16-byte per-database salt instead.
const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";

diesel::table! {
    /// Table with a marker string to verify encryption at rest
    canary (id) {
        /// Row identifier, the primary key
        id -> Integer,
        /// Marker string that must never appear unencrypted
        note -> Text,
    }
}

fn key() -> ReplicaKey {
    ReplicaKey::from_bytes([0x5a; ReplicaKey::LEN])
}

fn other_key() -> ReplicaKey {
    ReplicaKey::from_bytes([0xa5; ReplicaKey::LEN])
}

/// Install the sahpool VFS the way the DB worker does, refusing the in-memory
/// fallback, since these assertions are about bytes in OPFS.
///
/// `install` registers once per worker: a second call returns another handle
/// over the existing registration.
async fn storage() -> ReplicaStorage {
    let storage = ReplicaStorage::install().await.expect("install storage");
    assert_eq!(
        storage.vfs_name(),
        "opfs-sahpool",
        "the suite needs the real OPFS pool, not the in-memory fallback"
    );
    storage
}

/// The bytes OPFS holds for `name`, read through the pool's own export.
fn file_bytes(storage: &ReplicaStorage, name: &str) -> Vec<u8> {
    let ReplicaStorage::Opfs(util) = storage else {
        unreachable!("`storage` refuses the in-memory fallback");
    };
    util.export_db(name).expect("export the OPFS file bytes")
}

/// Open `name` in OPFS under `key`, exactly as `ReplicaStorage::db_url` plus the
/// worker's own unlock do.
fn open(storage: &ReplicaStorage, name: &str, key: Option<&ReplicaKey>) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(&storage.db_url(name)).expect("open the database");
    if let Some(key) = key {
        unlock(&mut conn, key).expect("apply the key");
    }
    conn
}

fn write_marker(conn: &mut SqliteConnection) {
    conn.batch_execute("CREATE TABLE canary (id INTEGER PRIMARY KEY, note TEXT NOT NULL);")
        .expect("create the canary table");
    diesel::insert_into(canary::table)
        .values((canary::id.eq(1), canary::note.eq(MARKER)))
        .execute(conn)
        .expect("insert the canary row");
}

/// Whether `haystack` contains `needle` as a contiguous byte run.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.len() <= haystack.len() && haystack.windows(needle.len()).any(|w| w == needle)
}

#[wasm_bindgen_test]
async fn an_encrypted_opfs_replica_round_trips_and_is_ciphertext_at_rest() {
    let storage = storage().await;
    let name = "e2-encrypted.sqlite";
    storage.delete_db(name).expect("clear an earlier file");
    let cipher = Some(key());

    write_marker(&mut open(&storage, name, cipher.as_ref()));

    let bytes = file_bytes(&storage, name);
    assert!(
        !bytes.starts_with(SQLITE_MAGIC),
        "an encrypted database opens with the key salt, not the SQLite magic"
    );
    assert!(
        !contains(&bytes, MARKER.as_bytes()),
        "the row value must not survive as plaintext in OPFS"
    );
    assert!(
        !contains(&bytes, b"CREATE TABLE canary"),
        "the schema text must not survive as plaintext either"
    );

    // A fresh connection with the cached key, which is the cold start a worker
    // restart or a leader failover performs.
    let note: String = canary::table
        .select(canary::note)
        .first(&mut open(&storage, name, cipher.as_ref()))
        .expect("read the canary row back");
    assert_eq!(note, MARKER);
}

#[wasm_bindgen_test]
async fn a_plaintext_opfs_replica_leaves_everything_readable() {
    // The control that makes the assertions above mean something: the same write
    // through a plaintext database keeps both the magic and the row on disk.
    let storage = storage().await;
    let name = "e2-plaintext.sqlite";
    storage.delete_db(name).expect("clear an earlier file");

    write_marker(&mut open(&storage, name, None));

    let bytes = file_bytes(&storage, name);
    assert!(bytes.starts_with(SQLITE_MAGIC));
    assert!(contains(&bytes, MARKER.as_bytes()));
    assert!(contains(&bytes, b"CREATE TABLE canary"));
}

#[wasm_bindgen_test]
async fn a_process_without_the_key_cannot_read_the_opfs_replica() {
    let storage = storage().await;
    let name = "e2-wrongkey.sqlite";
    storage.delete_db(name).expect("clear an earlier file");

    write_marker(&mut open(&storage, name, Some(&key())));

    // The reachable benign cause is a device whose IndexedDB key store was
    // cleared while its OPFS replica survived, which the next login re-keys.
    //
    // Each open below is scoped so only one connection to this file is ever
    // live: `sqlite-wasm-rs` supports one connection per database, and the
    // sahpool VFS keys its open-file bookkeeping by name, so two at once trip a
    // debug assertion in its `xClose`. connetto never holds two either.
    {
        let mut conn = SqliteConnection::establish(&storage.db_url(name))
            .expect("reopen the encrypted replica");
        let error = unlock(&mut conn, &other_key()).expect_err("a wrong key must not decrypt");
        assert!(
            matches!(error, UnlockError::WrongKey(_)),
            "the wrong key surfaces on the first schema read, got {error:?}"
        );
    }

    // Claiming the replica is plaintext fails too, and not by silently reading
    // garbage: the codec is never keyed, so the first header read fails.
    let mut unkeyed =
        SqliteConnection::establish(&storage.db_url(name)).expect("open the file with no key");
    assert!(
        unkeyed
            .batch_execute("SELECT count(*) FROM sqlite_master;")
            .is_err(),
        "no key at all must not read the replica either"
    );
}

#[wasm_bindgen_test]
async fn the_local_tier_is_encrypted_under_the_same_key_as_its_own_connection() {
    // In the browser the tier is a separate connection whose main schema IS the
    // tier file, so it carries its own key salt and is unlocked independently.
    // Native attaches the tier to the replica and inherits instead. Same key
    // either way: one device, one key.
    let storage = storage().await;
    let replica = "e2-tier-replica.sqlite";
    let tier = "e2-tier-frontend.sqlite";
    storage
        .delete_db(replica)
        .expect("clear an earlier replica");
    storage.delete_db(tier).expect("clear an earlier tier");
    let cipher = Some(key());

    write_marker(&mut open(&storage, replica, cipher.as_ref()));
    {
        let mut conn = open(&storage, tier, cipher.as_ref());
        conn.batch_execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) STRICT;")
            .expect("first-boot the tier from DDL");
        conn.batch_execute("INSERT INTO notes VALUES (1, 'connetto-tier-canary-4b90d215');")
            .expect("write into the tier");
    }

    let bytes = file_bytes(&storage, tier);
    assert!(
        !bytes.starts_with(SQLITE_MAGIC) && !contains(&bytes, b"connetto-tier-canary-4b90d215"),
        "the tier file is ciphertext too, which matters most of all: it has no server copy to re-sync from"
    );

    // Two independently salted encrypted files under one key, each reopened on
    // its own, which is the shape the DB worker holds open.
    let note: String = canary::table
        .select(canary::note)
        .first(&mut open(&storage, replica, cipher.as_ref()))
        .expect("reopen the replica");
    assert_eq!(note, MARKER);
    open(&storage, tier, cipher.as_ref())
        .batch_execute("SELECT count(*) FROM notes;")
        .expect("reopen the tier");
}

#[wasm_bindgen_test]
async fn the_sync_access_handle_is_released_when_an_encrypted_connection_drops() {
    let storage = storage().await;
    let name = "e2-handle.sqlite";
    storage.delete_db(name).expect("clear an earlier file");

    write_marker(&mut open(&storage, name, Some(&key())));

    // No await between the drop above and the delete below. If the handle were
    // released asynchronously, OPFS would still hold it and this would fail.
    // It is what an explicit data wipe rests on.
    storage
        .delete_db(name)
        .expect("delete the replica immediately after the connection dropped");
    assert!(
        !storage.exists(name),
        "the file is gone once the handle is released"
    );
}

/// The replica, the tier and the hub state share one key, so only their salts
/// keep their HMAC keys apart and stop a page of one from authenticating in
/// another.
#[wasm_bindgen_test]
async fn files_under_one_key_get_distinct_random_salts() {
    let storage = storage().await;
    let names = ["e2-salt-a.sqlite", "e2-salt-b.sqlite"];
    for name in names {
        storage.delete_db(name).expect("clear an earlier file");
    }
    // Each file and its rollback journal take a slot of the shared pool.
    storage.reserve(4).await.expect("room in the pool");
    for name in names {
        write_marker(&mut open(&storage, name, Some(&key())));
    }

    let [first, second] = names.map(|name| {
        let bytes = file_bytes(&storage, name);
        <[u8; 16]>::try_from(&bytes[..16]).expect("a page holds a 16-byte salt")
    });
    for name in names {
        storage.delete_db(name).expect("remove the file");
    }
    assert_ne!(first, [0; 16], "a new database's salt is random, not zero");
    assert_ne!(
        first, second,
        "two databases under one key get their own salts"
    );
}
