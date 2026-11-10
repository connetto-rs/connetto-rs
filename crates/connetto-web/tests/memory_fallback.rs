//! The in-memory storage a browser that refuses OPFS boots on.
//!
//! Runs on the page's main thread, where OPFS hands out no synchronous access
//! handles, so the sahpool install fails as it does in a Firefox private window
//! and `ReplicaStorage` falls back to the in-memory VFS.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use connetto_client::cipher::{ReplicaKey, unlock};
use connetto_web::storage::ReplicaStorage;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_wasm_rs::vfs::transfer::DbTransfer as _;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// A string written into the replica that must never appear in its bytes.
const MARKER: &str = "connetto-memory-canary-0c6e2b41";

fn key() -> ReplicaKey {
    ReplicaKey::from_bytes([0x5a; ReplicaKey::LEN])
}

/// `name` through the storage's own URL, keyed with [`key`].
fn open(storage: &ReplicaStorage, name: &str) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(&storage.db_url(name)).expect("open the database");
    unlock(&mut conn, &key()).expect("apply the key");
    conn
}

#[wasm_bindgen_test]
async fn the_in_memory_fallback_keeps_an_encrypted_replica_and_wipes_it() {
    let storage = ReplicaStorage::install().await.expect("install storage");
    assert_eq!(
        storage.vfs_name(),
        "memvfs",
        "the main thread has no OPFS sync access handles"
    );
    let name = "fallback-replica.sqlite";
    storage.delete_db(name).expect("clear an earlier file");

    open(&storage, name)
        .batch_execute(&format!(
            "CREATE TABLE canary (id INTEGER PRIMARY KEY, note TEXT NOT NULL);
             INSERT INTO canary VALUES (1, '{MARKER}');"
        ))
        .expect("write the replica");

    assert!(storage.exists(name), "the replica is in the in-memory VFS");
    assert!(
        storage
            .list()
            .expect("list the in-memory VFS")
            .iter()
            .any(|entry| entry == name),
        "and listed by name"
    );
    let ReplicaStorage::Memory(util) = &storage else {
        unreachable!("the storage reported the in-memory VFS");
    };
    let bytes = util.export_db(name).expect("export the in-memory bytes");
    assert!(
        !bytes.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()),
        "the fallback stays ciphertext"
    );
    open(&storage, name)
        .batch_execute("SELECT note FROM canary;")
        .expect("reopen the replica under its key");

    storage.delete_db(name).expect("wipe the replica");
    assert!(!storage.exists(name), "the wipe removes the replica");
    assert!(
        !storage
            .list()
            .expect("list the in-memory VFS")
            .iter()
            .any(|entry| entry == name),
        "and its name from the listing"
    );
}
