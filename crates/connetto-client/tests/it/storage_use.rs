//! What a replica and the shared store take on the device, read in constant
//! time without naming the identities present (R11 slice 7).

use super::holdings::{Session, inserts};
use super::shared_holdings::identity;
use super::support::SharedKeys;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_sharing_nothing_reports_its_replica_alone() {
    let mut s = Session::connected().await;
    let used = s.conn.storage_use().expect("read the storage use");
    assert!(used.shared.is_none(), "{used:?}");
    assert!(used.replica.file > 0, "{used:?}");
    assert!(used.replica.free <= used.replica.file, "{used:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shared_store_counts_the_identities_using_it() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    let identities = |s: &mut Session| {
        s.conn
            .storage_use()
            .expect("read the storage use")
            .shared
            .expect("the build shares tables")
            .identities
    };
    assert_eq!(identities(&mut alice), 1);
    let mut bob = identity(directory.path(), &keys, "bob").await;
    assert_eq!(identities(&mut alice), 2);
    assert_eq!(identities(&mut bob), 2);
    alice.conn.leave_shared_store(false).unwrap();
    assert_eq!(identities(&mut bob), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_identity_that_closed_still_counts() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    alice
        .conn
        .subscribe("a", "SELECT * FROM tags")
        .await
        .unwrap();
    alice.snapshot("a", vec![inserts("tags", &[1, 2])]).await;
    drop(alice.offline().await);
    let mut bob = identity(directory.path(), &keys, "bob").await;
    let used = bob.conn.storage_use().expect("read the storage use");
    assert_eq!(used.shared.expect("shared").identities, 2, "{used:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_rows_take_room_in_the_store_and_not_in_the_replica() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    alice
        .conn
        .subscribe("a", "SELECT * FROM tags")
        .await
        .unwrap();
    let before = alice.conn.storage_use().expect("read the storage use");
    let ids: Vec<i64> = (1..=3000).collect();
    alice.snapshot("a", vec![inserts("tags", &ids)]).await;
    let after = alice.conn.storage_use().expect("read the storage use");
    let shared = |used: &connetto_client::StorageUse| {
        used.shared
            .as_ref()
            .expect("the build shares tables")
            .bytes
            .file
    };
    let store_grew = shared(&after) - shared(&before);
    let replica_grew = after.replica.file.saturating_sub(before.replica.file);
    assert!(
        store_grew >= 100 * 1024,
        "3000 rows and their holdings land in the store, grown {store_grew} bytes"
    );
    assert!(
        replica_grew < store_grew / 10,
        "the replica keeps none of the shared rows, grown {replica_grew} against {store_grew}"
    );
}
