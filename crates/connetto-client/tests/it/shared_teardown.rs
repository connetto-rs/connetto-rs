//! An identity leaving the shared store takes its interest along, evicts what
//! no remaining identity keeps, and takes the store along when it was the last.

use std::path::Path;

use connetto_client::{
    Gate, MemoryRefreshStore, NativeClient, NativeClientBuilder, REPLICA_PREFIX, SharedDeparture,
    shared_store_name,
};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::ReplicaKeyStore;
use diesel::connection::SimpleConnection;

use super::shared_coverage::{connect, ids, schema, snapshot, subscribe};

/// `owner`'s native client over the shared device directory, offline.
async fn native(
    directory: &Path,
    keys: super::support::SharedKeys,
    owner: &str,
) -> NativeClient<FakeTransport> {
    Box::pin(
        NativeClientBuilder::new("ws://127.0.0.1:1/", schema())
            .with_dialer(super::support::NeverDial::<FakeTransport>::default())
            .signed_in(super::support::held(owner))
            .durable(directory, keys, MemoryRefreshStore::default())
            .with_gate(Gate::off())
            .connect(),
    )
    .await
    .expect("the durable client opens offline")
}

#[tokio::test]
async fn leaving_keeps_what_another_identity_covers_and_evicts_the_rest() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags WHERE id = 1").await;
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[1, 2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags").await;
    assert_eq!(
        bob.leave_shared_store(false).unwrap(),
        SharedDeparture::Kept
    );
    assert_eq!(ids(&mut alice), [1], "only the row alice covers stays");
}

#[tokio::test]
async fn the_last_identity_out_empties_the_store() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys, "alice", vec![snapshot("a", &[1])]).await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    assert_eq!(
        alice.leave_shared_store(false).unwrap(),
        SharedDeparture::LastOut
    );
    assert!(ids(&mut alice).is_empty(), "nobody keeps a row");
}

#[tokio::test]
async fn a_forced_departure_undoes_its_unsent_shared_writes() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    let mut bob = connect(directory.path(), keys, "bob", Vec::new()).await;
    bob.conn()
        .batch_execute("INSERT INTO tags VALUES (5, 'never sent')")
        .unwrap();
    assert!(bob.push().await.unwrap().is_some(), "the write is queued");
    assert_eq!(bob.leave_shared_store(true).unwrap(), SharedDeparture::Kept);
    assert_eq!(
        ids(&mut alice),
        [1],
        "the write the server never received leaves the table alice reads whole"
    );
}

#[tokio::test]
async fn forgetting_the_last_identity_removes_the_store_and_its_key() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let record = shared_store_name(REPLICA_PREFIX);
    let store = directory.path().join(&record);
    let client = native(directory.path(), keys.clone(), "alice").await;
    assert!(store.exists(), "the open created the shared store");
    client.forget_device(false).await.expect("the forget runs");
    assert!(!store.exists(), "the store is gone");
    assert!(
        keys.load(&record).await.unwrap().is_none(),
        "and its key record"
    );
}

#[tokio::test]
async fn forgetting_one_of_two_identities_keeps_the_store() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let record = shared_store_name(REPLICA_PREFIX);
    let mut alice = connect(
        directory.path(),
        keys.clone(),
        "alice",
        vec![snapshot("a", &[1])],
    )
    .await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    let bob = native(directory.path(), keys.clone(), "bob").await;
    bob.forget_device(false).await.expect("the forget runs");
    assert!(
        directory.path().join(&record).exists(),
        "alice still uses the store"
    );
    assert!(keys.load(&record).await.unwrap().is_some(), "and its key");
    assert_eq!(ids(&mut alice), [1], "and reads her rows");
}

#[tokio::test]
async fn a_shared_store_left_without_its_key_starts_afresh() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let record = shared_store_name(REPLICA_PREFIX);
    {
        let mut alice = connect(
            directory.path(),
            keys.clone(),
            "alice",
            vec![snapshot("a", &[1])],
        )
        .await;
        subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    }
    keys.clear(&record).await.unwrap();
    let mut bob = connect(directory.path(), keys, "bob", Vec::new()).await;
    assert!(
        ids(&mut bob).is_empty(),
        "the keyless file is gone and a fresh store replaced it"
    );
}

#[tokio::test]
async fn retiring_an_identity_that_never_left_evicts_what_only_it_covered() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    {
        let mut bob = connect(
            directory.path(),
            keys.clone(),
            "bob",
            vec![snapshot("b", &[1, 2])],
        )
        .await;
        subscribe(&mut bob, "b", "SELECT * FROM tags").await;
    }
    let mut alice = connect(directory.path(), keys, "alice", vec![snapshot("a", &[1])]).await;
    subscribe(&mut alice, "a", "SELECT * FROM tags WHERE id = 1").await;
    let bob = super::support::held("bob").replica_name().to_owned();
    alice.retire_shared_identity(&bob).unwrap();
    assert_eq!(ids(&mut alice), [1], "bob's row goes, alice's stays");
}
