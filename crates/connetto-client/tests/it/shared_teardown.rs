//! An identity leaving the shared store takes its interest along, evicts what
//! no remaining identity keeps, and takes the store along when it was the last.

use std::path::Path;

use connetto_client::teardown::ForgetError;
use connetto_client::{
    ClientError, ConnettoConnection, Gate, MemoryRefreshStore, NativeClient, NativeClientBuilder,
    REPLICA_PREFIX, ReplicaKey, SharedDeparture, WipePart, shared_store_name,
};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::ReplicaKeyStore;
use diesel::RunQueryDsl;
use diesel::connection::SimpleConnection;

use super::shared_coverage::{connect, ids, schema, snapshot, subscribe};

/// Start `owner`'s native client over the shared device directory, offline,
/// finishing any wipe left pending there first.
async fn start<KS>(
    directory: &Path,
    keys: KS,
    owner: &str,
) -> Result<NativeClient<FakeTransport>, ClientError>
where
    KS: ReplicaKeyStore<Error = ClientError> + Clone + Send + Sync + 'static,
{
    Box::pin(
        NativeClientBuilder::new("ws://127.0.0.1:1/", schema())
            .with_dialer(super::support::NeverDial::<FakeTransport>::default())
            .signed_in(super::support::held(owner))
            .durable(directory, keys, MemoryRefreshStore::default())
            .with_gate(Gate::off())
            .connect(),
    )
    .await
}

/// `owner`'s native client over the shared device directory, offline.
async fn native(
    directory: &Path,
    keys: super::support::SharedKeys,
    owner: &str,
) -> NativeClient<FakeTransport> {
    start(directory, keys, owner)
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

/// Mark `owner`'s replica under `directory` as a device wipe left pending, as
/// a revoked device's wipe records before its first part runs.
fn mark_pending_wipe(directory: &Path, owner: &str) -> String {
    let replica = super::support::held(owner).replica_name().to_owned();
    std::fs::write(directory.join(format!("{replica}.wipe-pending")), "")
        .expect("the marker a wipe leaves");
    replica
}

/// A start finishing a revoked identity's wipe takes that identity out of the
/// shared store first, undoing its queued shared writes and evicting what
/// only it covered, while the rows another identity covers stay.
#[tokio::test]
async fn a_start_finishing_a_pending_wipe_takes_that_identity_out_of_the_store() {
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
    {
        let mut bob = connect(
            directory.path(),
            keys.clone(),
            "bob",
            vec![snapshot("b", &[1, 2])],
        )
        .await;
        subscribe(&mut bob, "b", "SELECT * FROM tags").await;
        bob.conn()
            .batch_execute(
                "INSERT INTO tags VALUES (5, 'never sent'); \
                 UPDATE tags SET label = 'never sent' WHERE id = 1",
            )
            .unwrap();
        assert!(bob.push().await.unwrap().is_some(), "the write is queued");
    }
    assert_eq!(ids(&mut alice), [1, 2, 5], "bob's rows are in the store");
    let bob = mark_pending_wipe(directory.path(), "bob");

    let _carol = native(directory.path(), keys, "carol").await;

    assert!(
        !directory.path().join(&bob).exists(),
        "bob's replica is wiped"
    );
    assert_eq!(
        ids(&mut alice),
        [1],
        "bob's unsent insert is undone and the row only he covered goes"
    );
    let label: String = diesel::select(diesel::dsl::sql::<diesel::sql_types::Text>(
        "(SELECT label FROM tags WHERE id = 1)",
    ))
    .get_result(alice.conn())
    .unwrap();
    assert_eq!(
        label, "tag-1",
        "bob's unsent update of alice's row is undone"
    );
}

/// The last identity's pending wipe takes the store and its key along, so the
/// start that finished it opens a fresh store under a fresh key.
#[tokio::test]
async fn a_start_finishing_the_last_identitys_pending_wipe_removes_the_store() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let record = shared_store_name(REPLICA_PREFIX);
    {
        let mut bob = connect(
            directory.path(),
            keys.clone(),
            "bob",
            vec![snapshot("b", &[1])],
        )
        .await;
        subscribe(&mut bob, "b", "SELECT * FROM tags").await;
    }
    let left = keys.load(&record).await.unwrap().expect("the store's key");
    mark_pending_wipe(directory.path(), "bob");

    let _carol = native(directory.path(), keys.clone(), "carol").await;

    let fresh = keys
        .load(&record)
        .await
        .unwrap()
        .expect("the new store's key");
    assert!(
        left != fresh,
        "bob's store went with its key and a new one began"
    );
}

/// A departure the store refuses, whether its key cannot be read, it cannot
/// be opened, it cannot take back a queued write or it cannot drop the
/// identity's rows, leaves the wipe pending, and the start refuses, naming
/// the shared store as the part that failed.
#[tokio::test]
async fn a_pending_wipe_whose_departure_fails_refuses_the_start() {
    for broken in ["key", "file", "tags_rls", "_connetto_spare"] {
        let directory = tempfile::tempdir().unwrap();
        let keys = super::support::SharedKeys::default();
        let record = shared_store_name(REPLICA_PREFIX);
        {
            let mut bob = connect(
                directory.path(),
                keys.clone(),
                "bob",
                vec![snapshot("b", &[1])],
            )
            .await;
            subscribe(&mut bob, "b", "SELECT * FROM tags").await;
            bob.conn()
                .batch_execute("INSERT INTO tags VALUES (5, 'never sent')")
                .unwrap();
            assert!(bob.push().await.unwrap().is_some(), "the write is queued");
            if broken.starts_with('_') || broken == "tags_rls" {
                refuse_deletes(bob.conn(), broken);
            }
        }
        if broken == "file" {
            std::fs::write(directory.path().join(&record), b"not a database").unwrap();
        }
        let bob = mark_pending_wipe(directory.path(), "bob");
        let keys = StoreKeyRefusing {
            keys,
            refused: if broken == "key" { "load" } else { "" },
        };

        match start(directory.path(), keys, "carol").await {
            Err(ClientError::WipePending(report)) => {
                assert_eq!(
                    report.failed(),
                    [WipePart::Shared],
                    "with the store's {broken} broken only the store failed"
                );
            }
            Err(other) => panic!("expected the pending wipe to refuse, got {other}"),
            Ok(_) => panic!("expected the pending wipe to refuse the start"),
        }
        assert!(
            directory
                .path()
                .join(format!("{bob}.wipe-pending"))
                .exists(),
            "with the store's {broken} broken the wipe stays pending"
        );
    }
}

/// A pending wipe whose replica can no longer be unlocked still takes that
/// identity out of the store, with nothing of it left to undo.
#[tokio::test]
async fn a_pending_wipe_without_its_replica_key_still_leaves_the_store() {
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
    let bob = mark_pending_wipe(directory.path(), "bob");
    keys.clear(&bob).await.unwrap();

    let _carol = native(directory.path(), keys, "carol").await;

    assert_eq!(ids(&mut alice), [1], "the row only bob covered goes");
    assert!(
        !directory
            .path()
            .join(format!("{bob}.wipe-pending"))
            .exists(),
        "the wipe is complete"
    );
}

/// A pending wipe finds nothing to leave when the store or its key is already
/// gone, and the start finishes it.
#[tokio::test]
async fn a_pending_wipe_with_no_store_left_completes() {
    for gone in ["file", "key"] {
        let directory = tempfile::tempdir().unwrap();
        let keys = super::support::SharedKeys::default();
        let record = shared_store_name(REPLICA_PREFIX);
        {
            let mut bob = connect(
                directory.path(),
                keys.clone(),
                "bob",
                vec![snapshot("b", &[1])],
            )
            .await;
            subscribe(&mut bob, "b", "SELECT * FROM tags").await;
        }
        if gone == "file" {
            for suffix in ["", "-wal", "-shm"] {
                let path = directory.path().join(format!("{record}{suffix}"));
                if path.exists() {
                    std::fs::remove_file(path).unwrap();
                }
            }
        } else {
            keys.clear(&record).await.unwrap();
        }
        let bob = mark_pending_wipe(directory.path(), "bob");

        let _carol = native(directory.path(), keys, "carol").await;

        assert!(
            !directory
                .path()
                .join(format!("{bob}.wipe-pending"))
                .exists(),
            "with the store's {gone} gone the wipe is complete"
        );
        assert!(
            !directory.path().join(&bob).exists(),
            "with the store's {gone} gone bob's replica is wiped"
        );
    }
}

/// The rows of `table` in the shared store, as `client` reads them.
fn count(client: &mut ConnettoConnection<FakeTransport>, table: &str) -> i64 {
    diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
        "(SELECT count(*) FROM connetto_shared.{table})"
    )))
    .get_result(client.conn())
    .unwrap()
}

/// Make the shared store refuse every delete from `table`, for every
/// connection, since the store's own tables change only with a release.
fn refuse_deletes(db: &mut diesel::SqliteConnection, table: &str) {
    db.batch_execute(&format!(
        "CREATE TRIGGER connetto_shared.refuse_{table} BEFORE DELETE ON {table} \
         BEGIN SELECT RAISE(ABORT, 'the store refuses'); END"
    ))
    .unwrap();
}

/// A departure the store refuses in any part leaves the store as it was,
/// the unsent write it would have undone included.
#[tokio::test]
async fn a_departure_the_store_refuses_leaves_the_store_as_it_was() {
    let tables = ["tags_rls", "_connetto_interest", "_connetto_spare"];
    for refused in tables {
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
        bob.conn()
            .batch_execute("INSERT INTO tags VALUES (5, 'never sent')")
            .unwrap();
        assert!(bob.push().await.unwrap().is_some(), "the write is queued");
        let before: Vec<i64> = tables.iter().map(|t| count(&mut alice, t)).collect();
        refuse_deletes(alice.conn(), refused);

        assert!(
            bob.leave_shared_store(true).is_err(),
            "refusing deletes from {refused} fails the departure"
        );

        let after: Vec<i64> = tables.iter().map(|t| count(&mut alice, t)).collect();
        assert_eq!(
            after, before,
            "refusing deletes from {refused} leaves {tables:?} untouched"
        );
    }
}

/// A forget whose departure fails still wipes the replica, and names the
/// shared store as the part that failed.
#[tokio::test]
async fn a_forget_whose_departure_fails_still_wipes_the_replica() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys.clone(), "alice", Vec::new()).await;
    let bob = native(directory.path(), keys, "bob").await;
    let replica = super::support::held("bob").replica_name().to_owned();
    alice
        .conn()
        .batch_execute("DROP TABLE connetto_shared._connetto_spare")
        .unwrap();

    let Err(ForgetError::Incomplete(report)) = bob.forget_device(false).await else {
        panic!("a forget whose departure fails is incomplete");
    };
    assert_eq!(report.failed(), [WipePart::Shared], "only the store failed");
    assert!(
        !directory.path().join(&replica).exists(),
        "the replica is wiped"
    );
}

/// A key store that refuses one operation, `load` or `clear`, on the shared
/// store's key record, and serves everything else from its keys.
#[derive(Clone, Default)]
struct StoreKeyRefusing {
    keys: super::support::SharedKeys,
    refused: &'static str,
}

impl StoreKeyRefusing {
    fn refuses(&self, op: &str, name: &str) -> Result<(), ClientError> {
        if op == self.refused && name == shared_store_name(REPLICA_PREFIX) {
            return Err(ClientError::Auth(format!("the key store refused the {op}")));
        }
        Ok(())
    }
}

impl ReplicaKeyStore for StoreKeyRefusing {
    type Error = ClientError;

    async fn load(&self, name: &str) -> Result<Option<ReplicaKey>, ClientError> {
        self.refuses("load", name)?;
        self.keys.load(name).await
    }

    async fn store(&self, name: &str, key: &ReplicaKey) -> Result<(), ClientError> {
        self.keys.store(name, key).await
    }

    async fn clear(&self, name: &str) -> Result<(), ClientError> {
        self.refuses("clear", name)?;
        self.keys.clear(name).await
    }

    fn protection(&self) -> connetto_client::Custody {
        self.keys.protection()
    }
}

/// The last identity out whose store key cannot be cleared keeps the store
/// file, since the key goes first, and names the store as the part that
/// failed.
#[tokio::test]
async fn the_last_identity_out_keeps_a_store_whose_key_stays() {
    let directory = tempfile::tempdir().unwrap();
    let keys = StoreKeyRefusing {
        keys: super::support::SharedKeys::default(),
        refused: "clear",
    };
    let record = shared_store_name(REPLICA_PREFIX);
    let alice = start(directory.path(), keys.clone(), "alice")
        .await
        .expect("the durable client opens offline");

    let Err(error @ ForgetError::Incomplete(_)) = alice.forget_device(false).await else {
        panic!("a forget whose store stays is incomplete");
    };
    assert!(
        error
            .to_string()
            .contains("the key store refused the clear"),
        "the message names the cause, got {error}"
    );
    let ForgetError::Incomplete(report) = error else {
        unreachable!("the let else above binds error only as ForgetError::Incomplete");
    };
    assert_eq!(report.failed(), [WipePart::Shared], "only the store failed");
    assert!(directory.path().join(&record).exists(), "the store stays");
    assert!(
        keys.load(&record).await.unwrap().is_some(),
        "and so does its key"
    );
}

/// An identity cannot retire itself, since its own open rewrote its rows.
#[tokio::test]
async fn retiring_the_connections_own_identity_keeps_its_rows() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys, "alice", vec![snapshot("a", &[1])]).await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    let alice_id = super::support::held("alice").replica_name().to_owned();
    alice.retire_shared_identity(&alice_id).unwrap();
    assert_eq!(ids(&mut alice), [1], "alice still covers her row");
}
