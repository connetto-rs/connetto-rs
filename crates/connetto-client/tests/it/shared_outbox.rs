//! A write to a shared table is recorded in the writing identity's outbox and
//! spared in the store inside the transaction that makes it.

use connetto_client::ConnettoConnection;
use connetto_core::messages::{ControlMessage, MutationApplied};
use connetto_core::test_support::FakeTransport;
use diesel::RunQueryDsl;
use diesel::connection::SimpleConnection;

use super::shared_coverage::{connect, ids, open, serve, snapshot, subscribe};

/// `client`'s count of `query`'s single row.
fn count(client: &mut ConnettoConnection<FakeTransport>, query: &str) -> i64 {
    diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
        "({query})"
    )))
    .get_result(client.conn())
    .unwrap()
}

/// A shared write another identity's pass meets before it is pushed stays,
/// since its spare row committed with it.
#[tokio::test]
async fn an_unpushed_shared_write_survives_another_identitys_tidy() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys.clone(), "alice", Vec::new()).await;
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (3, 'unsent')")
        .unwrap();
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 2").await;
    bob.tidy().unwrap();
    assert_eq!(ids(&mut bob), [2, 3], "alice's unpushed row survives");
}

/// A key change spares the new key, so the renamed row survives another
/// identity's pass before it is pushed.
#[tokio::test]
async fn an_unpushed_key_change_keeps_the_renamed_row() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys.clone(), "alice", Vec::new()).await;
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (3, 'unsent')")
        .unwrap();
    assert!(
        alice.push().await.unwrap().is_some(),
        "the insert is queued"
    );
    alice
        .conn()
        .batch_execute("UPDATE tags SET id = 7 WHERE id = 3")
        .unwrap();
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 2").await;
    bob.tidy().unwrap();
    assert_eq!(ids(&mut bob), [2, 7], "the renamed row survives");
}

/// A rolled back write and connetto's own writes record nothing, in the
/// outbox or in the store.
#[tokio::test]
async fn only_a_committed_application_write_is_recorded() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = connect(directory.path(), keys, "alice", vec![snapshot("a", &[1])]).await;
    subscribe(&mut alice, "a", "SELECT * FROM tags").await;
    alice
        .conn()
        .batch_execute("BEGIN; INSERT INTO tags VALUES (3, 'undone'); ROLLBACK")
        .unwrap();
    assert_eq!(
        count(&mut alice, "SELECT count(*) FROM main._connetto_outbox"),
        0,
        "neither the snapshot nor the rolled back insert is in the outbox"
    );
    assert_eq!(
        count(
            &mut alice,
            "SELECT count(*) FROM connetto_shared._connetto_spare"
        ),
        0,
        "nor spared in the store"
    );
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (3, 'kept')")
        .unwrap();
    assert_eq!(
        count(&mut alice, "SELECT count(*) FROM main._connetto_outbox"),
        1,
        "the committed insert is recorded"
    );
    assert_eq!(
        count(
            &mut alice,
            "SELECT count(*) FROM connetto_shared._connetto_spare"
        ),
        1,
        "and spared"
    );
}

/// The store's tables change only with a release, so table DDL on them is
/// refused on the connection and through `change_schema`.
#[tokio::test]
async fn table_ddl_on_the_shared_store_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = open(directory.path(), keys, "alice")
        .await
        .expect("alice opens");
    for ddl in [
        "ALTER TABLE connetto_shared.tags_rls ADD COLUMN extra TEXT",
        "CREATE TABLE connetto_shared.fresh (id INTEGER PRIMARY KEY)",
        "DROP TABLE connetto_shared.tags_rls",
    ] {
        assert!(
            alice.conn().batch_execute(ddl).is_err(),
            "the connection refuses {ddl}"
        );
        assert!(
            alice.change_schema(ddl).is_err(),
            "change_schema refuses {ddl}"
        );
    }
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (3, 'still writable')")
        .unwrap();
    assert_eq!(ids(&mut alice), [3], "the table is as it was");
}

/// An acknowledgement rewrites the spare rows from what is still unsent,
/// the queued entries and the writes not yet pushed alike.
#[tokio::test]
async fn an_acknowledgement_keeps_sparing_the_writes_not_yet_pushed() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = open(directory.path(), keys.clone(), "alice")
        .await
        .expect("alice opens");
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (5, 'sent')")
        .unwrap();
    let client_seq = alice.push().await.unwrap().expect("a seq");
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (6, 'unpushed')")
        .unwrap();
    serve(
        &mut alice,
        vec![ControlMessage::MutationApplied(MutationApplied {
            client_seq,
        })],
    )
    .await;
    let mut bob = connect(directory.path(), keys, "bob", vec![snapshot("b", &[2])]).await;
    subscribe(&mut bob, "b", "SELECT * FROM tags WHERE id = 2").await;
    bob.tidy().unwrap();
    assert_eq!(
        ids(&mut bob),
        [2, 6],
        "the acknowledged row goes, the unpushed one stays"
    );
}

/// A row another identity rewrote after this identity's write uploads as it
/// is now, which the debug check admits for a shared table, since this
/// connection never sees the other identity's write.
#[tokio::test]
async fn a_push_after_another_identity_rewrote_the_row_goes_through() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut alice = open(directory.path(), keys.clone(), "alice")
        .await
        .expect("alice opens");
    alice
        .conn()
        .batch_execute("INSERT INTO tags VALUES (5, 'alice')")
        .unwrap();
    let mut bob = open(directory.path(), keys, "bob")
        .await
        .expect("bob opens");
    bob.conn()
        .batch_execute("UPDATE tags SET label = 'bob' WHERE id = 5")
        .unwrap();
    assert!(
        alice.push().await.unwrap().is_some(),
        "alice's write is queued"
    );
    assert!(bob.push().await.unwrap().is_some(), "and bob's");
}

/// The names of the `marks` rows `client` reads, in order.
fn marks(client: &mut ConnettoConnection<FakeTransport>) -> Vec<String> {
    #[derive(diesel::QueryableByName)]
    struct Mark {
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
    }
    diesel::sql_query("SELECT name FROM marks ORDER BY name")
        .load::<Mark>(client.conn())
        .unwrap()
        .into_iter()
        .map(|mark| mark.name)
        .collect()
}

/// A key of text, bytes and a real is spared in a form its row matches,
/// both as the write commits and as an acknowledgement rewrites the spare
/// rows from what is still unsent.
#[tokio::test]
async fn a_text_bytes_and_real_key_is_spared_in_a_form_its_row_matches() {
    let directory = tempfile::tempdir().unwrap();
    let keys = super::support::SharedKeys::default();
    let mut bob = connect(directory.path(), keys.clone(), "bob", Vec::new()).await;
    bob.subscribe_spec(
        "m",
        connetto_core::messages::SubscriptionSpec::new("SELECT * FROM marks WHERE name = 'none'"),
    )
    .await
    .unwrap();
    let mut alice = open(directory.path(), keys, "alice")
        .await
        .expect("alice opens");
    alice
        .conn()
        .batch_execute("INSERT INTO marks VALUES ('o''neil', X'00ff', 0.1)")
        .unwrap();
    bob.tidy().unwrap();
    assert_eq!(marks(&mut bob), ["o'neil"], "the unpushed row survives");

    let client_seq = alice.push().await.unwrap().expect("a seq");
    alice
        .conn()
        .batch_execute(
            "INSERT INTO marks VALUES ('far', X'02', 9e999); \
             INSERT INTO marks VALUES ('near', X'03', -9e999); \
             INSERT INTO marks VALUES ('second', X'01', 2.5)",
        )
        .unwrap();
    serve(
        &mut alice,
        vec![ControlMessage::MutationApplied(MutationApplied {
            client_seq,
        })],
    )
    .await;
    bob.tidy().unwrap();
    assert_eq!(
        marks(&mut bob),
        ["far", "near", "second"],
        "the acknowledged row goes, the unpushed ones stay, infinite keys too"
    );
}
