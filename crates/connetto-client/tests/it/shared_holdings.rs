//! R11 Part B: the holdings of the tables a build shares live in the shared
//! store under the identity that holds them, beside the holdings of its
//! unsent writes, as the shared columns of the Part B state table say.

use std::collections::BTreeSet;

use connetto_client::{
    ClientBuilder, ClientEvent, ConnettoConnection, DataDir, MemoryRefreshStore, SyncSchema,
};
use connetto_core::messages::{
    ControlMessage, FullResyncReason, MembershipOpened, MutationApplied, MutationReject,
    MutationRejectReason,
};
use connetto_server::LoopbackTransport;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

use super::holdings::{Patch, Session, held, holdings, inserts};
use super::support::SharedKeys;

const SCHEMA: &str = "-- connetto:major 1
CREATE TABLE tags (id INT PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE team_members (id INT PRIMARY KEY, order_id INT);
CREATE TABLE orders (id INT PRIMARY KEY, quantity INT);";
const TAGS: &str = "SELECT * FROM tags";
const ORDERS: &str = "SELECT * FROM orders";
const TERM: &str =
    "SELECT * FROM orders WHERE id IN (SELECT order_id FROM team_members WHERE id > 0)";
const LABEL: &str = "connetto-membership:team_members";
/// The holder of an identity's unsent writes.
const UNSENT: &str = "connetto-unsent";

fn schema() -> SyncSchema {
    SyncSchema::new(
        connetto_schema::translate_shared::<String, connetto_core::bots::TextIds>(
            SCHEMA,
            "",
            &["tags", "team_members"],
        )
        .expect("translate the shared tables"),
    )
}

/// `owner`'s client over the shared device directory, online over a wire the
/// test drives.
async fn identity(directory: &std::path::Path, keys: &SharedKeys, owner: &str) -> Session {
    let conn = ClientBuilder::new(
        schema(),
        super::support::NeverDial::<LoopbackTransport>::default(),
    )
    .signed_in(super::support::held(owner))
    .durable(
        DataDir::new(directory.to_path_buf()),
        keys.clone(),
        MemoryRefreshStore::default(),
    )
    .open_driven()
    .await
    .expect("open the account");
    Session::attach(conn).await
}

fn name(owner: &str) -> String {
    super::support::held(owner).replica_name().to_owned()
}

/// One store holding: identity, holder, table, key.
type StoreHolding = (String, String, String, String);

#[derive(QueryableByName)]
struct StoreRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    identity: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    holder: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    tbl: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    key: String,
}

fn store(conn: &mut ConnettoConnection<LoopbackTransport>) -> BTreeSet<StoreHolding> {
    diesel::sql_query(
        "SELECT identity, holder, tbl, key FROM connetto_shared._connetto_shared_holding",
    )
    .load::<StoreRow>(conn.conn())
    .expect("read the store's holdings")
    .into_iter()
    .map(|row| (row.identity, row.holder, row.tbl, row.key))
    .collect()
}

/// The store holdings `entries` name, each an owner, a holder, a logical
/// table and the ids held. The build splits every shared table behind its
/// policy view, so a holding names the backing table.
fn stored(entries: &[(&str, &str, &str, &[i64])]) -> BTreeSet<StoreHolding> {
    entries
        .iter()
        .flat_map(|(owner, holder, table, ids)| {
            let identity = name(owner);
            ids.iter().map(move |id| {
                (
                    identity.clone(),
                    (*holder).to_owned(),
                    format!("{table}_rls"),
                    format!("(\"id\" IS {id})"),
                )
            })
        })
        .collect()
}

fn write(conn: &mut ConnettoConnection<LoopbackTransport>, sql: &str) {
    conn.conn().batch_execute(sql).expect("write locally");
}

/// Make the shared store refuse `op` on its holdings, as a full disk would.
fn refuse_store_holdings(conn: &mut ConnettoConnection<LoopbackTransport>, op: &str) {
    conn.conn()
        .batch_execute(&format!(
            "CREATE TEMP TRIGGER refuse_{op} BEFORE {op} \
             ON connetto_shared._connetto_shared_holding \
             BEGIN SELECT RAISE(ABORT, 'the store refuses'); END"
        ))
        .expect("install the refusal");
}

async fn answer(s: &mut Session, seq: u64, applied: bool) {
    if applied {
        s.control(
            ControlMessage::MutationApplied(MutationApplied { client_seq: seq }),
            "the acknowledgement",
            |event| matches!(event, ClientEvent::MutationApplied { .. }),
        )
        .await;
    } else {
        s.control(
            ControlMessage::MutationReject(MutationReject {
                client_seq: seq,
                reason: MutationRejectReason::Unauthorized,
            }),
            "the rejection",
            |event| matches!(event, ClientEvent::MutationRejected { .. }),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_row_is_held_in_the_store_under_its_identity() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    alice.holding("a", TAGS, "tags", &[1, 2]).await;
    alice.holding("o", ORDERS, "orders", &[7]).await;
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", "a", "tags", &[1, 2])])
    );
    assert_eq!(
        holdings(&mut alice.conn),
        held(&[("o", "orders_rls", &[7])]),
        "a private table's holdings stay in the account's replica"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_departure_releases_only_its_identitys_holding() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    let mut bob = identity(directory.path(), &keys, "bob").await;
    alice.holding("a", TAGS, "tags", &[1]).await;
    bob.holding("b", TAGS, "tags", &[1]).await;
    alice.live("a", Patch::default().depart("tags", 1)).await;
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("bob", "b", "tags", &[1])])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_upstream_releases_every_holding_of_the_row() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    let mut bob = identity(directory.path(), &keys, "bob").await;
    alice.holding("a", TAGS, "tags", &[1, 2]).await;
    bob.holding("b", TAGS, "tags", &[1]).await;
    write(&mut bob.conn, "UPDATE tags SET label = 'bob' WHERE id = 1");
    alice.live("a", Patch::default().delete("tags", 1)).await;
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", "a", "tags", &[2])]),
        "the row is gone upstream, so no identity holds it, unsent or not"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsent_shared_write_is_held_until_it_is_answered() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;

    write(&mut alice.conn, "INSERT INTO tags VALUES (9, 'unsent')");
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", UNSENT, "tags", &[9])]),
        "the write's own transaction holds the row"
    );
    write(&mut alice.conn, "UPDATE tags SET id = 10 WHERE id = 9");
    let seq = alice.conn.push().await.unwrap().expect("a write");
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", UNSENT, "tags", &[10])]),
        "a moved key is held at its new key while queued"
    );
    answer(&mut alice, seq, true).await;
    assert!(
        store(&mut alice.conn).is_empty(),
        "an acknowledged write holds nothing more"
    );

    write(
        &mut alice.conn,
        "UPDATE tags SET label = 'refused' WHERE id = 10",
    );
    let seq = alice.conn.push().await.unwrap().expect("a write");
    answer(&mut alice, seq, false).await;
    assert!(
        store(&mut alice.conn).is_empty(),
        "a rolled back write holds nothing more"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resync_or_an_unsubscribe_releases_only_its_identitys_holdings() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    let mut bob = identity(directory.path(), &keys, "bob").await;
    alice.holding("a", TAGS, "tags", &[1]).await;
    alice.holding("a2", TAGS, "tags", &[1]).await;
    bob.holding("b", TAGS, "tags", &[1]).await;
    alice
        .resync("a", FullResyncReason::CursorOutsideRetention)
        .await;
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", "a2", "tags", &[1]), ("bob", "b", "tags", &[1])])
    );
    alice.conn.unsubscribe("a2").await.unwrap();
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("bob", "b", "tags", &[1])]),
        "bob's subscription is not alice's to prune"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_truncate_releases_every_identitys_subscription_holdings_but_no_unsent_one() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    let mut bob = identity(directory.path(), &keys, "bob").await;
    alice.holding("a", TAGS, "tags", &[1, 2]).await;
    bob.holding("b", TAGS, "tags", &[1]).await;
    write(&mut bob.conn, "UPDATE tags SET label = 'bob' WHERE id = 2");
    alice
        .resync(
            "a",
            FullResyncReason::TableTruncated {
                table: "tags".to_owned(),
            },
        )
        .await;
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("bob", UNSENT, "tags", &[2])])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_beyond_history_releases_its_identitys_subscription_holdings() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    let mut bob = identity(directory.path(), &keys, "bob").await;
    alice.holding("a", TAGS, "tags", &[1]).await;
    bob.holding("b", TAGS, "tags", &[1]).await;
    write(
        &mut alice.conn,
        "UPDATE tags SET label = 'alice' WHERE id = 1",
    );
    alice
        .resync("a", FullResyncReason::CursorBeyondHistory)
        .await;
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", UNSENT, "tags", &[1]), ("bob", "b", "tags", &[1])])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_membership_row_is_held_under_its_identitys_label() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    alice.conn.subscribe("t", TERM).await.unwrap();
    alice
        .control(
            ControlMessage::MembershipOpened(MembershipOpened {
                sub_id: LABEL.to_owned(),
                member_table: "team_members".to_owned(),
            }),
            "the membership notice",
            |event| matches!(event, ClientEvent::MembershipOpened { .. }),
        )
        .await;
    alice
        .snapshot(LABEL, vec![inserts("team_members", &[10])])
        .await;
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", LABEL, "team_members", &[10])])
    );
    alice.conn.unsubscribe("t").await.unwrap();
    assert!(store(&mut alice.conn).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaving_the_store_releases_every_holding_of_the_identity() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    let mut bob = identity(directory.path(), &keys, "bob").await;
    alice.holding("a", TAGS, "tags", &[1]).await;
    bob.holding("b", TAGS, "tags", &[1]).await;
    write(&mut alice.conn, "INSERT INTO tags VALUES (9, 'unsent')");
    alice.conn.leave_shared_store(false).unwrap();
    assert_eq!(store(&mut bob.conn), stored(&[("bob", "b", "tags", &[1])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_store_generation_releases_the_identitys_private_holdings() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    alice.holding("o", ORDERS, "orders", &[7]).await;
    write(
        &mut alice.conn,
        "DELETE FROM _connetto_shared_generation WHERE id = 1",
    );
    drop(alice.offline().await);
    let mut alice = identity(directory.path(), &keys, "alice").await;
    assert!(
        holdings(&mut alice.conn).is_empty(),
        "the replays re-snapshot, so the old generation's holdings go"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_write_whose_holding_cannot_be_written_is_not_made() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    refuse_store_holdings(&mut alice.conn, "INSERT");
    alice
        .conn
        .conn()
        .batch_execute("INSERT INTO tags VALUES (9, 'unsent')")
        .expect_err("the store refuses the write's holding");
    let stored_rows: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
        "(SELECT count(*) FROM tags)",
    ))
    .get_result(alice.conn.conn())
    .unwrap();
    assert_eq!(
        stored_rows, 0,
        "the write goes in with its holding or not at all"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsubscribe_whose_store_holdings_cannot_be_released_keeps_the_subscription() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    alice.holding("a", TAGS, "tags", &[1]).await;
    refuse_store_holdings(&mut alice.conn, "DELETE");
    alice
        .conn
        .unsubscribe("a")
        .await
        .expect_err("the store refuses the release");
    assert_eq!(alice.conn.declared_subscriptions().unwrap().len(), 1);
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", "a", "tags", &[1])])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_departure_whose_holdings_cannot_be_released_keeps_the_identity() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = identity(directory.path(), &keys, "alice").await;
    alice.holding("a", TAGS, "tags", &[1]).await;
    refuse_store_holdings(&mut alice.conn, "DELETE");
    alice
        .conn
        .leave_shared_store(false)
        .expect_err("the store refuses the release");
    assert_eq!(
        store(&mut alice.conn),
        stored(&[("alice", "a", "tags", &[1])]),
        "the identity leaves with its holdings or not at all"
    );
}
