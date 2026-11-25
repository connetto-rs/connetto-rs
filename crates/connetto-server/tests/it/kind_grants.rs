//! Docker-gated tests of the commands a caller's kind lacks (R108 slice 5).
//!
//! Every caller acts as its kind's role, and the deployment grants each kind
//! its own table and column privileges. A subscription or a write the kind's
//! privileges do not cover is refused up front with a reason naming the table
//! and the kind, while a table no kind may read keeps the fixed refusal that
//! discloses nothing. A `REVOKE` the marker row carries ends that kind's live
//! subscriptions on the table and leaves every other kind serving.

use std::collections::BTreeSet;

use connetto_core::CallerKind;
use connetto_core::messages::{
    BulkMessage, ControlMessage, MutationHeader, MutationPatch, MutationRejectReason,
    SUBSCRIPTION_REFUSED, Subscribe, SubscriptionNotGranted, SubscriptionSpec,
};
use connetto_core::traits::Transport as _;
use connetto_server::epoch::POLICY_CHANGES_TRIGGER_DDL;
use connetto_test_harness::server_fixture::PG_SERIAL;
use connetto_test_harness::{Fixture, RowValue, insert_changeset};
use diesel_async::RunQueryDsl as _;

use super::lifecycle::{admin_pool, next_control};
use super::live_rules::{
    Client, NEVER, Running, next_live_patch, replacement, setup, snapshot, subscribe,
};

/// Leave `anon` reading `orders` and nothing on `keep`, while `authenticated`
/// keeps every privilege the fixture grants.
const ANON_READS_ORDERS_ONLY: [&str; 2] = [
    "REVOKE INSERT, UPDATE, DELETE ON orders FROM anon",
    "REVOKE ALL ON keep FROM anon",
];

/// Subscribe with `query` as written.
async fn subscribe_to(client: &mut Client, subscription: &str, query: &str) {
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: subscription.to_owned(),
            spec: SubscriptionSpec::new(query),
        }))
        .await
        .expect("subscribe");
}

/// The refusal naming `table` and `kind` for `subscription`.
fn not_granted(subscription: &str, table: &str, kind: CallerKind) -> ControlMessage {
    ControlMessage::SubscriptionNotGranted(SubscriptionNotGranted {
        sub_id: subscription.to_owned(),
        table: table.to_owned(),
        kind,
    })
}

/// Insert order `id` with `status` as the connection's first write and answer
/// the frame the server settled it with.
async fn insert_order(client: &mut Client, id: i64, status: Option<&str>) -> ControlMessage {
    let changeset = insert_changeset(
        "orders",
        &["id", "price", "quantity", "status"],
        &[0],
        vec![
            RowValue::Integer(id),
            RowValue::Real(1.0),
            RowValue::Integer(1),
            status.map_or(RowValue::Null, |status| RowValue::Text(status.to_owned())),
        ],
    );
    client
        .send_control(ControlMessage::MutationHeader(MutationHeader::new(1, 1)))
        .await
        .expect("send the mutation header");
    client
        .send_bulk(BulkMessage::MutationPatch(MutationPatch::new(
            1,
            zstd::encode_all(changeset.as_slice(), 3).expect("compress"),
        )))
        .await
        .expect("send the mutation patch");
    next_control(client).await
}

/// One order id, as the administrator reads it.
#[derive(diesel::QueryableByName)]
struct OrderId {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
}

/// The ids of every order, read as the fixture's administrator.
async fn order_ids(fixture: &Fixture) -> BTreeSet<i64> {
    let pool = admin_pool(fixture).await;
    let mut connection = pool.get().await.expect("admin connection");
    diesel::sql_query("SELECT id::BIGINT AS id FROM orders")
        .load::<OrderId>(&mut *connection)
        .await
        .expect("read orders")
        .into_iter()
        .map(|row| row.id)
        .collect()
}

/// One order's status, as the administrator reads it.
#[derive(diesel::QueryableByName)]
struct OrderStatus {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    status: Option<String>,
}

/// The status of order `id`, read as the fixture's administrator, or `None`
/// when no such order exists.
async fn order_status(fixture: &Fixture, id: i64) -> Option<Option<String>> {
    let pool = admin_pool(fixture).await;
    let mut connection = pool.get().await.expect("admin connection");
    diesel::sql_query("SELECT status FROM orders WHERE id = $1")
        .bind::<diesel::sql_types::BigInt, _>(id)
        .load::<OrderStatus>(&mut *connection)
        .await
        .expect("read the order")
        .pop()
        .map(|row| row.status)
}

#[tokio::test]
async fn a_kind_is_refused_a_subscription_to_a_table_it_cannot_read() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    for statement in ANON_READS_ORDERS_ONLY {
        fixture.exec(statement).await;
    }
    let server = Running::start(&fixture, NEVER, false).await;

    let mut anonymous = server.connect_anonymous("anonymous-reader").await;
    subscribe(&mut anonymous, "keep", "keep").await;
    assert_eq!(
        next_control(&mut anonymous).await,
        not_granted("keep", "keep", CallerKind::Anon),
    );
    subscribe(&mut anonymous, "orders", "orders").await;
    assert_eq!(
        snapshot(&mut anonymous, "orders").await.0,
        BTreeSet::from([1, 2]),
        "the kind still reads the table it is granted",
    );

    let mut signed_in = server.connect_signed_in("signed-in-reader").await;
    subscribe(&mut signed_in, "keep", "keep").await;
    assert_eq!(
        snapshot(&mut signed_in, "keep").await.0,
        BTreeSet::from([1]),
        "another kind holding the grant reads the same table",
    );
    drop((anonymous, signed_in));
    server.stop().await;
}

#[tokio::test]
async fn a_kind_is_refused_a_write_it_cannot_make_before_the_database() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    for statement in ANON_READS_ORDERS_ONLY {
        fixture.exec(statement).await;
    }
    let server = Running::start(&fixture, NEVER, false).await;

    let mut anonymous = server.connect_anonymous("anonymous-writer").await;
    let ControlMessage::MutationReject(reject) =
        insert_order(&mut anonymous, 100, Some("written")).await
    else {
        panic!("the anonymous write was not refused");
    };
    assert_eq!(
        reject.reason,
        MutationRejectReason::NotGranted {
            table: "orders".to_owned(),
            kind: CallerKind::Anon,
        },
    );
    assert!(!order_ids(&fixture).await.contains(&100));

    let mut signed_in = server.connect_signed_in("signed-in-writer").await;
    assert!(matches!(
        insert_order(&mut signed_in, 101, Some("written")).await,
        ControlMessage::MutationApplied(_)
    ));
    assert!(order_ids(&fixture).await.contains(&101));
    drop((anonymous, signed_in));
    server.stop().await;
}

/// A write to a table no kind may read is refused as plain `Unauthorized`,
/// which names neither the table nor the kind.
#[tokio::test]
async fn a_write_to_a_table_no_kind_may_read_keeps_the_plain_refusal() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    fixture
        .exec("REVOKE ALL ON orders FROM anon, authenticated, bot_installation, bot, delegated")
        .await;
    let server = Running::start(&fixture, NEVER, false).await;

    let mut anonymous = server.connect_anonymous("hidden-writer").await;
    let ControlMessage::MutationReject(reject) =
        insert_order(&mut anonymous, 100, Some("written")).await
    else {
        panic!("the write to a hidden table was not refused");
    };
    assert_eq!(reject.reason, MutationRejectReason::Unauthorized);
    assert!(!order_ids(&fixture).await.contains(&100));
    drop(anonymous);
    server.stop().await;
}

/// An insert's `NULL` in a column the kind holds no `INSERT` on is no
/// assignment, so the row lands with the column's default, while a value there
/// is refused as `PostgreSQL` refuses it.
#[tokio::test]
async fn a_null_in_a_column_the_kind_may_not_insert_is_left_to_its_default() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    for statement in [
        "REVOKE INSERT ON orders FROM authenticated",
        "GRANT INSERT (id, price, quantity) ON orders TO authenticated",
    ] {
        fixture.exec(statement).await;
    }
    let server = Running::start(&fixture, NEVER, false).await;

    let mut leaving = server.connect_signed_in("leaves-status-out").await;
    let settled = insert_order(&mut leaving, 102, None).await;
    assert!(
        matches!(settled, ControlMessage::MutationApplied(_)),
        "the insert assigns only granted columns: {settled:?}",
    );
    assert_eq!(order_status(&fixture, 102).await, Some(None));

    let mut assigning = server.connect_signed_in("assigns-status").await;
    let ControlMessage::MutationReject(reject) =
        insert_order(&mut assigning, 103, Some("written")).await
    else {
        panic!("the insert assigning status was not refused");
    };
    assert_eq!(
        reject.reason,
        MutationRejectReason::NotGranted {
            table: "orders".to_owned(),
            kind: CallerKind::Authenticated,
        },
    );
    assert_eq!(order_status(&fixture, 103).await, None);
    drop((leaving, assigning));
    server.stop().await;
}

#[tokio::test]
async fn a_column_the_kind_cannot_read_refuses_a_query_naming_it() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    for statement in [
        "REVOKE ALL ON orders FROM anon",
        "GRANT SELECT (id, quantity) ON orders TO anon",
    ] {
        fixture.exec(statement).await;
    }
    let server = Running::start(&fixture, NEVER, false).await;

    let mut anonymous = server.connect_anonymous("column-reader").await;
    subscribe_to(&mut anonymous, "every-column", "SELECT * FROM orders").await;
    assert_eq!(
        next_control(&mut anonymous).await,
        not_granted("every-column", "orders", CallerKind::Anon),
    );
    drop(anonymous);
    server.stop().await;
}

#[tokio::test]
async fn a_table_no_kind_may_read_keeps_the_fixed_refusal() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    fixture
        .exec("REVOKE ALL ON keep FROM anon, authenticated, bot_installation, bot, delegated")
        .await;
    let server = Running::start(&fixture, NEVER, false).await;

    let mut anonymous = server.connect_anonymous("hidden-table").await;
    subscribe(&mut anonymous, "keep", "keep").await;
    let ControlMessage::NonFatalError(refusal) = next_control(&mut anonymous).await else {
        panic!("a table no kind reads answers the fixed refusal");
    };
    assert_eq!(refusal.related_to.as_deref(), Some("keep"));
    assert_eq!(refusal.detail, SUBSCRIPTION_REFUSED);
    drop(anonymous);
    server.stop().await;
}

#[tokio::test]
async fn a_revoke_at_the_marker_ends_only_that_kinds_subscriptions() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    fixture.exec(POLICY_CHANGES_TRIGGER_DDL).await;
    let server = Running::start(&fixture, NEVER, false).await;

    let mut anonymous = server.connect_anonymous("revoked-reader").await;
    subscribe(&mut anonymous, "keep", "keep").await;
    assert_eq!(
        snapshot(&mut anonymous, "keep").await.0,
        BTreeSet::from([1])
    );
    let mut signed_in = server.connect_signed_in("kept-reader").await;
    subscribe(&mut signed_in, "keep", "keep").await;
    assert_eq!(
        snapshot(&mut signed_in, "keep").await.0,
        BTreeSet::from([1])
    );

    fixture.exec("REVOKE SELECT ON keep FROM anon").await;
    assert_eq!(
        next_control(&mut anonymous).await,
        not_granted("keep", "keep", CallerKind::Anon),
    );
    assert_eq!(
        replacement(&mut signed_in, "keep").await,
        BTreeSet::from([1]),
        "the kind keeping its grant reads the table again under the new rules",
    );
    fixture
        .exec("INSERT INTO keep VALUES (2, 'after the revoke')")
        .await;
    assert_eq!(
        next_live_patch(&mut signed_in, "keep").await,
        BTreeSet::from([2]),
    );
    drop((anonymous, signed_in));
    server.stop().await;
    fixture
        .exec("DROP EVENT TRIGGER connetto_policy_changes_trigger")
        .await;
}

/// A `REVOKE` the marker row carries ends that kind's live aggregate on the
/// table with the refusal naming the table and the kind. The caller is signed
/// in, since an aggregate over a row-secured table reads as its viewer and an
/// anonymous caller has none.
#[tokio::test]
async fn a_revoke_at_the_marker_ends_that_kinds_aggregate() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    fixture.exec(POLICY_CHANGES_TRIGGER_DDL).await;
    let server = Running::start(&fixture, NEVER, false).await;

    let mut signed_in = server.connect_signed_in("revoked-counter").await;
    subscribe_to(&mut signed_in, "count", "SELECT COUNT(*) FROM keep").await;
    let columns = next_control(&mut signed_in).await;
    assert!(
        matches!(columns, ControlMessage::AggregateColumns(_)),
        "the aggregate opens with its columns: {columns:?}",
    );
    let answer = next_control(&mut signed_in).await;
    assert!(
        matches!(&answer, ControlMessage::AggregateUpdate(update) if update.sub_id == "count"),
        "the aggregate answers: {answer:?}",
    );

    fixture
        .exec("REVOKE SELECT ON keep FROM authenticated")
        .await;
    let refusal = loop {
        match next_control(&mut signed_in).await {
            ControlMessage::FullResyncRequired(resync) if resync.sub_id == "count" => {}
            ControlMessage::AggregateUpdate(update) if update.sub_id == "count" => {}
            other => break other,
        }
    };
    assert_eq!(
        refusal,
        not_granted("count", "keep", CallerKind::Authenticated)
    );
    drop(signed_in);
    server.stop().await;
    fixture
        .exec("DROP EVENT TRIGGER connetto_policy_changes_trigger")
        .await;
}
