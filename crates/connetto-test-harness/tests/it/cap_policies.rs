//! A stack provisioned the way CI provisions one must carry the generated cap
//! document, or the server's startup check refuses the boot (R91).
//!
//! Needs Docker: the fixture starts its own Postgres and authorization service.

use connetto_test_harness::stack::{DEMO_DEPLOYMENT, provision};
use diesel_async::RunQueryDsl as _;

/// One row of `pg_policies`, read by table name.
#[derive(diesel::QueryableByName)]
struct CapRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    tablename: String,
}

#[tokio::test]
async fn a_provisioned_stack_carries_the_generated_cap_document() {
    let provisioned = provision(&DEMO_DEPLOYMENT, "cap-doc-test", None)
        .await
        .expect("provision");
    let pool = provisioned.fixture.admin();
    let mut conn = pool.get().await.expect("admin connection");
    let present: Vec<CapRow> = diesel::sql_query(
        "SELECT tablename FROM pg_policies \
         WHERE policyname = 'photos_connetto_cap_read'",
    )
    .load(&mut *conn)
    .await
    .expect("read policies");
    let row = present
        .into_iter()
        .next()
        .expect("the cap read policy is applied by provision");
    assert_eq!(row.tablename, "photos", "the cap lands on the synced table");
}
