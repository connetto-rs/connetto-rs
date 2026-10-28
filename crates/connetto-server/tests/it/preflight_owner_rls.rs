//! Startup refuses an owner pool that row-level security filters.
//!
//! The owner pool reads the authorization facts and the rows withdrawn from
//! callers on behalf of every caller, so it has to see every row. Postgres
//! applies a table's policies to its owner only under `FORCE ROW LEVEL
//! SECURITY`, and to any other role that is neither a superuser nor
//! `BYPASSRLS`. The policies bind settings no owner connection sets, so a
//! filtered owner would read too few rows and lose grants without an error.
//!
//! Needs Docker: the fixture starts its own Postgres.

use connetto_server::preflight::{self, Artifact, PreflightError};
use connetto_test_harness::{Fixture, pool_for};

use super::e2e::with_user_url;

/// A membership table under a policy that binds a setting, owned by a role
/// that is neither a superuser nor `BYPASSRLS`, and readable by a second role.
const SETUP: [&str; 8] = [
    "CREATE ROLE frls_owner LOGIN PASSWORD 'frls_owner'",
    "CREATE ROLE frls_other LOGIN PASSWORD 'frls_other'",
    "CREATE TABLE frls_members (project_id BIGINT NOT NULL, user_id TEXT NOT NULL)",
    "ALTER TABLE frls_members OWNER TO frls_owner",
    "ALTER TABLE frls_members ENABLE ROW LEVEL SECURITY",
    "CREATE POLICY frls_members_p ON frls_members USING \
     (user_id = current_setting('app.user_id', true))",
    "GRANT SELECT ON frls_members TO frls_other",
    "INSERT INTO frls_members VALUES (1, 'alice')",
];

const TABLE: &str = "frls_members";

fn refusal_names_the_table(refused: &PreflightError) {
    let PreflightError::OwnerFiltered { table } = refused else {
        panic!("the wrong refusal: {refused}");
    };
    assert_eq!(table, TABLE, "the refusal names the filtered table");
}

#[tokio::test]
async fn an_owner_pool_that_row_level_security_filters_refuses_the_boot() {
    let fixture = Fixture::acquire().await;
    fixture.setup(&SETUP).await;
    let owner = pool_for(&with_user_url(
        fixture.admin_url(),
        "frls_owner",
        "frls_owner",
    ))
    .await;
    let required = [Artifact::ReadInFull(TABLE)];

    preflight::require(&owner, &required)
        .await
        .expect("the owner of a table that does not force row-level security reads every row");

    fixture
        .exec("ALTER TABLE frls_members FORCE ROW LEVEL SECURITY")
        .await;
    refusal_names_the_table(
        &preflight::require(&owner, &required)
            .await
            .expect_err("a forced table filters its owner"),
    );
    preflight::require(fixture.admin(), &required)
        .await
        .expect("a superuser reads every row of a forced table");

    let other = pool_for(&with_user_url(
        fixture.admin_url(),
        "frls_other",
        "frls_other",
    ))
    .await;
    fixture
        .exec("ALTER TABLE frls_members NO FORCE ROW LEVEL SECURITY")
        .await;
    refusal_names_the_table(
        &preflight::require(&other, &required)
            .await
            .expect_err("a role that does not own the table is filtered by its policies"),
    );
}
