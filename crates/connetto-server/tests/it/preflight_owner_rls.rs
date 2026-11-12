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

use std::time::Duration;

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
    let PreflightError::ReadFiltered { table, .. } = refused else {
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

#[tokio::test]
async fn an_audit_reader_requires_select_and_unfiltered_access() {
    let fixture = Fixture::acquire().await;
    fixture.setup(&SETUP).await;
    let reader = pool_for(&with_user_url(
        fixture.admin_url(),
        "frls_other",
        "frls_other",
    ))
    .await;
    let required = [Artifact::ReadInFull(TABLE)];
    let refused = preflight::require(&reader, &required)
        .await
        .expect_err("the reader is filtered");
    assert!(matches!(
        refused,
        PreflightError::ReadFiltered { ref table, ref role }
            if table == TABLE && role == "frls_other"
    ));
    fixture
        .exec("ALTER TABLE frls_members DISABLE ROW LEVEL SECURITY")
        .await;
    fixture
        .exec("REVOKE SELECT ON frls_members FROM frls_other")
        .await;
    let refused = preflight::require(&reader, &required)
        .await
        .expect_err("the reader lacks SELECT");
    assert!(matches!(
        refused,
        PreflightError::ReadDenied { ref table, ref role }
            if table == TABLE && role == "frls_other"
    ));
    fixture
        .exec("GRANT SELECT ON frls_members TO frls_other")
        .await;
    preflight::require(&reader, &required)
        .await
        .expect("the reader sees every audit row");
}

#[tokio::test]
async fn an_absent_audit_reader_table_refuses_the_boot() {
    let fixture = Fixture::acquire().await;
    let required = [Artifact::ReadInFull("unprovisioned_audit_events")];
    let refused = tokio::time::timeout(
        Duration::from_secs(15),
        preflight::require(fixture.admin(), &required),
    )
    .await
    .expect("the missing-table probe completes")
    .expect_err("the required audit table is absent");
    assert!(matches!(
        refused,
        PreflightError::Missing { noun: "table", ref name }
            if name == "unprovisioned_audit_events"
    ));
}

#[tokio::test]
async fn an_audit_reader_catalog_failure_is_reported_as_a_probe_error() {
    let fixture = Fixture::acquire().await;
    tokio::time::timeout(Duration::from_secs(15), async {
        fixture
            .setup(&[
                "CREATE ROLE audit_reader LOGIN PASSWORD 'audit_reader'",
                "GRANT SELECT ON auth_events TO audit_reader",
            ])
            .await;
        let reader = pool_for(&with_user_url(
            fixture.admin_url(),
            "audit_reader",
            "audit_reader",
        ))
        .await;
        let required = [Artifact::ReadInFull("auth_events")];
        preflight::require(&reader, &required)
            .await
            .expect("the reader can read the audit table");

        fixture
            .exec("REVOKE SELECT ON pg_catalog.pg_class FROM PUBLIC")
            .await;
        let refused = preflight::require(&reader, &required)
            .await
            .expect_err("catalog access is denied");
        let PreflightError::Probe { noun, name, source } = refused else {
            panic!("the catalog query must fail as a probe");
        };
        assert_eq!(noun, "table");
        assert_eq!(name, "auth_events");
        let diesel::result::Error::DatabaseError(_, detail) = source else {
            panic!("the probe must preserve the database error");
        };
        assert_eq!(detail.message(), "permission denied for table pg_class");

        fixture
            .exec("GRANT SELECT ON pg_catalog.pg_class TO audit_reader")
            .await;
        preflight::require(&reader, &required)
            .await
            .expect("catalog access restores the audit readiness check");
    })
    .await
    .expect("the catalog-access scenario completes");
}
