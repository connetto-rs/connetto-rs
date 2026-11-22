//! Startup refuses a reader connection that is not the reader role, or a reader
//! role that holds the privileges of a declared table's owner.
//!
//! Every caller reaches Postgres as the reader role, and both translators model
//! the policies and grants that bind that role. A connection that logs in as
//! another role, or switches to one through a role setting, is bound by other
//! rules. A role holding the owner's privileges skips the table's policies
//! unless the table forces row-level security, and holds every grant on it.
//!
//! Needs Docker: the fixture starts its own Postgres.

use connetto_server::preflight::{self, Artifact, PreflightError};
use connetto_test_harness::server_fixture::with_user_url;
use connetto_test_harness::{Fixture, pool_for};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::Pool;

const READER: &str = "rrole_reader";

const SETUP: [&str; 6] = [
    "CREATE ROLE rrole_reader LOGIN PASSWORD 'rrole_reader'",
    "CREATE ROLE rrole_other LOGIN PASSWORD 'rrole_other'",
    "CREATE ROLE rrole_switched NOLOGIN",
    "CREATE ROLE rrole_owner NOLOGIN",
    "CREATE TABLE rrole_items (id INT PRIMARY KEY)",
    "GRANT SELECT ON rrole_items TO rrole_reader",
];

/// A fresh pool logging in as `role`, so a role setting changed since the
/// last pool applies to every connection.
async fn pool_as(fixture: &Fixture, role: &str) -> Pool<AsyncPgConnection> {
    pool_for(&with_user_url(fixture.admin_url(), role, role)).await
}

#[tokio::test]
async fn a_connection_that_logs_in_or_acts_as_another_role_is_refused() {
    let fixture = Fixture::acquire().await;
    fixture.setup(&SETUP).await;
    let required = [Artifact::ActingRole(READER)];

    preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect("a connection logging in and acting as the reader role is fit");

    let refused = preflight::require(&pool_as(&fixture, "rrole_other").await, &required)
        .await
        .expect_err("a connection logging in as another role is refused");
    assert!(
        matches!(
            refused,
            PreflightError::ActingAs { ref expected, ref session_user, ref current_user }
                if expected == READER && session_user == "rrole_other" && current_user == "rrole_other"
        ),
        "{refused:?}"
    );

    fixture
        .exec("GRANT rrole_reader TO rrole_other WITH INHERIT FALSE")
        .await;
    fixture
        .exec("ALTER ROLE rrole_other SET role = 'rrole_reader'")
        .await;
    let refused = preflight::require(&pool_as(&fixture, "rrole_other").await, &required)
        .await
        .expect_err("a connection acting as the reader role but logging in as another is refused");
    assert!(
        matches!(
            refused,
            PreflightError::ActingAs { ref session_user, ref current_user, .. }
                if session_user == "rrole_other" && current_user == READER
        ),
        "{refused:?}"
    );

    fixture
        .exec("GRANT rrole_switched TO rrole_reader WITH INHERIT FALSE")
        .await;
    fixture
        .exec("ALTER ROLE rrole_reader SET role = 'rrole_switched'")
        .await;
    let refused = preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect_err("a reader whose role setting switches it to another role is refused");
    assert!(
        matches!(
            refused,
            PreflightError::ActingAs { ref session_user, ref current_user, .. }
                if session_user == READER && current_user == "rrole_switched"
        ),
        "{refused:?}"
    );
}

#[tokio::test]
async fn a_reader_holding_the_privileges_of_a_table_owner_is_refused() {
    let fixture = Fixture::acquire().await;
    fixture.setup(&SETUP).await;
    let required = [Artifact::PolicyBoundTable("rrole_items")];

    preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect("a reader that neither owns the table nor holds its owner's privileges is fit");

    fixture
        .exec("ALTER TABLE rrole_items OWNER TO rrole_reader")
        .await;
    let refused = preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect_err("a reader owning the table is refused");
    assert!(
        matches!(
            refused,
            PreflightError::OwnerPrivileges { ref table, ref role, ref owner }
                if table == "rrole_items" && role == READER && owner == READER
        ),
        "{refused:?}"
    );

    fixture
        .exec("ALTER TABLE rrole_items OWNER TO rrole_owner")
        .await;
    fixture
        .exec("GRANT rrole_owner TO rrole_reader WITH INHERIT FALSE")
        .await;
    preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect(
            "a membership that does not inherit the owner's privileges leaves the policies binding",
        );

    fixture
        .exec("GRANT rrole_owner TO rrole_reader WITH INHERIT TRUE")
        .await;
    let refused = preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect_err("a reader inheriting the owner's privileges is refused");
    assert!(
        matches!(
            refused,
            PreflightError::OwnerPrivileges { ref table, ref role, ref owner }
                if table == "rrole_items" && role == READER && owner == "rrole_owner"
        ),
        "{refused:?}"
    );
}

#[tokio::test]
async fn an_absent_declared_table_is_refused_as_missing() {
    let fixture = Fixture::acquire().await;
    fixture.setup(&SETUP).await;
    let refused = preflight::require(
        &pool_as(&fixture, READER).await,
        &[Artifact::PolicyBoundTable("rrole_absent")],
    )
    .await
    .expect_err("an absent table is refused");
    assert!(
        matches!(
            refused,
            PreflightError::Missing { noun: "table", ref name } if name == "rrole_absent"
        ),
        "{refused:?}"
    );
}
