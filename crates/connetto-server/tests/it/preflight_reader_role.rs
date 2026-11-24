//! Startup refuses a reader connection that is not the reader role, a kind
//! role the reader cannot safely act as, or a role that holds the privileges
//! of a declared table's owner.
//!
//! Every caller reaches Postgres through the reader role and acts as its
//! kind's role, and both translators model the policies and grants that bind
//! the kind roles. A connection that logs in as another role, or switches to
//! one through a role setting, is bound by other rules. A role holding the
//! owner's privileges skips the table's policies unless the table forces
//! row-level security, and holds every grant on it.
//!
//! Needs Docker: the fixture starts its own Postgres.

use connetto_server::preflight::{self, Artifact, KindRoleFault, PreflightError};
use connetto_test_harness::server_fixture::with_user_url;
use connetto_test_harness::{Fixture, pool_for};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::Pool;

const READER: &str = "rrole_reader";
const KIND: &str = "rrole_kind";

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
    let required = [Artifact::PolicyBoundTable {
        role: READER,
        table: "rrole_items",
    }];

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
        &[Artifact::PolicyBoundTable {
            role: READER,
            table: "rrole_absent",
        }],
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

/// A kind role owning a declared table skips its policies, so a caller of
/// that kind would read and write around them, whatever the reader holds.
#[tokio::test]
async fn a_kind_role_holding_the_privileges_of_a_table_owner_is_refused() {
    let fixture = Fixture::acquire().await;
    fixture.setup(&SETUP).await;
    fixture.exec("CREATE ROLE rrole_kind NOLOGIN").await;
    let required = [Artifact::PolicyBoundTable {
        role: KIND,
        table: "rrole_items",
    }];
    preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect("a kind role that does not own the table is fit");

    fixture
        .exec("ALTER TABLE rrole_items OWNER TO rrole_owner")
        .await;
    fixture
        .exec("GRANT rrole_owner TO rrole_kind WITH INHERIT TRUE")
        .await;
    let refused = preflight::require(&pool_as(&fixture, READER).await, &required)
        .await
        .expect_err("a kind role inheriting the owner's privileges is refused");
    assert!(
        matches!(
            refused,
            PreflightError::OwnerPrivileges { ref table, ref role, ref owner }
                if table == "rrole_items" && role == KIND && owner == "rrole_owner"
        ),
        "{refused:?}"
    );
}

/// The faults the reader's pool reports for the kind role.
async fn kind_faults(fixture: &Fixture) -> Vec<KindRoleFault> {
    match preflight::require(&pool_as(fixture, READER).await, &[Artifact::KindRole(KIND)]).await {
        Ok(()) => Vec::new(),
        Err(PreflightError::KindRole { role, faults }) if role == KIND => faults,
        Err(other) => panic!("a kind role is refused for its faults alone: {other:?}"),
    }
}

/// A kind role is refused when it is missing, when it could act on its own
/// or around every policy, and when the reader holds it in any way but the
/// one switch connetto makes per transaction.
#[tokio::test]
async fn a_kind_role_the_reader_cannot_safely_act_as_is_refused() {
    let fixture = Fixture::acquire().await;
    fixture.setup(&SETUP).await;
    let refused = preflight::require(
        &pool_as(&fixture, READER).await,
        &[Artifact::KindRole(KIND)],
    )
    .await
    .expect_err("a missing kind role is refused");
    assert!(
        matches!(
            refused,
            PreflightError::Missing { noun: "kind role", ref name } if name == KIND
        ),
        "{refused:?}"
    );

    fixture.exec("CREATE ROLE rrole_kind NOLOGIN").await;
    assert_eq!(
        kind_faults(&fixture).await,
        [KindRoleFault::NotSettable],
        "the reader must be able to switch to the kind role"
    );

    fixture
        .exec("GRANT rrole_kind TO rrole_reader WITH INHERIT FALSE, SET TRUE")
        .await;
    assert_eq!(kind_faults(&fixture).await, []);

    for (alter, undo, fault) in [
        (
            "ALTER ROLE rrole_kind LOGIN",
            "ALTER ROLE rrole_kind NOLOGIN",
            KindRoleFault::LogsIn,
        ),
        (
            "ALTER ROLE rrole_kind BYPASSRLS",
            "ALTER ROLE rrole_kind NOBYPASSRLS",
            KindRoleFault::BypassesRls,
        ),
        (
            "ALTER ROLE rrole_kind SUPERUSER",
            "ALTER ROLE rrole_kind NOSUPERUSER",
            KindRoleFault::Superuser,
        ),
        (
            "GRANT pg_read_all_data TO rrole_kind",
            "REVOKE pg_read_all_data FROM rrole_kind",
            KindRoleFault::ReadsAllData,
        ),
        (
            "GRANT pg_write_all_data TO rrole_kind",
            "REVOKE pg_write_all_data FROM rrole_kind",
            KindRoleFault::WritesAllData,
        ),
        (
            "GRANT rrole_kind TO rrole_reader WITH INHERIT TRUE",
            "GRANT rrole_kind TO rrole_reader WITH INHERIT FALSE",
            KindRoleFault::InheritedByReader,
        ),
    ] {
        fixture.exec(alter).await;
        let faults = kind_faults(&fixture).await;
        assert!(faults.contains(&fault), "{alter}: {faults:?}");
        fixture.exec(undo).await;
        assert_eq!(kind_faults(&fixture).await, [], "{undo}");
    }
}
