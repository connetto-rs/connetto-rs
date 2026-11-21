//! A deployment's roles document applied to one cluster twice.
//!
//! Needs Docker: the fixture starts its own Postgres.

use connetto_test_harness::Fixture;

/// A second provision finds the roles the first created, whether a name was
/// quoted, keeping its case, or unquoted, folding to lower case.
#[tokio::test]
async fn a_roles_document_applies_twice_to_one_cluster() {
    let fixture = Fixture::acquire().await;
    let roles = "CREATE ROLE \"Twice_Quoted\" NOLOGIN; CREATE ROLE Twice_Unquoted NOLOGIN;";
    fixture.setup_roles(roles).await;
    fixture.setup_roles(roles).await;
}
