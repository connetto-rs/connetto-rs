use std::sync::Arc;

use connetto_server::builder::{
    BuildError, Database, OidcProvider, OpenFga, ServerBuilder, ServerSchema, TokenKeys,
};
use connetto_server::defaults::ConnettoDefaults;
use connetto_server::openfga::{SetupError, Translated};
use connetto_server::{DefaultUuidResolver, Materializer, OidcProviderConfig, Registration};
use connetto_test_harness::{
    FIXTURE_READER, FIXTURE_ROLES, Fixture, MOCK_OAUTH_CLIENT_ID, MOCK_OAUTH_CLIENT_SECRET,
    MOCK_OAUTH_PROVIDER, MockOauth, OPLOG_TABLE, PUBLICATION, SLOT, isolated_session_keyring,
};
use tempfile::TempDir;

use super::e2e::{PG_DDL, PG_SERIAL, reset_fixture, signing_keys, with_user_url};
use super::lifecycle::BOUND;

mod unterminated {
    const AUDIT_DDL: &str = connetto_server::connetto_audit_ddl!("TEXT", "UUID");

    connetto_server::connetto_schema! {
        pub struct Deployment;
        id: String => diesel::sql_types::Text,
        audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
        audit_ddl: AUDIT_DDL.split_at(AUDIT_DDL.len() - 1).0,
    }
}

mod malformed {
    connetto_server::connetto_schema! {
        pub struct Deployment;
        id: String => diesel::sql_types::Text,
        audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
        audit_ddl: "CREATE TABLE auth_events (",
    }
}

#[test]
fn an_audit_declaration_without_a_final_delimiter_registers_receipts() {
    let schema = ServerSchema::new(PG_DDL, "", FIXTURE_ROLES, FIXTURE_READER);
    let catalog = schema
        .catalog::<unterminated::Deployment>()
        .expect("the deployment catalog is valid");
    let mut materializer = Materializer::builder(&catalog)
        .with_audit_schema::<unterminated::ConnettoAudit>()
        .build()
        .expect("the catalog builds");
    let receipt = materializer
        .register(
            1,
            "SELECT writer, seq FROM auth_events \
             WHERE op = 'write_couriered' AND writer = 7 AND seq = 8",
        )
        .expect("the private receipt query registers");
    assert!(matches!(receipt, Registration::Computed(_)));
    Translated::of::<String, unterminated::Deployment>(schema.documents(), false)
        .expect("authorization accepts the same audit declaration");
}

#[test]
fn malformed_documents_refuse_catalog_and_authorization_translation() {
    for (ddl, policies) in [
        ("CREATE TABLE orders (", ""),
        (PG_DDL, "CREATE POLICY orders_p ON orders USING ("),
    ] {
        let schema = ServerSchema::new(ddl, policies, FIXTURE_ROLES, FIXTURE_READER);
        assert!(matches!(
            schema.catalog::<ConnettoDefaults>(),
            Err(connetto_schema::SchemaError::Translate(_))
        ));
        assert!(matches!(
            Translated::of::<String, ConnettoDefaults>(schema.documents(), false),
            Err(SetupError::Catalog(_))
        ));
    }
}

async fn refusal_builder(fixture: &Fixture, policies: &str) -> (ServerBuilder, MockOauth) {
    reset_fixture(fixture.admin(), fixture).await;
    fixture.provision_auth_tables().await;
    let idp = MockOauth::start().await;
    let keys = TempDir::new().expect("a key directory");
    let (private, public) = signing_keys(&keys);
    let token = TokenKeys::from_pem(
        std::fs::read(private).expect("the private key"),
        std::fs::read(public).expect("the public key"),
    );
    let provider = OidcProvider::Generic(
        OidcProviderConfig::new(
            MOCK_OAUTH_PROVIDER,
            MOCK_OAUTH_CLIENT_ID,
            idp.issuer().to_owned(),
            "http://127.0.0.1:1/auth/callback",
        )
        .with_client_secret(Some(MOCK_OAUTH_CLIENT_SECRET.to_owned())),
    );
    let builder = ServerBuilder::new(
        Database::new(
            fixture.admin_url(),
            with_user_url(fixture.admin_url(), "app_reader", "app_reader"),
        ),
        ServerSchema::new(PG_DDL, policies, FIXTURE_ROLES, FIXTURE_READER),
        token,
        OpenFga::new("http://127.0.0.1:1", "unreached"),
    )
    .slot(SLOT)
    .publication(PUBLICATION)
    .oplog_table(OPLOG_TABLE)
    .oidc_providers(vec![provider]);
    (builder, idp)
}

#[tokio::test]
async fn a_malformed_audit_declaration_refuses_server_startup() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let (builder, _idp) = refusal_builder(&fixture, "").await;
    let refused = tokio::time::timeout(
        BOUND,
        builder
            .deployment_schema::<malformed::Deployment>(Arc::new(DefaultUuidResolver))
            .build(),
    )
    .await
    .expect("the build answers")
    .err()
    .expect("the malformed audit declaration refuses startup");
    assert!(matches!(
        refused,
        BuildError::Snapshot(connetto_server::SnapshotError::Catalog(_))
    ));
}

#[tokio::test]
async fn malformed_policies_refuse_server_startup() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let (builder, _idp) =
        refusal_builder(&fixture, "CREATE POLICY orders_p ON orders USING (").await;
    let refused = tokio::time::timeout(BOUND, builder.build())
        .await
        .expect("the build answers")
        .err()
        .expect("malformed policies refuse startup");
    assert!(matches!(
        refused,
        BuildError::Authorization(SetupError::Catalog(_))
    ));
}

#[tokio::test]
async fn an_unsupported_policy_refuses_server_startup() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let (builder, _idp) = refusal_builder(
        &fixture,
        "ALTER TABLE orders ENABLE ROW LEVEL SECURITY; \
         CREATE POLICY orders_p ON orders FOR ALL USING (mystery_function(status));",
    )
    .await;
    let refused = tokio::time::timeout(BOUND, builder.build())
        .await
        .expect("the build answers")
        .err()
        .expect("an unsupported policy refuses startup");
    assert!(matches!(
        refused,
        BuildError::Authorization(SetupError::Untranslated(_))
    ));
}
