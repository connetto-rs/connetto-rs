//! The database auth store preserves a typed `rosetta_uuid::Uuid` user id, the
//! id type the demos share between the client's SQLite replica and the
//! server's Postgres tables, across login, rotation, and revocation.

use std::sync::Arc;

use connetto_server::{
    AuthConfig, AuthHooks, DbAuthStore, EndCause, HookFuture, IssuedSession, ResolvedIdentity,
    VerifiedClaims,
};
use connetto_test_harness::Fixture;
use diesel_async::AsyncPgConnection;
use rosetta_uuid::Uuid;

connetto_server::connetto_schema! {
    pub struct TypedSchema;
    id: Uuid => rosetta_uuid::sql_types::Uuid,
    audit_row_key: Uuid => rosetta_uuid::sql_types::Uuid,
    audit_ddl: connetto_server::connetto_audit_ddl!("UUID", "UUID"),
}

/// A fixed namespace for the deterministic `(issuer, subject)` to `Uuid`
/// mapping, standing in for the developer's `AuthHooks::user_for`.
const NS: uuid::Uuid = uuid::Uuid::from_u128(0x2b7e_1516_28ae_d2a6_abf7_1588_09cf_4f3c);

fn identity(subject: &str) -> ResolvedIdentity {
    ResolvedIdentity {
        issuer: "https://issuer.example".to_owned(),
        subject: subject.to_owned(),
        email: None,
        name: None,
        amr: Vec::new(),
        acr: None,
    }
}

/// A sign-in through the provider configured as `test`.
fn origin() -> connetto_server::SessionOrigin<()> {
    connetto_server::SessionOrigin {
        provider: "test".to_owned(),
        description: (),
    }
}

/// The deterministic v5 UUID over `issuer|subject`.
fn expected_id(subject: &str) -> Uuid {
    uuid::Uuid::new_v5(&NS, format!("https://issuer.example|{subject}").as_bytes()).into()
}

/// Resolves verified identities to typed UUIDs.
struct TypedHooks;

impl AuthHooks<TypedSchema> for TypedHooks {
    fn user_for<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, Uuid> {
        let id: Uuid = uuid::Uuid::new_v5(
            &NS,
            format!("{}|{}", claims.issuer, claims.subject).as_bytes(),
        )
        .into();
        Box::pin(async move { Ok(id) })
    }
}

async fn store() -> (Fixture, DbAuthStore<TypedSchema>) {
    let fixture = Fixture::acquire().await;
    fixture.provision_auth_tables_over("UUID").await;
    let store = DbAuthStore::new(
        fixture.admin().clone(),
        AuthConfig::default().refresh_lifetimes(),
        Arc::new(TypedHooks),
    );
    (fixture, store)
}

/// Sign `subject` in on `store`, retaining no provider token and reading no
/// ban list.
async fn sign_in(store: &DbAuthStore<TypedSchema>, subject: &str) -> IssuedSession<Uuid> {
    store
        .sign_in(&identity(subject), origin(), None, None)
        .await
        .expect("sign in")
        .0
}

#[tokio::test]
async fn database_store_carries_a_typed_user_id() {
    let (_fixture, store) = store().await;

    let first = sign_in(&store, "dave").await;
    let second = sign_in(&store, "dave").await;

    // The resolved user id is the typed value, not a string, and the same
    // verified identity resolves to one id across logins.
    let expected = expected_id("dave");
    assert_eq!(first.context.user_id, expected);
    assert_eq!(first.context.user_id, second.context.user_id);
    assert_ne!(first.session_id, second.session_id, "distinct sessions");

    // A different identity resolves to a different typed id.
    let other = sign_in(&store, "erin").await;
    assert_ne!(other.context.user_id, first.context.user_id);
}

#[tokio::test]
async fn typed_id_survives_rotation_and_revocation() {
    let (_fixture, store) = store().await;

    let issued = sign_in(&store, "frank").await;
    assert!(
        store
            .session_is_live(issued.session_id)
            .await
            .expect("live check")
    );

    let rotated = store
        .rotate_refresh(&issued.refresh_token)
        .await
        .expect("rotate");
    assert_eq!(rotated.context.user_id, issued.context.user_id);
    assert_ne!(rotated.refresh_token, issued.refresh_token, "token rotates");

    store
        .revoke_session(issued.session_id, EndCause::Revoked)
        .await
        .expect("revoke");
    assert!(
        !store
            .session_is_live(issued.session_id)
            .await
            .expect("post-revoke live check")
    );
}

/// The database store records how a session began and stamps each refresh.
#[tokio::test]
async fn database_store_records_how_a_session_began() {
    let (_fixture, store) = store().await;
    let issued = sign_in(&store, "gina").await;

    let began = store
        .session_record(issued.session_id)
        .await
        .expect("read")
        .expect("the session is recorded");
    assert_eq!(began.user_id, expected_id("gina"));
    assert_eq!(began.provider, "test");
    assert_eq!(began.issuer, "https://issuer.example");
    assert_eq!(began.created_at, issued.created_at);
    assert_eq!(began.refreshed_at, None, "never refreshed yet");

    store
        .rotate_refresh(&issued.refresh_token)
        .await
        .expect("rotate");
    let refreshed = store
        .session_record(issued.session_id)
        .await
        .expect("read")
        .expect("the session is recorded")
        .refreshed_at
        .expect("the refresh is stamped");
    assert!(refreshed >= began.created_at);
}

/// The database store lists and ends only the typed user's sessions.
#[tokio::test]
async fn database_store_lists_and_ends_one_persons_sessions() {
    let (_fixture, store) = store().await;
    let mut hana = Vec::new();
    for _ in 0..3 {
        hana.push(sign_in(&store, "hana").await.session_id);
    }
    let ivan = sign_in(&store, "ivan").await;
    super::session_contract::ends_only_the_persons_sessions(
        &store,
        &expected_id("hana"),
        [hana[0], hana[1], hana[2]],
        ivan.session_id,
    )
    .await;
}
