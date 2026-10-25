//! The in-memory auth store is generic over the developer's distributed id
//! type. This drives it with `rosetta_uuid::Uuid` (the demo id) to prove the
//! resolved `AuthContext::user_id` is the typed value, not a string, and that
//! the same verified identity resolves to one id across logins while rotation
//! and revocation carry the typed id through.

use std::sync::Arc;

use connetto_server::{
    AuthConfig, AuthStore, IdentityResolver, InMemoryAuthStore, ResolveFuture, ResolvedIdentity,
    VerifiedClaims,
};
use rosetta_uuid::Uuid;

/// A fixed namespace for the deterministic `(issuer, subject)` to `Uuid`
/// mapping, standing in for the developer's `IdentityResolver`.
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

/// The deterministic v5 uuid over `issuer|subject`, mapped into the demo
/// `rosetta_uuid::Uuid`.
fn expected_id(subject: &str) -> Uuid {
    uuid::Uuid::new_v5(&NS, format!("https://issuer.example|{subject}").as_bytes()).into()
}

/// The typed resolver: the in-memory stand-in for a developer's
/// [`IdentityResolver`] over a typed distributed id.
struct TypedResolver;

impl IdentityResolver for TypedResolver {
    type Id = Uuid;

    fn resolve<'a>(&'a self, claims: &'a VerifiedClaims) -> ResolveFuture<'a, Uuid> {
        let id: Uuid = uuid::Uuid::new_v5(
            &NS,
            format!("{}|{}", claims.issuer, claims.subject).as_bytes(),
        )
        .into();
        Box::pin(async move { Ok(id) })
    }
}

fn store() -> InMemoryAuthStore<Uuid> {
    let config = AuthConfig::default();
    InMemoryAuthStore::<Uuid>::with_resolver(config.refresh_lifetimes(), Arc::new(TypedResolver))
}

#[tokio::test]
async fn in_memory_store_carries_a_typed_user_id() {
    let store = store();

    let first = store
        .create_session(&identity("dave"), origin())
        .await
        .expect("first session");
    let second = store
        .create_session(&identity("dave"), origin())
        .await
        .expect("second session");

    // The resolved user id is the typed value, not a string, and the same
    // verified identity resolves to one id across logins.
    let expected = expected_id("dave");
    assert_eq!(first.context.user_id, expected);
    assert_eq!(first.context.user_id, second.context.user_id);
    assert_ne!(first.session_id, second.session_id, "distinct sessions");

    // A different identity resolves to a different typed id.
    let other = store
        .create_session(&identity("erin"), origin())
        .await
        .expect("other session");
    assert_ne!(other.context.user_id, first.context.user_id);
}

#[tokio::test]
async fn typed_id_survives_rotation_and_revocation() {
    let store = store();

    let issued = store
        .create_session(&identity("frank"), origin())
        .await
        .expect("create");
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
        .revoke_session(issued.session_id)
        .await
        .expect("revoke");
    assert!(
        !store
            .session_is_live(issued.session_id)
            .await
            .expect("post-revoke live check")
    );
}

/// The in-memory store records how a session began and stamps each refresh,
/// as the database store does.
#[tokio::test]
async fn in_memory_store_records_how_a_session_began() {
    let store = store();
    let issued = store
        .create_session(&identity("gina"), origin())
        .await
        .expect("create");

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

/// The in-memory store lists one person's live sessions and ends one, all
/// or all but one, never another person's, as the database store does.
#[tokio::test]
async fn in_memory_store_lists_and_ends_one_persons_sessions() {
    let store = store();
    let mut hana = Vec::new();
    for _ in 0..3 {
        hana.push(
            store
                .create_session(&identity("hana"), origin())
                .await
                .expect("create"),
        );
    }
    let ivan = store
        .create_session(&identity("ivan"), origin())
        .await
        .expect("create");
    let user = expected_id("hana");
    let listed = |records: Vec<connetto_server::SessionRecord<Uuid, ()>>| {
        let mut ids: Vec<_> = records
            .into_iter()
            .map(|record| record.session_id)
            .collect();
        ids.sort_by_key(|id| *id.as_uuid());
        ids
    };
    let mut all: Vec<_> = hana.iter().map(|issued| issued.session_id).collect();
    all.sort_by_key(|id| *id.as_uuid());
    assert_eq!(listed(store.sessions_of(&user).await.expect("list")), all);

    assert!(
        !store.end_owned(&user, ivan.session_id).await.expect("end"),
        "another person's session ends nothing"
    );
    assert!(
        store
            .end_owned(&user, hana[0].session_id)
            .await
            .expect("end")
    );
    assert!(
        !store
            .end_owned(&user, hana[0].session_id)
            .await
            .expect("end"),
        "an ended session ends once"
    );
    assert_eq!(
        store
            .end_all_of(&user, Some(hana[2].session_id))
            .await
            .expect("end the others"),
        vec![hana[1].session_id]
    );
    assert_eq!(
        store.end_all_of(&user, None).await.expect("end all"),
        vec![hana[2].session_id]
    );
    assert_eq!(store.sessions_of(&user).await.expect("list"), Vec::new());
    assert!(
        store.session_is_live(ivan.session_id).await.expect("live"),
        "ivan's session is untouched"
    );
}
