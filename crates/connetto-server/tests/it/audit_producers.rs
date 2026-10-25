//! Every producer emits its own event kind, and a denial emits nothing.
//!
//! These run against the in-memory store with a capturing sink, so they assert
//! what connetto *emits* rather than what Postgres stores. The writing half is
//! `audit_table.rs`, which is Docker-gated. Splitting them keeps the assertion
//! that matters here, which `op` a producer chooses, free of database timing.

use std::sync::{Arc, Mutex};

use connetto_core::Subject;
use connetto_core::messages::Grant;
use connetto_server::audit::{AuditHook, AuthEvent, AuthOp};
use connetto_server::{
    AuthConfig, AuthService, InMemoryAuthStore, RequestGuard, ResolvedIdentity, SessionOrigin,
    SignInNotice, TokenAuthority, TokenPair,
};

/// Collects what connetto emitted, in order.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<AuthEvent<String>>>>);

impl Captured {
    fn hook(&self) -> AuditHook<String> {
        let sink = Arc::clone(&self.0);
        Arc::new(move |event| sink.lock().expect("sink").push(event))
    }

    fn ops(&self) -> Vec<AuthOp> {
        self.0.lock().expect("sink").iter().map(|e| e.op).collect()
    }

    /// The one event after the login row the fixture's sign-in recorded.
    fn only(&self) -> AuthEvent<String> {
        let events = self.0.lock().expect("sink");
        assert_eq!(
            events.first().map(|e| e.op),
            Some(AuthOp::LoggedIn),
            "the sign-in recorded its login first"
        );
        assert_eq!(
            events.len(),
            2,
            "expected exactly one event after the login, got {events:?}"
        );
        events[1].clone()
    }
}

fn service() -> (
    Arc<TokenAuthority>,
    Arc<AuthService<InMemoryAuthStore>>,
    Captured,
) {
    let config = AuthConfig::default();
    let authority = Arc::new(TokenAuthority::generate(&config).expect("generate keypair"));
    let store = Arc::new(InMemoryAuthStore::new(config.refresh_lifetimes()));
    let svc = Arc::new(AuthService::new(
        Arc::clone(&authority),
        store,
        Arc::new(RequestGuard::default()),
    ));
    let captured = Captured::default();
    svc.set_audit_hook(captured.hook());
    (authority, svc, captured)
}

/// The session a minted access token names.
fn session_of(authority: &TokenAuthority, access_token: &str) -> connetto_core::SessionId {
    let Subject::Identity(verified) = authority
        .check_grant::<String, String>(&Grant::new(access_token))
        .expect("verify")
        .subject
    else {
        panic!("expected identity subject");
    };
    verified.session_id
}

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

/// Sign `subject` in through the provider configured as `google`.
async fn login(svc: &AuthService<InMemoryAuthStore>, subject: &str) -> TokenPair<String> {
    svc.login(
        &identity(subject),
        SessionOrigin {
            provider: "google".to_owned(),
            description: (),
        },
    )
    .await
    .expect("login")
}

/// A sign-in is `logged_in`, naming the provider and issuer it came through
/// on the row itself, so the row reads on its own once the session row is
/// gone.
#[tokio::test]
async fn a_login_records_its_provider_and_issuer() {
    let (authority, svc, captured) = service();
    let pair = login(&svc, "erin").await;

    let events = captured.0.lock().expect("sink").clone();
    let [event] = events.as_slice() else {
        panic!("expected exactly the login row, got {events:?}");
    };
    assert_eq!(event.op, AuthOp::LoggedIn);
    assert_eq!(event.session, session_of(&authority, &pair.access_token));
    assert_eq!(event.user_id.as_ref(), Some(&pair.user_id));
    assert_eq!(event.provider.as_deref(), Some("google"));
    assert_eq!(event.issuer.as_deref(), Some("https://issuer.example"));
}

/// The application's sign-in callback hears of each sign-in with the
/// session it created, so it can tell the person.
#[tokio::test]
async fn a_sign_in_tells_the_application() {
    let (authority, svc, _captured) = service();
    let told = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&told);
    svc.set_sign_in_hook(Arc::new(move |notice| {
        sink.lock().expect("sink").push(notice);
    }));
    let before = std::time::SystemTime::now();
    let pair = login(&svc, "frank").await;

    let told = told.lock().expect("sink").clone();
    let [SignInNotice::SignedIn(session)] = told.as_slice() else {
        panic!("expected one sign-in notice, got {told:?}");
    };
    assert_eq!(
        session.session_id,
        session_of(&authority, &pair.access_token)
    );
    assert_eq!(session.user_id, pair.user_id);
    assert_eq!(session.provider, "google");
    assert_eq!(session.issuer, "https://issuer.example");
    assert!(
        session.created_at >= before,
        "the notice carries the sign-in time"
    );
    assert_eq!(session.refreshed_at, None);
}

/// A caller ending their own login is `logged_out`, not the generic revocation.
#[tokio::test]
async fn logout_records_logged_out() {
    let (_authority, svc, captured) = service();
    let pair = login(&svc, "alice").await;

    assert!(svc.logout(&pair.refresh_token).await.expect("logout"));

    let event = captured.only();
    assert_eq!(event.op, AuthOp::LoggedOut);
    assert_eq!(event.table_name, None, "a logout names no row");
    assert_eq!(event.pk, None, "a logout names no row");
}

/// The application revoking a login itself is `session_revoked`.
#[tokio::test]
async fn an_application_revoke_records_session_revoked() {
    let (authority, svc, captured) = service();
    let pair = login(&svc, "bob").await;
    let session = session_of(&authority, &pair.access_token);

    svc.revoke(session).await.expect("revoke");

    let event = captured.only();
    assert_eq!(event.op, AuthOp::SessionRevoked);
    assert_eq!(event.session, session);
}

/// The theft defence is `token_replayed`, and that is the whole point of the
/// split: it must not look like an ordinary logout.
#[tokio::test]
async fn token_reuse_records_token_replayed() {
    let (_authority, svc, captured) = service();
    let pair = login(&svc, "mallory").await;
    svc.refresh(&pair.refresh_token).await.expect("rotate");

    assert!(
        svc.refresh(&pair.refresh_token).await.is_err(),
        "a replayed refresh token is refused"
    );

    let event = captured.only();
    assert_eq!(event.op, AuthOp::TokenReplayed);
    assert_ne!(
        event.op,
        AuthOp::LoggedOut,
        "a stolen credential must be distinguishable from a logout"
    );
}

/// A logout naming no live session changes nothing, so it records nothing.
///
/// The endpoint deliberately answers the same either way, so without this the
/// table would leak whether a guessed token named a real session, turning an
/// audit row into the oracle the endpoint refuses to be.
#[tokio::test]
async fn a_logout_that_revoked_nothing_records_nothing() {
    let (_authority, svc, captured) = service();
    let _ = login(&svc, "carol").await;

    assert!(
        !svc.logout("connetto-not-a-real-token")
            .await
            .expect("logout"),
        "the token names no live session"
    );

    assert_eq!(
        captured.ops(),
        [AuthOp::LoggedIn],
        "nothing changed after the login, so nothing more is recorded"
    );
}

/// A refused login records nothing.
///
/// Denials go to structured logging by the split in `08-authorization.md`,
/// because a caller probing generates one per attempt. This is the half of the
/// split a future change is most likely to break.
#[tokio::test]
async fn denials_never_reach_the_audit_table() {
    let (_authority, svc, captured) = service();
    let pair = login(&svc, "dave").await;
    svc.logout(&pair.refresh_token).await.expect("logout");
    let before = captured.ops();
    assert_eq!(before, vec![AuthOp::LoggedIn, AuthOp::LoggedOut]);

    // Every refusal available on this surface: a rotated token for a dead
    // session, a refresh of a token that never existed, and a second logout.
    assert!(svc.refresh(&pair.refresh_token).await.is_err());
    assert!(svc.refresh("connetto-nonsense").await.is_err());
    assert!(!svc.logout(&pair.refresh_token).await.expect("logout"));

    assert_eq!(
        captured.ops(),
        before,
        "a refusal is a denial and belongs in the log, not this table"
    );
}
