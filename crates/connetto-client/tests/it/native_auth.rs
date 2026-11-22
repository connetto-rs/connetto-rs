//! Native acquisition.
//!
//! Serves connetto-server's auth router on a real loopback port with a
//! containerised OIDC provider, then drives the full native flow: the
//! authenticator binds its own loopback listener and opens a browser, a fake
//! browser walks the redirect chain and delivers the code to that listener, the
//! authenticator exchanges the code with its PKCE verifier, stores the refresh
//! token, and silently refreshes.
//!
//! It also proves that a typed `rosetta_uuid::Uuid` identity, the one id type
//! the server's Postgres tables and the client's SQLite replica share, crosses
//! the token endpoint as itself and names the client's replica file.

use std::sync::Arc;

use connetto_client::{
    AccountChoice, AccountChooser, AcquiredSession, Auth, AuthorizationSession, BrowserOpener,
    ClientError, Custody, Gate, Grant, MemoryKeyStore, MemoryRefreshStore, NativeAuthenticator,
    NativeClientBuilder, SessionFuture, StoredAuth, encode_identity, provision_replica_key,
    replica_db_name,
};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::{
    GrantRefused, HandshakeAuthority, RefreshFuture, RefreshTokenStore, ReplicaKeyStore,
};
use connetto_server::{
    AuthConfig, AuthHooks, AuthService, DbAuthStore, GenericOidcProvider, HookFuture,
    ProviderRegistry, RedirectPolicy, RequestGuard, TokenAuthority, VerifiedClaims, auth_router,
};
use connetto_test_harness::{ConnettoDefaults, Fixture, MOCK_OAUTH_PROVIDER, MockOauth};
use diesel_async::AsyncPgConnection;
use openidconnect::reqwest;
use rosetta_uuid::Uuid;

connetto_server::connetto_schema! {
    struct TypedAuthSchema;
    id: Uuid => rosetta_uuid::sql_types::Uuid,
    audit_row_key: Uuid => rosetta_uuid::sql_types::Uuid,
    audit_ddl: connetto_server::connetto_audit_ddl!("UUID", "UUID"),
}

/// A fixed namespace for the deterministic `subject` to `Uuid` mapping,
/// standing in for an app's own [`AuthHooks::user_for`].
const NS: uuid::Uuid = uuid::Uuid::from_u128(0x2b7e_1516_28ae_d2a6_abf7_1588_09cf_4f3c);

/// The typed id the hooks below answer for `subject`.
///
/// Keys on the subject alone because the test idp mints a fresh issuer per run.
fn typed_id(subject: &str) -> Uuid {
    uuid::Uuid::new_v5(&NS, subject.as_bytes()).into()
}

/// Hooks mapping verified claims to their own typed id.
struct TypedHooks;

impl AuthHooks<TypedAuthSchema> for TypedHooks {
    fn user_for<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, Uuid> {
        // The test provider mints a fresh issuer per run.
        let id = typed_id(&claims.subject);
        Box::pin(async move { Ok(id) })
    }
}

/// Serve the auth router and retain its provider and database fixtures.
async fn spawn_auth_server() -> (String, MockOauth, Fixture) {
    let (base, _service, idp, fixture) = spawn_auth_server_with_service(Vec::new()).await;
    (base, idp, fixture)
}

/// As [`spawn_auth_server`], also handing back the service so a test can ask the
/// real handshake verifier what it makes of a token.
async fn spawn_auth_server_with_service(
    redirect_allowlist: Vec<String>,
) -> (
    String,
    Arc<AuthService<ConnettoDefaults>>,
    MockOauth,
    Fixture,
) {
    let fixture = Fixture::acquire().await;
    // Bind connetto's listener first so the callback URL is known.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind auth server");
    let port = listener.local_addr().expect("addr").port();
    let base = format!("http://127.0.0.1:{port}");
    let callback = format!("{base}/auth/callback");

    let idp = MockOauth::start().await;
    let provider = GenericOidcProvider::discover(
        idp.oidc_config(MOCK_OAUTH_PROVIDER, callback),
        reqwest::Client::new(),
    )
    .await
    .expect("discover provider");

    let config = AuthConfig::default();
    let authority = Arc::new(TokenAuthority::generate(&config).expect("keypair"));
    let store = fixture.auth_store(config.refresh_lifetimes()).await;
    let service = Arc::new(AuthService::new(
        authority,
        store,
        Arc::new(RequestGuard::default()),
    ));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(provider));
    let router = auth_router(
        Arc::clone(&service),
        Arc::new(registry),
        RedirectPolicy::new(redirect_allowlist),
        connetto_server::CookieSameSite::Strict,
    );
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    (base, service, idp, fixture)
}

/// Serve an auth router with `rosetta_uuid::Uuid` identities and retain its
/// fixtures.
async fn spawn_typed_auth_server() -> (String, MockOauth, Fixture) {
    let fixture = Fixture::acquire().await;
    fixture.provision_auth_tables_over("UUID").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind typed auth server");
    let port = listener.local_addr().expect("addr").port();
    let base = format!("http://127.0.0.1:{port}");
    let callback = format!("{base}/auth/callback");

    let config = AuthConfig::default();
    let authority = Arc::new(TokenAuthority::generate(&config).expect("keypair"));
    let store = Arc::new(DbAuthStore::<TypedAuthSchema>::new(
        fixture.admin().clone(),
        config.refresh_lifetimes(),
        Arc::new(TypedHooks),
    ));
    let service = Arc::new(AuthService::new(
        authority,
        store,
        Arc::new(RequestGuard::default()),
    ));
    let mut registry = ProviderRegistry::new();

    let idp = MockOauth::start().await;
    let provider = GenericOidcProvider::discover(
        idp.oidc_config(MOCK_OAUTH_PROVIDER, callback),
        reqwest::Client::new(),
    )
    .await
    .expect("discover provider");
    registry.register(Arc::new(provider));

    let router = auth_router(
        service,
        Arc::new(registry),
        RedirectPolicy::default(),
        connetto_server::CookieSameSite::Strict,
    );
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    (base, idp, fixture)
}

/// Drive the full loopback login against `base` as `subject`.
async fn login_as(base: &str, subject: &str) -> AcquiredSession<Uuid> {
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    NativeAuthenticator::new(base.to_owned(), MOCK_OAUTH_PROVIDER, store, None)
        .with_browser_opener(fake_browser(subject))
        .login::<Uuid>()
        .await
        .expect("typed login")
}

/// The refresh store a [`NativeAuthenticator`] holds, spelled once.
type SharedRefresh = Arc<dyn RefreshTokenStore<Error = ClientError> + Send + Sync>;

/// A fake browser: given connetto's login URL, walk the real OIDC redirect
/// chain until the authenticator's loopback listener receives the code.
fn fake_browser(subject: &str) -> BrowserOpener {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("http client");
    let subject = subject.to_owned();
    Arc::new(move |login_url: &str| {
        let http = http.clone();
        let subject = subject.clone();
        let login_url = login_url.to_owned();
        tokio::spawn(async move {
            let mut next_url = login_url;
            let mut submitted = false;
            for _ in 0..6 {
                let resp = http.get(&next_url).send().await.expect("hop");
                if !submitted && resp.url().path().ends_with("/authorize") {
                    let form_url = resp.url().to_string();
                    let posted = http
                        .post(form_url)
                        .form(&[("username", subject.as_str())])
                        .send()
                        .await
                        .expect("submit login form");
                    submitted = true;
                    let Some(loc) = posted.headers().get("location") else {
                        break;
                    };
                    loc.to_str()
                        .expect("utf8 location")
                        .clone_into(&mut next_url);
                    continue;
                }
                match resp.headers().get("location") {
                    Some(loc) => {
                        loc.to_str()
                            .expect("utf8 location")
                            .clone_into(&mut next_url);
                    }
                    None => break,
                }
            }
        });
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_login_refreshes_and_silently_reacquires() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _idp, _fixture) = spawn_auth_server().await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());

    // Interactive login: no account stored yet, so nothing to try silently.
    let login =
        NativeAuthenticator::new(base.clone(), MOCK_OAUTH_PROVIDER, Arc::clone(&store), None)
            .with_browser_opener(fake_browser("native-user"))
            .login::<String>()
            .await
            .expect("login");
    assert!(!login.access_token.is_empty(), "access token acquired");
    assert!(!login.user_id.is_empty(), "login carries a user_id");
    assert!(
        login.session_expires_at > std::time::SystemTime::now(),
        "session expiry is in the future"
    );

    // Login stores the credential under the encoded user id, not a literal.
    let encoded_account = encode_identity(&login.user_id).expect("encode account");
    let first_refresh = store
        .load(&encoded_account)
        .await
        .expect("load")
        .expect("refresh stored");

    // Subsequent operations know which account to address.
    let authenticator = Arc::new(
        NativeAuthenticator::new(
            base.clone(),
            MOCK_OAUTH_PROVIDER,
            Arc::clone(&store),
            Some(encoded_account.clone()),
        )
        .with_browser_opener(fake_browser("native-user")),
    );

    // A silent refresh rotates the stored refresh token and keeps the identity.
    let refreshed = authenticator
        .refresh_access::<String>()
        .await
        .expect("refresh");
    assert!(!refreshed.access_token.is_empty(), "refreshed access token");
    assert_eq!(refreshed.user_id, login.user_id, "identity is continuous");
    let second_refresh = store
        .load(&encoded_account)
        .await
        .expect("load")
        .expect("refresh stored");
    assert_ne!(first_refresh, second_refresh, "refresh token rotated");

    // The token source refreshes without a browser, yielding the raw token.
    let via_source = authenticator.token_source().token().await.expect("source");
    assert!(
        !via_source.is_empty(),
        "token source yields an access token"
    );

    // A fresh authenticator sharing the store silently reacquires via refresh,
    // never opening the browser (the opener panics if called).
    let panicking: BrowserOpener =
        Arc::new(|_url: &str| panic!("browser opened during silent reacquire"));
    let silent = NativeAuthenticator::new(
        base,
        MOCK_OAUTH_PROVIDER,
        Arc::clone(&store),
        Some(encoded_account),
    )
    .with_browser_opener(panicking);
    let session = silent.acquire::<String>().await.expect("silent acquire");
    assert!(
        !session.access_token.is_empty(),
        "silent acquire returns an access token"
    );
    assert_eq!(session.user_id, login.user_id, "same identity on reacquire");
}

/// A first run has no remembered account, and the login is what reveals it.
/// The same authenticator's token source then has to refresh for that account,
/// because it is what the session's first reconnect asks for a fresh grant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_login_refreshes_for_its_own_reconnect() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _idp, _fixture) = spawn_auth_server().await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let authenticator = Arc::new(
        NativeAuthenticator::new(base, MOCK_OAUTH_PROVIDER, Arc::clone(&store), None)
            .with_browser_opener(fake_browser("first-run-user")),
    );
    let login = authenticator.login::<String>().await.expect("login");
    let account = encode_identity(&login.user_id).expect("encode account");
    let issued = store
        .load(&account)
        .await
        .expect("load")
        .expect("refresh stored");

    let token = authenticator
        .token_source()
        .token()
        .await
        .expect("the token source refreshes for the account the login revealed");
    assert!(!token.is_empty(), "a fresh access token");
    let rotated = store
        .load(&account)
        .await
        .expect("load")
        .expect("refresh stored");
    assert_ne!(issued, rotated, "the refresh rotated that account's token");
}

/// A typed `rosetta_uuid::Uuid` identity round-trips from server to client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_typed_user_id_round_trips_and_names_the_replica() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _idp, _fixture) = spawn_typed_auth_server().await;

    let alice = login_as(&base, "alice").await;
    let bob = login_as(&base, "bob").await;

    // The id arrives as the app's own type, carrying the exact value the
    // resolver minted rather than a re-parsed rendering of it.
    assert_eq!(alice.user_id, typed_id("alice"), "alice's typed id");
    assert_eq!(bob.user_id, typed_id("bob"), "bob's typed id");
    assert_ne!(alice.user_id, bob.user_id, "distinct identities");

    // That typed id, not a Display rendering of it, names the replica file.
    let alice_replica = replica_db_name("app.db", &alice.user_id).expect("alice replica");
    let bob_replica = replica_db_name("app.db", &bob.user_id).expect("bob replica");
    assert_ne!(
        alice_replica, bob_replica,
        "an account switch selects a different replica file",
    );
    assert_eq!(
        alice_replica,
        replica_db_name("app.db", &typed_id("alice")).expect("stable"),
        "one identity always returns to the same replica",
    );
}

/// Phase E1 acceptance on the native path, against the real auth router, as E3
/// re-shaped it: the key is minted on the device rather than carried by the
/// login response.
///
/// A first login mints a per-replica key and caches it, a later login resolving
/// the same identity keeps the cached key rather than minting another, a cold
/// start with no server in reach still resolves it, and two identities on one
/// device stay isolated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_key_is_provisioned_once_and_cached_per_identity() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _idp, _fixture) = spawn_typed_auth_server().await;
    let keys = MemoryKeyStore::default();

    let alice = login_as(&base, "alice").await;
    let alice_replica = replica_db_name("app.db", &alice.user_id).expect("alice replica");

    // First sight of this replica: a key is minted locally and written through.
    let provisioned = provision_replica_key(&keys, &alice_replica)
        .await
        .expect("a key is minted on first login");
    assert_eq!(
        keys.load(&alice_replica).await.expect("load"),
        Some(provisioned.clone()),
        "the minted key is cached for a later cold start",
    );

    // A second login for the same identity changes nothing about the key: the
    // server has no say in it, and provision-once means the cached key wins,
    // which is what stops a re-login from stranding the replica.
    let again = login_as(&base, "alice").await;
    assert_eq!(
        replica_db_name("app.db", &again.user_id).expect("replica"),
        alice_replica,
    );
    let effective = provision_replica_key(&keys, &alice_replica)
        .await
        .expect("resolve");
    assert_eq!(
        effective, provisioned,
        "the cached key survives a second login",
    );

    // The offline property: nothing but the local store is consulted, so the
    // replica opens with no valid credential and no network.
    let offline = keys
        .load(&alice_replica)
        .await
        .expect("load")
        .expect("the cached key reads back");
    assert_eq!(offline, provisioned, "an offline cold start reads it back");

    // A second identity on the same device gets its own key and its own
    // record, so neither can read the other's replica.
    let bob = login_as(&base, "bob").await;
    let bob_replica = replica_db_name("app.db", &bob.user_id).expect("bob replica");
    let bob_key = provision_replica_key(&keys, &bob_replica)
        .await
        .expect("bob is provisioned too");
    assert_ne!(bob_key, provisioned, "identities do not share a key");
    assert_eq!(
        keys.load(&alice_replica).await.expect("load"),
        Some(provisioned),
        "bob's login leaves alice's cached key untouched",
    );
}

/// Phase E3 acceptance, credential teardown against the real auth router.
///
/// A logout revokes the session server-side, which is the half a local clear
/// cannot give: the next handshake is refused even though the access token it was
/// minted with is still inside its own lifetime and still verifies by signature.
/// The stored refresh token is gone too, and a copy of it kept elsewhere is dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_logout_revokes_the_session_and_clears_the_local_credential() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, service, _idp, _fixture) = spawn_auth_server_with_service(Vec::new()).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());

    let login =
        NativeAuthenticator::new(base.clone(), MOCK_OAUTH_PROVIDER, Arc::clone(&store), None)
            .with_browser_opener(fake_browser("native-user"))
            .login::<String>()
            .await
            .expect("login");

    // Login stores the credential under the encoded user id.
    let encoded_account = encode_identity(&login.user_id).expect("encode account");
    let refresh = store
        .load(&encoded_account)
        .await
        .expect("load")
        .expect("the refresh token is stored");

    // For logout and further operations, build an authenticator that knows the account.
    let authenticator = NativeAuthenticator::new(
        base.clone(),
        MOCK_OAUTH_PROVIDER,
        Arc::clone(&store),
        Some(encoded_account.clone()),
    )
    .with_browser_opener(fake_browser("native-user"));

    // Before the logout the real handshake verifier accepts this token.
    let concrete = service.handshake_authority();
    let authority: &dyn HandshakeAuthority = &concrete;
    authority
        .check_grant(&Grant::new(login.access_token.clone()))
        .await
        .expect("a live session verifies");

    authenticator.logout().await.expect("logout");

    // Local state is gone, so nothing on this device can silently reacquire.
    assert_eq!(
        store.load(&encoded_account).await.expect("load"),
        None,
        "the refresh token is cleared",
    );

    // Server-side liveness is gone, which is what makes the logout mean
    // something: the access token is inside its 15 minute default TTL and its
    // signature still checks out, and the handshake refuses it anyway.
    match authority
        .check_grant(&Grant::new(login.access_token.clone()))
        .await
    {
        Err(GrantRefused::Revoked) => {}
        Err(other) => panic!("expected Revoked, got {other:?}"),
        Ok(_) => panic!("a logged-out session must be refused at the next handshake"),
    }

    // A copy of the refresh token kept anywhere else is dead too, so the session
    // cannot be resurrected into a fresh access token.
    let kept = MemoryRefreshStore::default();
    kept.store(&encoded_account, &refresh)
        .await
        .expect("seed the copy");
    let resurrect = NativeAuthenticator::new(
        base,
        MOCK_OAUTH_PROVIDER,
        Arc::new(kept),
        Some(encoded_account),
    )
    .with_browser_opener(Arc::new(|_url: &str| {
        panic!("no browser during a refresh attempt")
    }));
    match resurrect.refresh_access::<String>().await {
        Err(ClientError::Auth(_)) => {}
        Err(other) => panic!("expected a rejected credential, got {other:?}"),
        Ok(_) => panic!("a revoked session must not rotate its refresh token"),
    }

    // Idempotent: with nothing stored there is nothing to revoke.
    authenticator
        .logout()
        .await
        .expect("a second logout is a no-op");
}

/// Signing out of the session a first run just created revokes it, because the
/// login told the authenticator which account it now holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_login_logout_revokes_its_own_session() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, service, _idp, _fixture) = spawn_auth_server_with_service(Vec::new()).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let authenticator =
        NativeAuthenticator::new(base, MOCK_OAUTH_PROVIDER, Arc::clone(&store), None)
            .with_browser_opener(fake_browser("first-run-user"));
    let login = authenticator.login::<String>().await.expect("login");
    let account = encode_identity(&login.user_id).expect("encode account");

    authenticator.logout().await.expect("logout");

    assert_eq!(
        store.load(&account).await.expect("load"),
        None,
        "the refresh token is cleared"
    );
    let concrete = service.handshake_authority();
    let authority: &dyn HandshakeAuthority = &concrete;
    match authority.check_grant(&Grant::new(login.access_token)).await {
        Err(GrantRefused::Revoked) => {}
        Err(other) => panic!("expected Revoked, got {other:?}"),
        Ok(_) => panic!("the first run's session must be refused after its logout"),
    }
}

/// Offline logout, which decision three of phase E3 settles: the local clear
/// happens even when the revoke cannot be delivered, and the failure is reported
/// rather than swallowed, so the application knows the session stays live until it
/// expires on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offline_logout_still_clears_local_state_and_says_the_revoke_failed() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    // The encoded form of the id the mock provider would return on a real login.
    let encoded_account = encode_identity("native-user").expect("encode offline account");
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    store
        .store(&encoded_account, "session-id.secret")
        .await
        .expect("seed a credential");
    // Port 1 is reserved and nothing listens there, which is this test's stand-in
    // for a device with no connectivity.
    let authenticator = NativeAuthenticator::new(
        "http://127.0.0.1:1",
        MOCK_OAUTH_PROVIDER,
        Arc::clone(&store),
        Some(encoded_account.clone()),
    );

    match authenticator.logout().await {
        Err(ClientError::Transport(_)) => {}
        Err(other) => panic!("expected a transport failure, got {other:?}"),
        Ok(()) => panic!("an unreachable server must not report a successful revoke"),
    }
    assert_eq!(
        store.load(&encoded_account).await.expect("load"),
        None,
        "the credential is cleared even when the revoke never landed",
    );
}

#[tokio::test]
async fn memory_refresh_store_round_trips() {
    let store = MemoryRefreshStore::default();
    assert!(
        store.load("any-key").await.unwrap().is_none(),
        "empty at first"
    );
    store.store("any-key", "refresh-abc").await.unwrap();
    assert_eq!(
        store.load("any-key").await.unwrap().as_deref(),
        Some("refresh-abc")
    );
    store.store("any-key", "refresh-def").await.unwrap();
    assert_eq!(
        store.load("any-key").await.unwrap().as_deref(),
        Some("refresh-def"),
        "replaces"
    );
    store.clear("any-key").await.unwrap();
    assert!(store.load("any-key").await.unwrap().is_none(), "cleared");
}

/// The redirect a mobile build registers with its operating system.
const APP_REDIRECT: &str = "dev.connetto.test:/oauth2redirect";

/// Walk the login chain as `subject` and return the redirect the server sends
/// to `redirect`, where the operating system would hand the app its code.
async fn walk_to_app_redirect(login_url: String, subject: &str, redirect: &str) -> String {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("http client");
    let mut next = login_url;
    for _ in 0..6 {
        if next.starts_with(redirect) {
            return next;
        }
        let response = if next.ends_with("/authorize") || next.contains("/authorize?") {
            http.post(&next)
                .form(&[("username", subject)])
                .send()
                .await
                .expect("submit the login form")
        } else {
            http.get(&next).send().await.expect("follow a hop")
        };
        response
            .headers()
            .get("location")
            .expect("every hop redirects")
            .to_str()
            .expect("utf8 location")
            .clone_into(&mut next);
    }
    panic!("the login never reached {redirect}");
}

/// A scripted browser session. `authorize` walks the login as `subject` and,
/// with `die` set, records the redirect in `delivered` and never returns, as a
/// process the system killed would. `delivered` hands back what an earlier
/// process received.
struct ScriptedSession {
    subject: &'static str,
    die: bool,
    forge_state: bool,
    delivered: Arc<std::sync::Mutex<Option<String>>>,
    opened: Arc<std::sync::atomic::AtomicUsize>,
}

impl ScriptedSession {
    fn new(subject: &'static str) -> Self {
        Self {
            subject,
            die: false,
            forge_state: false,
            delivered: Arc::default(),
            opened: Arc::default(),
        }
    }
}

impl AuthorizationSession for ScriptedSession {
    fn authorize(&self, url: String) -> SessionFuture {
        self.opened
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (subject, die, forge) = (self.subject, self.die, self.forge_state);
        let delivered = Arc::clone(&self.delivered);
        Box::pin(async move {
            let mut redirect = walk_to_app_redirect(url, subject, APP_REDIRECT).await;
            if forge {
                redirect = forged_state(&redirect);
            }
            if die {
                *delivered.lock().expect("slot") = Some(redirect);
                return std::future::pending().await;
            }
            Ok(redirect)
        })
    }

    fn delivered(&self) -> Option<String> {
        self.delivered.lock().expect("slot").take()
    }
}

/// `redirect` with its `state` replaced by one no login sent.
fn forged_state(redirect: &str) -> String {
    let (before, after) = redirect.split_once("state=").expect("a state");
    let rest = after.split_once('&').map_or("", |(_, rest)| rest);
    format!("{before}state=forged&{rest}")
}

fn claimed(
    base: &str,
    store: &SharedRefresh,
    session: Arc<dyn AuthorizationSession>,
) -> NativeAuthenticator {
    let panicking: BrowserOpener =
        Arc::new(|_url: &str| panic!("a claimed redirect never opens the loopback browser"));
    NativeAuthenticator::new(
        base.to_owned(),
        MOCK_OAUTH_PROVIDER,
        Arc::clone(store),
        None,
    )
    .with_browser_opener(panicking)
    .with_claimed_redirect(APP_REDIRECT, session)
}

/// A mobile build signs in through its own redirect, which the server allows
/// by exact match, and the app's session hands back the URL carrying the code.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claimed_redirect_login_completes_through_the_apps_session() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _service, _idp, _fixture) =
        spawn_auth_server_with_service(vec![APP_REDIRECT.to_owned()]).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let authenticator = Arc::new(claimed(
        &base,
        &store,
        Arc::new(ScriptedSession::new("mobile-user")),
    ));

    let login = authenticator.login::<String>().await.expect("login");

    let account = encode_identity(&login.user_id).expect("encode account");
    assert!(
        store.load(&account).await.expect("load").is_some(),
        "the refresh token is stored for the account the login revealed"
    );
    assert_eq!(
        store.accounts().await.expect("accounts"),
        vec![account],
        "a finished login lists only the account"
    );
    assert_eq!(
        store.load("connetto-pending-login").await.expect("load"),
        None,
        "a finished login leaves no pending record"
    );
    let token = authenticator.token_source().token().await.expect("refresh");
    assert!(!token.is_empty(), "the session refreshes like any other");
}

/// A callback whose `state` is not the one this login sent is refused, so a
/// redirect planted by another party cannot complete somebody else's login.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claimed_redirect_with_a_foreign_state_is_refused() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _service, _idp, _fixture) =
        spawn_auth_server_with_service(vec![APP_REDIRECT.to_owned()]).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let mut session = ScriptedSession::new("mobile-user");
    session.forge_state = true;
    let authenticator = claimed(&base, &store, Arc::new(session));

    match authenticator.login::<String>().await {
        Err(ClientError::Auth(message)) => assert!(message.contains("state"), "{message}"),
        other => panic!(
            "expected a state refusal, got {:?}",
            other.map(|s| s.user_id)
        ),
    }
}

/// The system may kill the app while the login is open in the browser tab.
/// The redirect then starts a fresh process, which finishes that same login
/// from the persisted verifier without opening another tab.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_login_finishes_in_the_process_its_redirect_restarts() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _service, _idp, _fixture) =
        spawn_auth_server_with_service(vec![APP_REDIRECT.to_owned()]).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let mut dying = ScriptedSession::new("mobile-user");
    dying.die = true;
    let delivered = Arc::clone(&dying.delivered);
    let first = claimed(&base, &store, Arc::new(dying));
    let killed =
        tokio::time::timeout(std::time::Duration::from_secs(10), first.login::<String>()).await;
    assert!(
        killed.is_err(),
        "the first process never finishes its login"
    );
    drop(first);

    let restarted = ScriptedSession {
        delivered,
        ..ScriptedSession::new("mobile-user")
    };
    let opened = Arc::clone(&restarted.opened);
    let second = claimed(&base, &store, Arc::new(restarted));
    let login = second.login::<String>().await.expect("the resumed login");

    assert_eq!(
        opened.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the restarted process opens no second tab"
    );
    let account = encode_identity(&login.user_id).expect("encode account");
    assert_eq!(
        store.accounts().await.expect("accounts"),
        vec![account],
        "the resumed login lists only the account"
    );
    assert_eq!(
        store.load("connetto-pending-login").await.expect("load"),
        None,
        "the pending record is gone once the login finishes"
    );
}

/// A redirect the new process finds carrying another login's `state` is
/// discarded, and a new login runs instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_process_starts_over_on_a_foreign_redirect() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _service, _idp, _fixture) =
        spawn_auth_server_with_service(vec![APP_REDIRECT.to_owned()]).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let mut dying = ScriptedSession::new("mobile-user");
    dying.die = true;
    dying.forge_state = true;
    let delivered = Arc::clone(&dying.delivered);
    let first = claimed(&base, &store, Arc::new(dying));
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), first.login::<String>()).await;
    drop(first);

    let restarted = ScriptedSession {
        delivered,
        ..ScriptedSession::new("mobile-user")
    };
    let opened = Arc::clone(&restarted.opened);
    let second = claimed(&base, &store, Arc::new(restarted));
    second.login::<String>().await.expect("a fresh login");
    assert_eq!(
        opened.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the foreign redirect is discarded and one new tab opens"
    );
}

/// A resumed code the server refuses, as an expired one is, clears the
/// pending login and starts a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_process_starts_over_when_the_resumed_code_is_refused() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _service, _idp, _fixture) =
        spawn_auth_server_with_service(vec![APP_REDIRECT.to_owned()]).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let mut dying = ScriptedSession::new("mobile-user");
    dying.die = true;
    let delivered = Arc::clone(&dying.delivered);
    let first = claimed(&base, &store, Arc::new(dying));
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), first.login::<String>()).await;
    drop(first);
    let redirect = delivered.lock().expect("slot").take().expect("a redirect");
    let (before, after) = redirect.split_once("code=").expect("a code");
    let rest = after.split_once('&').map_or("", |(_, rest)| rest);
    *delivered.lock().expect("slot") = Some(format!("{before}code=expired&{rest}"));

    let restarted = ScriptedSession {
        delivered,
        ..ScriptedSession::new("mobile-user")
    };
    let opened = Arc::clone(&restarted.opened);
    let second = claimed(&base, &store, Arc::new(restarted));
    let login = second.login::<String>().await.expect("a fresh login");
    assert_eq!(
        opened.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the refused code is dropped and one new tab opens"
    );
    assert_eq!(
        store.accounts().await.expect("accounts"),
        vec![encode_identity(&login.user_id).expect("encode account")],
        "the new login's account is the only record listed"
    );
}

/// A new process with a pending login but no delivered redirect (the user came
/// back through the launcher) starts a new login rather than waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_process_without_a_redirect_starts_over() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _service, _idp, _fixture) =
        spawn_auth_server_with_service(vec![APP_REDIRECT.to_owned()]).await;
    let store: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    let mut dying = ScriptedSession::new("mobile-user");
    dying.die = true;
    let first = claimed(&base, &store, Arc::new(dying));
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), first.login::<String>()).await;
    drop(first);
    assert!(
        store.accounts().await.expect("accounts").is_empty(),
        "a pending login is never listed as an account"
    );

    let restarted = ScriptedSession::new("mobile-user");
    let second = claimed(&base, &store, Arc::new(restarted));
    let login = second.login::<String>().await.expect("a fresh login");
    assert_eq!(
        store.accounts().await.expect("accounts"),
        vec![encode_identity(&login.user_id).expect("encode account")],
        "the new login's account is the only record listed"
    );
}

/// A refresh store whose records every clone shares, so a test reads what a
/// build stored after handing the build its own clone.
#[derive(Clone, Default)]
struct SharedRefreshStore(Arc<MemoryRefreshStore>);

impl RefreshTokenStore for SharedRefreshStore {
    type Error = ClientError;

    fn load<'a>(&'a self, account: &'a str) -> RefreshFuture<'a, Option<String>, ClientError> {
        self.0.load(account)
    }

    fn store<'a>(&'a self, account: &'a str, token: &'a str) -> RefreshFuture<'a, (), ClientError> {
        self.0.store(account, token)
    }

    fn clear<'a>(&'a self, account: &'a str) -> RefreshFuture<'a, (), ClientError> {
        self.0.clear(account)
    }

    fn accounts(&self) -> RefreshFuture<'_, Vec<String>, ClientError> {
        self.0.accounts()
    }

    fn protection(&self) -> Custody {
        self.0.protection()
    }
}

const BUILDER_DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);";

/// A native builder whose dialer never reaches a sync server, so the client
/// it builds opens offline once the sign-in resolved.
fn offline_builder() -> NativeClientBuilder<FakeTransport> {
    NativeClientBuilder::new("ws://127.0.0.1:1/", super::support::bundle(BUILDER_DDL))
        .with_dialer(super::support::NeverDial::<FakeTransport>::default())
}

/// A provider sign-in over `store` choosing `choice`.
fn provider(
    base: &str,
    store: &SharedRefreshStore,
    choice: AccountChoice,
) -> StoredAuth<SharedRefreshStore> {
    Auth::new(base, MOCK_OAUTH_PROVIDER)
        .with_account(choice)
        .refresh_store(store.clone())
}

/// A browser opener that fails the test, for a sign-in that must not log in
/// interactively.
fn no_browser() -> BrowserOpener {
    Arc::new(|_url: &str| panic!("the sign-in opened a browser"))
}

/// Sign in as `subject` through a fresh login and return the account the
/// store holds it under.
async fn builder_login(base: &str, store: &SharedRefreshStore, subject: &str) -> String {
    let client = Box::pin(
        offline_builder()
            .signed_in(
                provider(base, store, AccountChoice::New)
                    .with_browser_opener(fake_browser(subject)),
            )
            .connect(),
    )
    .await
    .expect("the provider build signs in");
    let account = client
        .session()
        .expect("a provider build reports its session")
        .account()
        .to_owned();
    client.close().await;
    account
}

/// A provider sign-in through the builder logs in, keeps its replica on the
/// device, and reports the session it acquired. Forgetting the device then
/// revokes that session at the server, clears the refresh token and deletes
/// the replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_provider_build_reports_its_session_and_forgets_its_device() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _idp, _fixture) = spawn_auth_server().await;
    let store = SharedRefreshStore::default();
    let dir = tempfile::tempdir().expect("a temporary directory");

    let client = Box::pin(
        offline_builder()
            .signed_in(
                provider(&base, &store, AccountChoice::New)
                    .with_browser_opener(fake_browser("builder-user")),
            )
            .durable(dir.path(), super::support::SharedKeys::default())
            .with_gate(Gate::off())
            .connect(),
    )
    .await
    .expect("the provider build signs in and opens offline");

    let session = client
        .session()
        .expect("a provider build reports its session");
    let account = session.account().to_owned();
    assert_eq!(
        account,
        encode_identity(session.user_id()).expect("encode account"),
        "the session's account is its encoded user id"
    );
    assert_eq!(
        session.accounts(),
        std::slice::from_ref(&account),
        "the store lists the account"
    );
    assert!(
        session.expires_at() > std::time::SystemTime::now(),
        "the session expires in the future"
    );
    let issued = store
        .load(&account)
        .await
        .expect("load")
        .expect("the login stored its refresh token");
    assert!(
        std::fs::read_dir(dir.path())
            .expect("list")
            .next()
            .is_some(),
        "the durable build wrote its replica"
    );

    client
        .forget_device(true)
        .await
        .expect("the device is forgotten and the session revoked");
    assert_eq!(
        store.load(&account).await.expect("load"),
        None,
        "the refresh token is cleared"
    );
    assert!(
        std::fs::read_dir(dir.path())
            .expect("list")
            .next()
            .is_none(),
        "the replica is gone"
    );
    let replay: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    replay.store(&account, &issued).await.expect("store");
    let refused = NativeAuthenticator::new(base, MOCK_OAUTH_PROVIDER, replay, Some(account))
        .with_browser_opener(no_browser())
        .refresh_access::<String>()
        .await;
    assert!(
        refused.is_err(),
        "the server refuses the revoked refresh token"
    );
}

/// The default account choice signs back in as the account this device used
/// last, silently, through the refresh token the first build stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_last_used_build_signs_back_in_without_a_browser() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _idp, _fixture) = spawn_auth_server().await;
    let store = SharedRefreshStore::default();
    let account = Box::pin(builder_login(&base, &store, "returning-user")).await;

    let client = Box::pin(
        offline_builder()
            .signed_in(
                Auth::new(&base, MOCK_OAUTH_PROVIDER)
                    .refresh_store(store.clone())
                    .with_browser_opener(no_browser()),
            )
            .connect(),
    )
    .await
    .expect("the silent sign-in");
    assert_eq!(
        client.session().expect("a session").account(),
        account,
        "the same account signs back in"
    );
}

/// With two accounts stored, an `Ask` build signs in as the one the
/// platform's chooser picks from the stored list, without a browser.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ask_build_signs_in_as_the_account_the_chooser_picks() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let (base, _service, _idp, _fixture) =
        spawn_auth_server_with_service(vec![APP_REDIRECT.to_owned()]).await;
    let store = SharedRefreshStore::default();
    let first = Box::pin(builder_login(&base, &store, "first-user")).await;

    let client = Box::pin(
        offline_builder()
            .signed_in(
                provider(&base, &store, AccountChoice::New)
                    .with_browser_opener(no_browser())
                    .with_claimed_redirect(
                        APP_REDIRECT,
                        Arc::new(ScriptedSession::new("second-user")),
                    ),
            )
            .connect(),
    )
    .await
    .expect("the claimed-redirect sign-in");
    let second = client.session().expect("a session").account().to_owned();
    client.close().await;
    assert_ne!(first, second, "two distinct accounts are stored");

    let offered = Arc::new(std::sync::Mutex::new(Vec::new()));
    let chooser: AccountChooser = {
        let offered = Arc::clone(&offered);
        let pick = first.clone();
        Arc::new(move |accounts: Vec<String>| {
            *offered.lock().expect("offered") = accounts;
            let pick = pick.clone();
            Box::pin(async move { AccountChoice::Account(pick) })
        })
    };
    let client = Box::pin(
        offline_builder()
            .signed_in(
                provider(&base, &store, AccountChoice::Ask)
                    .with_browser_opener(no_browser())
                    .with_account_chooser(chooser),
            )
            .connect(),
    )
    .await
    .expect("the chosen account signs in silently");
    let mut listed = offered.lock().expect("offered").clone();
    listed.sort();
    let mut stored = vec![first.clone(), second];
    stored.sort();
    assert_eq!(
        listed, stored,
        "the chooser is offered every stored account"
    );
    assert_eq!(
        client.session().expect("a session").account(),
        first,
        "the build signs in as the chosen account"
    );
}

/// An `Ask` build with no chooser, or whose chooser answers `Ask`, is refused
/// before any login starts.
#[tokio::test]
async fn an_ask_build_without_an_answer_is_refused() {
    let store = SharedRefreshStore::default();
    let refused = Box::pin(
        offline_builder()
            .signed_in(
                provider("http://127.0.0.1:1", &store, AccountChoice::Ask)
                    .with_browser_opener(no_browser()),
            )
            .connect(),
    )
    .await;
    assert!(
        matches!(refused, Err(ClientError::Auth(_))),
        "an Ask with no chooser is refused"
    );

    let evasive: AccountChooser =
        Arc::new(|_accounts: Vec<String>| Box::pin(async { AccountChoice::Ask }));
    let refused = Box::pin(
        offline_builder()
            .signed_in(
                provider("http://127.0.0.1:1", &store, AccountChoice::Ask)
                    .with_browser_opener(no_browser())
                    .with_account_chooser(evasive),
            )
            .connect(),
    )
    .await;
    assert!(
        matches!(refused, Err(ClientError::Auth(_))),
        "a chooser answering Ask is refused"
    );
}

diesel::table! {
    /// The table [`BUILDER_DDL`] creates.
    items (id) {
        /// The item's key.
        id -> Integer,
        /// Its label.
        label -> Nullable<Text>,
    }
}

/// A durable provider build of `schema` over `dir` that signs `subject` in
/// through a fresh login and opens offline.
async fn durable_login(
    base: &str,
    store: &SharedRefreshStore,
    dir: &std::path::Path,
    keys: &super::support::SharedKeys,
    subject: &str,
    schema: connetto_client::SyncSchema,
) -> connetto_client::NativeClient<FakeTransport> {
    Box::pin(
        NativeClientBuilder::new("ws://127.0.0.1:1/", schema)
            .with_dialer(super::support::NeverDial::<FakeTransport>::default())
            .signed_in(
                provider(base, store, AccountChoice::New)
                    .with_browser_opener(fake_browser(subject)),
            )
            .durable(dir, keys.clone())
            .with_gate(Gate::off())
            .connect(),
    )
    .await
    .expect("the durable provider build signs in and opens offline")
}

/// Run the write `sql` through `client` and return the unsynced writes once
/// the outbox lists it, which it does because the build never reaches a
/// server.
async fn queue_write(
    client: &connetto_client::NativeClient<FakeTransport>,
    sql: &'static str,
) -> Vec<connetto_client::UnsyncedWrite> {
    use diesel::connection::SimpleConnection as _;
    let mut outbox = client
        .client()
        .unsynced_live()
        .await
        .expect("follow the outbox");
    client
        .client()
        .with_conn(move |conn| conn.conn().batch_execute(sql).expect("write offline"))
        .await
        .expect("the gate is off");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while outbox.work().writes.is_empty() {
            outbox.changed().await.expect("the outbox stays followed");
        }
    })
    .await
    .expect("the outbox lists the offline write");
    client.client().unsynced().await.expect("read the outbox")
}

/// A build sharing tables with two accounts on one data directory: `bob`
/// stored with an unsent shared insert of row 5, `alice` live with the
/// shared store attached, reading that row.
struct SharedAccounts {
    _idp: MockOauth,
    _fixture: connetto_test_harness::Fixture,
    _keyring: connetto_test_harness::SessionKeyringGuard,
    store: SharedRefreshStore,
    dir: tempfile::TempDir,
    keys: super::support::SharedKeys,
    alice_client: connetto_client::NativeClient<FakeTransport>,
    bob: String,
    bob_replica: String,
}

impl SharedAccounts {
    async fn signed_in() -> Self {
        use super::shared_coverage::schema;

        let keyring = connetto_test_harness::isolated_session_keyring();
        let (base, idp, fixture) = spawn_auth_server().await;
        let store = SharedRefreshStore::default();
        let dir = tempfile::tempdir().expect("a temporary directory");
        let keys = super::support::SharedKeys::default();
        let bob_client = Box::pin(durable_login(
            &base,
            &store,
            dir.path(),
            &keys,
            "bob",
            schema(),
        ))
        .await;
        let bob_session = bob_client.session().expect("bob's session");
        let bob = bob_session.account().to_owned();
        let bob_replica = replica_db_name(connetto_client::REPLICA_PREFIX, &bob_session.user_id())
            .expect("bob's replica name");
        Box::pin(queue_write(
            &bob_client,
            "INSERT INTO tags VALUES (5, 'never sent')",
        ))
        .await;
        bob_client.close().await;
        let alice_client = Box::pin(durable_login(
            &base,
            &store,
            dir.path(),
            &keys,
            "alice",
            schema(),
        ))
        .await;
        let accounts = Self {
            _idp: idp,
            _fixture: fixture,
            _keyring: keyring,
            store,
            dir,
            keys,
            alice_client,
            bob,
            bob_replica,
        };
        assert_eq!(
            accounts.shared_ids().await,
            [5],
            "bob's unsent insert is in the store alice reads"
        );
        accounts
    }

    /// The shared rows `alice` reads.
    async fn shared_ids(&self) -> Vec<i64> {
        self.alice_client
            .client()
            .with_conn(super::shared_coverage::ids)
            .await
            .expect("the gate is off")
    }
}

/// A build sharing tables deletes a stored account while the live account's
/// connection holds the shared store attached. The stored account's unsent
/// shared insert is undone in the store the live account reads, and the
/// store stays, since the live account is still in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_stored_account_undoes_its_unsent_shared_writes() {
    let accounts = Box::pin(SharedAccounts::signed_in()).await;
    accounts
        .alice_client
        .forget_stored_account(&accounts.bob, true)
        .await
        .expect("the forced delete removes bob");
    assert!(
        accounts.shared_ids().await.is_empty(),
        "bob's unsent insert is undone"
    );
    assert!(
        accounts
            .dir
            .path()
            .join(connetto_client::shared_store_name(
                connetto_client::REPLICA_PREFIX
            ))
            .exists(),
        "alice keeps the shared store"
    );
}

/// A departure the shared store refuses refuses the delete. Nothing of the
/// stored account goes, so the changesets the undo needs stay in its replica
/// for the next try.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_shared_departure_refuses_the_delete() {
    use connetto_client::teardown::{ForgetError, ForgetRefusal};
    use diesel::connection::SimpleConnection as _;

    let accounts = Box::pin(SharedAccounts::signed_in()).await;
    accounts
        .alice_client
        .client()
        .with_conn(|conn| {
            conn.conn()
                .batch_execute(
                    "CREATE TRIGGER connetto_shared.refuse_departures \
                     BEFORE DELETE ON _connetto_identity \
                     BEGIN SELECT RAISE(ABORT, 'the store refuses'); END",
                )
                .expect("make the store refuse departures");
        })
        .await
        .expect("the gate is off");

    assert!(
        matches!(
            accounts
                .alice_client
                .forget_stored_account(&accounts.bob, true)
                .await,
            Err(ForgetError::Refused(ForgetRefusal::Shared(_)))
        ),
        "the refused departure refuses the delete"
    );
    assert!(
        accounts.dir.path().join(&accounts.bob_replica).exists(),
        "bob's replica stays"
    );
    assert!(
        accounts
            .keys
            .load(&accounts.bob_replica)
            .await
            .expect("load")
            .is_some(),
        "and its key"
    );
    assert!(
        accounts
            .store
            .load(&accounts.bob)
            .await
            .expect("load")
            .is_some(),
        "and bob's sign-in"
    );
    assert_eq!(
        accounts.shared_ids().await,
        [5],
        "and his unsent insert, which a later try undoes"
    );
}

/// Two accounts signed in on one data directory: `bob` stored with one
/// unsent write in his closed replica, `alice` live with one of her own.
struct TwoAccounts {
    base: String,
    _idp: MockOauth,
    _fixture: connetto_test_harness::Fixture,
    _keyring: connetto_test_harness::SessionKeyringGuard,
    store: SharedRefreshStore,
    dir: tempfile::TempDir,
    keys: super::support::SharedKeys,
    alice_client: connetto_client::NativeClient<FakeTransport>,
    alice: String,
    alice_replica: String,
    alice_queued: Vec<connetto_client::UnsyncedWrite>,
    bob: String,
    bob_replica: String,
    bob_queued: Vec<connetto_client::UnsyncedWrite>,
    bob_token: String,
}

impl TwoAccounts {
    async fn signed_in() -> Self {
        Self::signed_in_with(|| super::support::bundle(BUILDER_DDL)).await
    }

    /// The two accounts on builds of the schema `schema` makes.
    async fn signed_in_with(schema: fn() -> connetto_client::SyncSchema) -> Self {
        let keyring = connetto_test_harness::isolated_session_keyring();
        let (base, idp, fixture) = spawn_auth_server().await;
        let store = SharedRefreshStore::default();
        let dir = tempfile::tempdir().expect("a temporary directory");
        let keys = super::support::SharedKeys::default();

        let bob_client = Box::pin(durable_login(
            &base,
            &store,
            dir.path(),
            &keys,
            "bob",
            schema(),
        ))
        .await;
        let bob_session = bob_client.session().expect("bob's session");
        let bob = bob_session.account().to_owned();
        let bob_replica = replica_db_name(connetto_client::REPLICA_PREFIX, &bob_session.user_id())
            .expect("bob's replica name");
        let bob_queued = Box::pin(queue_write(
            &bob_client,
            "INSERT INTO items VALUES (1, 'bob-offline')",
        ))
        .await;
        bob_client.close().await;
        let bob_token = store
            .load(&bob)
            .await
            .expect("load")
            .expect("bob's sign-in is stored");
        assert!(
            dir.path().join(&bob_replica).exists(),
            "bob's replica is on the device"
        );

        let alice_client = Box::pin(durable_login(
            &base,
            &store,
            dir.path(),
            &keys,
            "alice",
            schema(),
        ))
        .await;
        let alice_session = alice_client.session().expect("alice's session");
        let alice = alice_session.account().to_owned();
        let alice_replica =
            replica_db_name(connetto_client::REPLICA_PREFIX, &alice_session.user_id())
                .expect("alice's replica name");
        let alice_queued = Box::pin(queue_write(
            &alice_client,
            "INSERT INTO items VALUES (1, 'alice-offline')",
        ))
        .await;
        Self {
            base,
            _idp: idp,
            _fixture: fixture,
            _keyring: keyring,
            store,
            dir,
            keys,
            alice_client,
            alice,
            alice_replica,
            alice_queued,
            bob,
            bob_replica,
            bob_queued,
            bob_token,
        }
    }

    /// Whether bob's replica, its key and his stored sign-in are all on the
    /// device.
    async fn bob_is_stored(&self) -> bool {
        self.dir.path().join(&self.bob_replica).exists()
            && self
                .keys
                .load(&self.bob_replica)
                .await
                .expect("load")
                .is_some()
            && self.store.load(&self.bob).await.expect("load").as_deref()
                == Some(self.bob_token.as_str())
    }
}

/// The delete refuses the live account, an account the device does not
/// store, and a stored account whose closed replica holds unsent writes,
/// naming them. None of the refusals destroys anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_stored_account_refuses_without_destroying_anything() {
    use connetto_client::teardown::{ForgetError, ForgetRefusal};

    let accounts = Box::pin(TwoAccounts::signed_in()).await;
    let client = &accounts.alice_client;
    assert!(
        matches!(
            client.forget_stored_account(&accounts.alice, true).await,
            Err(ForgetError::Refused(ForgetRefusal::LiveAccount))
        ),
        "the live account is forgotten through forget_device"
    );
    assert!(
        matches!(
            client.forget_stored_account("nobody", true).await,
            Err(ForgetError::Refused(ForgetRefusal::UnknownAccount))
        ),
        "an account the device does not store is refused"
    );
    match client.forget_stored_account(&accounts.bob, false).await {
        Err(ForgetError::Refused(ForgetRefusal::Unsynced(blocked))) => {
            assert_eq!(
                blocked, accounts.bob_queued,
                "the refusal names bob's unsent writes"
            );
        }
        other => panic!("expected bob's unsent writes to refuse the delete, got {other:?}"),
    }
    assert!(
        accounts.bob_is_stored().await,
        "the refusals leave bob's replica, key and sign-in"
    );
    assert!(
        accounts.dir.path().join(&accounts.alice_replica).exists(),
        "and alice's replica"
    );
}

/// The schema [`BUILDER_DDL`] declares, its `items` behind a translated
/// policy, so the replica holds the policy views the build's map names.
fn policy_schema() -> connetto_client::SyncSchema {
    connetto_client::SyncSchema::new(
        connetto_translate::translate::<String, connetto_core::bots::TextIds>(
            connetto_translate::Documents::new(
                "-- connetto:major 1\nCREATE TABLE items (id INT PRIMARY KEY, label TEXT);",
                "ALTER TABLE items ENABLE ROW LEVEL SECURITY; \
                 CREATE POLICY items_p ON items USING (true) WITH CHECK (true);",
                connetto_test_harness::FIXTURE_ROLES,
                connetto_test_harness::FIXTURE_READER,
            ),
        )
        .expect("translate the sources"),
    )
}

/// A build whose tables carry translated policies reads a stored account's
/// closed replica as the live build opens it, so the delete names the
/// account's unsent writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_policy_build_names_a_stored_accounts_unsent_writes() {
    use connetto_client::teardown::{ForgetError, ForgetRefusal};

    let accounts = Box::pin(TwoAccounts::signed_in_with(policy_schema)).await;
    match accounts
        .alice_client
        .forget_stored_account(&accounts.bob, false)
        .await
    {
        Err(ForgetError::Refused(ForgetRefusal::Unsynced(blocked))) => {
            assert_eq!(
                blocked, accounts.bob_queued,
                "the refusal names bob's unsent writes"
            );
        }
        other => panic!("expected bob's unsent writes to refuse the delete, got {other:?}"),
    }
}

/// Forced, the delete revokes the stored account's session at the server and
/// removes its replica, key and stored credential, while the live account
/// keeps its replica, key, sign-in, unsent write and rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_account_deletes_another_stored_account() {
    use diesel::prelude::*;

    let accounts = Box::pin(TwoAccounts::signed_in()).await;
    let TwoAccounts {
        base,
        store,
        dir,
        keys,
        alice_client,
        alice,
        alice_replica,
        alice_queued,
        bob,
        bob_replica,
        bob_token,
        ..
    } = &accounts;
    alice_client
        .forget_stored_account(bob, true)
        .await
        .expect("the forced delete removes bob");
    assert!(
        !dir.path().join(bob_replica).exists(),
        "bob's replica is gone"
    );
    assert!(
        keys.load(bob_replica).await.expect("load").is_none(),
        "and its key record"
    );
    assert_eq!(
        store.load(bob).await.expect("load"),
        None,
        "and bob's sign-in"
    );
    assert!(
        !store.accounts().await.expect("accounts").contains(bob),
        "the device no longer lists bob"
    );
    let replay: SharedRefresh = Arc::new(MemoryRefreshStore::default());
    replay.store(bob, bob_token).await.expect("store");
    let refused = NativeAuthenticator::new(base, MOCK_OAUTH_PROVIDER, replay, Some(bob.clone()))
        .with_browser_opener(no_browser())
        .refresh_access::<String>()
        .await;
    assert!(
        refused.is_err(),
        "the server refuses bob's revoked refresh token"
    );

    assert!(
        dir.path().join(alice_replica).exists(),
        "alice's replica stays"
    );
    assert!(
        keys.load(alice_replica).await.expect("load").is_some(),
        "and its key"
    );
    assert!(
        store.load(alice).await.expect("load").is_some(),
        "and alice's sign-in"
    );
    assert_eq!(
        &alice_client
            .client()
            .unsynced()
            .await
            .expect("read the outbox"),
        alice_queued,
        "alice's unsent write is untouched"
    );
    let labels = alice_client
        .client()
        .with_conn(|conn| {
            items::table
                .select(items::label)
                .load::<Option<String>>(conn.conn())
                .expect("read alice's items")
        })
        .await
        .expect("the gate is off");
    assert_eq!(
        labels,
        vec![Some("alice-offline".to_owned())],
        "alice still reads her own row"
    );
}
