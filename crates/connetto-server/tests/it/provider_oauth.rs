//! Sign-in through a plain OAuth 2.0 provider, one that issues no ID token and
//! answers who the user is from its own user endpoint.
//!
//! The containerised provider stands in for GitHub. Its login form takes the
//! claims its user endpoint later answers, so a test chooses the numeric `id`
//! and the renameable `login` the way GitHub would report them.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use connetto_server::{
    AuthConfig, AuthService, AuthStore as _, CookieSameSite, IdentityProvider as _,
    InMemoryAuthStore, OAuthProvider, ProviderError, ProviderRegistry, RedirectPolicy,
    RequestGuard, VerifiedLogin, auth_router,
};
use connetto_test_harness::{Fixture, MockOauth};
use openidconnect::reqwest;
use serde_json::json;
use tokio::net::TcpListener;
use tower::ServiceExt;

use super::e2e::{PG_SERIAL, reset_fixture};
use super::lifecycle::{admin_pool, builder_over, wait_ready};

const CALLBACK: &str = "http://127.0.0.1:1/auth/callback";
const PROVIDER: &str = "github";
const ISSUER_LABEL: &str = "https://github.example";

/// The mock's plain OAuth face, its user endpoint read for the numeric `id`.
fn provider(idp: &MockOauth, user_url: String) -> OAuthProvider {
    OAuthProvider::new(idp.oauth_config(PROVIDER, ISSUER_LABEL, CALLBACK, user_url, "/id"))
        .expect("build the provider")
}

fn router(provider: OAuthProvider) -> (Arc<InMemoryAuthStore>, axum::Router) {
    let config = AuthConfig::default();
    let authority =
        Arc::new(connetto_server::TokenAuthority::generate(&config).expect("generate the keypair"));
    let store = Arc::new(InMemoryAuthStore::new(config.refresh_lifetimes()));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(provider));
    let service = Arc::new(AuthService::new(
        authority,
        Arc::clone(&store),
        Arc::new(RequestGuard::default()),
    ));
    let router = auth_router(
        service,
        Arc::new(registry),
        RedirectPolicy::default(),
        CookieSameSite::default(),
    );
    (store, router)
}

async fn get(router: &axum::Router, uri: &str) -> axum::http::Response<Body> {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("build the request");
    router.clone().oneshot(request).await.expect("route")
}

fn query_value(location: &str, key: &str) -> String {
    location
        .split(['?', '&'])
        .find_map(|pair| pair.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("no {key} in {location}"))
        .to_owned()
}

/// Sign in at the provider as `username` whose user endpoint reports `claims`,
/// and answer the code the provider redirected back with.
async fn authorize(authorize_url: &str, username: &str, claims: &serde_json::Value) -> String {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the http client");
    let response = client
        .post(authorize_url)
        .form(&[("username", username), ("claims", &claims.to_string())])
        .send()
        .await
        .expect("POST the login form");
    let location = response
        .headers()
        .get("location")
        .expect("the provider redirects back")
        .to_str()
        .expect("utf-8")
        .to_owned();
    query_value(&location, "code")
}

/// Run the whole login through the router and answer the callback's body.
async fn sign_in(
    router: &axum::Router,
    username: &str,
    claims: &serde_json::Value,
) -> serde_json::Value {
    let start = get(router, &format!("/auth/login?provider={PROVIDER}")).await;
    assert_eq!(start.status(), StatusCode::TEMPORARY_REDIRECT);
    let authorize_url = start.headers()["location"]
        .to_str()
        .expect("utf-8")
        .to_owned();
    let state = query_value(&authorize_url, "state");
    let code = authorize(&authorize_url, username, claims).await;
    let callback = get(router, &format!("/auth/callback?code={code}&state={state}")).await;
    let status = callback.status();
    let bytes = to_bytes(callback.into_body(), usize::MAX)
        .await
        .expect("the callback body");
    assert_eq!(
        status,
        StatusCode::OK,
        "the callback signs in: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("the callback answers JSON")
}

/// A login through the plain OAuth provider mints a session that names the
/// provider it came through and the provider's issuer label.
#[tokio::test]
async fn a_plain_oauth_login_mints_a_session_under_the_issuer_label() {
    let idp = MockOauth::start().await;
    let user_url = idp.user_url();
    let (store, router) = router(provider(&idp, user_url));

    let body = sign_in(
        &router,
        "octocat",
        &json!({ "id": 583_231, "login": "octocat" }),
    )
    .await;
    let user_id = body["user_id"].as_str().expect("a user id").to_owned();

    let sessions = store
        .sessions_of(&user_id)
        .await
        .expect("read the sessions");
    assert_eq!(sessions.len(), 1, "one sign-in, one session");
    assert_eq!(sessions[0].provider, PROVIDER);
    assert_eq!(sessions[0].issuer, ISSUER_LABEL);
}

/// The subject is the stable numeric id, so a user who renamed their login
/// between two sign-ins is still the same user.
#[tokio::test]
async fn a_renamed_login_with_the_same_id_is_the_same_user() {
    let idp = MockOauth::start().await;
    let user_url = idp.user_url();
    let (_store, router) = router(provider(&idp, user_url));

    let before = sign_in(
        &router,
        "octocat",
        &json!({ "id": 583_231, "login": "octocat" }),
    )
    .await;
    let after = sign_in(
        &router,
        "octo-renamed",
        &json!({ "id": 583_231, "login": "octo-renamed" }),
    )
    .await;
    let other = sign_in(&router, "hubot", &json!({ "id": 2, "login": "hubot" })).await;

    assert_eq!(
        before["user_id"], after["user_id"],
        "the rename keeps the user"
    );
    assert_ne!(
        before["user_id"], other["user_id"],
        "another id is another user"
    );
}

/// A user endpoint that refuses the access token refuses the login as a
/// failed exchange, and no session is minted.
#[tokio::test]
async fn a_user_endpoint_answering_401_refuses_the_login() {
    let idp = MockOauth::start().await;
    // Another issuer's user endpoint on the same provider refuses this
    // issuer's access token with 401.
    let user_url = idp.user_url().replace("/default/", "/elsewhere/");
    let provider = provider(&idp, user_url);
    let redirect = provider.begin_login().expect("begin the login");
    let code = authorize(
        &redirect.authorize_url,
        "octocat",
        &json!({ "id": 583_231 }),
    )
    .await;

    let refused = provider
        .complete_login(&code, &redirect.pkce_verifier, &redirect.nonce)
        .await
        .expect_err("the user endpoint refused the token");
    assert!(
        matches!(&refused, ProviderError::Exchange(detail) if detail.contains("401")),
        "a refused user endpoint is a failed exchange naming the status: {refused}"
    );
}

/// A user endpoint that answers no subject at the configured pointer refuses
/// the login rather than minting a session for an empty identity.
#[tokio::test]
async fn a_user_endpoint_without_the_subject_refuses_the_login() {
    let idp = MockOauth::start().await;
    let user_url = idp.user_url();
    let provider = provider(&idp, user_url);
    let redirect = provider.begin_login().expect("begin the login");
    let code = authorize(
        &redirect.authorize_url,
        "octocat",
        &json!({ "login": "octocat" }),
    )
    .await;

    let refused = provider
        .complete_login(&code, &redirect.pkce_verifier, &redirect.nonce)
        .await
        .expect_err("no id, no identity");
    assert!(
        matches!(&refused, ProviderError::Verification(detail) if detail.contains("/id")),
        "the refusal names the missing pointer: {refused}"
    );
}

/// The email and the name are read at their pointers into the user endpoint's
/// answer and reach the identity the session is minted for.
#[tokio::test]
async fn the_email_and_name_come_from_the_user_endpoint() {
    let idp = MockOauth::start().await;
    let provider = provider(&idp, idp.user_url());
    let redirect = provider.begin_login().expect("begin the login");
    let code = authorize(
        &redirect.authorize_url,
        "octocat",
        &json!({ "id": 583_231, "name": "The Octocat" }),
    )
    .await;

    let login = provider
        .complete_login(&code, &redirect.pkce_verifier, &redirect.nonce)
        .await
        .expect("the login completes");
    assert_eq!(login.identity.subject, "583231");
    // The mock derives every user's email from the login name.
    assert_eq!(
        login.identity.email.as_deref(),
        Some("octocat@example.test")
    );
    assert_eq!(login.identity.name.as_deref(), Some("The Octocat"));
}

/// A loopback server standing in for an email list and for a user endpoint
/// that answers no JSON. Any other path answers 404.
async fn stand_in() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the stand-in");
    let at = listener.local_addr().expect("local address");
    let routes = axum::Router::new()
        .route(
            "/emails",
            axum::routing::get(|| async {
                axum::Json(json!([
                    { "email": "old@example.com", "primary": false, "verified": true },
                    { "email": "main@example.com", "primary": true, "verified": true },
                ]))
            }),
        )
        .route(
            "/forbidden",
            axum::routing::get(|| async { StatusCode::FORBIDDEN }),
        )
        .route("/not-json", axum::routing::get(|| async { "plain text" }));
    let server = tokio::spawn(async move {
        axum::serve(listener, routes)
            .await
            .expect("serve the stand-in");
    });
    (at, server)
}

/// Sign in at `provider` as a user whose user endpoint reports `claims`.
async fn log_in(
    provider: &OAuthProvider,
    claims: &serde_json::Value,
) -> Result<VerifiedLogin, ProviderError> {
    let redirect = provider.begin_login().expect("begin the login");
    let code = authorize(&redirect.authorize_url, "octocat", claims).await;
    provider
        .complete_login(&code, &redirect.pkce_verifier, &redirect.nonce)
        .await
}

/// The email list's entry both primary and verified is the email, and a list
/// that answers 403 or 404 to the token, as GitHub's does to a GitHub App
/// without the email permission, costs the email and keeps the login.
#[tokio::test]
async fn the_email_list_decides_the_email_and_never_the_login() {
    let idp = MockOauth::start().await;
    let (at, server) = stand_in().await;

    for (list, expected) in [
        ("emails", Some("main@example.com")),
        ("forbidden", None),
        ("missing", None),
    ] {
        let provider = OAuthProvider::new(
            idp.oauth_config(PROVIDER, ISSUER_LABEL, CALLBACK, idp.user_url(), "/id")
                .with_verified_email_list(format!("http://{at}/{list}")),
        )
        .expect("build the provider");
        let login = log_in(&provider, &json!({ "id": 583_231 }))
            .await
            .unwrap_or_else(|err| panic!("the {list} list is no refusal: {err}"));
        assert_eq!(login.identity.subject, "583231");
        assert_eq!(
            login.identity.email.as_deref(),
            expected,
            "the email the {list} list yields"
        );
    }
    server.abort();
}

/// A user endpoint that answers no JSON, or cannot be reached at all,
/// refuses the login as a failed exchange naming the endpoint.
#[tokio::test]
async fn a_user_endpoint_answering_nothing_usable_refuses_the_login() {
    let idp = MockOauth::start().await;
    let (at, server) = stand_in().await;

    for (user_url, detail) in [
        (format!("http://{at}/not-json"), "answered no JSON"),
        ("http://127.0.0.1:1/user".to_owned(), "reading"),
    ] {
        let provider = provider(&idp, user_url.clone());
        let refused = log_in(&provider, &json!({ "id": 583_231 }))
            .await
            .err()
            .unwrap_or_else(|| panic!("{user_url} identifies nobody"));
        assert!(
            matches!(&refused, ProviderError::Exchange(text) if text.contains(detail) && text.contains(&user_url)),
            "the refusal names {user_url} and {detail:?}: {refused}"
        );
    }
    server.abort();
}

/// The retained refresh token buys a fresh access token at the token
/// endpoint, kept under the issuer label so the next refresh routes back.
#[tokio::test]
async fn the_retained_refresh_token_buys_a_fresh_access_token() {
    let idp = MockOauth::start().await;
    let provider = provider(&idp, idp.user_url());
    let login = log_in(&provider, &json!({ "id": 583_231 }))
        .await
        .expect("the login completes");
    let refresh_token = login
        .retained
        .refresh_token
        .expect("the provider issued a refresh token");

    let refreshed = provider
        .refresh_provider_token(&refresh_token)
        .await
        .expect("the refresh succeeds");
    assert_eq!(refreshed.issuer, ISSUER_LABEL);
    assert!(!refreshed.access_token.is_empty(), "a fresh access token");
    assert!(
        refreshed.refresh_token.is_some(),
        "a refresh token to reuse"
    );
}

/// A code or a refresh token the token endpoint refuses is a failed exchange
/// or a failed refresh, never an identity.
#[tokio::test]
async fn a_code_or_refresh_token_the_provider_refuses_is_an_error() {
    let idp = MockOauth::start().await;
    let provider = provider(&idp, idp.user_url());
    let redirect = provider.begin_login().expect("begin the login");

    let refused = provider
        .complete_login("not-a-code", &redirect.pkce_verifier, &redirect.nonce)
        .await
        .expect_err("an unknown code identifies nobody");
    assert!(
        matches!(refused, ProviderError::Exchange(_)),
        "an unknown code is a failed exchange: {refused}"
    );
    let refused = provider
        .refresh_provider_token("not-a-refresh-token")
        .await
        .expect_err("an unknown refresh token buys nothing");
    assert!(
        matches!(refused, ProviderError::Refresh(_)),
        "an unknown refresh token is a failed refresh: {refused}"
    );
}

/// A provider handed to the builder serves a login on the built router beside
/// the discovered OIDC provider.
#[tokio::test]
async fn a_provider_handed_to_the_builder_serves_a_login() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let base = format!("http://127.0.0.1:{port}");
    let (builder, idp, _keys) = builder_over(&fixture, port).await;
    let github = OAuthProvider::new(idp.oauth_config(
        PROVIDER,
        ISSUER_LABEL,
        &format!("{base}/auth/callback"),
        idp.user_url(),
        "/id",
    ))
    .expect("build the provider");
    let parts = builder
        .identity_provider(Arc::new(github))
        .build()
        .await
        .expect("the deployment assembles");
    let stream = tokio::spawn(parts.change_stream);
    let http = tokio::spawn(async move { axum::serve(listener, parts.router).await });
    wait_ready(&base).await;

    let agent = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build the http client");
    let start = agent
        .get(format!("{base}/auth/login?provider={PROVIDER}"))
        .send()
        .await
        .expect("start the login");
    assert!(start.status().is_redirection(), "{}", start.status());
    let authorize_url = start.headers()["location"]
        .to_str()
        .expect("utf-8")
        .to_owned();
    let state = query_value(&authorize_url, "state");
    let code = authorize(&authorize_url, "octocat", &json!({ "id": 583_231 })).await;
    let callback = agent
        .get(format!("{base}/auth/callback?code={code}&state={state}"))
        .send()
        .await
        .expect("answer the callback");
    let status = callback.status();
    let body = callback.text().await.expect("the callback body");
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "the callback signs in: {body}"
    );

    stream.abort();
    http.abort();
}
