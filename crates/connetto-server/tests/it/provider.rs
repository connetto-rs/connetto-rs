//! Phase 3 provider verification and token-retention tests (Docker-free).
//!
//! A locally minted, locally signed ID token exercises the real verification
//! path: the mapping to a [`ResolvedIdentity`](connetto_server::ResolvedIdentity), the nonce and audience checks,
//! and the MFA assurance bar. The retained-token accessor uses the containerised
//! OIDC provider so refresh and persistence run against real token responses.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use connetto_core::Subject;
use connetto_core::messages::Grant;
use connetto_server::{
    AssuranceRequirement, AuthConfig, AuthError, AuthService, GenericOidcProvider,
    IdentityProvider, InMemoryAuthStore, LoginRedirect, OidcProviderConfig, ProviderError,
    ProviderRegistry, RequestGuard, ResolvedIdentity, RetainedProviderToken, SessionId,
    TokenAuthority, VerifiedLogin,
};
use connetto_test_harness::{MOCK_OAUTH_PROVIDER, MockOauth};
use openidconnect::core::{
    CoreIdToken, CoreIdTokenClaims, CoreJsonWebKeySet, CoreJwsSigningAlgorithm,
    CoreRsaPrivateSigningKey,
};
use openidconnect::reqwest;
use openidconnect::{
    Audience, AuthenticationContextClass, AuthenticationMethodReference, EmptyAdditionalClaims,
    EndUserEmail, IssuerUrl, Nonce, PrivateSigningKey, StandardClaims, SubjectIdentifier,
};

const TEST_ISSUER: &str = "https://issuer.example";
const TEST_CLIENT: &str = "connetto-test-client";

/// A fixed RSA key in PKCS#1 PEM, the format `CoreRsaPrivateSigningKey` expects.
const TEST_RSA_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA0x+qU/28waGg9HJxUgCFeT4DHy7GTTMDVZU5vUjsnaAu16Of
JUB2DUimO4qs2GSLJaeG1cw4Szp2XLOVuQz8nVTg5aT53mQ6aSpVjje2ayOZM5+I
PQCiPGpl8HzakCwmHzKFVTMwoD0sjJt2QMJnZVdwKwF1Q/hl0Jd+5KAvKnHE2qUc
32BgKdTE9hdTLs3ss7xVdqVLM+7tubglErQMGoC3vjx8njPK3iOw5VmW49WQXb76
okg0I744zg+ADeuLXwhEIxmVdkrQ957aDg70Kx0+Di5jXLkFt613q7Rnnrra2hnj
bGh7ipOSj+/5+g4DNH+XnuRAv2LqHLgeLS7QLwIDAQABAoIBAApOLmNBLHiLKi8k
cvGcwucjJsXb46QbDFueGB5sM9iR3Bd8jiUkW17UoACiCUPazIv+/G7tNAZACU0H
GxTYVHBdl0i+X9ACNnOxtFFn2MisCSti6ySHJmQqkWVGwuhsr0OwlJ+PCx2XPthy
MjiBBMkGlpwSyyWRN28SJgiE5Sh+JHiQ9bH6b8hfkwzlc1DyCi9Kr0ZtyeG+l4fE
+U0QDwXvRtcXc//wai5JJp1714NKIYXF1n7ACqi5erHQ+I9jGJv3/YbN3mV0y/aj
u/QsGWY00XVk+GGM9tNj/57gD4asj2caWsSWW0m/u2R+WYJqUoo1v3qkP32aFf+q
kSVSDHkCgYEA7p/joBHFpcfvQlTnOLdvAe7fCwvVj2ScSNpF3Nsgc1BRg0fFg8LN
EIsXQd4tMI8qupnkw6HBX1Z2MzQp8VeAWODBVcZLaER7OB7CmIyuYXkZFCf4F+ub
lhnWIKaeC+eO8KJ7oue340Jd/D6G8GibqhkJxH13pZ7UQTCgj+fDuyMCgYEA4n8k
iZyy1a+m+hG+NfaVAOjOXFVwjxz9PEBQ/G40cLKiDw/E6L2ZS0YO16Zi+iBcBoIL
Ajfw6WMTu2TyaU/vKKUfCfzS4cj2dKQRbM5v5GBflDxI4OMLAAiHUGNNYiOEvIzI
wXF5TrsLKaBBbLzp6LWlOXONPt7PgxcZpsSP/YUCgYEAvFZ6BEbCptws3T/B16P/
+5ibdk562lhgeae9aFmTPTBxhZpKLHq9+4asbpJ7PE5jPTBlvHqY8zR8ymErkY6s
gHm0XozJy5vxXRP6JwkyQUChKKV7TPXqsQfnV5HqQB8dVJQJ3UPigX5KS+LWAj2u
TwzABtO4cYHwqRtGPw6AD90CgYEAsBngw6n1FdWjgu0GshhNU86um/XGNU95yT3M
eegJl9Ib1JATLk40AOWwppT0gbtlMZ4shwYNprhk4B+lpqICtdxkXLSZFfnVPW1P
KwT61FrmFXAlzcxZgiYfZy4+PV6WVq8za8wZYFBnZm72T2A2kbuhgiDIoihEuYzd
Yd+UgK0CgYEAv20gmqxCKhPxBCyaVJKigbf2I1IbefDegBLPVPuQrf7y1NLwrJWI
lhYiAFq59xlWdQRj19kdodgFXq7KtLxZyrf5MhCNxSpqRfPrq9UNK0aMuNbwhxXa
3vQ3JpihiWwwHKTQp4/q86pwterh4dXxAmAMVMJf+kwfMrpkU+yR2Ds=
-----END RSA PRIVATE KEY-----
";

fn signing_key() -> CoreRsaPrivateSigningKey {
    CoreRsaPrivateSigningKey::from_pem(TEST_RSA_PEM, None).expect("load test key")
}

/// A sign-in through the mock identity provider.
fn origin() -> connetto_server::SessionOrigin<()> {
    connetto_server::SessionOrigin {
        provider: MOCK_OAUTH_PROVIDER.to_owned(),
        description: (),
    }
}

fn http() -> openidconnect::reqwest::Client {
    openidconnect::reqwest::ClientBuilder::new()
        .build()
        .expect("build http client")
}

fn provider(assurance: AssuranceRequirement) -> GenericOidcProvider {
    let jwks = CoreJsonWebKeySet::new(vec![signing_key().as_verification_key()]);
    let config = OidcProviderConfig::new(
        "test",
        TEST_CLIENT,
        TEST_ISSUER,
        "https://app.example/callback",
    )
    .with_assurance(assurance);
    GenericOidcProvider::from_parts(
        config,
        "https://issuer.example/auth",
        "https://issuer.example/token",
        jwks,
        http(),
    )
    .expect("build provider")
}

fn mint(nonce: &str, audience: &str, amr: &[&str], acr: Option<&str>) -> CoreIdToken {
    let standard = StandardClaims::new(SubjectIdentifier::new("user-42".to_owned()))
        .set_email(Some(EndUserEmail::new("u@example.com".to_owned())));
    let mut claims = CoreIdTokenClaims::new(
        IssuerUrl::new(TEST_ISSUER.to_owned()).unwrap(),
        vec![Audience::new(audience.to_owned())],
        Utc::now() + chrono::Duration::hours(1),
        Utc::now(),
        standard,
        EmptyAdditionalClaims {},
    )
    .set_nonce(Some(Nonce::new(nonce.to_owned())));
    if !amr.is_empty() {
        claims = claims.set_auth_method_refs(Some(
            amr.iter()
                .map(|method| AuthenticationMethodReference::new((*method).to_owned()))
                .collect(),
        ));
    }
    if let Some(acr) = acr {
        claims = claims.set_auth_context_ref(Some(AuthenticationContextClass::new(acr.to_owned())));
    }
    CoreIdToken::new(
        claims,
        &signing_key(),
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        None,
        None,
    )
    .expect("mint id token")
}

#[test]
fn verified_id_token_maps_to_identity() {
    let provider = provider(AssuranceRequirement::none());
    let token = mint("nonce-1", TEST_CLIENT, &[], None);
    let identity = provider.verify_claims(&token, "nonce-1").expect("verify");
    assert_eq!(identity.subject, "user-42");
    assert_eq!(identity.issuer, TEST_ISSUER);
    assert_eq!(identity.email.as_deref(), Some("u@example.com"));
}

#[test]
fn a_wrong_nonce_or_audience_is_refused() {
    let provider = provider(AssuranceRequirement::none());

    let token = mint("nonce-1", TEST_CLIENT, &[], None);
    let wrong_nonce = provider.verify_claims(&token, "nonce-2");
    assert!(
        matches!(wrong_nonce, Err(ProviderError::Verification(_))),
        "a mismatched nonce is refused, got {wrong_nonce:?}",
    );

    let foreign_audience = mint("nonce-1", "some-other-client", &[], None);
    let refused = provider.verify_claims(&foreign_audience, "nonce-1");
    assert!(
        matches!(refused, Err(ProviderError::Verification(_))),
        "a token for another audience is refused, got {refused:?}",
    );
}

#[test]
fn mfa_assurance_requires_the_configured_amr() {
    let provider = provider(AssuranceRequirement::none().with_required_amr(["mfa"]));

    let without_mfa = mint("nonce-1", TEST_CLIENT, &["pwd"], None);
    let refused = provider.verify_claims(&without_mfa, "nonce-1");
    assert!(
        matches!(refused, Err(ProviderError::Assurance(_))),
        "a login without the required amr is refused, got {refused:?}",
    );

    let with_mfa = mint("nonce-1", TEST_CLIENT, &["mfa", "pwd"], Some("high"));
    let identity = provider
        .verify_claims(&with_mfa, "nonce-1")
        .expect("verify");
    assert_eq!(identity.subject, "user-42");
}

async fn service_with_real_provider() -> (
    Arc<TokenAuthority>,
    AuthService<InMemoryAuthStore>,
    VerifiedLogin,
    MockOauth,
) {
    const CALLBACK: &str = "http://127.0.0.1:1/auth/callback";
    let idp = MockOauth::start().await;
    let provider = GenericOidcProvider::discover(
        idp.oidc_config(MOCK_OAUTH_PROVIDER, CALLBACK),
        reqwest::Client::new(),
    )
    .await
    .expect("discover provider");
    let redirect = provider.begin_login().expect("begin login");
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build http client");
    let resp = http
        .post(&redirect.authorize_url)
        .form(&[("username", "provider-user")])
        .send()
        .await
        .expect("POST authorize");
    let location = resp
        .headers()
        .get("location")
        .expect("location header")
        .to_str()
        .expect("utf-8")
        .to_owned();
    let code = location
        .split(['?', '&'])
        .find_map(|pair| pair.strip_prefix("code="))
        .expect("code in location")
        .to_owned();
    let login = provider
        .complete_login(&code, &redirect.pkce_verifier, &redirect.nonce)
        .await
        .expect("complete login");
    let provider: Arc<dyn IdentityProvider> = Arc::new(provider);
    let config = AuthConfig::default();
    let authority = Arc::new(TokenAuthority::generate(&config).expect("keypair"));
    let store = Arc::new(InMemoryAuthStore::new(config.refresh_lifetimes()));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::clone(&provider));
    let service = AuthService::new(
        Arc::clone(&authority),
        store,
        Arc::new(RequestGuard::default()),
    )
    .with_registry(Arc::new(registry));
    (authority, service, login, idp)
}

#[tokio::test]
async fn accessor_refreshes_an_expired_provider_token() {
    let (authority, service, mut login, _idp) = service_with_real_provider().await;
    login.retained.access_token = "stale".to_owned();
    login.retained.expires_in = Some(Duration::ZERO);
    let pair = service
        .login_with_provider(&login, origin())
        .await
        .expect("login");
    let Subject::Identity(verified) = authority
        .check_grant::<String, String>(&Grant::new(&pair.access_token))
        .expect("verify")
        .subject
    else {
        panic!("expected identity subject");
    };
    let session_id = verified.session_id;

    // The stored token is expired, so the accessor refreshes it through the real idp.
    let refreshed = service
        .provider_access_token(session_id)
        .await
        .expect("accessor");
    assert!(
        refreshed.as_deref().is_some_and(|t| !t.is_empty()),
        "accessor returned a token",
    );
    assert_ne!(
        refreshed.as_deref(),
        Some("stale"),
        "the expired token was refreshed",
    );

    // The refreshed token has an expiry set by the real idp, so a second call
    // returns it as-is without another refresh.
    let again = service
        .provider_access_token(session_id)
        .await
        .expect("accessor");
    assert_eq!(refreshed, again, "second call returns the same token");
}

#[tokio::test]
async fn accessor_returns_a_still_valid_token_unrefreshed() {
    let (authority, service, mut login, _idp) = service_with_real_provider().await;
    login.retained.access_token = "still-good".to_owned();
    login.retained.expires_in = Some(Duration::from_secs(3600));
    let pair = service
        .login_with_provider(&login, origin())
        .await
        .expect("login");
    let Subject::Identity(verified) = authority
        .check_grant::<String, String>(&Grant::new(&pair.access_token))
        .expect("verify")
        .subject
    else {
        panic!("expected identity subject");
    };
    let session_id = verified.session_id;

    let token = service
        .provider_access_token(session_id)
        .await
        .expect("accessor");
    assert_eq!(
        token.as_deref(),
        Some("still-good"),
        "a valid token is returned without a refresh",
    );
}

/// The boxed future the object-safe provider methods answer.
type Answer<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A client of one shared issuer whose refresh answers an access token that
/// names the client, so a test sees which client a refresh went through.
struct SharedIssuerClient(&'static str);

impl IdentityProvider for SharedIssuerClient {
    fn name(&self) -> &str {
        self.0
    }

    fn issuer(&self) -> &str {
        TEST_ISSUER
    }

    fn begin_login(&self) -> Result<LoginRedirect, ProviderError> {
        Err(ProviderError::Config("no login in this test".to_owned()))
    }

    fn complete_login<'a>(
        &'a self,
        _code: &'a str,
        _pkce_verifier: &'a str,
        _nonce: &'a str,
    ) -> Answer<'a, Result<VerifiedLogin, ProviderError>> {
        Box::pin(async { Err(ProviderError::Config("no login in this test".to_owned())) })
    }

    fn refresh_provider_token<'a>(
        &'a self,
        _refresh_token: &'a str,
    ) -> Answer<'a, Result<RetainedProviderToken, ProviderError>> {
        Box::pin(async move {
            Ok(RetainedProviderToken {
                issuer: TEST_ISSUER.to_owned(),
                access_token: format!("issued to {}", self.0),
                refresh_token: None,
                expires_in: Some(Duration::from_secs(3600)),
            })
        })
    }
}

/// A service over an in-memory store with these shared-issuer clients
/// registered, or with no registry attached when `clients` is `None`.
fn shared_issuer_service(
    clients: Option<&[&'static str]>,
) -> (Arc<TokenAuthority>, AuthService<InMemoryAuthStore>) {
    let config = AuthConfig::default();
    let authority = Arc::new(TokenAuthority::generate(&config).expect("keypair"));
    let service = AuthService::new(
        Arc::clone(&authority),
        Arc::new(InMemoryAuthStore::new(config.refresh_lifetimes())),
        Arc::new(RequestGuard::default()),
    );
    let service = match clients {
        Some(clients) => {
            let mut registry = ProviderRegistry::new();
            for client in clients {
                registry.register(Arc::new(SharedIssuerClient(client)));
            }
            service.with_registry(Arc::new(registry))
        }
        None => service,
    };
    (authority, service)
}

/// A session signed in through `provider` whose retained provider access
/// token has already expired.
async fn expired_session(
    authority: &TokenAuthority,
    service: &AuthService<InMemoryAuthStore>,
    provider: &str,
    refresh_token: Option<&str>,
) -> SessionId {
    let login = VerifiedLogin {
        identity: ResolvedIdentity {
            issuer: TEST_ISSUER.to_owned(),
            subject: "shared-user".to_owned(),
            email: None,
            name: None,
            amr: Vec::new(),
            acr: None,
        },
        retained: RetainedProviderToken {
            issuer: TEST_ISSUER.to_owned(),
            access_token: "expired".to_owned(),
            refresh_token: refresh_token.map(str::to_owned),
            expires_in: Some(Duration::ZERO),
        },
    };
    let origin = connetto_server::SessionOrigin {
        provider: provider.to_owned(),
        description: (),
    };
    let pair = service
        .login_with_provider(&login, origin)
        .await
        .expect("login");
    let Subject::Identity(verified) = authority
        .check_grant::<String, String>(&Grant::new(&pair.access_token))
        .expect("verify")
        .subject
    else {
        panic!("expected identity subject");
    };
    verified.session_id
}

/// Two clients of one issuer, such as a web and a native Google client, each
/// refresh the provider tokens of the sessions signed in through them, since
/// the provider refuses a refresh token presented by another client.
#[tokio::test]
async fn a_refresh_goes_through_the_client_the_session_signed_in_with() {
    let (authority, service) = shared_issuer_service(Some(&["web", "native"]));
    for client in ["web", "native"] {
        let session = expired_session(&authority, &service, client, Some("refresh")).await;
        let token = service
            .provider_access_token(session)
            .await
            .expect("accessor");
        assert_eq!(
            token,
            Some(format!("issued to {client}")),
            "a session signed in through {client} refreshes through {client}"
        );
    }
}

/// An expired provider token nothing can renew is a refresh failure, never
/// handed back as though the provider would still accept it.
#[tokio::test]
async fn an_expired_provider_token_nothing_can_renew_is_refused() {
    for (case, clients, provider, refresh_token) in [
        (
            "the session's provider is not configured",
            Some(&["web"][..]),
            "retired",
            Some("refresh"),
        ),
        (
            "the provider issued no refresh token",
            Some(&["web"][..]),
            "web",
            None,
        ),
        ("no registry is attached", None, "web", Some("refresh")),
    ] {
        let (authority, service) = shared_issuer_service(clients);
        let session = expired_session(&authority, &service, provider, refresh_token).await;
        let answer = service.provider_access_token(session).await;
        assert!(
            matches!(answer, Err(AuthError::Provider(ProviderError::Refresh(_)))),
            "{case}: got {answer:?}"
        );
    }
}
