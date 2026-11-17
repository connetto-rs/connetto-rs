//! A plain OAuth 2.0 identity provider over `oauth2`, for a provider that
//! issues no ID token, plus a GitHub preset.
//!
//! connetto is the confidential client and runs Authorization Code with PKCE
//! and `state` as for an OIDC provider. With no signed token to verify, the
//! identity is what the provider's user endpoint answers when shown the access
//! token just exchanged, read over the provider's own TLS connection. The
//! subject, email and name are read at JSON pointers into that answer, and the
//! subject must be the provider's stable account id, never a renameable
//! handle. The issuer is a label the deployment chooses, the half of the
//! `(issuer, subject)` pair no token states. Such a provider reports no `amr`
//! or `acr`, so its configuration takes no assurance requirement.

use std::fmt;
use std::time::Duration;

use oauth2::basic::BasicClient;
use oauth2::reqwest;
use oauth2::{
    AuthType, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointNotSet,
    EndpointSet, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, RefreshToken, Scope,
    TokenResponse as _, TokenUrl,
};
use serde_json::Value;

use crate::authn::http::is_loopback_host;
use crate::authn::provider::{
    BoxFuture, IdentityProvider, LoginRedirect, ProviderError, RetainedProviderToken, VerifiedLogin,
};
use crate::authn::store::ResolvedIdentity;

/// The issuer label of the GitHub preset.
const GITHUB_ISSUER: &str = "https://github.com";
/// GitHub's authorize endpoint, for OAuth apps and GitHub Apps alike.
const GITHUB_AUTHORIZE_URL: &str = "https://github.com/login/oauth/authorize";
/// GitHub's token endpoint.
const GITHUB_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
/// GitHub's authenticated-user endpoint, whose `id` never changes.
const GITHUB_USER_URL: &str = "https://api.github.com/user";
/// GitHub's list of the user's email addresses, each flagged primary and
/// verified.
const GITHUB_EMAILS_URL: &str = "https://api.github.com/user/emails";
/// The scope that lets an OAuth app read the email list.
const GITHUB_EMAIL_SCOPE: &str = "user:email";

/// How long one call to the token or user endpoint may take, measured on a
/// monotonic clock, before the login fails.
const CALL_BUDGET: Duration = Duration::from_secs(10);
/// The `User-Agent` every call carries. GitHub's API refuses a request
/// without one.
const USER_AGENT: &str = concat!("connetto-server/", env!("CARGO_PKG_VERSION"));

/// The client type after the authorize and token endpoints are set.
type ConfiguredClient =
    BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

/// The three endpoints of a plain OAuth 2.0 provider.
#[derive(Debug, Clone)]
pub struct OAuthEndpoints {
    /// Where the user agent is sent to sign in and consent.
    pub authorize_url: String,
    /// Where connetto exchanges the authorization code.
    pub token_url: String,
    /// Where connetto reads who the user is, shown the access token as a
    /// bearer credential.
    pub user_url: String,
}

/// Where the email address comes from, with the list at a `U` location.
#[derive(Debug, Clone)]
enum EmailSource<U> {
    /// The provider reports none.
    Absent,
    /// A JSON pointer into the user endpoint's answer.
    Pointer(String),
    /// A list in the shape of GitHub's, its entry both primary and verified.
    VerifiedPrimary(U),
}

/// Confidential-client configuration for one plain OAuth 2.0 provider.
///
/// Built as a chain of calls like
/// [`OidcProviderConfig`](crate::OidcProviderConfig). The values with no
/// honest default are arguments to [`new`](Self::new).
#[derive(Clone)]
pub struct OAuthProviderConfig {
    name: String,
    client_id: String,
    client_secret: Option<String>,
    issuer: String,
    redirect_url: String,
    endpoints: OAuthEndpoints,
    subject_pointer: String,
    name_pointer: Option<String>,
    email: EmailSource<String>,
    scopes: Vec<String>,
    client_auth: AuthType,
}

impl fmt::Debug for OAuthProviderConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthProviderConfig")
            .field("name", &self.name)
            .field("client_id", &self.client_id)
            .field("client_secret", &self.client_secret.is_some())
            .field("issuer", &self.issuer)
            .field("redirect_url", &self.redirect_url)
            .field("endpoints", &self.endpoints)
            .field("subject_pointer", &self.subject_pointer)
            .field("name_pointer", &self.name_pointer)
            .field("email", &self.email)
            .field("scopes", &self.scopes)
            .finish_non_exhaustive()
    }
}

impl OAuthProviderConfig {
    /// A public client called `name` against `endpoints`, returning to
    /// `redirect_url`, whose users are the `(issuer, subject)` pairs of the
    /// `issuer` label and the user endpoint's value at `subject_pointer`. No
    /// secret, no scopes, and no email or name until each is named.
    ///
    /// The label must name this provider alone, never an issuer an OIDC
    /// provider verifies, which the server's build refuses. The subject must
    /// be the provider's stable account id, since a user who renames a handle
    /// at the pointer would become someone else.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        client_id: impl Into<String>,
        issuer: impl Into<String>,
        redirect_url: impl Into<String>,
        endpoints: OAuthEndpoints,
        subject_pointer: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            client_id: client_id.into(),
            client_secret: None,
            issuer: issuer.into(),
            redirect_url: redirect_url.into(),
            endpoints,
            subject_pointer: subject_pointer.into(),
            name_pointer: None,
            email: EmailSource::Absent,
            scopes: Vec::new(),
            client_auth: AuthType::BasicAuth,
        }
    }

    /// The GitHub preset, for an OAuth app or a GitHub App, called `name`.
    /// The issuer label is `https://github.com`, the subject the numeric `id`
    /// of `GET https://api.github.com/user`, the name its `name`, and the
    /// email the entry of `GET https://api.github.com/user/emails` that is
    /// both primary and verified, absent when none is or when the token may
    /// not read the list. It requests the `user:email` scope, which a GitHub
    /// App ignores in favour of its email permission.
    #[must_use]
    pub fn github(
        name: impl Into<String>,
        client_id: impl Into<String>,
        redirect_url: impl Into<String>,
    ) -> Self {
        Self {
            name_pointer: Some("/name".to_owned()),
            email: EmailSource::VerifiedPrimary(GITHUB_EMAILS_URL.to_owned()),
            scopes: vec![GITHUB_EMAIL_SCOPE.to_owned()],
            client_auth: AuthType::RequestBody,
            ..Self::new(
                name,
                client_id,
                GITHUB_ISSUER,
                redirect_url,
                OAuthEndpoints {
                    authorize_url: GITHUB_AUTHORIZE_URL.to_owned(),
                    token_url: GITHUB_TOKEN_URL.to_owned(),
                    user_url: GITHUB_USER_URL.to_owned(),
                },
                "/id",
            )
        }
    }

    /// The OAuth client secret, making this a confidential client. Takes an
    /// [`Option`] because every caller reads it from a place that may not
    /// have one.
    #[must_use]
    pub fn with_client_secret(mut self, secret: Option<String>) -> Self {
        self.client_secret = secret;
        self
    }

    /// The scopes to request, replacing any the preset requests.
    #[must_use]
    pub fn with_scopes(mut self, scopes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    /// The JSON pointer of the email address in the user endpoint's answer.
    /// connetto cannot tell whether the provider verified it, so a deployment
    /// names it only for a provider that reports verified addresses alone.
    #[must_use]
    pub fn with_email_pointer(mut self, pointer: impl Into<String>) -> Self {
        self.email = EmailSource::Pointer(pointer.into());
        self
    }

    /// The URL of an email list in the shape of GitHub's `GET /user/emails`,
    /// whose entry both primary and verified is the email, replacing any
    /// email pointer.
    #[must_use]
    pub fn with_verified_email_list(mut self, url: impl Into<String>) -> Self {
        self.email = EmailSource::VerifiedPrimary(url.into());
        self
    }

    /// The JSON pointer of the display name in the user endpoint's answer.
    #[must_use]
    pub fn with_name_pointer(mut self, pointer: impl Into<String>) -> Self {
        self.name_pointer = Some(pointer.into());
        self
    }
}

/// A plain OAuth 2.0 identity provider, which issues no ID token and is asked
/// who the user is at its user endpoint.
pub struct OAuthProvider {
    name: String,
    issuer: String,
    client: ConfiguredClient,
    http: reqwest::Client,
    scopes: Vec<Scope>,
    user_url: reqwest::Url,
    subject_pointer: String,
    name_pointer: Option<String>,
    email: EmailSource<reqwest::Url>,
}

impl OAuthProvider {
    /// Build the provider. No network, since a plain OAuth provider publishes
    /// nothing to discover.
    ///
    /// # Errors
    ///
    /// [`ProviderError::Config`] if the issuer label is empty, an endpoint
    /// or the redirect URL does not parse, an endpoint is plain `http` on a
    /// host other than loopback, a pointer does not start with `/`, or the
    /// HTTP client cannot be built.
    pub fn new(config: OAuthProviderConfig) -> Result<Self, ProviderError> {
        if config.issuer.is_empty() {
            return Err(ProviderError::Config(format!(
                "provider {} has an empty issuer label",
                config.name
            )));
        }
        let pointers = [Some(&config.subject_pointer), config.name_pointer.as_ref()]
            .into_iter()
            .chain([match &config.email {
                EmailSource::Pointer(pointer) => Some(pointer),
                EmailSource::Absent | EmailSource::VerifiedPrimary(_) => None,
            }])
            .flatten();
        for pointer in pointers {
            if !pointer.starts_with('/') {
                return Err(ProviderError::Config(format!(
                    "provider {}: the JSON pointer {pointer:?} does not start with /",
                    config.name
                )));
            }
        }
        let config_error = |err: &dyn fmt::Display| {
            ProviderError::Config(format!("provider {}: {err}", config.name))
        };
        // The identity is only as trustworthy as the connections the code and
        // the access token travel over, so every endpoint is TLS, loopback
        // aside for a provider on the same machine.
        let endpoint = |text: &str| {
            let parsed = url::Url::parse(text).map_err(|err| config_error(&err))?;
            match parsed.scheme() {
                "https" => Ok(parsed),
                "http" if is_loopback_host(&parsed) => Ok(parsed),
                _ => Err(config_error(&format_args!(
                    "the endpoint {text} is not https"
                ))),
            }
        };
        let authorize_url = endpoint(&config.endpoints.authorize_url)?;
        let token_url = endpoint(&config.endpoints.token_url)?;
        let user_url = endpoint(&config.endpoints.user_url)?;
        let email = match config.email {
            EmailSource::Absent => EmailSource::Absent,
            EmailSource::Pointer(pointer) => EmailSource::Pointer(pointer),
            EmailSource::VerifiedPrimary(list) => EmailSource::VerifiedPrimary(endpoint(&list)?),
        };
        let client = BasicClient::new(ClientId::new(config.client_id))
            .set_auth_uri(AuthUrl::from_url(authorize_url))
            .set_token_uri(TokenUrl::from_url(token_url))
            .set_redirect_uri(
                RedirectUrl::new(config.redirect_url).map_err(|err| config_error(&err))?,
            )
            .set_auth_type(config.client_auth);
        let client = match config.client_secret {
            Some(secret) => client.set_client_secret(ClientSecret::new(secret)),
            None => client,
        };
        // A provider endpoint that redirects is refused rather than followed,
        // so the access token is never shown to a host the deployment did
        // not name.
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CALL_BUDGET)
            .connect_timeout(CALL_BUDGET)
            .user_agent(USER_AGENT)
            .build()
            .map_err(|err| config_error(&err))?;
        Ok(Self {
            name: config.name,
            issuer: config.issuer,
            client,
            http,
            scopes: config.scopes.into_iter().map(Scope::new).collect(),
            user_url,
            subject_pointer: config.subject_pointer,
            name_pointer: config.name_pointer,
            email,
        })
    }

    /// GET `url` with the access token as a bearer credential.
    async fn get(
        &self,
        url: &reqwest::Url,
        access_token: &str,
    ) -> Result<reqwest::Response, ProviderError> {
        self.http
            .get(url.clone())
            .bearer_auth(access_token)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|err| ProviderError::Exchange(format!("reading {url}: {err}")))
    }

    /// GET `url` with the access token and parse a successful answer as JSON.
    async fn read_json(
        &self,
        url: &reqwest::Url,
        access_token: &str,
    ) -> Result<Value, ProviderError> {
        json_body(url, self.get(url, access_token).await?).await
    }

    /// The email address, read from where the configuration says it lives.
    async fn email(
        &self,
        user: &Value,
        access_token: &str,
    ) -> Result<Option<String>, ProviderError> {
        match &self.email {
            EmailSource::Absent => Ok(None),
            EmailSource::Pointer(pointer) => Ok(text_at(user, pointer)),
            EmailSource::VerifiedPrimary(list) => {
                let response = self.get(list, access_token).await?;
                // A token not allowed to read the list costs the email, never the login.
                if matches!(
                    response.status(),
                    reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::NOT_FOUND
                ) {
                    return Ok(None);
                }
                Ok(verified_primary(&json_body(list, response).await?))
            }
        }
    }

    /// Read who the user is from the user endpoint.
    async fn identity(&self, access_token: &str) -> Result<ResolvedIdentity, ProviderError> {
        let user = self.read_json(&self.user_url, access_token).await?;
        let subject = subject_at(&user, &self.subject_pointer).ok_or_else(|| {
            ProviderError::Verification(format!(
                "{} answered no subject at {}",
                self.user_url, self.subject_pointer
            ))
        })?;
        let name = self
            .name_pointer
            .as_deref()
            .and_then(|pointer| text_at(&user, pointer));
        let email = self.email(&user, access_token).await?;
        Ok(ResolvedIdentity {
            issuer: self.issuer.clone(),
            subject,
            email,
            name,
            amr: Vec::new(),
            acr: None,
        })
    }
}

/// Parse `response` from `url` as JSON, any status but success a failed
/// exchange that names it.
async fn json_body(
    url: &reqwest::Url,
    response: reqwest::Response,
) -> Result<Value, ProviderError> {
    let status = response.status();
    if !status.is_success() {
        return Err(ProviderError::Exchange(format!("{url} answered {status}")));
    }
    let body = response
        .bytes()
        .await
        .map_err(|err| ProviderError::Exchange(format!("reading {url}: {err}")))?;
    serde_json::from_slice(&body)
        .map_err(|err| ProviderError::Exchange(format!("{url} answered no JSON: {err}")))
}

/// The subject at `pointer`, a non-empty string as it is or an integer in
/// decimal, since GitHub's stable id is a JSON number.
fn subject_at(document: &Value, pointer: &str) -> Option<String> {
    match document.pointer(pointer)? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) if number.is_u64() || number.is_i64() => Some(number.to_string()),
        _ => None,
    }
}

/// The non-empty string at `pointer`, absent for anything else, `null`
/// included.
fn text_at(document: &Value, pointer: &str) -> Option<String> {
    document
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// The address of GitHub's email list entry that is both primary and
/// verified.
fn verified_primary(emails: &Value) -> Option<String> {
    emails.as_array()?.iter().find_map(|entry| {
        let flagged = |key: &str| entry.get(key).and_then(Value::as_bool) == Some(true);
        if flagged("primary") && flagged("verified") {
            text_at(entry, "/email")
        } else {
            None
        }
    })
}

impl IdentityProvider for OAuthProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The authorize redirect with PKCE, `state` and the scopes. The nonce is
    /// empty, because there is no ID token to bind it into.
    fn begin_login(&self) -> Result<LoginRedirect, ProviderError> {
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let (url, csrf) = self
            .client
            .authorize_url(CsrfToken::new_random)
            .add_scopes(self.scopes.iter().cloned())
            .set_pkce_challenge(challenge)
            .url();
        Ok(LoginRedirect {
            authorize_url: url.to_string(),
            state: csrf.secret().clone(),
            nonce: String::new(),
            pkce_verifier: verifier.secret().clone(),
        })
    }

    /// Exchange `code` with its PKCE verifier, then read the user from the
    /// user endpoint with the access token. `nonce` is unused.
    fn complete_login<'a>(
        &'a self,
        code: &'a str,
        pkce_verifier: &'a str,
        _nonce: &'a str,
    ) -> BoxFuture<'a, Result<VerifiedLogin, ProviderError>> {
        Box::pin(async move {
            let token_response = self
                .client
                .exchange_code(AuthorizationCode::new(code.to_owned()))
                .set_pkce_verifier(PkceCodeVerifier::new(pkce_verifier.to_owned()))
                .request_async(&self.http)
                .await
                .map_err(|err| ProviderError::Exchange(err.to_string()))?;
            let access_token = token_response.access_token().secret();
            let identity = self.identity(access_token).await?;
            let retained = RetainedProviderToken {
                issuer: self.issuer.clone(),
                access_token: access_token.clone(),
                refresh_token: token_response
                    .refresh_token()
                    .map(|token| token.secret().clone()),
                expires_in: token_response.expires_in(),
            };
            Ok(VerifiedLogin { identity, retained })
        })
    }

    fn refresh_provider_token<'a>(
        &'a self,
        refresh_token: &'a str,
    ) -> BoxFuture<'a, Result<RetainedProviderToken, ProviderError>> {
        Box::pin(async move {
            let response = self
                .client
                .exchange_refresh_token(&RefreshToken::new(refresh_token.to_owned()))
                .request_async(&self.http)
                .await
                .map_err(|err| ProviderError::Refresh(err.to_string()))?;
            // Reuse the presented token when the provider did not rotate.
            let rotated = response
                .refresh_token()
                .map_or_else(|| refresh_token.to_owned(), |token| token.secret().clone());
            Ok(RetainedProviderToken {
                issuer: self.issuer.clone(),
                access_token: response.access_token().secret().clone(),
                refresh_token: Some(rotated),
                expires_in: response.expires_in(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        OAuthEndpoints, OAuthProvider, OAuthProviderConfig, ProviderError, subject_at,
        verified_primary,
    };

    fn endpoints() -> OAuthEndpoints {
        OAuthEndpoints {
            authorize_url: "https://idp.example/authorize".to_owned(),
            token_url: "https://idp.example/token".to_owned(),
            user_url: "https://idp.example/user".to_owned(),
        }
    }

    #[test]
    fn a_numeric_or_string_subject_is_read_and_anything_else_is_not() {
        let user = json!({
            "id": 583_231,
            "uuid": "7f9c",
            "empty": "",
            "float": 1.5,
            "flag": true,
            "none": null,
        });
        assert_eq!(subject_at(&user, "/id").as_deref(), Some("583231"));
        assert_eq!(subject_at(&user, "/uuid").as_deref(), Some("7f9c"));
        for absent in ["/empty", "/float", "/flag", "/none", "/missing"] {
            assert_eq!(subject_at(&user, absent), None, "{absent} names no subject");
        }
    }

    #[test]
    fn the_github_email_is_the_primary_verified_entry_alone() {
        let emails = json!([
            { "email": "old@example.com", "primary": false, "verified": true },
            { "email": "main@example.com", "primary": true, "verified": true },
        ]);
        assert_eq!(
            verified_primary(&emails).as_deref(),
            Some("main@example.com")
        );

        let unverified = json!([
            { "email": "main@example.com", "primary": true, "verified": false },
            { "email": "old@example.com", "primary": false, "verified": true },
        ]);
        assert_eq!(
            verified_primary(&unverified),
            None,
            "an unverified primary is no email, and a verified secondary does not stand in"
        );
        assert_eq!(
            verified_primary(&json!({ "message": "Not Found" })),
            None,
            "an answer that is no list holds no email"
        );
    }

    #[test]
    fn a_pointer_without_its_leading_slash_is_refused() {
        let config = OAuthProviderConfig::new(
            "idp",
            "client",
            "https://idp.example",
            "https://app.example/callback",
            endpoints(),
            "id",
        );
        let refused = OAuthProvider::new(config)
            .err()
            .expect("a bare key is no pointer");
        assert!(
            matches!(&refused, ProviderError::Config(detail) if detail.contains("\"id\"")),
            "the refusal names the pointer: {refused}"
        );
    }

    #[test]
    fn an_empty_issuer_label_is_refused() {
        let config = OAuthProviderConfig::new(
            "idp",
            "client",
            "",
            "https://app.example/callback",
            endpoints(),
            "/id",
        );
        assert!(matches!(
            OAuthProvider::new(config),
            Err(ProviderError::Config(_))
        ));
    }

    #[test]
    fn a_plain_http_endpoint_is_refused_off_loopback() {
        let with_user_url = |user_url: &str| {
            OAuthProvider::new(OAuthProviderConfig::new(
                "idp",
                "client",
                "https://idp.example",
                "https://app.example/callback",
                OAuthEndpoints {
                    user_url: user_url.to_owned(),
                    ..endpoints()
                },
                "/id",
            ))
        };
        let refused = with_user_url("http://idp.example/user")
            .err()
            .expect("the access token would cross the network in the clear");
        assert!(
            matches!(&refused, ProviderError::Config(detail) if detail.contains("http://idp.example/user")),
            "the refusal names the endpoint: {refused}"
        );
        for loopback in ["http://127.0.0.1:8080/user", "http://localhost:8080/user"] {
            assert!(
                with_user_url(loopback).is_ok(),
                "{loopback} never leaves the machine"
            );
        }
    }

    #[test]
    fn the_config_debug_line_keeps_the_secret_out() {
        let config = OAuthProviderConfig::github("github", "client", "https://app.example/cb")
            .with_client_secret(Some("planted-secret".to_owned()));
        let shown = format!("{config:?}");
        assert!(!shown.contains("planted-secret"), "{shown}");
    }
}
