//! The claims a provider verified, and the deterministic user id over them.
//!
//! connetto owns no identities table. The deployment's
//! [`AuthHooks::user_for`](crate::authn::hooks::AuthHooks::user_for) maps the
//! [`VerifiedClaims`] to its own typed user id inside the login transaction.
//! See `docs/architecture/11-authentication.md`.

use uuid::Uuid;

/// The verified claims a provider asserted, handed to
/// [`AuthHooks::user_for`](crate::authn::hooks::AuthHooks::user_for).
///
/// `issuer` and `subject` are always present (the only guaranteed unique
/// identifier per `OpenID Connect Core` 5.7). The rest are optional and let a
/// deployment link accounts or gate on assurance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedClaims {
    /// The `iss` claim: the identity provider that issued the credential.
    pub issuer: String,
    /// The `sub` claim: locally unique within the issuer.
    pub subject: String,
    /// The verified `email` claim, if the provider asserted one.
    pub email: Option<String>,
    /// The `name` claim, if present.
    pub name: Option<String>,
    /// The `amr` claim: the authentication methods used.
    pub amr: Vec<String>,
    /// The `acr` claim: the authentication context class reference.
    pub acr: Option<String>,
}

/// Namespace for the deterministic `(issuer, subject)` to `user_id` mapping. A
/// fixed random UUID, per `OpenID Connect Core` 5.7 which makes the issuer and
/// subject pair the only guaranteed unique identifier.
const CONNETTO_ID_NAMESPACE: Uuid = Uuid::from_u128(0x1d3f_9c8a_4b62_4f1e_9a7d_2c5e_8b0f_6a41);

/// Compute the deterministic UUID v5 over `(issuer, subject)`.
#[must_use]
pub fn deterministic_uuid(issuer: &str, subject: &str) -> Uuid {
    Uuid::new_v5(
        &CONNETTO_ID_NAMESPACE,
        format!("{issuer}|{subject}").as_bytes(),
    )
}
