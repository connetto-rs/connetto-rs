//! The auth store: identity mapping, connetto's sessions, and the rotating
//! refresh tokens, held in Postgres through typed diesel queries over the
//! deployment's [`ConnettoStoreSchema`](crate::authn::schema::ConnettoStoreSchema).
//! See `docs/architecture/11-authentication.md`.
//!
//! The refresh token is `"<session_id>.<secret>"`. Only the SHA-256 of the
//! secret is stored, and every rotation replaces it. A presented secret whose
//! hash is not the current one is treated as reuse of a rotated-out token
//! (theft) and revokes the session.

use std::time::SystemTime;

use connetto_core::SessionId;
use connetto_core::auth::AuthContext;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::authn::hooks::HookError;
use crate::authn::identity::VerifiedClaims;
use crate::ban::BanError;

/// A caller identity resolved from a verified credential, ready to mint a
/// session for. Human-readable fields never serve as identity: only the
/// issuer and subject do, per the identity-mapping rule.
#[derive(Debug, Clone)]
pub struct ResolvedIdentity {
    /// The `iss` claim of the credential the identity was verified from.
    pub issuer: String,
    /// The `sub` claim, locally unique within the issuer.
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

impl ResolvedIdentity {
    /// The verified provider claims to hand
    /// [`AuthHooks::user_for`](crate::authn::hooks::AuthHooks::user_for).
    #[must_use]
    pub fn verified_claims(&self) -> VerifiedClaims {
        VerifiedClaims {
            issuer: self.issuer.clone(),
            subject: self.subject.clone(),
            email: self.email.clone(),
            name: self.name.clone(),
            amr: self.amr.clone(),
            acr: self.acr.clone(),
        }
    }
}

/// Where a sign-in came from, beside the identity it verified: the
/// configured provider it came through and the application's description of
/// it.
#[derive(Debug, Clone)]
pub struct SessionOrigin<Desc> {
    /// The configured provider name, such as `google`.
    pub provider: String,
    /// The application's description, which its describe callback returned.
    pub description: Desc,
}

/// A newly minted session: its id, the identity it carries, and the first
/// refresh token to hand the client.
#[derive(Debug, Clone)]
pub struct IssuedSession<Id = String> {
    /// The connetto-minted session id, named by the access token's `sid`.
    pub session_id: SessionId,
    /// The identity the session's access tokens carry.
    pub context: AuthContext<Id>,
    /// The refresh token to hand the client. Presented back to rotate.
    pub refresh_token: String,
    /// When this session stops being refreshable if never used again: the
    /// smaller of the idle deadline and the absolute ceiling. The client
    /// keeps it to warn before an offline session lapses with unsynced data.
    pub session_expires_at: SystemTime,
    /// When the session began, by the store's clock.
    pub created_at: SystemTime,
}

/// The result of a successful refresh: the rotated token plus the identity to
/// mint the new access token from.
#[derive(Debug, Clone)]
pub struct RefreshOutcome<Id = String> {
    /// The session that was refreshed.
    pub session_id: SessionId,
    /// The identity to mint the new access token from.
    pub context: AuthContext<Id>,
    /// The rotated refresh token, replacing the presented one.
    pub refresh_token: String,
    /// When this session stops being refreshable if never used again, the
    /// smaller of the (freshly slid) idle deadline and the absolute ceiling.
    pub session_expires_at: SystemTime,
}

/// Failure surfaced by a [`DbAuthStore`].
#[derive(Debug, thiserror::Error)]
pub enum AuthStoreError {
    /// No session matched, or the presented token was malformed.
    #[error("no such session")]
    NotFound,
    /// The refresh token is past its idle window or absolute ceiling.
    #[error("refresh token expired")]
    Expired,
    /// A rotated-out refresh token was presented for a live session. The
    /// session was revoked as a theft response.
    ///
    /// It names the session so the layer holding the revocation observer can
    /// close whatever connection that session still has open. Without the id
    /// the theft response could only refuse the next handshake, leaving the
    /// live socket streaming, which is the opposite of what a stolen
    /// credential warrants.
    #[error("refresh token reuse detected, session revoked")]
    Reuse {
        /// The session the replayed token named, now revoked.
        session_id: SessionId,
    },
    /// The application's hook refused the sign-in or failed, which rolled it
    /// back.
    #[error(transparent)]
    Hook(#[from] HookError),
    /// The identity is banned, read inside the sign-in's transaction.
    #[error("the identity is banned")]
    Banned,
    /// The ban list could not be read, so the sign-in is refused rather than
    /// let through.
    #[error("the ban list could not be read: {0}")]
    BanList(#[from] BanError),
    /// The backing store failed.
    #[error("auth store backend error: {0}")]
    Backend(String),
}

impl From<diesel::result::Error> for AuthStoreError {
    fn from(err: diesel::result::Error) -> Self {
        Self::Backend(err.to_string())
    }
}

/// A fresh 256-bit refresh secret as hex.
fn new_refresh_secret() -> String {
    format!(
        "{}{}",
        Uuid::new_v4().as_simple(),
        Uuid::new_v4().as_simple()
    )
}

/// SHA-256 of a refresh secret, the only form stored.
fn hash_secret(secret: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.finalize().into()
}

/// Constant-time equality of two refresh-secret hashes, so the comparison
/// cannot leak how many leading bytes matched through timing.
fn hashes_match(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq as _;
    a.ct_eq(b).into()
}

/// Mint a fresh session id. connetto-core carries no random number generator,
/// so the randomness belongs here, at the login that owns the session.
fn new_session_id() -> SessionId {
    SessionId::from_uuid(Uuid::new_v4())
}

/// Assemble the wire refresh token from a session id and secret.
///
/// One of the two sanctioned textual edges for a session id (the other is the
/// JWT `sid` claim). Everywhere else it moves as bytes.
fn format_refresh(session_id: SessionId, secret: &str) -> String {
    format!("{session_id}.{secret}")
}

/// Split a wire refresh token into its session id and secret.
///
/// A malformed id half is not found rather than an error shape of its own: a
/// caller may present anything here, and an unparseable id names no session.
pub(crate) fn split_refresh(token: &str) -> Option<(SessionId, &str)> {
    let (id, secret) = token.split_once('.')?;
    Some((id.parse().ok()?, secret))
}

pub use db::DbAuthStore;

mod db {
    use std::sync::Arc;

    use connetto_core::SessionId;
    use connetto_core::auth::AuthContext;
    use diesel_async::pooled_connection::bb8::Pool;
    use diesel_async::{AsyncConnection, AsyncPgConnection};

    use super::{
        AuthStoreError, IssuedSession, RefreshOutcome, ResolvedIdentity, SessionOrigin,
        format_refresh, hash_secret, hashes_match, new_refresh_secret, new_session_id,
        split_refresh,
    };
    use crate::authn::hooks::{AuthHooks, SignedIn};
    use crate::authn::provider::RetainedProviderToken;
    use crate::authn::schema::{
        ConnettoStoreSchema, Desc, NewSession, SessionRecord, SessionRow, SessionStamps,
    };
    use crate::authn::token::RefreshLifetimes;
    use crate::ban::{BanStore, lock_account};
    use crate::schema::ConnettoSchema;

    /// The Postgres auth store, generic over the deployment's schema. Durable
    /// across restart and the only variant a promoted standby carries.
    pub struct DbAuthStore<D: ConnettoSchema> {
        pool: Pool<AsyncPgConnection>,
        lifetimes: RefreshLifetimes,
        hooks: Arc<dyn AuthHooks<D>>,
    }

    impl<D: ConnettoSchema> DbAuthStore<D> {
        /// Build over a connection pool, running the application's `hooks`
        /// inside the transactions that record its auth events.
        #[must_use]
        pub fn new(
            pool: Pool<AsyncPgConnection>,
            lifetimes: RefreshLifetimes,
            hooks: Arc<dyn AuthHooks<D>>,
        ) -> Self {
            Self {
                pool,
                lifetimes,
                hooks,
            }
        }

        /// The application's hooks, which a caller tells once an event's
        /// transaction committed.
        #[must_use]
        pub fn hooks(&self) -> &dyn AuthHooks<D> {
            &*self.hooks
        }

        /// Delete the retained provider tokens of `sessions`, which were just
        /// revoked in the same transaction (R105 decision 5).
        async fn forget(
            conn: &mut AsyncPgConnection,
            sessions: Vec<SessionId>,
        ) -> Result<(), AuthStoreError> {
            <D::Auth as ConnettoStoreSchema>::forget_provider_tokens(conn, sessions)
                .await
                .map_err(backend)
        }
    }

    fn backend<E: core::fmt::Display>(err: E) -> AuthStoreError {
        AuthStoreError::Backend(err.to_string())
    }

    impl<D: ConnettoSchema> DbAuthStore<D> {
        /// The description every session carries when the deployment declares
        /// none, `None` when a describe callback must supply one.
        #[must_use]
        pub fn undescribed() -> Option<Desc<D>> {
            <D::Auth as ConnettoStoreSchema>::undescribed()
        }

        /// Sign `identity` in as one transaction: the application's
        /// [`user_for`](AuthHooks::user_for) answers the user id, the account's
        /// lock is taken, `bans` is read, the session row and `retained` are
        /// written, and [`signed_in`](AuthHooks::signed_in) runs. Any error
        /// rolls every write back, the hooks' own included.
        ///
        /// Answers the session with its first refresh token, and its row.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Hook`] when a hook refuses or fails,
        /// [`AuthStoreError::Banned`] when `bans` holds a ban that applies,
        /// [`AuthStoreError::BanList`] when it cannot be read, and
        /// [`AuthStoreError::Backend`] when a write fails.
        pub async fn sign_in(
            &self,
            identity: &ResolvedIdentity,
            origin: SessionOrigin<Desc<D>>,
            retained: Option<&RetainedProviderToken>,
            bans: Option<&dyn BanStore<D::Id>>,
        ) -> Result<(IssuedSession<D::Id>, SessionRecord<D::Id, Desc<D>>), AuthStoreError> {
            let claims = &identity.verified_claims();
            let secret = new_refresh_secret();
            let current_refresh_hash = hash_secret(&secret).to_vec();
            let session_id = new_session_id();
            let hooks = &*self.hooks;
            let lifetimes = &self.lifetimes;
            let mut conn = self.pool.get().await.map_err(backend)?;
            let (record, expires_at) = conn
                .transaction::<_, AuthStoreError, _>(async move |conn| {
                    let user_id = hooks.user_for(conn, claims).await?;
                    lock_account(conn, &user_id).await?;
                    if let Some(bans) = bans
                        && bans
                            .check(conn, &user_id)
                            .await?
                            .is_some_and(|ban| ban.applies)
                    {
                        return Err(AuthStoreError::Banned);
                    }
                    // The creation time and the deadlines are stamped by the
                    // database's clock, not this process's (chapter 21).
                    let SessionStamps {
                        created_at,
                        idle_deadline,
                        absolute_deadline,
                    } = <D::Auth as ConnettoStoreSchema>::new_session(
                        conn,
                        NewSession {
                            session_id,
                            user_id: user_id.clone(),
                            current_refresh_hash,
                            idle_window: lifetimes.idle_window(),
                            absolute_ceiling: lifetimes.absolute_ceiling(),
                            provider: origin.provider.clone(),
                            issuer: identity.issuer.clone(),
                            description: origin.description.clone(),
                        },
                    )
                    .await
                    .map_err(backend)?;
                    if let Some(token) = retained {
                        Self::retain(conn, session_id, token).await?;
                    }
                    let record = SessionRecord {
                        session_id,
                        user_id,
                        provider: origin.provider,
                        issuer: identity.issuer.clone(),
                        description: origin.description,
                        created_at: created_at.into(),
                        refreshed_at: None,
                    };
                    hooks
                        .signed_in(
                            conn,
                            &SignedIn {
                                user_id: &record.user_id,
                                claims,
                                session: &record,
                            },
                        )
                        .await?;
                    Ok((record, idle_deadline.min(absolute_deadline)))
                })
                .await?;
            let issued = IssuedSession {
                session_id,
                context: AuthContext {
                    user_id: record.user_id.clone(),
                },
                refresh_token: format_refresh(session_id, &secret),
                session_expires_at: expires_at.into(),
                created_at: record.created_at,
            };
            Ok((issued, record))
        }

        /// How the session began and when it last refreshed, `None` when the
        /// id names no session.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the read fails.
        pub async fn session_record(
            &self,
            session_id: SessionId,
        ) -> Result<Option<SessionRecord<D::Id, Desc<D>>>, AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            <D::Auth as ConnettoStoreSchema>::session_record(&mut conn, session_id)
                .await
                .map_err(backend)
        }

        /// Every live session of `user`, oldest first: not revoked, and
        /// neither deadline passed.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the read fails.
        pub async fn sessions_of(
            &self,
            user: &D::Id,
        ) -> Result<Vec<SessionRecord<D::Id, Desc<D>>>, AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            <D::Auth as ConnettoStoreSchema>::sessions_of(&mut conn, user.clone())
                .await
                .map_err(backend)
        }

        /// Revoke `session` when it is `user`'s and not yet revoked,
        /// answering whether it was.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the update fails.
        pub async fn end_owned(
            &self,
            user: &D::Id,
            session: SessionId,
        ) -> Result<bool, AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            let user = user.clone();
            conn.transaction::<_, AuthStoreError, _>(async move |conn| {
                let ended = <D::Auth as ConnettoStoreSchema>::end_owned(conn, user, session)
                    .await
                    .map_err(backend)?;
                if ended {
                    Self::forget(conn, vec![session]).await?;
                }
                Ok(ended)
            })
            .await
        }

        /// Revoke every live session of `user` but `keep`, answering the ones
        /// it revoked.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the update fails.
        pub async fn end_all_of(
            &self,
            user: &D::Id,
            keep: Option<SessionId>,
        ) -> Result<Vec<SessionId>, AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            let user = user.clone();
            conn.transaction::<_, AuthStoreError, _>(async move |conn| {
                let ended = <D::Auth as ConnettoStoreSchema>::end_all_of(conn, user, keep)
                    .await
                    .map_err(backend)?;
                Self::forget(conn, ended.clone()).await?;
                Ok(ended)
            })
            .await
        }

        /// Whether the session still exists, is not revoked, and is within
        /// both its idle window and its absolute ceiling. This is the
        /// handshake liveness check that makes revocation authoritative.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the read fails.
        pub async fn session_is_live(&self, session_id: SessionId) -> Result<bool, AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            let live = <D::Auth as ConnettoStoreSchema>::live_session(&mut conn, session_id)
                .await
                .map_err(backend)?;
            Ok(live.is_some_and(|revoked| !revoked))
        }

        /// Validate the presented refresh token, rotate it, extend the idle
        /// window, and return the rotated token plus the identity to re-mint
        /// access from.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::NotFound`] for an unknown or revoked session,
        /// [`AuthStoreError::Expired`] past a deadline,
        /// [`AuthStoreError::Reuse`] for a rotated-out secret, which revokes
        /// the session, and [`AuthStoreError::Backend`] when a statement fails.
        pub async fn rotate_refresh(
            &self,
            refresh_token: &str,
        ) -> Result<RefreshOutcome<D::Id>, AuthStoreError> {
            let (session_id, secret) =
                split_refresh(refresh_token).ok_or(AuthStoreError::NotFound)?;
            let presented_hash = hash_secret(secret).to_vec();
            let new_secret = new_refresh_secret();
            let new_hash = hash_secret(&new_secret).to_vec();
            let mut conn = self.pool.get().await.map_err(backend)?;
            let outcome = {
                conn.transaction::<_, AuthStoreError, _>(async move |conn| {
                    // The lapsed verdict is the database's, judged in the
                    // statement (chapter 21).
                    let row: Option<SessionRow<D::Id>> =
                        <D::Auth as ConnettoStoreSchema>::session_row_for_update(conn, session_id)
                            .await
                            .map_err(backend)?;
                    let SessionRow {
                        user_id,
                        current_refresh_hash,
                        revoked,
                        idle_lapsed,
                        absolute_lapsed,
                    } = row.ok_or(AuthStoreError::NotFound)?;
                    if revoked {
                        return Err(AuthStoreError::NotFound);
                    }
                    if absolute_lapsed || idle_lapsed {
                        return Err(AuthStoreError::Expired);
                    }
                    if !hashes_match(&presented_hash, &current_refresh_hash) {
                        // Reuse of a rotated-out token signals theft, but the
                        // error rolls this transaction back, so the revoke
                        // lands below in its own committed statement.
                        return Err(AuthStoreError::Reuse { session_id });
                    }
                    let (idle_deadline, absolute_deadline) =
                        <D::Auth as ConnettoStoreSchema>::rotation_update(
                            conn,
                            session_id,
                            new_hash,
                            self.lifetimes.idle_window(),
                        )
                        .await
                        .map_err(backend)?;
                    let context = AuthContext { user_id };
                    Ok(RefreshOutcome {
                        session_id,
                        context,
                        refresh_token: format_refresh(session_id, &new_secret),
                        session_expires_at: idle_deadline.min(absolute_deadline).into(),
                    })
                })
                .await
            };
            if matches!(outcome, Err(AuthStoreError::Reuse { .. })) {
                conn.transaction::<_, AuthStoreError, _>(async move |conn| {
                    <D::Auth as ConnettoStoreSchema>::revoke_update(conn, session_id)
                        .await
                        .map_err(backend)?;
                    Self::forget(conn, vec![session_id]).await
                })
                .await?;
            }
            outcome
        }

        /// Revoke the session, refusing it on the next handshake even while
        /// its access token is still time-valid.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the update fails.
        pub async fn revoke_session(&self, session_id: SessionId) -> Result<(), AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            conn.transaction::<_, AuthStoreError, _>(async move |conn| {
                <D::Auth as ConnettoStoreSchema>::revoke_update(conn, session_id)
                    .await
                    .map_err(backend)?;
                Self::forget(conn, vec![session_id]).await
            })
            .await
        }

        /// Revoke every session still live, returning how many were, the
        /// answer to a restore that revived rotated refresh tokens.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the update fails.
        pub async fn revoke_every_session(&self) -> Result<u64, AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            let revoked = conn
                .transaction::<_, AuthStoreError, _>(async move |conn| {
                    let revoked = <D::Auth as ConnettoStoreSchema>::revoke_every_update(conn)
                        .await
                        .map_err(backend)?;
                    <D::Auth as ConnettoStoreSchema>::forget_every_provider_token(conn)
                        .await
                        .map_err(backend)?;
                    Ok(revoked)
                })
                .await?;
            Ok(u64::try_from(revoked).unwrap_or(u64::MAX))
        }

        /// The session a presented refresh token names, once its secret is
        /// verified, or `None` when no live session matches.
        ///
        /// This is the authentication a logout needs, since the caller holds
        /// the credential it wants torn down and nothing else identifies the
        /// session to revoke. Unlike [`rotate_refresh`](Self::rotate_refresh)
        /// it does not rotate, and a mismatched secret is `None` rather than a
        /// theft signal, because an endpoint whose only effect is revocation
        /// must not become an oracle for guessed session ids.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the read fails.
        pub async fn session_for_refresh(
            &self,
            refresh_token: &str,
        ) -> Result<Option<SessionId>, AuthStoreError> {
            let Some((session_id, secret)) = split_refresh(refresh_token) else {
                return Ok(None);
            };
            let presented_hash = hash_secret(secret).to_vec();
            let mut conn = self.pool.get().await.map_err(backend)?;
            // The rotation read already selects the refresh hash and the
            // revoked flag, and its row lock serializes a logout against a
            // refresh landing at the same moment.
            let Some(SessionRow {
                current_refresh_hash,
                revoked,
                ..
            }) = <D::Auth as ConnettoStoreSchema>::session_row_for_update(&mut conn, session_id)
                .await
                .map_err(backend)?
            else {
                return Ok(None);
            };
            if revoked || !hashes_match(&presented_hash, &current_refresh_hash) {
                return Ok(None);
            }
            Ok(Some(session_id))
        }

        /// Store the retained provider tokens for a session, replacing any
        /// existing.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the write fails.
        pub async fn set_retained_provider_token(
            &self,
            session_id: SessionId,
            token: &RetainedProviderToken,
        ) -> Result<(), AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            Self::retain(&mut conn, session_id, token).await
        }

        /// Write `session_id`'s retained provider tokens on `conn`, replacing
        /// any existing.
        async fn retain(
            conn: &mut AsyncPgConnection,
            session_id: SessionId,
            token: &RetainedProviderToken,
        ) -> Result<(), AuthStoreError> {
            <D::Auth as ConnettoStoreSchema>::set_provider_token(
                conn,
                session_id,
                token.issuer.clone(),
                token.access_token.clone(),
                token.refresh_token.clone(),
                token.expires_in,
            )
            .await
            .map_err(backend)
        }

        /// Read a session's retained provider tokens, if any were stored, with
        /// the store's verdict on the access token's lifetime.
        ///
        /// # Errors
        ///
        /// [`AuthStoreError::Backend`] when the read fails.
        pub async fn retained_provider_token(
            &self,
            session_id: SessionId,
        ) -> Result<Option<crate::authn::provider::StoredProviderToken>, AuthStoreError> {
            let mut conn = self.pool.get().await.map_err(backend)?;
            <D::Auth as ConnettoStoreSchema>::provider_token(&mut conn, session_id)
                .await
                .map_err(backend)
        }
    }
}
