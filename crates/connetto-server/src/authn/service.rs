//! The authentication service: login, refresh, revoke, and the handshake
//! authority.
//!
//! [`AuthService`] mints connetto tokens at login and rotates them at refresh
//! over an [`AuthStore`]. [`ConnettoHandshakeAuthority`] is the real
//! [`HandshakeAuthority`] the server injects: it checks each grant's signature
//! locally, confirms in the same store that a login grant's run is still live,
//! so a revoked run is refused on the next handshake even while its token is
//! time-valid, and it signs the resume blob a caller with no identity presents.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use connetto_core::SessionId;
use connetto_core::auth::{Subject, VerifiedSession};
use connetto_core::messages::Grant;
use connetto_core::traits::{
    CheckedGrant, GrantCheckFuture, GrantRefused, HandleError, HandshakeAuthority,
};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex as AsyncMutex;

use crate::authn::provider::{
    ProviderError, ProviderRegistry, RetainedProviderToken, VerifiedLogin,
};
use crate::authn::schema::SessionRecord;
use crate::authn::sign_in::{LoginRequest, SessionDescriber, SignInHook, SignInNotice};
use crate::authn::store::{
    AuthStore, AuthStoreError, IssuedSession, ResolvedIdentity, SessionOrigin, split_refresh,
};
use crate::authn::token::{TokenAuthority, TokenError};
use crate::guard::RequestGuard;

/// The access token plus its rotating refresh token, returned by login and
/// refresh.
///
/// No key material rides this pair. The per-replica encryption key is minted on
/// the device that owns the replica, so the server never sees, stores, or
/// forwards one.
#[derive(Debug, Clone)]
pub struct TokenPair<Id> {
    /// The short-lived access token, presented as a grant on the handshake.
    pub access_token: String,
    /// The rotating refresh token, presented back to the refresh endpoint.
    pub refresh_token: String,
    /// The access token's lifetime in seconds, echoed as `expires_in`.
    pub expires_in_secs: u64,
    /// The typed `user_id` this session belongs to, carried to the client as
    /// the deployment's own id rather than as text. The client selects its
    /// replica file from it, so a re-authentication resolving to a different
    /// identity opens a different file instead of resuming this one's.
    pub user_id: Id,
    /// Unix-seconds instant the local session lapses if never refreshed again.
    /// The client warns before it passes with unsynced data queued.
    pub session_expires_at_secs: u64,
}

/// Unix seconds for a [`SystemTime`], clamped at the epoch.
fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

/// Failure of a login or refresh.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The auth store rejected the operation.
    #[error(transparent)]
    Store(#[from] AuthStoreError),
    /// Minting the access token failed.
    #[error(transparent)]
    Token(#[from] TokenError),
    /// A provider operation (a retained-token refresh) failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// The caller's refresh quota is exhausted. Wait at least this long before retrying.
    #[error("rate limited")]
    RateLimited(Duration),
    /// The deployment declares a session description and no describe
    /// callback supplies one.
    #[error("no describe callback supplies the session description")]
    Undescribed,
    /// The identity is banned, answered at the token endpoints with the plain
    /// `401` every refused credential gets (R36 decision 4).
    #[error("refused")]
    Banned,
    /// The ban list could not be read, so the identity is refused rather
    /// than let through (R36 fails closed).
    #[error("the ban list could not be read: {0}")]
    BanList(String),
}

/// Mints and rotates connetto tokens over an [`AuthStore`].
///
/// Generic over the store so the concrete choice (in-memory or database) is
/// made once at startup, and every awaited store future stays `Send`.
pub struct AuthService<S: AuthStore> {
    authority: Arc<TokenAuthority>,
    store: Arc<S>,
    /// The provider registry, for the lazy retained-token refresh. `None`
    /// leaves retained tokens un-refreshable (returned as stored).
    registry: Option<Arc<ProviderRegistry>>,
    /// Per-session async locks serializing retained-token refreshes so two
    /// concurrent callers on one node cannot double-refresh and trip the
    /// provider's rotation-reuse defense.
    refresh_locks: Mutex<HashMap<SessionId, Arc<AsyncMutex<()>>>>,
    /// Observer fired after a session is revoked, set once at startup. The
    /// deployment points it at the session manager so revocation closes the
    /// session's live connection rather than only refusing its next handshake.
    revocation_hook: std::sync::OnceLock<SessionRevocationHook>,
    /// Sink for the durable record of access changes, set once at startup.
    /// `None` records nothing, which is what an in-memory deployment wants.
    audit_hook: std::sync::OnceLock<crate::audit::AuditHook<S::Id>>,
    /// The application's sign-in callback, set once at startup. `None`
    /// tells nobody.
    sign_in_hook: std::sync::OnceLock<SignInHook<S::Id, S::Descriptor>>,
    /// The application's describe callback, `None` when the deployment
    /// declares no description.
    describer: Option<SessionDescriber<S::Descriptor>>,
    /// Every counter connetto keeps about a caller, shared with the session
    /// manager because the failed-renewal signal fires here and the other three
    /// fire there.
    guard: Arc<RequestGuard<S::Id>>,
}

/// Observes a session revocation. Fired synchronously after the store revoke
/// succeeds, so an async close belongs on a spawned task inside the hook.
pub type SessionRevocationHook = Arc<dyn Fn(SessionId) + Send + Sync>;

/// Why an access token names no live session.
#[derive(Debug, thiserror::Error)]
pub enum LivenessError {
    /// The token is not a valid login access token.
    #[error("not a valid login access token")]
    Refused,
    /// The token's session was revoked or lapsed.
    #[error("the session is no longer live")]
    Revoked,
    /// The token's identity is banned.
    #[error("the identity is banned")]
    Banned,
    /// The auth store could not answer.
    #[error(transparent)]
    Store(#[from] AuthStoreError),
    /// The ban list could not be read.
    #[error("the ban list could not be read: {0}")]
    BanList(String),
}

/// What a login row and a sign-in notice say about how a session began,
/// taken before the store consumes the origin.
struct Started<Desc> {
    provider: String,
    issuer: String,
    description: Desc,
}

impl<Desc: Clone> Started<Desc> {
    fn of(identity: &ResolvedIdentity, origin: &SessionOrigin<Desc>) -> Self {
        Self {
            provider: origin.provider.clone(),
            issuer: identity.issuer.clone(),
            description: origin.description.clone(),
        }
    }
}

impl<S: AuthStore> AuthService<S> {
    /// Build over a shared token authority and store, with no provider registry.
    ///
    /// The `guard` is the same instance the session manager holds, so a person's
    /// abuse tally spans both surfaces.
    #[must_use]
    pub fn new(
        authority: Arc<TokenAuthority>,
        store: Arc<S>,
        guard: Arc<RequestGuard<S::Id>>,
    ) -> Self {
        Self {
            authority,
            store,
            registry: None,
            refresh_locks: Mutex::new(HashMap::new()),
            revocation_hook: std::sync::OnceLock::new(),
            audit_hook: std::sync::OnceLock::new(),
            sign_in_hook: std::sync::OnceLock::new(),
            describer: None,
            guard,
        }
    }

    /// Attach the provider registry that backs the lazy retained-token accessor.
    #[must_use]
    pub fn with_registry(mut self, registry: Arc<ProviderRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Attach the application's describe callback, which every sign-in asks
    /// for the session's description.
    #[must_use]
    pub fn with_session_describer(mut self, describer: SessionDescriber<S::Descriptor>) -> Self {
        self.describer = Some(describer);
        self
    }

    /// The counters this service shares with the session manager.
    #[must_use]
    pub fn guard(&self) -> &Arc<RequestGuard<S::Id>> {
        &self.guard
    }

    /// Attach the revocation observer, once, after construction. A second call
    /// is ignored, which suits the one startup wiring point this exists for.
    pub fn set_revocation_hook(&self, hook: SessionRevocationHook) {
        let _ = self.revocation_hook.set(hook);
    }

    /// Attach the audit sink, once, after construction. A second call is
    /// ignored, matching the revocation observer beside it.
    ///
    /// Unset, connetto records nothing, which is the right default: the table
    /// is the deployment's and connetto emits no DDL, so a deployment that has
    /// not created it must not have writes attempted against it.
    pub fn set_audit_hook(&self, hook: crate::audit::AuditHook<S::Id>) {
        let _ = self.audit_hook.set(hook);
    }

    /// Attach the application's sign-in callback, once, after construction. A
    /// second call is ignored, matching the audit sink beside it.
    pub fn set_sign_in_hook(&self, hook: SignInHook<S::Id, S::Descriptor>) {
        let _ = self.sign_in_hook.set(hook);
    }

    /// Record one access change, if a sink is attached.
    fn record(&self, event: crate::audit::AuthEvent<S::Id>) {
        if let Some(hook) = self.audit_hook.get() {
            hook(event);
        }
    }

    /// The application's description of the sign-in `request` completes,
    /// from its describe callback, or the deployment's fixed one when it
    /// declares none.
    ///
    /// # Errors
    ///
    /// [`AuthError::Undescribed`] when the deployment declares a description
    /// and no describe callback is attached.
    pub async fn describe(&self, request: LoginRequest) -> Result<S::Descriptor, AuthError> {
        match &self.describer {
            Some(describer) => Ok(describer(request).await),
            None => S::undescribed().ok_or(AuthError::Undescribed),
        }
    }

    /// How `session_id` began and when it last refreshed, `None` when it
    /// names no session.
    pub(crate) async fn session_record(
        &self,
        session_id: SessionId,
    ) -> Result<Option<SessionRecord<S::Id, S::Descriptor>>, AuthError> {
        Ok(self.store.session_record(session_id).await?)
    }

    /// Create a session for a verified identity and mint its first token pair.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the store or the mint fails.
    pub async fn login(
        &self,
        identity: &ResolvedIdentity,
        origin: SessionOrigin<S::Descriptor>,
    ) -> Result<TokenPair<S::Id>, AuthError> {
        let now = SystemTime::now();
        let started = Started::of(identity, &origin);
        let user_id = self.store.resolve(identity).await?;
        self.refuse_banned(&user_id).await?;
        let issued = self
            .store
            .create_session_for(user_id, identity, origin)
            .await?;
        self.finish_login(issued, now, started, |issued| {
            tracing::info!(
                session = %issued.session_id,
                user = %issued.context.user_id,
                "login succeeded, session created"
            );
        })
    }

    /// Create a session for a verified login and retain its provider tokens.
    ///
    /// The login callback uses this: the identity and the provider tokens
    /// both come from the provider's verified response.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the store or the mint fails.
    pub async fn login_with_provider(
        &self,
        login: &VerifiedLogin,
        origin: SessionOrigin<S::Descriptor>,
    ) -> Result<TokenPair<S::Id>, AuthError> {
        let now = SystemTime::now();
        let started = Started::of(&login.identity, &origin);
        let user_id = self.store.resolve(&login.identity).await?;
        self.refuse_banned(&user_id).await?;
        let issued = self
            .store
            .create_session_for(user_id, &login.identity, origin)
            .await?;
        self.store
            .set_retained_provider_token(issued.session_id, &login.retained)
            .await?;
        self.finish_login(issued, now, started, |issued| {
            tracing::info!(
                session = %issued.session_id,
                user = %issued.context.user_id,
                issuer = %login.retained.issuer,
                "login succeeded, session created with retained provider tokens"
            );
        })
    }

    /// Mint the first token pair for a freshly created session, register its
    /// owner with the guard, record the login, tell the application, and
    /// assemble the response.
    ///
    /// `announce` runs once the mint has succeeded, so a failed mint still
    /// logs nothing. The owner registration is what R19 attributes later abuse
    /// through, so it must not fall off either login path.
    fn finish_login(
        &self,
        issued: IssuedSession<S::Id>,
        now: SystemTime,
        started: Started<S::Descriptor>,
        announce: impl FnOnce(&IssuedSession<S::Id>),
    ) -> Result<TokenPair<S::Id>, AuthError> {
        let access_token = self
            .authority
            .mint_access(&issued.context, issued.session_id, now)?;
        announce(&issued);
        self.guard
            .learn_owner(issued.session_id, &issued.context.user_id);
        self.record(
            crate::audit::AuthEvent::new(
                issued.session_id,
                Some(issued.context.user_id.clone()),
                crate::audit::AuthOp::LoggedIn,
            )
            .about_login(started.provider.clone(), started.issuer.clone()),
        );
        if let Some(hook) = self.sign_in_hook.get() {
            hook(SignInNotice::SignedIn(SessionRecord {
                session_id: issued.session_id,
                user_id: issued.context.user_id.clone(),
                provider: started.provider,
                issuer: started.issuer,
                description: started.description,
                created_at: issued.created_at,
                refreshed_at: None,
            }));
        }
        Ok(TokenPair {
            access_token,
            refresh_token: issued.refresh_token,
            expires_in_secs: self.authority.access_ttl().as_secs(),
            user_id: issued.context.user_id,
            session_expires_at_secs: unix_secs(issued.session_expires_at),
        })
    }

    /// A currently-valid provider access token for `session_id`, refreshing it
    /// inline against the provider when the stored one is about to expire and
    /// persisting the rotated refresh token.
    ///
    /// This is server-side only: it never emits a provider token to a client,
    /// which would break the Backend-For-Frontend boundary. Concurrent callers
    /// on one node are serialized per session. There is no background refresh.
    ///
    /// Returns `None` when the session retained no provider token. When no
    /// registry is attached or the issuer routes to no provider, the stored
    /// token is returned unrefreshed.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the store or the provider refresh fails.
    ///
    /// # Panics
    ///
    /// Panics if the `refresh_locks` mutex is poisoned by a thread that panicked while holding it.
    pub async fn provider_access_token(
        &self,
        session_id: SessionId,
    ) -> Result<Option<String>, AuthError> {
        let lock = {
            let mut locks = self.refresh_locks.lock().expect("refresh locks");
            Arc::clone(
                locks
                    .entry(session_id)
                    .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
            )
        };
        let _guard = lock.lock().await;

        let Some(retained) = self.store.retained_provider_token(session_id).await? else {
            return Ok(None);
        };
        if retained.still_valid {
            return Ok(Some(retained.access_token));
        }
        let (Some(refresh_token), Some(registry)) =
            (retained.refresh_token.as_deref(), self.registry.as_ref())
        else {
            return Ok(Some(retained.access_token));
        };
        let Some(provider) = registry.by_issuer(&retained.issuer) else {
            return Ok(Some(retained.access_token));
        };
        let refreshed = provider.refresh_provider_token(refresh_token).await?;
        // Preserve the tenant-specific issuer resolved at login.
        let refreshed = RetainedProviderToken {
            issuer: retained.issuer,
            ..refreshed
        };
        self.store
            .set_retained_provider_token(session_id, &refreshed)
            .await?;
        Ok(Some(refreshed.access_token))
    }

    /// Rotate the presented refresh token and mint a fresh access token.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the token is invalid, expired, reused, or the mint fails.
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenPair<S::Id>, AuthError> {
        // Parse the session id before touching the store. A token that does not
        // parse names nothing, so skip all metering and let the store return its
        // own error unchanged.
        let named_session = split_refresh(refresh_token).map(|(sid, _)| sid);
        if let Some(session_id) = named_session
            && let Some(wait) = self.guard.refresh_blocked(session_id)
        {
            return Err(AuthError::RateLimited(wait));
        }
        // The ban is read before the token rotates. A ban list that cannot
        // answer then leaves the token standing, where rotating first would
        // burn it and the caller's retry after the outage would read as a
        // stolen token. A ban landing after this read ends the session
        // through the hook, and the rotation below refuses an ended session.
        if let Some(session_id) = named_session
            && let Some(record) = self.store.session_record(session_id).await?
        {
            match self.refuse_banned(&record.user_id).await {
                Ok(()) => {}
                Err(AuthError::Banned) => {
                    if self.store.end_owned(&record.user_id, session_id).await? {
                        self.ended(
                            &record.user_id,
                            session_id,
                            crate::audit::AuthOp::EndedByBan,
                        );
                    }
                    return Err(AuthError::Banned);
                }
                Err(err) => return Err(err),
            }
        }
        let outcome = match self.store.rotate_refresh(refresh_token).await {
            Ok(outcome) => outcome,
            Err(err) => {
                // Only a credential failure counts. A store that is down fails
                // the honest attempts too, and charging those would turn an
                // outage into a lockout outliving it.
                if let Some(session_id) = named_session
                    && matches!(
                        err,
                        AuthStoreError::NotFound
                            | AuthStoreError::Expired
                            | AuthStoreError::Reuse { .. }
                    )
                {
                    let _ = self.guard.refresh_failed(session_id);
                }
                // The store revokes on theft but cannot close anything: the
                // observer lives here. Without this the stolen-credential case
                // was the one case that left the socket streaming.
                if let AuthStoreError::Reuse { session_id } = &err {
                    tracing::warn!(
                        session = %session_id,
                        "refresh token reuse detected, session revoked as theft"
                    );
                    self.notify_revoked(*session_id);
                    // Not `revoke_as`: the store already revoked inside the
                    // rotation, so going back through it would write the row a
                    // second time.
                    self.record(crate::audit::AuthEvent::new(
                        *session_id,
                        None,
                        crate::audit::AuthOp::TokenReplayed,
                    ));
                }
                return Err(err.into());
            }
        };
        let now = SystemTime::now();
        let access_token = self
            .authority
            .mint_access(&outcome.context, outcome.session_id, now)?;
        self.guard
            .learn_owner(outcome.session_id, &outcome.context.user_id);
        tracing::info!(
            session = %outcome.session_id,
            user = %outcome.context.user_id,
            "refresh token rotated, access token reissued"
        );
        Ok(TokenPair {
            access_token,
            refresh_token: outcome.refresh_token,
            expires_in_secs: self.authority.access_ttl().as_secs(),
            user_id: outcome.context.user_id,
            session_expires_at_secs: unix_secs(outcome.session_expires_at),
        })
    }

    /// Revoke a session, refusing it on the next handshake.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the store fails.
    pub async fn revoke(&self, session_id: SessionId) -> Result<(), AuthError> {
        self.revoke_as(session_id, crate::audit::AuthOp::SessionRevoked)
            .await
    }

    /// Revoke every live session, returning how many were live.
    ///
    /// A restore rewinds the session store to the backup, which brings back refresh tokens rotated away since, so the server calls this whenever it finds itself on a restored or another cluster (R70).
    /// No audit row is written, since the row names one session, and the server logs the count and the cause.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the store fails.
    pub async fn revoke_every_session(&self) -> Result<u64, AuthError> {
        Ok(self.store.revoke_every_session().await?)
    }

    /// Every live session of `user`, oldest first.
    pub(crate) async fn sessions_of(
        &self,
        user: &S::Id,
    ) -> Result<Vec<SessionRecord<S::Id, S::Descriptor>>, AuthError> {
        Ok(self.store.sessions_of(user).await?)
    }

    /// End `user`'s session `session` as the person's own choice, closing its
    /// live connection and recording `ended_by_owner`. Answers whether the
    /// session was `user`'s and still live.
    pub(crate) async fn end_session(
        &self,
        user: &S::Id,
        session: SessionId,
    ) -> Result<bool, AuthError> {
        if !self.store.end_owned(user, session).await? {
            return Ok(false);
        }
        self.ended(user, session, crate::audit::AuthOp::EndedByOwner);
        Ok(true)
    }

    /// End every live session of `user` but `keep`, closing each live
    /// connection and recording `ended_others`, or `ended_all` when nothing
    /// is kept. Answers the sessions it ended.
    pub(crate) async fn end_sessions(
        &self,
        user: &S::Id,
        keep: Option<SessionId>,
    ) -> Result<Vec<SessionId>, AuthError> {
        let op = if keep.is_some() {
            crate::audit::AuthOp::EndedOthers
        } else {
            crate::audit::AuthOp::EndedAll
        };
        let ended = self.store.end_all_of(user, keep).await?;
        for session in &ended {
            self.ended(user, *session, op);
        }
        Ok(ended)
    }

    /// End every session of the banned identity `user`, recording
    /// `ended_by_ban` for each (R105 decision 8).
    pub(crate) async fn end_banned(&self, user: &S::Id) -> Result<Vec<SessionId>, AuthError> {
        let ended = self.store.end_all_of(user, None).await?;
        for session in &ended {
            self.ended(user, *session, crate::audit::AuthOp::EndedByBan);
        }
        Ok(ended)
    }

    /// Refuse `user` when the ban list holds a ban on it, and when the list
    /// cannot be read.
    async fn refuse_banned(&self, user: &S::Id) -> Result<(), AuthError> {
        match self.guard.banned(user).await {
            Ok(None) => Ok(()),
            Ok(Some(_)) => Err(AuthError::Banned),
            Err(err) => Err(AuthError::BanList(err.detail().to_owned())),
        }
    }

    /// Revoke every live session of `user` for the application's own
    /// reasons, closing each live connection and recording
    /// `session_revoked`, the sign-out an erasure runs first. Answers the
    /// sessions it revoked.
    pub(crate) async fn revoke_person(&self, user: &S::Id) -> Result<Vec<SessionId>, AuthError> {
        let revoked = self.store.end_all_of(user, None).await?;
        for session in &revoked {
            self.ended(user, *session, crate::audit::AuthOp::SessionRevoked);
        }
        Ok(revoked)
    }

    /// Close an ended session's live connection and record why it ended.
    fn ended(&self, user: &S::Id, session: SessionId, op: crate::audit::AuthOp) {
        tracing::info!(session = %session, cause = op.label(), "session ended");
        self.notify_revoked(session);
        self.record(crate::audit::AuthEvent::new(
            session,
            Some(user.clone()),
            op,
        ));
    }

    /// The person and session a login access token names, once its signature
    /// verifies and its session is live in the store, the check the sync
    /// handshake makes (R105 decision 7).
    ///
    /// # Errors
    ///
    /// [`LivenessError::Refused`] for anything but a valid login access
    /// token, [`LivenessError::Revoked`] when its session ended,
    /// [`LivenessError::Banned`] when its identity is banned, and
    /// [`LivenessError::Store`] or [`LivenessError::BanList`] when the store
    /// or the ban list cannot answer.
    pub(crate) async fn verify_live(
        &self,
        access_token: &str,
    ) -> Result<VerifiedSession<S::Id>, LivenessError> {
        let checked = self
            .authority
            .check_grant::<S::Id, serde::de::IgnoredAny>(&Grant::new(access_token))
            .map_err(|_| LivenessError::Refused)?;
        let Subject::Identity(session) = checked.subject else {
            return Err(LivenessError::Refused);
        };
        if !self.store.session_is_live(session.session_id).await? {
            return Err(LivenessError::Revoked);
        }
        match self.guard.banned(&session.context.user_id).await {
            Ok(None) => Ok(session),
            Ok(Some(_)) => Err(LivenessError::Banned),
            Err(err) => Err(LivenessError::BanList(err.detail().to_owned())),
        }
    }

    /// Whether the store still holds `session_id` live, the handshake's own
    /// liveness check, exposed so a data download refuses a dead session.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the store fails.
    pub async fn session_is_live(&self, session_id: SessionId) -> Result<bool, AuthError> {
        Ok(self.store.session_is_live(session_id).await?)
    }

    /// Revoke, and record the cause. Three causes reach here and they are not
    /// interchangeable: an audit row saying only that a login ended cannot tell
    /// an ordinary logout from a stolen credential, which is the most valuable
    /// thing this table reports.
    ///
    /// The row names no user. Every producer here holds a session and not an
    /// identity, and `session` joins `connetto_sessions`, which has the user,
    /// so the alternative was a store round trip per revocation to denormalise
    /// a column the join already answers.
    async fn revoke_as(
        &self,
        session_id: SessionId,
        op: crate::audit::AuthOp,
    ) -> Result<(), AuthError> {
        self.store.revoke_session(session_id).await?;
        tracing::info!(session = %session_id, cause = op.label(), "session revoked");
        self.notify_revoked(session_id);
        self.record(crate::audit::AuthEvent::new(session_id, None, op));
        Ok(())
    }

    /// Tell the deployment's observer that a session died, so it can close the
    /// connection that session still holds. Every revocation path goes through
    /// here, which is what keeps the theft response and the logout equivalent.
    fn notify_revoked(&self, session_id: SessionId) {
        if let Some(hook) = self.revocation_hook.get() {
            hook(session_id);
        } else {
            tracing::warn!(
                session = %session_id,
                "revocation hook not set: the session's live socket will keep receiving patches \
                 until its next handshake is refused"
            );
        }
    }

    /// Log out the session the presented refresh token names: verify the token,
    /// then revoke.
    ///
    /// Returns whether a session was revoked. `false` means the token named no
    /// live session, which is indistinguishable to the caller from success on
    /// purpose: an endpoint whose only effect is revocation must not report
    /// whether a guessed credential existed.
    ///
    /// This is server-side liveness, not token expiry. The session is refused at
    /// the next handshake, and its refresh token stops rotating, while any access
    /// token already minted for it stays signature-valid until it expires. That
    /// bound is the access token's TTL and it is why the TTL is short.
    ///
    /// # Errors
    ///
    /// [`AuthError`] if the store fails.
    pub async fn logout(&self, refresh_token: &str) -> Result<bool, AuthError> {
        let Some(session_id) = self.store.session_for_refresh(refresh_token).await? else {
            return Ok(false);
        };
        self.revoke_as(session_id, crate::audit::AuthOp::LoggedOut)
            .await?;
        Ok(true)
    }

    /// Build the handshake authority sharing this service's token authority
    /// and store.
    #[must_use]
    pub fn handshake_authority(&self) -> ConnettoHandshakeAuthority<S> {
        ConnettoHandshakeAuthority {
            authority: Arc::clone(&self.authority),
            store: Arc::clone(&self.store),
        }
    }
}

/// The real [`HandshakeAuthority`]: check each grant against connetto's own
/// public key, confirm a login grant's run is still live in the store, and sign
/// the resume blob an unidentified run presents.
///
/// It closes the spoofing hole because a caller is only ever whoever a token
/// connetto signed says it is, and it closes the handle hole because a run with
/// no identity resumes only on a blob connetto signed.
pub struct ConnettoHandshakeAuthority<S: AuthStore> {
    authority: Arc<TokenAuthority>,
    store: Arc<S>,
}

impl<S: AuthStore> ConnettoHandshakeAuthority<S> {
    /// Build over a shared token authority and store.
    #[must_use]
    pub fn new(authority: Arc<TokenAuthority>, store: Arc<S>) -> Self {
        Self { authority, store }
    }
}

impl<S, Key> HandshakeAuthority<S::Id, Key> for ConnettoHandshakeAuthority<S>
where
    S: AuthStore + 'static,
    Key: DeserializeOwned + Send + 'static,
{
    fn check_grant<'a>(&'a self, grant: &'a Grant) -> GrantCheckFuture<'a, S::Id, Key> {
        Box::pin(async move {
            let checked = self
                .authority
                .check_grant::<S::Id, Key>(grant)
                .map_err(|err| GrantRefused::Invalid(err.to_string()))?;
            let subject = match checked.subject {
                // Only a login has something to keep alive. A capability is
                // withdrawn by deleting the relation that grants it, which the
                // authorization model answers per question, so asking a store
                // here would invent a liveness concept the design deliberately
                // has not.
                Subject::Identity(session) => {
                    let live = self
                        .store
                        .session_is_live(session.session_id)
                        .await
                        .map_err(|err| GrantRefused::Invalid(err.to_string()))?;
                    if !live {
                        return Err(GrantRefused::Revoked);
                    }
                    Subject::Identity(session)
                }
                subject @ Subject::Capability(_) => subject,
            };
            Ok(CheckedGrant {
                subject,
                expires_at: checked.expires_at,
            })
        })
    }

    fn mint_handle(&self, session_id: SessionId) -> Result<String, HandleError> {
        self.authority
            .mint_resume(session_id, SystemTime::now())
            .map_err(|err| HandleError(err.to_string()))
    }

    fn read_handle(&self, blob: &str) -> Result<SessionId, HandleError> {
        self.authority
            .verify_resume(blob)
            .map_err(|err| HandleError(err.to_string()))
    }
}
