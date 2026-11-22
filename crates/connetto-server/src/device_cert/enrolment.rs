//! Enrolling a signed-in device's key (R74 step 3), revoking it and publishing
//! the lists that say so (step 5).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use connetto_core::SessionId;
use connetto_core::device_cert::{
    AttestationLevel, CertificateRequest, CertificateSerial, IssueError, KeyId, ListError,
};
use connetto_core::messages::{DeviceSummary, EnrolGrant, EnrolRefusal, EnrolRequest, SignedList};
use diesel_async::AsyncConnection as _;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::PooledConnection;
use ring::rand::{SecureRandom, SystemRandom};
use tokio::time::Instant;

use super::{DeviceCertConfig, EnrolmentStore, LifetimeError, StatusList, verify};
use crate::authn::schema::{Desc, SessionRecord};
use crate::authn::{
    AuthService, AuthStoreError, CommittedNotice, ConnettoStoreSchema, DeviceEnrolled,
    DeviceRevoked, EndCause, HookError,
};
use crate::schema::ConnettoSchema;

/// The largest descriptor accepted, in bytes.
const DESCRIPTOR_LIMIT: usize = 4096;

/// One certificate the server issued, recorded before it is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enrolment<Id> {
    /// The account the certificate names.
    pub user: Id,
    /// The enrolled device key.
    pub key: KeyId,
    /// The certificate's serial.
    pub serial: [u8; 16],
    /// The issuer that signed it, whose list names it once its key is revoked.
    pub issuer: KeyId,
    /// When the certificate starts, which is when the key was last seen.
    pub issued_at: SystemTime,
    /// When the certificate expires.
    pub expires_at: SystemTime,
    /// The session that asked for it.
    pub session: SessionId,
    /// The device key's `SubjectPublicKeyInfo`, whose hash is the key id.
    pub public_key: Vec<u8>,
    /// The application's device descriptor, as sent.
    pub descriptor: Vec<u8>,
    /// The attestation level the first enrolment recorded, which renewals keep.
    pub attestation: AttestationLevel,
    /// The bot list the certificate carries, the one the enrolling session's
    /// connection binds (R77 decision 12), which a renewal replaces.
    pub bot_list: String,
}

/// One key's enrolment record, what a writer decision needs (R75): the account
/// that holds it, whether the enrolment stands, and whether the key's
/// certificate can still prove a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRecord<Id> {
    /// The account the key is enrolled under.
    pub user: Id,
    /// Whether the enrolment is revoked.
    pub revoked: bool,
    /// Whether the key holds a certificate not expired at the read.
    pub certificate_live: bool,
    /// Whether the key's certificate is not expired past the tolerance a
    /// peer still accepts it within, the read minus `DEVICE_SKEW` (R77
    /// decision 11).
    pub certificate_live_past_skew: bool,
    /// When the key's last certificate expires, `None` when it holds none.
    ///
    /// The server arms one timer per author it answers `Pending` for, at this
    /// instant plus `DEVICE_SKEW`, and answers `Abandoned` to the sequences
    /// still open when it fires (R77 decision 11).
    pub certificate_expiry: Option<SystemTime>,
    /// The device key's `SubjectPublicKeyInfo`.
    pub public_key: Vec<u8>,
    /// The bot list the key's last certificate carries, `None` for an
    /// enrolment recorded before the column held one, which binds nothing
    /// (R78 decision 9).
    pub bot_list: Option<String>,
}

/// What recording an enrolment found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// Recorded, so the certificate may be sent.
    Granted {
        /// Whether the key had no enrolment before, so this is its first.
        first: bool,
    },
    /// The key's enrolment is revoked, so nothing was recorded.
    Revoked,
    /// The key is enrolled under another account, so nothing was recorded.
    HeldElsewhere,
    /// The descriptor does not decode into the deployment's descriptor type,
    /// so nothing was recorded.
    UnreadableDescriptor,
}

/// One enrolled device of an account, as the device list shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    /// The device key.
    pub key: KeyId,
    /// When the key first enrolled.
    pub enrolled_at: SystemTime,
    /// When the key last enrolled or renewed.
    pub last_seen: SystemTime,
    /// When the key was revoked.
    pub revoked_at: Option<SystemTime>,
    /// The session that last enrolled or renewed it.
    pub session: SessionId,
    /// The descriptor the device last sent.
    pub descriptor: Vec<u8>,
}

/// What revoking a key found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Revocation<Id> {
    /// Revoked now, naming the account it was enrolled under and the session
    /// that last enrolled or renewed it.
    Revoked {
        /// The account the key was enrolled under.
        user: Id,
        /// The auth-store session the enrolment records.
        session: SessionId,
    },
    /// Revoked before, naming the session the enrolment recorded, so the
    /// revocation can be repeated against it.
    AlreadyRevoked {
        /// The auth-store session the enrolment records.
        session: SessionId,
    },
    /// No enrolment of that key, for that account when one is named.
    NotFound,
}

/// The enrolment table or the issuer it signs for could not be reached.
#[derive(Debug, thiserror::Error)]
pub enum EnrolmentError {
    /// The connection pool could not hand out a connection.
    #[error("the enrolment pool is unreachable")]
    Pool(#[source] diesel_async::pooled_connection::bb8::RunError),
    /// A statement against the enrolment tables failed.
    #[error("the enrolment tables refused a statement")]
    Query(#[from] diesel::result::Error),
    /// The descriptor would not encode into the tables' columns.
    #[error("the descriptor could not be encoded")]
    Descriptor(#[source] rmp_serde::encode::Error),
    /// A revocation list would not be signed under its issuer.
    #[error(transparent)]
    Signing(#[from] ListError),
    /// An issuer's list number does not fit a CRL Number.
    #[error("a list number does not fit a CRL Number")]
    ListNumber(#[source] std::num::TryFromIntError),
    /// The stored attestation level is not a level the server records.
    #[error("the stored attestation level is not a level")]
    StoredLevel,
    /// The auth store, or the application's hook in its transaction, failed.
    #[error(transparent)]
    Auth(#[from] AuthStoreError),
}

impl EnrolmentError {
    /// The application's refusal, when its hook refused the event.
    fn refusal(&self) -> Option<&str> {
        match self {
            Self::Auth(AuthStoreError::Hook(HookError::Refused(reason))) => Some(reason),
            _ => None,
        }
    }
}

/// The refusal an enrolment answers when the issuer would not sign its
/// certificate, logging the failures that are the server's.
fn issue_refusal(err: &IssueError) -> EnrolRefusal {
    match err {
        IssueError::Identity(_) => EnrolRefusal::Unidentified,
        IssueError::OutlivesIssuer | IssueError::Validity | IssueError::Sign(_) => {
            tracing::warn!(error = %err, "device certificate not issued");
            EnrolRefusal::IssuerUnavailable
        }
    }
}

/// A nonce handed to a session, spent by the next enrolment request whatever its outcome.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingChallenge {
    nonce: [u8; 32],
    expires: Instant,
}

/// Why a device could not be revoked.
#[derive(Debug, thiserror::Error)]
pub enum RevokeError {
    /// No enrolment of that key, for the caller's account.
    #[error("no such device")]
    NotFound,
    /// No device enrolment is installed on the server.
    #[error("no device enrolment is installed")]
    NotInstalled,
    /// The application's hook refused the revocation, with its reason.
    #[error("the application refused the revocation: {0}")]
    Refused(String),
    /// The enrolment table or the issuer failed.
    #[error("the device could not be revoked")]
    Unavailable(#[source] EnrolmentError),
}

impl RevokeError {
    /// The application's refusal as [`RevokeError::Refused`], any other
    /// failure as [`RevokeError::Unavailable`].
    fn of(err: EnrolmentError) -> Self {
        match err.refusal() {
            Some(reason) => Self::Refused(reason.to_owned()),
            None => Self::Unavailable(err),
        }
    }
}

/// A key revoked and committed, with what the caller closes and pushes.
#[derive(Debug)]
pub(crate) struct KeyRevoked<Id> {
    /// Whether the revocation is new.
    pub(crate) fresh: bool,
    /// The session the key last enrolled with.
    pub(crate) session: SessionId,
    /// The session's account, when the revocation ended the session.
    pub(crate) ended: Option<Id>,
    /// The lists published after the commit.
    pub(crate) lists: Vec<SignedList>,
}

/// A source the enrolment draws its nonces and serials from.
pub trait RandomSource: Send + Sync {
    /// Draw random bytes into `dest`.
    ///
    /// # Errors
    ///
    /// The source's error.
    fn fill(&self, dest: &mut [u8]) -> Result<(), ring::error::Unspecified>;
}

impl RandomSource for SystemRandom {
    fn fill(&self, dest: &mut [u8]) -> Result<(), ring::error::Unspecified> {
        SecureRandom::fill(self, dest)
    }
}

/// The issuer settings, the tables enrolments are recorded in, the auth
/// service whose hooks run in the same transactions, and the revocation
/// lists last published.
pub struct DeviceEnrolment<D: ConnettoSchema> {
    config: DeviceCertConfig,
    auth: Arc<AuthService<D>>,
    store: EnrolmentStore<D>,
    random: Box<dyn RandomSource>,
    lists: tokio::sync::Mutex<Option<Vec<SignedList>>>,
    /// The attestation status list an Android chain's serials are checked against.
    status: Option<StatusList>,
    /// The time a certificate is issued at.
    clock: fn() -> SystemTime,
}

/// Seconds since the Unix epoch, zero before it.
fn secs(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

impl<D: ConnettoSchema> DeviceEnrolment<D> {
    /// Issue under `config`, recording into the deployment's enrolment tables
    /// on the owner pool `auth`'s store runs on, with `auth`'s hooks in the
    /// same transactions, and ending through `auth` the session a revoked
    /// device last enrolled with, so a stolen device still signed in cannot
    /// enrol a fresh key and undo the report.
    #[must_use]
    pub fn new(config: DeviceCertConfig, auth: Arc<AuthService<D>>) -> Self {
        Self {
            config,
            auth,
            store: EnrolmentStore::default(),
            random: Box::new(SystemRandom::new()),
            lists: tokio::sync::Mutex::new(None),
            clock: SystemTime::now,
            status: None,
        }
    }

    /// Issue at the time `clock` reads, so a test stands in for a device
    /// whose clock differs from the server's.
    #[cfg(feature = "test-seams")]
    #[must_use]
    pub fn with_issue_clock(mut self, clock: fn() -> SystemTime) -> Self {
        self.clock = clock;
        self
    }

    /// A source the enrolment draws its serials from, so a test holds a
    /// draw the system source would not make.
    #[cfg(feature = "test-seams")]
    #[must_use]
    pub fn with_random(mut self, random: Box<dyn RandomSource>) -> Self {
        self.random = random;
        self
    }

    /// The attestation status list an Android chain's serials are checked
    /// against, fetched by the task the builder starts.
    #[must_use]
    pub fn with_status_list(mut self, list: StatusList) -> Self {
        self.status = Some(list);
        self
    }

    /// An owner connection, from the pool the auth store runs on.
    async fn connection(&self) -> Result<PooledConnection<'_, AsyncPgConnection>, EnrolmentError> {
        self.auth
            .store()
            .pool()
            .get()
            .await
            .map_err(EnrolmentError::Pool)
    }

    /// The enrolled key's record, for a bind signature check, delegating to the store.
    pub(crate) async fn key_record(
        &self,
        user: Option<&D::Id>,
        key: KeyId,
    ) -> Result<Option<KeyRecord<D::Id>>, EnrolRefusal> {
        let mut connection = self
            .connection()
            .await
            .map_err(|_| EnrolRefusal::IssuerUnavailable)?;
        self.store
            .key_record(&mut connection, user, key)
            .await
            .map_err(|_| EnrolRefusal::IssuerUnavailable)
    }

    /// A fresh nonce, replacing any the session held, and how long it stays valid.
    pub(crate) fn challenge(
        &self,
        pending: &mut Option<PendingChallenge>,
    ) -> Result<([u8; 32], Duration), EnrolRefusal> {
        let mut nonce = [0; 32];
        self.random
            .fill(&mut nonce)
            .map_err(|_| EnrolRefusal::IssuerUnavailable)?;
        let window = self.config.challenge_window();
        *pending = Some(PendingChallenge {
            nonce,
            expires: Instant::now() + window,
        });
        Ok((nonce, window))
    }

    /// Issue and record the certificate `request` asks for on behalf of
    /// `user`, carrying `bot_list`, the list the enrolling session's
    /// connection binds (R77 decision 12).
    pub(crate) async fn enrol(
        &self,
        user: &D::Id,
        session: SessionId,
        pending: &mut Option<PendingChallenge>,
        request: EnrolRequest,
        bot_list: &str,
    ) -> Result<EnrolGrant, EnrolRefusal> {
        let challenge = pending.take();
        if request.descriptor.len() > DESCRIPTOR_LIMIT {
            return Err(EnrolRefusal::InvalidRequest);
        }
        let csr =
            CertificateRequest::parse(&request.csr).map_err(|_| EnrolRefusal::InvalidRequest)?;
        let live = challenge.filter(|challenge| {
            Instant::now() < challenge.expires && &challenge.nonce == csr.challenge()
        });
        if live.is_none() {
            return Err(EnrolRefusal::ChallengeExpired);
        }
        let key = KeyId::of_public_key(csr.public_key());
        // The level the first enrolment recorded stands, whatever a renewal sends.
        let stored = match self.connection().await {
            Ok(mut connection) => {
                self.store
                    .stored_attestation(&mut connection, user, key)
                    .await
            }
            Err(err) => Err(err),
        };
        let level = match stored {
            Ok(Some(stored)) => stored,
            Ok(None) => verify(
                &self.config,
                self.status.as_ref(),
                request.attestation.as_ref(),
                &request.csr,
                csr.public_key(),
            )
            .map_err(|mismatch| {
                tracing::warn!(%mismatch, "the device attestation does not match its request");
                EnrolRefusal::InvalidRequest
            })?,
            Err(err) => {
                tracing::warn!(error = %err, "the enrolment table refused the stored level");
                return Err(EnrolRefusal::IssuerUnavailable);
            }
        };
        if !self.config.accepted_attestation().contains(&level) {
            return Err(EnrolRefusal::AttestationRequired);
        }
        let lifetime = self
            .config
            .lifetime_for(request.lifetime_secs.map(Duration::from_secs))
            .map_err(|refused| match refused {
                LifetimeError::OverCeiling { ceiling } => EnrolRefusal::OverCeiling {
                    ceiling_secs: ceiling.as_secs(),
                },
            })?;
        let serial = CertificateSerial::random(|bytes| self.random.fill(bytes))
            .map_err(|_| EnrolRefusal::IssuerUnavailable)?;
        let not_before = (self.clock)();
        let issuer = self.config.issuer();
        let leaf = issuer
            .issue(
                &csr,
                &user.to_string(),
                not_before,
                lifetime,
                serial,
                level,
                bot_list,
            )
            .map_err(|err| issue_refusal(&err))?;
        let descriptor = request.descriptor;
        let enrolment = Enrolment {
            user: user.clone(),
            key,
            serial: serial.as_bytes(),
            issuer: issuer.key_id(),
            issued_at: not_before,
            expires_at: not_before + lifetime,
            session,
            public_key: csr.public_key().to_vec(),
            descriptor: descriptor.clone(),
            attestation: level,
            bot_list: bot_list.to_owned(),
        };
        match self.record(enrolment).await {
            Ok((Recorded::Granted { .. }, first)) => {
                if let Some(session) = first {
                    self.tell_enrolled(session, key, descriptor, not_before);
                }
                Ok(EnrolGrant {
                    request_id: request.request_id,
                    chain: vec![leaf.into(), issuer.certificate().to_vec().into()],
                    revocation_lists: self.lists().await.unwrap_or_default(),
                })
            }
            Ok((Recorded::Revoked, _)) => Err(EnrolRefusal::Revoked),
            Ok((Recorded::HeldElsewhere | Recorded::UnreadableDescriptor, _)) => {
                Err(EnrolRefusal::InvalidRequest)
            }
            Err(err) => Err(record_refusal(&err)),
        }
    }

    /// Tell [`committed`](crate::AuthHooks::committed) that `key` first
    /// enrolled during `session` at `at`, once its transaction committed.
    fn tell_enrolled(
        &self,
        session: SessionRecord<D::Id, Desc<D>>,
        key: KeyId,
        device: Vec<u8>,
        at: SystemTime,
    ) {
        self.auth
            .store()
            .hooks()
            .committed(CommittedNotice::DeviceEnrolled {
                session,
                key,
                device,
                at,
            });
    }

    /// Record `enrolment` in one transaction that, on the key's first
    /// enrolment, also runs [`device_enrolled`](crate::AuthHooks::device_enrolled)
    /// with the session the key enrolled during, answering that session for
    /// the committed notice.
    async fn record(
        &self,
        enrolment: Enrolment<D::Id>,
    ) -> Result<(Recorded, Option<SessionRecord<D::Id, Desc<D>>>), EnrolmentError> {
        let session = enrolment.session;
        let key = enrolment.key;
        let descriptor = enrolment.descriptor.clone();
        let store = &self.store;
        let hooks = self.auth.store().hooks();
        let mut connection = self.connection().await?;
        connection
            .transaction::<_, EnrolmentError, _>(async move |c| {
                let recorded = store.record(c, enrolment).await?;
                if recorded != (Recorded::Granted { first: true }) {
                    return Ok((recorded, None));
                }
                let record = <D::Auth as ConnettoStoreSchema>::session_record(c, session)
                    .await?
                    .ok_or(diesel::result::Error::NotFound)?;
                hooks
                    .device_enrolled(
                        c,
                        &DeviceEnrolled {
                            session: &record,
                            key,
                            device: &descriptor,
                        },
                    )
                    .await
                    .map_err(AuthStoreError::Hook)?;
                Ok((recorded, Some(record)))
            })
            .await
    }

    /// `user`'s enrolments, revoked ones included, each with the session it
    /// last enrolled under.
    pub(crate) async fn enrolled(&self, user: &D::Id) -> Result<Vec<Device>, EnrolmentError> {
        let mut connection = self.connection().await?;
        self.store.devices(&mut connection, user).await
    }

    /// `user`'s devices, as the device list shows them.
    pub(crate) async fn devices(&self, user: &D::Id) -> Result<Vec<DeviceSummary>, EnrolmentError> {
        Ok(self
            .enrolled(user)
            .await?
            .into_iter()
            .map(|device| DeviceSummary {
                key_id: *device.key.as_bytes(),
                enrolled_at_secs: secs(device.enrolled_at),
                last_seen_secs: secs(device.last_seen),
                revoked_at_secs: device.revoked_at.map(secs),
                descriptor: device.descriptor,
            })
            .collect())
    }

    /// The lists last published, built on first use.
    ///
    /// # Errors
    ///
    /// [`EnrolmentError`] when the table or the issuer fails.
    pub async fn lists(&self) -> Result<Vec<SignedList>, EnrolmentError> {
        let mut lists = self.lists.lock().await;
        if let Some(lists) = lists.as_ref() {
            return Ok(lists.clone());
        }
        let built = self.build_lists().await?;
        *lists = Some(built.clone());
        Ok(built)
    }

    /// Sign a fresh list naming every unexpired serial of every revoked key,
    /// under the next number, and keep it as the published one. A failure
    /// drops the cached lists, so the next handshake or grant rebuilds them.
    pub(crate) async fn publish(&self) -> Result<Vec<SignedList>, EnrolmentError> {
        let mut lists = self.lists.lock().await;
        match self.build_lists().await {
            Ok(built) => {
                *lists = Some(built.clone());
                Ok(built)
            }
            Err(err) => {
                *lists = None;
                Err(err)
            }
        }
    }

    /// One list per issuer still signing, the current one and every retired
    /// one not yet expired, then the root's lists as given.
    async fn build_lists(&self) -> Result<Vec<SignedList>, EnrolmentError> {
        let now = SystemTime::now();
        let mut connection = self.connection().await?;
        let mut lists = Vec::new();
        for issuer in self.config.signing_issuers(now) {
            let revoked = self
                .store
                .revoked_serials(&mut connection, issuer.key_id())
                .await?;
            let number = self
                .store
                .next_list_number(&mut connection, issuer.key_id())
                .await?;
            // No list promises a next one later than the longest certificate
            // it could name stays valid.
            let list = issuer.sign_list(number, &revoked, now, now + self.config.ceiling())?;
            lists.push(SignedList {
                list,
                signer: issuer.certificate().to_vec(),
            });
        }
        lists.extend_from_slice(self.config.root_lists());
        Ok(lists)
    }

    /// Revoke `key`, `user`'s when one is named, and the session it last
    /// enrolled with, in one transaction that runs
    /// [`device_revoked`](crate::AuthHooks::device_revoked) when the
    /// revocation is new and
    /// [`sessions_ended`](crate::AuthHooks::sessions_ended) when the session
    /// was live, then publish the list that says so, repeating both when the
    /// key is already revoked. The caller closes the device's connection,
    /// then hands an ended session to [`session_ended`](Self::session_ended).
    ///
    /// # Errors
    ///
    /// [`RevokeError::NotFound`] for a key not enrolled, or not `user`'s,
    /// [`RevokeError::Refused`] when a hook refuses, which leaves the key and
    /// the session standing, and [`RevokeError::Unavailable`] when the table
    /// or the issuer fails.
    pub(crate) async fn revoke(
        &self,
        user: Option<&D::Id>,
        key: KeyId,
    ) -> Result<KeyRevoked<D::Id>, RevokeError> {
        let store = &self.store;
        let auth = self.auth.store();
        let hooks = auth.hooks();
        let mut connection = self.connection().await.map_err(RevokeError::Unavailable)?;
        let revoked = connection
            .transaction::<_, EnrolmentError, _>(async move |c| {
                let (owner, session) = match store.revoke(c, user, key, SystemTime::now()).await? {
                    Revocation::NotFound => return Ok(None),
                    Revocation::AlreadyRevoked { session } => (None, session),
                    Revocation::Revoked { user, session } => {
                        hooks
                            .device_revoked(
                                c,
                                &DeviceRevoked {
                                    user_id: &user,
                                    key,
                                },
                            )
                            .await
                            .map_err(AuthStoreError::Hook)?;
                        (Some(user), session)
                    }
                };
                let ended = auth.revoke_in(c, session, EndCause::Revoked).await?;
                Ok(Some((owner, session, ended)))
            })
            .await
            .map_err(RevokeError::of)?;
        let Some((owner, session, ended)) = revoked else {
            return Err(RevokeError::NotFound);
        };
        let fresh = owner.is_some();
        if let Some(user_id) = owner {
            hooks.committed(CommittedNotice::DeviceRevoked { user_id, key });
        }
        match self.publish().await {
            Ok(lists) => Ok(KeyRevoked {
                fresh,
                session,
                ended,
                lists,
            }),
            Err(err) => {
                // The revocation committed, so its ended session is closed
                // and recorded even though no list could be published.
                if let Some(user) = &ended {
                    self.session_ended(user, session);
                }
                Err(RevokeError::Unavailable(err))
            }
        }
    }

    /// Close and record `session` of `user`, ended by a device revocation
    /// after the device's own connection closed.
    pub(crate) fn session_ended(&self, user: &D::Id, session: SessionId) {
        self.auth.ended_with_device(user, session);
    }

    /// Revoke every live key of `user` on `transaction`, the caller's, only
    /// those last enrolled during `session` when one is named, running
    /// [`device_revoked`](crate::AuthHooks::device_revoked) for each.
    /// Answers each revoked key with the session it last enrolled with, for
    /// [`after_keys_revoked`](Self::after_keys_revoked) once the caller
    /// commits.
    pub(crate) async fn revoke_keys_in(
        &self,
        transaction: &mut AsyncPgConnection,
        user: &D::Id,
        session: Option<SessionId>,
    ) -> Result<Vec<(KeyId, SessionId)>, EnrolmentError> {
        let hooks = self.auth.store().hooks();
        let at = SystemTime::now();
        let mut revoked = Vec::new();
        for device in self.store.devices(transaction, user).await? {
            if device.revoked_at.is_some() || session.is_some_and(|only| only != device.session) {
                continue;
            }
            if let Revocation::Revoked { user, session } = self
                .store
                .revoke(transaction, Some(user), device.key, at)
                .await?
            {
                hooks
                    .device_revoked(
                        transaction,
                        &DeviceRevoked {
                            user_id: &user,
                            key: device.key,
                        },
                    )
                    .await
                    .map_err(AuthStoreError::Hook)?;
                revoked.push((device.key, session));
            }
        }
        Ok(revoked)
    }

    /// Tell [`committed`](crate::AuthHooks::committed) of each key of `user`
    /// a committed transaction revoked, and publish the list that says so.
    ///
    /// # Errors
    ///
    /// [`EnrolmentError`] when the table or the issuer fails.
    pub(crate) async fn after_keys_revoked(
        &self,
        user: &D::Id,
        keys: &[(KeyId, SessionId)],
    ) -> Result<Vec<SignedList>, EnrolmentError> {
        let hooks = self.auth.store().hooks();
        for (key, _) in keys {
            hooks.committed(CommittedNotice::DeviceRevoked {
                user_id: user.clone(),
                key: *key,
            });
        }
        self.publish().await
    }
}

/// The refusal an enrolment answers when recording it failed, the
/// application's own reason when its hook refused.
fn record_refusal(err: &EnrolmentError) -> EnrolRefusal {
    if let Some(reason) = err.refusal() {
        return EnrolRefusal::Refused {
            reason: reason.to_owned(),
        };
    }
    tracing::warn!(error = %err, "the enrolment table refused a record");
    EnrolRefusal::IssuerUnavailable
}
