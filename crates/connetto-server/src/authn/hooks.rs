//! The application's code inside connetto's auth transactions.
//!
//! [`AuthHooks`] has one method per auth event. A transactional method runs on
//! the owner connection inside the transaction that records its event, and its
//! error rolls the event back. [`AuthHooks::committed`] runs after the commit
//! and cannot fail the event. See `docs/architecture/11-authentication.md`.

use core::pin::Pin;
use std::time::SystemTime;

use connetto_core::SessionId;
use connetto_core::device_cert::KeyId;
use diesel_async::AsyncPgConnection;

use crate::authn::identity::{VerifiedClaims, deterministic_uuid};
use crate::authn::schema::{Desc, SessionRecord};
use crate::schema::ConnettoSchema;

/// The answer of one transactional hook.
pub type HookFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, HookError>> + Send + 'a>>;

/// Why a hook refused its event, which rolls the event back.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HookError {
    /// The application refuses the event. A refused login answers with this
    /// text.
    #[error("refused: {0}")]
    Refused(String),
    /// The application's own write failed.
    #[error("hook backend error: {0}")]
    Backend(String),
}

/// What a sign-in wrote, handed to [`AuthHooks::signed_in`] before it commits.
#[derive(Debug)]
pub struct SignedIn<'a, Id, Desc> {
    /// The user id [`AuthHooks::user_for`] answered.
    pub user_id: &'a Id,
    /// The claims the provider verified.
    pub claims: &'a VerifiedClaims,
    /// The session row the sign-in inserted.
    pub session: &'a SessionRecord<Id, Desc>,
}

/// A device key the account never had, enrolled during a session, handed to
/// [`AuthHooks::device_enrolled`] before it commits.
#[derive(Debug)]
pub struct DeviceEnrolled<'a, Id, Desc> {
    /// The session the key enrolled during.
    pub session: &'a SessionRecord<Id, Desc>,
    /// The enrolled key.
    pub key: KeyId,
    /// The device's descriptor, the `MessagePack` the device sent.
    pub device: &'a [u8],
}

/// An enrolled device key revoked, handed to [`AuthHooks::device_revoked`]
/// before it commits.
#[derive(Debug)]
pub struct DeviceRevoked<'a, Id> {
    /// The account the key was enrolled under.
    pub user_id: &'a Id,
    /// The revoked key.
    pub key: KeyId,
}

/// Live sessions of one account ended together, handed to
/// [`AuthHooks::sessions_ended`] before it commits.
#[derive(Debug)]
pub struct SessionsEnded<'a, Id> {
    /// The account the sessions belong to.
    pub user_id: &'a Id,
    /// The sessions that were live and are now ended.
    pub sessions: &'a [SessionId],
    /// Why they ended.
    pub cause: EndCause,
}

/// Why sessions ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndCause {
    /// The session's holder logged out.
    LoggedOut,
    /// The deployment revoked it, or the device enrolled during it was
    /// revoked.
    Revoked,
    /// A refresh token rotated away was presented again, which reads as theft.
    TokenReplayed,
    /// The person ended this one session of theirs.
    EndedByOwner,
    /// The person signed out everywhere else.
    EndedOthers,
    /// The person signed out everywhere.
    EndedAll,
    /// The account is banned.
    Banned,
    /// The person is erased.
    Erased,
    /// The server found the database restored or replaced at boot.
    DatabaseRestored,
}

/// An event whose transaction committed, handed to [`AuthHooks::committed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedNotice<Id, Desc> {
    /// A sign-in created a session.
    SignedIn {
        /// The user id [`AuthHooks::user_for`] answered.
        user_id: Id,
        /// The claims the provider verified.
        claims: VerifiedClaims,
        /// The session the sign-in created.
        session: SessionRecord<Id, Desc>,
    },
    /// A device key the account never had enrolled was enrolled during a
    /// session. A renewal of a known key tells nothing.
    DeviceEnrolled {
        /// The session the key enrolled during.
        session: SessionRecord<Id, Desc>,
        /// The enrolled key.
        key: KeyId,
        /// The device's descriptor, the `MessagePack` the device sent.
        device: Vec<u8>,
        /// When the enrolment was issued.
        at: SystemTime,
    },
    /// An enrolled device key was revoked.
    DeviceRevoked {
        /// The account the key was enrolled under.
        user_id: Id,
        /// The revoked key.
        key: KeyId,
    },
    /// Live sessions of one account ended.
    SessionsEnded {
        /// The account the sessions belong to.
        user_id: Id,
        /// The sessions that ended.
        sessions: Vec<SessionId>,
        /// Why they ended.
        cause: EndCause,
    },
    /// A person was erased, every session and device key of theirs revoked.
    PersonErased {
        /// The erased person.
        person: Id,
    },
}

/// The application's subscription to connetto's auth events. A method left at
/// its default subscribes to nothing.
pub trait AuthHooks<D: ConnettoSchema>: Send + Sync + 'static {
    /// Map a verified login to its user id, first in the login transaction,
    /// where an application registers or links its user.
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, D::Id>;

    /// The sign-in's session row and retained provider token are written and
    /// not yet committed.
    fn signed_in<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _signed_in: &'a SignedIn<'a, D::Id, Desc<D>>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// A key the account never had enrolled is enrolled, its rows written
    /// and not yet committed.
    fn device_enrolled<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _enrolled: &'a DeviceEnrolled<'a, D::Id, Desc<D>>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// An enrolled key is revoked and not yet committed.
    fn device_revoked<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _revoked: &'a DeviceRevoked<'a, D::Id>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Live sessions of one account are ended and not yet committed.
    fn sessions_ended<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _ended: &'a SessionsEnded<'a, D::Id>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// A person is erased, every session and device key of theirs revoked
    /// in this transaction, where the application deletes its own rows.
    fn person_erased<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _person: &'a D::Id,
    ) -> HookFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// An event's transaction committed. Runs synchronously and cannot fail
    /// the event, so mail or a call to a service belongs on a spawned task.
    fn committed(&self, _notice: CommittedNotice<D::Id, Desc<D>>) {}
}

/// The hooks a deployment that registers nothing runs: each login's user id
/// is the deterministic v5 UUID over `(issuer, subject)`, rendered as text.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultAuthHooks;

impl<D: ConnettoSchema<Id = String>> AuthHooks<D> for DefaultAuthHooks {
    fn user_for<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        let user_id = deterministic_uuid(&claims.issuer, &claims.subject).to_string();
        Box::pin(async move { Ok(user_id) })
    }
}
