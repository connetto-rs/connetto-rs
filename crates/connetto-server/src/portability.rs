//! A person's data requests (R61): who asks, and the records a download and
//! an erasure leave in `auth_events`.
//!
//! Both records are written and awaited, never fired and forgotten, because
//! they exist for disputes, so a request whose record cannot be written fails.

use crate::audit::{AuditWriteError, AuthEvent, AuthOp};
use connetto_core::SessionId;

/// Who asks for a person's data request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataRequester {
    /// A signed-in session, which a download requires to be live.
    Session(SessionId),
    /// Staff acting with no session behind them, as the application names
    /// them, for example `staff:marco` or a ticket id.
    Operator(String),
}

impl DataRequester {
    /// The audit event this requester leaves for `person`.
    pub(crate) fn event<Id>(&self, person: Id, op: AuthOp) -> AuthEvent<Id> {
        match self {
            Self::Session(session) => AuthEvent::new(*session, Some(person), op),
            Self::Operator(acted_by) => AuthEvent::by_operator(Some(person), op, acted_by.clone()),
        }
    }
}

/// A data request's record could not be written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DataRequestError {
    /// The record could not be written.
    #[error(transparent)]
    Audit(#[from] AuditWriteError),
}
