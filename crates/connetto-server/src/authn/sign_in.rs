//! What the application learns at a sign-in, and what it supplies.
//!
//! At each sign-in the server asks the application's describe callback for a
//! description of where the sign-in came from, given the request's headers
//! and the address the server saw. The session row keeps the description and
//! never the address (R36 decision 5). After the sign-in, and after a device
//! key's first enrolment, the server tells the application through its
//! sign-in callback, so the application can notify the person.

use core::pin::Pin;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use connetto_core::device_cert::KeyId;

use crate::authn::schema::SessionRecord;

/// The sign-in request a describe callback reads.
#[derive(Debug, Clone)]
pub struct LoginRequest {
    /// The configured provider the sign-in came through.
    pub provider: String,
    /// The headers of the request that completed the sign-in, the user
    /// agent's return from its provider.
    pub headers: axum::http::HeaderMap,
    /// The address the request came from, absent when the server is served
    /// without connect info. Behind a reverse proxy this is the proxy, and
    /// the forwarded header the deployment trusts names the person.
    pub peer: Option<SocketAddr>,
}

/// The application's description of a sign-in, returned for each
/// [`LoginRequest`].
pub type SessionDescriber<Desc> =
    Arc<dyn Fn(LoginRequest) -> Pin<Box<dyn Future<Output = Desc> + Send>> + Send + Sync>;

/// What the server tells the application through its sign-in callback.
#[derive(Debug, Clone)]
pub enum SignInNotice<Id, Desc> {
    /// A sign-in created a session.
    SignedIn(SessionRecord<Id, Desc>),
    /// A device key the account never had enrolled during a session. A
    /// renewal of a known key tells nothing.
    DeviceEnrolled {
        /// The session the key enrolled during.
        session: SessionRecord<Id, Desc>,
        /// The enrolled key.
        key: KeyId,
        /// The device's descriptor, the `MessagePack` the device sent, which
        /// decodes into the application's own device descriptor type.
        device: Vec<u8>,
        /// When the enrolment was issued.
        at: SystemTime,
    },
}

/// Receives each [`SignInNotice`]. Fired synchronously, so a notice that
/// sends mail or calls a service belongs on a spawned task inside the hook.
pub type SignInHook<Id, Desc> = Arc<dyn Fn(SignInNotice<Id, Desc>) + Send + Sync>;
