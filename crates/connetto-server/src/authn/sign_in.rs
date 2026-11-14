//! What the application supplies about a sign-in before it is recorded.
//!
//! At each sign-in the server asks the application's describe callback for a
//! description of where the sign-in came from, given the request's headers
//! and the address the server saw. The session row keeps the description and
//! never the address (R36 decision 5).

use core::pin::Pin;
use std::net::SocketAddr;
use std::sync::Arc;

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
