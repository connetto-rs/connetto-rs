//! The demo's processes over the connetto libraries, and the settings they
//! share.
//!
//! - `demo-server` serves sync, login and files, configured by
//!   [`server_env`].
//! - `demo-embed` mounts the same server in an application's own router.
//! - `demo-client` syncs one subscription and pushes optional writes.
//! - `demo-schema` writes and checks the tier exports a history holds.
//! - `demo-ca` runs the device certificate authority's offline ceremonies.
//! - `demo-idp` starts a local identity provider for the server to sign in
//!   through.
//! - `secret-store-probe` writes and reads back a replica key and a refresh
//!   token through one Linux secret store.

pub mod env;
pub mod server_env;
