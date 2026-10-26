//! Browser test worker entry points for the yew web demo photo surface, plus
//! the shared items the tests import through the demo.

use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

// Distinct names separate test records within the shared pool.
const ALIGN_DB_PREFIX: &str = "connetto-yew-align";
const ALIGN_AUTH_DB: &str = "connetto-yew-align-auth.sqlite";

const PHOTO_DB_PREFIX: &str = "connetto-yew-photo";
const PHOTO_AUTH_DB: &str = "connetto-yew-photo-auth.sqlite";

/// The one sync schema the worker and every tab build from.
pub use connetto_demo_support::demo_schema;
/// The deployment's base URLs and landing page, the browser tests' sign-in.
pub use connetto_demo_support::{AUTH_BASE, DEMO_WS_URL, auth_landing};

/// Worker entry point for the alignment test; uses `connetto-yew-align*` OPFS files.
///
/// # Errors
///
/// A string describing the VFS, upstream connect, or subscribe failure.
#[wasm_bindgen]
pub async fn db_worker_boot_align() -> Result<(), JsValue> {
    connetto_demo_support::web::boot_with(ALIGN_DB_PREFIX, ALIGN_AUTH_DB).await
}

/// Worker entry point for the photo test; uses `connetto-yew-photo*` OPFS files.
///
/// # Errors
///
/// A string describing the VFS, upstream connect, or subscribe failure.
#[wasm_bindgen]
pub async fn db_worker_photo_boot() -> Result<(), JsValue> {
    connetto_demo_support::web::boot_with(PHOTO_DB_PREFIX, PHOTO_AUTH_DB).await
}
