//! The demo's shared items and the browser test worker entry points, which
//! exist on the browser layer only.
#![cfg(feature = "web")]

use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

// Distinct names separate test records within the shared pool.
const ALIGN_DB_PREFIX: &str = "connetto-leptos-align";
const ALIGN_AUTH_DB: &str = "connetto-leptos-align-auth.sqlite";

const PHOTO_DB_PREFIX: &str = "connetto-leptos-photo";
const PHOTO_AUTH_DB: &str = "connetto-leptos-photo-auth.sqlite";

/// The one sync schema the worker and every tab build from.
pub use connetto_demo_support::demo_schema;
/// The deployment's base URLs and landing page, the browser tests' sign-in.
pub use connetto_demo_support::{AUTH_BASE, DEMO_WS_URL, auth_landing};

/// The one build the worker boots from and every window attaches through.
pub use connetto_demo_support::web::demo_builder;

/// Worker entry point for the alignment test, on `connetto-leptos-align*` OPFS files.
///
/// # Errors
///
/// A string describing the VFS, upstream connect, or subscribe failure.
#[wasm_bindgen]
pub async fn db_worker_boot_align() -> Result<(), JsValue> {
    connetto_demo_support::web::boot_with(ALIGN_DB_PREFIX, ALIGN_AUTH_DB).await
}

/// Worker entry point for the photo test, on `connetto-leptos-photo*` OPFS files.
///
/// # Errors
///
/// A string describing the VFS, upstream connect, or subscribe failure.
#[wasm_bindgen]
pub async fn db_worker_photo_boot() -> Result<(), JsValue> {
    connetto_demo_support::web::boot_with(PHOTO_DB_PREFIX, PHOTO_AUTH_DB).await
}
