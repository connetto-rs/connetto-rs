//! The demo's platform and the shared page, plus the browser test worker
//! entry points the `photo_flow` suite drives.

pub mod platform;
pub mod ui;

// The photo_flow suite spawns a dedicated worker pointed at the app's glue
// module, which auto-runs `main` in a worker context and then calls one of
// these. Each names its own OPFS files, so Chrome's delayed handle release
// after `Worker.terminate()` never blocks the next worker's file open.
#[cfg(target_arch = "wasm32")]
mod worker_tests {
    use wasm_bindgen::JsValue;
    use wasm_bindgen::prelude::wasm_bindgen;

    /// Worker entry point for the alignment test.
    ///
    /// # Errors
    ///
    /// A string describing the VFS, upstream connect, or subscribe failure.
    #[wasm_bindgen]
    pub async fn db_worker_boot_align() -> Result<(), JsValue> {
        connetto_demo_support::web::boot_with(
            "connetto-dioxus-align",
            "connetto-dioxus-align-auth.sqlite",
        )
        .await
    }

    /// Worker entry point for the photo test.
    ///
    /// # Errors
    ///
    /// A string describing the VFS, upstream connect, or subscribe failure.
    #[wasm_bindgen]
    pub async fn db_worker_photo_boot() -> Result<(), JsValue> {
        connetto_demo_support::web::boot_with(
            "connetto-dioxus-photo",
            "connetto-dioxus-photo-auth.sqlite",
        )
        .await
    }
}

#[cfg(target_arch = "wasm32")]
pub use worker_tests::{db_worker_boot_align, db_worker_photo_boot};
