//! A browser that refuses OPFS still boots a signed-in client, on a replica in
//! memory, and the boot says so, so the application can warn that a reload
//! loses every unsent write.
//!
//! A Firefox private window refuses `navigator.storage.getDirectory` with a
//! `SecurityError`. This suite makes the worker's storage manager answer the
//! same way before anything installs, then runs the logged-in startup.
//!
//! The browser-stack runner supplies Postgres, OpenFGA, the provider, keys, and the sync server.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{play_the_tab, worker_builder};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Make this worker's `navigator.storage.getDirectory` reject as a Firefox
/// private window does.
fn refuse_opfs() {
    let navigator = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("navigator"))
        .expect("the worker has a navigator");
    let storage = js_sys::Reflect::get(&navigator, &JsValue::from_str("storage"))
        .expect("the navigator has a storage manager");
    let refuse = js_sys::Function::new_no_args(
        "return Promise.reject(new DOMException('The operation is insecure.', 'SecurityError'))",
    );
    js_sys::Reflect::set(&storage, &JsValue::from_str("getDirectory"), &refuse)
        .expect("shadow getDirectory on the storage manager");
}

#[wasm_bindgen_test]
async fn a_signed_in_boot_without_opfs_reports_a_replica_in_memory() {
    refuse_opfs();
    let logins_served = play_the_tab();
    let booted = worker_builder()
        .boot::<String>()
        .await
        .expect("the startup completes without OPFS");
    assert_eq!(
        logins_served.get(),
        1,
        "the startup signed in through the tab"
    );
    assert!(booted.identity.is_some(), "the startup signed somebody in");
    assert!(
        !booted.replica_persistent,
        "the replica is in memory, so a reload loses it and every unsent write"
    );
    assert_eq!(
        booted.content_persistent,
        Some(false),
        "the content store falls back to memory too"
    );
}
