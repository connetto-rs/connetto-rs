//! The Leptos browser demo's entry point.
//!
//! The page boots the UI once. The dedicated DB worker runs this same wasm
//! module and boots the connetto DB tier instead, because a worker has no
//! Window. Built and served with trunk from `examples/leptos-demo`.

mod app;
mod icons;
mod platform;
mod view;

use leptos::mount::mount_to_body;
use leptos::prelude::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::spawn_local;

use connetto_web::deliver_login_code;

use crate::app::App;
use crate::platform::web::{run_db_worker, run_page};

fn main() {
    connetto_web::logging::init_console();
    // The dedicated DB worker runs this same wasm module. A worker has no
    // Window, so boot the DB tier there instead of rendering the UI.
    if web_sys::window().is_none() {
        spawn_local(async {
            if let Err(err) = run_db_worker().await {
                tracing::error!(error = ?err, "db worker failed");
            }
        });
        return;
    }

    let win = web_sys::window().expect("window context");

    // The OAuth popup lands here with ?code=&state= after the user
    // authenticates. Deliver the code to the waiting worker, then close the
    // popup so the original window continues uninterrupted.
    //
    // A same-window redirect (no opener) delivers the code the same way,
    // clears the query string, and falls through to render the app. In
    // practice the popup path is used so the original worker keeps its PKCE
    // state and can verify the returned state parameter.
    let search = win.location().search().unwrap_or_default();
    if let (Some(code), Some(state)) = (query_param(&search, "code"), query_param(&search, "state"))
    {
        let _ = deliver_login_code(&code, &state);

        // If this window was opened by window.open() for the auth popup, close
        // it now that the code has been delivered to the original window's
        // worker.
        let has_opener = js_sys::Reflect::get(win.as_ref(), &JsValue::from_str("opener"))
            .map(|value| !value.is_null() && !value.is_undefined())
            .unwrap_or(false);
        if has_opener {
            let _ = win.close();
            return;
        }

        // Outside a popup, clear the query string so a reload does not
        // re-deliver an already-spent code.
        let pathname = win.location().pathname().unwrap_or_else(|_| "/".to_owned());
        if let Ok(history) = win.history() {
            let _ = history.replace_state_with_url(&JsValue::NULL, "", Some(&pathname));
        }
    }

    let view = run_page();
    mount_to_body(move || view! { <App view=view /> });
}

/// One `key=value` pair from a URL query string, `None` when the key is
/// absent.
fn query_param(search: &str, key: &str) -> Option<String> {
    search.trim_start_matches('?').split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| v.to_owned())
    })
}
