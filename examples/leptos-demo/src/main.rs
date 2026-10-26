//! The Leptos demo's entry point, the same app in a browser and as the Tauri
//! app's UI. Built and served with trunk from `examples/leptos-demo`.

mod app;
mod icons;
mod platform;

use leptos::mount::mount_to_body;
use leptos::prelude::*;

use crate::app::App;

fn main() {
    // A platform context that renders nothing, the browser layer's DB worker
    // or its closing login popup, answers `None`.
    if let Some(view) = platform::run_page() {
        mount_to_body(move || view! { <App view=view /> });
    }
}
