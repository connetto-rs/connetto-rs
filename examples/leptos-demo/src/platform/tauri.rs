//! The Tauri layer. It renders the view the native core emits as its `view`
//! event and sends every control to the core's `act` command over invoke.
//! The core owns the client, the keyring, the OS gate and the content store.

use std::cell::RefCell;

use connetto_demo_support::view::{Action, View};
use leptos::prelude::*;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"], catch)]
    async fn invoke(command: &str, args: JsValue) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "event"], catch)]
    async fn listen(event: &str, handler: &Closure<dyn FnMut(JsValue)>)
    -> Result<JsValue, JsValue>;

    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error(message: &str);
}

/// The `view` event handler the page keeps.
type ViewHandler = Closure<dyn FnMut(JsValue)>;

thread_local! {
    /// The `view` event handler, held for the page's life.
    static VIEW_HANDLER: RefCell<Option<ViewHandler>> = const { RefCell::new(None) };
}

/// The payload of a Tauri event.
#[derive(serde::Deserialize)]
struct Event {
    payload: View,
}

/// The `act` command's arguments.
#[derive(Serialize)]
struct Act<'a> {
    action: &'a Action,
}

/// Listen for the core's views and ask for the first one, answering with the
/// one view signal the components read. Every view arrives as the `view`
/// event, the first one too, which the `view` command emits once the
/// listener stands.
pub fn run_page() -> Option<RwSignal<View>> {
    let view = RwSignal::new(View::initial());
    spawn_local(async move {
        let handler = Closure::<dyn FnMut(JsValue)>::new(move |event: JsValue| {
            match serde_wasm_bindgen::from_value::<Event>(event) {
                Ok(event) => view.set(event.payload),
                Err(err) => console_error(&format!("an unreadable view: {err}")),
            }
        });
        if let Err(err) = listen("view", &handler).await {
            console_error(&format!("listening for views: {err:?}"));
            return;
        }
        VIEW_HANDLER.with(|cell| *cell.borrow_mut() = Some(handler));
        if let Err(err) = invoke("view", JsValue::UNDEFINED).await {
            console_error(&format!("asking for the first view: {err:?}"));
        }
    });
    Some(view)
}

/// Send one user control to the native core.
pub fn act(action: Action) {
    // Tauri reads command arguments from a plain object, which the default
    // serializer does not make.
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();
    let args = match (Act { action: &action }).serialize(&serializer) {
        Ok(args) => args,
        Err(err) => {
            console_error(&format!("act arguments: {err}"));
            return;
        }
    };
    spawn_local(async move {
        if let Err(err) = invoke("act", args).await {
            console_error(&format!("act failed: {err:?}"));
        }
    });
}
