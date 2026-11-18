//! The Tauri layer. It renders the view the native core sends through the
//! channel the page hands it, sends every control to the core's `act`
//! command over invoke, and gives the panes the page source their live
//! queries follow through `tauri-plugin-connetto`. The core owns the client,
//! the keyring, the OS gate and the content store.

use std::cell::RefCell;
use std::collections::HashMap;

use connetto_demo_support as support;
use connetto_demo_support::view::{Action, View};
use connetto_tauri_page::TauriPage;
use js_sys::{Object, Reflect};
use leptos::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

#[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"])]
extern "C" {
    /// Tauri's channel, whose messages the core sends in order.
    type Channel;

    #[wasm_bindgen(constructor)]
    fn new(onmessage: &Closure<dyn FnMut(JsValue)>) -> Channel;

    #[wasm_bindgen(catch)]
    async fn invoke(command: &str, args: JsValue) -> Result<JsValue, JsValue>;
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error(message: &str);
}

/// The live source the panes follow their queries through.
pub type Source = TauriPage;

/// The views' channel and the callback its messages run.
struct ViewChannel {
    _channel: Channel,
    _handler: Closure<dyn FnMut(JsValue)>,
}

thread_local! {
    /// The views' channel, held for the page's life.
    static VIEW_CHANNEL: RefCell<Option<ViewChannel>> = const { RefCell::new(None) };
}

/// The `act` command's arguments.
#[derive(Serialize)]
struct Act<'a> {
    action: &'a Action,
}

/// The `photo_sources` command's arguments.
#[derive(Serialize)]
struct PhotoSources<'a> {
    photos: &'a [support::Photo],
}

/// The `team_note_accounts` command's arguments.
#[derive(Serialize)]
struct TeamNoteAccounts<'a> {
    ids: &'a [String],
}

/// Hand the core a channel for its views, answering with the one view signal
/// the components read. The core sends the current view through it at once
/// and every later one in the order it changed.
pub fn run_page() -> Option<RwSignal<View>> {
    let view = RwSignal::new(View::initial());
    let handler = Closure::<dyn FnMut(JsValue)>::new(move |message: JsValue| {
        match serde_wasm_bindgen::from_value::<View>(message) {
            Ok(next) => view.set(next),
            Err(err) => console_error(&format!("an unreadable view: {err}")),
        }
    });
    let channel = Channel::new(&handler);
    let arguments = Object::new();
    if let Err(err) = Reflect::set(&arguments, &JsValue::from_str("channel"), &channel) {
        console_error(&format!("the view command's arguments: {err:?}"));
        return Some(view);
    }
    VIEW_CHANNEL.with(|cell| {
        *cell.borrow_mut() = Some(ViewChannel {
            _channel: channel,
            _handler: handler,
        });
    });
    spawn_local(async move {
        if let Err(err) = invoke("view", arguments.into()).await {
            console_error(&format!("handing the core the view channel: {err:?}"));
        }
    });
    Some(view)
}

/// The page source, which reaches the client in the native core.
#[must_use]
pub fn source() -> Option<Source> {
    Some(TauriPage::new())
}

/// Send one user control to the native core.
pub fn act(action: Action) {
    let Some(arguments) = plain(&Act { action: &action }) else {
        return;
    };
    spawn_local(async move {
        if let Err(err) = invoke("act", arguments).await {
            console_error(&format!("act failed: {err:?}"));
        }
    });
}

/// Each photo's display source by row id, which the core resolves through
/// its content store, empty when the core cannot answer.
pub async fn photo_sources(rows: Vec<support::Photo>) -> HashMap<String, String> {
    ask("photo_sources", &PhotoSources { photos: &rows }).await
}

/// The account each still-provisional team note came from, by note id, which
/// the core reads from the notes' provenance.
pub async fn team_note_accounts(ids: Vec<String>) -> HashMap<String, String> {
    ask("team_note_accounts", &TeamNoteAccounts { ids: &ids }).await
}

/// Invoke `command` with `arguments` and read its answer, the answer type's
/// `Default` when the command or its answer fails.
async fn ask<Answer: DeserializeOwned + Default>(
    command: &str,
    arguments: &impl Serialize,
) -> Answer {
    let Some(arguments) = plain(arguments) else {
        return Answer::default();
    };
    match invoke(command, arguments).await {
        Ok(answer) => serde_wasm_bindgen::from_value(answer).unwrap_or_else(|err| {
            console_error(&format!("an unreadable {command} answer: {err}"));
            Answer::default()
        }),
        Err(err) => {
            console_error(&format!("{command} failed: {err:?}"));
            Answer::default()
        }
    }
}

/// `arguments` as the plain object Tauri reads command arguments from, which
/// the default serializer does not make.
fn plain(arguments: &impl Serialize) -> Option<JsValue> {
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();
    arguments
        .serialize(&serializer)
        .map_err(|err| console_error(&format!("command arguments: {err}")))
        .ok()
}
