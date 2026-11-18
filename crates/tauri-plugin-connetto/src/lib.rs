//! Tauri plugin serving connetto live queries from the native core to the
//! webview (R116).
//!
//! The client runs in the core, which keeps running while the platform pauses
//! the webview. The page names a registered [`LiveSpec`](connetto_client::LiveSpec)
//! with the `subscribe` command, and the plugin follows it on the source the
//! application put in the [`SourceSlot`], sending each answer through a
//! [`Channel`] the page created, as MessagePack bytes. A keyed spec's later
//! answers cross as diffs from the answer the page acknowledged with the `ack`
//! command, one message in flight at a time. A subscription ends on the
//! page's `stop` command, when the webview starts loading a page, or when its
//! window closes, never on a failed send, since a send to a page that went
//! still reports success.

mod forward;
mod hub;
mod registry;
#[cfg(test)]
mod tests;

use core::str::FromStr;
use std::borrow::Cow;
use std::sync::Arc;

use connetto_client::ClientError;
use connetto_client::LiveSource;
use connetto_client::live_wire::{SubscribeRequest, decode, encode};
use serde::Deserialize;
use tauri::ipc::{Channel, InvokeBody, InvokeResponseBody, JavaScriptChannelId, Request, Response};
use tauri::plugin::{Builder, TauriPlugin};
use tauri::webview::PageLoadEvent;
use tauri::{Manager, RunEvent, Runtime, State, Webview, WindowEvent};

use crate::hub::Hub;
pub use crate::registry::{Registry, SourceSlot};

/// Why the plugin refused a command.
#[derive(Debug, thiserror::Error)]
pub enum Refusal {
    /// The page named a spec the application did not register.
    #[error("no live spec is registered as {0}")]
    Unregistered(String),
    /// The subscribe command carried neither bytes nor an array of them.
    #[error("the subscribe command takes the request's MessagePack bytes")]
    NotBytes,
    /// The request or the spec it names did not decode.
    #[error(transparent)]
    Malformed(#[from] ClientError),
    /// The request named no channel of the webview.
    #[error("{0} names no channel")]
    Channel(String),
}

impl serde::Serialize for Refusal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

/// The plugin following the specs of `registry`, and the slot the application
/// puts its source in.
#[must_use]
pub fn init<R, Source>(registry: Registry<Source>) -> (TauriPlugin<R>, SourceSlot<Source>)
where
    R: Runtime,
    Source: LiveSource + Send + Sync,
{
    let slot = SourceSlot::default();
    let hub = Arc::new(Hub::new(registry.start_from(slot.clone())));
    let loading = Arc::clone(&hub);
    let closing = Arc::clone(&hub);
    let plugin = Builder::new("connetto")
        .invoke_handler(tauri::generate_handler![subscribe, stop, ack])
        .setup(move |app, _api| {
            app.manage(hub);
            Ok(())
        })
        .on_page_load(move |webview, payload| {
            if payload.event() == PageLoadEvent::Started {
                loading.page_started(webview.label());
            }
        })
        .on_event(move |_app, event| {
            if let RunEvent::WindowEvent {
                label,
                event: WindowEvent::Destroyed,
                ..
            } = event
            {
                closing.window_destroyed(label);
            }
        })
        .build();
    (plugin, slot)
}

/// The bytes of a command's body, which Android's webview posts as a JSON
/// array of numbers and every other webview as they are.
fn request_bytes(body: &InvokeBody) -> Result<Cow<'_, [u8]>, Refusal> {
    match body {
        InvokeBody::Raw(bytes) => Ok(Cow::Borrowed(bytes)),
        InvokeBody::Json(numbers) => Vec::<u8>::deserialize(numbers)
            .map(Cow::Owned)
            .map_err(|_| Refusal::NotBytes),
    }
}

/// Follow the spec the request's MessagePack body names.
#[tauri::command]
#[expect(
    clippy::needless_pass_by_value,
    reason = "Tauri hands command arguments over by value"
)]
fn subscribe<R: Runtime>(
    webview: Webview<R>,
    request: Request<'_>,
    hub: State<'_, Arc<Hub>>,
) -> Result<(), Refusal> {
    let request: SubscribeRequest = decode(&request_bytes(request.body())?)?;
    let channel: Channel<Response> = JavaScriptChannelId::from_str(&request.channel)
        .map_err(|_| Refusal::Channel(request.channel.clone()))?
        .channel_on(webview.clone());
    let window = webview.window().label().to_owned();
    hub.subscribe(
        webview.label(),
        &window,
        &request,
        Box::new(move |message| {
            if let Ok(bytes) = encode(&message) {
                let _ = channel.send(Response::new(InvokeResponseBody::Raw(bytes)));
            }
        }),
    )
}

/// End the webview's subscription `id`.
#[tauri::command]
#[expect(
    clippy::needless_pass_by_value,
    reason = "Tauri hands command arguments over by value"
)]
fn stop<R: Runtime>(webview: Webview<R>, id: u64, hub: State<'_, Arc<Hub>>) {
    hub.stop(webview.label(), webview.window().label(), id);
}

/// The webview applied message `seq` of its subscription `id`.
#[tauri::command]
#[expect(
    clippy::needless_pass_by_value,
    reason = "Tauri hands command arguments over by value"
)]
fn ack<R: Runtime>(webview: Webview<R>, id: u64, seq: u64, hub: State<'_, Arc<Hub>>) {
    hub.ack(webview.label(), id, seq);
}
