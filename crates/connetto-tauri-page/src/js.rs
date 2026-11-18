//! The plugin's commands and channels through `window.__TAURI__.core`.

use core::future::Future;
use core::pin::Pin;

use futures_channel::mpsc;
use js_sys::{Object, Reflect, Uint8Array};
use send_wrapper::SendWrapper;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::Core;

#[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"])]
extern "C" {
    /// Tauri's channel, whose messages the core sends in order.
    type Channel;

    #[wasm_bindgen(constructor)]
    fn new(onmessage: &Closure<dyn FnMut(JsValue)>) -> Channel;

    /// The name a command argument gives the channel.
    #[wasm_bindgen(method, js_name = toJSON)]
    fn name(this: &Channel) -> String;

    /// Forget the channel's callback.
    #[wasm_bindgen(method, js_name = cleanupCallback)]
    fn close(this: &Channel);

    #[wasm_bindgen(catch)]
    async fn invoke(command: &str, args: JsValue) -> Result<JsValue, JsValue>;
}

/// A channel the page keeps open, with the callback its messages run.
struct Open {
    channel: Channel,
    _onmessage: Closure<dyn FnMut(JsValue)>,
}

impl Drop for Open {
    fn drop(&mut self) {
        self.channel.close();
    }
}

/// The plugin's commands in the page's `window.__TAURI__`.
pub(crate) struct Js;

/// The arguments `{ id, seq }` of a command, as Tauri reads them from a plain
/// object.
fn arguments(id: u64, seq: Option<u64>) -> JsValue {
    let arguments = Object::new();
    #[expect(
        clippy::cast_precision_loss,
        reason = "ids and sequence numbers stay below 2^53"
    )]
    let number = |value: u64| JsValue::from_f64(value as f64);
    let _ = Reflect::set(&arguments, &"id".into(), &number(id));
    if let Some(seq) = seq {
        let _ = Reflect::set(&arguments, &"seq".into(), &number(seq));
    }
    arguments.into()
}

/// Send `command` without waiting for its answer, which neither stop nor ack
/// has.
fn send(command: &'static str, arguments: JsValue) {
    spawn_local(async move {
        let _ = invoke(command, arguments).await;
    });
}

impl Core for Js {
    fn channel(&self, inbox: mpsc::UnboundedSender<Vec<u8>>) -> (String, Box<dyn Send>) {
        let onmessage = Closure::<dyn FnMut(JsValue)>::new(move |message: JsValue| {
            let _ = inbox.unbounded_send(Uint8Array::new(&message).to_vec());
        });
        let channel = Channel::new(&onmessage);
        let name = channel.name();
        (
            name,
            Box::new(SendWrapper::new(Open {
                channel,
                _onmessage: onmessage,
            })),
        )
    }

    fn subscribe(
        &self,
        request: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        Box::pin(SendWrapper::new(async move {
            invoke(
                "plugin:connetto|subscribe",
                Uint8Array::from(request.as_slice()).into(),
            )
            .await
            .map(drop)
            .map_err(|refusal| {
                refusal
                    .as_string()
                    .unwrap_or_else(|| format!("{refusal:?}"))
            })
        }))
    }

    fn stop(&self, id: u64) {
        send("plugin:connetto|stop", arguments(id, None));
    }

    fn ack(&self, id: u64, seq: u64) {
        send("plugin:connetto|ack", arguments(id, Some(seq)));
    }
}
