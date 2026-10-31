//! The browser pieces every leptos demo suite drives the demo through: the
//! dev identity provider's login, a DB worker booted from the suite's own
//! compiled module, and a tab of that worker.

use connetto_client::{ClientBuilder, ConnettoConnection, FirstThen};
use connetto_leptos_demo::demo_schema;
use connetto_web::{MessageTransport, TabContent, workers};
use js_sys::Array;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{BroadcastChannel, DedicatedWorkerGlobalScope, Request, RequestInit, Worker};

/// Log one step of a suite to the browser console.
pub fn stage(msg: &str) {
    web_sys::console::log_1(&msg.into());
}

/// Fetch `url` from the worker or window the suite runs in.
async fn global_fetch_str(url: &str) -> web_sys::Response {
    let promise = js_sys::global()
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .map(|w| w.fetch_with_str(url))
        .unwrap_or_else(|_| web_sys::window().expect("window").fetch_with_str(url));
    JsFuture::from(promise)
        .await
        .expect("fetch")
        .dyn_into()
        .expect("Response")
}

/// Send `req` from the worker or window the suite runs in.
async fn global_fetch_req(req: &Request) -> web_sys::Response {
    let promise = js_sys::global()
        .dyn_into::<DedicatedWorkerGlobalScope>()
        .map(|w| w.fetch_with_request(req))
        .unwrap_or_else(|_| web_sys::window().expect("window").fetch_with_request(req));
    JsFuture::from(promise)
        .await
        .expect("fetch req")
        .dyn_into()
        .expect("Response")
}

/// Walk the login chain as `username` and read the delivered code and state
/// out of the final URL. The dev identity provider takes any username.
pub async fn walk_login_as(login_url: &str, username: &str) -> (String, String) {
    let resp = global_fetch_str(login_url).await;
    let form_url = resp.url();
    let init = RequestInit::new();
    init.set_method("POST");
    init.set_body(&JsValue::from_str(&format!("username={username}")));
    let req = Request::new_with_str_and_init(&form_url, &init).expect("login request");
    req.headers()
        .set("content-type", "application/x-www-form-urlencoded")
        .expect("header");
    let resp = global_fetch_req(&req).await;
    assert!(
        resp.ok(),
        "login chain ended at {} with status {}",
        resp.url(),
        resp.status()
    );
    let final_url = resp.url();
    let parsed = web_sys::Url::new(&final_url).expect("parse url");
    let params = parsed.search_params();
    (
        params
            .get("code")
            .unwrap_or_else(|| panic!("no code in {final_url}")),
        params
            .get("state")
            .unwrap_or_else(|| panic!("no state in {final_url}")),
    )
}

/// The URL of the suite's wasm-bindgen glue, which a spawned worker imports.
pub fn glue_url() -> String {
    let found = js_sys::eval(
        r#"performance.getEntriesByType("resource").map(e=>e.name).find(n=>n.endsWith("_bg.wasm"))"#,
    )
    .expect("resource entries")
    .as_string()
    .expect("wasm resource entry");
    let base = found.strip_suffix("_bg.wasm").expect("wasm suffix");
    format!("{base}.js")
}

/// Spawn a DB worker running the suite's compiled module under a fresh boot
/// identity, and call its `boot_fn` export.
pub fn spawn_worker(glue_url: &str, boot_fn: &str) -> (Worker, workers::BootIdentity) {
    let identity = workers::BootIdentity::mint();
    let id = identity.to_string();
    let src = format!(
        r#"self.connettoBoot = {id:?};
try {{
    const module = await new Promise((resolve, reject) => {{
        const timer = setTimeout(() => reject(new Error('compiled module handoff timed out')), 15000);
        function receive(event) {{
            const [boot, module] = event.data;
            if (boot !== self.connettoBoot) return;
            clearTimeout(timer);
            removeEventListener('message', receive);
            resolve(module);
        }}
        addEventListener('message', receive);
    }});
    const mod = await import({glue_url:?});
    await mod.default({{module_or_path: module}});
    await mod.{boot_fn}();
}} catch (error) {{
    new BroadcastChannel({hello:?}).postMessage('failed:' + self.connettoBoot + ':' + error);
    throw error;
}}
"#,
        hello = workers::HELLO_CHANNEL,
    );
    let parts = Array::of1(&JsValue::from_str(&src));
    let opts = web_sys::BlobPropertyBag::new();
    opts.set_type("text/javascript");
    let blob = web_sys::Blob::new_with_str_sequence_and_options(&parts, &opts).expect("blob");
    let url = web_sys::Url::create_object_url_with_blob(&blob).expect("object url");
    let worker_opts = web_sys::WorkerOptions::new();
    worker_opts.set_type(web_sys::WorkerType::Module);
    let w = Worker::new_with_options(&url, &worker_opts).expect("spawn worker");
    web_sys::Url::revoke_object_url(&url).ok();
    w.post_message(&Array::of2(
        &JsValue::from_str(&id),
        &wasm_bindgen::module(),
    ))
    .expect("compiled worker module");
    (w, identity)
}

/// A tab of the booted worker. The worker states who it is signed in as, so
/// the tab's mirror answers its policy views as the worker's replica does.
pub async fn connect_tab(
    client_id: &str,
) -> (
    TabContent<BroadcastChannel>,
    ConnettoConnection<MessageTransport<BroadcastChannel>>,
) {
    let wire = format!("connetto-wire-{client_id}");
    workers::announce_tab(&wire).await.expect("announce tab");
    let mut transport = MessageTransport::<BroadcastChannel>::new(&wire).expect("transport");
    let content = TabContent::new(&mut transport);
    let conn = ClientBuilder::new(
        demo_schema().relay_mirror(),
        FirstThen::new(transport, workers::tab_wire_factory(client_id.to_owned())),
    )
    .with_client_id(client_id)
    .connect_driven()
    .await
    .expect("tab connect");
    (content, conn)
}
