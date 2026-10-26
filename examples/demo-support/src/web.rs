//! The browser pieces every browser demo carries identically.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use connetto_client::{Auth, ExportScope, NoGate, SyncTuning, reconnect::ReconnectPolicy};
use connetto_core::Custody;
use connetto_web::auth::{AuthError, LogoutOutcome, PendingWork};
use connetto_web::builder::WebClientBuilder;
use connetto_web::{LOGIN_CHANNEL, workers};
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use web_sys::{BroadcastChannel, MessageEvent};

/// The OPFS file holding the worker's durable synced replica, the base name the worker appends the identity hash to.
pub const DB_NAME: &str = "connetto-relay.sqlite";
/// The OPFS file holding the worker-only refresh token.
pub const AUTH_DB: &str = "connetto-auth.sqlite";
/// The shared leader lock every window of this app races.
pub const LEADER_LOCK: &str = "connetto-demo-leader";
/// The channel the page uses to answer the worker's provider query at each boot.
pub const DEMO_PROVIDER_CHANNEL: &str = "connetto-demo-provider";
/// The OIDC provider the dev stack registers, the fallback a silent page answers with.
pub const AUTH_PROVIDER: &str = "dev-idp";
/// The file name the export download is offered under.
pub const EXPORT_FILE_NAME: &str = "connetto-local-data.zip";
/// The `localStorage` key holding the account the last successful sign-in used.
pub const STORED_ACCOUNT_KEY: &str = "connetto-demo-last-account";

/// The one build the worker boots from and every window attaches through.
pub fn demo_builder() -> WebClientBuilder {
    WebClientBuilder::new(crate::DEMO_WS_URL, crate::demo_schema())
        .with_tuning(SyncTuning::default().with_trim_threshold(5))
        .with_reconnect(
            ReconnectPolicy::new()
                .with_initial_backoff(Duration::from_millis(100))
                .with_max_backoff(Duration::from_secs(2)),
        )
        .with_upstream("db-upstream", crate::DEMO_QUERY)
        .with_upstream("db-photos-upstream", crate::PHOTO_QUERY)
        .with_content_namespace("connetto-photo-content")
        .with_content_heal_lost(
            "SELECT content_id FROM photos WHERE content_state = 'lost'",
            "content_id",
        )
}

/// Ask the page which OIDC provider to use for this boot, falling back to [`AUTH_PROVIDER`] after 200 ms of silence.
pub async fn ask_provider() -> String {
    let Ok(ch) = BroadcastChannel::new(DEMO_PROVIDER_CHANNEL) else {
        return AUTH_PROVIDER.to_owned();
    };
    let received: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let received_clone = Rc::clone(&received);
    let on_msg = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
        if let Some(text) = e.data().as_string()
            && text != "provider?"
        {
            *received_clone.borrow_mut() = Some(text);
        }
    });
    ch.set_onmessage(Some(on_msg.as_ref().unchecked_ref()));
    let _ = ch.post_message(&JsValue::from_str("provider?"));
    for _ in 0..8 {
        workers::sleep(Duration::from_millis(25)).await;
        if received.borrow().is_some() {
            break;
        }
    }
    ch.set_onmessage(None);
    ch.close();
    drop(on_msg);
    received
        .borrow()
        .clone()
        .unwrap_or_else(|| AUTH_PROVIDER.to_owned())
}

/// The page side of the provider handshake, answering the worker's `provider?` on each boot.
pub struct ProviderListener {
    channel: BroadcastChannel,
    on_message: Closure<dyn FnMut(MessageEvent)>,
}

impl ProviderListener {
    /// Listen on the provider channel and answer with the dev provider.
    #[must_use]
    #[expect(
        clippy::new_without_default,
        reason = "constructing it arms the channel listener"
    )]
    pub fn new() -> Self {
        let channel = BroadcastChannel::new(DEMO_PROVIDER_CHANNEL).expect("provider channel");
        let reply = channel.clone();
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
            if e.data().as_string().as_deref() == Some("provider?") {
                let _ = reply.post_message(&JsValue::from_str(AUTH_PROVIDER));
            }
        });
        channel.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        Self {
            channel,
            on_message,
        }
    }

    /// Stop answering and close the channel.
    pub fn close(self) {
        self.channel.set_onmessage(None);
        self.channel.close();
        drop(self.on_message);
    }
}

/// The page side of the login channel, handing the worker's parked login URL to a callback.
pub struct LoginUrlListener {
    channel: BroadcastChannel,
    on_message: Closure<dyn FnMut(MessageEvent)>,
}

impl LoginUrlListener {
    /// Listen on the login channel and call `on_url` with the URL a login request carries.
    #[must_use]
    pub fn new<F>(on_url: F) -> Self
    where
        F: FnMut(String) + 'static,
    {
        let channel = BroadcastChannel::new(LOGIN_CHANNEL).expect("login channel");
        let on_url = RefCell::new(on_url);
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
            let Some(text) = e.data().as_string() else {
                return;
            };
            let Ok(obj) = js_sys::JSON::parse(&text) else {
                return;
            };
            let Some(kind) = js_sys::Reflect::get(&obj, &JsValue::from_str("kind"))
                .ok()
                .and_then(|v| v.as_string())
            else {
                return;
            };
            if kind != "request" {
                return;
            }
            let Some(url) = js_sys::Reflect::get(&obj, &JsValue::from_str("url"))
                .ok()
                .and_then(|v| v.as_string())
            else {
                return;
            };
            on_url.borrow_mut()(url);
        });
        channel.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        Self {
            channel,
            on_message,
        }
    }

    /// Stop listening and close the channel.
    pub fn close(self) {
        self.channel.set_onmessage(None);
        self.channel.close();
        drop(self.on_message);
    }
}

/// The account the last successful sign-in on this origin used, read before the worker re-acquires the session.
#[must_use]
pub fn read_stored_account() -> Option<String> {
    let storage = web_sys::window()?.local_storage().ok().flatten()?;
    storage.get_item(STORED_ACCOUNT_KEY).ok().flatten()
}

/// Remember the account a successful sign-in used, so a reload knows a restore is under way.
pub fn save_stored_account(account: &str) {
    if let Some(window) = web_sys::window()
        && let Some(storage) = window.local_storage().ok().flatten()
    {
        let _ = storage.set_item(STORED_ACCOUNT_KEY, account);
    }
}

/// Drop the remembered account on logout, so the next load shows the sign-in screen.
pub fn clear_stored_account() {
    if let Some(window) = web_sys::window()
        && let Some(storage) = window.local_storage().ok().flatten()
    {
        let _ = storage.remove_item(STORED_ACCOUNT_KEY);
    }
}

/// Reload the page after logout, dropping the remembered account first.
pub fn reload_page() {
    clear_stored_account();
    let _ = js_sys::eval("location.reload()");
}

/// The origin of the worker's own URL, the page origin because the worker script is served from the same host.
#[must_use]
pub fn worker_origin() -> String {
    js_sys::eval("self.location.origin")
        .ok()
        .and_then(|v| v.as_string())
        .unwrap_or_default()
}

/// Offer `blob` to the user as a file download named `name`, through a synthetic anchor at an object URL.
///
/// # Errors
///
/// A `JsValue` when the object URL, the document, or the anchor cannot be made.
pub fn download_blob(name: &str, blob: &web_sys::Blob) -> Result<(), JsValue> {
    let url = web_sys::Url::create_object_url_with_blob(blob)?;
    let document = web_sys::window()
        .and_then(|window| window.document())
        .ok_or_else(|| JsValue::from_str("no document"))?;
    let anchor: web_sys::HtmlAnchorElement = document.create_element("a")?.dyn_into()?;
    anchor.set_href(&url);
    anchor.set_download(name);
    anchor.click();
    web_sys::Url::revoke_object_url(&url)
}

/// Export this device's local data through the worker and offer the archive as a download, answering with the line the pane shows.
pub async fn export_data() -> String {
    match workers::request_export(ExportScope::Everything).await {
        Ok(blob) => {
            let size = blob.size();
            match download_blob(EXPORT_FILE_NAME, &blob) {
                Ok(()) => format!("{size} bytes offered as {EXPORT_FILE_NAME}"),
                Err(err) => format!("download refused: {err:?}"),
            }
        }
        Err(err) => format!("export failed: {err}"),
    }
}

/// Restore one archive through the worker, keeping the file's version of every clash, and answer with the line the pane shows.
pub async fn import_data(blob: &web_sys::Blob) -> String {
    match workers::request_import(blob.clone()).await {
        Ok((outcome, clashes)) => {
            let mut msg = format!(
                "{} row(s) restored, {} kept, {} write(s) restored",
                outcome.rows_restored, outcome.rows_kept, outcome.writes_restored
            );
            if clashes > 0 {
                msg.push_str(&format!(" ({clashes} clash(es) resolved to file)"));
            }
            msg
        }
        Err(err) => format!("refused: {err}"),
    }
}

/// The header's gate line for the custody standing and the worker's reported unlock state.
#[must_use]
pub fn gate_text(custody: Custody, state: &'static str) -> &'static str {
    match custody {
        Custody::Verified => state,
        Custody::Unverified(_) | Custody::Ephemeral => {
            "none, local data opens without user verification"
        }
    }
}

/// The header's custody line for the standing, naming the at-rest policy and the reason it stands as it does in this browser.
#[must_use]
pub fn custody_text(custody: Custody) -> &'static str {
    match custody {
        Custody::Verified => "released only after user verification",
        Custody::Unverified(NoGate::Offerable) => {
            "stored without user verification, a passkey can be set up"
        }
        Custody::Unverified(NoGate::Declined) => {
            "stored without user verification, the passkey was declined"
        }
        Custody::Unverified(NoGate::Unsupported) => {
            "stored without user verification, the browser refused a passkey for this address"
        }
        Custody::Ephemeral => "no durable key, nothing is kept",
    }
}

/// The outcome line when the gate enrolment cannot run in this window.
pub const ENROL_WRONG_WINDOW: &str =
    "the passkey gate can only be set up in the window that owns the database";

/// The outcome line when the gate enrolment ceremony fails.
#[must_use]
pub fn enrolment_failed(err: impl core::fmt::Display) -> String {
    format!("enrolment failed: {err}")
}

/// What a delete-and-log-out request ended in before any reload.
pub enum DeleteOutcome {
    /// Local work is still unsent, so the account bar asks for the go ahead.
    Confirm(PendingWork),
    /// The worker refused the delete while work was still pending.
    Refused(PendingWork),
    /// The worker deleted and the page reloaded.
    Reloaded,
}

/// Log out keeping the local data, reloading the page when the worker agrees.
///
/// # Errors
///
/// The worker's [`AuthError`] when the request fails.
pub async fn logout_keep() -> Result<(), AuthError> {
    match connetto_web::auth::request_logout(false, false).await {
        Ok(_) => {
            reload_page();
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Delete the local data and log out.
///
/// # Errors
///
/// The worker's [`AuthError`] when the request fails.
pub async fn logout_delete() -> Result<DeleteOutcome, AuthError> {
    match connetto_web::auth::request_unsynced().await {
        Ok(pending) if pending.is_empty() => {
            match connetto_web::auth::request_logout(true, false).await {
                Ok(LogoutOutcome::Refused { pending }) => Ok(DeleteOutcome::Refused(pending)),
                Ok(_) => {
                    reload_page();
                    Ok(DeleteOutcome::Reloaded)
                }
                Err(err) => Err(err),
            }
        }
        Ok(pending) => Ok(DeleteOutcome::Confirm(pending)),
        Err(err) => Err(err),
    }
}

/// The confirmed or forced delete, discarding the unsent work and reloading.
///
/// # Errors
///
/// The worker's [`AuthError`] when the request fails.
pub async fn logout_delete_confirmed() -> Result<(), AuthError> {
    match connetto_web::auth::request_logout(true, true).await {
        Ok(_) => {
            reload_page();
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Boot the demo's DB tier in a worker context, the test workers' entry, each test naming its own OPFS files.
///
/// # Errors
///
/// A `JsValue` naming the VFS, upstream connect, or subscribe failure.
pub async fn boot_with(db_prefix: &'static str, auth_db: &'static str) -> Result<(), JsValue> {
    connetto_web::logging::init_console();
    WebClientBuilder::new(crate::DEMO_WS_URL, crate::demo_schema())
        .with_upstream("db-upstream", crate::DEMO_QUERY)
        .with_upstream("db-photos-upstream", crate::PHOTO_QUERY)
        .with_content_namespace("connetto-photo-content")
        .with_content_heal_lost(
            "SELECT content_id FROM photos WHERE content_state = 'lost'",
            "content_id",
        )
        .signed_in(Auth::new(crate::AUTH_BASE, AUTH_PROVIDER))
        .with_redirect_uri(crate::auth_landing())
        .with_auth_db_name(auth_db)
        .durable(db_prefix)
        .with_gate(connetto_client::Gate::off())
        .boot::<String>()
        .await
        .map(drop)
        .map_err(JsValue::from)
}
