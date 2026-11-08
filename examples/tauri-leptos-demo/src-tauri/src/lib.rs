//! The connetto demo as a Tauri 2 app.
//!
//! The client lives here, in the native core, which keeps running while the
//! platform pauses the WebView. The core turns the client's state into one
//! `View` from `connetto-demo-support` and emits it to the Leptos UI on every
//! change, and the UI calls back through the commands below. The platform
//! lifecycle drives the client's away input directly, with no UI engine in
//! between.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use connetto_client::teardown::{ForgetError, ForgetRefusal};
use connetto_client::{
    AccountChoice, AccountChooser, Auth, ClientError, ClientEvent, ConnettoClient, ExportScope,
    KeyringAuth, LiveHandle as _, LiveQuery, Moment, NativeAuthenticator, NativeClient,
    NativeTransport, SystemClock, dsl::Watchable as _,
};
use connetto_demo_support as support;
use connetto_file_client::FileId;
use connetto_file_client::{
    ContentClient, ContentEvent, ContentHandle, FsStore, ReqwestHttp, Resolved,
};
use diesel::prelude::*;
use parking_lot::Mutex;
use rosetta_uuid::Uuid;
use serde::Serialize;
use support::view::{AccountEntry, Action, LogoutState, Session, View};
use tauri::{AppHandle, Emitter as _, Manager as _, RunEvent, WindowEvent};
use tauri_plugin_dialog::DialogExt as _;
use tauri_plugin_fs::{FilePath, FsExt as _, OpenOptions};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

/// The deployment root this build ships, which every device certificate and
/// revocation list must chain to.
#[cfg(feature = "device-identity")]
const DEVICE_ROOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/device-root.der"));

const DEFAULT_WS: &str = "ws://127.0.0.1:7777/sync";
const DEFAULT_AUTH_ORIGIN: &str = "http://127.0.0.1:7777";
const DEFAULT_PG: &str = "postgres://postgres:postgres@127.0.0.1:55456/postgres";
const AUTH_PROVIDER: &str = "dev-idp";
/// The keyring service the demo stores its secrets under, a separate name in
/// an unattended build so a replica key or refresh token a gated earlier run
/// left in the keychain, which survives a reinstall, cannot meet the
/// unattended build and prompt.
const KEYRING_SERVICE: &str = if cfg!(feature = "unattended") {
    "connetto-tauri-demo-unattended"
} else {
    "connetto-tauri-demo"
};
/// The login redirect, whose scheme is the Android applicationId and the
/// iOS bundle identifier.
#[cfg(any(target_os = "android", target_os = "ios"))]
const APP_REDIRECT: &str = "dev.connetto.tauridemo:/oauth2redirect";
const LOG_FILTER: &str = "info,connetto_client::away=debug,connetto_tauri_demo::away=debug";

type Ws = NativeTransport;
type Content = Arc<ContentClient<Ws, FsStore, ReqwestHttp>>;

/// One signed-in session. Dropping it aborts the tasks feeding the view and
/// closes the client.
struct Parts {
    native: NativeClient<Ws, ContentHandle<Ws>>,
    content: Content,
    authenticator: Option<NativeAuthenticator>,
    backend: mpsc::UnboundedSender<BackendCmd>,
    holds: Arc<Holds>,
    _tasks: JoinSet<()>,
}

impl Drop for Parts {
    fn drop(&mut self) {
        let client = self.native.client().clone();
        // The wipe path already closed the client, so this is a second close,
        // which the client answers as a no-op.
        tauri::async_runtime::spawn(async move {
            client.close().await;
        });
    }
}

/// Where the core's session stands, as `plans/tauri-leptos-demos.md` tables it.
/// Starting and failed setups both hold no session and serve the same inputs.
#[derive(Default)]
enum Slot {
    #[default]
    Empty,
    Ready(Arc<Parts>),
    /// `forget_device` runs on the session, which only the lifecycle reaches.
    SigningOut(Arc<Parts>),
}

/// The per-session values the feeds share, which the view does not hold.
struct Holds {
    newest_order: Mutex<Option<Uuid>>,
    covered: Mutex<Option<usize>>,
    covered_orders: AtomicUsize,
    covered_notes: AtomicUsize,
    covered_photos: AtomicUsize,
    covered_products: AtomicUsize,
    thumb_refresh: AtomicU64,
    /// The hosts whose beacons show, as the link's events report them, the
    /// nearby panel's join picks the strongest of.
    #[cfg(feature = "peer")]
    nearby_hosts: Mutex<Vec<connetto_client::NearbyHost>>,
}

impl Default for Holds {
    fn default() -> Self {
        Self {
            newest_order: Mutex::new(None),
            covered: Mutex::new(None),
            covered_orders: AtomicUsize::new(0),
            covered_notes: AtomicUsize::new(0),
            covered_photos: AtomicUsize::new(0),
            covered_products: AtomicUsize::new(0),
            thumb_refresh: AtomicU64::new(0),
            #[cfg(feature = "peer")]
            nearby_hosts: Mutex::new(Vec::new()),
        }
    }
}

enum BackendCmd {
    Insert,
}

/// The core's state, managed by Tauri.
struct Core {
    view: Mutex<View>,
    slot: Mutex<Slot>,
    /// The parked login URL, `None` while no login stands parked.
    parked: watch::Sender<Option<String>>,
    /// The phone's tab login waits on the parked URL clearing.
    #[cfg_attr(
        not(any(target_os = "android", target_os = "ios")),
        expect(dead_code, reason = "only the phone's tab login waits on it")
    )]
    parked_rx: watch::Receiver<Option<String>>,
    /// The parked account picker's answer channel, `None` while none stands.
    picker: Mutex<Option<mpsc::UnboundedSender<Option<String>>>>,
    /// Whether a setup task is running, so a second request starts nothing.
    starting: AtomicBool,
}

impl Core {
    /// Change the view and emit it when it changed. The emission happens under
    /// the view lock, so two tasks' views reach the UI in the order they changed.
    fn update(&self, app: &AppHandle, change: impl FnOnce(&mut View)) {
        let mut view = self.view.lock();
        let before = view.clone();
        change(&mut view);
        if *view != before {
            emit_view(app, &view);
        }
    }

    /// The session, while it is ready to serve commands.
    fn session(&self) -> Option<Arc<Parts>> {
        match &*self.slot.lock() {
            Slot::Ready(session) => Some(Arc::clone(session)),
            Slot::Empty | Slot::SigningOut(_) => None,
        }
    }

    fn client(&self) -> Option<ConnettoClient<Ws>> {
        self.session()
            .map(|session| session.native.client().clone())
    }

    /// The client the lifecycle reaches, which a sign-out still in progress keeps.
    fn lifecycle_client(&self) -> Option<ConnettoClient<Ws>> {
        match &*self.slot.lock() {
            Slot::Ready(session) | Slot::SigningOut(session) => {
                Some(session.native.client().clone())
            }
            Slot::Empty => None,
        }
    }

    /// Move a ready session to signing out and hand it over, or `None` when
    /// no session is ready, a sign-out already in progress included.
    fn begin_sign_out(&self) -> Option<Arc<Parts>> {
        let mut slot = self.slot.lock();
        let Slot::Ready(session) = &*slot else {
            return None;
        };
        let session = Arc::clone(session);
        *slot = Slot::SigningOut(Arc::clone(&session));
        Some(session)
    }
}

fn emit_view(app: &AppHandle, view: &View) {
    if let Err(err) = app.emit("view", view) {
        tracing::warn!(error = %err, "emitting the view");
    }
}

/// Why a command failed, as the UI is told.
#[derive(Debug, thiserror::Error)]
enum CommandError {
    #[error("the session is not ready yet")]
    NotReady,
    #[error("the backend writer stopped")]
    BackendGone,
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Content(#[from] connetto_file_client::ContentError),
}

impl Serialize for CommandError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

/// Emit the current view through the event every change takes, since a
/// returned value travels apart from events and could land after a newer one.
#[tauri::command]
fn view(app: AppHandle, core: tauri::State<'_, Arc<Core>>) {
    emit_view(&app, &core.view.lock());
}

/// Every action the UI sends, served under the view and session the core holds.
#[tauri::command]
async fn act(
    app: AppHandle,
    core: tauri::State<'_, Arc<Core>>,
    action: Action,
) -> Result<(), CommandError> {
    serve(&app, &core, action).await
}

async fn serve(app: &AppHandle, core: &Arc<Core>, action: Action) -> Result<(), CommandError> {
    match action {
        Action::SignIn => sign_in(app, core).await,
        Action::Unlock => unlock(app, core).await,
        Action::ChooseAccount { account } => {
            choose_account(app, core, account);
            Ok(())
        }
        Action::AddAccount => add_account(app, core).await,
        Action::InsertBackend => insert_backend(core),
        Action::SwitchAccount { account } => switch_account(app, core, account).await,
        Action::EnrolGate => Ok(()),
        Action::LogOutKeep => log_out_keep(app, core).await,
        Action::LogOutDelete => log_out_delete(app, core).await,
        Action::ConfirmDelete => confirm_delete(app, core).await,
        Action::ForceDelete => Ok(()),
        Action::AddOrder => add_order(core).await,
        Action::RemoveNewest => remove_newest(core).await,
        Action::CancelLogout => {
            cancel_logout(app, core);
            Ok(())
        }
        Action::StagePhoto { name, bytes } => stage_photo(app, core, name, bytes).await,
        Action::Pin => pin_content(app, core).await,
        Action::Unpin => unpin_content(app, core).await,
        Action::FetchPinned => fetch_pinned(app, core).await,
        Action::TidyContent => tidy_content(app, core).await,
        Action::RetryUpload { file_id } => retry_upload(app, core, file_id).await,
        Action::AcknowledgeLost => acknowledge_lost(app, core).await,
        Action::SaveNote { body } => save_note(app, core, body).await,
        Action::FreeUpSpace => free_up_space(app, core).await,
        Action::Export => export(app, core).await,
        Action::Import { bytes, .. } => import(app, core, bytes).await,
        Action::EnableBluetooth
        | Action::HostHotspot
        | Action::StopHotspot
        | Action::JoinHotspot { .. }
        | Action::JoinPayload { .. }
        | Action::JoinNearby
        | Action::LeaveHotspot
        | Action::SetPeerLinks { .. } => {
            serve_nearby(app, core, action).await;
            Ok(())
        }
        Action::PickTeam { team } => {
            core.update(app, |v| {
                if let Some(view) = v.team.as_mut() {
                    view.picked = team;
                }
            });
            Ok(())
        }
        Action::AddTeamNote { body } => add_team_note(app, core, body).await,
        Action::TidyTeam => {
            #[cfg(feature = "peer")]
            tidy_team(app, core).await;
            Ok(())
        }
    }
}

/// The nearby panel's actions, which stand behind the peer feature and answer
/// nothing in a build that carries no peer link, since the panel never stands
/// there.
#[cfg(feature = "peer")]
async fn serve_nearby(app: &AppHandle, core: &Arc<Core>, action: Action) {
    match action {
        Action::EnableBluetooth => enable_bluetooth(app, core).await,
        Action::HostHotspot => host_hotspot(app, core).await,
        Action::StopHotspot => stop_hotspot(app, core).await,
        Action::JoinHotspot {
            ssid,
            passphrase,
            port,
        } => join_hotspot(app, core, ssid, passphrase, port).await,
        Action::JoinPayload { payload } => join_payload(app, core, payload).await,
        Action::JoinNearby => join_nearby(app, core).await,
        Action::LeaveHotspot => leave_hotspot(app, core).await,
        Action::SetPeerLinks { on } => set_peer_links(app, core, on),
        _ => {}
    }
}

/// Switch every nearby link on or off for the running app, the panel's line
/// following the client's standing.
#[cfg(feature = "peer")]
fn set_peer_links(app: &AppHandle, core: &Arc<Core>, on: bool) {
    let Some(session) = core.session() else {
        return;
    };
    session.native.set_peer_links(on);
    let off = !session.native.peer_links_on();
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.links_off = off;
        }
    });
}

/// The nearby panel's actions in a build without the peer link, which the
/// panel never sends, since it never stands there.
#[cfg(not(feature = "peer"))]
async fn serve_nearby(_app: &AppHandle, _core: &Arc<Core>, _action: Action) {}

/// Start the parked login the moment the user asks. The system browser on a
/// desktop, the platform's auth session on a phone, or a re-run of the setup
/// when no login is parked yet.
async fn sign_in(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    // The borrow ends here, since clearing the parked URL below takes the
    // write side of the same lock.
    let parked = core.parked.borrow().clone();
    match parked {
        #[cfg(any(target_os = "android", target_os = "ios"))]
        Some(_) => {
            core.update(app, |v| v.status = support::native::SIGN_IN_OPEN.to_owned());
            // Clearing the parked URL is what the parked login waits for, so
            // the platform's auth session opens on this click.
            let _ = core.parked.send(None);
        }
        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        Some(url) => {
            core.update(app, |v| v.status = support::native::SIGN_IN_OPEN.to_owned());
            let opener = connetto_client::system_browser_opener();
            if let Err(err) = opener(&url) {
                core.update(app, |v| {
                    v.logout = LogoutState::Error(support::native::sign_in_open_error(err));
                });
            }
        }
        None => {
            let slot_empty = matches!(*core.slot.lock(), Slot::Empty);
            if slot_empty && !core.starting.load(Ordering::Acquire) {
                start(app.clone(), core, AccountChoice::LastUsed);
            }
        }
    }
    Ok(())
}

async fn unlock(_app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let client = core.client().ok_or(CommandError::NotReady)?;
    client.unlock().await;
    Ok(())
}

/// Answer the parked account picker. The view returns to connecting until the
/// sign-in it triggered lands or fails.
fn choose_account(app: &AppHandle, core: &Arc<Core>, account: Option<String>) {
    if let Some(answer) = core.picker.lock().take() {
        let _ = answer.send(account);
        core.update(app, |v| {
            v.picking = None;
            v.session = Session::Connecting;
        });
    }
}

/// End the session and sign in as a new account, through a fresh login.
async fn add_account(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.begin_sign_out().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    client.close().await;
    *core.slot.lock() = Slot::Empty;
    drop(session);
    start(app.clone(), core, AccountChoice::New);
    Ok(())
}

fn insert_backend(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    session
        .backend
        .send(BackendCmd::Insert)
        .map_err(|_| CommandError::BackendGone)
}

/// Start over as another stored account, refused while local writes are unsent.
async fn switch_account(
    app: &AppHandle,
    core: &Arc<Core>,
    key: String,
) -> Result<(), CommandError> {
    let session = core.begin_sign_out().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    if let Some(line) = support::native::switch_account_guard(&client).await {
        *core.slot.lock() = Slot::Ready(session);
        core.update(app, |v| v.logout = LogoutState::Error(line));
        return Ok(());
    }
    client.close().await;
    *core.slot.lock() = Slot::Empty;
    drop(session);
    start(app.clone(), core, AccountChoice::Account(key));
    Ok(())
}

/// Log out, keeping the local data. The session is revoked and the stored
/// credential cleared, and the app starts over from the persisted cursor.
async fn log_out_keep(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.begin_sign_out().ok_or(CommandError::NotReady)?;
    core.update(app, |v| v.logout = LogoutState::Working);
    if let Some(authenticator) = &session.authenticator
        && let Err(err) = authenticator.logout().await
    {
        // The local clear stands either way, so the session drops and the
        // app starts over. The server keeps the session until it lapses.
        tracing::warn!(error = %err, "revoking the session");
    }
    let client = session.native.client().clone();
    client.close().await;
    *core.slot.lock() = Slot::Empty;
    drop(session);
    start(app.clone(), core, AccountChoice::LastUsed);
    Ok(())
}

/// Delete the local data and log out. `forget_device(false)` refuses while
/// writes are still unsent, which asks the user to confirm losing them.
async fn log_out_delete(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.begin_sign_out().ok_or(CommandError::NotReady)?;
    core.update(app, |v| v.logout = LogoutState::Working);
    wipe(app, core, session, false).await;
    Ok(())
}

/// The confirmed delete, discarding the unsent work.
async fn confirm_delete(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.begin_sign_out().ok_or(CommandError::NotReady)?;
    core.update(app, |v| v.logout = LogoutState::Working);
    wipe(app, core, session, true).await;
    Ok(())
}

/// Wipe the replica and start over, discarding the unsent work. A refused
/// wipe keeps the session, and one that closed the client starts over.
async fn wipe(app: &AppHandle, core: &Arc<Core>, session: Arc<Parts>, force: bool) {
    match session.native.forget_device(force).await {
        Ok(()) => {
            *core.slot.lock() = Slot::Empty;
            drop(session);
            start(app.clone(), core, AccountChoice::LastUsed);
        }
        Err(ForgetError::Refused(ForgetRefusal::Unsynced(unsynced))) => {
            *core.slot.lock() = Slot::Ready(session);
            core.update(app, |v| {
                v.logout = LogoutState::ConfirmDelete {
                    unsynced_count: u64::try_from(unsynced.len()).unwrap_or(u64::MAX),
                };
            });
        }
        Err(ForgetError::Refused(refusal)) => {
            *core.slot.lock() = Slot::Ready(session);
            core.update(app, |v| {
                v.logout = LogoutState::Error(format!("logout error: {refusal}"));
            });
        }
        Err(ForgetError::Incomplete(report)) => {
            *core.slot.lock() = Slot::Empty;
            drop(session);
            tracing::error!(%report, "the wipe left a report");
            start(app.clone(), core, AccountChoice::LastUsed);
        }
    }
}

fn cancel_logout(app: &AppHandle, core: &Arc<Core>) {
    core.update(app, |v| v.logout = LogoutState::Idle);
}

async fn add_order(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    let identity = user_id(&session);
    support::add_order(&client, &identity, support::fresh_quantity()).await?;
    Ok(())
}

async fn remove_newest(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    let Some(id) = *session.holds.newest_order.lock() else {
        return Ok(());
    };
    support::remove_newest_order(&client, id).await?;
    Ok(())
}

async fn stage_photo(
    app: &AppHandle,
    core: &Arc<Core>,
    name: String,
    bytes: Vec<u8>,
) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let identity = user_id(&session);
    let line = match support::mime_from_extension(std::path::Path::new(&name)) {
        None => "not an image: only .jpg .jpeg .png are accepted".to_owned(),
        Some(mime) => {
            let outcome = session
                .content
                .stage(bytes.as_slice(), mime, |conn, file_id| {
                    support::stage_photo_row(conn, file_id, &identity)
                })
                .await;
            match outcome {
                Ok(_) => format!("staged: {name}"),
                Err(err) => format!("stage failed: {err}"),
            }
        }
    };
    core.update(app, |v| v.stage_line = Some(line));
    Ok(())
}

async fn pin_content(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .pin_content("photos", support::PIN_PHOTOS_QUERY, "content_id")
        .await
        .map_or_else(support::pin_failed, |_| support::PIN_OK.to_owned());
    core.update(app, |v| v.content_line = Some(line));
    Ok(())
}

async fn unpin_content(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .unpin_content("photos")
        .await
        .map_or_else(support::unpin_failed, |_| support::UNPIN_OK.to_owned());
    core.update(app, |v| v.content_line = Some(line));
    Ok(())
}

async fn fetch_pinned(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .fetch_pinned()
        .await
        .map_or_else(support::fetch_failed, |ids| support::fetch_ok(ids.len()));
    session.holds.thumb_refresh.fetch_add(1, Ordering::Release);
    core.update(app, |v| v.content_line = Some(line));
    Ok(())
}

async fn tidy_content(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .tidy_content()
        .await
        .map_or_else(support::tidy_failed, support::tidy_ok);
    core.update(app, |v| v.content_line = Some(line));
    Ok(())
}

async fn retry_upload(app: &AppHandle, core: &Arc<Core>, hex: String) -> Result<(), CommandError> {
    let Some(file_id) = file_id_from_hex(&hex) else {
        return Ok(());
    };
    let session = core.session().ok_or(CommandError::NotReady)?;
    let short = support::short_hex(file_id.as_bytes());
    match session.content.retry_refused(file_id).await {
        Ok(()) => core.update(app, |v| {
            v.refused_uploads.retain(|(id, _)| *id != short);
        }),
        Err(err) => core.update(app, |v| {
            v.content_line = Some(format!("retry failed: {err}"))
        }),
    }
    Ok(())
}

async fn acknowledge_lost(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let ids: Vec<FileId> = {
        let view = core.view.lock();
        view.lost_bytes
            .iter()
            .filter_map(|hex| file_id_from_hex(hex))
            .collect()
    };
    if ids.is_empty() {
        return Ok(());
    }
    match session.content.forget_retired_content(&ids).await {
        Ok(()) => core.update(app, |v| v.lost_bytes.clear()),
        Err(err) => core.update(app, |v| {
            v.content_line = Some(format!("acknowledge failed: {err}"))
        }),
    }
    Ok(())
}

async fn save_note(app: &AppHandle, core: &Arc<Core>, body: String) -> Result<(), CommandError> {
    if body.is_empty() {
        return Ok(());
    }
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    match support::save_note(&client, &body).await {
        Ok(_) => core.update(app, |v| v.notes_saved += 1),
        Err(err) => tracing::warn!(error = %err, "the note save failed"),
    }
    Ok(())
}

async fn free_up_space(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    if let Err(err) = client.tidy().await {
        tracing::warn!(error = %err, "the trim pass failed");
    }
    reread_storage(core, app, &client).await;
    Ok(())
}

/// Write one note into the picked team, answering with the line the panel's
/// outcome shows.
async fn add_team_note(
    app: &AppHandle,
    core: &Arc<Core>,
    body: String,
) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    let team = core
        .view
        .lock()
        .team
        .as_ref()
        .map(|team| team.picked.clone())
        .unwrap_or_default();
    let author = user_id(&session);
    let body = body.trim().to_owned();
    if body.is_empty() {
        core.update(app, |v| {
            if let Some(team) = v.team.as_mut() {
                team.outcome = "the note needs some text".to_owned();
            }
        });
        return Ok(());
    }
    match support::add_team_note(&client, &team, &author, &body).await {
        Ok(_) => core.update(app, |v| {
            if let Some(team) = v.team.as_mut() {
                team.outcome = "note added".to_owned();
            }
        }),
        Err(err) => core.update(app, |v| {
            if let Some(team) = v.team.as_mut() {
                team.outcome = format!("note add failed: {err}");
            }
        }),
    }
    Ok(())
}

/// Run the eviction pass the tidy control offers, which spares provisional
/// rows, and tell the panel how many remain.
#[cfg(feature = "peer")]
async fn tidy_team(app: &AppHandle, core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    let client = session.native.client().clone();
    match client.tidy().await {
        Ok(()) => {
            let remaining = client
                .provisional_rows()
                .await
                .map(|rows| {
                    rows.iter()
                        .filter(|row| {
                            row.state == connetto_client::provisional::ProvisionalState::Provisional
                        })
                        .count()
                })
                .unwrap_or_default();
            core.update(app, |v| {
                if let Some(team) = v.team.as_mut() {
                    append_team_events(&mut team.events, vec![support::tidy_line(remaining)]);
                }
            });
        }
        Err(err) => {
            tracing::error!(error = %err, "the tidy pass failed");
            core.update(app, |v| {
                if let Some(team) = v.team.as_mut() {
                    append_team_events(
                        &mut team.events,
                        vec![format!("tidy: the pass failed: {err}")],
                    );
                }
            });
        }
    }
}
/// Ask the platform for the permission a host or join named as missing, on
/// the platform's main thread, where the prompt wants to be asked from.
#[cfg(feature = "peer")]
fn ask_peer_permission(app: &AppHandle, core: &Arc<Core>) {
    #[cfg(target_os = "android")]
    let session = core.session();
    #[cfg(not(target_os = "android"))]
    let _ = core;
    let sent = app.run_on_main_thread(move || {
        #[cfg(target_os = "android")]
        if let Some(session) = session
            && let Err(err) = session.native.request_peer_permissions()
        {
            tracing::warn!(error = %err, "the peer permissions could not be asked");
        }
        #[cfg(target_os = "macos")]
        connetto_peer_apple::hotspot::request_location_access();
    });
    if let Err(err) = sent {
        tracing::warn!(error = %err, "asking the platform permission");
    }
}

/// Turn Bluetooth on, through the system's prompt where it asks.
#[cfg(feature = "peer")]
async fn enable_bluetooth(app: &AppHandle, core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = "turning on bluetooth".to_owned();
        }
    });
    match session.native.enable_bluetooth().await {
        Ok(()) => core.update(app, |v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.outcome = "bluetooth is on".to_owned();
            }
        }),
        Err(err) => core.update(app, |v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.outcome = err.to_string();
            }
        }),
    }
}

/// Host a hotspot from the app, and show its offer as the panel's lines.
#[cfg(feature = "peer")]
async fn host_hotspot(app: &AppHandle, core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = "asking for the hotspot".to_owned();
        }
    });
    match session.native.host_hotspot().await {
        Ok(hosted) => core.update(app, |v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.offer = Some(support::nearby::offer_view(&hosted.offer));
                panel.outcome = support::nearby::hosted_outcome(&hosted.beacon);
            }
        }),
        Err(err) => {
            if matches!(err, connetto_client::HotspotError::MissingPermission(_)) {
                ask_peer_permission(app, core);
            }
            core.update(app, |v| {
                if let Some(panel) = v.nearby.as_mut() {
                    panel.outcome = err.to_string();
                }
            });
        }
    }
}

/// Stop the hotspot this device hosts.
#[cfg(feature = "peer")]
async fn stop_hotspot(app: &AppHandle, core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    session.native.stop_hotspot().await;
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.offer = None;
            panel.outcome = "stopped".to_owned();
        }
    });
}

/// The join's outcome, asking the platform for a missing permission where
/// the error names one.
#[cfg(feature = "peer")]
fn report_join_error(app: &AppHandle, core: &Arc<Core>, err: &connetto_client::JoinError) {
    if matches!(err, connetto_client::JoinError::MissingPermission(_)) {
        ask_peer_permission(app, core);
    }
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = err.to_string();
        }
    });
}

/// Join a hotspot by its offer's typed lines.
#[cfg(feature = "peer")]
async fn join_hotspot(
    app: &AppHandle,
    core: &Arc<Core>,
    ssid: String,
    passphrase: String,
    port: String,
) {
    join_offer(
        app,
        core,
        support::nearby::typed_offer(&ssid, &passphrase, &port).map_err(str::to_owned),
    )
    .await;
}

/// Join a hotspot by its pasted or scanned `WIFI:` payload.
#[cfg(feature = "peer")]
async fn join_payload(app: &AppHandle, core: &Arc<Core>, payload: String) {
    let offer = connetto_client::HotspotOffer::from_wifi_payload(payload.trim())
        .map_err(|err| err.to_string());
    join_offer(app, core, offer).await;
}

/// Join the hotspot `offer` names, or show why the form names none.
#[cfg(feature = "peer")]
async fn join_offer(
    app: &AppHandle,
    core: &Arc<Core>,
    offer: Result<connetto_client::HotspotOffer, String>,
) {
    let Some(session) = core.session() else {
        return;
    };
    let offer = match offer {
        Ok(offer) => offer,
        Err(outcome) => {
            core.update(app, |v| {
                if let Some(panel) = v.nearby.as_mut() {
                    panel.outcome = outcome;
                }
            });
            return;
        }
    };
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = support::nearby::JOINING_HOTSPOT.to_owned();
        }
    });
    match session.native.join_hotspot(&offer).await {
        Ok(gateway) => core.update(app, |v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.joined = true;
                panel.outcome = support::nearby::joined_outcome(gateway);
            }
        }),
        Err(err) => report_join_error(app, core, &err),
    }
}

/// Leave the joined hotspot.
#[cfg(feature = "peer")]
async fn leave_hotspot(app: &AppHandle, core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    session.native.leave_hotspot().await;
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.joined = false;
            panel.vpn_line = None;
            panel.outcome = "left".to_owned();
        }
    });
}

/// Join the strongest host whose beacon shows, one at a time.
#[cfg(feature = "peer")]
async fn join_nearby(app: &AppHandle, core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    // One nearby join at a time, so its outcome is the one shown.
    let outcome = core
        .view
        .lock()
        .nearby
        .as_ref()
        .map(|panel| panel.outcome.clone())
        .unwrap_or_default();
    if outcome == support::nearby::JOINING_NEARBY {
        return;
    }
    let host = {
        let hosts = session.holds.nearby_hosts.lock();
        support::nearby::strongest(&hosts)
    };
    let Some(host) = host else {
        core.update(app, |v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.outcome = "no host is nearby".to_owned();
            }
        });
        return;
    };
    core.update(app, |v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = support::nearby::JOINING_NEARBY.to_owned();
        }
    });
    match session.native.join_nearby(&host).await {
        Ok((identity, gateway)) => core.update(app, |v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.joined = true;
                panel.outcome = support::nearby::joined_nearby_outcome(gateway, &identity);
            }
        }),
        Err(err) => {
            if support::nearby::nearby_permission(&err).is_some() {
                ask_peer_permission(app, core);
            }
            core.update(app, |v| {
                if let Some(panel) = v.nearby.as_mut() {
                    panel.outcome = err.to_string();
                }
            });
        }
    }
}

/// Offer the Save As window, then write the archive where it points. A
/// dismissed window leaves the view unchanged.
async fn export(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let content = Arc::clone(&session.content);
    let (offer, pick) = oneshot::channel::<Option<FilePath>>();
    app.dialog()
        .file()
        .set_file_name("connetto-local-data.zip")
        .save_file(move |path| {
            let _ = offer.send(path);
        });
    let path = match pick.await {
        Ok(Some(path)) => path,
        Ok(None) | Err(_) => return Ok(()),
    };
    let line = export_to(app, content, path).await;
    core.update(app, |v| v.export_line = Some(line));
    Ok(())
}

/// Write the archive to the path the Save As window chose, answering with the
/// line the pane shows. The fs plugin opens Android content URIs.
async fn export_to(app: &AppHandle, content: Content, path: FilePath) -> String {
    let display = match &path {
        FilePath::Path(p) => p.display().to_string(),
        FilePath::Url(u) => u.to_string(),
    };
    let mut options = OpenOptions::new();
    options.read(false).write(true).create(true).truncate(true);
    let file = match &path {
        FilePath::Path(p) => app.fs().open(p.as_path(), options),
        FilePath::Url(u) => app.fs().open(u.clone(), options),
    };
    let file = match file {
        Ok(file) => file,
        Err(err) => return format!("export failed: {err}"),
    };
    let file = match content
        .export_local_data(ExportScope::Everything, file)
        .await
    {
        Ok(file) => file,
        Err(err) => {
            // A failed export leaves no empty archive behind where a path names
            // it. A content URI stays with the document provider that made it.
            if let FilePath::Path(p) = &path {
                let _ = std::fs::remove_file(p);
            }
            return format!("export failed: {err}");
        }
    };
    match file.metadata() {
        Ok(meta) => format!("Wrote {} bytes to {display}", meta.len()),
        Err(err) => format!("export failed: {err}"),
    }
}

/// Restore one archive, keeping the file's version of every clash, and answer
/// with the line the pane shows.
async fn import(app: &AppHandle, core: &Arc<Core>, bytes: Vec<u8>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = support::native::import_bytes(&session.content, bytes).await;
    core.update(app, |v| v.import_line = Some(line));
    Ok(())
}

/// The identity a client write names on the row, empty while none is live.
fn user_id(session: &Parts) -> String {
    session
        .native
        .session()
        .map_or_else(String::new, |session| session.user_id().to_owned())
}

/// A file id from its 64-hex spelling, `None` on any other length or digit.
fn file_id_from_hex(hex: &str) -> Option<FileId> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (i, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(FileId::from_bytes(bytes))
}

#[cfg(target_os = "android")]
fn platform_sign_in(app: AppHandle, core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(mobile::TabSession {
            app,
            core: Arc::clone(core),
            inner: support::native::TabSession::new(core.parked.clone(), core.parked_rx.clone()),
        }),
    )
    .with_keystore_prompt(Arc::new(support::native::KeystoreUnlock))
}

#[cfg(target_os = "ios")]
fn platform_sign_in(app: AppHandle, core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(mobile::TabSession {
            app,
            core: Arc::clone(core),
            inner: support::native::TabSession::new(core.parked.clone(), core.parked_rx.clone()),
        }),
    )
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn platform_sign_in(_app: AppHandle, _core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth
}

#[cfg(any(target_os = "android", target_os = "ios"))]
mod mobile {
    /// The app's sign-in session. It parks the login through the support
    /// session and marks the view offered before the platform's browser
    /// opens.
    pub(crate) struct TabSession {
        pub(crate) app: tauri::AppHandle,
        pub(crate) core: std::sync::Arc<super::Core>,
        pub(crate) inner: connetto_demo_support::native::TabSession,
    }

    impl connetto_client::AuthorizationSession for TabSession {
        fn authorize(&self, url: String) -> connetto_client::SessionFuture {
            let inner = self.inner.authorize(url);
            let app = self.app.clone();
            let core = std::sync::Arc::clone(&self.core);
            Box::pin(async move {
                core.update(&app, |v| v.session = super::Session::SignInOffered);
                inner.await
            })
        }

        fn delivered(&self) -> Option<String> {
            connetto_demo_support::native::delivered()
        }
    }
}

#[cfg(target_os = "android")]
mod android {
    /// Writes each formatted log line to logcat, since an Android app's
    /// stdout goes nowhere.
    pub(crate) struct Logcat;

    impl std::io::Write for Logcat {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            // An interior NUL would end the line early, so it is dropped.
            let line: Vec<u8> = buf
                .iter()
                .copied()
                .filter(|byte| *byte != 0 && *byte != b'\n')
                .collect();
            let message = std::ffi::CString::new(line).expect("NUL bytes were removed");
            // The discriminants are logcat's priorities, 0 to 8, so the cast
            // never truncates.
            let priority = android_log_sys::LogPriority::INFO as std::ffi::c_int;
            // SAFETY: both pointers are NUL-terminated strings that live
            // until the call returns, and logcat only reads them.
            unsafe {
                android_log_sys::__android_log_write(
                    priority,
                    c"RustStdoutStderr".as_ptr(),
                    message.as_ptr(),
                );
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}

/// The account chooser, parked in the view while it waits for the choice.
fn account_chooser(app: &AppHandle, core: &Arc<Core>) -> AccountChooser {
    let app = app.clone();
    let core = Arc::clone(core);
    Arc::new(move |accounts: Vec<String>| {
        let app = app.clone();
        let core = Arc::clone(&core);
        Box::pin(async move {
            if accounts.len() <= 1 {
                return AccountChoice::LastUsed;
            }
            let (answer, mut waiting) = mpsc::unbounded_channel::<Option<String>>();
            *core.picker.lock() = Some(answer);
            core.update(&app, |v| {
                v.session = Session::PickingAccount;
                v.picking = Some(accounts.into_iter().map(AccountEntry::new).collect());
            });
            match waiting.recv().await {
                Some(Some(key)) => AccountChoice::Account(key),
                Some(None) => AccountChoice::New,
                None => AccountChoice::LastUsed,
            }
        })
    })
}

/// Sign in, open the replica and start the pump, then feed the view.
async fn setup(
    app: AppHandle,
    core: Arc<Core>,
    data_dir: PathBuf,
    choice: AccountChoice,
) -> anyhow::Result<()> {
    let server = support::native::endpoint(
        std::env::var("CONNETTO_DEMO_WS").ok(),
        option_env!("CONNETTO_DEMO_BUILD_WS"),
        DEFAULT_WS,
    );
    let pg_url = support::native::endpoint(
        std::env::var("CONNETTO_DEMO_PG").ok(),
        option_env!("CONNETTO_DEMO_BUILD_PG"),
        DEFAULT_PG,
    );
    let auth_origin = support::native::endpoint(
        std::env::var("CONNETTO_DEMO_AUTH_ORIGIN").ok(),
        option_env!("CONNETTO_DEMO_BUILD_AUTH_ORIGIN"),
        DEFAULT_AUTH_ORIGIN,
    );

    // Park the loopback login URL instead of opening a browser, so the Sign in
    // control runs it when the user asks. A valid stored credential resolves
    // before the opener runs, so a restore launches nothing.
    let opener = {
        let app = app.clone();
        let core = Arc::clone(&core);
        let data_dir = data_dir.clone();
        Arc::new(move |url: &str| {
            tracing::info!("parked the loopback login: {url}");
            // The demo proof reads the parked URL to complete a login a
            // stand-in browser will not, so it also lands beside the replica.
            let _ = std::fs::write(data_dir.join("parked-login"), url);
            let _ = core.parked.send(Some(url.to_owned()));
            core.update(&app, |v| v.session = Session::SignInOffered);
            Ok(())
        })
    };

    let sign_in = platform_sign_in(
        app.clone(),
        &core,
        Auth::new(auth_origin.clone(), AUTH_PROVIDER)
            .with_account(choice)
            .keyring(KEYRING_SERVICE)
            .with_browser_opener(opener)
            .with_account_chooser(account_chooser(&app, &core)),
    );

    #[cfg(feature = "device-identity")]
    let deployment_roots = [DEVICE_ROOT.to_vec()];
    #[cfg(not(feature = "device-identity"))]
    let deployment_roots: [Vec<u8>; 0] = [];

    let (connected, pump) = support::native::connect(
        server,
        auth_origin,
        KEYRING_SERVICE,
        &data_dir,
        &deployment_roots,
        sign_in,
    )
    .await?;
    let native = connected.native;
    let client = native.client().clone();
    let content = connected.content;
    let authenticator = connected.authenticator;

    // Reset before the tasks start, since the count is pushed only on change.
    let session = native.session();
    let (user, account, accounts) = match session {
        Some(session) => (
            session.user_id().to_owned(),
            session.account().to_owned(),
            session.accounts().to_vec(),
        ),
        None => (String::new(), String::new(), Vec::new()),
    };
    let custody = native.custody();
    let gate = if custody.is_verified() {
        "open"
    } else {
        "none, local data opens without user verification"
    };
    // The nearby panel's standing, pre-read from the device the way the
    // first feed tick would.
    #[cfg(feature = "peer")]
    let nearby = {
        let port = native.peer_address().map(|address| address.port());
        let certificate = native.device_certificate();
        let me = certificate
            .as_ref()
            .map(|certificate| certificate.identity());
        support::view::NearbyView {
            peer_line: support::nearby::peer_line(port, me),
            bluetooth_line: format!("bluetooth: {}", native.bluetooth_state()),
            beacon_line: support::nearby::beacon_line(native.beacon_state()),
            outcome: "idle".to_owned(),
            can_host: !cfg!(any(target_os = "macos", target_os = "ios")),
            ..Default::default()
        }
    };
    core.update(&app, |v| {
        *v = View::initial();
        v.session = Session::SignedIn;
        v.attached = true;
        v.identity = Some(user.clone());
        v.account = Some(AccountEntry::new(account));
        v.known_accounts = accounts.into_iter().map(AccountEntry::new).collect();
        v.status = "connected".to_owned();
        v.gate_line = Some(format!("gate: {gate}"));
        v.custody_line = Some(format!("custody: {custody}"));
        v.device_line = support::native::device_label(&native);
        // Only a lock the gate reports later offers the unlock, and a build
        // with no gate never locks.
        v.offers_unlock = false;
        v.backend_writer = true;
        v.team = Some(support::view::TeamView {
            outcome: "idle".to_owned(),
            peer: cfg!(feature = "peer"),
            ..Default::default()
        });
        #[cfg(feature = "peer")]
        {
            v.nearby = Some(nearby);
        }
    });

    let (backend, commands) = mpsc::unbounded_channel::<BackendCmd>();
    let holds = Arc::new(Holds::default());
    let parts = Arc::new_cyclic(|parts| {
        let mut tasks = JoinSet::new();
        tasks.spawn(pump);
        tasks.spawn(feed_events(app.clone(), Arc::clone(&core), client.clone()));
        tasks.spawn(feed_count(app.clone(), Arc::clone(&core), client.clone()));
        tasks.spawn(feed_grouped(app.clone(), Arc::clone(&core), client.clone()));
        tasks.spawn(feed_orders(
            app.clone(),
            Arc::clone(&core),
            client.clone(),
            Arc::clone(&holds),
        ));
        tasks.spawn(feed_notes(
            app.clone(),
            Arc::clone(&core),
            client.clone(),
            Arc::clone(&holds),
        ));
        tasks.spawn(feed_photos(
            app.clone(),
            Arc::clone(&core),
            client.clone(),
            Arc::clone(&content),
            Arc::clone(&holds),
        ));
        tasks.spawn(feed_products(
            app.clone(),
            Arc::clone(&core),
            client.clone(),
            Arc::clone(&holds),
        ));
        tasks.spawn(feed_device(app.clone(), Arc::clone(&core), parts.clone()));
        tasks.spawn(feed_team(app.clone(), Arc::clone(&core), client.clone()));
        #[cfg(feature = "peer")]
        tasks.spawn(feed_nearby(app.clone(), Arc::clone(&core), parts.clone()));
        #[cfg(feature = "peer")]
        tasks.spawn(feed_peer_events(
            app.clone(),
            Arc::clone(&core),
            parts.clone(),
            client.clone(),
        ));
        #[cfg(feature = "peer")]
        tasks.spawn(feed_team_events(
            app.clone(),
            Arc::clone(&core),
            client.clone(),
        ));
        tasks.spawn(feed_content(
            app.clone(),
            Arc::clone(&core),
            Arc::clone(&content),
        ));
        tasks.spawn(write_backend(pg_url, user, commands));
        Parts {
            native,
            content,
            authenticator,
            backend,
            holds,
            _tasks: tasks,
        }
    });
    {
        let mut slot = core.slot.lock();
        // Only setup fills an empty slot, and only one setup runs at a time.
        if !matches!(*slot, Slot::Empty) {
            anyhow::bail!("a second session started");
        }
        *slot = Slot::Ready(Arc::clone(&parts));
    }

    // The footprint and the storage line are read once here, since the
    // feeds re-read them only when the covered rows move.
    let (app, client) = (app.clone(), client.clone());
    tauri::async_runtime::spawn(async move { reread_storage(&core, &app, &client).await });
    Ok(())
}

/// The client events drive the status and gate lines.
async fn feed_events(app: AppHandle, core: Arc<Core>, client: ConnettoClient<Ws>) {
    let mut events = client.events();
    while let Ok(event) = events.recv().await {
        let gate = match event {
            ClientEvent::Locked => Some("locked, verify it is you to continue"),
            ClientEvent::UnlockDismissed => Some("unlock dismissed, still locked"),
            ClientEvent::Unlocked => Some("open"),
            _ => None,
        };
        let status = support::status_label(&event);
        core.update(&app, |v| {
            if let Some(gate) = gate {
                v.gate_line = Some(format!("gate: {gate}"));
                v.offers_unlock = gate != "open";
            }
            if let Some(status) = status {
                v.status = status;
            }
        });
        // The stop names its cause, and the `Closed` that follows it would
        // only replace that.
        if matches!(event, ClientEvent::Stopped { .. }) {
            break;
        }
    }
}

/// The server-pushed count.
async fn feed_count(app: AppHandle, core: Arc<Core>, client: ConnettoClient<Ws>) {
    let mut handle = match support::orders_count::<Ws>().live(&client).await {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "the order count subscription stopped");
            return;
        }
    };
    loop {
        if let Some(count) = handle.snapshot() {
            core.update(&app, |v| v.server_count = Some(count));
        }
        if handle.changed().await.is_err() {
            break;
        }
    }
}

/// The server-pushed grouped count.
async fn feed_grouped(app: AppHandle, core: Arc<Core>, client: ConnettoClient<Ws>) {
    let mut handle = match support::orders_counts_by_quantity::<Ws>()
        .live(&client)
        .await
    {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "the grouped count subscription stopped");
            return;
        }
    };
    loop {
        let counts = handle.snapshot();
        core.update(&app, |v| v.grouped = support::grouped_label(&counts));
        if handle.changed().await.is_err() {
            break;
        }
    }
}

/// The visible orders rows, newest first, and the newest id the remove button
/// reaches.
async fn feed_orders(
    app: AppHandle,
    core: Arc<Core>,
    client: ConnettoClient<Ws>,
    holds: Arc<Holds>,
) {
    let mut handle = match support::orders_newest_first::<Ws>().live(&client).await {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "the orders list stopped");
            return;
        }
    };
    loop {
        let rows = handle.snapshot();
        let orders: Vec<support::view::OrderRow> = rows
            .iter()
            .map(|row| support::view::OrderRow {
                id: row.id.to_string(),
                quantity: row.quantity,
            })
            .collect();
        *holds.newest_order.lock() = rows.first().map(|row| row.id);
        holds.covered_orders.store(rows.len(), Ordering::Release);
        mirror(&core, &app, &client, &holds);
        core.update(&app, |v| v.orders = orders);
        if handle.changed().await.is_err() {
            break;
        }
    }
}

/// The device-private notes rows.
async fn feed_notes(
    app: AppHandle,
    core: Arc<Core>,
    client: ConnettoClient<Ws>,
    holds: Arc<Holds>,
) {
    let mut handle = match support::notes_rows::<Ws>().live(&client).await {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "the notes list stopped");
            return;
        }
    };
    loop {
        let rows = handle.snapshot();
        let notes: Vec<support::view::NoteRow> = rows
            .iter()
            .map(|row| support::view::NoteRow {
                id: row.id.to_string(),
                body: row.body.clone(),
            })
            .collect();
        holds.covered_notes.store(rows.len(), Ordering::Release);
        mirror(&core, &app, &client, &holds);
        core.update(&app, |v| v.notes = notes);
        if handle.changed().await.is_err() {
            break;
        }
    }
}

/// The photos rows and their display sources, re-resolved when the rows or
/// the pinned fetches move.
async fn feed_photos(
    app: AppHandle,
    core: Arc<Core>,
    client: ConnettoClient<Ws>,
    content: Content,
    holds: Arc<Holds>,
) {
    let mut handle = match support::photos_rows::<Ws>().live(&client).await {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "the photos list stopped");
            return;
        }
    };
    let mut key: (Vec<support::Photo>, u64) = (Vec::new(), 0);
    loop {
        let rows = handle.snapshot();
        let photos: Vec<support::view::PhotoRow> = rows
            .iter()
            .map(|row| support::view::PhotoRow {
                id: row.id.to_string(),
                state: support::photo_state(row.content_state.as_deref()).to_owned(),
            })
            .collect();
        let refresh = holds.thumb_refresh.load(Ordering::Acquire);
        holds.covered_photos.store(rows.len(), Ordering::Release);
        mirror(&core, &app, &client, &holds);
        core.update(&app, |v| v.photos = photos);
        if key.0 != rows || key.1 != refresh {
            // The spawned task owns its snapshot, so the key keeps the rows.
            let rows_for_thumbs = rows.clone();
            key = (rows, refresh);
            let (app, core, content) = (app.clone(), Arc::clone(&core), Arc::clone(&content));
            tauri::async_runtime::spawn(async move {
                let thumbs = thumbs(rows_for_thumbs, content).await;
                // A landed session is someone else's when this one is gone,
                // so only a live session's thumbs may reach the view.
                if core.session().is_some() {
                    core.update(&app, |v| v.photo_thumbs = thumbs);
                }
            });
        }
        if handle.changed().await.is_err() {
            break;
        }
    }
}

/// The shared catalogue rows, by name.
async fn feed_products(
    app: AppHandle,
    core: Arc<Core>,
    client: ConnettoClient<Ws>,
    holds: Arc<Holds>,
) {
    let mut handle = match support::products_by_name::<Ws>().live(&client).await {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "the catalogue list stopped");
            return;
        }
    };
    loop {
        let rows = handle.snapshot();
        let products = support::product_rows(&rows);
        holds.covered_products.store(rows.len(), Ordering::Release);
        mirror(&core, &app, &client, &holds);
        core.update(&app, |v| v.products = products);
        if handle.changed().await.is_err() {
            break;
        }
    }
}

/// The display source of each row, a server URL or local bytes as a `data:`
/// URL.
async fn thumbs(rows: Vec<support::Photo>, content: Content) -> HashMap<String, String> {
    use base64::Engine as _;

    let mut srcs = HashMap::new();
    for row in &rows {
        let Some(file_id) = support::photo_file_id(&row.content_id) else {
            continue;
        };
        let available = row.content_state.as_deref() == Some("available");
        match content.resolve(file_id).await {
            Ok(Resolved::Local { bytes, .. }) => {
                let enc = base64::engine::general_purpose::STANDARD.encode(&bytes);
                srcs.insert(row.id.to_string(), format!("data:image/jpeg;base64,{enc}"));
            }
            Ok(Resolved::Remote { url }) if available => {
                srcs.insert(row.id.to_string(), url);
            }
            _ => {}
        }
    }
    srcs
}

/// The device line, read again every second since enrolment runs beside the
/// pump. It holds the session weakly, since the session owns this task.
async fn feed_device(app: AppHandle, core: Arc<Core>, session: Weak<Parts>) {
    each_second(session, |session| {
        let label = support::native::device_label(&session.native);
        core.update(&app, |v| v.device_line = label);
    })
    .await;
}

/// Run `each` on the session once a second, holding it only for that call.
/// A session still inside `Arc::new_cyclic` does not upgrade yet, so a miss
/// waits for the next tick, and the session's own task set ends the loop.
async fn each_second<T>(session: Weak<T>, mut each: impl FnMut(&T)) {
    loop {
        if let Some(session) = session.upgrade() {
            each(&session);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The nearby panel's standing, read again every second, since the listener
/// opens beside the pump the way enrolment does. It holds the session weakly,
/// since the session owns this task.
#[cfg(feature = "peer")]
async fn feed_nearby(app: AppHandle, core: Arc<Core>, session: Weak<Parts>) {
    each_second(session, |session| {
        let port = session.native.peer_address().map(|address| address.port());
        let certificate = session.native.device_certificate();
        let me = certificate
            .as_ref()
            .map(|certificate| certificate.identity());
        let peer_line = support::nearby::peer_line(port, me);
        let bluetooth_line = format!("bluetooth: {}", session.native.bluetooth_state());
        let beacon_line = support::nearby::beacon_line(session.native.beacon_state());
        let mut linked: Vec<String> = session
            .native
            .linked_peers()
            .iter()
            .map(|link| support::nearby::linked_line(&link.peer, link.remote))
            .collect();
        linked.sort();
        core.update(&app, |v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.peer_line = peer_line;
                panel.bluetooth_line = bluetooth_line;
                panel.beacon_line = beacon_line;
                panel.linked = linked;
            }
        });
    })
    .await;
}

/// The hotspot events drive the nearby panel's offer, its outcome and the
/// hosts whose beacons show, the strongest of which its join reaches.
#[cfg(feature = "peer")]
async fn feed_peer_events(
    app: AppHandle,
    core: Arc<Core>,
    session: Weak<Parts>,
    client: ConnettoClient<Ws>,
) {
    let mut events = client.events();
    while let Ok(event) = events.recv().await {
        let Some(session) = session.upgrade() else {
            break;
        };
        match event {
            ClientEvent::HotspotStopped => {
                core.update(&app, |v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.offer = None;
                        panel.outcome = "stopped by the system or the user".to_owned();
                    }
                });
            }
            ClientEvent::HotspotLeft => {
                core.update(&app, |v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.joined = false;
                        panel.vpn_line = None;
                        panel.outcome = "the joined network was lost".to_owned();
                    }
                });
            }
            ClientEvent::HostNearby { host, prefix, rssi } => {
                let mut hosts = session.holds.nearby_hosts.lock();
                hosts.retain(|known| known.host != host);
                hosts.push(connetto_client::NearbyHost { host, prefix, rssi });
                core.update(&app, |v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.nearby = hosts.iter().map(support::nearby::nearby_line).collect();
                    }
                });
            }
            ClientEvent::HostGone { host } => {
                let mut hosts = session.holds.nearby_hosts.lock();
                hosts.retain(|known| known.host != host);
                core.update(&app, |v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.nearby = hosts.iter().map(support::nearby::nearby_line).collect();
                    }
                });
            }
            ClientEvent::JoinedNetworkBlockedByVpn => {
                core.update(&app, |v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.vpn_line = Some(support::nearby::VPN_LINE.to_owned());
                    }
                });
            }
            _ => {}
        }
    }
}

/// The team roster and the picked team's notes, whose peer rows re-read their
/// provenance every second, since the provenance moves without a row change.
async fn feed_team(app: AppHandle, core: Arc<Core>, client: ConnettoClient<Ws>) {
    let mut roster = match support::teams_of_account::<Ws>().live(&client).await {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "the team roster subscription stopped");
            core.update(&app, |v| {
                if let Some(team) = v.team.as_mut() {
                    team.error = Some(err.to_string());
                }
            });
            return;
        }
    };
    let mut last: Vec<String> = Vec::new();
    let mut notes: Option<LiveQuery<support::TeamNote>> = None;
    let mut members: Option<LiveQuery<String>> = None;
    let mut subscribed: Option<String> = None;
    loop {
        let rows = roster.snapshot();
        if rows != last {
            last = rows.clone();
            core.update(&app, |v| {
                if let Some(team) = v.team.as_mut() {
                    team.set_teams(rows);
                }
            });
        }
        let picked = core
            .view
            .lock()
            .team
            .as_ref()
            .map(|team| team.picked.clone())
            .unwrap_or_default();
        if subscribed.as_deref() != Some(picked.as_str()) {
            core.update(&app, |v| {
                if let Some(team) = v.team.as_mut() {
                    team.notes.clear();
                    team.members.clear();
                    team.error = None;
                }
            });
            notes = None;
            members = None;
            if picked.is_empty() {
                subscribed = None;
            } else {
                subscribed = Some(picked.clone());
                match support::team_members_of::<Ws>(picked.clone())
                    .live(&client)
                    .await
                {
                    Ok(handle) => members = Some(handle),
                    Err(err) => {
                        tracing::warn!(error = %err, "the team members subscription stopped");
                        core.update(&app, |v| {
                            if let Some(team) = v.team.as_mut() {
                                team.error = Some(err.to_string());
                            }
                        });
                    }
                }
                match support::team_notes_for::<Ws>(picked).live(&client).await {
                    Ok(handle) => notes = Some(handle),
                    Err(err) => {
                        tracing::warn!(error = %err, "the team notes subscription stopped");
                        core.update(&app, |v| {
                            if let Some(team) = v.team.as_mut() {
                                team.error = Some(err.to_string());
                            }
                        });
                    }
                }
            }
        }
        if let Some(notes) = &mut notes {
            let rows: Vec<support::TeamNote> = notes.snapshot().to_vec();
            let mut lines = Vec::with_capacity(rows.len());
            for row in &rows {
                #[cfg(feature = "peer")]
                let account: Option<String> = {
                    let key = vec![connetto_client::KeyValue::Blob(row.id.as_bytes().to_vec())];
                    client
                        .provenance("team_notes", &key)
                        .await
                        .ok()
                        .flatten()
                        .filter(|record| {
                            record.state
                                == connetto_client::provisional::ProvisionalState::Provisional
                        })
                        .map(|record| record.account)
                };
                #[cfg(not(feature = "peer"))]
                let account: Option<String> = None;
                lines.push(support::team_note_line(row, account.as_deref()));
            }
            let member_rows: Vec<String> = members
                .as_ref()
                .map(|handle| handle.snapshot().to_vec())
                .unwrap_or_default();
            core.update(&app, |v| {
                if let Some(team) = v.team.as_mut() {
                    team.notes = lines;
                    team.members = member_rows;
                }
            });
            let members_changed = async {
                match members.as_mut() {
                    Some(handle) => handle.changed().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                changed = notes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                changed = members_changed => {
                    if changed.is_err() {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        } else {
            tokio::select! {
                changed = roster.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }
}

/// The provisional events drive the team panel's activity lines, which keep
/// the newest [`MAX_TEAM_EVENT_LINES`].
#[cfg(feature = "peer")]
async fn feed_team_events(app: AppHandle, core: Arc<Core>, client: ConnettoClient<Ws>) {
    let mut events = client.events();
    while let Ok(event) = events.recv().await {
        let added = support::provisional_event_lines(&event);
        if added.is_empty() {
            continue;
        }
        core.update(&app, |v| {
            if let Some(team) = v.team.as_mut() {
                append_team_events(&mut team.events, added);
            }
        });
    }
}

/// The event lines the team panel keeps, newest last.
#[cfg(feature = "peer")]
const MAX_TEAM_EVENT_LINES: usize = 40;

/// Append the lines to the team panel's standing, keeping the newest
/// [`MAX_TEAM_EVENT_LINES`].
#[cfg(feature = "peer")]
fn append_team_events(events: &mut Vec<String>, added: Vec<String>) {
    events.extend(added);
    let excess = events.len().saturating_sub(MAX_TEAM_EVENT_LINES);
    if excess > 0 {
        events.drain(..excess);
    }
}
/// The content events drive the content line, the refused uploads and the
/// lost files.
async fn feed_content(app: AppHandle, core: Arc<Core>, content: Content) {
    let mut events = content.events();
    while let Ok(event) = events.recv().await {
        let line = support::native::content_label(&event);
        core.update(&app, |v| {
            v.content_line = Some(line);
            match &event {
                ContentEvent::UploadRefused { file_id, detail } => {
                    let id = support::short_hex(file_id.as_bytes());
                    if !v.refused_uploads.iter().any(|(hex, _)| *hex == id) {
                        v.refused_uploads.push((id, detail.clone()));
                    }
                }
                ContentEvent::BytesLost { file_id, .. } => {
                    let id = support::short_hex(file_id.as_bytes());
                    if !v.lost_bytes.contains(&id) {
                        v.lost_bytes.push(id);
                    }
                }
                _ => {}
            }
        });
    }
}

/// The covered rows moved, so the pane line and the mirror's footprint re-read.
fn mirror(core: &Arc<Core>, app: &AppHandle, client: &ConnettoClient<Ws>, holds: &Holds) {
    let total = holds.covered_orders.load(Ordering::Acquire)
        + holds.covered_notes.load(Ordering::Acquire)
        + holds.covered_photos.load(Ordering::Acquire)
        + holds.covered_products.load(Ordering::Acquire);
    let changed = holds.covered.lock().replace(total) != Some(total);
    if !changed {
        return;
    }
    core.update(app, |v| v.covered_rows = total);
    let (core, app, client) = (Arc::clone(core), app.clone(), client.clone());
    tauri::async_runtime::spawn(async move { reread_storage(&core, &app, &client).await });
}

/// Read the replica mirror's footprint and the storage line into the view.
async fn reread_storage(core: &Core, app: &AppHandle, client: &ConnettoClient<Ws>) {
    let (pages, free) = support::replica_footprint(client).await;
    let storage = support::storage_line(client).await;
    core.update(app, |v| {
        v.pages = pages;
        v.free_pages = free;
        v.storage = storage;
    });
}

/// The backend writer, straight to Postgres, as the signed-in user, since
/// the deployment policy names the owner on the row it writes.
async fn write_backend(
    pg_url: String,
    user: String,
    mut commands: mpsc::UnboundedReceiver<BackendCmd>,
) {
    use diesel_async::AsyncConnection as _;
    let mut pg = match diesel_async::AsyncPgConnection::establish(&pg_url).await {
        Ok(pg) => pg,
        Err(err) => {
            tracing::error!(error = %err, "connecting the backend writer to Postgres");
            return;
        }
    };
    while let Some(BackendCmd::Insert) = commands.recv().await {
        let insert = diesel_async::RunQueryDsl::execute(
            diesel::insert_into(support::orders::table)
                .values((
                    support::orders::owner_id.eq(user.as_str()),
                    support::orders::quantity.eq(support::fresh_quantity()),
                ))
                .on_conflict_do_nothing(),
            &mut pg,
        )
        .await;
        if let Err(err) = insert {
            tracing::error!(error = %err, "backend write failed");
        }
    }
}

/// Run setup in the background, showing its failure.
fn start(app: AppHandle, core: &Arc<Core>, choice: AccountChoice) {
    // Only one setup runs at a time, so a double-tapped control starts nothing.
    if core
        .starting
        .compare_exchange(false, true, Ordering::Release, Ordering::Acquire)
        .is_err()
    {
        tracing::warn!("a setup is already running; the request is dropped");
        return;
    }
    // The panes and the account bar belong to the session that ended, so a
    // new setup shows only the header until its own session lands.
    core.update(&app, |v| {
        *v = View {
            status: std::mem::take(&mut v.status),
            gate_line: v.gate_line.take(),
            custody_line: v.custody_line.take(),
            device_line: std::mem::take(&mut v.device_line),
            ..View::initial()
        };
    });
    let core = Arc::clone(core);
    tauri::async_runtime::spawn(async move {
        let data_dir = match app.path().app_data_dir() {
            Ok(dir) => dir.join("connetto-tauri-demo"),
            Err(err) => {
                fail(&app, &core, &anyhow::Error::from(err));
                core.starting.store(false, Ordering::Release);
                return;
            }
        };
        if let Err(err) = setup(app.clone(), core.clone(), data_dir, choice).await {
            fail(&app, &core, &err);
        }
        core.starting.store(false, Ordering::Release);
    });
}

fn fail(app: &AppHandle, core: &Arc<Core>, err: &anyhow::Error) {
    let detail = err
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ");
    tracing::error!(%detail, "the setup failed");
    *core.picker.lock() = None;
    core.update(app, |v| {
        v.session = Session::Connecting;
        v.status = format!("boot failed: {detail}");
        v.picking = None;
    });
}

fn init_logging() {
    #[cfg(target_os = "android")]
    connetto_core::logging::install(|| android::Logcat, LOG_FILTER);
    // The iOS proof reads the app's log from this file in its data container.
    #[cfg(target_os = "ios")]
    {
        let file = std::env::var_os("HOME").and_then(|home| {
            let dir = PathBuf::from(home).join("Library/Application Support");
            std::fs::create_dir_all(&dir).ok()?;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("connetto-demo.log"))
                .ok()
        });
        match file {
            Some(file) => connetto_core::logging::install(std::sync::Mutex::new(file), LOG_FILTER),
            None => connetto_core::logging::init_stdout_with_default(LOG_FILTER),
        }
    }
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    connetto_core::logging::init_stdout_with_default(LOG_FILTER);
}

/// Feed the client's away input from the platform lifecycle: `onPause` and
/// `onResume` on Android, the window's focus elsewhere.
fn feed_away(core: &Arc<Core>, event: &WindowEvent) {
    let away = match event {
        #[cfg(mobile)]
        WindowEvent::Suspended => true,
        #[cfg(mobile)]
        WindowEvent::Resumed => false,
        #[cfg(not(mobile))]
        WindowEvent::Focused(focused) => !focused,
        _ => return,
    };
    let at = Moment::now(&SystemClock);
    tracing::debug!(target: "connetto_tauri_demo::away", ?at, away, "the platform reported the lifecycle");
    let Some(client) = core.lifecycle_client() else {
        return;
    };
    tauri::async_runtime::spawn(async move {
        if away {
            client.away(at).await;
        } else {
            client.back(at).await;
        }
    });
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_logging();
    let (parked, parked_rx) = watch::channel(None::<String>);
    let core = Arc::new(Core {
        view: Mutex::new(View::initial()),
        slot: Mutex::new(Slot::Empty),
        parked,
        parked_rx,
        picker: Mutex::new(None),
        starting: AtomicBool::new(false),
    });
    let lifecycle = Arc::clone(&core);
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .manage(Arc::clone(&core))
        .invoke_handler(tauri::generate_handler![view, act])
        .setup(move |app| {
            start(app.handle().clone(), &core, AccountChoice::LastUsed);
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("building the Tauri app")
        .run(move |_app, event| {
            if let RunEvent::WindowEvent { event, .. } = &event {
                feed_away(&lifecycle, event);
            }
        });
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, Weak};
    use std::time::Duration;

    use super::each_second;

    /// A feed spawned while `Arc::new_cyclic` builds its session still reads it.
    #[test]
    fn each_second_reads_a_session_still_being_built() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .expect("runtime");
        let _entered = runtime.enter();
        let (seen, read) = mpsc::channel();
        let mut task = None;
        let _session = Arc::new_cyclic(|session: &Weak<u32>| {
            task = Some(tokio::spawn(each_second(session.clone(), move |value| {
                let _ = seen.send(*value);
            })));
            // An idle worker polls the feed while the session is still unbuilt.
            std::thread::sleep(Duration::from_millis(200));
            7
        });
        assert_eq!(read.recv_timeout(Duration::from_secs(5)), Ok(7));
        task.expect("spawned").abort();
    }
}
