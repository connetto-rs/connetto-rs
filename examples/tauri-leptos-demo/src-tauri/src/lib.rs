//! The connetto demo as a Tauri 2 app.
//!
//! The client lives in the native core, which keeps running while the
//! platform pauses the WebView. The core sends the application state as one
//! `View` from `connetto-demo-support` through the channel the page hands its
//! `view` command, in the order it changed. The page's panes follow their live
//! queries through `tauri-plugin-connetto`, which serves the descriptors
//! registered in `run`. The UI calls back through the commands below, and the
//! platform lifecycle drives the client's away input directly.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use connetto_client::teardown::{ForgetError, ForgetRefusal};
use connetto_client::{
    AccountChoice, AccountChooser, Auth, ClientError, ClientEvent, ConnettoClient, ExportScope,
    KeyringAuth, Moment, NativeAuthenticator, NativeClient, NativeTransport, SystemClock,
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
use tauri::{AppHandle, Manager as _, RunEvent, WindowEvent};
use tauri_plugin_connetto::{Registry, SourceSlot};
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
const LOG_FILTER: &str = "info,connetto_client::away=debug,connetto_tauri_demo::away=debug,tauri_plugin_connetto=debug,connetto_tauri_demo::live=info";

type Ws = NativeTransport;
type Content = Arc<ContentClient<Ws, FsStore, ReqwestHttp>>;

/// One signed-in session. Dropping it aborts the tasks feeding the view and
/// closes the client.
struct Parts {
    native: NativeClient<Ws, ContentHandle<Ws>>,
    content: Content,
    authenticator: Option<NativeAuthenticator>,
    backend: mpsc::UnboundedSender<BackendCmd>,
    #[cfg(feature = "peer")]
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

/// The nearby hosts available to the session's join action.
#[cfg(feature = "peer")]
#[derive(Default)]
struct Holds {
    nearby_hosts: Mutex<Vec<connetto_client::NearbyHost>>,
}

enum BackendCmd {
    Insert,
}

/// The core's state, managed by Tauri.
struct Core {
    view: Mutex<View>,
    channel: Mutex<Option<tauri::ipc::Channel<View>>>,
    sources: SourceSlot<ConnettoClient<Ws>>,
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
    /// Send changed views under the view lock to preserve their order.
    fn update(&self, change: impl FnOnce(&mut View)) {
        let mut view = self.view.lock();
        let before = view.clone();
        change(&mut view);
        if *view != before {
            self.send_view(&view);
        }
    }

    /// Send the view while retaining the channel after a send failure.
    fn send_view(&self, view: &View) {
        if let Some(channel) = self.channel.lock().as_ref()
            && let Err(error) = channel.send(view.clone())
        {
            tracing::warn!(%error, "sending the view");
        }
    }

    /// Replace the channel and send the current view in change order.
    fn follow_view(&self, channel: tauri::ipc::Channel<View>) {
        let current = self.view.lock();
        *self.channel.lock() = Some(channel);
        self.send_view(&current);
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

/// Replace the page's channel and send its initial view under the view lock.
#[tauri::command]
fn view(core: tauri::State<'_, Arc<Core>>, channel: tauri::ipc::Channel<View>) {
    core.follow_view(channel);
}

/// Resolve the current session's photo display sources.
#[tauri::command]
async fn photo_sources(
    core: tauri::State<'_, Arc<Core>>,
    photos: Vec<support::Photo>,
) -> Result<HashMap<String, String>, CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    Ok(thumbs(photos, Arc::clone(&session.content)).await)
}

/// Return the accounts of notes that remain provisional.
#[tauri::command]
async fn team_note_accounts(
    core: tauri::State<'_, Arc<Core>>,
    ids: Vec<String>,
) -> Result<HashMap<String, String>, CommandError> {
    #[cfg(feature = "peer")]
    {
        let session = core.session().ok_or(CommandError::NotReady)?;
        let client = session.native.client();
        let mut accounts = HashMap::new();
        for identifier in ids {
            let Ok(uuid) = identifier.parse::<Uuid>() else {
                continue;
            };
            let key = [connetto_client::KeyValue::Blob(uuid.as_bytes().to_vec())];
            if let Some(record) = client.provenance("team_notes", &key).await?
                && record.state == connetto_client::provisional::ProvisionalState::Provisional
            {
                accounts.insert(identifier, record.account);
            }
        }
        Ok(accounts)
    }
    #[cfg(not(feature = "peer"))]
    {
        let _ = (core, ids);
        Ok(HashMap::new())
    }
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
        Action::Unlock => unlock(core).await,
        Action::ChooseAccount { account } => {
            choose_account(core, account);
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
        Action::RemoveOrder { id } => remove_order(core, &id).await,
        Action::CancelLogout => {
            cancel_logout(core);
            Ok(())
        }
        Action::StagePhoto { name, bytes } => stage_photo(core, name, bytes).await,
        Action::Pin => pin_content(core).await,
        Action::Unpin => unpin_content(core).await,
        Action::FetchPinned => fetch_pinned(core).await,
        Action::TidyContent => tidy_content(core).await,
        Action::RetryUpload { file_id } => retry_upload(core, file_id).await,
        Action::AcknowledgeLost => acknowledge_lost(core).await,
        Action::SaveNote { body } => save_note(core, body).await,
        Action::FreeUpSpace => free_up_space(core).await,
        Action::RereadStorage => {
            let session = core.session().ok_or(CommandError::NotReady)?;
            reread_storage(core, session.native.client()).await;
            Ok(())
        }
        Action::Export => export(app, core).await,
        Action::Import { bytes, .. } => import(core, bytes).await,
        Action::EnableBluetooth
        | Action::HostHotspot
        | Action::StopHotspot
        | Action::JoinHotspot { .. }
        | Action::JoinPayload { .. }
        | Action::JoinNearby
        | Action::LeaveHotspot
        | Action::SetPeerLinks { .. } => {
            #[cfg(feature = "peer")]
            serve_nearby(app, core, action).await;
            Ok(())
        }
        Action::AddTeamNote { team, body } => add_team_note(core, team, body).await,
        Action::TidyTeam => {
            #[cfg(feature = "peer")]
            tidy_team(core).await;
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
        Action::EnableBluetooth => enable_bluetooth(core).await,
        Action::HostHotspot => host_hotspot(app, core).await,
        Action::StopHotspot => stop_hotspot(core).await,
        Action::JoinHotspot {
            ssid,
            passphrase,
            port,
        } => join_hotspot(app, core, ssid, passphrase, port).await,
        Action::JoinPayload { payload } => join_payload(app, core, payload).await,
        Action::JoinNearby => join_nearby(app, core).await,
        Action::LeaveHotspot => leave_hotspot(core).await,
        Action::SetPeerLinks { on } => set_peer_links(core, on),
        _ => {}
    }
}

/// Switch every nearby link on or off for the running app, the panel's line
/// following the client's standing.
#[cfg(feature = "peer")]
fn set_peer_links(core: &Arc<Core>, on: bool) {
    let Some(session) = core.session() else {
        return;
    };
    session.native.set_peer_links(on);
    let off = !session.native.peer_links_on();
    core.update(|v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.links_off = off;
        }
    });
}

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
            core.update(|v| v.status = support::native::SIGN_IN_OPEN.to_owned());
            // Clearing the parked URL is what the parked login waits for, so
            // the platform's auth session opens on this click.
            let _ = core.parked.send(None);
        }
        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        Some(url) => {
            core.update(|v| v.status = support::native::SIGN_IN_OPEN.to_owned());
            let opener = connetto_client::system_browser_opener();
            if let Err(err) = opener(&url) {
                core.update(|v| {
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

async fn unlock(core: &Arc<Core>) -> Result<(), CommandError> {
    let client = core.client().ok_or(CommandError::NotReady)?;
    client.unlock().await;
    Ok(())
}

/// Answer the parked account picker. The view returns to connecting until the
/// sign-in it triggered lands or fails.
fn choose_account(core: &Arc<Core>, account: Option<String>) {
    if let Some(answer) = core.picker.lock().take() {
        let _ = answer.send(account);
        core.update(|v| {
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
        core.update(|v| v.logout = LogoutState::Error(line));
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
    core.update(|v| v.logout = LogoutState::Working);
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
    core.update(|v| v.logout = LogoutState::Working);
    wipe(app, core, session, false).await;
    Ok(())
}

/// The confirmed delete, discarding the unsent work.
async fn confirm_delete(app: &AppHandle, core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.begin_sign_out().ok_or(CommandError::NotReady)?;
    core.update(|v| v.logout = LogoutState::Working);
    wipe(app, core, session, true).await;
    Ok(())
}

/// Wipe the replica and start over, discarding the unsent work. A refused
/// wipe keeps the session, and one that closed the client starts over. The
/// account's stored name goes with its data.
async fn wipe(app: &AppHandle, core: &Arc<Core>, session: Arc<Parts>, force: bool) {
    let account = user_id(&session);
    match session.native.forget_device(force).await {
        Ok(()) => {
            forget_name(app, &account);
            *core.slot.lock() = Slot::Empty;
            drop(session);
            start(app.clone(), core, AccountChoice::LastUsed);
        }
        Err(ForgetError::Refused(ForgetRefusal::Unsynced(unsynced))) => {
            *core.slot.lock() = Slot::Ready(session);
            core.update(|v| {
                v.logout = LogoutState::ConfirmDelete {
                    unsynced_count: u64::try_from(unsynced.len()).unwrap_or(u64::MAX),
                };
            });
        }
        Err(ForgetError::Refused(refusal)) => {
            *core.slot.lock() = Slot::Ready(session);
            core.update(|v| {
                v.logout = LogoutState::Error(format!("logout error: {refusal}"));
            });
        }
        Err(ForgetError::Incomplete(report)) => {
            forget_name(app, &account);
            *core.slot.lock() = Slot::Empty;
            drop(session);
            tracing::error!(%report, "the wipe left a report");
            start(app.clone(), core, AccountChoice::LastUsed);
        }
    }
}

/// Drop `account`'s login name from the device's stored names.
fn forget_name(app: &AppHandle, account: &str) {
    let Ok(data_dir) = demo_data_dir(app) else {
        return;
    };
    let mut names = support::native::read_stored_names(&data_dir);
    if names.forget(account)
        && let Err(err) = support::native::write_stored_names(&data_dir, &names)
    {
        tracing::warn!(error = %err, "dropping the account's name");
    }
}

fn cancel_logout(core: &Arc<Core>) {
    core.update(|v| v.logout = LogoutState::Idle);
}

async fn add_order(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    let identity = user_id(&session);
    support::add_order(&client, &identity, support::fresh_quantity()).await?;
    Ok(())
}

/// Remove the order identified by the page's visible row.
async fn remove_order(core: &Arc<Core>, identifier: &str) -> Result<(), CommandError> {
    let Ok(uuid) = identifier.parse::<Uuid>() else {
        return Ok(());
    };
    let session = core.session().ok_or(CommandError::NotReady)?;
    support::remove_newest_order(session.native.client(), uuid).await?;
    Ok(())
}

async fn stage_photo(core: &Arc<Core>, name: String, bytes: Vec<u8>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let identity = user_id(&session);
    let line = match support::mime_from_extension(std::path::Path::new(&name)) {
        None => "not an image: only .jpg .jpeg .png are accepted".to_owned(),
        Some(mime) => {
            let outcome = support::native::stage_photo(
                &session.content,
                bytes.as_slice(),
                mime,
                &identity,
                None,
            )
            .await;
            match outcome {
                Ok(_) => format!("staged: {name}"),
                Err(err) => format!("stage failed: {err}"),
            }
        }
    };
    core.update(|v| v.stage_line = Some(line));
    Ok(())
}

async fn pin_content(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .pin_content("photos", support::PIN_PHOTOS_QUERY, "content_id")
        .await
        .map_or_else(support::pin_failed, |_| support::PIN_OK.to_owned());
    core.update(|v| v.content_line = Some(line));
    Ok(())
}

async fn unpin_content(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .unpin_content("photos")
        .await
        .map_or_else(support::unpin_failed, |_| support::UNPIN_OK.to_owned());
    core.update(|v| v.content_line = Some(line));
    Ok(())
}

async fn fetch_pinned(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .fetch_pinned()
        .await
        .map_or_else(support::fetch_failed, |ids| support::fetch_ok(ids.len()));
    core.update(|view| {
        view.pinned_fetches += 1;
        view.content_line = Some(line);
    });
    Ok(())
}

async fn tidy_content(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = session
        .content
        .tidy_content()
        .await
        .map_or_else(support::tidy_failed, support::tidy_ok);
    core.update(|v| v.content_line = Some(line));
    Ok(())
}

async fn retry_upload(core: &Arc<Core>, hex: String) -> Result<(), CommandError> {
    let Some(file_id) = file_id_from_hex(&hex) else {
        return Ok(());
    };
    let session = core.session().ok_or(CommandError::NotReady)?;
    let short = support::short_hex(file_id.as_bytes());
    match session.content.retry_refused(file_id).await {
        Ok(()) => core.update(|v| {
            v.refused_uploads.retain(|(id, _)| *id != short);
        }),
        Err(err) => core.update(|v| v.content_line = Some(format!("retry failed: {err}"))),
    }
    Ok(())
}

async fn acknowledge_lost(core: &Arc<Core>) -> Result<(), CommandError> {
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
        Ok(()) => core.update(|v| v.lost_bytes.clear()),
        Err(err) => core.update(|v| v.content_line = Some(format!("acknowledge failed: {err}"))),
    }
    Ok(())
}

async fn save_note(core: &Arc<Core>, body: String) -> Result<(), CommandError> {
    if body.is_empty() {
        return Ok(());
    }
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    match support::save_note(&client, &body).await {
        Ok(_) => core.update(|v| v.notes_saved += 1),
        Err(err) => tracing::warn!(error = %err, "the note save failed"),
    }
    Ok(())
}

async fn free_up_space(core: &Arc<Core>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    if let Err(err) = client.tidy().await {
        tracing::warn!(error = %err, "the trim pass failed");
    }
    reread_storage(core, &client).await;
    Ok(())
}

/// Write a note into the team selected by the page.
async fn add_team_note(core: &Arc<Core>, team: String, body: String) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let client = session.native.client().clone();
    let author = user_id(&session);
    let body = body.trim().to_owned();
    if body.is_empty() {
        core.update(|v| {
            if let Some(team) = v.team.as_mut() {
                team.outcome = "the note needs some text".to_owned();
            }
        });
        return Ok(());
    }
    match support::add_team_note(&client, &team, &author, &body).await {
        Ok(_) => core.update(|v| {
            if let Some(team) = v.team.as_mut() {
                team.outcome = "note added".to_owned();
            }
        }),
        Err(err) => core.update(|v| {
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
async fn tidy_team(core: &Arc<Core>) {
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
            core.update(|v| {
                if let Some(team) = v.team.as_mut() {
                    append_team_events(
                        &mut team.events,
                        vec![support::view::TeamEvent::plain(support::tidy_line(
                            remaining,
                        ))],
                    );
                }
            });
        }
        Err(err) => {
            tracing::error!(error = %err, "the tidy pass failed");
            core.update(|v| {
                if let Some(team) = v.team.as_mut() {
                    append_team_events(
                        &mut team.events,
                        vec![support::view::TeamEvent::plain(format!(
                            "tidy: the pass failed: {err}"
                        ))],
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
async fn enable_bluetooth(core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    core.update(|v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = "turning on bluetooth".to_owned();
        }
    });
    match session.native.enable_bluetooth().await {
        Ok(()) => core.update(|v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.outcome = "bluetooth is on".to_owned();
            }
        }),
        Err(err) => core.update(|v| {
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
    core.update(|v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = "asking for the hotspot".to_owned();
        }
    });
    match session.native.host_hotspot().await {
        Ok(hosted) => core.update(|v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.offer = Some(support::nearby::offer_view(&hosted.offer));
                panel.outcome = support::nearby::hosted_outcome(&hosted.beacon);
            }
        }),
        Err(err) => {
            if matches!(err, connetto_client::HotspotError::MissingPermission(_)) {
                ask_peer_permission(app, core);
            }
            core.update(|v| {
                if let Some(panel) = v.nearby.as_mut() {
                    panel.outcome = err.to_string();
                }
            });
        }
    }
}

/// Stop the hotspot this device hosts.
#[cfg(feature = "peer")]
async fn stop_hotspot(core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    session.native.stop_hotspot().await;
    core.update(|v| {
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
    core.update(|v| {
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
            core.update(|v| {
                if let Some(panel) = v.nearby.as_mut() {
                    panel.outcome = outcome;
                }
            });
            return;
        }
    };
    core.update(|v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = support::nearby::JOINING_HOTSPOT.to_owned();
        }
    });
    match session.native.join_hotspot(&offer).await {
        Ok(gateway) => core.update(|v| {
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
async fn leave_hotspot(core: &Arc<Core>) {
    let Some(session) = core.session() else {
        return;
    };
    session.native.leave_hotspot().await;
    core.update(|v| {
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
        core.update(|v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.outcome = "no host is nearby".to_owned();
            }
        });
        return;
    };
    core.update(|v| {
        if let Some(panel) = v.nearby.as_mut() {
            panel.outcome = support::nearby::JOINING_NEARBY.to_owned();
        }
    });
    match session.native.join_nearby(&host).await {
        Ok((identity, gateway)) => core.update(|v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.joined = true;
                panel.outcome = support::nearby::joined_nearby_outcome(gateway, &identity);
            }
        }),
        Err(err) => {
            if support::nearby::nearby_permission(&err).is_some() {
                ask_peer_permission(app, core);
            }
            core.update(|v| {
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
    core.update(|v| v.export_line = Some(line));
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
async fn import(core: &Arc<Core>, bytes: Vec<u8>) -> Result<(), CommandError> {
    let session = core.session().ok_or(CommandError::NotReady)?;
    let line = support::native::import_bytes(&session.content, bytes).await;
    core.update(|v| v.import_line = Some(line));
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
fn platform_sign_in(core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(mobile::TabSession {
            core: Arc::clone(core),
            inner: support::native::TabSession::new(core.parked.clone(), core.parked_rx.clone()),
        }),
    )
    .with_keystore_prompt(Arc::new(support::native::KeystoreUnlock))
}

#[cfg(target_os = "ios")]
fn platform_sign_in(core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(mobile::TabSession {
            core: Arc::clone(core),
            inner: support::native::TabSession::new(core.parked.clone(), core.parked_rx.clone()),
        }),
    )
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn platform_sign_in(_core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth
}

#[cfg(any(target_os = "android", target_os = "ios"))]
mod mobile {
    /// The app's sign-in session. It parks the login through the support
    /// session and marks the view offered before the platform's browser
    /// opens.
    pub(crate) struct TabSession {
        pub(crate) core: std::sync::Arc<super::Core>,
        pub(crate) inner: connetto_demo_support::native::TabSession,
    }

    impl connetto_client::AuthorizationSession for TabSession {
        fn authorize(&self, url: String) -> connetto_client::SessionFuture {
            let inner = self.inner.authorize(url);
            let core = std::sync::Arc::clone(&self.core);
            Box::pin(async move {
                core.update(|v| v.session = super::Session::SignInOffered);
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

/// The account chooser, parked in the view while it waits for the choice,
/// naming each account from the stored names in `data_dir`.
fn account_chooser(core: &Arc<Core>, data_dir: &Path) -> AccountChooser {
    let core = Arc::clone(core);
    let data_dir = data_dir.to_owned();
    Arc::new(move |accounts: Vec<String>| {
        let core = Arc::clone(&core);
        let names = support::native::read_stored_names(&data_dir);
        Box::pin(async move {
            if accounts.len() <= 1 {
                return AccountChoice::LastUsed;
            }
            let (answer, mut waiting) = mpsc::unbounded_channel::<Option<String>>();
            *core.picker.lock() = Some(answer);
            core.update(|v| {
                v.session = Session::PickingAccount;
                v.picking = Some(
                    accounts
                        .into_iter()
                        .map(|key| AccountEntry::new(key, &names))
                        .collect(),
                );
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
async fn setup(core: Arc<Core>, data_dir: PathBuf, choice: AccountChoice) -> anyhow::Result<()> {
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
        let core = Arc::clone(&core);
        let data_dir = data_dir.clone();
        Arc::new(move |url: &str| {
            tracing::info!("parked the loopback login: {url}");
            // The demo proof reads the parked URL to complete a login a
            // stand-in browser will not, so it also lands beside the replica.
            let _ = std::fs::write(data_dir.join("parked-login"), url);
            let _ = core.parked.send(Some(url.to_owned()));
            core.update(|v| v.session = Session::SignInOffered);
            Ok(())
        })
    };

    let sign_in = platform_sign_in(
        &core,
        Auth::new(auth_origin.clone(), AUTH_PROVIDER)
            .with_account(choice)
            .keyring(KEYRING_SERVICE)
            .with_browser_opener(opener)
            .with_account_chooser(account_chooser(&core, &data_dir)),
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
            listening: support::nearby::listening(port, me),
            bluetooth_line: format!("bluetooth: {}", native.bluetooth_state()),
            beacon_line: support::nearby::beacon_line(native.beacon_state()),
            outcome: "idle".to_owned(),
            can_host: !cfg!(any(target_os = "macos", target_os = "ios")),
            ..Default::default()
        }
    };
    let names = support::native::read_stored_names(&data_dir);
    core.sources.set(Some(client.clone()));
    core.update(|v| {
        *v = View::initial();
        v.session = Session::SignedIn;
        v.attached = true;
        v.identity = Some(user.clone());
        v.account = Some(AccountEntry::new(account, &names));
        v.known_accounts = accounts
            .into_iter()
            .map(|key| AccountEntry::new(key, &names))
            .collect();
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
    #[cfg(feature = "peer")]
    let holds = Arc::new(Holds::default());
    let parts = Arc::new_cyclic(|parts| {
        let mut tasks = JoinSet::new();
        tasks.spawn(pump);
        tasks.spawn(feed_events(Arc::clone(&core), client.clone()));
        tasks.spawn(feed_own_name(
            Arc::clone(&core),
            client.clone(),
            data_dir.clone(),
            user.clone(),
        ));
        tasks.spawn(feed_device(Arc::clone(&core), parts.clone()));
        #[cfg(feature = "peer")]
        tasks.spawn(feed_nearby(Arc::clone(&core), parts.clone()));
        #[cfg(feature = "peer")]
        tasks.spawn(feed_peer_events(
            Arc::clone(&core),
            parts.clone(),
            client.clone(),
        ));
        #[cfg(feature = "peer")]
        tasks.spawn(feed_team_events(Arc::clone(&core), client.clone()));
        tasks.spawn(feed_content(Arc::clone(&core), Arc::clone(&content)));
        tasks.spawn(write_backend(pg_url, user, commands));
        Parts {
            native,
            content,
            authenticator,
            backend,
            #[cfg(feature = "peer")]
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

    // Read storage once before the page requests subsequent readings.
    tauri::async_runtime::spawn(async move { reread_storage(&core, &client).await });
    Ok(())
}

/// The client events drive the status and gate lines.
async fn feed_events(core: Arc<Core>, client: ConnettoClient<Ws>) {
    let mut events = client.events();
    while let Ok(event) = events.recv().await {
        match &event {
            ClientEvent::WatchReleased { sub_id } => {
                tracing::info!(target: "connetto_tauri_demo::live", sub_id, "watch released");
            }
            ClientEvent::SnapshotBegin { sub_id } | ClientEvent::Aggregate { sub_id, .. } => {
                tracing::info!(target: "connetto_tauri_demo::live", sub_id, "watch observed");
            }
            _ => {}
        }
        let gate = match event {
            ClientEvent::Locked => Some("locked, verify it is you to continue"),
            ClientEvent::UnlockDismissed => Some("unlock dismissed, still locked"),
            ClientEvent::Unlocked => Some("open"),
            _ => None,
        };
        let status = support::status_label(&event);
        core.update(|v| {
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
async fn feed_device(core: Arc<Core>, session: Weak<Parts>) {
    each_second(session, |session| {
        let label = support::native::device_label(&session.native);
        core.update(|v| v.device_line = label);
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
async fn feed_nearby(core: Arc<Core>, session: Weak<Parts>) {
    each_second(session, |session| {
        let port = session.native.peer_address().map(|address| address.port());
        let certificate = session.native.device_certificate();
        let me = certificate
            .as_ref()
            .map(|certificate| certificate.identity());
        let listening = support::nearby::listening(port, me);
        let bluetooth_line = format!("bluetooth: {}", session.native.bluetooth_state());
        let beacon_line = support::nearby::beacon_line(session.native.beacon_state());
        let mut linked: Vec<_> = session
            .native
            .linked_peers()
            .iter()
            .map(|link| support::nearby::linked_peer(&link.peer, link.remote))
            .collect();
        linked.sort();
        core.update(|v| {
            if let Some(panel) = v.nearby.as_mut() {
                panel.listening = listening;
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
async fn feed_peer_events(core: Arc<Core>, session: Weak<Parts>, client: ConnettoClient<Ws>) {
    let mut events = client.events();
    while let Ok(event) = events.recv().await {
        let Some(session) = session.upgrade() else {
            break;
        };
        match event {
            ClientEvent::HotspotStopped => {
                core.update(|v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.offer = None;
                        panel.outcome = "stopped by the system or the user".to_owned();
                    }
                });
            }
            ClientEvent::HotspotLeft => {
                core.update(|v| {
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
                core.update(|v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.nearby = hosts.iter().map(support::nearby::nearby_line).collect();
                    }
                });
            }
            ClientEvent::HostGone { host } => {
                let mut hosts = session.holds.nearby_hosts.lock();
                hosts.retain(|known| known.host != host);
                core.update(|v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.nearby = hosts.iter().map(support::nearby::nearby_line).collect();
                    }
                });
            }
            ClientEvent::JoinedNetworkBlockedByVpn => {
                core.update(|v| {
                    if let Some(panel) = v.nearby.as_mut() {
                        panel.vpn_line = Some(support::nearby::VPN_LINE.to_owned());
                    }
                });
            }
            _ => {}
        }
    }
}

/// The provisional events drive the team panel's activity lines, which keep
/// the newest [`MAX_TEAM_EVENT_LINES`].
#[cfg(feature = "peer")]
async fn feed_team_events(core: Arc<Core>, client: ConnettoClient<Ws>) {
    let mut events = client.events();
    while let Ok(event) = events.recv().await {
        let added = support::provisional_events(&event);
        if added.is_empty() {
            continue;
        }
        core.update(|v| {
            if let Some(team) = v.team.as_mut() {
                append_team_events(&mut team.events, added);
                team.provisional_events += 1;
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
fn append_team_events(
    events: &mut Vec<support::view::TeamEvent>,
    added: Vec<support::view::TeamEvent>,
) {
    events.extend(added);
    let excess = events.len().saturating_sub(MAX_TEAM_EVENT_LINES);
    if excess > 0 {
        events.drain(..excess);
    }
}
/// The content events drive the content line, the refused uploads and the
/// lost files.
async fn feed_content(core: Arc<Core>, content: Content) {
    let mut events = content.events();
    while let Ok(event) = events.recv().await {
        let line = support::native::content_label(&event);
        core.update(|v| {
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

/// Read the replica mirror's footprint and the storage line into the view.
async fn reread_storage(core: &Core, client: &ConnettoClient<Ws>) {
    let (pages, free) = support::replica_footprint(client).await;
    let storage = support::storage_line(client).await;
    core.update(|v| {
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
    core.sources.set(None);
    // The panes and the account bar belong to the session that ended, so a
    // new setup shows only the header until its own session lands.
    core.update(|v| {
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
        let data_dir = match demo_data_dir(&app) {
            Ok(dir) => dir,
            Err(err) => {
                fail(&core, &anyhow::Error::from(err));
                core.starting.store(false, Ordering::Release);
                return;
            }
        };
        if let Err(err) = setup(core.clone(), data_dir, choice).await {
            fail(&core, &err);
        }
        core.starting.store(false, Ordering::Release);
    });
}

/// The folder the replica, the parked login and the stored names live in.
fn demo_data_dir(app: &AppHandle) -> tauri::Result<PathBuf> {
    Ok(app.path().app_data_dir()?.join("connetto-tauri-demo"))
}

/// The live account's own `users` row names it in the view and in the
/// device's stored names, so Switch to names it once it is not live.
async fn feed_own_name(
    core: Arc<Core>,
    client: ConnettoClient<Ws>,
    data_dir: PathBuf,
    account: String,
) {
    support::follow_own_name(
        client,
        account.clone(),
        |name| {
            let mut names = support::native::read_stored_names(&data_dir);
            if !names.remember(&account, name) {
                return;
            }
            if let Err(err) = support::native::write_stored_names(&data_dir, &names) {
                tracing::warn!(error = %err, "keeping the account's name");
            }
            core.update(|v| v.name_account(&account, name));
        },
        |err| tracing::warn!(error = %err, "the account's name feed ended"),
    )
    .await;
}

fn fail(core: &Arc<Core>, err: &anyhow::Error) {
    let detail = err
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ");
    tracing::error!(%detail, "the setup failed");
    *core.picker.lock() = None;
    core.update(|v| {
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

/// Run the native session and the page's live-query plugin.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_logging();
    let (plugin, sources) = tauri_plugin_connetto::init(
        Registry::<ConnettoClient<Ws>>::new()
            .register_keyed::<support::OrdersNewestFirst>()
            .register_keyed::<support::NotesRows>()
            .register_keyed::<support::PhotosRows>()
            .register_keyed::<support::ProductsByName>()
            .register_keyed::<support::TeamNotesFor>()
            .register::<support::OrdersCount>()
            .register::<support::OrdersCountsByQuantity>()
            .register::<support::TeamsOfAccount>()
            .register::<support::TeamMembersOf>()
            .register_keyed::<support::UserNames>(),
    );
    let (parked, parked_rx) = watch::channel(None::<String>);
    let core = Arc::new(Core {
        view: Mutex::new(View::initial()),
        channel: Mutex::new(None),
        sources,
        slot: Mutex::new(Slot::Empty),
        parked,
        parked_rx,
        picker: Mutex::new(None),
        starting: AtomicBool::new(false),
    });
    let lifecycle = Arc::clone(&core);
    let page = Arc::clone(&core);
    tauri::Builder::default()
        .plugin(plugin)
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .manage(Arc::clone(&core))
        .invoke_handler(tauri::generate_handler![
            view,
            photo_sources,
            team_note_accounts,
            act
        ])
        .on_page_load(move |_webview, payload| {
            if payload.event() == tauri::webview::PageLoadEvent::Started {
                page.channel.lock().take();
            }
        })
        .setup(move |app| {
            start(app.handle().clone(), &core, AccountChoice::LastUsed);
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("building the Tauri app")
        .run(move |_, event| {
            if let RunEvent::WindowEvent { event, .. } = &event {
                if matches!(event, WindowEvent::Destroyed) {
                    lifecycle.channel.lock().take();
                }
                feed_away(&lifecycle, event);
            }
        });
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, Weak};
    use std::time::Duration;

    use parking_lot::Mutex;
    use tauri::ipc::{Channel, InvokeResponseBody};

    use super::{Core, Slot, View, each_second};

    /// Construct a core without a session or a page.
    fn core() -> Core {
        let (parked, parked_rx) = tokio::sync::watch::channel(None);
        Core {
            view: Mutex::new(View::initial()),
            channel: Mutex::new(None),
            sources: tauri_plugin_connetto::SourceSlot::default(),
            slot: Mutex::new(Slot::Empty),
            parked,
            parked_rx,
            picker: Mutex::new(None),
            starting: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// A replacement channel immediately receives the current view alone.
    #[test]
    fn view_channel_replaces_the_previous_page() {
        let core = core();
        let (first, first_messages) = mpsc::channel();
        core.follow_view(Channel::new(move |message| {
            first.send(message).expect("first page");
            Ok(())
        }));
        assert!(first_messages.try_recv().is_ok());
        core.view.lock().pinned_fetches = 3;
        let (second, second_messages) = mpsc::channel();
        core.follow_view(Channel::new(move |message| {
            second.send(message).expect("second page");
            Ok(())
        }));
        let InvokeResponseBody::Json(message) = second_messages.try_recv().expect("current view")
        else {
            panic!("the view uses JSON");
        };
        assert!(message.contains("\"pinned_fetches\":3"));
        assert!(first_messages.try_recv().is_err());
        core.channel.lock().take();
        core.send_view(&core.view.lock());
        assert!(second_messages.try_recv().is_err());
    }

    /// A failed send retains the page channel for later views.
    #[test]
    fn view_channel_survives_send_errors() {
        let core = core();
        let (attempts, received) = mpsc::channel();
        core.follow_view(Channel::new(move |_| {
            attempts.send(()).expect("send attempt");
            Err(std::io::Error::other("page send failed").into())
        }));
        assert!(core.channel.lock().is_some());
        core.send_view(&core.view.lock());
        assert_eq!(received.try_iter().count(), 2);
    }

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
