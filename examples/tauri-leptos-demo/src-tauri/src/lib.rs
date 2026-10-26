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
    Gate, ImportChoices, KeyringAuth, KeyringStore, LiveHandle as _, Moment, NativeAuthenticator,
    NativeClient, NativeClientBuilder, NativeTransport, SyncTuning, SystemClock,
    dsl::Watchable as _,
};
use connetto_demo_support as support;
use connetto_file_client::FileId;
use connetto_file_client::{
    Content as ContentPiece, ContentClient, ContentEvent, ContentHandle, FsStore, ReqwestHttp,
    Resolved,
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
const KEYRING_SERVICE: &str = "connetto-tauri-demo";
/// The login redirect, whose scheme is the Android applicationId and the
/// iOS bundle identifier.
#[cfg(any(target_os = "android", target_os = "ios"))]
const APP_REDIRECT: &str = "dev.connetto.tauridemo:/oauth2redirect";
/// The demo's re-check grace, matching the dioxus demo's.
const RECHECK_AFTER: Duration = Duration::from_secs(30);
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
    thumb_refresh: AtomicU64,
}

impl Default for Holds {
    fn default() -> Self {
        Self {
            newest_order: Mutex::new(None),
            covered: Mutex::new(None),
            covered_orders: AtomicUsize::new(0),
            covered_notes: AtomicUsize::new(0),
            covered_photos: AtomicUsize::new(0),
            thumb_refresh: AtomicU64::new(0),
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
    }
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
            core.update(app, |v| v.status = "the browser page is open".to_owned());
            // Clearing the parked URL is what the parked login waits for, so
            // the platform's auth session opens on this click.
            let _ = core.parked.send(None);
        }
        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        Some(url) => {
            core.update(app, |v| v.status = "the browser page is open".to_owned());
            let opener = connetto_client::system_browser_opener();
            if let Err(err) = opener(&url) {
                core.update(app, |v| {
                    v.logout = LogoutState::Error(format!("opening the browser: {err}"));
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
    match client.unsynced().await {
        Ok(unsynced) if unsynced.is_empty() => {
            client.close().await;
            *core.slot.lock() = Slot::Empty;
            drop(session);
            start(app.clone(), core, AccountChoice::Account(key));
            Ok(())
        }
        Ok(unsynced) => {
            *core.slot.lock() = Slot::Ready(session);
            core.update(app, |v| {
                v.logout = LogoutState::Error(format!(
                    "Cannot change account: {} write(s) not yet synced.",
                    unsynced.len()
                ));
            });
            Ok(())
        }
        Err(err) => {
            *core.slot.lock() = Slot::Ready(session);
            core.update(app, |v| {
                v.logout = LogoutState::Error(format!("Cannot change account: {err}"));
            });
            Ok(())
        }
    }
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
    let (pages, free) = support::replica_footprint(&client).await;
    core.update(app, |v| {
        v.pages = pages;
        v.free_pages = free;
    });
    Ok(())
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
    let content = Arc::clone(&session.content);
    let source = std::io::Cursor::new(bytes);
    let mut plan = match content.prepare_local_data_import(source).await {
        Ok(plan) => plan,
        Err(err) => {
            core.update(app, |v| v.import_line = Some(format!("refused: {err}")));
            return Ok(());
        }
    };
    let clash_count = plan.replica_plan().collisions().len();
    let choices = ImportChoices::keeping_the_file();
    let outcome = match content.apply_local_data_import(&mut plan, &choices).await {
        Ok(outcome) => outcome,
        Err(err) => {
            core.update(app, |v| {
                v.import_line = Some(format!("apply failed: {err}"))
            });
            return Ok(());
        }
    };
    let mut line = format!(
        "{} row(s) restored, {} kept, {} write(s) restored, {} content file(s)",
        outcome.rows_restored,
        outcome.rows_kept,
        outcome.writes_restored,
        plan.content_files(),
    );
    if clash_count > 0 {
        line.push_str(&format!(" ({clash_count} clash(es) resolved to the file)"));
    }
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

/// The status line for one content event.
fn content_label(event: &ContentEvent) -> String {
    match event {
        ContentEvent::Uploaded { file_id } => {
            format!("uploaded {}", support::short_hex(file_id.as_bytes()))
        }
        ContentEvent::UploadDeferred { file_id, detail } => {
            format!(
                "deferred {}: {detail}",
                support::short_hex(file_id.as_bytes())
            )
        }
        ContentEvent::UploadRefused { file_id, detail } => {
            format!(
                "refused {}: {detail}",
                support::short_hex(file_id.as_bytes())
            )
        }
        ContentEvent::BytesLost {
            file_id,
            unreadable,
        } => format!(
            "bytes lost {} ({unreadable} unreadable chunks)",
            support::short_hex(file_id.as_bytes())
        ),
        ContentEvent::Fetched { file_id } => {
            format!("fetched {}", support::short_hex(file_id.as_bytes()))
        }
        ContentEvent::LostRequeued { file_id } => {
            format!(
                "re-uploading lost {}",
                support::short_hex(file_id.as_bytes())
            )
        }
        ContentEvent::IntegrityPassFailed { detail } => format!("integrity check failed: {detail}"),
    }
}

/// An endpoint as the running environment sets it, else as the build
/// environment did, else `default`.
fn endpoint(runtime: Option<String>, built: Option<&'static str>, default: &str) -> String {
    runtime
        .or_else(|| built.map(str::to_owned))
        .unwrap_or_else(|| default.to_owned())
}

#[cfg(target_os = "android")]
fn platform_sign_in(app: AppHandle, core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(mobile::TabSession {
            app,
            core: Arc::clone(core),
            parked: core.parked.clone(),
            parked_rx: core.parked_rx.clone(),
        }),
    )
    .with_keystore_prompt(Arc::new(android::KeystoreUnlock))
}

#[cfg(target_os = "ios")]
fn platform_sign_in(app: AppHandle, core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(mobile::TabSession {
            app,
            core: Arc::clone(core),
            parked: core.parked.clone(),
            parked_rx: core.parked_rx.clone(),
        }),
    )
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn platform_sign_in(_app: AppHandle, _core: &Arc<Core>, auth: KeyringAuth) -> KeyringAuth {
    auth
}

#[cfg(any(target_os = "android", target_os = "ios"))]
mod mobile {
    use connetto_client::ClientError;

    /// How long a login in the browser tab may take.
    const LOGIN_WINDOW: std::time::Duration = std::time::Duration::from_secs(600);

    /// The Custom Tab on Android or the authentication session on iOS, and
    /// the app's redirect, through `connetto-auth-session`. The session opens
    /// when the Sign in click clears the parked URL, so a boot starts no
    /// in-app browser.
    pub(crate) struct TabSession {
        pub(crate) app: tauri::AppHandle,
        pub(crate) core: std::sync::Arc<super::Core>,
        pub(crate) parked: tokio::sync::watch::Sender<Option<String>>,
        pub(crate) parked_rx: tokio::sync::watch::Receiver<Option<String>>,
    }

    impl connetto_client::AuthorizationSession for TabSession {
        fn authorize(&self, url: String) -> connetto_client::SessionFuture {
            let mut waiting = self.parked_rx.clone();
            let parked = self.parked.clone();
            let app = self.app.clone();
            let core = std::sync::Arc::clone(&self.core);
            Box::pin(async move {
                // The login stands parked until the Sign in click clears the
                // parked URL, which is what the web's popup opening is.
                let _ = parked.send(Some(url.clone()));
                core.update(&app, |v| v.session = super::Session::SignInOffered);
                let _ = waiting.wait_for(|parked| parked.is_none()).await;
                connetto_auth_session::authorize(&url, LOGIN_WINDOW)
                    .await
                    .map_err(|err| ClientError::Auth(err.to_string()))
            })
        }

        fn delivered(&self) -> Option<String> {
            connetto_auth_session::delivered().unwrap_or_else(|err| {
                tracing::warn!(error = %err, "reading a delivered login redirect");
                None
            })
        }
    }
}

#[cfg(target_os = "android")]
mod android {
    use connetto_client::ClientError;

    /// How long the unlock prompt may stay up.
    const UNLOCK_WINDOW: std::time::Duration = std::time::Duration::from_secs(120);

    /// The Keystore unlock prompt, through `connetto-auth-session`.
    pub(crate) struct KeystoreUnlock;

    impl connetto_client::KeystorePrompt for KeystoreUnlock {
        fn device_secure(&self) -> Result<bool, ClientError> {
            connetto_auth_session::device_secure().map_err(|err| ClientError::Auth(err.to_string()))
        }

        fn approve(&self, cipher: &manganis::jni::objects::GlobalRef) -> Result<bool, ClientError> {
            connetto_auth_session::approve_unlock(
                cipher.as_obj(),
                "Unlock your synced data",
                UNLOCK_WINDOW,
            )
            .map_err(|err| ClientError::Auth(err.to_string()))
        }
    }

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

/// The device line, the identity and certificate standing a build with a
/// device identity reports.
#[cfg(feature = "device-identity")]
fn device_label(native: &NativeClient<Ws, ContentHandle<Ws>>) -> String {
    use connetto_core::device_cert::KeyHome;

    let home = match native.device_key_home() {
        None => return "opening the device key".to_owned(),
        Some(KeyHome::SecureEnclave) => "the Secure Enclave",
        Some(KeyHome::AndroidKeystore { strongbox: true }) => "StrongBox",
        Some(KeyHome::AndroidKeystore { strongbox: false }) => "the Android Keystore",
        Some(KeyHome::Tpm) => "the TPM",
        Some(KeyHome::Software) => "software in the keyring",
    };
    match native.device_certificate() {
        Some(certificate) => {
            let until = chrono::DateTime::<chrono::Utc>::from(certificate.not_after());
            format!(
                "certified until {}, key in {home}, {}",
                until.format("%Y-%m-%d %H:%M UTC"),
                certificate.attestation()
            )
        }
        None => format!("enrolling, key in {home}"),
    }
}

#[cfg(not(feature = "device-identity"))]
fn device_label(_native: &NativeClient<Ws, ContentHandle<Ws>>) -> String {
    "no device identity in this build".to_owned()
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
    use anyhow::Context as _;
    #[cfg(target_os = "android")]
    connetto_auth_session::initialize_tls()?;

    let server = endpoint(
        std::env::var("CONNETTO_DEMO_WS").ok(),
        option_env!("CONNETTO_DEMO_BUILD_WS"),
        DEFAULT_WS,
    );
    let pg_url = endpoint(
        std::env::var("CONNETTO_DEMO_PG").ok(),
        option_env!("CONNETTO_DEMO_BUILD_PG"),
        DEFAULT_PG,
    );
    let auth_origin = endpoint(
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

    tokio::fs::create_dir_all(&data_dir)
        .await
        .context("creating the application data directory")?;

    let builder = NativeClientBuilder::new(server, support::demo_schema())
        .with_tuning(SyncTuning::default().with_trim_threshold(5))
        .with_content(ContentPiece::new().with_heal_lost(
            "SELECT content_id FROM photos WHERE content_state = 'lost'",
            "content_id",
        ))
        .signed_in(sign_in)
        .durable(&data_dir)
        .with_gate(Gate::default().with_recheck(Some(RECHECK_AFTER)));
    #[cfg(feature = "device-identity")]
    let builder = builder.with_deployment_roots([DEVICE_ROOT.to_vec()]);
    #[cfg(all(feature = "device-identity", target_os = "android"))]
    let builder = builder.with_java_access(connetto_auth_session::java_access());
    let (native, pump) = builder.connect_with_pump().await.map_err(|err| match err {
        ClientError::Auth(_) => {
            anyhow::anyhow!(
                "the server refused the credential; check CONNETTO_AUTH and OIDC settings"
            )
        }
        other => anyhow::anyhow!("opening the encrypted replica: {other}"),
    })?;
    let client = native.client().clone();
    let content = match native.content() {
        Some(ContentHandle::Durable(content)) => Arc::clone(content),
        _ => anyhow::bail!("a durable build keeps its files beside the replica"),
    };

    // The authenticator the logout controls use to revoke the session without
    // touching the replica, which the wipe path destroys instead.
    let authenticator = native.session().map(|session| {
        NativeAuthenticator::new(
            auth_origin,
            AUTH_PROVIDER,
            Arc::new(KeyringStore::new(KEYRING_SERVICE)),
            Some(session.account().to_owned()),
        )
    });

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
        v.device_line = device_label(&native);
        // Only a lock the gate reports later offers the unlock, and a build
        // with no gate never locks.
        v.offers_unlock = false;
        v.backend_writer = true;
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
        tasks.spawn(feed_device(app.clone(), Arc::clone(&core), parts.clone()));
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

    // The footprint is read once here, since the feeds re-read it only when
    // the covered rows move.
    let (app, client) = (app.clone(), client.clone());
    tauri::async_runtime::spawn(async move {
        let (pages, free) = support::replica_footprint(&client).await;
        core.update(&app, |v| {
            v.pages = pages;
            v.free_pages = free;
        });
    });
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
    loop {
        let Some(session) = session.upgrade() else {
            return;
        };
        let label = device_label(&session.native);
        core.update(&app, |v| v.device_line = label);
        drop(session);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The content events drive the content line, the refused uploads and the
/// lost files.
async fn feed_content(app: AppHandle, core: Arc<Core>, content: Content) {
    let mut events = content.events();
    while let Ok(event) = events.recv().await {
        let line = content_label(&event);
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
        + holds.covered_photos.load(Ordering::Acquire);
    let changed = holds.covered.lock().replace(total) != Some(total);
    if !changed {
        return;
    }
    core.update(app, |v| v.covered_rows = total);
    let (core, app, client) = (Arc::clone(core), app.clone(), client.clone());
    tauri::async_runtime::spawn(async move {
        let (pages, free) = support::replica_footprint(&client).await;
        core.update(&app, |v| {
            v.pages = pages;
            v.free_pages = free;
        });
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
