//! The web platform. It boots the dedicated DB worker, attaches the tab
//! behind the leader election, and keeps the view signal current from the
//! client events and the worker's broadcasts. The panes follow their live
//! queries on the tab's client themselves.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;

use base64::Engine;
use connetto_client::{AccountChoice, Auth, ClientEvent, ConnettoClient, Custody, NoGate};
use connetto_demo_support as support;
use connetto_demo_support::web as glue;
use connetto_file_core::{FileId, MimeClass};
use connetto_web::builder::{AttachedTab, TabTopology};
use connetto_web::leader::Membership;
use connetto_web::unlock::serve_account_choice;
use connetto_web::{MessageTransport, TabContent, TabResolved, workers};
use js_sys::{Array, Uint8Array};
use leptos::prelude::*;
use rosetta_uuid::Uuid;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{
    BlobPropertyBag, FileSystemFileHandle, FileSystemWritableFileStream, SaveFilePickerOptions,
};

use connetto_demo_support::view::{AccountEntry, Action, LogoutState, Session, TeamView, View};

/// The tab-to-worker transport the window's client rides.
pub type Transport = MessageTransport<web_sys::BroadcastChannel>;
/// The window's client over the tab-to-worker transport.
pub type Client = ConnettoClient<Transport>;
/// The live source the panes follow their queries through, the tab's client.
pub type Source = Client;

/// Boot the connetto DB tier in the worker context with the demo config.
///
/// # Errors
///
/// A JS string describing the VFS, acquisition, upstream connect, or
/// subscribe failure.
async fn run_db_worker() -> Result<(), JsValue> {
    let origin = glue::worker_origin();
    let session = glue::demo_builder()
        .signed_in(
            Auth::new(support::AUTH_BASE, glue::ask_provider().await)
                .with_account(AccountChoice::Ask),
        )
        .with_redirect_uri(format!("{origin}/"))
        .with_auth_db_name(glue::AUTH_DB)
        .durable(glue::DB_NAME)
        .boot::<String>()
        .await
        .map_err(JsValue::from)?;
    glue::broadcast_boot_session(
        session.identity.as_deref(),
        session.account.as_deref(),
        session.session_expires_at,
    );
    Ok(())
}

/// The window's live connection to the DB worker, plus the topology tokens
/// it must keep alive, leadership (this window may own the worker) and the
/// tab liveness lock (dropped on page close, so the worker reaps this tab).
struct Boot {
    client: Client,
    /// The tab's content lane, split off the transport before the client took it.
    content: Rc<TabContent<web_sys::BroadcastChannel>>,
    /// Shared rather than owned, because the gate ceremony is awaited and the
    /// handle has to outlive the borrow that reaches it.
    membership: Rc<Membership>,
    _tab: AttachedTab,
}

/// The served URL of this app's wasm-bindgen glue module, recovered from the
/// wasm fetch the page already performed. Under trunk the wasm resource
/// entry names the glue by suffix swap.
fn glue_url() -> String {
    let found = js_sys::eval(
        r#"performance.getEntriesByType("resource").map((e) => e.name).find((n) => n.endsWith("_bg.wasm"))"#,
    )
    .ok()
    .and_then(|value| value.as_string())
    .expect("a loaded wasm resource entry");
    let base = found.strip_suffix("_bg.wasm").expect("wasm suffix");
    format!("{base}.js")
}

/// Join the topology and attach this window's tab client.
///
/// Trunk's glue does not self-initialize, so the winner of the leader
/// election spawns the worker from a generated bootstrap that imports the
/// glue and runs init.
async fn boot_window() -> Result<Boot, JsValue> {
    let glue = glue_url();
    let tab = glue::demo_builder()
        .attach(TabTopology {
            leader_lock: glue::LEADER_LOCK,
            glue_url: &glue,
            bootstrap: workers::WorkerBootstrap::Generated,
        })
        .await
        .map_err(|err| JsValue::from_str(&err.to_string()))?;
    let content = tab
        .content
        .clone()
        .ok_or_else(|| JsValue::from_str("the demo build keeps files"))?;
    Ok(Boot {
        client: tab.client.clone(),
        content,
        membership: Rc::clone(&tab.membership),
        _tab: tab,
    })
}

/// The page's channels and state cells, alive for the rest of the page's
/// life, kept alive by the platform handle [`act`] reaches.
struct PageHolds {
    boot: Rc<RefCell<Option<Boot>>>,
    login_url: Rc<RefCell<Option<String>>>,
    answer: Rc<Cell<Option<glue::AccountAnswer>>>,
    custody: Rc<Cell<Option<Custody>>>,
    gate: Rc<Cell<&'static str>>,
    _provider: glue::ProviderListener,
    _login: glue::LoginUrlListener,
    _boot_session: glue::BootSessionListener,
}

/// The web platform's one handle, the target of every [`act`].
#[derive(Clone)]
struct Platform {
    view: RwSignal<View>,
    holds: Rc<PageHolds>,
}

thread_local! {
    /// The page's platform, set once by [`run_page`].
    static PLATFORM: RefCell<Option<Platform>> = const { RefCell::new(None) };
    /// The account whose own name the page follows, so a second trigger for
    /// the same live account starts no second feed.
    static NAMED: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Serve one user control. A control sent before [`run_page`] set the
/// platform has nothing to act on and is dropped.
pub fn act(action: Action) {
    if let Some(platform) = PLATFORM.with(|cell| cell.borrow().clone()) {
        platform.act(action);
    }
}

/// Whether the logout or delete flow owns the page.
fn logging_out(v: &View) -> bool {
    v.logout == LogoutState::Working
        || matches!(v.logout, LogoutState::ConfirmDelete { .. })
        || v.refused_pending.is_some()
}

/// Start this wasm module in its browser context. The dedicated DB worker
/// runs this same module and boots the connetto DB tier, because a worker has
/// no Window. A login popup delivers its code and closes. Every other window
/// answers with the one view signal the components read.
pub fn run_page() -> Option<RwSignal<View>> {
    connetto_web::logging::init_console();
    let Some(win) = web_sys::window() else {
        spawn_local(async {
            if let Err(err) = run_db_worker().await {
                tracing::error!(error = ?err, "db worker failed");
            }
        });
        return None;
    };

    // The OAuth popup lands here with ?code=&state= after the user
    // authenticates. Deliver the code to the waiting worker, then close the
    // popup so the original window continues uninterrupted.
    //
    // A same-window redirect (no opener) delivers the code the same way,
    // clears the query string, and falls through to render the app. In
    // practice the popup path is used so the original worker keeps its PKCE
    // state and can verify the returned state parameter.
    let search = win.location().search().unwrap_or_default();
    if let (Some(code), Some(state)) = (query_param(&search, "code"), query_param(&search, "state"))
    {
        let _ = connetto_web::deliver_login_code(&code, &state);
        let has_opener = js_sys::Reflect::get(win.as_ref(), &JsValue::from_str("opener"))
            .map(|value| !value.is_null() && !value.is_undefined())
            .unwrap_or(false);
        if has_opener {
            let _ = win.close();
            return None;
        }
        // Outside a popup, clear the query string so a reload does not
        // re-deliver an already-spent code.
        let pathname = win.location().pathname().unwrap_or_else(|_| "/".to_owned());
        if let Ok(history) = win.history() {
            let _ = history.replace_state_with_url(&JsValue::NULL, "", Some(&pathname));
        }
    }
    Some(boot_page())
}

/// One `key=value` pair from a URL query string, `None` when the key is
/// absent.
fn query_param(search: &str, key: &str) -> Option<String> {
    search.trim_start_matches('?').split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| v.to_owned())
    })
}

/// Wire the page's channels, register the account chooser, and start the
/// boot, answering with the one view signal the components read.
fn boot_page() -> RwSignal<View> {
    let view = RwSignal::new(View::initial());
    if let Some(stored) = glue::read_stored_account() {
        view.update(|v| {
            v.session = Session::Restoring;
            v.stored_account = Some(AccountEntry::new(stored, &glue::read_stored_names()));
        });
    }

    let login_url: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let login_listener = {
        let parked = Rc::clone(&login_url);
        glue::LoginUrlListener::new(move |url| {
            *parked.borrow_mut() = Some(url);
            // A page with no live session offers the sign-in, and a picker
            // or a signed-in page keeps what it shows.
            view.update(|v| {
                if matches!(v.session, Session::Connecting | Session::Restoring) {
                    v.session = Session::SignInOffered;
                }
            });
        })
    };

    let boot_session_listener = {
        let latest: Rc<Cell<Option<u64>>> = Rc::new(Cell::new(None));
        glue::BootSessionListener::new(move |uid, account, expires_at| {
            if view.with_untracked(logging_out) {
                return;
            }
            let live = uid.clone();
            view.update(|v| {
                if let Some(uid) = uid {
                    v.identity = Some(uid);
                }
                match account {
                    Some(account) => {
                        glue::save_stored_account(&account);
                        let entry = AccountEntry::new(account, &glue::read_stored_names());
                        v.account = Some(entry.clone());
                        v.stored_account = Some(entry);
                    }
                    None => v.account = None,
                }
                v.session = Session::SignedIn;
            });
            if let (Some(account), Some(client)) = (live, source()) {
                follow_own_name(view, client, account);
            }
            // A signed-in session stands the team panel.
            stand_team(view);
            if let Some(secs) = expires_at {
                latest.set(Some(secs));
                let view = view;
                let latest = Rc::clone(&latest);
                spawn_local(async move {
                    let line = glue::expiry_warn_for_session(secs).await;
                    if latest.get() == Some(secs) {
                        view.update(|v| v.expiry_warning = line);
                    }
                });
            }
        })
    };

    let answer: Rc<Cell<Option<glue::AccountAnswer>>> = Rc::new(Cell::new(None));
    {
        let answer_cell = Rc::clone(&answer);
        serve_account_choice(move |accounts| {
            let answer_cell = Rc::clone(&answer_cell);
            async move {
                let names = glue::read_stored_names();
                view.update(|v| {
                    v.known_accounts = accounts
                        .iter()
                        .map(|key| AccountEntry::new(key.clone(), &names))
                        .collect();
                });
                // A signed-in page ignores the question, but the worker still
                // needs an answer to proceed with its boot.
                if accounts.len() <= 1
                    || view.with_untracked(|v| v.session == Session::SignedIn || logging_out(v))
                {
                    return AccountChoice::LastUsed;
                }
                glue::choose_account(
                    &accounts,
                    || {
                        view.update(|v| {
                            v.picking = Some(
                                accounts
                                    .iter()
                                    .map(|key| AccountEntry::new(key.clone(), &names))
                                    .collect(),
                            );
                            v.session = Session::PickingAccount;
                        });
                    },
                    || answer_cell.take(),
                )
                .await
            }
        });
    }

    let custody: Rc<Cell<Option<Custody>>> = Rc::new(Cell::new(None));
    let gate: Rc<Cell<&'static str>> = Rc::new(Cell::new("open"));
    let holds = Rc::new(PageHolds {
        boot: Rc::new(RefCell::new(None)),
        login_url,
        answer,
        custody,
        gate,
        _provider: glue::ProviderListener::new(),
        _login: login_listener,
        _boot_session: boot_session_listener,
    });

    {
        let holds = Rc::clone(&holds);
        spawn_local(async move {
            match boot_window().await {
                Ok(boot) => {
                    if let Ok(level) = workers::request_custody().await {
                        holds.custody.set(Some(level));
                        set_header_lines(&holds, &view);
                    }
                    reseed_refused_lost(&boot.content, &view).await;
                    let client = boot.client.clone();
                    // The panes mount on the attach below and take the client
                    // from the boot, so it stands first.
                    *holds.boot.borrow_mut() = Some(boot);
                    view.update(|v| {
                        v.attached = true;
                        v.status = "connected".to_owned();
                    });
                    stand_team(view);
                    if let Some(account) = view.with_untracked(|v| v.identity.clone()) {
                        follow_own_name(view, client.clone(), account);
                    }
                    refresh_storage(&view, &client);
                    let mut events = client.events();
                    while let Ok(event) = events.recv().await {
                        // The worker states its gate to every tab as the
                        // away-and-return re-check locks and unlocks it.
                        match event {
                            ClientEvent::Locked | ClientEvent::UnlockDismissed => {
                                holds.gate.set("locked")
                            }
                            ClientEvent::Unlocked => holds.gate.set("open"),
                            // An account switch replaces the worker, which the
                            // tab meets again through a reconnect, and the store
                            // it reads may now count another account.
                            ClientEvent::Reconnected | ClientEvent::SyncStatus(_) => {
                                refresh_storage(&view, &client);
                            }
                            _ => {}
                        }
                        set_header_lines(&holds, &view);
                        if let Some(label) = support::status_label(&event) {
                            view.update(|v| v.status = label);
                        }
                        // The stop names its cause, and the `Closed` that
                        // follows it would only replace that.
                        if matches!(event, ClientEvent::Stopped { .. }) {
                            break;
                        }
                    }
                }
                Err(err) => {
                    view.update(|v| v.status = format!("boot failed: {err:?}"));
                }
            }
        });
    }

    PLATFORM.with(|cell| *cell.borrow_mut() = Some(Platform { view, holds }));
    view
}

/// The header's gate and custody lines and the enrolment offer, from the
/// custody snapshot and the worker's reported unlock state.
fn set_header_lines(holds: &PageHolds, view: &RwSignal<View>) {
    let Some(custody) = holds.custody.get() else {
        return;
    };
    let state = holds.gate.get();
    view.update(|v| {
        v.gate_line = Some(format!("gate: {}", glue::gate_text(custody, state)));
        v.custody_line = Some(format!("custody: {}", glue::custody_text(custody)));
        v.enrol_offer = matches!(custody, Custody::Unverified(NoGate::Offerable));
    });
}

/// The content lane's refused and lost files, re-read from what the lane
/// persisted, so a restart and a fresh stage hide nothing.
async fn reseed_refused_lost(
    content: &Rc<TabContent<web_sys::BroadcastChannel>>,
    view: &RwSignal<View>,
) {
    let refused = content.refused_content().await.unwrap_or_default();
    let retired = content.retired_content().await.unwrap_or_default();
    view.update(|v| {
        for (id, detail) in refused {
            let hex = hex_of(id.as_bytes());
            if !v.refused_uploads.iter().any(|(known, _)| *known == hex) {
                v.refused_uploads.push((hex, detail));
            }
        }
        for id in retired {
            let hex = hex_of(id.as_bytes());
            if !v.lost_bytes.contains(&hex) {
                v.lost_bytes.push(hex);
            }
        }
    });
}

/// The storage line the header shows.
fn refresh_storage(view: &RwSignal<View>, client: &Client) {
    let view = *view;
    let client = client.clone();
    spawn_local(async move {
        let line = support::storage_line(&client).await;
        view.update(|v| v.storage = line);
    });
}

/// Follow `account`'s own `users` row on the tab's client, naming it in the
/// view and in this origin's stored names, so Switch to names it once it is
/// not live. The feed ends at an account switch, and the next account's
/// session starts its own.
fn follow_own_name(view: RwSignal<View>, client: Client, account: String) {
    let fresh = NAMED.with(|named| {
        let mut named = named.borrow_mut();
        let fresh = named.as_deref() != Some(account.as_str());
        if fresh {
            *named = Some(account.clone());
        }
        fresh
    });
    if !fresh {
        return;
    }
    spawn_local(async move {
        support::follow_own_name(
            client,
            account.clone(),
            |name| {
                let mut names = glue::read_stored_names();
                if names.remember(&account, name) {
                    glue::save_stored_names(&names);
                    view.update(|v| v.name_account(&account, name));
                }
            },
            |_| {},
        )
        .await;
        NAMED.with(|named| {
            let mut named = named.borrow_mut();
            if named.as_deref() == Some(account.as_str()) {
                *named = None;
            }
        });
    });
}

/// Stand the team panel while the session is signed in and no sign-out owns
/// the page. A sign-out or an account switch takes it down.
fn stand_team(view: RwSignal<View>) {
    view.update(|v| {
        if v.session == Session::SignedIn && !logging_out(v) && v.team.is_none() {
            v.team = Some(TeamView {
                outcome: "idle".to_owned(),
                peer: false,
                ..Default::default()
            });
        }
    });
}

/// The tab's client, which the panes follow their live queries on, once the
/// tab attached.
#[must_use]
pub fn source() -> Option<Client> {
    PLATFORM.with(|cell| cell.borrow().as_ref().and_then(Platform::client))
}

/// Each photo's display source by row id, a server URL once the server says
/// the row's content is available and a local `data:` URL before that,
/// empty before the tab attached.
pub async fn photo_sources(rows: Vec<support::Photo>) -> HashMap<String, String> {
    let content = PLATFORM.with(|cell| cell.borrow().as_ref().and_then(Platform::content));
    match content {
        Some(content) => photo_thumbs(&content, &rows).await,
        None => HashMap::new(),
    }
}

/// The account each still-provisional team note came from, by note id,
/// which no note has in a browser, since the browser carries no peer link.
pub async fn team_note_accounts(_ids: Vec<String>) -> HashMap<String, String> {
    HashMap::new()
}

/// Each photo's display source, a server URL once the server says the row's
/// content is available and a local `data:` URL before that.
async fn photo_thumbs(
    content: &Rc<TabContent<web_sys::BroadcastChannel>>,
    rows: &[support::Photo],
) -> HashMap<String, String> {
    let mut srcs: HashMap<String, String> = HashMap::new();
    for photo in rows {
        let Some(file_id) = support::photo_file_id(&photo.content_id) else {
            continue;
        };
        let available = photo.content_state.as_deref() == Some("available");
        match content.resolve(file_id).await {
            TabResolved::Remote { url } if available => {
                srcs.insert(photo.id.to_string(), url);
            }
            TabResolved::Local { blob } => {
                let mime = if blob.type_().is_empty() {
                    "image/jpeg".to_owned()
                } else {
                    blob.type_()
                };
                let Ok(buf) = JsFuture::from(blob.array_buffer()).await else {
                    continue;
                };
                let Ok(buf) = buf.dyn_into::<js_sys::ArrayBuffer>() else {
                    continue;
                };
                let enc = base64::engine::general_purpose::STANDARD
                    .encode(Uint8Array::new(&buf).to_vec());
                srcs.insert(photo.id.to_string(), format!("data:{mime};base64,{enc}"));
            }
            _ => {}
        }
    }
    srcs
}

/// A one-off Blob from raw bytes, the photo and import picks' staging.
fn blob_of(bytes: &[u8], mime: Option<&str>) -> web_sys::Blob {
    let arr = Uint8Array::from(bytes);
    let parts = Array::of1(&JsValue::from(arr));
    let bag = BlobPropertyBag::new();
    if let Some(mime) = mime {
        bag.set_type(mime);
    }
    web_sys::Blob::new_with_str_sequence_and_options(&parts, &bag).expect("blob from bytes")
}

/// The mime string a photo's bytes stage under, by the mime class.
fn mime_type(mime: MimeClass) -> &'static str {
    match mime {
        MimeClass::Jpeg => "image/jpeg",
        MimeClass::Png => "image/png",
        _ => "application/octet-stream",
    }
}

/// The hex encoding of a file id, the pane's display and the action's input.
fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The file id a hex string names, `None` when it is not one.
fn file_id_from_hex(hex: &str) -> Option<FileId> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(FileId::from_bytes(bytes))
}

impl Platform {
    fn client(&self) -> Option<Client> {
        self.holds.boot.borrow().as_ref().map(|b| b.client.clone())
    }

    fn content(&self) -> Option<Rc<TabContent<web_sys::BroadcastChannel>>> {
        self.holds
            .boot
            .borrow()
            .as_ref()
            .map(|b| Rc::clone(&b.content))
    }

    fn membership(&self) -> Option<Rc<Membership>> {
        self.holds
            .boot
            .borrow()
            .as_ref()
            .map(|b| Rc::clone(&b.membership))
    }

    fn page_ok(&self) -> bool {
        self.view
            .with_untracked(|v| v.session == Session::SignedIn && !logging_out(v))
    }

    fn content_ok(&self) -> bool {
        self.view
            .with_untracked(|v| v.session == Session::SignedIn && !logging_out(v) && v.attached)
    }

    /// Serve one user control, answering with the same calls the Yew demo makes.
    fn act(self, action: Action) {
        match action {
            Action::SignIn => self.sign_in(),
            // The browser has no native gate to unlock and no backend writer,
            // and the view never offers either.
            Action::Unlock | Action::InsertBackend => {}
            Action::ChooseAccount { account } => self.choose_account(account),
            Action::AddAccount => self.add_or_switch(None),
            Action::SwitchAccount { account } => self.add_or_switch(Some(account)),
            Action::EnrolGate => self.enrol_gate(),
            Action::LogOutKeep => self.logout_keep(),
            Action::LogOutDelete => self.logout_delete(),
            Action::ConfirmDelete | Action::ForceDelete => self.delete_confirmed(),
            Action::CancelLogout => {
                self.view.update(|v| {
                    v.logout = LogoutState::Idle;
                    v.refused_pending = None;
                });
                // An abandoned sign-out keeps the session, which stands the
                // panel again.
                stand_team(self.view);
            }
            Action::AddOrder => self.add_order(),
            Action::RemoveOrder { id } => self.remove_order(&id),
            Action::StagePhoto { name, bytes } => self.stage_photo(name, bytes),
            Action::Pin => self.pin(),
            Action::Unpin => self.unpin(),
            Action::FetchPinned => self.fetch_pinned(),
            Action::TidyContent => self.tidy_content(),
            Action::RetryUpload { file_id } => self.retry_upload(file_id),
            Action::AcknowledgeLost => self.acknowledge_lost(),
            Action::SaveNote { body } => self.save_note(body),
            Action::FreeUpSpace => self.free_up_space(),
            Action::RereadStorage => self.reread_storage(),
            Action::Export => self.export(),
            Action::Import { bytes, .. } => self.import(bytes),
            Action::AddTeamNote { team, body } => self.add_team_note(team, body),
            // The web's view carries no peer link, so the nearby pane's
            // controls and the team pane's tidy pass have nothing to act on.
            Action::EnableBluetooth
            | Action::HostHotspot
            | Action::StopHotspot
            | Action::JoinHotspot { .. }
            | Action::JoinPayload { .. }
            | Action::JoinNearby
            | Action::LeaveHotspot
            | Action::SetPeerLinks { .. }
            | Action::TidyTeam => {}
        }
    }

    /// Open the login popup at the parked URL, which stays parked while the
    /// worker waits, so a re-click re-opens it.
    fn sign_in(&self) {
        let v = self.view.read_untracked();
        if v.session != Session::SignInOffered {
            return;
        }
        drop(v);
        let Some(url) = self.holds.login_url.borrow().clone() else {
            return;
        };
        if let Some(win) = web_sys::window() {
            let _ = win.open_with_url_and_target_and_features(
                &url,
                "_blank",
                "popup,width=640,height=720",
            );
        }
    }

    /// Answer the parked account chooser, `None` picking the new-account entry.
    fn choose_account(&self, account: Option<String>) {
        let v = self.view.read_untracked();
        if v.session != Session::PickingAccount {
            return;
        }
        drop(v);
        let answer = match account {
            Some(key) => glue::AccountAnswer::Stored(key),
            None => glue::AccountAnswer::New,
        };
        self.holds.answer.set(Some(answer));
        self.view.update(|v| {
            v.picking = None;
            v.session = Session::Connecting;
        });
    }

    /// Sign the page back in as another stored account, or open a fresh
    /// sign-in. The page clears its session fields before the reboot, so the
    /// sign-in banner the replacement worker may need stays visible.
    fn add_or_switch(&self, account: Option<String>) {
        if !self.page_ok() {
            return;
        }
        let Some(membership) = self.membership() else {
            return;
        };
        self.view.update(|v| {
            v.identity = None;
            v.account = None;
            v.session = Session::Connecting;
            v.team = None;
        });
        let view = self.view;
        match account {
            None => {
                if let Err(err) = membership.add_account() {
                    view.update(|v| v.status = format!("add account failed: {err:?}"));
                }
            }
            Some(account) => {
                if let Err(err) = membership.switch_account(&account) {
                    view.update(|v| v.status = format!("switch failed: {err:?}"));
                }
            }
        }
    }

    /// Run the passkey gate enrolment ceremony in this window. The ceremony
    /// runs for as long as the user takes to present a finger, so the
    /// membership handle is cloned out and the borrow released before the
    /// await.
    fn enrol_gate(&self) {
        let v = self.view.read_untracked();
        if v.session != Session::SignedIn || !v.enrol_offer || logging_out(&v) {
            return;
        }
        drop(v);
        let Some(membership) = self.membership() else {
            return;
        };
        let holds = Rc::clone(&self.holds);
        let view = self.view;
        spawn_local(async move {
            match membership.enrol_gate().await {
                Ok(true) => {
                    if let Ok(level) = workers::request_custody().await {
                        holds.custody.set(Some(level));
                        set_header_lines(&holds, &view);
                    }
                }
                Ok(false) => {
                    view.update(|v| v.status = glue::ENROL_WRONG_WINDOW.to_owned());
                }
                Err(err) => {
                    view.update(|v| v.status = glue::enrolment_failed(err));
                }
            }
        });
    }

    fn logout_keep(&self) {
        if !self.page_ok() {
            return;
        }
        let view = self.view;
        view.update(|v| {
            v.logout = LogoutState::Working;
            v.team = None;
        });
        spawn_local(async move {
            if let Err(err) = glue::logout_keep().await {
                view.update(|v| v.logout = LogoutState::Error(err.to_string()));
            }
        });
    }

    fn logout_delete(&self) {
        if !self.page_ok() {
            return;
        }
        let view = self.view;
        view.update(|v| {
            v.logout = LogoutState::Working;
            v.team = None;
        });
        spawn_local(async move {
            match glue::logout_delete().await {
                Ok(glue::DeleteOutcome::Confirm(pending)) => {
                    view.update(|v| {
                        v.logout = LogoutState::ConfirmDelete {
                            unsynced_count: pending.len(),
                        };
                    });
                }
                Ok(glue::DeleteOutcome::Refused(pending)) => {
                    view.update(|v| {
                        v.logout = LogoutState::Idle;
                        v.refused_pending = Some(pending.len());
                    });
                }
                Ok(glue::DeleteOutcome::Reloaded) => {}
                Err(err) => {
                    view.update(|v| v.logout = LogoutState::Error(err.to_string()));
                }
            }
        });
    }

    fn delete_confirmed(&self) {
        let v = self.view.read_untracked();
        if !matches!(v.logout, LogoutState::ConfirmDelete { .. }) && v.refused_pending.is_none() {
            return;
        }
        drop(v);
        let view = self.view;
        view.update(|v| {
            v.logout = LogoutState::Working;
            v.refused_pending = None;
            v.team = None;
        });
        spawn_local(async move {
            if let Err(err) = glue::logout_delete_confirmed().await {
                view.update(|v| v.logout = LogoutState::Error(err.to_string()));
            }
        });
    }

    fn add_order(&self) {
        let v = self.view.read_untracked();
        if v.session != Session::SignedIn || logging_out(&v) {
            return;
        }
        let identity = v.identity.clone().unwrap_or_default();
        drop(v);
        if identity.is_empty() {
            return;
        }
        let Some(client) = self.client() else {
            return;
        };
        spawn_local(async move {
            let quantity = support::fresh_quantity();
            if let Err(err) = support::add_order(&client, &identity, quantity).await {
                tracing::error!(error = %err, "order insert failed");
            }
        });
    }

    /// Remove the order row `id` names, ignoring one that names no row id.
    fn remove_order(&self, id: &str) {
        if !self.page_ok() {
            return;
        }
        let Ok(id) = id.parse::<Uuid>() else {
            return;
        };
        let Some(client) = self.client() else {
            return;
        };
        spawn_local(async move {
            if let Err(err) = support::remove_newest_order(&client, id).await {
                tracing::error!(error = %err, "order remove failed");
            }
        });
    }

    fn save_note(&self, body: String) {
        if body.is_empty() || !self.page_ok() {
            return;
        }
        let Some(client) = self.client() else {
            return;
        };
        let view = self.view;
        spawn_local(async move {
            match support::save_note(&client, &body).await {
                Ok(_) => view.update(|v| v.notes_saved += 1),
                Err(err) => tracing::error!(error = %err, "note save failed"),
            }
        });
    }

    /// Write one note into `team`, answering with the pane's outcome line.
    fn add_team_note(&self, team: String, body: String) {
        let v = self.view.read_untracked();
        if v.session != Session::SignedIn || logging_out(&v) {
            return;
        }
        let author = v.identity.clone().unwrap_or_default();
        drop(v);
        if author.is_empty() || team.is_empty() {
            return;
        }
        let Some(client) = self.client() else {
            return;
        };
        let view = self.view;
        spawn_local(async move {
            let body = body.trim();
            if body.is_empty() {
                view.update(|v| {
                    if let Some(panel) = v.team.as_mut() {
                        panel.outcome = "the note needs some text".to_owned();
                    }
                });
                return;
            }
            match support::add_team_note(&client, &team, &author, body).await {
                Ok(_) => view.update(|v| {
                    if let Some(panel) = v.team.as_mut() {
                        panel.outcome = "note added".to_owned();
                    }
                }),
                Err(err) => view.update(|v| {
                    if let Some(panel) = v.team.as_mut() {
                        panel.outcome = format!("note add failed: {err}");
                    }
                }),
            }
        });
    }

    /// Stage one picked image file through the content lane, inserting the
    /// order and photo rows with the staged bytes.
    fn stage_photo(&self, name: String, bytes: Vec<u8>) {
        let v = self.view.read_untracked();
        if v.session != Session::SignedIn || logging_out(&v) {
            return;
        }
        let identity = v.identity.clone().unwrap_or_default();
        drop(v);
        if identity.is_empty() {
            return;
        }
        let Some(mime) = support::mime_from_extension(Path::new(&name)) else {
            self.view.update(|v| {
                v.stage_line = Some("not an image: only .jpg .jpeg .png are accepted".to_owned());
            });
            return;
        };
        let Some(content) = self.content() else {
            return;
        };
        let Some(client) = self.client() else {
            return;
        };
        let blob = blob_of(&bytes, Some(mime_type(mime)));
        let view = self.view;
        spawn_local(async move {
            let line = stage_picked(&content, &client, &blob, mime, &identity).await;
            view.update(|v| v.stage_line = Some(line));
            // The web lane has no content events, so the staged file's
            // refusal or loss only shows up on a re-read.
            reseed_refused_lost(&content, &view).await;
        });
    }

    /// Show the content lane's line from one of its operations.
    fn content_line(&self, make: impl Future<Output = String> + 'static) {
        let view = self.view;
        spawn_local(async move {
            let line = make.await;
            view.update(|v| v.content_line = Some(line));
        });
    }

    fn pin(&self) {
        if !self.content_ok() {
            return;
        }
        let Some(content) = self.content() else {
            return;
        };
        self.content_line(async move {
            content
                .pin_content("photos", support::PIN_PHOTOS_QUERY, "content_id")
                .await
                .map_or_else(support::pin_failed, |_| support::PIN_OK.to_owned())
        });
    }

    fn unpin(&self) {
        if !self.content_ok() {
            return;
        }
        let Some(content) = self.content() else {
            return;
        };
        self.content_line(async move {
            content
                .unpin_content("photos")
                .await
                .map_or_else(support::unpin_failed, |_| support::UNPIN_OK.to_owned())
        });
    }

    fn fetch_pinned(&self) {
        if !self.content_ok() {
            return;
        }
        let Some(content) = self.content() else {
            return;
        };
        let view = self.view;
        spawn_local(async move {
            let line = content
                .fetch_pinned()
                .await
                .map_or_else(support::fetch_failed, |ids| support::fetch_ok(ids.len()));
            view.update(|v| {
                v.pinned_fetches += 1;
                v.content_line = Some(line);
            });
        });
    }

    fn tidy_content(&self) {
        if !self.content_ok() {
            return;
        }
        let Some(content) = self.content() else {
            return;
        };
        self.content_line(async move {
            content
                .tidy()
                .await
                .map_or_else(support::tidy_failed, support::tidy_ok)
        });
    }

    /// Clear one refusal so the next upload walk attempts the file again.
    fn retry_upload(&self, file_id: String) {
        if !self.content_ok() {
            return;
        }
        let Some(id) = file_id_from_hex(&file_id) else {
            return;
        };
        let Some(content) = self.content() else {
            return;
        };
        let view = self.view;
        spawn_local(async move {
            match content.retry_refused(id).await {
                Ok(()) => {
                    view.update(|v| {
                        v.refused_uploads.retain(|(hex, _)| *hex != file_id);
                    });
                }
                Err(err) => {
                    tracing::error!(error = %err, "retry refused upload failed");
                }
            }
        });
    }

    /// Acknowledge every lost file.
    fn acknowledge_lost(&self) {
        let v = self.view.read_untracked();
        if v.session != Session::SignedIn || logging_out(&v) || !v.attached {
            return;
        }
        let ids: Vec<FileId> = v
            .lost_bytes
            .iter()
            .filter_map(|hex| file_id_from_hex(hex))
            .collect();
        drop(v);
        if ids.is_empty() {
            return;
        }
        let Some(content) = self.content() else {
            return;
        };
        let view = self.view;
        spawn_local(async move {
            match content.forget_retired_content(&ids).await {
                Ok(()) => {
                    view.update(|v| v.lost_bytes.clear());
                }
                Err(err) => {
                    tracing::error!(error = %err, "forget lost bytes failed");
                }
            }
        });
    }

    /// Run the replica's trim pass and refresh the footprint readout.
    fn free_up_space(&self) {
        if !self.page_ok() {
            return;
        }
        let Some(client) = self.client() else {
            return;
        };
        let view = self.view;
        spawn_local(async move {
            if let Err(err) = client.tidy().await {
                tracing::error!(error = %err, "free up space failed");
            }
            let (pages, free) = support::replica_footprint(&client).await;
            view.update(|v| {
                v.pages = pages;
                v.free_pages = free;
            });
        });
    }

    /// Read the replica mirror's footprint and the storage line again.
    fn reread_storage(&self) {
        let Some(client) = self.client() else {
            return;
        };
        let view = self.view;
        spawn_local(async move {
            let (pages, free) = support::replica_footprint(&client).await;
            let storage = support::storage_line(&client).await;
            view.update(|v| {
                v.pages = pages;
                v.free_pages = free;
                v.storage = storage;
            });
        });
    }

    /// Export the archive, through the save picker where the browser has it
    /// and a download link otherwise. A dismissed picker leaves the view
    /// unchanged.
    fn export(&self) {
        if !self.page_ok() {
            return;
        }
        let view = self.view;
        spawn_local(async move {
            if let Some(line) = export_save_as().await {
                view.update(|v| v.export_line = Some(line));
            }
        });
    }

    /// Restore one archive through the worker.
    fn import(&self, bytes: Vec<u8>) {
        if !self.page_ok() {
            return;
        }
        let blob = blob_of(&bytes, Some("application/zip"));
        let view = self.view;
        spawn_local(async move {
            let line = glue::import_data(&blob).await;
            view.update(|v| v.import_line = Some(line));
        });
    }
}

/// Stage one picked file as a photo, answering with the line the pane shows.
async fn stage_picked(
    content: &Rc<TabContent<web_sys::BroadcastChannel>>,
    client: &Client,
    blob: &web_sys::Blob,
    mime: MimeClass,
    identity: &str,
) -> String {
    match support::web::stage_photo(content, client, blob, mime, identity, None).await {
        Ok(_) => {
            // Push the staged mutation up now, as the browser builds always have.
            client.replay_pending().await.ok();
            "photo staged".to_owned()
        }
        Err(err) => format!("stage failed: {err}"),
    }
}

/// Whether a JS error is a picker dismissal.
fn is_abort(err: &JsValue) -> bool {
    js_sys::Reflect::get(err, &JsValue::from_str("name"))
        .ok()
        .and_then(|value| value.as_string())
        .is_some_and(|name| name == "AbortError")
}

/// The archive as a download at an object URL, the save picker's fallback.
fn download_export(blob: &web_sys::Blob, size: f64) -> String {
    match glue::download_blob(glue::EXPORT_FILE_NAME, blob) {
        Ok(()) => glue::export_offered_line(size),
        Err(err) => glue::download_refused_line(&err),
    }
}

/// The export through the save picker, `None` when the user dismissed it and
/// the view stays unchanged. The picker opens first, while the click's user
/// activation still holds, and the archive is read from the worker after.
async fn export_save_as() -> Option<String> {
    let picked = match web_sys::window().map(|window| {
        let options = SaveFilePickerOptions::new();
        options.set_suggested_name(Some(glue::EXPORT_FILE_NAME));
        window.show_save_file_picker_with_options(&options)
    }) {
        Some(Ok(promise)) => match JsFuture::from(promise).await {
            Ok(handle) => Some(handle.unchecked_into::<FileSystemFileHandle>()),
            Err(err) if is_abort(&err) => return None,
            Err(_) => None,
        },
        Some(Err(_)) | None => None,
    };
    let blob = match glue::export_archive().await {
        Ok(blob) => blob,
        Err(err) => return Some(err.to_string()),
    };
    let size = blob.size();
    let Some(handle) = picked else {
        return Some(download_export(&blob, size));
    };
    match write_file(&handle, &blob).await {
        Ok(()) => Some(glue::export_saved_line(size)),
        Err(err) => Some(glue::download_refused_line(&err)),
    }
}

/// Write the whole blob to the file the save picker returned.
async fn write_file(handle: &FileSystemFileHandle, blob: &web_sys::Blob) -> Result<(), JsValue> {
    let stream: FileSystemWritableFileStream = JsFuture::from(handle.create_writable())
        .await?
        .unchecked_into();
    JsFuture::from(stream.write_with_blob(blob)?).await?;
    JsFuture::from(stream.close()).await?;
    Ok(())
}
