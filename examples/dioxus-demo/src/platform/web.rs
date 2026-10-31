//! The browser platform: the dedicated DB worker and its boot, the tab
//! attachment behind the leader election, the popup login, and the app's
//! operations. The shared browser pieces ride on `connetto-demo-support`.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use connetto_client::{AccountChoice, Auth, ClientEvent, ConnettoClient, Custody, NoGate};
use connetto_demo_support as support;
use connetto_demo_support::{LogoutState, web as glue};
use connetto_web::builder::{AttachedTab, TabTopology};
use connetto_web::leader::Membership;
use connetto_web::unlock::serve_account_choice;
use connetto_web::{MessageTransport, TabContent, deliver_login_code, workers};
use dioxus::prelude::*;
use wasm_bindgen::JsValue;

use crate::platform::{Client, DemoContent};
use crate::ui::*;

/// Path the dev IdP redirects back to after login.
const AUTH_CALLBACK_PATH: &str = "/auth/callback";

/// Boot the connetto DB tier in the worker context, then broadcast the
/// boot's identity, account key and deadline to the pages so a fresh page
/// can ask the booted worker to post them again.
///
/// # Errors
///
/// A JS string describing the VFS, acquisition, upstream connect, or subscribe
/// failure.
pub async fn run_db_worker() -> Result<(), JsValue> {
    let origin = glue::worker_origin();
    let booted = glue::demo_builder()
        .signed_in(
            Auth::new(support::AUTH_BASE, glue::ask_provider().await)
                .with_account(AccountChoice::Ask),
        )
        .with_redirect_uri(format!("{origin}{AUTH_CALLBACK_PATH}"))
        .with_auth_db_name(glue::AUTH_DB)
        .durable(glue::DB_NAME)
        .boot::<String>()
        .await?;
    glue::broadcast_boot_session(
        booted.identity.as_deref(),
        booted.account.as_deref(),
        booted.session_expires_at,
    );
    Ok(())
}

/// The served URL of this app's wasm-bindgen glue module, the module script
/// the page itself loads. `dx serve` names it `/wasm/<name>.js`, and a release
/// build hashes it apart from the wasm's own hash, so neither name can be
/// derived from the other, while the page's script tag carries it in both.
/// Only the main thread has the page, which is why the worker receives the
/// glue URL as a spawn parameter.
fn glue_url() -> String {
    js_sys::eval(&format!(
        r#"[...document.querySelectorAll('script[type="module"][src]')].map((s) => s.src).find((s) => s.split("/").pop().startsWith("{}"))"#,
        env!("CARGO_PKG_NAME")
    ))
    .ok()
    .and_then(|value| value.as_string())
    .expect("the page loads the app's glue module")
}

/// The window's live connection to the DB worker, plus the topology tokens it
/// must keep alive: leadership (this window may own the worker) and the tab
/// liveness lock (dropped on unmount, so the worker reaps this tab).
pub struct Boot {
    pub client: ConnettoClient<MessageTransport<web_sys::BroadcastChannel>>,
    /// The tab's content lane, split off the transport before the client took it.
    pub content: Rc<TabContent<web_sys::BroadcastChannel>>,
    /// Shared rather than owned, because the gate ceremony is awaited and the
    /// handle has to outlive the borrow that reaches it.
    pub membership: Rc<Membership>,
    _tab: AttachedTab,
}

/// Join the topology and attach this window's tab client.
///
/// The winner of the leader election spawns the worker from the dx glue,
/// which auto-initializes on import and runs `main`, so no separate bootstrap
/// is needed.
async fn boot_window() -> Result<Boot, JsValue> {
    let glue = glue_url();
    let tab = glue::demo_builder()
        .attach(TabTopology {
            leader_lock: glue::LEADER_LOCK,
            glue_url: &glue,
            bootstrap: workers::WorkerBootstrap::Glue,
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

/// If the page URL carries `?code=...&state=...`, return the pair.
pub fn page_login_callback() -> Option<(String, String)> {
    // Returns [code, state] or null via a self-executing function.
    let result = js_sys::eval(
        "(function(){\
            var p=new URLSearchParams(location.search);\
            var c=p.get('code'),s=p.get('state');\
            return c&&s?[c,s]:null;\
        })()",
    )
    .ok()?;
    if result.is_null() || result.is_undefined() {
        return None;
    }
    let arr = js_sys::Array::from(&result);
    let code = arr.get(0).as_string()?;
    let state = arr.get(1).as_string()?;
    Some((code, state))
}

/// True when this page was opened by another page on this origin, which is how
/// a login popup is distinguished from the application itself.
pub fn is_login_popup() -> bool {
    js_sys::eval("window.opener !== null && window.opener !== undefined")
        .ok()
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

/// Strip the query string from the current URL without a page reload so a
/// spent authorization code is not replayed on refresh.
pub fn clear_url_query() {
    let _ = js_sys::eval("history.replaceState(null,'',location.pathname)");
}

/// Open `url` in a popup to run the login there.
///
/// Navigating this tab instead would destroy the page, and with it the
/// dedicated DB worker that asked for the login and holds the PKCE verifier.
/// A popup leaves the opener and its worker alive.
pub fn open_login_popup(url: &str) {
    // Encode the URL as a JSON string literal so it is safe regardless of
    // what characters the IdP URL carries.
    let encoded = js_sys::JSON::stringify(&JsValue::from_str(url))
        .ok()
        .and_then(|s| s.as_string())
        .unwrap_or_else(|| format!("\"{}\"", url.replace('"', "%22")));
    let _ = js_sys::eval(&format!(
        "window.open({encoded},'connetto-login','popup=yes,width=520,height=640')"
    ));
}

/// Ask the worker for the expiry warning, skipping the update if a newer
/// session arrived meanwhile.
async fn poll_expiry_warn(
    secs: u64,
    session_expires_at: Signal<Option<u64>>,
    mut expiry_warn: Signal<Option<String>>,
) {
    let warn = glue::expiry_warn_for_session(secs).await;
    if *session_expires_at.read() != Some(secs) {
        return;
    }
    expiry_warn.set(warn);
}

/// Export this device's local data and offer the archive as a download.
pub async fn export_data() -> String {
    glue::export_data().await
}

/// Restore one archive, keeping the file's version of every clash.
pub async fn import_data(blob: &web_sys::Blob) -> String {
    glue::import_data(blob).await
}

/// The platform's one app-level operation, the header's sign-in and unlock
/// affordances and the account bar's switch, add, enrol and logout buttons,
/// each reaching the worker through it.
#[derive(Clone)]
pub struct Ops {
    user_id: Signal<Option<String>>,
    live_account: Signal<Option<String>>,
    enrol_offerable: Signal<bool>,
    custody_snap: Signal<Option<Custody>>,
    enrol_msg: Signal<Option<String>>,
    logout_state: Signal<LogoutState>,
    /// The pending count a refused delete held, offered to the force delete.
    refused_pending: Signal<Option<u64>>,
    login_prompt: Signal<Option<String>>,
    sign_in_ready: Signal<bool>,
    held: Rc<RefCell<Option<Boot>>>,
}

impl Ops {
    /// The boot this window attached through, or `None` while it lands.
    fn boot(&self) -> Option<Rc<Membership>> {
        let held = self.held.borrow();
        held.as_ref().map(|boot| Rc::clone(&boot.membership))
    }

    /// The logout standing the account bar renders.
    pub fn logout_state(&self) -> Signal<LogoutState> {
        self.logout_state
    }

    /// The pending count a refused delete held, offered to the force delete.
    pub fn refused_pending(&self) -> Signal<Option<u64>> {
        self.refused_pending
    }

    /// Log out, keeping the local data. The worker drops the credential, and
    /// the replica and its key stay, so a returning user resumes from the
    /// persisted cursor.
    pub async fn logout_keep(&self) {
        let mut state = self.logout_state;
        state.set(LogoutState::Working);
        if let Err(err) = glue::logout_keep().await {
            state.set(LogoutState::Error(err.to_string()));
        }
    }

    /// Delete the local data and log out, asking for confirmation while
    /// local work is still unsent.
    pub async fn logout_delete(&self) {
        let mut state = self.logout_state;
        let mut refused = self.refused_pending;
        state.set(LogoutState::Working);
        match glue::logout_delete().await {
            Ok(glue::DeleteOutcome::Confirm(pending)) => state.set(LogoutState::ConfirmDelete {
                unsynced_count: pending.len(),
            }),
            Ok(glue::DeleteOutcome::Refused(pending)) => {
                state.set(LogoutState::Idle);
                refused.set(Some(pending.len()));
            }
            Ok(glue::DeleteOutcome::Reloaded) => {}
            Err(err) => state.set(LogoutState::Error(err.to_string())),
        }
    }

    /// The confirmed or forced delete, discarding the unsent work.
    pub async fn logout_delete_confirmed(&self) {
        let mut state = self.logout_state;
        let mut refused = self.refused_pending;
        state.set(LogoutState::Working);
        refused.set(None);
        if let Err(err) = glue::logout_delete_confirmed().await {
            state.set(LogoutState::Error(err.to_string()));
        }
    }

    /// Drop the standing logout confirmation, error or refusal.
    pub async fn dismiss(&self) {
        let mut state = self.logout_state;
        let mut refused = self.refused_pending;
        state.set(LogoutState::Idle);
        refused.set(None);
    }

    /// Sign the page back in as another stored account, through the worker.
    /// The page clears its session fields while the replacement worker
    /// re-acquires them and broadcasts the new ones.
    pub async fn switch_account(&self, key: &str) {
        let mut user_id = self.user_id;
        let mut live_account = self.live_account;
        user_id.set(None);
        live_account.set(None);
        let Some(membership) = self.boot() else {
            return;
        };
        if let Err(err) = membership.switch_account(key) {
            tracing::warn!(error = %err, "asking the worker to switch accounts");
        }
    }

    /// Ask the worker to open a fresh sign-in for a new account.
    pub async fn add_account(&self) {
        let Some(membership) = self.boot() else {
            return;
        };
        if let Err(err) = membership.add_account() {
            tracing::warn!(error = %err, "asking the worker to add an account");
        }
    }

    /// Run the passkey gate enrolment ceremony in this window. The ceremony
    /// runs for as long as the user takes to present a finger, so the
    /// membership handle is cloned out and the borrow released before the
    /// await.
    pub async fn enrol_gate(&self) {
        let mut enrol_offerable = self.enrol_offerable;
        let mut custody_snap = self.custody_snap;
        let mut enrol_msg = self.enrol_msg;
        let Some(membership) = self.boot() else {
            return;
        };
        match membership.enrol_gate().await {
            Ok(true) => {
                if let Ok(level) = workers::request_custody().await {
                    enrol_offerable.set(matches!(level, Custody::Unverified(NoGate::Offerable)));
                    custody_snap.set(Some(level));
                }
            }
            Ok(false) => {
                enrol_msg.set(Some(
                    "the passkey gate can only be set up in the window that owns the database"
                        .to_owned(),
                ));
            }
            Err(err) => enrol_msg.set(Some(format!("enrolment failed: {err}"))),
        }
    }

    /// The header's unlock affordance is never offered here, because the
    /// stored-secrets gate is the worker's to hold and the worker's unlock
    /// prompt is the browser's own ceremony.
    pub fn offers_unlock(&self) -> bool {
        false
    }

    /// The gate unlock, which the header never reaches here, kept for the
    /// platform interface the shared page calls through.
    pub async fn unlock(&self) {}

    /// Open the parked login URL in a popup, or no-op when none is parked yet.
    pub async fn sign_in(&self) {
        let mut login_prompt = self.login_prompt;
        let Some(url) = login_prompt.read().clone() else {
            return;
        };
        open_login_popup(&url);
        login_prompt.set(None);
        let mut sign_in_ready = self.sign_in_ready;
        sign_in_ready.set(false);
    }
}

/// The browser app: boots the tab, wires the channels and provides the
/// contexts every pane reads.
#[component]
pub fn App() -> Element {
    // Read ?code=...&state=... from the URL once, then clear it so a reload
    // does not attempt to redeem a spent authorization code.
    let pending = page_login_callback();
    if pending.is_some() {
        clear_url_query();
    }

    let mut client_slot: Signal<Option<Client>> = use_signal(|| None);
    let mut status: Signal<String> = use_signal(|| "connecting to the connetto stack".to_owned());
    let mut user_id: Signal<Option<String>> = use_signal(|| None);
    let mut login_prompt: Signal<Option<String>> = use_signal(|| None);
    let mut sign_in_ready: Signal<bool> = use_signal(|| false);
    let mut live_account: Signal<Option<String>> = use_signal(|| None);
    let mut stored_account: Signal<Option<String>> = use_signal(glue::read_stored_account);
    let mut session_expires_at: Signal<Option<u64>> = use_signal(|| None);
    let all_accounts: Signal<Vec<String>> = use_signal(Vec::new);
    let accounts: Signal<Vec<(String, String, bool)>> = use_signal(Vec::new);
    let picker_active: Signal<bool> = use_signal(|| false);
    let account_answer: Signal<Option<AccountChoice>> = use_signal(|| None);
    let mut content_slot: Signal<Option<DemoContent>> = use_signal(|| None);
    let expiry_warn: Signal<Option<String>> = use_signal(|| None);
    let mut footprint: Signal<(i64, i64)> = use_signal(|| (0_i64, 0_i64));
    let mut enrol_offerable: Signal<bool> = use_signal(|| false);
    let mut custody_line: Signal<Option<String>> = use_signal(|| None);
    let enrol_msg: Signal<Option<String>> = use_signal(|| None);
    let mut gate_line: Signal<Option<String>> = use_signal(|| None);
    let mut custody_snap: Signal<Option<Custody>> = use_signal(|| None);
    let device_line: Signal<String> = use_signal(|| "no device identity in this build".to_owned());
    let mut storage_line: Signal<String> = use_signal(|| support::STORAGE_UNREAD.to_owned());
    let logout_state: Signal<LogoutState> = use_signal(|| LogoutState::Idle);
    let refused_pending: Signal<Option<u64>> = use_signal(|| None);
    let mut gate: Signal<&'static str> = use_signal(|| "open");
    let clock: Signal<Option<&'static str>> = use_signal(|| None);

    use_context_provider(|| Status(status));
    use_context_provider(|| UserId(user_id));
    use_context_provider(|| AccountKey(live_account));
    use_context_provider(|| Accounts(accounts));
    use_context_provider(|| PickerActive(picker_active));
    use_context_provider(|| AccountAnswer(account_answer));
    use_context_provider(|| CustodyLine(custody_line));
    use_context_provider(|| GateLine(gate_line));
    use_context_provider(|| DeviceLine(device_line));
    use_context_provider(|| StorageLine(storage_line));
    use_context_provider(|| ExpiryWarn(expiry_warn));
    use_context_provider(|| ClientSlot(client_slot));
    use_context_provider(|| ContentSlot(content_slot));
    use_context_provider(|| Footprint(footprint));
    use_context_provider(|| EnrolAvailable(enrol_offerable));
    use_context_provider(|| EnrolMsg(enrol_msg));
    use_context_provider(|| Gate(gate));
    use_context_provider(|| Clock(clock));
    use_context_provider(|| SignInReady(sign_in_ready));

    // The header's gate and custody lines stand on the custody snapshot and,
    // where a gate exists, the worker's reported unlock state.
    use_effect(move || {
        let Some(custody) = *custody_snap.read() else {
            return;
        };
        let state = *gate.read();
        gate_line.set(Some(format!("gate: {}", glue::gate_text(custody, state))));
        custody_line.set(Some(format!("custody: {}", glue::custody_text(custody))));
    });

    // The account bar's list of every stored account as key, decoded label
    // and signed in right now, refreshed as the worker's list or the live
    // account moves.
    {
        let mut accounts_signal = accounts;
        use_effect(move || {
            let known = all_accounts.read().clone();
            let current = live_account.read().clone();
            accounts_signal.set(
                known
                    .iter()
                    .map(|key| {
                        (
                            key.clone(),
                            support::account_label(key),
                            current.as_deref() == Some(key.as_str()),
                        )
                    })
                    .collect(),
            );
        });
    }

    // Register the account chooser before the worker boots. For a single
    // stored credential the chooser returns immediately; for several it parks
    // the list in the picker and polls for the button click.
    use_hook(|| {
        serve_account_choice(move |accounts| {
            let mut known = all_accounts;
            let picker = picker_active;
            let answer = account_answer;
            async move {
                *known.write() = accounts.clone();
                if accounts.len() <= 1 {
                    return AccountChoice::LastUsed;
                }
                glue::choose_account(
                    &accounts,
                    move || *picker.write_unchecked() = true,
                    move || {
                        let choice = answer.read().clone()?;
                        *answer.write_unchecked() = None;
                        *picker.write_unchecked() = false;
                        Some(match choice {
                            AccountChoice::Account(key) => glue::AccountAnswer::Stored(key),
                            AccountChoice::New => glue::AccountAnswer::New,
                            _ => return None,
                        })
                    },
                )
                .await
            }
        });
    });

    // Answer the worker's provider query at each boot, always with the one
    // provider the dev stack registers.
    let _provider_listener = use_hook(|| Rc::new(glue::ProviderListener::new()));

    // Receive the session fields the worker broadcasts after its boot, the
    // identity, the account key remembered across reloads, and the deadline.
    let _uid_listener = use_hook(move || {
        Rc::new(glue::BootSessionListener::new(
            move |uid, account, expires_at| {
                if let Some(uid) = uid {
                    user_id.set(Some(uid));
                }
                match account {
                    Some(account) => {
                        glue::save_stored_account(&account);
                        stored_account.set(Some(account.clone()));
                        live_account.set(Some(account));
                    }
                    None => live_account.set(None),
                }
                session_expires_at.set(expires_at);
            },
        ))
    });

    // Receive the worker's login URL. A page load that is the OAuth callback
    // delivers the code and state to the worker. Any other page parks the URL
    // so a button can offer it, since a popup opened from this handler would
    // be blocked.
    let _login_listener = use_hook(move || {
        Rc::new(glue::LoginUrlListener::new(move |url| {
            if let Some((code, state)) = &pending {
                // This page is the callback, so it delivers the code to the worker.
                let _ = deliver_login_code(code, state);
            } else {
                login_prompt.set(Some(url));
                sign_in_ready.set(true);
            }
        }))
    });

    let raw_boot_hold: Rc<RefCell<Option<Boot>>> = use_hook(|| Rc::new(RefCell::new(None)));
    let ops = Ops {
        user_id,
        live_account,
        enrol_offerable,
        custody_snap,
        enrol_msg,
        logout_state,
        refused_pending,
        login_prompt,
        sign_in_ready,
        held: raw_boot_hold.clone(),
    };
    use_context_provider(|| ops);

    use_hook(move || {
        let boot_inner = raw_boot_hold.clone();
        spawn(async move {
            match boot_window().await {
                Ok(boot) => {
                    // Fetch the custody standing now that the worker has settled.
                    if let Ok(level) = workers::request_custody().await {
                        enrol_offerable
                            .set(matches!(level, Custody::Unverified(NoGate::Offerable)));
                        custody_snap.set(Some(level));
                    }
                    content_slot.set(Some(DemoContent::new(Rc::clone(&boot.content))));
                    let mut events = boot.client.events();
                    client_slot.set(Some(boot.client.clone()));
                    status.set("connected".to_owned());
                    storage_line.set(support::storage_line(&boot.client).await);
                    *boot_inner.borrow_mut() = Some(boot);
                    while let Ok(event) = events.recv().await {
                        // The worker states its gate to every tab as the
                        // away-and-return re-check locks and unlocks it.
                        match event {
                            ClientEvent::Locked | ClientEvent::UnlockDismissed => {
                                gate.set("locked")
                            }
                            ClientEvent::Unlocked => gate.set("open"),
                            _ => {}
                        }
                        if let Some(label) = support::status_label(&event) {
                            status.set(label);
                        }
                        // The stop names its cause, and the `Closed` that
                        // follows it would only replace that.
                        if matches!(event, ClientEvent::Stopped { .. }) {
                            break;
                        }
                    }
                }
                Err(err) => status.set(format!("boot failed: {err:?}")),
            }
        });
    });

    // The session expiry warning and the replica footprint, polled on a slow
    // pace while the session is known.
    {
        use_hook(move || {
            spawn(async move {
                loop {
                    workers::sleep(Duration::from_secs(5)).await;
                    if let Some(secs) = *session_expires_at.read() {
                        poll_expiry_warn(secs, session_expires_at, expiry_warn).await;
                    }
                    if let Some(client) = client_slot.read().clone() {
                        footprint.set(support::replica_footprint(&client).await);
                        storage_line.set(support::storage_line(&client).await);
                    }
                }
            })
        });
    }

    let ready = client_slot.read().is_some();
    let restoring = stored_account.read().is_some();
    rsx! {
        Page { ready: ready, restoring: restoring }
    }
}
