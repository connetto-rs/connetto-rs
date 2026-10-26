//! The native platform: the `NativeClientBuilder` setup, the loopback and
//! phone sign-in, the keyring, the data directory, the file export and
//! import, the away input and the session lifecycle the shell drives.

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use connetto_client::teardown::{ForgetError, expiry_warning};
use connetto_client::{
    AccountChoice, Auth, ClientEvent, ConnettoClient, Custody, KeyringAuth, NativeAuthenticator,
    NativeClient, NoGate,
};
use connetto_core::traits::RefreshTokenStore;
use connetto_demo_support as support;
use connetto_demo_support::LogoutState;
use connetto_dioxus::{use_away_input, use_live};
use connetto_file_client::{ContentClient, ContentHandle, FsStore, ReqwestHttp};
use dioxus::prelude::*;

use crate::platform::{Client, DemoContent};
use crate::ui::*;

const DEFAULT_WS: &str = "ws://127.0.0.1:7777/sync";
const DEFAULT_AUTH_ORIGIN: &str = "http://127.0.0.1:7777";
const KEYRING_SERVICE: &str = "connetto-dioxus-demo";
/// The login redirect on a phone, whose scheme is the app's bundle identifier
/// (`Dioxus.toml`), the scheme the bundled redirect activity claims on Android
/// and the authentication session catches on iOS.
#[cfg(any(target_os = "android", target_os = "ios"))]
const APP_REDIRECT: &str = "dev.connetto.dioxusdemo:/oauth2redirect";

/// The deployment root this build ships, which every device certificate and
/// revocation list must chain to (R74 decisions 3 and 30).
#[cfg(feature = "device-identity")]
const DEVICE_ROOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/device-root.der"));

/// The default log filter. The away and return inputs and the gate's
/// transitions are on, so a proof that fails at a re-check carries them.
pub const LOG_FILTER: &str = "info,connetto_client::away=debug,connetto_dioxus::away=debug";

type Ws = connetto_client::NativeTransport;
type Content = Arc<ContentClient<Ws, FsStore, ReqwestHttp>>;

/// On a phone, sign in through the platform's in-app browser tab and the app's
/// own redirect (RFC 8252 section 7.1), the tab opening on the Sign in click.
#[cfg(target_os = "ios")]
fn platform_sign_in(
    auth: KeyringAuth,
    parked: tokio::sync::watch::Sender<Option<String>>,
    parked_rx: tokio::sync::watch::Receiver<Option<String>>,
) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(support::native::TabSession::new(parked, parked_rx)),
    )
}

/// On Android, also unlock the Keystore-gated secrets through the platform's
/// biometric or device-credential prompt, which `connetto-auth-session` hosts.
#[cfg(target_os = "android")]
fn platform_sign_in(
    auth: KeyringAuth,
    parked: tokio::sync::watch::Sender<Option<String>>,
    parked_rx: tokio::sync::watch::Receiver<Option<String>>,
) -> KeyringAuth {
    auth.with_claimed_redirect(
        APP_REDIRECT,
        Arc::new(support::native::TabSession::new(parked, parked_rx)),
    )
    .with_keystore_prompt(Arc::new(support::native::KeystoreUnlock))
}

/// On a desktop, sign in through the system browser and a loopback listener
/// (RFC 8252 section 7.3), so the user's own browser serves the login with its
/// sessions and password manager.
#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn platform_sign_in(auth: KeyringAuth) -> KeyringAuth {
    auth
}

/// The app's private data directory on a phone, since Android sets neither
/// `HOME` nor `XDG_DATA_HOME` and the iOS sandbox refuses `HOME/.local`. A
/// failed lookup is logged and leaves the temp directory, where the first
/// write then fails with an error setup reports.
#[cfg(any(target_os = "android", target_os = "ios"))]
fn app_data_root() -> PathBuf {
    robius_directories::ProjectDirs::from("", "", "connetto-dioxus-demo").map_or_else(
        || {
            tracing::error!("the app's files directory is unavailable");
            std::env::temp_dir()
        },
        |dirs| dirs.data_dir().to_path_buf(),
    )
}

/// The data directory on the desktop, under `XDG_DATA_HOME` or the home.
#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn app_data_root() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        PathBuf::from(xdg)
    } else if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join(".local").join("share")
    } else {
        std::env::temp_dir()
    }
}

fn data_dir() -> PathBuf {
    app_data_root().join("connetto-dioxus-demo")
}

/// The log a phone build writes beside stdout, in the app's data directory,
/// since a phone keeps no app's stdout and a device proof copies this file
/// off the phone.
#[cfg(any(target_os = "android", target_os = "ios"))]
const PHONE_LOG: &str = "connetto-demo.log";

/// Log to stdout and to [`PHONE_LOG`], or to stdout alone where the file
/// cannot be opened. `RUST_LOG` overrides the default filter.
#[cfg(any(target_os = "android", target_os = "ios"))]
pub fn init_phone_logging() {
    use tracing_subscriber::fmt::writer::MakeWriterExt as _;

    let directives = std::env::var("RUST_LOG").unwrap_or_else(|_| LOG_FILTER.to_owned());
    let root = app_data_root();
    let file = std::fs::create_dir_all(&root).and_then(|()| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(PHONE_LOG))
    });
    match file {
        Ok(file) => connetto_core::logging::install(
            std::io::stdout.and(std::sync::Mutex::new(file)),
            &directives,
        ),
        Err(_) => connetto_core::logging::install(std::io::stdout, &directives),
    }
}

/// Write this device's local data to the export file, answering with the line
/// to show.
fn export_path() -> PathBuf {
    data_dir().join("connetto-local-data.zip")
}

fn create_export_file() -> std::io::Result<(PathBuf, std::fs::File)> {
    let path = export_path().with_extension("zip.part");
    std::fs::create_dir_all(data_dir())?;
    let file = std::fs::File::create(&path)?;
    Ok((path, file))
}

fn publish_export(part: &Path) -> std::io::Result<PathBuf> {
    let path = export_path();
    std::fs::rename(part, &path)?;
    Ok(path)
}

/// Write this device's local data to the export file, answering with the
/// line the pane shows.
pub async fn export_data(content: &DemoContent) -> String {
    let (part, file) = match create_export_file() {
        Ok(opened) => opened,
        Err(err) => return format!("could not open the export file: {err}"),
    };
    let file = match content
        .client()
        .export_local_data(connetto_client::ExportScope::Everything, file)
        .await
    {
        Ok(file) => file,
        Err(err) => return format!("export failed: {err}"),
    };
    let written = file.metadata().map(|meta| meta.len());
    drop(file);
    match (publish_export(&part), written) {
        (Ok(path), Ok(bytes)) => format!("Wrote {bytes} bytes to {}", path.display()),
        (Ok(path), Err(err)) => format!("wrote {} but could not measure it: {err}", path.display()),
        (Err(err), _) => format!("could not replace the last export: {err}"),
    }
}

/// Restore one archive, keeping the file's version of every clash, and answer
/// with the line the pane shows.
pub async fn import_data(content: &DemoContent, bytes: Result<Vec<u8>, String>) -> String {
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(err) => return format!("could not read it: {err}"),
    };
    support::native::import_bytes(content.client(), bytes).await
}

/// Everything one signed-in session runs on, and dropping it ends the
/// session. The content outbox holds a client clone, so the client is closed
/// here, which ends the pump and the outbox with it.
pub struct Parts {
    pub(crate) native: NativeClient<Ws, ContentHandle<Ws>>,
    pub(crate) client: ConnettoClient<Ws>,
    authenticator: Option<NativeAuthenticator>,
    content: Content,
    runtime: tokio::runtime::Handle,
}

impl Drop for Parts {
    fn drop(&mut self) {
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        runtime.spawn(async move { client.close().await });
    }
}

/// One session's parts, equal only to itself, so a new session remounts the UI.
#[derive(Clone)]
pub struct SessionParts(pub(crate) Rc<Parts>);

impl PartialEq for SessionParts {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

/// Ends the current session and runs setup again, signing in as the account
/// the last request chose.
#[derive(Clone, Copy)]
pub struct Restart {
    generation: Signal<u64>,
    choice: Signal<AccountChoice>,
}

impl Restart {
    /// Start over as the account last used.
    pub fn request(self) {
        self.request_as(AccountChoice::LastUsed);
    }

    /// Start over as `choice`.
    pub fn request_as(mut self, choice: AccountChoice) {
        self.choice.set(choice);
        *self.generation.write() += 1;
    }
}

/// The native entry: renders the shared page at once and runs the connection
/// behind it, parking the loopback login URL instead of opening a browser.
/// Android calls `main` from the activity's start, so nothing there may wait
/// on the network or on a login.
#[component]
pub fn Shell() -> Element {
    let generation = use_signal(|| 0_u64);
    let choice = use_signal(|| AccountChoice::LastUsed);
    let restart = Restart { generation, choice };
    use_context_provider(|| restart);
    let runtime = use_hook(tokio::runtime::Handle::current);
    // Windows Hello prompts over this window.
    #[cfg(target_os = "windows")]
    let owner = connetto_dioxus::use_hello_owner();
    // The browser holds the front after a login, so the window asks for it back.
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    let window = dioxus::desktop::window();

    // The standing signals the shared page reads, empty until the session lands.
    let parts: Signal<Option<SessionParts>> = use_signal(|| None);
    let status: Signal<String> = use_signal(|| "connecting to the connetto stack".to_owned());
    let user_id: Signal<Option<String>> = use_signal(|| None);
    let account_key: Signal<Option<String>> = use_signal(|| None);
    let accounts: Signal<Vec<(String, String, bool)>> = use_signal(Vec::new);
    let parked: Signal<Option<String>> = use_signal(|| None);
    let sign_in_ready: Signal<bool> = use_signal(|| false);
    let stored_account: Signal<bool> = use_signal(|| false);
    let gate: Signal<&'static str> = use_signal(|| "open");
    let gate_line: Signal<Option<String>> = use_signal(|| None);
    let custody_line: Signal<Option<String>> = use_signal(|| None);
    let device_line: Signal<String> = use_signal(|| "no device identity in this build".to_owned());
    let clock: Signal<Option<&'static str>> = use_signal(|| None);
    let expiry_warn: Signal<Option<String>> = use_signal(|| None);
    let footprint: Signal<(i64, i64)> = use_signal(|| (0_i64, 0_i64));
    let logout_state: Signal<LogoutState> = use_signal(|| LogoutState::Idle);
    let refused_pending: Signal<Option<u64>> = use_signal(|| None);
    let picker_active: Signal<bool> = use_signal(|| false);
    let account_answer: Signal<Option<AccountChoice>> = use_signal(|| None);
    let enrol_offerable: Signal<bool> = use_signal(|| false);
    let enrol_msg: Signal<Option<String>> = use_signal(|| None);
    let client_slot: Signal<Option<Client>> = use_signal(|| None);
    let content_slot: Signal<Option<DemoContent>> = use_signal(|| None);

    // Whether a stored credential restores straight into the skeleton.
    {
        let mut stored = stored_account;
        spawn(async move {
            let store = connetto_client::KeyringStore::new(KEYRING_SERVICE);
            if let Ok(Some(_)) = store.load(connetto_client::IDENTITY_RECORD).await {
                stored.set(true);
            }
        });
    }

    // The loopback login URL the setup parks instead of opening a browser; the
    // Sign in button reads it through this signal.
    let (parked_tx, parked_rx) = use_hook(|| tokio::sync::watch::channel(None::<String>));
    {
        let mut parked_signal = parked;
        let mut rx = parked_rx.clone();
        spawn(async move {
            while rx.changed().await.is_ok() {
                parked_signal.set(rx.borrow_and_update().clone());
            }
        });
    }

    use_context_provider(|| Status(status));
    use_context_provider(|| UserId(user_id));
    use_context_provider(|| AccountKey(account_key));
    use_context_provider(|| Accounts(accounts));
    use_context_provider(|| PickerActive(picker_active));
    use_context_provider(|| AccountAnswer(account_answer));
    use_context_provider(|| CustodyLine(custody_line));
    use_context_provider(|| GateLine(gate_line));
    use_context_provider(|| DeviceLine(device_line));
    use_context_provider(|| ExpiryWarn(expiry_warn));
    use_context_provider(|| Footprint(footprint));
    use_context_provider(|| crate::ui::Gate(gate));
    use_context_provider(|| Clock(clock));
    use_context_provider(|| EnrolAvailable(enrol_offerable));
    use_context_provider(|| EnrolMsg(enrol_msg));
    use_context_provider(|| SignInReady(sign_in_ready));
    use_context_provider(|| ClientSlot(client_slot));
    use_context_provider(|| ContentSlot(content_slot));

    let ops = Ops {
        parts,
        restart,
        logout: logout_state,
        refused_pending,
        picker_active,
        parked,
        status,
        #[cfg(any(target_os = "android", target_os = "ios"))]
        parked_tx: parked_tx.clone(),
    };
    use_context_provider(|| ops.clone());

    // The Sign in button offers the parked URL once the setup has parked one.
    {
        let mut ready = sign_in_ready;
        use_effect(move || {
            let _ = parked.read();
            ready.set(parked.read().is_some());
        });
    }

    // Run the setup as a task behind the page; the parked login stands until
    // the Sign in button opens it, and a restore lands the parts.
    use_effect(move || {
        let _ = generation();
        let account = choice.peek().clone();
        let mut p = parts;
        let mut uid = user_id;
        let mut ak = account_key;
        let mut cs = client_slot;
        let mut cslot = content_slot;
        let mut st = status;
        p.set(None);
        uid.set(None);
        ak.set(None);
        cs.set(None);
        cslot.set(None);
        st.set("connecting to the connetto stack".to_owned());
        let runtime = runtime.clone();
        let parked_tx = parked_tx.clone();
        #[cfg(any(target_os = "android", target_os = "ios"))]
        let parked_rx = parked_rx.clone();
        #[cfg(target_os = "windows")]
        let owner = Arc::clone(&owner);
        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        let window = window.clone();
        spawn(async move {
            let outcome = runtime
                .spawn(setup(
                    account,
                    parked_tx,
                    #[cfg(any(target_os = "android", target_os = "ios"))]
                    parked_rx,
                    #[cfg(target_os = "windows")]
                    owner,
                ))
                .await;
            match outcome {
                Ok(Ok(sp)) => {
                    let sp = SessionParts(Rc::new(sp));
                    cs.set(Some(sp.0.client.clone()));
                    cslot.set(Some(DemoContent::new(Arc::clone(&sp.0.content))));
                    let session = sp.0.native.session();
                    uid.set(session.map(|s| s.user_id().to_owned()));
                    ak.set(session.map(|s| s.account().to_owned()));
                    st.set("connected".to_owned());
                    p.set(Some(sp));
                }
                Ok(Err(err)) => st.set(err),
                Err(err) => st.set(format!("the setup task stopped: {err}")),
            }
            #[cfg(not(any(target_os = "android", target_os = "ios")))]
            window.set_focus();
        });
    });

    let session = *generation.read();
    let ready = parts.read().is_some();
    let restoring = *stored_account.read();
    let peer_panel: Option<Element> = {
        #[cfg(feature = "peer")]
        {
            parts
                .read()
                .clone()
                .map(|p| rsx! { PeerPanel { parts: p } })
        }
        #[cfg(not(feature = "peer"))]
        {
            None
        }
    };
    rsx! {
        if let Some(sp) = parts.read().clone() {
            Session { key: "{session}", parts: sp }
        }
        Page { ready: ready, restoring: restoring, peer: peer_panel }
    }
}

/// The session logic: mounted once the parts land, it wires the client's
/// events and the device standing into the standing signals and stands the
/// account rows. It renders nothing of its own; the shared page is the shell's.
#[component]
fn Session(parts: SessionParts) -> Element {
    let parts = parts.clone();
    let client = parts.0.client.clone();
    let Status(mut status) = use_context::<Status>();
    let UserId(mut user_id) = use_context::<UserId>();
    let AccountKey(mut account_key) = use_context::<AccountKey>();
    let Accounts(mut accounts) = use_context::<Accounts>();
    let Gate(mut gate) = use_context::<crate::ui::Gate>();
    let Clock(mut clock) = use_context::<Clock>();
    let DeviceLine(mut device_line) = use_context::<DeviceLine>();
    let GateLine(mut gate_line) = use_context::<GateLine>();
    let CustodyLine(mut custody_line) = use_context::<CustodyLine>();
    let ExpiryWarn(mut expiry_warn) = use_context::<ExpiryWarn>();
    let Footprint(mut footprint) = use_context::<Footprint>();

    use_away_input(&client);
    let orders_rows = use_live::<_, _, support::Order>(&client, support::orders_newest_first());
    let event_rx = client.events();
    use_hook(move || {
        spawn(async move {
            let mut rx = event_rx;
            while let Ok(event) = rx.recv().await {
                match event {
                    ClientEvent::Locked | ClientEvent::UnlockDismissed => gate.set("locked"),
                    ClientEvent::Unlocked => gate.set("open"),
                    ClientEvent::ClockOutsideWindow { ahead } => clock.set(Some(if ahead {
                        "the clock runs ahead of the server's, so peers will refuse this device"
                    } else {
                        "the clock runs behind the server's, so peers will refuse this device"
                    })),
                    _ => {}
                }
                if let Some(label) = support::status_label(&event) {
                    status.set(label);
                }
                if matches!(event, ClientEvent::Stopped { .. }) {
                    break;
                }
            }
        })
    });
    // The custody standing, read once at mount because native enrolment never
    // flips it, so the gate and custody lines treat it as a constant.
    let custody = parts.0.native.custody();
    use_effect(move || {
        let gate_now = *gate.read();
        gate_line.set(Some(format!("gate: {}", gate_text(gate_now, custody))));
    });
    // The session's standing, read once at mount into the signals the page's
    // account bar and status lines stand on.
    {
        let session = parts.0.native.session();
        user_id.set(session.map(|s| s.user_id().to_owned()));
        account_key.set(session.map(|s| s.account().to_owned()));
        if let Some(session) = session {
            let current = session.account().to_owned();
            accounts.set(
                session
                    .accounts()
                    .iter()
                    .map(|key| (key.to_owned(), support::account_label(key), *key == current))
                    .collect(),
            );
        }
        custody_line.set(Some(format!("custody: {}", custody_text(custody))));
        device_line.set(support::native::device_label(&parts.0.native));
    }
    {
        let parts = parts.clone();
        use_hook(move || {
            spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let label = support::native::device_label(&parts.0.native);
                    if *device_line.read() != label {
                        device_line.set(label);
                    }
                }
            })
        });
    }
    {
        let client = client.clone();
        let session_expires_at = parts.0.native.session().map(|s| s.expires_at());
        use_effect(move || {
            let _ = orders_rows.value().read().len();
            let client = client.clone();
            spawn(async move {
                expiry_warn.set(match session_expires_at {
                    Some(deadline) => expiry_text(&client, deadline).await,
                    None => None,
                });
                footprint.set(support::replica_footprint(&client).await);
            });
        });
    }
    rsx! {}
}

async fn setup(
    account: AccountChoice,
    parked: tokio::sync::watch::Sender<Option<String>>,
    #[cfg(any(target_os = "android", target_os = "ios"))] parked_rx: tokio::sync::watch::Receiver<
        Option<String>,
    >,
    #[cfg(target_os = "windows")] owner: Arc<dyn connetto_client::HelloOwner>,
) -> Result<Parts, String> {
    let server = support::native::endpoint(
        std::env::var("CONNETTO_DEMO_WS").ok(),
        option_env!("CONNETTO_DEMO_BUILD_WS"),
        DEFAULT_WS,
    );
    let auth_origin = support::native::endpoint(
        std::env::var("CONNETTO_DEMO_AUTH_ORIGIN").ok(),
        option_env!("CONNETTO_DEMO_BUILD_AUTH_ORIGIN"),
        DEFAULT_AUTH_ORIGIN,
    );

    // Park the loopback login URL instead of opening a browser: the opener
    // hands the URL to the app, which offers it from the shared Sign in
    // button. A valid stored credential resolves before the opener runs, so
    // a restore launches nothing.
    let sign_in = platform_sign_in(
        Auth::new(auth_origin.clone(), support::native::AUTH_PROVIDER)
            .with_account(account)
            .keyring(KEYRING_SERVICE)
            .with_browser_opener({
                let parked = parked.clone();
                Arc::new(move |url: &str| {
                    tracing::info!("parked the loopback login: {url}");
                    // The demo proof reads the parked URL to complete a login a
                    // stand-in browser will not, so it also lands beside the
                    // replica.
                    let _ = std::fs::write(data_dir().join("parked-login"), url);
                    let _ = parked.send(Some(url.to_owned()));
                    Ok(())
                })
            }),
        #[cfg(any(target_os = "android", target_os = "ios"))]
        parked,
        #[cfg(any(target_os = "android", target_os = "ios"))]
        parked_rx,
    );
    #[cfg(target_os = "windows")]
    let sign_in = sign_in.with_hello_owner(owner);

    #[cfg(feature = "device-identity")]
    let roots = vec![DEVICE_ROOT.to_vec()];
    #[cfg(not(feature = "device-identity"))]
    let roots = Vec::new();
    let (connected, pump) = support::native::connect(
        server,
        auth_origin,
        KEYRING_SERVICE,
        &data_dir(),
        &roots,
        sign_in,
    )
    .await
    .map_err(|err| {
        err.chain()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(": ")
    })?;
    tokio::spawn(pump);
    let client = connected.native.client().clone();
    Ok(Parts {
        native: connected.native,
        client,
        authenticator: connected.authenticator,
        content: connected.content,
        runtime: tokio::runtime::Handle::current(),
    })
}

/// The header's gate line for the lock standing and the custody, the standing
/// itself where a gate guards the data, the no-gate statement where nothing
/// does.
fn gate_text(gate: &'static str, custody: Custody) -> &'static str {
    match custody {
        Custody::Verified => gate,
        Custody::Unverified(_) | Custody::Ephemeral => {
            "none, local data opens without user verification"
        }
    }
}

/// The header's custody line for the standing, naming the at-rest policy and
/// the reason it stands as it does on this system.
fn custody_text(custody: Custody) -> &'static str {
    match custody {
        Custody::Verified => "released only after user verification",
        Custody::Unverified(NoGate::Offerable) => {
            "stored without user verification, a passkey can be set up"
        }
        Custody::Unverified(NoGate::Declined) => {
            "stored without user verification, the passkey was declined"
        }
        Custody::Unverified(NoGate::Unsupported) => {
            "stored without user verification, this system offers none"
        }
        Custody::Ephemeral => "no durable key, nothing is kept",
    }
}

/// The warning shown while the session nears expiry with local writes unsent.
async fn expiry_text(client: &Client, session_expires_at: std::time::SystemTime) -> Option<String> {
    let unsynced = client.unsynced().await.unwrap_or_default();
    let lead = Duration::from_secs(7 * 24 * 60 * 60);
    let warning = expiry_warning(
        std::time::SystemTime::now(),
        session_expires_at,
        lead,
        unsynced,
        0,
    )?;
    let remaining = warning
        .session_expires_at
        .duration_since(std::time::SystemTime::now())
        .unwrap_or_default();
    let days = remaining.as_secs() / 86400;
    Some(format!(
        "Session expires in {days} day(s): {} pending local item(s) at risk. \
         Stay connected to extend the deadline automatically.",
        warning.pending_count()
    ))
}

/// Start over as another account, or as a new one, refused while local
/// writes are unsent.
async fn change_account(
    client: &Client,
    choice: AccountChoice,
    restart: Restart,
    mut logout_state: Signal<LogoutState>,
) {
    if let Some(line) = support::native::switch_account_guard(client).await {
        logout_state.set(LogoutState::Error(line));
        return;
    }
    restart.request_as(choice);
}

/// The platform's one app-level operation, the header's unlock button and the
/// account bar's switch, add and logout buttons reaching the client through
/// it.
#[derive(Clone)]
pub struct Ops {
    parts: Signal<Option<SessionParts>>,
    restart: Restart,
    logout: Signal<LogoutState>,
    /// The pending count a refused delete held, offered to the force delete.
    refused_pending: Signal<Option<u64>>,
    picker_active: Signal<bool>,
    parked: Signal<Option<String>>,
    status: Signal<String>,
    #[cfg(any(target_os = "android", target_os = "ios"))]
    parked_tx: tokio::sync::watch::Sender<Option<String>>,
}

impl Ops {
    /// The logout standing the account bar renders.
    pub fn logout_state(&self) -> Signal<LogoutState> {
        self.logout
    }

    /// The pending count a refused delete held, offered to the force delete.
    pub fn refused_pending(&self) -> Signal<Option<u64>> {
        self.refused_pending
    }

    /// Switch to another stored account, refused while writes are unsent.
    pub async fn switch_account(&self, key: &str) {
        let Some(sp) = self.parts.peek().clone() else {
            return;
        };
        let client = sp.0.client.clone();
        change_account(
            &client,
            AccountChoice::Account(key.to_owned()),
            self.restart,
            self.logout,
        )
        .await;
    }

    /// Open the account picker.
    pub async fn add_account(&self) {
        let mut active = self.picker_active;
        active.set(true);
    }

    /// Log out, keeping the local data. The session is revoked and the stored
    /// credential cleared; the replica and its key stay, so a returning user
    /// resumes from the persisted cursor.
    pub async fn logout_keep(&self) {
        let mut state = self.logout;
        state.set(LogoutState::Working);
        let Some(sp) = self.parts.peek().clone() else {
            state.set(LogoutState::Error(
                "no session to revoke in this build".to_owned(),
            ));
            return;
        };
        let Some(authenticator) = &sp.0.authenticator else {
            state.set(LogoutState::Error(
                "no session to revoke in this build".to_owned(),
            ));
            return;
        };
        if let Err(err) = authenticator.logout().await {
            // The local clear stands either way, so the session drops and the
            // app starts over; the server keeps the session until it lapses.
            tracing::warn!(error = %err, "revoking the session");
        }
        self.restart.request();
    }

    /// Delete the local data and log out, asking for confirmation while
    /// local writes are still unsent.
    pub async fn logout_delete(&self) {
        let mut state = self.logout;
        state.set(LogoutState::Working);
        let Some(sp) = self.parts.peek().clone() else {
            state.set(LogoutState::Error("no session in this build".to_owned()));
            return;
        };
        let client = sp.0.client.clone();
        match client.unsynced().await {
            Ok(unsynced) if unsynced.is_empty() => self.logout_wipe(state).await,
            Ok(unsynced) => state.set(LogoutState::ConfirmDelete {
                unsynced_count: unsynced.len() as u64,
            }),
            Err(err) => state.set(LogoutState::Error(format!("logout error: {err}"))),
        }
    }

    /// The confirmed delete, discarding the unsent work.
    pub async fn logout_delete_confirmed(&self) {
        let mut state = self.logout;
        state.set(LogoutState::Working);
        self.logout_wipe(state).await;
    }

    /// Wipe the replica and start over, discarding the unsent work. A refused
    /// wipe keeps the session, and one that closed the client starts over
    /// showing what it left undone.
    async fn logout_wipe(&self, mut state: Signal<LogoutState>) {
        let Some(sp) = self.parts.peek().clone() else {
            state.set(LogoutState::Error("no session in this build".to_owned()));
            return;
        };
        match sp.0.native.forget_device(true).await {
            Ok(()) => self.restart.request(),
            Err(ForgetError::Refused(refusal)) => {
                state.set(LogoutState::Error(format!("logout error: {refusal}")));
            }
            Err(ForgetError::Incomplete(report)) => {
                state.set(LogoutState::Error(format!("logout error: {report}")));
                self.restart.request();
            }
        }
    }

    /// Drop the standing logout confirmation or error.
    pub async fn dismiss(&self) {
        let mut state = self.logout;
        state.set(LogoutState::Idle);
    }

    /// Whether the header offers the unlock affordance, offered here, the
    /// gate being this client's own.
    pub fn offers_unlock(&self) -> bool {
        true
    }

    /// Unlock the stored-secrets gate.
    pub async fn unlock(&self) {
        if let Some(sp) = self.parts.peek().clone() {
            sp.0.client.unlock().await;
        }
    }

    /// The passkey gate enrolment is a web affordance a native build cannot
    /// offer, so the account bar's button never appears here.
    pub async fn enrol_gate(&self) {}

    /// Start the parked login the moment the user asks: the system browser on
    /// a desktop, the platform's auth session on a phone, or a re-run of the
    /// setup when no login is parked yet.
    pub async fn sign_in(&self) {
        match self.parked.peek().clone() {
            #[cfg(any(target_os = "android", target_os = "ios"))]
            Some(_) => {
                let mut status = self.status;
                status.set(support::native::SIGN_IN_OPEN.to_owned());
                // Clearing the parked URL is what the parked login waits for,
                // so the platform's auth session opens on this click.
                let _ = self.parked_tx.send(None);
            }
            #[cfg(not(any(target_os = "android", target_os = "ios")))]
            Some(url) => {
                let mut status = self.status;
                status.set(support::native::SIGN_IN_OPEN.to_owned());
                let opener = connetto_client::system_browser_opener();
                if let Err(err) = opener(&url) {
                    let mut state = self.logout;
                    state.set(LogoutState::Error(support::native::sign_in_open_error(err)));
                }
            }
            None => self.restart.request(),
        }
    }
}

#[cfg(test)]
mod tests {
    use connetto_demo_support::native::endpoint;

    /// The running environment wins, so a desktop build made with the
    /// variables set still follows what it is launched with, and a phone
    /// build falls back to what it was built with.
    #[test]
    fn a_runtime_endpoint_wins_over_the_built_one_and_both_over_the_default() {
        assert_eq!(
            endpoint(Some("run:1".to_owned()), Some("built:2"), "def:3"),
            "run:1"
        );
        assert_eq!(endpoint(None, Some("built:2"), "def:3"), "built:2");
        assert_eq!(endpoint(None, None, "def:3"), "def:3");
    }
}
