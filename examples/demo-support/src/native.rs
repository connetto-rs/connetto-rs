//! The native pieces the desktop and phone demo builds share, on non-wasm
//! targets: the endpoint, the builder chain and its error map, the phone
//! sign-in and the Keystore prompt, the device line, the content event lines
//! and the native import, and the account-switch guard.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use connetto_client::{
    ClientError, ConnettoClient, CorePump, Gate, ImportChoices, KeyringAuth, KeyringStore,
    NativeAuthenticator, NativeClient, NativeClientBuilder, NativeTransport, SyncTuning,
};
use connetto_file_client::{
    Content as ContentPiece, ContentClient, ContentEvent, ContentHandle, FsStore, ReqwestHttp,
};

/// The OIDC provider the dev stack registers.
pub const AUTH_PROVIDER: &str = "dev-idp";
/// The status line while the sign-in browser page opens.
pub const SIGN_IN_OPEN: &str = "the browser page is open";
/// How long the app may be away before it asks for Face ID, a fingerprint or
/// the device passcode again, where the platform gates the stored secrets.
pub const RECHECK_AFTER: Duration = Duration::from_secs(30);
/// How long a login in the browser tab may take.
#[cfg(any(target_os = "android", target_os = "ios"))]
const LOGIN_WINDOW: Duration = Duration::from_secs(600);
/// How long the unlock prompt may stay up.
#[cfg(target_os = "android")]
const UNLOCK_WINDOW: Duration = Duration::from_secs(120);

/// An endpoint as the running environment sets it, else as the build
/// environment did, else `default`.
#[must_use]
pub fn endpoint(runtime: Option<String>, built: Option<&'static str>, default: &str) -> String {
    runtime
        .or_else(|| built.map(str::to_owned))
        .unwrap_or_else(|| default.to_owned())
}

/// The status line when the sign-in browser page fails to open.
#[must_use]
pub fn sign_in_open_error(err: impl core::fmt::Display) -> String {
    format!("opening the browser: {err}")
}

/// The Keystore unlock prompt, through `connetto-auth-session`.
#[cfg(target_os = "android")]
pub struct KeystoreUnlock;

#[cfg(target_os = "android")]
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

/// The Custom Tab on Android or the authentication session on iOS, and the
/// app's redirect, through `connetto-auth-session`. The session opens when
/// the Sign in click clears the parked URL, the same moment the web opens
/// its popup, so a boot starts no in-app browser.
#[cfg(any(target_os = "android", target_os = "ios"))]
pub struct TabSession {
    parked: tokio::sync::watch::Sender<Option<String>>,
    parked_rx: tokio::sync::watch::Receiver<Option<String>>,
}

#[cfg(any(target_os = "android", target_os = "ios"))]
impl TabSession {
    /// Wrap the parked pair the Sign in click clears.
    #[must_use]
    #[expect(
        clippy::new_without_default,
        reason = "it wraps the channel pair it waits on"
    )]
    pub fn new(
        parked: tokio::sync::watch::Sender<Option<String>>,
        parked_rx: tokio::sync::watch::Receiver<Option<String>>,
    ) -> Self {
        Self { parked, parked_rx }
    }

    /// Park the login URL and wait for the Sign in click to clear it, then
    /// authorize in the platform's in-app browser.
    pub fn authorize(
        &self,
        url: String,
    ) -> impl Future<Output = Result<String, ClientError>> + Send + 'static {
        let mut waiting = self.parked_rx.clone();
        let parked = self.parked.clone();
        async move {
            // The login stands parked until the Sign in click clears the
            // parked URL, which is what the web's popup opening is.
            let _ = parked.send(Some(url.clone()));
            let _ = waiting.wait_for(|parked| parked.is_none()).await;
            connetto_auth_session::authorize(&url, LOGIN_WINDOW)
                .await
                .map_err(|err| ClientError::Auth(err.to_string()))
        }
    }
}

#[cfg(any(target_os = "android", target_os = "ios"))]
impl connetto_client::AuthorizationSession for TabSession {
    fn authorize(&self, url: String) -> connetto_client::SessionFuture {
        Box::pin(self.authorize(url))
    }

    fn delivered(&self) -> Option<String> {
        delivered()
    }
}

/// A delivered login redirect, once the platform's redirect route has caught
/// one.
#[cfg(any(target_os = "android", target_os = "ios"))]
#[must_use]
pub fn delivered() -> Option<String> {
    connetto_auth_session::delivered().unwrap_or_else(|err| {
        tracing::warn!(error = %err, "reading a delivered login redirect");
        None
    })
}

/// The status line for one content event.
#[must_use]
pub fn content_label(event: &ContentEvent) -> String {
    match event {
        ContentEvent::Uploaded { file_id } => {
            format!("uploaded {}", crate::short_hex(file_id.as_bytes()))
        }
        ContentEvent::UploadDeferred { file_id, detail } => {
            format!(
                "deferred {}: {detail}",
                crate::short_hex(file_id.as_bytes())
            )
        }
        ContentEvent::UploadRefused { file_id, detail } => {
            format!("refused {}: {detail}", crate::short_hex(file_id.as_bytes()))
        }
        ContentEvent::BytesLost {
            file_id,
            unreadable,
        } => format!(
            "bytes lost {} ({unreadable} unreadable chunks)",
            crate::short_hex(file_id.as_bytes())
        ),
        ContentEvent::Fetched { file_id } => {
            format!("fetched {}", crate::short_hex(file_id.as_bytes()))
        }
        ContentEvent::LostRequeued { file_id } => {
            format!("re-uploading lost {}", crate::short_hex(file_id.as_bytes()))
        }
        ContentEvent::IntegrityPassFailed { detail } => format!("integrity check failed: {detail}"),
    }
}

/// The device line, the identity and certificate standing a build with a
/// device identity reports.
#[cfg(feature = "device-identity")]
#[must_use]
pub fn device_label(
    native: &NativeClient<NativeTransport, ContentHandle<NativeTransport>>,
) -> String {
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

/// The device line for a build without a device identity.
#[cfg(not(feature = "device-identity"))]
#[must_use]
pub fn device_label(
    _native: &NativeClient<NativeTransport, ContentHandle<NativeTransport>>,
) -> String {
    "no device identity in this build".to_owned()
}

/// Restore one archive from `bytes`, keeping the file's version of every
/// clash, and answer with the line the pane shows.
pub async fn import_bytes(
    content: &ContentClient<NativeTransport, FsStore, ReqwestHttp>,
    bytes: Vec<u8>,
) -> String {
    let source = std::io::Cursor::new(bytes);
    let mut plan = match content.prepare_local_data_import(source).await {
        Ok(plan) => plan,
        Err(err) => return format!("refused: {err}"),
    };
    let clash_count = plan.replica_plan().collisions().len();
    let choices = ImportChoices::keeping_the_file();
    let outcome = match content.apply_local_data_import(&mut plan, &choices).await {
        Ok(outcome) => outcome,
        Err(err) => return format!("apply failed: {err}"),
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
    line
}

/// What one signed-in session runs on. The content client holds the
/// device's files beside the replica, and the authenticator revokes the
/// session without touching the replica, which the wipe path destroys
/// instead.
pub struct Connected {
    /// The native client the session's tasks run on.
    pub native: NativeClient<NativeTransport, ContentHandle<NativeTransport>>,
    /// The durable content client beside the replica.
    pub content: Arc<ContentClient<NativeTransport, FsStore, ReqwestHttp>>,
    /// The session revocation handle, `None` while no session stands.
    pub authenticator: Option<NativeAuthenticator>,
}

/// Connect the demo's native client with the shared chain, and answer with
/// the session pieces and the pump the caller drives.
///
/// # Errors
///
/// A credential the server refuses, or the replica failing to open.
pub async fn connect(
    server: String,
    auth_origin: String,
    keyring_service: &str,
    data_dir: &Path,
    deployment_roots: &[Vec<u8>],
    sign_in: KeyringAuth,
) -> anyhow::Result<(Connected, CorePump)> {
    use anyhow::Context as _;

    #[cfg(target_os = "android")]
    connetto_auth_session::initialize_tls()?;

    tokio::fs::create_dir_all(data_dir)
        .await
        .context("creating the application data directory")?;

    // A dropped link redials the same address and resumes, so writes made
    // while offline upload once the server is reachable again.
    let base_builder = NativeClientBuilder::new(server, crate::demo_schema())
        .with_tuning(SyncTuning::default().with_trim_threshold(5))
        .with_content(ContentPiece::new().with_heal_lost(
            "SELECT content_id FROM photos WHERE content_state = 'lost'",
            "content_id",
        ))
        .signed_in(sign_in)
        .durable(data_dir)
        .with_gate(Gate::default().with_recheck(Some(RECHECK_AFTER)));

    // The roots install method only exists in a device-identity build, so the
    // roots the caller ships are applied there and nowhere else.
    let builder = if !deployment_roots.is_empty() {
        #[cfg(feature = "device-identity")]
        {
            let builder = base_builder.with_deployment_roots(deployment_roots.to_vec());
            #[cfg(target_os = "android")]
            let builder = builder.with_java_access(connetto_auth_session::java_access());
            builder
        }
        #[cfg(not(feature = "device-identity"))]
        base_builder
    } else {
        base_builder
    };
    let (native, pump) = builder.connect_with_pump().await.map_err(|err| match err {
        ClientError::Auth(_) => anyhow::anyhow!(
            "the server refused the credential; check CONNETTO_AUTH and OIDC settings"
        ),
        other => anyhow::anyhow!("opening the encrypted replica: {other}"),
    })?;
    let content = match native.content() {
        Some(ContentHandle::Durable(content)) => Arc::clone(content),
        _ => anyhow::bail!("a durable build keeps its files beside the replica"),
    };

    // The authenticator the logout controls use to revoke the session
    // without touching the replica, which the wipe path destroys instead.
    let authenticator = native.session().map(|session| {
        NativeAuthenticator::new(
            auth_origin,
            AUTH_PROVIDER,
            Arc::new(KeyringStore::new(keyring_service)),
            Some(session.account().to_owned()),
        )
    });

    Ok((
        Connected {
            native,
            content,
            authenticator,
        },
        pump,
    ))
}

/// Whether a switch to another account may start, answering with the
/// refusal line when local writes are still unsent.
#[must_use]
pub async fn switch_account_guard(client: &ConnettoClient<NativeTransport>) -> Option<String> {
    match client.unsynced().await {
        Ok(unsynced) if unsynced.is_empty() => None,
        Ok(unsynced) => Some(format!(
            "Cannot change account: {} write(s) not yet synced.",
            unsynced.len()
        )),
        Err(err) => Some(format!("Cannot change account: {err}")),
    }
}
