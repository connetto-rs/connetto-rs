//! The native-client helpers the sign-in suites share, the keyring sign-in
//! and the fake browser that walks the real OIDC redirect chain, the
//! signed-in durable build, and the bounded waits on the build's events and
//! certificate.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use connetto_client::{
    AccountChoice, Auth, BrowserOpener, ClientEvent, Keyring, KeyringAuth, KeyringKeyStore,
    NativeClient, NativeClientBuilder, NativeDurable, NativeTransport, SyncSchema,
};
use connetto_core::device_cert::DeviceCertificate;

/// A fresh-login keyring sign-in as `subject` under the provider named by
/// `provider`, the services named by `app_id`.
#[must_use]
pub fn fresh_login(base: &str, provider: &str, app_id: &str, subject: &str) -> KeyringAuth {
    Auth::new(base, provider)
        .with_account(AccountChoice::New)
        .keyring(app_id)
        .with_browser_opener(fake_browser(subject))
}

/// The silent sign-back-in the restart and reconnect phases use, the services
/// named by `app_id`.
#[must_use]
pub fn last_used(base: &str, provider: &str, app_id: &str) -> KeyringAuth {
    Auth::new(base, provider)
        .keyring(app_id)
        .with_browser_opener(no_browser())
}

/// A browser opener that fails the test, for a sign-in that must not log in
/// interactively.
///
/// # Panics
///
/// Panics when the opener is called, the sign-in the test refuses.
#[must_use]
pub fn no_browser() -> BrowserOpener {
    Arc::new(|_url: &str| panic!("the sign-in opened a browser"))
}

/// A fake browser. Given connetto's login URL, walk the real OIDC redirect
/// chain until the authenticator's loopback listener receives the code.
///
/// # Panics
///
/// Panics when the http client cannot build, or a hop of the walk fails.
#[must_use]
pub fn fake_browser(subject: &str) -> BrowserOpener {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("http client");
    let subject = subject.to_owned();
    Arc::new(move |login_url: &str| {
        let http = http.clone();
        let subject = subject.clone();
        let login_url = login_url.to_owned();
        tokio::spawn(async move {
            let mut next_url = login_url;
            let mut submitted = false;
            for _ in 0..6 {
                let resp = http.get(&next_url).send().await.expect("hop");
                if !submitted && resp.url().path().ends_with("/authorize") {
                    let form_url = resp.url().to_string();
                    let posted = http
                        .post(form_url)
                        .header("content-type", "application/x-www-form-urlencoded")
                        .body(format!("username={subject}"))
                        .send()
                        .await
                        .expect("submit the login form");
                    submitted = true;
                    let Some(loc) = posted.headers().get("location") else {
                        break;
                    };
                    loc.to_str()
                        .expect("utf8 location")
                        .clone_into(&mut next_url);
                    continue;
                }
                match resp.headers().get("location") {
                    Some(loc) => {
                        loc.to_str()
                            .expect("utf8 location")
                            .clone_into(&mut next_url);
                    }
                    None => break,
                }
            }
        });
        Ok(())
    })
}

/// A signed-in durable build over `server`, the schema it syncs, the sign-in
/// it names, the data directory it keeps and the deployment roots it verifies
/// device certificates against.
#[must_use]
pub fn signed_in(
    server: String,
    schema: SyncSchema,
    sign_in: KeyringAuth,
    dir: &Path,
    roots: Vec<Vec<u8>>,
) -> NativeDurable<NativeTransport, (), Keyring, KeyringKeyStore> {
    NativeClientBuilder::new(server, schema)
        .signed_in(sign_in)
        .durable(dir)
        .with_deployment_roots(roots)
}

/// Await an event matching `is` within `deadline`, skipping every other event.
pub async fn next_event(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    deadline: Duration,
    is: impl Fn(&ClientEvent) -> bool,
) -> Option<ClientEvent> {
    let until = Instant::now() + deadline;
    loop {
        let remaining = until.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, events.recv()).await {
            Ok(Ok(event)) if is(&event) => return Some(event),
            Ok(Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
            _ => return None,
        }
    }
}

/// Poll `device_certificate` until it holds a certificate, bounded by
/// `deadline`.
pub async fn wait_for_certificate(
    client: &NativeClient<NativeTransport>,
    deadline: Duration,
) -> Option<DeviceCertificate> {
    let until = Instant::now() + deadline;
    loop {
        if let Some(cert) = client.device_certificate() {
            return Some(cert);
        }
        if Instant::now() >= until {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Poll `unsynced` until it holds nothing, bounded by `deadline`.
pub async fn wait_unsynced_empty(
    client: &NativeClient<NativeTransport>,
    deadline: Duration,
) -> bool {
    let until = Instant::now() + deadline;
    loop {
        if client
            .client()
            .unsynced()
            .await
            .is_ok_and(|entries| entries.is_empty())
        {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
