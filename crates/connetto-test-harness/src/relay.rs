//! A TCP relay a device proof owns between an app and a server, so the proof
//! can take the app offline and back without touching the device. A relay can
//! also terminate TLS, which is how a stack serves a server that speaks plain
//! HTTP to devices that insist on HTTPS.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

/// Forwards every connection accepted on one address to `upstream` while it
/// runs, and resets each one while stopped.
pub struct Relay {
    listener: Arc<TcpListener>,
    listen: SocketAddr,
    upstream: String,
    tls: Option<TlsAcceptor>,
    /// The task serving the listener, forwarding or resetting.
    task: JoinHandle<()>,
}

impl Relay {
    /// Listen on `listen` and forward to `upstream`.
    ///
    /// # Errors
    ///
    /// When `listen` cannot be bound.
    pub async fn start(listen: &str, upstream: &str) -> Result<Self> {
        Self::launch(listen, upstream, None).await
    }

    /// Listen on `listen`, terminate TLS with the PEM certificate chain and
    /// key at `cert` and `key`, and forward the plain stream to `upstream`.
    ///
    /// # Errors
    ///
    /// When the certificate or key cannot be read or used, or `listen`
    /// cannot be bound.
    pub async fn start_tls(listen: &str, upstream: &str, cert: &Path, key: &Path) -> Result<Self> {
        let chain = CertificateDer::pem_file_iter(cert)
            .with_context(|| format!("reading {}", cert.display()))?
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("parsing {}", cert.display()))?;
        let key = PrivateKeyDer::from_pem_file(key)
            .with_context(|| format!("reading {}", key.display()))?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .context("the TLS protocol versions")?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .context("the TLS certificate and key")?;
        Self::launch(listen, upstream, Some(TlsAcceptor::from(Arc::new(config)))).await
    }

    async fn launch(listen: &str, upstream: &str, tls: Option<TlsAcceptor>) -> Result<Self> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding the relay on {listen}"))?;
        let listen = listener.local_addr().context("the relay's address")?;
        let listener = Arc::new(listener);
        let upstream = upstream.to_owned();
        let task = tokio::spawn(serve(Arc::clone(&listener), upstream.clone(), tls.clone()));
        Ok(Self {
            listener,
            listen,
            upstream,
            tls,
            task,
        })
    }

    /// The address the relay listens on.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.listen
    }

    /// Drop every connection through the relay and reset each new one,
    /// returning once the forwarding has ended. The listener stays, so the
    /// port stays the relay's.
    pub async fn stop(&mut self) {
        let refusing = refuse(Arc::clone(&self.listener));
        self.replace_task(refusing).await;
    }

    /// Forward new connections again.
    pub async fn resume(&mut self) {
        let serving = serve(
            Arc::clone(&self.listener),
            self.upstream.clone(),
            self.tls.clone(),
        );
        self.replace_task(serving).await;
    }

    /// End the current task, its connections with it, then run `next` in its
    /// place.
    async fn replace_task(&mut self, next: impl Future<Output = ()> + Send + 'static) {
        self.task.abort();
        let _ = (&mut self.task).await;
        self.task = tokio::spawn(next);
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Accept and reset until aborted, the answer of a port nothing listens on.
async fn refuse(listener: Arc<TcpListener>) {
    loop {
        if let Ok((client, _)) = listener.accept().await {
            let _ = client.set_zero_linger();
        }
    }
}

/// Accept and forward until aborted. The connections live in a set this task
/// owns, so aborting it aborts every one of them.
async fn serve(listener: Arc<TcpListener>, upstream: String, tls: Option<TlsAcceptor>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((client, _)) = accepted else { continue };
                let upstream = upstream.clone();
                let tls = tls.clone();
                connections.spawn(async move {
                    let Ok(mut server) = TcpStream::connect(&upstream).await else { return };
                    if let Some(tls) = tls {
                        if let Ok(mut client) = tls.accept(client).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    } else {
                        let mut client = client;
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    use super::Relay;

    /// An upstream that echoes every byte back.
    async fn echo() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind echo");
        let address = listener.local_addr().expect("echo address").to_string();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let (mut read, mut write) = socket.split();
                    let _ = tokio::io::copy(&mut read, &mut write).await;
                });
            }
        });
        address
    }

    async fn round_trip(stream: &mut TcpStream) -> std::io::Result<u8> {
        stream.write_all(&[7]).await?;
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut byte))
            .await
            .map_err(|_| std::io::Error::other("no echo"))??;
        Ok(byte[0])
    }

    /// Stopping drops a connection already through the relay and refuses new
    /// ones, which is what the app sees as losing the network, and resuming
    /// forwards new connections again on the same address.
    #[tokio::test]
    async fn stopping_drops_live_connections_and_resuming_forwards_again() {
        let mut relay = Relay::start("127.0.0.1:0", &echo().await)
            .await
            .expect("start");
        let mut live = TcpStream::connect(relay.address()).await.expect("connect");
        assert_eq!(
            round_trip(&mut live).await.expect("echo through the relay"),
            7
        );

        relay.stop().await;
        assert!(
            round_trip(&mut live).await.is_err(),
            "the live connection survived"
        );
        if let Ok(mut stray) = TcpStream::connect(relay.address()).await {
            assert!(
                round_trip(&mut stray).await.is_err(),
                "a stopped relay forwarded a new connection"
            );
        }

        relay.resume().await;
        let mut again = TcpStream::connect(relay.address())
            .await
            .expect("reconnect");
        assert_eq!(
            round_trip(&mut again).await.expect("echo after resuming"),
            7
        );
    }

    /// A stopped relay keeps its port, so no other listener takes it while
    /// the app is offline.
    #[tokio::test]
    async fn a_stopped_relay_keeps_its_port() {
        let mut relay = Relay::start("127.0.0.1:0", &echo().await)
            .await
            .expect("start");
        relay.stop().await;
        assert!(
            TcpListener::bind(relay.address()).await.is_err(),
            "another listener took the stopped relay's port"
        );
        relay.resume().await;
    }
}
