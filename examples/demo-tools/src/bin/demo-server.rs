//! The demo's sync server, a process around [`ServerBuilder`].
//!
//! It reads its settings through [`builder_from_env`], binds the one listener,
//! and serves through the builder's own lifecycle. It owns what a process
//! owns: the logging, the shutdown signal, and the exit code. It exits `1`
//! when the change stream cannot answer what a row looked like before it
//! changed, or gives up reconnecting, and returns its build or HTTP errors as
//! failures.
//!
//! [`ServerBuilder`]: connetto_server::builder::ServerBuilder

use anyhow::{Context, Result};
use connetto_demo_tools::env::var_or;
use connetto_demo_tools::server_env::builder_from_env;
use connetto_server::builder::ServeError;
use tokio::net::TcpListener;

/// Resolve on the first SIGINT or SIGTERM. On a platform without SIGTERM only
/// the interrupt arm can fire.
async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::warn!(error = %err, "no SIGTERM handler, interrupt only");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // `pg_walstream` reports every standby status update at `info`, which is
    // one line per ten seconds whether or not anything happened, and it
    // buries this server's own events. `RUST_LOG` brings it back.
    connetto_core::logging::init_stdout_with_default("info,pg_walstream=warn");
    let builder = builder_from_env().await?;
    let bind = var_or("CONNETTO_BIND", "127.0.0.1:8080");
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(bind = %bind, "serving the sync, login and file routes");
    match builder.serve(listener, shutdown_signal()).await {
        Ok(()) => Ok(()),
        Err(err @ ServeError::ChangeStreamUnusable(_)) => {
            tracing::error!(error = %err, "the change stream cannot answer, refusing to serve");
            std::process::exit(1);
        }
        Err(err @ ServeError::ChangeStreamStopped(_)) => {
            tracing::error!(error = %err, "the change stream stopped, refusing to serve");
            std::process::exit(1);
        }
        Err(err) => Err(err.into()),
    }
}
