//! The demo server embedded in an application's own axum router.
//!
//! It reads the settings `demo-server` reads, through
//! [`builder_from_env`], builds the [`ServerBuilder`] parts, and mounts them
//! beside a route of its own, so the application serves the sync, login and
//! file routes from its own listener. It exits `1` when the change stream
//! ends, because a supervisor that sees a clean exit would not restart one
//! that can no longer deliver.
//!
//! [`ServerBuilder`]: connetto_server::builder::ServerBuilder

use anyhow::{Context, Result};
use connetto_demo_tools::env::var_or;
use connetto_demo_tools::server_env::builder_from_env;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    connetto_core::logging::init_stdout_with_default("info,pg_walstream=warn");
    let parts = builder_from_env().await?.build().await?;
    // The application's own route beside the sync, login and file routes.
    let router = parts
        .router
        .route("/app/ping", axum::routing::get(|| async { "pong" }));
    let bind = var_or("CONNETTO_BIND", "127.0.0.1:8080");
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(bind = %bind, "serving");
    let mut change_stream = tokio::spawn(parts.change_stream);
    tokio::select! {
        outcome = &mut change_stream => {
            let terminal = match outcome {
                Ok(Ok(())) => false,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "the change stream stopped");
                    true
                }
                Err(err) => {
                    tracing::error!(error = %err, "the change stream task failed");
                    true
                }
            };
            let closed = parts.handle.shutdown().await;
            tracing::info!(closed, "the change stream ended, shutting down");
            // Live delivery has stopped, so the run ends nonzero, the way
            // the server's supervisor contract says a terminal stream ends.
            if terminal {
                std::process::exit(1);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            let closed = parts.handle.shutdown().await;
            tracing::info!(closed, "shutting down");
            change_stream.abort();
        }
        outcome = axum::serve(listener, router) => {
            if let Err(err) = outcome {
                tracing::error!(error = %err, "the listener stopped");
            }
            parts.handle.shutdown().await;
            change_stream.abort();
        }
    }
    Ok(())
}
