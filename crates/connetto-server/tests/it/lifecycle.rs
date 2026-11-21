//! The serving lifecycle against a real database, one situation per test:
//! the embedder's `build` parts, the `serve` shutdown signal, and the
//! terminal change-stream outcome that closes every session (R6 decision 4).
//!
//! Each test boots the `ServerBuilder` over its own fixture, the same way the
//! binary translates its environment, so the proof goes through the one
//! construction path.

use std::time::{Duration, Instant};

use connetto_core::PROTOCOL_VERSION;
use connetto_core::messages::{
    ControlMessage, FatalError, FatalErrorReason, Grant, Handshake, HandshakeAck,
    SUBSCRIPTION_REFUSED_SCHEMA, Subscribe, SubscriptionSpec,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_server::builder::{
    Database, OidcProvider, OpenFga, ServerBuilder, ServerSchema, TokenKeys,
};
use connetto_server::{OidcProviderConfig, RuntimeWritableCatalog, WebSocketTransport};
use connetto_test_harness::{
    Fixture, MOCK_OAUTH_CLIENT_ID, MOCK_OAUTH_CLIENT_SECRET, MOCK_OAUTH_PROVIDER, MockOauth,
    OPLOG_TABLE, PUBLICATION, SLOT, provision_cap_policies,
};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use openidconnect::reqwest;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};

use connetto_test_harness::server_fixture::{
    NO_POLICIES, OWNED_PG_DDL, OWNED_POLICIES, PG_DDL, PG_SERIAL, exec, mint_token, reset_fixture,
    served_major, signing_keys, with_user_url,
};

/// How long the readiness probe and the frame waits give the server.
pub(super) const BOUND: Duration = Duration::from_secs(30);

/// The one-construction-path server over `fixture`, the way `demo-server`
/// translates its environment: the owner and reader roles, the schema
/// documents, the persisted JWT keypair, the loopback identity provider and
/// the fixture's authorization store.
pub(super) async fn builder_over(
    fixture: &Fixture,
    port: u16,
) -> (ServerBuilder, MockOauth, TempDir) {
    let url = fixture.admin_url().to_owned();
    let reader = with_user_url(&url, "app_reader", "app_reader");
    let idp = MockOauth::start().await;
    let keys = TempDir::new().expect("key dir");
    let (private, public) = signing_keys(&keys);
    let token = TokenKeys::from_pem(
        std::fs::read(&private).expect("read the private half"),
        std::fs::read(&public).expect("read the public half"),
    );
    let (_channel, store) = fixture.fga_store().await;
    let provider = OidcProvider::Generic(
        OidcProviderConfig::new(
            MOCK_OAUTH_PROVIDER,
            MOCK_OAUTH_CLIENT_ID,
            idp.issuer().to_owned(),
            format!("http://127.0.0.1:{port}/auth/callback"),
        )
        .with_client_secret(Some(MOCK_OAUTH_CLIENT_SECRET.to_owned())),
    );
    let builder = ServerBuilder::new(
        Database::new(url, reader),
        ServerSchema::new(
            PG_DDL,
            NO_POLICIES,
            connetto_test_harness::FIXTURE_ROLES,
            connetto_test_harness::FIXTURE_READER,
        ),
        token,
        OpenFga::new(fixture.fga_url().await.to_owned(), store),
    )
    .slot(SLOT)
    .publication(PUBLICATION)
    .oplog_table(OPLOG_TABLE)
    .oidc_providers(vec![provider])
    .writable(RuntimeWritableCatalog::builder().writable("orders").build());
    (builder, idp, keys)
}

/// The one-construction-path server over the `owned` fixture, which carries a
/// policy and so a model shape. The change path asks the store what the row
/// granted, which is the path a key-only previous image trips.
pub(super) async fn builder_over_owned(
    fixture: &Fixture,
    port: u16,
) -> (ServerBuilder, MockOauth, TempDir) {
    let url = fixture.admin_url().to_owned();
    let reader = with_user_url(&url, "app_reader", "app_reader");
    let idp = MockOauth::start().await;
    let keys = TempDir::new().expect("key dir");
    let (private, public) = signing_keys(&keys);
    let token = TokenKeys::from_pem(
        std::fs::read(&private).expect("read the private half"),
        std::fs::read(&public).expect("read the public half"),
    );
    let (_channel, store) = fixture.fga_store().await;
    let provider = OidcProvider::Generic(
        OidcProviderConfig::new(
            MOCK_OAUTH_PROVIDER,
            MOCK_OAUTH_CLIENT_ID,
            idp.issuer().to_owned(),
            format!("http://127.0.0.1:{port}/auth/callback"),
        )
        .with_client_secret(Some(MOCK_OAUTH_CLIENT_SECRET.to_owned())),
    );
    let builder = ServerBuilder::new(
        Database::new(url, reader),
        ServerSchema::new(
            OWNED_PG_DDL,
            OWNED_POLICIES,
            connetto_test_harness::FIXTURE_ROLES,
            connetto_test_harness::FIXTURE_READER,
        ),
        token,
        OpenFga::new(fixture.fga_url().await.to_owned(), store),
    )
    .slot(SLOT)
    .publication(PUBLICATION)
    .oplog_table(OPLOG_TABLE)
    .oidc_providers(vec![provider])
    .writable(RuntimeWritableCatalog::builder().writable("owned").build());
    (builder, idp, keys)
}

/// The admin pool over the fixture's database.
pub(super) async fn admin_pool(fixture: &Fixture) -> Pool<AsyncPgConnection> {
    let manager =
        AsyncDieselConnectionManager::<AsyncPgConnection>::new(fixture.admin_url().to_owned());
    Pool::builder().build(manager).await.expect("build pool")
}

/// Wait until the login endpoint redirects, bounded.
///
/// # Panics
///
/// When the endpoint never answers within the bound.
pub(super) async fn wait_ready(base: &str) {
    let agent = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(2))
        .build()
        .expect("build the readiness client");
    let deadline = Instant::now() + BOUND;
    loop {
        assert!(
            Instant::now() < deadline,
            "the server never served {base}/auth/login"
        );
        match agent
            .get(format!("{base}/auth/login"))
            .query(&[("provider", MOCK_OAUTH_PROVIDER)])
            .send()
            .await
        {
            Ok(login) if login.status().is_redirection() => return,
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// The next frame that is a control frame, bounded.
///
/// # Panics
///
/// When the transport closes or stalls before the bound.
pub(super) async fn next_control(client: &mut WebSocketTransport<TcpStream>) -> ControlMessage {
    let deadline = Instant::now() + BOUND;
    loop {
        assert!(Instant::now() < deadline, "the session went quiet");
        let frame = tokio::time::timeout(Duration::from_secs(10), client.recv())
            .await
            .expect("the transport answers")
            .expect("the transport is open")
            .expect("the frame decodes");
        if let IncomingFrame::Control(control) = frame {
            return control;
        }
    }
}

/// Open a session on the sync route with `token` and wait for the ack.
pub(super) async fn live_session(addr: &str, token: &str) -> WebSocketTransport<TcpStream> {
    live_session_as(addr, token, "lifecycle").await
}

/// [`live_session`] under the named client id.
pub(super) async fn live_session_as(
    addr: &str,
    token: &str,
    client_id: &str,
) -> WebSocketTransport<TcpStream> {
    let tcp = TcpStream::connect(addr)
        .await
        .expect("connect the sync route");
    let mut client = WebSocketTransport::connect(&format!("ws://{addr}/sync"), tcp)
        .await
        .expect("the axum adapter answers the WebSocket handshake");
    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, client_id)
                .with_major(served_major())
                .with_grant(Grant::new(token)),
        ))
        .await
        .expect("send the handshake");
    // The server leads every connection with a challenge a person ignores (R91).
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    match next_control(&mut client).await {
        ControlMessage::HandshakeAck(HandshakeAck { connection_id, .. }) => {
            assert!(
                !connection_id.is_empty(),
                "the ack names the session it opens"
            );
        }
        other => panic!("the handshake answered {other:?}"),
    }
    client
}

/// The `build` terminal over the parts an embedder serves itself: the login
/// dance signs a user in, the sync route beside it answers a real session,
/// and the handle sees it live.
#[tokio::test]
async fn the_sync_route_serves_a_real_session_over_the_build_parts() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, _idp, _keys) = builder_over(&fixture, port).await;
    let base = format!("http://127.0.0.1:{port}");

    let parts = builder.build().await.expect("the deployment assembles");
    let handle = parts.handle.clone();
    let stream = tokio::spawn(parts.change_stream);
    let http = tokio::spawn(async move { axum::serve(listener, parts.router).await });
    wait_ready(&base).await;

    let (token, _) = mint_token(&base).await;
    let mut client = live_session(&format!("127.0.0.1:{port}"), &token).await;
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "orders".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM orders"),
        }))
        .await
        .expect("send the subscription");
    assert_eq!(
        handle.live_connections().await,
        1,
        "the handle sees the session the sync route opened"
    );
    handle.shutdown().await;
    stream.abort();
    http.abort();
}

/// The `serve` terminal on a shutdown signal: the signal ends `serve` with
/// `Ok`, and the live session's last frame says the server is shutting down.
#[tokio::test]
async fn a_shutdown_signal_ends_serve_with_the_sessions_closed() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, _idp, _keys) = builder_over(&fixture, port).await;
    let base = format!("http://127.0.0.1:{port}");

    let (fire, signal) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        let shutdown = async move {
            let _ = signal.await;
        };
        builder.serve(listener, shutdown).await
    });
    wait_ready(&base).await;

    let (token, _) = mint_token(&base).await;
    let mut client = live_session(&format!("127.0.0.1:{port}"), &token).await;

    fire.send(()).expect("fire the shutdown signal");
    let outcome = tokio::time::timeout(BOUND, serve)
        .await
        .expect("serve ends on the signal")
        .expect("serve joins");
    assert!(outcome.is_ok(), "the signal is a clean end: {outcome:?}");

    let last = next_control(&mut client).await;
    match last {
        ControlMessage::FatalError(FatalError {
            reason: FatalErrorReason::ServerShuttingDown,
        }) => {}
        other => panic!("the session's last frame says {other:?}"),
    }
}

/// The next refusal on `client`, past the snapshot's tail frames, asserted to
/// be the schema refusal of `sub_id`.
async fn expect_schema_refusal(client: &mut WebSocketTransport<TcpStream>, sub_id: &str) {
    let refusal = loop {
        if let ControlMessage::NonFatalError(refusal) = next_control(client).await {
            break refusal;
        }
    };
    assert_eq!(refusal.related_to.as_deref(), Some(sub_id));
    assert_eq!(refusal.detail, SUBSCRIPTION_REFUSED_SCHEMA);
}

/// Ask `client`'s server to subscribe to `table` again, which a refused
/// table answers with the schema refusal, and then end `serve` cleanly.
async fn still_serving_then_shut_down(
    client: &mut WebSocketTransport<TcpStream>,
    table: &str,
    fire: tokio::sync::oneshot::Sender<()>,
    serve: tokio::task::JoinHandle<Result<(), connetto_server::builder::ServeError>>,
) {
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "again".to_owned(),
            spec: SubscriptionSpec::new(format!("SELECT * FROM {table}")),
        }))
        .await
        .expect("send the second subscription");
    expect_schema_refusal(client, "again").await;
    fire.send(()).expect("fire the shutdown signal");
    let outcome = tokio::time::timeout(BOUND, serve)
        .await
        .expect("serve ends on the signal")
        .expect("serve joins");
    assert!(
        outcome.is_ok(),
        "the refusal never ended serve: {outcome:?}"
    );
}

/// A table the boot guard saw recording full previous images is switched to
/// `REPLICA IDENTITY DEFAULT`. The stream's `Relation` message ahead of the
/// next delete carries the weaker identity, so the guard refuses the table
/// before its key-only delete reaches anything, and `serve` keeps running
/// (R107 decision 10).
#[tokio::test]
async fn a_weakened_replica_identity_refuses_the_table_and_serve_keeps_running() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, _idp, _keys) = builder_over(&fixture, port).await;
    let base = format!("http://127.0.0.1:{port}");

    let (fire, signal) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        let shutdown = async move {
            let _ = signal.await;
        };
        builder.serve(listener, shutdown).await
    });
    wait_ready(&base).await;

    let (token, _) = mint_token(&base).await;
    let mut client = live_session(&format!("127.0.0.1:{port}"), &token).await;
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "orders".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM orders"),
        }))
        .await
        .expect("send the subscription");

    // The subscription is live only once its snapshot begins; a delete
    // ingested before it reaches no watcher, so the stream never refuses.
    match next_control(&mut client).await {
        ControlMessage::SnapshotBegin(snapshot) if snapshot.sub_id == "orders" => {}
        other => panic!("the snapshot opened with {other:?} instead"),
    }

    // The operator drops the previous images after the boot preflight passed,
    // so the stream's copy of the next delete carries the key and nothing
    // else, and the previous image cannot be judged.
    exec(&pool, "ALTER TABLE orders REPLICA IDENTITY DEFAULT").await;
    exec(&pool, "DELETE FROM orders WHERE id = 1").await;

    expect_schema_refusal(&mut client, "orders").await;
    still_serving_then_shut_down(&mut client, "orders", fire, serve).await;
}

/// The same refusal on a table the model shapes. Without the guard, the
/// key-only previous image reaches the upkeep's diff, which holds it as a
/// transient `AuthUnavailable` and retries it for ever.
#[tokio::test]
async fn a_weakened_replica_identity_refuses_a_model_table() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    // The `owned` fixture carries a policy, so the table reaches the model
    // with a shape and the store's diff reads its previous image.
    for stmt in [
        "DROP TABLE IF EXISTS owned CASCADE",
        "DROP TABLE IF EXISTS _connetto_settled",
        "DROP TABLE IF EXISTS _connetto_writers",
        OWNED_PG_DDL,
        "ALTER TABLE owned ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY owned_p ON owned USING (owner = current_setting('app.user_id', true))",
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_reader') \
         THEN CREATE ROLE app_reader LOGIN PASSWORD 'app_reader'; END IF; END $$",
        "GRANT USAGE ON SCHEMA public TO app_reader",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON owned TO app_reader",
        "GRANT SELECT ON auth_events TO app_reader",
    ] {
        exec(&pool, stmt).await;
    }
    connetto_test_harness::provision_writers(&pool).await;
    exec(
        &pool,
        "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, \
         _connetto_session_writers TO app_reader",
    )
    .await;
    provision_cap_policies(&pool, OWNED_PG_DDL, OWNED_POLICIES).await;
    exec(&pool, "INSERT INTO owned VALUES (1, 'alice', 'seed')").await;
    fixture.start_replication(&["owned"]).await;
    fixture.provision_auth_tables().await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, _idp, _keys) = builder_over_owned(&fixture, port).await;
    let base = format!("http://127.0.0.1:{port}");

    let (fire, signal) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        let shutdown = async move {
            let _ = signal.await;
        };
        builder.serve(listener, shutdown).await
    });
    wait_ready(&base).await;

    let (token, _) = mint_token(&base).await;
    let mut client = live_session(&format!("127.0.0.1:{port}"), &token).await;
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "owned".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM owned"),
        }))
        .await
        .expect("send the subscription");

    match next_control(&mut client).await {
        ControlMessage::SnapshotBegin(snapshot) if snapshot.sub_id == "owned" => {}
        other => panic!("the snapshot opened with {other:?} instead"),
    }

    exec(&pool, "ALTER TABLE owned REPLICA IDENTITY DEFAULT").await;
    exec(&pool, "DELETE FROM owned WHERE id = 1").await;

    expect_schema_refusal(&mut client, "owned").await;
    still_serving_then_shut_down(&mut client, "owned", fire, serve).await;
}
