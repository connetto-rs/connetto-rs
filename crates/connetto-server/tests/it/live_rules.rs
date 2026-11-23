//! Live authorization changes through the production server against Docker services.

use std::collections::BTreeSet;
use std::time::Duration;

use connetto_core::messages::{
    AckCredits, BulkMessage, ControlMessage, FatalErrorReason, FullResyncReason, Grant, Handshake,
    PauseCause, SUBSCRIPTION_REFUSED_SCHEMA, Subscribe, SubscriptionSpec,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, PROTOCOL_VERSION};
use connetto_server::builder::{
    Database, OidcProvider, OpenFga, ServerBuilder, ServerSchema, TokenKeys,
};
use connetto_server::epoch::POLICY_CHANGES_TRIGGER_DDL;
use connetto_server::{OidcProviderConfig, WebSocketTransport};
use connetto_test_harness::relay::Relay;
use connetto_test_harness::server_fixture::{
    PG_DDL, PG_SERIAL, exec, mint_token, reset_fixture, served_major, signing_keys, with_user_url,
};
use connetto_test_harness::{
    FIXTURE_READER, FIXTURE_ROLES, Fixture, MOCK_OAUTH_CLIENT_ID, MOCK_OAUTH_CLIENT_SECRET,
    MOCK_OAUTH_PROVIDER, MockOauth, OPLOG_TABLE, PUBLICATION, SLOT, provision_cap_policies,
};
use openfga_client::client::{OpenFgaServiceClient, ReadRequest};
use openfga_client::tonic::transport::Channel;
use sqlite_diff_rs::{ParsedDiffSet, PatchsetOp, Value};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};

use super::lifecycle::{BOUND, admin_pool, next_control, wait_ready};

const NEVER: Duration = Duration::from_secs(3600);
const KEEP_DDL: &str = "CREATE TABLE keep (id INT PRIMARY KEY, body TEXT, order_id INT);";
const POLICY: &str = "ALTER TABLE orders ENABLE ROW LEVEL SECURITY;\n\
    CREATE POLICY orders_live ON orders USING (quantity > 0);";
const HELPER: &str = "CREATE OR REPLACE FUNCTION public.live_rule(quantity integer) \
    RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT quantity > 0';";
const TIGHTEN: &str = "ALTER POLICY orders_live ON orders USING (quantity > 1)";
const TIGHTEN_HELPER: &str = "CREATE OR REPLACE FUNCTION public.live_rule(quantity integer) \
    RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT quantity > 1';";

/// The policies document the server is given, with the rule written inline or through the helper.
fn policies(helper: bool) -> String {
    if helper {
        format!(
            "{HELPER}\nALTER TABLE orders ENABLE ROW LEVEL SECURITY;\n\
            CREATE POLICY orders_live ON orders USING (public.live_rule(quantity));"
        )
    } else {
        POLICY.to_owned()
    }
}

type Client = WebSocketTransport<TcpStream>;

struct Running {
    address: String,
    token: String,
    user: String,
    channel: Channel,
    store: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), connetto_server::builder::ServeError>>,
    _identity_provider: MockOauth,
    _keys: TempDir,
}

impl Running {
    async fn start(fixture: &Fixture, interval: Duration, helper: bool) -> Self {
        Self::start_over(
            fixture,
            interval,
            helper,
            fixture.admin_url(),
            fixture.fga_url().await,
        )
        .await
    }

    async fn start_over(
        fixture: &Fixture,
        interval: Duration,
        helper: bool,
        owner_url: &str,
        authorization_url: &str,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address").to_string();
        let base = format!("http://{address}");
        let identity_provider = MockOauth::start().await;
        let keys = TempDir::new().expect("key directory");
        let (private, public) = signing_keys(&keys);
        let (channel, store) = fixture.fga_store().await;
        let policies = policies(helper);
        let builder = ServerBuilder::new(
            Database::new(
                owner_url.to_owned(),
                with_user_url(fixture.admin_url(), "app_reader", "app_reader"),
            ),
            ServerSchema::new(
                format!("{PG_DDL}\n{KEEP_DDL}"),
                policies,
                FIXTURE_ROLES,
                FIXTURE_READER,
            ),
            TokenKeys::from_pem(
                std::fs::read(private).expect("private key"),
                std::fs::read(public).expect("public key"),
            ),
            OpenFga::new(authorization_url.to_owned(), store.clone()),
        )
        .slot(SLOT)
        .publication(PUBLICATION)
        .oplog_table(OPLOG_TABLE)
        .drift_reconciliation(interval)
        .oidc_providers(vec![OidcProvider::Generic(
            OidcProviderConfig::new(
                MOCK_OAUTH_PROVIDER,
                MOCK_OAUTH_CLIENT_ID,
                identity_provider.issuer().to_owned(),
                format!("{base}/auth/callback"),
            )
            .with_client_secret(Some(MOCK_OAUTH_CLIENT_SECRET.to_owned())),
        )]);
        let (shutdown, signal) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            builder
                .serve(listener, async move {
                    let _ = signal.await;
                })
                .await
        });
        wait_ready(&base).await;
        let (token, user) = mint_token(&base).await;
        Self {
            address,
            token,
            user,
            channel,
            store,
            shutdown: Some(shutdown),
            task,
            _identity_provider: identity_provider,
            _keys: keys,
        }
    }

    async fn connect(&self, identity: &str, cursor: Option<Cursor>) -> Client {
        tokio::time::timeout(BOUND, async {
            let stream = TcpStream::connect(&self.address).await.expect("connect");
            let mut client =
                WebSocketTransport::connect(&format!("ws://{}/sync", self.address), stream)
                    .await
                    .expect("websocket");
            let mut handshake = Handshake::new(PROTOCOL_VERSION, identity)
                .with_major(served_major())
                .with_grant(Grant::new(&self.token));
            if let Some(cursor) = cursor {
                handshake = handshake
                    .with_cursor(cursor)
                    .with_cursor_subjects(core::iter::empty::<&str>());
            }
            client
                .send_control(ControlMessage::Handshake(handshake))
                .await
                .expect("handshake");
            assert!(matches!(
                next_control(&mut client).await,
                ControlMessage::Challenge(_)
            ));
            assert!(matches!(
                next_control(&mut client).await,
                ControlMessage::HandshakeAck(_)
            ));
            client
        })
        .await
        .expect("bounded connection")
    }

    async fn stop(mut self) {
        self.shutdown
            .take()
            .expect("shutdown sender")
            .send(())
            .expect("shutdown");
        tokio::time::timeout(BOUND, &mut self.task)
            .await
            .expect("bounded shutdown")
            .expect("server task")
            .expect("clean shutdown");
    }

    async fn facts(&self) -> BTreeSet<(String, String, String)> {
        let mut client = OpenFgaServiceClient::new(self.channel.clone());
        let mut facts = BTreeSet::new();
        let mut continuation_token = String::new();
        loop {
            let page = tokio::time::timeout(
                BOUND,
                client.read(ReadRequest {
                    store_id: self.store.clone(),
                    continuation_token,
                    ..ReadRequest::default()
                }),
            )
            .await
            .expect("bounded fact read")
            .expect("read facts")
            .into_inner();
            facts.extend(
                page.tuples
                    .into_iter()
                    .filter_map(|tuple| tuple.key)
                    .map(|key| (key.user, key.relation, key.object)),
            );
            if page.continuation_token.is_empty() {
                return facts;
            }
            continuation_token = page.continuation_token;
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn setup(fixture: &Fixture, helper: bool) {
    let pool = admin_pool(fixture).await;
    exec(
        &pool,
        "DROP EVENT TRIGGER IF EXISTS connetto_policy_changes_trigger",
    )
    .await;
    reset_fixture(&pool, fixture).await;
    for statement in [
        "DROP TABLE IF EXISTS keep CASCADE",
        KEEP_DDL,
        "GRANT SELECT, INSERT, UPDATE, DELETE ON keep TO app_reader",
        "INSERT INTO keep VALUES (1, 'sibling')",
        "UPDATE orders SET quantity = 1 WHERE id = 1",
        "INSERT INTO orders VALUES (2, 1.0, 2, 'visible')",
        "ALTER TABLE orders ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY orders_live ON orders USING (quantity > 0)",
    ] {
        exec(&pool, statement).await;
    }
    if helper {
        exec(&pool, HELPER).await;
        exec(
            &pool,
            "ALTER POLICY orders_live ON orders USING (public.live_rule(quantity))",
        )
        .await;
    }
    // Generated from the served documents, the caps leave `orders` no open policy beside its own rule.
    provision_cap_policies(&pool, &format!("{PG_DDL}\n{KEEP_DDL}"), &policies(helper)).await;
    fixture.start_replication(&["orders", "keep"]).await;
    fixture.provision_auth_tables().await;
}

async fn subscribe(client: &mut Client, subscription: &str, table: &str) {
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: subscription.to_owned(),
            spec: SubscriptionSpec::new(format!("SELECT * FROM {table}")),
        }))
        .await
        .expect("subscribe");
}

fn inserted_ids(bytes: &[u8]) -> BTreeSet<i64> {
    let decoded = zstd::decode_all(bytes).expect("decompress patch");
    let ParsedDiffSet::Patchset(patchset) = ParsedDiffSet::parse(&decoded).expect("parse patch")
    else {
        panic!("expected patchset");
    };
    patchset
        .iter()
        .filter_map(|operation| match operation {
            PatchsetOp::Insert { values, .. } => match values.first() {
                Some(Value::Integer(identity)) => Some(*identity),
                other => panic!("unexpected key {other:?}"),
            },
            PatchsetOp::Delete { .. } | PatchsetOp::Update { .. } => None,
        })
        .collect()
}

async fn snapshot(client: &mut Client, subscription: &str) -> (BTreeSet<i64>, Cursor) {
    tokio::time::timeout(BOUND, async {
        let ControlMessage::SnapshotBegin(begin) = next_control(client).await else {
            panic!("expected snapshot begin");
        };
        assert_eq!(begin.sub_id, subscription);
        let mut identities = BTreeSet::new();
        loop {
            match client
                .recv()
                .await
                .expect("receive")
                .expect("open transport")
            {
                IncomingFrame::Bulk(BulkMessage::SnapshotPatch(patch)) => {
                    assert_eq!(patch.sub_id, subscription);
                    identities.extend(inserted_ids(&patch.patchset_zstd));
                    client
                        .send_control(ControlMessage::AckCredits(AckCredits { credits: 1 }))
                        .await
                        .expect("credits");
                }
                IncomingFrame::Control(ControlMessage::SnapshotEnd(end)) => {
                    assert_eq!(end.sub_id, subscription);
                    return (identities, end.cursor);
                }
                other => panic!("unexpected snapshot frame {other:?}"),
            }
        }
    })
    .await
    .expect("bounded snapshot")
}

async fn replacement(client: &mut Client, subscription: &str) -> BTreeSet<i64> {
    let ControlMessage::FullResyncRequired(resync) = next_control(client).await else {
        panic!("expected authorization replacement");
    };
    assert_eq!(resync.sub_id, subscription);
    assert_eq!(resync.reason, FullResyncReason::AuthorizationChange);
    snapshot(client, subscription).await.0
}

/// Subscribe to `orders` and wait for a change the stream delivers, so the test's next change
/// meets a server past its connect. Answers the cursor of that delivered change.
async fn initial(fixture: &Fixture, server: &Running, identity: &str) -> (Client, Cursor) {
    let mut client = server.connect(identity, None).await;
    subscribe(&mut client, "orders", "orders").await;
    let (identities, _) = snapshot(&mut client, "orders").await;
    assert_eq!(identities, BTreeSet::from([1, 2]));
    fixture
        .exec("UPDATE orders SET price = price + 1 WHERE id = 2")
        .await;
    let cursor = tokio::time::timeout(BOUND, async {
        loop {
            match client
                .recv()
                .await
                .expect("receive")
                .expect("open transport")
            {
                IncomingFrame::Bulk(BulkMessage::LivePatch(patch)) => {
                    assert_eq!(patch.sub_id, "orders");
                    return patch.cursor;
                }
                IncomingFrame::Control(ControlMessage::RevocationUpdate(_)) => {}
                other => panic!("unexpected streaming frame {other:?}"),
            }
        }
    })
    .await
    .expect("the stream delivers a change");
    (client, cursor)
}

async fn live_after_change(client: &mut Client) {
    tokio::time::timeout(BOUND, async {
        loop {
            match client
                .recv()
                .await
                .expect("receive")
                .expect("open transport")
            {
                IncomingFrame::Bulk(BulkMessage::LivePatch(patch)) => {
                    assert_eq!(patch.sub_id, "orders");
                    let identities = inserted_ids(&patch.patchset_zstd);
                    assert!(!identities.contains(&3), "the new policy hides order 3");
                    if identities.contains(&4) {
                        return;
                    }
                }
                IncomingFrame::Control(ControlMessage::RevocationUpdate(_)) => {}
                other => panic!("unexpected live frame {other:?}"),
            }
        }
    })
    .await
    .expect("visible order follows the hidden order");
}

#[tokio::test]
async fn policy_tightening_replaces_rows_and_judges_later_events_under_new_rules() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let server = Running::start(&fixture, NEVER, false).await;
    let (mut client, _) = initial(&fixture, &server, "policy-live").await;
    fixture.exec(TIGHTEN).await;
    fixture
        .exec("UPDATE orders SET status = 'changed' WHERE id = 2")
        .await;
    assert_eq!(
        replacement(&mut client, "orders").await,
        BTreeSet::from([2])
    );
    fixture
        .exec("INSERT INTO orders VALUES (3, 1.0, 1, 'hidden')")
        .await;
    fixture
        .exec("INSERT INTO orders VALUES (4, 1.0, 2, 'visible')")
        .await;
    live_after_change(&mut client).await;
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn helper_trigger_follows_the_committed_change_before_later_rows() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, true).await;
    fixture.exec(POLICY_CHANGES_TRIGGER_DDL).await;
    let server = Running::start(&fixture, NEVER, true).await;
    let (mut client, _) = initial(&fixture, &server, "helper-trigger").await;
    fixture
        .exec(&format!("BEGIN; {TIGHTEN_HELPER} COMMIT;"))
        .await;
    assert_eq!(
        replacement(&mut client, "orders").await,
        BTreeSet::from([2])
    );
    fixture
        .exec("INSERT INTO orders VALUES (3, 1.0, 1, 'hidden')")
        .await;
    fixture
        .exec("INSERT INTO orders VALUES (4, 1.0, 2, 'visible')")
        .await;
    live_after_change(&mut client).await;
    drop(client);
    server.stop().await;
    fixture
        .exec("DROP EVENT TRIGGER connetto_policy_changes_trigger")
        .await;
}

#[tokio::test]
async fn reconciliation_finds_a_helper_change_without_a_marker_event() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, true).await;
    let server = Running::start(&fixture, Duration::from_secs(1), true).await;
    let (mut client, _) = initial(&fixture, &server, "helper-interval").await;
    fixture.exec(TIGHTEN_HELPER).await;
    assert_eq!(
        replacement(&mut client, "orders").await,
        BTreeSet::from([2])
    );
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn unsupported_live_policy_refuses_only_its_table() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let server = Running::start(&fixture, NEVER, false).await;
    let (mut client, _) = initial(&fixture, &server, "unsupported").await;
    fixture
        .exec("ALTER POLICY orders_live ON orders USING (random() > 0.5)")
        .await;
    fixture
        .exec("UPDATE orders SET status = 'changed' WHERE id = 2")
        .await;
    let ControlMessage::NonFatalError(refusal) = next_control(&mut client).await else {
        panic!("expected schema refusal");
    };
    assert_eq!(refusal.related_to.as_deref(), Some("orders"));
    assert_eq!(refusal.detail, SUBSCRIPTION_REFUSED_SCHEMA);
    subscribe(&mut client, "orders-again", "orders").await;
    let ControlMessage::NonFatalError(refusal) = next_control(&mut client).await else {
        panic!("expected persistent refusal");
    };
    assert_eq!(refusal.related_to.as_deref(), Some("orders-again"));
    assert_eq!(refusal.detail, SUBSCRIPTION_REFUSED_SCHEMA);
    subscribe(&mut client, "keep", "keep").await;
    assert_eq!(snapshot(&mut client, "keep").await.0, BTreeSet::from([1]));
    fixture
        .exec("INSERT INTO keep VALUES (2, 'still flowing')")
        .await;
    tokio::time::timeout(BOUND, async {
        loop {
            match client
                .recv()
                .await
                .expect("receive")
                .expect("open transport")
            {
                IncomingFrame::Bulk(BulkMessage::LivePatch(patch)) => {
                    assert_eq!(patch.sub_id, "keep");
                    assert_eq!(inserted_ids(&patch.patchset_zstd), BTreeSet::from([2]));
                    break;
                }
                IncomingFrame::Control(ControlMessage::RevocationUpdate(_)) => {}
                other => panic!("unexpected sibling frame {other:?}"),
            }
        }
    })
    .await
    .expect("sibling keeps flowing");
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn offline_client_resyncs_after_a_runtime_rules_change() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let server = Running::start(&fixture, NEVER, false).await;
    let (offline, cursor) = initial(&fixture, &server, "offline-runtime").await;
    drop(offline);
    let (mut observer, _) = initial(&fixture, &server, "runtime-observer").await;
    fixture.exec(TIGHTEN).await;
    fixture
        .exec("UPDATE orders SET status = 'changed' WHERE id = 2")
        .await;
    assert_eq!(
        replacement(&mut observer, "orders").await,
        BTreeSet::from([2])
    );
    let mut resumed = server.connect("offline-runtime", Some(cursor)).await;
    subscribe(&mut resumed, "orders", "orders").await;
    assert_eq!(
        replacement(&mut resumed, "orders").await,
        BTreeSet::from([2])
    );
    drop((resumed, observer));
    server.stop().await;
}

#[tokio::test]
async fn offline_client_resyncs_when_rules_change_between_processes() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let server = Running::start(&fixture, NEVER, false).await;
    let (offline, cursor) = initial(&fixture, &server, "offline-restart").await;
    drop(offline);
    server.stop().await;
    fixture.exec(TIGHTEN).await;
    let restarted = Running::start(&fixture, NEVER, false).await;
    let mut resumed = restarted.connect("offline-restart", Some(cursor)).await;
    subscribe(&mut resumed, "orders", "orders").await;
    assert_eq!(
        replacement(&mut resumed, "orders").await,
        BTreeSet::from([2])
    );
    drop(resumed);
    restarted.stop().await;
}

async fn relay_to(endpoint: &str) -> (Relay, String) {
    let mut url = openidconnect::reqwest::Url::parse(endpoint).expect("service URL");
    let upstream = format!(
        "{}:{}",
        url.host_str().expect("service host"),
        url.port_or_known_default().expect("service port")
    );
    let relay = tokio::time::timeout(BOUND, Relay::start("127.0.0.1:0", &upstream))
        .await
        .expect("bounded relay startup")
        .expect("relay");
    url.set_host(Some("127.0.0.1")).expect("relay host");
    url.set_port(Some(relay.address().port()))
        .expect("relay port");
    (relay, url.to_string())
}

/// How long a held follow may take to pause or resume. The server's owner pool waits out
/// bb8's 30 s checkout timeout while the database is unreachable, and once it answers again
/// the pool's next connection attempt can be up to 15 s away.
const SERVICE_RECOVERY: Duration = Duration::from_secs(90);

/// The next control frame, waiting up to [`SERVICE_RECOVERY`] for it.
async fn recovery_control(client: &mut Client) -> ControlMessage {
    tokio::time::timeout(SERVICE_RECOVERY, async {
        loop {
            if let IncomingFrame::Control(control) = client
                .recv()
                .await
                .expect("receive")
                .expect("open transport")
            {
                return control;
            }
        }
    })
    .await
    .expect("bounded follow recovery")
}

async fn expect_paused(client: &mut Client, expected: PauseCause) {
    match tokio::time::timeout(SERVICE_RECOVERY, client.recv())
        .await
        .expect("bounded follow pause")
        .expect("receive")
        .expect("open transport")
    {
        IncomingFrame::Control(ControlMessage::DeliveryPaused { cause }) => {
            assert_eq!(cause, expected);
        }
        other => panic!("expected paused delivery, got {other:?}"),
    }
}

async fn resumed_replacement(client: &mut Client) {
    let mut resumed = false;
    let mut replaced = false;
    while !resumed || !replaced {
        match recovery_control(client).await {
            ControlMessage::DeliveryResumed => {
                assert!(!resumed, "delivery resumes once");
                resumed = true;
            }
            ControlMessage::FullResyncRequired(resync) => {
                assert!(!replaced, "the held change replaces once");
                assert_eq!(resync.sub_id, "orders");
                assert_eq!(resync.reason, FullResyncReason::AuthorizationChange);
                assert_eq!(snapshot(client, "orders").await.0, BTreeSet::from([2]));
                replaced = true;
            }
            other => panic!("unexpected recovery frame {other:?}"),
        }
    }
}

#[tokio::test]
async fn authorization_outage_holds_a_rules_follow_and_resumes_before_later_rows() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let (mut authorization, authorization_url) = relay_to(fixture.fga_url().await).await;
    let server = Running::start_over(
        &fixture,
        NEVER,
        false,
        fixture.admin_url(),
        &authorization_url,
    )
    .await;
    let (mut client, _) = initial(&fixture, &server, "authorization-outage").await;
    tokio::time::timeout(BOUND, authorization.stop())
        .await
        .expect("cut authorization");
    fixture.exec(TIGHTEN).await;
    fixture
        .exec("UPDATE orders SET status = 'changed' WHERE id = 2")
        .await;
    expect_paused(&mut client, PauseCause::AuthServiceUnreachable).await;
    fixture
        .exec("INSERT INTO orders VALUES (3, 1.0, 1, 'hidden')")
        .await;
    tokio::time::timeout(BOUND, authorization.resume())
        .await
        .expect("restore authorization");
    resumed_replacement(&mut client).await;
    fixture
        .exec("INSERT INTO orders VALUES (4, 1.0, 2, 'visible')")
        .await;
    live_after_change(&mut client).await;
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn database_outage_during_fact_reconciliation_holds_and_resumes_the_follow() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let (mut authorization, authorization_url) = relay_to(fixture.fga_url().await).await;
    let (mut database, owner_url) = relay_to(fixture.admin_url()).await;
    let server = Running::start_over(&fixture, NEVER, false, &owner_url, &authorization_url).await;
    let (mut client, _) = initial(&fixture, &server, "database-outage").await;
    tokio::time::timeout(BOUND, authorization.stop())
        .await
        .expect("cut authorization");
    fixture.exec(TIGHTEN).await;
    fixture
        .exec("UPDATE orders SET status = 'changed' WHERE id = 2")
        .await;
    expect_paused(&mut client, PauseCause::AuthServiceUnreachable).await;
    tokio::time::timeout(BOUND, database.stop())
        .await
        .expect("cut database");
    tokio::time::timeout(BOUND, authorization.resume())
        .await
        .expect("restore authorization");
    expect_paused(&mut client, PauseCause::DatabaseUnreachable).await;
    tokio::time::timeout(BOUND, database.resume())
        .await
        .expect("restore database");
    resumed_replacement(&mut client).await;
    fixture
        .exec("INSERT INTO orders VALUES (3, 1.0, 1, 'hidden')")
        .await;
    fixture
        .exec("INSERT INTO orders VALUES (4, 1.0, 2, 'visible')")
        .await;
    live_after_change(&mut client).await;
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn shutdown_abandons_a_held_follow_without_restoring_authorization() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let (mut authorization, authorization_url) = relay_to(fixture.fga_url().await).await;
    let server = Running::start_over(
        &fixture,
        NEVER,
        false,
        fixture.admin_url(),
        &authorization_url,
    )
    .await;
    let (mut client, _) = initial(&fixture, &server, "shutdown-follow").await;
    tokio::time::timeout(BOUND, authorization.stop())
        .await
        .expect("cut authorization");
    fixture.exec(TIGHTEN).await;
    fixture
        .exec("UPDATE orders SET status = 'changed' WHERE id = 2")
        .await;
    expect_paused(&mut client, PauseCause::AuthServiceUnreachable).await;
    server.stop().await;
    match next_control(&mut client).await {
        ControlMessage::FatalError(fatal) => {
            assert_eq!(fatal.reason, FatalErrorReason::ServerShuttingDown);
        }
        other => panic!("expected shutdown while authorization remains unreachable, got {other:?}"),
    }
}

async fn expect_fact_movement(
    client: &mut Client,
    mut visible: BTreeSet<i64>,
    expected: &BTreeSet<i64>,
) {
    tokio::time::timeout(BOUND, async {
        loop {
            match client
                .recv()
                .await
                .expect("receive")
                .expect("open transport")
            {
                IncomingFrame::Control(ControlMessage::FullResyncRequired(resync)) => {
                    assert_eq!(resync.sub_id, "orders");
                    assert_eq!(resync.reason, FullResyncReason::AuthorizationChange);
                    visible = snapshot(client, "orders").await.0;
                }
                IncomingFrame::Bulk(BulkMessage::LivePatch(patch)) => {
                    assert_eq!(patch.sub_id, "orders");
                    let decoded =
                        zstd::decode_all(patch.patchset_zstd.as_slice()).expect("decompress");
                    let ParsedDiffSet::Patchset(patchset) =
                        ParsedDiffSet::parse(&decoded).expect("patch")
                    else {
                        panic!("expected patchset");
                    };
                    for operation in patchset.iter() {
                        match operation {
                            PatchsetOp::Insert { values, .. } => {
                                let Some(Value::Integer(identity)) = values.first() else {
                                    panic!("integer key");
                                };
                                visible.insert(*identity);
                            }
                            PatchsetOp::Delete { pk, .. } => {
                                let Some(Value::Integer(identity)) = pk.first() else {
                                    panic!("integer key");
                                };
                                visible.remove(identity);
                            }
                            PatchsetOp::Update { .. } => {}
                        }
                    }
                }
                IncomingFrame::Control(ControlMessage::RevocationUpdate(_)) => {}
                other => panic!("unexpected fact movement frame {other:?}"),
            }
            if &visible == expected {
                return;
            }
        }
    })
    .await
    .expect("live-only column changes subscriber visibility");
}

#[tokio::test]
async fn live_only_non_key_column_moves_database_queried_facts_and_subscriber_rows() {
    let _keyring = connetto_test_harness::isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    setup(&fixture, false).await;
    let server = Running::start(&fixture, NEVER, false).await;
    let (mut client, _) = initial(&fixture, &server, "live-column").await;
    let user = server.user.replace('\'', "''");
    fixture.exec("ALTER TABLE keep ADD COLUMN note TEXT").await;
    fixture
        .exec(&format!(
            "UPDATE keep SET body = '{user}', order_id = 1, note = 'OK' WHERE id = 1"
        ))
        .await;
    fixture
        .exec(&format!(
            "INSERT INTO keep (id, body, order_id, note) VALUES (2, '{user}', 2, 'no')"
        ))
        .await;
    // `lower` is evaluated only by SQL, so the server reads the live-only `note` by querying.
    fixture
        .exec(
            "ALTER POLICY orders_live ON orders USING (EXISTS (SELECT 1 FROM keep \
        WHERE keep.order_id = orders.id AND keep.body = current_setting('app.user_id', true) \
        AND lower(keep.note) = 'ok'))",
        )
        .await;
    fixture
        .exec("UPDATE orders SET status = 'joined' WHERE id = 2")
        .await;
    assert_eq!(
        replacement(&mut client, "orders").await,
        BTreeSet::from([1])
    );
    let before = server.facts().await;
    fixture
        .exec("UPDATE keep SET note = CASE WHEN id = 1 THEN 'no' ELSE 'Ok' END")
        .await;
    expect_fact_movement(&mut client, BTreeSet::from([1]), &BTreeSet::from([2])).await;
    let after = server.facts().await;
    assert!(
        before.difference(&after).next().is_some(),
        "the withdrawn fact leaves the store"
    );
    assert!(
        after.difference(&before).next().is_some(),
        "the granted fact enters the store"
    );
    drop(client);
    server.stop().await;
}
