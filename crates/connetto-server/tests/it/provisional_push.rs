//! Docker-gated tests for the bookkeeping tables the server settles into
//! (R77 decision 11).
//!
//! A client subscription reading one of the tables is refused, so the table's
//! rows never reach the client through the change fan-out, and startup
//! refuses a publication that leaves one of them out.
//!
//! Needs Docker: the fixture starts its own Postgres and the test drives real
//! logical replication so a delivered row would be observable.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use connetto_core::auth::Principal;
use connetto_core::messages::{ControlMessage, SUBSCRIPTION_REFUSED, Subscribe, SubscriptionSpec};
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::{IncomingFrame, Transport as _};
use connetto_server::{
    AbuseConfig, Artifact, ManagerBuilder, Materializer, NoConnector, PageSpec, PreflightError,
    ReconnectPolicy, RequestGuard, ResumePoint, SessionConfig, SessionManager, SnapshotEstimate,
    SnapshotPage, SnapshotSource, ThrottleConfig, loopback, pg_write_target, preflight,
};
use connetto_test_harness::stack::provision_enrolment_tables;
use connetto_test_harness::{
    ConnettoDefaults, Fixture, PUBLICATION, RosterAuth, SLOT, WITHHELD_ID, engine_ddl,
};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::bb8::Pool;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{ParserDB, PgStreamingCdcSource, PgStreamingConfig};

use super::ticket_shared::do_handshake_with;
use super::writer_shared::{PG_DDL, seed_notes, writable_catalog};

/// No subscriptions are made in these tests, so the snapshot source is never
/// invoked.
struct NoSnapshot;

impl SnapshotSource for NoSnapshot {
    type Error = Infallible;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn estimate(
        &self,
        _select_sql: &str,
        _binds: &[connetto_core::messages::BindValue],
        _caller: &Principal,
    ) -> Result<SnapshotEstimate, Self::Error> {
        Ok(SnapshotEstimate {
            rows: 0.0,
            width: 0,
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn snapshot_page(
        &self,
        _select_sql: &str,
        _binds: &[connetto_core::messages::BindValue],
        _auth: &Principal,
        _page: &PageSpec,
    ) -> Result<SnapshotPage, Self::Error> {
        Ok(SnapshotPage {
            patchset: Vec::new(),
            cursor: connetto_core::Cursor::new(Vec::new()),
            next: None,
            filled: false,
            widest_row: 0,
            rows: 0,
            bytes: 0,
        })
    }
}

/// The manager the tests take, its materializer carrying the registry and
/// bookkeeping tables, the one text the publication carries (R77 decision 11).
fn manager(
    pool: &Pool<AsyncPgConnection>,
    enrolment: Option<connetto_server::device_cert::DeviceEnrolment<String>>,
) -> Arc<SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults>> {
    let ddl = engine_ddl(PG_DDL);
    let manager = ManagerBuilder::new(
        Materializer::builder(&ddl)
            .with_write_catalog(writable_catalog())
            .build()
            .expect("build materializer"),
        NoSnapshot,
        RosterAuth::granting("dave")
            .and("bob")
            .withholding(WITHHELD_ID),
        Arc::new(TestGrantChecker),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(pool.clone(), &ddl).expect("build write target"),
    )
    .with_session(SessionConfig::default())
    .with_guard(Arc::new(RequestGuard::new(
        ThrottleConfig::default(),
        AbuseConfig::default(),
    )))
    .build();
    if let Some(enrolment) = enrolment
        && manager
            .install_device_enrolment(Arc::new(enrolment))
            .is_err()
    {
        panic!("the enrolment installs once");
    }
    manager
}
/// Start the change stream on the fixture's publication, its catalog the one
/// text the publication carries (R77 decision 11), so the settle reaches the
/// open question's filters.
fn start_ingest(
    manager: Arc<SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults>>,
    admin_url: String,
) {
    let ddl = engine_ddl(PG_DDL);
    let policy = ReconnectPolicy::new()
        .with_initial_backoff(Duration::from_millis(100))
        .with_max_backoff(Duration::from_secs(2))
        .with_max_attempts(Some(50))
        .with_healthy_after(Duration::from_secs(1));
    tokio::spawn(async move {
        let connect = |resume: ResumePoint| {
            let url = admin_url.clone();
            let ddl = ddl.clone();
            async move {
                let catalog =
                    ParserDB::parse::<PostgreSqlDialect>(&ddl).map_err(|err| format!("{err:?}"))?;
                let config = PgStreamingConfig::new(url, SLOT, PUBLICATION).start(resume.get());
                PgStreamingCdcSource::connect(config, catalog)
                    .await
                    .map_err(|err| err.to_string())
            }
        };
        manager
            .ingest_with_reconnect(connect, &policy, |_| {})
            .await
            .expect("ingest");
    });
}
/// A client's subscription reading one of the bookkeeping tables is refused
/// with the one fixed subscription refusal, so the table's rows (the writer's
/// `secret_hash`, the settled records, the enrolments) never reach the client
/// through the change fan-out (R77 decision 11). `insert` lands one row of the
/// table while the stream runs, so a delivered row would be observable.
async fn a_bookkeeping_subscription_is_refused(
    manager: Arc<SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults>>,
    fixture: &Fixture,
    table: &str,
    insert: &str,
) {
    let (server_transport, mut client) = loopback();
    let _server = tokio::spawn(manager.serve(server_transport));
    do_handshake_with(&mut client, "bk-bob", &["user:bob"]).await;
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "bk-sub".to_owned(),
            spec: SubscriptionSpec::new(format!("SELECT * FROM {table}")),
        }))
        .await
        .expect("send subscribe");
    loop {
        match client.recv().await.expect("recv") {
            Some(IncomingFrame::Control(ControlMessage::NonFatalError(err))) => {
                assert_eq!(
                    err.detail, SUBSCRIPTION_REFUSED,
                    "the refusal names the sub_id"
                );
                assert_eq!(err.related_to.as_deref(), Some("bk-sub"));
                break;
            }
            Some(
                IncomingFrame::Control(
                    ControlMessage::RevocationUpdate(_) | ControlMessage::HandshakeAck(_),
                )
                | IncomingFrame::Bulk(_),
            ) => {}
            other => panic!("expected the subscription refusal, got {other:?}"),
        }
    }
    // A row lands in the table while the stream runs; the refused client
    // receives none of it.
    fixture.exec(insert).await;
    let deadline = SystemTime::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return;
        }
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(Some(IncomingFrame::Bulk(_)))) => {
                panic!("a bulk frame reached a client whose subscription was refused")
            }
            Ok(Ok(Some(IncomingFrame::Control(
                ControlMessage::RevocationUpdate(_) | ControlMessage::HandshakeAck(_),
            )))) => {}
            Ok(Ok(Some(other))) => panic!("unexpected frame after the refusal: {other:?}"),
            Ok(Ok(None)) => panic!("the session closed"),
            Ok(Err(_)) => panic!("the connection errored"),
            Err(_) => return,
        }
    }
}

/// The writer table's `secret_hash` is refused to a subscribing client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writers_subscription_is_refused() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;
    let manager = manager(fixture.admin(), None);
    start_ingest(manager.clone(), fixture.admin_url().to_owned());
    a_bookkeeping_subscription_is_refused(
        manager,
        &fixture,
        "_connetto_writers",
        "INSERT INTO _connetto_writers (user_id) VALUES ('bk')",
    )
    .await;
}

/// The settled records are refused to a subscribing client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settled_subscription_is_refused() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;
    let manager = manager(fixture.admin(), None);
    start_ingest(manager.clone(), fixture.admin_url().to_owned());
    a_bookkeeping_subscription_is_refused(
        manager,
        &fixture,
        "_connetto_settled",
        "INSERT INTO _connetto_settled (writer, seq, entry_hash, previous_hash, outcome, path) \
         VALUES (1, 1, '\\x01', '\\x01', 'applied', 'direct')",
    )
    .await;
}

/// The device enrolments are refused to a subscribing client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_enrolments_subscription_is_refused() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;
    provision_enrolment_tables(&fixture).await;
    let manager = manager(fixture.admin(), None);
    start_ingest(manager.clone(), fixture.admin_url().to_owned());
    a_bookkeeping_subscription_is_refused(
        manager,
        &fixture,
        "connetto_device_enrolments",
        "INSERT INTO connetto_device_enrolments (key_id, user_id, session_id, enrolled_at, \
         last_seen, public_key, attestation) \
         VALUES ('\\x02', 'bk', gen_random_uuid(), now(), now(), '\\x03', 'unproven')",
    )
    .await;
}

/// Startup refuses a publication that leaves out a bookkeeping table the open
/// questions watch, and carries them all once the deployment adds them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflight_refuses_a_publication_without_the_bookkeeping_tables() {
    const PUBLICATION: &str = "r11_bookkeeping_pub";
    let fixture = Fixture::acquire().await;
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS notes CASCADE",
            "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, edited_at TEXT)",
            connetto_test_harness::WRITERS_DDL,
            connetto_test_harness::stack::ENROLMENT_DDL[0],
            "ALTER TABLE notes REPLICA IDENTITY FULL",
            &format!("CREATE PUBLICATION {PUBLICATION} FOR TABLE notes"),
        ])
        .await;

    let missing = preflight::require(
        fixture.admin(),
        &[Artifact::PublishedTable {
            publication: PUBLICATION,
            table: connetto_schema::BOOKKEEPING_TABLES[0],
        }],
    )
    .await
    .expect_err("the publication lacks the writer tables");
    let PreflightError::Missing { noun, name } = &missing else {
        panic!("expected the missing replicated table, got {missing:?}");
    };
    assert_eq!(*noun, "replicated table");
    assert_eq!(*name, connetto_schema::BOOKKEEPING_TABLES[0]);

    // The deployment adds the tables, and the same check passes.
    for table in connetto_schema::BOOKKEEPING_TABLES {
        fixture
            .exec(&format!(
                "ALTER PUBLICATION {PUBLICATION} ADD TABLE {table}"
            ))
            .await;
        fixture
            .exec(&format!("ALTER TABLE {table} REPLICA IDENTITY FULL"))
            .await;
    }
    preflight::require(
        fixture.admin(),
        &[
            Artifact::PublishedTable {
                publication: PUBLICATION,
                table: connetto_schema::BOOKKEEPING_TABLES[0],
            },
            Artifact::PublishedTable {
                publication: PUBLICATION,
                table: connetto_schema::BOOKKEEPING_TABLES[1],
            },
            Artifact::PublishedTable {
                publication: PUBLICATION,
                table: connetto_schema::BOOKKEEPING_TABLES[2],
            },
            Artifact::PreviousImages {
                publication: PUBLICATION,
            },
        ],
    )
    .await
    .expect("the publication now carries the bookkeeping tables");

    fixture
        .exec(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await;
}
