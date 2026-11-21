use std::sync::Arc;
use std::time::Duration;

use connetto_core::SessionId;
use connetto_core::exchange::MAX_MANIFEST_ROWS;
use connetto_core::messages::{
    ControlMessage, CourierAuditAnswer, CourierAuditCancel, CourierAuditOutcome,
    CourierAuditRequest, FatalErrorReason, Grant, Handshake, HandshakeAck, Ping,
};
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::{IncomingFrame, Transport as _};
use connetto_core::version::PROTOCOL_VERSION;
use connetto_server::audit::{AuthEvent, AuthOp, ConnettoAuditSchema};
use connetto_server::defaults::{_connetto_writers, ConnettoAudit};
use connetto_server::{
    LoopbackTransport, ManagerBuilder, Materializer, PgReadConnector, PgSnapshotSource,
    ReconnectEvent, ReconnectPolicy, ResumePoint, SessionManager, loopback, pg_write_target,
};
use connetto_test_harness::{
    ConnettoDefaults, Fixture, PUBLICATION, RosterAuth, SLOT, engine_ddl, provision_audit,
};
use diesel::ExpressionMethods;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, bb8::Pool};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use sqlparser::dialect::PostgreSqlDialect;
use subql::{ParserDB, PgStreamingCdcSource, PgStreamingConfig};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::writer_shared::{PG_DDL, seed_notes, writable_catalog};

const BOUND: Duration = Duration::from_secs(30);
const AUTHOR: [u8; 32] = [41; 32];
type Manager = SessionManager<PgSnapshotSource, RosterAuth, ConnettoDefaults, PgReadConnector>;

async fn bounded<T>(id: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, future)
        .await
        .unwrap_or_else(|_| panic!("deadline elapsed for {id}"))
}

struct Harness {
    fixture: Fixture,
    manager: Arc<Manager>,
    writer: i64,
    ingest: JoinHandle<()>,
    reconnects: mpsc::UnboundedReceiver<&'static str>,
    connections: mpsc::UnboundedReceiver<&'static str>,
    reads: Pool<AsyncPgConnection>,
    writes: Pool<AsyncPgConnection>,
    reconnect_gate: Arc<tokio::sync::Semaphore>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.ingest.abort();
    }
}

async fn author_writer(fixture: &Fixture) -> i64 {
    let mut conn = fixture.admin().get().await.expect("admin connection");
    diesel::insert_into(_connetto_writers::table)
        .values((
            _connetto_writers::user_id.eq("alice"),
            _connetto_writers::device_key.eq(AUTHOR.to_vec()),
            _connetto_writers::frontier.eq(0_i64),
        ))
        .returning(_connetto_writers::writer)
        .get_result(&mut *conn)
        .await
        .expect("insert author writer")
}

impl Harness {
    async fn new() -> Self {
        bounded("audit fixture", async {
            let fixture = Fixture::acquire().await;
            seed_notes(&fixture).await;
            provision_audit(fixture.admin()).await;
            fixture.start_replication(&["notes"]).await;
            let writer = author_writer(&fixture).await;
            let ddl = engine_ddl(PG_DDL);
            let pool = fixture.admin().clone();
            let reads = isolated_pool(fixture.admin_url()).await;
            let receipts_pool = isolated_pool(fixture.admin_url()).await;
            let manager = ManagerBuilder::new(
                Materializer::builder(&ddl)
                    .with_audit_schema::<ConnettoAudit>()
                    .with_write_catalog(writable_catalog())
                    .with_read_connector(PgReadConnector::with_session_setup(reads.clone()))
                    .build()
                    .expect("build audit materializer"),
                PgSnapshotSource::from_ddl(pool.clone(), &ddl).expect("snapshot source"),
                RosterAuth::granting("alice")
                    .and("bob")
                    .and("mallory")
                    .and_the_unnamed_caller(),
                Arc::new(TestGrantChecker),
                PgReadConnector::with_session_setup(reads.clone()),
                pg_write_target::<ConnettoDefaults>(receipts_pool.clone(), &ddl)
                    .expect("write target"),
            )
            .build();
            let (events, reconnects) = mpsc::unbounded_channel();
            let (connected, mut connections) = mpsc::unbounded_channel();
            let reconnect_gate = Arc::new(tokio::sync::Semaphore::new(1));
            let connect_gate = reconnect_gate.clone();
            let ingest_manager = manager.clone();
            let url = fixture.admin_url().to_owned();
            let ingest = tokio::spawn(async move {
                let policy = ReconnectPolicy::new()
                    .with_initial_backoff(Duration::from_millis(100))
                    .with_max_backoff(Duration::from_secs(1))
                    .with_max_attempts(Some(10));
                ingest_manager
                    .ingest_with_reconnect(
                        |resume: ResumePoint| {
                            let url = url.clone();
                            let ddl = ddl.clone();
                            let connected = connected.clone();
                            let connect_gate = connect_gate.clone();
                            async move {
                                bounded("audit-stream-permit", connect_gate.acquire())
                                    .await
                                    .expect("stream permit")
                                    .forget();
                                let catalog = ParserDB::parse::<PostgreSqlDialect>(&ddl)
                                    .map_err(|err| format!("{err:?}"))?;
                                let source = PgStreamingCdcSource::connect(
                                    PgStreamingConfig::new(url, SLOT, PUBLICATION)
                                        .start(resume.get()),
                                    catalog,
                                )
                                .await
                                .map_err(|err| err.to_string())?;
                                let _ = connected.send("audit-stream-connected");
                                Ok::<_, String>(source)
                            }
                        },
                        &policy,
                        |event| {
                            let id = match event {
                                ReconnectEvent::Retrying { .. } => "audit-stream-retrying",
                                ReconnectEvent::ReadRetrying { .. } => "audit-read-retrying",
                                ReconnectEvent::AuthRetrying { .. } => "audit-auth-retrying",
                                ReconnectEvent::GaveUp { .. } => "audit-stream-gave-up",
                            };
                            let _ = events.send(id);
                        },
                    )
                    .await
                    .expect("audit ingestion");
            });
            assert_eq!(connections.recv().await, Some("audit-stream-connected"));
            Self {
                fixture,
                manager,
                writer,
                ingest,
                reconnects,
                connections,
                reads,
                writes: receipts_pool,
                reconnect_gate,
            }
        })
        .await
    }

    async fn client(&self, id: &str, account: Option<&str>) -> LoopbackTransport {
        self.connection(id, account).await.0
    }

    async fn connection(
        &self,
        id: &str,
        account: Option<&str>,
    ) -> (LoopbackTransport, HandshakeAck, JoinHandle<()>) {
        let (server, mut client) = loopback();
        let manager = self.manager.clone();
        let completion = tokio::spawn(async move {
            let _ = manager.serve(server).await;
        });
        let mut handshake = Handshake::new(PROTOCOL_VERSION, id);
        if let Some(account) = account {
            handshake = handshake.with_grant(Grant::new(format!("user:{account}#{id}")));
        }
        send(&mut client, ControlMessage::Handshake(handshake)).await;
        let ack = bounded(id, async {
            assert!(matches!(
                client.recv().await.expect("receive challenge"),
                Some(IncomingFrame::Control(ControlMessage::Challenge(_)))
            ));
            let Some(IncomingFrame::Control(ControlMessage::HandshakeAck(ack))) = client
                .recv()
                .await
                .expect("receive handshake acknowledgement")
            else {
                panic!("expected handshake acknowledgement");
            };
            ack
        })
        .await;
        (client, ack, completion)
    }

    async fn receipt(&self, seq: i64, courier: &str) {
        bounded("commit audit receipt", async {
            let mut conn = self.fixture.admin().get().await.expect("admin connection");
            let mut event = AuthEvent::new(
                SessionId::from_uuid(uuid::Uuid::new_v4()),
                Some("alice".to_owned()),
                AuthOp::WriteCouriered,
            )
            .about_courier(courier.to_owned());
            event.writer = Some(self.writer);
            event.seq = Some(seq);
            ConnettoAudit::audit_insert(event)
                .execute(&mut *conn)
                .await
                .expect("commit courier receipt");
        })
        .await;
    }

    async fn sql(&self, sql: &str) {
        bounded("audit SQL", self.fixture.exec(sql)).await;
    }

    async fn event(&mut self, id: &'static str) {
        assert_eq!(bounded(id, self.reconnects.recv()).await, Some(id));
    }
}

async fn isolated_pool(url: &str) -> Pool<AsyncPgConnection> {
    Pool::builder()
        .max_size(1)
        .min_idle(1)
        .connection_timeout(Duration::from_millis(150))
        .build(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url.to_owned(),
        ))
        .await
        .expect("build isolated audit pool")
}

fn request(id: &str, seqs: &[u64], remaining_ms: u64) -> CourierAuditRequest {
    CourierAuditRequest {
        request_id: id.to_owned(),
        author: AUTHOR,
        seqs: seqs.to_vec(),
        remaining_ms,
    }
}

async fn send(client: &mut LoopbackTransport, message: ControlMessage) {
    bounded("send audit frame", client.send_control(message))
        .await
        .expect("send audit frame");
}

async fn barrier(client: &mut LoopbackTransport, nonce: u64) -> Vec<CourierAuditAnswer> {
    send(client, ControlMessage::Ping(Ping { nonce })).await;
    bounded("audit registration barrier", async {
        let mut answers = Vec::new();
        loop {
            match client.recv().await.expect("receive barrier frame") {
                Some(IncomingFrame::Control(ControlMessage::Pong(pong))) => {
                    assert_eq!(pong.nonce, nonce);
                    return answers;
                }
                Some(IncomingFrame::Control(ControlMessage::CourierAuditAnswer(answer))) => {
                    answers.push(answer);
                }
                Some(IncomingFrame::Control(ControlMessage::RevocationUpdate(_))) => {}
                other => panic!("unexpected barrier frame {other:?}"),
            }
        }
    })
    .await
}

async fn expect_answer(client: &mut LoopbackTransport, id: &str, outcome: CourierAuditOutcome) {
    bounded(id, async {
        loop {
            match client.recv().await.expect("receive audit answer") {
                Some(IncomingFrame::Control(ControlMessage::CourierAuditAnswer(answer))) => {
                    assert_eq!(answer.request_id, id);
                    assert_eq!(answer.outcome, outcome);
                    return;
                }
                Some(IncomingFrame::Control(ControlMessage::RevocationUpdate(_))) => {}
                other => panic!("unexpected audit frame {other:?}"),
            }
        }
    })
    .await;
}

async fn expect_control(client: &mut LoopbackTransport, id: &str, expected: ControlMessage) {
    bounded(id, async {
        loop {
            match client.recv().await.expect("receive ingestion status") {
                Some(IncomingFrame::Control(ControlMessage::RevocationUpdate(_))) => {}
                Some(IncomingFrame::Control(message)) => {
                    assert_eq!(message, expected);
                    return;
                }
                other => panic!("unexpected ingestion frame {other:?}"),
            }
        }
    })
    .await;
}

async fn expect_database_pause(client: &mut LoopbackTransport, id: &str) {
    expect_control(
        client,
        id,
        ControlMessage::DeliveryPaused {
            cause: connetto_core::messages::PauseCause::DatabaseUnreachable,
        },
    )
    .await;
}

async fn register(client: &mut LoopbackTransport, ask: CourierAuditRequest) {
    send(client, ControlMessage::CourierAuditRequest(ask)).await;
    assert_eq!(barrier(client, 1).await, []);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_receipts_confirm_author_and_actual_courier() {
    let h = Harness::new().await;
    h.receipt(1, "bob").await;
    h.receipt(2, "bob").await;
    for account in ["alice", "bob"] {
        let mut client = h.client(account, Some(account)).await;
        send(
            &mut client,
            ControlMessage::CourierAuditRequest(request(account, &[1, 2], 10_000)),
        )
        .await;
        expect_answer(&mut client, account, CourierAuditOutcome::Confirmed).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_after_registration_confirms_the_actual_courier() {
    let h = Harness::new().await;
    let mut client = h.client("after-registration", Some("bob")).await;
    register(&mut client, request("after-registration", &[1], 10_000)).await;
    h.receipt(1, "bob").await;
    expect_answer(
        &mut client,
        "after-registration",
        CourierAuditOutcome::Confirmed,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_and_invisible_receipts_expire_without_confirmation() {
    let h = Harness::new().await;
    h.receipt(1, "bob").await;
    let mut courier = h.client("partial", Some("bob")).await;
    register(&mut courier, request("partial", &[1, 2], 1_000)).await;
    h.receipt(2, "mallory").await;
    expect_answer(&mut courier, "partial", CourierAuditOutcome::Expired).await;
    let mut author = h.client("all-visible", Some("alice")).await;
    send(
        &mut author,
        ControlMessage::CourierAuditRequest(request("all-visible", &[1, 2], 10_000)),
    )
    .await;
    expect_answer(&mut author, "all-visible", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_partial_evidence_must_exist_again_before_confirmation() {
    let h = Harness::new().await;
    h.receipt(1, "bob").await;
    let mut client = h.client("deleted", Some("alice")).await;
    register(&mut client, request("deleted", &[1, 2], 1_000)).await;
    h.sql("DELETE FROM auth_events WHERE seq = 1").await;
    h.receipt(2, "bob").await;
    expect_answer(&mut client, "deleted", CourierAuditOutcome::Expired).await;
    h.receipt(1, "bob").await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("deleted", &[1, 2], 10_000)),
    )
    .await;
    expect_answer(&mut client, "deleted", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_duplicates_keep_the_original_monotonic_deadline() {
    let h = Harness::new().await;
    let mut client = h.client("deadline", Some("alice")).await;
    let started = Instant::now();
    register(&mut client, request("deadline", &[1], 1_000)).await;
    register(&mut client, request("deadline", &[1], 20_000)).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        expect_answer(&mut client, "deadline", CourierAuditOutcome::Expired),
    )
    .await
    .expect("the original deadline remains active");
    assert!(started.elapsed() >= Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_allows_later_committed_evidence_under_the_same_id() {
    let h = Harness::new().await;
    let mut client = h.client("expired", Some("bob")).await;
    register(&mut client, request("expired", &[1], 500)).await;
    expect_answer(&mut client, "expired", CourierAuditOutcome::Expired).await;
    h.receipt(1, "bob").await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("expired", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut client, "expired", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_releases_an_id_without_a_late_timer_closing_its_replacement() {
    let h = Harness::new().await;
    let mut client = h.client("cancel", Some("alice")).await;
    register(&mut client, request("reused", &[1], 500)).await;
    send(
        &mut client,
        ControlMessage::CourierAuditCancel(CourierAuditCancel {
            request_id: "reused".to_owned(),
        }),
    )
    .await;
    assert_eq!(barrier(&mut client, 2).await, []);
    register(&mut client, request("reused", &[2], 10_000)).await;
    register(&mut client, request("past-old-deadline", &[3], 1_000)).await;
    h.receipt(1, "bob").await;
    expect_answer(
        &mut client,
        "past-old-deadline",
        CourierAuditOutcome::Expired,
    )
    .await;
    h.receipt(2, "bob").await;
    expect_answer(&mut client, "reused", CourierAuditOutcome::Confirmed).await;
    send(
        &mut client,
        ControlMessage::CourierAuditCancel(CourierAuditCancel {
            request_id: "reused".to_owned(),
        }),
    )
    .await;
    assert_eq!(barrier(&mut client, 3).await, []);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_or_malformed_duplicates_refuse_and_release_the_id() {
    let h = Harness::new().await;
    let mut client = h.client("changed", Some("alice")).await;
    for change in 0..3 {
        register(&mut client, request("changed", &[1], 10_000)).await;
        let mut changed = request("changed", &[2], 10_000);
        match change {
            0 => {}
            1 => {
                changed.author = [42; 32];
                changed.seqs = vec![1];
            }
            _ => changed.seqs.clear(),
        }
        send(&mut client, ControlMessage::CourierAuditRequest(changed)).await;
        expect_answer(&mut client, "changed", CourierAuditOutcome::Refused).await;
    }
    h.receipt(2, "bob").await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("changed", &[2], 10_000)),
    )
    .await;
    expect_answer(&mut client, "changed", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_inputs_and_unknown_authors_are_refused_without_retaining_work() {
    let h = Harness::new().await;
    let mut client = h.client("invalid", Some("alice")).await;
    let too_many: Vec<_> =
        (1..=u64::try_from(MAX_MANIFEST_ROWS).expect("manifest rows fit") + 1).collect();
    let mut unknown = request("unknown", &[1], 10_000);
    unknown.author = [99; 32];
    let invalid = [
        request("", &[1], 10_000),
        request(&"i".repeat(129), &[1], 10_000),
        request("empty", &[], 10_000),
        request("zero", &[0], 10_000),
        request("descending", &[2, 1], 10_000),
        request("duplicate", &[1, 1], 10_000),
        request("overflow", &[u64::MAX], 10_000),
        request("no-time", &[1], 0),
        request("too-many", &too_many, 10_000),
        unknown,
    ];
    for ask in invalid {
        let id = ask.request_id.clone();
        send(&mut client, ControlMessage::CourierAuditRequest(ask)).await;
        expect_answer(&mut client, &id, CourierAuditOutcome::Refused).await;
    }
    h.receipt(1, "bob").await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("valid", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut client, "valid", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anonymous_observers_are_refused_even_when_receipts_exist() {
    let h = Harness::new().await;
    h.receipt(1, "bob").await;
    let mut client = h.client("anonymous", None).await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("anonymous", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut client, "anonymous", CourierAuditOutcome::Refused).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_ids_on_different_connections_keep_visibility_isolated() {
    let h = Harness::new().await;
    let mut author = h.client("owner-connection", Some("alice")).await;
    let mut stranger = h.client("stranger-connection", Some("mallory")).await;
    register(&mut author, request("shared-id", &[1], 10_000)).await;
    register(&mut stranger, request("shared-id", &[1], 1_000)).await;
    h.receipt(1, "bob").await;
    expect_answer(&mut author, "shared-id", CourierAuditOutcome::Confirmed).await;
    expect_answer(&mut stranger, "shared-id", CourierAuditOutcome::Expired).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_writer_evidence_survives_without_an_enrolment() {
    let h = Harness::new().await;
    let mut client = h.client("retired", Some("bob")).await;
    register(&mut client, request("retired", &[1], 10_000)).await;
    h.sql(&format!(
        "UPDATE _connetto_writers SET retired_at = now() WHERE writer = {}",
        h.writer,
    ))
    .await;
    h.receipt(1, "bob").await;
    expect_answer(&mut client, "retired", CourierAuditOutcome::Confirmed).await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("retired-again", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut client, "retired-again", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unrelated_audit_operations_never_witness_courier_receipts() {
    let h = Harness::new().await;
    let mut client = h.client("unrelated", Some("alice")).await;
    register(&mut client, request("unrelated", &[1], 1_000)).await;
    h.sql(&format!(
        "INSERT INTO auth_events (session, user_id, op, writer, seq, courier_user_id) \
         VALUES ('{}', 'alice', 'write_redelivered', {}, 1, 'bob')",
        uuid::Uuid::new_v4(),
        h.writer,
    ))
    .await;
    h.receipt(2, "bob").await;
    expect_answer(&mut client, "unrelated", CourierAuditOutcome::Expired).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connection_sequence_cap_preserves_other_questions_and_cancel_frees_capacity() {
    let h = Harness::new().await;
    let last = u64::try_from(MAX_MANIFEST_ROWS).expect("manifest bound fits");
    assert_eq!(last, 8_192);
    h.sql(&format!(
        "INSERT INTO auth_events (session, user_id, op, writer, seq, courier_user_id) \
         SELECT '{}', 'alice', 'write_couriered', {}, seq, 'bob' \
         FROM generate_series(1, {}) AS seq",
        uuid::Uuid::new_v4(),
        h.writer,
        last - 1,
    ))
    .await;
    let mut client = h.client("capacity", Some("alice")).await;
    let seqs: Vec<_> = (1..=last).collect();
    for index in 0..8 {
        register(
            &mut client,
            request(&format!("capacity-{index}"), &seqs, 60_000),
        )
        .await;
    }
    register(&mut client, request("capacity-7", &seqs, 60_000)).await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("over-capacity", &[last + 1], 60_000)),
    )
    .await;
    expect_answer(&mut client, "over-capacity", CourierAuditOutcome::Refused).await;
    send(
        &mut client,
        ControlMessage::CourierAuditCancel(CourierAuditCancel {
            request_id: "capacity-0".to_owned(),
        }),
    )
    .await;
    assert_eq!(barrier(&mut client, 2).await, []);
    register(&mut client, request("over-capacity", &[last + 1], 60_000)).await;
    h.receipt(i64::try_from(last + 1).expect("sequence fits"), "bob")
        .await;
    expect_answer(&mut client, "over-capacity", CourierAuditOutcome::Confirmed).await;
    h.receipt(i64::try_from(last).expect("sequence fits"), "bob")
        .await;
    bounded("capacity confirmations", async {
        let mut outstanding: std::collections::HashSet<_> =
            (1..8).map(|index| format!("capacity-{index}")).collect();
        while !outstanding.is_empty() {
            match client.recv().await.expect("receive capacity answer") {
                Some(IncomingFrame::Control(ControlMessage::CourierAuditAnswer(answer))) => {
                    assert!(
                        outstanding.remove(&answer.request_id),
                        "unexpected capacity answer"
                    );
                    assert_eq!(answer.outcome, CourierAuditOutcome::Confirmed);
                }
                Some(IncomingFrame::Control(ControlMessage::RevocationUpdate(_))) => {}
                other => panic!("unexpected capacity frame {other:?}"),
            }
        }
    })
    .await;
    assert_eq!(barrier(&mut client, 3).await, []);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transient_typed_reread_retains_its_delivery_until_ingest_recovery() {
    let mut h = Harness::new().await;
    let mut client = h.client("typed-recovery", Some("alice")).await;
    register(&mut client, request("typed-recovery", &[1], 10_000)).await;
    let writes = h.writes.clone();
    let held = bounded("hold typed read pool", writes.get())
        .await
        .expect("hold typed read pool");
    h.receipt(1, "bob").await;
    h.event("audit-read-retrying").await;
    expect_database_pause(&mut client, "typed-recovery-pause").await;
    drop(held);
    expect_answer(
        &mut client,
        "typed-recovery",
        CourierAuditOutcome::Confirmed,
    )
    .await;
    expect_control(
        &mut client,
        "typed-recovery-resume",
        ControlMessage::DeliveryResumed,
    )
    .await;
    assert_eq!(barrier(&mut client, 2).await, []);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn computed_read_recovery_drains_the_retained_audit_delivery() {
    let mut h = Harness::new().await;
    let mut client = h.client("computed-recovery", Some("bob")).await;
    register(&mut client, request("computed-recovery", &[1], 10_000)).await;
    let reads = h.reads.clone();
    let held = bounded("hold computed pool", reads.get())
        .await
        .expect("hold computed pool");
    h.receipt(1, "bob").await;
    h.event("audit-read-retrying").await;
    expect_database_pause(&mut client, "computed-recovery-pause").await;
    drop(held);
    expect_answer(
        &mut client,
        "computed-recovery",
        CourierAuditOutcome::Confirmed,
    )
    .await;
    expect_control(
        &mut client,
        "computed-recovery-resume",
        ControlMessage::DeliveryResumed,
    )
    .await;
    assert_eq!(barrier(&mut client, 2).await, []);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permanent_typed_read_failure_closes_the_question_and_refuses_new_observations() {
    let h = Harness::new().await;
    let mut client = h.client("typed-failure", Some("alice")).await;
    register(&mut client, request("typed-failure", &[1], 10_000)).await;
    h.sql("ALTER TABLE auth_events RENAME TO unavailable_auth_events")
        .await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("typed-failure", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut client, "typed-failure", CourierAuditOutcome::Failed).await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("opening-failure", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut client, "opening-failure", CourierAuditOutcome::Refused).await;
    h.sql("ALTER TABLE unavailable_auth_events RENAME TO auth_events")
        .await;
    h.receipt(1, "bob").await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("typed-failure", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut client, "typed-failure", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_permanent_computed_failure_closes_and_releases_its_question() {
    let mut h = Harness::new().await;
    let mut client = h.client("computed-failure", Some("alice")).await;
    register(&mut client, request("computed-failure", &[1], 10_000)).await;
    let reads = h.reads.clone();
    let held = bounded("hold computed pool", reads.get())
        .await
        .expect("hold computed pool");
    h.receipt(1, "bob").await;
    h.event("audit-read-retrying").await;
    expect_database_pause(&mut client, "computed-failure-pause").await;
    h.sql("ALTER TABLE auth_events RENAME TO unavailable_auth_events")
        .await;
    drop(held);
    expect_answer(&mut client, "computed-failure", CourierAuditOutcome::Failed).await;
    expect_control(
        &mut client,
        "computed-failure-resume",
        ControlMessage::DeliveryResumed,
    )
    .await;
    h.sql("ALTER TABLE unavailable_auth_events RENAME TO auth_events")
        .await;
    send(
        &mut client,
        ControlMessage::CourierAuditRequest(request("computed-failure", &[1], 10_000)),
    )
    .await;
    expect_answer(
        &mut client,
        "computed-failure",
        CourierAuditOutcome::Confirmed,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_reconnect_keeps_open_observations_and_their_filters() {
    let mut h = Harness::new().await;
    let mut client = h.client("stream-recovery", Some("bob")).await;
    register(&mut client, request("stream-recovery", &[1], 10_000)).await;
    register(&mut client, request("after-reconnect", &[2], 10_000)).await;
    h.sql(&format!(
        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
         WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL",
    ))
    .await;
    h.event("audit-stream-retrying").await;
    h.receipt(1, "bob").await;
    h.reconnect_gate.add_permits(1);
    assert_eq!(
        bounded("audit-stream-connected", h.connections.recv()).await,
        Some("audit-stream-connected"),
    );
    expect_answer(
        &mut client,
        "stream-recovery",
        CourierAuditOutcome::Confirmed,
    )
    .await;
    h.receipt(2, "bob").await;
    expect_answer(
        &mut client,
        "after-reconnect",
        CourierAuditOutcome::Confirmed,
    )
    .await;
}

async fn expect_closed(client: &mut LoopbackTransport, reason: FatalErrorReason) {
    bounded("audit session close", async {
        loop {
            match client.recv().await.expect("receive session close") {
                Some(IncomingFrame::Control(ControlMessage::FatalError(fatal))) => {
                    assert_eq!(fatal.reason, reason);
                    break;
                }
                Some(IncomingFrame::Control(ControlMessage::RevocationUpdate(_))) => {}
                other => panic!("unexpected closing frame {other:?}"),
            }
        }
        assert_eq!(client.recv().await.expect("receive transport end"), None);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_teardown_releases_questions_before_the_same_session_returns() {
    let h = Harness::new().await;
    let (mut client, _, served) = h.connection("reconnect-session", Some("bob")).await;
    register(&mut client, request("reused", &[1], 10_000)).await;
    drop(client);
    bounded("closed audit session", served)
        .await
        .expect("session task");
    h.receipt(1, "bob").await;
    let mut returned = h.client("reconnect-session", Some("bob")).await;
    send(
        &mut returned,
        ControlMessage::CourierAuditRequest(request("reused", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut returned, "reused", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supersession_cannot_close_a_successor_question_with_the_same_id() {
    let h = Harness::new().await;
    let (mut previous, _, served) = h.connection("same-session", Some("alice")).await;
    register(&mut previous, request("reused", &[1], 500)).await;
    let mut successor = h.client("same-session", Some("alice")).await;
    register(&mut successor, request("reused", &[2], 10_000)).await;
    expect_closed(&mut previous, FatalErrorReason::ConnectionSuperseded).await;
    bounded("superseded audit session", served)
        .await
        .expect("session task");
    register(&mut successor, request("past-old-deadline", &[3], 1_000)).await;
    h.receipt(1, "bob").await;
    expect_answer(
        &mut successor,
        "past-old-deadline",
        CourierAuditOutcome::Expired,
    )
    .await;
    h.receipt(2, "bob").await;
    expect_answer(&mut successor, "reused", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_revocation_ends_its_open_audit_questions() {
    let h = Harness::new().await;
    let (mut client, ack, served) = h.connection("revoked", Some("alice")).await;
    register(&mut client, request("revoked", &[1], 10_000)).await;
    let session =
        SessionId::from_uuid(uuid::Uuid::parse_str(&ack.session_token).expect("session handle"));
    assert!(
        bounded(
            "revoke audit session",
            h.manager
                .close_session(session, FatalErrorReason::SessionRevoked),
        )
        .await
    );
    expect_closed(&mut client, FatalErrorReason::SessionRevoked).await;
    bounded("revoked audit session", served)
        .await
        .expect("session task");
    h.receipt(1, "bob").await;
    let mut other = h.client("other-session", Some("alice")).await;
    send(
        &mut other,
        ControlMessage::CourierAuditRequest(request("revoked", &[1], 10_000)),
    )
    .await;
    expect_answer(&mut other, "revoked", CourierAuditOutcome::Confirmed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_shutdown_closes_open_audit_questions() {
    let h = Harness::new().await;
    let (mut client, _, served) = h.connection("shutdown", Some("alice")).await;
    register(&mut client, request("shutdown", &[1], 10_000)).await;
    assert_eq!(
        bounded("shutdown audit manager", h.manager.shutdown()).await,
        1
    );
    expect_closed(&mut client, FatalErrorReason::ServerShuttingDown).await;
    bounded("shutdown audit session", served)
        .await
        .expect("session task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reread_observes_evidence_deleted_since_the_initial_read() {
    let h = Harness::new().await;
    h.receipt(1, "bob").await;
    let mut client = h.client("replacement-evidence", Some("bob")).await;
    register(
        &mut client,
        request("replacement-evidence", &[1, 2], 10_000),
    )
    .await;
    h.sql("DELETE FROM auth_events WHERE seq = 1").await;
    h.receipt(2, "bob").await;
    register(&mut client, request("replacement-evidence", &[1, 2], 9_000)).await;
    h.receipt(1, "bob").await;
    expect_answer(
        &mut client,
        "replacement-evidence",
        CourierAuditOutcome::Confirmed,
    )
    .await;
}
