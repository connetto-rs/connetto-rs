//! Docker-gated tests for the pushed provisional verdict (R77 decision 11).
//!
//! A receiver asks the server about an author's sequence before it settles and
//! gets `Pending`. The server keeps the sequence as an open question for the
//! rest of the connection, and when the author's write settles the change
//! stream wakes the question, which answers the sequence with a further
//! `ProvisionalVerdicts` naming the ask's own `request_id`, so the wire gains
//! no message.
//!
//! Needs Docker: the fixture starts its own Postgres and the test drives real
//! logical replication so the settle reaches the open question.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use connetto_core::Major;
use connetto_core::auth::Principal;
use connetto_core::chain::offer_message;
use connetto_core::device_cert::KeyId;
use connetto_core::exchange::{
    Frontier, MAX_MANIFEST_ROWS, Offer, RetractCause, SignedOffer, Verdict,
};
use connetto_core::messages::{
    ControlMessage, MutationRejectReason, ProvisionalStatus, SUBSCRIPTION_REFUSED, Subscribe,
    SubscriptionSpec, WriterOutcome, WriterRequest, writer_bind_message,
};
use connetto_core::schema::SchemaVersion;
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::{IncomingFrame, Transport as _};
use connetto_server::defaults::_connetto_settled;
use connetto_server::writer_schema::{SettlePath, SettledOutcome};
use connetto_server::{
    AbuseConfig, Artifact, LoopbackTransport, ManagerBuilder, Materializer, NoConnector, PageSpec,
    PreflightError, ReconnectPolicy, RequestGuard, ResumePoint, SessionConfig, SessionManager,
    SnapshotEstimate, SnapshotPage, SnapshotSource, ThrottleConfig, loopback, pg_write_target,
    preflight,
};
use connetto_test_harness::stack::provision_enrolment_tables;
use connetto_test_harness::{
    Client, ConnettoDefaults, Fixture, PUBLICATION, RosterAuth, SLOT, WITHHELD_ID, engine_ddl,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use rcgen::{KeyPair, PublicKeyData as _, SigningKey as _};
use sqlparser::dialect::PostgreSqlDialect;
use subql::{ParserDB, PgStreamingCdcSource, PgStreamingConfig};

use super::enrolment::{device_key, rooted_issuer};
use super::ticket_shared::do_handshake_with;
use super::writer_shared::{Enrolments, PG_DDL, seed_notes, writable_catalog};

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

/// The device key the test signs as, its id and the bind and offer
/// signatures.
struct Key {
    pair: KeyPair,
    id: [u8; 32],
}

impl Key {
    /// The ECDSA P-256 SHA-256 signature over the connection's bind message.
    fn bind_signature(&self, nonce: &[u8; 32]) -> Vec<u8> {
        self.pair
            .sign(&writer_bind_message(nonce))
            .expect("sign the bind message")
            .clone()
    }

    /// The author's offer as the cursor stores it, the `MessagePack` of a
    /// [`SignedOffer`] signed over [`offer_message`].
    fn signed_offer(&self, newest: Option<Frontier>) -> Vec<u8> {
        let offer = Offer {
            builds: vec![(
                Major::new(1).expect("a positive major"),
                SchemaVersion::from_hash(vec![1; 32]),
            )],
            settled: None,
            newest,
            retractions: Vec::new(),
        };
        let message = offer_message(&self.id, &offer);
        let signature = self.pair.sign(&message).expect("sign the offer").clone();
        rmp_serde::to_vec_named(&SignedOffer { offer, signature }).expect("a signed offer encodes")
    }
}

/// The manager every test takes, its materializer carrying the registry and
/// bookkeeping tables, the one text the publication carries (R77 decision 11),
/// so the open question's row filters register.
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

/// Insert one settled record directly and advance the writer's frontier past
/// it, so a test settles a sequence the write path would not reach.
async fn settle(pool: &Pool<AsyncPgConnection>, writer: i64, seq: i64, outcome: SettledOutcome) {
    let mut conn = pool.get().await.expect("admin connection");
    diesel::insert_into(_connetto_settled::table)
        .values((
            _connetto_settled::writer.eq(writer),
            _connetto_settled::seq.eq(seq),
            _connetto_settled::entry_hash.eq(vec![0; 32]),
            _connetto_settled::previous_hash.eq(vec![0; 32]),
            _connetto_settled::outcome.eq(outcome.json()),
            _connetto_settled::path.eq(SettlePath::Direct),
        ))
        .execute(&mut *conn)
        .await
        .expect("insert the settled row");
    diesel::update(
        connetto_server::defaults::_connetto_writers::table
            .filter(connetto_server::defaults::_connetto_writers::writer.eq(writer)),
    )
    .set(connetto_server::defaults::_connetto_writers::frontier.eq(seq))
    .execute(&mut *conn)
    .await
    .expect("advance the frontier");
}

/// The writer number a device key bound to.
async fn writer_number(pool: &Pool<AsyncPgConnection>, key: [u8; 32]) -> i64 {
    let mut conn = pool.get().await.expect("admin connection");
    connetto_server::defaults::_connetto_writers::table
        .filter(connetto_server::defaults::_connetto_writers::device_key.eq(key.to_vec()))
        .select(connetto_server::defaults::_connetto_writers::writer)
        .first::<i64>(&mut *conn)
        .await
        .expect("read the writer number")
}

/// Send `ask` and return the next control frame, past any revocation lists
/// the server pushes unasked.
async fn ask(client: &mut LoopbackTransport, ask: ProvisionalStatus) -> ControlMessage {
    client
        .send_control(ControlMessage::ProvisionalStatus(ask))
        .await
        .expect("send the ask");
    loop {
        match client.recv().await.expect("recv") {
            Some(
                IncomingFrame::Control(
                    ControlMessage::RevocationUpdate(_) | ControlMessage::HandshakeAck(_),
                )
                | IncomingFrame::Bulk(_),
            ) => {}
            Some(IncomingFrame::Control(reply)) => return reply,
            None => panic!("the session closed"),
        }
    }
}

/// The next `ProvisionalVerdicts` for `request_id` within `timeout`, past the
/// first answer and any revocation lists.
async fn expect_push(
    client: &mut LoopbackTransport,
    request_id: &str,
    timeout: Duration,
) -> Vec<(u64, Verdict)> {
    let deadline = SystemTime::now() + timeout;
    loop {
        let remaining = deadline
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        let frame = match tokio::time::timeout(remaining, client.recv()).await {
            Ok(inner) => inner.expect("recv the pushed frame"),
            Err(err) => panic!("no pushed verdict for {request_id} within {timeout:?}: {err:?}"),
        };
        match frame {
            Some(IncomingFrame::Control(ControlMessage::ProvisionalVerdicts(verdicts)))
                if verdicts.request_id == request_id =>
            {
                return verdicts.verdicts;
            }
            Some(
                IncomingFrame::Control(
                    ControlMessage::RevocationUpdate(_) | ControlMessage::HandshakeAck(_),
                )
                | IncomingFrame::Bulk(_),
            ) => {}
            other => panic!("unexpected frame while awaiting the push: {other:?}"),
        }
    }
}

/// Bind `dave`'s device writer and open a pending question on sequence one.
async fn open_pending(
    manager: Arc<SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults>>,
    pool: &Pool<AsyncPgConnection>,
    dave: &Key,
    dave_id: [u8; 32],
    request_id: &str,
) -> (Client, LoopbackTransport, i64) {
    let (server_transport, dave_raw) = loopback();
    let _dave = tokio::spawn(manager.clone().serve(server_transport));
    let mut dave_client = Client::new(dave_raw);
    let (challenge, _ack) = dave_client
        .handshake_challenge_writing("pp-1", &["user:dave"], None, WriterRequest::Device, None)
        .await;
    let answer = dave_client
        .bind_device("pp-dave", dave_id, dave.bind_signature(&challenge.nonce))
        .await;
    assert_eq!(answer.outcome, WriterOutcome::Bound { frontier: 0 });

    let (server_transport, mut bob) = loopback();
    let _bob = tokio::spawn(manager.clone().serve(server_transport));
    do_handshake_with(&mut bob, "pp-bob", &["user:bob"]).await;
    let ControlMessage::ProvisionalVerdicts(first) = ask(
        &mut bob,
        ProvisionalStatus {
            request_id: request_id.to_owned(),
            author: dave_id,
            offer: dave.signed_offer(Some(Frontier {
                seq: 1,
                head: [1; 32],
            })),
            seqs: vec![1],
        },
    )
    .await
    else {
        panic!("expected the first answer");
    };
    assert_eq!(first.request_id, request_id);
    assert_eq!(first.verdicts, vec![(1, Verdict::Pending)]);
    let writer = writer_number(pool, dave_id).await;
    (dave_client, bob, writer)
}

/// A sequence pending when asked settles as an applied write, and the server
/// pushes `Applied` to the ask that asked it, naming the ask's own
/// `request_id`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settled_write_pushes_the_verdict_to_the_open_ask() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, mut bob, writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "pp-q1").await;

    // dave's write settles sequence one, and the change stream pushes it.
    settle(fixture.admin(), writer, 1, SettledOutcome::Applied).await;
    let verdicts = expect_push(&mut bob, "pp-q1", Duration::from_secs(10)).await;
    assert_eq!(verdicts, vec![(1, Verdict::Applied)]);
}

/// A pending sequence that settles as a refusal pushes the refusal's cause to
/// the open ask.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_write_pushes_its_cause_to_the_open_ask() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, mut bob, writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "pr-q1").await;

    // The sequence settles as an unauthorized refusal, and the change stream
    // pushes the cause.
    settle(
        fixture.admin(),
        writer,
        1,
        SettledOutcome::Rejected(MutationRejectReason::Unauthorized),
    )
    .await;
    let verdicts = expect_push(&mut bob, "pr-q1", Duration::from_secs(10)).await;
    assert_eq!(
        verdicts,
        vec![(
            1,
            Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized))
        )]
    );
}

// The enrolment table as this suite's seed and revoke read and write it.
diesel::table! {
    connetto_device_enrolments (key_id) {
        key_id -> Binary,
        user_id -> Text,
        session_id -> Uuid,
        enrolled_at -> Timestamptz,
        last_seen -> Timestamptz,
        revoked_at -> Nullable<Timestamptz>,
        public_key -> Binary,
        attestation -> Text,
    }
}

/// The byte string's lowercase hex, the literal Postgres spells a `bytea`.
fn hex_str(bytes: &[u8]) -> String {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(*byte >> 4)]);
        out.push(HEX[usize::from(*byte & 0x0f)]);
    }
    out
}

/// No `ProvisionalVerdicts` for `request_id` arrives within `timeout`, past
/// any revocation lists the server pushes unasked.
async fn no_push(client: &mut LoopbackTransport, request_id: &str, timeout: Duration) {
    let deadline = SystemTime::now() + timeout;
    loop {
        let remaining = deadline
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return;
        }
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(Some(IncomingFrame::Control(ControlMessage::ProvisionalVerdicts(verdicts)))))
                if verdicts.request_id == request_id =>
            {
                panic!("a pushed verdict for {request_id} arrived, though no question opened")
            }
            Ok(Ok(Some(
                IncomingFrame::Control(
                    ControlMessage::RevocationUpdate(_) | ControlMessage::HandshakeAck(_),
                )
                | IncomingFrame::Bulk(_),
            ))) => {}
            Ok(Ok(Some(other))) => panic!("unexpected frame while expecting no push: {other:?}"),
            Ok(Ok(None)) => panic!("the session closed"),
            Ok(Err(err)) => panic!("the connection errored while expecting no push: {err}"),
            Err(_) => return,
        }
    }
}

/// Bind `dave`'s device writer, returning its connection and writer number.
async fn bind_dave(
    manager: Arc<SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults>>,
    pool: &Pool<AsyncPgConnection>,
    dave: &Key,
    dave_id: [u8; 32],
) -> (Client, i64) {
    let (server_transport, dave_raw) = loopback();
    let _dave = tokio::spawn(manager.clone().serve(server_transport));
    let mut dave_client = Client::new(dave_raw);
    let (challenge, _ack) = dave_client
        .handshake_challenge_writing("pp-1", &["user:dave"], None, WriterRequest::Device, None)
        .await;
    let answer = dave_client
        .bind_device("pp-dave", dave_id, dave.bind_signature(&challenge.nonce))
        .await;
    assert_eq!(answer.outcome, WriterOutcome::Bound { frontier: 0 });
    (dave_client, writer_number(pool, dave_id).await)
}

/// Advance the writer's frontier directly, no settled record beside it, the
/// standing a pruned record leaves.
async fn advance_frontier(pool: &Pool<AsyncPgConnection>, writer: i64, seq: i64) {
    let mut conn = pool.get().await.expect("admin connection");
    diesel::update(
        connetto_server::defaults::_connetto_writers::table
            .filter(connetto_server::defaults::_connetto_writers::writer.eq(writer)),
    )
    .set(connetto_server::defaults::_connetto_writers::frontier.eq(seq))
    .execute(&mut *conn)
    .await
    .expect("advance the frontier");
}

/// An ask about a sequence the settled records already answer gets the final
/// verdict and opens no question, so no push ever follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_final_answer_opens_no_question() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    // The sequence settles before the ask, so the ask's own answer is final.
    let (_dave_client, writer) = bind_dave(manager.clone(), fixture.admin(), &dave, dave_id).await;
    settle(fixture.admin(), writer, 1, SettledOutcome::Applied).await;

    let (server_transport, mut bob) = loopback();
    let _bob = tokio::spawn(manager.clone().serve(server_transport));
    do_handshake_with(&mut bob, "pf-bob", &["user:bob"]).await;
    let ControlMessage::ProvisionalVerdicts(answer) = ask(
        &mut bob,
        ProvisionalStatus {
            request_id: "pf-q1".to_owned(),
            author: dave_id,
            offer: dave.signed_offer(Some(Frontier {
                seq: 1,
                head: [1; 32],
            })),
            seqs: vec![1],
        },
    )
    .await
    else {
        panic!("expected the answer");
    };
    assert_eq!(answer.verdicts, vec![(1, Verdict::Applied)]);
    no_push(&mut bob, "pf-q1", Duration::from_secs(5)).await;
}

/// A sequence its frontier passed with the record pruned pushes `Settled`,
/// so the receiver adjudicates it without a record to retract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pruned_record_pushes_settled_to_the_open_ask() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, mut bob, writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "ps-q1").await;

    // The frontier passes the sequence with no record beside it, as the prune
    // at the author's next handshake leaves it.
    advance_frontier(fixture.admin(), writer, 1).await;
    let verdicts = expect_push(&mut bob, "ps-q1", Duration::from_secs(10)).await;
    assert_eq!(verdicts, vec![(1, Verdict::Settled)]);
}

/// A writer retired with an open question pushes `Abandoned` to the ask the
/// moment the retire lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retired_writer_pushes_abandoned_to_the_open_ask() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, mut bob, writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "rt-q1").await;

    // The writer retires, and the change stream pushes abandoned.
    let mut conn = fixture.admin().get().await.expect("admin connection");
    diesel::update(
        connetto_server::defaults::_connetto_writers::table
            .filter(connetto_server::defaults::_connetto_writers::writer.eq(writer)),
    )
    .set(connetto_server::defaults::_connetto_writers::retired_at.eq(Some(chrono::Utc::now())))
    .execute(&mut *conn)
    .await
    .expect("retire the writer");
    let verdicts = expect_push(&mut bob, "rt-q1", Duration::from_secs(10)).await;
    assert_eq!(verdicts, vec![(1, Verdict::Abandoned)]);
}

/// A certificate at the end of its tolerance window opens the question as
/// pending, and the timer's `Abandoned` push lands the moment its deadline
/// passes, no row changing in the meantime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lapsing_certificate_pushes_abandoned_when_its_deadline_passes() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    // The certificate expired ten seconds before its tolerance window closes,
    // so the ask still reads the author live and its deadline passes shortly.
    let lapsing = SystemTime::now() - connetto_core::clock::DEVICE_SKEW + Duration::from_secs(10);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", lapsing).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, mut bob, _writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "ex-q1").await;

    // No row changes; the timer's deadline passes and re-reads the author.
    let verdicts = expect_push(&mut bob, "ex-q1", Duration::from_secs(30)).await;
    assert_eq!(verdicts, vec![(1, Verdict::Abandoned)]);
}

/// A certificate lapsing soon still answers the ask as pending, and once the
/// sequence settles and closes the author before the deadline, the aborted
/// timer pushes nothing when the deadline later passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_author_pushes_nothing_when_its_deadline_passes() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    // The certificate's tolerance window closes twenty seconds after the ask,
    // so its timer would push `Abandoned` if close did not abort it.
    let lapsing = SystemTime::now() - connetto_core::clock::DEVICE_SKEW + Duration::from_secs(20);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", lapsing).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, mut bob, writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "cx-q1").await;

    // The sequence settles while the certificate is still live, so the push is
    // the settled verdict and the author closes with its timer.
    settle(fixture.admin(), writer, 1, SettledOutcome::Applied).await;
    let verdicts = expect_push(&mut bob, "cx-q1", Duration::from_secs(10)).await;
    assert_eq!(verdicts, vec![(1, Verdict::Applied)]);

    // The certificate's deadline passes while the question is closed, and the
    // aborted timer pushes nothing.
    no_push(&mut bob, "cx-q1", Duration::from_secs(30)).await;
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

/// A revocation that lands in the enrolment table while a question is open
/// pushes `Abandoned` to the ask, the store and the stream reading one row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_enrolment_pushes_abandoned_to_the_open_ask() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;
    provision_enrolment_tables(&fixture).await;

    // The enrolment stands in the deployment's own tables, so the revocation
    // the stream carries and the record the server reads are one row.
    let pair = device_key();
    let spki = pair.subject_public_key_info().clone();
    let key_id = *KeyId::of_public_key(&spki).as_bytes();
    let key_hex = hex_str(&key_id);
    let spki_hex = hex_str(&spki);
    fixture
        .exec(&format!(
            "INSERT INTO connetto_device_enrolments (key_id, user_id, session_id, \
             enrolled_at, last_seen, public_key, attestation) \
             VALUES ('\\x{key_hex}', 'dave', '{session}', now(), now(), \
                     '\\x{spki_hex}', 'unproven')",
            session = uuid::Uuid::new_v4(),
        ))
        .await;
    fixture
        .exec(&format!(
            "INSERT INTO connetto_device_certificates (serial, key_id, issuer, expires_at) \
             VALUES ('\\x{serial_hex}', '\\x{key_hex}', '\\x{issuer_hex}', \
                     now() + interval '24 hours')",
            serial_hex = hex_str(&[7u8; 16]),
            issuer_hex = hex_str(&[9u8; 32]),
        ))
        .await;

    let (_, _, issuer) = rooted_issuer();
    let store = connetto_server::device_cert::pg_enrolment_store::<ConnettoDefaults>(
        fixture.admin().clone(),
    );
    let manager = manager(
        fixture.admin(),
        Some(connetto_server::device_cert::DeviceEnrolment::new(
            connetto_server::device_cert::DeviceCertConfig::new(issuer),
            store,
        )),
    );
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let dave = Key { pair, id: key_id };
    let (_dave_client, mut bob, _writer) =
        open_pending(manager, fixture.admin(), &dave, key_id, "rv-q1").await;

    // The revocation lands in the table, and the change stream pushes abandoned.
    let mut conn = fixture.admin().get().await.expect("admin connection");
    diesel::update(
        connetto_device_enrolments::table
            .filter(connetto_device_enrolments::key_id.eq(key_id.to_vec())),
    )
    .set(connetto_device_enrolments::revoked_at.eq(Some(chrono::Utc::now())))
    .execute(&mut *conn)
    .await
    .expect("revoke the enrolment");
    let verdicts = expect_push(&mut bob, "rv-q1", Duration::from_secs(10)).await;
    assert_eq!(verdicts, vec![(1, Verdict::Abandoned)]);
}

/// An ask whose open sequences would pass the session's bound is refused as
/// a whole, the refusal naming the ask, and it opens nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ask_beyond_the_open_bound_is_refused_naming_the_ask() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    // The offer signs a head high enough for nine manifest frames of asks.
    let frame = u64::try_from(MAX_MANIFEST_ROWS).expect("the manifest bound fits a sequence");
    let newest = frame * 9;
    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, _writer) = bind_dave(manager.clone(), fixture.admin(), &dave, dave_id).await;

    let (server_transport, mut bob) = loopback();
    let _bob = tokio::spawn(manager.clone().serve(server_transport));
    do_handshake_with(&mut bob, "pb-bob", &["user:bob"]).await;

    let offer = dave.signed_offer(Some(Frontier {
        seq: newest,
        head: [1; 32],
    }));
    // Eight frames open the whole bound, each answered pending.
    for chunk in 1..=8u32 {
        let first = u64::from(chunk - 1) * frame + 1;
        let last = u64::from(chunk) * frame;
        let ControlMessage::ProvisionalVerdicts(answer) = ask(
            &mut bob,
            ProvisionalStatus {
                request_id: format!("pb-q{chunk}"),
                author: dave_id,
                offer: offer.clone(),
                seqs: (first..=last).collect(),
            },
        )
        .await
        else {
            panic!("expected the pending answer");
        };
        assert_eq!(answer.verdicts.len(), MAX_MANIFEST_ROWS);
        assert!(
            answer
                .verdicts
                .iter()
                .all(|(_, verdict)| { *verdict == Verdict::Pending })
        );
    }
    // The ninth frame would pass the bound, so the ask is refused naming it.
    let first = u64::from(8u32) * frame + 1;
    let ControlMessage::NonFatalError(error) = ask(
        &mut bob,
        ProvisionalStatus {
            request_id: "pb-q9".to_owned(),
            author: dave_id,
            offer,
            seqs: (first..=newest).collect(),
        },
    )
    .await
    else {
        panic!("expected the bound refusal");
    };
    assert_eq!(error.related_to.as_deref(), Some("pb-q9"));
}

/// A connection that ends with an open question drops it, and the next ask
/// about the same author answers from the settled records alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_session_drops_its_open_question() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    let (_dave_client, bob, writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "cs-q1").await;
    drop(bob);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The sequence settles with the question's connection gone, and a fresh
    // session asks about it, answered from the settled records.
    settle(fixture.admin(), writer, 1, SettledOutcome::Applied).await;

    let (server_transport, mut bob2) = loopback();
    let _bob2 = tokio::spawn(manager.clone().serve(server_transport));
    do_handshake_with(&mut bob2, "cs-bob2", &["user:bob"]).await;
    let ControlMessage::ProvisionalVerdicts(answer) = ask(
        &mut bob2,
        ProvisionalStatus {
            request_id: "cs-q2".to_owned(),
            author: dave_id,
            offer: dave.signed_offer(Some(Frontier {
                seq: 1,
                head: [1; 32],
            })),
            seqs: vec![1],
        },
    )
    .await
    else {
        panic!("expected the settled answer");
    };
    assert_eq!(answer.verdicts, vec![(1, Verdict::Applied)]);
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

/// Terminate the walsender holding the slot, simulating a dropped stream. The
/// typed DSL cannot name a vendor function over the `pg_replication_slots`
/// system view, so the statement stays raw, the way `drop_slot` spells it.
async fn drop_walsender(pool: &Pool<AsyncPgConnection>) {
    connetto_test_harness::exec(
        pool,
        &format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL"
        ),
    )
    .await;
}

/// A change stream that drops and reconnects keeps pushing the pending
/// sequences. One that settles while the stream is down reaches the ask
/// through the reconnect's re-read, and one that settles after the reattach
/// reaches it through the question's row filters, which stay registered
/// across the drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconnect_pushes_what_settled_while_the_stream_was_down() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    fixture.start_replication(&["notes"]).await;

    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (dave_pair, dave_id) = enrol.enrol("dave", live).await;
    let dave = Key {
        pair: dave_pair,
        id: dave_id,
    };
    let manager = manager(fixture.admin(), Some(enrol.installable()));
    start_ingest(manager.clone(), fixture.admin_url().to_owned());

    // Two pending sequences under the same author, one ask each, so the drop
    // leaves both open under one set of row filters.
    let (_dave_client, mut bob, writer) =
        open_pending(manager.clone(), fixture.admin(), &dave, dave_id, "rr-q1").await;
    let ControlMessage::ProvisionalVerdicts(second) = ask(
        &mut bob,
        ProvisionalStatus {
            request_id: "rr-q2".to_owned(),
            author: dave_id,
            offer: dave.signed_offer(Some(Frontier {
                seq: 2,
                head: [2; 32],
            })),
            seqs: vec![2],
        },
    )
    .await
    else {
        panic!("expected the second answer");
    };
    assert_eq!(second.request_id, "rr-q2");
    assert_eq!(second.verdicts, vec![(2, Verdict::Pending)]);

    // Drop the stream, then settle the first sequence while it is down.
    drop_walsender(fixture.admin()).await;
    settle(fixture.admin(), writer, 1, SettledOutcome::Applied).await;
    // Wait past the reconnect backoff so the stream is back and the
    // reconnect's re-read has run.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The reconnect's re-read pushes what settled during the gap.
    let verdicts = expect_push(&mut bob, "rr-q1", Duration::from_secs(10)).await;
    assert_eq!(verdicts, vec![(1, Verdict::Applied)]);

    // The question stayed open, so a settle after the reattach is pushed too.
    settle(fixture.admin(), writer, 2, SettledOutcome::Applied).await;
    let verdicts = expect_push(&mut bob, "rr-q2", Duration::from_secs(10)).await;
    assert_eq!(verdicts, vec![(2, Verdict::Applied)]);
}
