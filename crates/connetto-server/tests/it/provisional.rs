//! Docker-gated tests for the ruling on a provisional ask (R77 decision 11).
//!
//! A receiver who never met the author again asks the server about the
//! author's provisional sequences. The server rules each sequence from the
//! author's writer's settled records and its enrolment, answering a verified
//! ask with one verdict per sequence and refusing what it cannot verify.
//!
//! Needs Docker: the fixture starts its own Postgres.

#![expect(
    clippy::too_many_lines,
    reason = "each test walks one row of the ruling table in order and a split would hide the sequence"
)]

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use connetto_core::Major;
use connetto_core::auth::Principal;
use connetto_core::chain::offer_message;
use connetto_core::exchange::{Frontier, Offer, RetractCause, SignedOffer, Verdict};
use connetto_core::messages::{
    ControlMessage, MutationRejectReason, PROVISIONAL_ASK_REFUSED, ProvisionalStatus,
    WriterOutcome, WriterRequest, writer_bind_message,
};
use connetto_core::schema::SchemaVersion;
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::{IncomingFrame, Transport as _};
use connetto_server::defaults::{_connetto_settled, _connetto_writers};
use connetto_server::device_cert::DeviceEnrolment;
use connetto_server::writer_schema::{SettlePath, SettledOutcome, StoredValue};
use connetto_server::{
    AbuseConfig, LoopbackTransport, ManagerBuilder, Materializer, NoConnector, PageSpec,
    RequestGuard, SessionConfig, SessionManager, SnapshotEstimate, SnapshotPage, SnapshotSource,
    ThrottleConfig, loopback, pg_write_target,
};
use connetto_test_harness::{
    Client, ConnettoDefaults, Fixture, RosterAuth, WITHHELD_ID, issue_writer, number_writer,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use rcgen::{KeyPair, SigningKey as _};

use super::writer_shared::{
    Enrolments, PG_DDL, insert_changeset, seed_notes, writable_catalog, writer_for_key,
};

/// The handshake authority every test manager takes.
fn test_verifier() -> std::sync::Arc<dyn connetto_core::HandshakeAuthority> {
    std::sync::Arc::new(TestGrantChecker)
}

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

/// One settled entry, read as admin, ordered by sequence.
struct SettledFacts {
    seq: i64,
    outcome: String,
}

/// The settled rows of `writer`, ordered by sequence.
async fn settled_rows(pool: &Pool<AsyncPgConnection>, writer: i64) -> Vec<SettledFacts> {
    let mut conn = pool.get().await.expect("admin connection");
    _connetto_settled::table
        .filter(_connetto_settled::writer.eq(writer))
        .select((_connetto_settled::seq, _connetto_settled::outcome))
        .order(_connetto_settled::seq.asc())
        .load::<(i64, String)>(&mut *conn)
        .await
        .expect("read the settled rows")
        .into_iter()
        .map(|(seq, outcome)| SettledFacts { seq, outcome })
        .collect()
}

/// Whether the writer row still stands.
async fn writer_row_stands(pool: &Pool<AsyncPgConnection>, writer: i64) -> bool {
    let mut conn = pool.get().await.expect("admin connection");
    _connetto_writers::table
        .filter(_connetto_writers::writer.eq(writer))
        .select(_connetto_writers::writer)
        .first::<i64>(&mut *conn)
        .await
        .is_ok()
}

/// Insert one settled record directly, so a test stands a record the write
/// path would not reach.
async fn insert_settled(
    pool: &Pool<AsyncPgConnection>,
    writer: i64,
    seq: i64,
    outcome: SettledOutcome,
) {
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
}

/// Stand a writer row directly at `frontier`, so a test rules a writer the
/// bind path would not reach.
async fn insert_writer_row(
    pool: &Pool<AsyncPgConnection>,
    user: &str,
    key_id: [u8; 32],
    frontier: i64,
) -> i64 {
    let mut conn = pool.get().await.expect("admin connection");
    diesel::insert_into(_connetto_writers::table)
        .values((
            _connetto_writers::user_id.eq(user),
            _connetto_writers::device_key.eq(key_id.to_vec()),
            _connetto_writers::frontier.eq(frontier),
        ))
        .returning(_connetto_writers::writer)
        .get_result::<i64>(&mut *conn)
        .await
        .expect("insert the writer row")
}

/// Move a writer's frontier directly.
async fn set_frontier(pool: &Pool<AsyncPgConnection>, writer: i64, frontier: i64) {
    let mut conn = pool.get().await.expect("admin connection");
    diesel::update(_connetto_writers::table.filter(_connetto_writers::writer.eq(writer)))
        .set(_connetto_writers::frontier.eq(frontier))
        .execute(&mut *conn)
        .await
        .expect("set the frontier");
}

/// The device key the test signs as, its id and both the bind and the offer
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
        self.signed_offer_bytes(&offer)
    }

    /// The offer's `MessagePack` signed by `signer` rather than the author's
    /// own key, the forgery the server refuses.
    fn signed_offer_by(&self, signer: &KeyPair, newest: Option<Frontier>) -> Vec<u8> {
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
        let signature = signer.sign(&message).expect("sign the offer").clone();
        rmp_serde::to_vec_named(&SignedOffer { offer, signature }).expect("a signed offer encodes")
    }

    fn signed_offer_bytes(&self, offer: &Offer) -> Vec<u8> {
        let message = offer_message(&self.id, offer);
        let signature = self.pair.sign(&message).expect("sign the offer").clone();
        rmp_serde::to_vec_named(&SignedOffer {
            offer: offer.clone(),
            signature,
        })
        .expect("a signed offer encodes")
    }
}

impl Enrolments {
    /// Enrol `user`'s fresh key and hand back the suite's [`Key`] and its id.
    async fn enrol_key(&self, user: &str, expires_at: SystemTime) -> (Key, [u8; 32]) {
        let (pair, id) = self.enrol(user, expires_at).await;
        (Key { pair, id }, id)
    }
}

fn manager(
    pool: &Pool<AsyncPgConnection>,
    guard: Arc<RequestGuard<String>>,
    enrolment: Option<DeviceEnrolment<String>>,
) -> Arc<SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults>> {
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL)
            .with_write_catalog(writable_catalog())
            .build()
            .expect("build materializer"),
        NoSnapshot,
        RosterAuth::granting("alice")
            .and("bob")
            .and_the_unnamed_caller()
            .withholding(WITHHELD_ID),
        test_verifier(),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(pool.clone(), PG_DDL).expect("build write target"),
    )
    .with_session(SessionConfig::default())
    .with_guard(guard)
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

/// The durable-apply acknowledgement of `client_seq`.
async fn expect_applied(client: &mut Client, client_seq: u64) {
    let ControlMessage::MutationApplied(applied) = client.next_control().await else {
        panic!("expected write {client_seq} to apply");
    };
    assert_eq!(applied.client_seq, client_seq);
}

/// Send `ask` and return the next control frame, past any revocation lists
/// the server pushes unasked.
async fn ask_provisional(client: &mut LoopbackTransport, ask: ProvisionalStatus) -> ControlMessage {
    client
        .send_control(ControlMessage::ProvisionalStatus(ask))
        .await
        .expect("send the ask");
    loop {
        match client.recv().await.expect("recv") {
            Some(
                IncomingFrame::Control(ControlMessage::RevocationUpdate(_))
                | IncomingFrame::Bulk(_),
            ) => {}
            Some(IncomingFrame::Control(reply)) => return reply,
            None => panic!("the session closed"),
        }
    }
}

/// A live writer's ruling answers every row of the verdict table from the
/// writer's settled records and its enrolment, in the ask's order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ruling_answers_every_row_of_the_verdict_table() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default(),
        AbuseConfig::default(),
    ));
    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (alice, alice_id) = enrol.enrol_key("alice", live).await;
    let (carol, carol_id) = enrol.enrol_key("carol", live).await;
    let (dave, dave_id) = enrol.enrol_key("dave", live).await;
    enrol.revoke(carol_id);
    let manager = manager(fixture.admin(), guard, Some(enrol.installable()));

    // alice binds her device writer and settles one applied entry, sequence 1.
    let (server_transport, client) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    let mut client = Client::new(client);
    let (challenge, ack) = client
        .handshake_challenge_writing("pv-1", &["user:alice"], None, WriterRequest::Device, None)
        .await;
    assert!(
        ack.writer.is_none(),
        "a device request takes no number grant"
    );
    let answer = client
        .bind_device("b1", alice_id, alice.bind_signature(&challenge.nonce))
        .await;
    assert_eq!(answer.outcome, WriterOutcome::Bound { frontier: 0 });
    client.upload(1, insert_changeset(2, "two", "t1")).await;
    expect_applied(&mut client, 1).await;
    client.close().await;
    server.await.expect("join alice").expect("session ok");

    let writer = writer_for_key(fixture.admin(), alice_id).await;
    // Sequences 2 and 3 stand as refusals and the frontier stands at 5, so
    // the recordless 4 and 5 settled.
    insert_settled(
        fixture.admin(),
        writer,
        2,
        SettledOutcome::Rejected(MutationRejectReason::Unauthorized),
    )
    .await;
    insert_settled(
        fixture.admin(),
        writer,
        3,
        SettledOutcome::Conflict {
            table: "notes".to_owned(),
            version_column: "edited_at".to_owned(),
            pk_columns: vec!["id".to_owned()],
            pk_values: vec![StoredValue::Integer(1)],
        },
    )
    .await;
    set_frontier(fixture.admin(), writer, 5).await;
    // carol's writer row stands at frontier 2 and her enrolment is revoked.
    insert_writer_row(fixture.admin(), "carol", carol_id, 2).await;

    // bob, who never met the author, asks.
    let (server_transport, mut recv) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    super::ticket_shared::do_handshake_with(&mut recv, "pv-recv", &["user:bob"]).await;

    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "p1".to_owned(),
            author: alice_id,
            offer: alice.signed_offer(Some(Frontier {
                seq: 5,
                head: [5; 32],
            })),
            seqs: vec![1, 2, 3, 4, 5],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling, got {reply:?}");
    };
    assert_eq!(verdicts.request_id, "p1");
    assert_eq!(
        verdicts.verdicts,
        vec![
            (1, Verdict::Applied),
            (
                2,
                Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized))
            ),
            (3, Verdict::Refused(RetractCause::Conflict)),
            (4, Verdict::Settled),
            (5, Verdict::Settled),
        ]
    );

    // carol is revoked, so a recordless sequence below the frontier settled
    // and one above it abandoned.
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "p2".to_owned(),
            author: carol_id,
            offer: carol.signed_offer(Some(Frontier {
                seq: 3,
                head: [3; 32],
            })),
            seqs: vec![1, 3],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling, got {reply:?}");
    };
    assert_eq!(verdicts.request_id, "p2");
    assert_eq!(
        verdicts.verdicts,
        vec![(1, Verdict::Settled), (3, Verdict::Abandoned)]
    );

    // dave stands enrolled but never bound a writer, so his sequence above
    // the absent frontier stays pending.
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "p3".to_owned(),
            author: dave_id,
            offer: dave.signed_offer(Some(Frontier {
                seq: 1,
                head: [1; 32],
            })),
            seqs: vec![1],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling, got {reply:?}");
    };
    assert_eq!(verdicts.request_id, "p3");
    assert_eq!(verdicts.verdicts, vec![(1, Verdict::Pending)]);

    // An author whose every write was acknowledged signs no newest, and the
    // ask about a sequence its settled frontier covers is ruled all the same
    // (R78 decision 52).
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "p4".to_owned(),
            author: dave_id,
            offer: dave.signed_offer_bytes(&Offer {
                builds: vec![(
                    Major::new(1).expect("a positive major"),
                    SchemaVersion::from_hash(vec![1; 32]),
                )],
                settled: Some(Frontier {
                    seq: 1,
                    head: [1; 32],
                }),
                newest: None,
                retractions: Vec::new(),
            }),
            seqs: vec![1],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling for a settled-only offer, got {reply:?}");
    };
    assert_eq!(verdicts.request_id, "p4");
    assert_eq!(verdicts.verdicts, vec![(1, Verdict::Pending)]);

    let _ = recv.close().await;
    server.await.expect("join bob").expect("session ok");
}

/// An ask the server cannot verify is refused non-fatal, the detail naming
/// the refusal and the session standing for a clean ask after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_overbound_or_forged_ask_is_refused() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default(),
        AbuseConfig::default(),
    ));
    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (x1, x1_id) = enrol.enrol_key("x1", live).await;
    let (x2, x2_id) = enrol.enrol_key("x2", live).await;
    let (x3, x3_id) = enrol.enrol_key("x3", live).await;
    let (_x4, x4_id) = enrol.enrol_key("x4", live).await;
    let manager = manager(fixture.admin(), guard, Some(enrol.installable()));

    let (server_transport, mut recv) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    super::ticket_shared::do_handshake_with(&mut recv, "pv-recv", &["user:bob"]).await;

    // x1, a valid offer but one sequence past the manifest bound.
    let bound = connetto_core::exchange::MAX_MANIFEST_ROWS;
    let seqs: Vec<u64> = (0..=bound as u64).collect();
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "q1".to_owned(),
            author: x1_id,
            offer: x1.signed_offer(Some(Frontier {
                seq: bound as u64,
                head: [1; 32],
            })),
            seqs,
        },
    )
    .await;
    let ControlMessage::NonFatalError(err) = reply else {
        panic!("expected the refusal, got {reply:?}");
    };
    assert_eq!(err.related_to.as_deref(), Some("q1"));
    assert_eq!(err.detail, PROVISIONAL_ASK_REFUSED);

    // x2, an offer signed by another key.
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "q2".to_owned(),
            author: x2_id,
            offer: x2.signed_offer_by(
                &x3.pair,
                Some(Frontier {
                    seq: 1,
                    head: [1; 32],
                }),
            ),
            seqs: vec![1],
        },
    )
    .await;
    let ControlMessage::NonFatalError(err) = reply else {
        panic!("expected the refusal, got {reply:?}");
    };
    assert_eq!(err.related_to.as_deref(), Some("q2"));
    assert_eq!(err.detail, PROVISIONAL_ASK_REFUSED);

    // x3, a valid offer but one sequence past its signed newest.
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "q3".to_owned(),
            author: x3_id,
            offer: x3.signed_offer(Some(Frontier {
                seq: 1,
                head: [1; 32],
            })),
            seqs: vec![2],
        },
    )
    .await;
    let ControlMessage::NonFatalError(err) = reply else {
        panic!("expected the refusal, got {reply:?}");
    };
    assert_eq!(err.related_to.as_deref(), Some("q3"));

    // x4, an offer that is not the MessagePack of a signed offer.
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "q4".to_owned(),
            author: x4_id,
            offer: b"garbage".to_vec(),
            seqs: vec![1],
        },
    )
    .await;
    let ControlMessage::NonFatalError(err) = reply else {
        panic!("expected the refusal, got {reply:?}");
    };
    assert_eq!(err.related_to.as_deref(), Some("q4"));

    // the session outlives the refusals, a clean ask of x1 stands.
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "q5".to_owned(),
            author: x1_id,
            offer: x1.signed_offer(Some(Frontier {
                seq: 1,
                head: [1; 32],
            })),
            seqs: vec![1],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling, got {reply:?}");
    };
    assert_eq!(verdicts.request_id, "q5");
    assert_eq!(verdicts.verdicts, vec![(1, Verdict::Pending)]);

    let _ = recv.close().await;
    server.await.expect("join bob").expect("session ok");
}

/// Settled records that cannot be read fail the ask and end the session,
/// rather than ruling a recordless sequence below the frontier settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_record_table_fails_the_ask() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default(),
        AbuseConfig::default(),
    ));
    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (x1, x1_id) = enrol.enrol_key("x1", live).await;
    let manager = manager(fixture.admin(), guard, Some(enrol.installable()));
    insert_writer_row(fixture.admin(), "x1", x1_id, 2).await;
    fixture
        .setup(&["DROP TABLE _connetto_settled CASCADE"])
        .await;

    let (server_transport, mut recv) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    super::ticket_shared::do_handshake_with(&mut recv, "pv-recv", &["user:bob"]).await;
    recv.send_control(ControlMessage::ProvisionalStatus(ProvisionalStatus {
        request_id: "s1".to_owned(),
        author: x1_id,
        offer: x1.signed_offer(Some(Frontier {
            seq: 2,
            head: [2; 32],
        })),
        seqs: vec![1],
    }))
    .await
    .expect("send the ask");
    let error = server
        .await
        .expect("join bob")
        .expect_err("the session ends on the failed read");
    assert!(
        matches!(error, connetto_server::SessionError::WriteTarget(_)),
        "got {error:?}"
    );
}

/// A follow-up ask for the same author on the same session gets its own
/// ruling, the way the receiver's next chunk is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follow_up_ask_for_the_same_author_gets_its_own_verdicts() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default(),
        AbuseConfig::default(),
    ));
    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (x1, x1_id) = enrol.enrol_key("x1", live).await;
    let manager = manager(fixture.admin(), guard, Some(enrol.installable()));

    let (server_transport, mut recv) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    super::ticket_shared::do_handshake_with(&mut recv, "pv-recv", &["user:bob"]).await;

    // the first ask stands.
    let offer = x1.signed_offer(Some(Frontier {
        seq: 2,
        head: [1; 32],
    }));
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "r1".to_owned(),
            author: x1_id,
            offer: offer.clone(),
            seqs: vec![1],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling, got {reply:?}");
    };
    assert_eq!(verdicts.request_id, "r1");
    assert_eq!(verdicts.verdicts, vec![(1, Verdict::Pending)]);

    // the follow-up ask for the same author gets its own ruling.
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "r2".to_owned(),
            author: x1_id,
            offer,
            seqs: vec![2],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling, got {reply:?}");
    };
    assert_eq!(verdicts.request_id, "r2");
    assert_eq!(verdicts.verdicts, vec![(2, Verdict::Pending)]);

    let _ = recv.close().await;
    server.await.expect("join bob").expect("session ok");
}

/// A prune and the revocation sweep drop only the applied records, the
/// refusals standing past them so the receiver still reads their cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prune_keeps_the_refusals_while_the_writer_row_stands() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default(),
        AbuseConfig::default(),
    ));
    let live = SystemTime::now() + Duration::from_hours(24);
    let enrol = Enrolments::new();
    let (alice, alice_id) = enrol.enrol_key("alice", live).await;
    let manager = manager(fixture.admin(), guard, Some(enrol.installable()));
    let admin = fixture.admin();

    // (a) the handshake prune of a number writer drops the applied record
    // below the ack, keeping the refusal.
    let (server_transport, client) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    let mut client = Client::new(client);
    let issue_ack = client
        .handshake_challenge_writing("pv-prune-1", &["user:alice"], None, issue_writer(), None)
        .await
        .1;
    let number = issue_ack
        .writer
        .map(|grant| grant.number)
        .expect("the issue names a number");
    client.close().await;
    server.await.expect("join issue").expect("session ok");

    let number_writer_id = i64::try_from(number).expect("the number fits");
    insert_settled(admin, number_writer_id, 1, SettledOutcome::Applied).await;
    insert_settled(
        admin,
        number_writer_id,
        2,
        SettledOutcome::Rejected(MutationRejectReason::Unauthorized),
    )
    .await;
    set_frontier(admin, number_writer_id, 3).await;

    let (server_transport, client) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    let mut client = Client::new(client);
    let _ = client
        .handshake_challenge_writing(
            "pv-prune-2",
            &["user:alice"],
            None,
            number_writer(number),
            Some(2),
        )
        .await;
    client.close().await;
    server.await.expect("join prune").expect("session ok");

    let rows = settled_rows(admin, number_writer_id).await;
    assert_eq!(rows.len(), 1, "only the refusal stands");
    assert_eq!(rows[0].seq, 2);
    assert_eq!(
        rows[0].outcome,
        SettledOutcome::Rejected(MutationRejectReason::Unauthorized).json()
    );
    assert!(
        writer_row_stands(admin, number_writer_id).await,
        "the writer row stands"
    );

    // (b) the revocation sweep drops the applied record of a revoked device
    // writer, keeping the refusal, and the receiver still reads its cause.
    let (server_transport, client) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    let mut client = Client::new(client);
    let (challenge, _ack) = client
        .handshake_challenge_writing(
            "pv-prune-3",
            &["user:alice"],
            None,
            WriterRequest::Device,
            None,
        )
        .await;
    let answer = client
        .bind_device("b1", alice_id, alice.bind_signature(&challenge.nonce))
        .await;
    assert_eq!(answer.outcome, WriterOutcome::Bound { frontier: 0 });
    client.upload(1, insert_changeset(2, "two", "t1")).await;
    expect_applied(&mut client, 1).await;
    client.close().await;
    server.await.expect("join alice").expect("session ok");

    let device_writer = writer_for_key(admin, alice_id).await;
    insert_settled(
        admin,
        device_writer,
        2,
        SettledOutcome::Rejected(MutationRejectReason::Unauthorized),
    )
    .await;
    set_frontier(admin, device_writer, 3).await;

    enrol.revoke(alice_id);
    let pruned = manager.sweep_writer_rows().await.expect("sweep the rows");
    assert_eq!(pruned, 1, "the sweep pruned the dead device writer");

    let rows = settled_rows(admin, device_writer).await;
    assert_eq!(rows.len(), 1, "only the refusal stands");
    assert_eq!(rows[0].seq, 2);
    assert!(
        writer_row_stands(admin, device_writer).await,
        "the writer row stands past the sweep"
    );

    // the receiver reads the refusal's cause past the sweep and the revocation,
    // and the pruned applied sequence answered settled.
    let (server_transport, mut recv) = loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    super::ticket_shared::do_handshake_with(&mut recv, "pv-recv", &["user:bob"]).await;
    let reply = ask_provisional(
        &mut recv,
        ProvisionalStatus {
            request_id: "p1".to_owned(),
            author: alice_id,
            offer: alice.signed_offer(Some(Frontier {
                seq: 3,
                head: [3; 32],
            })),
            seqs: vec![1, 2],
        },
    )
    .await;
    let ControlMessage::ProvisionalVerdicts(verdicts) = reply else {
        panic!("expected the ruling, got {reply:?}");
    };
    assert_eq!(
        verdicts.verdicts,
        vec![
            (1, Verdict::Settled),
            (
                2,
                Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized))
            ),
        ]
    );

    let _ = recv.close().await;
    server.await.expect("join bob").expect("session ok");
}
