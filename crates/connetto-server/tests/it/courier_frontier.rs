//! Docker-gated acceptance tests for the courier range and the writer-row
//! sweep (R75, R78 slice 1a).
//!
//! A courier presents one author's stored offer, the manifest rows it holds up
//! to the offer's newest entry and the bodies of those rows it holds. The rows
//! above the writer's frontier recompute the author's chain from the writer's
//! head to the offer's signed newest head, which binds every entry's major and
//! provenance original, so a courier can neither forge nor relabel an entry.
//! Rows at or below the frontier are answered with the verdict a provisional
//! ask gives, and the rows above it settle in order under the author's account
//! and the bot list its certificate carries, through the write path the direct
//! upload takes, as the `Courier` path. The range stops at a row without a
//! body or with a number original. A carried entry settles its provenance
//! original the way the direct upload does, so the logical write applies once
//! whichever copy arrives first. The sweep prunes the settled rows of the
//! device writers whose enrolment is revoked or has gone absent, keeps the
//! writer row and its frontier, and spares the writers whose enrolment still
//! stands, expired certificates included.
//!
//! Needs Docker: the fixture starts its own Postgres.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use super::writer_shared::{
    Enrolments, PG_DDL, insert_changeset, notes, seed_notes, writable_catalog, writer_for_key,
};
use connetto_core::Major;
use connetto_core::SessionId;
use connetto_core::auth::Principal;
use connetto_core::chain::{GENESIS, chain_walk, entry_hash, offer_message};
use connetto_core::clock::DEVICE_SKEW;
use connetto_core::device_cert::{AttestationLevel, KeyId};
use connetto_core::exchange::{
    CourierEntryOutcome, CourierOutcome, CourierRefusal, Frontier, ManifestRow, Offer, Origin,
    RetractCause, SignedOffer, Verdict,
};
use connetto_core::messages::{
    ControlMessage, MutationRejectReason, WriterOutcome, WriterRef, WriterRequest,
};
use connetto_core::test_support::TestGrantChecker;
use connetto_server::audit::{AuditHook, AuthEvent, AuthOp};
use connetto_server::ban::{Ban, BanFuture, BanStore, NewBan};
use connetto_server::courier::CourierEntry;
use connetto_server::defaults::{_connetto_settled, _connetto_writers};
use connetto_server::device_cert::{DeviceEnrolment, Enrolment, EnrolmentStore};
use connetto_server::writer_schema::{SettlePath, SettledOutcome};
use connetto_server::{
    AbuseConfig, ManagerBuilder, Materializer, NoConnector, PageSpec, RequestGuard, SessionConfig,
    SessionManager, SnapshotEstimate, SnapshotPage, SnapshotSource, ThrottleConfig,
    pg_write_target,
};
use connetto_test_harness::{Client, ConnettoDefaults, Fixture, RosterAuth, WITHHELD_ID};
use diesel::{ExpressionMethods, OptionalExtension as _, QueryDsl, SelectableHelper};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use parking_lot::Mutex;
use rcgen::{KeyPair, PublicKeyData as _, SigningKey as _};
use sha2::{Digest as _, Sha256};

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

diesel::table! {
    /// Backends waiting for database locks.
    pg_stat_activity (pid) {
        /// Backend identifier.
        pid -> Integer,
        /// The category of the backend's current wait.
        wait_event_type -> Nullable<Text>,
    }
}

#[derive(diesel::Queryable, diesel::Selectable, Debug, PartialEq)]
#[diesel(table_name = notes)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct Note {
    id: i32,
    body: String,
    edited_at: String,
}

fn note(id: i32, body: &str, edited_at: &str) -> Note {
    Note {
        id,
        body: body.to_owned(),
        edited_at: edited_at.to_owned(),
    }
}

/// The `notes` rows, read through the admin pool.
async fn notes(pool: &Pool<AsyncPgConnection>) -> Vec<Note> {
    let mut conn = pool.get().await.expect("admin connection");
    notes::table
        .order(notes::id)
        .select(Note::as_select())
        .load(&mut *conn)
        .await
        .expect("read notes")
}

/// The writer's account, frontier, head and retirement state, read as admin.
struct WriterFacts {
    user: Option<String>,
    frontier: i64,
    head: Option<Vec<u8>>,
    retired: bool,
}

async fn writer_facts(pool: &Pool<AsyncPgConnection>, writer: i64) -> WriterFacts {
    let mut conn = pool.get().await.expect("admin connection");
    let (user, frontier, head, retired) = _connetto_writers::table
        .filter(_connetto_writers::writer.eq(writer))
        .select((
            _connetto_writers::user_id,
            _connetto_writers::frontier,
            _connetto_writers::head,
            _connetto_writers::retired_at.is_not_null(),
        ))
        .get_result::<(Option<String>, i64, Option<Vec<u8>>, bool)>(&mut *conn)
        .await
        .expect("read the writer row");
    WriterFacts {
        user,
        frontier,
        head,
        retired,
    }
}

/// One settled entry, read as admin, ordered by sequence.
struct SettledFacts {
    seq: i64,
    entry_hash: Vec<u8>,
    outcome: String,
    path: SettlePath,
}

async fn settled_rows(pool: &Pool<AsyncPgConnection>, writer: i64) -> Vec<SettledFacts> {
    let mut conn = pool.get().await.expect("admin connection");
    _connetto_settled::table
        .filter(_connetto_settled::writer.eq(writer))
        .select((
            _connetto_settled::seq,
            _connetto_settled::entry_hash,
            _connetto_settled::outcome,
            _connetto_settled::path,
        ))
        .order(_connetto_settled::seq.asc())
        .load::<(i64, Vec<u8>, String, SettlePath)>(&mut *conn)
        .await
        .expect("read the settled rows")
        .into_iter()
        .map(|(seq, entry_hash, outcome, path)| SettledFacts {
            seq,
            entry_hash,
            outcome,
            path,
        })
        .collect()
}

/// Collects the audit events the guard emits, in order.
#[derive(Default)]
struct Captured(Arc<Mutex<Vec<AuthEvent<String>>>>);

impl Captured {
    /// A fresh sink with its audit hook.
    fn new() -> (Self, AuditHook<String>) {
        let captured = Self::default();
        let sink = Arc::clone(&captured.0);
        let hook = Arc::new(move |event| sink.lock().push(event));
        (captured, hook)
    }

    /// The writer-state events, `write_forked` and `write_redelivered`.
    fn write_events(&self) -> Vec<AuthEvent<String>> {
        self.0
            .lock()
            .iter()
            .filter(|event| matches!(event.op, AuthOp::WriteForked | AuthOp::WriteRedelivered))
            .cloned()
            .collect()
    }
}

/// The manager every test drives.
type Manager = Arc<SessionManager<NoSnapshot, RosterAuth, ConnettoDefaults>>;

impl Enrolments {
    /// Renew the same key with a certificate that stands until `expires_at`.
    async fn renew(&self, key: &KeyPair, user: &str, expires_at: SystemTime) {
        let key_id = *KeyId::of_public_key(&key.subject_public_key_info()).as_bytes();
        let record = Enrolment {
            user: user.to_owned(),
            key: KeyId::from_bytes(key_id),
            serial: [8; 16],
            issuer: KeyId::from_bytes([9; 32]),
            issued_at: SystemTime::now(),
            expires_at,
            session: SessionId::from_uuid(uuid::Uuid::new_v4()),
            public_key: key.subject_public_key_info().clone(),
            descriptor: vec![0x91, 0x01],
            attestation: AttestationLevel::Unproven,
            bot_list: connetto_core::BOT_LIST_WILDCARD.to_owned(),
        };
        self.store.record(record).await.expect("record the renewal");
    }
}

/// The ECDSA P-256 SHA-256 signature of `key` over `message`.
fn sign(key: &KeyPair, message: &[u8]) -> Vec<u8> {
    key.sign(message).expect("sign the message").clone()
}

/// The schema major every test range and connection declares.
fn major() -> Major {
    Major::new(1).expect("a positive major")
}

/// One entry of an author's chain, its changeset and the original it was
/// first written under.
struct Link {
    changeset: Vec<u8>,
    origin: Option<Origin>,
}

/// An entry the author's writer started itself.
fn plain(changeset: Vec<u8>) -> Link {
    Link {
        changeset,
        origin: None,
    }
}

/// An entry carried from `original`.
fn copied(changeset: Vec<u8>, original: Origin) -> Link {
    Link {
        changeset,
        origin: Some(original),
    }
}

/// The original at `seq` of the device writer `key`.
fn device_origin(key: [u8; 32], seq: u64) -> Origin {
    Origin {
        writer: WriterRef::Device(key),
        seq,
    }
}

/// An author's chain from genesis, one manifest row and one body per link
/// from sequence 1, every entry captured under [`major`].
struct Chain {
    rows: Vec<ManifestRow>,
    bodies: Vec<CourierEntry>,
}

fn chain(links: Vec<Link>) -> Chain {
    let mut rows = Vec::with_capacity(links.len());
    let mut bodies = Vec::with_capacity(links.len());
    for (seq, link) in (1..).zip(links) {
        rows.push(ManifestRow {
            seq,
            major: major(),
            origin: link.origin,
            digest: Sha256::digest(&link.changeset).into(),
        });
        bodies.push(CourierEntry {
            seq,
            changeset: link.changeset,
        });
    }
    Chain { rows, bodies }
}

impl Chain {
    /// The chain's hash at `seq`.
    fn head(&self, seq: u64) -> [u8; 32] {
        chain_walk(
            &GENESIS,
            self.rows
                .iter()
                .take_while(|row| row.seq <= seq)
                .map(|row| (row.seq, Some(row.major), row.origin, row.digest)),
        )
    }

    /// The chain's newest frontier.
    fn newest(&self) -> Frontier {
        let seq = self.rows.last().map_or(0, |row| row.seq);
        Frontier {
            seq,
            head: self.head(seq),
        }
    }

    /// The rows from `seq` on.
    fn rows_from(&self, seq: u64) -> Vec<ManifestRow> {
        self.rows
            .iter()
            .filter(|row| row.seq >= seq)
            .cloned()
            .collect()
    }

    /// The bodies from `seq` on.
    fn bodies_from(&self, seq: u64) -> Vec<CourierEntry> {
        self.bodies
            .iter()
            .filter(|body| body.seq >= seq)
            .cloned()
            .collect()
    }
}

/// The offer naming `newest` that `key` signs, encoded as the exchange
/// stores it.
fn offer(key: &KeyPair, key_id: [u8; 32], newest: Frontier) -> Vec<u8> {
    let offer = Offer {
        builds: Vec::new(),
        settled: None,
        newest: Some(newest),
        retractions: Vec::new(),
    };
    let signature = sign(key, &offer_message(&key_id, &offer));
    rmp_serde::to_vec_named(&SignedOffer { offer, signature }).expect("an offer encodes")
}

/// Deliver `chain` from `from` on, under the author's offer of its newest
/// entry.
async fn deliver(
    manager: &Manager,
    key: &KeyPair,
    key_id: [u8; 32],
    chain: &Chain,
    from: u64,
) -> CourierOutcome {
    manager
        .courier_deliver(
            KeyId::from_bytes(key_id),
            Some(major()),
            &offer(key, key_id, chain.newest()),
            &chain.rows_from(from),
            &chain.bodies_from(from),
        )
        .await
        .expect("deliver the range")
}

/// The answer of a row the range settled or found settled under `verdict`.
fn ruled(seq: u64, verdict: Verdict) -> (u64, CourierEntryOutcome) {
    (seq, CourierEntryOutcome::Ruled(verdict))
}

/// The answer of a row that stands applied.
fn applied(seq: u64) -> (u64, CourierEntryOutcome) {
    ruled(seq, Verdict::Applied)
}

/// The rows the range answered, failing the test on a refusal.
fn entries(outcome: CourierOutcome) -> Vec<(u64, CourierEntryOutcome)> {
    match outcome {
        CourierOutcome::Entries(entries) => entries,
        CourierOutcome::Refused(refusal) => panic!("expected answered rows, got {refusal:?}"),
    }
}

/// The writer row of `key_id`, `None` when the key holds none.
async fn writer_row(pool: &Pool<AsyncPgConnection>, key_id: [u8; 32]) -> Option<i64> {
    let mut conn = pool.get().await.expect("admin connection");
    _connetto_writers::table
        .filter(_connetto_writers::device_key.eq(key_id.to_vec()))
        .select(_connetto_writers::writer)
        .get_result(&mut *conn)
        .await
        .optional()
        .expect("read the device writer")
}

/// How many settled records stand across every writer.
async fn settled_count(pool: &Pool<AsyncPgConnection>) -> i64 {
    let mut conn = pool.get().await.expect("admin connection");
    _connetto_settled::table
        .count()
        .get_result(&mut *conn)
        .await
        .expect("count the settled rows")
}

fn writing_manager(
    pool: &Pool<AsyncPgConnection>,
    guard: Arc<RequestGuard<String>>,
    enrolment: Option<DeviceEnrolment<String>>,
    session: SessionConfig,
) -> Manager {
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL)
            .with_write_catalog(writable_catalog())
            .build()
            .expect("build materializer"),
        NoSnapshot,
        RosterAuth::granting("alice")
            .and("bob")
            .withholding(WITHHELD_ID),
        test_verifier(),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(pool.clone(), PG_DDL).expect("build write target"),
    )
    .with_session(session)
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

/// A guard auditing into a fresh sink.
fn audited_guard() -> (Captured, Arc<RequestGuard<String>>) {
    let (captured, hook) = Captured::new();
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default(),
        AbuseConfig::default(),
    ));
    guard.set_audit_hook(hook);
    (captured, guard)
}

/// A manager over a fresh store with an audited guard.
fn courier_manager(fixture: &Fixture, store: &Enrolments) -> (Captured, Manager) {
    let (captured, guard) = audited_guard();
    let manager = writing_manager(
        fixture.admin(),
        guard,
        Some(store.installable()),
        SessionConfig::default(),
    );
    (captured, manager)
}

/// A day from now, a certificate's expiry in most tests.
fn tomorrow() -> SystemTime {
    SystemTime::now() + Duration::from_hours(24)
}

/// The durable-apply acknowledgement of `client_seq`.
async fn expect_applied(client: &mut Client, client_seq: u64) {
    let ControlMessage::MutationApplied(applied) = client.next_control().await else {
        panic!("expected write {client_seq} to apply");
    };
    assert_eq!(applied.client_seq, client_seq);
}

/// A loopback connection driven as a harness `Client` declaring [`major`].
fn loopback_client(
    manager: &Manager,
) -> (
    Client,
    tokio::task::JoinHandle<Result<(), connetto_server::SessionError>>,
) {
    let (server_transport, client) = connetto_server::loopback();
    let server = tokio::spawn(manager.clone().serve(server_transport));
    (Client::new(client).declaring(major()), server)
}

/// The message a device key signs to bind to a connection's challenge.
fn bind_nonce_message(nonce: &[u8; 32]) -> Vec<u8> {
    connetto_core::messages::writer_bind_message(nonce)
}

/// Bind `key` as `user`'s device writer and upload `changesets` from
/// sequence 1, each acknowledged applied, then close.
async fn settle_directly(
    manager: &Manager,
    user: &str,
    key: &KeyPair,
    key_id: [u8; 32],
    changesets: Vec<Vec<u8>>,
) {
    let (mut client, server) = loopback_client(manager);
    let (challenge, _) = client
        .handshake_challenge_writing(
            "direct",
            &[&format!("user:{user}")],
            None,
            WriterRequest::Device,
            None,
        )
        .await;
    let answer = client
        .bind_device(
            "b1",
            key_id,
            sign(key, &bind_nonce_message(&challenge.nonce)),
        )
        .await;
    assert_eq!(answer.outcome, WriterOutcome::Bound { frontier: 0 });
    for (seq, changeset) in (1..).zip(changesets) {
        client.upload(seq, changeset).await;
        expect_applied(&mut client, seq).await;
    }
    client.close().await;
    server.await.expect("join the session").expect("session ok");
}

/// Seed a direct refusal at sequence 1 of `writer`, standing its frontier
/// at 1 with `head`.
async fn seed_direct_refusal(pool: &Pool<AsyncPgConnection>, writer: i64, head: &[u8; 32]) {
    let mut conn = pool.get().await.expect("admin connection");
    diesel::insert_into(_connetto_settled::table)
        .values((
            _connetto_settled::writer.eq(writer),
            _connetto_settled::seq.eq(1_i64),
            _connetto_settled::entry_hash.eq(head.as_slice()),
            _connetto_settled::previous_hash.eq(GENESIS),
            _connetto_settled::outcome
                .eq(SettledOutcome::Rejected(MutationRejectReason::Unauthorized).json()),
            _connetto_settled::path.eq(SettlePath::Direct),
        ))
        .execute(&mut *conn)
        .await
        .expect("refusal row");
    diesel::update(_connetto_writers::table.filter(_connetto_writers::writer.eq(writer)))
        .set((
            _connetto_writers::frontier.eq(1_i64),
            _connetto_writers::head.eq(head.as_slice()),
        ))
        .execute(&mut *conn)
        .await
        .expect("frontier");
}

/// A ban list refusing one account.
struct Banning(&'static str);

impl BanStore<String> for Banning {
    fn check<'a>(&'a self, user_id: &'a String) -> BanFuture<'a, Option<Ban>> {
        let ban = (user_id == self.0).then(|| Ban {
            session: SessionId::from_uuid(uuid::Uuid::nil()),
            reason: "test".to_owned(),
            banned_at: SystemTime::now().into(),
            expires_at: None,
            applies: true,
        });
        Box::pin(core::future::ready(Ok(ban)))
    }

    fn impose(&self, _ban: NewBan<String>) -> BanFuture<'_, ()> {
        Box::pin(core::future::ready(Ok(())))
    }

    fn lift<'a>(&'a self, _user_id: &'a String) -> BanFuture<'a, bool> {
        Box::pin(core::future::ready(Ok(false)))
    }
}

/// A courier range over a fresh author's chain settles every entry in order
/// as the `Courier` path, under the author's account identity, and stands
/// the author's writer row at the range's head.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_good_courier_range_settles_the_authors_entries_under_courier() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    let range = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
    ]);
    assert_eq!(
        entries(deliver(&manager, &key, key_id, &range, 1).await),
        vec![applied(1), applied(2)]
    );

    assert_eq!(
        notes(fixture.admin()).await,
        vec![
            note(1, "hello", "t0"),
            note(2, "two", "t1"),
            note(3, "three", "t2"),
        ]
    );
    let writer = writer_for_key(fixture.admin(), key_id).await;
    let facts = writer_facts(fixture.admin(), writer).await;
    assert_eq!(
        facts.user,
        Some("alice".to_owned()),
        "the range applies as the author"
    );
    assert_eq!(facts.frontier, 2);
    assert_eq!(facts.head, Some(range.head(2).to_vec()));
    assert!(!facts.retired);
    let rows = settled_rows(fixture.admin(), writer).await;
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.seq,
                row.path,
                row.entry_hash.clone(),
                row.outcome.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                1,
                SettlePath::Courier,
                range.head(1).to_vec(),
                r#""applied""#
            ),
            (
                2,
                SettlePath::Courier,
                range.head(2).to_vec(),
                r#""applied""#
            ),
        ]
    );
}

/// An offer the author's key did not sign, or one whose newest head the
/// rows do not reach, is refused unverified before anything settles, and
/// the refusal stands no writer row for the author.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forged_offer_is_refused_before_anything_settles() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (forger, _) = store.enrol("bob", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    let range = chain(vec![plain(insert_changeset(2, "two", "t1"))]);

    // Another key signs the author's offer.
    assert_eq!(
        deliver(&manager, &forger, key_id, &range, 1).await,
        CourierOutcome::Refused(CourierRefusal::Unverified)
    );
    // The author signed a chain the delivered body does not continue.
    let other = chain(vec![plain(insert_changeset(2, "forged", "t1"))]);
    let diverged = manager
        .courier_deliver(
            KeyId::from_bytes(key_id),
            Some(major()),
            &offer(&key, key_id, range.newest()),
            &other.rows,
            &other.bodies,
        )
        .await
        .expect("deliver the range");
    assert_eq!(
        diverged,
        CourierOutcome::Refused(CourierRefusal::Unverified)
    );
    // The author's offer names a newest entry past the rows the courier holds.
    let longer = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
    ]);
    let short = manager
        .courier_deliver(
            KeyId::from_bytes(key_id),
            Some(major()),
            &offer(&key, key_id, longer.newest()),
            &range.rows,
            &range.bodies,
        )
        .await
        .expect("deliver the range");
    assert_eq!(short, CourierOutcome::Refused(CourierRefusal::Unverified));

    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    assert_eq!(
        writer_row(fixture.admin(), key_id).await,
        None,
        "a refused range stands no writer row"
    );
    assert_eq!(notes(fixture.admin()).await, vec![note(1, "hello", "t0")]);
}

/// A range that does not hold together is refused malformed before anything
/// settles: an offer that does not decode, rows that skip a sequence or run
/// past the offer's newest, and a body that names no row, arrives out of
/// order or disagrees with its row's digest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_range_is_refused_before_anything_settles() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    let range = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
        plain(insert_changeset(4, "four", "t3")),
    ]);
    let signed = offer(&key, key_id, range.newest());
    let author = KeyId::from_bytes(key_id);
    let refused = |outcome: CourierOutcome, what: &str| {
        assert_eq!(
            outcome,
            CourierOutcome::Refused(CourierRefusal::Malformed),
            "{what}"
        );
    };

    refused(
        manager
            .courier_deliver(
                author,
                Some(major()),
                b"garbage",
                &range.rows,
                &range.bodies,
            )
            .await
            .expect("deliver"),
        "an offer that does not decode",
    );
    let skipping = vec![range.rows[0].clone(), range.rows[2].clone()];
    refused(
        manager
            .courier_deliver(author, Some(major()), &signed, &skipping, &[])
            .await
            .expect("deliver"),
        "rows that skip a sequence",
    );
    let short = offer(
        &key,
        key_id,
        Frontier {
            seq: 2,
            head: range.head(2),
        },
    );
    refused(
        manager
            .courier_deliver(author, Some(major()), &short, &range.rows, &range.bodies)
            .await
            .expect("deliver"),
        "rows past the offer's newest",
    );
    let stray = vec![CourierEntry {
        seq: 9,
        changeset: insert_changeset(9, "nine", "t9"),
    }];
    refused(
        manager
            .courier_deliver(author, Some(major()), &signed, &range.rows, &stray)
            .await
            .expect("deliver"),
        "a body that names no row",
    );
    let reversed = vec![range.bodies[1].clone(), range.bodies[0].clone()];
    refused(
        manager
            .courier_deliver(author, Some(major()), &signed, &range.rows, &reversed)
            .await
            .expect("deliver"),
        "bodies out of order",
    );
    let mut swapped = range.bodies.clone();
    swapped[1].changeset = insert_changeset(3, "swapped", "t2");
    refused(
        manager
            .courier_deliver(author, Some(major()), &signed, &range.rows, &swapped)
            .await
            .expect("deliver"),
        "a body that disagrees with its row's digest",
    );

    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    assert_eq!(notes(fixture.admin()).await, vec![note(1, "hello", "t0")]);
}

/// A range whose rows above the writer's frontier start past it is refused
/// with the gap, and settles nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_range_past_the_authors_frontier_is_refused() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    let range = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
        plain(insert_changeset(4, "four", "t3")),
    ]);
    settle_directly(
        &manager,
        "alice",
        &key,
        key_id,
        vec![range.bodies[0].changeset.clone()],
    )
    .await;

    assert_eq!(
        deliver(&manager, &key, key_id, &range, 3).await,
        CourierOutcome::Refused(CourierRefusal::SequenceGap {
            expected: 2,
            got: 3
        })
    );
    let writer = writer_for_key(fixture.admin(), key_id).await;
    assert_eq!(writer_facts(fixture.admin(), writer).await.frontier, 1);
    assert_eq!(settled_rows(fixture.admin(), writer).await.len(), 1);
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the gap settled nothing"
    );
}

/// A range starting at or below the writer's frontier answers those rows
/// with the verdict their records give, applying and auditing nothing for
/// them, and settles the rows above it from the writer's head.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_range_skips_the_settled_prefix_with_its_verdicts() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (captured, manager) = courier_manager(&fixture, &store);
    let range = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
        plain(insert_changeset(4, "four", "t3")),
    ]);
    settle_directly(&manager, "alice", &key, key_id, Vec::new()).await;
    let writer = writer_for_key(fixture.admin(), key_id).await;
    // The first entry stands refused under a direct record.
    seed_direct_refusal(fixture.admin(), writer, &range.head(1)).await;

    assert_eq!(
        entries(deliver(&manager, &key, key_id, &range, 1).await),
        vec![
            ruled(
                1,
                Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized))
            ),
            applied(2),
            applied(3),
        ]
    );
    assert!(
        captured.write_events().is_empty(),
        "the settled prefix is answered without an audit row"
    );
    let rows = settled_rows(fixture.admin(), writer).await;
    assert_eq!(
        rows.iter()
            .map(|row| (row.seq, row.path))
            .collect::<Vec<_>>(),
        vec![
            (1, SettlePath::Direct),
            (2, SettlePath::Courier),
            (3, SettlePath::Courier),
        ],
        "the prefix keeps its own record"
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![
            note(1, "hello", "t0"),
            note(3, "three", "t2"),
            note(4, "four", "t3"),
        ],
        "the refused entry never applies"
    );

    // Delivered again, every row is answered from its record.
    assert_eq!(
        entries(deliver(&manager, &key, key_id, &range, 1).await),
        vec![
            ruled(
                1,
                Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized))
            ),
            applied(2),
            applied(3),
        ]
    );
    assert_eq!(settled_rows(fixture.admin(), writer).await.len(), 3);
}

/// A range for a retired author is refused, and the author's record stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_range_for_a_retired_author_is_refused() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    let range = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
    ]);
    settle_directly(
        &manager,
        "alice",
        &key,
        key_id,
        vec![range.bodies[0].changeset.clone()],
    )
    .await;
    let (mut client, server) = loopback_client(&manager);
    client.handshake_with("ret-2", "user:alice").await;
    let answer = client.retire_writer("r1", WriterRef::Device(key_id)).await;
    assert_eq!(answer.outcome, WriterOutcome::Retired);
    client.close().await;
    server.await.expect("join 2").expect("session ok");

    assert_eq!(
        deliver(&manager, &key, key_id, &range, 2).await,
        CourierOutcome::Refused(CourierRefusal::WriterRetired)
    );
    let writer = writer_for_key(fixture.admin(), key_id).await;
    let facts = writer_facts(fixture.admin(), writer).await;
    assert!(facts.retired);
    assert_eq!(facts.frontier, 1, "the retirement stands the record");
    assert_eq!(settled_rows(fixture.admin(), writer).await.len(), 1);
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the refusal settled nothing"
    );
}

/// A revoked key is refused whatever the reason, and an enrolment past its
/// certificate's expiry plus `DEVICE_SKEW` is refused as absent, while one
/// within the tolerance still delivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_or_long_expired_author_is_refused_and_one_within_skew_delivers() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (revoked, revoked_id) = store.enrol("alice", tomorrow()).await;
    let (expired, expired_id) = store
        .enrol(
            "alice",
            SystemTime::now() - DEVICE_SKEW - Duration::from_mins(1),
        )
        .await;
    let (lapsing, lapsing_id) = store
        .enrol("alice", SystemTime::now() - DEVICE_SKEW / 2)
        .await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    store.revoke(revoked_id);

    let range = chain(vec![plain(insert_changeset(2, "two", "t1"))]);
    assert_eq!(
        deliver(&manager, &revoked, revoked_id, &range, 1).await,
        CourierOutcome::Refused(CourierRefusal::NotEnrolled),
        "a revoked key"
    );
    assert_eq!(
        deliver(&manager, &expired, expired_id, &range, 1).await,
        CourierOutcome::Refused(CourierRefusal::NotEnrolled),
        "a certificate past its expiry plus DEVICE_SKEW"
    );
    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");

    assert_eq!(
        entries(deliver(&manager, &lapsing, lapsing_id, &range, 1).await),
        vec![applied(1)],
        "a certificate within DEVICE_SKEW of its expiry"
    );
}

/// A banned author's range is refused before anything settles, so its
/// entries stay deliverable once the ban lifts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_banned_authors_range_is_refused_unsettled() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let guard = Arc::new(
        RequestGuard::new(ThrottleConfig::default(), AbuseConfig::default())
            .with_bans(Arc::new(Banning("alice"))),
    );
    let manager = writing_manager(
        fixture.admin(),
        guard,
        Some(store.installable()),
        SessionConfig::default(),
    );

    let range = chain(vec![plain(insert_changeset(2, "two", "t1"))]);
    assert_eq!(
        deliver(&manager, &key, key_id, &range, 1).await,
        CourierOutcome::Refused(CourierRefusal::AuthorBanned)
    );
    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    assert_eq!(writer_row(fixture.admin(), key_id).await, None);
    assert_eq!(notes(fixture.admin()).await, vec![note(1, "hello", "t0")]);
}

/// A couriered entry runs under the bot list the author's certificate
/// carries, the wildcard for a person, and a list that does not parse binds
/// the empty list, which sees and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_couriered_entry_binds_the_authors_certificate_bot_list() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    // The row records the list its write ran under.
    fixture
        .setup(&[
            "CREATE OR REPLACE FUNCTION stamp_bot_list() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN NEW.body := current_setting('app.bot_list', true); RETURN NEW; END $$",
            "CREATE TRIGGER stamp_bot_list BEFORE INSERT ON notes \
             FOR EACH ROW EXECUTE FUNCTION stamp_bot_list()",
        ])
        .await;
    let store = Enrolments::new();
    let (bot, bot_id) = store
        .enrol_listing("alice", tomorrow(), "notes:read,notes:insert")
        .await;
    let (person, person_id) = store.enrol("alice", tomorrow()).await;
    let (unreadable, unreadable_id) = store.enrol_listing("alice", tomorrow(), "notes:drop").await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    for (key, key_id, id) in [
        (&bot, bot_id, 2),
        (&person, person_id, 3),
        (&unreadable, unreadable_id, 4),
    ] {
        let range = chain(vec![plain(insert_changeset(id, "unstamped", "t1"))]);
        assert_eq!(
            entries(deliver(&manager, key, key_id, &range, 1).await),
            vec![applied(1)]
        );
    }
    assert_eq!(
        notes(fixture.admin()).await,
        vec![
            note(1, "hello", "t0"),
            note(2, "notes:read,notes:insert", "t1"),
            note(3, "*", "t1"),
            note(4, "", "t1"),
        ]
    );
}

/// The range stops at the first row the courier holds no body for, and at a
/// row whose original is a number writer, whose secret only that writer
/// holds, answering it `NotCouriered` and settling nothing past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_stops_at_a_digest_only_row_and_at_a_number_original() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (number_key, number_key_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    let range = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
        plain(insert_changeset(4, "four", "t3")),
    ]);
    let held: Vec<CourierEntry> = range
        .bodies
        .iter()
        .filter(|body| body.seq != 2)
        .cloned()
        .collect();
    let answer = manager
        .courier_deliver(
            KeyId::from_bytes(key_id),
            Some(major()),
            &offer(&key, key_id, range.newest()),
            &range.rows,
            &held,
        )
        .await
        .expect("deliver the range");
    assert_eq!(
        entries(answer),
        vec![applied(1), (2, CourierEntryOutcome::NotCouriered)]
    );
    let writer = writer_for_key(fixture.admin(), key_id).await;
    assert_eq!(writer_facts(fixture.admin(), writer).await.frontier, 1);

    let numbered = chain(vec![
        copied(
            insert_changeset(5, "five", "t4"),
            Origin {
                writer: WriterRef::Number(7),
                seq: 1,
            },
        ),
        plain(insert_changeset(6, "six", "t5")),
    ]);
    assert_eq!(
        entries(deliver(&manager, &number_key, number_key_id, &numbered, 1).await),
        vec![(1, CourierEntryOutcome::NotCouriered)]
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "nothing past a stop settled"
    );
}

/// A body captured under another major than the delivering connection's is
/// not couriered on it, since the drain wire of its own major delivers it,
/// and the range stops there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_of_another_major_is_not_couriered_on_this_connection() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    let range = chain(vec![plain(insert_changeset(2, "two", "t1"))]);
    let answer = manager
        .courier_deliver(
            KeyId::from_bytes(key_id),
            Major::new(2),
            &offer(&key, key_id, range.newest()),
            &range.rows,
            &range.bodies,
        )
        .await
        .expect("deliver the range");
    assert_eq!(
        entries(answer),
        vec![(1, CourierEntryOutcome::NotCouriered)]
    );
    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    assert_eq!(notes(fixture.admin()).await, vec![note(1, "hello", "t0")]);
}

/// The author's own later replay of a couriered entry is answered applied
/// from the record, audited `write_redelivered` across the two paths, and
/// applies the entry once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_reupload_of_a_courier_settled_entry_is_answered_from_the_record_and_audited() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (key, key_id) = store.enrol("alice", tomorrow()).await;
    let (captured, manager) = courier_manager(&fixture, &store);

    let range = chain(vec![plain(insert_changeset(2, "two", "t1"))]);
    assert_eq!(
        entries(deliver(&manager, &key, key_id, &range, 1).await),
        vec![applied(1)]
    );
    let writer = writer_for_key(fixture.admin(), key_id).await;

    let (mut client, server) = loopback_client(&manager);
    let (challenge, _) = client
        .handshake_challenge_writing("reup-1", &["user:alice"], None, WriterRequest::Device, None)
        .await;
    let answer = client
        .bind_device(
            "b1",
            key_id,
            sign(&key, &bind_nonce_message(&challenge.nonce)),
        )
        .await;
    assert_eq!(answer.outcome, WriterOutcome::Bound { frontier: 1 });
    client.upload(1, range.bodies[0].changeset.clone()).await;
    expect_applied(&mut client, 1).await;

    let events = captured.write_events();
    let [event] = events.as_slice() else {
        panic!(
            "the cross-path redelivery audits once, got {}",
            events.len()
        );
    };
    assert_eq!(event.op, AuthOp::WriteRedelivered);
    assert_eq!(event.user_id.as_deref(), Some("alice"));
    assert_eq!(event.writer, Some(writer));
    assert_eq!(event.seq, Some(1));
    assert_eq!(event.path, Some(SettlePath::Courier));
    assert_eq!(event.other_path, Some(SettlePath::Direct));
    let rows = settled_rows(fixture.admin(), writer).await;
    assert_eq!(rows.len(), 1, "the re-upload settles nothing new");
    assert_eq!(rows[0].path, SettlePath::Courier, "the first path stands");
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the entry applied once"
    );
    client.close().await;
    server.await.expect("join server").expect("session ok");
}

/// The sweep prunes the settled rows of the writers whose enrolment no
/// longer stands, keeps the writer row and its frontier, and spares the
/// writers whose enrolment still does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_prunes_a_revoked_writers_settled_rows_and_keeps_its_row() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (k1, k1_id) = store.enrol("alice", tomorrow()).await;
    let (k2, k2_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    settle_directly(
        &manager,
        "alice",
        &k1,
        k1_id,
        vec![insert_changeset(2, "two", "t1")],
    )
    .await;
    settle_directly(
        &manager,
        "alice",
        &k2,
        k2_id,
        vec![insert_changeset(3, "three", "t2")],
    )
    .await;

    // The revocation of the first key makes its writer sweepable.
    store.revoke(k1_id);
    let pruned = manager.sweep_writer_rows().await.expect("sweep the rows");
    assert_eq!(pruned, 1, "one writer's rows pruned");

    let writer_1 = writer_for_key(fixture.admin(), k1_id).await;
    assert!(settled_rows(fixture.admin(), writer_1).await.is_empty());
    let facts_1 = writer_facts(fixture.admin(), writer_1).await;
    assert_eq!(facts_1.frontier, 1, "the sweep keeps the frontier");
    assert!(facts_1.head.is_some(), "the sweep keeps the head");
    assert!(!facts_1.retired);
    let writer_2 = writer_for_key(fixture.admin(), k2_id).await;
    assert_eq!(settled_rows(fixture.admin(), writer_2).await.len(), 1);
    assert_eq!(
        notes(fixture.admin()).await,
        vec![
            note(1, "hello", "t0"),
            note(2, "two", "t1"),
            note(3, "three", "t2"),
        ],
        "the sweep prunes records, not rows"
    );

    // A second pass stands the same state.
    manager.sweep_writer_rows().await.expect("sweep again");
    assert!(settled_rows(fixture.admin(), writer_1).await.is_empty());
    assert_eq!(settled_rows(fixture.admin(), writer_2).await.len(), 1);
}

/// The sweep keeps the settled rows of a writer whose certificate has
/// expired but whose enrolment still stands, the courier refuses the
/// certificate once it is past its expiry plus `DEVICE_SKEW`, and a same-key
/// renewal keeps the writer delivering with its replay and fork evidence
/// intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_keeps_an_expired_writers_records_and_a_renewed_key_keeps_delivering() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    // The certificate expired an hour ago, the enrolment still stands.
    let (key, key_id) = store
        .enrol("alice", SystemTime::now() - Duration::from_hours(1))
        .await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    let range = chain(vec![
        plain(insert_changeset(2, "two", "t1")),
        plain(insert_changeset(3, "three", "t2")),
    ]);

    // The expired key still binds and settles, as an authenticated direct use.
    let (mut client, server) = loopback_client(&manager);
    let (challenge, _) = client
        .handshake_challenge_writing(
            "sweep-exp-1",
            &["user:alice"],
            None,
            WriterRequest::Device,
            None,
        )
        .await;
    let answer = client
        .bind_device(
            "b1",
            key_id,
            sign(&key, &bind_nonce_message(&challenge.nonce)),
        )
        .await;
    assert_eq!(answer.outcome, WriterOutcome::Bound { frontier: 0 });
    client.upload(1, range.bodies[0].changeset.clone()).await;
    expect_applied(&mut client, 1).await;
    let writer = writer_for_key(fixture.admin(), key_id).await;

    // The sweep spares the expired but unrevoked writer.
    let pruned = manager.sweep_writer_rows().await.expect("sweep the rows");
    assert_eq!(pruned, 0, "an expired certificate does not dead the writer");
    assert_eq!(
        settled_rows(fixture.admin(), writer).await.len(),
        1,
        "the replay and fork evidence stands"
    );

    // The courier refuses the certificate past its tolerance.
    assert_eq!(
        deliver(&manager, &key, key_id, &range, 2).await,
        CourierOutcome::Refused(CourierRefusal::NotEnrolled)
    );
    assert_eq!(
        writer_facts(fixture.admin(), writer).await.frontier,
        1,
        "the refusal settles nothing"
    );

    // The redelivery answers from the kept record, a different entry at the
    // kept sequence is refused as a fork, the same-key renewal stands, and
    // the writer keeps delivering.
    client.upload(1, range.bodies[0].changeset.clone()).await;
    expect_applied(&mut client, 1).await;
    client.upload(1, insert_changeset(2, "forked", "t9")).await;
    let ControlMessage::MutationReject(reject) = client.next_control().await else {
        panic!("expected the fork refusal");
    };
    assert_eq!(reject.client_seq, 1);
    assert_eq!(reject.reason, MutationRejectReason::Forked);
    store.renew(&key, "alice", tomorrow()).await;
    client.upload(2, insert_changeset(4, "four", "t3")).await;
    expect_applied(&mut client, 2).await;
    assert_eq!(
        settled_rows(fixture.admin(), writer).await.len(),
        2,
        "the renewed key settles on"
    );
    client.close().await;
    server.await.expect("join 1").expect("session ok");
}

/// The restored copy of an entry whose original already settled answers from
/// the original's record and settles without reapplying, so a row the
/// logical write later deleted is not recreated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_restore_answers_a_settled_original_without_reapplying() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (k1, k1_id) = store.enrol("alice", tomorrow()).await;
    let (k2, k2_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    // The courier settles the original entry.
    let cs1 = insert_changeset(2, "two", "t1");
    let original = chain(vec![plain(cs1.clone())]);
    assert_eq!(
        entries(deliver(&manager, &k1, k1_id, &original, 1).await),
        vec![applied(1)]
    );
    let writer_1 = writer_for_key(fixture.admin(), k1_id).await;

    // The logical write's row is later deleted, as an authorized write would.
    let mut conn = fixture.admin().get().await.expect("admin connection");
    diesel::delete(notes::table.filter(notes::id.eq(2_i32)))
        .execute(&mut *conn)
        .await
        .expect("delete the settled row");

    // The renewed key couriers the restored copy with the original's
    // identity, which settles without reapplying.
    let restored = chain(vec![copied(cs1, device_origin(k1_id, 1))]);
    assert_eq!(
        entries(deliver(&manager, &k2, k2_id, &restored, 1).await),
        vec![applied(1)],
        "the stored outcome stands"
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0")],
        "the deleted row is not recreated"
    );
    let writer_2 = writer_for_key(fixture.admin(), k2_id).await;
    let rows = settled_rows(fixture.admin(), writer_2).await;
    assert_eq!(rows.len(), 1, "the copy settles under the renewed key");
    assert_eq!(rows[0].path, SettlePath::Courier);
    assert_eq!(rows[0].outcome, r#""applied""#);
    assert_eq!(writer_facts(fixture.admin(), writer_1).await.frontier, 1);
    assert_eq!(settled_rows(fixture.admin(), writer_1).await.len(), 1);
}

/// The restored copy of an entry whose original stands unsettled settles the
/// original and the copy together, so the logical write applies once and both
/// identities advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_restore_settles_the_unsettled_original_with_the_copy() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    // The original's key enrolled but never bound, so it holds no writer row.
    let (_k1, k1_id) = store.enrol("alice", tomorrow()).await;
    let (k2, k2_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    let cs1 = insert_changeset(2, "two", "t1");
    let original_head = entry_hash(&GENESIS, 1, Some(major()), None, &cs1);
    let restored = chain(vec![copied(cs1, device_origin(k1_id, 1))]);
    assert_eq!(
        entries(deliver(&manager, &k2, k2_id, &restored, 1).await),
        vec![applied(1)]
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the logical write applied once"
    );

    // The original settled under the author's writer as the restore.
    let writer_1 = writer_for_key(fixture.admin(), k1_id).await;
    let rows_1 = settled_rows(fixture.admin(), writer_1).await;
    assert_eq!(rows_1.len(), 1, "the original settled with the copy");
    assert_eq!(rows_1[0].seq, 1);
    assert_eq!(rows_1[0].path, SettlePath::Restore);
    assert_eq!(rows_1[0].entry_hash, original_head.to_vec());
    assert_eq!(writer_facts(fixture.admin(), writer_1).await.frontier, 1);
    // The copy settled under the renewed key as the courier.
    let writer_2 = writer_for_key(fixture.admin(), k2_id).await;
    let rows_2 = settled_rows(fixture.admin(), writer_2).await;
    assert_eq!(rows_2.len(), 1);
    assert_eq!(rows_2[0].path, SettlePath::Courier);
    assert_eq!(writer_facts(fixture.admin(), writer_2).await.frontier, 1);
}

/// The restored copy of an entry whose original settled refused answers with
/// the stored refusal and settles it, so the logical write never applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_restore_keeps_a_refused_original_outcome() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (k1, k1_id) = store.enrol("alice", tomorrow()).await;
    let (k2, k2_id) = store.enrol("alice", tomorrow()).await;
    let (captured, manager) = courier_manager(&fixture, &store);

    // The author binds, standing its writer row at frontier 0, and the
    // original's refusal stands settled under it, as a direct denial does.
    settle_directly(&manager, "alice", &k1, k1_id, Vec::new()).await;
    let writer_1 = writer_for_key(fixture.admin(), k1_id).await;
    let cs1 = insert_changeset(2, "two", "t1");
    let head1 = entry_hash(&GENESIS, 1, Some(major()), None, &cs1);
    seed_direct_refusal(fixture.admin(), writer_1, &head1).await;

    // The renewed key couriers the restored copy.
    let restored = chain(vec![copied(cs1, device_origin(k1_id, 1))]);
    assert_eq!(
        entries(deliver(&manager, &k2, k2_id, &restored, 1).await),
        vec![ruled(
            1,
            Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized))
        )],
        "the stored refusal stands"
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0")],
        "the refused write never applies"
    );
    let writer_2 = writer_for_key(fixture.admin(), k2_id).await;
    let rows_2 = settled_rows(fixture.admin(), writer_2).await;
    assert_eq!(rows_2.len(), 1, "the copy settles the stored refusal");
    assert_eq!(
        SettledOutcome::from_json(rows_2[0].outcome.as_bytes()).expect("stored refusal"),
        SettledOutcome::Rejected(MutationRejectReason::Unauthorized),
    );
    assert_eq!(rows_2[0].path, SettlePath::Courier);
    let events = captured.write_events();
    let [event] = events.as_slice() else {
        panic!("expected one cross-path audit event");
    };
    assert_eq!(event.op, AuthOp::WriteRedelivered);
    assert_eq!(event.path, Some(SettlePath::Direct));
    assert_eq!(event.other_path, Some(SettlePath::Courier));
    assert_eq!(writer_facts(fixture.admin(), writer_1).await.frontier, 1);
    assert_eq!(settled_rows(fixture.admin(), writer_1).await.len(), 1);
}

/// A courier that strips, substitutes or adds an entry's provenance original
/// breaks the chain the author's offer signs, so the range is refused
/// unverified and settles nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_offer_covers_the_entries_provenance() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (_k1, k1_id) = store.enrol("alice", tomorrow()).await;
    let (k2, k2_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    let cs1 = insert_changeset(2, "two", "t1");
    let signed = chain(vec![copied(cs1.clone(), device_origin(k1_id, 1))]);
    let plain_chain = chain(vec![plain(cs1.clone())]);
    let substituted = chain(vec![copied(
        cs1,
        Origin {
            writer: WriterRef::Number(5),
            seq: 1,
        },
    )]);
    let author = KeyId::from_bytes(k2_id);
    for (offered, delivered, what) in [
        (&signed, &plain_chain, "a stripped original"),
        (&signed, &substituted, "a substituted original"),
        (&plain_chain, &signed, "an added original"),
    ] {
        let answer = manager
            .courier_deliver(
                author,
                Some(major()),
                &offer(&k2, k2_id, offered.newest()),
                &delivered.rows,
                &delivered.bodies,
            )
            .await
            .expect("deliver the range");
        assert_eq!(
            answer,
            CourierOutcome::Refused(CourierRefusal::Unverified),
            "{what}"
        );
    }
    assert_eq!(settled_count(fixture.admin()).await, 0, "nothing settled");
    assert_eq!(writer_row(fixture.admin(), k2_id).await, None);
    assert_eq!(notes(fixture.admin()).await, vec![note(1, "hello", "t0")]);
}

/// The restored copy of an entry whose original is another account's writer
/// settles under the author plainly, never touching the foreign original.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_restore_of_a_foreign_original_applies_plain() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (k2, k2_id) = store.enrol("alice", tomorrow()).await;
    let (k3, k3_id) = store.enrol("bob", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    // bob's key binds and settles its own first entry.
    settle_directly(
        &manager,
        "bob",
        &k3,
        k3_id,
        vec![insert_changeset(5, "five", "t1")],
    )
    .await;
    let writer_3 = writer_for_key(fixture.admin(), k3_id).await;

    // alice's renewed key couriers a copy claiming the foreign original.
    let restored = chain(vec![copied(
        insert_changeset(6, "six", "t2"),
        device_origin(k3_id, 1),
    )]);
    assert_eq!(
        entries(deliver(&manager, &k2, k2_id, &restored, 1).await),
        vec![applied(1)]
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![
            note(1, "hello", "t0"),
            note(5, "five", "t1"),
            note(6, "six", "t2")
        ],
        "the copy applies as a plain write"
    );
    let writer_2 = writer_for_key(fixture.admin(), k2_id).await;
    let rows_2 = settled_rows(fixture.admin(), writer_2).await;
    assert_eq!(rows_2.len(), 1, "the copy settled under the author");
    assert_eq!(rows_2[0].path, SettlePath::Courier);
    assert_eq!(writer_facts(fixture.admin(), writer_3).await.frontier, 1);
    assert_eq!(settled_rows(fixture.admin(), writer_3).await.len(), 1);
}

/// The restored copy of an entry whose original sequence is past the
/// original's frontier settles neither the copy nor the original and fails
/// the range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_restore_with_a_provenance_gap_settles_neither() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (k1, k1_id) = store.enrol("alice", tomorrow()).await;
    let (k2, k2_id) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);
    settle_directly(
        &manager,
        "alice",
        &k1,
        k1_id,
        vec![insert_changeset(2, "two", "t1")],
    )
    .await;
    let writer_1 = writer_for_key(fixture.admin(), k1_id).await;

    // The renewed key couriers a copy whose original sequence skips the
    // original's frontier.
    let restored = chain(vec![copied(
        insert_changeset(3, "three", "t2"),
        device_origin(k1_id, 3),
    )]);
    let error = manager
        .courier_deliver(
            KeyId::from_bytes(k2_id),
            Some(major()),
            &offer(&k2, k2_id, restored.newest()),
            &restored.rows,
            &restored.bodies,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, connetto_server::SessionError::Protocol(_)),
        "the gap is a protocol failure"
    );
    assert_eq!(
        notes(fixture.admin()).await,
        vec![note(1, "hello", "t0"), note(2, "two", "t1")],
        "the copy never applies"
    );
    let writer_2 = writer_for_key(fixture.admin(), k2_id).await;
    assert!(
        settled_rows(fixture.admin(), writer_2).await.is_empty(),
        "the copy settles nothing"
    );
    assert_eq!(
        writer_facts(fixture.admin(), writer_1).await.frontier,
        1,
        "the original settles nothing"
    );
}

/// A refused restored copy settles the original it names with the same
/// refusal, so neither identity applies the logical write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_courier_restore_settles_the_unsettled_original() {
    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (_, original_key) = store.enrol("alice", tomorrow()).await;
    let (author, author_key) = store.enrol("alice", tomorrow()).await;
    let (_captured, manager) = courier_manager(&fixture, &store);

    let restored = chain(vec![copied(vec![0], device_origin(original_key, 1))]);
    let answered = entries(deliver(&manager, &author, author_key, &restored, 1).await);
    assert!(
        matches!(
            answered.as_slice(),
            [(
                1,
                CourierEntryOutcome::Ruled(Verdict::Refused(RetractCause::Rejected(
                    MutationRejectReason::Malformed { .. }
                )))
            )]
        ),
        "a signed malformed logical write reaches settlement"
    );
    let original_writer = writer_for_key(fixture.admin(), original_key).await;
    let copy_writer = writer_for_key(fixture.admin(), author_key).await;
    let original_rows = settled_rows(fixture.admin(), original_writer).await;
    let copy_rows = settled_rows(fixture.admin(), copy_writer).await;
    assert_eq!(
        original_rows.len(),
        1,
        "the logical refusal settles its original"
    );
    assert_eq!(copy_rows.len(), 1);
    assert_eq!(original_rows[0].outcome, copy_rows[0].outcome);
    assert_eq!(original_rows[0].path, SettlePath::Restore);
    assert_eq!(copy_rows[0].path, SettlePath::Courier);
    assert_eq!(
        writer_facts(fixture.admin(), original_writer)
            .await
            .frontier,
        1
    );
    assert_eq!(writer_facts(fixture.admin(), copy_writer).await.frontier, 1);
    assert_eq!(notes(fixture.admin()).await, [note(1, "hello", "t0")]);
}

async fn wait_for_transaction_barrier(pool: &Pool<AsyncPgConnection>) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: i64 = {
                let mut observer = pool.get().await.expect("activity connection");
                pg_stat_activity::table
                    .filter(pg_stat_activity::wait_event_type.eq("Lock"))
                    .count()
                    .get_result(&mut *observer)
                    .await
                    .expect("blocked backend count")
            };
            if blocked >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both restored copies reach the transaction barrier");
}

/// Two couriers restoring copies of one unsettled original reserve the
/// original before applying, so the logical write applies once and the
/// losing copy is answered from the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_courier_restores_reserve_the_original_before_applying() {
    use diesel_async::{AsyncConnection as _, SimpleAsyncConnection as _};

    let fixture = Fixture::acquire().await;
    seed_notes(&fixture).await;
    let store = Enrolments::new();
    let (_, original_key) = store.enrol("alice", tomorrow()).await;
    let authors = [
        store.enrol("alice", tomorrow()).await,
        store.enrol("alice", tomorrow()).await,
    ];
    let (captured, manager) = courier_manager(&fixture, &store);
    let restored = Arc::new(chain(vec![copied(
        insert_changeset(2, "two", "t1"),
        device_origin(original_key, 1),
    )]));
    let pool = fixture.admin().clone();
    let mut blocker = fixture.admin().get().await.expect("lock connection");
    let jobs = blocker
        .transaction::<_, diesel::result::Error, _>(async move |conn| {
            conn.batch_execute("LOCK TABLE notes IN ACCESS EXCLUSIVE MODE")
                .await?;
            let mut jobs = Vec::with_capacity(authors.len());
            for (key, id) in authors {
                let manager = Arc::clone(&manager);
                let signed = offer(&key, id, restored.newest());
                let rows = restored.rows.clone();
                let bodies = restored.bodies.clone();
                jobs.push(tokio::spawn(async move {
                    let delivery = tokio::time::timeout(
                        Duration::from_secs(30),
                        manager.courier_deliver(
                            KeyId::from_bytes(id),
                            Some(major()),
                            &signed,
                            &rows,
                            &bodies,
                        ),
                    )
                    .await
                    .expect("courier deadline")
                    .expect("courier delivery");
                    (id, delivery)
                }));
            }
            wait_for_transaction_barrier(&pool).await;
            Ok(jobs)
        })
        .await
        .expect("release application-table lock");
    for job in jobs {
        let (id, delivery) = tokio::time::timeout(Duration::from_secs(30), job)
            .await
            .expect("join deadline")
            .expect("courier task");
        assert_eq!(entries(delivery), vec![applied(1)], "author {id:?}");
    }
    let original_writer = writer_for_key(fixture.admin(), original_key).await;
    assert_eq!(
        settled_rows(fixture.admin(), original_writer).await.len(),
        1
    );
    assert_eq!(
        writer_facts(fixture.admin(), original_writer)
            .await
            .frontier,
        1
    );
    assert_eq!(
        notes(fixture.admin()).await,
        [note(1, "hello", "t0"), note(2, "two", "t1")]
    );
    let events = captured.write_events();
    assert_eq!(
        events.len(),
        1,
        "the losing restore audits its original replay"
    );
    assert_eq!(events[0].op, AuthOp::WriteRedelivered);
}
