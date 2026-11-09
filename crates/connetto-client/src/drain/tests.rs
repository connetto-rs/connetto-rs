use core::fmt::Write as _;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use connetto_core::chain::GENESIS;
use connetto_core::exchange::{Frontier, ManifestRow, SignedOffer};
use connetto_core::messages::{
    BulkMessage, Challenge, ControlMessage, FatalError, FatalErrorReason, HandshakeAck, HeldWork,
    HeldWrite, MutationApplied, MutationReject, MutationRejectReason, WriterGrant, WriterRef,
    WriterRequest, WriterSecret,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, LoopbackError, LoopbackTransport, Major, SchemaVersion, loopback};
use diesel::connection::SimpleConnection as _;
use diesel::prelude::*;
use sha2::{Digest, Sha256};

use crate::peer::{self, HeldState, PeerAuthor};
use crate::{ClientConfig, ClientEvent, ConnettoConnection, Replica, SyncStatus};

/// A loopback end whose receive fails once the test breaks it, the way an
/// erroring socket's does.
pub(crate) struct Wire {
    end: LoopbackTransport,
    broken: Arc<AtomicBool>,
}

impl Transport for Wire {
    type Error = LoopbackError;

    async fn send_control(&mut self, message: ControlMessage) -> Result<(), LoopbackError> {
        self.end.send_control(message).await
    }

    async fn send_bulk(&mut self, message: BulkMessage) -> Result<(), LoopbackError> {
        self.end.send_bulk(message).await
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, LoopbackError> {
        if self.broken.load(Ordering::Acquire) {
            return Err(LoopbackError::Disconnected);
        }
        self.end.recv().await
    }

    async fn close(&mut self) -> Result<(), LoopbackError> {
        self.end.close().await
    }
}

/// A connection whose drain wire a test can break.
pub(crate) type Conn = ConnettoConnection<Wire>;

pub(crate) fn major(number: u32) -> Major {
    Major::new(number).expect("a major")
}

/// The bundle hash the held major 1 was built from.
pub(crate) fn old_version() -> SchemaVersion {
    SchemaVersion::from_hash(b"the major 1 bundle".to_vec())
}

/// A major 2 replica with no wire and nothing queued.
pub(crate) fn replica() -> Conn {
    replica_named("drain", crate::SyncTuning::default())
}

/// [`replica`] with the build's `tuning`.
pub(crate) fn replica_tuned(tuning: crate::SyncTuning) -> Conn {
    replica_named("drain", tuning)
}

/// Queue a write of row `id` under `writer`, as the build at `captured`
/// captured it. An issued number holds [`secret_of`] its number, as the grant
/// that issued it leaves it.
pub(crate) fn queue(conn: &mut Conn, writer: WriterRef, id: i32, captured: Major) -> u64 {
    conn.db
        .batch_execute(&format!("INSERT INTO items (id) VALUES ({id})"))
        .expect("write locally");
    let built = crate::outbox::build(&mut conn.db, &conn.config.shared_tables)
        .expect("read the outbox")
        .expect("the write is recorded");
    let seq = conn
        .append_entry(
            &writer,
            &built.changeset,
            None,
            Some(captured),
            Some(built.through),
        )
        .expect("queue the write");
    if let WriterRef::Number(number) = writer
        && crate::is_real_writer(&writer)
    {
        let secret = secret_of(number);
        conn.writers
            .get_mut(&writer)
            .expect("the queue stands")
            .secret = Some(secret);
        crate::persist_writer_secret(&mut conn.db, &writer, Some(&secret))
            .expect("store the secret");
    }
    seq
}

/// The secret a test number is issued under, one per number.
pub(crate) fn secret_of(number: u64) -> WriterSecret {
    WriterSecret::from_bytes([u8::try_from(number).unwrap_or(u8::MAX); WriterSecret::LEN])
}

/// The handshake request naming the test number `number` with its secret.
pub(crate) fn number_request(number: u64) -> WriterRequest {
    WriterRequest::Number {
        number,
        secret: secret_of(number),
    }
}

/// Record major 1 as held, as a rebuild leaves it, and owe its drain.
pub(crate) fn hold_major_one(conn: &mut Conn) {
    conn.db
        .batch_execute(crate::majors::HELD_DDL)
        .expect("the held batch table");
    diesel::insert_into(crate::majors::held_batch::table)
        .values((
            crate::majors::held_batch::major.eq(1_i64),
            crate::majors::held_batch::version.eq(old_version().hash()),
        ))
        .execute(&mut conn.db)
        .expect("record the major 1 batch");
    conn.load_drain().expect("load the drain");
}

/// What the far end of a drain dial does once it has sent its answers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Then {
    /// It keeps the wire for the test to script.
    Stay,
    /// It hangs up at once, so the drain's next send fails.
    HangUp,
}

/// A drain wire, its far end, and the switch that breaks its receive.
pub(crate) fn wire() -> (Wire, LoopbackTransport, Arc<AtomicBool>) {
    let (end, old) = loopback();
    let broken = Arc::new(AtomicBool::new(false));
    let wire = Wire {
        end,
        broken: Arc::clone(&broken),
    };
    (wire, old, broken)
}

/// Dial the owed drain over a loopback pair whose far end answers the
/// handshake, which presents major 1, with `answers` and then does `then`.
/// Returns the writer the drain asked for, the far end, and the switch that
/// breaks the drain's receive.
pub(crate) async fn dial(
    conn: &mut Conn,
    answers: Vec<ControlMessage>,
    then: Then,
) -> (WriterRequest, LoopbackTransport, Arc<AtomicBool>) {
    let (wire, mut old, broken) = wire();
    let timer = conn.drain_timer().expect("the held batch owes a dial");
    let reply = async {
        let Some(ControlMessage::Handshake(handshake)) = sent(&mut old).await else {
            panic!("the drain opens with a handshake");
        };
        assert_eq!(
            handshake.schema_major,
            Some(major(1)),
            "the drain presents major 1"
        );
        for answer in answers {
            old.send_control(answer)
                .await
                .expect("answer the handshake");
        }
        if then == Then::HangUp {
            old.close().await.expect("hang up");
        }
        handshake.writer
    };
    let (attached, asked) = tokio::join!(conn.attach_drain(timer, wire), reply);
    attached.expect("attach the drain");
    (asked, old, broken)
}

/// The old major's acknowledgement of the drain handshake, granting `grant`.
pub(crate) fn ack(grant: Option<WriterGrant>) -> ControlMessage {
    handshake_ack("old", Some(old_version()), grant, false)
}

/// The old major's acknowledgement, granting writer `number` with nothing
/// settled yet.
pub(crate) fn granting(number: u64) -> ControlMessage {
    ack(Some(WriterGrant {
        number,
        frontier: 0,
    }))
}

/// The refusal of a deployment that no longer hosts major 1.
pub(crate) fn major_one_gone() -> ControlMessage {
    ControlMessage::FatalError(FatalError::new(FatalErrorReason::MajorMismatch {
        server: major(2),
        client: Some(major(1)),
    }))
}

/// The next control frame the far end receives, `None` once the wire
/// closed, bounded.
pub(crate) async fn sent(old: &mut LoopbackTransport) -> Option<ControlMessage> {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), old.recv())
            .await
            .expect("a frame within the bound")
            .expect("receive")
        {
            Some(IncomingFrame::Control(message)) => return Some(message),
            Some(IncomingFrame::Bulk(_)) => {}
            None => return None,
        }
    }
}

/// Apply the next write the far end receives, returning its sequence.
pub(crate) async fn apply_next(old: &mut LoopbackTransport) -> u64 {
    let Some(ControlMessage::MutationHeader(header)) = sent(old).await else {
        panic!("a held write goes up");
    };
    old.send_control(ControlMessage::MutationApplied(MutationApplied {
        client_seq: header.client_seq,
    }))
    .await
    .expect("apply the write");
    header.client_seq
}

/// One pump step, bounded.
pub(crate) async fn pump(conn: &mut Conn) -> ClientEvent {
    tokio::time::timeout(Duration::from_secs(5), conn.pump_one())
        .await
        .expect("a pump step within the bound")
        .expect("a pump step")
}

/// The hex of `bytes`, the tests' raw SQL reading the stored bytes back.
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The scripted server's `HandshakeAck`, every id the wire's own `id`.
pub(crate) fn handshake_ack(
    id: &str,
    schema_version: Option<SchemaVersion>,
    writer: Option<WriterGrant>,
    courier: bool,
) -> ControlMessage {
    ControlMessage::HandshakeAck(HandshakeAck {
        connection_id: id.to_owned(),
        session_token: id.to_owned(),
        resume_token: id.to_owned(),
        current_cursor: Cursor::new(Vec::new()),
        schema_version,
        sunset_at_secs: None,
        initial_credits: 64,
        writer,
        courier,
    })
}

/// The next frame the far end receives, bounded.
pub(crate) async fn next(old: &mut LoopbackTransport) -> IncomingFrame {
    tokio::time::timeout(Duration::from_secs(5), old.recv())
        .await
        .expect("a frame within the bound")
        .expect("receive")
        .expect("the wire is open")
}

/// The major 2 bundle hash the build presents.
pub(crate) fn main_version() -> SchemaVersion {
    SchemaVersion::from_hash(b"the major 2 bundle".to_vec())
}

/// The certificate end a test cursor carries, the retained rows live on.
pub(crate) const CERT_END: i64 = 4_102_444_800;

/// A major 2 replica for `client_id` with no wire and nothing queued, the
/// build's `tuning` on.
pub(crate) fn replica_named<T>(client_id: &str, tuning: crate::SyncTuning) -> ConnettoConnection<T>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    let mut config = ClientConfig::new(client_id)
        .with_schema_major(Some(major(2)))
        .with_schema_version(Some(main_version()));
    config.tuning = tuning;
    let mut conn = ConnettoConnection::open(
        &Replica::in_memory(),
        "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        &config,
        None,
    )
    .expect("open the major 2 replica");
    assert_eq!(
        conn.notices.pop_front(),
        Some(ClientEvent::SyncStatus(SyncStatus::Offline)),
        "the offline open states itself first"
    );
    conn
}

/// Hold one entry of `author` at `seq` as `state`, its body `body` and its
/// digest the body's hash.
pub(crate) fn hold_entry<T: connetto_core::traits::Transport>(
    conn: &mut ConnettoConnection<T>,
    author: [u8; 32],
    seq: u64,
    state: HeldState,
    major: Major,
    body: Option<&[u8]>,
) {
    let digest: [u8; 32] = {
        let mut hasher = Sha256::new();
        if let Some(body) = body {
            hasher.update(body);
        }
        hasher.finalize().into()
    };
    let row = ManifestRow {
        seq,
        major,
        origin: None,
        digest,
    };
    let author = PeerAuthor {
        key_id: author.to_vec(),
        account: "courier-author".to_owned(),
        bot_list: None,
    };
    let position =
        i64::try_from(peer::load_next_position(&mut conn.db).expect("a position")).expect("fits");
    peer::store_entry(
        &mut conn.db,
        &author,
        &peer::NewEntry {
            row: &row,
            state,
            body,
            received_at: 1,
            position,
        },
    )
    .expect("store the entry");
}

/// The signed offer the tests stand, nothing settled, `newest` naming the
/// offer's newest entry.
pub(crate) fn offer_with(builds: &[(Major, SchemaVersion)], newest: Option<u64>) -> SignedOffer {
    SignedOffer {
        offer: connetto_core::exchange::Offer {
            builds: builds.to_vec(),
            settled: None,
            newest: newest.map(|seq| Frontier {
                seq,
                head: [99; 32],
            }),
            retractions: Vec::new(),
        },
        signature: vec![1, 2, 3],
    }
}

/// Store `author`'s cursor behind `offer`, the test's certificate end on.
pub(crate) fn store_offer<T: connetto_core::traits::Transport>(
    conn: &mut ConnettoConnection<T>,
    author: [u8; 32],
    offer: &SignedOffer,
) {
    let encoded = rmp_serde::to_vec_named(offer).expect("the offer encodes");
    crate::exchange::upsert_cursor(
        &mut conn.db,
        author.as_ref(),
        "courier-author",
        &Frontier {
            seq: 0,
            head: GENESIS,
        },
        &encoded,
        CERT_END,
    )
    .expect("keep the cursor");
}

/// Answer the handshake the scripted `server` owes and attach `conn` on
/// `client`, the answer standing `challenge` and `ack`, draining the
/// notices `skip` holds.
pub(crate) async fn attach_scripted<T>(
    mut conn: ConnettoConnection<T>,
    client: T,
    mut server: LoopbackTransport,
    challenge: Option<[u8; 32]>,
    ack: ControlMessage,
    skip: impl Fn(&ClientEvent) -> bool,
) -> (ConnettoConnection<T>, LoopbackTransport)
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    let reply = async {
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::Handshake(_)) = frame else {
            panic!("the wire opens with the handshake");
        };
        if let Some(nonce) = challenge {
            server
                .send_control(ControlMessage::Challenge(Challenge { nonce }))
                .await
                .expect("the challenge");
        }
        server
            .send_control(ack)
            .await
            .expect("answer the handshake");
    };
    let (attached, ()) = tokio::join!(conn.attach(client), reply);
    attached.expect("attach");
    while let Some(notice) = conn.notices.pop_front() {
        if !skip(&notice) {
            conn.notices.push_front(notice);
            break;
        }
    }
    (conn, server)
}

/// R31: a major held under two writers drains one writer per wire, the
/// oldest write's writer first, and the next dial binds the other at once.
#[tokio::test]
async fn a_batch_held_under_two_writers_drains_one_writer_per_wire() {
    let mut conn = replica();
    let first = queue(&mut conn, WriterRef::Number(7), 1, major(1));
    let second = queue(&mut conn, WriterRef::Number(8), 2, major(1));
    hold_major_one(&mut conn);

    let (asked, mut old, _broken) = dial(&mut conn, vec![granting(7)], Then::Stay).await;
    assert_eq!(asked, number_request(7), "the oldest write's writer");
    assert_eq!(apply_next(&mut old).await, first);
    assert_eq!(
        pump(&mut conn).await,
        ClientEvent::Held(HeldWork::Applied {
            major: major(1),
            write: HeldWrite {
                writer: WriterRef::Number(7),
                seq: first,
            },
        })
    );
    assert_eq!(
        pump(&mut conn).await,
        ClientEvent::Held(HeldWork::WireLost { major: major(1) }),
        "the wire bound to the first writer closes with the major still held"
    );
    assert_eq!(
        conn.drain_timer().map(|timer| timer.wait()),
        Some(Duration::ZERO)
    );

    let (asked, mut old, _broken) = dial(&mut conn, vec![granting(8)], Then::Stay).await;
    assert_eq!(asked, number_request(8));
    assert_eq!(apply_next(&mut old).await, second);
    let mut events = Vec::new();
    while !events.contains(&ClientEvent::Held(HeldWork::Drained { major: major(1) })) {
        events.push(pump(&mut conn).await);
    }
    assert_eq!(conn.unsynced(), [] as [crate::UnsyncedWrite; 0]);
}

/// R31: a held write queued after another major's earlier write waits, so
/// nothing lands ahead of a write only the other major can apply.
#[tokio::test]
async fn a_held_write_behind_another_majors_earlier_write_waits() {
    let mut conn = replica();
    queue(&mut conn, WriterRef::Number(8), 1, major(2));
    queue(&mut conn, WriterRef::Number(7), 2, major(1));
    hold_major_one(&mut conn);

    let (asked, mut old, _broken) = dial(&mut conn, vec![granting(7)], Then::Stay).await;
    assert_eq!(asked, number_request(7));
    assert_eq!(
        sent(&mut old).await,
        None,
        "the wire closes without sending the held write"
    );
    assert!(
        conn.drain_timer()
            .is_some_and(|timer| timer.wait() > Duration::ZERO),
        "the drain looks again after a backoff"
    );
    assert_eq!(conn.unsynced().len(), 2);
}

/// R70 decision 22 on R31: a held number with no secret is never named again,
/// so the drain asks for a fresh number and the held write names nothing.
#[tokio::test]
async fn a_held_number_without_a_secret_drains_under_a_fresh_number_naming_nothing() {
    let mut conn = replica();
    queue(&mut conn, WriterRef::Number(7), 1, major(1));
    conn.writers
        .get_mut(&WriterRef::Number(7))
        .expect("the queue stands")
        .secret = None;
    hold_major_one(&mut conn);

    let (asked, mut old, _broken) = dial(&mut conn, vec![granting(9)], Then::Stay).await;
    assert!(
        matches!(asked, WriterRequest::Issue { .. }),
        "the secretless number is never presented"
    );
    let Some(ControlMessage::MutationHeader(header)) = sent(&mut old).await else {
        panic!("the issued number sends the held write");
    };
    assert_eq!(
        header.provenance, None,
        "the write names nothing past a number it cannot name"
    );
}

/// R31: losing a major held under two device keys retires the device once.
#[tokio::test]
async fn losing_a_major_held_under_two_device_keys_retires_the_device_once() {
    let mut conn = replica();
    let first = queue(&mut conn, WriterRef::Device([1; 32]), 1, major(1));
    let second = queue(&mut conn, WriterRef::Device([2; 32]), 2, major(1));
    hold_major_one(&mut conn);

    let (asked, _old, _broken) = dial(&mut conn, vec![major_one_gone()], Then::Stay).await;
    assert!(
        matches!(asked, WriterRequest::Issue { .. }),
        "no certificate binds either key"
    );
    assert_eq!(
        [pump(&mut conn).await, pump(&mut conn).await],
        [
            ClientEvent::NonFatal {
                related_to: None,
                detail: "the device's writer retired, a fresh key enrolls".to_owned(),
            },
            ClientEvent::Held(HeldWork::Lost {
                major: major(1),
                writes: vec![
                    HeldWrite {
                        writer: WriterRef::Device([1; 32]),
                        seq: first,
                    },
                    HeldWrite {
                        writer: WriterRef::Device([2; 32]),
                        seq: second,
                    },
                ],
                files: 0,
            }),
        ]
    );
    assert_eq!(conn.unsynced(), [] as [crate::UnsyncedWrite; 0]);
}

/// R31: a drain wire whose receive fails is dropped, its held work reported
/// as left without a wire, and the drain dials again after a backoff.
#[tokio::test]
async fn a_drain_wire_whose_receive_fails_redials_after_a_backoff() {
    let mut conn = replica();
    queue(&mut conn, WriterRef::Number(7), 1, major(1));
    hold_major_one(&mut conn);

    let (_asked, mut old, broken) = dial(&mut conn, vec![granting(7)], Then::Stay).await;
    assert!(matches!(
        sent(&mut old).await,
        Some(ControlMessage::MutationHeader(_))
    ));
    broken.store(true, Ordering::Release);
    assert_eq!(
        pump(&mut conn).await,
        ClientEvent::Held(HeldWork::WireLost { major: major(1) })
    );
    assert!(
        conn.drain_timer()
            .is_some_and(|timer| timer.wait() > Duration::ZERO),
        "the drain dials again after a backoff"
    );
    assert_eq!(conn.unsynced().len(), 1);
}

/// Queue rows `ids` under number 7 as major 1 captured them, drain them to
/// major 1's server and pump until the batch drained, so the replica
/// retains them with major 1's bundle hash. Returns their sequences.
async fn drain_major_one_writes(conn: &mut Conn, ids: &[i32]) -> Vec<u64> {
    let seqs: Vec<u64> = ids
        .iter()
        .map(|id| queue(conn, WriterRef::Number(7), *id, major(1)))
        .collect();
    hold_major_one(conn);
    let (_asked, mut old, _broken) = dial(conn, vec![granting(7)], Then::Stay).await;
    for seq in &seqs {
        assert_eq!(apply_next(&mut old).await, *seq);
    }
    while pump(conn).await != ClientEvent::Held(HeldWork::Drained { major: major(1) }) {}
    seqs
}

/// The old major's acknowledgement binding number 7 at `frontier`.
fn binding_seven_at(frontier: u64) -> ControlMessage {
    ack(Some(WriterGrant {
        number: 7,
        frontier,
    }))
}

/// The sequences number 7's armed run still holds.
fn armed(conn: &Conn) -> Vec<u64> {
    conn.restore
        .as_ref()
        .map(|restore| restore.entries.keys().copied().collect())
        .unwrap_or_default()
}

/// R78 decision 27: a run reaching into an older major opens a drain for it
/// although its batch drained long ago, presents it with the bundle hash its
/// rows carry, and re-sends that prefix, while the main wire holds the run's
/// current-major part until the prefix settles.
#[tokio::test]
async fn a_run_reaching_into_an_older_major_drains_its_prefix_first() {
    let mut conn = replica();
    let prefix = drain_major_one_writes(&mut conn, &[1, 2]).await;
    let current = queue(&mut conn, WriterRef::Number(7), 3, major(2));
    conn.bound_writer = Some(WriterRef::Number(7));
    let _ = conn
        .handle_control(ControlMessage::MutationApplied(MutationApplied {
            client_seq: current,
        }))
        .await
        .expect("the major 2 write settles");
    conn.bound_writer = None;
    assert_eq!(conn.drain_timer(), None, "nothing is held any more");

    assert!(
        conn.arm_restore(WriterRef::Number(7), 0)
            .expect("arm the run"),
        "the whole run is retained"
    );
    assert_eq!(armed(&conn), vec![prefix[0], prefix[1], current]);
    assert!(conn.run_holds_older_major(WriterRef::Number(7)));
    assert_eq!(
        conn.drain_timer().map(|timer| timer.wait()),
        Some(Duration::ZERO),
        "the run's older major owes a dial at once"
    );

    let (asked, mut old, _broken) = dial(&mut conn, vec![binding_seven_at(0)], Then::Stay).await;
    assert_eq!(asked, number_request(7));
    assert_eq!(apply_next(&mut old).await, prefix[0]);
    assert_eq!(apply_next(&mut old).await, prefix[1]);
    let mut events = Vec::new();
    while !events.contains(&ClientEvent::Held(HeldWork::Drained { major: major(1) })) {
        events.push(pump(&mut conn).await);
    }
    assert!(
        events.contains(&ClientEvent::WriteRestored {
            client_seq: prefix[1],
            refused: None,
        }),
        "the re-sent entries settle as restored, saw {events:?}"
    );
    assert_eq!(armed(&conn), vec![current], "the main wire's part is left");
    assert!(!conn.run_holds_older_major(WriterRef::Number(7)));
}

/// R78 decision 27: a restore met first on the drain wire arms the retained
/// run there, re-sending it ahead of the writer's held entry, instead of
/// sending the held entry into a gap.
#[tokio::test]
async fn a_restore_met_first_on_the_drain_wire_resends_the_run_ahead_of_the_held_write() {
    let mut conn = replica();
    let prefix = drain_major_one_writes(&mut conn, &[1, 2]).await;
    let held = queue(&mut conn, WriterRef::Number(7), 3, major(1));
    hold_major_one(&mut conn);

    let (_asked, mut old, _broken) = dial(&mut conn, vec![binding_seven_at(0)], Then::Stay).await;
    assert_eq!(apply_next(&mut old).await, prefix[0]);
    assert_eq!(apply_next(&mut old).await, prefix[1]);
    assert_eq!(apply_next(&mut old).await, held);
    while pump(&mut conn).await != ClientEvent::Held(HeldWork::Drained { major: major(1) }) {}
    assert_eq!(armed(&conn), Vec::<u64>::new());
    assert_eq!(conn.unsynced(), [] as [crate::UnsyncedWrite; 0]);
}

/// R78 decision 27: a run whose older major no server serves any more cannot
/// complete, so the writer moves on and the run's entries are reported lost.
#[tokio::test]
async fn a_run_whose_older_major_is_gone_is_lost_and_the_writer_moves_on() {
    let mut conn = replica();
    let prefix = drain_major_one_writes(&mut conn, &[1, 2]).await;
    assert!(
        conn.arm_restore(WriterRef::Number(7), 0)
            .expect("arm the run")
    );

    let (_asked, _old, _broken) = dial(&mut conn, vec![major_one_gone()], Then::Stay).await;
    let mut events = Vec::new();
    loop {
        let event = pump(&mut conn).await;
        let lost = matches!(event, ClientEvent::Held(HeldWork::Lost { .. }));
        events.push(event);
        if lost {
            break;
        }
    }
    assert_eq!(
        events.last(),
        Some(&ClientEvent::Held(HeldWork::Lost {
            major: major(1),
            writes: prefix
                .iter()
                .map(|seq| HeldWrite {
                    writer: WriterRef::Number(7),
                    seq: *seq,
                })
                .collect(),
            files: 0,
        }))
    );
    assert_eq!(armed(&conn), Vec::<u64>::new());
    assert!(
        !conn.writers.contains_key(&WriterRef::Number(7)),
        "the rewound number is abandoned"
    );
}

/// Settle number 7's write `id`, captured under major 2, on the main wire,
/// so the replica retains it with major 2's bundle hash. Returns its
/// sequence.
async fn settle_major_two_write(conn: &mut Conn, id: i32) -> u64 {
    let seq = queue(conn, WriterRef::Number(7), id, major(2));
    conn.bound_writer = Some(WriterRef::Number(7));
    let _ = conn
        .handle_control(ControlMessage::MutationApplied(MutationApplied {
            client_seq: seq,
        }))
        .await
        .expect("the major 2 write settles");
    conn.bound_writer = None;
    seq
}

/// R78 decision 27: while the run holds an older major's entry the main wire
/// sends nothing of its writer, and it sends the run's current-major part
/// once the drain settled the prefix.
#[tokio::test]
async fn the_main_wire_waits_for_the_drain_to_settle_the_older_prefix() {
    let mut conn = replica();
    let prefix = drain_major_one_writes(&mut conn, &[1, 2]).await;
    let current = settle_major_two_write(&mut conn, 3).await;
    let (end, mut server, _broken) = wire();
    conn.wire = Some(crate::Wire {
        transport: end,
        connection_id: "main".to_owned(),
        #[cfg(feature = "device-identity")]
        challenge: None,
        #[cfg(feature = "device-identity")]
        number: crate::ConnectionNumber(1),
        held_notices: Vec::new(),
    });
    conn.bound_writer = Some(WriterRef::Number(7));
    assert!(
        conn.arm_restore(WriterRef::Number(7), 0)
            .expect("arm the run")
    );

    conn.replay_pending().await.expect("replay");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), server.recv())
            .await
            .is_err(),
        "the main wire sends nothing while the older prefix is outstanding"
    );

    let (_asked, mut old, _broken) = dial(&mut conn, vec![binding_seven_at(0)], Then::Stay).await;
    assert_eq!(apply_next(&mut old).await, prefix[0]);
    assert_eq!(apply_next(&mut old).await, prefix[1]);
    while pump(&mut conn).await != ClientEvent::Held(HeldWork::Drained { major: major(1) }) {}
    let Some(ControlMessage::MutationHeader(header)) = sent(&mut server).await else {
        panic!("the main wire sends the run's current-major part");
    };
    assert_eq!(header.client_seq, current);
}

/// R70 decision 21 on the drain: a writer the old major holds below what
/// this replica saw settled, with no retained run, moves its held entry to a
/// fresh number naming nothing past the restored frontier, as the main wire
/// moves its queue, instead of sending it into a gap.
#[tokio::test]
async fn a_rewound_writer_on_the_drain_without_a_retained_run_moves_on() {
    let mut conn = replica_tuned(crate::SyncTuning::default().with_acked_retention(Duration::ZERO));
    drain_major_one_writes(&mut conn, &[1, 2]).await;
    queue(&mut conn, WriterRef::Number(7), 3, major(1));
    hold_major_one(&mut conn);

    let (_asked, mut old, _broken) = dial(&mut conn, vec![binding_seven_at(0)], Then::Stay).await;
    assert_eq!(sent(&mut old).await, None, "nothing goes into the gap");

    let (asked, mut old, _broken) = dial(&mut conn, vec![granting(9)], Then::Stay).await;
    assert!(
        matches!(asked, WriterRequest::Issue { .. }),
        "the rewound number is never presented again"
    );
    let Some(ControlMessage::MutationHeader(header)) = sent(&mut old).await else {
        panic!("the fresh number sends the held write");
    };
    assert_eq!(header.client_seq, 1);
    assert_eq!(
        header.provenance, None,
        "nothing past the restored frontier is named"
    );
}

/// R78 decision 27: a re-sent entry the old major answers `Forked` finds
/// another entry standing where the run was, so the run goes for good and
/// the writer's held entry moves to a fresh number on the next dial.
#[tokio::test]
async fn a_forked_answer_on_the_drain_moves_the_rewound_writer_on() {
    let mut conn = replica();
    let prefix = drain_major_one_writes(&mut conn, &[1, 2]).await;
    queue(&mut conn, WriterRef::Number(7), 3, major(1));
    hold_major_one(&mut conn);
    assert!(
        conn.arm_restore(WriterRef::Number(7), 0)
            .expect("arm the run")
    );
    let (_asked, mut old, _broken) = dial(&mut conn, vec![binding_seven_at(0)], Then::Stay).await;
    let Some(ControlMessage::MutationHeader(header)) = sent(&mut old).await else {
        panic!("the run goes up");
    };
    assert_eq!(header.client_seq, prefix[0]);
    conn.handle_drain_frame(Some(IncomingFrame::Control(
        ControlMessage::MutationReject(MutationReject {
            client_seq: prefix[0],
            reason: MutationRejectReason::Forked,
        }),
    )))
    .await
    .expect("the fork settles");
    assert_eq!(armed(&conn), Vec::<u64>::new());

    let (asked, mut old, _broken) = dial(&mut conn, vec![granting(9)], Then::Stay).await;
    assert!(
        matches!(asked, WriterRequest::Issue { .. }),
        "the forked number is never presented again"
    );
    let Some(ControlMessage::MutationHeader(header)) = sent(&mut old).await else {
        panic!("the fresh number sends the held write");
    };
    assert_eq!(header.client_seq, 1);
    assert_eq!(header.provenance, None);
}

/// R78 decision 27: a drain bound at or past the run's end finds the server
/// already holds it, so the run goes and the batch drains.
#[tokio::test]
async fn a_drain_bound_past_the_run_drops_it_and_drains() {
    let mut conn = replica();
    drain_major_one_writes(&mut conn, &[1, 2]).await;
    assert!(
        conn.arm_restore(WriterRef::Number(7), 0)
            .expect("arm the run")
    );
    let (_asked, _old, _broken) = dial(&mut conn, vec![binding_seven_at(2)], Then::Stay).await;
    assert_eq!(armed(&conn), Vec::<u64>::new());
    assert_eq!(
        pump(&mut conn).await,
        ClientEvent::Held(HeldWork::Drained { major: major(1) })
    );
}

/// R78 decision 26: an older-major row with no bundle hash cannot be
/// presented to that major's server, so the run is not armed.
#[tokio::test]
async fn a_run_whose_older_rows_carry_no_bundle_hash_is_not_armed() {
    let mut conn = replica();
    drain_major_one_writes(&mut conn, &[1, 2]).await;
    diesel::update(crate::acked::_connetto_acked::table)
        .set(crate::acked::_connetto_acked::version.eq(None::<Vec<u8>>))
        .execute(&mut conn.db)
        .expect("forget the bundle hashes");
    assert!(
        !conn
            .arm_restore(WriterRef::Number(7), 0)
            .expect("read the run"),
        "the run is left to the writer's retirement"
    );
}

#[cfg(all(feature = "peer", feature = "device-identity"))]
pub(crate) mod r78_4c {
    use std::sync::Arc;
    use std::time::SystemTime;

    use base64::Engine as _;
    use connetto_core::LoopbackTransport;
    use connetto_core::device_cert::{
        AttestationLevel, CertificateRequest, CertificateSerial, CertificateSigner, DeploymentId,
        DeviceCertificate, DeviceIssuer, RootCa, public_key_info,
    };
    use connetto_core::exchange::{CourierEntryOutcome, CourierOutcome};
    use connetto_core::messages::{BulkMessage, ControlMessage, CourierAnswer, WriterRequest};

    use super::*;
    use crate::device_key::open_software_key;
    use crate::enrolment::tests::{Memory, RunKeys, attesting_key};
    use crate::enrolment::{DeviceKeys, Held};
    use crate::{CourierCandidate, CourierError};

    const AUTHOR: [u8; 32] = [0x11; 32];

    /// The device's certificate and the keys that open its key, standing
    /// fresh.
    pub(crate) async fn device_identity() -> (Held, Arc<dyn DeviceKeys>) {
        let (key, der) = attesting_key();
        let records = Memory::default();
        let issuer_key = open_software_key(&records, "issuer")
            .await
            .expect("issuer key");
        let issuer_pkcs8 = base64::engine::general_purpose::STANDARD
            .decode(
                records
                    .0
                    .lock()
                    .get(&crate::device_key_record("issuer"))
                    .expect("stored")
                    .as_bytes(),
            )
            .expect("base64");
        let root = RootCa::create(
            DeploymentId::from_uuid(uuid::Uuid::from_u128(7)),
            SystemTime::now() - Duration::from_hours(10),
            Duration::from_hours(3650 * 24),
        )
        .expect("the root");
        let certificate = root
            .sign_issuer(
                &public_key_info(&issuer_key.key),
                SystemTime::now() - Duration::from_hours(10),
                Duration::from_hours(400 * 24),
                CertificateSerial::new([2; 16]).expect("the serial is positive"),
            )
            .expect("the issuer signs");
        let issuer_der = certificate.clone();
        let issuer = DeviceIssuer::from_pkcs8(certificate, &issuer_pkcs8, root.certificate())
            .expect("the issuer loads");
        let csr = CertificateRequest::build(&CertificateSigner::new(&key), &[1; 32])
            .expect("the csr builds");
        let request = CertificateRequest::parse(&csr).expect("the csr parses");
        let not_before = SystemTime::now();
        // The leaf stays inside the issuer's window, which started ten hours
        // ago and runs 400 days.
        let lifetime = Duration::from_hours(399 * 24);
        let certificate = issuer
            .issue(
                &request,
                "alice",
                not_before,
                lifetime,
                CertificateSerial::new([3; 16]).expect("the serial is positive"),
                AttestationLevel::Unproven,
                connetto_core::BOT_LIST_WILDCARD,
            )
            .expect("the issuer signs");
        (
            Held {
                leaf: DeviceCertificate::parse(&certificate).expect("the profile"),
                certificate,
                issuer: issuer_der,
                lifetime: Some(lifetime),
            },
            Arc::new(RunKeys { der }),
        )
    }

    /// A replica standing behind a test device, with a main wire whose
    /// courier path is on, the author's row held and the offer stored.
    async fn couriered_author() -> (Conn, LoopbackTransport, u64) {
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("store the certificate");
        hold_entry(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(1),
            Some(b"the old note"),
        );
        store_offer(
            &mut conn,
            AUTHOR,
            &offer_with(&[(major(1), old_version())], Some(1)),
        );
        let (client, server) = loopback();
        let wire = Wire {
            end: client,
            broken: Arc::new(AtomicBool::new(false)),
        };
        let (conn, main) = attach_scripted(
            conn,
            wire,
            server,
            Some([9; 32]),
            handshake_ack("main", Some(main_version()), None, true),
            |event| {
                matches!(
                    event,
                    ClientEvent::SyncStatus(_) | ClientEvent::CourierCandidates(_)
                )
            },
        )
        .await;
        let mut conn = conn;
        conn.writer_keys = Some(keys);
        (conn, main, 1)
    }

    /// R78 decision 47: a range whose major the device's own drain is
    /// draining rides that wire, the call owing no dial of its own, and a
    /// `CourierAnswer` on the drain wire settles its rows.
    #[tokio::test]
    async fn a_courier_range_rides_the_drain_draining_its_major() {
        let (mut conn, _main, seq) = couriered_author().await;
        // The device's own drain drains major 1: it owes its own writes of
        // the major, so the dial binds its writer and the batch stays past
        // the first settle.
        queue(&mut conn, WriterRef::Device([1; 32]), 2, major(1));
        queue(&mut conn, WriterRef::Device([1; 32]), 3, major(1));
        hold_major_one(&mut conn);
        let (_asked, mut old, _broken) = dial(&mut conn, vec![granting(7)], Then::Stay).await;
        let wait = conn.courier(AUTHOR).await.expect("send the range");
        assert!(
            conn.courier_drain_timer().is_none(),
            "the device's own drain is shared, no second dial"
        );
        // The shared drain wire carries the device's own writes first.
        for client_seq in [1u64, 2] {
            let frame = next(&mut old).await;
            let IncomingFrame::Control(ControlMessage::MutationHeader(_)) = frame else {
                panic!("the drain wire carries the device's own write, not {frame:?}");
            };
            let frame = next(&mut old).await;
            let IncomingFrame::Bulk(BulkMessage::MutationPatch(_)) = frame else {
                panic!("the drain wire carries the mutation body, not {frame:?}");
            };
            old.send_control(ControlMessage::MutationApplied(MutationApplied {
                client_seq,
            }))
            .await
            .expect("apply the device's own write");
        }
        let frame = next(&mut old).await;
        let IncomingFrame::Control(ControlMessage::CourierRange(range)) = frame else {
            panic!("the drain wire takes the range, not {frame:?}");
        };
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [seq]
        );
        let frame = next(&mut old).await;
        let IncomingFrame::Bulk(BulkMessage::CourierBody(_)) = frame else {
            panic!("the range declares one body, not {frame:?}");
        };
        old.send_control(ControlMessage::CourierAnswer(CourierAnswer {
            request_id: range.request_id,
            outcome: CourierOutcome::Entries(vec![(
                seq,
                CourierEntryOutcome::Ruled(connetto_core::exchange::Verdict::Applied),
            )]),
        }))
        .await
        .expect("answer the range");
        // The device's own writes settle, then the answer settles the range.
        loop {
            match conn
                .pump_one_or(std::future::pending::<()>())
                .await
                .unwrap()
            {
                Some(
                    ClientEvent::Held(HeldWork::Applied { .. } | HeldWork::Drained { .. })
                    | ClientEvent::CourierCandidates(_),
                ) => {}
                None => break,
                other => panic!("the range settles, not {other:?}"),
            }
        }
        let report = tokio::time::timeout(Duration::from_secs(5), wait.receiver)
            .await
            .expect("the call stands")
            .expect("the answer lands")
            .expect("the range settles");
        assert_eq!(report.settled, [seq]);
        assert_eq!(report.refused, [] as [u64; 0]);
        assert_eq!(report.kept, [] as [u64; 0]);
        assert!(
            conn.drain.wire.is_some(),
            "the device's own drain keeps its wire"
        );
        assert!(
            matches!(
                peer::state_of(
                    &mut conn.db,
                    &AUTHOR,
                    i64::try_from(seq).expect("a test seq"),
                ),
                Ok(Some(HeldState::Retained))
            ),
            "the row becomes retained"
        );
    }

    /// R78 decision 47: a courier dial that never attaches within its bound
    /// ends the call `TimedOut`, the rows kept and the dial no longer owed.
    #[tokio::test]
    async fn a_courier_dial_that_never_attaches_times_out() {
        let (mut conn, mut main, seq) = couriered_author().await;
        let wait = conn.courier(AUTHOR).await.expect("owe the dial");
        assert!(conn.courier_drain_timer().is_some(), "the dial is owed");
        conn.courier.open.as_mut().unwrap().deadline = Duration::ZERO;
        // The call's candidates notice is queued ahead of the tick.
        assert!(
            matches!(pump(&mut conn).await, ClientEvent::CourierCandidates(_)),
            "the candidates notice lands first"
        );
        main.send_control(ControlMessage::NonFatalError(
            connetto_core::messages::NonFatalError {
                related_to: None,
                detail: "a tick".to_owned(),
            },
        ))
        .await
        .expect("tick the pump");
        assert_eq!(
            pump(&mut conn).await,
            ClientEvent::NonFatal {
                related_to: None,
                detail: "a tick".to_owned(),
            },
            "the tick passes through"
        );
        let err = tokio::time::timeout(Duration::from_secs(5), wait.receiver)
            .await
            .expect("the call stands")
            .expect("the wait drops")
            .expect_err("the bound lapsed");
        assert!(matches!(err, CourierError::TimedOut));
        assert!(
            conn.courier_drain_timer().is_none(),
            "the dial goes with the call"
        );
        let candidates = conn.courier_candidates().expect("read the candidates");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].seqs, [seq], "the rows stay candidates");
    }

    /// R78 decision 47: a handshake answering that the deployment no longer
    /// serves the major ends the call `MajorUnserved`, the rows kept and out
    /// of the candidates until the next session.
    #[tokio::test]
    async fn a_courier_major_the_server_no_longer_serves_is_unserved() {
        let (mut conn, _main, seq) = couriered_author().await;
        let wait = conn.courier(AUTHOR).await.expect("owe the dial");
        let timer = conn.courier_drain_timer().expect("the dial is owed");
        let (courier, mut old, _broken) = wire();
        let reply = async {
            let frame = next(&mut old).await;
            let IncomingFrame::Control(ControlMessage::Handshake(handshake)) = frame else {
                panic!("the courier wire opens with the handshake");
            };
            assert_eq!(
                handshake.schema_major,
                Some(major(1)),
                "the courier wire presents major 1"
            );
            assert!(
                matches!(handshake.writer, WriterRequest::Device),
                "the device writer is presented"
            );
            old.send_control(major_one_gone())
                .await
                .expect("the major is gone");
        };
        let (attached, ()) = tokio::join!(conn.attach_courier_drain(timer, courier), reply);
        attached.expect("attach the courier wire");
        let err = tokio::time::timeout(Duration::from_secs(5), wait.receiver)
            .await
            .expect("the call stands")
            .expect("the wait drops")
            .expect_err("the major is unserved");
        assert!(
            matches!(err, CourierError::MajorUnserved(unserved) if unserved == major(1)),
            "the call names the unserved major"
        );
        assert!(conn.courier.open.is_none(), "the range is closed");
        assert!(conn.courier_wire.is_none(), "the courier wire is dropped");
        assert!(conn.courier_drain_timer().is_none(), "no dial is left owed");
        assert!(
            matches!(
                peer::state_of(
                    &mut conn.db,
                    &AUTHOR,
                    i64::try_from(seq).expect("a test seq"),
                ),
                Ok(Some(HeldState::CourierOnly))
            ),
            "the rows stay held"
        );
        assert_eq!(
            conn.courier_candidates().expect("read the candidates"),
            [] as [CourierCandidate; 0],
            "the major's rows are out until the next session"
        );
    }

    /// R78 decision 47: the handshake's courier flag on the courier wire is
    /// read as on the main wire, off answering Unavailable with no range.
    #[tokio::test]
    async fn a_courier_wire_whose_path_is_off_answers_unavailable() {
        let (mut conn, _main, _seq) = couriered_author().await;
        let wait = conn.courier(AUTHOR).await.expect("owe the dial");
        let timer = conn.courier_drain_timer().expect("the dial is owed");
        let (courier, mut old, _broken) = wire();
        let reply = async {
            let frame = next(&mut old).await;
            let IncomingFrame::Control(ControlMessage::Handshake(_)) = frame else {
                panic!("the courier wire opens with the handshake");
            };
            old.send_control(ack(None))
                .await
                .expect("answer the handshake");
        };
        let (attached, ()) = tokio::join!(conn.attach_courier_drain(timer, courier), reply);
        attached.expect("attach the courier wire");
        let err = tokio::time::timeout(Duration::from_secs(5), wait.receiver)
            .await
            .expect("the call stands")
            .expect("the wait drops")
            .expect_err("the path is off");
        assert!(matches!(err, CourierError::Unavailable));
        assert_eq!(sent(&mut old).await, None, "the wire closes with no range");
        let candidates = conn.courier_candidates().expect("read the candidates");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].seqs, [1], "the rows stay candidates");
    }
}
