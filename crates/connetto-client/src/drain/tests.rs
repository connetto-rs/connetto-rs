use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use connetto_core::messages::{
    BulkMessage, ControlMessage, FatalError, FatalErrorReason, HandshakeAck, HeldWork, HeldWrite,
    MutationApplied, MutationReject, MutationRejectReason, WriterGrant, WriterRef, WriterRequest,
    WriterSecret,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, LoopbackError, LoopbackTransport, Major, SchemaVersion, loopback};
use diesel::connection::SimpleConnection as _;
use diesel::prelude::*;

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
    replica_tuned(crate::SyncTuning::default())
}

/// [`replica`] with the build's `tuning`.
pub(crate) fn replica_tuned(tuning: crate::SyncTuning) -> Conn {
    let mut config = ClientConfig::new("drain")
        .with_schema_major(Some(major(2)))
        .with_schema_version(Some(SchemaVersion::from_hash(
            b"the major 2 bundle".to_vec(),
        )));
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
    ControlMessage::HandshakeAck(HandshakeAck {
        connection_id: "old".to_owned(),
        session_token: "old".to_owned(),
        resume_token: "old".to_owned(),
        current_cursor: Cursor::new(Vec::new()),
        schema_version: Some(old_version()),
        sunset_at_secs: None,
        courier: false,
        initial_credits: 64,
        writer: grant,
    })
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
