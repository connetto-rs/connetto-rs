use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use connetto_core::messages::{
    BulkMessage, ControlMessage, FatalError, FatalErrorReason, HandshakeAck, HeldWork, HeldWrite,
    MutationApplied, WriterGrant, WriterRef, WriterRequest, WriterSecret,
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
    let config = ClientConfig::new("drain")
        .with_schema_major(Some(major(2)))
        .with_schema_version(Some(SchemaVersion::from_hash(
            b"the major 2 bundle".to_vec(),
        )));
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
    let changeset = conn.session.changeset().expect("capture the write");
    let seq = conn
        .append_entry(&writer, &changeset, None, Some(captured), true)
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
