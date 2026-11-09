//! The courier's offer on the main wire, R78 slice 4: the session stands the
//! author's held rows behind its stored offer, one range at a time, the
//! server ruling each row and the candidates restating as the run moves.

use connetto_core::chain::GENESIS;
use connetto_core::exchange::{
    CourierEntryOutcome, CourierOutcome, CourierRefusal, Frontier, ManifestRow, SignedOffer,
    Verdict,
};
use connetto_core::messages::{
    BulkMessage, ControlMessage, CourierAnswer, CourierBody, CourierRange, HandshakeAck,
    ProvisionalStatus, ProvisionalVerdicts, WriterGrant,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, LoopbackTransport, Major, SchemaVersion, loopback};
use core::fmt::Write as _;
use core::time::Duration;
use diesel::connection::SimpleConnection as _;
use sha2::{Digest, Sha256};
use tokio::time::timeout;

use crate::peer::{self, HeldState, PeerAuthor};
use crate::{
    ClientConfig, ClientEvent, ConnettoConnection, CourierCandidate, CourierError, Replica,
};

type Conn = ConnettoConnection<LoopbackTransport>;

const BOUND: Duration = Duration::from_secs(5);
/// The certificate end a test cursor carries, the retained rows live on.
const CERT_END: i64 = 4_102_444_800;
const AUTHOR: [u8; 32] = [0x01; 32];
const OTHER: [u8; 32] = [0x02; 32];
const THIRD: [u8; 32] = [0x03; 32];

/// The stored byte form of a device key, for the tests' raw SQL.
fn hex_of(author: [u8; 32]) -> String {
    let mut out = String::new();
    for byte in author {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn major(number: u32) -> Major {
    Major::new(number).expect("a major")
}

fn version() -> SchemaVersion {
    SchemaVersion::from_hash(b"the major 2 bundle".to_vec())
}

/// A major 2 replica with no wire and nothing queued.
fn replica() -> Conn {
    let config = ClientConfig::new("courier")
        .with_schema_major(Some(major(2)))
        .with_schema_version(Some(version()));
    let mut conn = ConnettoConnection::open(
        &Replica::in_memory(),
        "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        &config,
        None,
    )
    .expect("open the major 2 replica");
    assert!(
        matches!(conn.notices.pop_front(), Some(ClientEvent::SyncStatus(_))),
        "the offline open states itself first"
    );
    conn
}

/// Hold one entry of `author` at `seq` as `state`, its body the test's own
/// and its digest the body's hash.
fn hold(
    conn: &mut Conn,
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
    let author = author_of(author);
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

fn author_of(author: [u8; 32]) -> PeerAuthor {
    PeerAuthor {
        key_id: author.to_vec(),
        account: "courier-author".to_owned(),
        bot_list: None,
    }
}

/// The signed offer the tests stand, nothing settled, `newest` naming the
/// offer's newest entry.
fn offer(newest: Option<u64>) -> SignedOffer {
    SignedOffer {
        offer: connetto_core::exchange::Offer {
            builds: vec![(major(2), version())],
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

/// Store `author`'s cursor with the test's offer.
fn adopt(conn: &mut Conn, author: [u8; 32], newest: Option<u64>) {
    let encoded = rmp_serde::to_vec_named(&offer(newest)).expect("the offer encodes");
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

/// Attach the replica to a scripted server whose courier path is `on`,
/// answering the handshake and draining the state notices.
async fn attach(mut conn: Conn, on: bool) -> (Conn, LoopbackTransport) {
    let (client, mut server) = loopback();
    let reply = async {
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::Handshake(_)) = frame else {
            panic!("the wire opens with the handshake");
        };
        server
            .send_control(ControlMessage::HandshakeAck(HandshakeAck {
                connection_id: "courier".to_owned(),
                session_token: "courier".to_owned(),
                resume_token: "courier".to_owned(),
                current_cursor: Cursor::new(Vec::new()),
                schema_version: Some(version()),
                sunset_at_secs: None,
                initial_credits: 64,
                writer: Some(WriterGrant {
                    number: 1,
                    frontier: 0,
                }),
                courier: on,
            }))
            .await
            .expect("answer the handshake");
    };
    let (attached, ()) = tokio::join!(conn.attach(client), reply);
    attached.expect("attach the courier");
    while matches!(conn.notices.pop_front(), Some(ClientEvent::SyncStatus(_))) {}
    (conn, server)
}

/// The next frame within the bound, the wire open.
async fn next(server: &mut LoopbackTransport) -> IncomingFrame {
    timeout(BOUND, server.recv())
        .await
        .expect("a frame within the bound")
        .expect("the transport stands")
        .expect("the wire is open")
}

/// The next ask the connection sends.
async fn next_ask(server: &mut LoopbackTransport) -> ProvisionalStatus {
    let frame = next(server).await;
    let IncomingFrame::Control(ControlMessage::ProvisionalStatus(ask)) = frame else {
        panic!("the far end takes the ask, not {frame:?}");
    };
    ask
}

/// The next courier range the connection sends.
async fn next_range(server: &mut LoopbackTransport) -> CourierRange {
    let frame = next(server).await;
    let IncomingFrame::Control(ControlMessage::CourierRange(range)) = frame else {
        panic!("the far end takes the range, not {frame:?}");
    };
    range
}

/// The next body the connection sends.
async fn next_body(server: &mut LoopbackTransport) -> CourierBody {
    let frame = next(server).await;
    let IncomingFrame::Bulk(BulkMessage::CourierBody(body)) = frame else {
        panic!("the far end takes a body, not {frame:?}");
    };
    body
}

/// Rule the ask the server side.
async fn deliver_verdicts(
    server: &mut LoopbackTransport,
    request_id: &str,
    verdicts: &[(u64, Verdict)],
) {
    server
        .send_control(ControlMessage::ProvisionalVerdicts(ProvisionalVerdicts {
            request_id: request_id.to_owned(),
            verdicts: verdicts.to_vec(),
        }))
        .await
        .expect("send the verdicts");
}

/// Answer the range the server side.
async fn deliver_answer(server: &mut LoopbackTransport, request_id: &str, outcome: CourierOutcome) {
    server
        .send_control(ControlMessage::CourierAnswer(CourierAnswer {
            request_id: request_id.to_owned(),
            outcome,
        }))
        .await
        .expect("send the answer");
}

/// The candidate event, nothing else.
fn expect_candidates(event: ClientEvent) -> CourierCandidate {
    match event {
        ClientEvent::CourierCandidates(candidate) => candidate,
        other => panic!("the candidates restate, not {other:?}"),
    }
}

/// A real changeset of one row of `items`, built on a scratch replica so the
/// test connection's own queue stays empty.
fn real_changeset(id: i32) -> Vec<u8> {
    let mut scratch = replica();
    scratch
        .db
        .batch_execute(&format!("INSERT INTO items (id) VALUES ({id})"))
        .expect("write on the scratch replica");
    crate::outbox::build(&mut scratch.db, &scratch.config.shared_tables)
        .expect("read the outbox")
        .expect("the write is recorded")
        .changeset
}

/// The C0 line: no path, no range, the author's rows never moving.
#[tokio::test]
async fn path_off_answers_unavailable_sends_nothing() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the first note"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, false).await;
    assert!(matches!(
        conn.courier(AUTHOR).await,
        Err(CourierError::Unavailable)
    ));
    assert_eq!(
        conn.courier_candidates().expect("read the candidates"),
        [] as [CourierCandidate; 0]
    );
    let ask = next_ask(&mut server).await;
    assert_eq!(
        ask.author, AUTHOR,
        "the ask still goes out, the range does not"
    );
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    assert!(matches!(
        conn.courier(AUTHOR).await,
        Err(CourierError::Unavailable)
    ));
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::CourierOnly))
    ));
}

/// The range carries every held row of the build's major, the bodies
/// declared and sent in sequence order, a digest-only row among them.
#[tokio::test]
async fn range_carries_rows_and_declares_bodies_in_order() {
    let b1 = real_changeset(1);
    let b2 = b"the courier only body";
    let b4 = b"the fourth note";
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&b1),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b2),
    );
    hold(&mut conn, AUTHOR, 3, HeldState::DigestOnly, major(2), None);
    hold(
        &mut conn,
        AUTHOR,
        4,
        HeldState::Provisional,
        major(2),
        Some(b4),
    );
    adopt(&mut conn, AUTHOR, Some(4));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    assert_eq!(ask.seqs, [2, 3], "the unapplied rows are asked");
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    assert_eq!(range.author, AUTHOR);
    assert_eq!(
        range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(range.bodies, 3);
    let body = next_body(&mut server).await;
    assert_eq!(body.seq, 1);
    assert_eq!(
        zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
        b1
    );
    let body = next_body(&mut server).await;
    assert_eq!(body.seq, 2, "the bodies run in sequence order");
    let body = next_body(&mut server).await;
    assert_eq!(body.seq, 4, "the digest only row declares no body");
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![
            (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (2, CourierEntryOutcome::Ruled(Verdict::Settled)),
            (3, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (4, CourierEntryOutcome::Ruled(Verdict::Settled)),
        ]),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("settle the range");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1, 2, 3, 4]);
    assert_eq!(report.refused, [] as [u64; 0]);
    assert_eq!(report.kept, [] as [u64; 0]);
    assert_eq!(report.retry_after, None);
    for seq in [1, 2, 3, 4] {
        assert!(matches!(
            peer::state_of(&mut conn.db, &AUTHOR, seq),
            Ok(Some(HeldState::Retained))
        ));
    }
}

/// The run is contiguous and of the build's major only, so the range stops
/// at the first row of another major and leaves the rest held.
#[tokio::test]
async fn range_stops_at_first_row_of_another_major() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"under the build"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::Provisional,
        major(1),
        Some(b"under the other"),
    );
    hold(
        &mut conn,
        AUTHOR,
        3,
        HeldState::Provisional,
        major(2),
        Some(b"never reached"),
    );
    adopt(&mut conn, AUTHOR, Some(3));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    assert_eq!(
        range.rows.len(),
        1,
        "the run stops at the first foreign row"
    );
    assert_eq!(range.rows[0].seq, 1);
    assert_eq!(range.bodies, 1);
    let body = next_body(&mut server).await;
    assert_eq!(body.seq, 1);
    let candidates = conn.courier_candidates().expect("read the candidates");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].seqs, [1]);
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("settle the range");
    assert!(matches!(event, ClientEvent::Closed));
    wait.receiver
        .await
        .expect("the call stands")
        .expect("settled");
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::Retained))
    ));
    for seq in [2, 3] {
        assert!(matches!(
            peer::state_of(&mut conn.db, &AUTHOR, seq),
            Ok(Some(HeldState::Provisional))
        ));
    }
}

/// One range per author at a time, open or queued, so a second call for the
/// same author answers busy at once.
#[tokio::test]
async fn second_call_for_open_author_answers_busy() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"one"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::Provisional,
        major(2),
        Some(b"two"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"other"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let _wait = conn.courier(AUTHOR).await.expect("send the first range");
    let _ = next_range(&mut server).await;
    let busy = conn
        .courier(AUTHOR)
        .await
        .expect_err("the author's range is open");
    assert!(matches!(busy, CourierError::Busy));
    let _queued = conn.courier(OTHER).await.expect("queue the other range");
    let busy = conn
        .courier(OTHER)
        .await
        .expect_err("the queued author is busy too");
    assert!(matches!(busy, CourierError::Busy));
}

/// A range the socket refuses ends the session, so the call fails with the
/// transport's error and the next call finds the path gone.
#[tokio::test]
async fn a_range_the_socket_refuses_ends_the_session() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"one"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, server) = attach(conn, true).await;
    drop(server);
    let err = conn
        .courier(AUTHOR)
        .await
        .expect_err("the socket refuses the range");
    assert!(matches!(
        err,
        CourierError::Client(crate::ClientError::Transport(_))
    ));
    let gone = conn
        .courier(AUTHOR)
        .await
        .expect_err("the session ended with the send");
    assert!(matches!(gone, CourierError::Unavailable));
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::Provisional))
    ));
}

/// A call for another author queues behind the open range and its range goes
/// out once the first answer settles.
#[tokio::test]
async fn range_for_other_author_queues_and_sends_after_answer() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let range_a = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let wait_b = conn.courier(OTHER).await.expect("queue the second range");
    // The queued call waits out what is left of the first range's 30 s and
    // then its own 30 s, never more than both bounds and their slack.
    assert!(
        wait_b.bound > wait_a.bound && wait_b.bound <= wait_a.bound + Duration::from_secs(30),
        "the queued call waits the first range's deadline too, bounds {:?} and {:?}",
        wait_a.bound,
        wait_b.bound
    );
    deliver_answer(
        &mut server,
        &range_a.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    let mut pump = Box::pin(conn.pump_one());
    let range_b = tokio::select! {
        range = next_range(&mut server) => range,
        event = &mut pump => panic!("the pump surfaced {event:?} before the queued range went out"),
    };
    drop(pump);
    assert_eq!(range_b.author, OTHER);
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range_b.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("settle the second range");
    assert!(matches!(event, ClientEvent::Closed));
    wait_a
        .receiver
        .await
        .expect("the first call stands")
        .expect("the first range settles");
    let report_b = wait_b
        .receiver
        .await
        .expect("the second call stands")
        .expect("the second range settles");
    assert_eq!(report_b.settled, [1]);
}

/// A ruled answer settles its rows the way a ruling would: the provisional
/// ones retained, the refused one gone, the report the server's outcomes.
#[tokio::test]
async fn ruled_answer_settles_rows() {
    let b1 = real_changeset(1);
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&b1),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the refused"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let _ask = next_ask(&mut server).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    assert_eq!(range.rows.len(), 2);
    assert_eq!(range.bodies, 2);
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![
            (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (
                2,
                CourierEntryOutcome::Ruled(Verdict::Refused(
                    connetto_core::exchange::RetractCause::Conflict,
                )),
            ),
        ]),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("settle the range");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1]);
    assert_eq!(report.refused, [2]);
    assert_eq!(report.kept, [] as [u64; 0]);
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::Retained))
    ));
    assert!(
        matches!(peer::state_of(&mut conn.db, &AUTHOR, 2), Ok(None)),
        "the refused row is gone"
    );
}

/// A ruling that pushed first and the answer that still rules the same row
/// changes nothing the second time, decision 44.
#[tokio::test]
async fn push_before_answer_settles_row_answer_changes_nothing() {
    let b1 = real_changeset(1);
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&b1),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Applied)]).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![
            (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (2, CourierEntryOutcome::Ruled(Verdict::Settled)),
        ]),
    )
    .await;
    let event = conn.pump_one().await.expect("settle the push");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [2], "the settled row leaves the run");
    let event = conn.pump_one().await.expect("settle the answer");
    let candidate = expect_candidates(event);
    assert!(candidate.seqs.is_empty(), "nothing is left to courier");
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1, 2]);
    for seq in [1, 2] {
        assert!(matches!(
            peer::state_of(&mut conn.db, &AUTHOR, seq),
            Ok(Some(HeldState::Retained))
        ));
    }
}

/// The answer first and the ruling that rules the same row after changes
/// nothing the second time either, decision 44 the other way.
#[tokio::test]
async fn answer_before_push_changes_nothing() {
    let b1 = real_changeset(1);
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&b1),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![
            (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (2, CourierEntryOutcome::Ruled(Verdict::Settled)),
        ]),
    )
    .await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Applied)]).await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("settle both");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1, 2]);
    assert_eq!(report.kept, [] as [u64; 0]);
    for seq in [1, 2] {
        assert!(matches!(
            peer::state_of(&mut conn.db, &AUTHOR, seq),
            Ok(Some(HeldState::Retained))
        ));
    }
}

/// `NotCouriered` keeps the row from it held and excludes the author until a
/// new offer stands, the empty set raised in between.
#[tokio::test]
async fn not_couriered_keeps_rows_excludes_until_new_offer() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the second"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[(1, Verdict::Pending), (2, Verdict::Pending)],
    )
    .await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [1, 2]);
    assert_eq!(candidate.last, Some(Verdict::Pending));
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![
            (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (2, CourierEntryOutcome::NotCouriered),
        ]),
    )
    .await;
    let candidate = expect_candidates(conn.pump_one().await.expect("the answer settles"));
    assert!(
        candidate.seqs.is_empty(),
        "the excluded author raises nothing"
    );
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1]);
    assert_eq!(report.kept, [2]);
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 2),
        Ok(Some(HeldState::CourierOnly))
    ));
    assert_eq!(
        conn.courier_candidates().expect("read the candidates"),
        [] as [CourierCandidate; 0]
    );
    assert!(matches!(
        conn.courier(AUTHOR).await,
        Err(CourierError::NothingHeld)
    ));
    conn.adopt_peer_offer(
        &author_of(AUTHOR),
        &offer(Some(2)),
        &Frontier {
            seq: 0,
            head: [77; 32],
        },
        CERT_END,
    )
    .expect("a new offer stands");
    let candidate = expect_candidates(conn.pump_one().await.expect("the offer restates"));
    assert_eq!(candidate.seqs, [2], "only the settled row leaves the run");
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A Retry keeps the rows held, names its wait in the report and reports the
/// run at the retried row.
#[tokio::test]
async fn retry_keeps_rows_candidates_reports_wait() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the second"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[(1, Verdict::Pending), (2, Verdict::Pending)],
    )
    .await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let _ = expect_candidates(conn.pump_one().await.expect("restate"));
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![
            (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (
                2,
                CourierEntryOutcome::Retry {
                    retry_after_ms: 1500,
                },
            ),
        ]),
    )
    .await;
    let candidate = expect_candidates(conn.pump_one().await.expect("the answer settles"));
    assert_eq!(
        candidate.seqs,
        [2],
        "the retry keeps the run at the retried row"
    );
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1]);
    assert_eq!(report.kept, [2]);
    assert_eq!(report.retry_after, Some(Duration::from_millis(1500)));
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A Disabled refusal turns the path off for the session, the rows staying.
#[tokio::test]
async fn disabled_refusal_enters_c0() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&real_changeset(1)),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Refused(CourierRefusal::Disabled),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the refusal settles");
    assert!(matches!(event, ClientEvent::Closed));
    let refused = wait
        .receiver
        .await
        .expect("the call stands")
        .expect_err("the path is off");
    assert!(matches!(
        refused,
        CourierError::Refused(CourierRefusal::Disabled)
    ));
    assert!(matches!(
        conn.courier(AUTHOR).await,
        Err(CourierError::Unavailable)
    ));
    assert_eq!(
        conn.courier_candidates().expect("read the candidates"),
        [] as [CourierCandidate; 0]
    );
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::Provisional))
    ));
}

/// An Uncertified refusal excludes the author until the next session, a new
/// offer taking the exclusion in between.
#[tokio::test]
async fn uncertified_excludes_until_next_session() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the second"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[(1, Verdict::Pending), (2, Verdict::Pending)],
    )
    .await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let _ = expect_candidates(conn.pump_one().await.expect("restate"));
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Refused(CourierRefusal::Uncertified),
    )
    .await;
    let candidate = expect_candidates(conn.pump_one().await.expect("the refusal settles"));
    assert!(
        candidate.seqs.is_empty(),
        "the excluded author raises nothing"
    );
    let refused = wait
        .receiver
        .await
        .expect("the call stands")
        .expect_err("the range is refused");
    assert!(matches!(
        refused,
        CourierError::Refused(CourierRefusal::Uncertified)
    ));
    for seq in [1, 2] {
        assert!(matches!(
            peer::state_of(&mut conn.db, &AUTHOR, seq),
            Ok(Some(HeldState::CourierOnly))
        ));
    }
    assert!(matches!(
        conn.courier(AUTHOR).await,
        Err(CourierError::NothingHeld)
    ));
    conn.adopt_peer_offer(
        &author_of(AUTHOR),
        &offer(Some(2)),
        &Frontier {
            seq: 0,
            head: [77; 32],
        },
        CERT_END,
    )
    .expect("a new offer stands");
    let candidate = expect_candidates(conn.pump_one().await.expect("the offer restates"));
    assert_eq!(candidate.seqs, [1, 2], "the new offer takes the exclusion");
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A `Malformed` refusal changes nothing but stands noted, the author's rows
/// and its candidacy staying as they were.
#[tokio::test]
async fn malformed_keeps_author_candidate() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the only row"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let _ = expect_candidates(conn.pump_one().await.expect("restate"));
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Refused(CourierRefusal::Malformed),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the refusal settles");
    assert!(matches!(event, ClientEvent::Closed));
    let refused = wait
        .receiver
        .await
        .expect("the call stands")
        .expect_err("the range is refused");
    assert!(matches!(
        refused,
        CourierError::Refused(CourierRefusal::Malformed)
    ));
    assert!(
        !conn.courier.excluded.contains_key(&AUTHOR),
        "a malformed refusal excludes nothing"
    );
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::CourierOnly))
    ));
}

/// A `NotEnrolled` refusal takes the rows the courier offered, courier-only,
/// digest-only and retained, keeping the author's own.
#[tokio::test]
async fn not_enrolled_takes_courier_held_rows() {
    writer_refusal_takes_rows(CourierRefusal::NotEnrolled).await;
}

/// A `WriterRetired` refusal takes the rows the courier offered, keeping the
/// author's own.
#[tokio::test]
async fn writer_retired_takes_courier_held_rows() {
    writer_refusal_takes_rows(CourierRefusal::WriterRetired).await;
}

/// The shared shape of a writer refusal: the offered rows gone, the author's
/// own kept, and a later ruling taking the author's exclusion.
async fn writer_refusal_takes_rows(refusal: CourierRefusal) {
    let b1 = real_changeset(1);
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&b1),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the courier only"),
    );
    hold(
        &mut conn,
        AUTHOR,
        3,
        HeldState::Retained,
        major(2),
        Some(b"the retained"),
    );
    adopt(&mut conn, AUTHOR, Some(3));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    assert_eq!(
        range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1, 2],
        "the retained row waits out of the 4b range"
    );
    assert_eq!(range.bodies, 2);
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Refused(refusal),
    )
    .await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Abandoned)]).await;
    let event = conn.pump_one().await.expect("the refusal settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let refused = wait
        .receiver
        .await
        .expect("the call stands")
        .expect_err("the range is refused");
    assert!(matches!(refused, CourierError::Refused(_)));
    assert!(matches!(peer::state_of(&mut conn.db, &AUTHOR, 1), Ok(None)));
    assert!(
        matches!(peer::state_of(&mut conn.db, &AUTHOR, 2), Ok(None)),
        "the courier only row goes"
    );
    assert!(
        matches!(peer::state_of(&mut conn.db, &AUTHOR, 3), Ok(None)),
        "the retained row goes"
    );
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
}

/// An Unverified refusal takes the offered rows and raises the author's
/// chain invalid, the author's own row kept.
#[tokio::test]
async fn unverified_takes_rows_and_raises_peer_chain_invalid() {
    let b1 = real_changeset(1);
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&b1),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the courier only"),
    );
    hold(
        &mut conn,
        AUTHOR,
        3,
        HeldState::Retained,
        major(2),
        Some(b"the retained"),
    );
    adopt(&mut conn, AUTHOR, Some(3));
    let (mut conn, mut server) = attach(conn, true).await;
    let _ask = next_ask(&mut server).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    assert_eq!(range.rows.len(), 2);
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Refused(CourierRefusal::Unverified),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the refusal settles");
    let ClientEvent::PeerChainInvalid { author } = event else {
        panic!("the chain stands invalid, not {event:?}");
    };
    assert_eq!(author, AUTHOR.to_vec());
    let refused = wait
        .receiver
        .await
        .expect("the call stands")
        .expect_err("the range is refused");
    assert!(matches!(
        refused,
        CourierError::Refused(CourierRefusal::Unverified)
    ));
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::Provisional)),
    ));
    assert!(matches!(peer::state_of(&mut conn.db, &AUTHOR, 2), Ok(None)));
    assert!(matches!(peer::state_of(&mut conn.db, &AUTHOR, 3), Ok(None)));
}

/// An `AuthorBanned` refusal keeps every row of the author, the entries staying
/// deliverable once the ban lifts.
#[tokio::test]
async fn author_banned_keeps_every_row() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&real_changeset(1)),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the courier only"),
    );
    hold(
        &mut conn,
        AUTHOR,
        3,
        HeldState::Retained,
        major(2),
        Some(b"the retained"),
    );
    adopt(&mut conn, AUTHOR, Some(3));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &ask.request_id, &[(2, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(
        candidate.seqs,
        [1, 2],
        "the held rows stand beside the offer"
    );
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Refused(CourierRefusal::AuthorBanned),
    )
    .await;
    let event = conn.pump_one().await.expect("the refusal settles");
    assert!(matches!(
        event,
        ClientEvent::CourierCandidates(candidate) if candidate.seqs.is_empty()
    ));
    let refused = wait
        .receiver
        .await
        .expect("the call stands")
        .expect_err("the range is refused");
    assert!(matches!(
        refused,
        CourierError::Refused(CourierRefusal::AuthorBanned)
    ));
    for (seq, state) in [
        (1, HeldState::Provisional),
        (2, HeldState::CourierOnly),
        (3, HeldState::Retained),
    ] {
        assert!(matches!(
            peer::state_of(&mut conn.db, &AUTHOR, seq),
            Ok(Some(expected)) if expected == state,
        ));
    }
    assert!(
        matches!(conn.courier(AUTHOR).await, Err(CourierError::NothingHeld)),
        "the ban holds the author's rows back until the next session"
    );
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A `SequenceGap` refusal keeps the rows until the author's next offer, the
/// empty set raised in between.
#[tokio::test]
async fn sequence_gap_keeps_rows_until_new_offer() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the only row"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let _ = expect_candidates(conn.pump_one().await.expect("restate"));
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Refused(CourierRefusal::SequenceGap {
            expected: 2,
            got: 5,
        }),
    )
    .await;
    let candidate = expect_candidates(conn.pump_one().await.expect("the refusal settles"));
    assert!(
        candidate.seqs.is_empty(),
        "the excluded author raises nothing"
    );
    let refused = wait
        .receiver
        .await
        .expect("the call stands")
        .expect_err("the range is refused");
    assert!(matches!(
        refused,
        CourierError::Refused(CourierRefusal::SequenceGap { .. })
    ));
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::CourierOnly))
    ));
    conn.adopt_peer_offer(
        &author_of(AUTHOR),
        &offer(Some(1)),
        &Frontier {
            seq: 0,
            head: [77; 32],
        },
        CERT_END,
    )
    .expect("a new offer stands");
    let candidate = expect_candidates(conn.pump_one().await.expect("the offer restates"));
    assert_eq!(candidate.seqs, [1]);
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A range whose answer overruns its bound drops the wait at the deadline,
/// holds the range open for the late answer, which settles the rows, and the
/// queued range goes out after.
#[tokio::test]
async fn over_bound_wait_drops_and_late_answer_settles() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&real_changeset(1)),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the queued"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let range_a = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    conn.courier
        .open
        .as_mut()
        .expect("the range is open")
        .deadline = Duration::ZERO;
    let late = conn
        .courier(OTHER)
        .await
        .expect_err("queued behind the late answer");
    assert!(matches!(late, CourierError::QueuedBehindLateAnswer));
    let outcome = conn
        .pump_one_or(std::future::ready(()))
        .await
        .expect("the step stands");
    assert!(outcome.is_none(), "the lapse stands no event");
    let reply = wait_a
        .receiver
        .await
        .expect("the wait stands")
        .expect_err("the bound ran");
    assert!(matches!(reply, CourierError::TimedOut));
    deliver_answer(
        &mut server,
        &range_a.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    let mut pump = Box::pin(conn.pump_one());
    let range_b = tokio::select! {
        range = next_range(&mut server) => range,
        event = &mut pump => panic!("the pump surfaced {event:?} before the queued range went out"),
    };
    drop(pump);
    assert_eq!(
        range_b.author, OTHER,
        "the queued range goes out after the late answer"
    );
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range_b.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("settle the late answer");
    assert!(matches!(event, ClientEvent::Closed));
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::Retained)),
    ));
    assert!(matches!(
        peer::state_of(&mut conn.db, &OTHER, 1),
        Ok(Some(HeldState::Retained)),
    ));
}

/// A call queued behind a range whose wait already dropped answers the
/// queued refusal at once, the range queued without a wait behind it.
#[tokio::test]
async fn call_queued_behind_lapsed_range_answers_at_once() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(&real_changeset(1)),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the queued"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let _wait = conn.courier(AUTHOR).await.expect("send the range");
    let _range = next_range(&mut server).await;
    conn.courier
        .open
        .as_mut()
        .expect("the range is open")
        .deadline = Duration::ZERO;
    let late = conn.courier(OTHER).await.expect_err("answers at once");
    assert!(matches!(late, CourierError::QueuedBehindLateAnswer));
    assert_eq!(conn.courier.queued.len(), 1, "the range is queued");
    assert!(
        conn.courier
            .open
            .as_ref()
            .expect("the range stays open")
            .wait_dropped
    );
    server.close().await.expect("hang up");
}

/// A lost session ends every open and queued wait with `SessionLost` and keeps
/// the rows held.
#[tokio::test]
async fn lost_session_ends_wait_keeps_rows() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let _range_a = next_range(&mut server).await;
    let wait_b = conn.courier(OTHER).await.expect("queue the second range");
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the loss stands");
    assert!(matches!(event, ClientEvent::Closed));
    let lost_a = wait_a
        .receiver
        .await
        .expect("the first wait stands")
        .expect_err("the session is lost");
    assert!(matches!(lost_a, CourierError::SessionLost));
    let lost_b = wait_b
        .receiver
        .await
        .expect("the second wait stands")
        .expect_err("the session is lost");
    assert!(matches!(lost_b, CourierError::SessionLost));
    for author in [AUTHOR, OTHER] {
        assert!(matches!(
            peer::state_of(&mut conn.db, &author, 1),
            Ok(Some(HeldState::Provisional))
        ));
    }
    assert!(matches!(
        conn.courier(AUTHOR).await,
        Err(CourierError::Unavailable)
    ));
}

/// The candidates restate on every change of the set, an empty set included,
/// and never without one.
#[tokio::test]
async fn candidates_raised_on_each_change_not_without_one() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::CourierOnly,
        major(2),
        Some(b"the second"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[(1, Verdict::Pending), (2, Verdict::Pending)],
    )
    .await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [1, 2]);
    assert_eq!(candidate.last, Some(Verdict::Pending));
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Applied)]).await;
    let event = conn.pump_one().await.expect("the push settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [2]);
    deliver_verdicts(&mut server, &ask.request_id, &[(2, Verdict::Applied)]).await;
    let event = conn.pump_one().await.expect("the push settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert!(candidate.seqs.is_empty(), "the empty set raises once");
    // A ruling the rows no longer await changes nothing and raises nothing.
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Settled)]).await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(
        matches!(event, ClientEvent::Closed),
        "a repeated settled ruling raises no event"
    );
}

/// The candidates read reports the account, the sequences, the bytes the
/// bodies stand and the last ruling heard on the run's lowest row.
#[tokio::test]
async fn candidates_read_reports_bytes_and_last() {
    let body = b"12345";
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(body),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let _ = expect_candidates(conn.pump_one().await.expect("restate"));
    let candidates = conn.courier_candidates().expect("read the candidates");
    assert_eq!(candidates.len(), 1);
    let candidate = &candidates[0];
    assert_eq!(candidate.author, AUTHOR.to_vec());
    assert_eq!(candidate.account, "courier-author");
    assert_eq!(candidate.seqs, [1]);
    assert_eq!(candidate.bytes, 5);
    assert_eq!(candidate.last, Some(Verdict::Pending));
}

/// An author never ruled names no last, its candidate standing from its held
/// rows alone.
#[tokio::test]
async fn candidates_last_is_none_without_a_ruling() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the only row"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, _server) = attach(conn, true).await;
    let candidates = conn.courier_candidates().expect("read the candidates");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].seqs, [1]);
    assert_eq!(candidates[0].last, None, "no ruling was ever heard");
}

/// The pump's own tick drops the wait on a courier range that overran its
/// bound, the range staying open for its late answer (decision 33).
#[tokio::test]
async fn the_pump_drops_the_lapsed_range_wait() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let _range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    conn.courier
        .open
        .as_mut()
        .expect("the range is open")
        .deadline = Duration::ZERO;
    let event = conn
        .pump_one_or(std::future::ready(()))
        .await
        .expect("the step stands");
    assert!(event.is_none(), "the lapse stands no event");
    assert!(
        conn.courier
            .open
            .as_ref()
            .expect("the range stays open")
            .wait_dropped
    );
    let reply = wait
        .receiver
        .await
        .expect("the wait stands")
        .expect_err("the bound ran");
    assert!(matches!(reply, CourierError::TimedOut));
}

/// An answer naming no range this connection sent stands a non-fatal notice
/// and never touches the session's state.
#[tokio::test]
async fn an_answer_with_no_open_range_stands_nonfatal() {
    let (mut conn, mut server) = attach(replica(), true).await;
    deliver_answer(
        &mut server,
        "courier-999",
        CourierOutcome::Entries(Vec::new()),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the stranger stands");
    let ClientEvent::NonFatal { related_to, detail } = event else {
        panic!("the stranger stands a notice, not {event:?}");
    };
    assert_eq!(related_to.as_deref(), Some("courier-999"));
    assert!(detail.contains("no open range"), "{detail}");
}

/// A queued range whose rows no longer stand hears `NothingHeld` at once
/// when the open range settles, the queue going on without it.
#[tokio::test]
async fn a_queued_range_whose_rows_are_gone_hears_nothing_held() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let range_a = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let wait_b = conn.courier(OTHER).await.expect("queue the second range");
    // The second author's run is gone before the first range settles.
    conn.db
        .batch_execute(&format!(
            "DELETE FROM _connetto_peer_entry WHERE author = x'{}'; \
             DELETE FROM _connetto_peer_cursor WHERE author = x'{}'",
            hex_of(OTHER),
            hex_of(OTHER)
        ))
        .expect("drop the other author's run");
    deliver_answer(
        &mut server,
        &range_a.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the first range settles");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait_a
        .receiver
        .await
        .expect("the first call stands")
        .expect("the first range settles");
    assert_eq!(report.settled, [1]);
    let gone = wait_b
        .receiver
        .await
        .expect("the second call stands")
        .expect_err("the rows are gone");
    assert!(matches!(gone, CourierError::NothingHeld));
}

/// A queued range whose rows no longer decode fails the flush and the pump
/// step, the settled range having already heard its report.
#[tokio::test]
async fn a_queued_range_with_corrupt_rows_fails_the_flush() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let range_a = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _wait_b = conn.courier(OTHER).await.expect("queue the second range");
    conn.db
        .batch_execute(&format!(
            "UPDATE _connetto_peer_entry SET digest = x'{}' WHERE author = x'{}'",
            "00".repeat(8),
            hex_of(OTHER)
        ))
        .expect("corrupt the other author's digest");
    deliver_answer(
        &mut server,
        &range_a.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    let err = conn
        .pump_one()
        .await
        .expect_err("the flush fails on the corrupt rows");
    assert!(err.to_string().contains("holds no digest"), "{err}");
    let report = wait_a
        .receiver
        .await
        .expect("the first call stands")
        .expect("the first range settles");
    assert_eq!(report.settled, [1]);
}

/// A flush whose send cannot reach the server requeues the range behind the
/// error, and the session's end ends the requeued wait.
#[tokio::test]
async fn a_flush_send_failure_requeues_the_range() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let range_a = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let wait_b = conn.courier(OTHER).await.expect("queue the second range");
    deliver_answer(
        &mut server,
        &range_a.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    server.close().await.expect("hang up");
    let err = conn
        .pump_one()
        .await
        .expect_err("the flush cannot send the queued range");
    assert!(matches!(err, crate::ClientError::Transport(_)), "{err}");
    assert_eq!(
        conn.courier.queued.len(),
        1,
        "the range goes back to the queue"
    );
    let report = wait_a
        .receiver
        .await
        .expect("the first call stands")
        .expect("the first range settles");
    assert_eq!(report.settled, [1]);
    // The tick finds the send still failing, the range queued again.
    let err = conn
        .pump_one()
        .await
        .expect_err("the pump's tick finds the send still failing");
    assert!(matches!(err, crate::ClientError::Transport(_)), "{err}");
    let err = conn
        .pump_one_or(std::future::ready(()))
        .await
        .expect_err("the step's tick finds the send still failing");
    assert!(matches!(err, crate::ClientError::Transport(_)), "{err}");
    conn.db
        .batch_execute(&format!(
            "DELETE FROM _connetto_peer_entry WHERE author = x'{}'; \
             DELETE FROM _connetto_peer_cursor WHERE author = x'{}'",
            hex_of(OTHER),
            hex_of(OTHER)
        ))
        .expect("drop the other author's run");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
    let gone = wait_b
        .receiver
        .await
        .expect("the second call stands")
        .expect_err("the rows are gone");
    assert!(matches!(gone, CourierError::NothingHeld));
}

/// An author with held rows but no stored offer holds nothing to courier.
#[tokio::test]
async fn an_author_without_a_stored_offer_hears_nothing_held() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    let (mut conn, _server) = attach(conn, true).await;
    let err = conn.courier(AUTHOR).await.expect_err("no offer stands");
    assert!(matches!(err, CourierError::NothingHeld));
}

/// A stored offer whose author holds no standing rows holds nothing to
/// courier.
#[tokio::test]
async fn an_offer_without_standing_rows_hears_nothing_held() {
    let mut conn = replica();
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, _server) = attach(conn, true).await;
    let err = conn.courier(AUTHOR).await.expect_err("nothing stands");
    assert!(matches!(err, CourierError::NothingHeld));
}

/// A stored offer that does not decode refuses the call rather than stand a
/// range from it.
#[tokio::test]
async fn a_stored_offer_that_does_not_decode_refuses_the_call() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, _server) = attach(conn, true).await;
    conn.db
        .batch_execute("UPDATE _connetto_peer_cursor SET offer = x'00'")
        .expect("corrupt the stored offer");
    let err = conn
        .courier(AUTHOR)
        .await
        .expect_err("the offer does not decode");
    assert!(
        matches!(err, CourierError::Client(crate::ClientError::Session(_))),
        "{err:?}"
    );
    assert!(
        err.to_string().contains("a stored offer does not decode"),
        "{err}"
    );
}

/// A cursor whose author key is not a device key is skipped in the
/// candidates, never an error.
#[tokio::test]
async fn a_cursor_of_another_length_is_skipped_in_the_candidates() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, _server) = attach(conn, true).await;
    conn.db
        .batch_execute(&format!(
            "INSERT INTO _connetto_peer_cursor \
             (author, account, seq, head, offer, expires_at) \
             VALUES (x'{short}', 'short', 0, zeroblob(32), x'', {CERT_END})",
            short = "42".repeat(5)
        ))
        .expect("store the short cursor");
    let candidates = conn.courier_candidates().expect("read the candidates");
    assert_eq!(
        candidates.len(),
        1,
        "the short key is skipped, not an error: {candidates:?}"
    );
    assert_eq!(candidates[0].author, AUTHOR.to_vec());
}

/// A retained-only author stands no candidate, its settled rows out of the
/// run the main wire would send.
#[tokio::test]
async fn a_cursor_without_standing_rows_is_absent_from_the_candidates() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the retained"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, _server) = attach(conn, true).await;
    let candidates = conn.courier_candidates().expect("read the candidates");
    assert!(candidates.is_empty(), "{candidates:?}");
}

/// An offer whose newest runs past what a sequence stores clips the bound
/// instead of refusing the run.
#[tokio::test]
async fn an_offer_newest_past_the_replica_reads_every_row() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the held"),
    );
    adopt(&mut conn, AUTHOR, Some(u64::MAX));
    let (mut conn, _server) = attach(conn, true).await;
    let candidates = conn.courier_candidates().expect("the bound clips");
    assert_eq!(candidates.len(), 1, "{candidates:?}");
    assert_eq!(candidates[0].seqs, [1]);
}

/// A provisional ask that overran its bound keeps its author's candidates
/// restated, the ask waiting out the lapse for a fresh session.
#[tokio::test]
async fn a_lapsed_ask_restates_the_author_candidates() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the held"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let _ask = next_ask(&mut server).await;
    conn.provisional
        .in_flight
        .as_mut()
        .expect("the ask is in flight")
        .deadline = Duration::ZERO;
    let event = conn
        .pump_one_or(std::future::ready(()))
        .await
        .expect("the lapse stands")
        .expect("the lapse restates");
    let candidate = expect_candidates(event);
    assert_eq!(candidate.seqs, [1]);
    assert!(conn.provisional.in_flight.is_none(), "the ask is dropped");
    assert!(
        conn.provisional.pending.is_empty(),
        "the lapse keeps no ask for this session"
    );
}

/// A verdict the server ruled Pending keeps the row held and the report's
/// kept list, the row no nearer settled.
#[tokio::test]
async fn a_pending_verdict_keeps_the_row_in_the_report() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the held"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let _ask = next_ask(&mut server).await;
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Pending))]),
    )
    .await;
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [1]);
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("end the session");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.kept, [1]);
    assert!(
        report.settled.is_empty() && report.refused.is_empty(),
        "{report:?}"
    );
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::CourierOnly))
    ));
}

/// A call that finds the open range's wait already dropped hears the queue
/// at once, the dropped range keeping no reply to send again.
#[tokio::test]
async fn a_call_after_a_lapsed_range_hears_the_queue_at_once() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the other"),
    );
    hold(
        &mut conn,
        THIRD,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the third"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    adopt(&mut conn, THIRD, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let _range = next_range(&mut server).await;
    conn.courier
        .open
        .as_mut()
        .expect("the range is open")
        .deadline = Duration::ZERO;
    let queued = conn
        .courier(OTHER)
        .await
        .expect_err("queued behind the late answer");
    assert!(matches!(queued, CourierError::QueuedBehindLateAnswer));
    let again = conn
        .courier(THIRD)
        .await
        .expect_err("the wait is already dropped");
    assert!(matches!(again, CourierError::QueuedBehindLateAnswer));
    assert_eq!(conn.courier.queued.len(), 2, "both ranges are queued");
    let reply = wait_a
        .receiver
        .await
        .expect("the first wait stands")
        .expect_err("the bound ran");
    assert!(matches!(reply, CourierError::TimedOut));
}

/// A lost session drains a queued range whose wait already dropped, keeping
/// no reply to end, and ends the session without a second send.
#[tokio::test]
async fn a_lost_session_ends_a_wait_that_already_dropped() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let _range = next_range(&mut server).await;
    conn.courier
        .open
        .as_mut()
        .expect("the range is open")
        .deadline = Duration::ZERO;
    let queued = conn
        .courier(OTHER)
        .await
        .expect_err("queued behind the late answer");
    assert!(matches!(queued, CourierError::QueuedBehindLateAnswer));
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the loss stands");
    assert!(matches!(event, ClientEvent::Closed));
    // The open range's wait dropped on the lapse, so the loss keeps no reply
    // to end it, and the queued range queued without one.
    let reply = wait_a
        .receiver
        .await
        .expect("the first wait stands")
        .expect_err("the bound ran on the lapse");
    assert!(matches!(reply, CourierError::TimedOut));
}

/// A taken row whose author key is not a device key stands its offer and
/// entry, naming no candidate the key cannot carry.
#[tokio::test]
async fn a_short_author_key_stands_its_offer_without_a_candidate() {
    let conn0 = replica();
    let (mut conn, _server) = attach(conn0, true).await;
    let body = b"the taken row";
    let digest: [u8; 32] = Sha256::digest(body).into();
    let row = ManifestRow {
        seq: 1,
        major: major(2),
        origin: None,
        digest,
    };
    let author = PeerAuthor {
        key_id: vec![0x42; 10],
        account: "short".to_owned(),
        bot_list: None,
    };
    let encoded = rmp_serde::to_vec_named(&offer(Some(1))).expect("the offer encodes");
    crate::exchange::upsert_cursor(
        &mut conn.db,
        &author.key_id,
        "short",
        &Frontier {
            seq: 0,
            head: GENESIS,
        },
        &encoded,
        CERT_END,
    )
    .expect("adopt the short key's offer");
    let cursor = Frontier {
        seq: 1,
        head: [7; 32],
    };
    conn.take_peer_row(&author, &row, None, &cursor)
        .expect("the short key stands its offer");
    assert!(matches!(
        peer::state_of(&mut conn.db, &author.key_id, 1),
        Ok(Some(HeldState::DigestOnly))
    ));
    assert!(
        conn.notices
            .iter()
            .all(|event| !matches!(event, ClientEvent::CourierCandidates(_))),
        "the short key names no candidate"
    );
}

/// A taken row whose author holds a corrupt entry behind it refuses the
/// offer restatement, the taken row keeping its hold.
#[tokio::test]
async fn a_corrupt_row_behind_the_taken_one_refuses_the_offer() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the corrupt"),
    );
    conn.db
        .batch_execute(&format!(
            "UPDATE _connetto_peer_entry SET digest = x'{}' WHERE author = x'{}'",
            "00".repeat(8),
            hex_of(AUTHOR)
        ))
        .expect("corrupt the earlier row");
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, _server) = attach(conn, true).await;
    let body = b"the taken row";
    let digest: [u8; 32] = Sha256::digest(body).into();
    let row = ManifestRow {
        seq: 2,
        major: major(2),
        origin: None,
        digest,
    };
    let cursor = Frontier {
        seq: 2,
        head: [8; 32],
    };
    let err = conn
        .take_peer_row(&author_of(AUTHOR), &row, None, &cursor)
        .expect_err("the corrupter refuses the offer");
    assert!(err.to_string().contains("holds no digest"), "{err}");
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 2),
        Ok(Some(HeldState::DigestOnly))
    ));
}

/// A stored major the build cannot express refuses the held-row read, never
/// a row misread.
#[test]
fn a_major_the_build_cannot_express_refuses_the_rows() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    conn.db
        .batch_execute("UPDATE _connetto_peer_entry SET major = 5000000000")
        .expect("corrupt the major");
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX)
        .err()
        .expect("the major stands no build");
    assert!(
        err.to_string().contains("a major the build cannot express"),
        "{err}"
    );
}

/// A stored major no schema declares refuses the held-row read the same
/// way.
#[test]
fn a_major_no_schema_declares_refuses_the_rows() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    conn.db
        .batch_execute("UPDATE _connetto_peer_entry SET major = 0")
        .expect("corrupt the major");
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX)
        .err()
        .expect("zero declares no major");
    assert!(
        err.to_string().contains("a major the build cannot express"),
        "{err}"
    );
}

/// A stored state no held row stores refuses the held-row read.
#[test]
fn an_unknown_state_refuses_the_rows() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    conn.db
        .batch_execute("UPDATE _connetto_peer_entry SET state = 99")
        .expect("corrupt the state");
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX)
        .err()
        .expect("the state stands no held row");
    assert!(err.to_string().contains("no held state"), "{err}");
}

/// An origin that does not decode refuses the held-row read.
#[test]
fn an_unreadable_origin_refuses_the_rows() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    conn.db
        .batch_execute("UPDATE _connetto_peer_entry SET origin = x'ff'")
        .expect("corrupt the origin");
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX)
        .err()
        .expect("the origin does not decode");
    assert!(err.to_string().contains("an unreadable origin"), "{err}");
}

/// A digest of another length refuses the held-row read.
#[test]
fn a_digest_of_another_length_refuses_the_rows() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the held"),
    );
    conn.db
        .batch_execute(&format!(
            "UPDATE _connetto_peer_entry SET digest = x'{}'",
            "00".repeat(8)
        ))
        .expect("corrupt the digest");
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX)
        .err()
        .expect("the digest holds no 32 bytes");
    assert!(err.to_string().contains("no digest"), "{err}");
}
