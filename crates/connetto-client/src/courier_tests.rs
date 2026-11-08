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
