//! The courier's offer on the main wire, R78 slice 4: the session stands the
//! author's held rows behind its stored offer, one range at a time, the
//! server ruling each row and the candidates restating as the run moves.

use connetto_core::chain::GENESIS;
use connetto_core::exchange::{
    CourierEntryOutcome, CourierOutcome, CourierRefusal, Frontier, ManifestRow, SignedOffer,
    Verdict,
};
use connetto_core::messages::{
    BulkMessage, ControlMessage, CourierAnswer, CourierBody, CourierRange, FullResyncReason,
    FullResyncRequired, ProvisionalStatus, ProvisionalVerdicts, SnapshotEnd, WriterGrant,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, LoopbackTransport, Major, SchemaVersion, loopback};
use core::time::Duration;
use diesel::RunQueryDsl as _;
use diesel::connection::SimpleConnection as _;
use sha2::{Digest, Sha256};
use tokio::time::timeout;

use crate::drain::tests::major;
use crate::drain::tests::{
    attach_scripted, handshake_ack, hex, hold_entry, main_version, next, offer_with, store_offer,
};
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

fn version() -> SchemaVersion {
    main_version()
}

/// A major 2 replica with no wire and nothing queued.
fn replica() -> Conn {
    crate::drain::tests::replica_named("courier", crate::SyncTuning::default())
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
    hold_entry(conn, author, seq, state, major, body);
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
    offer_with(&[(major(2), version())], newest)
}

/// Store `author`'s cursor with the test's offer.
fn adopt(conn: &mut Conn, author: [u8; 32], newest: Option<u64>) {
    store_offer(conn, author, &offer(newest));
}

/// The signed offer the tests stand for a fully synced author, the queue
/// empty and `settled` naming the acked frontier.
fn settled_offer(settled: u64) -> SignedOffer {
    SignedOffer {
        offer: connetto_core::exchange::Offer {
            builds: vec![(major(2), version())],
            settled: Some(Frontier {
                seq: settled,
                head: [77; 32],
            }),
            newest: None,
            retractions: Vec::new(),
        },
        signature: vec![1, 2, 3],
    }
}

/// Store `author`'s cursor with the settled-only offer.
fn adopt_settled(conn: &mut Conn, author: [u8; 32], settled: u64) {
    store_offer(conn, author, &settled_offer(settled));
}

/// Attach the replica to a scripted server whose courier path is `on`,
/// answering the handshake and draining the state notices.
async fn attach(conn: Conn, on: bool) -> (Conn, LoopbackTransport) {
    let (client, server) = loopback();
    attach_scripted(
        conn,
        client,
        server,
        None,
        handshake_ack(
            "courier",
            Some(version()),
            Some(WriterGrant {
                number: 1,
                frontier: 0,
            }),
            on,
        ),
        |event| matches!(event, ClientEvent::SyncStatus(_)),
    )
    .await
}

/// Attach a main replica that carries a device identity to a scripted server
/// whose courier path is `on`. The handshake presents the device writer, so
/// the server challenges it, answers without a grant, and binds the writer.
async fn attach_device(conn: Conn, on: bool) -> (Conn, LoopbackTransport) {
    let (client, server) = loopback();
    attach_scripted(
        conn,
        client,
        server,
        Some([5; 32]),
        handshake_ack("courier", Some(version()), None, on),
        |event| matches!(event, ClientEvent::SyncStatus(_)),
    )
    .await
}

/// The server's full resync, `FullResyncRequired` first and the replacement
/// snapshot's end, pumped to the end event.
async fn resync(conn: &mut Conn, server: &mut LoopbackTransport) {
    server
        .send_control(ControlMessage::FullResyncRequired(FullResyncRequired {
            sub_id: "items".to_owned(),
            reason: FullResyncReason::CursorOutsideRetention,
        }))
        .await
        .expect("resync");
    let event = conn.pump_one().await.expect("the resync lands");
    assert!(matches!(event, ClientEvent::FullResync { .. }));
    server
        .send_control(ControlMessage::SnapshotEnd(SnapshotEnd {
            sub_id: "items".to_owned(),
            cursor: Cursor::new(Vec::new()),
        }))
        .await
        .expect("end the snapshot");
    loop {
        match conn.pump_one().await.expect("pump") {
            ClientEvent::SnapshotEnd { .. } => return,
            ClientEvent::Closed => panic!("the session ended mid resync"),
            _ => {}
        }
    }
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
    assert_eq!(ask.seqs, [1, 2, 3, 4], "every unretained row is asked");
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

/// The run is of the build's major, so the first range stops at the first
/// row of another major, the build's later row standing its own range
/// behind it (R78 slice 4).
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
    let _ask = next_ask(&mut server).await;
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
    assert_eq!(
        candidates[0].seqs,
        [1, 3],
        "the build's rows stand, the foreign row held"
    );
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    let event = conn.pump_one().await.expect("the first range settles");
    assert_eq!(
        expect_candidates(event).seqs,
        [3],
        "the settled range leaves the build's later row"
    );
    let range = next_range(&mut server).await;
    assert_eq!(
        range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [3],
        "the build's later row stands its own range"
    );
    let body = next_body(&mut server).await;
    assert_eq!(body.seq, 3);
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![(3, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    let event = conn.pump_one().await.expect("the call ends");
    assert_eq!(
        expect_candidates(event).seqs,
        [] as [u64; 0],
        "the author's candidates restate empty"
    );
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("settled");
    assert_eq!(report.settled, [1, 3]);
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 1),
        Ok(Some(HeldState::Retained))
    ));
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 2),
        Ok(Some(HeldState::Provisional))
    ));
    assert!(matches!(
        peer::state_of(&mut conn.db, &AUTHOR, 3),
        Ok(Some(HeldState::Retained))
    ));
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
    let _ask = next_ask(&mut server).await;
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
    let _ask = next_ask(&mut server).await;
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

/// A provisional application row whose guard refuses its undo.
fn guarded_entry() -> Conn {
    let body = real_changeset(1);
    let mut conn = replica();
    let row = ManifestRow {
        seq: 1,
        major: major(2),
        origin: None,
        digest: Sha256::digest(&body).into(),
    };
    assert!(matches!(
        conn.apply_peer_entry(&author_of(AUTHOR), &row, &body)
            .expect("apply the author's row"),
        peer::PeerApplyOutcome::Applied { .. }
    ));
    adopt(&mut conn, AUTHOR, Some(1));
    conn.conn()
        .batch_execute(
            "CREATE TEMP TRIGGER keep_items BEFORE DELETE ON main.items \
             BEGIN SELECT RAISE(IGNORE); END;",
        )
        .expect("guard the application row");
    conn
}

#[tokio::test]
async fn a_store_refused_undo_fails_provisional_and_courier_rulings() {
    use diesel::{ExpressionMethods as _, QueryDsl as _};

    diesel::table! {
        items (id) {
            id -> BigInt,
        }
    }

    Box::pin(timeout(BOUND, async {
        for by_courier in [false, true] {
            let mut conn = guarded_entry();
            let records = conn.provisional_rows().expect("read the provenance");
            assert_eq!(records.len(), 1);
            let (mut conn, mut server) = attach(conn, true).await;
            let ask = next_ask(&mut server).await;
            let _wait = if by_courier {
                let wait = conn.courier(AUTHOR).await.expect("send the range");
                let range = next_range(&mut server).await;
                let _ = next_body(&mut server).await;
                deliver_answer(
                    &mut server,
                    &range.request_id,
                    CourierOutcome::Entries(vec![(
                        1,
                        CourierEntryOutcome::Ruled(Verdict::Abandoned),
                    )]),
                )
                .await;
                Some(wait)
            } else {
                deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Abandoned)]).await;
                None
            };
            let error = loop {
                match conn.pump_one().await {
                    Err(error) => break error,
                    Ok(event) => assert!(
                        !matches!(
                            event,
                            ClientEvent::ProvisionalRetracted { .. } | ClientEvent::Closed
                        ),
                        "the failed ruling must preserve the row, got {event:?}"
                    ),
                }
            };
            assert!(
                matches!(&error, crate::ClientError::Apply(detail)
                    if detail.contains("refused putting back")),
                "courier={by_courier}, {error}"
            );
            assert_eq!(
                items::table
                    .filter(items::id.eq(1_i64))
                    .count()
                    .get_result::<i64>(conn.conn())
                    .expect("read the guarded row"),
                1
            );
            assert_eq!(
                conn.provisional_rows().expect("read the provenance"),
                records
            );
            assert!(matches!(
                peer::state_of(&mut conn.db, &AUTHOR, 1),
                Ok(Some(HeldState::Provisional))
            ));

            conn.conn()
                .batch_execute("DROP TRIGGER keep_items")
                .expect("release the row guard");
            conn.retract_peer_entry(
                &author_of(AUTHOR),
                &connetto_core::exchange::Retraction {
                    seq: 1,
                    cause: connetto_core::exchange::RetractCause::Abandoned,
                },
            )
            .expect("retry the retraction");
            assert_eq!(
                items::table
                    .count()
                    .get_result::<i64>(conn.conn())
                    .expect("read the application rows"),
                0
            );
            assert_eq!(
                conn.provisional_rows().expect("read the provenance"),
                Vec::new()
            );
            assert!(matches!(peer::state_of(&mut conn.db, &AUTHOR, 1), Ok(None)));
        }
    }))
    .await
    .expect("both ruling failures arrive within the bound");
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
    let _ask = next_ask(&mut server).await;
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
    let (mut conn, mut server) = Box::pin(attach(conn, true)).await;
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
    let retained = next_ask(&mut server).await;
    assert_eq!(
        retained.seqs,
        [3],
        "the session asks about the retained row"
    );
    deliver_verdicts(&mut server, &retained.request_id, &[(3, Verdict::Settled)]).await;
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
    let _ask = next_ask(&mut server).await;
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
    let _ask = next_ask(&mut server).await;
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
    let _ask = next_ask(&mut server).await;
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
    let _ask = next_ask(&mut server).await;
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
    let _ask = next_ask(&mut server).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let range_a = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let wait_b = conn.courier(OTHER).await.expect("queue the second range");
    // The second author's run is gone before the first range settles.
    conn.db
        .batch_execute(&format!(
            "DELETE FROM _connetto_peer_entry WHERE author = x'{}'; \
             DELETE FROM _connetto_peer_cursor WHERE author = x'{}'",
            hex(&OTHER),
            hex(&OTHER)
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
    let _ask = next_ask(&mut server).await;
    let wait_a = conn.courier(AUTHOR).await.expect("send the first range");
    let range_a = next_range(&mut server).await;
    let _ = next_body(&mut server).await;
    let _wait_b = conn.courier(OTHER).await.expect("queue the second range");
    conn.db
        .batch_execute(&format!(
            "UPDATE _connetto_peer_entry SET digest = x'{}' WHERE author = x'{}'",
            "00".repeat(8),
            hex(&OTHER)
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
    let _ask = next_ask(&mut server).await;
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
            hex(&OTHER),
            hex(&OTHER)
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

/// A stored offer without a newest binds the ask and the range to its
/// settled frontier, and a row past it stays held, unasked.
#[tokio::test]
async fn an_offer_without_a_newest_binds_the_range_to_the_settled_frontier() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Provisional,
        major(2),
        Some(b"the first"),
    );
    hold(&mut conn, AUTHOR, 2, HeldState::DigestOnly, major(2), None);
    hold(&mut conn, AUTHOR, 3, HeldState::DigestOnly, major(2), None);
    hold(
        &mut conn,
        AUTHOR,
        4,
        HeldState::Provisional,
        major(2),
        Some(b"the held above"),
    );
    adopt_settled(&mut conn, AUTHOR, 3);
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    assert_eq!(
        ask.seqs,
        [1, 2, 3],
        "the ask stops at the offer's frontier, which the server refuses past"
    );
    let wait = conn.courier(AUTHOR).await.expect("send the range");
    let range = next_range(&mut server).await;
    let signed: SignedOffer =
        rmp_serde::from_slice(&range.offer[..]).expect("the stored offer decodes");
    assert!(signed.offer.newest.is_none(), "the offer signs no newest");
    assert_eq!(
        signed.offer.settled,
        Some(Frontier {
            seq: 3,
            head: [77; 32]
        })
    );
    assert_eq!(
        range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1, 2, 3],
        "the row past the settled frontier is not offered"
    );
    assert_eq!(range.bodies, 1, "only row 1 holds a body");
    let body = next_body(&mut server).await;
    assert_eq!(body.seq, 1);
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![
            (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (2, CourierEntryOutcome::Ruled(Verdict::Applied)),
            (3, CourierEntryOutcome::Ruled(Verdict::Applied)),
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
    assert_eq!(report.settled, [1, 2, 3]);
    assert!(
        matches!(
            peer::state_of(&mut conn.db, &AUTHOR, 4),
            Ok(Some(HeldState::Provisional))
        ),
        "row 4 stays held"
    );
}

/// A stored offer that signs neither a settled nor a newest frontier covers
/// no sequence, so the session asks nothing the server would refuse whole.
#[tokio::test]
async fn an_offer_without_a_frontier_asks_nothing() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::CourierOnly,
        major(2),
        Some(b"the held"),
    );
    adopt(&mut conn, AUTHOR, None);
    let (mut conn, mut server) = attach(conn, true).await;
    assert!(
        timeout(Duration::from_millis(100), server.recv())
            .await
            .is_err(),
        "no ask names a sequence the offer does not sign"
    );
    assert!(
        matches!(
            peer::state_of(&mut conn.db, &AUTHOR, 1),
            Ok(Some(HeldState::CourierOnly))
        ),
        "the row stays held for the author's next offer"
    );
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
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
    let _ask = next_ask(&mut server).await;
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
    let _ask = next_ask(&mut server).await;
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
/// entry, naming no candidate the key cannot carry, and the next session
/// asks nothing about it.
#[tokio::test]
async fn a_short_author_key_stands_its_offer_without_a_candidate() {
    let conn0 = replica();
    let (mut conn, mut server) = attach(conn0, true).await;
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
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
    let (_conn, mut server) = attach(conn, true).await;
    assert!(
        timeout(Duration::from_millis(100), server.recv())
            .await
            .is_err(),
        "no ask names a key the server cannot rule"
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
            hex(&AUTHOR)
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
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX, None)
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
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX, None)
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
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX, None)
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
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX, None)
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
    let err = peer::held_at_or_below(&mut conn.db, &AUTHOR, i64::MAX, None)
        .err()
        .expect("the digest holds no 32 bytes");
    assert!(err.to_string().contains("no digest"), "{err}");
}

/// The attach asks the retained rows, and a full resync's end asks nothing more (R114 decision 1).
#[tokio::test]
async fn attach_asks_retained_rows_and_a_resync_end_asks_nothing_more() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::Retained,
        major(2),
        Some(b"the second"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    assert_eq!(ask.author, AUTHOR);
    assert_eq!(ask.seqs, [1, 2]);
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[(1, Verdict::Applied), (2, Verdict::Applied)],
    )
    .await;
    resync(&mut conn, &mut server).await;
    assert!(
        timeout(Duration::from_millis(100), server.recv())
            .await
            .is_err(),
        "the resync's end asks nothing the attach asked"
    );
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
    for seq in [1, 2] {
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, seq),
                Ok(Some(HeldState::Retained)),
            ),
            "the row stays retained"
        );
    }
}

/// A restart after a Pending answer asks again, and the Pending answer stands the row a candidate again (R114 decision 1).
#[tokio::test]
async fn restart_after_a_pending_answer_stands_the_candidacy_again() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the only row"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [1]);
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    assert_eq!(ask.author, AUTHOR);
    assert_eq!(ask.seqs, [1], "the restart asks the retained row again");
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [1], "the candidacy stands again");
    assert_eq!(candidate.last, Some(Verdict::Pending));
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
}

/// An Abandoned answer deletes the retained rows it names and no other, a Settled one beside it staying retained (R114 decision 2).
#[tokio::test]
async fn abandoned_answer_deletes_only_the_retained_rows_it_names() {
    let mut conn = replica();
    for (seq, body) in [
        (1u64, b"the first" as &[u8]),
        (2, b"the second" as &[u8]),
        (3, b"the third" as &[u8]),
    ] {
        hold(
            &mut conn,
            AUTHOR,
            seq,
            HeldState::Retained,
            major(2),
            Some(body),
        );
    }
    adopt(&mut conn, AUTHOR, Some(3));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    assert_eq!(ask.seqs, [1, 2, 3]);
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[
            (1, Verdict::Settled),
            (2, Verdict::Abandoned),
            (3, Verdict::Abandoned),
        ],
    )
    .await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    assert!(
        matches!(
            peer::state_of(&mut conn.db, &AUTHOR, 1),
            Ok(Some(HeldState::Retained)),
        ),
        "the settled row stays retained"
    );
    for seq in [2, 3] {
        assert!(
            matches!(peer::state_of(&mut conn.db, &AUTHOR, seq), Ok(None)),
            "the abandoned row {seq} is deleted"
        );
    }
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(
        matches!(event, ClientEvent::Closed),
        "no candidate stood, so none restates"
    );
}

/// A pushed Abandoned deletes a candidate and restates the author's emptied set (R114 decision 2).
#[tokio::test]
async fn pushed_abandoned_deletes_a_candidate_and_restates_the_set() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the only row"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [1]);
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Abandoned)]).await;
    let event = conn.pump_one().await.expect("the push settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert!(candidate.seqs.is_empty(), "nothing is left to courier");
    assert!(
        matches!(peer::state_of(&mut conn.db, &AUTHOR, 1), Ok(None)),
        "the abandoned candidate is deleted"
    );
    assert!(
        matches!(conn.courier(AUTHOR).await, Err(CourierError::NothingHeld)),
        "the courier holds nothing of the author"
    );
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A Pending answer to the session's ask stands the rows as candidates with the last ruling heard, and the courier sends them (R78 decisions 45 and 50).
#[tokio::test]
async fn pending_answer_marks_retained_rows_and_the_courier_sends_them() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::Retained,
        major(2),
        Some(b"the second"),
    );
    adopt(&mut conn, AUTHOR, Some(2));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    assert_eq!(ask.seqs, [1, 2]);
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
    let seqs: Vec<u64> = range.rows.iter().map(|row| row.seq).collect();
    assert_eq!(seqs, [1, 2], "the marked rows stand in the range");
    assert_eq!(range.bodies, 2);
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
    server.close().await.expect("hang up");
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert!(candidate.seqs.is_empty(), "the rulings end the candidacy");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1, 2]);
    assert_eq!(report.refused, [] as [u64; 0]);
    for seq in [1, 2] {
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, seq),
                Ok(Some(HeldState::Retained)),
            ),
            "the row stays retained"
        );
    }
}

/// A ruling other than Pending to the session's ask leaves the row retained and out of the run, breaking its contiguity (R78 decision 45).
#[tokio::test]
async fn other_answers_leave_retained_rows_out_of_the_run() {
    let mut conn = replica();
    for (seq, body) in [
        (1u64, b"the first" as &[u8]),
        (2, b"the second" as &[u8]),
        (3, b"the third" as &[u8]),
    ] {
        hold(
            &mut conn,
            AUTHOR,
            seq,
            HeldState::Retained,
            major(2),
            Some(body),
        );
    }
    adopt(&mut conn, AUTHOR, Some(3));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    assert_eq!(ask.seqs, [1, 2, 3]);
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[
            (1, Verdict::Applied),
            (2, Verdict::Pending),
            (3, Verdict::Settled),
        ],
    )
    .await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(
        candidate.seqs,
        [2],
        "the other rulings leave their rows out"
    );
    assert_eq!(candidate.last, Some(Verdict::Pending));
    for seq in [1, 2, 3] {
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, seq),
                Ok(Some(HeldState::Retained)),
            ),
            "the row stays retained"
        );
    }
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A push ruling a retained row the session's ask answered Pending ends its candidacy with the row still retained and the candidates restated (R78 decisions 45 and 46).
#[tokio::test]
async fn push_ending_marked_retained_rows_keeps_them_retained() {
    let mut conn = replica();
    for (seq, body) in [
        (1u64, b"the first" as &[u8]),
        (2, b"the second" as &[u8]),
        (3, b"the third" as &[u8]),
    ] {
        hold(
            &mut conn,
            AUTHOR,
            seq,
            HeldState::Retained,
            major(2),
            Some(body),
        );
    }
    adopt(&mut conn, AUTHOR, Some(3));
    let (mut conn, mut server) = attach(conn, true).await;
    let ask = next_ask(&mut server).await;
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[
            (1, Verdict::Pending),
            (2, Verdict::Pending),
            (3, Verdict::Pending),
        ],
    )
    .await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [1, 2, 3]);
    deliver_verdicts(
        &mut server,
        &ask.request_id,
        &[(1, Verdict::Applied), (2, Verdict::Settled)],
    )
    .await;
    let event = conn.pump_one().await.expect("the push settles");
    assert!(
        matches!(event, ClientEvent::NonFatal { .. }),
        "no row event stands for the ruled rows"
    );
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert_eq!(candidate.seqs, [3], "the set left restates");
    assert_eq!(candidate.last, Some(Verdict::Pending));
    for seq in [1, 2, 3] {
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, seq),
                Ok(Some(HeldState::Retained)),
            ),
            "the row stays retained whatever the verdict"
        );
    }
    deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Applied)]).await;
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(
        matches!(event, ClientEvent::Closed),
        "the repeated ruling raises no event"
    );
}

/// A courier answer Refused on a retained row the session's ask answered Pending ends its candidacy and keeps the row retained (R78 decision 45).
#[tokio::test]
async fn courier_answer_refused_keeps_marked_retained_rows() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        AUTHOR,
        2,
        HeldState::Retained,
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
            (
                1,
                CourierEntryOutcome::Ruled(Verdict::Refused(
                    connetto_core::exchange::RetractCause::Conflict,
                )),
            ),
            (2, CourierEntryOutcome::Ruled(Verdict::Applied)),
        ]),
    )
    .await;
    server.close().await.expect("hang up");
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert!(candidate.seqs.is_empty(), "the rulings end the candidacy");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
    let report = wait
        .receiver
        .await
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.refused, [1]);
    assert_eq!(report.settled, [2]);
    assert_eq!(report.kept, [] as [u64; 0]);
    for seq in [1, 2] {
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, seq),
                Ok(Some(HeldState::Retained)),
            ),
            "the row stays retained whatever the answer"
        );
    }
}

/// A retention pass that takes a marked retained row drops the mark and restates the author's candidates (R78 decision 17).
#[tokio::test]
async fn pruned_marked_retained_row_restates_candidates() {
    let mut conn = replica();
    hold(
        &mut conn,
        AUTHOR,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the first"),
    );
    hold(
        &mut conn,
        OTHER,
        1,
        HeldState::Retained,
        major(2),
        Some(b"the other"),
    );
    adopt(&mut conn, AUTHOR, Some(1));
    adopt(&mut conn, OTHER, Some(1));
    let (mut conn, mut server) = attach(conn, true).await;
    let first = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &first.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let _ = expect_candidates(conn.pump_one().await.expect("restate"));
    let second = next_ask(&mut server).await;
    deliver_verdicts(&mut server, &second.request_id, &[(1, Verdict::Pending)]).await;
    let event = conn.pump_one().await.expect("the ruling settles");
    assert!(matches!(event, ClientEvent::NonFatal { .. }));
    let _ = expect_candidates(conn.pump_one().await.expect("restate"));
    conn.renew_peer_expiry(&AUTHOR, 0).expect("move the expiry");
    let candidate = expect_candidates(conn.pump_one().await.expect("restate"));
    assert!(
        candidate.seqs.is_empty(),
        "the pruned author's set restates empty"
    );
    assert!(
        matches!(peer::state_of(&mut conn.db, &AUTHOR, 1), Ok(None)),
        "the row is pruned"
    );
    assert!(
        matches!(
            peer::state_of(&mut conn.db, &OTHER, 1),
            Ok(Some(HeldState::Retained)),
        ),
        "the other author's row stays"
    );
    let candidates = conn.courier_candidates().expect("read the candidates");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].seqs, [1]);
    server.close().await.expect("hang up");
    let event = conn.pump_one().await.expect("the session ends");
    assert!(matches!(event, ClientEvent::Closed));
}

/// A session riding under a number stands certified by the number, so its
/// courier range rides the main wire at once, the scripted server settling
/// it like any sent call.
#[tokio::test]
async fn a_number_session_couriers_under_its_number() {
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
    assert_eq!(ask.seqs, [1], "the unapplied row is asked");
    let wait = conn.courier(AUTHOR).await.expect("the call rides at once");
    let range = next_range(&mut server).await;
    assert_eq!(
        range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
        [1],
        "the range rides the main wire at once"
    );
    let body = timeout(Duration::from_secs(5), next_body(&mut server))
        .await
        .expect("the body rides at once");
    assert_eq!(
        zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
        b"the only row"
    );
    deliver_answer(
        &mut server,
        &range.request_id,
        CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
    )
    .await;
    // The answer settles the last range; no candidates event rests on the
    // wire since the session never raised this author's set, so one step
    // settles it and the call's own wait is the settlement.
    conn.pump_one_or(std::future::pending::<()>())
        .await
        .expect("one step settles the answer");
    let report = timeout(Duration::from_secs(5), wait.receiver)
        .await
        .expect("the call settles at once")
        .expect("the call stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1]);
    let candidates = conn.courier_candidates().expect("the candidates stand");
    assert!(
        candidates.is_empty(),
        "the settled row leaves no candidates"
    );
    assert!(
        !conn.courier.excluded.contains_key(&AUTHOR),
        "a settled call stands no exclusion"
    );
}

#[cfg(feature = "device-identity")]
mod r78_4c {
    use connetto_core::exchange::Verdict;
    use connetto_core::messages::{
        Challenge, ControlMessage, WriterAnswer, WriterOutcome, WriterRef, WriterRequest,
    };
    use connetto_core::{LoopbackTransport, SchemaVersion, loopback};
    use tokio::time::timeout;

    use super::*;
    use crate::drain::tests::r78_4c::device_identity;
    use crate::drain::tests::{granting, handshake_ack, hold_major_one, old_version, queue, sent};
    use crate::peer;

    /// The major 3 bundle hash a build standing at major 3 presents.
    fn version3() -> SchemaVersion {
        SchemaVersion::from_hash(b"the major 3 bundle".to_vec())
    }

    /// A major 3 replica with no wire and nothing queued, so the build stands
    /// above two older majors the offer can list.
    fn replica3() -> Conn {
        let config = ClientConfig::new("courier")
            .with_schema_major(Some(major(3)))
            .with_schema_version(Some(version3()));
        let mut conn = ConnettoConnection::open(
            &Replica::in_memory(),
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &config,
            None,
        )
        .expect("open the major 3 replica");
        assert!(
            matches!(conn.notices.pop_front(), Some(ClientEvent::SyncStatus(_))),
            "the offline open states itself first"
        );
        conn
    }

    /// Attach the major 3 replica to a scripted server whose courier path is
    /// `on`, answering the handshake with the build's version and draining
    /// the state notices.
    async fn attach3(conn: Conn, on: bool) -> (Conn, LoopbackTransport) {
        let (client, server) = loopback();
        attach_scripted(
            conn,
            client,
            server,
            Some([5; 32]),
            handshake_ack("courier", Some(version3()), None, on),
            |event| matches!(event, ClientEvent::SyncStatus(_)),
        )
        .await
    }

    /// A `courier` call made before the session's device writer is bound
    /// holds in the queue and goes out on the bind's answer, so the range
    /// never rides a wire that would refuse it `Uncertified`.
    #[tokio::test]
    async fn a_call_before_the_bind_holds_until_the_bind_lands() {
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(2),
            Some(b"the only row"),
        );
        adopt(&mut conn, AUTHOR, Some(1));
        conn.writer_keys = Some(keys);
        let (mut conn, mut server) = attach_device(conn, true).await;
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::BindDevice(bind)) = frame else {
            panic!("the wire takes the device bind, not {frame:?}");
        };
        let ask = next_ask(&mut server).await;
        deliver_verdicts(&mut server, &ask.request_id, &[(1, Verdict::Pending)]).await;
        let event = conn.pump_one().await.expect("the ruling settles");
        assert!(matches!(event, ClientEvent::NonFatal { .. }));
        let _ = expect_candidates(conn.pump_one().await.expect("restate"));
        let wait = conn.courier(AUTHOR).await.expect("the call holds");
        // The call holds for the bind's answer, so no range rides the wire.
        assert!(
            conn.courier.open.is_none(),
            "the range does not ride before the bind"
        );
        server
            .send_control(ControlMessage::WriterAnswer(WriterAnswer {
                request_id: bind.request_id,
                outcome: WriterOutcome::Bound { frontier: 0 },
            }))
            .await
            .expect("the bind lands");
        let event = conn.pump_one().await.expect("the bind settles");
        assert!(matches!(event, ClientEvent::NonFatal { .. }));
        let range = next_range(&mut server).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [1],
            "the held call's range rides after the bind"
        );
        let body = next_body(&mut server).await;
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b"the only row"
        );
        deliver_answer(
            &mut server,
            &range.request_id,
            CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        let candidate = expect_candidates(conn.pump_one().await.expect("the range settles"));
        assert!(
            candidate.seqs.is_empty(),
            "the settled row leaves the candidates"
        );
        let report = wait
            .receiver
            .await
            .expect("the call stands")
            .expect("the range settles");
        assert_eq!(report.settled, [1]);
        assert!(
            !conn.courier.excluded.contains_key(&AUTHOR),
            "a held call's refusal stands no exclusion"
        );
    }

    /// A `courier` call made while the session's device bind defers to the
    /// keys arriving holds in the queue, the keys arriving planning and
    /// landing the bind, and the call's range goes out once.
    #[tokio::test]
    async fn a_call_held_while_the_bind_defers_to_the_keys_rides_when_they_arrive() {
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(2),
            Some(b"the only row"),
        );
        adopt(&mut conn, AUTHOR, Some(1));
        // The keys are not handed over at the handshake, so the bind defers
        // to their arrival.
        let (mut conn, mut server) = attach_device(conn, true).await;
        let ask = next_ask(&mut server).await;
        assert_eq!(ask.seqs, [1], "the unapplied row is asked");
        let wait = conn.courier(AUTHOR).await.expect("the call holds");
        // The call holds for the deferred bind, so no frame rides the wire.
        assert!(
            conn.courier.open.is_none(),
            "the range does not ride before the bind"
        );
        assert!(
            timeout(Duration::ZERO, server.recv()).await.is_err(),
            "no frame rides before the keys arrive"
        );
        // The keys arrive: the bind plans, rides the main wire, and lands.
        let (steering, _steering_rx) = tokio::sync::mpsc::unbounded_channel();
        conn.with_device_writer(keys, steering)
            .await
            .expect("the keys arrive");
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::BindDevice(bind)) = frame else {
            panic!("the wire takes the device bind, not {frame:?}");
        };
        server
            .send_control(ControlMessage::WriterAnswer(WriterAnswer {
                request_id: bind.request_id,
                outcome: WriterOutcome::Bound { frontier: 0 },
            }))
            .await
            .expect("the bind lands");
        let event = conn.pump_one().await.expect("the bind settles");
        assert!(matches!(event, ClientEvent::NonFatal { .. }));
        // The held call's range rides once.
        let range = next_range(&mut server).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [1],
            "the held call's range rides after the bind"
        );
        let body = timeout(Duration::from_secs(5), next_body(&mut server))
            .await
            .expect("the body rides at once");
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b"the only row"
        );
        deliver_answer(
            &mut server,
            &range.request_id,
            CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        // The answer settles the last range; no candidates event rests on
        // the wire since the session never raised this author's set, so one
        // step settles it and the call's own wait is the settlement.
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step settles the answer");
        let report = timeout(Duration::from_secs(5), wait.receiver)
            .await
            .expect("the call settles at once")
            .expect("the call stands")
            .expect("the range settles");
        assert_eq!(report.settled, [1]);
        let candidates = conn.courier_candidates().expect("the candidates stand");
        assert!(
            candidates.is_empty(),
            "the settled row leaves no candidates"
        );
        assert!(
            timeout(Duration::ZERO, server.recv()).await.is_err(),
            "the range rode once"
        );
        assert!(
            !conn.courier.excluded.contains_key(&AUTHOR),
            "a held call's refusal stands no exclusion"
        );
    }

    /// A bind that is refused leaves the certificate retired and no bind
    /// owed or planned while the fresh key is out, so the next call fails
    /// at once with `Uncertified`, no frame riding and the author not
    /// excluded, a later call still possible.
    #[tokio::test]
    async fn a_refused_bind_fails_the_next_call_uncertified_without_excluding() {
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(2),
            Some(b"the only row"),
        );
        adopt(&mut conn, AUTHOR, Some(1));
        let (mut conn, mut server) = attach_device(conn, true).await;
        let ask = next_ask(&mut server).await;
        assert_eq!(ask.seqs, [1], "the unapplied row is asked");
        let (steering, _steering_rx) = tokio::sync::mpsc::unbounded_channel();
        conn.with_device_writer(keys, steering)
            .await
            .expect("the keys arrive");
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::BindDevice(bind)) = frame else {
            panic!("the wire takes the device bind, not {frame:?}");
        };
        // The bind is refused: the device retires and a fresh key enrolls.
        server
            .send_control(ControlMessage::WriterAnswer(WriterAnswer {
                request_id: bind.request_id,
                outcome: WriterOutcome::NotEnrolled,
            }))
            .await
            .expect("the bind is refused");
        let event = conn.pump_one().await.expect("the refusal settles");
        assert!(matches!(event, ClientEvent::NonFatal { .. }));
        let err = conn.courier(AUTHOR).await.expect_err("no bind can come");
        assert!(
            matches!(err, CourierError::Refused(CourierRefusal::Uncertified)),
            "the call stands uncertified at once"
        );
        assert!(
            timeout(Duration::ZERO, server.recv()).await.is_err(),
            "no frame rides the wire"
        );
        assert!(
            !conn.courier.excluded.contains_key(&AUTHOR),
            "the author is not excluded"
        );
        let candidates = conn.courier_candidates().expect("the candidates stand");
        assert_eq!(candidates.len(), 1, "the author stands a candidate");
        assert_eq!(
            candidates[0].seqs,
            [1],
            "the author's row stands a candidate"
        );
        let err = conn
            .courier(AUTHOR)
            .await
            .expect_err("a later call is possible");
        assert!(
            matches!(err, CourierError::Refused(CourierRefusal::Uncertified)),
            "the later call stands the same refusal"
        );
    }

    /// A dial range made while the keys are pending holds for the keys:
    /// they arrive, the main bind lands, and exactly one dial rides, its
    /// own `BindDevice` before the range, the author never excluded.
    #[tokio::test]
    async fn a_dial_range_holds_while_the_keys_are_pending() {
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(1),
            Some(b"the only row"),
        );
        store_offer(
            &mut conn,
            AUTHOR,
            &offer_with(&[(major(1), old_version())], Some(1)),
        );
        // The keys are not handed over at the handshake, so the dial's
        // bind defers to their arrival.
        let (mut conn, mut server) = attach_device(conn, true).await;
        let ask = next_ask(&mut server).await;
        assert_eq!(ask.seqs, [1], "the unapplied row is asked");
        let wait = conn.courier(AUTHOR).await.expect("the call holds");
        // The call holds for the keys, so no range and no dial ride yet.
        assert!(
            conn.courier.open.is_none(),
            "the range does not ride before the keys arrive"
        );
        assert!(
            timeout(Duration::ZERO, server.recv()).await.is_err(),
            "no frame rides before the keys arrive"
        );
        // The keys arrive: the main wire's bind rides and lands, and the
        // held call dials.
        let (steering, _steering_rx) = tokio::sync::mpsc::unbounded_channel();
        conn.with_device_writer(keys, steering)
            .await
            .expect("the keys arrive");
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::BindDevice(bind)) = frame else {
            panic!("the main wire takes the device bind, not {frame:?}");
        };
        server
            .send_control(ControlMessage::WriterAnswer(WriterAnswer {
                request_id: bind.request_id,
                outcome: WriterOutcome::Bound { frontier: 0 },
            }))
            .await
            .expect("the bind lands");
        let event = conn.pump_one().await.expect("the bind settles");
        assert!(matches!(event, ClientEvent::NonFatal { .. }));
        let timer = conn.courier_drain_timer().expect("major 1 owes a dial");
        let mut old = dial_courier_wire(&mut conn, timer, major(1), old_version()).await;
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step");
        let range = next_range(&mut old).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [1],
            "the held call's range rides the dial after the keys"
        );
        let body = next_body(&mut old).await;
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b"the only row"
        );
        deliver_answer(
            &mut old,
            &range.request_id,
            CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        // The answer settles the last range; no candidates event rests on
        // the wire since the session never raised this author's set, so one
        // step settles it and the call's own wait is the settlement.
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step settles the answer");
        let report = timeout(Duration::from_secs(5), wait.receiver)
            .await
            .expect("the call settles within the bound")
            .expect("the call stands")
            .expect("the range settles");
        assert_eq!(report.settled, [1]);
        let candidates = conn.courier_candidates().expect("the candidates stand");
        assert!(
            candidates.is_empty(),
            "the settled row leaves no candidates"
        );
        assert!(
            matches!(
                timeout(Duration::from_secs(5), old.recv())
                    .await
                    .expect("within the bound"),
                Ok(None)
            ),
            "the courier wire closes behind its last answer"
        );
        assert!(
            timeout(Duration::ZERO, server.recv()).await.is_err(),
            "the main wire owes nothing more"
        );
        assert!(
            !conn.courier.excluded.contains_key(&AUTHOR),
            "the author is never excluded"
        );
    }

    /// Dial the courier wire for `presented` against a scripted server: the
    /// challenge, the handshake answered with the courier path on and the
    /// major's version, the device writer bound. Returns the far end.
    async fn dial_courier_wire(
        conn: &mut Conn,
        timer: crate::DrainTimer,
        presented: Major,
        version: SchemaVersion,
    ) -> LoopbackTransport {
        let (courier, mut old) = loopback();
        let reply = async {
            let frame = next(&mut old).await;
            let IncomingFrame::Control(ControlMessage::Handshake(handshake)) = frame else {
                panic!("the courier wire opens with the handshake");
            };
            assert_eq!(
                handshake.schema_major,
                Some(presented),
                "the courier wire presents the range's major"
            );
            assert!(
                matches!(handshake.writer, WriterRequest::Device),
                "the device writer is presented"
            );
            old.send_control(ControlMessage::Challenge(Challenge { nonce: [7; 32] }))
                .await
                .expect("the challenge");
            old.send_control(handshake_ack("old", Some(version), None, true))
                .await
                .expect("the handshake is answered");
            let frame = next(&mut old).await;
            let IncomingFrame::Control(ControlMessage::BindDevice(bind)) = frame else {
                panic!("the device writer binds, not {frame:?}");
            };
            let request_id = bind.request_id;
            old.send_control(ControlMessage::WriterAnswer(WriterAnswer {
                request_id,
                outcome: WriterOutcome::Bound { frontier: 0 },
            }))
            .await
            .expect("the writer binds");
        };
        let (attached, ()) = tokio::join!(conn.attach_courier_drain(timer, courier), reply);
        attached.expect("the courier wire attaches");
        old
    }

    /// R78 decision 47: a range spanning majors couriers the older major on
    /// its own dial first and continues on the main wire, the call settling
    /// every row and the courier wire closing behind its last answer.
    #[expect(
        clippy::too_many_lines,
        reason = "one test walks the whole two-wire call"
    )]
    #[tokio::test]
    async fn a_spanning_range_couriers_the_older_major_on_its_own_wire_first() {
        let b1 = real_changeset(1);
        let b2 = real_changeset(2);
        let b3 = real_changeset(3);
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(1),
            Some(&b1),
        );
        hold(
            &mut conn,
            AUTHOR,
            2,
            HeldState::CourierOnly,
            major(1),
            Some(&b2),
        );
        hold(
            &mut conn,
            AUTHOR,
            3,
            HeldState::Provisional,
            major(2),
            Some(&b3),
        );
        store_offer(
            &mut conn,
            AUTHOR,
            &offer_with(&[(major(1), old_version()), (major(2), version())], Some(3)),
        );
        let (mut conn, mut server) = attach_device(conn, true).await;
        let ask = next_ask(&mut server).await;
        assert_eq!(ask.seqs, [1, 2, 3], "every unretained row is asked");
        // The keys arrive after the handshake: the dial certifies on its own
        // wire and the call rides it at once while the main wire's bind is
        // still owed.
        let (steering, _steering_rx) = tokio::sync::mpsc::unbounded_channel();
        conn.with_device_writer(keys, steering)
            .await
            .expect("the keys arrive");
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::BindDevice(bind)) = frame else {
            panic!("the main wire takes the device bind, not {frame:?}");
        };
        let wait = conn
            .courier(AUTHOR)
            .await
            .expect("the call rides the dial at once");
        assert!(
            conn.courier.open.is_some(),
            "the dial range rides before the main bind"
        );
        let timer = conn.courier_drain_timer().expect("major 1 owes a dial");
        let mut old = dial_courier_wire(&mut conn, timer, major(1), old_version()).await;
        // The dial's bound writer frees the range for the courier wire.
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step");
        let range = next_range(&mut old).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [1, 2],
            "the older major's rows ride the courier wire"
        );
        let body = next_body(&mut old).await;
        assert_eq!(body.seq, 1);
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b1
        );
        let body = next_body(&mut old).await;
        assert_eq!(body.seq, 2);
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b2
        );
        deliver_answer(
            &mut old,
            &range.request_id,
            CourierOutcome::Entries(vec![
                (1, CourierEntryOutcome::Ruled(Verdict::Applied)),
                (2, CourierEntryOutcome::Ruled(Verdict::Applied)),
            ]),
        )
        .await;
        let event = conn.pump_one().await.expect("the older major settles");
        assert_eq!(
            expect_candidates(event).seqs,
            [3],
            "the remaining range keeps the author's candidates"
        );
        // The advance to the main wire held for its bind, the settled rows
        // standing ruled; the bind lands and the advance rides at last.
        server
            .send_control(ControlMessage::WriterAnswer(WriterAnswer {
                request_id: bind.request_id,
                outcome: WriterOutcome::Bound { frontier: 0 },
            }))
            .await
            .expect("the bind lands");
        let event = conn.pump_one().await.expect("the bind settles");
        assert!(matches!(event, ClientEvent::NonFatal { .. }));
        let range = next_range(&mut server).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [3],
            "the current major's rows ride the main wire"
        );
        let body = next_body(&mut server).await;
        assert_eq!(body.seq, 3);
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b3
        );
        deliver_answer(
            &mut server,
            &range.request_id,
            CourierOutcome::Entries(vec![(3, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        let event = conn.pump_one().await.expect("the call ends");
        assert_eq!(
            expect_candidates(event).seqs,
            [] as [u64; 0],
            "the author's candidates restated empty"
        );
        let report = timeout(BOUND, wait.receiver)
            .await
            .expect("the call stands")
            .expect("the answer lands")
            .expect("every row settles");
        assert_eq!(report.settled, [1, 2, 3]);
        assert_eq!(report.refused, [] as [u64; 0]);
        assert_eq!(report.kept, [] as [u64; 0]);
        assert!(
            matches!(
                timeout(BOUND, old.recv()).await.expect("within the bound"),
                Ok(None)
            ),
            "the courier wire closes behind its last answer"
        );
    }

    /// R78 decision 47: rows that span two older majors the offer builds ride
    /// each major's own courier dial in major order, the build's major
    /// following on the main wire, each courier wire closing behind its last
    /// answer.
    #[expect(
        clippy::too_many_lines,
        reason = "one test walks the two dials and the main wire"
    )]
    #[tokio::test]
    async fn two_older_majors_each_dial_their_own_wire_before_the_main() {
        let b1 = real_changeset(1);
        let b2 = real_changeset(2);
        let b3 = real_changeset(3);
        let (cert, keys) = device_identity().await;
        let mut conn = replica3();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(1),
            Some(&b1),
        );
        hold(
            &mut conn,
            AUTHOR,
            2,
            HeldState::CourierOnly,
            major(2),
            Some(&b2),
        );
        hold(
            &mut conn,
            AUTHOR,
            3,
            HeldState::Provisional,
            major(3),
            Some(&b3),
        );
        store_offer(
            &mut conn,
            AUTHOR,
            &offer_with(
                &[
                    (major(1), old_version()),
                    (major(2), version()),
                    (major(3), version3()),
                ],
                Some(3),
            ),
        );
        let (mut conn, mut server) = attach3(conn, true).await;
        let ask = next_ask(&mut server).await;
        assert_eq!(ask.seqs, [1, 2, 3], "every unretained row is asked");
        // The keys arrive after the handshake: each dial certifies on its
        // own wire and the call rides the first at once while the main
        // wire's bind is still owed.
        let (steering, _steering_rx) = tokio::sync::mpsc::unbounded_channel();
        conn.with_device_writer(keys, steering)
            .await
            .expect("the keys arrive");
        let frame = next(&mut server).await;
        let IncomingFrame::Control(ControlMessage::BindDevice(bind)) = frame else {
            panic!("the main wire takes the device bind, not {frame:?}");
        };
        let wait = conn
            .courier(AUTHOR)
            .await
            .expect("the call rides the first dial at once");
        assert!(
            conn.courier.open.is_some(),
            "the first dial range rides before the main bind"
        );
        let timer = conn.courier_drain_timer().expect("major 1 owes a dial");
        let mut old = dial_courier_wire(&mut conn, timer, major(1), old_version()).await;
        // The dial's bound writer frees the range for the courier wire.
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step");
        let range = next_range(&mut old).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [1],
            "the oldest major's row rides its own courier wire"
        );
        let body = next_body(&mut old).await;
        assert_eq!(body.seq, 1);
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b1
        );
        deliver_answer(
            &mut old,
            &range.request_id,
            CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        let event = conn.pump_one().await.expect("the oldest major settles");
        assert_eq!(
            expect_candidates(event).seqs,
            [2, 3],
            "the settled major leaves the rest"
        );
        // The second older major dials its own wire.
        let timer = conn.courier_drain_timer().expect("major 2 owes a dial");
        let mut mid = dial_courier_wire(&mut conn, timer, major(2), version()).await;
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step");
        let range = next_range(&mut mid).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [2],
            "the middle major's row rides its own courier wire"
        );
        let body = next_body(&mut mid).await;
        assert_eq!(body.seq, 2);
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b2
        );
        deliver_answer(
            &mut mid,
            &range.request_id,
            CourierOutcome::Entries(vec![(2, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        let event = conn.pump_one().await.expect("the middle major settles");
        assert_eq!(
            expect_candidates(event).seqs,
            [3],
            "the settled major leaves the build's row"
        );
        // The advance to the main wire held for its bind; the bind lands
        // and the build's major follows at last.
        server
            .send_control(ControlMessage::WriterAnswer(WriterAnswer {
                request_id: bind.request_id,
                outcome: WriterOutcome::Bound { frontier: 0 },
            }))
            .await
            .expect("the bind lands");
        let event = conn.pump_one().await.expect("the bind settles");
        assert!(matches!(event, ClientEvent::NonFatal { .. }));
        let range = next_range(&mut server).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [3],
            "the build's major rides the main wire"
        );
        let body = next_body(&mut server).await;
        assert_eq!(body.seq, 3);
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b3
        );
        deliver_answer(
            &mut server,
            &range.request_id,
            CourierOutcome::Entries(vec![(3, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        let event = conn.pump_one().await.expect("the call ends");
        assert_eq!(
            expect_candidates(event).seqs,
            [] as [u64; 0],
            "the author's candidates restate empty"
        );
        let report = timeout(BOUND, wait.receiver)
            .await
            .expect("the call stands")
            .expect("the answer lands")
            .expect("every row settles");
        assert_eq!(report.settled, [1, 2, 3]);
        assert_eq!(report.refused, [] as [u64; 0]);
        assert_eq!(report.kept, [] as [u64; 0]);
        assert!(
            matches!(
                timeout(BOUND, old.recv()).await.expect("within the bound"),
                Ok(None)
            ),
            "the first courier wire closes behind its last answer"
        );
        assert!(
            matches!(
                timeout(BOUND, mid.recv()).await.expect("within the bound"),
                Ok(None)
            ),
            "the second courier wire closes behind its last answer"
        );
    }

    /// R78 decision 47: a held row whose major the stored offer does not
    /// build is not a courier candidate and owes no dial, and the settle
    /// restates nothing, the session never having raised the author's
    /// candidates.
    #[tokio::test]
    async fn a_major_the_offer_does_not_build_is_not_couriered() {
        let b1 = real_changeset(1);
        let b2 = real_changeset(2);
        let mut conn = replica();
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(1),
            Some(&b1),
        );
        hold(
            &mut conn,
            AUTHOR,
            2,
            HeldState::Provisional,
            major(2),
            Some(&b2),
        );
        adopt(&mut conn, AUTHOR, Some(2));
        let (mut conn, mut server) = attach(conn, true).await;
        let ask = next_ask(&mut server).await;
        assert_eq!(ask.seqs, [1, 2], "every unretained row is asked");
        let candidates = conn.courier_candidates().expect("the candidates read");
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].seqs,
            [2],
            "only the listed major's row is a candidate"
        );
        let wait = conn.courier(AUTHOR).await.expect("the range goes out");
        assert!(conn.courier_drain_timer().is_none(), "no dial is owed");
        let range = next_range(&mut server).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [2]
        );
        let body = next_body(&mut server).await;
        assert_eq!(body.seq, 2);
        assert_eq!(
            zstd::decode_all(&body.changeset_zstd[..]).expect("a body"),
            b2
        );
        deliver_answer(
            &mut server,
            &range.request_id,
            CourierOutcome::Entries(vec![(2, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        server.close().await.expect("hang up");
        let event = conn.pump_one().await.expect("the range settles");
        assert!(matches!(event, ClientEvent::Closed));
        let report = timeout(BOUND, wait.receiver)
            .await
            .expect("the call stands")
            .expect("the answer lands")
            .expect("the row settles");
        assert_eq!(report.settled, [2]);
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, 1),
                Ok(Some(HeldState::CourierOnly)),
            ),
            "the unlisted major's row stays held"
        );
    }

    /// R78 decision 47: a run of held rows longer than the manifest's bound
    /// rides its older major at the cut, the run's tail staying held behind
    /// it.
    #[tokio::test]
    async fn a_run_longer_than_the_manifest_cut_rides_at_the_cut() {
        let cut = u64::try_from(connetto_core::exchange::MAX_MANIFEST_ROWS)
            .expect("the bound fits a seq");
        let rows = cut + 1;
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        for seq in 1..=rows {
            hold(
                &mut conn,
                AUTHOR,
                seq,
                HeldState::CourierOnly,
                major(1),
                Some(b"x"),
            );
        }
        store_offer(
            &mut conn,
            AUTHOR,
            &offer_with(&[(major(1), old_version())], Some(rows)),
        );
        let (mut conn, _server) = attach_device(conn, true).await;
        conn.writer_keys = Some(keys);
        let wait = conn.courier(AUTHOR).await.expect("the call owes a dial");
        let timer = conn.courier_drain_timer().expect("major 1 owes a dial");
        let mut old = dial_courier_wire(&mut conn, timer, major(1), old_version()).await;
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step");
        let range = next_range(&mut old).await;
        assert_eq!(
            range.rows.len(),
            connetto_core::exchange::MAX_MANIFEST_ROWS,
            "the run rides at the manifest cut"
        );
        assert_eq!(
            range.rows.last().map(|row| row.seq),
            Some(cut),
            "the cut falls on the last ranged row"
        );
        let tail = i64::try_from(rows).expect("the tail fits");
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, tail),
                Ok(Some(HeldState::CourierOnly))
            ),
            "the tail row stays held behind the cut"
        );
        deliver_answer(
            &mut old,
            &range.request_id,
            CourierOutcome::Entries(vec![(1, CourierEntryOutcome::NotCouriered)]),
        )
        .await;
        let _ = conn
            .pump_one_or(tokio::time::sleep(BOUND))
            .await
            .expect("the range settles");
        let report = timeout(BOUND, wait.receiver)
            .await
            .expect("the call stands")
            .expect("the answer lands")
            .expect("the call ends");
        assert_eq!(
            report.kept.len(),
            connetto_core::exchange::MAX_MANIFEST_ROWS,
            "the ranged rows stay held"
        );
        assert_eq!(report.settled, [] as [u64; 0]);
        assert_eq!(report.refused, [] as [u64; 0]);
    }

    /// R78 decision 47: a settle whose next range would ride the main wire
    /// that is gone fails the call on the advance, the next range's row
    /// staying held.
    #[tokio::test]
    async fn an_advance_to_the_main_wire_that_is_gone_fails_the_call() {
        let b1 = real_changeset(1);
        let b2 = real_changeset(2);
        let (cert, keys) = device_identity().await;
        let mut conn = replica();
        conn.store_device_certificate(&cert)
            .await
            .expect("the certificate stores");
        hold(
            &mut conn,
            AUTHOR,
            1,
            HeldState::CourierOnly,
            major(1),
            Some(&b1),
        );
        hold(
            &mut conn,
            AUTHOR,
            2,
            HeldState::Provisional,
            major(2),
            Some(&b2),
        );
        store_offer(
            &mut conn,
            AUTHOR,
            &offer_with(&[(major(1), old_version()), (major(2), version())], Some(2)),
        );
        let (mut conn, mut server) = attach_device(conn, true).await;
        conn.writer_keys = Some(keys);
        let ask = next_ask(&mut server).await;
        assert_eq!(ask.seqs, [1, 2], "every unretained row is asked");
        let wait = conn.courier(AUTHOR).await.expect("the call owes a dial");
        let timer = conn.courier_drain_timer().expect("major 1 owes a dial");
        let mut old = dial_courier_wire(&mut conn, timer, major(1), old_version()).await;
        conn.pump_one_or(std::future::pending::<()>())
            .await
            .expect("one step");
        let range = next_range(&mut old).await;
        assert_eq!(
            range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            [1],
            "the older major's row rides the courier wire"
        );
        // The main wire goes before the advance, so the advance's send fails.
        conn.wire = None;
        deliver_answer(
            &mut old,
            &range.request_id,
            CourierOutcome::Entries(vec![(1, CourierEntryOutcome::Ruled(Verdict::Applied))]),
        )
        .await;
        let _ = conn
            .pump_one_or(tokio::time::sleep(BOUND))
            .await
            .expect("the older major settles");
        let err = timeout(BOUND, wait.receiver)
            .await
            .expect("the call stands")
            .expect("the answer lands")
            .expect_err("the advance fails");
        assert!(
            matches!(err, CourierError::Refused(CourierRefusal::Uncertified)),
            "the advance to the gone main wire stands uncertified"
        );
        assert!(
            matches!(
                peer::state_of(&mut conn.db, &AUTHOR, 2),
                Ok(Some(HeldState::Provisional))
            ),
            "the main row stays held"
        );
    }

    /// R78 decision 47: a range sent on a wire that is gone answers
    /// `NotConnected`, and a courier dial that owns the range needs no wire.
    #[tokio::test]
    async fn a_send_on_a_gone_wire_answers_not_connected() {
        let mut conn = replica();
        let range = crate::PlannedRange {
            major: major(2),
            version: Some(version()),
            rows: vec![ManifestRow {
                seq: 1,
                major: major(2),
                origin: None,
                digest: [0; 32],
            }],
            bodies: vec![],
        };
        let err = conn
            .send_on_wire(crate::CourierOpenWire::Main, "r", AUTHOR, &[], &range)
            .await
            .expect_err("no main wire");
        assert!(
            matches!(err, crate::ClientError::NotConnected),
            "the main wire is gone"
        );
        let err = conn
            .send_on_wire(crate::CourierOpenWire::Drain, "r", AUTHOR, &[], &range)
            .await
            .expect_err("no drain wire");
        assert!(
            matches!(err, crate::ClientError::NotConnected),
            "the drain wire is gone"
        );
        conn.send_on_wire(crate::CourierOpenWire::Courier, "r", AUTHOR, &[], &range)
            .await
            .expect("the dial owns the range");
    }

    /// R78 decision 47: a build that stands no schema major couriers nothing
    /// held, the plan it cannot stand naming the author's rows to no one.
    #[tokio::test]
    async fn a_build_with_no_schema_major_couriers_nothing_held() {
        let config = ClientConfig::new("courier").with_schema_version(Some(version()));
        let conn = ConnettoConnection::open(
            &Replica::in_memory(),
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &config,
            None,
        )
        .expect("open the replica");
        assert!(
            conn.config.schema_major.is_none(),
            "the build stands no major"
        );
        let (mut conn, _server) = attach(conn, true).await;
        assert!(
            matches!(conn.courier(AUTHOR).await, Err(CourierError::NothingHeld)),
            "the plan it cannot stand names nothing"
        );
    }

    /// A courier completes beside an unfinished batch drain, with or without the main wire.
    #[tokio::test]
    async fn a_courier_completes_beside_a_batch_drain_with_or_without_the_main_wire() {
        for keep_main in [true, false] {
            timeout(BOUND, async {
                let (cert, keys) = device_identity().await;
                let mut conn = replica3();
                conn.store_device_certificate(&cert)
                    .await
                    .expect("store the certificate");
                hold(
                    &mut conn,
                    AUTHOR,
                    2,
                    HeldState::CourierOnly,
                    major(2),
                    Some(&real_changeset(2)),
                );
                store_offer(
                    &mut conn,
                    AUTHOR,
                    &offer_with(&[(major(2), version())], Some(2)),
                );
                let (mut conn, _main) = attach3(conn, true).await;
                conn.writer_keys = Some(keys);
                queue(&mut conn, WriterRef::Device([1; 32]), 3, major(1));
                queue(&mut conn, WriterRef::Device([1; 32]), 4, major(1));
                hold_major_one(&mut conn);
                let (wire, mut drain) = loopback();
                let timer = conn.drain_timer().expect("the batch owes a dial");
                let handshake = async {
                    assert!(matches!(sent(&mut drain).await, Some(ControlMessage::Handshake(_))));
                    drain.send_control(granting(7)).await.expect("grant the drain writer");
                };
                let (attached, ()) = tokio::join!(conn.attach_drain(timer, wire), handshake);
                attached.expect("attach the batch drain");
                let wait = conn.courier(AUTHOR).await.expect("courier the held row");
                let timer = conn.courier_drain_timer().expect("the range owes its own dial");
                let mut courier = dial_courier_wire(&mut conn, timer, major(2), version()).await;
                if !keep_main {
                    conn.wire = None;
                }
                let finish = async {
                    let mut receiver = wait.receiver;
                    loop {
                        tokio::select! {
                            biased;
                            report = &mut receiver => break report.expect("the call stands").expect("the row settles"),
                            event = conn.pump_one() => { event.expect("the pump stays connected"); }
                        }
                    }
                };
                let answer = async {
                    let range = next_range(&mut courier).await;
                    assert_eq!(range.rows.iter().map(|row| row.seq).collect::<Vec<_>>(), [2]);
                    let received = next_body(&mut courier).await;
                    assert_eq!(received.seq, 2);
                    deliver_answer(
                        &mut courier,
                        &range.request_id,
                        CourierOutcome::Entries(vec![(2, CourierEntryOutcome::Ruled(Verdict::Applied))]),
                    )
                    .await;
                };
                let (report, ()) = tokio::join!(finish, answer);
                assert_eq!(report.settled, [2]);
                assert!(report.refused.is_empty() && report.kept.is_empty());
            })
            .await
            .expect("the courier completes within the bound");
        }
    }
}
