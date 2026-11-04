//! The author's next sessions hear what was couriered (R78 decision 12).
//!
//! A scripted server sends a `CourierNotice` frame after its handshake
//! answer, and the connection answers `CourierNoticeAck` with the frame's id
//! at once, raising one `WritesCouriered` per frame. The connection holds no
//! memory of a frame, so a later connection delivering the same rows raises
//! them again, as the server re-sends them after an unacked drop.

use std::time::Duration;

use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection};
use connetto_core::exchange::{RetractCause, Verdict};
use connetto_core::messages::{
    ControlMessage, CourierNotice, CourierNoticeAck, CourierNoticeRow, WriterRef,
};
use connetto_core::traits::IncomingFrame;

use super::support::{self, Script};

const DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                   status TEXT NOT NULL);";

/// The two rows the scenarios settle, one applied and one refused.
fn rows() -> Vec<CourierNoticeRow> {
    vec![
        CourierNoticeRow {
            courier: "bruno".to_owned(),
            writer: WriterRef::Device([7; 32]),
            seq: 41,
            verdict: Verdict::Applied,
        },
        CourierNoticeRow {
            courier: "bruno".to_owned(),
            writer: WriterRef::Number(2),
            seq: 42,
            verdict: Verdict::Refused(RetractCause::Conflict),
        },
    ]
}

/// One notice frame with the scenario's rows.
fn notice(id: &str) -> IncomingFrame {
    IncomingFrame::Control(ControlMessage::CourierNotice(CourierNotice {
        notice_id: id.to_owned(),
        notices: rows(),
    }))
}

/// A connected replica over the policy-free schema, its scripted server
/// answering the handshake and then serving the queued frames.
async fn connected(server: &Script) -> ConnettoConnection<Script> {
    ClientBuilder::new(support::bundle(DDL), support::Once::new(server.clone()))
        .signed_in(support::held("r78"))
        .connect_driven()
        .await
        .expect("connect")
}

/// The rows the `WritesCouriered` events in `events` carry.
fn written(events: &[ClientEvent]) -> Vec<&[CourierNoticeRow]> {
    events
        .iter()
        .filter_map(|event| match event {
            ClientEvent::WritesCouriered { notices } => Some(notices.as_slice()),
            _ => None,
        })
        .collect()
}

/// The ids the receipts the scripted server recorded quote, in order.
fn acked(server: &Script) -> Vec<String> {
    server.sent(|message| match message {
        ControlMessage::CourierNoticeAck(CourierNoticeAck { notice_id }) => Some(notice_id.clone()),
        _ => None,
    })
}

/// Draw events until `count` `WritesCouriered` events stand, then one quiet
/// window more, so a frame the connection raised twice cannot hide behind
/// the count.
async fn drawn(conn: &mut ConnettoConnection<Script>, count: usize) -> Vec<ClientEvent> {
    let mut seen = Vec::new();
    loop {
        match conn
            .pump_one_or(tokio::time::sleep(Duration::from_millis(200)))
            .await
            .expect("the pump stands")
        {
            Some(event) => {
                if matches!(event, ClientEvent::Closed) {
                    panic!("the wire ended at {event:?}");
                }
                seen.push(event);
                if written(&seen).len() == count {
                    break;
                }
            }
            None => panic!("the wire went quiet before the count stood"),
        }
    }
    while let Some(event) = conn
        .pump_one_or(tokio::time::sleep(Duration::from_millis(200)))
        .await
        .expect("the pump stands")
    {
        seen.push(event);
    }
    seen
}

/// The frame the scripted server answers raises one `WritesCouriered` with
/// the frame's rows, and the connection's at-once receipt quotes the frame's
/// id.
#[tokio::test]
async fn a_notice_raises_writes_couriered_and_acks_at_once() {
    let script = Script::default();
    script.say(support::handshake_ack());
    script.say(notice("n1"));
    let mut conn = connected(&script).await;
    let seen = drawn(&mut conn, 1).await;
    let couriered = written(&seen);
    assert_eq!(
        couriered.len(),
        1,
        "one frame raises one event, the pump saw {seen:?}"
    );
    assert_eq!(
        couriered[0],
        rows().as_slice(),
        "the event carries the frame's rows"
    );
    assert_eq!(
        acked(&script),
        vec!["n1".to_owned()],
        "the receipt quotes the frame's id, and only it"
    );
}

/// The same rows on a second connection, after the server dropped the first
/// before deleting what it owed, raise again, since the connection holds no
/// memory of the first frame.
#[tokio::test]
async fn a_second_connection_hears_the_same_notice_again() {
    let first = Script::default();
    first.say(support::handshake_ack());
    first.say(notice("n1"));
    let mut conn = connected(&first).await;
    let seen = drawn(&mut conn, 1).await;
    assert_eq!(
        written(&seen).len(),
        1,
        "the first connection raised the frame"
    );
    assert_eq!(
        acked(&first),
        vec!["n1".to_owned()],
        "the first receipt quotes the frame's id"
    );
    first.hang_up();
    drop(conn);

    let second = Script::default();
    second.say(support::handshake_ack());
    second.say(notice("n2"));
    let mut conn = connected(&second).await;
    let seen = drawn(&mut conn, 1).await;
    let couriered = written(&seen);
    assert_eq!(
        couriered.len(),
        1,
        "the second connection raises the same rows again"
    );
    assert_eq!(
        couriered[0],
        rows().as_slice(),
        "the second event carries the same rows"
    );
    assert_eq!(
        acked(&second),
        vec!["n2".to_owned()],
        "the second receipt quotes its own frame's id"
    );
}
