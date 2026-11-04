//! What the tabs of a relay hub learn from a courier notice (R78 decision
//! 41): the worker's connection acks the server's frame at once, the hub
//! relays the frame's rows to the tab under an id of its own, and the tab's
//! receipt of the relayed frame is absorbed by the hub rather than going
//! upstream. Runs in a dedicated worker for the wasm SQLite build.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use std::sync::Arc;

use connetto_core::exchange::{RetractCause, Verdict};
use connetto_core::messages::{
    ControlMessage, CourierNotice, CourierNoticeAck, CourierNoticeRow, Ping, Pong, WriterRef,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_web::MessageTransport;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::MessagePort;

#[path = "hub_mismatch/support.rs"]
mod support;

use support::{Plan, Script, Waits, handshake, hub_with_tab_live, next_frame};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The two entries the scenario's courier settled, one applied and one
/// refused.
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

/// The notice frame the hub relays, skipping whatever the tab draws first.
async fn next_notice(tab: &mut MessageTransport<MessagePort>) -> CourierNotice {
    loop {
        let frame = next_frame(tab, "relayed the notice").await;
        if let IncomingFrame::Control(ControlMessage::CourierNotice(notice)) = frame {
            return notice;
        }
    }
}

/// The ids the worker's connection's receipts quote, in the order they went
/// out.
fn acked(sent: &[ControlMessage]) -> Vec<String> {
    sent.iter()
        .filter_map(|message| match message {
            ControlMessage::CourierNoticeAck(CourierNoticeAck { notice_id }) => {
                Some(notice_id.clone())
            }
            _ => None,
        })
        .collect()
}

/// The worker's connection raises the notice and acks the server's frame at
/// once, the hub relays the frame's rows to the tab under its own id, and
/// the tab's receipt of the relayed frame is absorbed by the hub.
#[wasm_bindgen_test]
async fn a_courier_notice_reaches_the_tab_and_the_receipts_split() {
    let (mut tab, _worker_close, worker_late, worker_sent) = hub_with_tab_live(
        Script::accepting(true),
        Arc::new(Plan::new(vec![])),
        Arc::new(Waits::new()),
    )
    .await;
    handshake(&mut tab).await;
    worker_late
        .send(IncomingFrame::Control(ControlMessage::CourierNotice(
            CourierNotice {
                notice_id: "server-1".to_owned(),
                notices: rows(),
            },
        )))
        .expect("hand the worker its notice");
    let notice = next_notice(&mut tab).await;
    assert_eq!(
        notice.notices,
        rows(),
        "the relayed frame carries the settled rows"
    );
    assert_ne!(
        notice.notice_id, "server-1",
        "the relayed frame carries the hub's own id"
    );
    assert_eq!(
        acked(&worker_sent.lock()),
        vec!["server-1".to_owned()],
        "the worker's receipt quotes the server's id and only it"
    );
    // The tab's own receipt of the relayed frame, which the real tab's
    // connection sends the moment it raises the event.
    tab.send_control(ControlMessage::CourierNoticeAck(CourierNoticeAck {
        notice_id: notice.notice_id.clone(),
    }))
    .await
    .expect("post the tab's receipt");
    // The hub absorbed it, so the tab's channel still stands and the tab can
    // still talk.
    tab.send_control(ControlMessage::Ping(Ping { nonce: 1 }))
        .await
        .expect("the tab pings");
    let frame = next_frame(&mut tab, "answered the ping").await;
    assert!(
        matches!(
            frame,
            IncomingFrame::Control(ControlMessage::Pong(Pong { nonce: 1 }))
        ),
        "the tab's channel stands after its receipt, got {frame:?}"
    );
    assert_eq!(
        acked(&worker_sent.lock()),
        vec!["server-1".to_owned()],
        "the tab's receipt never went upstream"
    );
}
