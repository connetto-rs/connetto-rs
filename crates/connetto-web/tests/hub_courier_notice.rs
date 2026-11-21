//! What the tabs of a relay hub learn from a courier notice (R78 decision
//! 41 as amended): the worker's connection holds the server's frame, the hub
//! relays it under the server's id to every tab that handshakes while it is
//! held, and the first tab's receipt is the one the worker acks upstream.
//! Runs in a dedicated worker for the wasm SQLite build.

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

/// Ping the hub from the tab and wait for its answer, so every frame the tab
/// sent before has been handled.
async fn barrier(tab: &mut MessageTransport<MessagePort>, nonce: u64) {
    tab.send_control(ControlMessage::Ping(Ping { nonce }))
        .await
        .expect("the tab pings");
    loop {
        if let IncomingFrame::Control(ControlMessage::Pong(Pong { nonce: answered })) =
            next_frame(tab, "answered the ping").await
            && answered == nonce
        {
            return;
        }
    }
}

/// Post the tab's receipt of the frame `notice_id`, as the tab's own
/// connection does the moment it raises the event.
async fn receipt(tab: &mut MessageTransport<MessagePort>, notice_id: &str) {
    tab.send_control(ControlMessage::CourierNoticeAck(CourierNoticeAck {
        notice_id: notice_id.to_owned(),
    }))
    .await
    .expect("post the tab's receipt");
}

/// The server's frame reaches the handshaken tab under the server's id, the
/// worker acks it upstream only on the tab's receipt, and a repeated receipt
/// sends nothing more.
#[wasm_bindgen_test]
async fn the_tabs_receipt_is_the_one_the_worker_acks() {
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
    assert_eq!(notice.notice_id, "server-1", "the tab sees the server's id");
    assert_eq!(notice.notices, rows(), "the relayed frame carries the rows");
    assert!(
        acked(&worker_sent.lock()).is_empty(),
        "the worker holds the frame until a tab acks it"
    );
    receipt(&mut tab, "server-1").await;
    barrier(&mut tab, 1).await;
    assert_eq!(acked(&worker_sent.lock()), ["server-1"]);
    receipt(&mut tab, "server-1").await;
    barrier(&mut tab, 2).await;
    assert_eq!(
        acked(&worker_sent.lock()),
        ["server-1"],
        "a repeated receipt names a frame the worker no longer holds"
    );
}

/// A frame the worker receives before any tab handshakes waits in the
/// worker, reaches the tab at its handshake, and is acked upstream on the
/// tab's receipt.
#[wasm_bindgen_test]
async fn a_notice_before_the_first_tab_handshake_waits_for_the_tab() {
    let (mut tab, _worker_close, worker_late, worker_sent) = hub_with_tab_live(
        Script::accepting(true),
        Arc::new(Plan::new(vec![])),
        Arc::new(Waits::new()),
    )
    .await;
    worker_late
        .send(IncomingFrame::Control(ControlMessage::CourierNotice(
            CourierNotice {
                notice_id: "server-early".to_owned(),
                notices: rows(),
            },
        )))
        .expect("hand the worker its notice");
    // Long enough for the hub's pump to take the frame with no tab there.
    connetto_web::workers::sleep(core::time::Duration::from_millis(300)).await;
    assert!(
        acked(&worker_sent.lock()).is_empty(),
        "the worker holds the frame with no tab to ack it"
    );
    handshake(&mut tab).await;
    let notice = next_notice(&mut tab).await;
    assert_eq!(notice.notice_id, "server-early");
    assert_eq!(notice.notices, rows());
    receipt(&mut tab, "server-early").await;
    barrier(&mut tab, 1).await;
    assert_eq!(acked(&worker_sent.lock()), ["server-early"]);
}
