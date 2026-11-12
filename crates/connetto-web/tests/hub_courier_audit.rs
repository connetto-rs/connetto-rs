//! Tab-owned courier audit receipt observations.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use core::time::Duration;

use connetto_client::away::{Clock, SystemClock};
use connetto_core::exchange::MAX_MANIFEST_ROWS;
use connetto_core::messages::{ControlMessage, CourierAuditOutcome, CourierAuditRequest};
use connetto_core::traits::Transport;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

#[path = "hub_courier_audit/fixture.rs"]
mod fixture;

#[expect(
    dead_code,
    reason = "shared content helpers serve several independent browser suites"
)]
#[path = "content_archive/support.rs"]
mod support;

#[expect(dead_code, reason = "only the content staging helpers are needed here")]
#[path = "content_relay/fixture.rs"]
mod content_fixture;

use fixture::{Bound, Harness, Peer, Tab, upstream};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen_test]
async fn a_tab_observes_its_committed_courier_receipt() {
    let bound = Bound::new();
    let (harness, mut peer) = Harness::new(bound, None, false).await;
    let mut tab = harness.tab(bound, "receipt-owner").await;
    tab.audit("tab-receipt", 7, vec![11, 12], 10_000).await;
    let request = peer.request(bound, 7, &[11, 12]).await;
    assert_ne!(request.request_id, "tab-receipt");
    peer.answer(&request.request_id, CourierAuditOutcome::Confirmed);
    tab.answer(bound, "tab-receipt", CourierAuditOutcome::Confirmed)
        .await;
    tab.barrier(bound, 113).await;
}

#[wasm_bindgen_test]
async fn equal_tab_ids_have_distinct_upstream_routes_and_private_answers() {
    let bound = Bound::new();
    let (harness, mut peer) = Harness::new(bound, None, false).await;
    let mut first = harness.tab(bound, "first-owner").await;
    let mut second = harness.tab(bound, "second-owner").await;
    first.audit("same-id", 7, vec![11], 10_000).await;
    let a = peer.request(bound, 7, &[11]).await;
    second.audit("same-id", 8, vec![12], 10_000).await;
    let b = peer.request(bound, 8, &[12]).await;
    assert_ne!(a.request_id, b.request_id);
    assert_ne!(a.request_id, "same-id");
    assert_ne!(b.request_id, "same-id");
    peer.answer(&b.request_id, CourierAuditOutcome::Refused);
    second
        .answer(bound, "same-id", CourierAuditOutcome::Refused)
        .await;
    first.barrier(bound, 114).await;
    peer.answer(&a.request_id, CourierAuditOutcome::Confirmed);
    first
        .answer(bound, "same-id", CourierAuditOutcome::Confirmed)
        .await;
    second.barrier(bound, 115).await;
}

#[wasm_bindgen_test]
async fn cancellation_does_not_cancel_another_tabs_equal_id_or_settle_a_replacement() {
    let bound = Bound::new();
    let (harness, mut peer) = Harness::new(bound, None, false).await;
    let mut first = harness.tab(bound, "cancel-owner").await;
    let mut second = harness.tab(bound, "retained-owner").await;
    first.audit("same-id", 7, vec![21], 10_000).await;
    let old = peer.request(bound, 7, &[21]).await;
    second.audit("same-id", 8, vec![22], 10_000).await;
    let retained = peer.request(bound, 8, &[22]).await;
    first.cancel("same-id").await;
    peer.cancelled(bound, &old.request_id).await;
    first.audit("same-id", 9, vec![23], 10_000).await;
    let replacement = peer.request(bound, 9, &[23]).await;
    assert_ne!(old.request_id, replacement.request_id);
    peer.answer(&old.request_id, CourierAuditOutcome::Confirmed);
    peer.answer(&retained.request_id, CourierAuditOutcome::Confirmed);
    second
        .answer(bound, "same-id", CourierAuditOutcome::Confirmed)
        .await;
    peer.answer(&replacement.request_id, CourierAuditOutcome::Refused);
    first
        .answer(bound, "same-id", CourierAuditOutcome::Refused)
        .await;
    first.barrier(bound, 116).await;
    second.barrier(bound, 117).await;
}

#[wasm_bindgen_test]
async fn changed_inputs_refuse_and_cancel_the_id_before_it_can_be_reused() {
    let bound = Bound::new();
    let (harness, mut peer) = Harness::new(bound, None, false).await;
    let mut tab = harness.tab(bound, "changed-owner").await;
    for (index, (author, seqs)) in [(8, vec![31]), (7, vec![32])].into_iter().enumerate() {
        let id = format!("changed-{index}");
        tab.audit(&id, 7, vec![31], 10_000).await;
        let old = peer.request(bound, 7, &[31]).await;
        tab.audit(&id, author, seqs, 10_000).await;
        tab.answer(bound, &id, CourierAuditOutcome::Refused).await;
        peer.cancelled(bound, &old.request_id).await;
        tab.audit(&id, 9, vec![33], 10_000).await;
        let replacement = peer.request(bound, 9, &[33]).await;
        assert_ne!(replacement.request_id, old.request_id);
        peer.answer(&old.request_id, CourierAuditOutcome::Confirmed);
        peer.answer(&replacement.request_id, CourierAuditOutcome::Failed);
        tab.answer(bound, &id, CourierAuditOutcome::Failed).await;
        tab.barrier(bound, 120 + u64::try_from(index).expect("two cases"))
            .await;
    }
}

#[wasm_bindgen_test]
async fn identical_duplicates_keep_the_original_route_and_expiry() {
    let bound = Bound::new();
    let (harness, mut peer) = Harness::new(bound, None, false).await;
    let mut tab = harness.tab(bound, "duplicate-owner").await;
    let expiry = Bound::until(SystemClock.monotonic() + Duration::from_secs(2));
    tab.audit("duplicate", 7, vec![41], 600).await;
    let original = peer.request(bound, 7, &[41]).await;
    tab.audit("duplicate", 7, vec![41], 60_000).await;
    tab.barrier(bound, 122).await;
    tab.answer(expiry, "duplicate", CourierAuditOutcome::Expired)
        .await;
    peer.cancelled(bound, &original.request_id).await;
    tab.barrier(bound, 123).await;
}

#[wasm_bindgen_test]
async fn malformed_requests_are_refused_without_retention_or_disconnection() {
    let bound = Bound::new();
    let (harness, mut peer) = Harness::new(bound, None, false).await;
    let mut tab = harness.tab(bound, "invalid-owner").await;
    let cases = [
        (String::new(), vec![1], 10_000),
        (
            "x".repeat(CourierAuditRequest::MAX_REQUEST_ID_BYTES + 1),
            vec![1],
            10_000,
        ),
        ("empty".to_owned(), vec![], 10_000),
        ("zero".to_owned(), vec![0], 10_000),
        ("descending".to_owned(), vec![2, 1], 10_000),
        ("repeated".to_owned(), vec![1, 1], 10_000),
        ("overflow".to_owned(), vec![u64::MAX], 10_000),
        ("no-time".to_owned(), vec![1], 0),
        (
            "too-many".to_owned(),
            (1..=u64::try_from(MAX_MANIFEST_ROWS + 1).expect("manifest limit")).collect(),
            10_000,
        ),
    ];
    for (id, seqs, remaining_ms) in cases {
        tab.audit(&id, 7, seqs, remaining_ms).await;
        tab.answer(bound, &id, CourierAuditOutcome::Refused).await;
    }
    tab.audit("after-invalid", 9, vec![51], 10_000).await;
    let valid = peer.request(bound, 9, &[51]).await;
    peer.answer(&valid.request_id, CourierAuditOutcome::Confirmed);
    tab.answer(bound, "after-invalid", CourierAuditOutcome::Confirmed)
        .await;
}

fn full_batch() -> Vec<u64> {
    (1..=u64::try_from(MAX_MANIFEST_ROWS).expect("manifest limit")).collect()
}

async fn fill(
    tab: &mut Tab,
    peer: &mut Peer,
    bound: Bound,
    prefix: &str,
    count: usize,
) -> Vec<CourierAuditRequest> {
    let mut routes = Vec::new();
    for index in 0..count {
        let id = format!("{prefix}-{index}");
        tab.audit(&id, 7, full_batch(), 12_000).await;
        routes.push(peer.request(bound, 7, &full_batch()).await);
    }
    routes
}

#[wasm_bindgen_test]
async fn shared_capacity_is_released_by_cancellation_and_tab_departure() {
    let bound = Bound::new();
    let (harness, mut peer) = Harness::new(bound, None, false).await;
    let mut first = harness.tab(bound, "capacity-owner").await;
    let mut second = harness.tab(bound, "capacity-contender").await;
    let routes = fill(&mut first, &mut peer, bound, "full", 8).await;
    second.audit("over-capacity", 8, vec![1], 10_000).await;
    second
        .answer(bound, "over-capacity", CourierAuditOutcome::Refused)
        .await;
    first.cancel("full-0").await;
    peer.cancelled(bound, &routes[0].request_id).await;
    second.audit("released-slot", 8, full_batch(), 10_000).await;
    let replacement = peer.request(bound, 8, &full_batch()).await;
    peer.answer(&replacement.request_id, CourierAuditOutcome::Confirmed);
    second
        .answer(bound, "released-slot", CourierAuditOutcome::Confirmed)
        .await;
    bound
        .run("capacity-owner departure", first.wire.close())
        .await
        .expect("close capacity owner");
    let expected: std::collections::HashSet<_> = routes[1..]
        .iter()
        .map(|route| route.request_id.clone())
        .collect();
    let mut cancellations = std::collections::HashSet::new();
    while cancellations.len() < expected.len() {
        if let ControlMessage::CourierAuditCancel(cancel) =
            peer.control(bound, "departed capacity owner").await
        {
            assert!(expected.contains(&cancel.request_id));
            assert!(cancellations.insert(cancel.request_id));
        }
    }
    second
        .audit("after-departure", 9, full_batch(), 10_000)
        .await;
    let request = peer.request(bound, 9, &full_batch()).await;
    peer.answer(&request.request_id, CourierAuditOutcome::Confirmed);
    second
        .answer(bound, "after-departure", CourierAuditOutcome::Confirmed)
        .await;
}

#[wasm_bindgen_test]
async fn same_account_reconnect_uses_a_fresh_route_and_the_original_deadline() {
    let bound = Bound::new();
    let (recovery, mut fresh) = upstream(bound, false, false);
    let (harness, mut old) = Harness::new(bound, Some(recovery), false).await;
    let mut tab = harness.tab(bound, "reconnect-owner").await;
    let started = SystemClock.monotonic();
    tab.audit("reconnecting", 7, vec![61], 3_000).await;
    let original = old.request(bound, 7, &[61]).await;
    old.disconnect();
    fresh.handshake(bound).await;
    tab.audit("attachment-clock", 8, vec![62], 700).await;
    tab.answer(bound, "attachment-clock", CourierAuditOutcome::Expired)
        .await;
    let mut fresh = harness.redial(bound).await;
    let reissued = fresh.request(bound, 7, &[61]).await;
    assert_ne!(original.request_id, reissued.request_id);
    assert!(reissued.remaining_ms < 2_400);
    fresh.answer(&original.request_id, CourierAuditOutcome::Confirmed);
    tab.answer(
        Bound::until(started + Duration::from_millis(3_500)),
        "reconnecting",
        CourierAuditOutcome::Expired,
    )
    .await;
    fresh.cancelled(bound, &reissued.request_id).await;
    tab.barrier(bound, 124).await;
}

#[wasm_bindgen_test]
async fn an_existing_observation_expires_during_an_unanswered_handshake() {
    let bound = Bound::new();
    let (recovery, mut fresh) = upstream(bound, false, false);
    let (harness, mut old) = Harness::new(bound, Some(recovery), false).await;
    let mut tab = harness.tab(bound, "handshake-expiry").await;
    let started = SystemClock.monotonic();
    tab.audit("held-handshake", 7, vec![63], 2_000).await;
    old.request(bound, 7, &[63]).await;
    old.disconnect();
    fresh.handshake(bound).await;
    assert!(SystemClock.monotonic() < started + Duration::from_secs(2));
    tab.answer(bound, "held-handshake", CourierAuditOutcome::Expired)
        .await;
    fresh.accept();
    tab.barrier(bound, 125).await;
}

async fn stalled_controls(hold_replay: bool) {
    let bound = Bound::new();
    let (recovery, mut fresh) = upstream(bound, false, hold_replay);
    let (harness, mut old) = Harness::new(bound, Some(recovery), hold_replay).await;
    let mut first = harness.tab(bound, "stalled-owner").await;
    let mut second = harness.tab(bound, "stalled-survivor").await;
    let _routes = fill(&mut first, &mut old, bound, "stalled", 8).await;
    old.disconnect();
    fresh.handshake(bound).await;
    if hold_replay {
        fresh.accept();
        fresh.replaying(bound).await;
    }
    first.ping(130).await;
    first.cancel("stalled-0").await;
    first
        .audit("freed-during-stall", 9, full_batch(), 600)
        .await;
    first
        .answer_before_pong(bound, &["freed-during-stall"], 130)
        .await;
    harness.hub.kill(first.id);
    second.ping(131).await;
    second
        .audit("departed-during-stall", 8, full_batch(), 600)
        .await;
    second
        .audit("departed-second-slot", 8, full_batch(), 600)
        .await;
    second
        .answer_before_pong(
            bound,
            &["departed-during-stall", "departed-second-slot"],
            131,
        )
        .await;
    if hold_replay {
        fresh.replay.release();
    } else {
        fresh = harness.redial(bound).await;
    }
    second.pong(bound, 131).await;
    second.audit("after-stall", 9, vec![71], 10_000).await;
    let request = fresh.request(bound, 9, &[71]).await;
    fresh.answer(&request.request_id, CourierAuditOutcome::Confirmed);
    second
        .answer(bound, "after-stall", CourierAuditOutcome::Confirmed)
        .await;
}

#[wasm_bindgen_test]
async fn attachment_stall_preserves_expiry_cancellation_and_departure_behind_application_frames() {
    stalled_controls(false).await;
}

#[wasm_bindgen_test]
async fn replay_stall_preserves_expiry_cancellation_and_departure_behind_application_frames() {
    stalled_controls(true).await;
}

#[wasm_bindgen_test]
async fn an_in_flight_content_upload_keeps_audit_answers_expiry_and_cancellation_live() {
    use std::cell::RefCell;
    use std::rc::Rc;

    use connetto_file_client::{BrowserStore, ContentArchive};
    use wasm_bindgen::JsCast;

    let bound = Bound::new();
    let _serial = bound
        .run(
            "audit-content-storage",
            connetto_web::locks::hold_lock(connetto_web::workers::DB_ALIVE_LOCK),
        )
        .await;
    let scope = js_sys::global()
        .dyn_into::<web_sys::DedicatedWorkerGlobalScope>()
        .expect("dedicated worker");
    let store = bound
        .run(
            "audit-content-store",
            BrowserStore::install(&scope, "r113-audit-upload"),
        )
        .await
        .expect("install content store");
    let uploaded = Rc::new(RefCell::new(Vec::new()));
    let mut held = support::install_blocked_content_transport(&uploaded);
    let (harness, mut peer) =
        Harness::with_content(bound, ContentArchive::new(store, [113; 32])).await;
    let mut tab = harness.tab(bound, "upload-owner").await;
    let photo = b"courier audit remains live while this photograph waits for its upload response";
    let file_id = content_fixture::declared_id(photo);
    content_fixture::stage(&tab.wire, file_id, &content_fixture::blob_of(photo));
    let changeset = content_fixture::captured_insert(1, file_id.as_bytes());
    bound
        .run(
            "upload-mutation-1",
            content_fixture::send_mutation(&mut tab.wire, 1, &changeset),
        )
        .await;
    let ticket = loop {
        if let ControlMessage::ContentTicketRequest(request) =
            peer.control(bound, "upload ticket").await
        {
            assert_eq!(request.file_id, *file_id.as_bytes());
            break request;
        }
    };
    peer.grant(&ticket);
    let started = bound
        .run(&ticket.request_id, held.next_started())
        .await
        .expect("upload start");
    assert!(started.contains("/chunks/"));
    assert!(started.ends_with(&format!("?t={}", ticket.request_id)));
    tab.audit("upload-confirmation", 7, vec![81], 10_000).await;
    let confirmation = peer.request(bound, 7, &[81]).await;
    peer.answer(&confirmation.request_id, CourierAuditOutcome::Confirmed);
    tab.answer(bound, "upload-confirmation", CourierAuditOutcome::Confirmed)
        .await;
    tab.audit("upload-cancellation", 8, vec![82], 10_000).await;
    let cancelled = peer.request(bound, 8, &[82]).await;
    tab.cancel("upload-cancellation").await;
    peer.cancelled(bound, &cancelled.request_id).await;
    tab.audit("upload-expiry", 9, vec![83], 600).await;
    let expires = peer.request(bound, 9, &[83]).await;
    peer.answer(&cancelled.request_id, CourierAuditOutcome::Confirmed);
    tab.answer(bound, "upload-expiry", CourierAuditOutcome::Expired)
        .await;
    peer.cancelled(bound, &expires.request_id).await;
    assert!(
        uploaded.borrow().is_empty(),
        "upload remains held through audit completion"
    );
    held.release();
    let commit = bound
        .run(&ticket.request_id, held.next_committed())
        .await
        .expect("upload commit");
    assert_eq!(
        commit,
        format!(
            "https://content.invalid/files/{file_id}/commit?t={}",
            ticket.request_id,
        )
    );
    assert_eq!(uploaded.borrow().as_slice(), &[photo.to_vec()]);
    tab.barrier(bound, 140).await;
    drop(harness);
}

#[wasm_bindgen_test]
async fn bounded_observation_retains_a_pending_attachment_after_tab_departure() {
    let bound = Bound::new();
    let (recovery, mut fresh) = upstream(bound, false, false);
    let (harness, mut old) = Harness::new(bound, Some(recovery), false).await;
    let mut departing = harness.tab(bound, "departing-owner").await;
    let mut survivor = harness.tab(bound, "surviving-owner").await;
    survivor.audit("attachment-owner", 7, vec![92], 2_000).await;
    old.request(bound, 7, &[92]).await;
    old.disconnect();
    fresh.handshake(bound).await;
    harness.hub.kill(departing.id);
    departing
        .audit("departed-control", 8, vec![93], 10_000)
        .await;
    departing.cancel("departed-control").await;
    survivor
        .answer(bound, "attachment-owner", CourierAuditOutcome::Expired)
        .await;
    fresh.accept();
    survivor.barrier(bound, 141).await;
}
