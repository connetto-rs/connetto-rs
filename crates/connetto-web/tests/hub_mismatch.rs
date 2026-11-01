//! What the tabs of a relay hub learn when the hub's own upstream meets a
//! reason the hub's build can never attach under: a refusal at the attach
//! ends the hub and tells every tab the reason, a live close of that class
//! skips the first redial, and a rate-limited refusal sets the floor on the
//! next wait. Runs in a dedicated worker for the wasm SQLite build.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use core::time::Duration;

use std::sync::Arc;

use connetto_core::Major;
use connetto_core::messages::{ControlMessage, FatalErrorReason, SyncStatus};
use connetto_core::traits::{IncomingFrame, Transport};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

#[path = "hub_mismatch/support.rs"]
mod support;

use support::{Plan, Script, Waits, handshake, hub_with_tab, next_fatal, next_frame_within};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The hub's own attach drew a refusal its build can never attach under, so
/// the hub ends and tells every attached tab the reason it would have heard
/// from a refused handshake.
#[wasm_bindgen_test]
async fn a_hub_that_cannot_attach_tells_its_tabs_why() {
    let mismatch = FatalErrorReason::MajorMismatch {
        server: Major::new(2).expect("two is a major"),
        client: Some(support::major()),
    };
    let plan = Arc::new(Plan::new(vec![Script::refusing(mismatch.clone())]));
    let (mut tab, _close) = hub_with_tab(
        Script::accepting(false),
        Arc::clone(&plan),
        Arc::new(Waits::new()),
    )
    .await;
    let refusal = next_fatal(&mut tab).await;
    assert_eq!(
        refusal, mismatch,
        "the tab hears the reason the attach drew"
    );
    assert_eq!(plan.dials(), 1, "the hub dials once and ends");
    let closed = tokio::select! {
        frame = tab.recv() => frame.expect("the tab's transport"),
        () = connetto_web::workers::sleep(Duration::from_secs(5)) => {
            panic!("the hub never ended after the refusal")
        }
    };
    assert!(closed.is_none(), "the ended hub closes the tab's channel");
}

/// A live close that names a reason the hub's build can never attach under
/// skips the first redial, since the hub already answered the question the
/// redial would ask again.
#[wasm_bindgen_test]
async fn a_live_close_of_the_terminal_class_ends_the_hub_without_a_redial() {
    let mismatch = FatalErrorReason::MajorMismatch {
        server: Major::new(2).expect("two is a major"),
        client: Some(support::major()),
    };
    let plan = Arc::new(Plan::new(vec![]));
    let (mut tab, worker_close) = hub_with_tab(
        Script::accepting(true),
        Arc::clone(&plan),
        Arc::new(Waits::new()),
    )
    .await;
    handshake(&mut tab).await;
    worker_close
        .send(mismatch.clone())
        .expect("the worker close");
    let refusal = next_fatal(&mut tab).await;
    assert_eq!(refusal, mismatch);
    assert_eq!(
        plan.dials(),
        0,
        "the redial the close already answered was skipped"
    );
}

/// The hub's resume is refused with a rate-limit that names a window, so the
/// hub's next wait floors on that window instead of probing the answer, and the
/// tabs keep being served once the resume lands.
#[wasm_bindgen_test]
async fn a_rate_limited_refusal_floors_the_hubs_next_wait() {
    let rate_limited = FatalErrorReason::RateLimited {
        retry_after_ms: 5000,
    };
    let plan = Arc::new(Plan::new(vec![
        Script::refusing(rate_limited.clone()),
        Script::accepting(true),
    ]));
    let waits = Arc::new(Waits::new());
    let (mut tab, worker_close) =
        hub_with_tab(Script::accepting(true), Arc::clone(&plan), waits.clone()).await;
    handshake(&mut tab).await;
    worker_close
        .send(rate_limited.clone())
        .expect("the worker close");
    // The close posts an Offline, so waiting on it first skips whatever the
    // hub sent the tab while the worker was still live.
    loop {
        let frame = next_frame_within(&mut tab, "went offline", Duration::from_secs(10)).await;
        if matches!(
            frame,
            IncomingFrame::Control(ControlMessage::SyncStatus(SyncStatus::Offline))
        ) {
            break;
        }
    }
    loop {
        let frame = next_frame_within(
            &mut tab,
            "restated the connection status",
            Duration::from_secs(10),
        )
        .await;
        if matches!(
            frame,
            IncomingFrame::Control(ControlMessage::SyncStatus(SyncStatus::Connected))
        ) {
            break;
        }
    }
    let waits = waits.take();
    assert!(
        waits.len() >= 2,
        "the hub waited between every two dials, got {waits:?}, dials = {}, dialed = {:?}",
        plan.dials(),
        plan.dialed()
    );
    assert!(
        *waits.last().expect("a recorded wait") >= Duration::from_millis(5000),
        "the wait after the rate-limited refusal floors on the server's window, got {waits:?}"
    );
    assert_eq!(plan.dials(), 2);
}
