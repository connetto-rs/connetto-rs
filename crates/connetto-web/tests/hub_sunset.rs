//! What the tabs of a relay hub learn from a major's sunset: the hub's own
//! acknowledgement carries the instant into every tab's acknowledgement, and
//! a live close of the sunset class tells every attached tab the instant
//! before ending the hub without a redial. Runs in a dedicated worker for
//! the wasm SQLite build.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use core::time::Duration;

use std::sync::Arc;

use connetto_core::messages::FatalErrorReason;
use connetto_core::traits::Transport;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

#[path = "hub_mismatch/support.rs"]
mod support;

use support::{Plan, Script, Waits, handshake_ack, hub_with_tab, next_fatal};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The sunset the scenarios carry, a moment in 2023, in seconds.
const SUNSET: u64 = 1_700_000_000;

/// The hub's own attach acknowledged carrying the major's sunset, so every
/// tab the hub acknowledges carries the same instant.
#[wasm_bindgen_test]
async fn a_tab_acknowledged_under_a_sunset_carries_the_instant() {
    let plan = Arc::new(Plan::new(vec![]));
    let (mut tab, _close) = hub_with_tab(
        Script::accepting_with_sunset(true, SUNSET),
        Arc::clone(&plan),
        Arc::new(Waits::new()),
    )
    .await;
    let ack = handshake_ack(&mut tab).await;
    assert_eq!(
        ack.sunset_at_secs,
        Some(SUNSET),
        "the tab's ack carries the instant the hub's attach carried"
    );
}

/// The hub's own attach acknowledged without a sunset, so a tab acknowledged
/// before any close carries none, and the live close of the sunset class
/// tells the tab the instant before ending the hub without a redial.
#[wasm_bindgen_test]
async fn a_live_sunset_close_tells_the_tabs_the_instant_and_skips_the_redial() {
    let plan = Arc::new(Plan::new(vec![Script::accepting_with_sunset(true, SUNSET)]));
    let (mut tab, worker_close) = hub_with_tab(
        Script::accepting(true),
        Arc::clone(&plan),
        Arc::new(Waits::new()),
    )
    .await;
    let ack = handshake_ack(&mut tab).await;
    assert_eq!(
        ack.sunset_at_secs, None,
        "the tab's ack carries no sunset until the close names one"
    );
    worker_close
        .send(FatalErrorReason::Sunset {
            server: support::major(),
            client: None,
            sunset_at_secs: SUNSET,
        })
        .expect("the worker close");
    let reason = next_fatal(&mut tab).await;
    assert!(
        matches!(
            reason,
            FatalErrorReason::Sunset {
                server,
                client: None,
                sunset_at_secs: got
            } if server == support::major() && got == SUNSET
        ),
        "the tab hears the sunset with its instant, got {reason:?}"
    );
    assert_eq!(
        plan.dials(),
        0,
        "the redial the close already answered was skipped"
    );
    let closed = tokio::select! {
        frame = tab.recv() => frame.expect("the tab's transport"),
        () = connetto_web::workers::sleep(Duration::from_secs(5)) => {
            panic!("the hub never ended after the sunset close")
        }
    };
    assert!(closed.is_none(), "the ended hub closes the tab's channel");
}
