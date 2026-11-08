//! The DB worker's upstream survives a real server restart.
//!
//! The stack stops its server, keeps it down for a while and starts it again
//! on the same address. A tab behind the worker's relay hub is told the worker
//! went offline, writes while the server is down, and is told it is connected
//! again. The write then reaches Postgres, and a row another client writes
//! after the restart reaches the tab, so the hub drained its queue and
//! subscribed again.
//!
//! The browser-stack runner supplies Postgres, OpenFGA, the provider, keys, and the sync server.

#![cfg(target_arch = "wasm32")]

mod common;
mod harness;

use std::cell::Cell;
use std::rc::Rc;

use connetto_client::{ClientEvent, ConnettoConnection};
use connetto_core::Transport;
use connetto_core::messages::SyncStatus;
use connetto_wasm_smoke::workers::{DEMO_QUERY, await_db_worker_ready};
use connetto_wasm_smoke::{leader, locks};
use diesel::prelude::*;
use futures_channel::oneshot;
use harness::orders;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{Request, RequestInit, Response, WorkerGlobalScope};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// How long the stack keeps the server down, long enough for the tab to hear
/// the outage and write before the server returns.
const DOWN_MS: u32 = 3000;

/// Asks the stack to stop the server, keep it down for `down_ms` and start it
/// again, returning once it is back.
async fn restart_the_server(down_ms: u32) {
    let init = RequestInit::new();
    init.set_method("POST");
    init.set_body(&down_ms.to_string().into());
    let request =
        Request::new_with_str_and_init(&format!("{}/dev/restart", common::AUTH_BASE), &init)
            .expect("build the restart request");
    let scope = js_sys::global()
        .dyn_into::<WorkerGlobalScope>()
        .expect("worker scope");
    let response: Response = JsFuture::from(scope.fetch_with_request(&request))
        .await
        .expect("the browser stack must be running")
        .dyn_into()
        .expect("response");
    assert!(
        response.ok(),
        "the stack refused to restart the server with status {}",
        response.status()
    );
}

/// Inserts one order through `conn` and returns the id the table's default minted.
fn insert_order<T: Transport>(
    conn: &mut ConnettoConnection<T>,
    identity: &str,
) -> rosetta_uuid::Uuid {
    let before: std::collections::HashSet<rosetta_uuid::Uuid> = orders::table
        .select(orders::id)
        .load(conn.conn())
        .expect("ids before the insert")
        .into_iter()
        .collect();
    diesel::insert_into(orders::table)
        .values((orders::owner_id.eq(identity), orders::quantity.eq(3_i64)))
        .execute(conn.conn())
        .expect("insert an order");
    orders::table
        .select(orders::id)
        .load::<rosetta_uuid::Uuid>(conn.conn())
        .expect("ids after the insert")
        .into_iter()
        .find(|id| !before.contains(id))
        .expect("the newly minted id")
}

#[wasm_bindgen_test]
async fn the_hub_rides_out_a_server_restart() {
    harness::relay_worker_breadcrumbs();
    // The worker logs in for itself, and only a tab can answer that request.
    common::play_the_tab();
    let base = harness::unique_base();
    let membership = leader::join(
        &format!("connetto-hub-restart-{base}"),
        &harness::glue_url(),
    );
    await_db_worker_ready(&[]).await.expect("db worker ready");
    harness::stage("db worker ready");

    let (_, identity) = common::mint_session().await;
    let client_id = rosetta_uuid::Uuid::new_v4().to_string();
    let tab_lock = locks::hold_lock(&locks::tab_lock_name(&client_id)).await;
    let mut tab = harness::connect_tab(&client_id).await;
    tab.subscribe("hub-restart-tab", DEMO_QUERY)
        .await
        .expect("tab subscribe");
    harness::pump_until(&mut tab, |event| {
        matches!(event, ClientEvent::SnapshotEnd { .. })
    })
    .await;
    harness::stage("tab snapshot ended");

    let back = Rc::new(Cell::new(false));
    let (restarted_tx, restarted) = oneshot::channel::<()>();
    spawn_local({
        let back = Rc::clone(&back);
        async move {
            restart_the_server(DOWN_MS).await;
            back.set(true);
            let _ = restarted_tx.send(());
        }
    });
    harness::pump_until(&mut tab, |event| {
        *event == ClientEvent::SyncStatus(SyncStatus::Offline)
    })
    .await;
    harness::stage("tab told the worker is offline");

    let written = insert_order(&mut tab, &identity);
    tab.push().await.expect("tab push").expect("mutation sent");
    assert!(
        !back.get(),
        "the write must be made while the server is down, so raise DOWN_MS"
    );
    harness::stage("tab wrote while the server was down");

    harness::pump_until(&mut tab, |event| {
        *event == ClientEvent::SyncStatus(SyncStatus::Connected)
    })
    .await;
    restarted.await.expect("the restart request answered");
    harness::stage("tab told the worker is connected again");

    // A session of its own, since a second connection under the worker's
    // would evict it.
    let (observer_token, _) = common::mint_session().await;
    let mut observer = harness::connect_server(observer_token, &identity).await;
    observer
        .subscribe("hub-restart-observer", DEMO_QUERY)
        .await
        .expect("observer subscribe");
    harness::pump_until_row(&mut observer, written).await;
    harness::stage("the write made while the server was down reached it");

    let after = harness::write_row(&mut observer, 18, &identity).await;
    harness::pump_until_row(&mut tab, after).await;
    harness::stage("a row written after the restart reached the tab");

    observer.close().await.expect("close the observer");
    tab.close().await.expect("close the tab");
    tab_lock.release();
    drop(membership);
}
