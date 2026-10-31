//! Catalogue browser tests for the leptos web demo.
//!
//! The catalogue is the schema's one shared table, held once per device in
//! the shared store that every account on it reads. This test signs two
//! accounts in on the same origin, each through the demo's own boot path and
//! its own login, and checks the shared store's identities go from one to two
//! as the second account opens the store the first created, both accounts
//! reading the twelve catalogue rows the deployment seeds.
//!
//! The dev IdP accepts any username and maps each one to its own identity, so
//! the two accounts are two usernames. The two workers share the replica
//! prefix and with it the shared store file, while each takes its own account
//! index, which is what makes the second boot sign in anew rather than
//! refresh the first account.
//!
//! Every boot reserves its own room in the OPFS pool, eight slots beyond what
//! the pool already holds, so the second identity's open, which writes its
//! replica and the shared store in one transaction, grows the pool at the
//! boot. The test itself never installs the pool, since a context holding it
//! keeps the workers' access handles and sends them to an in-memory replica,
//! and each run starts on a fresh browser profile with no catalogue files.

#![cfg(all(target_arch = "wasm32", feature = "web"))]

mod common;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use common::{connect_tab, glue_url, spawn_worker, stage, walk_login_as};
use connetto_client::dsl::Watchable;
use connetto_client::{ClientEvent, ConnettoClient, LiveQuery, StorageUse};
use connetto_demo_support::{PRODUCT_QUERY, Product, products_by_name};
use connetto_web::auth::{LOGIN_CHANNEL, LoginMessage, deliver_login_code};
use connetto_web::{locks, workers};
use futures_channel::oneshot;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{BroadcastChannel, MessageEvent};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The login the first account walks, the dev IdP's form taking any one.
const USER_FIRST: &str = "catalogue-first";
/// The login the second account walks, a distinct identity on the same origin.
const USER_SECOND: &str = "catalogue-second";
/// The catalogue rows the deployment seeds.
const PRODUCT_ROWS: usize = 12;

const SUITE_LOCK_CATALOGUE: &str = "connetto-leptos-catalogue-suite";

/// The page side of the workers' logins, handing each parked login URL to the
/// queue's next username.
fn play_catalogue_tab(usernames: Rc<RefCell<VecDeque<String>>>) {
    let ch = BroadcastChannel::new(LOGIN_CHANNEL).expect("login channel");
    let cb =
        wasm_bindgen::closure::Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
            let Some(text) = e.data().as_string() else {
                return;
            };
            let Ok(LoginMessage::Request { url }) = serde_json::from_str::<LoginMessage>(&text)
            else {
                return;
            };
            let username = usernames
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| panic!("a login the queue did not name"));
            spawn_local(async move {
                let (code, state) = walk_login_as(&url, &username).await;
                deliver_login_code(&code, &state).expect("deliver login code");
            });
        });
    ch.set_onmessage(Some(cb.as_ref().unchecked_ref()));
    cb.forget();
    std::mem::forget(ch);
}

/// One tab's read of the catalogue, the twelve rows the worker's upstream
/// holds and the storage answer the worker gives for its account.
async fn read_the_catalogue(client_id: &str) -> (Vec<Product>, StorageUse) {
    let _tab_lock = locks::hold_lock(&locks::tab_lock_name(client_id)).await;
    let (_content, mut conn) = connect_tab(client_id).await;
    conn.subscribe(&format!("catalogue-{client_id}"), PRODUCT_QUERY)
        .await
        .expect("subscribe");
    loop {
        let event = conn.pump_one().await.expect("pump");
        assert_ne!(event, ClientEvent::Closed, "connection closed early");
        if matches!(event, ClientEvent::SnapshotEnd { .. }) {
            break;
        }
    }
    stage("catalogue subscription ready");

    let (client, pump) = ConnettoClient::with_pump(conn);
    let (done_tx, done_rx) = oneshot::channel::<()>();
    spawn_local(async move {
        pump.await;
        let _ = done_tx.send(());
    });

    let mut live: LiveQuery<Product> = products_by_name().live(&client).await.expect("live query");
    let rows = loop {
        if live.rows().len() == PRODUCT_ROWS {
            break live.rows().to_vec();
        }
        live.changed().await.expect("live refresh");
    };
    assert!(
        rows.iter()
            .any(|product| product.name == "Espresso machine"),
        "the catalogue lost its espresso machine"
    );
    let used = client.storage_use().await.expect("storage read");

    drop(live);
    drop(client);
    done_rx.await.expect("pump exited");
    (rows, used)
}

/// Two accounts on one origin, the second reading the first's catalogue from
/// the shared store the first created.
#[wasm_bindgen_test]
async fn a_second_account_reads_the_first_accounts_catalogue_from_the_shared_store() {
    let usernames = Rc::new(RefCell::new(VecDeque::new()));
    play_catalogue_tab(Rc::clone(&usernames));
    let _serial = locks::hold_lock(SUITE_LOCK_CATALOGUE).await;

    // The first account signs in and opens the store.
    usernames.borrow_mut().push_back(USER_FIRST.to_owned());
    let (worker, boot) = spawn_worker(&glue_url(), "db_worker_catalogue_boot");
    if let Err(error) = workers::await_db_worker_ready(&[boot]).await {
        worker.terminate();
        panic!("catalogue readiness rejected {error:?}");
    }
    stage("first catalogue worker booted");

    let client_id = rosetta_uuid::Uuid::new_v4().to_string();
    let (rows, used) = read_the_catalogue(&client_id).await;
    assert_eq!(rows.len(), PRODUCT_ROWS, "the catalogue read its rows");
    let shared = used.shared.expect("the schema shares the catalogue");
    assert_eq!(
        shared.identities, 1,
        "one account should hold the shared store"
    );
    stage("first account reads the catalogue alone");

    worker.terminate();
    // Chrome releases terminated workers' OPFS handles asynchronously.
    workers::sleep(core::time::Duration::from_millis(200)).await;
    stage("first account ended");

    // The second account opens the same store on the same origin.
    usernames.borrow_mut().push_back(USER_SECOND.to_owned());
    let (worker, boot) = spawn_worker(&glue_url(), "db_worker_catalogue_second_boot");
    if let Err(error) = workers::await_db_worker_ready(&[boot]).await {
        worker.terminate();
        panic!("second catalogue readiness rejected {error:?}");
    }
    stage("second catalogue worker booted");

    let client_id = rosetta_uuid::Uuid::new_v4().to_string();
    let (rows, used) = read_the_catalogue(&client_id).await;
    assert_eq!(rows.len(), PRODUCT_ROWS, "the catalogue read its rows");
    let shared = used.shared.expect("the schema shares the catalogue");
    assert_eq!(
        shared.identities, 2,
        "the second account should join the shared store"
    );
    stage("second account reads the catalogue from the shared store");

    worker.terminate();
    // Chrome releases terminated workers' OPFS handles asynchronously.
    workers::sleep(core::time::Duration::from_millis(200)).await;
}
