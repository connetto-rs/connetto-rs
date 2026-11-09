//! The live pump's courier dial (R78 slice 4c, decision 47): the range its
//! wire owes is dialed by the pump over the reconnect factory, an
//! uncertified bind refusing the call, a refused dial backing off.

use core::fmt::Write as _;
use std::time::Duration;

use connetto_client::{CourierError, FirstThen, NativeClient, NativeClientBuilder, SyncSchema};
use connetto_core::exchange::{CourierRefusal, Frontier, Offer, SignedOffer};
use connetto_core::messages::{ControlMessage, HandshakeAck, WriterGrant};
use connetto_core::schema::SchemaBundle;
use connetto_core::{Cursor, IncomingFrame, Major, SchemaVersion};
use diesel::connection::SimpleConnection as _;
use tokio::time::timeout;

use super::support::{self, Script};

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);";
/// The bound a pump-driven dial owes on.
const BOUND: Duration = Duration::from_secs(5);
/// The device key the tests courier, its offer standing major 1 so the
/// range rides the courier's dial rather than the major-2 build's main wire.
const AUTHOR: [u8; 32] = [0x07; 32];
/// The certificate end a test cursor carries.
const CERT_END: i64 = 4_102_444_800;

fn hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes {
        write!(out, "{byte:02x}").expect("the hex appends");
    }
    out
}

/// A test schema bundle standing major 2, one major above the author's offer.
fn bundle2() -> SyncSchema {
    SyncSchema::new(
        SchemaBundle::new(
            "-- connetto:major 2",
            "",
            DDL,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares major 2"),
    )
}

/// The author's offer the tests seed, major 1, newest at the seeded row.
fn offer_bytes() -> Vec<u8> {
    let offer = SignedOffer {
        offer: Offer {
            builds: vec![(
                Major::new(1).expect("a major"),
                SchemaVersion::from_hash(b"-- connetto:major 1".to_vec()),
            )],
            settled: None,
            newest: Some(Frontier {
                seq: 1,
                head: [9; 32],
            }),
            retractions: Vec::new(),
        },
        signature: vec![1, 2, 3],
    };
    rmp_serde::to_vec_named(&offer).expect("the offer encodes")
}

/// The seeded held row and its major-1 offer, the run the dial stands.
fn seed_sql() -> String {
    let author = hex(&AUTHOR);
    format!(
        "INSERT INTO _connetto_peer_entry \
         (author, seq, state, major, origin, digest, changeset, account, bot_list, received_at, position) \
         VALUES (x'{author}', 1, 1, 1, NULL, x'0707070707070707070707070707070707070707070707070707070707070707', x'68656c64', 'courier-author', NULL, 1, 1); \
         INSERT INTO _connetto_peer_cursor (author, account, seq, head, offer, expires_at) \
         VALUES (x'{author}', 'courier-author', 0, zeroblob(32), x'{offer}', {CERT_END})",
        offer = hex(&offer_bytes())
    )
}

/// The handshake answer that grants the courier path, the support one with
/// it on.
fn courier_ack() -> IncomingFrame {
    IncomingFrame::Control(ControlMessage::HandshakeAck(HandshakeAck {
        connection_id: "courier".to_owned(),
        session_token: "courier".to_owned(),
        resume_token: "courier".to_owned(),
        current_cursor: Cursor::from(Vec::new()),
        schema_version: None,
        sunset_at_secs: None,
        courier: true,
        initial_credits: 64,
        writer: Some(WriterGrant {
            number: 1,
            frontier: 0,
        }),
    }))
}

/// An anonymous native build whose main wire is `main`, the dialer handing
/// the courier dial's wire off `second`, its pump spawned.
async fn build(
    main: &Script,
    second: &Script,
) -> (NativeClient<Script>, tokio::task::JoinHandle<()>) {
    main.say(courier_ack());
    second.say(courier_ack());
    let second = second.clone();
    let dialer = FirstThen::new(main.clone(), move || {
        let wire = second.clone();
        async move { Ok::<Script, support::Gone>(wire) }
    });
    let (native, pump) = NativeClientBuilder::new("ws://127.0.0.1:1", bundle2())
        .with_dialer(dialer)
        .connect_with_pump()
        .await
        .expect("the build connects");
    (native, tokio::spawn(pump))
}

/// An anonymous native build whose main wire is `main`, every dial after it
/// refused, its pump spawned.
async fn build_with_refusing_dials(
    main: &Script,
) -> (NativeClient<Script>, tokio::task::JoinHandle<()>) {
    main.say(courier_ack());
    let dialer = FirstThen::new(main.clone(), move || async {
        Err::<Script, support::Gone>(support::Gone::Closed)
    });
    let (native, pump) = NativeClientBuilder::new("ws://127.0.0.1:1", bundle2())
        .with_dialer(dialer)
        .connect_with_pump()
        .await
        .expect("the build connects");
    (native, tokio::spawn(pump))
}

/// The pump dials the range its wire owes, and the bind the device cannot
/// stand refuses the call, the dial owing nothing more.
#[tokio::test]
async fn the_live_pump_dials_the_owed_courier_range_and_the_bind_refuses_it() {
    let main = Script::default();
    let second = Script::default();
    let (native, pump) = build(&main, &second).await;
    let client = native.client().clone();
    client
        .with_conn(|conn| conn.conn().batch_execute(&seed_sql()))
        .await
        .expect("the gate is open")
        .expect("the seed lands");
    let err = timeout(BOUND, client.courier(&AUTHOR))
        .await
        .expect("the pump dials the range in time")
        .expect_err("the device stands no bind");
    assert!(
        matches!(err, CourierError::Refused(CourierRefusal::Uncertified)),
        "the uncertified bind refuses the range: {err:?}"
    );
    assert_eq!(
        client
            .with_conn(|conn| conn.courier_drain_timer())
            .await
            .expect("the gate is open"),
        None,
        "the refused dial owes nothing more"
    );
    drop(pump);
}

/// A dial the factory refuses backs off under the pump, the range's wait
/// growing while the call keeps its bound.
#[tokio::test]
async fn a_refused_courier_dial_backs_off_under_the_live_pump() {
    let main = Script::default();
    let (native, pump) = build_with_refusing_dials(&main).await;
    let client = native.client().clone();
    client
        .with_conn(|conn| conn.conn().batch_execute(&seed_sql()))
        .await
        .expect("the gate is open")
        .expect("the seed lands");
    let call = client.clone();
    tokio::spawn(async move {
        let _ = call.courier(&AUTHOR).await;
    });
    timeout(BOUND, async {
        loop {
            let wait = client
                .with_conn(|conn| conn.courier_drain_timer().map(|timer| timer.wait()))
                .await
                .expect("the gate is open");
            if wait == Some(Duration::ZERO) || wait.is_none() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            break;
        }
    })
    .await
    .expect("the failed dial backs off in time");
    drop(pump);
}
