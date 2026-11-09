//! The application's courier access on the main wire (R78 slice 4a): the
//! client's read and call beside the native build's delegation of both,
//! gated like the client's other access.

use core::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use connetto_client::{
    ClientError, ClientEvent, ConnettoClient, CourierError, GateAskOutcome, Moment, NativeClient,
    NativeClientBuilder, SystemClock,
};
use connetto_core::exchange::{
    CourierEntryOutcome, CourierOutcome, Frontier, Offer, SignedOffer, Verdict,
};
use connetto_core::messages::{ControlMessage, CourierAnswer, HandshakeAck, WriterGrant};
use connetto_core::{Cursor, IncomingFrame, Major, SchemaVersion};
use diesel::connection::SimpleConnection as _;
use tokio::time::timeout;

use super::support::{self, LockedMechanism, Script};

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);";
const BOUND: Duration = Duration::from_secs(10);
/// The device key the tests courier.
const AUTHOR: [u8; 32] = [0x07; 32];
/// The certificate end a test cursor carries.
const CERT_END: i64 = 4_102_444_800;

fn hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
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

/// The seeded held row and its offer, the run the candidates and the range
/// stand.
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

/// Seed the held row and the offer on the connected client.
async fn seed(client: &ConnettoClient<support::Script>) {
    client
        .with_conn(|conn| conn.conn().batch_execute(&seed_sql()))
        .await
        .expect("the gate is open")
        .expect("the seed lands");
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

/// An anonymous native build over the scripted wire, its pump spawned. The
/// caller keeps its own handle on the wire.
async fn build(script: &Script, ack: IncomingFrame) -> NativeClient<support::Script> {
    script.say(ack);
    NativeClientBuilder::new("ws://127.0.0.1:1", support::bundle(DDL))
        .with_dialer(support::Once::new(script.clone()))
        .connect()
        .await
        .expect("the build connects")
}

/// The client's courier read and call and the native build's delegation of
/// both are refused while the gate is locked, the way the gate refuses the
/// client's other access.
#[tokio::test]
async fn a_locked_gate_refuses_the_courier_read_and_call() {
    let script = Script::default();
    let native = build(&script, courier_ack()).await;
    let client = native.client().clone();
    let mechanism = LockedMechanism::new();
    client
        .enable_gate(Some(Duration::ZERO), Arc::new(mechanism.clone()))
        .await;
    client.unlock().await;
    mechanism.until_asked(1).await;
    mechanism.resolve(GateAskOutcome::Approved);
    let mut events = client.events();
    timeout(BOUND, async {
        loop {
            let event = events.recv().await.expect("the client is alive");
            if matches!(event, ClientEvent::Unlocked) {
                break;
            }
        }
    })
    .await
    .expect("approval opens the gate");
    client.away(Moment::now(&SystemClock)).await;
    client.back(Moment::now(&SystemClock)).await;
    timeout(BOUND, async {
        loop {
            let event = events.recv().await.expect("the client is alive");
            if matches!(event, ClientEvent::Locked) {
                break;
            }
        }
    })
    .await
    .expect("the zero grace re-locks on the return");
    let refused = client
        .courier_candidates()
        .await
        .expect_err("the gate is locked");
    assert!(matches!(refused, ClientError::Locked));
    let refused = client
        .courier(&AUTHOR)
        .await
        .expect_err("the gate is locked");
    assert!(matches!(refused, CourierError::Client(ClientError::Locked)));
    // The native delegation is refused the same way.
    let refused = native
        .courier_candidates()
        .await
        .expect_err("the gate is locked");
    assert!(matches!(refused, ClientError::Locked));
    let refused = native
        .courier(&AUTHOR)
        .await
        .expect_err("the gate is locked");
    assert!(matches!(refused, CourierError::Client(ClientError::Locked)));
}

/// A courier call whose author key is not a device key is refused before
/// the wire moves, and the path off answers unavailable.
#[tokio::test]
async fn the_client_refuses_courier_calls_before_the_wire_moves() {
    let script = Script::default();
    let native = build(&script, courier_ack()).await;
    let client = native.client().clone();
    let short = client
        .courier(&AUTHOR[..31])
        .await
        .expect_err("the key is 31 bytes");
    assert!(matches!(
        short,
        CourierError::Client(ClientError::Session(_))
    ));
    assert!(
        short.to_string().contains("an author key must be 32 bytes"),
        "{short}"
    );
    let long_key = [7u8; 33];
    let long = client
        .courier(&long_key)
        .await
        .expect_err("the key is 33 bytes");
    assert!(matches!(
        long,
        CourierError::Client(ClientError::Session(_))
    ));
    assert!(
        long.to_string().contains("an author key must be 32 bytes"),
        "{long}"
    );
    assert!(
        script
            .sent(|message| match message {
                ControlMessage::CourierRange(_) => Some(()),
                _ => None,
            })
            .is_empty(),
        "no range went out for a refused key"
    );
    // The path off answers unavailable.
    let script = Script::default();
    let native = build(&script, support::handshake_ack()).await;
    let off = native
        .client()
        .courier(&AUTHOR)
        .await
        .expect_err("the path is off");
    assert!(matches!(off, CourierError::Unavailable));
}

/// The native build reads the session's courier candidates off the seeded
/// run.
#[tokio::test]
async fn the_native_build_reads_the_courier_candidates() {
    let script = Script::default();
    let native = build(&script, courier_ack()).await;
    let client = native.client().clone();
    seed(&client).await;
    let candidates = native.courier_candidates().await.expect("the read stands");
    assert_eq!(candidates.len(), 1, "{candidates:?}");
    assert_eq!(candidates[0].author, AUTHOR.to_vec());
    assert_eq!(candidates[0].seqs, [1]);
    assert_eq!(candidates[0].bytes, 4, "the seeded body is four bytes");
}

/// The native build's courier call is refused on a wrong key length and on
/// the path off, the way the client's is.
#[tokio::test]
async fn the_native_build_refuses_courier_calls() {
    let script = Script::default();
    let native = build(&script, courier_ack()).await;
    let short = native
        .courier(&AUTHOR[..31])
        .await
        .expect_err("the key is 31 bytes");
    assert!(matches!(
        short,
        CourierError::Client(ClientError::Session(_))
    ));
    assert!(
        short.to_string().contains("an author key must be 32 bytes"),
        "{short}"
    );
    let script = Script::default();
    let native = build(&script, support::handshake_ack()).await;
    let off = native.courier(&AUTHOR).await.expect_err("the path is off");
    assert!(matches!(off, CourierError::Unavailable));
}

/// The native build's courier call sends the range on the main wire and
/// settles the server's answer, the settled row leaving the run.
#[tokio::test]
async fn the_native_build_couriers_the_range_and_settles_the_answer() {
    let script = Script::default();
    let native = build(&script, courier_ack()).await;
    let client = native.client().clone();
    seed(&client).await;
    let call = tokio::spawn({
        let client = client.clone();
        async move { client.courier(&AUTHOR).await }
    });
    let range = timeout(BOUND, async {
        loop {
            if let Some(range) = script
                .sent(|message| match message {
                    ControlMessage::CourierRange(range) => Some(range.clone()),
                    _ => None,
                })
                .into_iter()
                .next()
            {
                break range;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the range goes out in time");
    assert_eq!(range.author, AUTHOR);
    assert_eq!(range.rows.len(), 1, "{range:?}");
    assert_eq!(range.rows[0].seq, 1);
    assert_eq!(range.bodies, 1);
    script.say(IncomingFrame::Control(ControlMessage::CourierAnswer(
        CourierAnswer {
            request_id: range.request_id,
            outcome: CourierOutcome::Entries(vec![(
                1,
                CourierEntryOutcome::Ruled(Verdict::Applied),
            )]),
        },
    )));
    let report = timeout(BOUND, call)
        .await
        .expect("the call settles in time")
        .expect("the call task stands")
        .expect("the range settles");
    assert_eq!(report.settled, [1]);
    let candidates = native.courier_candidates().await.expect("the read stands");
    assert!(
        candidates.is_empty(),
        "the settled row leaves the run: {candidates:?}"
    );
}

/// A courier call whose pump is not driven ends on its own outer bound, the
/// range waiting out the answer nobody settles.
#[tokio::test(start_paused = true)]
async fn a_courier_call_without_a_driven_pump_times_out() {
    let script = Script::default();
    script.say(courier_ack());
    let (native, pump) = NativeClientBuilder::new("ws://127.0.0.1:1", support::bundle(DDL))
        .with_dialer(support::Once::new(script.clone()))
        .connect_with_pump()
        .await
        .expect("the build connects");
    // The pump stays parked: only the call's own outer bound can end it.
    let client = native.client().clone();
    client
        .with_conn(|conn| conn.conn().batch_execute(&seed_sql()))
        .await
        .expect("the gate is open")
        .expect("the seed lands");
    let started = tokio::time::Instant::now();
    let err = client.courier(&AUTHOR).await.expect_err("the bound runs");
    assert!(matches!(err, CourierError::TimedOut));
    assert!(
        started.elapsed() >= Duration::from_secs(35),
        "the outer bound is the range's wait plus its slack"
    );
    drop(pump);
}
