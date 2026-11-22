//! Aggregate delivery, the parts the relay depends on.
//!
//! Phase 1 of the relay-parity plan makes aggregate subscriptions transparent
//! through the browser relay hub. The hub forwards the worker client's
//! `ClientEvent::AggregateColumns` and `ClientEvent::Aggregate` back down to
//! the owning tab as the frames they mirror, so each event MUST carry every
//! field its wire message does. The server has emitted grouped deltas since
//! the R82 adoption (proven end to end in connetto-server's
//! `grouped_wire.rs`), and this test pins the client's decode alone with
//! hand-crafted frames over a loopback, so a decode regression cannot hide
//! behind the server's construction.

use connetto_client::{ClientBuilder, ClientEvent};
use connetto_core::Cursor;
use connetto_core::messages::{
    AggregateChange, AggregateColumns, AggregateUpdate, ColumnType, ControlMessage, HandshakeAck,
    SqliteValue,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_server::{LoopbackTransport, loopback};

const SQLITE_DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, quantity INTEGER);";

/// Complete the wire handshake as a fake server, then push the columns of a
/// grouped answer and one update to it.
fn aggregate_pusher(update: AggregateUpdate) -> LoopbackTransport {
    let (mut fake_server, client_end) = loopback();
    tokio::spawn(async move {
        let Ok(Some(IncomingFrame::Control(ControlMessage::Handshake(_)))) =
            fake_server.recv().await
        else {
            return;
        };
        let _ = fake_server
            .send_control(ControlMessage::HandshakeAck(HandshakeAck {
                connection_id: "agg".to_owned(),
                session_token: "agg".to_owned(),
                resume_token: "agg".to_owned(),
                current_cursor: Cursor::new(Vec::new()),
                schema_version: None,
                sunset_at_secs: None,
                courier: false,
                initial_credits: 64,
                writer: Some(connetto_core::messages::WriterGrant {
                    number: 1,
                    frontier: 0,
                }),
            }))
            .await;
        let _ = fake_server
            .send_control(ControlMessage::AggregateColumns(AggregateColumns {
                sub_id: update.sub_id.clone(),
                columns: vec![ColumnType::Text, ColumnType::Integer],
            }))
            .await;
        let _ = fake_server
            .send_control(ControlMessage::AggregateUpdate(update))
            .await;
        while let Ok(Some(_)) = fake_server.recv().await {}
    });
    client_end
}

// The decoded events mirror the wire frames field for field, so a relay can
// rebuild faithful frames, the declared columns first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_update_decodes_columns_and_keyed_row() {
    let transport = aggregate_pusher(AggregateUpdate {
        sub_id: "by-region".to_owned(),
        change: AggregateChange::Upsert {
            key: b"region=eu".to_vec(),
            row: vec![SqliteValue::Text("eu".to_owned()), SqliteValue::Integer(3)],
        },
    });
    let mut conn = ClientBuilder::new(
        super::support::bundle(SQLITE_DDL),
        super::support::Once::new(transport),
    )
    .signed_in(super::support::held("token"))
    .connect_driven()
    .await
    .expect("connect");

    // The build opens the replica offline before it attaches the transport, so
    // the connection states Offline and then Connected ahead of anything the
    // server sends.
    assert_eq!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::SyncStatus(connetto_client::SyncStatus::Offline)
    );
    assert_eq!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::SyncStatus(connetto_client::SyncStatus::Connected)
    );
    assert_eq!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::AggregateColumns {
            sub_id: "by-region".to_owned(),
            columns: vec![ColumnType::Text, ColumnType::Integer],
        },
    );
    assert_eq!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::Aggregate {
            sub_id: "by-region".to_owned(),
            change: AggregateChange::Upsert {
                key: b"region=eu".to_vec(),
                row: vec![SqliteValue::Text("eu".to_owned()), SqliteValue::Integer(3)],
            },
        },
    );
}

// A removal frame decodes faithfully so a relay can forward the key-departure
// signal to the tab.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_removal_decodes_as_remove() {
    let transport = aggregate_pusher(AggregateUpdate {
        sub_id: "by-region".to_owned(),
        change: AggregateChange::Remove {
            key: b"region=eu".to_vec(),
        },
    });
    let mut conn = ClientBuilder::new(
        super::support::bundle(SQLITE_DDL),
        super::support::Once::new(transport),
    )
    .signed_in(super::support::held("token"))
    .connect_driven()
    .await
    .expect("connect");

    assert_eq!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::SyncStatus(connetto_client::SyncStatus::Offline)
    );
    assert_eq!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::SyncStatus(connetto_client::SyncStatus::Connected)
    );
    assert!(matches!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::AggregateColumns { .. }
    ));
    assert_eq!(
        conn.pump_one().await.expect("pump"),
        ClientEvent::Aggregate {
            sub_id: "by-region".to_owned(),
            change: AggregateChange::Remove {
                key: b"region=eu".to_vec(),
            },
        },
    );
}
