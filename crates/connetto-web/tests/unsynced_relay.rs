//! A tab follows the worker's unsynced work: a tab write the server refuses
//! shows refused in the tab, and the tab's dismissal clears it on the worker.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use core::future::{Future, ready};

use std::time::Duration;

use connetto_client::{
    ClientBuilder, ConnettoClient, FirstThen, KeyValue, LiveUnsynced, RowOp, SyncSchema,
    UnsyncedWork, WriteState, WrittenRow,
};
use connetto_core::Cursor;
use connetto_core::messages::{
    BulkMessage, ControlMessage, HandshakeAck, MutationReject, MutationRejectReason, PriorRefusal,
    WriterGrant,
};
use connetto_core::schema::SchemaBundle;
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_web::{MessageTransport, RelayHub};
use diesel::prelude::*;
use futures_channel::mpsc;
use futures_util::StreamExt;
use wasm_bindgen_futures::spawn_local;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::MessagePort;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

diesel::table! {
    /// The written table.
    items (id) {
        /// The row's key.
        id -> diesel::sql_types::BigInt,
        /// The row's label.
        label -> diesel::sql_types::Text,
    }
}

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)";

/// The schema source the bundle declares, which names its major.
const SOURCE: &str = "-- connetto:major 1";

/// The client schema over [`DDL`].
fn schema() -> SyncSchema {
    SyncSchema::new(
        SchemaBundle::new(
            SOURCE,
            "",
            connetto_core::ReplicaCopy::new(
                DDL,
                Vec::<(String, String)>::new(),
                Vec::<String>::new(),
            ),
            None::<&str>,
        )
        .expect("the source declares major 1"),
    )
}

/// A dialer handing out `transport` once and nothing after.
fn once<T: Transport + 'static>(
    transport: T,
) -> FirstThen<impl FnMut() -> core::future::Ready<Result<T, &'static str>>> {
    FirstThen::new(transport, || core::future::ready(Err("spent")))
}

/// A server that refuses every write as unauthorized.
struct Refusing {
    incoming: mpsc::UnboundedReceiver<IncomingFrame>,
    answers: mpsc::UnboundedSender<IncomingFrame>,
}

impl Refusing {
    fn new() -> Self {
        let (answers, incoming) = mpsc::unbounded();
        answers
            .unbounded_send(IncomingFrame::Control(ControlMessage::HandshakeAck(
                HandshakeAck {
                    connection_id: "unsynced".to_owned(),
                    session_token: "unsynced".to_owned(),
                    resume_token: "unsynced".to_owned(),
                    current_cursor: Cursor::new(Vec::new()),
                    schema_version: None,
                    sunset_at_secs: None,
                    courier: false,
                    initial_credits: 64,
                    writer: Some(WriterGrant {
                        number: 1,
                        frontier: 0,
                    }),
                },
            )))
            .expect("queue handshake");
        Self { incoming, answers }
    }
}

impl Transport for Refusing {
    type Error = core::convert::Infallible;

    fn send_control(
        &mut self,
        message: ControlMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        if let ControlMessage::MutationHeader(header) = message {
            self.answers
                .unbounded_send(IncomingFrame::Control(ControlMessage::MutationReject(
                    MutationReject {
                        client_seq: header.client_seq,
                        reason: MutationRejectReason::Unauthorized,
                    },
                )))
                .expect("queue refusal");
        }
        ready(Ok(()))
    }

    fn send_bulk(
        &mut self,
        _message: BulkMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, Self::Error> {
        loop {
            if let Some(frame) = self.incoming.next().await {
                return Ok(Some(frame));
            }
            core::future::pending::<()>().await;
        }
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }
}

/// The snapshot once `done` holds for it, awaited on the handle's changes.
async fn until(
    handle: &mut LiveUnsynced,
    mut done: impl FnMut(&UnsyncedWork) -> bool,
) -> UnsyncedWork {
    let deadline = connetto_web::workers::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        let work = handle.work();
        if done(&work) {
            return work;
        }
        tokio::select! {
            changed = handle.changed() => changed.expect("the handle stays live"),
            () = &mut deadline => panic!("the snapshot never got there, last {work:?}"),
        }
    }
}

#[wasm_bindgen_test]
async fn a_tab_sees_its_refused_write_and_dismisses_it() {
    let worker = ClientBuilder::new(schema(), once(Refusing::new()))
        .with_client_id("unsynced-worker")
        .connect_driven()
        .await
        .expect("worker connect");
    let (hub, pump, _notices) = RelayHub::new(worker, ":memory:").expect("hub meta");
    spawn_local(async move {
        pump.await.expect("hub pump");
    });
    let channel = web_sys::MessageChannel::new().expect("message channel");
    hub.attach(MessageTransport::<MessagePort>::new(channel.port1()));
    let tab = ClientBuilder::new(
        schema().relay_mirror(),
        once(MessageTransport::<MessagePort>::new(channel.port2())),
    )
    .with_client_id("unsynced-tab")
    .connect_driven()
    .await
    .expect("tab connect");
    let (client, tab_pump) = ConnettoClient::with_pump(tab);
    spawn_local(tab_pump);

    let mut unsynced = client.unsynced_live().await.expect("follow the outbox");
    client
        .with_conn(|conn| {
            diesel::insert_into(items::table)
                .values((items::id.eq(7), items::label.eq("refused")))
                .execute(conn.conn())
                .expect("tab insert");
        })
        .await
        .expect("the write lands in the tab");

    let refused_state =
        WriteState::Refused(PriorRefusal::Rejected(MutationRejectReason::Unauthorized));
    let refused = until(
        &mut unsynced,
        |work| matches!(work.writes.as_slice(), [write] if write.state == refused_state),
    )
    .await;
    assert_eq!(
        refused.writes[0].rows,
        vec![WrittenRow {
            table: "items".to_owned(),
            key: vec![KeyValue::Int(7)],
            op: RowOp::Insert,
        }]
    );

    let write = &refused.writes[0].write;
    client
        .dismiss_refusal(write.writer, write.seq)
        .await
        .expect("the worker dismisses the refusal");
    until(&mut unsynced, |work| *work == UnsyncedWork::default()).await;
}
