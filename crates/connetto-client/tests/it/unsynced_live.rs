//! An application follows its own unsynced writes live: each write shows as
//! queued, sent or deferred until the server takes it, and a refused write
//! stays in view, across a restart, until the application dismisses it.

use std::sync::Arc;
use std::time::Duration;

use connetto_client::{
    ClientBuilder, ConnettoClient, DataDir, FileState, KeyValue, LiveSpec as _, LiveUnsynced,
    MemoryRefreshStore, RowOp, TrackedWrite, Unsynced, UnsyncedFile, UnsyncedWork, UnsyncedWrite,
    Watchable as _, WriteState, WriterRef, WrittenRow,
};
use connetto_core::messages::{
    ControlMessage, MutationApplied, MutationReject, MutationRejectReason, PriorRefusal,
    RateLimited, TabDismissAnswer, TabDismissOutcome,
};
use connetto_core::traits::IncomingFrame;
use diesel::prelude::*;
use tempfile::TempDir;
use tokio::sync::mpsc;

use super::support::{Script, Unreachable, handshake_ack};

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)";
const BOUND: Duration = Duration::from_secs(10);

diesel::table! {
    /// Synced test table.
    items (id) {
        /// Item identifier, the primary key
        id -> Integer,
        /// Optional item label
        label -> Nullable<Text>,
    }
}

/// A client on a replica in `dir` that starts offline and reaches each
/// server the test sends on the returned channel, one per dial.
async fn offline_client(
    dir: &TempDir,
) -> (
    ConnettoClient<Script>,
    mpsc::UnboundedSender<Script>,
    tokio::task::JoinHandle<()>,
) {
    let (servers, waiting) = mpsc::unbounded_channel::<Script>();
    let waiting = Arc::new(tokio::sync::Mutex::new(waiting));
    let mut first = true;
    let dialer = move || {
        let offline = core::mem::replace(&mut first, false);
        let waiting = Arc::clone(&waiting);
        async move {
            if offline {
                return Err(Unreachable);
            }
            waiting.lock().await.recv().await.ok_or(Unreachable)
        }
    };
    let credential = super::support::held("tester");
    let key_store = super::support::key_store(&credential).await;
    let (running, pump) = ClientBuilder::new(super::support::bundle(DDL), dialer)
        .with_sleeper(tokio::time::sleep)
        .signed_in(credential)
        .durable(
            DataDir::new(dir.path().to_path_buf()),
            key_store,
            MemoryRefreshStore::default(),
        )
        .connect_with_pump()
        .await
        .expect("the client opens offline");
    (running.client().clone(), servers, tokio::spawn(pump))
}

/// A server that has acknowledged the handshake it is about to receive.
fn server() -> Script {
    let script = Script::default();
    script.say(handshake_ack());
    script
}

/// The snapshot once `done` holds for it, awaited on the handle's changes.
async fn until(
    handle: &mut LiveUnsynced,
    mut done: impl FnMut(&UnsyncedWork) -> bool,
) -> UnsyncedWork {
    tokio::time::timeout(BOUND, async {
        loop {
            let work = handle.work();
            if done(&work) {
                return work;
            }
            handle.changed().await.expect("the handle stays live");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the snapshot never got there, last {:?}", handle.work()))
}

/// The one write of `work` in `state`, if that is all it holds.
fn only_write(work: &UnsyncedWork, state: &WriteState) -> Option<TrackedWrite> {
    match work.writes.as_slice() {
        [write] if write.state == *state => Some(write.clone()),
        _ => None,
    }
}

/// The row a test write inserts.
fn inserted_item() -> Vec<WrittenRow> {
    vec![WrittenRow {
        table: "items".to_owned(),
        key: vec![KeyValue::Int(1)],
        op: RowOp::Insert,
    }]
}

async fn write_item(client: &ConnettoClient<Script>) {
    client
        .with_conn(|conn| {
            diesel::insert_into(items::table)
                .values((items::id.eq(1), items::label.eq("unsynced")))
                .execute(conn.conn())
                .expect("write the row");
        })
        .await
        .expect("the write lands locally");
}

/// The sequence of the `count`th write `script` was sent.
async fn sent_write(script: &Script, count: usize) -> u64 {
    script
        .until_sent(count, |message| match message {
            ControlMessage::MutationHeader(header) => Some(header.client_seq),
            _ => None,
        })
        .await
}

/// An offline write shows queued with the row it inserted, connecting sends
/// it, a lost socket puts it back in the queue, and the server's apply
/// takes it out.
#[tokio::test]
async fn a_write_moves_from_queued_to_sent_to_gone() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, servers, pump) = offline_client(&dir).await;
    let mut unsynced = client.unsynced_live().await.expect("follow the outbox");
    assert_eq!(unsynced.work(), UnsyncedWork::default());

    write_item(&client).await;
    let queued = until(&mut unsynced, |work| {
        only_write(work, &WriteState::Queued).is_some()
    })
    .await;
    assert_eq!(queued.writes[0].rows, inserted_item());

    let first = server();
    servers.send(first.clone()).expect("the dialer waits");
    let seq = sent_write(&first, 1).await;
    let sent = until(&mut unsynced, |work| {
        only_write(work, &WriteState::Sent).is_some()
    })
    .await;
    assert_eq!(sent.writes[0].write.seq, seq);
    assert_eq!(sent.writes[0].rows, inserted_item());

    first.hang_up();
    until(&mut unsynced, |work| {
        only_write(work, &WriteState::Queued).is_some_and(|write| write.write.seq == seq)
    })
    .await;

    let second = server();
    servers.send(second.clone()).expect("the dialer waits");
    let resent = sent_write(&second, 1).await;
    assert_eq!(resent, seq, "the new wire carries the same write");
    until(&mut unsynced, |work| {
        only_write(work, &WriteState::Sent).is_some()
    })
    .await;
    second.say(IncomingFrame::Control(ControlMessage::MutationApplied(
        MutationApplied { client_seq: seq },
    )));
    until(&mut unsynced, |work| *work == UnsyncedWork::default()).await;

    client.close().await;
    pump.await.expect("the pump exits");
}

/// A write the server cannot settle yet shows deferred until it is applied.
#[tokio::test]
async fn a_deferred_write_shows_deferred_until_applied() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, servers, pump) = offline_client(&dir).await;
    let mut unsynced = client.unsynced_live().await.expect("follow the outbox");
    let script = server();
    servers.send(script.clone()).expect("the dialer waits");
    write_item(&client).await;
    let seq = sent_write(&script, 1).await;
    script.say(IncomingFrame::Control(ControlMessage::RateLimited(
        RateLimited {
            related_to: Some(seq.to_string()),
            retry_after_ms: 3_600_000,
        },
    )));
    let deferred = until(&mut unsynced, |work| {
        only_write(work, &WriteState::Deferred).is_some()
    })
    .await;
    assert_eq!(deferred.writes[0].write.seq, seq);
    assert_eq!(deferred.writes[0].rows, inserted_item());

    script.say(IncomingFrame::Control(ControlMessage::MutationApplied(
        MutationApplied { client_seq: seq },
    )));
    until(&mut unsynced, |work| *work == UnsyncedWork::default()).await;

    client.close().await;
    pump.await.expect("the pump exits");
}

/// A refused write leaves its table but stays in the snapshot, across a
/// restart, until the application dismisses it.
#[tokio::test]
async fn a_refused_write_stays_until_dismissed() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, servers, pump) = offline_client(&dir).await;
    let mut unsynced = client.unsynced_live().await.expect("follow the outbox");
    let script = server();
    servers.send(script.clone()).expect("the dialer waits");
    write_item(&client).await;
    let seq = sent_write(&script, 1).await;
    script.say(IncomingFrame::Control(ControlMessage::MutationReject(
        MutationReject {
            client_seq: seq,
            reason: MutationRejectReason::Unauthorized,
        },
    )));
    let refused_state =
        WriteState::Refused(PriorRefusal::Rejected(MutationRejectReason::Unauthorized));
    let refused = until(&mut unsynced, |work| {
        only_write(work, &refused_state).is_some()
    })
    .await;
    assert_eq!(refused.writes[0].write.seq, seq);
    assert_eq!(refused.writes[0].rows, inserted_item());
    let left = client
        .with_conn(|conn| {
            items::table
                .count()
                .get_result::<i64>(conn.conn())
                .expect("count the items")
        })
        .await
        .expect("read the table");
    assert_eq!(left, 0, "the refused row is rolled back");
    client.close().await;
    pump.await.expect("the pump exits");
    drop((unsynced, servers));

    let (client, _servers, pump) = offline_client(&dir).await;
    let mut unsynced = client.unsynced_live().await.expect("follow the outbox");
    assert_eq!(unsynced.work(), refused, "the refusal outlives the restart");
    let writer = refused.writes[0].write.writer;
    client
        .dismiss_refusal(writer, seq)
        .await
        .expect("dismiss the refusal");
    until(&mut unsynced, |work| *work == UnsyncedWork::default()).await;

    client.close().await;
    pump.await.expect("the pump exits");
}

/// A tab follows the snapshot its worker states, through the descriptor
/// every frontend uses, and dismisses a refusal by asking the worker.
#[tokio::test]
async fn a_tab_follows_its_workers_snapshot_and_dismisses_through_it() {
    let (client, script, pump) = super::support::relay_tab().await;
    let mut unsynced = Unsynced
        .query::<Script>()
        .live(&client)
        .await
        .expect("follow the worker's outbox");
    assert_eq!(unsynced.work(), UnsyncedWork::default());

    let writer = WriterRef::Number(1);
    let stated = UnsyncedWork {
        writes: vec![TrackedWrite {
            write: UnsyncedWrite {
                writer,
                seq: 3,
                major: None,
            },
            state: WriteState::Refused(PriorRefusal::Rejected(MutationRejectReason::Unauthorized)),
            rows: inserted_item(),
        }],
        files: vec![UnsyncedFile {
            file_id: "ab".repeat(32),
            major: None,
            state: FileState::Queued,
        }],
    };
    script.say(IncomingFrame::Control(ControlMessage::TabUnsynced(
        stated.clone(),
    )));
    until(&mut unsynced, |work| *work == stated).await;

    let call = {
        let client = client.clone();
        tokio::spawn(async move { client.dismiss_refusal(writer, 3).await })
    };
    let request = tokio::time::timeout(
        BOUND,
        script.until_sent(1, |message| match message {
            ControlMessage::TabDismissRefusal(request) => Some(request.clone()),
            _ => None,
        }),
    )
    .await
    .expect("the tab asks its worker");
    assert_eq!((request.writer, request.seq), (writer, 3));
    script.say(IncomingFrame::Control(ControlMessage::TabDismissAnswer(
        TabDismissAnswer {
            request_id: request.request_id,
            outcome: TabDismissOutcome::Dismissed,
        },
    )));
    tokio::time::timeout(BOUND, call)
        .await
        .expect("the call returns once answered")
        .expect("the call task completes")
        .expect("the worker dismissed it");

    drop((unsynced, client));
    let _ = tokio::time::timeout(BOUND, pump).await;
}
