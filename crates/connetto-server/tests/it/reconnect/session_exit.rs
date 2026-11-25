use std::io;
use std::sync::Arc;
use std::time::Duration;

use connetto_core::messages::{BulkMessage, ControlMessage, Grant, Handshake};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, PROTOCOL_VERSION};
use connetto_server::{LoopbackTransport, Materializer, ReadFence, SessionError, loopback};
use connetto_test_harness::{Fixture, RosterAuth, WITHHELD_ID};
use subql::{PgCommitPosition, PgLsn, PgSqliteEmuSource};
use tokio::task::JoinHandle;

use super::{
    FencedManager, FencedSeed, PG_DDL, client_replica, fenced_manager, insert, next_bulk,
    next_control, orders, subscribe,
};

struct RefuseLivePatch(LoopbackTransport);

impl Transport for RefuseLivePatch {
    type Error = io::Error;

    async fn send_control(&mut self, message: ControlMessage) -> Result<(), Self::Error> {
        self.0.send_control(message).await.map_err(io::Error::other)
    }

    async fn send_bulk(&mut self, message: BulkMessage) -> Result<(), Self::Error> {
        if matches!(message, BulkMessage::LivePatch(_)) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "live patch write failed",
            ));
        }
        self.0.send_bulk(message).await.map_err(io::Error::other)
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, Self::Error> {
        self.0.recv().await.map_err(io::Error::other)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.0.close().await.map_err(io::Error::other)
    }
}

async fn connect<T: Transport + Send + 'static>(
    manager: &Arc<FencedManager>,
    server: T,
    client: &mut LoopbackTransport,
    resume: Option<Cursor>,
) -> JoinHandle<Result<(), SessionError>> {
    let serving = Arc::clone(manager);
    let task = tokio::spawn(serving.serve(server));
    let mut handshake =
        Handshake::new(PROTOCOL_VERSION, "client-a").with_grant(Grant::new("user:client-a"));
    if let Some(cursor) = resume {
        handshake = handshake
            .with_cursor(cursor)
            .with_cursor_subjects(core::iter::empty::<&str>());
    }
    client
        .send_control(ControlMessage::Handshake(handshake))
        .await
        .expect("send handshake");
    assert!(matches!(
        next_control(client).await,
        ControlMessage::Challenge(_)
    ));
    assert!(matches!(
        next_control(client).await,
        ControlMessage::HandshakeAck(_)
    ));
    task
}

async fn failed_patch_can_be_resumed(missed_by_snapshot: bool) {
    let fixture = Fixture::acquire().await;
    let seed = FencedSeed::default();
    let manager = fenced_manager(
        &fixture,
        seed.clone(),
        PG_DDL,
        RosterAuth::granting("client-a").withholding(WITHHELD_ID),
    );
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open source");
    let held = insert(&mut source, 1).await;
    manager.dispatch_event(&held).await.expect("dispatch seed");
    let pending = insert(&mut source, 2).await;
    let cursor = if missed_by_snapshot {
        manager
            .dispatch_event(&pending)
            .await
            .expect("dispatch missed row");
        let after = PgLsn(pending.position().commit_lsn().0 + 1);
        let held_transaction = held.position().xid().0;
        let missed_transaction = pending.position().xid().0;
        let fence = ReadFence::parse(
            &format!(
                "{held_transaction}:{}:{missed_transaction}",
                missed_transaction + 1
            ),
            after,
        )
        .expect("snapshot fence");
        connetto_server::snapshot_cursor(PgCommitPosition::before_commit(after), Some(&fence))
    } else {
        connetto_server::snapshot_cursor(held.position(), None)
    };
    *seed.cursor.lock() = cursor;

    let (server, mut client) = loopback();
    let task = connect(&manager, RefuseLivePatch(server), &mut client, None).await;
    subscribe(&mut client).await;
    assert!(matches!(
        next_control(&mut client).await,
        ControlMessage::SnapshotBegin(_)
    ));
    let BulkMessage::SnapshotPatch(snapshot) = next_bulk(&mut client).await else {
        panic!("expected seed snapshot");
    };
    let ControlMessage::SnapshotEnd(completion) = next_control(&mut client).await else {
        panic!("expected completed snapshot");
    };
    if !missed_by_snapshot {
        manager
            .dispatch_event(&pending)
            .await
            .expect("dispatch live row");
    }
    let outcome = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the failed write ends the session")
        .expect("session task completes");
    assert!(
        matches!(outcome, Err(SessionError::Transport(_))),
        "{outcome:?}"
    );
    assert_eq!(manager.live_connections().await, 0);
    assert_eq!(manager.open_sessions(), 0);
    assert!(
        client
            .recv()
            .await
            .expect("read the closed connection")
            .is_none()
    );

    resume_patch(&manager, completion.cursor, &snapshot.patchset_zstd).await;
}

async fn resume_patch(manager: &Arc<FencedManager>, cursor: Cursor, snapshot: &[u8]) {
    let (server, mut returning) = loopback();
    let recovery = connect(manager, server, &mut returning, Some(cursor)).await;
    subscribe(&mut returning).await;
    let BulkMessage::LivePatch(replayed) = next_bulk(&mut returning).await else {
        panic!("expected the undelivered row on resume");
    };
    let mut replica = client_replica();
    let applier = Materializer::builder(PG_DDL)
        .build()
        .expect("build applier");
    applier
        .apply_diffset(snapshot, &mut replica)
        .expect("apply snapshot");
    applier
        .apply_diffset(&replayed.patchset_zstd, &mut replica)
        .expect("apply replay");
    assert_eq!(
        orders(&mut replica)
            .into_iter()
            .map(|order| order.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(matches!(
        next_control(&mut returning).await,
        ControlMessage::SnapshotEnd(_)
    ));
    returning.close().await.expect("close resumed client");
    recovery
        .await
        .expect("resumed session task completes")
        .expect("resumed session closes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_missed_patch_ends_the_session_and_can_be_resumed() {
    tokio::time::timeout(Duration::from_secs(90), failed_patch_can_be_resumed(true))
        .await
        .expect("missed-patch failure and recovery complete before the deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_live_patch_ends_the_session_and_can_be_resumed() {
    tokio::time::timeout(Duration::from_secs(90), failed_patch_can_be_resumed(false))
        .await
        .expect("live-patch failure and recovery complete before the deadline");
}
