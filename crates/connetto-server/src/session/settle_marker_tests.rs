use super::*;
use crate::defaults::ConnettoDefaults;
use crate::write_target::PendingWrite;
use crate::{ManagerBuilder, PgSnapshotSource, pg_write_target};
use connetto_core::loopback;
use connetto_core::test_support::TestGrantChecker;
use connetto_test_harness::RosterAuth;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;

async fn manager() -> Arc<SessionManager<PgSnapshotSource, RosterAuth, ConnettoDefaults>> {
    const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, quantity INT);";
    let pool = Pool::builder()
        .max_size(1)
        .build(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            "postgres://nobody@127.0.0.1:9/none",
        ))
        .await
        .expect("pool");
    ManagerBuilder::new(
        Materializer::builder(DDL).build().expect("materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), DDL).expect("snapshot source"),
        RosterAuth::granting_nobody(),
        Arc::new(TestGrantChecker),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(pool, DDL).expect("write target"),
    )
    .build()
}

fn patch(label: &str) -> LivePatch {
    LivePatch::new(label.to_owned(), Cursor::new(Vec::new()), vec![1])
}

async fn receive(client: &mut connetto_core::transport::LoopbackTransport) -> IncomingFrame {
    tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("frame deadline")
        .expect("receive")
        .expect("frame")
}

#[tokio::test]
async fn settling_precedes_the_first_patch_once_for_each_bound_session() {
    let manager = manager().await;
    let mut receivers = Vec::new();
    let mut senders = Vec::new();
    for connection_num in [1, 2] {
        let session_id = SessionId::from_token_hash(&format!("session-{connection_num}"));
        let (outbound, receiver) = outbound_channel(4096);
        senders.push(outbound.clone());
        manager.sessions.lock().await.insert(
            session_id,
            LiveSession {
                connection_num,
                tx: outbound,
                user: None,
            },
        );
        manager
            .writer_sessions
            .lock()
            .await
            .entry(17)
            .or_default()
            .insert(
                session_id,
                WriterSession {
                    connection_num,
                    writer: connetto_core::messages::WriterRef::Number(17),
                },
            );
        receivers.push(receiver);
    }
    manager
        .pending_writes
        .lock()
        .insert(23, PendingWrite::new(vec![(17, 9)]));
    for outbound in &senders {
        manager
            .queue_live(PgXid(23), outbound, patch("first"))
            .await;
        manager
            .queue_live(PgXid(23), outbound, patch("second"))
            .await;
    }
    manager.settle_commit(PgXid(23)).await;
    for mut receiver in receivers {
        let Outbound::Live(_, Some(marker)) = receiver.try_recv().expect("marker") else {
            panic!("expected settling marker");
        };
        assert_eq!(
            marker.writer,
            connetto_core::messages::WriterRef::Number(17)
        );
        assert_eq!(marker.through_seq, 9);
        assert!(marker.before_patch);
        assert!(matches!(receiver.try_recv(), Ok(Outbound::Live(_, None))));
        assert!(receiver.try_recv().is_err());
    }
    assert!(manager.pending_writes.lock().is_empty());
}

#[tokio::test]
async fn a_patchless_commit_settles_after_earlier_credit_blocked_patches() {
    let manager = manager().await;
    let session_id = SessionId::from_token_hash("patchless");
    let (outbound, mut receiver) = outbound_channel(4096);
    manager.sessions.lock().await.insert(
        session_id,
        LiveSession {
            connection_num: 1,
            tx: outbound.clone(),
            user: None,
        },
    );
    manager
        .writer_sessions
        .lock()
        .await
        .entry(17)
        .or_default()
        .insert(
            session_id,
            WriterSession {
                connection_num: 1,
                writer: connetto_core::messages::WriterRef::Device([3; 32]),
            },
        );
    manager
        .pending_writes
        .lock()
        .insert(23, PendingWrite::new(vec![(17, 9)]));
    manager
        .queue_live(PgXid(22), &outbound, patch("earlier"))
        .await;
    manager.settle_commit(PgXid(23)).await;
    let Outbound::Live(earlier, None) = receiver.try_recv().expect("earlier patch") else {
        panic!("expected earlier patch");
    };
    let Outbound::WritesSettled(marker) = receiver.try_recv().expect("marker") else {
        panic!("expected settling marker");
    };
    assert!(!marker.before_patch);
    assert_eq!(
        marker.writer,
        connetto_core::messages::WriterRef::Device([3; 32])
    );
    assert_eq!(marker.through_seq, 9);
    assert_eq!(
        outbound.queue.queued.load(Ordering::Acquire),
        budgeted_bytes(&earlier) + settled_bytes(&marker),
    );
    let mut pending = VecDeque::from([
        Deliverable::Budgeted(earlier),
        Deliverable::WritesSettled(marker),
    ]);
    let (mut server, mut client) = loopback();
    let mut credits = 0;
    flush(&mut server, &mut credits, &mut pending, &outbound.queue)
        .await
        .expect("blocked flush");
    assert_eq!(pending.len(), 2);
    credits = 1;
    flush(&mut server, &mut credits, &mut pending, &outbound.queue)
        .await
        .expect("flush");
    assert!(matches!(
        receive(&mut client).await,
        IncomingFrame::Bulk(BulkMessage::LivePatch(_))
    ));
    assert!(matches!(
        receive(&mut client).await,
        IncomingFrame::Control(ControlMessage::WritesSettled(_))
    ));
    assert_eq!(credits, 0);
    assert!(pending.is_empty());
    assert_eq!(outbound.queue.queued.load(Ordering::Acquire), 0);
}
