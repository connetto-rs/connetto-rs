//! A relay tab's storage read travels to the worker that holds the replica
//! and the shared store (R11 slice 7), as a share-key change does.

use std::time::Duration;

use connetto_client::{ClientBuilder, ClientError, ConnettoClient, SharedStoreUse, StorageUse};
use connetto_core::messages::{
    ControlMessage, GateState, StoreBytes, TabStorageAnswer, TabStorageOutcome, TabStorageUse,
};
use connetto_core::traits::IncomingFrame;

use super::support::{Script, handshake_ack, key_gated_notes as schema};

impl Script {
    fn storage_reads(&self) -> Vec<TabStorageUse> {
        self.sent(|message| match message {
            ControlMessage::TabStorageUse(read) => Some(read.clone()),
            _ => None,
        })
    }

    /// The `count`th storage read the tab sent, waited for within five seconds.
    async fn storage_read(&self, count: usize) -> TabStorageUse {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let reads = self.storage_reads();
            if reads.len() >= count {
                return reads[count - 1].clone();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the tab sent no storage read in time"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn storage_answer(&self, request_id: &str, outcome: TabStorageOutcome) {
        self.say(IncomingFrame::Control(ControlMessage::TabStorageAnswer(
            TabStorageAnswer {
                request_id: request_id.to_owned(),
                outcome,
            },
        )));
    }
}

/// A tab mirror attached to a scripted relay, its pump running.
async fn tab() -> (ConnettoClient<Script>, Script, tokio::task::JoinHandle<()>) {
    let script = Script::default();
    script.say(handshake_ack());
    let mut conn = ClientBuilder::new(
        schema().relay_mirror(),
        super::support::NeverDial::<Script>::default(),
    )
    .with_client_id("tab")
    .open_driven()
    .expect("the mirror opens");
    conn.attach(script.clone())
        .await
        .expect("the relay acknowledges the tab");
    let (client, pump) = ConnettoClient::with_pump(conn);
    (client, script, tokio::spawn(pump))
}

fn worker_use() -> StorageUse {
    StorageUse {
        replica: StoreBytes {
            file: 81_920,
            free: 4_096,
        },
        shared: Some(SharedStoreUse {
            bytes: StoreBytes {
                file: 40_960,
                free: 0,
            },
            identities: 2,
        }),
    }
}

/// The tab returns what the worker read, and maps each refusal onto the error
/// a native call returns.
#[tokio::test]
async fn a_tab_returns_the_workers_storage_use_and_its_refusals() {
    let (client, script, pump) = tab().await;
    for (index, outcome) in [
        TabStorageOutcome::Use(worker_use()),
        TabStorageOutcome::Locked,
        TabStorageOutcome::Refused("the replica is gone".to_owned()),
    ]
    .into_iter()
    .enumerate()
    {
        let call = {
            let client = client.clone();
            tokio::spawn(async move { client.storage_use().await })
        };
        let read = script.storage_read(index + 1).await;
        script.storage_answer(&read.request_id, outcome.clone());
        let result = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("the call returns once answered")
            .expect("the call task completes");
        match (outcome, result) {
            (TabStorageOutcome::Use(sent), Ok(got)) => assert_eq!(got, sent),
            (TabStorageOutcome::Locked, Err(ClientError::Locked))
            | (TabStorageOutcome::Refused(_), Err(ClientError::Session(_))) => {}
            (outcome, result) => panic!("{outcome:?} surfaced as {result:?}"),
        }
    }
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), pump).await;
}

/// A tab wire that drops fails a pending read instead of leaving it waiting.
#[tokio::test]
async fn a_dropped_tab_wire_fails_the_pending_read() {
    let (client, script, pump) = tab().await;
    let call = {
        let client = client.clone();
        tokio::spawn(async move { client.storage_use().await })
    };
    script.storage_read(1).await;
    script.hang_up();
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("the call fails when the wire drops")
        .expect("the call task completes");
    assert!(
        matches!(result, Err(ClientError::Transport(_))),
        "a dropped wire surfaces as a transport failure, got {result:?}"
    );
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), pump).await;
}

/// A tab that already learned the gate is locked refuses before asking.
#[tokio::test]
async fn a_locked_tab_reads_nothing() {
    let (client, script, pump) = tab().await;
    script.say(IncomingFrame::Control(ControlMessage::GateState(
        GateState::Locked,
    )));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !matches!(client.with_conn(|_| ()).await, Err(ClientError::Locked)) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the tab never applied the lock"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(matches!(
        client.storage_use().await,
        Err(ClientError::Locked)
    ));
    assert_eq!(
        script.storage_reads(),
        Vec::new(),
        "a locked tab asks nothing"
    );
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), pump).await;
}

/// A mirror's own database is its worker's copy, so reading it directly
/// refuses rather than reporting the mirror.
#[tokio::test]
async fn a_mirror_connection_refuses_a_direct_read() {
    let mut conn = ClientBuilder::new(
        schema().relay_mirror(),
        super::support::NeverDial::<Script>::default(),
    )
    .with_client_id("tab")
    .open_driven()
    .expect("the mirror opens");
    assert!(matches!(conn.storage_use(), Err(ClientError::Session(_))));
}
