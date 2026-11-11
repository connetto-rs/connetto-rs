//! Custody of a replicated column's files on a device other than their
//! author: an on-demand copy entering custody within the budget, kept after
//! its row settles until the state column reads `available`, released on a
//! retraction or once its author lapsed, returning on `lost`, and the
//! reports naming it only to peers the gate admits (R79 decisions 12, 13, 15
//! and 16).

use std::time::Duration;

use connetto_client::live::ConnettoClient;
use connetto_client::peer::PeerAuthor;
use connetto_core::exchange::{RetractCause, Retraction};
use connetto_exchange::media::Media;
use connetto_file_client::{ContentEvent, FileId, Resolved};
use tokio::sync::mpsc;

use crate::peer_support::{
    Device, Devices, apply_entry, beside, beside_both, captured_photo, declare_replicated, lapse,
    listener, port, replica, set_state, stage,
};
use crate::retention::pseudorandom;
use crate::support::Scripted;

/// How long a proof waits for a report or an event before failing.
const BOUND: Duration = Duration::from_secs(10);

/// The author whose entry the receiver applies.
fn anna() -> PeerAuthor {
    PeerAuthor {
        key_id: vec![7; 32],
        account: "anna".to_owned(),
        bot_list: Some("*".to_owned()),
    }
}

/// Whether `device` answers `file_id` from its own store, with no driver
/// running and no server.
async fn held(device: &Device, file_id: FileId) -> bool {
    matches!(
        device.resolve(file_id).await.expect("resolve"),
        Resolved::Local { .. }
    )
}

/// Settles anna's entry 1 on `client`.
async fn settle(client: &ConnettoClient<Scripted>) {
    client
        .with_conn(|conn| conn.adjudicate_peer_frontier(&anna().key_id, 1))
        .await
        .expect("the replica is open")
        .expect("adjudicate anna's frontier");
}

/// Retracts anna's entry 1 on `client`.
async fn retract(client: &ConnettoClient<Scripted>) {
    client
        .with_conn(|conn| {
            conn.retract_peer_entry(
                &anna(),
                &Retraction {
                    seq: 1,
                    cause: RetractCause::Abandoned,
                },
            )
        })
        .await
        .expect("the replica is open")
        .expect("retract anna's photo");
}

/// The next custody report `inbox` receives, room reports skipped since
/// their timing follows a clock.
async fn next(inbox: &mut mpsc::UnboundedReceiver<Media>) -> Media {
    loop {
        let message = tokio::time::timeout(BOUND, inbox.recv())
            .await
            .expect("a report arrives")
            .expect("the listener runs");
        if !matches!(message, Media::Room { .. }) {
            return message;
        }
    }
}

/// A receiver that applied anna's provisional photo row, row 7 naming a file
/// a linked holder serves, and fetched the file on demand.
struct Camp {
    _dirs: [tempfile::TempDir; 3],
    client: ConnettoClient<Scripted>,
    receiver: Device,
    file_id: FileId,
}

impl Camp {
    /// The camp with `budget` bytes for custody on the receiver.
    async fn new(budget: u64) -> Self {
        let dirs = [
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
        ];
        let (_, holder) = declare_replicated(replica(dirs[0].path(), None).await).await;
        let file_id = stage(&holder, 1, "anna", &pseudorandom(61, 300_000)).await;
        let (client, receiver) = replica(dirs[1].path(), None).await;
        let receiver = receiver.with_custody_budget(budget).with_custody_reserve(0);
        let (client, receiver) = declare_replicated((client, receiver)).await;
        apply_entry(
            &client,
            &anna(),
            1,
            captured_photo(dirs[2].path(), 7, file_id.as_bytes().to_vec(), None).await,
        )
        .await;

        let devices = Devices::new();
        let (holder_node, holder_addr, holder_events) = devices.node(&devices.device("r77", 2));
        let (receiver_node, _, receiver_events) = devices.node(&devices.device("anna", 3));
        receiver_node
            .link(holder_addr)
            .await
            .expect("the link completes");
        beside_both(
            (&holder, port(holder_node, holder_events)),
            (&receiver, port(receiver_node, receiver_events)),
            async {
                assert!(
                    matches!(
                        receiver.resolve(file_id).await.expect("resolve"),
                        Resolved::Local { .. }
                    ),
                    "the receiver fetches the file from the holder"
                );
            },
        )
        .await;
        Self {
            _dirs: dirs,
            client,
            receiver,
            file_id,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_in_custody_outlives_its_settled_row_until_the_state_reads_available() {
    let camp = Camp::new(64 * 1024 * 1024).await;
    settle(&camp.client).await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 0);
    assert!(
        held(&camp.receiver, camp.file_id).await,
        "the server has not acknowledged the file, so custody keeps it"
    );

    set_state(&camp.client, 7, "pending").await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 0);

    set_state(&camp.client, 7, "available").await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
    assert!(
        !held(&camp.receiver, camp.file_id).await,
        "an available file is ordinary cache, which the tidy removes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_over_the_budget_stays_cache() {
    let camp = Camp::new(1000).await;
    settle(&camp.client).await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
    assert!(
        !held(&camp.receiver, camp.file_id).await,
        "a copy that did not fit never entered custody"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retracted_row_takes_its_custody_copy_with_it() {
    let camp = Camp::new(64 * 1024 * 1024).await;
    retract(&camp.client).await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
    assert!(!held(&camp.receiver, camp.file_id).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_whose_author_lapsed_goes_while_its_row_is_still_provisional() {
    let camp = Camp::new(64 * 1024 * 1024).await;
    lapse(&camp.client, &anna()).await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
    assert!(
        !held(&camp.receiver, camp.file_id).await,
        "no courier can carry the row any more, so neither custody nor the spare keeps it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cache_copy_of_a_lost_file_returns_to_custody() {
    let camp = Camp::new(1000).await;
    settle(&camp.client).await;
    set_state(&camp.client, 7, "lost").await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 0);
    assert!(
        held(&camp.receiver, camp.file_id).await,
        "the server lacks the file, so the cache copy is kept for its re-upload"
    );

    set_state(&camp.client, 7, "available").await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_report_names_only_the_held_files_the_peer_may_read() {
    let camp = Camp::new(64 * 1024 * 1024).await;
    let devices = Devices::new();
    let (receiver_node, receiver_addr, receiver_events) = devices.node(&devices.device("r77", 3));
    let (reader, _, reader_events) = devices.node(&devices.device("anna", 4));
    let (stranger, _, stranger_events) = devices.node(&devices.device("carl", 5));
    let mut reader_inbox = listener(reader_events);
    let mut stranger_inbox = listener(stranger_events);
    beside(
        &camp.receiver,
        port(receiver_node, receiver_events),
        async {
            reader
                .link(receiver_addr)
                .await
                .expect("the link completes");
            stranger
                .link(receiver_addr)
                .await
                .expect("the link completes");
            let Media::Holdings {
                held,
                leaving,
                last,
                ..
            } = next(&mut reader_inbox).await
            else {
                panic!("a report opens the link");
            };
            assert_eq!(held, [*camp.file_id.as_bytes()]);
            assert!(leaving.is_empty() && last);
            let Media::Holdings { held, .. } = next(&mut stranger_inbox).await else {
                panic!("a report opens the link");
            };
            assert!(held.is_empty(), "carl reads none of anna's rows");

            settle(&camp.client).await;
            set_state(&camp.client, 7, "available").await;
            camp.receiver.tidy_content().await.expect("tidy");
            assert_eq!(
                next(&mut reader_inbox).await,
                Media::Held {
                    added: Vec::new(),
                    removed: vec![*camp.file_id.as_bytes()],
                    leaving: Vec::new(),
                }
            );
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_author_hears_how_many_devices_hold_its_file() {
    let dirs = [
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    ];
    let (_, author) = declare_replicated(replica(dirs[0].path(), None).await).await;
    let file_id = stage(&author, 1, "anna", &pseudorandom(62, 300_000)).await;
    let (client, receiver) = replica(dirs[1].path(), None).await;
    let receiver = receiver.with_custody_reserve(0);
    let (client, receiver) = declare_replicated((client, receiver)).await;
    apply_entry(
        &client,
        &anna(),
        1,
        captured_photo(dirs[2].path(), 7, file_id.as_bytes().to_vec(), None).await,
    )
    .await;

    let devices = Devices::new();
    let (author_node, author_addr, author_events) = devices.node(&devices.device("anna", 2));
    let (receiver_node, _, receiver_events) = devices.node(&devices.device("anna", 3));
    let mut heard = author.events();
    receiver_node
        .link(author_addr)
        .await
        .expect("the link completes");
    beside_both(
        (&author, port(author_node, author_events)),
        (&receiver, port(receiver_node, receiver_events)),
        async {
            assert!(matches!(
                receiver.resolve(file_id).await.expect("resolve"),
                Resolved::Local { .. }
            ));
            tokio::time::timeout(BOUND, async {
                loop {
                    if let ContentEvent::Replicated {
                        file_id: heard_of,
                        holders,
                    } = heard.recv().await.expect("the author's events run")
                    {
                        assert_eq!(heard_of, file_id);
                        assert_eq!(holders, 1);
                        return;
                    }
                }
            })
            .await
            .expect("the author hears the receiver's custody");
        },
    )
    .await;
}
