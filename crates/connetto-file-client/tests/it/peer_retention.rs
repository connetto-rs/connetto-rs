//! The retention pass on a device holding a file a peer's provisional row
//! names: spared until the row is settled, removed at once when the row is
//! retracted, caught at the driver's start when the retraction's event went
//! unheard, and no longer spared once the author's certificate lapsed (R79
//! decisions 6, 7 and 15).

use std::time::Duration;

use connetto_client::live::ConnettoClient;
use connetto_client::peer::PeerAuthor;
use connetto_core::exchange::{RetractCause, Retraction};
use connetto_file_client::{FileId, Resolved};

use crate::peer_support::{
    Device, Devices, apply_entry, beside_both, captured_photo, declared, lapse, port, stage,
};
use crate::retention::pseudorandom;
use crate::support::Scripted;

/// The author whose entry the receiver applies.
fn anna() -> PeerAuthor {
    PeerAuthor {
        key_id: vec![7; 32],
        account: "anna".to_owned(),
        bot_list: Some("*".to_owned()),
    }
}

/// Applies `body` as anna's entry `seq` on `client`.
async fn apply(client: &ConnettoClient<Scripted>, seq: u64, body: Vec<u8>) {
    apply_entry(client, &anna(), seq, body).await;
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

/// Whether `device` answers `file_id` from its own store, with no driver
/// running and no server.
async fn held(device: &Device, file_id: FileId) -> bool {
    matches!(
        device.resolve(file_id).await.expect("resolve"),
        Resolved::Local { .. }
    )
}

/// A receiver that applied anna's provisional photo row and fetched its file
/// from a linked holder, as cache.
struct Camp {
    dirs: [tempfile::TempDir; 3],
    client: ConnettoClient<Scripted>,
    receiver: Device,
    file_id: FileId,
}

impl Camp {
    async fn new() -> Self {
        let dirs = [
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
        ];
        let (_, holder) = declared(dirs[0].path()).await;
        let bytes = pseudorandom(60, 300_000);
        let file_id = stage(&holder, 1, "anna", &bytes).await;
        let (client, receiver) = declared(dirs[1].path()).await;
        apply(
            &client,
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
            dirs,
            client,
            receiver,
            file_id,
        }
    }
}

/// A sleeper that never ends a wait, so the outbox driver only ever waits on
/// events.
fn never(_: Duration) -> std::future::Pending<()> {
    std::future::pending()
}

/// Polls until `device` no longer holds `file_id`, bounded.
async fn until_gone(device: &Device, file_id: FileId) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while held(device, file_id).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the file is still held"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_a_provisional_row_names_survives_a_tidy_until_the_row_is_settled() {
    let camp = Camp::new().await;
    camp.receiver.tidy_content().await.expect("tidy");
    assert!(
        held(&camp.receiver, camp.file_id).await,
        "a provisional row names the file, so the tidy spares it"
    );

    camp.client
        .with_conn(|conn| conn.adjudicate_peer_frontier(&anna().key_id, 1))
        .await
        .expect("the replica is open")
        .expect("adjudicate anna's frontier");
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
    assert!(
        !held(&camp.receiver, camp.file_id).await,
        "a settled row's file is ordinary cache, which the tidy removes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_provisional_value_that_is_no_file_id_spares_nothing_and_stops_nothing() {
    let camp = Camp::new().await;
    apply(
        &camp.client,
        2,
        captured_photo(camp.dirs[2].path(), 8, b"short".to_vec(), None).await,
    )
    .await;
    camp.receiver.tidy_content().await.expect("tidy");
    assert!(
        held(&camp.receiver, camp.file_id).await,
        "the first row still names the file"
    );

    camp.client
        .with_conn(|conn| conn.adjudicate_peer_frontier(&anna().key_id, 2))
        .await
        .expect("the replica is open")
        .expect("adjudicate anna's frontier");
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retraction_removes_the_file_at_once() {
    let camp = Camp::new().await;
    tokio::select! {
        () = camp.receiver.drive_outbox(never) => panic!("the outbox driver ended"),
        () = async {
            assert!(
                held(&camp.receiver, camp.file_id).await,
                "the start's pass spares the provisional row's file"
            );
            retract(&camp.client).await;
            until_gone(&camp.receiver, camp.file_id).await;
        } => {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retraction_no_driver_heard_is_caught_when_the_driver_starts() {
    let camp = Camp::new().await;
    retract(&camp.client).await;
    assert!(
        held(&camp.receiver, camp.file_id).await,
        "nothing tidied while no driver ran"
    );
    tokio::select! {
        () = camp.receiver.drive_outbox(never) => panic!("the outbox driver ended"),
        () = until_gone(&camp.receiver, camp.file_id) => {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_whose_author_lapsed_is_spared_no_longer() {
    let camp = Camp::new().await;
    lapse(&camp.client, &anna()).await;
    assert_eq!(camp.receiver.tidy_content().await.expect("tidy"), 1);
    assert!(
        !held(&camp.receiver, camp.file_id).await,
        "no courier can carry the row any more"
    );
}
