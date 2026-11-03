//! Thumbnails fetched unasked: a receiver takes each file a prefetch column
//! names on an author's provisional rows from that author, when the entry
//! applies and when the author's link comes up, and only when the file is
//! at most 1 MiB (R79 decision 8).

use std::time::Duration;

use connetto_client::live::ConnettoClient;
use connetto_client::peer::PeerAuthor;
use connetto_file_client::{ContentEvent, FileId, Resolved};
use connetto_peer::test_support::Peer;
use tokio::sync::broadcast;

use crate::peer_support::{
    Device, Devices, Fault, apply_entry, bare_holder, beside, beside_both, captured_photo,
    declared, fetches, port, replica, stage, wants,
};
use crate::retention::pseudorandom;
use crate::support::{Scripted, learn_file_id};

/// A receiver declaring `photos.content_id`, and `photos.thumb_id` as a
/// thumbnail column.
async fn receiver(dir: &std::path::Path) -> (ConnettoClient<Scripted>, Device) {
    let (client, content) = replica(dir, None).await;
    let content = content
        .with_peer_content("photos", "content_id")
        .await
        .expect("declare the photo column")
        .with_peer_content("photos", "thumb_id")
        .prefetch()
        .await
        .expect("declare the thumbnail column");
    (client, content)
}

/// The author `device` is, under anna's account.
fn author_of(device: &Peer) -> PeerAuthor {
    PeerAuthor {
        key_id: device.key_id().as_bytes().to_vec(),
        account: "anna".to_owned(),
        bot_list: Some("*".to_owned()),
    }
}

/// Applies `author`'s photo 7, its full image `full` and its thumbnail
/// `thumb`, on `client`.
async fn apply_photo(
    client: &ConnettoClient<Scripted>,
    dir: &std::path::Path,
    author: &PeerAuthor,
    full: FileId,
    thumb: FileId,
) {
    let body = captured_photo(
        dir,
        7,
        full.as_bytes().to_vec(),
        Some(thumb.as_bytes().to_vec()),
    )
    .await;
    apply_entry(client, author, 1, body).await;
}

/// Waits for the receiver to announce `file_id` fetched, bounded.
async fn fetched(events: &mut broadcast::Receiver<ContentEvent>, file_id: FileId) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(ContentEvent::Fetched { file_id: got }) = events.recv().await
                && got == file_id
            {
                return;
            }
        }
    })
    .await
    .expect("the thumbnail arrives unasked");
}

/// Whether `device` holds a manifest for `file_id`, read without fetching.
async fn holds(device: &Device, file_id: FileId) -> bool {
    device
        .outboard(file_id)
        .await
        .expect("read the bookkeeping")
        .is_some()
}

/// A holder device of anna's holding a thumbnail, and a receiver of anna's.
struct Camp {
    dirs: [tempfile::TempDir; 3],
    devices: Devices,
    holder: Device,
    holder_device: Peer,
    client: ConnettoClient<Scripted>,
    receiver: Device,
    thumb: FileId,
}

impl Camp {
    async fn new() -> Self {
        let dirs = [
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
        ];
        let (_, holder) = declared(dirs[0].path()).await;
        let thumb = stage(&holder, 1, "anna", &pseudorandom(70, 50_000)).await;
        let (client, receiver) = receiver(dirs[1].path()).await;
        let devices = Devices::new();
        let holder_device = devices.device("anna", 2);
        Self {
            dirs,
            devices,
            holder,
            holder_device,
            client,
            receiver,
            thumb,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_applied_entry_brings_its_thumbnail_and_not_its_full_image() {
    let camp = Camp::new().await;
    let (holder_node, holder_addr, holder_events) = camp.devices.node(&camp.holder_device);
    let (receiver_node, _, receiver_events) = camp.devices.node(&camp.devices.device("anna", 3));
    receiver_node
        .link(holder_addr)
        .await
        .expect("the link completes");
    let mut events = camp.receiver.events();
    let full = FileId::from_bytes([9; 32]);

    Box::pin(beside_both(
        (&camp.holder, port(holder_node, holder_events)),
        (&camp.receiver, port(receiver_node, receiver_events)),
        async {
            // The link-up pass runs first and finds nothing, so the
            // thumbnail can only come from the apply.
            tokio::time::sleep(Duration::from_millis(500)).await;
            apply_photo(
                &camp.client,
                camp.dirs[2].path(),
                &author_of(&camp.holder_device),
                full,
                camp.thumb,
            )
            .await;
            fetched(&mut events, camp.thumb).await;
            assert!(holds(&camp.receiver, camp.thumb).await);
            assert!(
                !holds(&camp.receiver, full).await,
                "only a prefetch column's file comes unasked"
            );
        },
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_applied_before_the_author_linked_brings_its_thumbnail_at_link_up() {
    let camp = Camp::new().await;
    let (holder_node, holder_addr, holder_events) = camp.devices.node(&camp.holder_device);
    let (receiver_node, _, receiver_events) = camp.devices.node(&camp.devices.device("anna", 3));
    let mut events = camp.receiver.events();

    Box::pin(beside_both(
        (&camp.holder, port(holder_node, holder_events)),
        (&camp.receiver, port(receiver_node.clone(), receiver_events)),
        async {
            apply_photo(
                &camp.client,
                camp.dirs[2].path(),
                &author_of(&camp.holder_device),
                FileId::from_bytes([9; 32]),
                camp.thumb,
            )
            .await;
            assert!(
                !holds(&camp.receiver, camp.thumb).await,
                "the author is not linked yet"
            );
            receiver_node
                .link(holder_addr)
                .await
                .expect("the link completes");
            fetched(&mut events, camp.thumb).await;
        },
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thumbnail_above_one_mebibyte_is_left_for_an_on_demand_fetch() {
    let dirs = [
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    ];
    let (client, receiver) = receiver(dirs[0].path()).await;
    let bytes = pseudorandom(71, 2 * 1024 * 1024);
    let thumb = learn_file_id(&bytes).await;
    let devices = Devices::new();
    let author_device = devices.device("anna", 2);
    let (author_node, author_addr, author_events) = devices.node(&author_device);
    let author = bare_holder(author_node, author_events, bytes.clone(), Fault::None);
    let (receiver_node, _, receiver_events) = devices.node(&devices.device("anna", 3));
    receiver_node
        .link(author_addr)
        .await
        .expect("the link completes");

    Box::pin(beside(
        &receiver,
        port(receiver_node, receiver_events),
        async {
            apply_photo(
                &client,
                dirs[1].path(),
                &author_of(&author_device),
                FileId::from_bytes([9; 32]),
                thumb,
            )
            .await;
            while wants(&author) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(
                fetches(&author),
                Vec::new(),
                "the prefetch reads the manifest and asks for no range"
            );
            match receiver.resolve(thumb).await.expect("resolve") {
                Resolved::Local { bytes: held, .. } => assert!(held == bytes),
                other => panic!("an on-demand fetch still brings it, not {other:?}"),
            }
        },
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prefetch_asks_its_author_alone_and_drops_on_busy() {
    let dirs = [
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    ];
    let (client, receiver) = receiver(dirs[0].path()).await;
    let bytes = pseudorandom(72, 50_000);
    let thumb = learn_file_id(&bytes).await;
    let devices = Devices::new();
    let (author_device, other_device) = (devices.device("anna", 2), devices.device("anna", 4));
    let (author_node, author_addr, author_events) = devices.node(&author_device);
    let (other_node, other_addr, other_events) = devices.node(&other_device);
    let author = bare_holder(author_node, author_events, bytes.clone(), Fault::AlwaysBusy);
    let other = bare_holder(other_node, other_events, bytes, Fault::None);
    let (receiver_node, _, receiver_events) = devices.node(&devices.device("anna", 3));
    for addr in [author_addr, other_addr] {
        receiver_node.link(addr).await.expect("the link completes");
    }

    Box::pin(beside(
        &receiver,
        port(receiver_node, receiver_events),
        async {
            apply_photo(
                &client,
                dirs[1].path(),
                &author_of(&author_device),
                FileId::from_bytes([9; 32]),
                thumb,
            )
            .await;
            // Past the first on-demand retry, which comes after 5 s.
            tokio::time::sleep(Duration::from_secs(6)).await;
            assert_eq!(wants(&author), 1, "a busy author is not asked again");
            assert_eq!(wants(&other), 0, "a prefetch asks the author alone");
        },
    ))
    .await;
}
