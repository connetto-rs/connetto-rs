//! A content client fetching a file from linked holders on loopback: every
//! piece verified against the file id, a forged piece and a lying manifest
//! refused, and a fetch resumed at the next chunk after a lost link (R79
//! decisions 3, 4, 5 and 10).

use std::sync::Arc;
use std::time::Duration;

use connetto_exchange::media::{MAX_PIECE_BYTES, Media};
use connetto_file_client::{ContentEvent, FileId, Resolved};
use connetto_file_core::{MemStore, MimeClass, encode_range, process_file_with_outboard};
use connetto_peer::{CloseReason, Node, PeerEvent};
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::peer_support::{
    Device, Devices, beside, beside_both, declare, declared, port, replica, stage,
};
use crate::retention::pseudorandom;
use crate::support::Scripted;

/// How a bare holder misbehaves.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// It serves the file as it is.
    None,
    /// It flips a byte in the middle of every encoded range.
    ForgePiece,
    /// It names every chunk by a hash the chunk does not have.
    LieInManifest,
    /// It closes every link at its second `Fetch`.
    DropAtSecondFetch,
}

/// A bare node serving `bytes` as a holder would, chunked as the generic
/// class chunks it, with `fault`, answering the requests it records.
fn bare_holder(
    node: Node,
    mut events: mpsc::UnboundedReceiver<PeerEvent>,
    bytes: Vec<u8>,
    fault: Fault,
) -> Arc<Mutex<Vec<Media>>> {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&asked);
    tokio::spawn(async move {
        let store = MemStore::new();
        let (manifest, outboard) =
            process_file_with_outboard(bytes.as_slice(), MimeClass::Generic, &store)
                .await
                .expect("chunk the file");
        let mut fetches = 0;
        while let Some(event) = events.recv().await {
            let PeerEvent::Media { peer, message } = event else {
                continue;
            };
            record.lock().push(message.clone());
            match message {
                Media::Want { request_id, .. } => {
                    let chunks = manifest
                        .chunks()
                        .iter()
                        .map(|chunk| {
                            let mut hash = *chunk.hash.as_bytes();
                            if fault == Fault::LieInManifest {
                                hash[0] ^= 1;
                            }
                            (hash, chunk.len)
                        })
                        .collect();
                    let run = Media::Manifest {
                        request_id,
                        chunks,
                        last: true,
                    };
                    let _ = node.send_media(&peer, run).await;
                }
                Media::Fetch {
                    request_id,
                    start,
                    len,
                    ..
                } => {
                    fetches += 1;
                    if fault == Fault::DropAtSecondFetch && fetches == 2 {
                        node.stop(CloseReason::Closed);
                        return;
                    }
                    let mut encoded = Vec::new();
                    encode_range(&manifest, &store, &outboard, start, len, &mut encoded)
                        .await
                        .expect("encode the range");
                    if fault == Fault::ForgePiece {
                        let middle = encoded.len() / 2;
                        encoded[middle] ^= 1;
                    }
                    let pieces: Vec<&[u8]> = encoded.chunks(MAX_PIECE_BYTES).collect();
                    let last = pieces.len() - 1;
                    for (index, piece) in pieces.into_iter().enumerate() {
                        let piece = Media::Piece {
                            request_id,
                            encoded: piece.to_vec(),
                            last: index == last,
                        };
                        let _ = node.send_media(&peer, piece).await;
                    }
                }
                _ => {}
            }
        }
    });
    asked
}

/// The `Fetch` requests a bare holder saw, as `(start, len)`.
fn fetches(asked: &Mutex<Vec<Media>>) -> Vec<(u64, u64)> {
    asked
        .lock()
        .iter()
        .filter_map(|message| match message {
            Media::Fetch { start, len, .. } => Some((*start, *len)),
            _ => None,
        })
        .collect()
}

/// Asserts `device` resolves `file_id` to `bytes` from its own store.
fn assert_local(resolved: &Resolved, bytes: &[u8]) {
    match resolved {
        Resolved::Local { bytes: held, .. } => assert!(held == bytes, "the fetched bytes differ"),
        other => panic!("local bytes, not {other:?}"),
    }
}

/// Resolves `file_id` until it answers locally, for a holder whose link the
/// driver learns of on its own time.
async fn resolve_once_linked(device: &Device, file_id: FileId) -> Resolved {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let resolved = device.resolve(file_id).await.expect("resolve");
        if matches!(resolved, Resolved::Local { .. }) || tokio::time::Instant::now() > deadline {
            return resolved;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offline_device_fetches_a_file_from_a_linked_holder_and_holds_it() {
    let (holder_dir, receiver_dir) = (
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    );
    let (_, holder) = declared(holder_dir.path()).await;
    let bytes = pseudorandom(5, 3 * 1024 * 1024 + 77);
    let file_id = stage(&holder, 1, "anna", &bytes).await;
    let (_, receiver) = declared(receiver_dir.path()).await;
    let mut events = receiver.events();

    let devices = Devices::new();
    let (holder_device, anna_device) = (devices.device("r77", 2), devices.device("anna", 3));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (anna_node, _, anna_events) = devices.node(&anna_device);
    anna_node
        .link(holder_addr)
        .await
        .expect("the link completes");
    let (holder_port, anna_port) = (
        port(holder_node, holder_events),
        port(anna_node, anna_events),
    );

    beside_both((&holder, holder_port), (&receiver, anna_port), async {
        let resolved = receiver.resolve(file_id).await.expect("resolve");
        assert_local(&resolved, &bytes);
        assert_eq!(
            events.try_recv().expect("the fetch is announced"),
            ContentEvent::Fetched { file_id }
        );
    })
    .await;

    assert_local(
        &receiver.resolve(file_id).await.expect("resolve again"),
        &bytes,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ticket_the_server_refuses_turns_to_a_linked_holder() {
    let (holder_dir, receiver_dir) = (
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    );
    let (_, holder) = declared(holder_dir.path()).await;
    let bytes = pseudorandom(6, 200_000);
    let file_id = stage(&holder, 1, "anna", &bytes).await;
    let (_, receiver) = declare(replica(receiver_dir.path(), Some(Scripted::new([]))).await).await;

    let devices = Devices::new();
    let (holder_device, anna_device) = (devices.device("r77", 2), devices.device("anna", 3));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (anna_node, _, anna_events) = devices.node(&anna_device);
    anna_node
        .link(holder_addr)
        .await
        .expect("the link completes");
    let (holder_port, anna_port) = (
        port(holder_node, holder_events),
        port(anna_node, anna_events),
    );

    beside_both((&holder, holder_port), (&receiver, anna_port), async {
        assert_local(&receiver.resolve(file_id).await.expect("resolve"), &bytes);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forged_piece_is_refused_and_the_next_holder_serves() {
    let (holder_dir, receiver_dir) = (
        tempfile::tempdir().expect("temp dir"),
        tempfile::tempdir().expect("temp dir"),
    );
    let (_, holder) = declared(holder_dir.path()).await;
    let bytes = pseudorandom(7, 300_000);
    let file_id = stage(&holder, 1, "anna", &bytes).await;
    let (_, receiver) = declared(receiver_dir.path()).await;

    let devices = Devices::new();
    let (holder_device, forger_device, anna_device) = (
        devices.device("r77", 2),
        devices.device("mallory", 4),
        devices.device("anna", 3),
    );
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (forger_node, forger_addr, forger_events) = devices.node(&forger_device);
    let (anna_node, _, anna_events) = devices.node(&anna_device);
    let forger = bare_holder(forger_node, forger_events, bytes.clone(), Fault::ForgePiece);
    anna_node
        .link(forger_addr)
        .await
        .expect("the link completes");
    let (holder_port, anna_port) = (
        port(holder_node, holder_events),
        port(anna_node.clone(), anna_events),
    );

    beside_both((&holder, holder_port), (&receiver, anna_port), async {
        assert_eq!(
            receiver.resolve(file_id).await.expect("resolve"),
            Resolved::Unavailable,
            "a forged piece fails verification and nothing else holds the file"
        );
        assert_eq!(
            fetches(&forger).len(),
            1,
            "the forger was asked for a range"
        );
        assert_eq!(
            receiver
                .outboard(file_id)
                .await
                .expect("read the bookkeeping"),
            None,
            "nothing was committed"
        );

        anna_node
            .link(holder_addr)
            .await
            .expect("the second link completes");
        assert_local(&resolve_once_linked(&receiver, file_id).await, &bytes);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manifest_naming_chunks_the_file_lacks_is_refused() {
    let receiver_dir = tempfile::tempdir().expect("temp dir");
    let (_, receiver) = declared(receiver_dir.path()).await;
    let bytes = pseudorandom(8, 300_000);
    let file_id = crate::support::learn_file_id(&bytes).await;

    let devices = Devices::new();
    let (liar_device, anna_device) = (devices.device("mallory", 4), devices.device("anna", 3));
    let (liar_node, liar_addr, liar_events) = devices.node(&liar_device);
    let (anna_node, _, anna_events) = devices.node(&anna_device);
    let liar = bare_holder(liar_node, liar_events, bytes, Fault::LieInManifest);
    anna_node.link(liar_addr).await.expect("the link completes");

    beside(&receiver, port(anna_node, anna_events), async {
        assert_eq!(
            receiver.resolve(file_id).await.expect("resolve"),
            Resolved::Unavailable
        );
        assert_eq!(
            fetches(&liar).len(),
            1,
            "the lie shows at the first chunk, and no further chunk is asked for"
        );
        assert_eq!(
            receiver
                .outboard(file_id)
                .await
                .expect("read the bookkeeping"),
            None
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fetch_resumes_at_the_next_chunk_after_a_lost_link() {
    let receiver_dir = tempfile::tempdir().expect("temp dir");
    let (_, receiver) = declared(receiver_dir.path()).await;
    let bytes = pseudorandom(9, 4 * 1024 * 1024 + 11);
    let file_id = crate::support::learn_file_id(&bytes).await;

    let devices = Devices::new();
    let (first_device, second_device, anna_device) = (
        devices.device("r77", 2),
        devices.device("r78", 5),
        devices.device("anna", 3),
    );
    let (first_node, first_addr, first_events) = devices.node(&first_device);
    let (second_node, second_addr, second_events) = devices.node(&second_device);
    let (anna_node, _, anna_events) = devices.node(&anna_device);
    let first = bare_holder(
        first_node,
        first_events,
        bytes.clone(),
        Fault::DropAtSecondFetch,
    );
    let second = bare_holder(second_node, second_events, bytes.clone(), Fault::None);
    anna_node
        .link(first_addr)
        .await
        .expect("the link completes");

    beside(&receiver, port(anna_node.clone(), anna_events), async {
        assert_eq!(
            receiver.resolve(file_id).await.expect("resolve"),
            Resolved::Unavailable,
            "the only holder left partway"
        );
        let served = fetches(&first);
        assert_eq!(
            served.len(),
            2,
            "the first chunk crossed and the second was asked"
        );

        anna_node
            .link(second_addr)
            .await
            .expect("the second link completes");
        assert_local(&resolve_once_linked(&receiver, file_id).await, &bytes);
        assert_eq!(
            fetches(&second).first().map(|(start, _)| *start),
            Some(served[0].1),
            "the second holder is asked from the chunk after the one already held"
        );
    })
    .await;
}
