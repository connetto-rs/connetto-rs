//! A content client serving a bare peer node on loopback: the manifest and
//! the verified ranges to a requester its policy views admit, and the same
//! refusal to everyone else (R79 decisions 1, 2 and 4).

use std::time::Duration;

use connetto_core::device_cert::DeviceIdentity;
use connetto_exchange::media::Media;
use connetto_file_client::ContentError;
use connetto_file_core::decode_range;
use connetto_peer::{Node, PeerEvent};
use tokio::sync::mpsc;

use crate::peer_support::{Devices, beside, declared, hand_over, name_file, port, replica, stage};
use crate::retention::pseudorandom;

/// The replies to `request` from `holder`, up to the last one, bounded, the
/// holder's custody reports left out.
async fn ask(
    requester: &Node,
    events: &mut mpsc::UnboundedReceiver<PeerEvent>,
    holder: &DeviceIdentity,
    request: Media,
) -> Vec<Media> {
    requester
        .send_media(
            holder,
            crate::peer_support::link_to(requester, holder),
            request,
        )
        .await
        .expect("the request queues");
    let mut replies = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("a reply within the bound")
            .expect("the requester lives");
        let PeerEvent::Media { message, .. } = event else {
            continue;
        };
        if message.request_id().is_none() {
            continue;
        }
        let done = match &message {
            Media::Manifest { last, .. } | Media::Piece { last, .. } => *last,
            _ => true,
        };
        replies.push(message);
        if done {
            return replies;
        }
    }
}

/// The manifest rows the replies carry, in order.
fn manifest_rows(replies: &[Media]) -> Vec<([u8; 32], u64)> {
    replies
        .iter()
        .flat_map(|reply| match reply {
            Media::Manifest { chunks, .. } => chunks.clone(),
            other => panic!("a manifest run, not {other:?}"),
        })
        .collect()
}

/// The encoded range the pieces carry, each piece within the wire's bound.
fn encoded(replies: &[Media]) -> Vec<u8> {
    replies
        .iter()
        .flat_map(|reply| match reply {
            Media::Piece { encoded, .. } => {
                assert!(encoded.len() <= connetto_exchange::media::MAX_PIECE_BYTES);
                encoded.clone()
            }
            other => panic!("a piece, not {other:?}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_admitted_requester_gets_the_manifest_and_a_range_that_verifies() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, content) = declared(dir.path()).await;
    let bytes = pseudorandom(79, 7 * 1024 * 1024 + 300);
    let file_id = stage(&content, 1, "anna", &bytes).await;

    let devices = Devices::new();
    let (holder_device, anna_device) = (devices.device("r77", 2), devices.device("anna", 3));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (anna, _, mut anna_events) = devices.node(&anna_device);
    let port = port(holder_node, holder_events);

    beside(&content, port, async {
        let holder = anna.link(holder_addr).await.expect("the link completes");
        let manifest = manifest_rows(
            &ask(
                &anna,
                &mut anna_events,
                &holder,
                Media::Want {
                    request_id: 1,
                    file_id: *file_id.as_bytes(),
                },
            )
            .await,
        );
        assert!(manifest.len() > 1, "the file spans several chunks");
        assert_eq!(
            manifest.iter().map(|(_, len)| len).sum::<u64>(),
            u64::try_from(bytes.len()).expect("the size fits")
        );

        // A range across the first chunk boundary, longer than one piece.
        let (start, len) = (manifest[0].1 - 1_000, 2_000_000);
        let pieces = ask(
            &anna,
            &mut anna_events,
            &holder,
            Media::Fetch {
                request_id: 2,
                file_id: *file_id.as_bytes(),
                start,
                len,
            },
        )
        .await;
        assert!(pieces.len() > 1, "the range crosses as several pieces");
        let decoded = decode_range(
            file_id,
            u64::try_from(bytes.len()).expect("the size fits"),
            start,
            len,
            encoded(&pieces).as_slice(),
        )
        .await
        .expect("every piece verifies against the file id");
        let from = usize::try_from(start).expect("the offset fits");
        assert_eq!(decoded, bytes[from..from + 2_000_000]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_requester_and_an_absent_file_get_the_same_answer() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, content) = declared(dir.path()).await;
    let annas = stage(&content, 1, "anna", &pseudorandom(1, 40_000)).await;

    let devices = Devices::new();
    let holder_device = devices.device("r77", 2);
    let (anna_device, carl_device) = (devices.device("anna", 3), devices.device("carl", 4));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (anna, _, mut anna_events) = devices.node(&anna_device);
    let (carl, _, mut carl_events) = devices.node(&carl_device);
    let port = port(holder_node, holder_events);

    beside(&content, port, async {
        let holder = carl.link(holder_addr).await.expect("carl links");
        for request in [
            Media::Want {
                request_id: 1,
                file_id: *annas.as_bytes(),
            },
            Media::Fetch {
                request_id: 2,
                file_id: *annas.as_bytes(),
                start: 0,
                len: 100,
            },
        ] {
            let request_id = request.request_id().expect("a request carries its id");
            assert_eq!(
                ask(&carl, &mut carl_events, &holder, request).await,
                vec![Media::Refused { request_id }],
                "a requester who reads no row naming the file is refused"
            );
        }

        let holder = anna.link(holder_addr).await.expect("anna links");
        assert_eq!(
            ask(
                &anna,
                &mut anna_events,
                &holder,
                Media::Want {
                    request_id: 3,
                    file_id: [7; 32],
                },
            )
            .await,
            vec![Media::Refused { request_id: 3 }],
            "a file no row names is refused like a forbidden one"
        );
        assert_eq!(
            ask(
                &anna,
                &mut anna_events,
                &holder,
                Media::Fetch {
                    request_id: 4,
                    file_id: *annas.as_bytes(),
                    start: 39_990,
                    len: 100,
                },
            )
            .await,
            vec![Media::Refused { request_id: 4 }],
            "a range past the end of the file is refused"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_right_withdrawn_after_the_manifest_refuses_the_next_fetch() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, content) = declared(dir.path()).await;
    let annas = stage(&content, 1, "anna", &pseudorandom(2, 40_000)).await;

    let devices = Devices::new();
    let (holder_device, anna_device) = (devices.device("r77", 2), devices.device("anna", 3));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (anna, _, mut anna_events) = devices.node(&anna_device);
    let port = port(holder_node, holder_events);

    beside(&content, port, async {
        let holder = anna.link(holder_addr).await.expect("the link completes");
        let replies = ask(
            &anna,
            &mut anna_events,
            &holder,
            Media::Want {
                request_id: 1,
                file_id: *annas.as_bytes(),
            },
        )
        .await;
        assert_eq!(manifest_rows(&replies).len(), 1);

        hand_over(&client, 1, "carl").await;
        assert_eq!(
            ask(
                &anna,
                &mut anna_events,
                &holder,
                Media::Fetch {
                    request_id: 2,
                    file_id: *annas.as_bytes(),
                    start: 0,
                    len: 40_000,
                },
            )
            .await,
            vec![Media::Refused { request_id: 2 }],
            "the gate runs again at every fetch"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_whose_link_falls_stops_and_the_holder_serves_on() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, content) = declared(dir.path()).await;
    let bytes = pseudorandom(4, 7 * 1024 * 1024);
    let file_id = stage(&content, 1, "anna", &bytes).await;

    let devices = Devices::new();
    let holder_device = devices.device("r77", 2);
    let (leaving_device, staying_device) = (devices.device("anna", 3), devices.device("anna", 4));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (leaving, _, mut leaving_events) = devices.node(&leaving_device);
    let (staying, _, mut staying_events) = devices.node(&staying_device);
    let port = port(holder_node, holder_events);

    beside(&content, port, async {
        let holder = leaving.link(holder_addr).await.expect("the link completes");
        leaving
            .send_media(
                &holder,
                crate::peer_support::link_to(&leaving, &holder),
                Media::Fetch {
                    request_id: 1,
                    file_id: *file_id.as_bytes(),
                    start: 0,
                    len: 6 * 1024 * 1024,
                },
            )
            .await
            .expect("the request queues");
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), leaving_events.recv())
                .await
                .expect("a piece within the bound")
                .expect("the requester lives");
            if matches!(event, PeerEvent::Media { ref message, .. } if message.request_id().is_some()) {
                break;
            }
        }
        leaving.stop(connetto_peer::CloseReason::Closed);

        let holder = staying.link(holder_addr).await.expect("the link completes");
        let replies = ask(
            &staying,
            &mut staying_events,
            &holder,
            Media::Want {
                request_id: 2,
                file_id: *file_id.as_bytes(),
            },
        )
        .await;
        assert!(
            matches!(replies.last(), Some(Media::Manifest { last: true, .. })),
            "the holder serves the next requester, got {replies:?}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_a_readable_row_names_but_the_holder_lacks_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, content) = declared(dir.path()).await;
    name_file(&client, 1, "anna", [5; 32]).await;

    let devices = Devices::new();
    let (holder_device, anna_device) = (devices.device("r77", 2), devices.device("anna", 3));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (anna, _, mut anna_events) = devices.node(&anna_device);
    let port = port(holder_node, holder_events);

    beside(&content, port, async {
        let holder = anna.link(holder_addr).await.expect("the link completes");
        for request in [
            Media::Want {
                request_id: 1,
                file_id: [5; 32],
            },
            Media::Fetch {
                request_id: 2,
                file_id: [5; 32],
                start: 0,
                len: 100,
            },
        ] {
            let request_id = request.request_id().expect("a request carries its id");
            assert_eq!(
                ask(&anna, &mut anna_events, &holder, request).await,
                vec![Media::Refused { request_id }],
                "the gate admits anna, and the holder has no bytes to give"
            );
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_with_no_declared_column_refuses_every_request() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, content) = replica(dir.path(), None).await;
    let annas = stage(&content, 1, "anna", &pseudorandom(3, 40_000)).await;

    let devices = Devices::new();
    let (holder_device, anna_device) = (devices.device("r77", 2), devices.device("anna", 3));
    let (holder_node, holder_addr, holder_events) = devices.node(&holder_device);
    let (anna, _, mut anna_events) = devices.node(&anna_device);
    let port = port(holder_node, holder_events);

    beside(&content, port, async {
        let holder = anna.link(holder_addr).await.expect("the link completes");
        assert_eq!(
            ask(
                &anna,
                &mut anna_events,
                &holder,
                Media::Want {
                    request_id: 1,
                    file_id: *annas.as_bytes(),
                },
            )
            .await,
            vec![Media::Refused { request_id: 1 }]
        );
    })
    .await;
}

#[tokio::test]
async fn a_declaration_naming_a_column_the_replica_lacks_is_refused() {
    for (table, column) in [("photos", "banner_id"), ("albums", "content_id")] {
        let dir = tempfile::tempdir().expect("temp dir");
        let refused = replica(dir.path(), None)
            .await
            .1
            .with_peer_content(table, column)
            .await
            .err();
        assert!(
            matches!(
                &refused,
                Some(ContentError::PeerContentColumnMissing { table: t, column: c })
                    if t == table && c == column
            ),
            "{refused:?}"
        );
    }
}
