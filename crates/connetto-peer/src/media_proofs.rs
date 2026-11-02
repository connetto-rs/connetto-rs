//! The media frames on the link, at peer protocol version 3 (R79 slice 2).

use std::time::Duration;

use connetto_core::device_cert::{AttestationLevel, DeviceIdentity};
use connetto_exchange::MAX_MANIFEST_ROWS;
use connetto_exchange::media::{MAX_PIECE_BYTES, Media};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::frame::{PeerFrame, read_frame, write_frame};
use crate::test_support::{DAY, Deployment, events, node, serve, whole_second};
use crate::tests::{await_event, linked_event, next_events, raw_linked};
use crate::{CloseReason, Node, PeerEvent, PeerSendError};

/// One message of each kind, every reply quoting `request_id`.
fn media_messages(request_id: u64) -> [Media; 6] {
    [
        Media::Want {
            request_id,
            file_id: [1; 32],
        },
        Media::Manifest {
            request_id,
            chunks: vec![([2; 32], 6_291_456), ([3; 32], 17)],
            last: true,
        },
        Media::Fetch {
            request_id,
            file_id: [1; 32],
            start: 4096,
            len: 1024,
        },
        Media::Piece {
            request_id,
            encoded: vec![9; 300],
            last: false,
        },
        Media::Refused { request_id },
        Media::Busy { request_id },
    ]
}

#[tokio::test]
async fn every_media_message_round_trips_its_encoding() {
    let messages = media_messages(5);
    let (mut tx, mut rx) = tokio::io::duplex(64 * 1024);
    for message in &messages {
        write_frame(&mut tx, &PeerFrame::Media(message.clone()))
            .await
            .expect("the frame encodes");
    }
    for message in messages {
        let read = read_frame(&mut rx)
            .await
            .expect("the frame decodes")
            .expect("the frame stands");
        assert_eq!(read, PeerFrame::Media(message));
    }
}

#[tokio::test]
async fn a_manifest_at_the_row_bound_and_a_whole_piece_encode_under_the_ceiling() {
    let frames = [
        PeerFrame::Media(Media::Manifest {
            request_id: 1,
            chunks: vec![([0xff; 32], u64::MAX); MAX_MANIFEST_ROWS],
            last: true,
        }),
        PeerFrame::Media(Media::Piece {
            request_id: 2,
            encoded: vec![0xff; MAX_PIECE_BYTES],
            last: true,
        }),
    ];
    for frame in frames {
        let (mut tx, mut rx) = tokio::io::duplex(8 * 1024 * 1024);
        write_frame(&mut tx, &frame)
            .await
            .expect("the bound-sized frame encodes");
        let read = read_frame(&mut rx)
            .await
            .expect("it decodes")
            .expect("it stands");
        assert_eq!(read, frame);
    }
}

/// Send `messages` from `sender` to `to` and see each reach `rx` in send
/// order, naming `from`.
async fn media_reaches_in_order(
    sender: &Node,
    from: &DeviceIdentity,
    to: &DeviceIdentity,
    rx: &mut mpsc::UnboundedReceiver<PeerEvent>,
    messages: [Media; 6],
) {
    for message in &messages {
        sender
            .send_media(to, message.clone())
            .await
            .expect("the queue takes it");
    }
    for message in messages {
        let event = await_event(rx, Duration::from_secs(10), |event| {
            matches!(event, PeerEvent::Media { .. })
        })
        .await;
        assert_eq!(
            event,
            PeerEvent::Media {
                peer: from.clone(),
                message
            }
        );
    }
}

#[tokio::test]
async fn linked_nodes_deliver_each_media_message_in_order_naming_the_sender() {
    let now = whole_second();
    let deployment = Deployment::new(41, now);
    let issuer = deployment.add_issuer(now, [1; 16]);
    let alice = deployment.device(
        &issuer,
        "alice",
        now,
        DAY,
        [1; 16],
        AttestationLevel::Unproven,
    );
    let bob = deployment.device(
        &issuer,
        "bob",
        now,
        DAY,
        [2; 16],
        AttestationLevel::Unproven,
    );

    let (a_tx, mut a_rx) = events();
    let (b_tx, mut b_rx) = events();
    let a = node(deployment.root_der(), a_tx);
    let b = node(deployment.root_der(), b_tx);
    let addr = serve(&b, &bob);
    let _ = serve(&a, &alice);
    let peer = a.link(addr).await.expect("the link completes");
    assert_eq!(peer, bob.identity);
    assert_eq!(next_events(&mut a_rx, 1).await, vec![linked_event(&bob)]);
    assert_eq!(next_events(&mut b_rx, 1).await, vec![linked_event(&alice)]);

    media_reaches_in_order(
        &a,
        &alice.identity,
        &bob.identity,
        &mut b_rx,
        media_messages(7),
    )
    .await;
    media_reaches_in_order(
        &b,
        &bob.identity,
        &alice.identity,
        &mut a_rx,
        media_messages(8),
    )
    .await;

    a.stop(CloseReason::Closed);
    b.stop(CloseReason::Closed);
}

#[tokio::test]
async fn a_media_send_to_an_unlinked_peer_hands_the_message_back() {
    let now = whole_second();
    let deployment = Deployment::new(42, now);
    let issuer = deployment.add_issuer(now, [1; 16]);
    let bob = deployment.device(
        &issuer,
        "bob",
        now,
        DAY,
        [2; 16],
        AttestationLevel::Unproven,
    );
    let (a_tx, _a_rx) = events();
    let a = node(deployment.root_der(), a_tx);
    let message = media_messages(1)[0].clone();
    let err = a
        .send_media(&bob.identity, message.clone())
        .await
        .expect_err("no link stands");
    assert!(
        matches!(err, PeerSendError::Unlinked { message: m } if m == message),
        "the message comes back to its sender"
    );
}

#[tokio::test]
async fn a_media_message_over_the_bounds_is_refused_both_ways() {
    let now = whole_second();
    let deployment = Deployment::new(43, now);
    let issuer = deployment.add_issuer(now, [1; 16]);
    let host = deployment.device(
        &issuer,
        "host",
        now,
        DAY,
        [1; 16],
        AttestationLevel::Unproven,
    );
    let over = deployment.device(
        &issuer,
        "over",
        now,
        DAY,
        [2; 16],
        AttestationLevel::Unproven,
    );

    let (h_tx, mut h_rx) = events();
    let host_node = node(deployment.root_der(), h_tx);
    let host_addr = serve(&host_node, &host);
    let piece = Media::Piece {
        request_id: 3,
        encoded: vec![0; MAX_PIECE_BYTES + 1],
        last: true,
    };

    let err = host_node
        .send_media(&over.identity, piece.clone())
        .await
        .expect_err("the bound holds");
    assert!(matches!(err, PeerSendError::FrameAboveBounds { .. }));

    let mut raw = raw_linked(deployment.root_der(), &over, host_addr).await;
    let body = rmp_serde::to_vec_named(&PeerFrame::Media(piece)).expect("the raw body encodes");
    let len = u32::try_from(body.len()).expect("the body is under the ceiling");
    raw.write_all(&len.to_be_bytes())
        .await
        .expect("the length writes");
    raw.write_all(&body).await.expect("the body writes");
    raw.flush().await.expect("the frame flushes");
    let event = await_event(&mut h_rx, Duration::from_secs(10), |event| {
        matches!(event, PeerEvent::Unlinked { .. })
    })
    .await;
    assert_eq!(
        event,
        PeerEvent::Unlinked {
            peer: over.identity.clone(),
            reason: CloseReason::Protocol
        }
    );
    host_node.stop(CloseReason::Closed);
}
