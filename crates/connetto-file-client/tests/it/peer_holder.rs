//! A content client serving a bare peer node on loopback: the manifest and
//! the verified ranges to a requester its policy views admit, and the same
//! refusal to everyone else (R79 decisions 1, 2 and 4).

use std::time::Duration;

use connetto_client::SyncSchema;
use connetto_client::live::ConnettoClient;
use connetto_client::media::{MediaEvent, MediaPort};
use connetto_core::device_cert::{AttestationLevel, DeviceIdentity};
use connetto_exchange::media::Media;
use connetto_file_client::{ContentClient, ContentError, FsStore, MimeClass};
use connetto_file_core::{FileId, decode_range};
use connetto_peer::test_support::{DAY, Deployment, Issuer, Peer, node, serve, whole_second};
use connetto_peer::{Node, PeerEvent};
use diesel::prelude::*;
use tokio::sync::mpsc;

use crate::retention::pseudorandom;
use crate::support::{RecordingHttp, Scripted, attach_content, cold_over, started};

/// A photo table each person reads only their own rows of, which the
/// holder, signed in as the suite's tester, curates whole.
const PHOTO_SCHEMA: &str = "-- connetto:major 1\n \
                            CREATE TABLE photos (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                            content_id BYTEA NOT NULL); \
                            ALTER TABLE photos ENABLE ROW LEVEL SECURITY;";

const PHOTO_POLICIES: &str = "CREATE POLICY photos_read ON photos FOR SELECT \
                              USING (owner = current_setting('app.user_id', true)); \
                              CREATE POLICY photos_curate ON photos FOR SELECT \
                              USING (current_setting('app.user_id', true) = 'tester'); \
                              CREATE POLICY photos_insert ON photos FOR INSERT WITH CHECK (true); \
                              CREATE POLICY photos_update ON photos FOR UPDATE USING (true) \
                              WITH CHECK (true);";

diesel::table! {
    /// The holder's photos.
    photos (id) {
        /// Primary key.
        id -> BigInt,
        /// The row's owner.
        owner -> Text,
        /// The file the row names.
        content_id -> Binary,
    }
}

type Holder = ContentClient<Scripted, FsStore, RecordingHttp>;

/// One minted deployment and the devices the proofs link.
struct Devices {
    deployment: Deployment,
    issuer: Issuer,
}

impl Devices {
    fn new() -> Self {
        let deployment = Deployment::new(79, whole_second());
        let issuer = deployment.add_issuer(whole_second(), [1; 16]);
        Self { deployment, issuer }
    }

    fn device(&self, account: &str, serial: u8) -> Peer {
        self.deployment.device(
            &self.issuer,
            account,
            whole_second(),
            DAY,
            [serial; 16],
            AttestationLevel::Unproven,
        )
    }

    /// A node serving `peer` on loopback, with its events.
    fn node(
        &self,
        peer: &Peer,
    ) -> (
        Node,
        std::net::SocketAddr,
        mpsc::UnboundedReceiver<PeerEvent>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let node = node(self.deployment.root_der(), tx);
        let addr = serve(&node, peer);
        (node, addr, rx)
    }
}

/// The holder's media port over its bare node, fed the link events and
/// media frames the enrolment task would forward.
fn port(node: Node, mut events: mpsc::UnboundedReceiver<PeerEvent>) -> MediaPort {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            let forwarded = match event {
                PeerEvent::Linked { peer, bot_list, .. } => MediaEvent::Linked { peer, bot_list },
                PeerEvent::Unlinked { peer, .. } => MediaEvent::Unlinked { peer },
                PeerEvent::Media { peer, message } => MediaEvent::Message { peer, message },
                _ => continue,
            };
            if tx.send(forwarded).is_err() {
                return;
            }
        }
    });
    MediaPort::over(node, rx)
}

/// A holder replica over the photo policies, and its content client.
async fn holder(dir: &std::path::Path) -> (ConnettoClient<Scripted>, Holder) {
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            PHOTO_SCHEMA,
            PHOTO_POLICIES,
        )
        .expect("translate the photo schema"),
    );
    let client = started(cold_over::<Scripted>(&dir.join("replica.sqlite"), schema).await);
    let content = attach_content(
        client.clone(),
        &dir.join("chunks"),
        RecordingHttp::default(),
    )
    .await;
    (client, content)
}

/// A holder declaring `photos.content_id`.
async fn declared(dir: &std::path::Path) -> (ConnettoClient<Scripted>, Holder) {
    let (client, content) = holder(dir).await;
    let content = content
        .with_peer_content("photos", "content_id")
        .await
        .expect("declare the content column");
    (client, content)
}

/// Stages `bytes` as photo `id` owned by `owner`.
async fn stage(content: &Holder, id: i64, owner: &str, bytes: &[u8]) -> FileId {
    let owner = owner.to_owned();
    let (file_id, ()) = content
        .stage(bytes, MimeClass::Generic, move |conn, file_id| {
            diesel::insert_into(photos::table)
                .values((
                    photos::id.eq(id),
                    photos::owner.eq(owner),
                    photos::content_id.eq(file_id.as_bytes().to_vec()),
                ))
                .execute(conn)
                .map(|_| ())
        })
        .await
        .expect("stage the photo");
    file_id
}

/// The replies to `request` from `holder`, up to the last one, bounded.
async fn ask(
    requester: &Node,
    events: &mut mpsc::UnboundedReceiver<PeerEvent>,
    holder: &DeviceIdentity,
    request: Media,
) -> Vec<Media> {
    requester
        .send_media(holder, request)
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

/// Runs `proof` beside the holder's media driver.
async fn beside(content: &Holder, port: MediaPort, proof: impl Future<Output = ()>) {
    tokio::select! {
        () = content.drive_media(port) => panic!("the port ended under the proof"),
        () = proof => {}
    }
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
            let request_id = request.request_id();
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

        let owner = client
            .with_conn(|conn| {
                diesel::update(photos::table.filter(photos::id.eq(1)))
                    .set(photos::owner.eq("carl"))
                    .execute(conn.conn())?;
                photos::table
                    .filter(photos::id.eq(1))
                    .select(photos::owner)
                    .first::<String>(conn.conn())
            })
            .await
            .expect("the replica is open")
            .expect("hand the photo to carl");
        assert_eq!(owner, "carl");
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
async fn a_holder_with_no_declared_column_refuses_every_request() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, content) = holder(dir.path()).await;
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
    for (table, column) in [("photos", "thumb_id"), ("albums", "content_id")] {
        let dir = tempfile::tempdir().expect("temp dir");
        let refused = holder(dir.path())
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
