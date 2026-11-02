//! What the peer proofs share: a policy replica each device runs a content
//! client over, minted devices on loopback nodes, and a media port over a
//! bare node.

use connetto_client::SyncSchema;
use connetto_client::live::ConnettoClient;
use connetto_client::media::{MediaEvent, MediaPort};
use connetto_core::device_cert::AttestationLevel;
use connetto_file_client::{ContentClient, FsStore, MimeClass};
use connetto_file_core::FileId;
use connetto_peer::test_support::{DAY, Deployment, Issuer, Peer, node, serve, whole_second};
use connetto_peer::{Node, PeerEvent};
use diesel::prelude::*;
use tokio::sync::mpsc;

use crate::support::{RecordingHttp, Scripted, attach_content, cold_over, started};

/// A photo table each person reads only their own rows of, which the
/// device, signed in as the suite's tester, curates whole.
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
    /// The device's photos.
    photos (id) {
        /// Primary key.
        id -> BigInt,
        /// The row's owner.
        owner -> Text,
        /// The file the row names.
        content_id -> Binary,
    }
}

/// A device's content client.
pub type Device = ContentClient<Scripted, FsStore, RecordingHttp>;

/// One minted deployment and the devices the proofs link.
pub struct Devices {
    deployment: Deployment,
    issuer: Issuer,
}

impl Devices {
    pub fn new() -> Self {
        let deployment = Deployment::new(79, whole_second());
        let issuer = deployment.add_issuer(whole_second(), [1; 16]);
        Self { deployment, issuer }
    }

    pub fn device(&self, account: &str, serial: u8) -> Peer {
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
    pub fn node(
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

/// A media port over a bare node, fed the link events and media frames the
/// enrolment task would forward.
pub fn port(node: Node, mut events: mpsc::UnboundedReceiver<PeerEvent>) -> MediaPort {
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

/// A replica over the photo policies, with `transport` attached when one is
/// given, and its content client.
pub async fn replica(
    dir: &std::path::Path,
    transport: Option<Scripted>,
) -> (ConnettoClient<Scripted>, Device) {
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            PHOTO_SCHEMA,
            PHOTO_POLICIES,
        )
        .expect("translate the photo schema"),
    );
    let mut conn = cold_over::<Scripted>(&dir.join("replica.sqlite"), schema).await;
    if let Some(transport) = transport {
        conn.attach(transport).await.expect("attach the transport");
    }
    let client = started(conn);
    let content = attach_content(
        client.clone(),
        &dir.join("chunks"),
        RecordingHttp::default(),
    )
    .await;
    (client, content)
}

/// An offline device declaring `photos.content_id`.
pub async fn declared(dir: &std::path::Path) -> (ConnettoClient<Scripted>, Device) {
    declare(replica(dir, None).await).await
}

/// `device` with `photos.content_id` declared.
pub async fn declare(
    (client, content): (ConnettoClient<Scripted>, Device),
) -> (ConnettoClient<Scripted>, Device) {
    let content = content
        .with_peer_content("photos", "content_id")
        .await
        .expect("declare the content column");
    (client, content)
}

/// Stages `bytes` as photo `id` owned by `owner`.
pub async fn stage(content: &Device, id: i64, owner: &str, bytes: &[u8]) -> FileId {
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

/// Hands photo `id` to `owner`.
pub async fn hand_over(client: &ConnettoClient<Scripted>, id: i64, owner: &str) {
    let now = client
        .with_conn(|conn| {
            diesel::update(photos::table.filter(photos::id.eq(id)))
                .set(photos::owner.eq(owner))
                .execute(conn.conn())?;
            photos::table
                .filter(photos::id.eq(id))
                .select(photos::owner)
                .first::<String>(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("hand the photo over");
    assert_eq!(now, owner);
}

/// Runs `proof` beside `device`'s media driver.
pub async fn beside(device: &Device, port: MediaPort, proof: impl Future<Output = ()>) {
    tokio::select! {
        () = device.drive_media(port) => panic!("the port ended under the proof"),
        () = proof => {}
    }
}

/// Runs `proof` beside the media drivers of two devices.
pub async fn beside_both(
    (first, first_port): (&Device, MediaPort),
    (second, second_port): (&Device, MediaPort),
    proof: impl Future<Output = ()>,
) {
    tokio::select! {
        () = first.drive_media(first_port) => panic!("the first port ended under the proof"),
        () = second.drive_media(second_port) => panic!("the second port ended under the proof"),
        () = proof => {}
    }
}
