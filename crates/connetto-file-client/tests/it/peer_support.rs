//! What the peer proofs share: a policy replica each device runs a content
//! client over, minted devices on loopback nodes, and a media port over a
//! bare node.

use std::sync::Arc;

use connetto_client::SyncSchema;
use connetto_client::live::ConnettoClient;
use connetto_client::media::{MediaEvent, MediaPort};
use connetto_client::peer::PeerAuthor;
use connetto_core::clock::DEVICE_SKEW;
use connetto_core::device_cert::{AttestationLevel, DeviceIdentity};
use connetto_core::exchange::ManifestRow;
use connetto_core::messages::{BulkMessage, LivePatch};
use connetto_core::schema::SchemaBundle;
use connetto_core::traits::IncomingFrame;
use connetto_core::{Cursor, Major};
use connetto_exchange::media::{MAX_PIECE_BYTES, Media};
use connetto_file_client::{ContentClient, FsStore, MimeClass};
use connetto_file_core::{FileId, MemStore, encode_range, process_file_with_outboard};
use connetto_peer::test_support::{DAY, Deployment, Issuer, Peer, node, serve, whole_second};
use connetto_peer::{CloseReason, Node, PeerEvent};
use diesel::prelude::*;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use sqlite_diff_rs::{DiffOps, PatchSet, PatchsetFormat, SimpleTable, Update, Value};
use tokio::sync::mpsc;

use crate::support::{RecordingHttp, Scripted, attach_content, cold_over, started};

/// A photo table with a thumbnail column and the state column the server's
/// setter writes, each person reading only their own rows, which the device,
/// signed in as the suite's tester, curates whole.
const PHOTO_SCHEMA: &str = "-- connetto:major 1\n \
                            CREATE TABLE photos (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                            content_id BYTEA NOT NULL, thumb_id BYTEA, content_state TEXT); \
                            ALTER TABLE photos ENABLE ROW LEVEL SECURITY;";

const PHOTO_POLICIES: &str = "CREATE POLICY photos_read ON photos FOR SELECT \
                              USING (owner = current_setting('app.user_id', true)); \
                              CREATE POLICY photos_curate ON photos FOR SELECT \
                              USING (current_setting('app.user_id', true) IN ('tester', 'curator')); \
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
        /// The photo's thumbnail, a file of its own.
        thumb_id -> Nullable<Binary>,
        /// What the server holds of the file, written by its setter.
        content_state -> Nullable<Text>,
    }
}

diesel::table! {
    /// The copies a device holds in custody for another device's rows.
    _connetto_content_custody (file_id) {
        /// The held file.
        file_id -> Binary,
        /// `held`, or `leaving` or `pressed` for a copy marked leaving.
        state -> Text,
    }
}

/// Writes `state` into `file_id`'s custody record on `client`, as placement
/// does when it marks the copy leaving or withdraws the mark.
pub async fn set_custody_state(client: &ConnettoClient<Scripted>, file_id: FileId, state: &str) {
    let state = state.to_owned();
    let written = client
        .with_conn(move |conn| {
            diesel::update(_connetto_content_custody::table)
                .filter(_connetto_content_custody::file_id.eq(file_id.as_bytes().to_vec()))
                .set(_connetto_content_custody::state.eq(state))
                .execute(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("write the custody state");
    assert_eq!(written, 1, "the file is in custody");
}

/// The state of `file_id`'s custody copy on `client`, `None` for none.
pub async fn custody_state(client: &ConnettoClient<Scripted>, file_id: FileId) -> Option<String> {
    client
        .with_conn(move |conn| {
            _connetto_content_custody::table
                .filter(_connetto_content_custody::file_id.eq(file_id.as_bytes().to_vec()))
                .select(_connetto_content_custody::state)
                .first::<String>(conn.conn())
                .optional()
        })
        .await
        .expect("the replica is open")
        .expect("read the custody record")
}

diesel::table! {
    /// The chunk rows of every manifest a device holds.
    _connetto_content_chunks (file_id, ordinal) {
        /// The file.
        file_id -> Binary,
        /// The chunk's place in the file.
        ordinal -> BigInt,
    }
}

/// How many chunk rows of `file_id`'s manifest `client` holds.
pub async fn held_chunks(client: &ConnettoClient<Scripted>, file_id: FileId) -> i64 {
    client
        .with_conn(move |conn| {
            _connetto_content_chunks::table
                .filter(_connetto_content_chunks::file_id.eq(file_id.as_bytes().to_vec()))
                .count()
                .get_result(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("count the chunk rows")
}

diesel::table! {
    /// connetto's record of each peer author's last verified offer, whose
    /// certificate end the custody proofs move into the past.
    _connetto_peer_cursor (author) {
        /// The author's key id.
        author -> Binary,
        /// The author's account.
        account -> Text,
        /// The last verified sequence.
        seq -> BigInt,
        /// The chain hash at it.
        head -> Binary,
        /// The author's verified offer.
        offer -> Binary,
        /// The author's certificate end, in Unix seconds.
        expires_at -> BigInt,
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

/// The number of `node`'s live link to `peer`.
pub fn link_to(node: &Node, peer: &DeviceIdentity) -> u64 {
    node.linked()
        .into_iter()
        .find(|link| &link.peer == peer)
        .and_then(|link| link.link)
        .expect("the peer is linked")
}

/// The media event the enrolment task forwards for `event`, `None` for one
/// it keeps to itself.
fn forwarded(event: PeerEvent) -> Option<MediaEvent> {
    Some(match event {
        PeerEvent::Linked {
            peer,
            link,
            bot_list,
            ..
        }
        | PeerEvent::LinkReplaced {
            peer,
            link,
            bot_list,
            ..
        } => MediaEvent::Linked {
            peer,
            link,
            bot_list,
        },
        PeerEvent::Unlinked { peer, .. } => MediaEvent::Unlinked { peer },
        PeerEvent::Media {
            peer,
            link,
            message,
        } => MediaEvent::Message {
            peer,
            link,
            message,
        },
        _ => return None,
    })
}

/// A media port over a bare node, fed the link events and media frames the
/// enrolment task would forward.
pub fn port(node: Node, events: mpsc::UnboundedReceiver<PeerEvent>) -> MediaPort {
    relay(node, events).port()
}

/// Media ports over one bare node, the latest taken fed what the enrolment
/// task would forward, so a proof can start a device's media driver again
/// on links that stay up.
pub struct Relay {
    node: Node,
    to: Arc<Mutex<Option<mpsc::UnboundedSender<MediaEvent>>>>,
}

impl Relay {
    /// The node the relay feeds from.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// A fresh port, which takes the events from here on.
    pub fn port(&self) -> MediaPort {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.to.lock() = Some(tx);
        MediaPort::over(self.node.clone(), rx)
    }
}

/// A relay over `node` and its `events`.
pub fn relay(node: Node, mut events: mpsc::UnboundedReceiver<PeerEvent>) -> Relay {
    let to: Arc<Mutex<Option<mpsc::UnboundedSender<MediaEvent>>>> = Arc::default();
    let feeding = Arc::clone(&to);
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Some(event) = forwarded(event)
                && let Some(port) = feeding.lock().as_ref()
            {
                let _ = port.send(event);
            }
        }
    });
    Relay { node, to }
}

/// A replica over the photo policies, with `transport` attached when one is
/// given, and its content client.
pub async fn replica(
    dir: &std::path::Path,
    transport: Option<Scripted>,
) -> (ConnettoClient<Scripted>, Device) {
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            connetto_schema::Documents::new(
                PHOTO_SCHEMA,
                PHOTO_POLICIES,
                connetto_test_harness::FIXTURE_ROLES,
                connetto_test_harness::FIXTURE_READER,
            ),
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

/// `device` with `photos.content_id` declared and replicated to three
/// copies, acknowledged by `photos.content_state`.
pub async fn declare_replicated(
    (client, content): (ConnettoClient<Scripted>, Device),
) -> (ConnettoClient<Scripted>, Device) {
    let content = content
        .with_peer_content("photos", "content_id")
        .replicate(3, "content_state")
        .await
        .expect("declare the replicated column");
    (client, content)
}

/// Writes `state` into photo `id`'s state column straight on the replica,
/// for a proof that runs the retention pass itself.
pub async fn set_state(client: &ConnettoClient<Scripted>, id: i64, state: &str) {
    let state = state.to_owned();
    client
        .with_conn(move |conn| {
            diesel::update(photos::table.filter(photos::id.eq(id)))
                .set(photos::content_state.eq(state))
                .execute(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("write the state");
}

/// Delivers the server setter's write of `state` into photo `id`'s state
/// column as a live patch at `cursor`, the way sync carries it.
pub fn deliver_state(server: &Scripted, cursor: u8, id: i64, state: &str) {
    let schema = SimpleTable::new(
        "photos",
        &[
            ("id", ""),
            ("owner", ""),
            ("content_id", ""),
            ("thumb_id", ""),
            ("content_state", ""),
        ],
        &[0],
    );
    let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(schema)
        .set(0, Value::Integer(id))
        .expect("set the key")
        .set(4, Value::Text(state.to_owned()))
        .expect("set the state");
    let patch = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .update(update)
        .build();
    server.say(IncomingFrame::Bulk(BulkMessage::LivePatch(LivePatch::new(
        "custody".to_owned(),
        Cursor::new(vec![cursor]),
        zstd::encode_all(patch.as_slice(), 3).expect("compress the patch"),
    ))));
}

/// Records a cursor for `author` whose certificate end puts its lapse,
/// the end plus `DEVICE_SKEW`, `seconds` from now, negative for the past.
pub async fn lapse_in(client: &ConnettoClient<Scripted>, author: &PeerAuthor, seconds: i64) {
    let author = author.clone();
    client
        .with_conn(move |conn| {
            let now = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("after the epoch")
                    .as_secs(),
            )
            .expect("a Unix time fits");
            let skew = i64::try_from(DEVICE_SKEW.as_secs()).expect("the skew fits");
            diesel::replace_into(_connetto_peer_cursor::table)
                .values((
                    _connetto_peer_cursor::author.eq(author.key_id),
                    _connetto_peer_cursor::account.eq(author.account),
                    _connetto_peer_cursor::seq.eq(1),
                    _connetto_peer_cursor::head.eq(vec![0; 32]),
                    _connetto_peer_cursor::offer.eq(Vec::<u8>::new()),
                    _connetto_peer_cursor::expires_at.eq(now + seconds - skew),
                ))
                .execute(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("store the cursor");
}

/// Every media message a bare node receives, as it arrives.
pub fn listener(mut events: mpsc::UnboundedReceiver<PeerEvent>) -> mpsc::UnboundedReceiver<Media> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let PeerEvent::Media { message, .. } = event
                && tx.send(message).is_err()
            {
                return;
            }
        }
    });
    rx
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

/// Inserts photo `id` owned by `owner` naming `file_id`, whose bytes the
/// device never held.
pub async fn name_file(client: &ConnettoClient<Scripted>, id: i64, owner: &str, file_id: [u8; 32]) {
    client
        .with_conn(|conn| {
            diesel::insert_into(photos::table)
                .values((
                    photos::id.eq(id),
                    photos::owner.eq(owner),
                    photos::content_id.eq(file_id.to_vec()),
                ))
                .execute(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("name the file");
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

/// Runs `proof` beside `device`'s media driver, the driver polled first so
/// it is up before the proof's first step, which another worker thread can
/// carry to its end within one poll.
pub async fn beside(device: &Device, port: MediaPort, proof: impl Future<Output = ()>) {
    tokio::select! {
        biased;
        () = device.drive_media(port) => panic!("the port ended under the proof"),
        () = proof => {}
    }
}

/// Runs `proof` beside the media drivers of every device in `drivers`,
/// each polled before the proof as [`beside`] polls its one.
pub async fn beside_all(drivers: Vec<(&Device, MediaPort)>, proof: impl Future<Output = ()>) {
    let mut running: futures_util::stream::FuturesUnordered<_> = drivers
        .into_iter()
        .map(|(device, port)| device.drive_media(port))
        .collect();
    tokio::select! {
        biased;
        Some(()) = futures_util::StreamExt::next(&mut running) => {
            panic!("a port ended under the proof")
        }
        () = proof => {}
    }
}

/// Runs `proof` beside the media drivers of two devices, both polled before
/// the proof as [`beside`] polls its one.
pub async fn beside_both(
    (first, first_port): (&Device, MediaPort),
    (second, second_port): (&Device, MediaPort),
    proof: impl Future<Output = ()>,
) {
    tokio::select! {
        biased;
        () = first.drive_media(first_port) => panic!("the first port ended under the proof"),
        () = second.drive_media(second_port) => panic!("the second port ended under the proof"),
        () = proof => {}
    }
}

/// How a bare holder misbehaves.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// It serves the file as it is.
    None,
    /// It flips a byte in the middle of every encoded range.
    ForgePiece,
    /// It names every chunk by a hash the chunk does not have.
    LieInManifest,
    /// It closes every link at its second `Fetch`.
    DropAtSecondFetch,
    /// It answers every request `Busy`.
    AlwaysBusy,
    /// It answers its first request `Busy` and serves the file after.
    BusyFirst,
    /// It answers nothing.
    Silent,
    /// It names a chunk of no bytes.
    EmptyChunk,
    /// It sends more manifest rows than a manifest may hold, in two runs.
    TooManyRows,
}

/// The peer, link and message of a media event, `None` for any other.
fn media_of(event: PeerEvent) -> Option<(DeviceIdentity, u64, Media)> {
    match event {
        PeerEvent::Media {
            peer,
            link,
            message,
        } => Some((peer, link, message)),
        _ => None,
    }
}

/// A bare node serving `bytes` as a holder would, chunked as the generic
/// class chunks it, with `fault`, answering the requests it records.
pub fn bare_holder(
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
        let mut busy = fault == Fault::AlwaysBusy || fault == Fault::BusyFirst;
        while let Some(event) = events.recv().await {
            let Some((peer, link, message)) = media_of(event) else {
                continue;
            };
            record.lock().push(message.clone());
            let Some(request_id) = message.request_id() else {
                continue;
            };
            match (fault, message) {
                (Fault::Silent, _) => {}
                _ if busy => {
                    busy = fault == Fault::AlwaysBusy;
                    let _ = node
                        .send_media(&peer, link, Media::Busy { request_id })
                        .await;
                }
                (Fault::TooManyRows, Media::Want { .. }) => {
                    for last in [false, true] {
                        let run = Media::Manifest {
                            request_id,
                            chunks: vec![([7; 32], 1); 5_000],
                            last,
                        };
                        let _ = node.send_media(&peer, link, run).await;
                    }
                }
                (_, Media::Want { .. }) => {
                    let chunks = manifest
                        .chunks()
                        .iter()
                        .map(|chunk| {
                            let mut hash = *chunk.hash.as_bytes();
                            if fault == Fault::LieInManifest {
                                hash[0] ^= 1;
                            }
                            let len = if fault == Fault::EmptyChunk {
                                0
                            } else {
                                chunk.len
                            };
                            (hash, len)
                        })
                        .collect();
                    let run = Media::Manifest {
                        request_id,
                        chunks,
                        last: true,
                    };
                    let _ = node.send_media(&peer, link, run).await;
                }
                (
                    _,
                    Media::Fetch {
                        request_id,
                        start,
                        len,
                        ..
                    },
                ) => {
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
                        let _ = node.send_media(&peer, link, piece).await;
                    }
                }
                _ => {}
            }
        }
    });
    asked
}

/// How many `Want` requests a bare holder saw.
pub fn wants(asked: &Mutex<Vec<Media>>) -> usize {
    asked
        .lock()
        .iter()
        .filter(|message| matches!(message, Media::Want { .. }))
        .count()
}

/// The `Fetch` requests a bare holder saw, as `(start, len)`.
pub fn fetches(asked: &Mutex<Vec<Media>>) -> Vec<(u64, u64)> {
    asked
        .lock()
        .iter()
        .filter_map(|message| match message {
            Media::Fetch { start, len, .. } => Some((*start, *len)),
            _ => None,
        })
        .collect()
}

/// The photo table on an author's plain replica, the columns the policy
/// table has.
const PLAIN_PHOTOS: &str = "CREATE TABLE photos (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                            content_id BLOB NOT NULL, thumb_id BLOB, content_state TEXT)";

/// One captured queue entry's body.
#[derive(QueryableByName)]
struct Body {
    #[diesel(sql_type = diesel::sql_types::Binary)]
    changeset: Vec<u8>,
}

/// The changeset an author's replica in `dir` captures for inserting anna's
/// photo `id` naming `content` and `thumb`, the author's next entry.
pub async fn captured_photo(
    dir: &std::path::Path,
    id: i64,
    content: Vec<u8>,
    thumb: Option<Vec<u8>>,
) -> Vec<u8> {
    let schema = SyncSchema::new(
        SchemaBundle::new(
            "-- connetto:major 1",
            "",
            PLAIN_PHOTOS,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares major 1"),
    );
    let mut author = cold_over::<Scripted>(&dir.join("author.sqlite"), schema).await;
    author
        .attach(Scripted::new([]))
        .await
        .expect("attach the transport");
    diesel::insert_into(photos::table)
        .values((
            photos::id.eq(id),
            photos::owner.eq("anna"),
            photos::content_id.eq(content),
            photos::thumb_id.eq(thumb),
        ))
        .execute(author.conn())
        .expect("insert the author's photo");
    author.flush().await.expect("flush the author's write");
    diesel::sql_query("SELECT changeset FROM _connetto_pending ORDER BY position DESC LIMIT 1")
        .get_result::<Body>(author.conn())
        .expect("capture the author's photo")
        .changeset
}

/// Applies `body` as `author`'s entry `seq` on `client`.
pub async fn apply_entry(
    client: &ConnettoClient<Scripted>,
    author: &PeerAuthor,
    seq: u64,
    body: Vec<u8>,
) {
    let row = ManifestRow {
        seq,
        major: Major::new(1).expect("major 1"),
        origin: None,
        digest: Sha256::digest(&body).into(),
    };
    let author = author.clone();
    client
        .with_conn(move |conn| conn.apply_peer_entry(&author, &row, &body))
        .await
        .expect("the replica is open")
        .expect("apply the author's entry");
}
