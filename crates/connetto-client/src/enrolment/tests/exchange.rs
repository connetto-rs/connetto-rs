//! Two devices on loopback, each an enrolment task over a real replica,
//! exchanging their device writers' entries (R77 slice 6b).

use core::time::Duration;
use std::sync::Arc;

use connetto_core::exchange::{Frontier, Offer, SignedOffer};
use connetto_core::messages::{ControlMessage, MutationApplied, WriterRef};
use diesel::prelude::*;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use super::{
    Authority, HOUR, RunKeys, attesting_key, authority, far_peer, inbox, issue_for, peer,
    poll_until, positive_serial, whole_second,
};
use crate::ClientError;
use crate::ClientEvent;
use crate::drain::tests::{Conn, major, queue, replica};
use crate::enrolment::task::Link;
use crate::enrolment::{EnrolHandle, Enroller, KeptList, run};

/// The task's view of an offline replica, which never reaches a server, so
/// the only traffic is the peer link's.
#[derive(Clone)]
struct ReplicaLink {
    conn: Arc<tokio::sync::Mutex<Conn>>,
    events: broadcast::Sender<ClientEvent>,
    emitted: Arc<Mutex<Vec<ClientEvent>>>,
    end: watch::Receiver<()>,
    /// How many adopted offers, then taken rows, the replica refuses next,
    /// as a full disk refuses a write.
    refusals: Arc<Mutex<(u32, u32)>>,
}

/// Spend one of the refusals `count` holds, as the replica's error.
fn refused(count: &mut u32) -> Result<(), ClientError> {
    if *count == 0 {
        return Ok(());
    }
    *count -= 1;
    Err(ClientError::Session(
        "the replica refused the write".to_owned(),
    ))
}

impl Link for ReplicaLink {
    fn alive(&self) -> bool {
        true
    }

    fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.events.subscribe()
    }

    fn emit(&self, event: ClientEvent) {
        self.emitted.lock().push(event);
    }

    fn ended(&self) -> impl Future<Output = ()> + Send {
        let mut end = self.end.clone();
        async move {
            let _ = end.changed().await;
        }
    }

    fn connection(&self) -> impl Future<Output = Option<crate::ConnectionNumber>> + Send {
        core::future::ready(None)
    }

    fn ask(
        &self,
        _request_id: String,
        _msg: ControlMessage,
    ) -> impl Future<Output = Result<oneshot::Receiver<super::Answer>, ClientError>> + Send {
        core::future::ready(Err(ClientError::NotConnected))
    }

    fn store(&self, _held: super::Held) -> impl Future<Output = Result<(), ClientError>> + Send {
        core::future::ready(Ok(()))
    }

    fn forget(&self) -> impl Future<Output = Result<(), ClientError>> + Send {
        core::future::ready(Ok(()))
    }

    fn store_list(&self, _kept: KeptList) -> impl Future<Output = Result<(), ClientError>> + Send {
        core::future::ready(Ok(()))
    }

    async fn offer(&self, key_id: [u8; 32]) -> Result<Option<Offer>, ClientError> {
        self.conn.lock().await.own_offer(key_id)
    }

    async fn build(&self) -> Option<(connetto_core::Major, connetto_core::SchemaVersion)> {
        self.conn.lock().await.own_build()
    }

    async fn serve_range(
        &self,
        key_id: [u8; 32],
        from: u64,
        to: u64,
        build: (connetto_core::Major, connetto_core::SchemaVersion),
    ) -> Result<Option<crate::exchange::Served>, ClientError> {
        self.conn.lock().await.serve_range(key_id, from, to, &build)
    }

    async fn serve_body(&self, key_id: [u8; 32], seq: u64) -> Result<Option<Vec<u8>>, ClientError> {
        self.conn.lock().await.serve_body(key_id, seq)
    }

    async fn peer_cursor(&self, author: Vec<u8>) -> Result<Frontier, ClientError> {
        self.conn.lock().await.peer_cursor(&author)
    }

    async fn adopt(
        &self,
        author: crate::peer::PeerAuthor,
        offer: SignedOffer,
        cursor: Frontier,
    ) -> Result<(), ClientError> {
        refused(&mut self.refusals.lock().0)?;
        self.conn
            .lock()
            .await
            .adopt_peer_offer(&author, &offer, &cursor)
    }

    async fn take_row(
        &self,
        author: crate::peer::PeerAuthor,
        row: connetto_core::exchange::ManifestRow,
        body: Option<Vec<u8>>,
        cursor: Frontier,
    ) -> Result<(), ClientError> {
        refused(&mut self.refusals.lock().1)?;
        self.conn
            .lock()
            .await
            .take_peer_row(&author, &row, body.as_deref(), &cursor)
    }
}

/// One device, with its replica, its task's link and handle, the node the
/// task drives, its device writer's key id and the switch that ends its run.
struct Device {
    conn: Arc<tokio::sync::Mutex<Conn>>,
    link: ReplicaLink,
    handle: EnrolHandle,
    node: connetto_peer::Node,
    key_id: [u8; 32],
    run: tokio::task::JoinHandle<()>,
    end: watch::Sender<()>,
}

impl Device {
    /// The device writer's queued rows 1 onward, row `n` captured under
    /// major `majors[n - 1]`, its certificate from `authority` under
    /// `serial`, its task running with its listener up.
    async fn start(authority: &Authority, serial: u8, majors: &[u32]) -> Self {
        let (key, der) = attesting_key();
        let key_id = *connetto_core::device_cert::key_id(&key).as_bytes();
        let held = issue_for(
            authority,
            &key,
            whole_second(),
            12 * HOUR,
            positive_serial(serial),
        );
        let mut conn = replica();
        for (id, captured) in (1..).zip(majors) {
            queue(&mut conn, WriterRef::Device(key_id), id, major(*captured));
        }
        conn.bound_writer = Some(WriterRef::Device(key_id));
        let peer = peer(authority);
        let node = peer.node.clone();
        let (hotspot_tx, _hotspot_rx) = mpsc::unbounded_channel();
        let (bluetooth_tx, _bluetooth_rx) = mpsc::unbounded_channel();
        let (enroller, handle) = Enroller::new(
            Arc::new(RunKeys { der }),
            Some(HOUR),
            Vec::new(),
            vec![authority.root.clone()],
            Some(held),
            Vec::new(),
            inbox(),
            peer,
            hotspot_tx,
            bluetooth_tx,
        );
        conn.steering = Some(handle.steering());
        let (events, _events) = broadcast::channel(64);
        let (end, end_rx) = watch::channel(());
        let conn = Arc::new(tokio::sync::Mutex::new(conn));
        let link = ReplicaLink {
            conn: Arc::clone(&conn),
            events,
            emitted: Arc::new(Mutex::new(Vec::new())),
            end: end_rx,
            refusals: Arc::new(Mutex::new((0, 0))),
        };
        let run = tokio::spawn(run(link.clone(), enroller));
        poll_until(|| handle.peer_address().is_some()).await;
        Self {
            conn,
            link,
            handle,
            node,
            key_id,
            run,
            end,
        }
    }

    /// The chain hash of the device writer's queued `seq`.
    async fn head_at(&self, seq: u64) -> [u8; 32] {
        self.conn.lock().await.writers[&WriterRef::Device(self.key_id)].pending[&seq].hash
    }

    async fn stop(self) {
        self.end.send(()).expect("the run ends");
        self.run.await.expect("the run ends");
    }
}

/// The ids of the replica's `items` rows, comma-joined in order, read
/// without waiting on a busy lock.
fn items(conn: &tokio::sync::Mutex<Conn>) -> Option<String> {
    let mut conn = conn.try_lock().ok()?;
    diesel::select(diesel::dsl::sql::<diesel::sql_types::Text>(
        "(SELECT coalesce(group_concat(id, ','), '') FROM (SELECT id FROM items ORDER BY id))",
    ))
    .get_result(&mut conn.db)
    .ok()
}

/// The stored peer entries of the replica.
fn stored_entries(conn: &tokio::sync::Mutex<Conn>) -> Option<i64> {
    let mut conn = conn.try_lock().ok()?;
    diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
        "(SELECT count(*) FROM _connetto_peer_entry)",
    ))
    .get_result(&mut conn.db)
    .ok()
}

/// R77 slice 6b, the receiver flow end to end. A linked receiver verifies
/// the author's offer against the key the link proved, requests the range,
/// verifies the chain and applies every entry with its cursor. The author's
/// next local write reaches the receiver through a fresh offer, and the
/// author's settlement adjudicates what the receiver holds.
#[tokio::test]
async fn two_linked_devices_exchange_their_writers_entries() {
    let authority = authority(whole_second()).await;
    let anna = Device::start(&authority, 5, &[2, 2, 2]).await;
    let ben = Device::start(&authority, 9, &[]).await;

    ben.node
        .link(anna.handle.peer_address().expect("anna listens"))
        .await
        .expect("the link completes");
    poll_until(|| items(&ben.conn).is_some_and(|ids| ids == "1,2,3")).await;
    let anna_head = anna.head_at(3).await;
    assert_eq!(
        ben.conn
            .lock()
            .await
            .peer_cursor(&anna.key_id)
            .expect("read the cursor"),
        Frontier {
            seq: 3,
            head: anna_head,
        },
        "the cursor stands at the verified head"
    );

    // Anna's next write reaches Ben through her fresh offer.
    queue(
        &mut *anna.conn.lock().await,
        WriterRef::Device(anna.key_id),
        4,
        major(2),
    );
    poll_until(|| items(&ben.conn).is_some_and(|ids| ids == "1,2,3,4")).await;

    // The server answers all four, so Anna's settled frontier moves and Ben
    // adjudicates every entry he stored.
    for seq in 1..=4 {
        anna.conn
            .lock()
            .await
            .handle_control(ControlMessage::MutationApplied(MutationApplied {
                client_seq: seq,
            }))
            .await
            .expect("the ack lands");
    }
    poll_until(|| stored_entries(&ben.conn) == Some(0)).await;
    assert!(
        ben.conn
            .lock()
            .await
            .provisional_rows()
            .expect("read the records")
            .iter()
            .all(|row| row.state == crate::provisional::ProvisionalState::Adjudicated),
        "the settled frontier adjudicates every record"
    );
    assert!(
        ben.link
            .emitted
            .lock()
            .iter()
            .all(|event| !matches!(event, ClientEvent::PeerChainInvalid { .. })),
        "nothing failed verification"
    );

    ben.stop().await;
    anna.stop().await;
}

/// R77 state table, offer with an invalid signature. The receiver raises
/// `PeerChainInvalid` naming the author and takes nothing of the offer.
#[tokio::test]
async fn an_offer_the_link_key_does_not_verify_raises_chain_invalid() {
    let authority = authority(whole_second()).await;
    let ben = Device::start(&authority, 9, &[]).await;
    let (far, _far_events) = far_peer(&authority, positive_serial(7));
    let ben_identity = far
        .link(ben.handle.peer_address().expect("ben listens"))
        .await
        .expect("the link completes");
    let forged = SignedOffer {
        offer: Offer {
            builds: Vec::new(),
            settled: None,
            newest: Some(Frontier {
                seq: 1,
                head: [1; 32],
            }),
            retractions: Vec::new(),
        },
        signature: vec![0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01],
    };
    far.send_exchange(&ben_identity, connetto_exchange::Message::Offer(forged))
        .await
        .expect("the offer goes out");
    poll_until(|| {
        ben.link
            .emitted
            .lock()
            .iter()
            .any(|event| matches!(event, ClientEvent::PeerChainInvalid { .. }))
    })
    .await;
    assert_eq!(
        stored_entries(&ben.conn),
        Some(0),
        "nothing of the offer was taken"
    );
    ben.stop().await;
}

/// Poll `is` until it holds, within `bound`.
async fn poll_within(bound: Duration, mut is: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + bound;
    while !is() {
        assert!(
            std::time::Instant::now() < deadline,
            "the condition never held within {bound:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// R77 state table, applying a row fails and decision 22: an offer and an
/// entry the replica refuses to take abort the request, and the receiver
/// requests again after the backoff until the range lands.
#[tokio::test]
async fn a_commit_the_replica_refuses_is_retried_after_the_backoff() {
    let authority = authority(whole_second()).await;
    let anna = Device::start(&authority, 5, &[2, 2]).await;
    let ben = Device::start(&authority, 9, &[]).await;
    *ben.link.refusals.lock() = (1, 1);

    ben.node
        .link(anna.handle.peer_address().expect("anna listens"))
        .await
        .expect("the link completes");
    // The refused offer waits 5 s and the refused entry 10 s more.
    poll_within(Duration::from_secs(40), || {
        items(&ben.conn).is_some_and(|ids| ids == "1,2")
    })
    .await;
    assert_eq!(*ben.link.refusals.lock(), (0, 0), "both refusals were met");

    ben.stop().await;
    anna.stop().await;
}

/// R77 decision 8: an entry captured under another build crosses as its
/// manifest row alone and moves the cursor past it, between entries that
/// apply.
#[tokio::test]
async fn an_entry_of_another_build_moves_the_cursor_without_its_body() {
    let authority = authority(whole_second()).await;
    let anna = Device::start(&authority, 5, &[2, 1, 2]).await;
    let ben = Device::start(&authority, 9, &[]).await;

    ben.node
        .link(anna.handle.peer_address().expect("anna listens"))
        .await
        .expect("the link completes");
    poll_until(|| items(&ben.conn).is_some_and(|ids| ids == "1,3")).await;
    let head = anna.head_at(3).await;
    poll_until(|| {
        ben.conn.try_lock().is_ok_and(|mut conn| {
            conn.peer_cursor(&anna.key_id)
                .is_ok_and(|cursor| cursor == Frontier { seq: 3, head })
        })
    })
    .await;

    ben.stop().await;
    anna.stop().await;
}

/// Request `1..=1` of `device` from `far` under `id` until the device takes
/// the request, and answer whether a manifest crossed before the end and
/// how the request ended.
async fn request_once(
    far: &connetto_peer::Node,
    events: &mut mpsc::UnboundedReceiver<connetto_peer::PeerEvent>,
    device: &connetto_core::device_cert::DeviceIdentity,
    id: u64,
) -> (bool, connetto_core::exchange::EndOutcome) {
    use connetto_core::exchange::EndOutcome;
    use connetto_exchange::{Message, Transfer};
    let mut request_id = id;
    loop {
        far.send_exchange(
            device,
            Message::Transfer(Transfer::Request {
                request_id,
                from: 1,
                to: 1,
                build: (
                    major(2),
                    connetto_core::SchemaVersion::from_hash(vec![1; 32]),
                ),
            }),
        )
        .await
        .expect("the request goes out");
        let mut manifest = false;
        let outcome = loop {
            let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
                .await
                .expect("an answer within the bound")
                .expect("the far node lives");
            match event {
                connetto_peer::PeerEvent::Exchange {
                    message: Message::Transfer(Transfer::Manifest { request_id: of, .. }),
                    ..
                } if of == request_id => manifest = true,
                connetto_peer::PeerEvent::Exchange {
                    message:
                        Message::Transfer(Transfer::End {
                            request_id: of,
                            outcome,
                        }),
                    ..
                } if of == request_id => break outcome,
                _ => {}
            }
        };
        // The previous stream's end may not have reached the machine yet.
        if outcome != EndOutcome::Busy {
            return (manifest, outcome);
        }
        request_id += 100;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// R77 decision 18: a range the replica no longer holds or cannot read, and
/// a body that left the queue or cannot be read mid-stream, end the request
/// `NotAvailable`, the manifest crossing first in the body cases.
#[tokio::test]
async fn a_range_the_device_cannot_serve_ends_not_available() {
    use connetto_core::exchange::{EndOutcome, ManifestRow};
    let (key, der) = attesting_key();
    let key_id = *connetto_core::device_cert::key_id(&key).as_bytes();
    let now = whole_second();
    let authority = authority(now).await;
    let held = issue_for(&authority, &key, now, 12 * HOUR, positive_serial(5));
    let (far, mut far_events) = far_peer(&authority, positive_serial(9));
    let served = crate::exchange::Served {
        previous: [0; 32],
        rows: vec![ManifestRow {
            seq: 1,
            major: major(2),
            origin: None,
            digest: [3; 32],
        }],
        bodies: vec![1],
    };
    let (run, state, handle, end_tx) = super::offering_device(
        authority,
        Arc::new(RunKeys { der }),
        held,
        Some(super::offer_at(1)),
    )
    .await;
    {
        let mut state = state.lock();
        state.ranges = [
            Ok(None),
            Err(()),
            Ok(Some(served.clone())),
            Ok(Some(served)),
        ]
        .into();
        state.bodies = [Ok(None), Err(())].into();
    }
    let device = far
        .link(handle.peer_address().expect("the listener"))
        .await
        .expect("the link completes");
    assert_eq!(key_id, *device.key().as_bytes());

    for (id, crossed) in [(1, false), (2, false), (3, true), (4, true)] {
        assert_eq!(
            request_once(&far, &mut far_events, &device, id).await,
            (crossed, EndOutcome::NotAvailable),
            "request {id}"
        );
    }

    end_tx.send(()).expect("the run ends");
    run.await.expect("the run ends");
}

/// R77 slice 6b: a peer whose stored cursor cannot be read starts from
/// nothing, and the link still exchanges offers.
#[tokio::test]
async fn an_unreadable_cursor_starts_over_without_blocking_the_link() {
    let (key, der) = attesting_key();
    let spki = connetto_core::device_cert::public_key_info(&key);
    let key_id = *connetto_core::device_cert::key_id(&key).as_bytes();
    let now = whole_second();
    let authority = authority(now).await;
    let held = issue_for(&authority, &key, now, 12 * HOUR, positive_serial(5));
    let (far, mut far_events) = far_peer(&authority, positive_serial(9));
    let (run, state, handle, end_tx) = super::offering_device(
        authority,
        Arc::new(RunKeys { der }),
        held,
        Some(super::offer_at(1)),
    )
    .await;
    state.lock().cursor_fails = true;
    far.link(handle.peer_address().expect("the listener"))
        .await
        .expect("the link completes");
    super::offer_received(&mut far_events, key_id, &super::offer_at(1), &spki).await;

    end_tx.send(()).expect("the run ends");
    run.await.expect("the run ends");
}
