//! A content client fetching a file from linked holders on loopback: every
//! piece verified against the file id, a forged piece and a lying manifest
//! refused, a fetch resumed at the next chunk after a lost link, busy and
//! silent holders bounded in time (R79 decisions 1, 3, 4, 5 and 10).

use std::sync::Arc;
use std::time::Duration;

use connetto_client::media::{MediaEvent, MediaPort};
use connetto_exchange::media::Media;
use connetto_file_client::{ContentEvent, FileId, Resolved};
use connetto_peer::Node;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::peer_support::{
    Device, Devices, Fault, bare_holder, beside, beside_both, declare, declared, fetches, port,
    replica, stage, wants,
};
use crate::retention::pseudorandom;
use crate::support::Scripted;

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

/// A receiver in camp with bare holders of one file, none linked yet.
struct Camp {
    _dir: tempfile::TempDir,
    receiver: Device,
    devices: Devices,
    anna: Node,
    port: Option<MediaPort>,
    bytes: Vec<u8>,
    file_id: FileId,
    serial: u8,
}

impl Camp {
    async fn new(bytes: Vec<u8>) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let (_, receiver) = declared(dir.path()).await;
        let file_id = crate::support::learn_file_id(&bytes).await;
        let devices = Devices::new();
        let (anna, _, events) = devices.node(&devices.device("anna", 3));
        let port = Some(port(anna.clone(), events));
        Self {
            _dir: dir,
            receiver,
            devices,
            anna,
            port,
            bytes,
            file_id,
            serial: 10,
        }
    }

    /// A bare holder with `fault`, its address and what it was asked.
    fn holder(&mut self, fault: Fault) -> (std::net::SocketAddr, Arc<Mutex<Vec<Media>>>) {
        self.serial += 1;
        let device = self.devices.device("r77", self.serial);
        let (node, addr, events) = self.devices.node(&device);
        (addr, bare_holder(node, events, self.bytes.clone(), fault))
    }

    /// A bare holder with `fault`, linked now.
    async fn linked_holder(&mut self, fault: Fault) -> Arc<Mutex<Vec<Media>>> {
        let (addr, asked) = self.holder(fault);
        self.anna.link(addr).await.expect("the link completes");
        asked
    }

    fn port(&mut self) -> MediaPort {
        self.port.take().expect("one driver per camp")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_holders_are_asked_again_until_the_on_demand_bound() {
    let mut camp = Camp::new(pseudorandom(10, 100_000)).await;
    let busy = camp.linked_holder(Fault::AlwaysBusy).await;
    let port = camp.port();
    beside(&camp.receiver, port, async {
        let began = std::time::Instant::now();
        assert_eq!(
            camp.receiver.resolve(camp.file_id).await.expect("resolve"),
            Resolved::Unavailable
        );
        let waited = began.elapsed();
        assert_eq!(
            wants(&busy),
            3,
            "asked at once, after 5 s and after 10 s more, and not again past 30 s"
        );
        assert!(
            waited >= Duration::from_secs(15) && waited < Duration::from_secs(30),
            "{waited:?}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_linking_while_the_others_are_busy_is_asked_at_once() {
    let mut camp = Camp::new(pseudorandom(11, 100_000)).await;
    let busy = camp.linked_holder(Fault::AlwaysBusy).await;
    let (free_addr, free) = camp.holder(Fault::None);
    let port = camp.port();
    beside(&camp.receiver, port, async {
        let link_when_busy = async {
            while wants(&busy) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            camp.anna.link(free_addr).await.expect("the link completes");
            std::time::Instant::now()
        };
        let (resolved, linked_at) =
            tokio::join!(camp.receiver.resolve(camp.file_id), link_when_busy);
        assert_local(&resolved.expect("resolve"), &camp.bytes);
        assert!(
            linked_at.elapsed() < Duration::from_secs(5),
            "the new link wakes the fetch before its backoff ends"
        );
        assert_eq!(wants(&free), 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_holder_lapses_after_the_frame_bound() {
    let mut camp = Camp::new(pseudorandom(12, 100_000)).await;
    let silent = camp.linked_holder(Fault::Silent).await;
    let port = camp.port();
    beside(&camp.receiver, port, async {
        let began = std::time::Instant::now();
        assert_eq!(
            camp.receiver.resolve(camp.file_id).await.expect("resolve"),
            Resolved::Unavailable
        );
        assert!(began.elapsed() >= Duration::from_secs(30));
        assert_eq!(wants(&silent), 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_resolves_of_one_file_share_one_fetch() {
    let mut camp = Camp::new(pseudorandom(13, 3 * 1024 * 1024)).await;
    let holder = camp.linked_holder(Fault::None).await;
    let port = camp.port();
    beside(&camp.receiver, port, async {
        let (first, second) = tokio::join!(
            camp.receiver.resolve(camp.file_id),
            camp.receiver.resolve(camp.file_id)
        );
        assert_local(&first.expect("resolve"), &camp.bytes);
        assert_local(&second.expect("resolve"), &camp.bytes);
        assert_eq!(
            wants(&holder),
            1,
            "the second resolve joined the first fetch"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manifest_no_file_can_have_is_refused_before_any_fetch() {
    for fault in [Fault::EmptyChunk, Fault::TooManyRows] {
        let mut camp = Camp::new(pseudorandom(14, 100_000)).await;
        let holder = camp.linked_holder(fault).await;
        let port = camp.port();
        beside(&camp.receiver, port, async {
            assert_eq!(
                camp.receiver.resolve(camp.file_id).await.expect("resolve"),
                Resolved::Unavailable
            );
            assert_eq!(fetches(&holder), Vec::new());
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fetch_waiting_when_the_client_ends_answers_unavailable_and_the_driver_returns() {
    let mut camp = Camp::new(pseudorandom(16, 100_000)).await;
    let silent = camp.linked_holder(Fault::Silent).await;
    // A port whose events the proof feeds, so dropping the feed ends it as
    // the client's end does. The driver reads the links at its start.
    let (feed, events) = mpsc::unbounded_channel::<MediaEvent>();
    let port = MediaPort::over(camp.anna.clone(), events);
    let ending = async {
        while wants(&silent) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        drop(feed);
    };
    let (resolved, (), ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            camp.receiver.resolve(camp.file_id),
            camp.receiver.drive_media(port),
            ending
        )
    })
    .await
    .expect("the driver returns once its port ends");
    assert_eq!(resolved.expect("resolve"), Resolved::Unavailable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resolve_waiting_when_the_driver_stops_answers_unavailable() {
    let mut camp = Camp::new(pseudorandom(15, 100_000)).await;
    let silent = camp.linked_holder(Fault::Silent).await;
    let port = camp.port();
    let stopped_driver = async {
        while wants(&silent) == 0 {
            tokio::task::yield_now().await;
        }
    };
    let (resolved, ()) = tokio::join!(camp.receiver.resolve(camp.file_id), async {
        tokio::select! {
            () = camp.receiver.drive_media(port) => panic!("the port ended"),
            () = stopped_driver => {}
        }
    });
    assert_eq!(resolved.expect("resolve"), Resolved::Unavailable);
    assert_eq!(
        camp.receiver.resolve(camp.file_id).await.expect("resolve"),
        Resolved::Unavailable,
        "with no driver running an offline device asks nobody"
    );
}
