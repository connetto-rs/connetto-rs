//! The holder's upload: a custody copy whose row settled while the server
//! lacks the file goes up credited to the row's owner, at once on the
//! device whose courier call settled it and after the holder's wait
//! otherwise, also on a device that took the copy over from a leaving
//! holder, and a copy whose state reads `available` drops its upload, a
//! refused one or one a running walk had listed (R79 decision 17).

use std::time::Duration;

use connetto_client::live::ConnettoClient;
use connetto_client::peer::PeerAuthor;
use connetto_core::exchange::{Frontier, Offer, SignedOffer};
use connetto_core::messages::{CONTENT_TICKET_REFUSED, ContentVerb};
use connetto_core::{Major, SchemaVersion};
use connetto_file_client::{ContentEvent, FileId, Resolved};
use diesel::prelude::*;
use tokio::sync::broadcast;

use crate::peer_support::{
    Device, Devices, apply_entry, beside_both, captured_photo, declare_replicated, port, replica,
    replica_with, set_custody_state, set_state, stage,
};
use crate::retention::pseudorandom;
use crate::support::{RecordingHttp, Scripted, TicketAnswer, learn_file_id};

/// How long a proof waits for an event before failing.
const BOUND: Duration = Duration::from_secs(10);

/// The write ticket the scripted server grants.
const INTENT_URL: &str = "https://files.test/files/ab/intent?t=TOKEN";

/// The wait of the best-ranked holder, which a proof moves the settling
/// past.
const PAST_THE_WAIT: i64 = 11 * 60;

diesel::table! {
    /// The files a device must upload.
    _connetto_content_outbox (file_id) {
        /// The file awaiting upload.
        file_id -> Binary,
        /// The account a carried upload credits.
        carried_for -> Nullable<Text>,
    }
}

diesel::table! {
    /// The copies a device holds in custody for another device's rows.
    _connetto_content_custody (file_id) {
        /// The held file.
        file_id -> Binary,
        /// When the device learned the row settled, in Unix seconds.
        settled_seen_at -> Nullable<BigInt>,
        /// The account credited by a carried upload.
        owner -> Nullable<Text>,
    }
}

diesel::table! {
    /// connetto's record of each peer author's last verified offer.
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

/// The author whose photo the holders keep.
fn anna() -> PeerAuthor {
    PeerAuthor {
        key_id: vec![7; 32],
        account: "anna".to_owned(),
        bot_list: Some("*".to_owned()),
    }
}

/// The wall clock in Unix seconds.
fn unix_now() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_secs(),
    )
    .expect("a Unix time fits")
}

/// A device holding anna's photo, row 7, in custody, its replica attached
/// to `server`, which the proof scripts after a first refused read so the
/// device fetched the photo from a linked holder.
struct Holder {
    dirs: [tempfile::TempDir; 3],
    client: ConnettoClient<Scripted>,
    server: Scripted,
    device: Device,
    file_id: FileId,
}

impl Holder {
    /// A holder whose file server accepts the upload.
    async fn new(server: Scripted) -> Self {
        Self::with_http(
            server,
            RecordingHttp::new([(200_u16, br#"{"needed":[]}"#.to_vec()), (200_u16, vec![])]),
        )
        .await
    }

    /// A holder whose file server answers through `http`.
    async fn with_http(server: Scripted, http: RecordingHttp) -> Self {
        let dirs = [
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
        ];
        let (_, source) = declare_replicated(replica(dirs[0].path(), None).await).await;
        let file_id = stage(&source, 1, "anna", &pseudorandom(61, 300_000)).await;
        let (client, device) = replica_with(dirs[1].path(), Some(server.clone()), http).await;
        let device = device
            .with_custody_budget(64 * 1024 * 1024)
            .with_custody_reserve(0);
        let (client, device) = declare_replicated((client, device)).await;
        apply_entry(
            &client,
            &anna(),
            1,
            captured_photo(dirs[2].path(), 7, file_id.as_bytes().to_vec(), None).await,
        )
        .await;
        store_offer(&client).await;

        let devices = Devices::new();
        let (source_node, source_addr, source_events) = devices.node(&devices.device("r77", 2));
        // The curator reads every photo, so the holder's gate serves it.
        let (node, _, events) = devices.node(&devices.device("curator", 3));
        node.link(source_addr).await.expect("the link completes");
        beside_both(
            (&source, port(source_node, source_events)),
            (&device, port(node, events)),
            async {
                assert!(
                    matches!(
                        device.resolve(file_id).await.expect("resolve"),
                        Resolved::Local { .. }
                    ),
                    "the device fetches the photo from the holder into custody"
                );
            },
        )
        .await;
        Self {
            dirs,
            client,
            server,
            device,
            file_id,
        }
    }

    /// A device whose replica holds anna's photo row naming `file_id` but no
    /// copy of it and no cursor of hers, so only a holder can tell it whom
    /// to credit.
    async fn taker(server: Scripted, file_id: FileId) -> Self {
        let dirs = [
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
            tempfile::tempdir().expect("temp dir"),
        ];
        let http = RecordingHttp::new([(200_u16, br#"{"needed":[]}"#.to_vec()), (200_u16, vec![])]);
        let (client, device) = replica_with(dirs[1].path(), Some(server.clone()), http).await;
        let device = device
            .with_custody_budget(64 * 1024 * 1024)
            .with_custody_reserve(0);
        let (client, device) = declare_replicated((client, device)).await;
        apply_entry(
            &client,
            &anna(),
            1,
            captured_photo(dirs[2].path(), 7, file_id.as_bytes().to_vec(), None).await,
        )
        .await;
        Self {
            dirs,
            client,
            server,
            device,
            file_id,
        }
    }

    /// Runs `proof` beside the device's media and outbox drivers, the media
    /// driver on a node of its own linked to nobody.
    async fn driven(&self, proof: impl Future<Output = ()>) {
        let devices = Devices::new();
        let (node, _, events) = devices.node(&devices.device("curator", 3));
        tokio::select! {
            biased;
            () = self.device.drive_media(port(node, events)) => panic!("the port ended under the proof"),
            () = self.device.drive_outbox(never) => panic!("the outbox driver ended"),
            () = proof => {}
        }
    }

    /// Settles anna's entry by her frontier, as a verdict would.
    async fn settle(&self) {
        self.client
            .with_conn(|conn| conn.adjudicate_peer_frontier(&anna().key_id, 1))
            .await
            .expect("the replica is open")
            .expect("adjudicate anna's frontier");
    }

    /// Records the settling and moves it `seconds` into the past.
    async fn settled_ago(&self, seconds: i64) {
        self.settle().await;
        self.device.tidy_content().await.expect("tidy");
        let file_id = self.file_id;
        let moved = self
            .client
            .with_conn(move |conn| {
                diesel::update(_connetto_content_custody::table)
                    .filter(_connetto_content_custody::file_id.eq(file_id.as_bytes().to_vec()))
                    .set(_connetto_content_custody::settled_seen_at.eq(unix_now() - seconds))
                    .execute(conn.conn())
            })
            .await
            .expect("the replica is open")
            .expect("move the settling");
        assert_eq!(moved, 1, "the copy is in custody");
    }

    /// Runs a proof after a linked reader receives this holder's manifest.
    async fn reported(&self, proof: impl Future<Output = ()>) {
        let devices = Devices::new();
        let (node, addr, events) = devices.node(&devices.device("curator", 3));
        let (reader, _, mut replies) = devices.node(&devices.device("anna", 4));
        let identity = reader.link(addr).await.expect("link the reader");
        crate::peer_support::beside(&self.device, port(node, events), async {
            let replies = crate::peer_holder::ask(
                &reader,
                &mut replies,
                &identity,
                connetto_exchange::media::Media::Want {
                    request_id: 1,
                    file_id: *self.file_id.as_bytes(),
                },
            )
            .await;
            assert!(matches!(
                replies.last(),
                Some(connetto_exchange::media::Media::Manifest { last: true, .. })
            ));
            proof.await;
        })
        .await;
    }

    /// Queues this copy for its original owner's upload.
    async fn queue_carried(&self) {
        let file_id = self.file_id;
        self.client
            .with_conn(move |conn| {
                diesel::insert_into(_connetto_content_outbox::table)
                    .values((
                        _connetto_content_outbox::file_id.eq(file_id.as_bytes().to_vec()),
                        _connetto_content_outbox::carried_for.eq("anna"),
                    ))
                    .execute(conn.conn())
            })
            .await
            .expect("the replica is open")
            .expect("queue the carried copy");
    }

    /// The write tickets the device asked for.
    fn writes(&self) -> Vec<Option<String>> {
        self.server
            .tickets()
            .into_iter()
            .filter(|ticket| matches!(ticket.verb, ContentVerb::Write { .. }))
            .map(|ticket| ticket.for_owner)
            .collect()
    }
}

/// A sleeper that never ends a wait, so the outbox driver only ever waits on
/// events.
fn never(_: Duration) -> std::future::Pending<()> {
    std::future::pending()
}

/// Stores anna's cursor with an offer whose frontier is her entry 1 and a
/// certificate a day from its end, so a courier carries her entry and the
/// upload credits her account.
async fn store_offer(client: &ConnettoClient<Scripted>) {
    let offer = SignedOffer {
        offer: Offer {
            builds: vec![(
                Major::new(1).expect("a major"),
                SchemaVersion::from_hash(b"-- connetto:major 1".to_vec()),
            )],
            settled: None,
            newest: Some(Frontier {
                seq: 1,
                head: [9; 32],
            }),
            retractions: Vec::new(),
        },
        signature: vec![1, 2, 3],
    };
    let offer = rmp_serde::to_vec_named(&offer).expect("the offer encodes");
    let author = anna();
    client
        .with_conn(move |conn| {
            diesel::replace_into(_connetto_peer_cursor::table)
                .values((
                    _connetto_peer_cursor::author.eq(author.key_id),
                    _connetto_peer_cursor::account.eq(author.account),
                    _connetto_peer_cursor::seq.eq(1),
                    _connetto_peer_cursor::head.eq(vec![9; 32]),
                    _connetto_peer_cursor::offer.eq(offer),
                    _connetto_peer_cursor::expires_at.eq(unix_now() + 24 * 3600),
                ))
                .execute(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("store anna's cursor");
}

/// The next event about `file_id` among `wanted`, bounded.
async fn next_about(
    events: &mut broadcast::Receiver<ContentEvent>,
    file_id: FileId,
    wanted: impl Fn(&ContentEvent) -> bool,
) -> ContentEvent {
    tokio::time::timeout(BOUND, async {
        loop {
            let event = events.recv().await.expect("the content events run");
            let about = match &event {
                ContentEvent::Uploaded { file_id: about }
                | ContentEvent::UploadRefused { file_id: about, .. } => *about,
                _ => continue,
            };
            if about == file_id && wanted(&event) {
                return event;
            }
        }
    })
    .await
    .expect("the event arrives within the bound")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_courier_uploads_its_copy_at_once_credited_to_the_author() {
    let holder = Holder::new(
        Scripted::new([
            TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
            TicketAnswer::Grant(INTENT_URL.to_owned()),
        ])
        .couriering(),
    )
    .await;
    let mut events = holder.device.events();
    holder
        .driven(async {
            let report = holder
                .client
                .courier(&anna().key_id)
                .await
                .expect("the courier call settles");
            assert_eq!(report.settled, [1], "the server applied anna's entry");
            next_about(&mut events, holder.file_id, |event| {
                matches!(event, ContentEvent::Uploaded { .. })
            })
            .await;
        })
        .await;
    assert_eq!(
        holder.writes(),
        [Some("anna".to_owned())],
        "the courier's upload is credited to anna, with no wait"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_uploads_once_its_wait_ran_out_credited_to_the_author() {
    let holder = Holder::new(Scripted::new([
        TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
        TicketAnswer::Grant(INTENT_URL.to_owned()),
    ]))
    .await;
    holder.settled_ago(PAST_THE_WAIT).await;
    let mut events = holder.device.events();
    holder
        .driven(async {
            next_about(&mut events, holder.file_id, |event| {
                matches!(event, ContentEvent::Uploaded { .. })
            })
            .await;
        })
        .await;
    assert_eq!(holder.writes(), [Some("anna".to_owned())]);
    holder
        .reported(async {
            assert_eq!(holder.device.flush_outbox().await.expect("flush again"), 0);
            assert_eq!(holder.writes(), [Some("anna".to_owned())]);
            assert!(matches!(
                holder
                    .device
                    .resolve(holder.file_id)
                    .await
                    .expect("resolve"),
                Resolved::Local { .. }
            ));
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_upload_goes_with_the_copy_once_the_state_reads_available() {
    // The file server answers the intent 413, a permanent refusal.
    let holder = Holder::with_http(
        Scripted::new([
            TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
            TicketAnswer::Grant(INTENT_URL.to_owned()),
        ]),
        RecordingHttp::new([(413_u16, vec![])]),
    )
    .await;
    holder.settled_ago(PAST_THE_WAIT).await;
    let mut events = holder.device.events();
    holder
        .driven(async {
            next_about(&mut events, holder.file_id, |event| {
                matches!(event, ContentEvent::UploadRefused { .. })
            })
            .await;
        })
        .await;
    assert_eq!(holder.writes(), [Some("anna".to_owned())]);
    assert_eq!(
        holder
            .device
            .refused_content()
            .await
            .expect("read the refusals")
            .len(),
        1,
        "the refused upload waits for the application"
    );
    holder
        .reported(async {
            assert_eq!(holder.device.flush_outbox().await.expect("flush"), 0);
            assert_eq!(holder.writes(), [Some("anna".to_owned())]);
            assert!(matches!(
                holder
                    .device
                    .resolve(holder.file_id)
                    .await
                    .expect("resolve"),
                Resolved::Local { .. }
            ));
        })
        .await;

    set_state(&holder.client, 7, "available").await;
    assert_eq!(
        holder.device.tidy_content().await.expect("tidy"),
        1,
        "the server holds the photo, so the copy is cache the tidy removes"
    );
    assert!(
        holder
            .device
            .refused_content()
            .await
            .expect("read the refusals")
            .is_empty(),
        "the upload went with the copy"
    );
    assert!(
        holder
            .device
            .retired_content()
            .await
            .expect("read the losses")
            .is_empty(),
        "a carried copy is no authored data lost"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upload_cancelled_while_a_walk_runs_never_goes_up() {
    let (http, hold) =
        RecordingHttp::new([(200_u16, br#"{"needed":[]}"#.to_vec()), (200_u16, vec![])])
            .holding_first();
    let holder = Holder::with_http(
        Scripted::new([
            TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
            TicketAnswer::Grant(INTENT_URL.to_owned()),
            TicketAnswer::Grant(INTENT_URL.to_owned()),
        ]),
        http,
    )
    .await;
    holder.settled_ago(0).await;
    // A photo of the device's own, listed ahead of the carried copy, whose
    // held upload keeps the walk on it while the state flips.
    let mut seed = 0;
    let own = loop {
        let bytes = pseudorandom(seed, 4096);
        if learn_file_id(&bytes).await.as_bytes() < holder.file_id.as_bytes() {
            break bytes;
        }
        seed += 1;
    };
    stage(&holder.device, 8, "curator", &own).await;
    let carried = holder.file_id;
    holder
        .client
        .with_conn(move |conn| {
            diesel::insert_into(_connetto_content_outbox::table)
                .values((
                    _connetto_content_outbox::file_id.eq(carried.as_bytes().to_vec()),
                    _connetto_content_outbox::carried_for.eq("anna"),
                ))
                .execute(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("queue the carried upload as the driver does");
    // The pin keeps the bytes once the copy is cache, so only the cancelled
    // entry stands between the walk and a second upload.
    holder
        .device
        .pin_content("photos", "SELECT content_id FROM photos", "content_id")
        .await
        .expect("pin every photo");

    let (flushed, ()) = tokio::join!(holder.device.flush_outbox(), async {
        tokio::time::timeout(BOUND, hold.arrived())
            .await
            .expect("the own upload starts");
        set_state(&holder.client, 7, "available").await;
        holder.device.tidy_content().await.expect("tidy");
        hold.release();
    });
    assert_eq!(flushed.expect("flush"), 1, "the own photo went up");
    assert_eq!(
        holder.writes(),
        [None],
        "the cancelled copy asked for no ticket, under any account"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_taken_over_after_its_row_settled_uploads_for_the_same_owner() {
    let leaving = Holder::new(Scripted::new([TicketAnswer::Refuse(
        CONTENT_TICKET_REFUSED.to_owned(),
    )]))
    .await;
    leaving.settle().await;
    // Pressed, the holder hands its copy on and never withdraws the mark.
    set_custody_state(&leaving.client, leaving.file_id, "pressed").await;
    let taker = Holder::taker(
        Scripted::new([TicketAnswer::Grant(INTENT_URL.to_owned())]),
        leaving.file_id,
    )
    .await;
    taker.settle().await;

    let devices = Devices::new();
    let (leaving_node, leaving_addr, leaving_events) = devices.node(&devices.device("r77", 4));
    let (taker_node, _, taker_events) = devices.node(&devices.device("curator", 5));
    taker_node
        .link(leaving_addr)
        .await
        .expect("the link completes");
    let mut fetched = taker.device.events();
    beside_both(
        (&leaving.device, port(leaving_node, leaving_events)),
        (&taker.device, port(taker_node, taker_events)),
        async {
            tokio::time::timeout(BOUND, async {
                loop {
                    if let ContentEvent::Fetched { file_id } =
                        fetched.recv().await.expect("the content events run")
                        && file_id == taker.file_id
                    {
                        return;
                    }
                }
            })
            .await
            .expect("the taker pulls the leaving copy");
        },
    )
    .await;

    taker.settled_ago(PAST_THE_WAIT).await;
    let mut events = taker.device.events();
    taker
        .driven(async {
            next_about(&mut events, taker.file_id, |event| {
                matches!(event, ContentEvent::Uploaded { .. })
            })
            .await;
        })
        .await;
    assert_eq!(
        taker.writes(),
        [Some("anna".to_owned())],
        "the taker credits the owner the leaving holder named"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_holder_uploads_each_copy_when_its_stagger_expires() {
    let holder = Holder::with_http(
        Scripted::new([
            TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
            TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
            TicketAnswer::Grant(INTENT_URL.to_owned()),
            TicketAnswer::Grant(INTENT_URL.to_owned()),
        ]),
        RecordingHttp::new([
            (200, br#"{"needed":[]}"#.to_vec()),
            (200, vec![]),
            (200, br#"{"needed":[]}"#.to_vec()),
            (200, vec![]),
        ]),
    )
    .await;
    let source_dir = tempfile::tempdir().expect("source");
    let row_dir = tempfile::tempdir().expect("row");
    let (_, source) = declare_replicated(replica(source_dir.path(), None).await).await;
    let second = stage(&source, 1, "anna", &pseudorandom(62, 300_000)).await;
    apply_entry(
        &holder.client,
        &anna(),
        2,
        captured_photo(row_dir.path(), 8, second.as_bytes().to_vec(), None).await,
    )
    .await;
    let devices = Devices::new();
    let (source_node, source_addr, source_events) = devices.node(&devices.device("r77", 2));
    let (node, _, events) = devices.node(&devices.device("curator", 3));
    node.link(source_addr).await.expect("link the source");
    beside_both(
        (&source, port(source_node, source_events)),
        (&holder.device, port(node, events)),
        async {
            assert!(matches!(
                holder
                    .device
                    .resolve(second)
                    .await
                    .expect("fetch the second photo"),
                Resolved::Local { .. }
            ));
        },
    )
    .await;
    holder
        .client
        .with_conn(|conn| conn.adjudicate_peer_frontier(&anna().key_id, 2))
        .await
        .expect("the replica is open")
        .expect("settle both photos");
    holder.device.tidy_content().await.expect("tidy");
    let earliest = unix_now() - 10 * 60 + 2;
    holder
        .client
        .with_conn(|conn| {
            diesel::update(_connetto_content_custody::table)
                .set(_connetto_content_custody::settled_seen_at.eq(earliest))
                .execute(conn.conn())?;
            diesel::update(_connetto_content_custody::table)
                .filter(_connetto_content_custody::file_id.eq(second.as_bytes().as_slice()))
                .set(_connetto_content_custody::settled_seen_at.eq(earliest + 2))
                .execute(conn.conn())
        })
        .await
        .expect("the replica is open")
        .expect("place both waits near their ends");
    let mut events = holder.device.events();
    holder
        .driven(async {
            next_about(&mut events, holder.file_id, |event| {
                matches!(event, ContentEvent::Uploaded { .. })
            })
            .await;
            assert_eq!(holder.writes(), [Some("anna".to_owned())]);
            next_about(&mut events, second, |event| {
                matches!(event, ContentEvent::Uploaded { .. })
            })
            .await;
        })
        .await;
    assert_eq!(
        holder.writes(),
        [Some("anna".to_owned()), Some("anna".to_owned())]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custody_without_an_owner_waits_for_its_authors_cursor() {
    let holder = Holder::new(Scripted::new([
        TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
        TicketAnswer::Grant(INTENT_URL.to_owned()),
    ]))
    .await;
    holder.settled_ago(PAST_THE_WAIT).await;
    holder
        .client
        .with_conn(|conn| {
            conn.conn()
                .transaction::<_, diesel::result::Error, _>(|db| {
                    diesel::update(_connetto_content_custody::table)
                        .set(_connetto_content_custody::owner.eq(None::<String>))
                        .execute(db)?;
                    diesel::delete(_connetto_peer_cursor::table).execute(db)
                })
        })
        .await
        .expect("the replica is open")
        .expect("restore custody without its cursor");
    holder
        .reported(async {
            assert_eq!(holder.device.flush_outbox().await.expect("flush"), 0);
            assert_eq!(holder.writes(), Vec::<Option<String>>::new());
            assert_eq!(
                holder.device.bytes(holder.file_id).await.expect("read"),
                Some(pseudorandom(61, 300_000))
            );
        })
        .await;
    store_offer(&holder.client).await;
    let mut events = holder.device.events();
    holder
        .driven(async {
            next_about(&mut events, holder.file_id, |event| {
                matches!(event, ContentEvent::Uploaded { .. })
            })
            .await;
        })
        .await;
    assert_eq!(holder.writes(), [Some("anna".to_owned())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn losing_a_carried_copy_releases_custody_without_reporting_authored_loss() {
    use connetto_file_core::{MemStore, MimeClass, process_file};

    let manifest = process_file(
        pseudorandom(61, 300_000).as_slice(),
        MimeClass::Generic,
        &MemStore::new(),
    )
    .await
    .expect("chunk the photo");
    let needed: Vec<String> = manifest
        .chunks()
        .iter()
        .map(|chunk| chunk.hash.to_string())
        .collect();
    let http = RecordingHttp::new([(
        200,
        serde_json::to_vec(&serde_json::json!({ "needed": needed })).expect("encode the intent"),
    )]);
    let holder = Holder::with_http(
        Scripted::new([
            TicketAnswer::Refuse(CONTENT_TICKET_REFUSED.to_owned()),
            TicketAnswer::Grant(INTENT_URL.to_owned()),
        ]),
        http.clone(),
    )
    .await;
    holder.queue_carried().await;
    let mut events = holder.device.events();
    std::fs::remove_dir_all(holder.dirs[1].path().join("chunks")).expect("lose the stored bytes");

    assert_eq!(holder.device.flush_outbox().await.expect("flush"), 0);
    assert_eq!(
        crate::peer_support::custody_state(&holder.client, holder.file_id).await,
        None
    );
    assert_eq!(
        holder.device.retired_content().await.expect("losses"),
        Vec::<FileId>::new()
    );
    assert_eq!(
        holder.device.refused_content().await.expect("refusals"),
        Vec::<(FileId, String)>::new()
    );
    assert!(
        events.try_recv().is_err(),
        "custody loss is not authored loss"
    );
    assert_eq!(holder.device.flush_outbox().await.expect("flush again"), 0);
    assert_eq!(holder.writes(), [Some("anna".to_owned())]);
    assert_eq!(http.sent().len(), 1, "no incomplete upload is committed");
}
