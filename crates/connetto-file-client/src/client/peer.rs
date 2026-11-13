//! The media driver: one loop over the peer links' media traffic that serves
//! this device's files to linked peers and fetches files from them, and the
//! files the retention pass spares for provisional rows (R79).

pub(super) mod custody;
mod fetch;
mod place;
mod serve;

use core::fmt::Display;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use connetto_client::media::{MediaEvent, MediaPort, MediaSender};
use connetto_client::{ClientEvent, ConnettoConnection};
use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::MAX_MANIFEST_ROWS;
use connetto_exchange::media::{Ask, MAX_REPORT_ENTRIES, Media, MediaAction, MediaLinks, Priority};
use connetto_file_core::{
    ChunkHash, ChunkInventory, ChunkMeta, ChunkStore, FileId, FreeSpace, Manifest, MaybeSend,
};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc, oneshot};

pub(super) use fetch::Order;
use fetch::{
    ChunkOutcome, Fetch, LAST_BACKOFF, MAX_PREFETCH_BYTES, ON_DEMAND_BOUND, Purpose, Stage,
    candidate_order, holders_manifest,
};

use super::{ContentClient, ContentEvent};
use crate::db;
use crate::error::ContentError;
use crate::http::ContentHttp;
use crate::resolve::Resolved;

/// What the links ask the driver, each fetch tagged by its own number.
type Action = MediaAction<DeviceIdentity, u64>;

/// The free space custody leaves to the device unless the application sets
/// another reserve (R79 decision 16).
pub(super) const CUSTODY_RESERVE: u64 = 512 * 1024 * 1024;

/// How often a device reports its room and rates at most (R79 decision 13).
const ROOM_EVERY: Duration = Duration::from_secs(10);

/// One column declared to hold file ids for the peer links.
pub(in crate::client) struct PeerColumn {
    /// The table.
    pub(super) table: String,
    /// The column.
    pub(super) column: String,
    /// Whether a receiver fetches the files it names unasked, a thumbnail
    /// column.
    pub(super) prefetch: bool,
    /// The copies linked devices keep of the files it names, `None` for a
    /// column that is not replicated.
    pub(super) replicate: Option<Replicate>,
}

/// How a column's files are replicated (R79 decision 12).
pub(in crate::client) struct Replicate {
    /// The devices other than the author that should hold each file.
    pub(super) copies: u8,
    /// The column of the same table the server's setter writes `available`
    /// or `lost` into.
    pub(super) state: String,
}

/// A column declaration [`ContentClient::with_peer_content`] starts, recorded
/// once awaited.
#[must_use = "a declaration is recorded only once awaited"]
pub struct PeerContent<T: Transport, B: ChunkStore + Clone, H: ContentHttp> {
    content: ContentClient<T, B, H>,
    column: PeerColumn,
}

impl<T: Transport, B: ChunkStore + Clone, H: ContentHttp> PeerContent<T, B, H> {
    /// Marks the column as a thumbnail column. A receiver then fetches each
    /// file it names on an author's provisional row from that author unasked,
    /// when the author's entry applies and when the author's link comes up,
    /// behind every fetch the application waits on and only when the file is
    /// at most 1 MiB (R79 decision 8).
    pub fn prefetch(mut self) -> Self {
        self.column.prefetch = true;
        self
    }

    /// Replicates the files the column names to `copies` linked devices
    /// other than the author, held in custody until `state`, a column of the
    /// same table, reads `available` (R79 decisions 12 and 15).
    ///
    /// A device takes a file it fetched on demand into custody when it fits
    /// in [`with_custody_budget`](ContentClient::with_custody_budget), keeps
    /// it while the row is provisional and after it settles until `state`
    /// reads `available`, drops it when the row is retracted or its author's
    /// certificate lapsed, and takes a cache copy back when `state` reads
    /// `lost`. Every build of the application declares the same replication.
    pub fn replicate(mut self, copies: u8, state: &str) -> Self {
        self.column.replicate = Some(Replicate {
            copies,
            state: state.to_owned(),
        });
        self
    }
}

impl<T, B, H> IntoFuture for PeerContent<T, B, H>
where
    T: Transport + MaybeSend + Send + 'static,
    T::Error: Display,
    B: ChunkStore + FreeSpace + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp + Send + 'static,
{
    type Output = Result<ContentClient<T, B, H>, ContentError>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.content.declare(self.column))
    }
}

/// The files a declared column of a row whose peer apply still awaits a
/// verdict names, which the retention pass keeps (R79 decision 6), leaving
/// out the rows of an author whose certificate lapsed, which no courier can
/// carry any more (decision 15).
///
/// # Errors
///
/// [`ContentError::Client`] when the provisional records, a declared column
/// or an author's cursor cannot be read, so the pass evicts nothing rather
/// than a file it should keep.
pub(super) fn provisionally_named<T>(
    conn: &mut ConnettoConnection<T>,
    declared: &[PeerColumn],
    reach: &mut custody::Reach,
) -> Result<HashSet<FileId>, ContentError>
where
    T: Transport,
    T::Error: Display,
{
    let mut named = HashSet::new();
    for declared in declared {
        for blob in conn.provisional_blobs(&declared.table, &declared.column)? {
            if let Ok(id) = <[u8; 32]>::try_from(blob.value.as_slice())
                && !reach.lapsed(conn, &blob.author)?
            {
                named.insert(FileId::from_bytes(id));
            }
        }
    }
    Ok(named)
}

/// Work the driver runs beside its loop.
enum Job {
    /// One message over the peer's link numbered `link`.
    Send {
        peer: DeviceIdentity,
        link: u64,
        message: Media,
    },
    /// Holder reports to one peer, numbered for its link `link` and sent in
    /// order over it alone.
    Report {
        peer: DeviceIdentity,
        link: u64,
        messages: Vec<Media>,
    },
    /// The answer to a request the links admitted, until `stop` fires or
    /// drops.
    Serve {
        peer: DeviceIdentity,
        link: u64,
        request_id: u64,
        ask: Ask,
        stop: oneshot::Receiver<()>,
    },
    /// The verification and write of one chunk of a fetch.
    Chunk {
        tag: u64,
        generation: u64,
        file_id: FileId,
        size: u64,
        start: u64,
        chunk: ChunkMeta,
        pieces: mpsc::UnboundedReceiver<Vec<u8>>,
    },
    /// The commit of a fetch whose chunks are all in, taken into custody
    /// when it fits in `budget`, also for a settled row when the fetch
    /// `takes_over` a leaving copy.
    Commit {
        tag: u64,
        manifest: Manifest,
        budget: u64,
        takes_over: bool,
    },
}

/// What a job settled.
enum Done {
    Sent,
    Reported {
        peer: DeviceIdentity,
    },
    Served {
        peer: DeviceIdentity,
        request_id: u64,
    },
    Chunk {
        tag: u64,
        generation: u64,
        outcome: ChunkOutcome,
    },
    Committed {
        tag: u64,
        manifest: Manifest,
        outcome: Result<bool, ContentError>,
    },
}

impl<T, B, H> ContentClient<T, B, H>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkStore + FreeSpace + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    /// Declares `column` of `table` as holding file ids a linked peer may be
    /// served and fetched from, recorded once the returned declaration is
    /// awaited.
    ///
    /// A peer asking for a file is served when the policy views, under the
    /// requester's certificate, show it a row of a declared column naming the
    /// file. With nothing declared every request is refused. A fetch asks the
    /// authors of the provisional rows whose declared columns name the file
    /// first. Every build of the application declares the same columns.
    /// [`PeerContent::prefetch`] marks a thumbnail column, and
    /// [`PeerContent::replicate`] a column whose files linked devices keep
    /// until the server holds them.
    ///
    /// # Errors
    ///
    /// Awaiting answers [`ContentError::PeerContentColumnMissing`] when the
    /// replica holds no such table, column or state column, and
    /// [`ContentError::NoCopies`] for a replication to no copies.
    pub fn with_peer_content(self, table: &str, column: &str) -> PeerContent<T, B, H> {
        PeerContent {
            content: self,
            column: PeerColumn {
                table: table.to_owned(),
                column: column.to_owned(),
                prefetch: false,
                replicate: None,
            },
        }
    }

    /// Records `declared` once the replica is shown to hold it.
    async fn declare(mut self, declared: PeerColumn) -> Result<Self, ContentError> {
        if declared
            .replicate
            .as_ref()
            .is_some_and(|replicate| replicate.copies == 0)
        {
            return Err(ContentError::NoCopies {
                table: declared.table,
                column: declared.column,
            });
        }
        let query = format!("SELECT * FROM \"{}\"", declared.table.replace('"', "\"\""));
        let mut probes = vec![declared.column.clone()];
        probes.extend(
            declared
                .replicate
                .as_ref()
                .map(|replicate| replicate.state.clone()),
        );
        let missing = self
            .client
            .with_conn(move |conn| {
                probes
                    .into_iter()
                    .find(|probe| !crate::retain::answers_column(conn.conn(), &query, probe))
            })
            .await?;
        if let Some(column) = missing {
            return Err(ContentError::PeerContentColumnMissing {
                table: declared.table,
                column,
            });
        }
        self.peer_content.push(declared);
        Ok(self)
    }

    /// Sets the bytes the copies this device holds for others may take,
    /// a quarter of the free space measured when the media driver starts
    /// unless set (R79 decision 16).
    #[must_use]
    pub fn with_custody_budget(mut self, bytes: u64) -> Self {
        self.custody_budget = Some(bytes);
        self
    }

    /// Sets the free space custody always leaves to the device, 512 MiB
    /// unless set (R79 decision 16).
    #[must_use]
    pub fn with_custody_reserve(mut self, bytes: u64) -> Self {
        self.custody_reserve = bytes;
        self
    }

    /// The bytes this device has left for copies held for others when
    /// `held` are taken: what the budget leaves, never eating into the
    /// reserve.
    pub(super) fn room(&self, budget: u64, held: u64) -> u64 {
        let free = self.store.free_bytes().unwrap_or(0);
        budget
            .saturating_sub(held)
            .min(free.saturating_sub(self.custody_reserve))
    }

    /// The copies in custody and the bytes they take, nothing when the
    /// bookkeeping cannot be read.
    async fn custody_held(&self) -> (Vec<CustodyCopy>, u64) {
        self.client
            .with_conn(|conn| -> Result<_, ContentError> {
                let copies = db::custody_states(conn.conn())?
                    .into_iter()
                    .map(|(file_id, state, size)| CustodyCopy {
                        file_id,
                        state,
                        size,
                    })
                    .collect();
                Ok((copies, db::custody_bytes(conn.conn())?))
            })
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default()
    }

    /// Runs the custody pass of the C table's rows that need no ranking,
    /// signalling the reports when custody changed, and answers the earliest
    /// Unix second a custody author lapses, `None` when the pass could not
    /// run or no custody copy waits on a courierable author.
    async fn custody_refresh(&self) -> Option<i64> {
        let declared = &self.peer_content;
        let refreshed = self
            .client
            .with_conn(|conn| -> Result<_, ContentError> {
                let named = custody::named(conn, declared, &mut custody::Reach::default())?;
                let now = custody::unix_now();
                let changed = conn
                    .transact_with_bookkeeping(
                        |c| custody::pass(c, declared, &named, now, &mut HashSet::new()),
                        |_| Ok::<(), ContentError>(()),
                    )
                    .map(|(changed, ())| changed)?;
                Ok((changed, custody::next_lapse(conn, declared)?))
            })
            .await
            .ok()
            .and_then(Result::ok);
        let (changed, lapse) = refreshed?;
        if changed {
            self.custody_changed.notify_one();
        }
        lapse
    }

    /// The media driver. It answers every linked peer's `Want` with the
    /// manifest and every `Fetch` with the verified encoding of the range,
    /// each after the gate admits the requester, and `Refused` otherwise. It
    /// also fetches the files [`resolve`](Self::resolve) hands it from linked
    /// holders, verifying every piece against the file id, and prefetches the
    /// files a [`prefetch`](PeerContent::prefetch) column names on an author's
    /// provisional rows from that author, when its entry applies and when its
    /// link comes up.
    ///
    /// It also reports to every linked peer the files it holds in custody
    /// that the peer's gate admits, its room and its measured rates, in full
    /// when the link comes up, as a change whenever custody changes, and its
    /// room and rates at most every 10 seconds when they changed, and raises
    /// [`ContentEvent::Replicated`] when the number of linked devices
    /// holding one of its own unsent replicated files changes.
    ///
    /// It runs the custody half of [`tidy_content`](Self::tidy_content) at
    /// its start, on every sync change, on every peer row a server write
    /// confirms, a verdict or frontier settles or a retraction removes, and
    /// when a custody copy's author lapses, so a copy leaves custody once its
    /// state column reads `available` and a cache copy returns on `lost`
    /// without the application calling anything.
    ///
    /// A peer is served one request at a time and at most two peers at once,
    /// and a request beyond that is answered `Busy`. A stream stops when its
    /// peer's link falls, and answers `Refused` when the file is evicted or a
    /// chunk cannot be read partway through.
    ///
    /// Returned as a future rather than spawned, the shape
    /// [`drive_outbox`](Self::drive_outbox) has. It ends when the port's
    /// client ends, answering every fetch still waiting as unavailable.
    pub async fn drive_media(&self, mut port: MediaPort)
    where
        B: ChunkInventory,
    {
        let sender = port.sender().clone();
        let (orders_tx, mut orders) = mpsc::unbounded_channel();
        let _orders = OrdersOpen::open(&self.peer_orders, orders_tx);
        let mut applied = self.client.events();
        let mut applies_open = true;
        let budget = self
            .custody_budget
            .unwrap_or_else(|| self.store.free_bytes().unwrap_or(0) / 4);
        let jobs_sender = sender.clone();
        let mut driver = Driver::new(self, sender.clone(), budget, move |job| {
            self.run_job(jobs_sender.clone(), job)
        });
        let lapse = driver.beside_jobs(self.custody_refresh()).await;
        driver.lapse_at = lapse.map(wake_at);
        let actions = driver.found_links().await;
        driver.act(actions, Instant::now()).await;
        loop {
            driver.launch();
            if let Some(done) = driver.finished.pop_front() {
                let actions = driver.done(done, Instant::now()).await;
                driver.act(actions, Instant::now()).await;
                continue;
            }
            if driver.custody_due && applied.is_empty() {
                driver.custody_due = false;
                let lapse = driver.beside_jobs(self.custody_refresh()).await;
                driver.lapse_at = lapse.map(wake_at);
                driver.placement_due = true;
                continue;
            }
            if driver.placement_due && applied.is_empty() {
                driver.placement_due = false;
                let actions = driver.place(Instant::now()).await;
                driver.act(actions, Instant::now()).await;
                continue;
            }
            let wake = driver.next_wake();
            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(
                wake.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600)),
            ));
            let actions = tokio::select! {
                event = port.recv() => {
                    let now = Instant::now();
                    match event {
                        None => {
                            driver.end();
                            return;
                        }
                        Some(MediaEvent::Linked { peer, link, .. }) => {
                            driver.link_up(peer, link, now).await
                        }
                        Some(MediaEvent::Unlinked { peer }) => driver.link_down(&peer).await,
                        // A message from a link already replaced belongs to
                        // a report numbering that ended with it.
                        Some(MediaEvent::Message { peer, link, message }) => {
                            if driver.link_of.get(&peer) == Some(&link) {
                                driver.links.received(&peer, message, now)
                            } else {
                                Vec::new()
                            }
                        }
                    }
                }
                Some(order) = orders.recv() => driver.order(order, Instant::now()).await,
                event = applied.recv(), if applies_open => match event {
                    Ok(ClientEvent::ProvisionalApplied { author, .. }) => {
                        driver.prefetch(&author, Instant::now()).await
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        applies_open = false;
                        Vec::new()
                    }
                    other => {
                        driver.custody_due |= moves_custody(other.as_ref());
                        Vec::new()
                    }
                },
                Some(done) = driver.running.next(), if !driver.running.is_empty() => {
                    driver.done(done, Instant::now()).await
                }
                () = self.custody_changed.notified() => {
                    driver.custody_changed().await;
                    driver.placement_due = true;
                    // A copy that entered custody since the last pass may
                    // hang on an author who lapses before any other wake.
                    driver.custody_due = true;
                    Vec::new()
                }
                () = sleep, if wake.is_some() => driver.tick(Instant::now()).await,
            };
            driver.act(actions, Instant::now()).await;
            driver.open_link().await;
        }
    }

    /// Runs `job` to its end.
    async fn run_job(&self, sender: MediaSender, job: Job) -> Done {
        match job {
            Job::Send {
                peer,
                link,
                message,
            } => {
                let _ = sender.send(&peer, link, message).await;
                Done::Sent
            }
            Job::Report {
                peer,
                link,
                messages,
            } => {
                for message in messages {
                    if sender.send(&peer, link, message).await.is_err() {
                        break;
                    }
                }
                Done::Reported { peer }
            }
            Job::Serve {
                peer,
                link,
                request_id,
                ask,
                stop,
            } => {
                tokio::select! {
                    () = self.serve(&sender, &peer, link, request_id, ask) => {}
                    _ = stop => {}
                }
                Done::Served { peer, request_id }
            }
            Job::Chunk {
                tag,
                generation,
                file_id,
                size,
                start,
                chunk,
                pieces,
            } => Done::Chunk {
                tag,
                generation,
                outcome: self.take_chunk(file_id, size, start, chunk, pieces).await,
            },
            Job::Commit {
                tag,
                manifest,
                budget,
                takes_over,
            } => {
                let outcome = self.commit_fetched(&manifest, budget, takes_over).await;
                Done::Committed {
                    tag,
                    manifest,
                    outcome,
                }
            }
        }
    }
}

impl<T, B, H> ContentClient<T, B, H>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkStore + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    /// The answer a linked peer's copy gives, `None` when no media driver
    /// runs.
    pub(super) async fn peer_stage(
        &self,
        file_id: FileId,
    ) -> Result<Option<Resolved>, ContentError> {
        let Some(orders) = self.peer_orders.lock().clone() else {
            return Ok(None);
        };
        let (answer, answered) = oneshot::channel();
        // A driver stopping now drops the order and its answer, which reads
        // as unavailable below.
        let _ = orders.send(Order { file_id, answer });
        let Ok(fetched) = answered.await else {
            return Ok(Some(Resolved::Unavailable));
        };
        if !fetched? {
            return Ok(Some(Resolved::Unavailable));
        }
        Ok(Some(
            self.local_answer(file_id)
                .await?
                .unwrap_or(Resolved::Unavailable),
        ))
    }
}

/// A device's room and its measured rates by key id, as one report states
/// them.
type RoomTold = (u64, Vec<([u8; 32], u64)>);

/// One copy this device holds in custody.
#[derive(Clone)]
struct CustodyCopy {
    file_id: FileId,
    /// [`db::HELD`], [`db::LEAVING`] or [`db::PRESSED`].
    state: String,
    /// The file's size in bytes.
    size: u64,
}

impl CustodyCopy {
    /// Whether the copy is marked leaving, as surplus or under pressure.
    fn leaving(&self) -> bool {
        self.state != db::HELD
    }
}

/// Whether a client event can move a custody row, an event the broadcast
/// dropped counting as one. A dropped apply is caught when its author's link
/// comes up again.
fn moves_custody(event: Result<&ClientEvent, &broadcast::error::RecvError>) -> bool {
    matches!(
        event,
        Ok(ClientEvent::LivePatch { .. }
            | ClientEvent::SnapshotEnd { .. }
            | ClientEvent::FullResync { .. }
            | ClientEvent::ProvisionalConfirmed { .. }
            | ClientEvent::ProvisionalSettled { .. }
            | ClientEvent::ProvisionalRetracted { .. })
            | Err(broadcast::error::RecvError::Lagged(_))
    )
}

/// The monotonic instant just past the Unix second `lapse`, so a pass then
/// reads the author as lapsed.
fn wake_at(lapse: i64) -> Instant {
    let ahead = lapse.saturating_sub(custody::unix_now()).saturating_add(1);
    Instant::now() + Duration::from_secs(u64::try_from(ahead).unwrap_or(0))
}

/// The entries of `list` within `window`, empty past its end.
fn slice(list: &[[u8; 32]], window: &core::ops::Range<usize>) -> Vec<[u8; 32]> {
    let end = window.end.min(list.len());
    list.get(window.start.min(end)..end)
        .map_or_else(Vec::new, <[[u8; 32]]>::to_vec)
}

/// Keeps `resolve`'s way to the driver open while the driver runs.
struct OrdersOpen<'a>(&'a Mutex<Option<mpsc::UnboundedSender<Order>>>);

impl<'a> OrdersOpen<'a> {
    fn open(
        slot: &'a Mutex<Option<mpsc::UnboundedSender<Order>>>,
        orders: mpsc::UnboundedSender<Order>,
    ) -> Self {
        *slot.lock() = Some(orders);
        Self(slot)
    }
}

impl Drop for OrdersOpen<'_> {
    fn drop(&mut self) {
        *self.0.lock() = None;
    }
}

/// The driver's state: the links, the fetches and the streams it serves,
/// and the jobs running beside its loop, which `spawn` starts.
struct Driver<'c, T: Transport, B: ChunkStore + Clone, H: ContentHttp, F, J> {
    content: &'c ContentClient<T, B, H>,
    sender: MediaSender,
    links: MediaLinks<DeviceIdentity, u64>,
    fetches: HashMap<u64, Fetch>,
    tags: HashMap<FileId, u64>,
    next_tag: u64,
    streams: HashMap<DeviceIdentity, (u64, oneshot::Sender<()>)>,
    jobs: Vec<Job>,
    spawn: F,
    running: FuturesUnordered<J>,
    /// Jobs that ended while the driver awaited the connection, handled
    /// before the next event.
    finished: VecDeque<Done>,
    /// The bytes custody may take.
    budget: u64,
    /// Each linked peer's live link, whose number every send names and
    /// whose messages alone the links read.
    link_of: HashMap<DeviceIdentity, u64>,
    /// The custody files each peer was last told of.
    told: HashMap<DeviceIdentity, Told>,
    /// The room and rates each peer was last told.
    told_room: HashMap<DeviceIdentity, RoomTold>,
    /// When the room and rates are next checked for a change.
    room_at: Option<Instant>,
    /// Reports waiting behind the one in flight to each peer, numbered for
    /// its live link, so they arrive in order. A link that falls takes its
    /// queue with it.
    reports: HashMap<DeviceIdentity, Vec<Media>>,
    /// The peers a report job is sending to now.
    reporting: HashSet<DeviceIdentity>,
    /// The devices last heard of keeping and leaving each of this device's
    /// own replicated files.
    heard: HashMap<FileId, (usize, usize)>,
    /// When the earliest custody author's certificate lapses, which the
    /// custody pass then acts on.
    lapse_at: Option<Instant>,
    /// Set by every event that can move a custody row, and cleared by one
    /// pass once the burst behind it is read.
    custody_due: bool,
    /// A peer whose link just came up, told everything once the fetches its
    /// link wakes went out.
    opening: Option<DeviceIdentity>,
    /// Each leaving attempt's marks, by file, the number of the report that
    /// carried the mark to each peer by key id (R79 decision 16).
    marks: HashMap<FileId, BTreeMap<[u8; 32], u64>>,
    /// The sizes of files this device holds no manifest of, learned from a
    /// holder's manifest.
    sizes: HashMap<FileId, u64>,
    /// Set by every event that can move a placement, cleared by one pass.
    placement_due: bool,
    /// The pressed copies no linked device can take over.
    stranded: HashSet<FileId>,
}

/// What one peer was last told of this device's custody.
#[derive(Default)]
struct Told {
    /// The files told held.
    held: HashSet<[u8; 32]>,
    /// The files told leaving.
    leaving: HashSet<[u8; 32]>,
}

impl<'c, T, B, H, F, J> Driver<'c, T, B, H, F, J>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkInventory + FreeSpace + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
    F: Fn(Job) -> J,
    J: Future<Output = Done>,
{
    fn new(
        content: &'c ContentClient<T, B, H>,
        sender: MediaSender,
        budget: u64,
        spawn: F,
    ) -> Self {
        Self {
            content,
            sender,
            links: MediaLinks::new(),
            fetches: HashMap::new(),
            tags: HashMap::new(),
            next_tag: 0,
            streams: HashMap::new(),
            jobs: Vec::new(),
            spawn,
            running: FuturesUnordered::new(),
            finished: VecDeque::new(),
            budget,
            link_of: HashMap::new(),
            told: HashMap::new(),
            told_room: HashMap::new(),
            room_at: None,
            reports: HashMap::new(),
            reporting: HashSet::new(),
            heard: HashMap::new(),
            lapse_at: None,
            custody_due: false,
            opening: None,
            marks: HashMap::new(),
            sizes: HashMap::new(),
            placement_due: false,
            stranded: HashSet::new(),
        }
    }

    /// Starts every queued job.
    fn launch(&mut self) {
        for job in self.jobs.drain(..) {
            self.running.push((self.spawn)(job));
        }
    }

    /// Awaits `work` while the running jobs go on. A job waiting on the
    /// connection's lock holds its place in the lock's queue and makes
    /// progress only while polled, so work queued behind it would otherwise
    /// wait forever.
    async fn beside_jobs<O>(&mut self, work: impl Future<Output = O>) -> O {
        self.launch();
        let mut work = core::pin::pin!(work);
        loop {
            tokio::select! {
                out = &mut work => return out,
                Some(done) = self.running.next(), if !self.running.is_empty() => {
                    self.finished.push_back(done);
                }
            }
        }
    }

    /// The earliest instant a request lapses, busy holders are asked again
    /// or the room is checked.
    fn next_wake(&self) -> Option<Instant> {
        self.fetches
            .values()
            .filter_map(|fetch| fetch.retry_at)
            .chain(self.links.next_deadline())
            .chain(self.room_at)
            .chain(self.lapse_at)
            .min()
    }

    /// `peer`'s link fell, or a new one replaced it: every ask on it is lost,
    /// and what it was told and its report go with it. A leaving mark it
    /// carried is numbered again by the next link's opening report, and
    /// since reports are numbered from one growing counter, nothing the peer
    /// acknowledges over a new link can predate that report.
    async fn link_down(&mut self, peer: &DeviceIdentity) -> Vec<Action> {
        let actions = self.links.unlinked(peer);
        self.link_of.remove(peer);
        self.told.remove(peer);
        self.told_room.remove(peer);
        self.reports.remove(peer);
        if self.links.linked_peers().next().is_none() {
            self.room_at = None;
        }
        self.placement_due = true;
        self.count_holders().await;
        actions
    }

    /// `peer`'s link numbered `link` came up: the fetches it wakes and the
    /// prefetches of its rows go first, and its opening report follows. The
    /// link the driver found at its start changes nothing, and a new link
    /// replacing one is a fall and a rise.
    async fn link_up(&mut self, peer: DeviceIdentity, link: u64, now: Instant) -> Vec<Action> {
        let mut actions = match self.link_of.get(&peer) {
            Some(known) if *known == link => return Vec::new(),
            Some(_) => self.link_down(&peer).await,
            None => Vec::new(),
        };
        self.link_of.insert(peer.clone(), link);
        self.links.linked(&peer);
        for fetch in self.fetches.values_mut() {
            if fetch.retry_at.is_some() {
                fetch.retry_at = Some(now);
            }
        }
        actions.extend(self.retry_due(now).await);
        actions.extend(self.prefetch(peer.key().as_bytes(), now).await);
        self.opening = Some(peer);
        self.placement_due = true;
        actions
    }

    /// Takes on the links that stood when the driver started, telling each
    /// peer everything and prefetching from each.
    async fn found_links(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();
        for linked in self.sender.linked() {
            self.link_of.insert(linked.peer.clone(), linked.link);
            self.links.linked(&linked.peer);
            self.holdings(&linked.peer, Instant::now()).await;
            actions.extend(
                self.prefetch(linked.peer.key().as_bytes(), Instant::now())
                    .await,
            );
        }
        self.placement_due = true;
        actions
    }

    /// Tells the peer whose link just came up everything it may know.
    async fn open_link(&mut self) {
        if let Some(peer) = self.opening.take() {
            self.holdings(&peer, Instant::now()).await;
        }
    }

    /// The driver's wake: requests that lapsed, busy holders due again, the
    /// room check, and the custody pass once an author lapsed.
    async fn tick(&mut self, now: Instant) -> Vec<Action> {
        let mut actions = self.links.timeout(now);
        actions.extend(self.retry_due(now).await);
        self.room_due(now).await;
        if self.lapse_at.is_some_and(|at| at <= now) {
            self.lapse_at = None;
            self.custody_due = true;
        }
        actions
    }

    /// The port ended, so no fetch can finish.
    fn end(&mut self) {
        for (_, fetch) in self.fetches.drain() {
            fetch.answer(Ok(false));
        }
    }

    /// This device's measured rate from each linked peer, by key id.
    fn rates(&self) -> Vec<([u8; 32], u64)> {
        self.links
            .linked_peers()
            .filter_map(|peer| Some((*peer.key().as_bytes(), self.links.rate(peer)?)))
            .collect()
    }

    /// The files of `custody` `peer`'s gate admits, and those of them marked
    /// leaving.
    async fn admitted(&mut self, peer: &DeviceIdentity, custody: &[CustodyCopy]) -> Told {
        let content = self.content;
        let sender = self.sender.clone();
        self.beside_jobs(async move {
            let mut admitted = Told::default();
            for copy in custody {
                if content.admits(&sender, peer, copy.file_id.as_bytes()).await {
                    admitted.held.insert(*copy.file_id.as_bytes());
                    if copy.leaving() {
                        admitted.leaving.insert(*copy.file_id.as_bytes());
                    }
                }
            }
            admitted
        })
        .await
    }

    /// Numbers `messages`, stamps each with the latest report heard from
    /// `peer` over its link, records the number of the report that carries
    /// each leaving mark to it, and queues them behind its earlier reports.
    fn report(&mut self, peer: &DeviceIdentity, messages: Vec<Media>) {
        let seen = self.links.seen(peer);
        let queued = self.reports.entry(peer.clone()).or_default();
        for mut message in messages {
            let number = self
                .content
                .next_report
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
            message.stamp(number, seen);
            if let Media::Holdings { leaving, .. } | Media::Held { leaving, .. } = &message {
                for file in leaving {
                    self.marks
                        .entry(FileId::from_bytes(*file))
                        .or_default()
                        .insert(*peer.key().as_bytes(), number);
                }
            }
            queued.push(message);
        }
        self.send_reports(peer);
    }

    /// Starts the job sending `peer`'s queued reports unless one runs.
    fn send_reports(&mut self, peer: &DeviceIdentity) {
        if self.reporting.contains(peer) {
            return;
        }
        let (Some(&link), Some(messages)) = (
            self.link_of.get(peer),
            self.reports
                .remove(peer)
                .filter(|queued| !queued.is_empty()),
        ) else {
            return;
        };
        self.reporting.insert(peer.clone());
        self.jobs.push(Job::Report {
            peer: peer.clone(),
            link,
            messages,
        });
    }

    /// The copies this device tells its peers it holds: those in custody,
    /// and its own unsent files of a replicated column, which it holds
    /// without taking them into custody, so a settled row whose provisional
    /// record is gone still counts its author's copy (R79 decision 13).
    async fn told_copies(&mut self, custody: &[CustodyCopy]) -> Vec<CustodyCopy> {
        let content = self.content;
        let declared = &content.peer_content;
        let own = self
            .beside_jobs(
                content
                    .client
                    .with_conn(|conn| custody::own_replicated(conn.conn(), declared)),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        // A file both in custody and unsent shows twice, which the sets the
        // reports are built from fold into one.
        let mut told = custody.to_vec();
        told.extend(own.into_iter().map(|file_id| CustodyCopy {
            file_id,
            state: db::HELD.to_owned(),
            size: 0,
        }));
        told
    }

    /// Keeps a leaving attempt for every copy marked leaving, one a driver
    /// started again found in the bookkeeping included, and none for any
    /// other.
    fn track_marks(&mut self, custody: &[CustodyCopy]) {
        self.marks.retain(|file_id, _| {
            custody
                .iter()
                .any(|copy| copy.file_id == *file_id && copy.leaving())
        });
        for copy in custody.iter().filter(|copy| copy.leaving()) {
            self.marks.entry(copy.file_id).or_default();
        }
    }

    /// Tells `peer`, whose link came up, everything it may know of this
    /// device's custody, room and rates.
    async fn holdings(&mut self, peer: &DeviceIdentity, now: Instant) {
        let content = self.content;
        let (custody, held) = self.beside_jobs(content.custody_held()).await;
        self.track_marks(&custody);
        let told = self.told_copies(&custody).await;
        let admitted = self.admitted(peer, &told).await;
        let room = self.content.room(self.budget, held);
        let rates = self.rates();
        let files: Vec<[u8; 32]> = admitted.held.iter().copied().collect();
        let leaving: Vec<[u8; 32]> = admitted.leaving.iter().copied().collect();
        let runs = files
            .len()
            .max(leaving.len())
            .div_ceil(MAX_REPORT_ENTRIES)
            .max(1);
        let messages = (0..runs)
            .map(|run| {
                let window = run * MAX_REPORT_ENTRIES..(run + 1) * MAX_REPORT_ENTRIES;
                Media::Holdings {
                    room,
                    rates: rates.clone(),
                    held: slice(&files, &window),
                    leaving: slice(&leaving, &window),
                    number: 0,
                    seen: None,
                    last: run + 1 == runs,
                }
            })
            .collect();
        self.told.insert(peer.clone(), admitted);
        self.report(peer, messages);
        self.told_room.insert(peer.clone(), (room, rates));
        self.room_at.get_or_insert(now + ROOM_EVERY);
    }

    /// Custody changed, so every told peer hears what entered and left of
    /// the files its gate admits, and which of them were marked leaving or
    /// had their mark withdrawn.
    async fn custody_changed(&mut self) {
        let content = self.content;
        let (custody, _) = self.beside_jobs(content.custody_held()).await;
        self.track_marks(&custody);
        let told = self.told_copies(&custody).await;
        let peers: Vec<DeviceIdentity> = self.told.keys().cloned().collect();
        for peer in peers {
            let now = self.admitted(&peer, &told).await;
            let entry = self.told.entry(peer.clone()).or_default();
            let was = core::mem::replace(entry, now);
            let now = &*entry;
            let added: Vec<[u8; 32]> = now.held.difference(&was.held).copied().collect();
            let removed: Vec<[u8; 32]> = was.held.difference(&now.held).copied().collect();
            let staying: Vec<[u8; 32]> = was
                .leaving
                .intersection(&now.held)
                .filter(|file| !now.leaving.contains(*file))
                .copied()
                .collect();
            let leaving: Vec<[u8; 32]> = now.leaving.difference(&was.leaving).copied().collect();
            let longest = [added.len(), removed.len(), staying.len(), leaving.len()]
                .into_iter()
                .max()
                .unwrap_or(0);
            if longest == 0 {
                continue;
            }
            let messages = (0..longest.div_ceil(MAX_REPORT_ENTRIES))
                .map(|run| {
                    let window = run * MAX_REPORT_ENTRIES..(run + 1) * MAX_REPORT_ENTRIES;
                    Media::Held {
                        added: slice(&added, &window),
                        removed: slice(&removed, &window),
                        staying: slice(&staying, &window),
                        leaving: slice(&leaving, &window),
                        number: 0,
                        seen: None,
                    }
                })
                .collect();
            self.report(&peer, messages);
        }
    }

    /// Answers `peer`, whose report newly marked leaving `marked`, at once
    /// when this device holds one of them in custody, the custody it tells
    /// brought up to date first so the answer shows it as it stands.
    async fn acknowledge(&mut self, peer: &DeviceIdentity, marked: &[[u8; 32]]) {
        if marked.is_empty() {
            return;
        }
        let content = self.content;
        let (custody, _) = self.beside_jobs(content.custody_held()).await;
        let told = self.told_copies(&custody).await;
        if !told
            .iter()
            .any(|copy| marked.contains(copy.file_id.as_bytes()))
        {
            return;
        }
        self.custody_changed().await;
        self.report(
            peer,
            vec![Media::Held {
                added: Vec::new(),
                removed: Vec::new(),
                staying: Vec::new(),
                leaving: Vec::new(),
                number: 0,
                seen: None,
            }],
        );
    }

    /// Tells every linked peer the room and rates that changed since it was
    /// last told, once the check is due.
    async fn room_due(&mut self, now: Instant) {
        if self.room_at.is_none_or(|at| at > now) {
            return;
        }
        self.press().await;
        let content = self.content;
        let (_, held) = self.beside_jobs(content.custody_held()).await;
        let current = (self.content.room(self.budget, held), self.rates());
        let peers: Vec<DeviceIdentity> = self.told_room.keys().cloned().collect();
        for peer in peers {
            if self.told_room.get(&peer) == Some(&current) {
                continue;
            }
            let (room, rates) = current.clone();
            self.report(
                &peer,
                vec![Media::Room {
                    room,
                    rates,
                    number: 0,
                    seen: None,
                }],
            );
            self.told_room.insert(peer, current.clone());
        }
        self.room_at = Some(now + ROOM_EVERY);
    }

    /// Counts the linked devices reporting each of this device's own unsent
    /// replicated files held, apart from those marking it leaving, raising
    /// [`ContentEvent::Replicated`] for each count that changed.
    async fn count_holders(&mut self) {
        let content = self.content;
        let declared = &content.peer_content;
        let own = self
            .beside_jobs(
                content
                    .client
                    .with_conn(|conn| custody::own_replicated(conn.conn(), declared)),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        self.heard.retain(|file_id, _| own.contains(file_id));
        for file_id in own {
            let mut counts = (0, 0);
            for report in self
                .links
                .linked_peers()
                .filter_map(|peer| self.links.report(peer))
                .filter(|report| report.holds(file_id.as_bytes()))
            {
                if report.is_leaving(file_id.as_bytes()) {
                    counts.1 += 1;
                } else {
                    counts.0 += 1;
                }
            }
            if self.heard.get(&file_id).copied().unwrap_or_default() != counts {
                self.heard.insert(file_id, counts);
                let (holders, leaving) = counts;
                let _ = self.content.events.send(ContentEvent::Replicated {
                    file_id,
                    holders,
                    leaving,
                });
            }
        }
    }

    /// Carries out `actions` and every action they lead to.
    async fn act(&mut self, actions: Vec<Action>, now: Instant) {
        let mut queue = VecDeque::from(actions);
        while let Some(action) = queue.pop_front() {
            let more = match action {
                MediaAction::Send { peer, message } => {
                    if let Some(&link) = self.link_of.get(&peer) {
                        self.jobs.push(Job::Send {
                            peer,
                            link,
                            message,
                        });
                    }
                    Vec::new()
                }
                MediaAction::Serve {
                    peer,
                    request_id,
                    ask,
                } => {
                    let Some(&link) = self.link_of.get(&peer) else {
                        self.links.served(&peer, request_id);
                        continue;
                    };
                    let (stop, stopped) = oneshot::channel();
                    self.streams.insert(peer.clone(), (request_id, stop));
                    self.jobs.push(Job::Serve {
                        peer,
                        link,
                        request_id,
                        ask,
                        stop: stopped,
                    });
                    Vec::new()
                }
                MediaAction::StopServing { peer } => {
                    self.streams.remove(&peer);
                    Vec::new()
                }
                MediaAction::Reply {
                    peer,
                    tag,
                    message,
                    done,
                } => self.reply(peer, tag, message, done, now).await,
                MediaAction::Lapsed { peer, tag } => {
                    let mut more = self.links.proceed(&peer, None, now);
                    if self.asks(tag, &peer) {
                        more.extend(self.next_holder(tag, now));
                    }
                    more
                }
                MediaAction::Lost { peer, tag } => {
                    if self.asks(tag, &peer) {
                        self.next_holder(tag, now)
                    } else {
                        Vec::new()
                    }
                }
                MediaAction::Reported {
                    peer,
                    marked,
                    restarted,
                } => {
                    if restarted {
                        self.holdings(&peer, now).await;
                    }
                    self.acknowledge(&peer, &marked).await;
                    self.placement_due = true;
                    self.count_holders().await;
                    Vec::new()
                }
            };
            queue.extend(more);
        }
    }

    /// Whether the fetch `tag` is asking `peer` now.
    fn asks(&self, tag: u64, peer: &DeviceIdentity) -> bool {
        self.fetches
            .get(&tag)
            .is_some_and(|fetch| fetch.holder.as_ref() == Some(peer))
    }

    /// `resolve` asked for a file, which joins a fetch of it already running.
    /// A prefetch, custody pull or size probe it joins turns on demand,
    /// asking every linked holder once the holder asked now is done.
    async fn order(&mut self, order: Order, now: Instant) -> Vec<Action> {
        if let Some(&tag) = self.tags.get(&order.file_id) {
            let content = self.content;
            let authors = self
                .beside_jobs(content.provisional_authors(order.file_id))
                .await;
            let linked = candidate_order(self.links.linked_peers(), &authors);
            if let Some(fetch) = self.fetches.get_mut(&tag) {
                fetch.answers.push(order.answer);
                if fetch.purpose != Purpose::OnDemand {
                    fetch.purpose = Purpose::OnDemand;
                    fetch.candidates = linked
                        .into_iter()
                        .filter(|peer| fetch.holder.as_ref() != Some(peer))
                        .collect();
                }
            }
            return Vec::new();
        }
        let tag = self.next_tag;
        self.next_tag += 1;
        let mut fetch = Fetch::new(order.file_id, now);
        fetch.answers.push(order.answer);
        self.fetches.insert(tag, fetch);
        self.tags.insert(order.file_id, tag);
        self.start_round(tag, order.file_id, now).await
    }

    /// Queues a prefetch from `author`, when linked, of every file a
    /// prefetch column names on its provisional rows that the device lacks
    /// and fetches nothing of yet.
    async fn prefetch(&mut self, author: &[u8], now: Instant) -> Vec<Action> {
        let Some(peer) = self
            .links
            .linked_peers()
            .find(|peer| peer.key().as_bytes().as_slice() == author)
            .cloned()
        else {
            return Vec::new();
        };
        let mut actions = Vec::new();
        let content = self.content;
        for file_id in self.beside_jobs(content.prefetches_of(author)).await {
            if self.tags.contains_key(&file_id) {
                continue;
            }
            let tag = self.next_tag;
            self.next_tag += 1;
            let mut fetch = Fetch::new(file_id, now);
            fetch.purpose = Purpose::Prefetch;
            fetch.candidates = VecDeque::from([peer.clone()]);
            self.fetches.insert(tag, fetch);
            self.tags.insert(file_id, tag);
            actions.extend(self.next_holder(tag, now));
        }
        actions
    }

    /// Lines up every linked holder for the fetch `tag` of `file_id` and asks
    /// the first.
    async fn start_round(&mut self, tag: u64, file_id: FileId, now: Instant) -> Vec<Action> {
        let content = self.content;
        let authors = self.beside_jobs(content.provisional_authors(file_id)).await;
        let candidates = candidate_order(self.links.linked_peers(), &authors);
        if let Some(fetch) = self.fetches.get_mut(&tag) {
            fetch.candidates = candidates;
            fetch.round_busy = None;
            fetch.retry_at = None;
        }
        self.next_holder(tag, now)
    }

    /// Every fetch whose busy holders are due again starts a new round.
    async fn retry_due(&mut self, now: Instant) -> Vec<Action> {
        let due: Vec<(u64, FileId)> = self
            .fetches
            .iter()
            .filter(|(_, fetch)| fetch.retry_at.is_some_and(|at| at <= now))
            .map(|(tag, fetch)| (*tag, fetch.file_id))
            .collect();
        let mut actions = Vec::new();
        for (tag, file_id) in due {
            actions.extend(self.start_round(tag, file_id, now).await);
        }
        actions
    }

    /// Drops the fetch `tag`'s holder and asks the next linked one from
    /// `Want`. With none left, an on-demand round every holder answered
    /// `Busy` is tried again later within the on-demand bound, and the fetch
    /// is otherwise unavailable, a prefetch dropped until its next trigger.
    fn next_holder(&mut self, tag: u64, now: Instant) -> Vec<Action> {
        let Some(fetch) = self.fetches.get_mut(&tag) else {
            return Vec::new();
        };
        fetch.generation += 1;
        fetch.stage = Stage::Idle;
        fetch.holder = None;
        let linked: Vec<&DeviceIdentity> = self.links.linked_peers().collect();
        while let Some(peer) = fetch.candidates.pop_front() {
            if linked.contains(&&peer) {
                fetch.holder = Some(peer.clone());
                fetch.stage = Stage::Wanting(Vec::new());
                let want = Ask::Want {
                    file_id: *fetch.file_id.as_bytes(),
                };
                let priority = fetch.priority();
                return self.links.ask(&peer, want, tag, priority, now);
            }
        }
        if fetch.purpose == Purpose::OnDemand
            && fetch.round_busy == Some(true)
            && now + fetch.backoff <= fetch.progress + ON_DEMAND_BOUND
        {
            fetch.retry_at = Some(now + fetch.backoff);
            fetch.backoff = (fetch.backoff * 2).min(LAST_BACKOFF);
            return Vec::new();
        }
        self.finish(tag, Ok(false));
        Vec::new()
    }

    /// Ends the fetch `tag`, answering every resolve waiting on it.
    fn finish(&mut self, tag: u64, outcome: Result<bool, ContentError>) {
        if let Some(fetch) = self.fetches.remove(&tag) {
            self.tags.remove(&fetch.file_id);
            fetch.answer(outcome);
        }
    }

    /// A reply to the fetch `tag` from `peer`.
    async fn reply(
        &mut self,
        peer: DeviceIdentity,
        tag: u64,
        message: Media,
        done: bool,
        now: Instant,
    ) -> Vec<Action> {
        if !self.asks(tag, &peer) {
            return self.release(&peer, done, now);
        }
        match message {
            Media::Manifest { chunks, last, .. } => {
                self.manifest_run(peer, tag, chunks, last, done, now).await
            }
            Media::Piece { encoded, last, .. } => {
                if let Some(fetch) = self.fetches.get_mut(&tag)
                    && let Stage::Fetching { pieces, .. } = &mut fetch.stage
                {
                    if let Some(sink) = pieces {
                        let _ = sink.send(encoded);
                    }
                    if last {
                        *pieces = None;
                    }
                }
                self.release(&peer, done, now)
            }
            Media::Refused { .. } | Media::Busy { .. } => {
                let busy = matches!(message, Media::Busy { .. });
                if let Some(fetch) = self.fetches.get_mut(&tag) {
                    fetch.round_busy = Some(busy && fetch.round_busy.unwrap_or(true));
                }
                let mut more = self.release(&peer, done, now);
                more.extend(self.next_holder(tag, now));
                more
            }
            // A request or a report never arrives as a reply, since the links
            // route both before matching a request id.
            Media::Want { .. }
            | Media::Fetch { .. }
            | Media::Holdings { .. }
            | Media::Held { .. }
            | Media::Room { .. } => self.release(&peer, done, now),
        }
    }

    /// Lets `peer`'s link go on to its next ask once a reply ended a request.
    fn release(&mut self, peer: &DeviceIdentity, done: bool, now: Instant) -> Vec<Action> {
        if done {
            self.links.proceed(peer, None, now)
        } else {
            Vec::new()
        }
    }

    /// A run of the holder's manifest rows. The last run asks for the first
    /// chunk the store lacks, or commits when it holds them all.
    async fn manifest_run(
        &mut self,
        peer: DeviceIdentity,
        tag: u64,
        chunks: Vec<([u8; 32], u64)>,
        last: bool,
        done: bool,
        now: Instant,
    ) -> Vec<Action> {
        let Some(fetch) = self.fetches.get_mut(&tag) else {
            return self.release(&peer, done, now);
        };
        let Stage::Wanting(rows) = &mut fetch.stage else {
            return self.release(&peer, done, now);
        };
        fetch.round_busy = Some(false);
        rows.extend(chunks.into_iter().map(|(hash, len)| ChunkMeta {
            hash: ChunkHash::from_bytes(hash),
            len,
        }));
        if rows.len() > MAX_MANIFEST_ROWS {
            let mut more = self.release(&peer, done, now);
            more.extend(self.next_holder(tag, now));
            return more;
        }
        if !last {
            return Vec::new();
        }
        let rows = std::mem::take(rows);
        let Some(manifest) = holders_manifest(fetch.file_id, rows) else {
            let mut more = self.release(&peer, done, now);
            more.extend(self.next_holder(tag, now));
            return more;
        };
        let purpose = fetch.purpose;
        let size: u64 = manifest.chunks().iter().map(|chunk| chunk.len).sum();
        if purpose == Purpose::Size {
            self.sizes.insert(manifest.file_id(), size);
            self.placement_due = true;
            self.finish(tag, Ok(false));
            return self.release(&peer, done, now);
        }
        if purpose == Purpose::Prefetch && size > MAX_PREFETCH_BYTES {
            self.finish(tag, Ok(false));
            return self.release(&peer, done, now);
        }
        let next = self.chunk_or_commit(tag, manifest, 0).await;
        if purpose == Purpose::OnDemand {
            return self.links.proceed(&peer, next.map(|ask| (ask, tag)), now);
        }
        // Any other fetch's next ask queues behind every on-demand one.
        let mut more = self.links.proceed(&peer, None, now);
        if let Some(ask) = next {
            more.extend(self.links.ask(&peer, ask, tag, Priority::Prefetch, now));
        }
        more
    }

    /// Asks for the first chunk of `manifest` from `from` the store lacks,
    /// starting the job its pieces go to, or starts the commit when the store
    /// holds them all.
    async fn chunk_or_commit(&mut self, tag: u64, manifest: Manifest, from: usize) -> Option<Ask> {
        let missing = self.content.first_missing(&manifest, from).await;
        let fetch = self.fetches.get_mut(&tag)?;
        match missing {
            Err(err) => {
                self.finish(tag, Err(err));
                None
            }
            Ok(None) => {
                fetch.stage = Stage::Committing;
                let takes_over = fetch.purpose == Purpose::Custody;
                self.jobs.push(Job::Commit {
                    tag,
                    manifest,
                    budget: self.budget,
                    takes_over,
                });
                None
            }
            Ok(Some((index, start))) => {
                let chunk = manifest.chunks()[index].clone();
                let size = manifest.chunks().iter().map(|chunk| chunk.len).sum();
                let (sink, pieces) = mpsc::unbounded_channel();
                let file_id = fetch.file_id;
                fetch.stage = Stage::Fetching {
                    manifest,
                    chunk: index,
                    pieces: Some(sink),
                };
                self.jobs.push(Job::Chunk {
                    tag,
                    generation: fetch.generation,
                    file_id,
                    size,
                    start,
                    chunk: chunk.clone(),
                    pieces,
                });
                Some(Ask::Fetch {
                    file_id: *file_id.as_bytes(),
                    start,
                    len: chunk.len,
                })
            }
        }
    }

    /// A job ended.
    async fn done(&mut self, done: Done, now: Instant) -> Vec<Action> {
        match done {
            Done::Sent => Vec::new(),
            Done::Reported { peer } => {
                self.reporting.remove(&peer);
                if self.links.linked_peers().any(|linked| *linked == peer) {
                    self.send_reports(&peer);
                } else {
                    self.reports.remove(&peer);
                }
                Vec::new()
            }
            Done::Served { peer, request_id } => {
                self.links.served(&peer, request_id);
                if self
                    .streams
                    .get(&peer)
                    .is_some_and(|(id, _)| *id == request_id)
                {
                    self.streams.remove(&peer);
                }
                Vec::new()
            }
            Done::Chunk {
                tag,
                generation,
                outcome,
            } => self.chunk_done(tag, generation, outcome, now).await,
            Done::Committed {
                tag,
                manifest,
                outcome,
            } => match outcome {
                Ok(true) => {
                    self.finish(tag, Ok(true));
                    Vec::new()
                }
                // A chunk went missing before the commit held them all, so
                // the fetch asks for it again.
                Ok(false) => self.ask_on(tag, manifest, 0, now).await,
                Err(err) => {
                    self.finish(tag, Err(err));
                    Vec::new()
                }
            },
        }
    }

    /// A chunk job of the fetch `tag` ended.
    async fn chunk_done(
        &mut self,
        tag: u64,
        generation: u64,
        outcome: ChunkOutcome,
        now: Instant,
    ) -> Vec<Action> {
        let Some(fetch) = self.fetches.get_mut(&tag) else {
            return Vec::new();
        };
        if fetch.generation != generation {
            return Vec::new();
        }
        match outcome {
            ChunkOutcome::Written => {
                fetch.progress = now;
                let Stage::Fetching {
                    manifest, chunk, ..
                } = std::mem::replace(&mut fetch.stage, Stage::Idle)
                else {
                    return Vec::new();
                };
                self.ask_on(tag, manifest, chunk + 1, now).await
            }
            ChunkOutcome::Forged => self.next_holder(tag, now),
            ChunkOutcome::Store(err) => {
                self.finish(tag, Err(ContentError::Store(err)));
                Vec::new()
            }
        }
    }

    /// Asks the fetch `tag`'s holder for the next chunk of `manifest` from
    /// `from` the store lacks, or commits.
    async fn ask_on(
        &mut self,
        tag: u64,
        manifest: Manifest,
        from: usize,
        now: Instant,
    ) -> Vec<Action> {
        let Some(ask) = self.chunk_or_commit(tag, manifest, from).await else {
            return Vec::new();
        };
        let Some((holder, priority)) = self
            .fetches
            .get(&tag)
            .and_then(|fetch| Some((fetch.holder.clone()?, fetch.priority())))
        else {
            return Vec::new();
        };
        self.links.ask(&holder, ask, tag, priority, now)
    }
}
