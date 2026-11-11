//! The media driver: one loop over the peer links' media traffic that serves
//! this device's files to linked peers and fetches files from them, and the
//! files the retention pass spares for provisional rows (R79).

pub(super) mod custody;
mod fetch;
mod serve;

use core::fmt::Display;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use connetto_client::media::{MediaEvent, MediaPort, MediaSender};
use connetto_client::{ClientEvent, ConnettoConnection};
use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::MAX_MANIFEST_ROWS;
use connetto_exchange::media::{Ask, MAX_REPORT_ENTRIES, Media, MediaAction, MediaLinks, Priority};
use connetto_file_core::{
    ChunkHash, ChunkMeta, ChunkStore, FileId, FreeSpace, Manifest, MaybeSend,
};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc, oneshot};

pub(super) use fetch::Order;
use fetch::{
    ChunkOutcome, Fetch, LAST_BACKOFF, MAX_PREFETCH_BYTES, ON_DEMAND_BOUND, Stage, candidate_order,
    holders_manifest,
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
    /// One message, a `Busy` the links answered.
    Send {
        peer: DeviceIdentity,
        message: Media,
    },
    /// Holder reports to one peer, sent in order.
    Report {
        peer: DeviceIdentity,
        messages: Vec<Media>,
    },
    /// The answer to a request the links admitted, until `stop` fires or
    /// drops.
    Serve {
        peer: DeviceIdentity,
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
    /// when it fits in `budget`.
    Commit {
        tag: u64,
        manifest: Manifest,
        budget: u64,
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

    /// The files in custody and the bytes they take, nothing when the
    /// bookkeeping cannot be read.
    async fn custody_held(&self) -> (Vec<FileId>, u64) {
        self.client
            .with_conn(|conn| -> Result<_, ContentError> {
                Ok((db::custody(conn.conn())?, db::custody_bytes(conn.conn())?))
            })
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default()
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
    /// A peer is served one request at a time and at most two peers at once,
    /// and a request beyond that is answered `Busy`. A stream stops when its
    /// peer's link falls, and answers `Refused` when the file is evicted or a
    /// chunk cannot be read partway through.
    ///
    /// Returned as a future rather than spawned, the shape
    /// [`drive_outbox`](Self::drive_outbox) has. It ends when the port's
    /// client ends, answering every fetch still waiting as unavailable.
    pub async fn drive_media(&self, mut port: MediaPort) {
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
        let mut actions = Vec::new();
        for (peer, _) in sender.linked() {
            driver.links.linked(&peer);
            driver.holdings(&peer, Instant::now()).await;
            actions.extend(driver.prefetch(peer.key().as_bytes(), Instant::now()).await);
        }
        driver.act(actions, Instant::now()).await;
        loop {
            driver.launch();
            if let Some(done) = driver.finished.pop_front() {
                let actions = driver.done(done, Instant::now()).await;
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
                        Some(MediaEvent::Linked { peer, .. }) => {
                            driver.linked(&peer, now);
                            driver.holdings(&peer, now).await;
                            driver.prefetch(peer.key().as_bytes(), now).await
                        }
                        Some(MediaEvent::Unlinked { peer }) => {
                            let actions = driver.links.unlinked(&peer);
                            driver.unlinked(&peer).await;
                            actions
                        }
                        Some(MediaEvent::Message { peer, message }) => {
                            driver.links.received(&peer, message, now)
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
                    // A dropped apply is caught when the author's link comes
                    // up again.
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => Vec::new(),
                },
                Some(done) = driver.running.next(), if !driver.running.is_empty() => {
                    driver.done(done, Instant::now()).await
                }
                () = self.custody_changed.notified() => {
                    driver.custody_changed().await;
                    Vec::new()
                }
                () = sleep, if wake.is_some() => {
                    let now = Instant::now();
                    let mut actions = driver.links.timeout(now);
                    actions.extend(driver.retry_due(now).await);
                    driver.room_due(now).await;
                    actions
                }
            };
            driver.act(actions, Instant::now()).await;
        }
    }

    /// Runs `job` to its end.
    async fn run_job(&self, sender: MediaSender, job: Job) -> Done {
        match job {
            Job::Send { peer, message } => {
                let _ = sender.send(&peer, message).await;
                Done::Sent
            }
            Job::Report { peer, messages } => {
                for message in messages {
                    if sender.send(&peer, message).await.is_err() {
                        break;
                    }
                }
                Done::Reported { peer }
            }
            Job::Serve {
                peer,
                request_id,
                ask,
                stop,
            } => {
                tokio::select! {
                    () = self.serve(&sender, &peer, request_id, ask) => {}
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
            } => {
                let outcome = self.commit_fetched(&manifest, budget).await;
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
    /// The custody files each peer was last told of.
    told: HashMap<DeviceIdentity, HashSet<[u8; 32]>>,
    /// The room and rates each peer was last told.
    told_room: HashMap<DeviceIdentity, RoomTold>,
    /// When the room and rates are next checked for a change.
    room_at: Option<Instant>,
    /// Reports waiting behind the one in flight to each peer, so they
    /// arrive in order.
    reports: HashMap<DeviceIdentity, Vec<Media>>,
    /// The peers a report job is sending to now.
    reporting: HashSet<DeviceIdentity>,
    /// The holders last heard of for each of this device's own replicated
    /// files.
    heard: HashMap<FileId, usize>,
}

impl<'c, T, B, H, F, J> Driver<'c, T, B, H, F, J>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkStore + FreeSpace + Clone + Sync + MaybeSend + 'static,
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
            told: HashMap::new(),
            told_room: HashMap::new(),
            room_at: None,
            reports: HashMap::new(),
            reporting: HashSet::new(),
            heard: HashMap::new(),
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
            .min()
    }

    /// `peer` linked, which also wakes every fetch waiting on busy holders.
    /// A peer the driver already counts, from the links it found at its
    /// start, keeps its state, so a request in flight to it stands.
    fn linked(&mut self, peer: &DeviceIdentity, now: Instant) {
        if !self.links.linked_peers().any(|linked| linked == peer) {
            self.links.linked(peer);
        }
        for fetch in self.fetches.values_mut() {
            if fetch.retry_at.is_some() {
                fetch.retry_at = Some(now);
            }
        }
    }

    /// `peer`'s link fell, taking what it was told and its report with it.
    async fn unlinked(&mut self, peer: &DeviceIdentity) {
        self.told.remove(peer);
        self.told_room.remove(peer);
        self.reports.remove(peer);
        if self.links.linked_peers().next().is_none() {
            self.room_at = None;
        }
        self.count_holders().await;
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

    /// The files of `custody` `peer`'s gate admits.
    async fn admitted(&mut self, peer: &DeviceIdentity, custody: &[FileId]) -> HashSet<[u8; 32]> {
        let content = self.content;
        let sender = self.sender.clone();
        self.beside_jobs(async move {
            let mut admitted = HashSet::new();
            for file_id in custody {
                if content.admits(&sender, peer, file_id.as_bytes()).await {
                    admitted.insert(*file_id.as_bytes());
                }
            }
            admitted
        })
        .await
    }

    /// Queues `messages` to `peer` behind its earlier reports.
    fn report(&mut self, peer: &DeviceIdentity, messages: Vec<Media>) {
        self.reports
            .entry(peer.clone())
            .or_default()
            .extend(messages);
        self.send_reports(peer);
    }

    /// Starts the job sending `peer`'s queued reports unless one runs.
    fn send_reports(&mut self, peer: &DeviceIdentity) {
        if self.reporting.contains(peer) {
            return;
        }
        let Some(messages) = self
            .reports
            .remove(peer)
            .filter(|queued| !queued.is_empty())
        else {
            return;
        };
        self.reporting.insert(peer.clone());
        self.jobs.push(Job::Report {
            peer: peer.clone(),
            messages,
        });
    }

    /// Tells `peer`, whose link came up, everything it may know of this
    /// device's custody, room and rates.
    async fn holdings(&mut self, peer: &DeviceIdentity, now: Instant) {
        let content = self.content;
        let (custody, held) = self.beside_jobs(content.custody_held()).await;
        let admitted = self.admitted(peer, &custody).await;
        let room = self.content.room(self.budget, held);
        let rates = self.rates();
        let files: Vec<[u8; 32]> = admitted.iter().copied().collect();
        let mut runs: Vec<&[[u8; 32]]> = files.chunks(MAX_REPORT_ENTRIES).collect();
        if runs.is_empty() {
            runs.push(&[]);
        }
        let last = runs.len() - 1;
        let messages = runs
            .into_iter()
            .enumerate()
            .map(|(index, run)| Media::Holdings {
                room,
                rates: if index == 0 {
                    rates.clone()
                } else {
                    Vec::new()
                },
                held: run.to_vec(),
                leaving: Vec::new(),
                last: index == last,
            })
            .collect();
        self.report(peer, messages);
        self.told.insert(peer.clone(), admitted);
        self.told_room.insert(peer.clone(), (room, rates));
        self.room_at.get_or_insert(now + ROOM_EVERY);
    }

    /// Custody changed, so every told peer hears what entered and left of
    /// the files its gate admits.
    async fn custody_changed(&mut self) {
        let content = self.content;
        let (custody, _) = self.beside_jobs(content.custody_held()).await;
        let peers: Vec<DeviceIdentity> = self.told.keys().cloned().collect();
        for peer in peers {
            let admitted = self.admitted(&peer, &custody).await;
            let Some(told) = self.told.get_mut(&peer) else {
                continue;
            };
            let added: Vec<[u8; 32]> = admitted.difference(told).copied().collect();
            let removed: Vec<[u8; 32]> = told.difference(&admitted).copied().collect();
            *told = admitted;
            if added.is_empty() && removed.is_empty() {
                continue;
            }
            let runs = added.len().max(removed.len()).div_ceil(MAX_REPORT_ENTRIES);
            let messages = (0..runs)
                .map(|run| {
                    let window = run * MAX_REPORT_ENTRIES..(run + 1) * MAX_REPORT_ENTRIES;
                    Media::Held {
                        added: slice(&added, &window),
                        removed: slice(&removed, &window),
                        leaving: Vec::new(),
                    }
                })
                .collect();
            self.report(&peer, messages);
        }
    }

    /// Tells every linked peer the room and rates that changed since it was
    /// last told, once the check is due.
    async fn room_due(&mut self, now: Instant) {
        if self.room_at.is_none_or(|at| at > now) {
            return;
        }
        let content = self.content;
        let (_, held) = self.beside_jobs(content.custody_held()).await;
        let current = (self.content.room(self.budget, held), self.rates());
        let peers: Vec<DeviceIdentity> = self.told_room.keys().cloned().collect();
        for peer in peers {
            if self.told_room.get(&peer) == Some(&current) {
                continue;
            }
            let (room, rates) = current.clone();
            self.report(&peer, vec![Media::Room { room, rates }]);
            self.told_room.insert(peer, current.clone());
        }
        self.room_at = Some(now + ROOM_EVERY);
    }

    /// Counts the linked devices reporting each of this device's own unsent
    /// replicated files, raising [`ContentEvent::Replicated`] for each count
    /// that changed.
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
            let holders = self
                .links
                .linked_peers()
                .filter(|peer| {
                    self.links
                        .report(peer)
                        .is_some_and(|report| report.holds(file_id.as_bytes()))
                })
                .count();
            if self.heard.get(&file_id).copied().unwrap_or(0) != holders {
                self.heard.insert(file_id, holders);
                let _ = self
                    .content
                    .events
                    .send(ContentEvent::Replicated { file_id, holders });
            }
        }
    }

    /// Carries out `actions` and every action they lead to.
    async fn act(&mut self, actions: Vec<Action>, now: Instant) {
        let mut queue = VecDeque::from(actions);
        while let Some(action) = queue.pop_front() {
            let more = match action {
                MediaAction::Send { peer, message } => {
                    self.jobs.push(Job::Send { peer, message });
                    Vec::new()
                }
                MediaAction::Serve {
                    peer,
                    request_id,
                    ask,
                } => {
                    let (stop, stopped) = oneshot::channel();
                    self.streams.insert(peer.clone(), (request_id, stop));
                    self.jobs.push(Job::Serve {
                        peer,
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
                MediaAction::Reported { .. } => {
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
    /// A prefetch it joins turns on demand, asking every linked holder once
    /// its author is done.
    async fn order(&mut self, order: Order, now: Instant) -> Vec<Action> {
        if let Some(&tag) = self.tags.get(&order.file_id) {
            let content = self.content;
            let authors = self
                .beside_jobs(content.provisional_authors(order.file_id))
                .await;
            let linked = candidate_order(self.links.linked_peers(), &authors);
            if let Some(fetch) = self.fetches.get_mut(&tag) {
                fetch.answers.push(order.answer);
                if fetch.prefetch {
                    fetch.prefetch = false;
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
            fetch.prefetch = true;
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
        if !fetch.prefetch
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
        let prefetch = fetch.prefetch;
        if prefetch
            && manifest.chunks().iter().map(|chunk| chunk.len).sum::<u64>() > MAX_PREFETCH_BYTES
        {
            self.finish(tag, Ok(false));
            return self.release(&peer, done, now);
        }
        let next = self.chunk_or_commit(tag, manifest, 0).await;
        if !prefetch {
            return self.links.proceed(&peer, next.map(|ask| (ask, tag)), now);
        }
        // A prefetch's next ask queues behind every on-demand one.
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
                self.jobs.push(Job::Commit {
                    tag,
                    manifest,
                    budget: self.budget,
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
