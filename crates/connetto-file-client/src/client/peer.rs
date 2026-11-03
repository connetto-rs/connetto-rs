//! The media driver: one loop over the peer links' media traffic that serves
//! this device's files to linked peers and fetches files from them (R79).

mod fetch;
mod serve;

use core::fmt::Display;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use connetto_client::media::{MediaEvent, MediaPort, MediaSender};
use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::MAX_MANIFEST_ROWS;
use connetto_exchange::media::{Ask, Media, MediaAction, MediaLinks, Priority};
use connetto_file_core::{ChunkHash, ChunkMeta, ChunkStore, FileId, Manifest, MaybeSend};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

pub(super) use fetch::Order;
use fetch::{
    ChunkOutcome, Fetch, LAST_BACKOFF, ON_DEMAND_BOUND, Stage, candidate_order, holders_manifest,
};

use super::ContentClient;
use crate::error::ContentError;
use crate::http::ContentHttp;
use crate::resolve::Resolved;

/// What the links ask the driver, each fetch tagged by its own number.
type Action = MediaAction<DeviceIdentity, u64>;

/// Work the driver runs beside its loop.
enum Job {
    /// One message, a `Busy` the links answered.
    Send {
        peer: DeviceIdentity,
        message: Media,
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
    /// The commit of a fetch whose chunks are all in.
    Commit { tag: u64, manifest: Manifest },
}

/// What a job settled.
enum Done {
    Sent,
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
    B: ChunkStore + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    /// Declares `column` of `table` as holding file ids a linked peer may be
    /// served and fetched from.
    ///
    /// A peer asking for a file is served when the policy views, under the
    /// requester's certificate, show it a row of a declared column naming the
    /// file. With nothing declared every request is refused. A fetch asks the
    /// authors of the provisional rows whose declared columns name the file
    /// first. Every build of the application declares the same columns.
    ///
    /// # Errors
    ///
    /// [`ContentError::PeerContentColumnMissing`] when the replica holds no
    /// such table or column.
    pub async fn with_peer_content(
        mut self,
        table: &str,
        column: &str,
    ) -> Result<Self, ContentError> {
        let query = format!("SELECT * FROM \"{}\"", table.replace('"', "\"\""));
        let probe = column.to_owned();
        if !self
            .client
            .with_conn(move |conn| crate::retain::answers_column(conn.conn(), &query, &probe))
            .await?
        {
            return Err(ContentError::PeerContentColumnMissing {
                table: table.to_owned(),
                column: column.to_owned(),
            });
        }
        self.peer_content
            .push((table.to_owned(), column.to_owned()));
        Ok(self)
    }

    /// The media driver. It answers every linked peer's `Want` with the
    /// manifest and every `Fetch` with the verified encoding of the range,
    /// each after the gate admits the requester, and `Refused` otherwise. It
    /// also fetches the files [`resolve`](Self::resolve) hands it from linked
    /// holders, verifying every piece against the file id.
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
        let mut driver = Driver::new(self);
        for (peer, _) in sender.linked() {
            driver.links.linked(&peer);
        }
        let mut jobs = FuturesUnordered::new();
        loop {
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
                            Vec::new()
                        }
                        Some(MediaEvent::Unlinked { peer }) => driver.links.unlinked(&peer),
                        Some(MediaEvent::Message { peer, message }) => {
                            driver.links.received(&peer, message, now)
                        }
                    }
                }
                Some(order) = orders.recv() => driver.order(order, Instant::now()).await,
                Some(done) = jobs.next() => driver.done(done, Instant::now()).await,
                () = sleep, if wake.is_some() => {
                    let now = Instant::now();
                    let mut actions = driver.links.timeout(now);
                    actions.extend(driver.retry_due(now).await);
                    actions
                }
            };
            driver.act(actions, Instant::now()).await;
            for job in driver.jobs.drain(..) {
                jobs.push(self.run_job(sender.clone(), job));
            }
        }
    }

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

    /// Runs `job` to its end.
    async fn run_job(&self, sender: MediaSender, job: Job) -> Done {
        match job {
            Job::Send { peer, message } => {
                let _ = sender.send(&peer, message).await;
                Done::Sent
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
            Job::Commit { tag, manifest } => {
                let outcome = self.commit_fetched(&manifest).await;
                Done::Committed {
                    tag,
                    manifest,
                    outcome,
                }
            }
        }
    }
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

/// The driver's state: the links, the fetches and the streams it serves.
struct Driver<'c, T: Transport, B: ChunkStore + Clone, H: ContentHttp> {
    content: &'c ContentClient<T, B, H>,
    links: MediaLinks<DeviceIdentity, u64>,
    fetches: HashMap<u64, Fetch>,
    tags: HashMap<FileId, u64>,
    next_tag: u64,
    streams: HashMap<DeviceIdentity, (u64, oneshot::Sender<()>)>,
    jobs: Vec<Job>,
}

impl<'c, T, B, H> Driver<'c, T, B, H>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkStore + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    fn new(content: &'c ContentClient<T, B, H>) -> Self {
        Self {
            content,
            links: MediaLinks::new(),
            fetches: HashMap::new(),
            tags: HashMap::new(),
            next_tag: 0,
            streams: HashMap::new(),
            jobs: Vec::new(),
        }
    }

    /// The earliest instant a request lapses or busy holders are asked
    /// again.
    fn next_wake(&self) -> Option<Instant> {
        self.fetches
            .values()
            .filter_map(|fetch| fetch.retry_at)
            .chain(self.links.next_deadline())
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

    /// The port ended, so no fetch can finish.
    fn end(&mut self) {
        for (_, fetch) in self.fetches.drain() {
            fetch.answer(Ok(false));
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
    async fn order(&mut self, order: Order, now: Instant) -> Vec<Action> {
        if let Some(fetch) = self
            .tags
            .get(&order.file_id)
            .and_then(|tag| self.fetches.get_mut(tag))
        {
            fetch.answers.push(order.answer);
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

    /// Lines up every linked holder for the fetch `tag` of `file_id` and asks
    /// the first.
    async fn start_round(&mut self, tag: u64, file_id: FileId, now: Instant) -> Vec<Action> {
        let authors = self.content.provisional_authors(file_id).await;
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
    /// `Want`. With none left, a round every holder answered `Busy` is tried
    /// again later within the on-demand bound, and the fetch is otherwise
    /// unavailable.
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
                return self.links.ask(&peer, want, tag, Priority::OnDemand, now);
            }
        }
        if fetch.round_busy == Some(true) && now + fetch.backoff <= fetch.progress + ON_DEMAND_BOUND
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
            Media::Want { .. } | Media::Fetch { .. } => self.release(&peer, done, now),
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
        let next = self.chunk_or_commit(tag, manifest, 0).await;
        self.links.proceed(&peer, next.map(|ask| (ask, tag)), now)
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
                self.jobs.push(Job::Commit { tag, manifest });
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
        match self
            .fetches
            .get(&tag)
            .and_then(|fetch| fetch.holder.clone())
        {
            Some(holder) => self.links.ask(&holder, ask, tag, Priority::OnDemand, now),
            None => Vec::new(),
        }
    }
}
