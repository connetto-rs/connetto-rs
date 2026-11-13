//! The receiver's side: the candidate order, the prefetch reading, the
//! verified chunk writes and the commit (R79 decisions 3, 4, 5, 8 and 10).

use core::fmt::Display;
use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::media::Priority;
use connetto_file_core::{
    ChunkHash, ChunkMeta, ChunkStore, FileId, FreeSpace, Manifest, MaybeSend, decode_range,
    outboard_of,
};
use iroh_io::AsyncStreamReader;
use tokio::sync::{mpsc, oneshot};

use super::super::{ContentClient, ContentEvent, FETCHED_CLASS, Keeper};
use super::custody;
use crate::db;
use crate::error::ContentError;
use crate::http::ContentHttp;

/// How long an on-demand fetch keeps retrying holders that all answered
/// `Busy`, counted from its start or its last verified chunk.
pub(super) const ON_DEMAND_BOUND: Duration = Duration::from_secs(30);

/// The first wait before asking busy holders again.
pub(super) const FIRST_BACKOFF: Duration = Duration::from_secs(5);

/// The longest wait before asking busy holders again.
pub(super) const LAST_BACKOFF: Duration = Duration::from_mins(5);

/// The largest file a prefetch fetches, so a misdeclared column never pulls
/// a video unasked (R79 decision 8).
pub(super) const MAX_PREFETCH_BYTES: u64 = 1024 * 1024;

/// A file `resolve` hands the media driver, with where to answer whether its
/// bytes arrived.
pub(in crate::client) struct Order {
    /// The file to fetch.
    pub(super) file_id: FileId,
    /// Where the outcome goes.
    pub(super) answer: oneshot::Sender<Result<bool, ContentError>>,
}

/// One file the driver fetches, and every resolve waiting on it.
pub(super) struct Fetch {
    /// The file.
    pub(super) file_id: FileId,
    /// The resolves waiting, the first one taking a failure's own error.
    pub(super) answers: Vec<oneshot::Sender<Result<bool, ContentError>>>,
    /// The holders left to ask in this round, in order.
    pub(super) candidates: VecDeque<DeviceIdentity>,
    /// The holder asked now.
    pub(super) holder: Option<DeviceIdentity>,
    /// Where the fetch stands with that holder.
    pub(super) stage: Stage,
    /// Bumped at every new holder, so a chunk job of an abandoned holder is
    /// recognised when it ends.
    pub(super) generation: u64,
    /// `Some(true)` while every holder of this round answered `Busy`.
    pub(super) round_busy: Option<bool>,
    /// The next wait before asking busy holders again.
    pub(super) backoff: Duration,
    /// When busy holders are asked again.
    pub(super) retry_at: Option<Instant>,
    /// The fetch's start, or its last verified chunk.
    pub(super) progress: Instant,
    /// What the fetch is for.
    pub(super) purpose: Purpose,
}

/// What a fetch is for, which sets its queue, its holders and its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Purpose {
    /// A resolve waits on it, every linked holder asked and busy ones
    /// asked again within the on-demand bound.
    OnDemand,
    /// A thumbnail asked of its author alone, behind every on-demand ask and
    /// dropped rather than retried.
    Prefetch,
    /// A copy placement takes into custody (R79 decision 14), behind every
    /// on-demand ask and dropped rather than retried, since the next
    /// evaluation asks again.
    Custody,
    /// Only the manifest, which tells placement the file's size, then
    /// dropped.
    Size,
}

impl Fetch {
    /// An on-demand fetch of `file_id` started at `now`.
    pub(super) fn new(file_id: FileId, now: Instant) -> Self {
        Self {
            file_id,
            answers: Vec::new(),
            candidates: VecDeque::new(),
            holder: None,
            stage: Stage::Idle,
            generation: 0,
            round_busy: None,
            backoff: FIRST_BACKOFF,
            retry_at: None,
            progress: now,
            purpose: Purpose::OnDemand,
        }
    }

    /// The priority its asks queue at.
    pub(super) fn priority(&self) -> Priority {
        if self.purpose == Purpose::OnDemand {
            Priority::OnDemand
        } else {
            Priority::Prefetch
        }
    }

    /// Answers every waiting resolve with `outcome`.
    pub(super) fn answer(self, outcome: Result<bool, ContentError>) {
        let mut answers = self.answers.into_iter();
        let shared = match &outcome {
            Ok(fetched) => Ok(*fetched),
            Err(err) => Err(err.to_string()),
        };
        if let Some(first) = answers.next() {
            let _ = first.send(outcome);
        }
        for answer in answers {
            let _ = answer.send(shared.clone().map_err(ContentError::Store));
        }
    }
}

/// Where a fetch stands with its holder.
pub(super) enum Stage {
    /// Between holders, or waiting to ask busy ones again.
    Idle,
    /// A `Want` sent, its manifest rows arriving.
    Wanting(Vec<ChunkMeta>),
    /// A `Fetch` sent for the chunk at `chunk`, its pieces going to the
    /// chunk job until the last one.
    Fetching {
        /// The holder's manifest.
        manifest: Manifest,
        /// The chunk asked for.
        chunk: usize,
        /// Where the pieces go.
        pieces: Option<mpsc::UnboundedSender<Vec<u8>>>,
    },
    /// Every chunk is in and the commit runs.
    Committing,
}

/// What a chunk job settled.
pub(super) enum ChunkOutcome {
    /// The chunk verified and is in the store.
    Written,
    /// A piece did not verify against the file id, or the chunk does not
    /// hash to its manifest row.
    Forged,
    /// The store refused the write.
    Store(String),
}

/// The holders to ask for a file, the linked authors of the provisional rows
/// naming it first in their rows' order, then every other linked peer in the
/// order its link came up.
pub(super) fn candidate_order<'a>(
    linked: impl IntoIterator<Item = &'a DeviceIdentity>,
    authors: &[Vec<u8>],
) -> VecDeque<DeviceIdentity> {
    let linked: Vec<&DeviceIdentity> = linked.into_iter().collect();
    let mut order = VecDeque::with_capacity(linked.len());
    for author in authors {
        if let Some(peer) = linked
            .iter()
            .find(|peer| peer.key().as_bytes().as_slice() == author.as_slice())
            && !order.contains(*peer)
        {
            order.push_back((*peer).clone());
        }
    }
    for peer in linked {
        if !order.contains(peer) {
            order.push_back(peer.clone());
        }
    }
    order
}

/// A manifest a holder sent, `None` when a row cannot be a chunk.
pub(super) fn holders_manifest(file_id: FileId, rows: Vec<ChunkMeta>) -> Option<Manifest> {
    rows.iter()
        .all(|row| (1..=connetto_exchange::media::MAX_FETCH_LEN).contains(&row.len))
        .then(|| Manifest::new(file_id, rows))
}

impl<T, B, H> ContentClient<T, B, H>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkStore + FreeSpace + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    /// The file's provisional authors, by key id, in the order their rows
    /// name it through the declared columns.
    pub(super) async fn provisional_authors(&self, file_id: FileId) -> Vec<Vec<u8>> {
        let declared = &self.peer_content;
        self.client
            .with_conn(|conn| {
                let mut authors = Vec::new();
                for declared in declared {
                    for blob in conn
                        .provisional_blobs(&declared.table, &declared.column)
                        .unwrap_or_default()
                    {
                        if blob.value.as_slice() == file_id.as_bytes().as_slice()
                            && !authors.contains(&blob.author)
                        {
                            authors.push(blob.author);
                        }
                    }
                }
                authors
            })
            .await
            .unwrap_or_default()
    }

    /// The files the prefetch columns of `author`'s provisional rows name, in
    /// row order, skipping those the device already holds.
    pub(super) async fn prefetches_of(&self, author: &[u8]) -> Vec<FileId> {
        let declared = &self.peer_content;
        let named: Vec<FileId> = self
            .client
            .with_conn(|conn| {
                let mut named = Vec::new();
                for declared in declared.iter().filter(|declared| declared.prefetch) {
                    for blob in conn
                        .provisional_blobs(&declared.table, &declared.column)
                        .unwrap_or_default()
                    {
                        if blob.author == author
                            && let Ok(id) = <[u8; 32]>::try_from(blob.value.as_slice())
                            && !named.contains(&FileId::from_bytes(id))
                        {
                            named.push(FileId::from_bytes(id));
                        }
                    }
                }
                named
            })
            .await
            .unwrap_or_default();
        let mut lacking = Vec::with_capacity(named.len());
        for file_id in named {
            if !matches!(self.local_answer(file_id).await, Ok(Some(_))) {
                lacking.push(file_id);
            }
        }
        lacking
    }

    /// The first chunk of `manifest` from `from` the store lacks, with its
    /// offset in the file.
    pub(super) async fn first_missing(
        &self,
        manifest: &Manifest,
        from: usize,
    ) -> Result<Option<(usize, u64)>, ContentError> {
        let store = self.store_for(FETCHED_CLASS);
        let mut offset = 0;
        for (index, chunk) in manifest.chunks().iter().enumerate() {
            if index >= from
                && !store
                    .has_chunk(&chunk.hash)
                    .await
                    .map_err(|err| ContentError::Store(err.to_string()))?
            {
                return Ok(Some((index, offset)));
            }
            offset += chunk.len;
        }
        Ok(None)
    }

    /// Verifies the pieces of `chunk`, at `start` of the file `file_id` of
    /// `size` bytes, against the file id as they arrive, checks the chunk
    /// against its manifest row, and writes it.
    pub(super) async fn take_chunk(
        &self,
        file_id: FileId,
        size: u64,
        start: u64,
        chunk: ChunkMeta,
        pieces: mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> ChunkOutcome {
        let arriving = Arriving {
            pieces,
            buffer: Bytes::new(),
        };
        let Ok(bytes) = decode_range(file_id, size, start, chunk.len, arriving).await else {
            return ChunkOutcome::Forged;
        };
        if ChunkHash::from_data(&bytes) != chunk.hash {
            return ChunkOutcome::Forged;
        }
        let _writing = self.content_writes.lock().await;
        match self
            .store_for(FETCHED_CLASS)
            .write_chunk(&chunk.hash, &bytes)
            .await
        {
            Ok(()) => ChunkOutcome::Written,
            Err(err) => ChunkOutcome::Store(err.to_string()),
        }
    }

    /// Commits a peer-fetched file once every chunk is in the store, `false`
    /// when a chunk went missing before the commit could hold them. A file a
    /// replicated column of a provisional row names enters custody when it
    /// fits in the room `budget` leaves, and is cache otherwise. A fetch that
    /// `takes_over` a leaving copy also keeps a file a settled row names
    /// while no state reads `available`.
    pub(super) async fn commit_fetched(
        &self,
        manifest: &Manifest,
        budget: u64,
        takes_over: bool,
    ) -> Result<bool, ContentError> {
        let _writing = self.content_writes.lock().await;
        let store = self.store_for(FETCHED_CLASS);
        for chunk in manifest.chunks() {
            if !store
                .has_chunk(&chunk.hash)
                .await
                .map_err(|err| ContentError::Store(err.to_string()))?
            {
                return Ok(false);
            }
        }
        let outboard = outboard_of(manifest, &store)
            .await
            .map_err(|err| ContentError::Store(err.to_string()))?;
        let outboard = self.store_outboard(&outboard).await?;
        let custody = self.custody_author(manifest, budget, takes_over).await?;
        self.commit_cached(manifest, outboard, custody.as_ref())
            .await?;
        if custody.is_some() {
            self.custody_changed.notify_one();
        }
        let _ = self.events.send(ContentEvent::Fetched {
            file_id: manifest.file_id(),
        });
        Ok(true)
    }

    /// Whom custody keeps `manifest`'s file for when it fits in the room
    /// `budget` leaves, the courierable author of a provisional row whose
    /// replicated column names it, or a settled row when the fetch
    /// `takes_over` a leaving copy of a file no state reads `available` for,
    /// `None` for cache.
    async fn custody_author(
        &self,
        manifest: &Manifest,
        budget: u64,
        takes_over: bool,
    ) -> Result<Option<Keeper>, ContentError> {
        let declared = &self.peer_content;
        let file_id = manifest.file_id();
        let (author, held) = self
            .client
            .with_conn(|conn| -> Result<_, ContentError> {
                let named = custody::named(conn, declared, &mut custody::Reach::default())?;
                let author = match named.author_of(file_id) {
                    Some(author) => Some(Keeper::Author(author.to_vec())),
                    None if takes_over
                        && !db::is_unsent(conn.conn(), file_id)?
                        && custody::settled_open(conn, declared, file_id)? =>
                    {
                        Some(Keeper::Settled)
                    }
                    None => None,
                };
                Ok((author, db::custody_bytes(conn.conn())?))
            })
            .await??;
        let size: u64 = manifest.chunks().iter().map(|chunk| chunk.len).sum();
        Ok(author.filter(|_| size <= self.room(budget, held)))
    }
}

/// The encoded range as its pieces arrive, ending when the sender drops.
struct Arriving {
    pieces: mpsc::UnboundedReceiver<Vec<u8>>,
    buffer: Bytes,
}

impl Arriving {
    /// Fills the buffer with the next non-empty piece, `false` at the end.
    async fn fill(&mut self) -> bool {
        while self.buffer.is_empty() {
            match self.pieces.recv().await {
                Some(piece) => self.buffer = Bytes::from(piece),
                None => return false,
            }
        }
        true
    }
}

impl AsyncStreamReader for Arriving {
    /// Up to `len` bytes, fewer only at the end, since the decoder reads a
    /// leaf with `read_bytes_exact` and a leaf can span two pieces.
    async fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        if !self.fill().await {
            return Ok(Bytes::new());
        }
        if self.buffer.len() >= len {
            return Ok(self.buffer.split_to(len));
        }
        let mut joined = BytesMut::with_capacity(len);
        while joined.len() < len && self.fill().await {
            let take = (len - joined.len()).min(self.buffer.len());
            joined.extend_from_slice(&self.buffer.split_to(take));
        }
        Ok(joined.freeze())
    }

    async fn read<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        let mut out = [0; L];
        let mut filled = 0;
        while filled < L {
            if !self.fill().await {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let take = (L - filled).min(self.buffer.len());
            out[filled..filled + take].copy_from_slice(&self.buffer.split_to(take));
            filled += take;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use connetto_core::device_cert::DeviceIdentity;

    use super::candidate_order;

    fn peer(key: u8) -> DeviceIdentity {
        DeviceIdentity::from_uri(&format!(
            "connetto://00000000-0000-0000-0000-00000000004f/account/anna/device/{}",
            format!("{key:02x}").repeat(32)
        ))
        .expect("a canonical identity")
    }

    #[test]
    fn linked_authors_go_first_in_row_order_then_every_link_in_link_order() {
        let linked = [peer(1), peer(2), peer(3), peer(4)];
        let authors = [vec![3; 32], vec![9; 32], vec![2; 32], vec![3; 32]];
        assert_eq!(
            candidate_order(&linked, &authors),
            [peer(3), peer(2), peer(1), peer(4)]
        );
    }

    #[test]
    fn with_no_author_linked_the_link_order_stands() {
        let linked = [peer(1), peer(2)];
        assert_eq!(candidate_order(&linked, &[vec![7; 32]]), [peer(1), peer(2)]);
    }
}
