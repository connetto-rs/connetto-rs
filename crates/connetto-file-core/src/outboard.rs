//! A file's BLAKE3 outboard and the byte ranges it proves against the file id.
//!
//! [`FileId`] is the root of BLAKE3's own hash tree, so the parent hashes of
//! that tree over 16 KiB groups, kept beside the chunks, let a holder send any
//! byte range with the hashes that prove it and let a receiver check each group
//! against the file id as it arrives. The encoding is `bao-tree`'s, which is
//! bao's wire format with chunk groups.

use std::io;

use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext, encode_ranges_validated};
use bao_tree::io::outboard::PostOrderMemOutboard;
use bao_tree::io::{BaoContentItem, DecodeError, EncodeError, Leaf, round_up_to_chunks};
use bao_tree::{BaoTree, BlockSize, ByteRanges, ChunkRanges};
use blake3::hazmat::{
    ChainingValue, HasherExt, Mode, merge_subtrees_non_root, merge_subtrees_root,
};
use bytes::Bytes;
use iroh_io::{AsyncSliceReader, AsyncStreamReader, AsyncStreamWriter};
use thiserror::Error;

use crate::identity::FileId;
use crate::manifest::Manifest;
use crate::store::ChunkStore;

/// Sixteen BLAKE3 chunks of 1 KiB, the group one outboard pair covers.
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

const GROUP_BYTES: usize = 16 * 1024;

/// The parent hashes of a file's BLAKE3 tree over 16 KiB groups, in post order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outboard {
    file_id: FileId,
    size: u64,
    pairs: Vec<u8>,
}

impl Outboard {
    /// Rebuilds an outboard from stored parent hashes, `None` when `pairs` is
    /// not the length a file of `size` bytes has.
    #[must_use]
    pub fn new(file_id: FileId, size: u64, pairs: Vec<u8>) -> Option<Self> {
        (u64::try_from(pairs.len()).ok() == Some(BaoTree::new(size, BLOCK_SIZE).outboard_size()))
            .then_some(Self {
                file_id,
                size,
                pairs,
            })
    }

    /// The file this outboard proves.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.file_id
    }

    /// The file's length in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// The parent hash pairs, 64 bytes each, in post order.
    #[must_use]
    pub fn pairs(&self) -> &[u8] {
        &self.pairs
    }

    /// The parent hash pairs, for storing.
    #[must_use]
    pub fn into_pairs(self) -> Vec<u8> {
        self.pairs
    }

    fn tree(&self) -> PostOrderMemOutboard<&[u8]> {
        PostOrderMemOutboard {
            root: blake3::Hash::from_bytes(*self.file_id.as_bytes()),
            tree: BaoTree::new(self.size, BLOCK_SIZE),
            data: self.pairs.as_slice(),
        }
    }
}

/// Builds a file's outboard and identity from its bytes in file order.
///
/// A group is hashed only once a later byte arrives, since the group ending
/// the file may be the root and BLAKE3 finalizes a root differently.
#[derive(Debug, Default)]
pub struct OutboardBuilder {
    group: Vec<u8>,
    groups: u64,
    stack: Vec<ChainingValue>,
    pairs: Vec<u8>,
    size: u64,
}

impl OutboardBuilder {
    /// Starts an empty file.
    #[must_use]
    pub fn new() -> Self {
        Self {
            group: Vec::with_capacity(GROUP_BYTES),
            ..Self::default()
        }
    }

    /// Adds the next bytes in file order.
    pub fn update(&mut self, mut data: &[u8]) {
        self.size = self
            .size
            .saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
        while !data.is_empty() {
            if self.group.len() == GROUP_BYTES {
                self.push_group();
            }
            let take = (GROUP_BYTES - self.group.len()).min(data.len());
            self.group.extend_from_slice(&data[..take]);
            data = &data[take..];
        }
    }

    /// Finishes the file, its identity being the outboard's root.
    #[must_use]
    pub fn finish(mut self) -> Outboard {
        if self.groups == 0 {
            return Outboard {
                file_id: FileId::from_bytes(*blake3::hash(&self.group).as_bytes()),
                size: self.size,
                pairs: Vec::new(),
            };
        }
        self.push_group();
        // A file of two or more groups leaves at least two subtrees, folded from the top.
        let mut right = self.stack.pop().unwrap_or_default();
        let mut root = blake3::Hash::from_bytes(right);
        while let Some(left) = self.stack.pop() {
            self.pairs.extend_from_slice(&left);
            self.pairs.extend_from_slice(&right);
            if self.stack.is_empty() {
                root = merge_subtrees_root(&left, &right, Mode::Hash);
            } else {
                right = merge_subtrees_non_root(&left, &right, Mode::Hash);
            }
        }
        Outboard {
            file_id: FileId::from_bytes(*root.as_bytes()),
            size: self.size,
            pairs: self.pairs,
        }
    }

    fn push_group(&mut self) {
        let offset = self.groups * u64::try_from(GROUP_BYTES).expect("16 KiB fits in u64");
        let cv = blake3::Hasher::new()
            .set_input_offset(offset)
            .update(&self.group)
            .finalize_non_root();
        let complete =
            usize::try_from(self.groups.count_ones()).expect("a bit count fits in usize");
        while self.stack.len() > complete {
            self.merge_top();
        }
        self.stack.push(cv);
        self.groups += 1;
        self.group.clear();
    }

    fn merge_top(&mut self) {
        let right = self.stack.pop().expect("a right subtree");
        let left = self.stack.pop().expect("a left subtree");
        self.pairs.extend_from_slice(&left);
        self.pairs.extend_from_slice(&right);
        self.stack
            .push(merge_subtrees_non_root(&left, &right, Mode::Hash));
    }
}

/// A failure to build, serve or check a byte range of a stored file.
#[derive(Debug, Error)]
pub enum RangeError<E: std::error::Error + Send + Sync + 'static> {
    /// The chunk store failed to read a chunk.
    #[error(transparent)]
    Store(E),
    /// The stored bytes or outboard do not hash to the file id.
    #[error("the stored bytes do not hash to the file id")]
    Mismatch,
    /// The range is empty or runs past the end of the file.
    #[error("the range is empty or runs past the end of the file")]
    OutOfBounds,
    /// The encoding could not be written.
    #[error("writing the encoded range failed: {0}")]
    Write(io::Error),
}

/// A failure to verify an encoded range against a file id.
#[derive(Debug, Error)]
pub enum DecodeRangeError {
    /// The range is empty or runs past the end of the file.
    #[error("the range is empty or runs past the end of the file")]
    OutOfBounds,
    /// A piece did not hash into the file id, or the encoding ended early.
    #[error("the encoded range does not verify against the file id: {0}")]
    Verify(DecodeError),
}

/// Computes the outboard of a stored file, reading every chunk once.
///
/// # Errors
///
/// [`RangeError::Store`] when a chunk read fails, and [`RangeError::Mismatch`]
/// when the stored bytes do not hash to the manifest's file id.
pub async fn outboard_of<S: ChunkStore>(
    manifest: &Manifest,
    store: &S,
) -> Result<Outboard, RangeError<S::Error>> {
    let mut builder = OutboardBuilder::new();
    for meta in manifest.chunks() {
        let bytes = store
            .read_chunk(&meta.hash)
            .await
            .map_err(RangeError::Store)?;
        builder.update(&bytes);
    }
    let outboard = builder.finish();
    if outboard.file_id != manifest.file_id() {
        return Err(RangeError::Mismatch);
    }
    Ok(outboard)
}

/// Writes the bao encoding of `len` bytes from `start` of a stored file.
///
/// The encoding covers whole 16 KiB groups around the range, and every group
/// read from the store is checked against the outboard before it is written.
///
/// # Errors
///
/// [`RangeError::OutOfBounds`] for an empty range or one past the end,
/// [`RangeError::Mismatch`] when the store or the outboard disagree with the
/// file id, [`RangeError::Store`] on a chunk read failure and
/// [`RangeError::Write`] when `encoded` refuses the bytes.
pub async fn encode_range<S, W>(
    manifest: &Manifest,
    store: &S,
    outboard: &Outboard,
    start: u64,
    len: u64,
    encoded: W,
) -> Result<(), RangeError<S::Error>>
where
    S: ChunkStore,
    W: AsyncStreamWriter,
{
    let size = manifest.chunks().iter().map(|meta| meta.len).sum::<u64>();
    if outboard.file_id != manifest.file_id() || outboard.size != size {
        return Err(RangeError::Mismatch);
    }
    let ranges = byte_range(size, start, len).ok_or(RangeError::OutOfBounds)?;
    let mut reader = ManifestReader::new(manifest, store);
    let result = encode_ranges_validated(&mut reader, outboard.tree(), &ranges, encoded).await;
    if let Some(failure) = reader.failure.take() {
        return Err(failure);
    }
    result.map_err(|err| match err {
        EncodeError::Io(err) => RangeError::Write(err),
        EncodeError::ParentWrite(_) | EncodeError::LeafWrite(_) => {
            RangeError::Write(io::ErrorKind::BrokenPipe.into())
        }
        EncodeError::ParentHashMismatch(_)
        | EncodeError::LeafHashMismatch(_)
        | EncodeError::SizeMismatch => RangeError::Mismatch,
    })
}

/// Reads and verifies the bao encoding of `len` bytes from `start` of the file
/// `file_id`, `size` bytes long, answering exactly those bytes.
///
/// # Errors
///
/// [`DecodeRangeError::OutOfBounds`] for an empty range or one past the end,
/// and [`DecodeRangeError::Verify`] when a piece fails its hash or the
/// encoding ends before the range does.
pub async fn decode_range<R: AsyncStreamReader>(
    file_id: FileId,
    size: u64,
    start: u64,
    len: u64,
    encoded: R,
) -> Result<Vec<u8>, DecodeRangeError> {
    let ranges = byte_range(size, start, len).ok_or(DecodeRangeError::OutOfBounds)?;
    let end = start + len;
    let mut out = vec![0u8; usize::try_from(len).map_err(|_| DecodeRangeError::OutOfBounds)?];
    let mut covered = 0u64;
    let root = blake3::Hash::from_bytes(*file_id.as_bytes());
    let mut decoder = ResponseDecoder::new(root, ranges, BaoTree::new(size, BLOCK_SIZE), encoded);
    while let ResponseDecoderNext::More((next, item)) = decoder.next().await {
        if let BaoContentItem::Leaf(Leaf { offset, data }) =
            item.map_err(DecodeRangeError::Verify)?
        {
            covered += copy_overlap(&mut out, start, end, offset, &data);
        }
        decoder = next;
    }
    if covered != len {
        return Err(DecodeRangeError::Verify(DecodeError::LeafNotFound(
            bao_tree::ChunkNum::full_chunks(start + covered),
        )));
    }
    Ok(out)
}

/// The chunk ranges covering `len` bytes from `start`, or `None` when empty or
/// past the end.
fn byte_range(size: u64, start: u64, len: u64) -> Option<ChunkRanges> {
    let end = start
        .checked_add(len)
        .filter(|end| len > 0 && *end <= size)?;
    Some(round_up_to_chunks(&ByteRanges::from(start..end)))
}

/// Copies the part of a leaf at `offset` that falls in `start..end`, answering
/// how many bytes it copied.
fn copy_overlap(out: &mut [u8], start: u64, end: u64, offset: u64, data: &Bytes) -> u64 {
    let leaf_end = offset + u64::try_from(data.len()).expect("a leaf length fits in u64");
    let from = start.max(offset);
    let to = end.min(leaf_end);
    if from >= to {
        return 0;
    }
    let source = usize::try_from(from - offset).expect("inside one leaf");
    let target = usize::try_from(from - start).expect("inside the range");
    let count = usize::try_from(to - from).expect("inside one leaf");
    out[target..target + count].copy_from_slice(&data[source..source + count]);
    to - from
}

/// A stored file read by offset, one decrypted chunk held at a time.
struct ManifestReader<'a, S: ChunkStore> {
    manifest: &'a Manifest,
    store: &'a S,
    starts: Vec<u64>,
    size: u64,
    held: Option<(usize, Bytes)>,
    failure: Option<RangeError<S::Error>>,
}

impl<'a, S: ChunkStore> ManifestReader<'a, S> {
    fn new(manifest: &'a Manifest, store: &'a S) -> Self {
        let mut starts = Vec::with_capacity(manifest.chunks().len());
        let mut size = 0u64;
        for meta in manifest.chunks() {
            starts.push(size);
            size += meta.len;
        }
        Self {
            manifest,
            store,
            starts,
            size,
            held: None,
            failure: None,
        }
    }

    async fn chunk(&mut self, index: usize) -> io::Result<Bytes> {
        if let Some((held, bytes)) = &self.held
            && *held == index
        {
            return Ok(bytes.clone());
        }
        let meta = &self.manifest.chunks()[index];
        let bytes = match self.store.read_chunk(&meta.hash).await {
            Ok(bytes) => bytes,
            Err(err) => {
                self.failure = Some(RangeError::Store(err));
                return Err(io::Error::other("the chunk store failed a read"));
            }
        };
        if u64::try_from(bytes.len()).ok() != Some(meta.len) {
            self.failure = Some(RangeError::Mismatch);
            return Err(io::Error::other("a stored chunk has the wrong length"));
        }
        let bytes = Bytes::from(bytes);
        self.held = Some((index, bytes.clone()));
        Ok(bytes)
    }
}

impl<S: ChunkStore> AsyncSliceReader for ManifestReader<'_, S> {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let end = offset
            .saturating_add(u64::try_from(len).unwrap_or(u64::MAX))
            .min(self.size);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let mut index = self.starts.partition_point(|start| *start <= offset) - 1;
        let mut out: Option<Vec<u8>> = None;
        let mut at = offset;
        while at < end {
            let chunk = self.chunk(index).await?;
            let chunk_start = self.starts[index];
            let chunk_end = chunk_start + self.manifest.chunks()[index].len;
            let from = usize::try_from(at - chunk_start).expect("inside the chunk");
            let to = usize::try_from(end.min(chunk_end) - chunk_start).expect("inside the chunk");
            if out.is_none() && end <= chunk_end {
                return Ok(chunk.slice(from..to));
            }
            out.get_or_insert_with(|| {
                Vec::with_capacity(usize::try_from(end - offset).unwrap_or(0))
            })
            .extend_from_slice(&chunk[from..to]);
            at = chunk_end.min(end);
            index += 1;
        }
        Ok(Bytes::from(out.unwrap_or_default()))
    }

    fn size(&mut self) -> impl Future<Output = io::Result<u64>> {
        std::future::ready(Ok(self.size))
    }
}
