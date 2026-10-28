//! The `files/` part of a download (R61 step 4): the files the person's rows
//! name, read from the content store chunk by chunk and verified as they pass.

use std::collections::HashSet;

use bytes::Bytes;
use diesel_async::AsyncPgConnection;
use serde::Serialize;

/// The raw 32-byte BLAKE3 identity a file column holds.
pub(super) type FileKey = [u8; 32];

/// The error reading one chunk from the content store.
#[cfg(feature = "content")]
pub type ChunkReadError = connetto_file_server::StoreError;

/// The error reading one chunk from the content store, which a server built
/// without the `content` feature never reads.
#[cfg(not(feature = "content"))]
pub type ChunkReadError = std::convert::Infallible;

/// One chunk of a file's committed manifest, in order.
pub(super) struct ChunkRef {
    pub(super) hash: [u8; 32],
    pub(super) len: u64,
}

/// A file's committed manifest being read, borrowing the connection.
#[cfg(feature = "content")]
type ManifestRead<'c> = std::pin::Pin<
    Box<dyn Future<Output = Result<Option<Vec<ChunkRef>>, diesel::result::Error>> + Send + 'c>,
>;

/// Reads a file's committed manifest under the deployment's file schema.
#[cfg(feature = "content")]
type ManifestLoader = for<'c> fn(&'c mut AsyncPgConnection, FileKey) -> ManifestRead<'c>;

/// The content store a download reads file bytes from, with the loader of
/// the deployment's file manifests.
#[cfg(feature = "content")]
pub(crate) struct FileSource {
    store: connetto_file_server::AnyStore,
    load: ManifestLoader,
}

/// The content store a download reads file bytes from, which a server built
/// without the `content` feature never has.
#[cfg(not(feature = "content"))]
pub(crate) enum FileSource {}

#[cfg(feature = "content")]
impl FileSource {
    /// Read file bytes from `store` and manifests through the file schema `S`.
    pub(crate) fn new<S: connetto_file_server::ConnettoFileSchema>(
        store: connetto_file_server::AnyStore,
    ) -> Self {
        Self {
            store,
            load: load_manifest::<S>,
        }
    }

    /// The chunks of `file`'s committed manifest, `None` when it has none.
    pub(super) async fn manifest(
        &self,
        conn: &mut AsyncPgConnection,
        file: FileKey,
    ) -> Result<Option<Vec<ChunkRef>>, diesel::result::Error> {
        (self.load)(conn, file).await
    }

    /// One chunk's bytes.
    pub(super) async fn chunk(&self, hash: [u8; 32]) -> Result<Bytes, ChunkReadError> {
        self.store
            .read(&connetto_file_core::ChunkHash::from_bytes(hash))
            .await
    }
}

#[cfg(not(feature = "content"))]
impl FileSource {
    pub(super) async fn manifest(
        &self,
        _conn: &mut AsyncPgConnection,
        _file: FileKey,
    ) -> Result<Option<Vec<ChunkRef>>, diesel::result::Error> {
        match *self {}
    }

    pub(super) async fn chunk(&self, _hash: [u8; 32]) -> Result<Bytes, ChunkReadError> {
        match *self {}
    }
}

#[cfg(feature = "content")]
fn load_manifest<S: connetto_file_server::ConnettoFileSchema>(
    conn: &mut AsyncPgConnection,
    file: FileKey,
) -> ManifestRead<'_> {
    Box::pin(async move {
        let id = connetto_file_core::FileId::from_bytes(file);
        let manifest = connetto_file_server::load_any_committed_manifest::<S>(conn, &id).await?;
        Ok(manifest.map(|manifest| {
            manifest
                .chunks()
                .iter()
                .map(|chunk| ChunkRef {
                    hash: *chunk.hash.as_bytes(),
                    len: chunk.len,
                })
                .collect()
        }))
    })
}

/// A file the rows name, with the name of the first row naming it.
pub(super) struct FileRef {
    pub(super) id: FileKey,
    pub(super) name: Option<String>,
}

/// Why a file the rows name is not in `files/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MissingReason {
    /// No committed manifest: the upload never finished, or the store lost
    /// the bytes.
    NotUploaded,
    /// The file's first chunk could not be read.
    Unreadable,
    /// The file's first chunk did not match its recorded hash or length.
    Corrupt,
    /// The column held something other than a 32-byte file id.
    NotAFileId,
}

/// A file listed under `missing_files`.
#[derive(Serialize)]
pub(super) struct MissingFile {
    pub(super) file_id: String,
    pub(super) reason: MissingReason,
}

/// A file written under `files/`.
#[derive(Serialize)]
pub(super) struct PlacedFile {
    pub(super) file_id: String,
    pub(super) path: String,
}

/// The files the rows name, each once and in the order first named.
#[derive(Default)]
pub(super) struct FileRefs {
    pub(super) files: Vec<FileRef>,
    seen: HashSet<FileKey>,
    pub(super) malformed: Vec<MissingFile>,
}

impl FileRefs {
    /// Note the file a row's file cell names, under the row's name cell.
    ///
    /// A cell that is not 32 bytes is listed as no file id, under its hex
    /// when it holds bytes at all.
    pub(super) fn note(
        &mut self,
        cell: &super::archive::Cell,
        name: Option<&super::archive::Cell>,
    ) {
        use super::archive::Cell;
        let id = match cell {
            Cell::Null => return,
            Cell::Blob(bytes) => FileKey::try_from(bytes.as_slice()).map_err(|_| hex(bytes)),
            Cell::Text(_) | Cell::Integer(_) | Cell::Real(_) => Err(String::new()),
        };
        let id = match id {
            Ok(id) => id,
            Err(file_id) => {
                self.malformed.push(MissingFile {
                    file_id,
                    reason: MissingReason::NotAFileId,
                });
                return;
            }
        };
        if self.seen.insert(id) {
            let name = match name {
                Some(Cell::Text(name)) => Some(name.clone()),
                _ => None,
            };
            self.files.push(FileRef { id, name });
        }
    }
}

/// Lowercase hex of `bytes`.
pub(super) fn hex(bytes: &[u8]) -> String {
    use core::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// The entry names given so far, so no two files share one.
#[derive(Default)]
pub(super) struct EntryNames {
    /// Lowercased, since a case-insensitive file system unpacks `A.jpg` and
    /// `a.jpg` onto one file.
    taken: HashSet<String>,
}

impl EntryNames {
    /// The entry name under `files/` for `id`, from `name` when it holds one.
    ///
    /// A name loses every path separator and control character and its
    /// leading dots, so no entry escapes `files/` or hides itself. A name
    /// another file already took gains the head of this file's id before its
    /// extension, then the whole id, then a counter after the id, so every
    /// name returned is one no earlier file holds.
    pub(super) fn place(&mut self, id: &FileKey, name: Option<&str>) -> String {
        let id_hex = hex(id);
        let base = name
            .map(clean)
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| id_hex.clone());
        if self.taken.insert(base.to_lowercase()) {
            return base;
        }
        let (stem, extension) = match base.rfind('.') {
            Some(dot) if dot > 0 => base.split_at(dot),
            _ => (base.as_str(), ""),
        };
        let short = format!("{stem} ({}){extension}", &id_hex[..8]);
        if self.taken.insert(short.to_lowercase()) {
            return short;
        }
        let whole = format!("{stem} ({id_hex}){extension}");
        if self.taken.insert(whole.to_lowercase()) {
            return whole;
        }
        // Each taken candidate adds one name, so the count of names taken
        // bounds the search.
        let mut n = 2_usize;
        loop {
            let numbered = format!("{stem} ({id_hex} {n}){extension}");
            if self.taken.insert(numbered.to_lowercase()) {
                return numbered;
            }
            n += 1;
        }
    }
}

/// `name` with every control character dropped, every path separator
/// replaced, and its surrounding whitespace and leading dots removed.
fn clean(name: &str) -> String {
    let replaced: String = name
        .chars()
        .filter(|c| !c.is_control())
        .map(|c| {
            if matches!(c, '/' | '\\' | ':') {
                '_'
            } else {
                c
            }
        })
        .collect();
    replaced.trim().trim_start_matches('.').trim().to_owned()
}

/// Whether `bytes` are the chunk `chunk` records.
pub(super) fn verifies(bytes: &[u8], chunk: &ChunkRef) -> bool {
    bytes.len() as u64 == chunk.len && *blake3::hash(bytes).as_bytes() == chunk.hash
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: FileKey = [0xab; 32];
    const C: FileKey = [0xcd; 32];
    const E: FileKey = [0xef; 32];
    /// Shares A's first four bytes, so the two short forms coincide.
    const A2: FileKey = {
        let mut id = [0x01; 32];
        id[0] = 0xab;
        id[1] = 0xab;
        id[2] = 0xab;
        id[3] = 0xab;
        id
    };

    /// Names keep a person's titles, never leave `files/`, and never let two
    /// files unpack onto one path.
    #[test]
    fn entry_names_keep_titles_stay_inside_and_never_collide() {
        let mut names = EntryNames::default();
        assert_eq!(names.place(&A, Some("beach.jpg")), "beach.jpg");
        assert_eq!(names.place(&C, Some("Beach.jpg")), "Beach (cdcdcdcd).jpg");
        assert_eq!(names.place(&E, None), hex(&E));
        let mut names = EntryNames::default();
        assert_eq!(names.place(&A, Some("../../etc/passwd")), "_.._etc_passwd");
        assert_eq!(names.place(&C, Some(".hidden")), "hidden");
        assert_eq!(names.place(&E, Some("  \u{7}  ")), hex(&E));
        let mut names = EntryNames::default();
        assert_eq!(names.place(&A, Some("notes")), "notes");
        assert_eq!(
            names.place(&C, Some("notes (abababab)")),
            "notes (abababab)"
        );
        assert_eq!(
            names.place(&A2, Some("notes")),
            format!("notes ({})", hex(&A2)),
            "a short form another file took falls back to the whole id"
        );
    }

    /// Titles the person controls can spell every fallback in advance, the
    /// bare id of an untitled file included, and the files still land on
    /// distinct names.
    #[test]
    fn entry_names_stay_distinct_when_titles_spell_the_fallbacks() {
        let mut names = EntryNames::default();
        let whole = format!("notes ({})", hex(&A2));
        let mut placed = vec![
            names.place(&A, Some("notes")),
            names.place(&C, Some("notes (abababab)")),
            names.place(&E, Some(&whole)),
            names.place(&A2, Some("notes")),
            names.place(&[0x11; 32], Some(&hex(&[0x22; 32]))),
            names.place(&[0x22; 32], None),
        ];
        let count = placed.len();
        placed.sort_by_key(|name| name.to_lowercase());
        placed.dedup_by_key(|name| name.to_lowercase());
        assert_eq!(placed.len(), count, "two files share a name: {placed:?}");
    }
}
