use std::collections::HashSet;
use std::io::{Read, Seek};

use connetto_core::Major;

use crate::ClientError;

use super::super::{
    ArchiveAttachment, ExportScope, FORMAT, Incoming, LOCAL_ROWS, MANIFEST, MAX_ATTACHMENTS_BYTES,
    PEER_CURSORS, PEER_ENTRIES, PENDING, SYNCED_ROWS, VERSION, read_error,
};
use super::{Entry, Manifest, checked_attachment_total, validate_attachment_path};

/// An open archive, which reads one declared attachment at a time.
///
/// The source stays open behind the plan the caller holds, so an attachment
/// costs its own bytes and nothing else, and an archive of attachments costs
/// the largest one.
pub(crate) struct ArchiveReader<R> {
    zip: zip::ZipArchive<R>,
}

/// Open an archive, refusing a format or version this build does not know
/// before any entry is decompressed.
pub(crate) fn open<R: Read + Seek>(source: R) -> Result<(Incoming, ArchiveReader<R>), ClientError> {
    let mut zip = zip::ZipArchive::new(source).map_err(read_error)?;
    let manifest = read_manifest(&mut zip)?;
    if manifest.format != FORMAT {
        return Err(ClientError::Import(format!(
            "the archive is a {} file, not a connetto export",
            manifest.format
        )));
    }
    if manifest.version != VERSION {
        return Err(ClientError::Import(format!(
            "the archive is version {}, and this build reads version {VERSION}",
            manifest.version
        )));
    }
    let scope = parse_scope(&manifest.scope)?;
    let declared = validate_archive_layout(&mut zip, &manifest, scope)?;
    let synced_present = declared.contains(SYNCED_ROWS);
    let local_rows = read_entry(&mut zip, LOCAL_ROWS)?;
    let pending = read_pending_queue(&mut zip)?;
    let peer_entries = read_peer_entries(&mut zip)?;
    let peer_cursors = read_peer_cursors(&mut zip)?;
    let attachments = declare_attachments(&mut zip, &manifest.entries)?;
    Ok((
        Incoming {
            scope,
            fingerprint: manifest.schema_fingerprint,
            major: manifest.major,
            account: manifest.account,
            synced_present,
            local_rows,
            pending,
            peer_entries,
            peer_cursors,
            attachments,
        },
        ArchiveReader { zip },
    ))
}

impl<R: Read + Seek> ArchiveReader<R> {
    /// Reads one attachment into `into`, replacing whatever it held.
    ///
    /// The buffer is the caller's so a walk over many attachments reuses one
    /// allocation.
    pub(crate) fn read_attachment(
        &mut self,
        path: &str,
        into: &mut Vec<u8>,
    ) -> Result<(), ClientError> {
        validate_attachment_path(path, ClientError::Import)?;
        let mut file = self.zip.by_name(path).map_err(|_| {
            ClientError::Import(format!("the archive carries no attachment at {path}"))
        })?;
        let size = file.size();
        let capacity = capacity_for(path, size)?;
        into.clear();
        into.reserve(capacity);
        file.read_to_end(into).map_err(read_error)?;
        if into.len() != capacity {
            return Err(ClientError::Import(format!(
                "archive attachment {path} declares {size} bytes and reads back {}",
                into.len()
            )));
        }
        Ok(())
    }
}

fn capacity_for(path: &str, size: u64) -> Result<usize, ClientError> {
    usize::try_from(size).map_err(|_| {
        ClientError::Import(format!(
            "archive attachment {path} does not fit this platform"
        ))
    })
}

fn read_manifest<R: Read + Seek>(zip: &mut zip::ZipArchive<R>) -> Result<Manifest, ClientError> {
    let mut entry = zip.by_name(MANIFEST).map_err(|_| {
        ClientError::Import(
            "the archive carries no manifest, so it is not a connetto export".to_owned(),
        )
    })?;
    let mut text = String::new();
    entry.read_to_string(&mut text).map_err(read_error)?;
    serde_json::from_str(&text)
        .map_err(|err| ClientError::Import(format!("the manifest does not parse: {err}")))
}

fn parse_scope(scope: &str) -> Result<ExportScope, ClientError> {
    match scope {
        "everything" => Ok(ExportScope::Everything),
        "unsynced" => Ok(ExportScope::Unsynced),
        other => Err(ClientError::Import(format!(
            "the archive names an unknown scope {other}"
        ))),
    }
}

fn read_pending_queue<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
) -> Result<Vec<super::super::ArchivedEntry>, ClientError> {
    match read_entry(zip, PENDING)? {
        Some(bytes) => decode_pending(&bytes),
        None => Ok(Vec::new()),
    }
}

/// The stored peer entries, position order, absent from an archive a build
/// without the peer tables made (R77 decision 16).
fn read_peer_entries<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
) -> Result<Vec<super::super::ArchivedPeerEntry>, ClientError> {
    match read_entry(zip, PEER_ENTRIES)? {
        Some(bytes) => decode_peer_entries(&bytes),
        None => Ok(Vec::new()),
    }
}

/// The per-author peer cursors, absent from an archive a build without the
/// peer tables made (R77 decision 16).
fn read_peer_cursors<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
) -> Result<Vec<super::super::ArchivedPeerCursor>, ClientError> {
    match read_entry(zip, PEER_CURSORS)? {
        Some(bytes) => decode_peer_cursors(&bytes),
        None => Ok(Vec::new()),
    }
}

fn validate_archive_layout<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
    manifest: &Manifest,
    scope: ExportScope,
) -> Result<HashSet<String>, ClientError> {
    if manifest.compression != "per-entry" {
        return Err(ClientError::Import(format!(
            "the archive names unsupported compression {}",
            manifest.compression
        )));
    }
    let declared = declared_paths(&manifest.entries)?;
    validate_physical_layout(stored_paths(zip)?, &declared)?;
    validate_scope(&declared, scope)?;
    Ok(declared)
}

fn declared_paths(entries: &[Entry]) -> Result<HashSet<String>, ClientError> {
    let mut declared = HashSet::new();
    for entry in entries {
        validate_entry_declaration(entry)?;
        if !declared.insert(entry.path.clone()) {
            return Err(ClientError::Import(format!(
                "the archive manifest repeats {}",
                entry.path
            )));
        }
    }
    Ok(declared)
}

fn stored_paths<R: Read + Seek>(zip: &zip::ZipArchive<R>) -> Result<HashSet<String>, ClientError> {
    let mut stored = HashSet::new();
    for name in zip.file_names() {
        let path = name.to_owned();
        if !stored.insert(path.clone()) {
            return Err(ClientError::Import(format!(
                "the archive repeats entry {path}"
            )));
        }
    }
    Ok(stored)
}

fn validate_physical_layout(
    mut stored: HashSet<String>,
    declared: &HashSet<String>,
) -> Result<(), ClientError> {
    if !stored.remove(MANIFEST) {
        return Err(ClientError::Import(
            "the archive carries no manifest".to_owned(),
        ));
    }
    for path in declared {
        if !stored.remove(path) {
            return Err(ClientError::Import(format!(
                "the archive manifest names {path}, but the entry is absent"
            )));
        }
    }
    if let Some(path) = stored.into_iter().next() {
        return Err(ClientError::Import(format!(
            "the archive entry {path} is not declared by its manifest"
        )));
    }
    Ok(())
}

fn validate_scope(declared: &HashSet<String>, scope: ExportScope) -> Result<(), ClientError> {
    if declared.contains(SYNCED_ROWS) != matches!(scope, ExportScope::Everything) {
        return Err(ClientError::Import(
            "the archive scope contradicts its synced rows entry".to_owned(),
        ));
    }
    Ok(())
}

fn validate_entry_declaration(entry: &Entry) -> Result<(), ClientError> {
    match entry.kind.as_str() {
        "rows" if matches!(entry.path.as_str(), SYNCED_ROWS | LOCAL_ROWS) => {
            require_encoding(entry, "zstd")
        }
        "pending" if entry.path == PENDING => require_encoding(entry, "zstd"),
        "peer-entries" if entry.path == PEER_ENTRIES => require_encoding(entry, "zstd"),
        "peer-cursors" if entry.path == PEER_CURSORS => require_encoding(entry, "zstd"),
        "attachment" => {
            validate_attachment_path(&entry.path, ClientError::Import)?;
            require_encoding(entry, "identity")
        }
        "rows" | "pending" | "peer-entries" | "peer-cursors" => Err(ClientError::Import(format!(
            "archive entry {} contradicts kind {}",
            entry.path, entry.kind
        ))),
        kind => Err(ClientError::Import(format!(
            "unknown archive entry kind {kind}"
        ))),
    }
}

fn require_encoding(entry: &Entry, expected: &str) -> Result<(), ClientError> {
    if entry.encoding.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(ClientError::Import(format!(
            "archive entry {} must use {expected} encoding",
            entry.path
        )))
    }
}

fn read_entry<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
    path: &str,
) -> Result<Option<Vec<u8>>, ClientError> {
    read_entry_bounded(zip, path, MAX_ATTACHMENTS_BYTES)
}

/// Reads one compressed entry, refusing output above `limit`.
///
/// The limit is a parameter so a test proves the boundary over kilobytes rather than over
/// the gigabytes the shipped ceiling names.
fn read_entry_bounded<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
    path: &str,
    limit: u64,
) -> Result<Option<Vec<u8>>, ClientError> {
    let Ok(entry) = zip.by_name(path) else {
        return Ok(None);
    };
    // The ZIP entry streams into the decoder rather than through a buffer of its own,
    // because its own compression expands unboundedly before any ceiling could apply.
    let decoder = zstd::stream::read::Decoder::new(entry)?;
    let ceiling =
        usize::try_from(limit).expect("the decompression limit fits usize on supported targets");
    let mut decompressed = Vec::new();
    // One byte past the ceiling, so an entry of exactly the ceiling is accepted and the
    // one that follows is refused.
    decoder
        .take(limit.saturating_add(1))
        .read_to_end(&mut decompressed)?;
    if decompressed.len() > ceiling {
        return Err(ClientError::Import(format!(
            "archive entry {path} exceeds the decompression limit"
        )));
    }
    Ok(Some(decompressed))
}

/// Names every attachment the archive carries and sizes it from the directory.
///
/// The per-entry and aggregate ceilings are decided here, so an archive above
/// either is refused before one attachment body is read.
fn declare_attachments<R: Read + Seek>(
    zip: &mut zip::ZipArchive<R>,
    entries: &[Entry],
) -> Result<Vec<ArchiveAttachment>, ClientError> {
    let mut attachments = Vec::new();
    let mut total = 0_u64;
    for entry in entries.iter().filter(|entry| entry.kind == "attachment") {
        let file = zip.by_name(&entry.path).map_err(read_error)?;
        if file.compression() != zip::CompressionMethod::Stored {
            return Err(ClientError::Import(format!(
                "archive attachment {} must use ZIP Stored compression",
                entry.path
            )));
        }
        let size = file.size();
        total = checked_attachment_total(&entry.path, size, total, ClientError::Import)?;
        capacity_for(&entry.path, size)?;
        attachments.push(ArchiveAttachment::from_raw(entry.path.clone(), size));
    }
    Ok(attachments)
}

pub(super) fn decode_pending(
    bytes: &[u8],
) -> Result<Vec<super::super::ArchivedEntry>, ClientError> {
    use connetto_core::messages::Provenance;

    let malformed = || ClientError::Import("the queue entry is malformed".to_owned());
    let count = u64::from_be_bytes(
        bytes
            .get(..8)
            .ok_or_else(malformed)?
            .try_into()
            .map_err(|_| malformed())?,
    );
    // Every entry costs at least its fixed 56 bytes, so a count above what the
    // entry can hold is refused before one vector per entry is allocated.
    let capacity = u64::try_from(bytes.len().saturating_sub(8) / 56).unwrap_or(u64::MAX);
    if count > capacity {
        return Err(malformed());
    }
    let mut at: usize = 8;
    let mut entries = Vec::new();
    for _ in 0..count {
        let writer = read_writer(bytes, &mut at, &malformed)?;
        let secret = read_secret(bytes, &mut at, &malformed)?;
        let seq = u64::from_be_bytes(
            bytes
                .get(at..at.checked_add(8).ok_or_else(malformed)?)
                .ok_or_else(malformed)?
                .try_into()
                .map_err(|_| malformed())?,
        );
        at = at.checked_add(8).ok_or_else(malformed)?;
        let hash: [u8; 32] = bytes
            .get(at..at.checked_add(32).ok_or_else(malformed)?)
            .ok_or_else(malformed)?
            .try_into()
            .map_err(|_| malformed())?;
        at = at.checked_add(32).ok_or_else(malformed)?;
        let major = match u32::from_be_bytes(
            bytes
                .get(at..at.checked_add(4).ok_or_else(malformed)?)
                .ok_or_else(malformed)?
                .try_into()
                .map_err(|_| malformed())?,
        ) {
            0 => None,
            raw => Some(Major::new(raw).ok_or_else(malformed)?),
        };
        at = at.checked_add(4).ok_or_else(malformed)?;
        let prov_present = *bytes.get(at).ok_or_else(malformed)?;
        at = at.checked_add(1).ok_or_else(malformed)?;
        let provenance = if prov_present == 1 {
            let prov_writer = read_writer(bytes, &mut at, &malformed)?;
            let prov_seq = u64::from_be_bytes(
                bytes
                    .get(at..at.checked_add(8).ok_or_else(malformed)?)
                    .ok_or_else(malformed)?
                    .try_into()
                    .map_err(|_| malformed())?,
            );
            at = at.checked_add(8).ok_or_else(malformed)?;
            let prov_secret = read_secret(bytes, &mut at, &malformed)?;
            Some(Provenance {
                writer: prov_writer,
                seq: prov_seq,
                secret: prov_secret,
            })
        } else if prov_present == 0 {
            None
        } else {
            return Err(malformed());
        };
        let len = usize::try_from(u64::from_be_bytes(
            bytes
                .get(at..at.checked_add(8).ok_or_else(malformed)?)
                .ok_or_else(malformed)?
                .try_into()
                .map_err(|_| malformed())?,
        ))
        .map_err(|_| malformed())?;
        at += 8;
        let changeset = bytes
            .get(at..at.checked_add(len).ok_or_else(malformed)?)
            .ok_or_else(malformed)?
            .to_vec();
        at = at.checked_add(len).ok_or_else(malformed)?;
        entries.push(super::super::ArchivedEntry {
            writer,
            seq,
            hash,
            major,
            changeset,
            secret,
            provenance,
        });
    }
    Ok(entries)
}

/// The stored peer entries behind their framing, in position order
/// (R77 decision 16).
pub(super) fn decode_peer_entries(
    bytes: &[u8],
) -> Result<Vec<super::super::ArchivedPeerEntry>, ClientError> {
    use super::super::ArchivedPeerEntry;

    let malformed = || ClientError::Import("the peer entry record is malformed".to_owned());
    let mut at: usize = 0;
    let count = u64::from_be_bytes(read_array(bytes, &mut at, &malformed)?);
    // Every entry costs at least its fixed 59 bytes, so a count above what the
    // entry can hold is refused before one vector per entry is allocated.
    if count > u64::try_from((bytes.len() - at) / 59).unwrap_or(u64::MAX) {
        return Err(malformed());
    }
    let mut entries = Vec::new();
    for _ in 0..count {
        let author = read_bytes_field(bytes, &mut at, &malformed)?;
        let seq = i64::from_be_bytes(read_array(bytes, &mut at, &malformed)?);
        let major = i64::from_be_bytes(read_array(bytes, &mut at, &malformed)?);
        let changeset = read_bytes_field(bytes, &mut at, &malformed)?;
        let account = read_text_field(bytes, &mut at, &malformed)?;
        let bot_list = match read_array::<1>(bytes, &mut at, &malformed)? {
            [0] => None,
            [1] => Some(read_text_field(bytes, &mut at, &malformed)?),
            _ => return Err(malformed()),
        };
        let received_at = i64::from_be_bytes(read_array(bytes, &mut at, &malformed)?);
        entries.push(ArchivedPeerEntry {
            author,
            seq,
            major,
            changeset,
            account,
            bot_list,
            received_at,
        });
    }
    Ok(entries)
}

/// The per-author peer cursors behind their framing (R77 decision 16).
pub(super) fn decode_peer_cursors(
    bytes: &[u8],
) -> Result<Vec<super::super::ArchivedPeerCursor>, ClientError> {
    use super::super::ArchivedPeerCursor;

    let malformed = || ClientError::Import("the peer cursor record is malformed".to_owned());
    let mut at: usize = 0;
    let count = u64::from_be_bytes(read_array(bytes, &mut at, &malformed)?);
    // Every cursor costs at least its fixed 64 bytes, so a count above what
    // the entry can hold is refused before one vector per cursor is allocated.
    if count > u64::try_from((bytes.len() - at) / 64).unwrap_or(u64::MAX) {
        return Err(malformed());
    }
    let mut cursors = Vec::new();
    for _ in 0..count {
        let author = read_bytes_field(bytes, &mut at, &malformed)?;
        let account = read_text_field(bytes, &mut at, &malformed)?;
        let seq = i64::from_be_bytes(read_array(bytes, &mut at, &malformed)?);
        let head = read_array(bytes, &mut at, &malformed)?;
        let offer = read_bytes_field(bytes, &mut at, &malformed)?;
        cursors.push(ArchivedPeerCursor {
            author,
            account,
            seq,
            head,
            offer,
        });
    }
    Ok(cursors)
}

/// The `N` bytes at `at`, advancing past them.
fn read_array<const N: usize>(
    bytes: &[u8],
    at: &mut usize,
    malformed: &impl Fn() -> ClientError,
) -> Result<[u8; N], ClientError> {
    let field = *bytes
        .get(*at..)
        .and_then(<[u8]>::first_chunk::<N>)
        .ok_or_else(malformed)?;
    *at += N;
    Ok(field)
}

/// A big-endian length-prefixed byte string at `at`, advancing past it.
fn read_bytes_field(
    bytes: &[u8],
    at: &mut usize,
    malformed: &impl Fn() -> ClientError,
) -> Result<Vec<u8>, ClientError> {
    let len = u64::from_be_bytes(read_array(bytes, at, malformed)?);
    let value = usize::try_from(len)
        .ok()
        .and_then(|len| bytes.get(*at..)?.get(..len))
        .ok_or_else(malformed)?;
    *at += value.len();
    Ok(value.to_vec())
}

/// A length-prefixed UTF-8 string at `at`, advancing past it.
fn read_text_field(
    bytes: &[u8],
    at: &mut usize,
    malformed: &impl Fn() -> ClientError,
) -> Result<String, ClientError> {
    String::from_utf8(read_bytes_field(bytes, at, malformed)?).map_err(|_| malformed())
}

/// A writer secret behind its presence byte, absent when the byte is zero.
fn read_secret(
    bytes: &[u8],
    at: &mut usize,
    malformed: &impl Fn() -> ClientError,
) -> Result<Option<connetto_core::messages::WriterSecret>, ClientError> {
    use connetto_core::messages::WriterSecret;

    let present = *bytes.get(*at).ok_or_else(malformed)?;
    *at = at.checked_add(1).ok_or_else(malformed)?;
    match present {
        0 => Ok(None),
        1 => {
            let end = at.checked_add(WriterSecret::LEN).ok_or_else(malformed)?;
            let secret: [u8; WriterSecret::LEN] = bytes
                .get(*at..end)
                .ok_or_else(malformed)?
                .try_into()
                .map_err(|_| malformed())?;
            *at = end;
            Ok(Some(WriterSecret::from_bytes(secret)))
        }
        _ => Err(malformed()),
    }
}

fn read_writer(
    bytes: &[u8],
    at: &mut usize,
    malformed: &dyn Fn() -> ClientError,
) -> Result<connetto_core::messages::WriterRef, ClientError> {
    let len = usize::try_from(u64::from_be_bytes(
        bytes
            .get(*at..at.checked_add(8).ok_or_else(malformed)?)
            .ok_or_else(malformed)?
            .try_into()
            .map_err(|_| malformed())?,
    ))
    .map_err(|_| malformed())?;
    *at += 8;
    let id = std::str::from_utf8(
        bytes
            .get(*at..at.checked_add(len).ok_or_else(malformed)?)
            .ok_or_else(malformed)?,
    )
    .map_err(|_| malformed())?;
    *at += len;
    crate::writer_from_id(id).ok_or_else(malformed)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use serde_json::json;

    use super::super::super::ExportScope;
    use super::super::super::{Archive, Incoming};
    use super::{decode_peer_cursors, decode_peer_entries, decode_pending};
    use connetto_core::Major;

    /// Opens one archive held in a buffer and keeps only what it declares.
    fn read(bytes: &[u8]) -> Result<Incoming, crate::ClientError> {
        super::open(std::io::Cursor::new(bytes)).map(|(incoming, _)| incoming)
    }

    /// One archive in a buffer, the shape the refusal cases build by hand.
    fn write(archive: &Archive<'_>) -> Result<Vec<u8>, crate::ClientError> {
        super::super::write::start(Vec::new(), archive)?.finish()
    }

    /// What an export writes, an import reads back unchanged, each queued
    /// write keeping the major it was captured under or none.
    #[test]
    fn an_archive_round_trips() {
        let number = connetto_core::messages::WriterRef::Number(4);
        let secret = |byte| {
            Some(connetto_core::messages::WriterSecret::from_bytes(
                [byte; 32],
            ))
        };
        let pending = vec![
            crate::archive::ArchivedEntry {
                writer: number,
                seq: 1,
                hash: [9u8; 32],
                major: Major::new(1),
                changeset: vec![9u8; 16],
                secret: secret(5),
                provenance: Some(connetto_core::messages::Provenance {
                    writer: connetto_core::messages::WriterRef::Number(2),
                    seq: 3,
                    secret: secret(6),
                }),
            },
            crate::archive::ArchivedEntry {
                writer: number,
                seq: 2,
                hash: [8u8; 32],
                major: None,
                changeset: vec![8u8; 4],
                secret: secret(5),
                provenance: None,
            },
            crate::archive::ArchivedEntry {
                writer: connetto_core::messages::WriterRef::Number(0),
                seq: 1,
                hash: [7u8; 32],
                major: None,
                changeset: vec![7u8; 16],
                secret: None,
                provenance: Some(connetto_core::messages::Provenance {
                    writer: connetto_core::messages::WriterRef::Number(3),
                    seq: 5,
                    secret: None,
                }),
            },
        ];
        let (peer_entries, peer_cursors) = peer_fixture();
        let archive = Archive {
            scope: ExportScope::Unsynced,
            fingerprint: "abc123".to_owned(),
            major: Major::new(2),
            account: Some("\"alice\"".to_owned()),
            synced_rows: None,
            local_rows: Some(vec![7u8; 64]),
            pending: pending.clone(),
            peer_entries: peer_entries.clone(),
            peer_cursors: peer_cursors.clone(),
            attachments: &[],
        };
        let bytes = write(&archive).expect("write");
        let read_back = read(&bytes).expect("read");
        assert_eq!(read_back.scope, ExportScope::Unsynced);
        assert_eq!(read_back.fingerprint, "abc123");
        assert_eq!(read_back.major, Major::new(2));
        assert_eq!(read_back.account.as_deref(), Some("\"alice\""));
        assert!(!read_back.synced_present);
        assert_eq!(read_back.local_rows, Some(vec![7u8; 64]));
        assert_eq!(read_back.pending, pending);
        let (expected_entries, expected_cursors) = peer_fixture();
        assert_eq!(read_back.peer_entries, expected_entries);
        assert_eq!(read_back.peer_cursors, expected_cursors);

        // The peer state rides in the other scope as well.
        let (peer_entries, peer_cursors) = peer_fixture();
        let everything = Archive {
            scope: ExportScope::Everything,
            fingerprint: "abc123".to_owned(),
            major: Major::new(2),
            account: None,
            synced_rows: Some(vec![3u8; 32]),
            local_rows: None,
            pending: Vec::new(),
            peer_entries,
            peer_cursors,
            attachments: &[],
        };
        let read_back = read(&write(&everything).expect("write")).expect("read");
        let (expected_entries, expected_cursors) = peer_fixture();
        assert_eq!(read_back.peer_entries, expected_entries);
        assert_eq!(read_back.peer_cursors, expected_cursors);
    }

    /// One author's entries and cursors the round-trip tests name.
    fn peer_fixture() -> (
        Vec<super::super::super::ArchivedPeerEntry>,
        Vec<super::super::super::ArchivedPeerCursor>,
    ) {
        use super::super::super::{ArchivedPeerCursor, ArchivedPeerEntry};

        let entries = vec![
            ArchivedPeerEntry {
                author: vec![1u8, 2],
                seq: 7,
                major: 3,
                changeset: vec![4u8, 5, 6],
                account: "peer-anna".to_owned(),
                bot_list: Some("*".to_owned()),
                received_at: 1_700_000_000,
            },
            ArchivedPeerEntry {
                author: vec![9u8],
                seq: 1,
                major: 1,
                changeset: vec![],
                account: "peer-ben".to_owned(),
                bot_list: None,
                received_at: 1_700_000_001,
            },
        ];
        let cursors = vec![
            ArchivedPeerCursor {
                author: vec![1u8, 2],
                account: "peer-anna".to_owned(),
                seq: 7,
                head: [3u8; 32],
                offer: vec![77u8, 88],
            },
            ArchivedPeerCursor {
                author: vec![9u8],
                account: "peer-ben".to_owned(),
                seq: 1,
                head: [4u8; 32],
                offer: vec![],
            },
        ];
        (entries, cursors)
    }

    /// A version 4 archive carries no capture major per write, so this build
    /// refuses it instead of guessing one.
    #[test]
    fn a_version_4_archive_is_refused() {
        let manifest = json!({
            "format": "connetto-local-data",
            "version": 4,
            "scope": "unsynced",
            "schema_fingerprint": "abc123",
            "major": 1,
            "account": null,
            "compression": "per-entry",
            "note": "test",
            "entries": [],
        });
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        zip.start_file("manifest.json", zip::write::SimpleFileOptions::default())
            .expect("start manifest");
        zip.write_all(&serde_json::to_vec(&manifest).expect("encode manifest"))
            .expect("write manifest");
        let bytes = zip.finish().expect("finish archive").into_inner();
        let error = read(&bytes).expect_err("a version 4 archive must be refused");
        assert!(
            error.to_string().contains("version 4"),
            "the refusal names the version: {error}"
        );
    }

    #[test]
    fn compressed_attachments_are_refused_before_their_body_is_read() {
        let manifest = json!({
            "format": "connetto-local-data",
            "version": super::VERSION,
            "scope": "unsynced",
            "schema_fingerprint": "abc123",
            "major": 1,
            "account": null,
            "compression": "per-entry",
            "note": "test",
            "entries": [
                {"kind": "attachment", "path": "content/chunks/abc", "encoding": "identity"}
            ],
        });
        let cursor = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let stored = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip.start_file("manifest.json", stored)
            .expect("start manifest");
        zip.write_all(&serde_json::to_vec(&manifest).expect("encode manifest"))
            .expect("write manifest");
        zip.start_file("content/chunks/abc", stored)
            .expect("start attachment");
        zip.write_all(&vec![0_u8; 64 * 1024])
            .expect("write attachment");
        let mut bytes = zip.finish().expect("finish archive").into_inner();
        set_zip_compression(&mut bytes, b"content/chunks/abc", 8);

        let error = read(&bytes).expect_err("compressed attachment");
        assert!(
            error
                .to_string()
                .contains("Compression method not supported")
        );
    }

    /// Entry declarations must exactly describe the archive payload.
    #[test]
    fn malformed_entry_declarations_are_refused() {
        assert_invalid_entries(
            &json!([{"kind": "unknown", "path": "mystery", "encoding": "identity"}]),
            &[("mystery", b"")],
            "unknown archive entry kind",
        );
        assert_invalid_entries(
            &json!([{"kind": "pending", "path": "pending.changesets", "encoding": "identity"}]),
            &[("pending.changesets", b"")],
            "must use zstd",
        );
        assert_invalid_entries(&json!([]), &[("undeclared", b"")], "not declared");
    }

    /// A file that is not one of ours is refused by name rather than parsed.
    #[test]
    fn a_foreign_file_is_refused() {
        assert!(read(b"not a zip at all").is_err());
    }

    /// An entry whose output passes the ceiling is refused, naming the entry, while an
    /// entry of exactly the ceiling is read.
    #[test]
    fn the_decompression_ceiling_is_the_boundary() {
        let limit: u64 = 64 * 1024;
        let exact = usize::try_from(limit).expect("limit fits usize");
        for (len, expected) in [(exact, None), (exact + 1, Some("decompression limit"))] {
            let bytes =
                zip_with_entry(&zstd::encode_all(vec![0u8; len].as_slice(), 1).expect("zstd"));
            let mut zip =
                zip::ZipArchive::new(std::io::Cursor::new(bytes.as_slice())).expect("zip");
            let outcome = super::read_entry_bounded(&mut zip, "device-private.patchset", limit);
            match (expected, outcome) {
                (None, Ok(Some(entry))) => {
                    assert_eq!(entry.len(), exact, "the exact ceiling reads");
                }
                (Some(needle), Err(error)) => {
                    let text = error.to_string();
                    assert!(text.contains(needle), "expected {needle} in error: {text}");
                    assert!(
                        text.contains("device-private.patchset"),
                        "expected the entry name in error: {text}"
                    );
                }
                (_, outcome) => panic!("unexpected outcome for {len} bytes: {outcome:?}"),
            }
        }
    }

    /// One archive holding a manifest and one zstd row entry, for the reader tests.
    fn zip_with_entry(entry: &[u8]) -> Vec<u8> {
        let manifest = json!({
            "format": "connetto-local-data",
            "version": super::VERSION,
            "scope": "unsynced",
            "schema_fingerprint": "abc123",
            "major": 1,
            "account": null,
            "compression": "per-entry",
            "note": "test",
            "entries": [{"kind": "rows", "path": "device-private.patchset", "encoding": "zstd"}],
        });
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("manifest.json", options)
            .expect("start manifest");
        zip.write_all(&serde_json::to_vec(&manifest).expect("manifest json"))
            .expect("write manifest");
        zip.start_file("device-private.patchset", options)
            .expect("start entry");
        zip.write_all(entry).expect("write entry");
        zip.finish().expect("finish archive").into_inner()
    }

    /// A queue entry with a length field of `u64::MAX` is refused rather than panicking.
    #[test]
    fn oversized_queue_entry_is_refused() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        let error = decode_pending(&bytes).expect_err("overflowing queue entry must be refused");
        assert!(
            error.to_string().contains("malformed"),
            "expected 'malformed' in error: {error}"
        );
    }

    #[test]
    fn a_truncated_capture_major_is_refused() {
        let entry = crate::archive::ArchivedEntry {
            writer: connetto_core::messages::WriterRef::Number(u64::MAX),
            seq: 1,
            hash: [7; 32],
            major: Major::new(1),
            changeset: vec![8],
            secret: None,
            provenance: None,
        };
        let mut bytes = super::super::write::encode_pending(std::slice::from_ref(&entry));
        assert_eq!(
            decode_pending(&bytes)
                .expect("a valid capture major")
                .as_slice(),
            std::slice::from_ref(&entry)
        );
        // Count, writer length and id, the secret's presence byte, sequence
        // and hash precede the major.
        let major_at = 8 + 8 + crate::writer_id(&entry.writer).len() + 1 + 8 + 32;
        for kept in (0..4).rev() {
            bytes.truncate(major_at + kept);
            assert!(matches!(
                decode_pending(&bytes),
                Err(crate::ClientError::Import(_))
            ));
        }
    }

    /// A writer secret behind a presence byte other than 0 or 1, or cut
    /// short, is refused rather than read as the fields after it.
    #[test]
    fn a_malformed_writer_secret_is_refused() {
        use connetto_core::messages::WriterSecret;

        // The longest writer id, so a cut inside the secret still passes the
        // entry count check and reaches the secret's read.
        let entry = crate::archive::ArchivedEntry {
            writer: connetto_core::messages::WriterRef::Number(u64::MAX),
            seq: 1,
            hash: [7; 32],
            major: Major::new(1),
            changeset: vec![8],
            secret: Some(WriterSecret::from_bytes([5; WriterSecret::LEN])),
            provenance: None,
        };
        let bytes = super::super::write::encode_pending(std::slice::from_ref(&entry));
        assert_eq!(
            decode_pending(&bytes)
                .expect("a valid writer secret")
                .as_slice(),
            std::slice::from_ref(&entry)
        );
        // Count, then the writer's length and id, precede the presence byte.
        let presence_at = 8 + 8 + crate::writer_id(&entry.writer).len();
        let mut unknown = bytes.clone();
        unknown[presence_at] = 2;
        let mut cut = bytes;
        cut.truncate(presence_at + WriterSecret::LEN);
        for malformed in [unknown, cut] {
            assert!(matches!(
                decode_pending(&malformed),
                Err(crate::ClientError::Import(_))
            ));
        }
    }

    /// A peer entry record cut short at any byte, counting more entries than
    /// it holds, carrying an account that is not text or a presence byte the
    /// framing does not name, is refused rather than read as the fields
    /// after it.
    #[test]
    fn a_malformed_peer_entry_record_is_refused() {
        use super::super::super::ArchivedPeerEntry;

        let entry = ArchivedPeerEntry {
            author: vec![1u8, 2],
            seq: 7,
            major: 3,
            changeset: vec![4u8, 5, 6],
            account: "peer-anna".to_owned(),
            bot_list: Some("*".to_owned()),
            received_at: 1_700_000_000,
        };
        let bytes = super::super::write::encode_peer_entries(std::slice::from_ref(&entry));
        assert_eq!(
            decode_peer_entries(&bytes).expect("a valid peer entry"),
            std::slice::from_ref(&entry).to_vec()
        );
        // The bot-list presence byte, the count and the fields preceding it.
        let presence_at = 8 + 8 + 2 + 8 + 8 + 8 + 3 + 8 + 10;
        let mut unknown = bytes.clone();
        unknown[presence_at] = 2;
        // The account's first byte, made invalid UTF-8.
        let mut not_text = bytes.clone();
        not_text[presence_at - 9] = 0xFF;
        // A count above what the record can hold.
        let mut overcount = bytes.clone();
        overcount[..8].copy_from_slice(&2u64.to_be_bytes());
        let cuts = (0..bytes.len()).map(|len| bytes[..len].to_vec());
        for malformed in [unknown, not_text, overcount].into_iter().chain(cuts) {
            assert!(matches!(
                decode_peer_entries(&malformed),
                Err(crate::ClientError::Import(_))
            ));
        }
    }

    /// A peer cursor record cut short at any byte, its head included,
    /// counting more cursors than it holds or carrying an account that is not
    /// text, is refused rather than read shorter.
    #[test]
    fn a_malformed_peer_cursor_record_is_refused() {
        use super::super::super::ArchivedPeerCursor;

        let cursor = ArchivedPeerCursor {
            author: vec![1u8, 2],
            account: "peer-anna".to_owned(),
            seq: 7,
            head: [3u8; 32],
            offer: vec![77u8],
        };
        let bytes = super::super::write::encode_peer_cursors(std::slice::from_ref(&cursor));
        assert_eq!(
            decode_peer_cursors(&bytes).expect("a valid peer cursor"),
            std::slice::from_ref(&cursor).to_vec()
        );
        // The account's first byte, made invalid UTF-8.
        let mut not_text = bytes.clone();
        not_text[8 + 8 + 2 + 8] = 0xFF;
        // A count above what the record can hold.
        let mut overcount = bytes.clone();
        overcount[..8].copy_from_slice(&2u64.to_be_bytes());
        let cuts = (0..bytes.len()).map(|len| bytes[..len].to_vec());
        for malformed in [not_text, overcount].into_iter().chain(cuts) {
            assert!(matches!(
                decode_peer_cursors(&malformed),
                Err(crate::ClientError::Import(_))
            ));
        }
    }

    /// A version 6 archive carries no peer state, so this build refuses it
    /// instead of guessing the cursors it did not record.
    #[test]
    fn a_version_6_archive_is_refused() {
        let manifest = json!({
            "format": "connetto-local-data",
            "version": 6,
            "scope": "unsynced",
            "schema_fingerprint": "abc123",
            "major": 1,
            "account": null,
            "compression": "per-entry",
            "note": "test",
            "entries": [],
        });
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        zip.start_file("manifest.json", zip::write::SimpleFileOptions::default())
            .expect("start manifest");
        zip.write_all(&serde_json::to_vec(&manifest).expect("encode manifest"))
            .expect("write manifest");
        let bytes = zip.finish().expect("finish archive").into_inner();
        let error = read(&bytes).expect_err("a version 6 archive must be refused");
        assert!(
            error.to_string().contains("version 6"),
            "the refusal names the version: {error}"
        );
    }

    /// An attachment above the per-entry ceiling is refused from what the
    /// directory declares, so the bytes it claims never have to exist.
    ///
    /// The archive here is a few hundred bytes long and declares an entry of
    /// 256 MiB plus one, which is the only honest way to test a ceiling that
    /// large.
    #[test]
    fn an_attachment_above_the_entry_ceiling_is_refused_without_reading_it() {
        let declaration = super::super::super::ArchiveAttachment::new("content/chunks/big", 5)
            .expect("attachment declaration");
        let mut export = super::super::write::start(
            Vec::new(),
            &Archive {
                scope: ExportScope::Unsynced,
                fingerprint: "abc123".to_owned(),
                major: Major::new(1),
                account: None,
                synced_rows: None,
                local_rows: None,
                pending: Vec::new(),
                peer_entries: Vec::new(),
                peer_cursors: Vec::new(),
                attachments: std::slice::from_ref(&declaration),
            },
        )
        .expect("start");
        export
            .write_attachment("content/chunks/big", b"small")
            .expect("attachment body");
        let mut bytes = export.finish().expect("finish");
        let claimed = super::super::super::MAX_ATTACHMENT_BYTES + 1;
        set_zip_uncompressed_size(&mut bytes, b"content/chunks/big", claimed);
        assert!(
            bytes.len() < 4096,
            "the archive stays small, which is what makes the refusal meaningful"
        );

        let error = read(&bytes).expect_err("an oversized entry must be refused");
        let text = error.to_string();
        assert!(
            text.contains("content/chunks/big") && text.contains(&claimed.to_string()),
            "the refusal names the entry and its size: {text}"
        );
    }

    fn assert_invalid_entries(
        entries: &serde_json::Value,
        files: &[(&str, &[u8])],
        expected: &str,
    ) {
        let manifest = json!({
            "format": "connetto-local-data",
            "version": super::VERSION,
            "scope": "unsynced",
            "schema_fingerprint": "abc123",
            "major": 1,
            "account": null,
            "compression": "per-entry",
            "note": "test",
            "entries": entries,
        });
        let cursor = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("manifest.json", options)
            .expect("start manifest");
        zip.write_all(&serde_json::to_vec(&manifest).expect("encode manifest"))
            .expect("write manifest");
        for (path, bytes) in files {
            zip.start_file(path, options).expect("start entry");
            zip.write_all(bytes).expect("write entry");
        }
        let bytes = zip.finish().expect("finish archive").into_inner();
        let error = read(&bytes).expect_err("malformed entries must be refused");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?} in {error}"
        );
    }

    fn set_zip_compression(bytes: &mut [u8], path: &[u8], method: u16) {
        let method = method.to_le_bytes();
        let positions: Vec<_> = bytes
            .windows(path.len())
            .enumerate()
            .filter_map(|(at, value)| (value == path).then_some(at))
            .collect();
        for at in positions {
            if at >= 30 && &bytes[at - 30..at - 26] == b"PK\x03\x04" {
                bytes[at - 22..at - 20].copy_from_slice(&method);
            } else if at >= 46 && &bytes[at - 46..at - 42] == b"PK\x01\x02" {
                bytes[at - 36..at - 34].copy_from_slice(&method);
            }
        }
    }

    /// Rewrites one entry's declared uncompressed size in both headers.
    fn set_zip_uncompressed_size(bytes: &mut [u8], path: &[u8], size: u64) {
        let size = u32::try_from(size).unwrap_or(u32::MAX).to_le_bytes();
        let positions: Vec<_> = bytes
            .windows(path.len())
            .enumerate()
            .filter_map(|(at, value)| (value == path).then_some(at))
            .collect();
        for at in positions {
            if at >= 30 && &bytes[at - 30..at - 26] == b"PK\x03\x04" {
                bytes[at - 8..at - 4].copy_from_slice(&size);
            } else if at >= 46 && &bytes[at - 46..at - 42] == b"PK\x01\x02" {
                bytes[at - 22..at - 18].copy_from_slice(&size);
            }
        }
    }
}
