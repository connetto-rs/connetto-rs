# connetto-file-core

[![Tests](https://github.com/connetto-rs/connetto-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/connetto-rs/connetto-rs/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/connetto-rs/connetto-rs/blob/main/LICENSE)
[![docs.rs](https://docs.rs/connetto-file-core/badge.svg)](https://docs.rs/connetto-file-core)
[![crates.io](https://img.shields.io/crates/v/connetto-file-core.svg)](https://crates.io/crates/connetto-file-core)

Platform-neutral file core for connetto-rs. A file's identity is the BLAKE3 hash of its raw bytes, computed in the same pass as `FastCDC` chunking. The manifest records the ordered (hash, length) pairs for each chunk. The `ChunkStore` trait addresses storage by plaintext chunk hash; `EncryptingStore` wraps any store and adds per-chunk zstd compression followed by XChaCha20-Poly1305 encryption, transparently to callers of `process_file` and `reassemble`.

```rust
use connetto_file_core::{
    MimeClass, MemStore, EncryptingStore, process_file, reassemble,
};

# futures_executor::block_on(async {
let data: Vec<u8> = b"ATGCATGCATGCATGCATGC".iter().copied().cycle().take(512).collect();
let root_key = [0u8; 32];
let store = EncryptingStore::new(MemStore::new(), &root_key);

let manifest = process_file(&data, MimeClass::Fasta, &store).await.unwrap();
let recovered = reassemble(&manifest, &store).await.unwrap();
assert_eq!(data, recovered);
# });
```

The same pass can build the file's `Outboard`, the parent hashes of BLAKE3's tree over 16 KiB groups, with which `encode_range` streams any byte range of a stored file and `decode_range` accepts it only as the bytes the file id names.

```rust
use connetto_file_core::{
    MemStore, MimeClass, decode_range, encode_range, process_file_with_outboard,
};

# futures_executor::block_on(async {
let data: Vec<u8> = (0..100_000u32).flat_map(u32::to_le_bytes).collect();
let store = MemStore::new();
let (manifest, outboard) = process_file_with_outboard(data.as_slice(), MimeClass::Generic, &store)
    .await
    .unwrap();

let mut encoded = Vec::new();
encode_range(&manifest, &store, &outboard, 50_000, 1_000, &mut encoded).await.unwrap();
let size = u64::try_from(data.len()).unwrap();
let bytes = decode_range(manifest.file_id(), size, 50_000, 1_000, encoded.as_slice()).await.unwrap();
assert_eq!(bytes, &data[50_000..51_000]);
# });
```
