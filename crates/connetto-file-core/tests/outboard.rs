//! The outboard and verified range streaming, checked against `bao-tree` and
//! BLAKE3 themselves. Native-only, since proptest forks for persistence.

#![cfg(not(target_arch = "wasm32"))]

use bao_tree::BlockSize;
use bao_tree::io::outboard::PostOrderMemOutboard;
use connetto_file_core::{
    ChunkHash, ChunkStore, EncryptingStore, FileId, Manifest, MemStore, MimeClass, Outboard,
    OutboardBuilder, RangeError, decode_range, encode_range, outboard_of,
    process_file_with_outboard,
};
use iroh_io::AsyncStreamWriter;
use proptest::prelude::*;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("single-thread runtime")
}

/// Deterministic bytes that do not compress, drawn from BLAKE3's extendable
/// output.
fn random_bytes(seed: u32, len: usize) -> Vec<u8> {
    let mut out = vec![0; len];
    blake3::Hasher::new()
        .update(&seed.to_le_bytes())
        .finalize_xof()
        .fill(&mut out);
    out
}

fn len_of(data: &[u8]) -> u64 {
    u64::try_from(data.len()).expect("length fits in u64")
}

/// A file and a byte range inside it, the range never empty.
fn file_and_range() -> impl Strategy<Value = (Vec<u8>, u64, u64)> {
    (1usize..=200_000, any::<u32>())
        .prop_flat_map(|(len, seed)| {
            (Just(random_bytes(seed, len)), 0..len)
                .prop_flat_map(move |(data, start)| (Just(data), Just(start), 1..=len - start))
        })
        .prop_map(|(data, start, len)| {
            (
                data,
                u64::try_from(start).expect("fits"),
                u64::try_from(len).expect("fits"),
            )
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn builder_matches_bao_tree_and_blake3(
        data in proptest::collection::vec(any::<u8>(), 0..=150_000),
        cuts in proptest::collection::vec(1usize..40_000, 0..12),
    ) {
        let mut builder = OutboardBuilder::new();
        let mut rest = data.as_slice();
        for cut in cuts {
            let (head, tail) = rest.split_at(cut.min(rest.len()));
            builder.update(head);
            rest = tail;
        }
        builder.update(rest);
        let outboard = builder.finish();
        let expected = PostOrderMemOutboard::create(&data, BlockSize::from_chunk_log(4));
        prop_assert_eq!(outboard.file_id(), FileId::from_bytes(*blake3::hash(&data).as_bytes()));
        prop_assert_eq!(outboard.size(), len_of(&data));
        prop_assert_eq!(outboard.pairs(), expected.data.as_slice());
    }

    #[test]
    fn every_range_decodes_against_the_file_id((data, start, len) in file_and_range()) {
        let store = EncryptingStore::new(MemStore::new(), &[3u8; 32]);
        let (manifest, outboard) = rt()
            .block_on(process_file_with_outboard(data.as_slice(), MimeClass::Generic, &store))
            .unwrap();
        let mut encoded = Vec::new();
        rt().block_on(encode_range(&manifest, &store, &outboard, start, len, &mut encoded)).unwrap();
        let decoded = rt()
            .block_on(decode_range(manifest.file_id(), len_of(&data), start, len, encoded.as_slice()))
            .unwrap();
        let from = usize::try_from(start).unwrap();
        let to = from + usize::try_from(len).unwrap();
        prop_assert_eq!(decoded.as_slice(), &data[from..to]);
    }
}

#[test]
fn staging_outboard_is_the_one_the_stored_chunks_give() {
    let data = random_bytes(7, 5 * 1024 * 1024);
    let store = EncryptingStore::new(MemStore::new(), &[1u8; 32]);
    let (manifest, staged) = rt()
        .block_on(process_file_with_outboard(
            data.as_slice(),
            MimeClass::Generic,
            &store,
        ))
        .unwrap();
    assert!(
        manifest.chunks().len() > 1,
        "the file must span several chunks"
    );
    assert_eq!(staged.file_id(), manifest.file_id());
    let computed = rt().block_on(outboard_of(&manifest, &store)).unwrap();
    assert_eq!(computed, staged);
}

/// A stored file of several chunks, its manifest and its outboard.
fn several_chunks(seed: u32) -> (Vec<u8>, MemStore, Manifest, Outboard) {
    let data = random_bytes(seed, 5 * 1024 * 1024);
    let store = MemStore::new();
    let (manifest, outboard) = rt()
        .block_on(process_file_with_outboard(
            data.as_slice(),
            MimeClass::Generic,
            &store,
        ))
        .unwrap();
    assert!(
        manifest.chunks().len() > 1,
        "the file must span several chunks"
    );
    (data, store, manifest, outboard)
}

#[test]
fn ranges_across_chunk_boundaries_decode() {
    let (data, store, manifest, outboard) = several_chunks(23);
    let first = manifest.chunks()[0].len;
    for (start, len) in [(0, len_of(&data)), (first - 1000, 5000), (first, 1)] {
        let mut encoded = Vec::new();
        rt().block_on(encode_range(
            &manifest,
            &store,
            &outboard,
            start,
            len,
            &mut encoded,
        ))
        .unwrap();
        let decoded = rt()
            .block_on(decode_range(
                manifest.file_id(),
                len_of(&data),
                start,
                len,
                encoded.as_slice(),
            ))
            .unwrap();
        let from = usize::try_from(start).unwrap();
        assert_eq!(decoded.as_slice(), &data[from..from + decoded.len()]);
        assert_eq!(len_of(&decoded), len);
    }
}

#[test]
fn a_missing_chunk_is_a_store_error_on_both_reads() {
    let (_, store, manifest, outboard) = several_chunks(29);
    rt().block_on(store.delete_chunk(&manifest.chunks()[1].hash))
        .unwrap();
    let mut encoded = Vec::new();
    let served = rt().block_on(encode_range(
        &manifest,
        &store,
        &outboard,
        0,
        outboard.size(),
        &mut encoded,
    ));
    assert!(matches!(served, Err(RangeError::Store(_))), "{served:?}");
    assert!(matches!(
        rt().block_on(outboard_of(&manifest, &store)),
        Err(RangeError::Store(_))
    ));
}

#[test]
fn a_stored_chunk_of_another_length_is_a_mismatch() {
    let (data, store, manifest, outboard) = several_chunks(31);
    let hash = manifest.chunks()[0].hash;
    rt().block_on(store.write_chunk(&hash, &data[..100]))
        .unwrap();
    let mut encoded = Vec::new();
    let served = rt().block_on(encode_range(
        &manifest,
        &store,
        &outboard,
        0,
        10,
        &mut encoded,
    ));
    assert!(matches!(served, Err(RangeError::Mismatch)), "{served:?}");
}

#[test]
fn another_files_outboard_is_a_mismatch() {
    let (_, store, manifest, _) = several_chunks(37);
    let (_, _, _, other) = several_chunks(41);
    let mut encoded = Vec::new();
    let served = rt().block_on(encode_range(&manifest, &store, &other, 0, 10, &mut encoded));
    assert!(matches!(served, Err(RangeError::Mismatch)), "{served:?}");
}

#[test]
fn stored_pairs_rebuild_an_outboard_only_at_the_files_length() {
    let (_, _, manifest, outboard) = several_chunks(43);
    let size = outboard.size();
    let pairs = outboard.clone().into_pairs();
    assert_eq!(
        Outboard::new(manifest.file_id(), size, pairs.clone()),
        Some(outboard)
    );
    let mut longer = pairs;
    longer.extend_from_slice(&[0; 64]);
    assert_eq!(Outboard::new(manifest.file_id(), size, longer), None);
}

/// A sink that refuses every write with one error kind, as a link that went
/// away.
struct Refusing(std::io::ErrorKind);

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the trait methods are async and the refusal needs no await"
)]
impl AsyncStreamWriter for Refusing {
    async fn write(&mut self, _data: &[u8]) -> std::io::Result<()> {
        Err(self.0.into())
    }

    async fn write_bytes(&mut self, _data: bytes::Bytes) -> std::io::Result<()> {
        Err(self.0.into())
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_refusing_sink_is_a_write_error() {
    let (_, store, manifest, outboard) = several_chunks(47);
    for kind in [
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::ConnectionReset,
    ] {
        let served = rt().block_on(encode_range(
            &manifest,
            &store,
            &outboard,
            0,
            70_000,
            Refusing(kind),
        ));
        assert!(matches!(served, Err(RangeError::Write(_))), "{served:?}");
    }
}

#[test]
fn a_forged_byte_is_refused() {
    let data = random_bytes(11, 300_000);
    let store = MemStore::new();
    let (manifest, outboard) = rt()
        .block_on(process_file_with_outboard(
            data.as_slice(),
            MimeClass::Generic,
            &store,
        ))
        .unwrap();
    let mut encoded = Vec::new();
    rt().block_on(encode_range(
        &manifest,
        &store,
        &outboard,
        40_000,
        100_000,
        &mut encoded,
    ))
    .unwrap();
    let last = encoded.len() - 1;
    encoded[last] ^= 1;
    let refused = rt().block_on(decode_range(
        manifest.file_id(),
        len_of(&data),
        40_000,
        100_000,
        encoded.as_slice(),
    ));
    assert!(refused.is_err(), "a flipped byte must not decode");
}

#[test]
fn a_range_against_another_file_id_is_refused() {
    let data = random_bytes(13, 70_000);
    let store = MemStore::new();
    let (manifest, outboard) = rt()
        .block_on(process_file_with_outboard(
            data.as_slice(),
            MimeClass::Generic,
            &store,
        ))
        .unwrap();
    let mut encoded = Vec::new();
    rt().block_on(encode_range(
        &manifest,
        &store,
        &outboard,
        0,
        70_000,
        &mut encoded,
    ))
    .unwrap();
    let other = FileId::from_bytes([9u8; 32]);
    let refused = rt().block_on(decode_range(other, 70_000, 0, 70_000, encoded.as_slice()));
    assert!(
        refused.is_err(),
        "bytes of one file must not decode as another"
    );
}

#[test]
fn a_store_holding_other_bytes_is_caught_on_both_ends() {
    let data = random_bytes(17, 90_000);
    let store = MemStore::new();
    let (manifest, outboard) = rt()
        .block_on(process_file_with_outboard(
            data.as_slice(),
            MimeClass::Generic,
            &store,
        ))
        .unwrap();
    let hash: ChunkHash = manifest.chunks()[0].hash;
    let mut tampered = data.clone();
    tampered[45_000] ^= 1;
    rt().block_on(store.write_chunk(&hash, &tampered)).unwrap();
    assert!(matches!(
        rt().block_on(outboard_of(&manifest, &store)),
        Err(RangeError::Mismatch)
    ));
    let mut encoded = Vec::new();
    let served = rt().block_on(encode_range(
        &manifest,
        &store,
        &outboard,
        0,
        90_000,
        &mut encoded,
    ));
    assert!(matches!(served, Err(RangeError::Mismatch)), "{served:?}");
}

#[test]
fn a_range_past_the_end_is_refused() {
    let data = random_bytes(19, 20_000);
    let store = MemStore::new();
    let (manifest, outboard) = rt()
        .block_on(process_file_with_outboard(
            data.as_slice(),
            MimeClass::Generic,
            &store,
        ))
        .unwrap();
    let mut encoded = Vec::new();
    let served = rt().block_on(encode_range(
        &manifest,
        &store,
        &outboard,
        19_000,
        2_000,
        &mut encoded,
    ));
    assert!(matches!(served, Err(RangeError::OutOfBounds)), "{served:?}");
}
