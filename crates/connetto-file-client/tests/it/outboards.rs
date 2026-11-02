//! Every held file's BLAKE3 outboard: written with the chunks it proves,
//! computed again when its object is gone, and evicted with its manifest
//! (R79 decision 4).

use connetto_file_client::{ContentClient, FsStore};
use connetto_file_core::{ChunkHash, MimeClass, OutboardBuilder};
use tempfile::tempdir;

use crate::staging::walk_files;
use crate::support::{
    RecordingHttp, Scripted, attach_content, connected_client, insert_row_and_pin_album,
    learn_file_id, offline_content, stage_photo,
};

const INTENT_URL: &str = "https://files.test/files/aa/intent?t=TOKEN";
const READ_URL: &str = "https://files.test/files/ab?t=TOKEN";

fn pseudorandom(seed: u32, len: usize) -> Vec<u8> {
    let mut state = seed.max(1);
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// The parent hashes `bytes` have, built straight from the bytes.
fn pairs_of(bytes: &[u8]) -> Vec<u8> {
    let mut builder = OutboardBuilder::new();
    builder.update(bytes);
    builder.finish().into_pairs()
}

/// Where the store keeps the object holding `pairs`.
fn object_path(chunks: &std::path::Path, pairs: &[u8]) -> std::path::PathBuf {
    let hex = ChunkHash::from_data(pairs).to_string();
    chunks.join(&hex[..2]).join(&hex)
}

async fn outboard_pairs(
    content: &ContentClient<Scripted, FsStore, RecordingHttp>,
    file_id: connetto_file_core::FileId,
) -> Vec<u8> {
    content
        .outboard(file_id)
        .await
        .expect("read the outboard")
        .expect("a held file has an outboard")
        .into_pairs()
}

#[tokio::test]
async fn staging_stores_the_outboard_beside_the_chunks() {
    let dir = tempdir().expect("temp dir");
    let (_client, content) = offline_content(dir.path()).await;
    let bytes = pseudorandom(3, 6 * 1024 * 1024);
    let file_id = stage_photo(&content, 1, &bytes, MimeClass::Generic).await;

    let expected = pairs_of(&bytes);
    assert!(
        object_path(&dir.path().join("chunks"), &expected).exists(),
        "the stage wrote the outboard with the chunks"
    );
    let outboard = content
        .outboard(file_id)
        .await
        .expect("read the outboard")
        .expect("a held file has an outboard");
    assert_eq!(outboard.file_id(), file_id);
    assert_eq!(outboard.size(), u64::try_from(bytes.len()).expect("fits"));
    assert_eq!(outboard.pairs(), expected.as_slice());
}

#[tokio::test]
async fn a_file_of_one_group_stores_no_outboard_object() {
    let dir = tempdir().expect("temp dir");
    let (_client, content) = offline_content(dir.path()).await;
    let file_id = stage_photo(&content, 1, b"a small photo", MimeClass::Jpeg).await;
    assert_eq!(
        walk_files(&dir.path().join("chunks")).len(),
        1,
        "one chunk and no outboard object"
    );
    assert_eq!(outboard_pairs(&content, file_id).await, Vec::<u8>::new());
}

#[tokio::test]
async fn a_lost_outboard_object_is_computed_again_and_kept() {
    let dir = tempdir().expect("temp dir");
    let chunks = dir.path().join("chunks");
    let (_client, content) = offline_content(dir.path()).await;
    let bytes = pseudorandom(5, 40 * 1024);
    let file_id = stage_photo(&content, 1, &bytes, MimeClass::Generic).await;
    let expected = pairs_of(&bytes);
    let object = object_path(&chunks, &expected);
    std::fs::remove_file(&object).expect("lose the outboard object");

    assert_eq!(outboard_pairs(&content, file_id).await, expected);
    assert!(object.exists(), "the computed outboard is stored again");
}

#[tokio::test]
async fn a_file_not_held_has_no_outboard() {
    let dir = tempdir().expect("temp dir");
    let (_client, content) = offline_content(dir.path()).await;
    let absent = learn_file_id(b"a photo this device never saw").await;
    assert!(
        content
            .outboard(absent)
            .await
            .expect("read the outboard")
            .is_none()
    );
}

#[tokio::test]
async fn a_pinned_fetch_stores_the_outboard() {
    let dir = tempdir().expect("temp dir");
    let chunks = dir.path().join("chunks");
    let bytes = pseudorandom(7, 100 * 1024);
    let file_id = learn_file_id(&bytes).await;
    let http = RecordingHttp::new(vec![(200, bytes.clone())]);
    let client = connected_client(
        &dir.path().join("replica.sqlite"),
        Scripted::granting(READ_URL),
    )
    .await;
    let content = attach_content(client.clone(), &chunks, http).await;
    insert_row_and_pin_album(&client, &content, file_id).await;
    content.fetch_pinned().await.expect("fetch the pinned file");

    let expected = pairs_of(&bytes);
    assert!(object_path(&chunks, &expected).exists());
    assert_eq!(outboard_pairs(&content, file_id).await, expected);
}

#[tokio::test]
async fn tidy_takes_an_evicted_files_outboard_and_keeps_a_pinned_ones() {
    let dir = tempdir().expect("temp dir");
    let chunks = dir.path().join("chunks");
    let replies = vec![
        (200, br#"{"needed":[]}"#.to_vec()),
        (200, Vec::new()),
        (200, br#"{"needed":[]}"#.to_vec()),
        (200, Vec::new()),
    ];
    let client = connected_client(
        &dir.path().join("replica.sqlite"),
        Scripted::granting(INTENT_URL),
    )
    .await;
    let content = attach_content(client, &chunks, RecordingHttp::new(replies)).await;
    let evicted_bytes = pseudorandom(11, 50 * 1024);
    let kept_bytes = pseudorandom(13, 60 * 1024);
    stage_photo(&content, 1, &evicted_bytes, MimeClass::Generic).await;
    let kept = stage_photo(&content, 2, &kept_bytes, MimeClass::Generic).await;
    assert_eq!(content.flush_outbox().await.expect("walk the outbox"), 2);
    content
        .pin_content(
            "kept",
            "SELECT content_id FROM photos WHERE id = 2",
            "content_id",
        )
        .await
        .expect("pin the second photo");

    assert_eq!(content.tidy_content().await.expect("tidy"), 1);
    assert!(
        !object_path(&chunks, &pairs_of(&evicted_bytes)).exists(),
        "the evicted file's outboard went with its chunks"
    );
    let kept_pairs = pairs_of(&kept_bytes);
    assert!(
        object_path(&chunks, &kept_pairs).exists(),
        "the sweep keeps the outboard a manifest still has"
    );
    assert_eq!(outboard_pairs(&content, kept).await, kept_pairs);
}
