//! A staged file stays in the client's unsynced work until its upload
//! commits, and a refused upload stays there, refused, until a retry.

use std::io;
use std::time::Duration;

use connetto_client::{FileState, LiveUnsynced, UnsyncedFile, UnsyncedWork};
use connetto_file_client::{ContentClient, FsStore};
use connetto_file_core::MimeClass;
use tempfile::tempdir;

use crate::support::{ROOT_KEY, RecordingHttp, Scripted, connected_client};

const GRANT: &str = "https://files.test/files/\
    0000000000000000000000000000000000000000000000000000000000000000/intent?t=TOKEN";

const BOUND: Duration = Duration::from_secs(10);

/// The snapshot once `done` holds for it, awaited on the handle's changes.
async fn until(
    handle: &mut LiveUnsynced,
    mut done: impl FnMut(&UnsyncedWork) -> bool,
) -> UnsyncedWork {
    tokio::time::timeout(BOUND, async {
        loop {
            let work = handle.work();
            if done(&work) {
                return work;
            }
            handle.changed().await.expect("the handle stays live");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the snapshot never got there, last {:?}", handle.work()))
}

/// A connected content client answering uploads with `http`, its staged
/// file's identity, and a handle following the unsynced work.
async fn staged(
    dir: &std::path::Path,
    http: RecordingHttp,
) -> (
    ContentClient<Scripted, FsStore, RecordingHttp>,
    String,
    LiveUnsynced,
) {
    let client = connected_client(&dir.join("replica.sqlite"), Scripted::granting(GRANT)).await;
    let mut unsynced = client.unsynced_live().await.expect("follow the outbox");
    let content = ContentClient::attach(client, FsStore::new(dir.join("chunks")), ROOT_KEY, http)
        .await
        .expect("attach content");
    let (file_id, ()) = content
        .stage(
            io::Cursor::new(b"a photograph waiting to upload"),
            MimeClass::Jpeg,
            |_conn, _id| Ok(()),
        )
        .await
        .expect("stage");
    let file_id = file_id.to_string();
    let queued = vec![UnsyncedFile {
        file_id: file_id.clone(),
        major: None,
        state: FileState::Queued,
    }];
    until(&mut unsynced, |work| work.files == queued).await;
    (content, file_id, unsynced)
}

#[tokio::test]
async fn a_staged_file_leaves_once_its_upload_commits() {
    let dir = tempdir().expect("temp dir");
    let http = RecordingHttp::new([(200_u16, br#"{"needed":[]}"#.to_vec()), (200_u16, vec![])]);
    let (content, _file_id, mut unsynced) = staged(dir.path(), http).await;
    assert_eq!(content.flush_outbox().await.expect("flush"), 1);
    until(&mut unsynced, |work| *work == UnsyncedWork::default()).await;
}

#[tokio::test]
async fn a_refused_upload_stays_refused_until_retried() {
    let dir = tempdir().expect("temp dir");
    let (content, file_id, mut unsynced) =
        staged(dir.path(), RecordingHttp::new([(413_u16, vec![])])).await;
    content.flush_outbox().await.expect("flush");
    let refused = until(&mut unsynced, |work| {
        matches!(
            work.files.as_slice(),
            [UnsyncedFile {
                state: FileState::Refused(_),
                ..
            }]
        )
    })
    .await;
    assert_eq!(refused.files[0].file_id, file_id);

    let identity = content.refused_content().await.expect("read refused")[0].0;
    content.retry_refused(identity).await.expect("retry");
    until(&mut unsynced, |work| {
        matches!(
            work.files.as_slice(),
            [UnsyncedFile {
                state: FileState::Queued,
                ..
            }]
        )
    })
    .await;
}
