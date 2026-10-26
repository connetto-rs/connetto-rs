//! Content bookkeeping never rides an application upload.

use core::time::Duration;
use std::sync::{Arc, Mutex};

use super::photo_fixture::{
    attach_content, device, photos, provision, spawn_sync_server, stage_photo, start_file_server,
    wait_for_pg_state, wait_for_upload,
};
use connetto_client::ClientEvent;
use connetto_test_harness::Fixture;
use diesel::prelude::*;
use tempfile::tempdir;
use tokio::sync::broadcast::error::RecvError;

/// An upload's outbox delete leaves the next application write landing unrefused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker: the fixture starts its own Postgres"]
async fn the_content_outbox_delete_never_rides_an_application_write() {
    let fixture = Fixture::acquire().await;
    provision(&fixture).await;
    let chunk_dir = tempdir().expect("temp dir");
    let (.., signer, files) = start_file_server(&fixture, chunk_dir.path()).await;

    let server = Arc::new(spawn_sync_server(&fixture, signer).await);

    let replica_dir = tempdir().expect("temp dir");
    let a = device(
        &replica_dir.path().join("a.sqlite"),
        "user:alice#writer",
        move || {
            let server = Arc::clone(&server);
            async move { Ok(server.attach()) }
        },
    )
    .await;

    // A broadcast receiver only sees events sent after it subscribes.
    let mut events = a.events();
    let seen = Arc::new(Mutex::new(Vec::<ClientEvent>::new()));
    {
        let seen = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => seen.lock().expect("seen lock poisoned").push(event),
                    Err(RecvError::Lagged(n)) => panic!(
                        "the collector lagged {n} events, so the negative check \
                         would run on a partial list"
                    ),
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    let a_content = attach_content(a.clone(), chunk_dir.path()).await;
    let file_id = stage_photo(&a_content).await;

    let uploaded = wait_for_upload(&a_content).await;
    assert_eq!(uploaded, 1, "the outbox walk sent the one file waiting");
    wait_for_pg_state(&fixture, file_id.as_bytes(), "available").await;

    // The next applied event belongs to this device's final write.
    let events_before = seen.lock().expect("seen lock poisoned").len();
    a.with_conn(|conn| {
        diesel::insert_into(photos::table)
            .values((
                photos::id.eq(2),
                photos::owner.eq("bob"),
                photos::content_id.eq::<Option<Vec<u8>>>(None),
                photos::edited_at.eq("t1"),
            ))
            .execute(conn.conn())
            .expect("write the ordinary application row")
    })
    .await
    .expect("the application write runs");

    wait_for_app_row(&fixture).await;

    wait_for_applied(&seen, events_before).await;
    let seen = seen.lock().expect("seen lock poisoned").clone();
    let rejected: Vec<u64> = seen
        .iter()
        .filter_map(|event| match event {
            ClientEvent::MutationRejected { client_seq, .. } => Some(*client_seq),
            _ => None,
        })
        .collect();
    assert_eq!(
        rejected,
        Vec::<u64>::new(),
        "the server refused a mutation carrying content bookkeeping"
    );

    files.abort();
}

/// Waits, bounded, until Postgres holds the application's second row.
async fn wait_for_app_row(fixture: &Fixture) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut conn = fixture.admin().get().await.expect("admin connection");
    loop {
        let rows: Vec<(i32, Option<String>)> = diesel_async::RunQueryDsl::load(
            photos::table
                .filter(photos::id.eq(2))
                .select((photos::id, photos::owner)),
            &mut *conn,
        )
        .await
        .expect("read the application's own row");
        if rows
            .iter()
            .any(|(_, owner)| owner.as_deref() == Some("bob"))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the application's own write never reached Postgres"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits, bounded, until the collector takes in an applied event after `before`.
async fn wait_for_applied(seen: &Arc<Mutex<Vec<ClientEvent>>>, before: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let applied = seen
            .lock()
            .expect("seen lock poisoned")
            .iter()
            .skip(before)
            .any(|event| matches!(event, ClientEvent::MutationApplied { .. }));
        if applied {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the application write's applied event never arrived"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
