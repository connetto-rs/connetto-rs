use core::time::Duration;
use std::time::Instant;

use super::test_http::{
    BOUND, CEILING, NO_CONTENT, TRICKLE_READ, drain_body, local_listener, read_head, write_reply,
};
use super::*;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// The deadline shared by all attempts.
const BUDGET: Duration = Duration::from_secs(30);

/// How long a trickling server waits between reads, well inside the bound.
const TRICKLE_PAUSE: Duration = Duration::from_millis(25);

/// Large enough to outlast the idle bound at the trickle rate.
const TRICKLED: usize = 64 * 1024 * 1024;

/// A server that reads a request at the trickle rate and then answers it.
fn trickling_server(listener: TcpListener) -> JoinHandle<Option<usize>> {
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        let payload = drain_body(&mut stream, TRICKLE_READ, TRICKLE_PAUSE).await;
        write_reply(&mut stream, NO_CONTENT).await;
        payload
    })
}

/// A moving upload outlasts its idle bound and delivers every payload byte.
#[tokio::test]
async fn a_moving_upload_outlasts_its_bound() {
    let transport = ReqwestHttp::new().with_idle_bound(BOUND);
    let budget_end = Instant::now() + BUDGET;
    loop {
        let (listener, address) = local_listener().await;
        let mut server = trickling_server(listener);
        let url = format!("http://{address}/chunks/deadbeef?t=ticket");
        let started = Instant::now();
        let activity = Activity::new();
        let request = transport
            .client
            .put(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(streamed(vec![7_u8; TRICKLED], &activity));

        match tokio::time::timeout(
            budget_end.saturating_duration_since(Instant::now()),
            transport.run(request, &url, &activity),
        )
        .await
        {
            Err(_) => {
                server.abort();
                panic!("the runner starved the trickled link for the whole {BUDGET:?}");
            }
            Ok(Ok(reply)) => {
                assert_eq!(reply.status, 204, "the server answered the chunk");
                assert!(
                    started.elapsed() > BOUND,
                    "the upload must outlast the bound for this to prove anything, took {:?}",
                    started.elapsed()
                );
                let payload = tokio::time::timeout(CEILING, &mut server)
                    .await
                    .expect("the server is done once it answered the chunk")
                    .expect("server");
                assert_eq!(
                    payload,
                    Some(TRICKLED),
                    "every payload byte arrived, framing aside"
                );
                return;
            }
            Ok(Err(HttpFailure::Transport(ReqwestHttpError::Idle { bound }))) => {
                // The single-threaded runtime keeps the watchdog's record stable until this read.
                let last = activity.last.load(Ordering::Relaxed);
                let silence = activity.start.elapsed().as_millis() - u128::from(last);
                assert!(
                    silence >= bound.as_millis(),
                    "the transport aborted a moving upload, with only {silence}ms of client-visible silence against a {bound:?} bound"
                );
                match tokio::time::timeout(CEILING, &mut server).await {
                    Ok(Err(join)) => panic!("the trickling server panicked: {join}"),
                    Ok(Ok(_)) => {}
                    Err(_) => {
                        server.abort();
                    }
                }
                assert!(
                    Instant::now() < budget_end,
                    "the runner starved the trickled link for the whole {BUDGET:?}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok(Err(other)) => {
                panic!("a moving upload is never aborted, it got {other}")
            }
        }
    }
}
