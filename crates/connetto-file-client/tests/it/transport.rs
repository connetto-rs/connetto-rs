//! The native transport's idle bound and its redirect refusal, against local
//! listeners that control the timing of every byte.
//!
//! A bound of a few hundred milliseconds stands in for the shipped thirty
//! seconds, and the bodies are large enough that a pull cannot outrun the
//! socket buffers, which is what makes a stalled send observable at all.

#[path = "http.rs"]
mod http;

use core::time::Duration;
use std::net::SocketAddr;
use std::time::Instant;

use connetto_file_client::{ContentHttp, HttpFailure, HttpReply, ReqwestHttp, ReqwestHttpError};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::transport::http::{
    BOUND, CEILING, NO_CONTENT, TRICKLE_READ, drain_body, local_listener, read_head, write_reply,
};

/// A body past any socket buffer, so a server that stops reading stalls the
/// pulls the watchdog observes.
const UNBUFFERABLE: usize = 32 * 1024 * 1024;

/// A transport under the test bound.
fn transport() -> ReqwestHttp {
    ReqwestHttp::new().with_idle_bound(BOUND)
}

/// A server that answers one request head with `reply` and reads no body.
fn deaf_server(listener: TcpListener, reply: Option<&'static str>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        if let Some(reply) = reply {
            write_reply(&mut stream, reply).await;
        }
        core::future::pending::<()>().await;
    })
}

/// The stage each abort test drives, named by what it aborts.
async fn put_unbufferable(address: SocketAddr) -> Result<HttpReply, HttpFailure<ReqwestHttpError>> {
    transport()
        .put(&chunk_url(address), vec![7_u8; UNBUFFERABLE])
        .await
}

/// Where one chunk's bytes go on a local listener.
fn chunk_url(address: SocketAddr) -> String {
    format!("http://{address}/chunks/deadbeef?t=ticket")
}

/// Where an upload declares its manifest on a local listener.
fn intent_url(address: SocketAddr) -> String {
    format!("http://{address}/files/deadbeef/intent?t=ticket")
}

/// Drives one stage that must abort, and answers the failure it aborted with.
///
/// The ceiling is what makes the injected bound load-bearing, because a
/// transport that kept a bound of its own would answer nowhere near it, and
/// the floor is the bound itself, which a transport may never abort before.
async fn abort_of<T>(
    work: impl Future<Output = Result<T, HttpFailure<ReqwestHttpError>>>,
    what: &str,
) -> HttpFailure<ReqwestHttpError> {
    let started = Instant::now();
    let answer = tokio::time::timeout(CEILING, work)
        .await
        .unwrap_or_else(|_| panic!("{what} must abort under the injected bound, not its own"));
    let Err(failure) = answer else {
        panic!("{what} must abort");
    };
    assert!(
        matches!(
            failure,
            HttpFailure::Transport(ReqwestHttpError::Idle { .. })
        ),
        "{what} must abort as the transport's idle failure, got {failure}"
    );
    assert!(
        started.elapsed() >= BOUND,
        "{what} must not abort before the bound, aborted after {:?}",
        started.elapsed()
    );
    failure
}

/// A server that stops reading stalls the send, and the send aborts.
#[tokio::test]
async fn a_stalled_upload_aborts_after_the_bound() {
    let (listener, address) = local_listener().await;
    let server = deaf_server(listener, None);

    abort_of(put_unbufferable(address), "a stalled send").await;

    server.abort();
}

/// Silence while the server holds the answer back aborts the same way.
#[tokio::test]
async fn a_stalled_status_wait_aborts_after_the_bound() {
    let (listener, address) = local_listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        let _ = drain_body(&mut stream, TRICKLE_READ, Duration::ZERO).await;
        core::future::pending::<()>().await;
    });

    abort_of(
        transport().put(&chunk_url(address), b"a small chunk".to_vec()),
        "a withheld status line",
    )
    .await;

    server.abort();
}

/// A reply that stops before its length is reached aborts the download.
#[tokio::test]
async fn a_stalled_download_aborts_after_the_bound() {
    let (listener, address) = local_listener().await;
    let server = deaf_server(
        listener,
        Some("HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\nfour"),
    );

    abort_of(
        transport().get(&format!("http://{address}/files/deadbeef?t=ticket"), None),
        "a stalled reply body",
    )
    .await;

    server.abort();
}

/// The transport's own client follows nothing, so a `3xx` arrives as a reply
/// for the negotiation to refuse.
#[tokio::test]
async fn a_redirect_reply_is_answered_rather_than_followed() {
    let (listener, address) = local_listener().await;
    let (elsewhere, elsewhere_address) = local_listener().await;
    let elsewhere_server = tokio::spawn(async move {
        tokio::time::timeout(BOUND, elsewhere.accept())
            .await
            .is_ok()
    });
    let reply = format!(
        "HTTP/1.1 302 Found\r\nLocation: http://{elsewhere_address}/chunks/deadbeef?t=ticket\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        let _ = drain_body(&mut stream, TRICKLE_READ, Duration::ZERO).await;
        write_reply(&mut stream, &reply).await;
    });

    let status = transport()
        .put(&chunk_url(address), b"a small chunk".to_vec())
        .await
        .expect("a redirect reply is a reply")
        .status;

    assert_eq!(status, 302, "the hop is answered rather than taken");
    assert!(
        !elsewhere_server.await.expect("second listener"),
        "nothing reached the address the hop named"
    );
    server.await.expect("server");
}

/// An application client whose policy follows lands its reply elsewhere, and
/// the transport refuses that reply by the address it landed on.
///
/// A `302` on a `POST` becomes a bodiless `GET`, which is a request the
/// redirect layer can replay, so the hop is taken and the manifest is dropped
/// on the way. The refusal is what stops the negotiation from reading an
/// answer another origin wrote.
#[tokio::test]
async fn a_followed_redirect_is_refused_by_the_address_it_landed_on() {
    let (listener, address) = local_listener().await;
    let (elsewhere, elsewhere_address) = local_listener().await;
    let elsewhere_server = tokio::spawn(async move {
        let (mut stream, _) = elsewhere.accept().await.expect("accept");
        let head = read_head(&mut stream).await;
        write_reply(&mut stream, NO_CONTENT).await;
        head
    });
    let reply = format!(
        "HTTP/1.1 302 Found\r\nLocation: http://{elsewhere_address}/chunks/deadbeef?t=ticket\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        let _ = drain_body(&mut stream, TRICKLE_READ, Duration::ZERO).await;
        write_reply(&mut stream, &reply).await;
    });

    let failure = ReqwestHttp::with_client(reqwest::Client::new())
        .with_idle_bound(BOUND)
        .post(&intent_url(address), Some(br#"{"total_len":13}"#.to_vec()))
        .await
        .map(|reply| reply.status)
        .expect_err("a reply from another address is refused");

    match failure {
        HttpFailure::Redirected { origin } => assert_eq!(
            origin.as_deref(),
            Some(format!("http://{elsewhere_address}").as_str()),
            "the refusal names the origin the reply landed on"
        ),
        other @ HttpFailure::Transport(_) => {
            panic!("a followed redirect must be refused, got {other}")
        }
    }
    assert!(
        elsewhere_server
            .await
            .expect("second listener")
            .starts_with("GET "),
        "a 302 lands as a bodiless GET, which is why the origin is what is refused"
    );
    server.await.expect("server");
}

/// A streamed body is never replayed to a hop, so a `307` on a chunk upload
/// arrives as its own reply and nothing reaches the address it named.
#[tokio::test]
async fn a_streamed_body_never_lands_on_the_address_a_hop_named() {
    let (listener, address) = local_listener().await;
    let (elsewhere, elsewhere_address) = local_listener().await;
    let elsewhere_server = tokio::spawn(async move {
        tokio::time::timeout(BOUND, elsewhere.accept())
            .await
            .is_ok()
    });
    let reply = format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{elsewhere_address}/chunks/deadbeef?t=ticket\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        let _ = drain_body(&mut stream, TRICKLE_READ, Duration::ZERO).await;
        write_reply(&mut stream, &reply).await;
    });

    let status = ReqwestHttp::with_client(reqwest::Client::new())
        .with_idle_bound(BOUND)
        .put(&chunk_url(address), b"a small chunk".to_vec())
        .await
        .expect("an unreplayable body ends the chain")
        .status;

    assert_eq!(status, 307, "the reply of the hop is what arrives");
    assert!(
        !elsewhere_server.await.expect("second listener"),
        "no byte of a streamed body reaches the address the hop named"
    );
    server.await.expect("server");
}

/// A hop to another path on the same origin is refused as well, because the
/// ticket points at one address and only that address answers for it.
#[tokio::test]
async fn a_same_origin_redirect_is_refused_too() {
    let (listener, address) = local_listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        let reply = "HTTP/1.1 302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        write_reply(&mut stream, reply).await;
        let (mut second, _) = listener.accept().await.expect("second accept");
        read_head(&mut second).await;
        write_reply(&mut second, NO_CONTENT).await;
    });

    let failure = ReqwestHttp::with_client(reqwest::Client::new())
        .with_idle_bound(BOUND)
        .get(&format!("http://{address}/files/deadbeef?t=ticket"), None)
        .await
        .map(|reply| reply.status)
        .expect_err("a reply from another path is refused");

    match failure {
        HttpFailure::Redirected { origin } => assert_eq!(
            origin.as_deref(),
            Some(format!("http://{address}").as_str()),
            "the refusal names the origin, which here is the one asked"
        ),
        other @ HttpFailure::Transport(_) => {
            panic!("a same-origin hop must be refused, got {other}")
        }
    }
    server.await.expect("server");
}

/// A bound that never elapses is what an ordinary exchange runs under, so the
/// watchdog costs a completed request nothing.
#[tokio::test]
async fn an_answered_request_reports_what_the_server_sent() {
    let (listener, address) = local_listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        read_head(&mut stream).await;
        let reply = "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\nneeded!";
        write_reply(&mut stream, reply).await;
    });

    let reply = transport()
        .get(&format!("http://{address}/files/deadbeef?t=ticket"), None)
        .await
        .expect("an answered request");
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body, b"needed!");
    server.await.expect("server");
}
