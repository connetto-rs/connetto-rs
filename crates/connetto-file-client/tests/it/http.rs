//! Local HTTP fixtures for native transport timing and redirect tests.

use core::time::Duration;
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The bound every test injects, in place of the shipped thirty seconds.
pub const BOUND: Duration = Duration::from_millis(500);

/// The deadline for an expected abort or a server join.
pub const CEILING: Duration = Duration::from_secs(4);

/// How much one drain takes per read.
pub const TRICKLE_READ: usize = 1024 * 1024;

/// A reply that closes the connection, so no test waits on a reused one.
pub const NO_CONTENT: &str =
    "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// A local listener and the address to send it requests at.
pub async fn local_listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("a bound address");
    (listener, address)
}

/// Reads one request head, which is all a test needs to know what arrived.
pub async fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte).await {
            Ok(0) | Err(_) => break,
            Ok(_) => head.push(byte[0]),
        }
    }
    String::from_utf8_lossy(&head).into_owned()
}

/// Drains a paced chunked body and counts its payload bytes.
pub async fn drain_body(stream: &mut TcpStream, step: usize, pause: Duration) -> Option<usize> {
    let mut buffer = vec![0_u8; step];
    let mut body = Vec::new();
    while !body.ends_with(b"0\r\n\r\n") {
        let read = match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        body.extend_from_slice(&buffer[..read]);
        tokio::time::sleep(pause).await;
    }
    chunked_payload(&body)
}

/// Counts payload bytes only when every chunk and delimiter is complete.
pub fn chunked_payload(body: &[u8]) -> Option<usize> {
    let mut rest = body;
    let mut payload = 0_usize;
    loop {
        let line = rest.windows(2).position(|pair| pair == b"\r\n")?;
        let size = usize::from_str_radix(core::str::from_utf8(&rest[..line]).ok()?, 16).ok()?;
        rest = rest.get(line.saturating_add(2)..)?;
        if size == 0 {
            return rest.starts_with(b"\r\n").then_some(payload);
        }
        if rest.get(size..size.saturating_add(2))? != b"\r\n" {
            return None;
        }
        payload = payload.saturating_add(size);
        rest = rest.get(size.saturating_add(2)..)?;
    }
}

/// Answers one request head with `reply`, once the request has fully arrived.
pub async fn write_reply(stream: &mut TcpStream, reply: &str) {
    let _ = stream.write_all(reply.as_bytes()).await;
    let _ = stream.flush().await;
}
