//! R94: content through the builder.
//!
//! A build with `with_content(Content::default())` returns a client whose
//! content handle stages a file, and the bytes land in the store the place
//! dictates, in memory or durable beside the replica, the entry row is
//! written in the same transaction, and the staged bytes resolve locally.

use connetto_client::{
    Grant, HeldCredential, MemoryKeyStore, MemoryRefreshStore, NativeClientBuilder, SyncSchema,
    TransportFactory,
};
use connetto_core::schema::SchemaBundle;
use connetto_core::test_support::FakeTransport;
use connetto_file_client::{Content, ContentHandle, MimeClass, Resolved};
use diesel::prelude::*;
use tempfile::tempdir;

use crate::support::{DDL, photos};

/// The same photo table as Postgres holds it.
const PG_DDL: &str = "-- connetto:major 1\nCREATE TABLE photos (id INT PRIMARY KEY, \
                     content_state TEXT);";

/// The photograph both legs stage.
const PHOTO: &[u8] = b"a photograph's bytes, standing in for one taken in the field";

fn schema() -> SyncSchema {
    SyncSchema::new(
        SchemaBundle::new(
            PG_DDL,
            "",
            DDL,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares major 1"),
    )
}

/// The refusal the one-shot dialer hands back after its transport.
#[derive(Debug, thiserror::Error)]
enum DialRefused {
    #[error("the dialer handed out its last transport")]
    Exhausted,
}

/// A dialer that hands out exactly one transport, the one the test provides.
struct OneShot {
    transport: Option<FakeTransport>,
}

impl TransportFactory for OneShot {
    type Transport = FakeTransport;
    type Error = DialRefused;

    fn connect(&mut self) -> impl Future<Output = Result<FakeTransport, DialRefused>> + Send {
        let transport = self.transport.take().ok_or(DialRefused::Exhausted);
        async move { transport }
    }
}

/// A build without a directory places its content in memory, and the staged
/// file is what its row names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_content_stages_a_file_in_memory() {
    let client = NativeClientBuilder::new("ws://127.0.0.1:1", schema())
        .with_dialer(OneShot {
            transport: Some(FakeTransport::accepting_but_silent()),
        })
        .with_content(Content::default())
        .connect()
        .await
        .expect("the build connects");
    let ContentHandle::InMemory(content) = client.content().expect("the build attached content")
    else {
        panic!("a build without a directory places its content in memory");
    };
    let (file_id, ()) = content
        .stage(PHOTO, MimeClass::Jpeg, |conn, file_id| {
            diesel::insert_into(photos::table)
                .values((
                    photos::id.eq(1),
                    photos::content_id.eq(file_id.as_bytes().to_vec()),
                ))
                .execute(conn)
                .map(|_| ())
        })
        .await
        .expect("stage the file");
    let rows: Vec<Vec<u8>> = client
        .client()
        .with_conn(|conn| {
            photos::table
                .select(photos::content_id)
                .load(conn.conn())
                .expect("read the row")
        })
        .await
        .expect("the client is not locked");
    assert_eq!(
        rows,
        vec![file_id.as_bytes().to_vec()],
        "the staged file is what the row names"
    );
    assert!(
        matches!(
            content
                .resolve(file_id)
                .await
                .expect("resolve the staged file"),
            Resolved::Local { .. }
        ),
        "the staged bytes resolve from the local store"
    );
}

/// A durable build places its content at the content directory beside the
/// replica, and the staged file is what its row names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_content_stages_a_file_durable() {
    let dir = tempdir().expect("a temp dir");
    let client = Box::pin(
        NativeClientBuilder::new("ws://127.0.0.1:1", schema())
            .with_dialer(OneShot {
                transport: Some(FakeTransport::accepting_but_silent()),
            })
            .with_content(Content::default())
            .signed_in(
                HeldCredential::new(Grant::new("user:bob"), "bob").expect("the credential holds"),
            )
            .durable(
                dir.path(),
                MemoryKeyStore::default(),
                MemoryRefreshStore::default(),
            )
            .connect(),
    )
    .await
    .expect("the durable build connects");
    let ContentHandle::Durable(content) = client.content().expect("the build attached content")
    else {
        panic!("a durable build places its content at the content directory");
    };
    let (file_id, ()) = content
        .stage(PHOTO, MimeClass::Jpeg, |conn, file_id| {
            diesel::insert_into(photos::table)
                .values((
                    photos::id.eq(1),
                    photos::content_id.eq(file_id.as_bytes().to_vec()),
                ))
                .execute(conn)
                .map(|_| ())
        })
        .await
        .expect("stage the file");
    let rows: Vec<Vec<u8>> = client
        .client()
        .with_conn(|conn| {
            photos::table
                .select(photos::content_id)
                .load(conn.conn())
                .expect("read the row")
        })
        .await
        .expect("the client is not locked");
    assert_eq!(
        rows,
        vec![file_id.as_bytes().to_vec()],
        "the staged file is what the row names"
    );
    assert!(
        matches!(
            content
                .resolve(file_id)
                .await
                .expect("resolve the staged file"),
            Resolved::Local { .. }
        ),
        "the staged bytes resolve from the content directory"
    );
}

/// A replica-key store shared across builds, so one process can reopen the same replica.
struct SharedKeys(std::sync::Arc<MemoryKeyStore>);

impl connetto_core::traits::ReplicaKeyStore for SharedKeys {
    type Error = connetto_client::ClientError;

    async fn load(&self, name: &str) -> Result<Option<connetto_client::ReplicaKey>, Self::Error> {
        self.0.load(name).await
    }

    async fn store(
        &self,
        name: &str,
        key: &connetto_client::ReplicaKey,
    ) -> Result<(), Self::Error> {
        self.0.store(name, key).await
    }

    async fn clear(&self, name: &str) -> Result<(), Self::Error> {
        self.0.clear(name).await
    }

    fn protection(&self) -> connetto_client::Custody {
        self.0.protection()
    }
}

/// A closed and dropped client releases its replica at once, so the same process reopens it even with a file still unsent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_client_with_unsent_content_reopens_at_once() {
    let dir = tempdir().expect("a temp dir");
    let keys = std::sync::Arc::new(MemoryKeyStore::default());
    for round in 0..20 {
        let client = Box::pin(
            NativeClientBuilder::new("ws://127.0.0.1:1", schema())
                .with_dialer(OneShot { transport: None })
                .with_content(Content::default())
                .signed_in(
                    HeldCredential::new(Grant::new("user:bob"), "bob")
                        .expect("the credential holds"),
                )
                .durable(
                    dir.path(),
                    SharedKeys(std::sync::Arc::clone(&keys)),
                    MemoryRefreshStore::default(),
                )
                .connect(),
        )
        .await
        .unwrap_or_else(|err| panic!("round {round} reopens the replica: {err}"));
        if round == 0 {
            let ContentHandle::Durable(content) =
                client.content().expect("the build attached content")
            else {
                panic!("a durable build places its content at the content directory");
            };
            content
                .stage(PHOTO, MimeClass::Jpeg, |conn, file_id| {
                    diesel::insert_into(photos::table)
                        .values((
                            photos::id.eq(1),
                            photos::content_id.eq(file_id.as_bytes().to_vec()),
                        ))
                        .execute(conn)
                        .map(|_| ())
                })
                .await
                .expect("stage the file");
        }
        client.client().close().await;
        drop(client);
    }
}

/// An in-memory build attaching `piece`.
#[cfg(feature = "peer")]
async fn in_memory(
    piece: Content,
) -> Result<
    connetto_client::NativeClient<FakeTransport, ContentHandle<FakeTransport>>,
    connetto_client::ClientError,
> {
    NativeClientBuilder::new("ws://127.0.0.1:1", schema())
        .with_dialer(OneShot {
            transport: Some(FakeTransport::accepting_but_silent()),
        })
        .with_content(piece)
        .connect()
        .await
}

/// The photo column declared for prefetch and replication on its state column.
#[cfg(feature = "peer")]
fn declared_piece() -> Content {
    Content::new().with_peer_content(
        connetto_file_client::PeerDeclaration::new("photos", "content_id")
            .prefetch()
            .replicate(3, "content_state"),
    )
}

/// A piece declaring no peer content never asks for the media port, which
/// stays free for whatever else the application runs on it.
#[cfg(feature = "peer")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_piece_without_peer_content_takes_no_media_port() {
    use connetto_client::AttachContent;

    let client = in_memory(Content::default())
        .await
        .expect("the build connects");
    let handle = client.content().expect("the build attached content");
    let mut asked = false;
    let driver = <Content as AttachContent<FakeTransport>>::media_driver(handle, || {
        asked = true;
        None
    });
    assert!(driver.is_none(), "nothing declared runs no media driver");
    assert!(!asked, "nothing declared leaves the media port untaken");
}

/// A declared piece whose port another caller already took starts no driver.
#[cfg(feature = "peer")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_piece_with_the_port_taken_starts_no_driver() {
    use connetto_client::AttachContent;

    let client = in_memory(declared_piece())
        .await
        .expect("the build connects");
    let handle = client.content().expect("the build attached content");
    let mut asked = false;
    let driver = <Content as AttachContent<FakeTransport>>::media_driver(handle, || {
        asked = true;
        None
    });
    assert!(asked, "declared content asks for the media port");
    assert!(driver.is_none(), "a taken port starts no media driver");
}

/// A declared piece with the port free runs the driver, which returns once
/// the port's events end, as they do when the client closes.
#[cfg(feature = "peer")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_piece_runs_its_driver_until_the_port_ends() {
    use connetto_client::AttachContent;
    use connetto_client::media::MediaPort;
    use connetto_peer::test_support::{Deployment, node, whole_second};

    let client = in_memory(declared_piece())
        .await
        .expect("the build connects");
    let handle = client.content().expect("the build attached content");
    let deployment = Deployment::new(80, whole_second());
    let (node_events, _node_rx) = tokio::sync::mpsc::unbounded_channel();
    let (media_tx, media_rx) = tokio::sync::mpsc::unbounded_channel();
    let port = MediaPort::over(node(deployment.root_der(), node_events), media_rx);
    let driver = <Content as AttachContent<FakeTransport>>::media_driver(handle, || Some(port))
        .expect("declared content with a free port runs the media driver");
    drop(media_tx);
    tokio::time::timeout(std::time::Duration::from_secs(10), driver)
        .await
        .expect("the driver returns once its port's events end");
}

/// A replication naming a state column the table lacks fails the build,
/// naming the column, so the piece applied the replication.
#[cfg(feature = "peer")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replication_on_a_missing_state_column_fails_the_build() {
    let piece = Content::new().with_peer_content(
        connetto_file_client::PeerDeclaration::new("photos", "content_id")
            .replicate(3, "no_such_state"),
    );
    let Err(err) = in_memory(piece).await else {
        panic!("a replication on a missing column builds");
    };
    assert!(
        err.to_string().contains("no_such_state"),
        "the refusal names the missing column, got {err}"
    );
}
