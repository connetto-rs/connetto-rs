//! The content piece the builder attaches, carrying the transfer idle bound
//! and the lost-file heal queries, and the running client it builds.

use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use connetto_client::live::ConnettoClient;
use connetto_client::{AttachContent, ClientError, ContentPlace, TokioSleeper};
use connetto_core::Transport;
use connetto_core::traits::MaybeSend;
use connetto_file_core::{ChunkInventory, FreeSpace, MemStore};

use crate::http::{DEFAULT_IDLE_BOUND, ReqwestHttp};
use crate::{ContentClient, FsStore};

/// The content piece a build attaches.
///
/// `Default` is the 30 s transfer idle bound and no heal queries.
#[derive(Clone, Debug)]
pub struct Content {
    idle_bound: Duration,
    heal_queries: Vec<(String, String)>,
    #[cfg(feature = "peer")]
    peer_columns: Vec<PeerDeclaration>,
}

impl Default for Content {
    fn default() -> Self {
        Self {
            idle_bound: DEFAULT_IDLE_BOUND,
            heal_queries: Vec::new(),
            #[cfg(feature = "peer")]
            peer_columns: Vec::new(),
        }
    }
}

impl Content {
    /// An empty piece, the 30 s idle bound and no heal queries.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The idle bound a stalled chunk transfer gives up at.
    #[must_use]
    pub fn with_transfer_idle_bound(mut self, bound: Duration) -> Self {
        self.idle_bound = bound;
        self
    }

    /// A heal query, the `query` that names the files the server marked lost
    /// and the `file_id_column` holding their file ids. Repeatable.
    #[must_use]
    pub fn with_heal_lost(mut self, query: impl Into<String>, column: impl Into<String>) -> Self {
        self.heal_queries.push((query.into(), column.into()));
        self
    }

    /// Adds a peer column declaration to this content piece.
    #[cfg(feature = "peer")]
    #[must_use]
    pub fn with_peer_content(mut self, declaration: PeerDeclaration) -> Self {
        self.peer_columns.push(declaration);
        self
    }
}

/// A column whose files the native client's media driver serves and fetches.
#[cfg(feature = "peer")]
#[derive(Clone, Debug)]
pub struct PeerDeclaration {
    table: String,
    column: String,
    prefetch: bool,
    replicate: Option<(u8, String)>,
}

#[cfg(feature = "peer")]
impl PeerDeclaration {
    /// Declares a file-id column for peer media.
    #[must_use]
    pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
            prefetch: false,
            replicate: None,
        }
    }

    /// Fetches provisional files up to 1 MiB without an application request.
    #[must_use]
    pub fn prefetch(mut self) -> Self {
        self.prefetch = true;
        self
    }

    /// Keeps `copies` on linked devices until the state column reads `available`.
    #[must_use]
    pub fn replicate(mut self, copies: u8, state: impl Into<String>) -> Self {
        self.replicate = Some((copies, state.into()));
        self
    }
}

/// The running content client a piece builds, placed durable or in memory.
pub enum ContentHandle<T: Transport> {
    /// Durable, at the content directory beside the replica file.
    Durable(Arc<ContentClient<T, FsStore, ReqwestHttp>>),
    /// In memory, nothing at rest.
    InMemory(Arc<ContentClient<T, MemStore, ReqwestHttp>>),
}

impl<T: Transport + MaybeSend + 'static> AttachContent<T> for Content
where
    T::Error: Display,
{
    type Handle = ContentHandle<T>;

    fn held_content() -> connetto_client::HeldContent {
        crate::db::held_content()
    }

    #[cfg(feature = "peer")]
    fn media_driver(
        handle: &Self::Handle,
        take_port: impl FnOnce() -> Option<connetto_client::media::MediaPort>,
    ) -> Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>> {
        match handle {
            ContentHandle::Durable(content) if content.has_peer_content() => {
                let port = take_port()?;
                let content = Arc::clone(content);
                Some(Box::pin(async move { content.drive_media(port).await }))
            }
            ContentHandle::InMemory(content) if content.has_peer_content() => {
                let port = take_port()?;
                let content = Arc::clone(content);
                Some(Box::pin(async move { content.drive_media(port).await }))
            }
            _ => None,
        }
    }

    async fn attach(
        self,
        client: ConnettoClient<T>,
        root_key: [u8; 32],
        place: ContentPlace,
    ) -> Result<ContentHandle<T>, ClientError> {
        let http = ReqwestHttp::new().with_idle_bound(self.idle_bound);
        Ok(match place {
            ContentPlace::Durable(dir) => ContentHandle::Durable(
                running(client, FsStore::new(dir), root_key, http, &self).await?,
            ),
            ContentPlace::InMemory => ContentHandle::InMemory(
                running(client, MemStore::new(), root_key, http, &self).await?,
            ),
        })
    }
}

/// Attaches the piece's declarations before sharing the client with its outbox driver.
async fn running<T, B>(
    client: ConnettoClient<T>,
    store: B,
    root_key: [u8; 32],
    http: ReqwestHttp,
    piece: &Content,
) -> Result<Arc<ContentClient<T, B, ReqwestHttp>>, ClientError>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkInventory + FreeSpace + Clone + Sync + MaybeSend + 'static,
{
    let mut content = ContentClient::attach(client, store, root_key, http)
        .await
        .map_err(|err| ClientError::Content(err.to_string()))?;
    for (query, column) in &piece.heal_queries {
        content = content
            .heal_lost(query, column)
            .await
            .map_err(|err| ClientError::Content(err.to_string()))?;
    }
    #[cfg(feature = "peer")]
    for declaration in &piece.peer_columns {
        let mut peer = content.with_peer_content(&declaration.table, &declaration.column);
        if declaration.prefetch {
            peer = peer.prefetch();
        }
        if let Some((copies, state)) = &declaration.replicate {
            peer = peer.replicate(*copies, state);
        }
        content = peer
            .await
            .map_err(|err| ClientError::Content(err.to_string()))?;
    }
    let content = Arc::new(content);
    tokio::spawn({
        let content = Arc::clone(&content);
        async move {
            content.drive_outbox(TokioSleeper).await;
        }
    });
    Ok(content)
}
