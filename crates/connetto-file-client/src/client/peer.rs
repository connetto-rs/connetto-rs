//! The holder's side of the peer link: the declared content columns and the
//! serving of `Want` and `Fetch` (R79 decisions 1, 2 and 4).

use core::fmt::Display;
use std::collections::HashMap;
use std::io;
use std::time::Instant;

use bytes::Bytes;
use connetto_client::media::{MediaEvent, MediaPort, MediaSender};
use connetto_client::peer::PeerAuthor;
use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::MAX_MANIFEST_ROWS;
use connetto_exchange::media::{Ask, MAX_PIECE_BYTES, Media, MediaAction, MediaLinks};
use connetto_file_core::{ChunkStore, FileId, MaybeSend, encode_range};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use iroh_io::AsyncStreamWriter;
use tokio::sync::oneshot;

use super::{ContentClient, FETCHED_CLASS};
use crate::db;
use crate::error::ContentError;
use crate::http::ContentHttp;

/// What the driver sends one peer outside the machine's own loop.
enum Job {
    /// One message, a `Busy` the machine answered.
    Send(Media),
    /// The answer to a request the machine admitted.
    Serve {
        /// The id every reply quotes.
        request_id: u64,
        /// The request.
        ask: Ask,
    },
}

impl<T, B, H> ContentClient<T, B, H>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkStore + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    /// Declares `column` of `table` as holding file ids a linked peer may be
    /// served.
    ///
    /// A peer asking for a file is served when the policy views, under the
    /// requester's certificate, show it a row of a declared column naming the
    /// file. With nothing declared every request is refused. Every build of
    /// the application declares the same columns.
    ///
    /// # Errors
    ///
    /// [`ContentError::PeerContentColumnMissing`] when the replica holds no
    /// such table or column.
    pub async fn with_peer_content(
        mut self,
        table: &str,
        column: &str,
    ) -> Result<Self, ContentError> {
        let query = format!("SELECT * FROM \"{}\"", table.replace('"', "\"\""));
        let probe = column.to_owned();
        if !self
            .client
            .with_conn(move |conn| crate::retain::answers_column(conn.conn(), &query, &probe))
            .await?
        {
            return Err(ContentError::PeerContentColumnMissing {
                table: table.to_owned(),
                column: column.to_owned(),
            });
        }
        self.peer_content
            .push((table.to_owned(), column.to_owned()));
        Ok(self)
    }

    /// The media driver: answers every linked peer's `Want` with the
    /// manifest and every `Fetch` with the verified encoding of the range,
    /// each after the gate admits the requester, and `Refused` otherwise.
    ///
    /// A peer is served one request at a time and at most two peers at once,
    /// and a request beyond that is answered `Busy`. A stream stops when its
    /// peer's link falls, and answers `Refused` when the file is evicted or a
    /// chunk cannot be read partway through.
    ///
    /// Returned as a future rather than spawned, the shape
    /// [`drive_outbox`](Self::drive_outbox) has. It ends when the port's
    /// client ends.
    pub async fn drive_media(&self, mut port: MediaPort) {
        let sender = port.sender().clone();
        let mut links = MediaLinks::<DeviceIdentity, ()>::new();
        for (peer, _) in sender.linked() {
            links.linked(&peer);
        }
        let mut jobs = FuturesUnordered::new();
        let mut streams: HashMap<DeviceIdentity, (u64, oneshot::Sender<()>)> = HashMap::new();
        loop {
            let actions = tokio::select! {
                event = port.recv() => match event {
                    None => return,
                    Some(MediaEvent::Linked { peer, .. }) => {
                        links.linked(&peer);
                        Vec::new()
                    }
                    Some(MediaEvent::Unlinked { peer }) => links.unlinked(&peer),
                    Some(MediaEvent::Message { peer, message }) => {
                        links.received(&peer, message, Instant::now())
                    }
                },
                Some((peer, served)) = jobs.next() => {
                    if let Some(request_id) = served {
                        links.served(&peer, request_id);
                        if streams.get(&peer).is_some_and(|(id, _)| *id == request_id) {
                            streams.remove(&peer);
                        }
                    }
                    Vec::new()
                }
            };
            for action in actions {
                match action {
                    MediaAction::Send { peer, message } => {
                        jobs.push(self.job(sender.clone(), peer, Job::Send(message), None));
                    }
                    MediaAction::Serve {
                        peer,
                        request_id,
                        ask,
                    } => {
                        let (stop, stopped) = oneshot::channel();
                        streams.insert(peer.clone(), (request_id, stop));
                        jobs.push(self.job(
                            sender.clone(),
                            peer,
                            Job::Serve { request_id, ask },
                            Some(stopped),
                        ));
                    }
                    MediaAction::StopServing { peer } => {
                        streams.remove(&peer);
                    }
                    // This device asks nothing, so nothing answers it.
                    MediaAction::Reply { .. }
                    | MediaAction::Lapsed { .. }
                    | MediaAction::Lost { .. } => {}
                }
            }
        }
    }

    /// Runs `job` for `peer` until it ends or `stop` fires or drops,
    /// answering the request id a serve held.
    async fn job(
        &self,
        sender: MediaSender,
        peer: DeviceIdentity,
        job: Job,
        stop: Option<oneshot::Receiver<()>>,
    ) -> (DeviceIdentity, Option<u64>) {
        let served = match &job {
            Job::Send(_) => None,
            Job::Serve { request_id, .. } => Some(*request_id),
        };
        let stopped = async {
            match stop {
                Some(stop) => {
                    let _ = stop.await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            () = self.run(&sender, &peer, job) => {}
            () = stopped => {}
        }
        (peer, served)
    }

    /// Carries out `job` for `peer`.
    async fn run(&self, sender: &MediaSender, peer: &DeviceIdentity, job: Job) {
        let (request_id, ask) = match job {
            Job::Send(message) => {
                let _ = sender.send(peer, message).await;
                return;
            }
            Job::Serve { request_id, ask } => (request_id, ask),
        };
        let served = match ask {
            Ask::Want { file_id } => self.serve_manifest(sender, peer, request_id, file_id).await,
            Ask::Fetch {
                file_id,
                start,
                len,
            } => {
                self.serve_range(sender, peer, request_id, file_id, start, len)
                    .await
            }
        };
        if !served {
            let _ = sender.send(peer, Media::Refused { request_id }).await;
        }
    }

    /// Streams the manifest of `file_id`, `false` to refuse `peer`, whom the
    /// gate refuses, or a file the device holds no manifest for.
    async fn serve_manifest(
        &self,
        sender: &MediaSender,
        peer: &DeviceIdentity,
        request_id: u64,
        file_id: [u8; 32],
    ) -> bool {
        if !self.admits(sender, peer, &file_id).await {
            return false;
        }
        let Ok(Ok(Some(manifest))) = self
            .client
            .with_conn(|conn| db::load_manifest(conn.conn(), FileId::from_bytes(file_id)))
            .await
        else {
            return false;
        };
        let rows: Vec<([u8; 32], u64)> = manifest
            .chunks()
            .iter()
            .map(|chunk| (*chunk.hash.as_bytes(), chunk.len))
            .collect();
        let mut runs: Vec<&[([u8; 32], u64)]> = rows.chunks(MAX_MANIFEST_ROWS).collect();
        if runs.is_empty() {
            runs.push(&[]);
        }
        let last = runs.len() - 1;
        for (index, run) in runs.into_iter().enumerate() {
            let run = Media::Manifest {
                request_id,
                chunks: run.to_vec(),
                last: index == last,
            };
            // A send fails only once the link is gone, which takes the
            // refusal with it.
            if sender.send(peer, run).await.is_err() {
                break;
            }
        }
        true
    }

    /// Streams the verified encoding of `len` bytes of `file_id` from
    /// `start`, `false` to refuse `peer`, whom the gate refuses, a file the
    /// device holds no manifest or outboard for, or a range that fails to
    /// encode.
    async fn serve_range(
        &self,
        sender: &MediaSender,
        peer: &DeviceIdentity,
        request_id: u64,
        file_id: [u8; 32],
        start: u64,
        len: u64,
    ) -> bool {
        if !self.admits(sender, peer, &file_id).await {
            return false;
        }
        let file_id = FileId::from_bytes(file_id);
        let Ok(Ok(Some(manifest))) = self
            .client
            .with_conn(|conn| db::load_manifest(conn.conn(), file_id))
            .await
        else {
            return false;
        };
        let Ok(Some(outboard)) = self.outboard(file_id).await else {
            return false;
        };
        let mut pieces = Pieces {
            sender,
            peer,
            request_id,
            pending: Vec::new(),
        };
        let store = self.store_for(FETCHED_CLASS);
        if encode_range(&manifest, &store, &outboard, start, len, &mut pieces)
            .await
            .is_err()
        {
            return false;
        }
        let _ = pieces.finish().await;
        true
    }

    /// Whether `peer`, still linked, reads a row of a declared column naming
    /// `file_id`, judged under the bot list its certificate carries now.
    async fn admits(
        &self,
        sender: &MediaSender,
        peer: &DeviceIdentity,
        file_id: &[u8; 32],
    ) -> bool {
        let Some((_, bot_list)) = sender
            .linked()
            .into_iter()
            .find(|(linked, _)| linked == peer)
        else {
            return false;
        };
        let requester = PeerAuthor {
            key_id: peer.key().as_bytes().to_vec(),
            account: peer.account().to_owned(),
            bot_list,
        };
        let declared = &self.peer_content;
        self.client
            .with_conn(|conn| {
                declared.iter().any(|(table, column)| {
                    conn.peer_may_read(&requester, table, column, file_id)
                        .unwrap_or(false)
                })
            })
            .await
            .unwrap_or(false)
    }
}

/// The sink a range's encoding streams into, cut into pieces of at most
/// [`MAX_PIECE_BYTES`] as it grows.
struct Pieces<'a> {
    sender: &'a MediaSender,
    peer: &'a DeviceIdentity,
    request_id: u64,
    pending: Vec<u8>,
}

impl Pieces<'_> {
    /// Sends `encoded` as the next piece.
    async fn send(&self, encoded: Vec<u8>, last: bool) -> io::Result<()> {
        self.sender
            .send(
                self.peer,
                Media::Piece {
                    request_id: self.request_id,
                    encoded,
                    last,
                },
            )
            .await
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    /// Sends what is left as the last piece, never empty, since `write`
    /// sends a full piece only once a byte follows it.
    async fn finish(mut self) -> io::Result<()> {
        let encoded = std::mem::take(&mut self.pending);
        self.send(encoded, true).await
    }
}

impl AsyncStreamWriter for Pieces<'_> {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.pending.extend_from_slice(data);
        while self.pending.len() > MAX_PIECE_BYTES {
            let rest = self.pending.split_off(MAX_PIECE_BYTES);
            let full = std::mem::replace(&mut self.pending, rest);
            self.send(full, false).await?;
        }
        Ok(())
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        self.write(&data).await
    }

    fn sync(&mut self) -> impl Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }
}
