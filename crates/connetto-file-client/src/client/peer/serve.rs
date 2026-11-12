//! The holder's side: the gate, and the streaming of a manifest or a range
//! to an admitted requester (R79 decisions 1, 2 and 4).

use core::fmt::Display;
use std::io;

use bytes::Bytes;
use connetto_client::media::MediaSender;
use connetto_client::peer::PeerAuthor;
use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::MAX_MANIFEST_ROWS;
use connetto_exchange::media::{Ask, MAX_PIECE_BYTES, Media};
use connetto_file_core::{ChunkStore, FileId, MaybeSend, encode_range};
use iroh_io::AsyncStreamWriter;

use super::super::{ContentClient, FETCHED_CLASS};
use crate::db;
use crate::http::ContentHttp;

impl<T, B, H> ContentClient<T, B, H>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkStore + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    /// Answers `ask` from `peer` under `request_id`, `Refused` when the gate
    /// refuses the requester or the device cannot serve the file.
    pub(super) async fn serve(
        &self,
        sender: &MediaSender,
        peer: &DeviceIdentity,
        request_id: u64,
        ask: Ask,
    ) {
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
    pub(super) async fn admits(
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
                declared.iter().any(|declared| {
                    conn.peer_may_read(&requester, &declared.table, &declared.column, file_id)
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
