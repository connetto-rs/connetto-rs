//! The device-to-device exchange as the enrolment task drives it (R77), the
//! device writer's own offer, signed once for every linked peer, and the
//! `connetto-exchange` state machine fed the link's events and messages,
//! whose actions this module carries out against the replica and the link.

#[cfg(feature = "peer")]
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "peer")]
use std::time::Instant;

#[cfg(feature = "peer")]
use connetto_core::chain::{offer_message, verify_offer};
#[cfg(feature = "peer")]
use connetto_core::device_cert::{DeviceIdentity, DeviceKeyError};
#[cfg(feature = "peer")]
use connetto_core::exchange::{EndOutcome, Offer, SignedOffer};
#[cfg(feature = "peer")]
use connetto_core::{Major, SchemaVersion};
#[cfg(feature = "peer")]
use connetto_exchange::{Action, Exchange, Message, Transfer, manifest};
#[cfg(feature = "peer")]
use connetto_peer::{CloseReason, PeerSendError};
#[cfg(feature = "peer")]
use tokio::sync::mpsc;

#[cfg(feature = "peer")]
use super::{Link, Run};
#[cfg(feature = "peer")]
use crate::ClientEvent;
#[cfg(feature = "peer")]
use crate::peer::PeerAuthor;

/// A signature over the own offer, computed on the blocking pool since a
/// chip signs slowly.
#[cfg(feature = "peer")]
type Signing = tokio::task::JoinHandle<Result<Vec<u8>, DeviceKeyError>>;

/// What the signature in flight came to.
#[cfg(feature = "peer")]
type Signed = Result<Result<Vec<u8>, DeviceKeyError>, tokio::task::JoinError>;

/// What woke the exchange, never anything when the build has no peer link.
#[cfg(feature = "peer")]
pub(super) enum Wake {
    /// The own offer's signature landed.
    Signed(Signed),
    /// A served stream to `.0` under request `.1` ended.
    Served(DeviceIdentity, u64),
    /// A request's frame bound or a retry fell due.
    Deadline,
}
#[cfg(not(feature = "peer"))]
pub(super) type Wake = core::convert::Infallible;

/// An author as its link proved it.
#[cfg(feature = "peer")]
struct Linked {
    /// Who the author is to the peer apply.
    author: PeerAuthor,
    /// The `SubjectPublicKeyInfo` the link proved, which its offers verify
    /// against.
    public_key: Vec<u8>,
}

/// The exchange's state in the task, empty when the build has no peer link.
pub(super) struct PeerExchange {
    /// The own offer being signed and its signature in flight.
    #[cfg(feature = "peer")]
    signing: Option<(Offer, Signing)>,
    /// Whether the replica changed while the signature was in flight, so the
    /// offer is read and signed again once it lands.
    #[cfg(feature = "peer")]
    stale: bool,
    /// The state machine of both roles.
    #[cfg(feature = "peer")]
    machine: Exchange<DeviceIdentity>,
    /// The linked authors.
    #[cfg(feature = "peer")]
    linked: HashMap<DeviceIdentity, Linked>,
    /// The range each peer is being served, under its request id.
    #[cfg(feature = "peer")]
    streams: HashMap<DeviceIdentity, (u64, tokio::task::JoinHandle<()>)>,
    /// Where a stream reports its end.
    #[cfg(feature = "peer")]
    served_tx: mpsc::UnboundedSender<(DeviceIdentity, u64)>,
    /// The streams' ends, as the task reads them.
    #[cfg(feature = "peer")]
    served_rx: mpsc::UnboundedReceiver<(DeviceIdentity, u64)>,
}

impl PeerExchange {
    /// An exchange requesting under `build`, none when the build declares no
    /// schema version.
    pub(super) fn new(#[cfg(feature = "peer")] build: Option<(Major, SchemaVersion)>) -> Self {
        #[cfg(feature = "peer")]
        let (served_tx, served_rx) = mpsc::unbounded_channel();
        Self {
            #[cfg(feature = "peer")]
            signing: None,
            #[cfg(feature = "peer")]
            stale: false,
            #[cfg(feature = "peer")]
            machine: Exchange::new(build),
            #[cfg(feature = "peer")]
            linked: HashMap::new(),
            #[cfg(feature = "peer")]
            streams: HashMap::new(),
            #[cfg(feature = "peer")]
            served_tx,
            #[cfg(feature = "peer")]
            served_rx,
        }
    }

    /// The next thing the exchange has to act on, or a future that never
    /// resolves without the peer link.
    pub(super) async fn next(&mut self) -> Wake {
        #[cfg(feature = "peer")]
        {
            let deadline = self.machine.next_deadline();
            let signing = &mut self.signing;
            let served = &mut self.served_rx;
            tokio::select! {
                signed = async {
                    match signing {
                        Some((_, handle)) => handle.await,
                        None => core::future::pending().await,
                    }
                } => Wake::Signed(signed),
                Some((peer, request_id)) = served.recv() => Wake::Served(peer, request_id),
                () = async {
                    match deadline {
                        Some(deadline) => {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                        }
                        None => core::future::pending().await,
                    }
                } => Wake::Deadline,
            }
        }
        #[cfg(not(feature = "peer"))]
        core::future::pending().await
    }
}

/// The exchange's entry points, one line of R77's state table each.
#[cfg(feature = "peer")]
impl<L: Link> Run<L> {
    /// The replica's offer changed, by a local write, a settled or refused
    /// entry, or a fresh key. A signature in flight comes out stale, and
    /// otherwise the linked peers get a fresh offer, or the next link up
    /// signs one when no peer is linked.
    pub(super) async fn offer_changed(&mut self) {
        self.exchange.machine.clear_own_offer();
        if self.exchange.signing.is_some() {
            self.exchange.stale = true;
        } else if !self.exchange.linked.is_empty() {
            self.sign_offer().await;
        }
    }

    /// The device writer retired, so offers stop, requests are answered
    /// `NotAvailable` and a signature in flight is dropped, until a fresh
    /// key enrols.
    pub(super) async fn offers_retired(&mut self) {
        if let Some((_, signing)) = self.exchange.signing.take() {
            signing.abort();
        }
        self.exchange.stale = false;
        let actions = self.exchange.machine.retire();
        self.act(actions).await;
    }

    /// Read the device writer's offer and start its signature. A device
    /// without a certificate, or whose writer retired, offers nothing. A
    /// certificate is only held beside the key the run opened for it.
    async fn sign_offer(&mut self) {
        self.exchange.stale = false;
        let (Some(held), Some(key)) = (&self.held, self.key.clone()) else {
            return;
        };
        let key_id = *held.leaf.identity().key().as_bytes();
        let offer = match self.link.offer(key_id).await {
            Ok(Some(offer)) => offer,
            Ok(None) => return,
            Err(err) => {
                tracing::warn!(error = %err, "the device writer's offer could not be read");
                return;
            }
        };
        let message = offer_message(&key_id, &offer);
        let signing = tokio::task::spawn_blocking(move || key.sign(&message));
        self.exchange.signing = Some((offer, signing));
    }

    /// Act on what woke the exchange.
    pub(super) async fn exchange_woke(&mut self, wake: Wake) {
        match wake {
            Wake::Signed(signed) => self.offer_signed(signed).await,
            Wake::Served(peer, request_id) => {
                // A late end of an earlier stream leaves the current one.
                if self
                    .exchange
                    .streams
                    .get(&peer)
                    .is_some_and(|(serving, _)| *serving == request_id)
                {
                    self.exchange.streams.remove(&peer);
                }
                let actions = self.exchange.machine.served(&peer, request_id);
                self.act(actions).await;
            }
            Wake::Deadline => {
                let actions = self.exchange.machine.timeout(Instant::now());
                self.act(actions).await;
            }
        }
    }

    /// The own offer's signature landed. A replica that changed meanwhile
    /// is read and signed again, and otherwise the machine sends the offer to
    /// every linked peer.
    async fn offer_signed(&mut self, signed: Signed) {
        let (offer, _) = self
            .exchange
            .signing
            .take()
            .expect("a signature lands only while one is in flight");
        if self.exchange.stale {
            self.sign_offer().await;
            return;
        }
        let signature = match signed {
            Ok(Ok(signature)) => signature,
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "the device key could not sign the offer");
                return;
            }
            Err(err) => {
                tracing::warn!(error = %err, "the offer's signature did not finish");
                return;
            }
        };
        let actions = self
            .exchange
            .machine
            .set_own_offer(SignedOffer { offer, signature });
        self.act(actions).await;
    }

    /// A link came up with `peer`, whose certificate proved `public_key` and
    /// carries `bot_list`.
    pub(super) async fn peer_linked(
        &mut self,
        peer: DeviceIdentity,
        public_key: Vec<u8>,
        bot_list: Option<String>,
    ) {
        let author = PeerAuthor {
            key_id: peer.key().as_bytes().to_vec(),
            account: peer.account().to_owned(),
            bot_list,
        };
        let cursor = match self.link.peer_cursor(author.key_id.clone()).await {
            Ok(cursor) => cursor,
            Err(err) => {
                tracing::warn!(error = %err, "a peer's cursor could not be read, so it starts over");
                crate::exchange::FRESH_CURSOR
            }
        };
        self.exchange
            .linked
            .insert(peer.clone(), Linked { author, public_key });
        let actions = self.exchange.machine.linked(peer, cursor);
        self.act(actions).await;
        if self.exchange.machine.own_offer().is_none() && self.exchange.signing.is_none() {
            self.sign_offer().await;
        }
    }

    /// The last link to `peer` closed.
    pub(super) async fn peer_unlinked(&mut self, peer: &DeviceIdentity) {
        self.exchange.linked.remove(peer);
        let actions = self.exchange.machine.unlinked(peer);
        self.act(actions).await;
    }

    /// `peer` sent `message`. An offer is checked against the key the link
    /// proved before the machine sees it.
    pub(super) async fn peer_message(&mut self, peer: &DeviceIdentity, message: Message) {
        let now = Instant::now();
        let actions = match message {
            Message::Offer(signed) => {
                let valid = self.exchange.linked.get(peer).is_some_and(|linked| {
                    verify_offer(&linked.public_key, peer.key().as_bytes(), &signed)
                });
                self.exchange.machine.offer(peer, signed, valid, now)
            }
            Message::Transfer(transfer) => self.exchange.machine.message(peer, transfer, now),
        };
        self.act(actions).await;
    }

    /// Carry out the machine's actions in order, feeding a failed commit back
    /// to it.
    async fn act(&mut self, actions: Vec<Action<DeviceIdentity>>) {
        let mut actions = VecDeque::from(actions);
        while let Some(action) = actions.pop_front() {
            match action {
                Action::Send { peer, message } => {
                    self.send(&peer, message).await;
                }
                Action::Adopt {
                    peer,
                    offer,
                    cursor,
                } => {
                    let author = self.linked_author(&peer);
                    if let Err(err) = self.link.adopt(author, offer, cursor).await {
                        tracing::warn!(error = %err, "a peer's offer could not be taken");
                        self.commit_failed(&peer, Commit::Adopt, &mut actions).await;
                    }
                }
                Action::Apply {
                    peer,
                    row,
                    body,
                    cursor,
                } => {
                    self.take_row(&peer, row, Some(body), cursor, &mut actions)
                        .await;
                }
                Action::Skip { peer, row, cursor } => {
                    self.take_row(&peer, row, None, cursor, &mut actions).await;
                }
                Action::ChainInvalid { peer } => {
                    self.link.emit(ClientEvent::PeerChainInvalid {
                        author: peer.key().as_bytes().to_vec(),
                    });
                }
                Action::Serve {
                    peer,
                    request_id,
                    from,
                    to,
                    build,
                } => {
                    let key_id = self
                        .held
                        .as_ref()
                        .map(|held| *held.leaf.identity().key().as_bytes());
                    let stream = tokio::spawn(stream(
                        self.link.clone(),
                        self.peer.clone(),
                        peer.clone(),
                        key_id,
                        Range {
                            request_id,
                            from,
                            to,
                            build,
                        },
                        self.exchange.served_tx.clone(),
                    ));
                    let previous = self.exchange.streams.insert(peer, (request_id, stream));
                    debug_assert!(previous.is_none(), "a peer is served one range at a time");
                }
                Action::StopServing { peer } => {
                    if let Some((_, stream)) = self.exchange.streams.remove(&peer) {
                        stream.abort();
                    }
                }
            }
        }
    }

    /// Take `peer`'s entry `row` with the cursor at it, feeding a failed
    /// commit back to the machine.
    async fn take_row(
        &mut self,
        peer: &DeviceIdentity,
        row: connetto_core::exchange::ManifestRow,
        body: Option<Vec<u8>>,
        cursor: connetto_core::exchange::Frontier,
        actions: &mut VecDeque<Action<DeviceIdentity>>,
    ) {
        let author = self.linked_author(peer);
        if let Err(err) = self.link.take_row(author, row, body, cursor).await {
            tracing::warn!(error = %err, "a peer's entry could not be taken");
            self.commit_failed(peer, Commit::Row, actions).await;
        }
    }

    /// Queue `message` on the link to `peer`. A link that closed meanwhile
    /// reaches the machine as unlinked, while a message over the wire's
    /// bounds is a defect of this device the receiver would wait out.
    async fn send(&self, peer: &DeviceIdentity, message: Message) {
        match self.peer.send_exchange(peer, message).await {
            Ok(()) => {}
            Err(err @ PeerSendError::FrameAboveBounds { .. }) => {
                tracing::error!(error = %err, "an exchange message could not be sent");
            }
            Err(err) => tracing::debug!(error = %err, "an exchange message did not reach a peer"),
        }
    }

    /// A commit for `peer` failed, so the commits queued after it for that
    /// peer are dropped and the machine restarts from the cursor the replica
    /// holds, owing the offer again when it was the adopt that failed. A
    /// replica that cannot even read that cursor closes the link, so the
    /// exchange starts over from a fresh read at the next link up.
    async fn commit_failed(
        &mut self,
        peer: &DeviceIdentity,
        commit: Commit,
        actions: &mut VecDeque<Action<DeviceIdentity>>,
    ) {
        actions.retain(|action| match action {
            Action::Adopt { peer: of, .. }
            | Action::Apply { peer: of, .. }
            | Action::Skip { peer: of, .. } => of != peer,
            _ => true,
        });
        let key_id = self.linked_author(peer).key_id;
        match self.link.peer_cursor(key_id).await {
            Ok(stored) => match commit {
                Commit::Adopt => self
                    .exchange
                    .machine
                    .adopt_failed(peer, stored, Instant::now()),
                Commit::Row => self
                    .exchange
                    .machine
                    .commit_failed(peer, stored, Instant::now()),
            },
            Err(err) => {
                tracing::warn!(error = %err, "a peer's cursor could not be read after a failed commit");
                self.peer.close(peer, CloseReason::Closed);
            }
        }
    }

    /// The author `peer`'s link proved. The machine acts only for linked
    /// peers, and a link's author is kept from its link up to its unlink.
    fn linked_author(&self, peer: &DeviceIdentity) -> PeerAuthor {
        self.exchange
            .linked
            .get(peer)
            .expect("the machine acts only for a linked peer")
            .author
            .clone()
    }
}

/// Which commit of a peer's exchange failed.
#[cfg(feature = "peer")]
#[derive(Clone, Copy)]
enum Commit {
    /// The adopt of the peer's offer.
    Adopt,
    /// An entry's apply or skip.
    Row,
}

/// A range a peer requested.
#[cfg(feature = "peer")]
struct Range {
    request_id: u64,
    from: u64,
    to: u64,
    build: (Major, SchemaVersion),
}

/// Stream `range` of the device writer `key_id` to `peer`, ending it
/// `Complete`, or `NotAvailable` when the writer no longer holds it whole,
/// then report the end to the task. A link that closes mid-stream ends it
/// silently.
#[cfg(feature = "peer")]
async fn stream<L: Link>(
    link: L,
    node: connetto_peer::Node,
    peer: DeviceIdentity,
    key_id: Option<[u8; 32]>,
    range: Range,
    served: mpsc::UnboundedSender<(DeviceIdentity, u64)>,
) {
    let outcome = match key_id {
        Some(key_id) => serve(&link, &node, &peer, key_id, &range).await,
        None => Some(EndOutcome::NotAvailable),
    };
    if let Some(outcome) = outcome {
        let end = Message::Transfer(Transfer::End {
            request_id: range.request_id,
            outcome,
        });
        // A link that closed meanwhile ends the exchange with it.
        let _ = node.send_exchange(&peer, end).await;
    }
    let _ = served.send((peer, range.request_id));
}

/// The manifest and bodies of `range`, and how it ends, `None` once the link
/// closed.
#[cfg(feature = "peer")]
async fn serve<L: Link>(
    link: &L,
    node: &connetto_peer::Node,
    peer: &DeviceIdentity,
    key_id: [u8; 32],
    range: &Range,
) -> Option<EndOutcome> {
    let served = match link
        .serve_range(key_id, range.from, range.to, range.build.clone())
        .await
    {
        Ok(Some(served)) => served,
        Ok(None) => return Some(EndOutcome::NotAvailable),
        Err(err) => {
            tracing::warn!(error = %err, "a requested range could not be read");
            return Some(EndOutcome::NotAvailable);
        }
    };
    for message in manifest(range.request_id, served.previous, served.rows) {
        node.send_exchange(peer, message).await.ok()?;
    }
    for seq in served.bodies {
        let changeset = match link.serve_body(key_id, seq).await {
            Ok(Some(changeset)) => changeset,
            Ok(None) => return Some(EndOutcome::NotAvailable),
            Err(err) => {
                tracing::warn!(error = %err, "a requested body could not be read");
                return Some(EndOutcome::NotAvailable);
            }
        };
        let body = Message::Transfer(Transfer::Body {
            request_id: range.request_id,
            seq,
            changeset,
        });
        node.send_exchange(peer, body).await.ok()?;
    }
    Some(EndOutcome::Complete)
}
