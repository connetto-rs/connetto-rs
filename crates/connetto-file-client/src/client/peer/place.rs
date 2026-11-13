//! Placement in the media driver: which replicated files this device pulls
//! into custody, marks leaving, withdraws and releases, and the copies it
//! marks leaving under storage pressure (R79 decisions 12 to 16).

use core::fmt::Display;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::media::placement::{
    Candidate, Mine, Peer, Placement, Verdict, View, decide, hash_term,
};
use connetto_file_core::{ChunkInventory, FileId, FreeSpace, MaybeSend};

use super::fetch::{Fetch, Purpose};
use super::{Action, ContentClient, ContentEvent, CustodyCopy, Done, Driver, Job, custody};
use crate::db;
use crate::error::ContentError;
use crate::http::ContentHttp;

/// What placement decided for one file.
struct Decided {
    file_id: FileId,
    verdict: Verdict,
    /// This device's custody state of the file, `None` when it holds none.
    state: Option<String>,
    /// The key ids of the authors of the provisional rows naming it.
    authors: Vec<Vec<u8>>,
    /// The file's size in bytes.
    size: u64,
}

impl<T, B, H> ContentClient<T, B, H>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkInventory + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
{
    /// Takes `file_id` out of custody, and for a copy leaving under
    /// pressure deletes its manifest, outboard and every chunk no other
    /// manifest names, unless it is unsent or pinned.
    async fn release_custody(&self, file_id: FileId, delete: bool) -> Result<(), ContentError> {
        let _writing = self.content_writes.lock().await;
        let referenced = self
            .client
            .with_conn(|conn| {
                conn.transact_with_bookkeeping(
                    |c| -> Result<_, ContentError> {
                        db::leave_custody(c, file_id)?;
                        if !delete
                            || db::is_unsent(c, file_id)?
                            || crate::retain::pinned_ids(c)?.contains(&file_id)
                        {
                            return Ok(None);
                        }
                        db::drop_manifest(c, file_id)?;
                        Ok(Some(db::referenced_hashes(c)?))
                    },
                    |_| Ok::<(), ContentError>(()),
                )
                .map(|(referenced, ())| referenced)
            })
            .await??;
        if let Some(referenced) = referenced {
            self.delete_unreferenced(&referenced).await?;
        }
        Ok(())
    }

    /// Sets the custody state of each file in `states`.
    async fn set_custody_states(&self, states: Vec<(FileId, &'static str)>) {
        if states.is_empty() {
            return;
        }
        let _ = self
            .client
            .with_conn(move |conn| -> Result<(), ContentError> {
                for (file_id, state) in states {
                    db::set_custody_state(conn.conn(), file_id, state)?;
                }
                Ok(())
            })
            .await;
    }
}

impl<T, B, H, F, J> Driver<'_, T, B, H, F, J>
where
    T: Transport + MaybeSend + 'static,
    T::Error: Display,
    B: ChunkInventory + FreeSpace + Clone + Sync + MaybeSend + 'static,
    H: ContentHttp,
    F: Fn(Job) -> J,
    J: Future<Output = Done>,
{
    /// Ranks every replicated file this device holds in custody or could
    /// pull, and acts on each verdict: a pull starts or stops, a copy above
    /// the count is marked leaving, a leaving mark is withdrawn, an
    /// acknowledged copy is released, and stranded pressed copies raise
    /// [`ContentEvent::CustodyPressed`]. A file whose size is unknown has
    /// its manifest asked for first.
    pub(super) async fn place(&mut self, now: Instant) -> Vec<Action> {
        let Some(own) = self.sender.own() else {
            return Vec::new();
        };
        let content = self.content;
        let declared = &content.peer_content;
        let Ok(Ok((placed, unsent))) = self
            .beside_jobs(content.client.with_conn(|conn| -> Result<_, ContentError> {
                let placed = custody::placing(conn, declared)?;
                Ok((placed, custody::own_replicated(conn.conn(), declared)?))
            }))
            .await
        else {
            return Vec::new();
        };
        let (custody, held_bytes) = self.beside_jobs(content.custody_held()).await;
        let room = content.room(self.budget, held_bytes);
        let own_key = *own.key().as_bytes();
        let mine: HashMap<FileId, &CustodyCopy> =
            custody.iter().map(|copy| (copy.file_id, copy)).collect();
        let linked: Vec<DeviceIdentity> = self.links.linked_peers().cloned().collect();
        // A settled row's file is pulled only to take over a copy a linked
        // holder marks leaving, never on its own while the server may still
        // be receiving it from its author.
        let handed_on = |file_id: &FileId| {
            linked.iter().any(|peer| {
                self.links
                    .report(peer)
                    .is_some_and(|report| report.is_leaving(file_id.as_bytes()))
            })
        };
        let files: Vec<(FileId, &custody::Placed)> = placed
            .iter()
            .filter(|(file_id, info)| {
                !unsent.contains(*file_id)
                    && !info
                        .authors
                        .iter()
                        .any(|author| author.as_slice() == own_key)
                    && (info.pulled
                        || mine.contains_key(*file_id)
                        || (info.settled && handed_on(file_id)))
            })
            .map(|(file_id, info)| (*file_id, info))
            .collect();
        let admitted = self.admitted_takers(&files, &mine, &linked).await;

        let my_rates: BTreeMap<[u8; 32], u64> = self.rates().into_iter().collect();
        let me = Candidate {
            key: own_key,
            account: own.account(),
            room,
            rates: &my_rates,
        };
        let mut decided = Vec::new();
        let mut probes = Vec::new();
        for (file_id, info) in &files {
            let copy = mine.get(file_id).copied();
            match self.judge(*file_id, info, copy, me, &linked, &admitted) {
                Ok((verdict, size)) => decided.push(Decided {
                    file_id: *file_id,
                    verdict,
                    state: copy.map(|copy| copy.state.clone()),
                    authors: info.authors.clone(),
                    size,
                }),
                Err(sources) if room > 0 => probes.push((*file_id, sources)),
                Err(_) => {}
            }
        }
        self.act_on(decided, probes, held_bytes, now).await
    }

    /// The verdict for `file_id` with its size, or when its size is unknown
    /// the key ids of the linked devices that could tell it, its authors and
    /// holders.
    fn judge(
        &self,
        file_id: FileId,
        info: &custody::Placed,
        copy: Option<&CustodyCopy>,
        me: Candidate<'_>,
        linked: &[DeviceIdentity],
        admitted: &HashSet<(FileId, DeviceIdentity)>,
    ) -> Result<(Verdict, u64), Vec<[u8; 32]>> {
        let is_author = |peer: &DeviceIdentity| {
            info.authors
                .iter()
                .any(|author| author.as_slice() == peer.key().as_bytes())
        };
        let no_marks = BTreeMap::new();
        let mine = match copy {
            None => Mine::Absent,
            Some(copy) if copy.state == db::HELD => Mine::Held,
            Some(copy) => Mine::Leaving {
                pressed: copy.state == db::PRESSED,
                marks: self.marks.get(&file_id).unwrap_or(&no_marks),
            },
        };
        let peers: Vec<Peer<'_>> = linked
            .iter()
            .filter(|peer| !is_author(peer))
            .filter_map(|peer| {
                let report = self.links.report(peer)?;
                let view = if report.is_leaving(file_id.as_bytes()) {
                    View::Leaving
                } else if report.holds(file_id.as_bytes()) {
                    View::Held
                } else {
                    View::Absent
                };
                Some(Peer {
                    candidate: Candidate {
                        key: *peer.key().as_bytes(),
                        account: peer.account(),
                        room: report.room(),
                        rates: report.rates(),
                    },
                    admitted: admitted.contains(&(file_id, peer.clone())),
                    view,
                    seen: report.seen(),
                })
            })
            .collect();
        let authors: Vec<[u8; 32]> = linked
            .iter()
            .filter(|peer| is_author(peer))
            .map(|peer| *peer.key().as_bytes())
            .collect();
        let size = copy
            .map(|copy| copy.size)
            .or_else(|| self.sizes.get(&file_id).copied());
        let Some(size) = size else {
            let holders = peers
                .iter()
                .filter(|peer| peer.view != View::Absent)
                .map(|peer| peer.candidate.key);
            return Err(authors.iter().copied().chain(holders).collect());
        };
        let verdict = decide(&Placement {
            file: *file_id.as_bytes(),
            size,
            copies: info.copies,
            me,
            mine,
            peers,
            authors,
        });
        Ok((verdict, size))
    }

    /// The pairs of a file and a linked peer, not its author and reporting
    /// it lacks the file, whose gate on this device admits the file, asked
    /// only for files this device lacks or presses, where takers matter.
    async fn admitted_takers(
        &mut self,
        files: &[(FileId, &custody::Placed)],
        mine: &HashMap<FileId, &CustodyCopy>,
        linked: &[DeviceIdentity],
    ) -> HashSet<(FileId, DeviceIdentity)> {
        let mut asks = Vec::new();
        for (file_id, info) in files {
            if mine
                .get(file_id)
                .is_some_and(|copy| copy.state != db::PRESSED)
            {
                continue;
            }
            for peer in linked {
                let author = info
                    .authors
                    .iter()
                    .any(|author| author.as_slice() == peer.key().as_bytes());
                if !author
                    && self
                        .links
                        .report(peer)
                        .is_some_and(|report| !report.holds(file_id.as_bytes()))
                {
                    asks.push((*file_id, peer.clone()));
                }
            }
        }
        let content = self.content;
        let sender = self.sender.clone();
        self.beside_jobs(async move {
            let mut admitted = HashSet::new();
            for (file_id, peer) in asks {
                if content.admits(&sender, &peer, file_id.as_bytes()).await {
                    admitted.insert((file_id, peer));
                }
            }
            admitted
        })
        .await
    }

    /// Carries out `decided` and asks for the manifests `probes` name.
    async fn act_on(
        &mut self,
        decided: Vec<Decided>,
        probes: Vec<(FileId, Vec<[u8; 32]>)>,
        held_bytes: u64,
        now: Instant,
    ) -> Vec<Action> {
        let content = self.content;
        let mut actions = Vec::new();
        let mut states = Vec::new();
        let mut changed = false;
        let mut stranded = HashSet::new();
        for Decided {
            file_id,
            verdict,
            state,
            authors,
            size,
        } in decided
        {
            match verdict {
                Verdict::Pull => {
                    if !self.tags.contains_key(&file_id) {
                        let candidates = self.candidates(file_id, &authors);
                        actions.extend(self.start(file_id, Purpose::Custody, candidates, now));
                    }
                }
                Verdict::Nothing => {
                    if state.is_none() {
                        self.stop_pull(file_id);
                    }
                }
                Verdict::Surplus => {
                    states.push((file_id, db::LEAVING));
                    self.marks.insert(file_id, BTreeMap::new());
                }
                Verdict::Withdraw => {
                    states.push((file_id, db::HELD));
                    self.marks.remove(&file_id);
                }
                Verdict::Release => {
                    let pressed = state.as_deref() == Some(db::PRESSED);
                    let released = self
                        .beside_jobs(content.release_custody(file_id, pressed))
                        .await;
                    if released.is_ok() {
                        self.marks.remove(&file_id);
                        self.sizes.insert(file_id, size);
                        changed = true;
                    }
                }
                Verdict::Stranded => {
                    stranded.insert(file_id);
                }
            }
        }
        for (file_id, sources) in probes {
            if self.tags.contains_key(&file_id) {
                continue;
            }
            let candidates: VecDeque<DeviceIdentity> = self
                .links
                .linked_peers()
                .filter(|peer| sources.contains(peer.key().as_bytes()))
                .cloned()
                .collect();
            if !candidates.is_empty() {
                actions.extend(self.start(file_id, Purpose::Size, candidates, now));
            }
        }
        changed |= !states.is_empty();
        self.beside_jobs(content.set_custody_states(states)).await;
        if changed {
            self.custody_changed().await;
            self.placement_due = true;
        }
        if stranded
            .iter()
            .any(|file_id| !self.stranded.contains(file_id))
        {
            let _ = content
                .events
                .send(ContentEvent::CustodyPressed { held_bytes });
        }
        self.stranded = stranded;
        actions
    }

    /// Starts a fetch of `file_id` for `purpose`, asking `candidates` in
    /// order.
    fn start(
        &mut self,
        file_id: FileId,
        purpose: Purpose,
        candidates: VecDeque<DeviceIdentity>,
        now: Instant,
    ) -> Vec<Action> {
        let tag = self.next_tag;
        self.next_tag += 1;
        let mut fetch = Fetch::new(file_id, now);
        fetch.purpose = purpose;
        fetch.candidates = candidates;
        self.fetches.insert(tag, fetch);
        self.tags.insert(file_id, tag);
        self.next_holder(tag, now)
    }

    /// Stops a custody pull of `file_id`, its written chunks left to the
    /// orphan sweep. A fetch a resolve waits on goes on.
    fn stop_pull(&mut self, file_id: FileId) {
        if let Some(&tag) = self.tags.get(&file_id)
            && self
                .fetches
                .get(&tag)
                .is_some_and(|fetch| fetch.purpose == Purpose::Custody)
        {
            self.finish(tag, Ok(false));
        }
    }

    /// Marks leaving under pressure the copies this device ranks lowest
    /// for, its copies above the count first, until their bytes cover what
    /// its free space lacks under the reserve or its custody holds over the
    /// budget, less what pressed copies already cover.
    pub(super) async fn press(&mut self) {
        let content = self.content;
        let Ok(free) = content.store.free_bytes() else {
            return;
        };
        let (custody, held) = self.beside_jobs(content.custody_held()).await;
        let shortfall = content
            .custody_reserve
            .saturating_sub(free)
            .max(held.saturating_sub(self.budget));
        let pressed: u64 = custody
            .iter()
            .filter(|copy| copy.state == db::PRESSED)
            .map(|copy| copy.size)
            .sum();
        let mut owed = shortfall.saturating_sub(pressed);
        let Some(own) = self.sender.own().filter(|_| owed > 0) else {
            return;
        };
        let mut order: Vec<&CustodyCopy> = custody
            .iter()
            .filter(|copy| copy.state != db::PRESSED)
            .collect();
        order.sort_by_key(|copy| {
            (
                copy.state == db::HELD,
                hash_term(copy.file_id.as_bytes(), own.key().as_bytes()),
            )
        });
        let mut states = Vec::new();
        for copy in order {
            if owed == 0 {
                break;
            }
            owed = owed.saturating_sub(copy.size);
            states.push((copy.file_id, db::PRESSED));
            // A copy already leaving as surplus goes on with its attempt.
            if copy.state == db::HELD {
                self.marks.insert(copy.file_id, BTreeMap::new());
            }
        }
        self.beside_jobs(content.set_custody_states(states)).await;
        self.custody_changed().await;
        self.placement_due = true;
    }
}
