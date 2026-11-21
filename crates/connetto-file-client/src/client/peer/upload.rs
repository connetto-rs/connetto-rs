//! The holder's upload: a custody copy whose row settled while the server
//! lacks the file is uploaded, credited to the row's owner, at once by a
//! courier and otherwise after a wait staggered by the holder's rank (R79
//! decision 17).

use core::fmt::Display;
use std::collections::BTreeSet;
use std::time::Duration;

use connetto_core::device_cert::DeviceIdentity;
use connetto_core::traits::Transport;
use connetto_exchange::media::placement::{Candidate, rank};
use connetto_file_core::{ChunkInventory, FileId, FreeSpace, MaybeSend};

use super::{CustodyCopy, Done, Driver, Job, custody, wake_at};
use crate::db;
use crate::error::ContentError;
use crate::http::ContentHttp;

/// How long each rank waits after the holder learned the row settled.
const UPLOAD_STAGGER: Duration = Duration::from_mins(10);

/// This device's rank, from 1, among `holders` of `file` by decision 14's
/// score, `me` counted among them.
fn holder_rank(file: &[u8; 32], me: Candidate<'_>, holders: &[Candidate<'_>]) -> u64 {
    let mut candidates = holders.to_vec();
    candidates.push(me);
    let position = rank(file, &candidates, &BTreeSet::new())
        .iter()
        .position(|candidate| candidate.key == me.key)
        .unwrap_or(0);
    u64::try_from(position).map_or(u64::MAX, |position| position.saturating_add(1))
}

/// When, in Unix seconds, a holder of rank `rank` uploads a copy whose row
/// it learned settled at `settled`, at once when its own courier call
/// settled it.
fn upload_due(settled: i64, rank: u64, couriered: bool) -> i64 {
    if couriered {
        return settled;
    }
    let wait = rank.saturating_mul(UPLOAD_STAGGER.as_secs());
    settled.saturating_add(i64::try_from(wait).unwrap_or(i64::MAX))
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
    /// Queues the upload of every settled custody copy whose wait ended,
    /// marking it queued in `custody`, and sets the wake for the earliest
    /// wait still running. The upload credits the owner the copy entered
    /// custody with, else the account its author's cursor names. A copy
    /// with neither, one a `lost` state returned, is left to the heal path.
    pub(super) async fn queue_uploads(
        &mut self,
        custody: &mut [CustodyCopy],
        me: Candidate<'_>,
        linked: &[DeviceIdentity],
    ) {
        let now = custody::unix_now();
        let mut next: Option<i64> = None;
        let mut due = Vec::new();
        for copy in custody.iter() {
            let (db::Upload::None, Some(settled)) = (copy.upload, copy.settled_seen_at) else {
                continue;
            };
            if copy.owner.is_none() && copy.author.is_none() {
                continue;
            }
            let author = copy.author.as_deref();
            let holders: Vec<Candidate<'_>> = linked
                .iter()
                .filter(|peer| Some(peer.key().as_bytes().as_slice()) != author)
                .filter_map(|peer| {
                    let report = self.links.report(peer)?;
                    report.holds(copy.file_id.as_bytes()).then(|| Candidate {
                        key: *peer.key().as_bytes(),
                        account: peer.account(),
                        room: report.room(),
                        rates: report.rates(),
                    })
                })
                .collect();
            let rank = holder_rank(copy.file_id.as_bytes(), me, &holders);
            let couriered = author.is_some_and(|author| self.couriered.contains(author));
            let at = upload_due(settled, rank, couriered);
            if at <= now {
                due.push((copy.file_id, copy.owner.clone(), copy.author.clone()));
            } else {
                next = Some(next.map_or(at, |next: i64| next.min(at)));
            }
        }
        self.couriered.clear();
        self.upload_at = next.map(wake_at);
        if due.is_empty() {
            return;
        }
        let content = self.content;
        let queued: Vec<FileId> = self
            .beside_jobs(content.client.with_conn(move |conn| {
                let mut queued = Vec::new();
                for (file_id, owner, author) in due {
                    let owner = match (owner, author) {
                        (Some(owner), _) => owner,
                        (None, Some(author)) => match conn.author_account(&author)? {
                            Some(owner) => owner,
                            None => continue,
                        },
                        (None, None) => continue,
                    };
                    db::enqueue_carried(conn.conn(), file_id, &owner)?;
                    queued.push(file_id);
                }
                Ok::<_, ContentError>(queued)
            }))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        if queued.is_empty() {
            return;
        }
        for copy in custody.iter_mut() {
            if queued.contains(&copy.file_id) {
                copy.upload = db::Upload::Queued;
            }
        }
        content.outbox_wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn candidate<'a>(
        key: u8,
        account: &'a str,
        rates: &'a BTreeMap<[u8; 32], u64>,
    ) -> Candidate<'a> {
        Candidate {
            key: [key; 32],
            account,
            room: 1 << 30,
            rates,
        }
    }

    /// Two holders reading each other's reports take ranks 1 and 2, so
    /// their uploads fall 10 minutes apart and never at once.
    #[test]
    fn two_holders_agree_on_distinct_ranks() {
        let rates = BTreeMap::new();
        let first = candidate(1, "bob", &rates);
        let second = candidate(2, "carol", &rates);
        for file in [[3; 32], [4; 32], [5; 32]] {
            let mut ranks = [
                holder_rank(&file, first, &[second]),
                holder_rank(&file, second, &[first]),
            ];
            ranks.sort_unstable();
            assert_eq!(ranks, [1, 2], "file {file:?}");
        }
    }

    /// The best-ranked holder waits one stagger, the next two, and a courier
    /// none.
    #[test]
    fn the_wait_grows_with_the_rank_and_a_courier_does_not_wait() {
        assert_eq!(upload_due(1_000, 1, false), 1_600);
        assert_eq!(upload_due(1_000, 2, false), 2_200);
        assert_eq!(upload_due(1_000, 2, true), 1_000);
    }
}
