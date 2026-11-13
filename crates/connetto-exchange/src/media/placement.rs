//! Placement: which linked devices keep a replicated file and when a holder
//! hands its copy on (R79 decisions 12 to 16).
//!
//! Every device ranks the candidates for a file with the same function over
//! the reports it holds, so devices that read the same reports agree with no
//! coordinator. [`decide`] answers what this device does next for one file,
//! the C table of R79's "Every event in every situation", and reads no
//! store, sends nothing and keeps no state.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// One device a placement ranks.
#[derive(Clone, Copy, Debug)]
pub struct Candidate<'a> {
    /// The device's key id.
    pub key: [u8; 32],
    /// The account its certificate names.
    pub account: &'a str,
    /// The bytes it has left for copies held for others.
    pub room: u64,
    /// Its measured rate from each device it fetched from, by key id.
    pub rates: &'a BTreeMap<[u8; 32], u64>,
}

/// What this device reads of one linked peer's custody of the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum View {
    /// P0, the peer does not hold it.
    Absent,
    /// P1, the peer holds it.
    Held,
    /// P2, the peer holds it and wants another device to take it over.
    Leaving,
}

/// One linked peer that reported, as a placement reads it.
#[derive(Clone, Copy, Debug)]
pub struct Peer<'a> {
    /// The peer as a candidate.
    pub candidate: Candidate<'a>,
    /// Whether the peer's gate on this device admits the file, so it could
    /// pull it.
    pub admitted: bool,
    /// The peer's custody of the file.
    pub view: View,
    /// The number of the latest report of this device's the peer had
    /// received when it sent its latest report.
    pub seen: Option<u64>,
}

/// This device's own custody of the file.
#[derive(Clone, Copy, Debug)]
pub enum Mine<'a> {
    /// C0 or C1, not held.
    Absent,
    /// C2, held.
    Held,
    /// C3 or C4, held and leaving. `marks` holds, by peer key id, the number
    /// of the report that carried this attempt's leaving mark to that peer.
    Leaving {
        /// Whether the copy leaves under pressure, which never withdraws.
        pressed: bool,
        /// The attempt's mark on each link that carried it.
        marks: &'a BTreeMap<[u8; 32], u64>,
    },
}

/// Everything one device knows of one file.
#[derive(Clone, Debug)]
pub struct Placement<'a> {
    /// The file.
    pub file: [u8; 32],
    /// The file's size in bytes.
    pub size: u64,
    /// The devices other than the author that should hold it.
    pub copies: usize,
    /// This device, its room read fresh.
    pub me: Candidate<'a>,
    /// This device's custody of the file.
    pub mine: Mine<'a>,
    /// Every linked peer that reported, the authors of the rows naming the
    /// file left out.
    pub peers: Vec<Peer<'a>>,
    /// The key ids of the linked authors of the rows naming the file, a
    /// source a pull can use.
    pub authors: Vec<[u8; 32]>,
}

/// What this device does next for one file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing, and a pull of it running stops.
    Nothing,
    /// Pull it for custody, or keep pulling it.
    Pull,
    /// Mark the held copy leaving, as a copy above the count.
    Surplus,
    /// Withdraw the leaving mark and keep the copy.
    Withdraw,
    /// Leave custody, the count holding without this device.
    Release,
    /// Keep the pressed copy, since no candidate linked can take it over.
    Stranded,
}

/// The first 8 bytes of `BLAKE3(file ‖ key)` read as a big-endian integer.
#[must_use]
pub fn hash_term(file: &[u8; 32], key: &[u8; 32]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(file);
    hasher.update(key);
    let digest = hasher.finalize();
    let mut head = [0; 8];
    head.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_be_bytes(head)
}

/// The weighted rendezvous score of draft-ietf-bess-weighted-hrw-01 section
/// 4, `-room / ln(h / 2^64)`, with `h / 2^64` read from the top 53 bits of
/// `h` so it lies strictly between 0 and 1 in an `f64`.
#[must_use]
pub fn score(file: &[u8; 32], key: &[u8; 32], room: u64) -> f64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "53 bits are exact in an f64, and a room only weighs a score"
    )]
    let (unit, weight) = (
        ((hash_term(file, key) >> 11) as f64 + 0.5) / (1_u64 << 53) as f64,
        room as f64,
    );
    -weight / unit.ln()
}

/// `candidates` best first by [`score`], ties broken by [`hash_term`] and
/// then the key, each account's best device ahead of any second device, a
/// device of an account in `covered`, which already holds a copy, counting
/// as a second device.
#[must_use]
pub fn rank<'a>(
    file: &[u8; 32],
    candidates: &[Candidate<'a>],
    covered: &BTreeSet<&str>,
) -> Vec<Candidate<'a>> {
    let mut scored: Vec<(f64, u64, Candidate<'a>)> = candidates
        .iter()
        .map(|candidate| {
            (
                score(file, &candidate.key, candidate.room),
                hash_term(file, &candidate.key),
                *candidate,
            )
        })
        .collect();
    scored.sort_by(|(a_score, a_hash, a), (b_score, b_hash, b)| {
        b_score
            .partial_cmp(a_score)
            .unwrap_or(Ordering::Equal)
            .then(b_hash.cmp(a_hash))
            .then(a.key.cmp(&b.key))
    });
    let mut accounts = BTreeSet::new();
    let (first, second): (Vec<_>, Vec<_>) = scored
        .into_iter()
        .map(|(_, _, candidate)| candidate)
        .partition(|candidate| {
            !covered.contains(candidate.account) && accounts.insert(candidate.account)
        });
    first.into_iter().chain(second).collect()
}

/// The candidates that could pull the file, best first, after leaving out
/// those whose room is under `size` and those whose best rate to a device in
/// `sources` is under a quarter of the median of the others' same rates. A
/// candidate with no measured rate to any source stays and joins no median.
#[must_use]
pub fn takers<'a>(
    file: &[u8; 32],
    size: u64,
    candidates: &[Candidate<'a>],
    sources: &[[u8; 32]],
    covered: &BTreeSet<&str>,
) -> Vec<Candidate<'a>> {
    let roomy: Vec<Candidate<'a>> = candidates
        .iter()
        .filter(|candidate| candidate.room >= size)
        .copied()
        .collect();
    let best: Vec<Option<u64>> = roomy
        .iter()
        .map(|candidate| {
            sources
                .iter()
                .filter(|source| **source != candidate.key)
                .filter_map(|source| candidate.rates.get(source).copied())
                .max()
        })
        .collect();
    let eligible: Vec<Candidate<'a>> = roomy
        .iter()
        .zip(&best)
        .enumerate()
        .filter(|(index, (_, rate))| {
            let Some(rate) = rate else {
                return true;
            };
            let others: Vec<u64> = best
                .iter()
                .enumerate()
                .filter(|(other, _)| other != index)
                .filter_map(|(_, rate)| *rate)
                .collect();
            median(others).is_none_or(|median| u128::from(*rate) * 4 >= median)
        })
        .map(|(_, (candidate, _))| *candidate)
        .collect();
    rank(file, &eligible, covered)
}

/// The median of `rates`, the mean of the two middle ones for an even count,
/// `None` for none.
fn median(mut rates: Vec<u64>) -> Option<u128> {
    rates.sort_unstable();
    let middle = rates.len() / 2;
    match rates.len() {
        0 => None,
        len if len % 2 == 1 => Some(u128::from(rates[middle])),
        _ => Some(u128::midpoint(
            u128::from(rates[middle - 1]),
            u128::from(rates[middle]),
        )),
    }
}

/// What this device does next for the file `placement` describes.
#[must_use]
pub fn decide(placement: &Placement<'_>) -> Verdict {
    let file = &placement.file;
    let holding = |view: View| placement.peers.iter().filter(move |peer| peer.view == view);
    let mine_held = matches!(placement.mine, Mine::Held);
    let unmarked = holding(View::Held).count() + usize::from(mine_held);
    let sources: Vec<[u8; 32]> = holding(View::Held)
        .chain(holding(View::Leaving))
        .map(|peer| peer.candidate.key)
        .chain(placement.authors.iter().copied())
        .chain((!matches!(placement.mine, Mine::Absent)).then_some(placement.me.key))
        .collect();
    let others: Vec<Candidate<'_>> = placement
        .peers
        .iter()
        .filter(|peer| peer.admitted && peer.view == View::Absent)
        .map(|peer| peer.candidate)
        .collect();
    match placement.mine {
        Mine::Absent => {
            let missing = placement.copies.saturating_sub(unmarked);
            let covered: BTreeSet<&str> = holding(View::Held)
                .map(|peer| peer.candidate.account)
                .collect();
            let mut candidates = others;
            candidates.push(placement.me);
            let ranked = takers(file, placement.size, &candidates, &sources, &covered);
            // With the count met and one account holding two copies, the best
            // taker of an account holding none pulls one more, and the
            // surplus handover then releases the account's second copy.
            let spread = missing == 0
                && unmarked == placement.copies
                && covered.len() < unmarked
                && ranked
                    .first()
                    .is_some_and(|candidate| !covered.contains(candidate.account));
            let pulls = ranked
                .into_iter()
                .take(missing + usize::from(spread))
                .any(|candidate| candidate.key == placement.me.key);
            if pulls {
                Verdict::Pull
            } else {
                Verdict::Nothing
            }
        }
        Mine::Held => {
            if unmarked <= placement.copies {
                return Verdict::Nothing;
            }
            let mut holders: Vec<Candidate<'_>> =
                holding(View::Held).map(|peer| peer.candidate).collect();
            holders.push(placement.me);
            let kept = rank(file, &holders, &BTreeSet::new())
                .into_iter()
                .take(placement.copies)
                .any(|candidate| candidate.key == placement.me.key);
            if kept {
                Verdict::Nothing
            } else {
                Verdict::Surplus
            }
        }
        Mine::Leaving { pressed, marks } => {
            let acknowledged = holding(View::Held)
                .filter(|peer| {
                    marks
                        .get(&peer.candidate.key)
                        .is_some_and(|mark| peer.seen.is_some_and(|seen| seen >= *mark))
                })
                .count();
            if acknowledged >= placement.copies {
                return Verdict::Release;
            }
            if pressed {
                return if takers(file, placement.size, &others, &sources, &BTreeSet::new())
                    .is_empty()
                {
                    Verdict::Stranded
                } else {
                    Verdict::Nothing
                };
            }
            let lacking = placement.copies.saturating_sub(unmarked);
            let mut leavers: Vec<[u8; 32]> = holding(View::Leaving)
                .map(|peer| peer.candidate.key)
                .chain([placement.me.key])
                .collect();
            leavers.sort_by(|a, b| hash_term(file, b).cmp(&hash_term(file, a)).then(a.cmp(b)));
            if leavers
                .iter()
                .take(lacking)
                .any(|key| *key == placement.me.key)
            {
                Verdict::Withdraw
            } else {
                Verdict::Nothing
            }
        }
    }
}

#[cfg(test)]
mod tests;
