//! The courier path's input (R75, R78).
//!
//! A courier presents one author's stored
//! [`SignedOffer`], the manifest rows it
//! holds up to the offer's newest frontier, and the bodies of those rows it
//! holds. The rows recompute the author's chain from the writer's head to the
//! signed newest head, which binds every entry's capture major and provenance
//! original, so a courier cannot forge, relabel or substitute either. The
//! range settles through the write path the direct upload takes, as
//! [`SettlePath::Courier`](crate::writer_schema::SettlePath::Courier), under
//! [`SessionManager::courier_deliver`](crate::session::SessionManager::courier_deliver),
//! which answers a [`CourierOutcome`](connetto_core::exchange::CourierOutcome).

use connetto_core::bots::BotList;
use connetto_core::exchange::{MAX_MANIFEST_ROWS, ManifestRow, SignedOffer};
use sha2::{Digest as _, Sha256};

use crate::provisional::AskFacts;

/// The body of one manifest row of a courier range, the entry's changeset as
/// the uncompressed patchset bytes the author queued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CourierEntry {
    /// The sequence of the row the body belongs to.
    pub seq: u64,
    /// The entry's changeset, uncompressed patchset bytes.
    pub changeset: Vec<u8>,
}

/// What a courier range's checks established about its author.
pub(crate) struct CourierAuthor<Id> {
    /// The account the author's key is enrolled under.
    pub user: Id,
    /// The bot list the author's last certificate carries, as stored.
    pub bot_list: Option<String>,
    /// The author's device key id, the writer row's key.
    pub key: Vec<u8>,
    /// The author's writer row, `None` for a key that never bound.
    pub writer: Option<i64>,
    /// The writer's frontier, 0 for a key that never bound.
    pub frontier: u64,
    /// The writer's head, [`GENESIS`](connetto_core::chain::GENESIS) for a
    /// key that never bound.
    pub head: [u8; 32],
}

impl<Id> CourierAuthor<Id> {
    /// The facts a ruling of the author's records consults, for a writer the
    /// checks found live and unretired.
    pub(crate) fn facts(&self) -> AskFacts {
        AskFacts {
            frontier: self.frontier,
            retired: false,
            abandoned: false,
        }
    }
}

/// The author's offer when the range holds together, `None` when it is
/// malformed: the offer does not decode, the rows exceed
/// [`MAX_MANIFEST_ROWS`], start at sequence 0, skip or repeat a sequence or
/// run past the offer's newest, or a body names no row, arrives out of order
/// or disagrees with its row's digest.
pub(crate) fn well_formed(
    offer: &[u8],
    rows: &[ManifestRow],
    bodies: &[CourierEntry],
) -> Option<SignedOffer> {
    let signed: SignedOffer = rmp_serde::from_slice(offer).ok()?;
    if rows.len() > MAX_MANIFEST_ROWS
        || rows.first().is_some_and(|first| first.seq == 0)
        || !rows
            .windows(2)
            .all(|pair| pair[0].seq.checked_add(1) == Some(pair[1].seq))
    {
        return None;
    }
    let newest = signed.offer.newest.as_ref().map(|newest| newest.seq);
    if rows
        .last()
        .is_some_and(|last| newest.is_none_or(|newest| last.seq > newest))
    {
        return None;
    }
    // One pass over the rows, so a body out of order or repeated names no
    // row the pass has left.
    let mut unmatched = rows.iter();
    for body in bodies {
        let row = unmatched.find(|row| row.seq == body.seq)?;
        if <[u8; 32]>::from(Sha256::digest(&body.changeset)) != row.digest {
            return None;
        }
    }
    Some(signed)
}

/// The bot list a couriered entry applies under, from the text the author's
/// enrolment records (R78 decision 9): none for the person wildcard, the
/// parsed list for a bot, and the empty list, which sees and writes nothing,
/// for an enrolment recorded before the column held one or a text that does
/// not parse.
pub(crate) fn author_bot_list(stored: Option<&str>) -> Option<BotList> {
    match stored {
        Some(connetto_core::BOT_LIST_WILDCARD) => None,
        Some(text) => Some(BotList::from_text(text).unwrap_or_else(|| BotList::new([]))),
        None => Some(BotList::new([])),
    }
}

#[cfg(test)]
mod tests {
    use connetto_core::Major;
    use connetto_core::bots::Verb;
    use connetto_core::exchange::{Frontier, Offer};

    use super::*;

    fn row(seq: u64, changeset: &[u8]) -> ManifestRow {
        ManifestRow {
            seq,
            major: Major::new(1).expect("a positive major"),
            origin: None,
            digest: Sha256::digest(changeset).into(),
        }
    }

    fn body(seq: u64, changeset: &[u8]) -> CourierEntry {
        CourierEntry {
            seq,
            changeset: changeset.to_vec(),
        }
    }

    fn offer(newest: Option<u64>) -> Vec<u8> {
        rmp_serde::to_vec_named(&SignedOffer {
            offer: Offer {
                builds: Vec::new(),
                settled: None,
                newest: newest.map(|seq| Frontier { seq, head: [0; 32] }),
                retractions: Vec::new(),
            },
            signature: Vec::new(),
        })
        .expect("an offer encodes")
    }

    /// Rows in sequence up to the offer's newest with bodies in order, some
    /// rows digest-only, hold together, and an empty range does too.
    #[test]
    fn a_contiguous_range_with_matching_bodies_is_well_formed() {
        let rows = [row(4, b"four"), row(5, b"five"), row(6, b"six")];
        let bodies = [body(4, b"four"), body(6, b"six")];
        assert!(well_formed(&offer(Some(6)), &rows, &bodies).is_some());
        assert!(well_formed(&offer(None), &[], &[]).is_some());
    }

    /// One malformed range, named, with its offer, rows and bodies.
    type Case = (&'static str, Vec<u8>, Vec<ManifestRow>, Vec<CourierEntry>);

    /// Every way a range fails to hold together is malformed.
    #[test]
    fn a_range_that_does_not_hold_together_is_malformed() {
        let rows = [row(4, b"four"), row(5, b"five")];
        let cases: [Case; 9] = [
            (
                "garbage offer",
                b"garbage".to_vec(),
                rows.to_vec(),
                Vec::new(),
            ),
            (
                "a zeroth sequence",
                offer(Some(1)),
                vec![row(0, b"zero"), row(1, b"one")],
                Vec::new(),
            ),
            (
                "a skipped sequence",
                offer(Some(6)),
                vec![row(4, b"four"), row(6, b"six")],
                Vec::new(),
            ),
            (
                "a repeated sequence",
                offer(Some(5)),
                vec![row(4, b"four"), row(4, b"four")],
                Vec::new(),
            ),
            (
                "rows past the newest",
                offer(Some(4)),
                rows.to_vec(),
                Vec::new(),
            ),
            (
                "rows with no newest",
                offer(None),
                rows.to_vec(),
                Vec::new(),
            ),
            (
                "a body naming no row",
                offer(Some(5)),
                rows.to_vec(),
                vec![body(7, b"seven")],
            ),
            (
                "bodies out of order",
                offer(Some(5)),
                rows.to_vec(),
                vec![body(5, b"five"), body(4, b"four")],
            ),
            (
                "a body off its digest",
                offer(Some(5)),
                rows.to_vec(),
                vec![body(4, b"other")],
            ),
        ];
        for (what, offer, rows, bodies) in cases {
            assert!(well_formed(&offer, &rows, &bodies).is_none(), "{what}");
        }
    }

    /// More rows than one manifest frame carries are malformed.
    #[test]
    fn a_range_past_the_manifest_bound_is_malformed() {
        let rows: Vec<ManifestRow> = (1..=u64::try_from(MAX_MANIFEST_ROWS).expect("fits") + 1)
            .map(|seq| row(seq, b"x"))
            .collect();
        let newest = rows.last().map(|row| row.seq);
        assert!(well_formed(&offer(newest), &rows, &[]).is_none());
        assert!(well_formed(&offer(newest), &rows[1..], &[]).is_some());
    }

    /// The person wildcard binds no list, a bot's text its list, and a
    /// missing or unreadable text the empty list.
    #[test]
    fn the_stored_text_maps_to_the_bound_list() {
        assert_eq!(
            author_bot_list(Some(connetto_core::BOT_LIST_WILDCARD)),
            None
        );
        assert_eq!(
            author_bot_list(Some("orders:read")),
            Some(BotList::new([("orders".to_owned(), Verb::Read)]))
        );
        assert_eq!(author_bot_list(None), Some(BotList::new([])));
        assert_eq!(author_bot_list(Some("orders:drop")), Some(BotList::new([])));
    }
}
