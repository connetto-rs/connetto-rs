//! The courier path (R75, R78).
//!
//! A courier presents one author's stored [`SignedOffer`], the manifest rows
//! it holds up to the offer's frontier, and the number of bodies that
//! follow. The rows recompute the author's chain from the writer's head to
//! the offer's frontier head, which binds every entry's capture major and
//! provenance original, so a courier cannot forge, relabel or substitute
//! either. Each body settles as it lands through the write path the direct
//! upload takes, as
//! [`SettlePath::Courier`](crate::writer_schema::SettlePath::Courier), and
//! the range answers one [`CourierOutcome`] after its last counted body. One
//! range's state is driven by the connection's `CourierRange` and
//! `CourierBody` frames or by
//! [`SessionManager::courier_deliver`](crate::session::SessionManager::courier_deliver).

use std::sync::Arc;

use connetto_core::Major;
use connetto_core::SessionId;
use connetto_core::auth::Principal;
use connetto_core::bots::BotList;
use connetto_core::exchange::{
    CourierEntryOutcome, CourierOutcome, CourierRefusal, MAX_MANIFEST_ROWS, ManifestRow,
    SignedOffer,
};
use sha2::{Digest as _, Sha256};

use crate::provisional::AskFacts;

/// The account and session a courier range settles under (R78 decisions 10
/// and 12), the `write_couriered` audit row's `session` and the notice rows'
/// `courier_user_id`. On the wire path the range's connection carries them,
/// and the in-process [`SessionManager::courier_deliver`](crate::session::SessionManager::courier_deliver)
/// takes them from its caller.
#[derive(Clone)]
pub struct CourierIdentity<Id> {
    /// The courier's account.
    pub user_id: Id,
    /// The courier's session.
    pub session: SessionId,
}

/// The body of one manifest row of a courier range, the entry's changeset as
/// the uncompressed patchset bytes the author queued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CourierEntry {
    /// The sequence of the row the body belongs to.
    pub seq: u64,
    /// The entry's changeset, uncompressed patchset bytes.
    pub changeset: Vec<u8>,
}

/// A courier body no conforming client sends, which ends the connection
/// (R78 decision 29).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CourierViolation {
    /// A body arrived while no range was open.
    #[error("a courier body with no range open")]
    NoRange,
    /// A range opened while another was still open.
    #[error("a courier range while another is open")]
    RangeOpen,
    /// The body quotes another range.
    #[error("a courier body of another range")]
    OtherRange,
    /// The body names a sequence the range holds no row for.
    #[error("a courier body naming sequence {0}, no row of the range")]
    NoRow(u64),
    /// The body does not follow the previous one in sequence order.
    #[error("a courier body for sequence {0} out of order")]
    OutOfOrder(u64),
    /// The body's envelope does not decompress.
    #[error("a courier body for sequence {0} does not decompress")]
    Envelope(u64),
    /// The body's changeset disagrees with its row's digest.
    #[error("a courier body for sequence {0} disagrees with its row's digest")]
    Digest(u64),
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
/// run past the offer's frontier, or `bodies` exceeds the rows.
pub(crate) fn well_formed(offer: &[u8], rows: &[ManifestRow], bodies: u32) -> Option<SignedOffer> {
    let signed: SignedOffer = rmp_serde::from_slice(offer).ok()?;
    if rows.len() > MAX_MANIFEST_ROWS
        || usize::try_from(bodies).map_or(true, |bodies| bodies > rows.len())
        || rows.first().is_some_and(|first| first.seq == 0)
        || !rows
            .windows(2)
            .all(|pair| pair[0].seq.checked_add(1) == Some(pair[1].seq))
    {
        return None;
    }
    let frontier = signed.offer.frontier().map(|frontier| frontier.seq);
    if rows
        .last()
        .is_some_and(|last| frontier.is_none_or(|frontier| last.seq > frontier))
    {
        return None;
    }
    Some(signed)
}

/// The identity a range's entries apply under and the writer they settle
/// on, made when the first body applies.
pub(crate) struct Applier<Id, Key> {
    /// The author's writer row.
    pub writer: i64,
    /// The author's identity, its bot list and no subjects.
    pub principal: Arc<Principal<Id, Key>>,
    /// The session the identity carries.
    pub session: uuid::Uuid,
}

/// A range whose checks passed, settling its rows above the frontier as
/// their bodies land.
pub(crate) struct Settling<Id, Key> {
    /// What the checks established about the author.
    pub author: CourierAuthor<Id>,
    /// The account and session the range settles under.
    pub courier: CourierIdentity<Id>,
    /// The major the connection declared, every body's capture major.
    pub major: Option<Major>,
    /// The index of the next row to settle, the first above the frontier
    /// before any settles.
    pub next: usize,
    /// The chain's hash before the next row.
    pub previous: [u8; 32],
    /// The identity and writer, once a body applied.
    pub applier: Option<Applier<Id, Key>>,
    /// Whether the range stopped at a row, its remaining bodies draining.
    stopped: bool,
}

impl<Id, Key> Settling<Id, Key> {
    /// The settling state of a range whose next row to settle is `next`,
    /// chained from `previous`, settling under `courier`.
    pub(crate) fn new(
        author: CourierAuthor<Id>,
        courier: CourierIdentity<Id>,
        major: Option<Major>,
        next: usize,
        previous: [u8; 32],
    ) -> Self {
        Self {
            author,
            courier,
            major,
            next,
            previous,
            applier: None,
            stopped: false,
        }
    }
}

/// Where a range stands.
enum Stage<Id, Key> {
    /// Refused before anything settled, its bodies draining.
    Refused(CourierRefusal),
    /// Settling its rows above the frontier, or stopped at one.
    Settling(Settling<Id, Key>),
}

/// What a body that passed the range and order checks asks of the caller.
pub(crate) enum Admitted<'a, Id, Key> {
    /// The range is refused or stopped, so the body is dropped unread.
    Dropped,
    /// The body belongs to a row of a settling range and must decompress
    /// and match the row's digest before it settles.
    Row(Slot<'a, Id, Key>),
}

/// A settling range's state beside the row an admitted body belongs to.
pub(crate) struct Slot<'a, Id, Key> {
    /// The index of the body's row.
    pub index: usize,
    /// The range's rows.
    pub rows: &'a [ManifestRow],
    /// The range's settling state.
    pub settling: &'a mut Settling<Id, Key>,
    /// The rows answered so far.
    answers: &'a mut Vec<(u64, CourierEntryOutcome)>,
}

impl<Id, Key> Slot<'_, Id, Key> {
    /// Check the body's changeset against its row's digest.
    pub(crate) fn check_digest(&self, changeset: &[u8]) -> Result<(), CourierViolation> {
        let row = &self.rows[self.index];
        if <[u8; 32]>::from(Sha256::digest(changeset)) == row.digest {
            Ok(())
        } else {
            Err(CourierViolation::Digest(row.seq))
        }
    }

    /// Answer the row at `index` and, when `stops`, stop the range.
    pub(crate) fn answer(&mut self, index: usize, outcome: CourierEntryOutcome, stops: bool) {
        self.answers.push((self.rows[index].seq, outcome));
        self.settling.stopped |= stops;
    }

    /// Answer the next unsettled row with `outcome`, when the range has one
    /// left, and stop the range.
    pub(crate) fn stop_at_next(&mut self, outcome: CourierEntryOutcome) {
        if let Some(row) = self.rows.get(self.settling.next) {
            self.answers.push((row.seq, outcome));
        }
        self.settling.stopped = true;
    }
}

/// One courier range open on a connection (R78 decisions 28 to 32).
pub(crate) struct CourierRun<Id, Key> {
    /// The id the bodies and the answer quote.
    request_id: String,
    /// The range's rows, in sequence order.
    rows: Vec<ManifestRow>,
    /// The bodies still to arrive.
    bodies_left: u32,
    /// The sequence of the previous body.
    last_body: Option<u64>,
    /// The rows answered so far, in sequence order.
    answers: Vec<(u64, CourierEntryOutcome)>,
    /// Where the range stands.
    stage: Stage<Id, Key>,
}

impl<Id, Key> CourierRun<Id, Key> {
    /// A range refused before anything settled, draining its `bodies`.
    pub(crate) fn refused(
        request_id: String,
        rows: Vec<ManifestRow>,
        bodies: u32,
        refusal: CourierRefusal,
    ) -> Self {
        Self {
            request_id,
            rows,
            bodies_left: bodies,
            last_body: None,
            answers: Vec::new(),
            stage: Stage::Refused(refusal),
        }
    }

    /// A range whose checks passed, its rows at or below the frontier already
    /// answered in `answers`.
    pub(crate) fn settling(
        request_id: String,
        rows: Vec<ManifestRow>,
        bodies: u32,
        answers: Vec<(u64, CourierEntryOutcome)>,
        settling: Settling<Id, Key>,
    ) -> Self {
        Self {
            request_id,
            rows,
            bodies_left: bodies,
            last_body: None,
            answers,
            stage: Stage::Settling(settling),
        }
    }

    /// The refusal of a range refused before anything settled.
    pub(crate) fn refusal(&self) -> Option<CourierRefusal> {
        match self.stage {
            Stage::Refused(refusal) => Some(refusal),
            Stage::Settling(_) => None,
        }
    }

    /// Whether every counted body has arrived, so the range answers.
    pub(crate) fn done(&self) -> bool {
        self.bodies_left == 0
    }

    /// Check a body's range and order, counting it, and tell the caller
    /// whether it is read.
    pub(crate) fn admit(
        &mut self,
        request_id: &str,
        seq: u64,
    ) -> Result<Admitted<'_, Id, Key>, CourierViolation> {
        if request_id != self.request_id {
            return Err(CourierViolation::OtherRange);
        }
        let Some(index) = self
            .rows
            .first()
            .and_then(|first| seq.checked_sub(first.seq))
            .and_then(|offset| usize::try_from(offset).ok())
            .filter(|index| *index < self.rows.len())
        else {
            return Err(CourierViolation::NoRow(seq));
        };
        if self.last_body.is_some_and(|last| seq <= last) {
            return Err(CourierViolation::OutOfOrder(seq));
        }
        self.last_body = Some(seq);
        self.bodies_left = self.bodies_left.saturating_sub(1);
        Ok(match &mut self.stage {
            Stage::Settling(settling) if !settling.stopped => Admitted::Row(Slot {
                index,
                rows: &self.rows,
                settling,
                answers: &mut self.answers,
            }),
            Stage::Settling(_) | Stage::Refused(_) => Admitted::Dropped,
        })
    }

    /// The range's one answer, the next row above the frontier left without
    /// a body answered `NotCouriered` unless the range stopped.
    pub(crate) fn finish(mut self) -> (String, CourierOutcome) {
        let outcome = match self.stage {
            Stage::Refused(refusal) => CourierOutcome::Refused(refusal),
            Stage::Settling(Settling { next, stopped, .. }) => {
                if let Some(row) = self.rows.get(next).filter(|_| !stopped) {
                    self.answers
                        .push((row.seq, CourierEntryOutcome::NotCouriered));
                }
                CourierOutcome::Entries(self.answers)
            }
        };
        (self.request_id, outcome)
    }
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

    fn offer(settled: Option<u64>, newest: Option<u64>) -> Vec<u8> {
        rmp_serde::to_vec_named(&SignedOffer {
            offer: Offer {
                builds: Vec::new(),
                settled: settled.map(|seq| Frontier { seq, head: [0; 32] }),
                newest: newest.map(|seq| Frontier { seq, head: [0; 32] }),
                retractions: Vec::new(),
            },
            signature: Vec::new(),
        })
        .expect("an offer encodes")
    }

    /// Rows in sequence up to the offer's frontier hold together with any
    /// body count up to the rows, and an empty range with no bodies does
    /// too.
    #[test]
    fn a_contiguous_range_counting_no_more_bodies_than_rows_is_well_formed() {
        let rows = [row(4, b"four"), row(5, b"five"), row(6, b"six")];
        for bodies in 0..=3 {
            assert!(
                well_formed(&offer(None, Some(6)), &rows, bodies).is_some(),
                "{bodies}"
            );
        }
        // The settled frontier is the bound when the offer signs no newest,
        // and when it runs past the newest.
        assert!(well_formed(&offer(Some(6), None), &rows, 0).is_some());
        assert!(well_formed(&offer(Some(6), Some(4)), &rows, 0).is_some());
        assert!(well_formed(&offer(None, None), &[], 0).is_some());
    }

    /// Every way a range fails to hold together is malformed.
    #[test]
    fn a_range_that_does_not_hold_together_is_malformed() {
        let rows = vec![row(4, b"four"), row(5, b"five")];
        let cases: [(&str, Vec<u8>, Vec<ManifestRow>, u32); 8] = [
            ("garbage offer", b"garbage".to_vec(), rows.clone(), 0),
            (
                "a zeroth sequence",
                offer(None, Some(1)),
                vec![row(0, b"zero"), row(1, b"one")],
                0,
            ),
            (
                "a skipped sequence",
                offer(None, Some(6)),
                vec![row(4, b"four"), row(6, b"six")],
                0,
            ),
            (
                "a repeated sequence",
                offer(None, Some(5)),
                vec![row(4, b"four"), row(4, b"four")],
                0,
            ),
            (
                "rows past the newest",
                offer(None, Some(4)),
                rows.clone(),
                0,
            ),
            // With no newest, the settled frontier is the bound.
            (
                "rows past the settled",
                offer(Some(4), None),
                rows.clone(),
                0,
            ),
            // With no frontier at all, every row runs past both.
            ("rows with no frontier", offer(None, None), rows.clone(), 0),
            ("more bodies than rows", offer(None, Some(5)), rows, 3),
        ];
        for (what, offer, rows, bodies) in cases {
            assert!(well_formed(&offer, &rows, bodies).is_none(), "{what}");
        }
    }

    /// More rows than one manifest frame carries are malformed.
    #[test]
    fn a_range_past_the_manifest_bound_is_malformed() {
        let rows: Vec<ManifestRow> = (1..=u64::try_from(MAX_MANIFEST_ROWS).expect("fits") + 1)
            .map(|seq| row(seq, b"x"))
            .collect();
        let newest = rows.last().map(|row| row.seq);
        assert!(well_formed(&offer(None, newest), &rows, 0).is_none());
        assert!(well_formed(&offer(None, newest), &rows[1..], 0).is_some());
    }

    fn refused(bodies: u32) -> CourierRun<String, String> {
        CourierRun::refused(
            "c1".to_owned(),
            vec![row(4, b"four"), row(5, b"five"), row(6, b"six")],
            bodies,
            CourierRefusal::AuthorBanned,
        )
    }

    /// A body is admitted only for the open range, for one of its rows and
    /// after the previous body, and each admitted body counts toward the
    /// declared total.
    #[test]
    fn a_body_must_name_the_range_a_row_and_follow_the_previous_one() {
        let mut run = refused(3);
        assert_eq!(run.admit("c2", 4).err(), Some(CourierViolation::OtherRange));
        assert_eq!(run.admit("c1", 3).err(), Some(CourierViolation::NoRow(3)));
        assert_eq!(run.admit("c1", 7).err(), Some(CourierViolation::NoRow(7)));
        assert!(matches!(run.admit("c1", 5), Ok(Admitted::Dropped)));
        assert_eq!(
            run.admit("c1", 5).err(),
            Some(CourierViolation::OutOfOrder(5))
        );
        assert_eq!(
            run.admit("c1", 4).err(),
            Some(CourierViolation::OutOfOrder(4))
        );
        assert!(!run.done(), "one of three bodies counted");
        assert!(matches!(run.admit("c1", 6), Ok(Admitted::Dropped)));
        assert!(!run.done(), "two of three bodies counted");
    }

    /// A refused range answers its refusal once its bodies have drained,
    /// and a range counting no bodies is done at once.
    #[test]
    fn a_refused_range_answers_after_its_last_body() {
        let mut run = refused(1);
        assert!(!run.done());
        assert!(matches!(run.admit("c1", 6), Ok(Admitted::Dropped)));
        assert!(run.done());
        assert_eq!(
            run.finish(),
            (
                "c1".to_owned(),
                CourierOutcome::Refused(CourierRefusal::AuthorBanned)
            )
        );
        assert!(refused(0).done());
    }

    fn settling(bodies: u32) -> CourierRun<String, String> {
        settling_from(bodies, 0)
    }

    /// A settling range over rows 4 to 6 whose next row to settle is the one
    /// at `next`.
    fn settling_from(bodies: u32, next: usize) -> CourierRun<String, String> {
        CourierRun::settling(
            "c1".to_owned(),
            vec![row(4, b"four"), row(5, b"five"), row(6, b"six")],
            bodies,
            Vec::new(),
            Settling::new(
                CourierAuthor {
                    user: "alice".to_owned(),
                    bot_list: None,
                    key: Vec::new(),
                    writer: None,
                    frontier: 3,
                    head: [0; 32],
                },
                CourierIdentity {
                    user_id: "bob".to_owned(),
                    session: SessionId::from_uuid(uuid::Uuid::new_v4()),
                },
                None,
                next,
                [0; 32],
            ),
        )
    }

    /// A stop on a body of a range whose every row is already answered adds
    /// no row, and the range answers no `NotCouriered` past it.
    #[test]
    fn a_stop_with_no_row_left_answers_nothing_more() {
        let mut run = settling_from(1, 3);
        let Ok(Admitted::Row(mut slot)) = run.admit("c1", 6) else {
            panic!("a settling range reads its body");
        };
        slot.stop_at_next(CourierEntryOutcome::Retry { retry_after_ms: 9 });
        assert!(run.done());
        assert_eq!(run.finish().1, CourierOutcome::Entries(Vec::new()));
    }

    /// A settling range reads its bodies, checking each against its own
    /// row's digest, until a stop, after which it drops them and answers no
    /// row it did not reach.
    #[test]
    fn a_settling_range_reads_its_bodies_until_it_stops() {
        let mut run = settling(3);
        let Ok(Admitted::Row(mut slot)) = run.admit("c1", 4) else {
            panic!("a settling range reads its body");
        };
        assert_eq!(slot.index, 0);
        assert_eq!(slot.check_digest(b"four"), Ok(()));
        assert_eq!(slot.check_digest(b"five"), Err(CourierViolation::Digest(4)));
        slot.answer(0, CourierEntryOutcome::Retry { retry_after_ms: 0 }, true);
        assert!(matches!(run.admit("c1", 5), Ok(Admitted::Dropped)));
        assert!(matches!(run.admit("c1", 6), Ok(Admitted::Dropped)));
        assert!(run.done());
        assert_eq!(
            run.finish().1,
            CourierOutcome::Entries(vec![(4, CourierEntryOutcome::Retry { retry_after_ms: 0 })]),
            "a stopped range answers no row past the stop"
        );
    }

    /// A range whose last body leaves rows unreached answers the next one
    /// `NotCouriered`.
    #[test]
    fn a_range_short_of_bodies_answers_its_next_row_not_couriered() {
        let run = settling(0);
        assert_eq!(
            run.finish().1,
            CourierOutcome::Entries(vec![(4, CourierEntryOutcome::NotCouriered)])
        );
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
