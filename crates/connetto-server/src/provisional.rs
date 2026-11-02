//! The ruling of one verified provisional ask (R77 decision 11).

use crate::writer_schema::SettledOutcome;
use connetto_core::exchange::{RetractCause, Verdict};

/// The one-sided facts of an author the ruling consults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AskFacts {
    /// The writer's frontier, 0 when its row is absent.
    pub frontier: u64,
    /// Whether the writer's row retired.
    pub retired: bool,
    /// Whether the author's enrolment is absent, revoked or past its expiry
    /// plus tolerance.
    pub abandoned: bool,
}

/// The ruling of one asked sequence `seq`, from the record the writer's
/// settled rows hold for it, `None` when pruned or never settled.
///
/// A record answers its outcome whatever the frontier, a recordless sequence
/// at or below the frontier settled, and a recordless one above it is
/// abandoned of a writer that can no longer deliver and pending of one that
/// can.
#[must_use]
pub(crate) fn verdict_of(facts: &AskFacts, seq: u64, record: Option<&SettledOutcome>) -> Verdict {
    if let Some(outcome) = record {
        return match outcome {
            SettledOutcome::Applied => Verdict::Applied,
            SettledOutcome::Rejected(reason) => {
                Verdict::Refused(RetractCause::Rejected(reason.clone()))
            }
            SettledOutcome::Conflict { .. } => Verdict::Refused(RetractCause::Conflict),
        };
    }
    if seq <= facts.frontier {
        return Verdict::Settled;
    }
    if facts.retired || facts.abandoned {
        Verdict::Abandoned
    } else {
        Verdict::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connetto_core::messages::MutationRejectReason;

    #[test]
    fn every_row_of_the_verdict_table() {
        let facts = AskFacts {
            frontier: 5,
            retired: false,
            abandoned: false,
        };
        // A record answers its outcome whatever the frontier.
        assert_eq!(
            verdict_of(&facts, 3, Some(&SettledOutcome::Applied)),
            Verdict::Applied
        );
        assert_eq!(
            verdict_of(
                &facts,
                4,
                Some(&SettledOutcome::Rejected(
                    MutationRejectReason::Unauthorized
                ))
            ),
            Verdict::Refused(RetractCause::Rejected(MutationRejectReason::Unauthorized))
        );
        assert_eq!(
            verdict_of(
                &facts,
                5,
                Some(&SettledOutcome::Conflict {
                    table: "notes".into(),
                    version_column: "updated_at".into(),
                    pk_columns: vec!["id".into()],
                    pk_values: vec![]
                })
            ),
            Verdict::Refused(RetractCause::Conflict)
        );
        // A recordless sequence at or below the frontier settled.
        assert_eq!(verdict_of(&facts, 5, None), Verdict::Settled);
        assert_eq!(verdict_of(&facts, 0, None), Verdict::Settled);
        // A recordless one above it is pending of a live author.
        assert_eq!(verdict_of(&facts, 6, None), Verdict::Pending);
    }

    #[test]
    fn a_recordless_sequence_above_the_frontier_is_abandoned_of_a_dead_author() {
        for retired in [false, true] {
            for abandoned in [false, true] {
                if !retired && !abandoned {
                    continue;
                }
                let facts = AskFacts {
                    frontier: 5,
                    retired,
                    abandoned,
                };
                assert_eq!(verdict_of(&facts, 6, None), Verdict::Abandoned);
                // At or below the frontier a recordless sequence still settled.
                assert_eq!(verdict_of(&facts, 5, None), Verdict::Settled);
            }
        }
    }

    #[test]
    fn a_record_at_the_frontier_answers_its_outcome() {
        let facts = AskFacts {
            frontier: 5,
            retired: false,
            abandoned: false,
        };
        assert_eq!(
            verdict_of(&facts, 5, Some(&SettledOutcome::Applied)),
            Verdict::Applied
        );
    }
}
