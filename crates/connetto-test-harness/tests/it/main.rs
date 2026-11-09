//! Single merged integration-test target. Each module below was a standalone
//! `tests/*.rs` binary until 2026-08-31; merging them collapses 12 link steps
//! into one per build cycle.

mod capability_live;

mod cap_policies;

mod catchup_cost;

mod courier_offer;

mod courier_restore;

mod demo;

mod fanout_counters;

mod fanout_delegated;

mod fanout_load;

mod grant_withdrawal;
mod interleaved_commit;

mod membership_term;

mod outage;

mod parity;

mod peer_exchange;

mod peer_media;

mod provisional_adjudication;

mod read_rule_fold;

mod session_handle;

mod smoke;

mod transitions;

mod truncate;
