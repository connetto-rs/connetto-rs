//! Connection-owned, privately correlated courier audit observations.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use connetto_core::exchange::MAX_MANIFEST_ROWS;
use connetto_core::messages::{
    ControlMessage, CourierAuditAnswer, CourierAuditCancel, CourierAuditOutcome,
    CourierAuditRequest,
};
use connetto_core::traits::Transport;
use tokio::sync::{Notify, oneshot};

use crate::{ClientError, Clock, ConnettoConnection, SystemClock};

const MAX_RETAINED_SEQUENCES: usize = 65_536;
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

/// Why an observation could not confirm committed courier receipts.
#[derive(Debug, thiserror::Error)]
pub enum CourierAuditError {
    /// The author, sequences or duration are invalid.
    #[error("invalid courier audit observation")]
    InvalidInput,
    /// The observation exceeds the request or connection capacity.
    #[error("courier audit observation capacity exhausted")]
    Capacity,
    /// The original deadline elapsed without confirmation.
    #[error("courier audit observation expired")]
    Expired,
    /// The server refused the observation.
    #[error("courier audit observation refused")]
    Refused,
    /// The server could not complete the observation.
    #[error("courier audit observation failed")]
    Failed,
    /// The owning authenticated session ended.
    #[error("courier audit session ended")]
    SessionEnded,
    /// The client could not register or drive the observation.
    #[error(transparent)]
    Client(#[from] ClientError),
}

pub(crate) struct CourierAuditWait {
    reply: oneshot::Receiver<Result<(), CourierAuditError>>,
    active: Arc<AtomicBool>,
    cancellation_dirty: Arc<AtomicBool>,
    wake: Arc<Notify>,
}

impl CourierAuditWait {
    pub(crate) async fn wait(mut self) -> Result<(), CourierAuditError> {
        let result = (&mut self.reply)
            .await
            .unwrap_or(Err(CourierAuditError::SessionEnded));
        self.active.store(false, Ordering::Release);
        result
    }
}

impl Drop for CourierAuditWait {
    fn drop(&mut self) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.cancellation_dirty.store(true, Ordering::Release);
            self.wake.notify_one();
        }
    }
}

struct Observation {
    author: [u8; 32],
    seqs: Vec<u64>,
    deadline: Duration,
    route: Option<String>,
    active: Arc<AtomicBool>,
    reply: oneshot::Sender<Result<(), CourierAuditError>>,
}

#[derive(Default)]
pub(crate) struct CourierAuditState {
    observations: HashMap<u64, Observation>,
    deadlines: BTreeSet<(Duration, u64)>,
    pending: BTreeSet<u64>,
    routes: HashMap<String, u64>,
    cancellations: VecDeque<String>,
    retained_sequences: usize,
    cancellation_dirty: Arc<AtomicBool>,
    ended: bool,
}

impl CourierAuditState {
    fn remove(&mut self, id: u64, outcome: Result<(), CourierAuditError>, cancel: bool) {
        let Some(observation) = self.observations.remove(&id) else {
            return;
        };
        self.deadlines.remove(&(observation.deadline, id));
        self.pending.remove(&id);
        self.retained_sequences -= observation.seqs.len();
        if let Some(route) = observation.route {
            self.routes.remove(&route);
            if cancel {
                self.cancellations.push_back(route);
            }
        }
        observation.active.store(false, Ordering::Release);
        let _ = observation.reply.send(outcome);
    }

    fn reap(&mut self, now: Duration) {
        if self.cancellation_dirty.swap(false, Ordering::AcqRel) {
            for (id, observation) in self
                .observations
                .extract_if(|_, observation| !observation.active.load(Ordering::Acquire))
            {
                self.deadlines.remove(&(observation.deadline, id));
                self.pending.remove(&id);
                self.retained_sequences -= observation.seqs.len();
                if let Some(route) = observation.route {
                    self.routes.remove(&route);
                    self.cancellations.push_back(route);
                }
            }
        }
        while let Some(&(deadline, id)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            self.remove(id, Err(CourierAuditError::Expired), true);
        }
    }

    pub(crate) fn disconnected(&mut self) {
        self.routes.clear();
        self.cancellations.clear();
        for (&id, observation) in &mut self.observations {
            observation.route = None;
            self.pending.insert(id);
        }
    }
}

impl<T> ConnettoConnection<T>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    pub(crate) fn begin_courier_audit(
        &mut self,
        author: &[u8],
        seqs: &[u64],
        deadline: Duration,
        wake: Arc<Notify>,
    ) -> Result<CourierAuditWait, CourierAuditError> {
        let state = &mut self.courier_audit;
        if state.ended {
            return Err(CourierAuditError::SessionEnded);
        }
        let author = author
            .try_into()
            .map_err(|_| CourierAuditError::InvalidInput)?;
        if seqs.is_empty()
            || seqs
                .iter()
                .any(|seq| *seq == 0 || i64::try_from(*seq).is_err())
        {
            return Err(CourierAuditError::InvalidInput);
        }
        let now = SystemClock.monotonic();
        if deadline <= now {
            return Err(CourierAuditError::Expired);
        }
        if u64::try_from(deadline.saturating_sub(now).as_millis()).is_err() {
            return Err(CourierAuditError::InvalidInput);
        }
        let mut seqs = if seqs.len() <= MAX_MANIFEST_ROWS {
            seqs.to_vec()
        } else {
            let mut unique = HashSet::with_capacity(MAX_MANIFEST_ROWS + 1);
            for &seq in seqs {
                unique.insert(seq);
                if unique.len() > MAX_MANIFEST_ROWS {
                    return Err(CourierAuditError::Capacity);
                }
            }
            unique.into_iter().collect()
        };
        seqs.sort_unstable();
        seqs.dedup();
        state.reap(now);
        // Each queued cancellation reserves the slot released by its observation.
        if state.retained_sequences + state.cancellations.len() + seqs.len()
            > MAX_RETAINED_SEQUENCES
        {
            return Err(CourierAuditError::Capacity);
        }
        let id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        let (reply, receiver) = oneshot::channel();
        let active = Arc::new(AtomicBool::new(true));
        state.retained_sequences += seqs.len();
        state.observations.insert(
            id,
            Observation {
                author,
                seqs,
                deadline,
                route: None,
                active: Arc::clone(&active),
                reply,
            },
        );
        state.deadlines.insert((deadline, id));
        state.pending.insert(id);
        Ok(CourierAuditWait {
            reply: receiver,
            active,
            cancellation_dirty: Arc::clone(&state.cancellation_dirty),
            wake,
        })
    }

    pub(crate) async fn process_courier_audit(&mut self) -> Result<(), ClientError> {
        self.courier_audit.reap(SystemClock.monotonic());
        loop {
            let Some(wire) = self.wire.as_mut() else {
                return Ok(());
            };
            let state = &mut self.courier_audit;
            let message = if let Some(route) = state.cancellations.front() {
                ControlMessage::CourierAuditCancel(CourierAuditCancel {
                    request_id: route.clone(),
                })
            } else if let Some(&id) = state.pending.first() {
                let Some(observation) = state.observations.get_mut(&id) else {
                    state.pending.remove(&id);
                    continue;
                };
                let now = SystemClock.monotonic();
                if !observation.active.load(Ordering::Acquire) || observation.deadline <= now {
                    state.remove(id, Err(CourierAuditError::Expired), true);
                    continue;
                }
                let route = observation.route.get_or_insert_with(|| {
                    let serial = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
                    let route = format!("courier-audit-{serial}");
                    state.routes.insert(route.clone(), id);
                    route
                });
                let remaining = observation.deadline.saturating_sub(now);
                let remaining_ms = u64::try_from(remaining.as_millis())
                    .unwrap_or(u64::MAX)
                    .max(1);
                ControlMessage::CourierAuditRequest(CourierAuditRequest {
                    request_id: route.clone(),
                    author: observation.author,
                    seqs: observation.seqs.clone(),
                    remaining_ms,
                })
            } else {
                return Ok(());
            };
            let cancelling = !state.cancellations.is_empty();
            let sent = wire.transport.send_control(message).await;
            if let Err(error) = sent {
                self.disconnected();
                return Err(ClientError::Transport(error.to_string()));
            }
            if cancelling {
                state.cancellations.pop_front();
            } else {
                state.pending.pop_first();
            }
            state.reap(SystemClock.monotonic());
        }
    }

    pub(crate) fn finish_courier_audit(&mut self) {
        let state = &mut self.courier_audit;
        state.ended = true;
        for (_, observation) in state.observations.drain() {
            observation.active.store(false, Ordering::Release);
            let _ = observation.reply.send(Err(CourierAuditError::SessionEnded));
        }
        state.deadlines.clear();
        state.pending.clear();
        state.routes.clear();
        state.cancellations.clear();
        state.retained_sequences = 0;
    }

    pub(crate) fn settle_courier_audit(&mut self, answer: &CourierAuditAnswer) {
        let state = &mut self.courier_audit;
        state.reap(SystemClock.monotonic());
        let Some(&id) = state.routes.get(&answer.request_id) else {
            return;
        };
        let outcome = match answer.outcome {
            CourierAuditOutcome::Confirmed => Ok(()),
            CourierAuditOutcome::Expired => Err(CourierAuditError::Expired),
            CourierAuditOutcome::Refused => Err(CourierAuditError::Refused),
            CourierAuditOutcome::Failed => Err(CourierAuditError::Failed),
        };
        state.remove(id, outcome, false);
    }
}

impl Drop for CourierAuditState {
    fn drop(&mut self) {
        for observation in self.observations.values() {
            observation.active.store(false, Ordering::Release);
        }
    }
}
