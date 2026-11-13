//! Private observations of committed courier audit receipts.

use std::collections::{BTreeMap, HashMap, btree_map::Entry};
use std::sync::Arc;
use std::time::Duration;

use connetto_core::SessionId;
use connetto_core::auth::{CapabilityKey, Principal};
use connetto_core::messages::{
    BindValue, ControlMessage, CourierAuditAnswer, CourierAuditOutcome, CourierAuditRequest,
};
use connetto_core::traits::ContentTicketSigner;
use subql::SubscriptionId;
use subql::backend::Postgres;
use subql::visibility::VisibilityPolicy;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use super::{
    FailedAuthorization, Outbound, OutboundTx, SessionError, SessionManager, SessionState,
    SnapshotSource,
};
use crate::audit::ConnettoAuditSchema;
use crate::materializer::{ReadConnector, Registration};
use crate::oplog::Oplog;
use crate::reexec::{FailedRead, ReadBudget};
use crate::schema::ConnettoSchema;
use crate::write_target::WriteError;

const MAX_OPEN_SEQUENCES: usize = 65_536;
type Owner = (SessionId, u64);

#[derive(Default)]
struct Connection {
    requests: HashMap<Arc<str>, u64>,
    sequences: usize,
}

struct Filter {
    consumer: u64,
    subscription: SubscriptionId,
}

struct Question<Id> {
    owner: Owner,
    request_id: Arc<str>,
    viewer: Id,
    author: [u8; 32],
    writer: i64,
    sequences: Vec<i64>,
    missing: BTreeMap<i64, Filter>,
    deadline: Instant,
    outbound: OutboundTx,
    timer: JoinHandle<()>,
}

impl<Id> Drop for Question<Id> {
    fn drop(&mut self) {
        self.timer.abort();
    }
}

pub(super) struct AuditState<Id> {
    questions: HashMap<u64, Question<Id>>,
    connections: HashMap<Owner, Connection>,
    consumers: HashMap<u64, u64>,
    subscriptions: HashMap<SubscriptionId, u64>,
}

impl<Id> Default for AuditState<Id> {
    fn default() -> Self {
        Self {
            questions: HashMap::new(),
            connections: HashMap::new(),
            consumers: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }
}

impl<Id> AuditState<Id> {
    fn token(&self, owner: Owner, request_id: &str) -> Option<u64> {
        self.connections
            .get(&owner)?
            .requests
            .get(request_id)
            .copied()
    }

    fn insert(&mut self, token: u64, question: Question<Id>) {
        let connection = self.connections.entry(question.owner).or_default();
        connection.sequences += question.sequences.len();
        connection
            .requests
            .insert(Arc::clone(&question.request_id), token);
        self.questions.insert(token, question);
    }

    fn remove(&mut self, token: u64) -> Option<Question<Id>> {
        let question = self.questions.remove(&token)?;
        if let Some(connection) = self.connections.get_mut(&question.owner) {
            connection.requests.remove(question.request_id.as_ref());
            connection.sequences -= question.sequences.len();
            if connection.requests.is_empty() {
                self.connections.remove(&question.owner);
            }
        }
        for filter in question.missing.values() {
            self.consumers.remove(&filter.consumer);
            self.subscriptions.remove(&filter.subscription);
        }
        Some(question)
    }
}

fn answer(outbound: &OutboundTx, request_id: String, outcome: CourierAuditOutcome) {
    let _ = outbound.send(Outbound::Control(ControlMessage::CourierAuditAnswer(
        CourierAuditAnswer {
            request_id,
            outcome,
        },
    )));
}

impl<Snap, Auth, C, O, Id, Key, D, S> SessionManager<Snap, Auth, D, C, O, Id, Key, S>
where
    Snap: SnapshotSource<Id, Key>,
    Auth: VisibilityPolicy<Watcher = Arc<Principal<Id, Key>>, Backend = Postgres>,
    Auth::Error: core::fmt::Display + FailedAuthorization,
    C: ReadConnector,
    C::Error: FailedRead,
    O: Oplog,
    Id: core::fmt::Display + Clone + Send + Sync + 'static,
    Key: CapabilityKey,
    D: ConnettoSchema<Id = Id>,
    S: ContentTicketSigner + 'static,
    Self: Send + Sync + 'static,
{
    pub(super) async fn handle_courier_audit(
        self: &Arc<Self>,
        request: CourierAuditRequest,
        state: &SessionState<Id, Key>,
    ) {
        if !request.is_well_formed() || state.principal.identity().is_none() {
            self.cancel_courier_audit(&request.request_id, state).await;
            answer(
                &state.outbound,
                request.request_id,
                CourierAuditOutcome::Refused,
            );
            return;
        }
        let Some(deadline) =
            Instant::now().checked_add(Duration::from_millis(request.remaining_ms))
        else {
            self.cancel_courier_audit(&request.request_id, state).await;
            answer(
                &state.outbound,
                request.request_id,
                CourierAuditOutcome::Refused,
            );
            return;
        };
        let owner = match self.courier_audit_owner(state, deadline).await {
            Ok(owner) => owner,
            Err(outcome) => {
                answer(&state.outbound, request.request_id, outcome);
                return;
            }
        };
        let existing = if let Ok(audit) = timeout_at(deadline, self.courier_audit.lock()).await {
            audit.token(owner, &request.request_id).and_then(|token| {
                audit
                    .questions
                    .get(&token)
                    .map(|question| question.deadline)
            })
        } else {
            answer(
                &state.outbound,
                request.request_id,
                CourierAuditOutcome::Expired,
            );
            return;
        };
        let gate_deadline = existing.map_or(deadline, |original| original.min(deadline));
        let Ok(_gate) = timeout_at(gate_deadline, self.dispatch_gate.lock()).await else {
            if existing.is_none() {
                answer(
                    &state.outbound,
                    request.request_id,
                    CourierAuditOutcome::Expired,
                );
            }
            return;
        };
        match self.courier_audit_owner(state, gate_deadline).await {
            Ok(current) if current == owner => {}
            Err(CourierAuditOutcome::Expired) if existing.is_some() => return,
            Err(outcome) => {
                answer(&state.outbound, request.request_id, outcome);
                return;
            }
            Ok(_) => {
                answer(
                    &state.outbound,
                    request.request_id,
                    CourierAuditOutcome::Refused,
                );
                return;
            }
        }
        let mut audit = self.courier_audit.lock().await;
        if let Some(token) = audit.token(owner, &request.request_id) {
            let identical = audit.questions.get(&token).is_some_and(|question| {
                question.author == request.author
                    && question.sequences.len() == request.seqs.len()
                    && question
                        .sequences
                        .iter()
                        .zip(&request.seqs)
                        .all(|(stored, incoming)| u64::try_from(*stored) == Ok(*incoming))
            });
            if identical {
                let _ = self
                    .reread_courier_audit_locked(&mut audit, token, false)
                    .await;
            } else {
                self.close_courier_audit_locked(
                    &mut audit,
                    token,
                    Some(CourierAuditOutcome::Refused),
                )
                .await;
            }
            return;
        }
        self.open_courier_audit_locked(&mut audit, owner, request, state, deadline)
            .await;
    }

    async fn courier_audit_owner(
        &self,
        state: &SessionState<Id, Key>,
        deadline: Instant,
    ) -> Result<Owner, CourierAuditOutcome> {
        let sessions = timeout_at(deadline, self.sessions.lock())
            .await
            .map_err(|_| CourierAuditOutcome::Expired)?;
        let live = sessions
            .get(&state.session_id)
            .filter(|live| live.tx.tx.same_channel(&state.outbound.tx))
            .ok_or(CourierAuditOutcome::Refused)?;
        Ok((state.session_id, live.connection_num))
    }

    async fn open_courier_audit_locked(
        self: &Arc<Self>,
        audit: &mut AuditState<Id>,
        owner: Owner,
        request: CourierAuditRequest,
        state: &SessionState<Id, Key>,
        deadline: Instant,
    ) {
        let outstanding = audit
            .connections
            .get(&owner)
            .map_or(0, |connection| connection.sequences);
        if outstanding + request.seqs.len() > MAX_OPEN_SEQUENCES {
            answer(
                &state.outbound,
                request.request_id,
                CourierAuditOutcome::Refused,
            );
            return;
        }
        let read_deadline = deadline.min(Instant::now() + self.guard.reexec_budget().timeout);
        let writer =
            match timeout_at(read_deadline, self.target.writer_by_key(&request.author)).await {
                Ok(Ok(Some(writer))) if Instant::now() < deadline => writer.writer,
                _ => {
                    let outcome = if Instant::now() >= deadline {
                        CourierAuditOutcome::Expired
                    } else {
                        CourierAuditOutcome::Refused
                    };
                    answer(&state.outbound, request.request_id, outcome);
                    return;
                }
            };
        let Some(identity) = state.principal.identity() else {
            answer(
                &state.outbound,
                request.request_id,
                CourierAuditOutcome::Refused,
            );
            return;
        };
        let Ok(sequences) = request
            .seqs
            .into_iter()
            .map(i64::try_from)
            .collect::<Result<Vec<_>, _>>()
        else {
            answer(
                &state.outbound,
                request.request_id,
                CourierAuditOutcome::Refused,
            );
            return;
        };
        let token = self.next_consumer_id();
        let weak = Arc::downgrade(self);
        let timer = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            if let Some(manager) = weak.upgrade() {
                manager.expire_courier_audit(token).await;
            }
        });
        audit.insert(
            token,
            Question {
                owner,
                request_id: request.request_id.into(),
                viewer: identity.user_id.clone(),
                author: request.author,
                writer,
                sequences,
                missing: BTreeMap::new(),
                deadline,
                outbound: state.outbound.clone(),
                timer,
            },
        );
        let _ = self.reread_courier_audit_locked(audit, token, true).await;
    }

    async fn reread_courier_audit_locked(
        &self,
        audit: &mut AuditState<Id>,
        token: u64,
        opening: bool,
    ) -> Result<(), SessionError> {
        let Some(question) = audit.questions.get(&token) else {
            return Ok(());
        };
        let deadline = question.deadline;
        let failure = if opening {
            CourierAuditOutcome::Refused
        } else {
            CourierAuditOutcome::Failed
        };
        if Instant::now() >= deadline {
            self.close_courier_audit_locked(audit, token, Some(CourierAuditOutcome::Expired))
                .await;
            return Ok(());
        }
        let read_deadline = deadline.min(Instant::now() + self.guard.reexec_budget().timeout);
        let receipts = timeout_at(
            read_deadline,
            self.target.courier_audit_receipts(
                &question.viewer,
                question.writer,
                &question.sequences,
            ),
        )
        .await;
        if Instant::now() >= deadline {
            self.close_courier_audit_locked(audit, token, Some(CourierAuditOutcome::Expired))
                .await;
            return Ok(());
        }
        let mut receipts = match receipts {
            Ok(Ok(receipts)) => receipts,
            Ok(Err(WriteError::Transient(detail))) if !opening => {
                return Err(SessionError::ReadUnavailable(detail));
            }
            _ => {
                self.close_courier_audit_locked(audit, token, Some(failure))
                    .await;
                return Ok(());
            }
        };
        receipts.sort_unstable();
        if question
            .sequences
            .iter()
            .all(|sequence| receipts.binary_search(sequence).is_ok())
        {
            self.close_courier_audit_locked(audit, token, Some(CourierAuditOutcome::Confirmed))
                .await;
            return Ok(());
        }
        if let Err(outcome) = self
            .sync_courier_audit_filters(audit, token, &receipts, failure)
            .await
        {
            self.close_courier_audit_locked(audit, token, Some(outcome))
                .await;
        }
        Ok(())
    }

    async fn sync_courier_audit_filters(
        &self,
        audit: &mut AuditState<Id>,
        token: u64,
        receipts: &[i64],
        failure: CourierAuditOutcome,
    ) -> Result<(), CourierAuditOutcome> {
        let AuditState {
            questions,
            consumers,
            subscriptions,
            ..
        } = audit;
        let Some(question) = questions.get_mut(&token) else {
            return Ok(());
        };
        let mut materializer = timeout_at(question.deadline, self.materializer.lock())
            .await
            .map_err(|_| CourierAuditOutcome::Expired)?;
        question.missing.retain(|sequence, filter| {
            if receipts.binary_search(sequence).is_err() {
                return true;
            }
            materializer.unregister(filter.subscription);
            consumers.remove(&filter.consumer);
            subscriptions.remove(&filter.subscription);
            false
        });
        for sequence in &question.sequences {
            if Instant::now() >= question.deadline {
                return Err(CourierAuditOutcome::Expired);
            }
            if receipts.binary_search(sequence).is_ok() {
                continue;
            }
            let Entry::Vacant(entry) = question.missing.entry(*sequence) else {
                continue;
            };
            let consumer = self.next_consumer_id();
            let budget = ReadBudget::new(
                self.guard
                    .reexec_budget()
                    .timeout
                    .min(question.deadline.saturating_duration_since(Instant::now())),
            );
            let registration = materializer
                .register_translated(
                    consumer,
                    D::Audit::COURIER_RECEIPT_FILTER_SQL,
                    &[
                        BindValue::Integer(question.writer),
                        BindValue::Integer(*sequence),
                    ],
                    None,
                    budget,
                    None,
                )
                .map_err(|_| failure)?;
            let subscription = match registration {
                Registration::Row(subscription) => subscription,
                Registration::Computed(capture) => capture.subscription_id,
            };
            entry.insert(Filter {
                consumer,
                subscription,
            });
            consumers.insert(consumer, token);
            subscriptions.insert(subscription, token);
        }
        if Instant::now() >= question.deadline {
            return Err(CourierAuditOutcome::Expired);
        }
        Ok(())
    }

    async fn close_courier_audit_locked(
        &self,
        audit: &mut AuditState<Id>,
        token: u64,
        outcome: Option<CourierAuditOutcome>,
    ) {
        if !audit.questions.contains_key(&token) {
            return;
        }
        let mut materializer = self.materializer.lock().await;
        let Some(question) = audit.remove(token) else {
            return;
        };
        for filter in question.missing.values() {
            materializer.unregister(filter.subscription);
        }
        if let Some(outcome) = outcome {
            let outcome = if Instant::now() >= question.deadline {
                CourierAuditOutcome::Expired
            } else {
                outcome
            };
            answer(&question.outbound, question.request_id.to_string(), outcome);
        }
    }

    async fn expire_courier_audit(&self, token: u64) {
        let _gate = self.dispatch_gate.lock().await;
        let mut audit = self.courier_audit.lock().await;
        if audit
            .questions
            .get(&token)
            .is_some_and(|question| question.deadline <= Instant::now())
        {
            self.close_courier_audit_locked(&mut audit, token, Some(CourierAuditOutcome::Expired))
                .await;
        }
    }

    pub(super) async fn cancel_courier_audit(
        &self,
        request_id: &str,
        state: &SessionState<Id, Key>,
    ) {
        let _gate = self.dispatch_gate.lock().await;
        let owner = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(&state.session_id)
                .filter(|live| live.tx.tx.same_channel(&state.outbound.tx))
                .map(|live| (state.session_id, live.connection_num))
        };
        let Some(owner) = owner else {
            return;
        };
        let mut audit = self.courier_audit.lock().await;
        if let Some(token) = audit.token(owner, request_id) {
            self.close_courier_audit_locked(&mut audit, token, None)
                .await;
        }
    }

    pub(super) async fn drop_courier_audit(&self, session_id: SessionId, connection_num: u64) {
        let _gate = self.dispatch_gate.lock().await;
        let mut audit = self.courier_audit.lock().await;
        while let Some(token) = audit
            .connections
            .get(&(session_id, connection_num))
            .and_then(|connection| connection.requests.values().next().copied())
        {
            self.close_courier_audit_locked(&mut audit, token, None)
                .await;
        }
    }

    pub(super) async fn wake_courier_audit_locked(
        &self,
        consumer_id: u64,
    ) -> Result<(), SessionError> {
        let mut audit = self.courier_audit.lock().await;
        let Some(token) = audit.consumers.get(&consumer_id).copied() else {
            return Ok(());
        };
        self.reread_courier_audit_locked(&mut audit, token, false)
            .await
    }

    pub(super) async fn fail_courier_audit_locked(&self, subscription_id: SubscriptionId) -> bool {
        let mut audit = self.courier_audit.lock().await;
        let Some(token) = audit.subscriptions.get(&subscription_id).copied() else {
            return false;
        };
        self.close_courier_audit_locked(&mut audit, token, Some(CourierAuditOutcome::Failed))
            .await;
        true
    }
}
