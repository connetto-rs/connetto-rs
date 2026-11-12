//! Tab correlation for connection-owned audit observations.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use connetto_client::{ConnettoConnection, CourierAuditError};
use connetto_core::Transport;
use connetto_core::messages::{
    ControlMessage, CourierAuditAnswer, CourierAuditOutcome, CourierAuditRequest,
};
use futures_util::future::{AbortHandle, Abortable, LocalBoxFuture};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::Notify;

use super::{TabId, TabOut, TabState};

struct Observation {
    author: [u8; 32],
    seqs: Vec<u64>,
    generation: u64,
    abort: AbortHandle,
}

impl Drop for Observation {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(super) struct Completion {
    tab: TabId,
    request_id: String,
    generation: u64,
    outcome: Option<Result<(), CourierAuditError>>,
}

#[derive(Default)]
pub(super) struct AuditRequests {
    tabs: HashMap<TabId, HashMap<String, Observation>>,
    pending: FuturesUnordered<LocalBoxFuture<'static, Completion>>,
    generation: u64,
    pub(super) wake: Arc<Notify>,
}

impl AuditRequests {
    pub(super) fn request<T>(
        &mut self,
        worker: &mut ConnettoConnection<T>,
        tab: TabId,
        out: &tokio::sync::mpsc::UnboundedSender<TabOut>,
        request: CourierAuditRequest,
        deadline: Duration,
    ) where
        T: Transport,
        T::Error: core::fmt::Display,
    {
        if !request.is_well_formed() {
            self.cancel(tab, &request.request_id);
            answer(out, request.request_id, CourierAuditOutcome::Refused);
            return;
        }
        if let Some(existing) = self
            .tabs
            .get(&tab)
            .and_then(|rows| rows.get(&request.request_id))
        {
            if existing.author == request.author && existing.seqs == request.seqs {
                return;
            }
            self.cancel(tab, &request.request_id);
            answer(out, request.request_id, CourierAuditOutcome::Refused);
            return;
        }
        let Some(generation) = self.generation.checked_add(1) else {
            answer(out, request.request_id, CourierAuditOutcome::Refused);
            return;
        };
        let wait = match worker.begin_courier_audit(
            &request.author,
            &request.seqs,
            deadline,
            Arc::clone(&self.wake),
        ) {
            Ok(wait) => wait,
            Err(error) => {
                answer(out, request.request_id, outcome(&Err(error)));
                return;
            }
        };
        self.generation = generation;
        let (abort, registration) = AbortHandle::new_pair();
        let request_id = request.request_id.clone();
        self.pending.push(
            async move {
                Completion {
                    tab,
                    request_id,
                    generation,
                    outcome: Abortable::new(wait.wait(), registration).await.ok(),
                }
            }
            .boxed_local(),
        );
        self.tabs.entry(tab).or_default().insert(
            request.request_id,
            Observation {
                author: request.author,
                seqs: request.seqs,
                generation,
                abort,
            },
        );
    }

    pub(super) fn cancel(&mut self, tab: TabId, request_id: &str) {
        if let Some(rows) = self.tabs.get_mut(&tab) {
            rows.remove(request_id);
            if rows.is_empty() {
                self.tabs.remove(&tab);
            }
        }
    }

    pub(super) fn remove_tab(&mut self, tab: TabId) {
        self.tabs.remove(&tab);
    }

    pub(super) fn end(&mut self, tabs: &HashMap<TabId, TabState>) {
        for (id, observations) in self.tabs.drain() {
            if let Some(tab) = tabs.get(&id) {
                for (request_id, _) in observations {
                    answer(&tab.out, request_id, CourierAuditOutcome::Failed);
                }
            }
        }
        self.pending.clear();
    }

    pub(super) async fn next(&mut self) -> Completion {
        match self.pending.next().await {
            Some(completion) => completion,
            None => core::future::pending().await,
        }
    }

    pub(super) fn reap(&mut self, tabs: &HashMap<TabId, TabState>) {
        while let Some(Some(completion)) = self.pending.next().now_or_never() {
            self.complete(tabs, completion);
        }
    }

    pub(super) fn complete(&mut self, tabs: &HashMap<TabId, TabState>, completion: Completion) {
        let Some(rows) = self.tabs.get_mut(&completion.tab) else {
            return;
        };
        if rows
            .get(&completion.request_id)
            .is_none_or(|row| row.generation != completion.generation)
        {
            return;
        }
        rows.remove(&completion.request_id);
        if rows.is_empty() {
            self.tabs.remove(&completion.tab);
        }
        if let Some(result) = completion.outcome
            && let Some(tab) = tabs.get(&completion.tab)
        {
            answer(&tab.out, completion.request_id, outcome(&result));
        }
    }
}

fn outcome(result: &Result<(), CourierAuditError>) -> CourierAuditOutcome {
    match result {
        Ok(()) => CourierAuditOutcome::Confirmed,
        Err(CourierAuditError::Expired) => CourierAuditOutcome::Expired,
        Err(
            CourierAuditError::InvalidInput
            | CourierAuditError::Capacity
            | CourierAuditError::Refused,
        ) => CourierAuditOutcome::Refused,
        Err(
            CourierAuditError::Failed
            | CourierAuditError::SessionEnded
            | CourierAuditError::Client(_),
        ) => CourierAuditOutcome::Failed,
    }
}

fn answer(
    out: &tokio::sync::mpsc::UnboundedSender<TabOut>,
    request_id: String,
    outcome: CourierAuditOutcome,
) {
    let _ = out.send(TabOut::Control(ControlMessage::CourierAuditAnswer(
        CourierAuditAnswer {
            request_id,
            outcome,
        },
    )));
}
