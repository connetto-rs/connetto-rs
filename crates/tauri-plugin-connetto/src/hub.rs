//! Every webview's subscriptions, started, acknowledged and ended by the
//! plugin's commands and hooks.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, PoisonError};

use connetto_client::live_wire::SubscribeRequest;
use tokio::sync::mpsc;

use crate::Refusal;
use crate::forward::{Outlet, forward};
use crate::registry::Start;

/// One webview's subscriptions.
#[derive(Default)]
struct Page {
    /// The label of the window the webview sits in.
    window: String,
    /// The running subscriptions, each by the sender of its acknowledgements,
    /// whose drop ends it.
    running: HashMap<u64, mpsc::UnboundedSender<u64>>,
    /// The ids stopped before their subscribe arrived.
    stopped: HashSet<u64>,
}

/// Every webview's subscriptions, by webview label.
pub(crate) struct Hub {
    start: Start,
    pages: Mutex<HashMap<String, Page>>,
}

impl Hub {
    pub(crate) fn new(start: Start) -> Self {
        Self {
            start,
            pages: Mutex::new(HashMap::new()),
        }
    }

    fn pages(&self) -> std::sync::MutexGuard<'_, HashMap<String, Page>> {
        self.pages.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Start `request` for the webview `page` in `window`, its messages going
    /// to `outlet`, unless the webview already stopped it.
    pub(crate) fn subscribe(
        &self,
        page: &str,
        window: &str,
        request: &SubscribeRequest,
        outlet: Outlet,
    ) -> Result<(), Refusal> {
        let mut pages = self.pages();
        let held = pages.entry(page.to_owned()).or_insert_with(|| Page {
            window: window.to_owned(),
            ..Page::default()
        });
        if held.stopped.remove(&request.id) {
            return Ok(());
        }
        let starting = (self.start)(&request.name, &request.descriptor)?;
        let (acks, acknowledged) = mpsc::unbounded_channel();
        held.running.insert(request.id, acks);
        tauri::async_runtime::spawn(forward(starting, outlet, acknowledged));
        Ok(())
    }

    /// End the subscription `id` of the webview `page` in `window`, or
    /// remember it when its subscribe has not arrived.
    pub(crate) fn stop(&self, page: &str, window: &str, id: u64) {
        let mut pages = self.pages();
        let held = pages.entry(page.to_owned()).or_insert_with(|| Page {
            window: window.to_owned(),
            ..Page::default()
        });
        if held.running.remove(&id).is_none() {
            held.stopped.insert(id);
        }
    }

    /// The webview acknowledged message `seq` of its subscription `id`.
    pub(crate) fn ack(&self, page: &str, id: u64, seq: u64) {
        if let Some(acks) = self
            .pages()
            .get(page)
            .and_then(|held| held.running.get(&id))
        {
            let _ = acks.send(seq);
        }
    }

    /// The webview started loading a page, so every subscription of the one
    /// before ends and its remembered ids are forgotten.
    pub(crate) fn page_started(&self, page: &str) {
        self.pages().remove(page);
    }

    /// The window closed, ending every subscription of its webviews.
    pub(crate) fn window_destroyed(&self, window: &str) {
        self.pages().retain(|_, held| held.window != window);
    }
}
