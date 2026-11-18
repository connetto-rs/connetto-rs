//! Validation of native live-channel traces.

use std::collections::{HashMap, HashSet};

use anyhow::{Context as _, Result, bail, ensure};
use serde_json::Value;

/// A webview label and its subscription id.
pub type Subscription = (String, u64);

/// The subscriptions observed in the native trace.
#[derive(Default)]
pub struct Subscriptions {
    /// Observed wire watches not yet released.
    pub watches: HashSet<String>,
    /// Subscriptions not yet ended.
    pub open: HashSet<Subscription>,
    /// Each outstanding message sequence.
    pub in_flight: HashMap<Subscription, u64>,
    /// The descriptor name of each subscription.
    pub specs: HashMap<Subscription, String>,
}

impl Subscriptions {
    /// Apply one native event in log order.
    fn take(&mut self, event: &Value) -> Result<()> {
        match event["message"].as_str() {
            Some("watch observed") => {
                self.watches
                    .insert(event["sub_id"].as_str().context("watch id")?.to_owned());
            }
            Some("watch released") => {
                self.watches
                    .remove(event["sub_id"].as_str().context("watch id")?);
            }
            _ => {}
        }
        let Some(subscription) = subscription(event) else {
            return Ok(());
        };
        match event["message"].as_str() {
            Some("subscription starting") => {
                self.specs.insert(
                    subscription,
                    event["spec"].as_str().context("spec name")?.to_owned(),
                );
            }
            Some("message sent") => {
                self.open.insert(subscription.clone());
                if event["kind"] != "failed" {
                    let sequence = event["seq"].as_u64().context("message sequence")?;
                    ensure!(
                        self.in_flight
                            .insert(subscription.clone(), sequence)
                            .is_none(),
                        "more than one unacknowledged message for {subscription:?}"
                    );
                }
            }
            Some("acknowledgement taken") => {
                let sequence = event["seq"].as_u64().context("acknowledgement sequence")?;
                ensure!(
                    self.in_flight.remove(&subscription) == Some(sequence),
                    "acknowledgement has no matching message for {subscription:?}"
                );
            }
            Some("subscription ending") => {
                self.open.remove(&subscription);
                self.in_flight.remove(&subscription);
            }
            _ => {}
        }
        Ok(())
    }
}

/// Read a subscription key from a structured plugin event.
pub fn subscription(event: &Value) -> Option<Subscription> {
    Some((
        event["webview"].as_str()?.to_owned(),
        event["subscription"].as_u64()?,
    ))
}

/// Check the old subscriptions end before the new account starts its feeds.
///
/// # Errors
///
/// Returns an error when an old subscription overlaps or outlives the account transition.
fn check_switch(events: &[Value], open: &HashSet<Subscription>) -> Result<(usize, usize)> {
    let mut stopped = HashSet::new();
    let mut ended = HashSet::new();
    let mut started = HashSet::new();
    let mut delivered = HashSet::new();
    let mut clean = 0;
    let mut failed = 0;
    for event in events {
        let Some(subscription) = subscription(event) else {
            continue;
        };
        if open.contains(&subscription) {
            match event["message"].as_str() {
                Some("stop command received") => {
                    stopped.insert(subscription);
                }
                Some("subscription ending") => {
                    if event["error"]
                        .as_str()
                        .is_some_and(|error| !error.is_empty())
                    {
                        failed += 1;
                    } else {
                        ensure!(
                            stopped.contains(&subscription),
                            "old subscription {subscription:?} ended without a page stop"
                        );
                        clean += 1;
                    }
                    ensure!(
                        ended.insert(subscription),
                        "an old subscription ended twice"
                    );
                }
                Some("message sent") => {
                    ensure!(
                        !ended.contains(&subscription),
                        "old subscription {subscription:?} sent a message after its end"
                    );
                }
                Some("subscription starting") => {
                    bail!("the new account reused old subscription {subscription:?}");
                }
                _ => {}
            }
        } else {
            match event["message"].as_str() {
                Some("subscription starting") => {
                    ensure!(
                        ended == *open,
                        "new subscription {subscription:?} started before every old subscription ended"
                    );
                    ensure!(
                        started.insert(subscription),
                        "a new subscription started twice"
                    );
                }
                Some("message sent") => {
                    ensure!(
                        started.contains(&subscription),
                        "new subscription {subscription:?} sent without starting"
                    );
                    if !delivered.contains(&subscription) {
                        ensure!(
                            event["seq"] == 1 && event["kind"] != "failed",
                            "new subscription {subscription:?} has no successful first message"
                        );
                        delivered.insert(subscription);
                    }
                }
                _ => {}
            }
        }
    }
    ensure!(ended == *open, "old subscriptions remain open");
    ensure!(
        !started.is_empty() && started == delivered,
        "new subscriptions have not delivered their first messages"
    );
    Ok((clean, failed))
}

/// The validated events and current subscription state of a native trace.
#[derive(Default)]
pub struct LiveTrace {
    /// The subscriptions reconstructed from the events.
    pub subscriptions: Subscriptions,
    /// Accepted structured events in log order.
    pub events: Vec<Value>,
}

impl LiveTrace {
    /// Parse and validate one log line, returning whether it belongs to the trace.
    ///
    /// # Errors
    ///
    /// Returns an error when a channel event violates the in-flight invariant.
    pub fn take_line(&mut self, line: &str) -> Result<bool> {
        if let Ok(event) = serde_json::from_str::<Value>(line)
            && matches!(
                event["target"].as_str(),
                Some("tauri_plugin_connetto" | "connetto_tauri_demo::live")
            )
        {
            self.subscriptions.take(&event)?;
            self.events.push(event);
            return Ok(true);
        }
        Ok(false)
    }

    /// Snapshot the old account's open subscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error when the account has no subscriptions.
    pub fn before_switch(&self) -> Result<HashSet<Subscription>> {
        ensure!(
            !self.subscriptions.open.is_empty(),
            "the account has no live subscriptions"
        );
        Ok(self.subscriptions.open.clone())
    }

    /// Validate the account transition after the marked event index.
    ///
    /// # Errors
    ///
    /// Returns an error when the marker is invalid or the account transition violates the contract.
    pub fn check_switch(
        &self,
        after: usize,
        open: &HashSet<Subscription>,
    ) -> Result<(usize, usize)> {
        let events = self.events.get(after..).context("switch trace marker")?;
        check_switch(events, open)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Structured log lines reconstruct watches, messages and account transitions.
    #[test]
    fn reconstructs_native_trace() {
        let mut trace = LiveTrace::default();
        assert!(trace.before_switch().is_err());
        assert!(trace.check_switch(1, &HashSet::new()).is_err());
        assert!(!trace.take_line("logcat marker").unwrap());
        assert!(!trace.take_line(r#"{"target":"other"}"#).unwrap());
        let events = [
            json!({"message":"watch observed","sub_id":"wire-1"}),
            json!({"message":"subscription starting","subscription":1,"webview":"main","spec":"orders"}),
            json!({"message":"message sent","subscription":1,"webview":"main","seq":1,"kind":"keyed"}),
            json!({"message":"change held","subscription":1,"webview":"main"}),
            json!({"message":"acknowledgement taken","subscription":1,"webview":"main","seq":1}),
        ];
        for mut event in events {
            event["target"] = json!("tauri_plugin_connetto");
            assert!(trace.take_line(&event.to_string()).unwrap());
        }
        assert_eq!(trace.subscriptions.specs.values().next().unwrap(), "orders");
        assert!(trace.subscriptions.watches.contains("wire-1"));
        let open = trace.before_switch().unwrap();
        let after = trace.events.len();
        let events = [
            json!({"message":"message sent","subscription":1,"webview":"main","seq":2,"kind":"failed"}),
            json!({"message":"subscription ending","subscription":1,"webview":"main","error":"closed"}),
            json!({"message":"watch released","sub_id":"wire-1"}),
            json!({"message":"subscription starting","subscription":2,"webview":"main","spec":"orders"}),
            json!({"message":"message sent","subscription":2,"webview":"main","seq":1,"kind":"keyed"}),
            json!({"message":"acknowledgement taken","subscription":2,"webview":"main","seq":1}),
            json!({"message":"message sent","subscription":2,"webview":"main","seq":2,"kind":"diff"}),
        ];
        for mut event in events {
            event["target"] = json!("connetto_tauri_demo::live");
            assert!(trace.take_line(&event.to_string()).unwrap());
        }
        assert!(trace.subscriptions.watches.is_empty());
        assert_eq!(trace.check_switch(after, &open).unwrap(), (0, 1));
        assert_eq!(
            trace.subscriptions.in_flight.get(&("main".to_owned(), 2)),
            Some(&2)
        );
    }

    /// Missing fields and unmatched acknowledgements fail validation.
    #[test]
    fn rejects_invalid_channel_events() {
        for event in [
            json!({"message":"watch observed"}),
            json!({"message":"watch released"}),
            json!({"message":"subscription starting","subscription":1,"webview":"main"}),
            json!({"message":"message sent","subscription":1,"webview":"main","kind":"keyed"}),
            json!({"message":"acknowledgement taken","subscription":1,"webview":"main"}),
            json!({"message":"acknowledgement taken","subscription":1,"webview":"main","seq":1}),
        ] {
            assert!(Subscriptions::default().take(&event).is_err());
        }
        let mut trace = LiveTrace::default();
        assert!(
            trace
                .take_line(r#"{"target":"tauri_plugin_connetto","message":"watch observed"}"#)
                .is_err()
        );
        assert_eq!(subscription(&json!({"webview":"main"})), None);
    }

    /// Duplicate, reused and incomplete subscription lifetimes fail validation.
    #[test]
    fn rejects_invalid_switch_lifetimes() {
        let open = HashSet::from([("main".to_owned(), 1)]);
        let end = json!({"message":"subscription ending","subscription":1,"webview":"main","error":"closed"});
        let start = json!({"message":"subscription starting","subscription":2,"webview":"main"});
        let first = json!({"message":"message sent","subscription":2,"webview":"main","seq":1,"kind":"keyed"});
        for events in [
            vec![],
            vec![json!({"message":"subscription starting","subscription":1,"webview":"main"})],
            vec![end.clone(), end.clone()],
            vec![end.clone(), start.clone(), start.clone()],
            vec![end.clone(), first],
            vec![end.clone(), start.clone()],
            vec![
                end,
                start,
                json!({"message":"message sent","subscription":2,"webview":"main","seq":1,"kind":"failed"}),
            ],
        ] {
            assert!(check_switch(&events, &open).is_err());
        }
    }

    /// A clean end requires a preceding page stop for that subscription.
    #[test]
    fn rejects_clean_end_without_stop() {
        let open = HashSet::from([("main".to_owned(), 1)]);
        let events = [
            json!({"message":"subscription ending","subscription":1,"webview":"main"}),
            json!({"message":"subscription starting","subscription":2,"webview":"main"}),
            json!({"message":"message sent","subscription":2,"webview":"main","seq":1,"kind":"keyed"}),
        ];
        assert!(
            check_switch(&events, &open)
                .unwrap_err()
                .to_string()
                .contains("without a page stop")
        );
    }

    /// An ended old subscription cannot send another message.
    #[test]
    fn rejects_message_after_end() {
        let open = HashSet::from([("main".to_owned(), 1)]);
        let events = [
            json!({"message":"subscription ending","subscription":1,"webview":"main","error":"closed"}),
            json!({"message":"subscription starting","subscription":2,"webview":"main"}),
            json!({"message":"message sent","subscription":2,"webview":"main","seq":1,"kind":"keyed"}),
            json!({"message":"message sent","subscription":1,"webview":"main","seq":2,"kind":"diff"}),
        ];
        assert!(
            check_switch(&events, &open)
                .unwrap_err()
                .to_string()
                .contains("after its end")
        );
    }

    /// New subscriptions cannot start while an old subscription remains.
    #[test]
    fn rejects_new_start_before_old_end() {
        let open = HashSet::from([("main".to_owned(), 1)]);
        let events = [
            json!({"message":"subscription starting","subscription":2,"webview":"main"}),
            json!({"message":"subscription ending","subscription":1,"webview":"main","error":"closed"}),
            json!({"message":"message sent","subscription":2,"webview":"main","seq":1,"kind":"keyed"}),
        ];
        assert!(
            check_switch(&events, &open)
                .unwrap_err()
                .to_string()
                .contains("before every old subscription ended")
        );
    }

    /// Page stops and feed failures both release the old account.
    #[test]
    fn accepts_stops_and_errors_before_new_subscriptions() {
        let open = HashSet::from([("main".to_owned(), 1), ("main".to_owned(), 2)]);
        let events = [
            json!({"message":"stop command received","subscription":1,"webview":"main"}),
            json!({"message":"subscription ending","subscription":1,"webview":"main"}),
            json!({"message":"subscription ending","subscription":2,"webview":"main","error":"closed"}),
            json!({"message":"subscription starting","subscription":3,"webview":"main"}),
            json!({"message":"message sent","subscription":3,"webview":"main","seq":1,"kind":"keyed"}),
        ];
        assert_eq!(check_switch(&events, &open).unwrap(), (1, 1));
    }

    /// An acknowledgement frees only its matching in-flight message.
    #[test]
    fn matching_acknowledgement_releases_message() {
        let mut subscriptions = Subscriptions::default();
        subscriptions.take(&json!({"message":"message sent","subscription":1,"webview":"main","seq":1,"kind":"keyed"})).unwrap();
        subscriptions.take(&json!({"message":"acknowledgement taken","subscription":1,"webview":"main","seq":1})).unwrap();
        assert!(subscriptions.in_flight.is_empty());
        assert_eq!(subscriptions.open.len(), 1);
    }

    /// A second message without an acknowledgement fails the proof.
    #[test]
    fn detects_unacknowledged_overlap() {
        let mut subscriptions = Subscriptions::default();
        subscriptions.take(&json!({"message":"message sent","subscription":1,"webview":"main","seq":1,"kind":"keyed"})).unwrap();
        assert!(subscriptions.take(&json!({"message":"message sent","subscription":1,"webview":"main","seq":2,"kind":"diff"})).is_err());
    }
}
