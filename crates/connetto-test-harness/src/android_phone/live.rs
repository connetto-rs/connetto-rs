//! The Tauri channel's ordering, lifetime and backpressure on Android.

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use connetto_client::live_wire::{SubscribeRequest, encode};
use connetto_test_harness::android_phone::Device;
use connetto_test_harness::demo::{insert_proof_order, move_order_first, order_ids};
use connetto_test_harness::inspector::PageSession;
use connetto_test_harness::step;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::{Instant, timeout_at};

const TRACE_BOUND: Duration = Duration::from_secs(30);
const ROWS: &str = "Array.from(document.querySelectorAll('table')).find(table => table.querySelector('thead')?.textContent === 'idquantity')?.querySelectorAll('tbody tr') || []";
const IDENTIFIERS: &str = "Array.from(rows, row => row.querySelector('code').title)";
type Subscription = (String, u64);

/// The subscriptions observed in the native trace.
#[derive(Default)]
struct Subscriptions {
    watches: HashSet<String>,
    open: HashSet<Subscription>,
    in_flight: HashMap<Subscription, u64>,
    specs: HashMap<Subscription, String>,
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
fn subscription(event: &Value) -> Option<Subscription> {
    Some((
        event["webview"].as_str()?.to_owned(),
        event["subscription"].as_u64()?,
    ))
}

/// Check the old subscriptions end before the new account starts its feeds.
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

/// A bounded logcat stream with its complete native channel evidence.
pub(super) struct LiveTrace {
    _process: Child,
    lines: Lines<BufReader<ChildStdout>>,
    evidence: std::fs::File,
    subscriptions: Subscriptions,
    events: Vec<Value>,
    marker: u64,
}

impl LiveTrace {
    /// Attach to the logcat buffer cleared before the app launched.
    pub(super) fn start(device: &Device, evidence: &Path) -> Result<Self> {
        let mut process = Command::new("adb")
            .args([
                "-s",
                &device.serial,
                "logcat",
                "-v",
                "raw",
                "RustStdoutStderr:I",
                "CONNETTO_PROOF:I",
                "*:S",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let output = process.stdout.take().context("logcat stdout")?;
        Ok(Self {
            _process: process,
            lines: BufReader::new(output).lines(),
            evidence: std::fs::File::create(evidence.join("live-channel.jsonl"))?,
            subscriptions: Subscriptions::default(),
            events: Vec::new(),
            marker: 0,
        })
    }

    /// Read the next line within the caller's monotonic deadline.
    async fn next(&mut self, deadline: Instant) -> Result<String> {
        let line = timeout_at(deadline, self.lines.next_line())
            .await
            .context("live trace deadline")??
            .context("logcat ended")?;
        if let Ok(event) = serde_json::from_str::<Value>(&line)
            && matches!(
                event["target"].as_str(),
                Some("tauri_plugin_connetto" | "connetto_tauri_demo::live")
            )
        {
            writeln!(self.evidence, "{line}")?;
            self.subscriptions.take(&event)?;
            self.events.push(event);
        }
        Ok(line)
    }

    /// Mark logcat and consume everything preceding the correlated marker.
    pub(super) async fn mark(&mut self, device: &Device) -> Result<usize> {
        self.marker += 1;
        let marker = format!("connetto-live-proof-{}", self.marker);
        let deadline = Instant::now() + TRACE_BOUND;
        device
            .adb(&["shell", "log", "-t", "CONNETTO_PROOF", &marker])
            .await?;
        while self.next(deadline).await? != marker {}
        Ok(self.events.len())
    }

    /// Wait for the named trace condition without polling the device.
    async fn until(
        &mut self,
        after: usize,
        condition: impl Fn(&[Value], &Subscriptions) -> bool,
    ) -> Result<()> {
        let deadline = Instant::now() + TRACE_BOUND;
        while !condition(&self.events[after..], &self.subscriptions) {
            self.next(deadline).await?;
        }
        Ok(())
    }

    /// Record the subscriptions whose account is about to close.
    pub(super) async fn before_switch(
        &mut self,
        device: &Device,
    ) -> Result<(usize, HashSet<Subscription>)> {
        let after = self.mark(device).await?;
        ensure!(
            !self.subscriptions.open.is_empty(),
            "the account has no live subscriptions"
        );
        Ok((after, self.subscriptions.open.clone()))
    }

    /// Check the account transition through every old end and new first message.
    pub(super) async fn after_switch(
        &mut self,
        device: &Device,
        before: (usize, HashSet<Subscription>),
    ) -> Result<()> {
        let (after, open) = before;
        self.until(after, |events, _| {
            events
                .iter()
                .filter(|event| {
                    event["message"] == "message sent"
                        && event["seq"] == 1
                        && event["kind"] != "failed"
                        && subscription(event).is_some_and(|key| !open.contains(&key))
                })
                .count()
                >= open.len()
        })
        .await?;
        self.mark(device).await?;
        let (clean, failed) = check_switch(&self.events[after..], &open)?;
        println!(
            "live account switch subscriptions={} clean_with_stop={clean} failed={failed} ended_before_new={} new_first_messages={}",
            open.len(),
            open.len(),
            self.events[after..]
                .iter()
                .filter(|event| event["message"] == "message sent"
                    && event["seq"] == 1
                    && event["kind"] != "failed")
                .count()
        );
        Ok(())
    }
}

/// Arm a mutation observer that resolves when the orders satisfy `condition`.
async fn arm_rows(app: &mut PageSession, condition: &str) -> Result<()> {
    app.evaluate(&format!(
        r"(() => {{
        window.__connettoLiveRows = new Promise(resolve => {{
            const observer = new MutationObserver(check);
            function check() {{
                const rows = {ROWS};
                const identifiers = {IDENTIFIERS};
                if ({condition}) {{ observer.disconnect(); resolve(identifiers); }}
            }}
            observer.observe(document, {{subtree:true, childList:true, attributes:true}});
            check();
        }});
        return true;
    }})()"
    ))
    .await?;
    Ok(())
}

/// Await the already armed DOM condition through its correlated CDP request.
async fn finish_rows(app: &mut PageSession) -> Result<Vec<String>> {
    let result = app.call("Runtime.evaluate", json!({"expression":"window.__connettoLiveRows", "awaitPromise":true, "returnByValue":true})).await?;
    ensure!(
        result.get("exceptionDetails").is_none(),
        "DOM observer failed {result}"
    );
    serde_json::from_value(result["result"]["value"].clone()).context("orders DOM identifiers")
}

/// Wait for the DOM order to equal the committed native source snapshot.
pub(super) async fn equal_rows(app: &mut PageSession, database: &str) -> Result<Vec<String>> {
    let expected = order_ids(database).await?;
    let condition = format!(
        "JSON.stringify(identifiers) === {}",
        serde_json::to_string(&serde_json::to_string(&expected)?)?
    );
    arm_rows(app, &condition).await?;
    let actual = finish_rows(app).await?;
    ensure!(actual == expected, "page order differs from Postgres");
    Ok(actual)
}

/// Exercise the live panes over their real native channel.
pub(super) async fn prove(
    device: &Device,
    app: &mut PageSession,
    database: &str,
    evidence: &Path,
) -> Result<LiveTrace> {
    let mut trace = LiveTrace::start(device, evidence)?;
    trace.mark(device).await?;
    step("live rows through the channel");
    let mut identifiers = equal_rows(app, database).await?;
    ensure!(!identifiers.is_empty(), "the live orders pane is empty");
    println!("live channel rows={}", identifiers.len());
    device.screenshot(evidence, "live-channel-rows").await?;

    step("an unregistered descriptor is refused");
    trace.mark(device).await?;
    let channel = app.evaluate("(() => { window.__connettoProofChannel = new window.__TAURI__.core.Channel(); return window.__connettoProofChannel.toJSON(); })()").await?;
    let name = "unregistered_android_proof_spec";
    let request = SubscribeRequest {
        id: u64::from(u32::MAX),
        name: name.to_owned(),
        descriptor: encode(&())?.into(),
        channel: channel.as_str().context("Tauri channel id")?.to_owned(),
    };
    let bytes = serde_json::to_string(&encode(&request)?)?;
    let result = app.call("Runtime.evaluate", json!({"expression":format!("window.__TAURI__.core.invoke('plugin:connetto|subscribe', new Uint8Array({bytes})).then(() => null, error => String(error))"), "awaitPromise":true, "returnByValue":true})).await?;
    let refusal = result["result"]["value"]
        .as_str()
        .context("unregistered subscription was accepted")?;
    ensure!(
        refusal.contains(name) && refusal.contains("registered"),
        "unexpected subscription refusal {refusal}"
    );
    println!("live unregistered refusal={refusal}");
    device
        .screenshot(evidence, "live-unregistered-refused")
        .await?;

    step("a moved row keeps its DOM node");
    if identifiers.len() < 2 {
        arm_rows(app, "identifiers.length >= 2").await?;
        insert_proof_order(database).await?;
        finish_rows(app).await?;
        identifiers = equal_rows(app, database).await?;
    }
    let moved = identifiers.pop().context("oldest order")?;
    identifiers.insert(0, moved.clone());
    let tagged = app.evaluate(&format!("(() => {{ const rows = {ROWS}; const row = Array.from(rows).find(row => row.querySelector('code').title === {}); row.__connettoIdentity = 'oldest-order'; return true; }})()", serde_json::to_string(&moved)?)).await?;
    ensure!(tagged == true, "oldest row was not tagged");
    let after = trace.mark(device).await?;
    arm_rows(
        app,
        &format!("identifiers[0] === {}", serde_json::to_string(&moved)?),
    )
    .await?;
    move_order_first(database, &moved).await?;
    finish_rows(app).await?;
    let kept = app.evaluate(&format!("(() => {{ const rows = {ROWS}; return rows[0].__connettoIdentity === 'oldest-order'; }})()")).await?;
    ensure!(kept == true, "the moved order lost its DOM node");
    ensure!(
        equal_rows(app, database).await? == identifiers,
        "reordering added or removed a row"
    );
    trace
        .until(after, |events, _| {
            events
                .iter()
                .any(|event| event["message"] == "acknowledgement taken")
        })
        .await?;
    println!(
        "live reordered rows={} node_preserved=true",
        identifiers.len()
    );
    device.screenshot(evidence, "live-moved-node").await?;

    step("a paused page holds one message per subscription");
    trace.mark(device).await?;
    trace
        .until(0, |_, subscriptions| subscriptions.in_flight.is_empty())
        .await?;
    let orders = trace
        .subscriptions
        .specs
        .iter()
        .find(|(key, name)| {
            name.as_str() == "orders_newest_first" && trace.subscriptions.open.contains(*key)
        })
        .map(|(key, _)| key.clone())
        .context("orders subscription trace")?;
    arm_rows(
        app,
        &format!("identifiers.length === {}", identifiers.len() + 3),
    )
    .await?;
    app.call("Debugger.enable", json!({})).await?;
    app.call("Debugger.pause", json!({})).await?;
    let paused = trace.mark(device).await?;
    let pause_result = async {
        for _ in 0..3 {
            let after = trace.mark(device).await?;
            insert_proof_order(database).await?;
            trace
                .until(after, |events, _| {
                    events.iter().any(|event| {
                        subscription(event).as_ref() == Some(&orders)
                            && matches!(
                                event["message"].as_str(),
                                Some("message sent" | "change held")
                            )
                    })
                })
                .await?;
        }
        trace.mark(device).await?;
        ensure!(
            !trace.events[paused..]
                .iter()
                .any(|event| event["message"] == "acknowledgement taken"),
            "Debugger.pause allowed channel acknowledgements"
        );
        let sent = trace.events[paused..]
            .iter()
            .filter(|event| event["message"] == "message sent")
            .count();
        let held = trace.events[paused..]
            .iter()
            .filter(|event| event["message"] == "change held")
            .count();
        ensure!(sent > 0 && held >= 2, "pause did not exercise held changes");
        println!(
            "live paused subscriptions={} sent={sent} held={held} max_unacknowledged=1",
            trace.subscriptions.open.len()
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    app.call("Debugger.resume", json!({})).await?;
    app.call("Debugger.disable", json!({})).await?;
    pause_result?;
    finish_rows(app).await?;
    equal_rows(app, database).await?;
    device.screenshot(evidence, "live-resumed-rows").await?;

    step("a reload releases every watch");
    let after = trace.mark(device).await?;
    let open = trace.subscriptions.open.clone();
    let watches = trace.subscriptions.watches.clone();
    ensure!(
        !open.is_empty() && !watches.is_empty(),
        "reload has no subscriptions or wire watches"
    );
    app.call("Page.reload", json!({})).await?;
    trace
        .until(after, |events, _| {
            let ended: HashSet<_> = events
                .iter()
                .filter(|event| event["message"] == "subscription ending")
                .filter_map(subscription)
                .collect();
            let released: HashSet<_> = events
                .iter()
                .filter(|event| event["message"] == "watch released")
                .filter_map(|event| event["sub_id"].as_str())
                .collect();
            ended == open && released == watches.iter().map(String::as_str).collect()
        })
        .await?;
    trace.mark(device).await?;
    let released: HashSet<_> = trace.events[after..]
        .iter()
        .filter(|event| event["message"] == "watch released")
        .filter_map(|event| event["sub_id"].as_str())
        .collect();
    ensure!(
        released == watches.iter().map(String::as_str).collect(),
        "reload released a different wire watch set"
    );
    *app = device.ready_app().await?;
    equal_rows(app, database).await?;
    println!(
        "live reload subscriptions={} ended={} watches={} releases={} watch_ids={released:?}",
        open.len(),
        open.len(),
        watches.len(),
        released.len()
    );
    device.screenshot(evidence, "live-reloaded-rows").await?;
    Ok(trace)
}

#[cfg(test)]
mod tests {
    use super::*;

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
