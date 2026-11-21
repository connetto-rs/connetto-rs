//! The Tauri channel's ordering, lifetime and backpressure on Android.

use std::collections::HashSet;
use std::io::Write as _;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use connetto_client::live_wire::{SubscribeRequest, encode};
use connetto_test_harness::android_phone::Device;
use connetto_test_harness::android_phone::live::{
    LiveTrace as TraceModel, Subscription, Subscriptions, subscription,
};
use connetto_test_harness::demo::orders;
use connetto_test_harness::inspector::PageSession;
use connetto_test_harness::pool_for;
use connetto_test_harness::step;
use diesel::{ExpressionMethods as _, QueryDsl as _};
use diesel_async::RunQueryDsl as _;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::{Instant, timeout_at};

const TRACE_BOUND: Duration = Duration::from_secs(30);
const ROWS: &str = "Array.from(document.querySelectorAll('table')).find(table => table.querySelector('thead')?.textContent === 'idquantity')?.querySelectorAll('tbody tr') || []";
const IDENTIFIERS: &str = "Array.from(rows, row => row.querySelector('code').title)";

/// A bounded logcat stream with its complete native channel evidence.
pub(super) struct LiveTrace {
    _process: Child,
    lines: Lines<BufReader<ChildStdout>>,
    evidence: std::fs::File,
    model: TraceModel,
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
            model: TraceModel::default(),
            marker: 0,
        })
    }

    /// Read the next line within the caller's monotonic deadline.
    async fn next(&mut self, deadline: Instant) -> Result<String> {
        let line = timeout_at(deadline, self.lines.next_line())
            .await
            .context("live trace deadline")??
            .context("logcat ended")?;
        if self.model.take_line(&line)? {
            writeln!(self.evidence, "{line}")?;
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
        Ok(self.model.events.len())
    }

    /// Wait for the named trace condition without polling the device.
    async fn until(
        &mut self,
        after: usize,
        condition: impl Fn(&[Value], &Subscriptions) -> bool,
    ) -> Result<()> {
        let deadline = Instant::now() + TRACE_BOUND;
        while !condition(&self.model.events[after..], &self.model.subscriptions) {
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
        Ok((after, self.model.before_switch()?))
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
        let (clean, failed) = self.model.check_switch(after, &open)?;
        println!(
            "live account switch subscriptions={} clean_with_stop={clean} failed={failed} ended_before_new={} new_first_messages={}",
            open.len(),
            open.len(),
            self.model.events[after..]
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
    let identifiers = rows(device, app, database, evidence).await?;
    refusal(device, app, evidence, &mut trace).await?;
    let row_count = reorder(device, app, database, evidence, &mut trace, identifiers).await?;
    pause(device, app, database, evidence, &mut trace, row_count).await?;
    reload(device, app, database, evidence, &mut trace).await?;
    Ok(trace)
}

/// Check the initial live rows against Postgres.
async fn rows(
    device: &Device,
    app: &mut PageSession,
    database: &str,
    evidence: &Path,
) -> Result<Vec<String>> {
    step("live rows through the channel");
    let identifiers = equal_rows(app, database).await?;
    ensure!(!identifiers.is_empty(), "the live orders pane is empty");
    println!("live channel rows={}", identifiers.len());
    device.screenshot(evidence, "live-channel-rows").await?;
    Ok(identifiers)
}

/// Check an unregistered descriptor is refused.
async fn refusal(
    device: &Device,
    app: &mut PageSession,
    evidence: &Path,
    trace: &mut LiveTrace,
) -> Result<()> {
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
    Ok(())
}

/// Check a reordered row retains its DOM node.
async fn reorder(
    device: &Device,
    app: &mut PageSession,
    database: &str,
    evidence: &Path,
    trace: &mut LiveTrace,
    mut identifiers: Vec<String>,
) -> Result<usize> {
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
    Ok(identifiers.len())
}

/// Check a paused page holds one message per subscription.
async fn pause(
    device: &Device,
    app: &mut PageSession,
    database: &str,
    evidence: &Path,
    trace: &mut LiveTrace,
    row_count: usize,
) -> Result<()> {
    step("a paused page holds one message per subscription");
    trace.mark(device).await?;
    trace
        .until(0, |_, subscriptions| subscriptions.in_flight.is_empty())
        .await?;
    let orders = trace
        .model
        .subscriptions
        .specs
        .iter()
        .find(|(key, name)| {
            name.as_str() == "orders_newest_first" && trace.model.subscriptions.open.contains(*key)
        })
        .map(|(key, _)| key.clone())
        .context("orders subscription trace")?;
    arm_rows(app, &format!("identifiers.length === {}", row_count + 3)).await?;
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
            !trace.model.events[paused..]
                .iter()
                .any(|event| event["message"] == "acknowledgement taken"),
            "Debugger.pause allowed channel acknowledgements"
        );
        let sent = trace.model.events[paused..]
            .iter()
            .filter(|event| event["message"] == "message sent")
            .count();
        let held = trace.model.events[paused..]
            .iter()
            .filter(|event| event["message"] == "change held")
            .count();
        ensure!(sent > 0 && held >= 2, "pause did not exercise held changes");
        println!(
            "live paused subscriptions={} sent={sent} held={held} max_unacknowledged=1",
            trace.model.subscriptions.open.len()
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
    Ok(())
}

/// Check a reload releases every old subscription and the wire watch each
/// held, while the watches the application core keeps for itself stay.
async fn reload(
    device: &Device,
    app: &mut PageSession,
    database: &str,
    evidence: &Path,
    trace: &mut LiveTrace,
) -> Result<()> {
    step("a reload releases every watch");
    let after = trace.mark(device).await?;
    let open = trace.model.subscriptions.open.clone();
    let watches = trace.model.subscriptions.page_watches();
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
    let released: HashSet<_> = trace.model.events[after..]
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
    Ok(())
}

/// Read the order keys newest first.
///
/// # Errors
///
/// Returns an error when Postgres cannot read the orders.
async fn order_ids(pg_url: &str) -> Result<Vec<String>> {
    let pool = pool_for(pg_url).await;
    let mut connection = pool.get().await.context("a Postgres connection")?;
    let identifiers: Vec<uuid::Uuid> = orders::table
        .select(orders::id)
        .order((orders::created_at.desc(), orders::id.desc()))
        .load(&mut connection)
        .await?;
    Ok(identifiers
        .into_iter()
        .map(|identifier| identifier.to_string())
        .collect())
}

/// Move one order to the newest position without changing its key.
///
/// # Errors
///
/// Returns an error when the key is invalid or Postgres cannot update it.
async fn move_order_first(pg_url: &str, identifier: &str) -> Result<()> {
    let pool = pool_for(pg_url).await;
    let mut connection = pool.get().await.context("a Postgres connection")?;
    let changed = diesel::update(orders::table.find(uuid::Uuid::parse_str(identifier)?))
        .set(orders::created_at.eq(diesel::dsl::now))
        .execute(&mut connection)
        .await?;
    anyhow::ensure!(changed == 1, "the moved order must exist");
    Ok(())
}

/// Insert an order for the account owning an existing order.
///
/// # Errors
///
/// Returns an error when no order exists or Postgres refuses the insert.
async fn insert_proof_order(pg_url: &str) -> Result<()> {
    let pool = pool_for(pg_url).await;
    let mut connection = pool.get().await.context("a Postgres connection")?;
    let owner: String = orders::table
        .select(orders::owner_id)
        .first(&mut connection)
        .await?;
    diesel::insert_into(orders::table)
        .values((
            orders::id.eq(uuid::Uuid::new_v4()),
            orders::owner_id.eq(owner),
            orders::quantity.eq(1_i64),
        ))
        .execute(&mut connection)
        .await?;
    Ok(())
}
