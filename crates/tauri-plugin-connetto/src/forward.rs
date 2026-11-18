//! One subscription's forwarder, which sends its answers to the webview with
//! one message in flight.

use connetto_client::ClientError;
use connetto_client::live_wire::{FeedBody, FeedMessage, KeyedDiff};
use serde_bytes::ByteBuf;
use tokio::sync::mpsc;

use crate::registry::{Answer, Starting};

/// Where a subscription's messages go.
pub(crate) type Outlet = Box<dyn Fn(FeedMessage) + Send + Sync>;

/// The messages one subscription has sent, and the one in flight.
struct Sent {
    outlet: Outlet,
    subscription: u64,
    webview: String,
    seq: u64,
    /// The answer the webview acknowledged last.
    base: Option<Answer>,
    /// The message not yet acknowledged, and the answer it brings the webview
    /// to.
    in_flight: Option<(u64, Answer)>,
}

impl Sent {
    /// Send what brings the webview from its base to `latest`, if anything
    /// does.
    fn send(&mut self, latest: Answer) {
        let body = match (&self.base, &latest) {
            (Some(base), latest) if base == latest => return,
            (Some(Answer::Keyed(base)), Answer::Keyed(rows)) => {
                match KeyedDiff::between(base, rows) {
                    Some(diff) => FeedBody::Diff(diff),
                    None => return,
                }
            }
            (_, Answer::Keyed(rows)) => FeedBody::Keyed(rows.clone()),
            (_, Answer::Whole(bytes)) => FeedBody::Whole(ByteBuf::from(bytes.clone())),
        };
        self.seq += 1;
        self.log_sent(&body);
        (self.outlet)(FeedMessage {
            seq: self.seq,
            body,
        });
        self.in_flight = Some((self.seq, latest));
    }

    /// Send the error that ends the subscription.
    fn fail(&mut self, failure: &ClientError) {
        self.seq += 1;
        let body = FeedBody::Failed(failure.to_string());
        self.log_sent(&body);
        tracing::debug!(target: "tauri_plugin_connetto", subscription = self.subscription, webview = self.webview, error = %failure, "subscription ending");
        (self.outlet)(FeedMessage {
            seq: self.seq,
            body,
        });
    }

    /// Log the kind and sequence of one outgoing message.
    fn log_sent(&self, body: &FeedBody) {
        let kind = match body {
            FeedBody::Whole(_) => "whole",
            FeedBody::Keyed(_) => "keyed",
            FeedBody::Diff(_) => "diff",
            FeedBody::Failed(_) => "failed",
        };
        tracing::debug!(target: "tauri_plugin_connetto", subscription = self.subscription, webview = self.webview, seq = self.seq, kind, "message sent");
    }
}

/// Follow `starting` and send its answers through `outlet`, until the feed
/// fails or `acks` closes, which is how the subscription is stopped.
///
/// A change that lands while a message is in flight waits, and the
/// acknowledgement of that message sends the diff to the latest answer, so a
/// webview that stops reading holds at most one message.
pub(crate) async fn forward(
    starting: Starting,
    outlet: Outlet,
    mut acks: mpsc::UnboundedReceiver<u64>,
    subscription: u64,
    webview: String,
) {
    let mut sent = Sent {
        outlet,
        subscription,
        webview,
        seq: 0,
        base: None,
        in_flight: None,
    };
    let mut starting = starting;
    let mut feed = loop {
        tokio::select! {
            started = &mut starting => match started {
                Ok(feed) => break feed,
                Err(failure) => return sent.fail(&failure),
            },
            ack = acks.recv() => if ack.is_none() {
                tracing::debug!(target: "tauri_plugin_connetto", subscription, webview = sent.webview, "subscription ending");
                return;
            },
        }
    };
    match feed.answer() {
        Ok(answer) => sent.send(answer),
        Err(failure) => return sent.fail(&failure),
    }
    let mut behind = false;
    loop {
        tokio::select! {
            changed = feed.changed() => {
                if let Err(failure) = changed {
                    return sent.fail(&failure);
                }
                if sent.in_flight.is_some() {
                    tracing::debug!(target: "tauri_plugin_connetto", subscription, webview = sent.webview, "change held");
                    behind = true;
                    continue;
                }
                match feed.answer() {
                    Ok(answer) => sent.send(answer),
                    Err(failure) => return sent.fail(&failure),
                }
            }
            ack = acks.recv() => {
                let Some(seq) = ack else {
                    tracing::debug!(target: "tauri_plugin_connetto", subscription, webview = sent.webview, "subscription ending");
                    return;
                };
                if sent.in_flight.as_ref().is_none_or(|(sent_seq, _)| *sent_seq != seq) {
                    continue;
                }
                sent.base = sent.in_flight.take().map(|(_, answer)| answer);
                tracing::debug!(target: "tauri_plugin_connetto", subscription, webview = sent.webview, seq, "acknowledgement taken");
                if core::mem::take(&mut behind) {
                    match feed.answer() {
                        Ok(answer) => sent.send(answer),
                        Err(failure) => return sent.fail(&failure),
                    }
                }
            }
        }
    }
}
