//! The core the demos share. It holds the view and the actions, the wording
//! and helpers every pane renders, and behind the `client` feature the demo
//! schema, its rows and queries, and the browser pieces the demos carry
//! identically.

use std::collections::HashMap;

#[cfg(feature = "client")]
mod data;
#[cfg(feature = "client")]
pub use data::*;

pub mod view;

#[cfg(all(target_arch = "wasm32", feature = "web"))]
pub mod web;

/// The part of an id before its first `-`, or the whole string when it has none.
#[must_use]
pub fn short_id(id: &str) -> &str {
    id.split('-').next().unwrap_or(id)
}

/// First eight hex characters of a byte slice, for display.
#[must_use]
pub fn short_hex(bytes: &[u8]) -> String {
    bytes.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

/// `quantity: count` pairs in quantity order, or `none` for an empty snapshot.
#[must_use]
pub fn grouped_label(counts: &HashMap<i64, i64>) -> String {
    let mut sorted: Vec<(i64, i64)> = counts.iter().map(|(q, c)| (*q, *c)).collect();
    sorted.sort_unstable();
    if sorted.is_empty() {
        return "none".to_owned();
    }
    sorted
        .iter()
        .map(|(quantity, count)| format!("{quantity}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The one stylesheet the demo panes render.
pub const CSS: &str = r#"
:root { --accent: #2f6feb; --accent-hover: #2559c7; --danger: #b42318; --border: #d0d7de; --muted: #57606a; --surface: #f6f8fa; }
body { font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; margin: 0; color: #1f2328; background: #fff; }
.wrap { padding: 16px; max-width: 960px; margin: 0 auto; }
h1 { font-size: 1.4em; margin: 0 0 8px; }
h1, h2 { display: flex; align-items: center; gap: 8px; }
.panes { display: flex; gap: 16px; flex-wrap: wrap; }
.pane { flex: 1 1 300px; border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; background: #fff; }
.pane h2 { display: flex; align-items: center; gap: 8px; margin: 0 0 8px; font-size: 1.05em; }
.badge { font-size: 0.7em; padding: 2px 8px; border-radius: 999px; font-weight: 600; }
.synced { background: #ddf4ff; color: #0a3069; }
.local { background: #ffebe9; color: #82071e; }
.trim { background: #dafbe1; color: #116329; }
.status { color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.9em; }
.id { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.9em; background: var(--surface); border-radius: 4px; padding: 0 4px; }
table { border-collapse: collapse; width: 100%; margin-top: 8px; }
th, td { border-bottom: 1px solid var(--border); padding: 6px 8px; text-align: left; }
th { font-size: 0.85em; color: var(--muted); font-weight: 600; }
input { font: inherit; padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; }
input:focus { outline: 2px solid var(--accent); outline-offset: -1px; border-color: var(--accent); }
button { display: inline-flex; align-items: center; gap: 6px; font: inherit; font-size: 0.9em; font-weight: 500; padding: 6px 12px; border: 1px solid var(--border); border-radius: 6px; background: var(--surface); color: #1f2328; cursor: pointer; transition: background-color .12s, border-color .12s; }
button svg { flex: none; fill: currentColor; }
button:hover:not(:disabled) { background: #eaeef2; border-color: #afb8c1; }
button:active:not(:disabled) { background: #dde3e9; }
button:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
button:disabled { opacity: .5; cursor: not-allowed; }
button.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
button.primary:hover:not(:disabled) { background: var(--accent-hover); border-color: var(--accent-hover); }
button.danger { background: #fff; color: var(--danger); border-color: #f5c2bd; }
button.danger:hover:not(:disabled) { background: #fff1f0; border-color: var(--danger); }
input[type=file]::file-selector-button { font: inherit; font-size: 0.9em; padding: 6px 12px; margin-right: 8px; border: 1px solid var(--border); border-radius: 6px; background: var(--surface); cursor: pointer; }
input[type=file] { border: none; padding: 0; }
.row { display: flex; gap: 8px; margin-top: 8px; flex-wrap: wrap; align-items: center; }
.auth-banner { display: flex; flex-direction: column; gap: 8px; border: 1px solid var(--border); border-radius: 10px; padding: 10px 14px; margin-bottom: 12px; background: var(--surface); }
.auth-banner p { margin: 0; }
.confirm { background: #fff8c5; border: 1px solid #d4a72c; border-radius: 8px; padding: 8px 12px; }
.confirm p { margin: 0 0 6px; }
.warn { color: #7d4e00; background: #fff8c5; border: 1px solid #d4a72c; border-radius: 6px; padding: 4px 8px; margin: 4px 0; }
.error { color: var(--danger); }
.peer { border: 1px solid var(--border); border-radius: 10px; padding: 8px 14px; margin-bottom: 12px; }
img.thumb { max-height: 60px; border-radius: 4px; }
"#;

/// The orders pane's title.
pub const ORDERS_PANE_TITLE: &str = "orders";
/// The orders pane's badge.
pub const ORDERS_BADGE: &str = "synced";
/// The photos pane's title.
pub const PHOTOS_PANE_TITLE: &str = "photos";
/// The photos pane's badge.
pub const PHOTOS_BADGE: &str = "synced";
/// The notes pane's title.
pub const NOTES_PANE_TITLE: &str = "notes";
/// The notes pane's badge.
pub const NOTES_BADGE: &str = "device-only";
/// The retention pane's title.
pub const RETENTION_PANE_TITLE: &str = "retention";
/// The retention pane's badge.
pub const RETENTION_BADGE: &str = "trim";
/// The your-data pane's title.
pub const YOUR_DATA_PANE_TITLE: &str = "your data";
/// The your-data pane's badge.
pub const YOUR_DATA_BADGE: &str = "device-only";

/// The orders pane's readout line.
#[must_use]
pub fn orders_readout(row_count: usize, total_quantity: i64) -> String {
    format!(
        "count {row_count}, total quantity {total_quantity}. Newest first. Converges across every window through Postgres."
    )
}

/// The orders pane's count line prefix.
pub const ORDERS_COUNT_PREFIX: &str = "COUNT(*) pushed by the server: ";
/// The orders pane's grouped line prefix.
pub const ORDERS_GROUPED_PREFIX: &str = "COUNT(*) grouped by quantity: ";

/// The orders pane's count value, `pending` while the subscription has not landed.
#[must_use]
pub fn orders_count_value(value: Option<i64>) -> String {
    value.map_or_else(|| "pending".to_owned(), |value| value.to_string())
}

/// The photos pane's readout line.
#[must_use]
pub fn photos_readout(row_count: usize) -> String {
    format!("count {row_count}. Pick an image file to stage and upload through the content route.")
}

/// The notes pane's readout line.
#[must_use]
pub fn notes_readout(row_count: usize) -> String {
    format!(
        "count {row_count}. Converges across this device's windows through the DB worker, never the server."
    )
}

/// The notes pane's input placeholder.
pub const NOTES_PLACEHOLDER: &str = "a note that stays on this device";

/// The retention pane's readout line, the covered rows among them.
#[must_use]
pub fn retention_readout(pages: i64, free: i64, covered_rows: usize) -> String {
    let kb = pages * 4;
    format!(
        "Replica mirror: {pages} pages (~{kb} KB), {free} free to reclaim. Covered rows: {covered_rows}."
    )
}

/// The retention pane's second line.
pub const RETENTION_EXPLAINER: &str = "Ending a subscription evicts the rows no live subscription still covers, and the trimming pass hands the freed pages back to storage.";

/// The your-data pane's intro line.
pub const YOUR_DATA_INTRO: &str = "Save a zip archive of this device's local data. The archive is not encrypted and holds every row the device can read: whoever holds the file holds the data.";
/// The your-data pane's import line.
pub const YOUR_DATA_IMPORT_INTRO: &str =
    "Restore an archive from this account. The file's version wins every clash.";

/// The label a photos row shows for its content state.
#[must_use]
pub fn photo_state(content_state: Option<&str>) -> &str {
    content_state.unwrap_or("pending")
}

/// The query the photos pin names, every photo's content id.
pub const PIN_PHOTOS_QUERY: &str = "SELECT content_id FROM photos";
/// The pin success line.
pub const PIN_OK: &str = "pinned photos";
/// The unpin success line.
pub const UNPIN_OK: &str = "unpinned photos";

/// The pin failure line.
#[must_use]
pub fn pin_failed(err: impl core::fmt::Display) -> String {
    format!("pin failed: {err}")
}

/// The unpin failure line.
#[must_use]
pub fn unpin_failed(err: impl core::fmt::Display) -> String {
    format!("unpin failed: {err}")
}

/// The fetch failure line.
#[must_use]
pub fn fetch_failed(err: impl core::fmt::Display) -> String {
    format!("fetch failed: {err}")
}

/// The fetch success line.
#[must_use]
pub fn fetch_ok(files: usize) -> String {
    format!("fetched {files} pinned file(s)")
}

/// The content tidy failure line.
#[must_use]
pub fn tidy_failed(err: impl core::fmt::Display) -> String {
    format!("content tidy failed: {err}")
}

/// The content tidy success line.
#[must_use]
pub fn tidy_ok(files: usize) -> String {
    format!("content tidy: {files} file(s) evicted")
}

/// The shared standing of the logout controls.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LogoutState {
    /// Showing the two logout buttons.
    #[default]
    Idle,
    /// A delete would lose local writes, so asking for the go ahead.
    ConfirmDelete {
        /// The pending local items a delete would discard.
        unsynced_count: u64,
    },
    /// A logout or unsynced-count request is in flight.
    Working,
    /// The request failed.
    Error(String),
}

/// The delete confirmation line.
#[must_use]
pub fn logout_confirm_line(unsynced_count: u64) -> String {
    format!("You have {unsynced_count} pending local item(s) that would be permanently lost.")
}

/// The second-refusal line, when the worker kept the local data.
#[must_use]
pub fn logout_refused_line(pending_count: u64) -> String {
    format!("Refused: {pending_count} local item(s) still pending.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_id_without_a_dash_keeps_the_whole_string() {
        assert_eq!(short_id("abc"), "abc");
        assert_eq!(short_id("abcd-1234-5678"), "abcd");
    }

    #[test]
    fn grouped_label_orders_by_quantity() {
        let mut counts = HashMap::new();
        counts.insert(15, 2);
        counts.insert(5, 7);
        counts.insert(25, 1);
        assert_eq!(grouped_label(&counts), "5: 7, 15: 2, 25: 1");
        assert_eq!(grouped_label(&HashMap::new()), "none");
    }
}
