//! The core the browser demos share. It holds the demo schema and its row
//! types, the wording and helpers every pane renders, and the browser pieces
//! the demos carry identically.

use std::collections::HashMap;
use std::time::SystemTime;

use connetto_client::teardown::ExpiryWarning;
use connetto_client::{ClientError, ClientEvent, ConnettoClient, SyncSchema};
use connetto_core::messages::{FatalErrorReason, FullResyncReason, SyncStatus};
use connetto_core::traits::{MaybeSend, Transport};
use connetto_file_core::{FileId, MimeClass};
use diesel::prelude::*;
use rosetta_uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/connetto-schema.rs"));

pub use connetto_demo_deployment::{AUTH_BASE, DEMO_WS_URL, auth_landing};

#[cfg(target_arch = "wasm32")]
pub mod web;

/// The one orders subscription every window drives, the visible rows only.
pub const DEMO_QUERY: &str = "SELECT * FROM orders WHERE quantity > 0";
/// The extra upstream subscription for the photos rows.
pub const PHOTO_QUERY: &str = "SELECT * FROM photos";

// The synced tier, as the deployment schema translates it to the replica.
diesel::table! {
    /// The synced order rows.
    orders (id) {
        /// The client-minted key.
        id -> rosetta_uuid::sql_types::Uuid,
        /// Who owns the row, which the policy compares against the caller.
        owner_id -> diesel::sql_types::Text,
        /// The line quantity.
        quantity -> diesel::sql_types::BigInt,
        /// When the order was written, local time.
        created_at -> diesel::sql_types::Timestamp,
    }
}

// The device-private tier, translated from the shared `frontend.sql`.
diesel::table! {
    /// The device-private note rows.
    notes (id) {
        /// The replica-minted rowid.
        id -> diesel::sql_types::BigInt,
        /// The note text.
        body -> diesel::sql_types::Text,
    }
}

diesel::table! {
    /// The synced photo rows.
    photos (id) {
        /// The client-minted key.
        id -> rosetta_uuid::sql_types::Uuid,
        /// The parent order.
        order_id -> rosetta_uuid::sql_types::Uuid,
        /// Who owns the parent order.
        owner_id -> diesel::sql_types::Text,
        /// The BLAKE3 identity the file server stores and serves the bytes under.
        content_id -> diesel::sql_types::Binary,
        /// `NULL` until the file server commit writes `available`.
        content_state -> Nullable<diesel::sql_types::Text>,
    }
}

/// One row of the synced `orders` table.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = orders)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Order {
    /// The row's id.
    pub id: Uuid,
    /// The account the row belongs to.
    pub owner_id: String,
    /// The order's quantity.
    pub quantity: i64,
    /// When the row was created.
    pub created_at: chrono::NaiveDateTime,
}

/// One row of the device-private `notes` table.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = notes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Note {
    /// The row's id.
    pub id: i64,
    /// The note's body.
    pub body: String,
}

/// One row of the synced `photos` table.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = photos)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Photo {
    /// The row's id.
    pub id: Uuid,
    /// The order the row belongs to.
    pub order_id: Uuid,
    /// The account the row belongs to.
    pub owner_id: String,
    /// The content client's identity for the row's bytes.
    pub content_id: Vec<u8>,
    /// The row's content state, absent while the bytes are pending.
    pub content_state: Option<String>,
}

/// The one sync schema the worker, every tab and every device build from.
#[must_use]
pub fn demo_schema() -> SyncSchema {
    SyncSchema::new(connetto_schema_bundle::bundle())
}

/// A short status line word for one client event, or `None` to ignore it.
#[must_use]
pub fn status_label(event: &ClientEvent) -> Option<String> {
    match event {
        ClientEvent::Reconnecting { attempt } => Some(format!("reconnecting (attempt {attempt})")),
        ClientEvent::Reconnected => Some("reconnected".to_owned()),
        ClientEvent::SyncStatus(status) => Some(match status {
            SyncStatus::Connected => "online".to_owned(),
            SyncStatus::Offline => "offline".to_owned(),
        }),
        ClientEvent::NonFatal { related_to, detail } => Some(match related_to {
            Some(id) => format!("non-fatal on {id}: {detail}"),
            None => format!("non-fatal: {detail}"),
        }),
        ClientEvent::FullResync { sub_id, reason } => {
            Some(format!("{sub_id}: full resync, {}", resync_reason(reason)))
        }
        ClientEvent::MutationApplied { client_seq } => {
            Some(format!("mutation {client_seq} applied"))
        }
        ClientEvent::MutationRejected {
            client_seq, reason, ..
        } => Some(format!("mutation {client_seq} rejected: {reason}")),
        ClientEvent::MutationConflict {
            client_seq,
            server_row,
            ..
        } => Some(server_row.as_ref().map_or_else(
            || format!("mutation {client_seq} conflicted, the server row is gone"),
            |row| {
                format!(
                    "mutation {client_seq} conflicted, server holds {}",
                    row.row_json
                )
            },
        )),
        // The reconnect policy uses a fixed backoff and never reads retry_after_ms, so the value is informational.
        ClientEvent::RateLimited { retry_after_ms, .. } => Some(format!(
            "rate limited: server asks {retry_after_ms}ms before retrying \
             (client reconnects on its own fixed schedule)"
        )),
        ClientEvent::ServerClosed {
            reason: FatalErrorReason::RateLimited { retry_after_ms },
        } => Some(format!(
            "connection closed: rate limit exceeded (server suggests {retry_after_ms}ms wait)"
        )),
        ClientEvent::ServerClosed { reason } => {
            Some(format!("server closed the connection: {reason:?}"))
        }
        ClientEvent::Stopped { detail } => Some(format!("the client stopped: {detail}")),
        ClientEvent::Closed => Some("connection closed".to_owned()),
        ClientEvent::AuthenticationRequired => {
            Some("session expired, sign out and sign in again".to_owned())
        }
        _ => None,
    }
}

/// The human-readable form of why a subscription's rows were replaced wholesale.
#[must_use]
pub fn resync_reason(reason: &FullResyncReason) -> String {
    match reason {
        FullResyncReason::CursorOutsideRetention => {
            "the cursor left the retention window".to_owned()
        }
        FullResyncReason::AuthorizationChange => "a grant moved".to_owned(),
        FullResyncReason::TableTruncated { table } => {
            format!("table {table} was emptied")
        }
        FullResyncReason::SnapshotInterrupted => {
            "the snapshot read failed part way through".to_owned()
        }
        FullResyncReason::CursorBeyondHistory => {
            "the cursor is past the retained history".to_owned()
        }
    }
}

/// A device-unique id for a local-only `notes` row, the millisecond clock above 21 random bits.
#[must_use]
#[cfg(target_arch = "wasm32")]
pub fn fresh_note_id() -> i64 {
    let span = f64::from(1u32 << 21);
    let now = js_sys::Date::now();
    debug_assert!(now.is_finite() && (0.0..span * span).contains(&now));
    // Deliberate truncation: whole milliseconds, below 2^42 until 2109.
    let millis = now as i64;
    let r = js_sys::Math::random() * span;
    debug_assert!(r.is_finite() && (0.0..span).contains(&r));
    // Deliberate quantization: [0.0, 2^21) -> [0, 2^21 - 1].
    (millis << 21) | r as i64
}

/// A fresh note id for the device-private tier.
#[must_use]
#[cfg(not(target_arch = "wasm32"))]
pub fn fresh_note_id() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let millis = i64::try_from(millis).unwrap_or(0);
    debug_assert!(millis < 1 << 42, "the shifted clock would overflow an i64");
    let bytes: [u8; 16] = Uuid::new_v4().into();
    // A random v4's low 11 bits are version and variant, so the tag is 21 bits, a deliberate bit-width reduction.
    let tag = u32::from_le_bytes(bytes[0..4].try_into().expect("four bytes")) >> 11;
    (millis << 21) | i64::from(tag)
}

/// The replica's physical footprint, total pages and the free pages a trim can reclaim.
pub async fn replica_footprint<T>(client: &ConnettoClient<T>) -> (i64, i64)
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    client
        .with_conn(|conn| {
            let db = conn.conn();
            let pages = db.page_count(None).unwrap_or(0);
            let free = db.freelist_count(None).unwrap_or(0);
            (pages, free)
        })
        .await
        .unwrap_or((0, 0))
}

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

/// The label the account bar shows for a stored credential key, the decoded identity.
#[must_use]
pub fn account_label(key: &str) -> String {
    connetto_client::decode_identity::<String>(key).unwrap_or_else(|_| key.to_owned())
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

/// MIME class inferred from a file path extension, or `None` for non-images.
#[must_use]
pub fn mime_from_extension(path: &std::path::Path) -> Option<MimeClass> {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_lowercase())
        .as_deref()
    {
        Some("jpg" | "jpeg") => Some(MimeClass::Jpeg),
        Some("png") => Some(MimeClass::Png),
        _ => None,
    }
}

/// Decode the 32-byte `content_id` column to a `FileId`.
#[must_use]
pub fn photo_file_id(content_id: &[u8]) -> Option<FileId> {
    <[u8; 32]>::try_from(content_id)
        .ok()
        .map(FileId::from_bytes)
}

/// The quantity a new demo order names, 5 to 50.
#[must_use]
pub fn fresh_quantity() -> i64 {
    let millis = current_millis();
    ((millis % 9 + 1) * 5) as i64
}

/// The epoch milliseconds now. A browser has no standard-library clock, so the
/// web build reads the JavaScript clock.
fn current_millis() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        let v = js_sys::Date::now();
        // `u64::MAX as f64` rounds up to 2^64, so anything below it converts.
        if v.is_finite() && v >= 0.0 && v < u64::MAX as f64 {
            v as u64 // deliberate truncation: fractional part discarded
        } else {
            0
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64)
    }
}

/// Inserts one orders row and a photos row referencing it in one transaction, answering with the photo id.
///
/// The inserts name their `id` and `owner_id` and omit `created_at`, which the orders table's `now()` default mints.
///
/// # Errors
///
/// A diesel error when either insert fails.
pub fn stage_photo_row(
    conn: &mut diesel::SqliteConnection,
    file_id: FileId,
    owner_id: &str,
) -> Result<Uuid, diesel::result::Error> {
    conn.transaction(|conn| {
        let order_id = Uuid::new_v4();
        let photo_id = Uuid::new_v4();
        diesel::insert_into(orders::table)
            .values((
                orders::id.eq(order_id),
                orders::owner_id.eq(owner_id),
                orders::quantity.eq(1_i64),
            ))
            .execute(conn)?;
        diesel::insert_into(photos::table)
            .values((
                photos::id.eq(photo_id),
                photos::order_id.eq(order_id),
                photos::owner_id.eq(owner_id),
                photos::content_id.eq(file_id.as_bytes().to_vec()),
            ))
            .execute(conn)?;
        Ok(photo_id)
    })
}

/// The visible orders rows, newest first, the pane's list subscription.
#[must_use]
pub fn orders_newest_first() -> impl diesel::query_builder::Query {
    orders::table.order((orders::created_at.desc(), orders::id.desc()))
}

/// The orders `COUNT(*)` the server pushes.
#[must_use]
pub fn orders_count() -> impl diesel::query_builder::Query {
    orders::table.count()
}

/// The orders `COUNT(*)` grouped by quantity.
#[must_use]
pub fn orders_counts_by_quantity() -> impl diesel::query_builder::Query {
    orders::table
        .group_by(orders::quantity)
        .select((orders::quantity, diesel::dsl::count_star()))
}

/// The notes rows, oldest first, the pane's list subscription.
#[must_use]
pub fn notes_rows() -> impl diesel::query_builder::Query {
    notes::table.order(notes::id.asc())
}

/// The photos rows, oldest first, the pane's list subscription.
#[must_use]
pub fn photos_rows() -> impl diesel::query_builder::Query {
    photos::table.order(photos::id.asc())
}

/// Insert one orders row the add button names.
///
/// # Errors
///
/// A client error when queueing the write fails, or the write's diesel error.
pub async fn add_order<T>(
    client: &ConnettoClient<T>,
    identity: &str,
    quantity: i64,
) -> Result<usize, ClientError>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    let result = client
        .with_conn(move |conn| {
            diesel::insert_into(orders::table)
                .values((orders::owner_id.eq(identity), orders::quantity.eq(quantity)))
                .execute(conn.conn())
        })
        .await
        .and_then(|insert| insert.map_err(Into::into));
    if let Err(err) = &result {
        tracing::error!(error = %err, "order insert failed");
    }
    result
}

/// Remove one orders row by id, the pane's remove-newest write.
///
/// # Errors
///
/// A client error when queueing the write fails, or the write's diesel error.
pub async fn remove_newest_order<T>(
    client: &ConnettoClient<T>,
    id: Uuid,
) -> Result<usize, ClientError>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    let result = client
        .with_conn(move |conn| {
            diesel::delete(orders::table.filter(orders::id.eq(id))).execute(conn.conn())
        })
        .await
        .and_then(|insert| insert.map_err(Into::into));
    if let Err(err) = &result {
        tracing::error!(error = %err, "order remove failed");
    }
    result
}

/// Insert one device-private notes row with a fresh id, the pane's save write.
///
/// # Errors
///
/// A client error when queueing the write fails, or the write's diesel error.
pub async fn save_note<T>(client: &ConnettoClient<T>, body: &str) -> Result<usize, ClientError>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    let id = fresh_note_id();
    let result = client
        .with_conn(move |conn| {
            diesel::insert_into(notes::table)
                .values((notes::id.eq(id), notes::body.eq(body)))
                .execute(conn.conn())
        })
        .await
        .and_then(|insert| insert.map_err(Into::into));
    if let Err(err) = &result {
        tracing::error!(error = %err, "note save failed");
    }
    result
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

/// The session expiry warning line, the browser wording.
#[must_use]
pub fn expiry_warn_line(warn: &ExpiryWarning, now: SystemTime) -> String {
    let n = warn.pending_count();
    let secs = warn
        .session_expires_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let now_secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = secs.saturating_sub(now_secs) / 86400;
    format!("Session lapses in {days} day(s). {n} local item(s) at risk. Connect to refresh.")
}

/// The shared standing of the logout controls.
#[derive(Clone, PartialEq)]
pub enum LogoutState {
    /// Showing the two logout buttons.
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
