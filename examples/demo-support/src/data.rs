//! The pieces that reach the connetto client and the replica, the schema, its
//! rows, queries and writes, and the lines derived from client events. A UI
//! that renders a view another process computes builds without them.

use std::time::SystemTime;

use connetto_client::teardown::ExpiryWarning;
use connetto_client::{
    ClientError, ClientEvent, ConnettoClient, LiveGroups, LiveQuery, LiveValue, StorageUse,
    SyncSchema, Watchable,
};
use connetto_core::messages::{FatalErrorReason, FullResyncReason, SyncStatus};
use connetto_core::traits::{MaybeSend, Transport};
use connetto_file_core::{FileId, MimeClass};
use diesel::prelude::*;
use diesel::sqlite::pragma::{FreelistCount, PageCount, SchemaPragma};
use rosetta_uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/connetto-schema.rs"));

pub use connetto_demo_deployment::{AUTH_BASE, DEMO_WS_URL, auth_landing};

/// The one orders subscription every window drives, the visible rows only.
pub const DEMO_QUERY: &str = "SELECT * FROM orders WHERE quantity > 0";
/// The extra upstream subscription for the photos rows.
pub const PHOTO_QUERY: &str = "SELECT * FROM photos";
/// The catalogue every account reads, one copy per device in the shared
/// store (R11).
pub const PRODUCT_QUERY: &str = "SELECT * FROM products";

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

diesel::table! {
    /// The public catalogue, shared by every account on the device.
    products (id) {
        /// The deployment's key.
        id -> rosetta_uuid::sql_types::Uuid,
        /// The product's name.
        name -> diesel::sql_types::Text,
        /// The price in cents.
        price_cents -> diesel::sql_types::BigInt,
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

/// One row of the shared `products` catalogue.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = products)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Product {
    /// The row's id.
    pub id: Uuid,
    /// The product's name.
    pub name: String,
    /// The price in cents.
    pub price_cents: i64,
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
            let pages = PageCount::get::<i64>(None, db).unwrap_or(0);
            let free = FreelistCount::get::<i64>(None, db).unwrap_or(0);
            (pages, free)
        })
        .await
        .unwrap_or((0, 0))
}

/// The storage line, what this account's replica and the device's shared
/// store take, read from the worker in a tab.
pub async fn storage_line<T>(client: &ConnettoClient<T>) -> String
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    match client.storage_use().await {
        Ok(used) => describe_storage(&used),
        Err(err) => format!("storage: unread ({err})"),
    }
}

/// `used` as the storage line words it, the bytes in use rounded to KiB.
#[must_use]
pub fn describe_storage(used: &StorageUse) -> String {
    let kib =
        |bytes: connetto_core::messages::StoreBytes| bytes.file.saturating_sub(bytes.free) / 1024;
    let replica = kib(used.replica);
    match &used.shared {
        Some(shared) => {
            let accounts = if shared.identities == 1 {
                "1 account".to_owned()
            } else {
                format!("{} accounts", shared.identities)
            };
            format!(
                "storage: this account {replica} KiB, shared store {} KiB used by {accounts}",
                kib(shared.bytes)
            )
        }
        None => format!("storage: this account {replica} KiB, nothing shared"),
    }
}

/// The catalogue rows as the pane shows them.
#[must_use]
pub fn product_rows(products: &[Product]) -> Vec<crate::view::ProductRow> {
    products
        .iter()
        .map(|product| crate::view::ProductRow {
            name: product.name.clone(),
            price: crate::price_label(product.price_cents),
        })
        .collect()
}

/// The label the account bar shows for a stored credential key, the decoded identity.
#[must_use]
pub fn account_label(key: &str) -> String {
    connetto_client::decode_identity::<String>(key).unwrap_or_else(|_| key.to_owned())
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
pub fn orders_newest_first<T>() -> impl Watchable<T, Order, Handle = LiveQuery<Order>>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    orders::table.order((orders::created_at.desc(), orders::id.desc()))
}

/// The orders `COUNT(*)` the server pushes.
#[must_use]
pub fn orders_count<T>() -> impl Watchable<T, i64, Handle = LiveValue<i64>>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    orders::table.count()
}

/// The orders `COUNT(*)` grouped by quantity.
#[must_use]
pub fn orders_counts_by_quantity<T>() -> impl Watchable<T, (i64, i64), Handle = LiveGroups<i64, i64>>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    orders::table
        .group_by(orders::quantity)
        .select((orders::quantity, diesel::dsl::count_star()))
}

/// The notes rows, oldest first, the pane's list subscription.
#[must_use]
pub fn notes_rows<T>() -> impl Watchable<T, Note, Handle = LiveQuery<Note>>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    notes::table.order(notes::id.asc())
}

/// The photos rows, oldest first, the pane's list subscription.
#[must_use]
pub fn photos_rows<T>() -> impl Watchable<T, Photo, Handle = LiveQuery<Photo>>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    photos::table.order(photos::id.asc())
}

/// The catalogue, by name.
#[must_use]
pub fn products_by_name<T>() -> impl Watchable<T, Product, Handle = LiveQuery<Product>>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    products::table.order(products::name.asc())
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
