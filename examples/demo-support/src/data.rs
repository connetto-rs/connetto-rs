//! The pieces that reach the connetto client and the replica, the schema, its
//! rows, queries and writes, and the lines derived from client events. A UI
//! that renders a view another process computes builds without them.

use std::time::SystemTime;

use connetto_client::teardown::ExpiryWarning;
use connetto_client::{
    ClientError, ClientEvent, ConnettoClient, KeyedSpec, LiveGroups, LiveQuery, LiveSource,
    LiveSpec, LiveValue, StorageUse, SyncSchema, Watchable,
};
use connetto_core::messages::{FatalErrorReason, FullResyncReason, SyncStatus};
use connetto_core::traits::{MaybeSend, Transport};
use connetto_file_core::{FileId, MimeClass};
use diesel::prelude::*;
use diesel::sqlite::pragma::{FreelistCount, PageCount, SchemaPragma};
use rosetta_uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/connetto-translate.rs"));

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
        /// The preview's content identity.
        thumb_id -> Nullable<diesel::sql_types::Binary>,
        /// The preview's upload state.
        thumb_state -> Nullable<diesel::sql_types::Text>,
        /// The team allowed to read the photo.
        team_id -> Nullable<diesel::sql_types::Text>,
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

diesel::table! {
    /// The team membership rows, synced to every replica the team reads.
    team_members (team_id, member_id) {
        /// The team the membership sits in.
        team_id -> diesel::sql_types::Text,
        /// The account the membership names, the demo's account ids.
        member_id -> diesel::sql_types::Text,
    }
}

diesel::table! {
    /// The team note rows, readable and writable by the team's members.
    team_notes (id) {
        /// The replica-minted key.
        id -> rosetta_uuid::sql_types::Uuid,
        /// The team the note sits in.
        team_id -> diesel::sql_types::Text,
        /// Who wrote the note, which the policy settles the row against.
        author_id -> diesel::sql_types::Text,
        /// The note's body.
        body -> diesel::sql_types::Text,
    }
}

diesel::table! {
    /// Each account's login name, synced for the caller's own account and
    /// everyone sharing a team with it.
    users (id) {
        /// The account id.
        id -> diesel::sql_types::Text,
        /// The login name, the identity provider's subject.
        login_name -> diesel::sql_types::Text,
    }
}

diesel::define_sql_function! {
    /// The signed-in account's id, the replica's answer for the caller, which
    /// the roster's membership filter compares against.
    fn current_app_user() -> diesel::sql_types::Text;
}

/// One row of the synced `orders` table.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
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
#[derive(Queryable, Selectable, Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = notes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Note {
    /// The row's id.
    pub id: i64,
    /// The note's body.
    pub body: String,
}

/// One row of the synced `photos` table.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
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
    /// The preview's content identity.
    pub thumb_id: Option<Vec<u8>>,
    /// The preview's upload state.
    pub thumb_state: Option<String>,
    /// The team allowed to read the photo.
    pub team_id: Option<String>,
}

/// One row of the shared `products` catalogue.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
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

/// One row of the synced `team_members` roster.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = team_members)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct TeamMember {
    /// The team the membership sits in.
    pub team_id: String,
    /// The account the membership names.
    pub member_id: String,
}

/// One row of the synced `team_notes` table.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = team_notes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct TeamNote {
    /// The note's id.
    pub id: Uuid,
    /// The team the note sits in.
    pub team_id: String,
    /// Who wrote the note.
    pub author_id: String,
    /// The note's body.
    pub body: String,
}

/// One row of the synced `users` table.
#[derive(
    Queryable, Selectable, Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = users)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct User {
    /// The account id.
    pub id: String,
    /// The login name.
    pub login_name: String,
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
        FullResyncReason::TableRefused { table } => {
            format!("table {table} no longer matches its schema")
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

/// Encodes a JPEG preview within 256 pixels per side without enlarging the image.
#[must_use]
pub fn make_photo_thumbnail(bytes: &[u8]) -> Option<Vec<u8>> {
    let decoded = image::load_from_memory(bytes).ok()?;
    let thumbnail = if decoded.width() > 256 || decoded.height() > 256 {
        decoded.thumbnail(256, 256)
    } else {
        decoded
    };
    let mut output = Vec::new();
    image::codecs::jpeg::JpegEncoder::new(&mut output)
        .encode_image(&thumbnail.into_rgb8())
        .ok()?;
    Some(output)
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
    thumb_id: Option<FileId>,
    team_id: Option<&str>,
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
                photos::content_id.eq(file_id.as_bytes().as_slice()),
                photos::thumb_id.eq(thumb_id.as_ref().map(|id| id.as_bytes().as_slice())),
                photos::team_id.eq(team_id),
            ))
            .execute(conn)?;
        Ok(photo_id)
    })
}

/// The visible orders rows, newest first.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrdersNewestFirst;

impl LiveSpec for OrdersNewestFirst {
    const NAME: &'static str = "orders_newest_first";
    type Row = Order;
    type Handle = LiveQuery<Order>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        orders::table.order((orders::created_at.desc(), orders::id.desc()))
    }
}

impl KeyedSpec for OrdersNewestFirst {
    type Key = Uuid;

    fn key(row: &Order) -> Uuid {
        row.id
    }
}

/// The orders `COUNT(*)`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrdersCount;

impl LiveSpec for OrdersCount {
    const NAME: &'static str = "orders_count";
    type Row = i64;
    type Handle = LiveValue<i64>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        orders::table.count()
    }
}

/// The orders `COUNT(*)` grouped by quantity.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrdersCountsByQuantity;

impl LiveSpec for OrdersCountsByQuantity {
    const NAME: &'static str = "orders_counts_by_quantity";
    type Row = (i64, i64);
    type Handle = LiveGroups<i64, i64>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        orders::table
            .group_by(orders::quantity)
            .select((orders::quantity, diesel::dsl::count_star()))
    }
}

/// The notes rows, oldest first.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NotesRows;

impl LiveSpec for NotesRows {
    const NAME: &'static str = "notes_rows";
    type Row = Note;
    type Handle = LiveQuery<Note>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        notes::table.order(notes::id.asc())
    }
}

impl KeyedSpec for NotesRows {
    type Key = i64;

    fn key(row: &Note) -> i64 {
        row.id
    }
}

/// The photos rows, oldest first.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhotosRows;

impl LiveSpec for PhotosRows {
    const NAME: &'static str = "photos_rows";
    type Row = Photo;
    type Handle = LiveQuery<Photo>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        photos::table.order(photos::id.asc())
    }
}

impl KeyedSpec for PhotosRows {
    type Key = Uuid;

    fn key(row: &Photo) -> Uuid {
        row.id
    }
}

/// The catalogue ordered by name.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProductsByName;

impl LiveSpec for ProductsByName {
    const NAME: &'static str = "products_by_name";
    type Row = Product;
    type Handle = LiveQuery<Product>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        products::table.order(products::name.asc())
    }
}

impl KeyedSpec for ProductsByName {
    type Key = Uuid;

    fn key(row: &Product) -> Uuid {
        row.id
    }
}

/// The teams in the signed-in account's roster.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TeamsOfAccount;

impl LiveSpec for TeamsOfAccount {
    const NAME: &'static str = "teams_of_account";
    type Row = String;
    type Handle = LiveQuery<String>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        team_members::table
            .filter(team_members::member_id.eq(current_app_user()))
            .select(team_members::team_id)
    }
}

/// The picked team's members, the subscription that syncs the roster rows a
/// peer's note settles against, since the note policy reads the author's
/// membership on the replica that applies it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TeamMembersOf {
    /// The team whose roster is watched.
    pub team: String,
}

impl LiveSpec for TeamMembersOf {
    const NAME: &'static str = "team_members_of";
    type Row = String;
    type Handle = LiveQuery<String>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        team_members::table
            .filter(team_members::team_id.eq(self.team.clone()))
            .select(team_members::member_id)
            .order(team_members::member_id.asc())
    }
}

/// One team's notes, the pane's list subscription.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TeamNotesFor {
    /// The team whose notes are watched.
    pub team: String,
}

impl LiveSpec for TeamNotesFor {
    const NAME: &'static str = "team_notes_for";
    type Row = TeamNote;
    type Handle = LiveQuery<TeamNote>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        team_notes::table
            .filter(team_notes::team_id.eq(self.team.clone()))
            .order(team_notes::id.asc())
    }
}

impl KeyedSpec for TeamNotesFor {
    type Key = Uuid;

    fn key(row: &TeamNote) -> Uuid {
        row.id
    }
}

/// Every `users` row this account reads, its own and its teammates', the
/// names every badge shows.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserNames;

impl LiveSpec for UserNames {
    const NAME: &'static str = "user_names";
    type Row = User;
    type Handle = LiveQuery<User>;

    fn query<T>(&self) -> impl Watchable<T, Self::Row, Handle = Self::Handle> + use<T>
    where
        T: Transport + MaybeSend + 'static,
        T::Error: core::fmt::Display,
    {
        users::table.order(users::id.asc())
    }
}

impl KeyedSpec for UserNames {
    type Key = String;

    fn key(row: &User) -> String {
        row.id.clone()
    }
}

/// Follow the live `account`'s own login name from `source`, passing it to
/// `name` on every snapshot that holds the account's row, until the feed
/// fails with the error passed to `fail` or this future drops.
pub async fn follow_own_name<Source: LiveSource>(
    source: Source,
    account: String,
    mut name: impl FnMut(&str),
    fail: impl FnOnce(ClientError),
) {
    connetto_client::follow(
        source,
        UserNames,
        |rows| {
            if let Some(row) = rows.iter().find(|row| row.id == account) {
                name(&row.login_name);
            }
        },
        fail,
    )
    .await;
}

/// Insert one team note the pane's add button names, the replica minting its
/// id the way the deployment's key default does.
///
/// # Errors
///
/// A client error when queueing the write fails, or the write's diesel error.
pub async fn add_team_note<T>(
    client: &ConnettoClient<T>,
    team: &str,
    author: &str,
    body: &str,
) -> Result<usize, ClientError>
where
    T: Transport + MaybeSend + 'static,
    T::Error: core::fmt::Display,
{
    let result = client
        .with_conn(move |conn| {
            diesel::insert_into(team_notes::table)
                .values((
                    team_notes::team_id.eq(team),
                    team_notes::author_id.eq(author),
                    team_notes::body.eq(body),
                ))
                .execute(conn.conn())
        })
        .await
        .and_then(|insert| insert.map_err(Into::into));
    if let Err(err) = &result {
        tracing::error!(error = %err, "team note insert failed");
    }
    result
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

// The team notes panel's lines (R77 slice 8), the runner-readable form the
// panel shows and the star walk reads off the page dumps, one line per
// applied, confirmed, rolled back or refused peer write.

#[cfg(feature = "peer")]
fn hex_text(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// One primary-key column as the panel's lines name it, a 16-byte blob as its
/// uuid, the columns joined by `/` where the key spans more than one.
#[cfg(feature = "peer")]
#[must_use]
pub fn key_text(key: &[connetto_client::KeyValue]) -> String {
    key.iter()
        .map(|value| match value {
            connetto_client::KeyValue::Null => "null".to_owned(),
            connetto_client::KeyValue::Int(value) => value.to_string(),
            connetto_client::KeyValue::Real(value) => value.to_string(),
            connetto_client::KeyValue::Text(value) => value.clone(),
            connetto_client::KeyValue::Blob(bytes) => {
                let array: Result<[u8; 16], std::array::TryFromSliceError> =
                    bytes.as_slice().try_into();
                match array {
                    Ok(array) => Uuid::from(array).to_string(),
                    Err(_) => hex_text(bytes),
                }
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// One team note row as the panel shows it, the peer-applied rows marked
/// provisional with the provenance's account beside the row's own author.
#[must_use]
pub fn team_note_line(note: &TeamNote, provisional_account: Option<&str>) -> String {
    let body = note.body.replace(['\n', '\r', '\t'], " ");
    match provisional_account {
        Some(account) => {
            format!(
                "note: {body} by {} (provisional from {account})",
                note.author_id
            )
        }
        None => format!("note: {body} by {}", note.author_id),
    }
}

/// The retraction's cause as the rolled-back line says it, the refused entry
/// by the server's own reason.
#[cfg(feature = "peer")]
#[must_use]
pub fn retract_cause(cause: &connetto_core::exchange::RetractCause) -> String {
    match cause {
        connetto_core::exchange::RetractCause::Rejected(reason) => reason.to_string(),
        connetto_core::exchange::RetractCause::Conflict => {
            "two devices settled different changes at the sequence".to_owned()
        }
        connetto_core::exchange::RetractCause::Abandoned => {
            "the entry was never settled anywhere".to_owned()
        }
    }
}

/// The panel's one event per row an R77 event names, `applied` rows with the
/// author's account, the rest the table and key alone, and none for the
/// events the panel shows nothing for.
#[cfg(feature = "peer")]
#[must_use]
pub fn provisional_events(event: &ClientEvent) -> Vec<crate::view::TeamEvent> {
    use crate::view::TeamEvent;
    match event {
        ClientEvent::ProvisionalApplied { rows, .. } => rows
            .iter()
            .map(|row| TeamEvent {
                line: format!("provisional: applied {} {}", row.table, key_text(&row.key)),
                by: Some(row.account.clone()),
            })
            .collect(),
        ClientEvent::ProvisionalConfirmed { rows } => rows
            .iter()
            .map(|row| {
                TeamEvent::plain(format!(
                    "provisional: confirmed {} {}",
                    row.table,
                    key_text(&row.key)
                ))
            })
            .collect(),
        ClientEvent::ProvisionalRetracted { cause, rows, .. } => rows
            .iter()
            .map(|(table, key)| {
                TeamEvent::plain(format!(
                    "provisional: rolled back {table} {} because {}",
                    key_text(key),
                    retract_cause(cause)
                ))
            })
            .collect(),
        ClientEvent::PeerWriteRefused { rows, .. } => rows
            .iter()
            .map(|(table, key)| {
                TeamEvent::plain(format!("provisional: refused {table} {}", key_text(key)))
            })
            .collect(),
        // The device's own offline write the server refused, which its peers
        // see rolled back through the verdicts, stands as the same line.
        ClientEvent::MutationRejected { reason, rows, .. } => rows
            .iter()
            .map(|row| {
                TeamEvent::plain(format!(
                    "provisional: rolled back {} {} because {reason}",
                    row.table,
                    key_text(&row.key)
                ))
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The tidy control's line, the provisional rows the eviction pass kept.
#[cfg(feature = "peer")]
#[must_use]
pub fn tidy_line(remaining: usize) -> String {
    format!("tidy: {remaining} provisional row(s) remain")
}

#[cfg(all(test, feature = "peer"))]
mod team_lines_tests {
    use super::*;
    use connetto_client::{KeyValue, provisional::ProvisionalRow};

    const NOTE_ID: [u8; 16] = [
        0x9f, 0x1c, 0x80, 0x01, 0x85, 0x5d, 0x4b, 0x1e, 0x9c, 0x2f, 0x73, 0xa1, 0x6b, 0x0d, 0x4e,
        0x5a,
    ];

    fn note_key() -> Vec<KeyValue> {
        vec![KeyValue::Blob(NOTE_ID.to_vec())]
    }

    fn record(table: &str, account: &str) -> ProvisionalRow {
        ProvisionalRow {
            table: table.to_owned(),
            key: note_key(),
            author: account.as_bytes().to_vec(),
            seq: 3,
            account: account.to_owned(),
            received_at: 0,
            state: connetto_client::provisional::ProvisionalState::Provisional,
            displaced_author: None,
            displaced_seq: None,
        }
    }

    /// The key's columns stand as their text, the 16-byte blob as its uuid.
    #[test]
    fn key_text_names_each_column_and_the_uuid() {
        let id: Uuid = NOTE_ID.into();
        assert_eq!(key_text(&note_key()), id.to_string());
        assert_eq!(key_text(&[KeyValue::Int(-7)]), "-7");
        assert_eq!(key_text(&[KeyValue::Text("camp".to_owned())]), "camp");
        assert_eq!(key_text(&[KeyValue::Null]), "null");
        assert_eq!(key_text(&[KeyValue::Blob(vec![0x01, 0x02])]), "0102");
        assert_eq!(
            key_text(&[KeyValue::Text("camp".to_owned()), KeyValue::Int(2)]),
            "camp/2"
        );
    }

    /// A settled row names its author alone, a peer-applied row adds the
    /// provenance's account.
    #[test]
    fn the_note_line_marks_the_peer_row_provisional() {
        let note = TeamNote {
            id: NOTE_ID.into(),
            team_id: "camp".to_owned(),
            author_id: "bob".to_owned(),
            body: "head to the ridge".to_owned(),
        };
        assert_eq!(
            team_note_line(&note, None),
            "note: head to the ridge by bob"
        );
        assert_eq!(
            team_note_line(&note, Some("bob@demo.dev")),
            "note: head to the ridge by bob (provisional from bob@demo.dev)"
        );
        // A wrapped body stays on one line, the page dump's lines holding.
        let wrapped = TeamNote {
            body: "head to the\nridge".to_owned(),
            ..note
        };
        assert_eq!(
            team_note_line(&wrapped, None),
            "note: head to the ridge by bob"
        );
    }

    /// The retraction's cause stands as the line's reason, the refused entry
    /// by the server's reason.
    #[test]
    fn the_cause_lines_name_the_reason() {
        use connetto_core::exchange::RetractCause;
        use connetto_core::messages::MutationRejectReason;
        assert_eq!(
            retract_cause(&RetractCause::Rejected(MutationRejectReason::Unauthorized)),
            "unauthorized"
        );
        assert_eq!(
            retract_cause(&RetractCause::Conflict),
            "two devices settled different changes at the sequence"
        );
        assert_eq!(
            retract_cause(&RetractCause::Abandoned),
            "the entry was never settled anywhere"
        );
    }

    /// The panel's text of each event `provisional_events` stands.
    fn provisional_event_lines(event: &ClientEvent) -> Vec<String> {
        provisional_events(event)
            .iter()
            .map(crate::view::TeamEvent::text)
            .collect()
    }

    /// Each R77 event stands one line per row it names, the other events
    /// none, and an applied row names its author's account apart.
    #[test]
    fn each_event_stands_its_lines() {
        let id: Uuid = NOTE_ID.into();
        let applied = ClientEvent::ProvisionalApplied {
            author: "bob".as_bytes().to_vec(),
            seq: 3,
            rows: vec![record("team_notes", "bob@demo.dev")],
        };
        assert_eq!(
            provisional_event_lines(&applied),
            [format!(
                "provisional: applied team_notes {id} by bob@demo.dev"
            )]
        );
        assert_eq!(
            provisional_events(&applied)[0].by.as_deref(),
            Some("bob@demo.dev")
        );
        let confirmed = ClientEvent::ProvisionalConfirmed {
            rows: vec![record("team_notes", "bob@demo.dev")],
        };
        assert_eq!(
            provisional_event_lines(&confirmed),
            [format!("provisional: confirmed team_notes {id}")]
        );
        let retracted = ClientEvent::ProvisionalRetracted {
            author: "carol".as_bytes().to_vec(),
            seq: 7,
            cause: connetto_core::exchange::RetractCause::Rejected(
                connetto_core::messages::MutationRejectReason::Unauthorized,
            ),
            rows: vec![("team_notes".to_owned(), note_key())],
        };
        assert_eq!(
            provisional_event_lines(&retracted),
            [format!(
                "provisional: rolled back team_notes {id} because unauthorized"
            )]
        );
        let refused = ClientEvent::PeerWriteRefused {
            author: "carol".as_bytes().to_vec(),
            seq: 7,
            rows: vec![("team_notes".to_owned(), note_key())],
        };
        assert_eq!(
            provisional_event_lines(&refused),
            [format!("provisional: refused team_notes {id}")]
        );
        // The device's own offline note, refused when the server comes back,
        // stands as the same rollback line its peers show.
        let rejected = ClientEvent::MutationRejected {
            client_seq: 3,
            reason: connetto_core::messages::MutationRejectReason::Unauthorized,
            rows: vec![connetto_client::AffectedRow {
                table: "team_notes".to_owned(),
                key: note_key(),
            }],
        };
        assert_eq!(
            provisional_event_lines(&rejected),
            [format!(
                "provisional: rolled back team_notes {id} because unauthorized"
            )]
        );
        assert!(provisional_event_lines(&ClientEvent::Reconnected).is_empty());
        assert_eq!(tidy_line(0), "tidy: 0 provisional row(s) remain");
        assert_eq!(tidy_line(3), "tidy: 3 provisional row(s) remain");
    }
}

#[cfg(test)]
mod photo_tests {
    use super::*;
    use diesel::connection::SimpleConnection;
    use image::{DynamicImage, GenericImageView, ImageFormat};
    use std::io::Cursor;

    #[test]
    fn thumbnails_are_jpeg_and_preserve_dimensions_without_upscaling() {
        for format in [ImageFormat::Png, ImageFormat::Jpeg] {
            for (width, height, expected) in [
                (640, 320, (256, 128)),
                (320, 640, (128, 256)),
                (32, 16, (32, 16)),
            ] {
                let mut input = Cursor::new(Vec::new());
                DynamicImage::new_rgb8(width, height)
                    .write_to(&mut input, format)
                    .expect("encode fixture");
                let bytes = make_photo_thumbnail(input.get_ref()).expect("thumbnail");
                assert_eq!(image::guess_format(&bytes).unwrap(), ImageFormat::Jpeg);
                assert_eq!(
                    image::load_from_memory(&bytes).unwrap().dimensions(),
                    expected
                );
            }
        }
    }

    #[test]
    fn undecodable_photo_has_no_thumbnail() {
        assert!(make_photo_thumbnail(b"unsupported image").is_none());
    }

    #[test]
    fn staged_photo_names_thumbnail_and_team() {
        let mut conn = diesel::SqliteConnection::establish(":memory:").unwrap();
        conn.batch_execute(
            "CREATE TABLE orders (id BLOB PRIMARY KEY, owner_id TEXT NOT NULL, quantity INTEGER NOT NULL);
             CREATE TABLE photos (id BLOB PRIMARY KEY, order_id BLOB NOT NULL, owner_id TEXT NOT NULL,
                 content_id BLOB NOT NULL, content_state TEXT, thumb_id BLOB, thumb_state TEXT, team_id TEXT);",
        )
        .unwrap();
        let full = FileId::from_bytes([1; 32]);
        let thumb = FileId::from_bytes([2; 32]);
        for (thumbnail, team) in [(Some(thumb), Some("team")), (None, None)] {
            let id = stage_photo_row(&mut conn, full, "author", thumbnail, team).unwrap();
            let row = photos::table.find(id).first::<Photo>(&mut conn).unwrap();
            assert_eq!(row.content_id, full.as_bytes());
            assert_eq!(
                row.thumb_id.as_deref(),
                thumbnail.as_ref().map(|id| id.as_bytes().as_slice())
            );
            assert_eq!(row.team_id.as_deref(), team);
            assert_eq!(row.thumb_state, None);
            assert_eq!(row.content_state, None);
        }
    }
}

#[cfg(test)]
mod live_spec_tests {
    use super::*;

    /// A row read natively crosses to the page as MessagePack, the codec a
    /// descriptor's snapshot travels in, and decodes to the same row.
    #[test]
    fn rows_cross_messagepack_unchanged() {
        let order = Order {
            id: Uuid::new_v4(),
            owner_id: "alice".to_owned(),
            quantity: 7,
            created_at: chrono::NaiveDate::from_ymd_opt(2026, 11, 17)
                .and_then(|day| day.and_hms_micro_opt(12, 34, 56, 789))
                .expect("a valid instant"),
        };
        let photo = Photo {
            id: Uuid::new_v4(),
            order_id: order.id,
            owner_id: "alice".to_owned(),
            content_id: vec![0, 1, 254, 255],
            content_state: None,
            thumb_id: Some(vec![9; 32]),
            thumb_state: Some("uploaded".to_owned()),
            team_id: None,
        };
        let encoded = rmp_serde::to_vec_named(&vec![order.clone()]).expect("encode orders");
        let decoded: Vec<Order> = rmp_serde::from_slice(&encoded).expect("decode orders");
        assert_eq!(decoded, vec![order]);
        let encoded = rmp_serde::to_vec_named(&vec![photo.clone()]).expect("encode photos");
        let decoded: Vec<Photo> = rmp_serde::from_slice(&encoded).expect("decode photos");
        assert_eq!(decoded, vec![photo]);
    }
}
