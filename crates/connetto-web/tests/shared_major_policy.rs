//! Browser account and shared-cache upgrades preserve durable work.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use connetto_client::cipher::unlock;
use connetto_client::{
    ClientBuilder, ClientError, ClientEvent, ConnettoConnection, ContentPlace, Custody, FirstThen,
    Gate, Grant, HeldCredential, Located, ReplicaKey, ReplicaPlace, SyncSchema, UnsyncedWrite,
};
use connetto_core::messages::{AggregateUpdate, ControlMessage, SnapshotEnd, SubscriptionSpec};
use connetto_core::schema::{SchemaBundle, SharedLayout};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::{ReplicaKeyStore, Transport};
use connetto_core::{Cursor, IncomingFrame, MAJOR_DIRECTIVE, Major};
use connetto_web::storage::ReplicaStorage;
use diesel::SqliteConnection;
use diesel::prelude::*;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The account table at major 1.
const ORDERS_V1: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT)";
/// The account table at major 2, whose added column changes the build's hash.
const ORDERS_V2: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT, \
     rank INTEGER NOT NULL DEFAULT 0)";
/// The account table a same-major, different-source build declares.
const ORDERS_V1B: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT, note TEXT)";
/// The physical table of the split shared table, in the device-wide file.
const SHARED_STORE: &str =
    "CREATE TABLE connetto_shared.vocabulary_store (id INTEGER PRIMARY KEY, word TEXT)";
/// The connection-local reader over the shared physical table.
const TEMP_READER: &str =
    "CREATE TEMP VIEW vocabulary AS SELECT id, word FROM connetto_shared.vocabulary_store";
/// The replay cursor the scripted run persists, non-empty on purpose.
const RESUME_CURSOR: [u8; 16] = [0x5a; 16];

/// The unshared replica DDL, every table as an ordinary main-schema table,
/// the split one read through its logical view.
fn mirror(orders: &str) -> String {
    format!(
        "{orders}; CREATE TABLE vocabulary_store (id INTEGER PRIMARY KEY, word TEXT); \
         CREATE VIEW vocabulary AS SELECT id, word FROM vocabulary_store"
    )
}

diesel::table! {
    /// The private table, in the account file, at every major.
    orders (id) {
        /// Order identifier.
        id -> Integer,
        /// The label.
        label -> Nullable<Text>,
    }
}

diesel::table! {
    /// The logical reader over the split shared table's physical table.
    vocabulary (id) {
        /// Entry identifier.
        id -> Integer,
        /// The entry's word.
        word -> Text,
    }
}

diesel::table! {
    /// The physical table of the split shared table, in the shared file on a
    /// worker connection.
    vocabulary_store (id) {
        /// Entry identifier.
        id -> Integer,
        /// The entry's word.
        word -> Text,
    }
}

diesel::table! {
    /// The client's pending queue, read by the assertions.
    #[sql_name = "_connetto_pending"]
    pending (writer, seq) {
        /// The writer the entry is queued under.
        writer -> Text,
        /// Its sequence in that writer's queue.
        seq -> BigInt,
        /// The stored changeset.
        changeset -> Binary,
        /// Its chain hash.
        entry_hash -> Binary,
        /// The major it was captured under.
        major -> Nullable<BigInt>,
    }
}

diesel::table! {
    /// A writer's durable frontier, read by the assertions.
    #[sql_name = "_connetto_writers"]
    writers (id) {
        /// The writer.
        id -> Text,
        /// The next sequence to capture.
        next_seq -> BigInt,
        /// The head of the entry chain.
        head -> Binary,
    }
}

diesel::table! {
    /// The one-row record of the build the account file was made under.
    #[sql_name = "_connetto_build"]
    connetto_build (id) {
        /// Always 1.
        id -> Integer,
        /// The bundle hash.
        version -> Binary,
    }
}

diesel::table! {
    /// An old major a rebuild left work for, read by the assertions.
    #[sql_name = "_connetto_held_batch"]
    held_batch (major) {
        /// The held major.
        major -> BigInt,
        /// The bundle hash its drain presents.
        version -> Binary,
    }
}

diesel::table! {
    /// The replica-local metadata, the resume state a lifetime change clears.
    #[sql_name = "_connetto_meta"]
    meta (id) {
        /// Always 1.
        id -> Integer,
        /// The replay cursor.
        cursor -> Binary,
        /// The subscription coverage at the cursor.
        cursor_subjects -> Nullable<Binary>,
    }
}

diesel::table! {
    /// The rested aggregate answers, cleared with the resume state.
    #[sql_name = "_connetto_aggregates"]
    aggregates (query_id, binds, group_key) {
        /// The query row the value belongs to.
        query_id -> Integer,
        /// The subscription's binds.
        binds -> Binary,
        /// The group key, empty for a scalar.
        group_key -> Binary,
        /// The group values, when there are any.
        group_values_json -> Nullable<Text>,
        /// The `JSON` the server pushed.
        result_json -> Text,
        /// When the value was last written.
        updated_at -> BigInt,
    }
}

diesel::table! {
    /// The registered query specs a lifetime change keeps.
    #[sql_name = "_connetto_query"]
    query (id) {
        /// The query row.
        id -> Integer,
        /// The registered SQL.
        sql -> Text,
    }
}

diesel::table! {
    /// The subscription registrations a lifetime change keeps.
    #[sql_name = "_connetto_subscription"]
    subscription (id) {
        /// The subscription id.
        id -> Text,
        /// Its query row.
        query_id -> Integer,
        /// Its delivery priority.
        priority -> Integer,
        /// The pin's name, when one names it.
        pin_name -> Nullable<Text>,
        /// When it was stopped, when it is.
        stopped_at -> Nullable<BigInt>,
        /// Its grace in seconds.
        grace_secs -> Integer,
    }
}

/// One queued entry's stored bytes, chain hash and capture major.
#[derive(Debug, PartialEq, diesel::Queryable)]
struct QueuedRow {
    changeset: Vec<u8>,
    entry_hash: Vec<u8>,
    major: Option<i64>,
}

/// One `user_version` reading, which no `table!` can name.
#[derive(Debug, diesel::QueryableByName)]
struct UserVersion {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    user_version: i32,
}

/// The row shape of a raw count probe, which needs a by-name row type.
#[derive(Debug, diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// The record the client locates for the shared store.
fn shared_record() -> String {
    connetto_client::shared_store_name(connetto_client::REPLICA_PREFIX)
}

/// The pool entry of identity `id`'s replica.
fn replica_name(id: &str) -> String {
    connetto_client::replica_db_name(connetto_client::REPLICA_PREFIX, id)
        .expect("a string identity serializes")
}

/// A deterministic replica key for the case's identity.
fn key(byte: u8) -> ReplicaKey {
    ReplicaKey::from_bytes([byte; ReplicaKey::LEN])
}

/// A dialer handing out `transport` once and nothing after.
fn once<T: Transport + 'static>(
    transport: T,
) -> FirstThen<impl FnMut() -> core::future::Ready<Result<T, &'static str>>> {
    FirstThen::new(transport, || core::future::ready(Err("spent")))
}

/// The account and split shared schema at `major`.
fn bundle(major: u32, orders: &str) -> SchemaBundle {
    SchemaBundle::new(
        format!("{MAJOR_DIRECTIVE} {major}\n{orders}"),
        "",
        mirror(orders),
        [("vocabulary", "vocabulary_store")],
        ["vocabulary"],
        None::<&str>,
    )
    .expect("the source declares its major")
    .with_shared_tables(["vocabulary"])
    .with_shared_layout(SharedLayout::new(
        format!("{orders}; {TEMP_READER}; {SHARED_STORE}"),
        format!("{orders}; {TEMP_READER}"),
        format!("{SHARED_STORE}; {TEMP_READER}"),
        TEMP_READER,
    ))
}

/// The worker's encrypted replica entries in the OPFS pool.
struct Opfs<'a> {
    storage: &'a ReplicaStorage,
}

impl ReplicaPlace for Opfs<'_> {
    fn locate(&self, name: &str) -> Result<Located, ClientError> {
        Ok(Located::new(
            name,
            self.storage.db_url(name),
            self.storage.exists(name),
            ContentPlace::InMemory,
        ))
    }
}

/// Independent account and device keys for the encrypted fixture files.
struct Keys {
    replica: ReplicaKey,
    shared: ReplicaKey,
}

impl ReplicaKeyStore for Keys {
    type Error = ClientError;

    fn load(
        &self,
        name: &str,
    ) -> impl core::future::Future<Output = Result<Option<ReplicaKey>, ClientError>> {
        let key = if name == shared_record() {
            &self.shared
        } else {
            &self.replica
        };
        core::future::ready(Ok(Some(key.clone())))
    }

    fn store(
        &self,
        _name: &str,
        _key: &ReplicaKey,
    ) -> impl core::future::Future<Output = Result<(), ClientError>> {
        core::future::ready(Ok(()))
    }

    fn clear(&self, _name: &str) -> impl core::future::Future<Output = Result<(), ClientError>> {
        core::future::ready(Ok(()))
    }

    fn protection(&self) -> Custody {
        Custody::Ephemeral
    }
}

/// Identity `id` over `schema`, on the OPFS pool, opened offline with no
/// transport for a caller that attaches one by hand.
async fn open(
    storage: &ReplicaStorage,
    id: &str,
    replica_key: &ReplicaKey,
    schema: SchemaBundle,
) -> Result<ConnettoConnection<FakeTransport>, ClientError> {
    ClientBuilder::new(
        SyncSchema::new(schema).with_shared_store(true),
        once(FakeTransport::accepting()),
    )
    .signed_in(
        HeldCredential::new(Grant::new(format!("user:{id}")), id)
            .expect("a string identity serializes"),
    )
    .durable(
        Opfs { storage },
        Keys {
            replica: replica_key.clone(),
            shared: key(0x33),
        },
        connetto_client::MemoryRefreshStore::default(),
    )
    .with_gate(Gate::off())
    .open_driven()
    .await
}

/// The pool entry `name` opened raw under `key`, for the record a refused
/// open must not have written.
fn raw_open(storage: &ReplicaStorage, name: &str, key: &ReplicaKey) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(&storage.db_url(name)).expect("open the database");
    unlock(&mut conn, key).expect("key the database");
    conn
}

/// The major the account file records, 0 for none. A vendor pragma, which
/// the typed DSL cannot express.
fn user_version(conn: &mut SqliteConnection) -> i32 {
    diesel::sql_query("PRAGMA main.user_version")
        .get_result::<UserVersion>(conn)
        .expect("read user_version")
        .user_version
}

/// The bundle hash the account file records beside its major.
fn recorded_version(conn: &mut SqliteConnection) -> Vec<u8> {
    connetto_build::table
        .select(connetto_build::version)
        .first(conn)
        .expect("read the build record")
}

/// The queued entry's stored bytes, chain hash and capture major.
fn queued_row(conn: &mut SqliteConnection) -> QueuedRow {
    pending::table
        .select((pending::changeset, pending::entry_hash, pending::major))
        .first(conn)
        .expect("read the queued bytes")
}

/// Drive `conn` one frame at a time until `is_target` holds, bounded so a
/// scripted frame that never arrives fails the case rather than hanging the
/// worker.
async fn pump_until<F>(
    conn: &mut ConnettoConnection<FakeTransport>,
    mut is_target: F,
) -> Result<(), ClientError>
where
    F: FnMut(&ClientEvent) -> bool,
{
    for _ in 0..64 {
        if is_target(&conn.pump_one().await?) {
            return Ok(());
        }
    }
    Err(ClientError::Protocol(
        "the scripted frame never arrived".to_owned(),
    ))
}

/// Clear the device-wide store and the case's replica entries, and grow the
/// pool for the run, so the one shared file the origin keeps never leaks a
/// state into another case.
async fn clear(storage: &ReplicaStorage, names: &[&str]) {
    for name in names {
        storage.delete_db(name).expect("clear the entry");
    }
    storage.reserve(8).await.expect("room in the pool");
}

/// Seed a durable cursor, aggregate answer, pin, queued write and optional shared row.
async fn seeded_run(
    storage: &ReplicaStorage,
    id: &str,
    v1: &SchemaBundle,
    shared_row: bool,
) -> (Vec<UnsyncedWrite>, QueuedRow) {
    let mut conn = open(storage, id, &key(0x11), v1.clone())
        .await
        .expect("fresh open");
    let row_spec = SubscriptionSpec::new("SELECT id, label FROM orders");
    let stat_spec = SubscriptionSpec::new("SELECT count(*) FROM vocabulary_store");
    conn.subscribe_spec("resume", row_spec.clone())
        .await
        .expect("register the row spec");
    conn.pin_subscription("pinned", "resume", &row_spec)
        .expect("name the pin");
    conn.subscribe_spec("statistic", stat_spec)
        .await
        .expect("register the computed spec");
    conn.attach(FakeTransport::accepting_then_delivering(vec![
        IncomingFrame::Control(ControlMessage::SnapshotEnd(SnapshotEnd {
            sub_id: "resume".to_owned(),
            cursor: Cursor::new(RESUME_CURSOR.to_vec()),
        })),
        IncomingFrame::Control(ControlMessage::AggregateUpdate(AggregateUpdate {
            sub_id: "statistic".to_owned(),
            group_key: None,
            group_values_json: None,
            result_json: Some("42".to_owned()),
            is_full_result: true,
        })),
    ]))
    .await
    .expect("attach the scripted server");
    diesel::insert_into(orders::table)
        .values((orders::id.eq(1), orders::label.eq("local")))
        .execute(&mut *conn.conn())
        .expect("write offline");
    conn.push().await.expect("queue").expect("a seq");
    if shared_row {
        diesel::insert_into(vocabulary_store::table)
            .values((
                vocabulary_store::id.eq(5),
                vocabulary_store::word.eq("kept"),
            ))
            .execute(&mut *conn.conn())
            .expect("write the shared store");
    }
    pump_until(&mut conn, |event| {
        matches!(
            event,
            ClientEvent::Aggregate { sub_id, .. } if sub_id.as_str() == "statistic"
        )
    })
    .await
    .expect("the scripted frames land");
    let cursor: Vec<u8> = meta::table
        .select(meta::cursor)
        .first(&mut *conn.conn())
        .expect("read the replay cursor");
    assert_eq!(
        cursor,
        RESUME_CURSOR.to_vec(),
        "the snapshot end persisted the cursor"
    );
    (conn.unsynced(), queued_row(&mut *conn.conn()))
}

/// A fresh account at a newer major opens against the file an older run
/// stamped, upgrading the shared cache in place, and the older run's queued
/// write survives its own rebuild under its writer, its sequence, its chain
/// hash and the major it was captured in.
#[wasm_bindgen_test]
async fn a_shared_major_upgrade_keeps_another_accounts_queued_write_under_its_major() {
    let storage = ReplicaStorage::install().await;
    let alice = replica_name("rmaj-upg-alice");
    let bob = replica_name("rmaj-upg-bob");
    clear(&storage, &[alice.as_str(), shared_record().as_str()]).await;

    let v1 = bundle(1, ORDERS_V1);
    let v2 = bundle(2, ORDERS_V2);

    let (writer, seq, queued) = {
        let mut conn = open(&storage, "rmaj-upg-alice", &key(0x11), v1.clone())
            .await
            .expect("the older build opens");
        diesel::insert_into(orders::table)
            .values((orders::id.eq(1), orders::label.eq("unsent")))
            .execute(&mut *conn.conn())
            .expect("write offline");
        let seq = conn.push().await.expect("queue").expect("a seq");
        let queued = queued_row(&mut *conn.conn());
        let writer = conn.unsynced().first().expect("one queued write").writer;
        (writer, seq, queued)
    };

    // Bob's fresh account at the newer build opens, upgrading the shared
    // cache in place beside alice's file.
    drop(
        open(&storage, "rmaj-upg-bob", &key(0x22), v2.clone())
            .await
            .expect("the newer build opens"),
    );

    // Alice reopens at the newer build: her rebuild serves the new schema
    // and keeps the write under its writer and the major it was captured in.
    let mut conn = open(&storage, "rmaj-upg-alice", &key(0x11), v2.clone())
        .await
        .expect("reopen the newer build");
    assert_eq!(user_version(conn.conn()), 2);
    assert_eq!(recorded_version(conn.conn()), v2.version().hash());
    assert_eq!(
        conn.unsynced(),
        vec![UnsyncedWrite {
            writer,
            seq,
            major: Some(Major::new(1).expect("one is a major")),
        }],
        "the write stays queued under its writer and the major it was captured in"
    );
    assert_eq!(
        queued_row(&mut *conn.conn()),
        queued,
        "the held bytes keep their changeset and chain hash"
    );
    let frontier: (i64, Vec<u8>) = writers::table
        .select((writers::next_seq, writers::head))
        .first(&mut *conn.conn())
        .expect("read the writer frontier");
    assert_eq!(
        frontier,
        (
            i64::try_from(seq).expect("a sequence fits a signed count") + 1,
            queued.entry_hash
        ),
        "the frontier keeps the head of the chain the write started"
    );
    let held: (i64, Vec<u8>) = held_batch::table
        .select((held_batch::major, held_batch::version))
        .first(&mut *conn.conn())
        .expect("read the held batch");
    assert_eq!(
        held,
        (1, v1.version().hash().to_vec()),
        "the rebuild holds the old major's batch under its hash"
    );

    drop(conn);
    clear(
        &storage,
        &[alice.as_str(), bob.as_str(), shared_record().as_str()],
    )
    .await;
}

/// The shared cache a major upgrade rebuilds serves the rows written after
/// the upgrade to the accounts that open at the upgraded major, and the
/// second account's open clears none of them.
#[wasm_bindgen_test]
async fn a_second_account_opening_after_a_shared_upgrade_retains_the_shared_rows() {
    let storage = ReplicaStorage::install().await;
    let alice = replica_name("rmaj-late-alice");
    let bob = replica_name("rmaj-late-bob");
    let carol = replica_name("rmaj-late-carol");
    clear(
        &storage,
        &[
            alice.as_str(),
            bob.as_str(),
            carol.as_str(),
            shared_record().as_str(),
        ],
    )
    .await;

    let v1 = bundle(1, ORDERS_V1);
    let v2 = bundle(2, ORDERS_V2);

    // Alice's run at the older major writes the shared rows, then ends.
    {
        let mut conn = open(&storage, "rmaj-late-alice", &key(0x11), v1.clone())
            .await
            .expect("the older build opens");
        diesel::insert_into(vocabulary_store::table)
            .values((
                vocabulary_store::id.eq(1),
                vocabulary_store::word.eq("first"),
            ))
            .execute(&mut *conn.conn())
            .expect("alice writes the shared store");
        diesel::insert_into(vocabulary_store::table)
            .values((
                vocabulary_store::id.eq(2),
                vocabulary_store::word.eq("second"),
            ))
            .execute(&mut *conn.conn())
            .expect("alice writes the shared store");
    }

    // Bob's fresh account at the newer major opens against the older shared
    // cache and upgrades it in place, then writes the post-upgrade row.
    {
        let mut conn = open(&storage, "rmaj-late-bob", &key(0x22), v2.clone())
            .await
            .expect("the newer build opens the older shared cache");
        diesel::insert_into(vocabulary_store::table)
            .values((
                vocabulary_store::id.eq(3),
                vocabulary_store::word.eq("after"),
            ))
            .execute(&mut *conn.conn())
            .expect("bob writes the upgraded shared store");
    }

    // Carol, the second account to open at the upgraded major, reads the
    // shared rows written after the upgrade through the logical reader.
    let mut conn = open(&storage, "rmaj-late-carol", &key(0x44), v2.clone())
        .await
        .expect("the second account opens at the upgraded major");
    let rows: Vec<(i32, String)> = vocabulary::table
        .select((vocabulary::id, vocabulary::word))
        .order(vocabulary::id)
        .load(&mut *conn.conn())
        .expect("the reader serves the shared rows");
    assert_eq!(
        rows,
        vec![(3, "after".to_owned())],
        "the rows written after the upgrade are the rows the second account retains"
    );
    assert_eq!(user_version(conn.conn()), 2);
    assert_eq!(recorded_version(conn.conn()), v2.version().hash());

    drop(conn);
    clear(
        &storage,
        &[
            alice.as_str(),
            bob.as_str(),
            carol.as_str(),
            shared_record().as_str(),
        ],
    )
    .await;
}

/// The shared file at a newer major refuses the older build, even a fresh
/// account whose own file has no record yet, and the refusal stamps neither
/// file.
#[wasm_bindgen_test]
async fn an_older_build_refuses_a_newer_shared_file_even_for_a_fresh_account() {
    let storage = ReplicaStorage::install().await;
    let alice = replica_name("rmaj-newer-alice");
    let bob = replica_name("rmaj-newer-bob");
    clear(
        &storage,
        &[alice.as_str(), bob.as_str(), shared_record().as_str()],
    )
    .await;

    let v1 = bundle(1, ORDERS_V1);
    let v2 = bundle(2, ORDERS_V2);

    // Alice's run at the newer major stamps the shared file, and leaves a
    // shared row and a queued write beside it.
    let queued: Vec<UnsyncedWrite> = {
        let mut conn = open(&storage, "rmaj-newer-alice", &key(0x11), v2.clone())
            .await
            .expect("the newer build opens");
        diesel::insert_into(vocabulary_store::table)
            .values((
                vocabulary_store::id.eq(1),
                vocabulary_store::word.eq("kept"),
            ))
            .execute(&mut *conn.conn())
            .expect("alice writes the shared store");
        diesel::insert_into(orders::table)
            .values((orders::id.eq(1), orders::label.eq("kept")))
            .execute(&mut *conn.conn())
            .expect("write offline");
        conn.push().await.expect("queue").expect("a seq");
        conn.unsynced()
    };

    // A fresh account at the older build opens against the advanced shared
    // file, and is refused before either file changes.
    match open(&storage, "rmaj-newer-bob", &key(0x22), v1.clone()).await {
        Err(ClientError::ReplicaNewer { recorded, build }) => {
            assert_eq!(recorded, Major::new(2).expect("two is a major"));
            assert_eq!(build, Major::new(1).expect("one is a major"));
        }
        Err(other) => panic!("expected a newer-replica refusal, got {other:?}"),
        Ok(_) => panic!("an older build opened a newer shared file"),
    }

    // The fresh file the refusal left behind carries no record at all. The
    // DSL cannot probe a schema's catalogue, so the raw count stays.
    {
        let mut raw = raw_open(&storage, &bob, &key(0x22));
        assert_eq!(user_version(&mut raw), 0);
        let tables: CountRow = diesel::sql_query(
            "SELECT count(*) AS count FROM main.sqlite_master WHERE type = 'table'",
        )
        .get_result(&mut raw)
        .expect("read the catalogue");
        assert_eq!(tables.count, 0, "the refusal wrote nothing into bob's file");
    }

    // And alice's file serves her shared row, her queue and her record at
    // the current build.
    let mut conn = open(&storage, "rmaj-newer-alice", &key(0x11), v2.clone())
        .await
        .expect("the current build reopens");
    let word: String = vocabulary_store::table
        .select(vocabulary_store::word)
        .first(&mut *conn.conn())
        .expect("read the shared row");
    assert_eq!(word, "kept");
    assert_eq!(conn.unsynced(), queued);
    assert_eq!(user_version(conn.conn()), 2);
    assert_eq!(recorded_version(conn.conn()), v2.version().hash());

    drop(conn);
    clear(
        &storage,
        &[alice.as_str(), bob.as_str(), shared_record().as_str()],
    )
    .await;
}

/// The shared file at this major from another source refuses the open, and
/// the refusal leaves the record the first run stamped still serving.
#[wasm_bindgen_test]
async fn a_same_major_shared_hash_mismatch_is_refused_without_mutation() {
    let storage = ReplicaStorage::install().await;
    let alice = replica_name("rmaj-mismatch-alice");
    let bob = replica_name("rmaj-mismatch-bob");
    clear(
        &storage,
        &[alice.as_str(), bob.as_str(), shared_record().as_str()],
    )
    .await;

    let a = bundle(1, ORDERS_V1);
    let b = bundle(1, ORDERS_V1B);
    let a_version = a.version();
    let b_version = b.version();
    assert_ne!(
        a_version, b_version,
        "the same major from another source is another build"
    );

    let queued: Vec<UnsyncedWrite> = {
        let mut conn = open(&storage, "rmaj-mismatch-alice", &key(0x11), a.clone())
            .await
            .expect("the first build opens");
        diesel::insert_into(vocabulary_store::table)
            .values((
                vocabulary_store::id.eq(1),
                vocabulary_store::word.eq("kept"),
            ))
            .execute(&mut *conn.conn())
            .expect("alice writes the shared store");
        diesel::insert_into(orders::table)
            .values((orders::id.eq(1), orders::label.eq("kept")))
            .execute(&mut *conn.conn())
            .expect("write offline");
        conn.push().await.expect("queue").expect("a seq");
        conn.unsynced()
    };

    match open(&storage, "rmaj-mismatch-bob", &key(0x22), b.clone()).await {
        Err(ClientError::ReplicaSchemaChanged {
            major,
            recorded,
            build,
        }) => {
            assert_eq!(major, Major::new(1).expect("one is a major"));
            assert_eq!(recorded, a_version);
            assert_eq!(build, b_version);
        }
        Err(other) => panic!("expected a schema-hash refusal, got {other:?}"),
        Ok(_) => panic!("a different build at the same major opened the shared cache"),
    }

    let mut conn = open(&storage, "rmaj-mismatch-alice", &key(0x11), a.clone())
        .await
        .expect("the first build reopens");
    let word: String = vocabulary_store::table
        .select(vocabulary_store::word)
        .first(&mut *conn.conn())
        .expect("read the shared row");
    assert_eq!(word, "kept");
    assert_eq!(conn.unsynced(), queued);
    assert_eq!(user_version(conn.conn()), 1);
    assert_eq!(recorded_version(conn.conn()), a.version().hash());

    drop(conn);
    clear(
        &storage,
        &[alice.as_str(), bob.as_str(), shared_record().as_str()],
    )
    .await;
}

/// Replacing a cache invalidates resume state without losing account work or pins.
#[wasm_bindgen_test]
async fn a_replaced_shared_cache_clears_resume_state_but_keeps_pending_work() {
    let storage = ReplicaStorage::install().await;
    let alice = replica_name("rmaj-gen-alice");
    clear(&storage, &[alice.as_str(), shared_record().as_str()]).await;

    let v1 = bundle(1, ORDERS_V1);
    let v1_version = v1.version();
    let (queued, row) = seeded_run(&storage, "rmaj-gen-alice", &v1, false).await;

    // The shared cache is replaced while alice's run is off: her file keeps
    // its record, the device-wide file goes.
    storage
        .delete_db(&shared_record())
        .expect("replace the shared cache");

    let mut conn = open(&storage, "rmaj-gen-alice", &key(0x11), v1.clone())
        .await
        .expect("reopen after the replacement");
    assert!(
        !conn.has_ever_synced(),
        "the new lifetime invalidates the account's resume state"
    );
    let meta_row: Option<(Vec<u8>, Option<Vec<u8>>)> = meta::table
        .select((meta::cursor, meta::cursor_subjects))
        .first(&mut *conn.conn())
        .optional()
        .expect("read the metadata");
    assert!(
        meta_row
            .as_ref()
            .is_none_or(|(cursor, _)| cursor.is_empty()),
        "the replay cursor is cleared: {meta_row:?}"
    );
    assert_eq!(
        aggregates::table
            .count()
            .get_result::<i64>(&mut *conn.conn())
            .expect("count the rested values"),
        0,
        "the rested statistic is cleared"
    );
    let subs: Vec<(String, Option<String>)> = subscription::table
        .select((subscription::id, subscription::pin_name))
        .load(&mut *conn.conn())
        .expect("read the registrations");
    assert!(
        subs.contains(&("resume".to_owned(), Some("pinned".to_owned()))),
        "the registered spec keeps its named pin: {subs:?}"
    );
    assert!(
        subs.iter().any(|(id, _)| id == "statistic"),
        "the computed spec is registered still: {subs:?}"
    );
    let specs: Vec<String> = query::table
        .select(query::sql)
        .load(&mut *conn.conn())
        .expect("read the query specs");
    assert!(
        specs.iter().any(|sql| sql.contains("count(*)")),
        "the computed spec's SQL is registered still: {specs:?}"
    );
    assert_eq!(
        conn.unsynced(),
        queued,
        "the queued write keeps its writer, sequence and major"
    );
    assert_eq!(
        queued_row(&mut *conn.conn()),
        row,
        "the held bytes are untouched"
    );
    assert_eq!(user_version(conn.conn()), 1);
    assert_eq!(recorded_version(conn.conn()), v1_version.hash());
    // The shared store is back, fresh under the device key.
    diesel::insert_into(vocabulary_store::table)
        .values((
            vocabulary_store::id.eq(9),
            vocabulary_store::word.eq("rebuilt"),
        ))
        .execute(&mut *conn.conn())
        .expect("write the rebuilt shared store");
    let word: String = vocabulary_store::table
        .select(vocabulary_store::word)
        .first(&mut *conn.conn())
        .expect("read it back");
    assert_eq!(word, "rebuilt");

    drop(conn);
    clear(&storage, &[alice.as_str(), shared_record().as_str()]).await;
}

/// A reopen at the same records and lifetime keeps the account's replay
/// cursor, rested statistic, registrations and queued work, and the shared
/// rows the previous run wrote.
#[wasm_bindgen_test]
async fn a_same_generation_reopen_keeps_the_accounts_resume_state() {
    let storage = ReplicaStorage::install().await;
    let alice = replica_name("rmaj-keep-alice");
    clear(&storage, &[alice.as_str(), shared_record().as_str()]).await;

    let v1 = bundle(1, ORDERS_V1);
    let v1_version = v1.version();
    let (queued, row) = seeded_run(&storage, "rmaj-keep-alice", &v1, true).await;

    let mut conn = open(&storage, "rmaj-keep-alice", &key(0x11), v1.clone())
        .await
        .expect("reopen the same build");
    assert!(
        conn.has_ever_synced(),
        "the same lifetime keeps the account's resume state"
    );
    let cursor: Vec<u8> = meta::table
        .select(meta::cursor)
        .first(&mut *conn.conn())
        .expect("read the replay cursor");
    assert_eq!(
        cursor,
        RESUME_CURSOR.to_vec(),
        "the replay cursor survives the reopen"
    );
    let rested: String = aggregates::table
        .select(aggregates::result_json)
        .first(&mut *conn.conn())
        .expect("read the rested value");
    assert_eq!(rested, "42", "the rested statistic survives the reopen");
    let subs: Vec<(String, Option<String>)> = subscription::table
        .select((subscription::id, subscription::pin_name))
        .load(&mut *conn.conn())
        .expect("read the registrations");
    assert!(
        subs.contains(&("resume".to_owned(), Some("pinned".to_owned()))),
        "the registered spec keeps its named pin: {subs:?}"
    );
    assert!(
        subs.iter().any(|(id, _)| id == "statistic"),
        "the computed spec is registered still: {subs:?}"
    );
    assert_eq!(
        conn.unsynced(),
        queued,
        "the queued write survives the reopen"
    );
    assert_eq!(
        queued_row(&mut *conn.conn()),
        row,
        "the held bytes are untouched"
    );
    let word: String = vocabulary::table
        .select(vocabulary::word)
        .first(&mut *conn.conn())
        .expect("read the shared row through the reader");
    assert_eq!(word, "kept");
    assert_eq!(user_version(conn.conn()), 1);
    assert_eq!(recorded_version(conn.conn()), v1_version.hash());

    drop(conn);
    clear(&storage, &[alice.as_str(), shared_record().as_str()]).await;
}
