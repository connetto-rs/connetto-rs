//! Account and device-wide cache upgrades preserve durable work.

use connetto_client::{
    ClientBuilder, ClientError, ConnettoConnection, DataDir, MemoryRefreshStore, SyncSchema,
};
use connetto_core::schema::{SchemaBundle, SharedLayout};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::ReplicaKeyStore;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

use super::support::{NeverDial, SharedKeys, held};

type Client = ConnettoConnection<FakeTransport>;

const NOTES: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, label TEXT NOT NULL);";
const WORDS: &str =
    "CREATE TABLE connetto_shared.vocabulary_store (id INTEGER PRIMARY KEY, label TEXT NOT NULL);";
const READER: &str =
    "CREATE TEMP VIEW vocabulary AS SELECT * FROM connetto_shared.vocabulary_store;";

fn schema(major: u32) -> SyncSchema {
    SyncSchema::new(
        SchemaBundle::new(
            format!("{} {major}\n{NOTES}", connetto_core::MAJOR_DIRECTIVE),
            "",
            format!(
                "{NOTES} CREATE TABLE vocabulary_store (id INTEGER PRIMARY KEY, label TEXT NOT NULL); CREATE VIEW vocabulary AS SELECT * FROM vocabulary_store;"
            ),
            [("vocabulary", "vocabulary_store")],
            ["vocabulary"],
            None::<&str>,
        )
        .unwrap()
        .with_shared_tables(["vocabulary"])
        .with_shared_layout(SharedLayout::new(
            format!("{NOTES} {WORDS} {READER}"),
            format!("{NOTES} {READER}"),
            format!("{WORDS} {READER}"),
            READER,
        )),
    )
}

async fn open(
    directory: &std::path::Path,
    keys: &SharedKeys,
    owner: &str,
    schema: SyncSchema,
) -> Result<Client, ClientError> {
    ClientBuilder::new(schema, NeverDial::<FakeTransport>::default())
        .signed_in(held(owner))
        .durable(
            DataDir::new(directory),
            keys.clone(),
            MemoryRefreshStore::default(),
        )
        .open_driven()
        .await
}

#[derive(Clone, Copy, Debug)]
enum State {
    Fresh,
    Unrecorded,
    Current,
    Behind,
    HashMismatch,
    Newer,
    Missing,
}

impl State {
    fn refuses(self) -> bool {
        matches!(self, Self::HashMismatch | Self::Newer | Self::Missing)
    }
}

const STATES: [State; 7] = [
    State::Fresh,
    State::Unrecorded,
    State::Current,
    State::Behind,
    State::HashMismatch,
    State::Newer,
    State::Missing,
];

fn set_state(db: &mut SqliteConnection, store: &str, state: State, old_hash: &[u8]) {
    match state {
        State::Fresh => db.batch_execute(
            "DROP TABLE connetto_shared.vocabulary_store; DROP TABLE connetto_shared._connetto_build; PRAGMA connetto_shared.user_version = 0",
        ).unwrap(),
        State::Unrecorded => db.batch_execute(&format!(
            "DROP TABLE {store}._connetto_build; PRAGMA {store}.user_version = 0",
        )).unwrap(),
        State::Current => {},
        State::Behind => {
            diesel::sql_query(format!("UPDATE {store}._connetto_build SET version = ?"))
                .bind::<diesel::sql_types::Binary, _>(old_hash)
                .execute(db).unwrap();
            db.batch_execute(&format!("PRAGMA {store}.user_version = 1")).unwrap();
            if store == "main" {
                db.batch_execute("UPDATE _connetto_pending SET major = 1").unwrap();
            }
        },
        State::HashMismatch => db.batch_execute(&format!(
            "UPDATE {store}._connetto_build SET version = X'01'",
        )).unwrap(),
        State::Newer => db.batch_execute(&format!("PRAGMA {store}.user_version = 3")).unwrap(),
        State::Missing => db.batch_execute(&format!("DROP TABLE {store}._connetto_build")).unwrap(),
    }
}

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    text: String,
}

fn texts(db: &mut SqliteConnection, sql: impl Into<String>) -> Vec<String> {
    diesel::sql_query(sql)
        .load::<TextRow>(db)
        .unwrap()
        .into_iter()
        .map(|row| row.text)
        .collect()
}

#[derive(QueryableByName)]
struct Number {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

fn number(db: &mut SqliteConnection, sql: impl Into<String>) -> i64 {
    diesel::sql_query(sql)
        .get_result::<Number>(db)
        .unwrap()
        .value
}

fn count(db: &mut SqliteConnection, table: &str) -> i64 {
    number(db, format!("SELECT COUNT(*) AS value FROM {table}"))
}

#[derive(QueryableByName, Debug, PartialEq, Eq)]
struct Pending {
    #[diesel(sql_type = diesel::sql_types::Text)]
    writer: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    seq: i64,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    changeset: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    entry_hash: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    major: Option<i64>,
}

fn pending(db: &mut SqliteConnection) -> Vec<Pending> {
    diesel::sql_query(
        "SELECT writer, seq, changeset, entry_hash, major FROM _connetto_pending ORDER BY position",
    )
    .load(db)
    .unwrap()
}

fn has_table(db: &mut SqliteConnection, store: &str, table: &str) -> bool {
    number(
        db,
        format!(
            "SELECT COUNT(*) AS value FROM {store}.sqlite_master WHERE type = 'table' AND name = '{table}'",
        ),
    ) != 0
}

fn snapshot(db: &mut SqliteConnection, store: &str) -> Vec<String> {
    #[derive(QueryableByName)]
    struct Version {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        user_version: i32,
    }
    let mut rows = texts(
        db,
        format!(
            "SELECT type || ' ' || name || ' ' || COALESCE(sql, '') AS text FROM {store}.sqlite_master ORDER BY type, name",
        ),
    );
    let major = diesel::sql_query(format!("PRAGMA {store}.user_version"))
        .get_result::<Version>(db)
        .unwrap()
        .user_version;
    rows.push(format!("major {major}"));
    for (table, projection) in [
        ("notes", "id || ' ' || label"),
        ("vocabulary_store", "id || ' ' || label"),
        ("_connetto_build", "hex(version)"),
        (
            "_connetto_meta",
            "hex(cursor) || ' ' || COALESCE(hex(cursor_subjects), '')",
        ),
        ("_connetto_shared_generation", "hex(generation)"),
        (
            "_connetto_writers",
            "id || ' ' || next_seq || ' ' || hex(head)",
        ),
        (
            "_connetto_pending",
            "writer || ' ' || seq || ' ' || hex(changeset) || ' ' || hex(entry_hash) || ' ' || major",
        ),
    ] {
        if has_table(db, store, table) {
            let mut data = texts(
                db,
                format!("SELECT {projection} AS text FROM {store}.{table}"),
            );
            data.sort_unstable();
            rows.push(format!("{table} {data:?}"));
        }
    }
    if store == "connetto_shared" && has_table(db, store, "_connetto_build") {
        rows.extend(texts(
            db,
            "SELECT hex(generation) AS text FROM connetto_shared._connetto_build",
        ));
    }
    rows
}

async fn raw(
    directory: &std::path::Path,
    keys: &SharedKeys,
    owner: &str,
    shared: bool,
) -> SqliteConnection {
    let credential = held(owner);
    let name = credential.replica_name();
    let key = keys.load(name).await.unwrap().unwrap();
    let mut db = SqliteConnection::establish(directory.join(name).to_str().unwrap()).unwrap();
    connetto_client::cipher::unlock(&mut db, &key).unwrap();
    if shared {
        let name = connetto_client::shared_store_name(connetto_client::REPLICA_PREFIX);
        let key = keys.load(&name).await.unwrap().unwrap();
        connetto_client::harden::attach_keyed_in_window(
            &mut db,
            directory.join(&name).to_str().unwrap(),
            "connetto_shared",
            connetto_client::harden::AttachPermits::Write,
            &key,
        )
        .unwrap();
    }
    db
}

/// Both stores' contents before an open, for a refusal to leave untouched.
struct Before {
    main: Option<Vec<String>>,
    shared: Option<Vec<String>>,
}

/// Assert `error` names the record `refusal` broke and both stores kept `before`.
async fn assert_refused_unchanged(
    directory: &std::path::Path,
    keys: &SharedKeys,
    refusal: State,
    error: ClientError,
    before: Before,
    label: &str,
) {
    assert!(
        matches!(
            (refusal, error),
            (
                State::HashMismatch,
                ClientError::ReplicaSchemaChanged { .. }
            ) | (State::Newer, ClientError::ReplicaNewer { .. })
                | (State::Missing, ClientError::ReplicaRecordMissing { .. })
        ),
        "wrong refusal for {label}"
    );
    let mut db = raw(directory, keys, "account", before.shared.is_some()).await;
    match before.main {
        Some(main) => assert_eq!(snapshot(&mut db, "main"), main, "{label}"),
        None => assert_eq!(snapshot(&mut db, "main"), ["major 0"]),
    }
    if let Some(shared) = before.shared {
        assert_eq!(snapshot(&mut db, "connetto_shared"), shared, "{label}");
    }
}

/// Open `owner` at `built`, queue one private write, then add a cached word
/// and a resume cursor behind the client's back, returning the raw files.
async fn seed(
    directory: &std::path::Path,
    keys: &SharedKeys,
    owner: &str,
    built: SyncSchema,
    shared: bool,
) -> SqliteConnection {
    let mut client = open(directory, keys, owner, built).await.unwrap();
    client
        .conn()
        .batch_execute("INSERT INTO notes VALUES (1, 'private')")
        .unwrap();
    client.push().await.unwrap().unwrap();
    drop(client);
    let words = if shared {
        "connetto_shared.vocabulary_store"
    } else {
        "vocabulary_store"
    };
    let mut db = raw(directory, keys, owner, shared).await;
    db.batch_execute(&format!("INSERT INTO {words} VALUES (1, 'cached')"))
        .unwrap();
    db.batch_execute("INSERT INTO _connetto_meta (id, cursor) VALUES (1, X'0102')")
        .unwrap();
    db
}

#[tokio::test]
async fn every_account_and_shared_record_combination_preserves_work_or_refuses_without_mutation() {
    let current = schema(2);
    let previous = schema(1);
    for account in STATES {
        for shared in std::iter::once(None).chain(STATES.into_iter().map(Some)) {
            let directory = tempfile::tempdir().unwrap();
            let keys = SharedKeys::default();
            let built = current.clone().with_shared_store(shared.is_some());
            let owner = if matches!(account, State::Fresh) {
                "seed"
            } else {
                "account"
            };
            let mut db = seed(
                directory.path(),
                &keys,
                owner,
                built.clone(),
                shared.is_some(),
            )
            .await;
            if !matches!(account, State::Fresh) {
                set_state(&mut db, "main", account, previous.version().hash());
            }
            if let Some(state) = shared {
                set_state(&mut db, "connetto_shared", state, previous.version().hash());
            }
            let before_main = (!matches!(account, State::Fresh)).then(|| snapshot(&mut db, "main"));
            let before_shared = shared.map(|_| snapshot(&mut db, "connetto_shared"));
            let before_pending = pending(&mut db);
            let before_writers = texts(
                &mut db,
                "SELECT id || ' ' || next_seq || ' ' || hex(head) AS text FROM _connetto_writers ORDER BY id",
            );
            drop(db);
            let refused = if account.refuses() {
                Some(account)
            } else {
                shared.filter(|state| state.refuses())
            };
            let result = open(directory.path(), &keys, "account", built).await;
            let label = format!("account {account:?}, shared {shared:?}");
            if let Some(refusal) = refused {
                let Err(error) = result else {
                    panic!("opened refused {label}");
                };
                let before = Before {
                    main: before_main,
                    shared: before_shared,
                };
                assert_refused_unchanged(directory.path(), &keys, refusal, error, before, &label)
                    .await;
                continue;
            }
            let mut client = result.unwrap_or_else(|error| panic!("{label}: {error}"));
            let fresh_main = matches!(account, State::Fresh | State::Behind);
            let fresh_shared = matches!(shared, Some(State::Fresh | State::Behind));
            assert_eq!(
                count(client.conn(), "notes"),
                i64::from(!fresh_main),
                "{label}"
            );
            let kept_words = if shared.is_some() {
                !fresh_shared
            } else {
                !fresh_main
            };
            assert_eq!(
                count(client.conn(), "vocabulary"),
                i64::from(kept_words),
                "{label}"
            );
            if matches!(account, State::Fresh) {
                assert_eq!(client.unsynced(), [] as [connetto_client::UnsyncedWrite; 0]);
            } else {
                assert_eq!(pending(client.conn()), before_pending, "{label}");
                assert_eq!(
                    texts(
                        client.conn(),
                        "SELECT id || ' ' || next_seq || ' ' || hex(head) AS text FROM _connetto_writers ORDER BY id"
                    ),
                    before_writers
                );
            }
            let resume_kept = !fresh_main
                && !matches!(
                    shared,
                    Some(State::Fresh | State::Unrecorded | State::Behind)
                );
            assert_eq!(
                count(client.conn(), "_connetto_meta"),
                i64::from(resume_kept),
                "{label}"
            );
        }
    }
}

#[tokio::test]
async fn another_accounts_upgrade_preserves_queued_writes_and_rebuilds_the_shared_cache_once() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut alice = open(directory.path(), &keys, "alice", schema(1))
        .await
        .unwrap();
    alice.conn().batch_execute("INSERT INTO notes VALUES (1, 'alice pending'); INSERT INTO connetto_shared.vocabulary_store VALUES (1, 'old cache')").unwrap();
    let first_seq = alice.push().await.unwrap().unwrap();
    let queued = pending(alice.conn());
    drop(alice);
    let mut bob = open(directory.path(), &keys, "bob", schema(1))
        .await
        .unwrap();
    bob.conn()
        .batch_execute("INSERT INTO notes VALUES (2, 'bob pending')")
        .unwrap();
    bob.push().await.unwrap().unwrap();
    let bob_queued = pending(bob.conn());
    drop(bob);
    let mut bob = open(directory.path(), &keys, "bob", schema(2))
        .await
        .unwrap();
    assert_eq!(pending(bob.conn()), bob_queued);
    assert_eq!(count(bob.conn(), "vocabulary"), 0);
    bob.conn()
        .batch_execute("INSERT INTO connetto_shared.vocabulary_store VALUES (2, 'new cache')")
        .unwrap();
    let generation = texts(
        bob.conn(),
        "SELECT hex(generation) AS text FROM connetto_shared._connetto_build",
    );
    drop(bob);
    {
        let mut raw_alice = raw(directory.path(), &keys, "alice", false).await;
        assert_eq!(pending(&mut raw_alice), queued);
    }
    let mut alice = open(directory.path(), &keys, "alice", schema(2))
        .await
        .unwrap();
    assert_eq!(pending(alice.conn()), queued);
    assert_eq!(
        texts(
            alice.conn(),
            "SELECT label AS text FROM vocabulary ORDER BY id"
        ),
        ["new cache"]
    );
    assert_eq!(
        texts(
            alice.conn(),
            "SELECT hex(generation) AS text FROM connetto_shared._connetto_build"
        ),
        generation
    );
    alice
        .conn()
        .batch_execute("INSERT INTO notes VALUES (3, 'next major')")
        .unwrap();
    let next_seq = alice.push().await.unwrap().unwrap();
    assert_eq!(next_seq, first_seq + 1);
    assert_eq!(
        pending(alice.conn())
            .iter()
            .map(|entry| entry.major)
            .collect::<Vec<_>>(),
        [Some(1), Some(2)]
    );
}

#[tokio::test]
async fn replacing_a_current_major_cache_invalidates_resume_but_keeps_private_work_and_pins() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut client = open(directory.path(), &keys, "alice", schema(2))
        .await
        .unwrap();
    client
        .conn()
        .batch_execute("INSERT INTO notes VALUES (1, 'pending')")
        .unwrap();
    client.push().await.unwrap().unwrap();
    client
        .subscribe("words", "SELECT * FROM vocabulary")
        .await
        .unwrap();
    let spec = client
        .declared_subscriptions()
        .unwrap()
        .into_iter()
        .find(|(id, _)| id == "words")
        .unwrap()
        .1;
    client
        .pin_subscription("dictionary", "words", &spec)
        .unwrap();
    client
        .conn()
        .batch_execute("INSERT INTO _connetto_meta (id, cursor) VALUES (1, X'0102')")
        .unwrap();
    let queued = pending(client.conn());
    let original_generation = texts(
        client.conn(),
        "SELECT hex(generation) AS text FROM connetto_shared._connetto_build",
    );
    drop(client);
    let mut client = open(directory.path(), &keys, "alice", schema(2))
        .await
        .unwrap();
    assert_eq!(count(client.conn(), "_connetto_meta"), 1);
    assert_eq!(
        texts(
            client.conn(),
            "SELECT hex(generation) AS text FROM connetto_shared._connetto_build"
        ),
        original_generation
    );
    set_state(client.conn(), "connetto_shared", State::Fresh, &[]);
    drop(client);
    let mut client = open(directory.path(), &keys, "alice", schema(2))
        .await
        .unwrap();
    assert_eq!(count(client.conn(), "_connetto_meta"), 0);
    assert_eq!(count(client.conn(), "notes"), 1);
    assert_eq!(pending(client.conn()), queued);
    assert_eq!(
        client.pins().unwrap(),
        [(
            "dictionary".to_owned(),
            "SELECT * FROM vocabulary".to_owned()
        )]
    );
    assert_ne!(
        texts(
            client.conn(),
            "SELECT hex(generation) AS text FROM connetto_shared._connetto_build"
        ),
        original_generation
    );
}

#[tokio::test]
async fn a_failed_layout_rolls_back_both_records_and_both_files_data() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut client = open(directory.path(), &keys, "alice", schema(1))
        .await
        .unwrap();
    client.conn().batch_execute("INSERT INTO notes VALUES (1, 'pending'); INSERT INTO connetto_shared.vocabulary_store VALUES (1, 'cached')").unwrap();
    client.push().await.unwrap().unwrap();
    let before_main = snapshot(client.conn(), "main");
    let before_shared = snapshot(client.conn(), "connetto_shared");
    drop(client);
    let broken = SyncSchema::new(
        SchemaBundle::new(
            "-- connetto:major 2",
            "",
            NOTES,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .unwrap()
        .with_shared_tables(["vocabulary"])
        .with_shared_layout(SharedLayout::new(
            "SELECT missing_layout_function()",
            "",
            "",
            "",
        )),
    );
    assert!(matches!(
        open(directory.path(), &keys, "alice", broken).await,
        Err(ClientError::Db(_))
    ));
    let mut db = raw(directory.path(), &keys, "alice", true).await;
    assert_eq!(snapshot(&mut db, "main"), before_main);
    assert_eq!(snapshot(&mut db, "connetto_shared"), before_shared);
}

#[tokio::test]
async fn a_pre_major_queue_is_adopted_without_losing_its_bytes_or_writer_frontier() {
    let directory = tempfile::tempdir().unwrap();
    let keys = SharedKeys::default();
    let mut client = open(directory.path(), &keys, "alice", schema(1))
        .await
        .unwrap();
    client
        .conn()
        .batch_execute(
            "INSERT INTO notes VALUES (1, 'legacy pending'); \
         INSERT INTO connetto_shared.vocabulary_store VALUES (1, 'legacy cache')",
        )
        .unwrap();
    let first_seq = client.push().await.unwrap().unwrap();
    let mut original = pending(client.conn());
    original[0].major = None;
    client
        .conn()
        .batch_execute(
            "PRAGMA main.user_version = 0; \
         DROP TABLE _connetto_build; \
         DROP TABLE _connetto_build_renames; \
         DROP TABLE _connetto_shared_generation; \
         ALTER TABLE _connetto_pending DROP COLUMN major; \
         PRAGMA connetto_shared.user_version = 0; \
         DROP TABLE connetto_shared._connetto_build",
        )
        .unwrap();
    drop(client);
    let mut client = open(directory.path(), &keys, "alice", schema(1))
        .await
        .unwrap();
    assert_eq!(pending(client.conn()), original);
    assert_eq!(
        texts(client.conn(), "SELECT label AS text FROM notes"),
        ["legacy pending"]
    );
    assert_eq!(
        texts(client.conn(), "SELECT label AS text FROM vocabulary"),
        ["legacy cache"]
    );
    client
        .conn()
        .batch_execute("INSERT INTO notes VALUES (2, 'new pending')")
        .unwrap();
    assert_eq!(client.push().await.unwrap().unwrap(), first_seq + 1);
    assert_eq!(
        pending(client.conn())
            .iter()
            .map(|entry| entry.major)
            .collect::<Vec<_>>(),
        [None, Some(1)],
    );
}
