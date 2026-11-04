//! Shared fixtures and helpers for the writer write-path test modules.

use std::sync::Arc;
use std::time::SystemTime;

use connetto_core::SessionId;
use connetto_core::device_cert::{AttestationLevel, KeyId};
use connetto_server::RuntimeWritableCatalog;
use connetto_server::defaults::_connetto_writers;
use connetto_server::device_cert::{
    DeviceCertConfig, DeviceEnrolment, Enrolment, EnrolmentStore, MemoryEnrolments,
};
use connetto_test_harness::{Fixture, provision_writers};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use rcgen::{KeyPair, PublicKeyData as _};
use sqlite_diff_rs::{ChangeSet, ChangesetFormat, DiffOps, Insert, SimpleTable, Update, Value};

use super::enrolment::{device_key, rooted_issuer};

/// The notes table these tests sync.
pub(super) const PG_DDL: &str =
    "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, edited_at TEXT);";

diesel::table! {
    /// Row from the notes test fixture.
    notes (id) {
        /// Note identifier, the primary key.
        id -> diesel::sql_types::Integer,
        /// Note text.
        body -> diesel::sql_types::Text,
        /// Timestamp of the last edit.
        edited_at -> diesel::sql_types::Text,
    }
}

/// Reset the fixture to a fresh `notes` table seeded with one versioned row
/// and the writer tables provisioned by the admin.
pub(super) async fn seed_notes(fixture: &Fixture) {
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS notes CASCADE",
            "DROP TABLE IF EXISTS _connetto_settled",
            "DROP TABLE IF EXISTS _connetto_session_writers",
            "DROP TABLE IF EXISTS _connetto_writers",
            "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, edited_at TEXT)",
        ])
        .await;
    provision_writers(fixture.admin()).await;
    let mut conn = fixture.admin().get().await.expect("admin connection");
    diesel::insert_into(notes::table)
        .values((
            notes::id.eq(1_i32),
            notes::body.eq("hello"),
            notes::edited_at.eq("t0"),
        ))
        .execute(&mut *conn)
        .await
        .expect("seed row");
}

/// The notes table's writable catalog, versioned on `edited_at`.
pub(super) fn writable_catalog() -> RuntimeWritableCatalog {
    RuntimeWritableCatalog::builder()
        .versioned("notes", "edited_at")
        .build()
}

/// The notes table as the changesets spell it.
pub(super) fn note_table() -> SimpleTable {
    SimpleTable::new(
        "notes",
        &[("id", ""), ("body", ""), ("edited_at", "")],
        &[0],
    )
}

/// A changeset that inserts one full row.
pub(super) fn insert_changeset(id: i64, body: &str, edited_at: &str) -> Vec<u8> {
    let insert = Insert::<_, String, Vec<u8>>::from(note_table())
        .set(0, Value::Integer(id))
        .expect("set id")
        .set(1, Value::Text(body.to_owned()))
        .expect("set body")
        .set(2, Value::Text(edited_at.to_owned()))
        .expect("set edited_at");
    ChangeSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(insert)
        .build()
}

/// A changeset that updates one row, carrying the old image (the version basis).
pub(super) fn update_changeset(
    id: i64,
    old_body: &str,
    new_body: &str,
    old_edited_at: &str,
    new_edited_at: &str,
) -> Vec<u8> {
    let update = Update::<_, ChangesetFormat, String, Vec<u8>>::from(note_table())
        .set(0, Value::Integer(id), Value::Integer(id))
        .expect("set id")
        .set(
            1,
            Value::Text(old_body.to_owned()),
            Value::Text(new_body.to_owned()),
        )
        .expect("set body")
        .set(
            2,
            Value::Text(old_edited_at.to_owned()),
            Value::Text(new_edited_at.to_owned()),
        )
        .expect("set edited_at");
    ChangeSet::<SimpleTable, String, Vec<u8>>::new()
        .update(update)
        .build()
}

/// The writer number a device key bound to.
pub(super) async fn writer_for_key(pool: &Pool<AsyncPgConnection>, key_id: [u8; 32]) -> i64 {
    let mut conn = pool.get().await.expect("admin connection");
    _connetto_writers::table
        .filter(_connetto_writers::device_key.eq(key_id.to_vec()))
        .select(_connetto_writers::writer)
        .get_result(&mut *conn)
        .await
        .expect("read the device writer")
}

/// The device key enrolments the tests install on their managers, in memory.
pub(super) struct Enrolments {
    pub(super) store: Arc<MemoryEnrolments<String>>,
}

impl Enrolments {
    /// An empty store, fresh per test.
    pub(super) fn new() -> Self {
        Self {
            store: Arc::new(MemoryEnrolments::default()),
        }
    }

    /// Enrol `user`'s fresh key with a certificate that stands until
    /// `expires_at`, and hand back the key and its id.
    pub(super) async fn enrol(&self, user: &str, expires_at: SystemTime) -> (KeyPair, [u8; 32]) {
        let key = device_key();
        let key_id = *KeyId::of_public_key(&key.subject_public_key_info()).as_bytes();
        let record = Enrolment {
            user: user.to_owned(),
            key: KeyId::from_bytes(key_id),
            serial: [7; 16],
            issuer: KeyId::from_bytes([9; 32]),
            issued_at: SystemTime::now(),
            expires_at,
            session: SessionId::from_uuid(uuid::Uuid::new_v4()),
            public_key: key.subject_public_key_info().clone(),
            descriptor: vec![0x91, 0x01],
            attestation: AttestationLevel::Unproven,
        };
        self.store
            .record(record)
            .await
            .expect("record the certificate");
        (key, key_id)
    }

    /// Revoke `key_id` whoever enrolled it, as an operator would.
    pub(super) fn revoke(&self, key_id: [u8; 32]) {
        self.store.revoke_key(KeyId::from_bytes(key_id));
    }

    /// The enrolment the manager takes, over this store and a fresh issuer.
    pub(super) fn installable(&self) -> DeviceEnrolment<String> {
        let (_, _, issuer) = rooted_issuer();
        let store = Arc::clone(&self.store);
        let store: Arc<dyn EnrolmentStore<String>> = store;
        DeviceEnrolment::new(DeviceCertConfig::new(issuer), store)
    }
}
