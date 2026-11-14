//! Needs Docker: the fixture starts its own Postgres.
//!
//! The Postgres enrolment store over tables `connetto_schema!` names (R74
//! decisions 17, 26, 27 and 28): the decisions it takes, the descriptor it
//! keeps in typed columns, the serials a list names and the list numbers.

use std::time::{Duration, SystemTime};

use connetto_core::SessionId;
use connetto_core::device_cert::{AttestationLevel, KeyId};
use connetto_server::device_cert::{Enrolment, EnrolmentStore, Recorded, Revocation};
use connetto_test_harness::Fixture;
use diesel::ExpressionMethods as _;
use diesel_async::RunQueryDsl as _;

const HOUR: Duration = Duration::from_hours(1);

mod schema {
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct Phone {
        pub name: String,
        pub model: Option<String>,
    }

    connetto_server::connetto_schema! {
        pub struct PhoneSchema;
        id: String => diesel::sql_types::Text,
        audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
        audit_ddl: connetto_server::connetto_audit_ddl!("TEXT", "UUID"),
        device_descriptor: Phone {
            name: diesel::sql_types::Text,
            model: diesel::sql_types::Nullable<diesel::sql_types::Text>,
        },
    }
}

use schema::{Phone, PhoneSchema};

const DDL: [&str; 3] = [
    "CREATE TABLE connetto_device_enrolments (\
     key_id BYTEA PRIMARY KEY, user_id TEXT NOT NULL, session_id UUID NOT NULL, \
     enrolled_at TIMESTAMPTZ NOT NULL, last_seen TIMESTAMPTZ NOT NULL, revoked_at TIMESTAMPTZ, \
     public_key BYTEA NOT NULL, attestation TEXT NOT NULL, bot_list TEXT, \
     name TEXT NOT NULL, model TEXT)",
    "CREATE TABLE connetto_device_certificates (\
     serial BYTEA PRIMARY KEY, \
     key_id BYTEA NOT NULL REFERENCES connetto_device_enrolments (key_id), \
     issuer BYTEA NOT NULL, expires_at TIMESTAMPTZ NOT NULL)",
    "CREATE TABLE connetto_device_lists (issuer BYTEA PRIMARY KEY, last_number BIGINT NOT NULL)",
];

/// The store over a fresh fixture's tables.
async fn store(fixture: &Fixture) -> EnrolmentStore<PhoneSchema> {
    for statement in DDL {
        fixture.exec(statement).await;
    }
    EnrolmentStore::default()
}

fn key(byte: u8) -> KeyId {
    KeyId::from_bytes([byte; 32])
}

fn session(n: u128) -> SessionId {
    SessionId::from_uuid(uuid::Uuid::from_u128(n))
}

fn phone(name: &str) -> Vec<u8> {
    rmp_serde::to_vec_named(&Phone {
        name: name.to_owned(),
        model: Some("Pixel".to_owned()),
    })
    .expect("encode")
}

/// One certificate for `user`'s key `key`, under `issuer`, expiring `ttl` from `at`.
fn enrolment(
    user: &str,
    key: KeyId,
    serial: u8,
    issuer: KeyId,
    at: SystemTime,
    attestation: AttestationLevel,
) -> Enrolment<String> {
    Enrolment {
        user: user.to_owned(),
        key,
        serial: [serial; 16],
        issuer,
        issued_at: at,
        expires_at: at + HOUR,
        session: session(u128::from(serial)),
        public_key: Vec::new(),
        descriptor: phone(&format!("{user} {serial}")),
        attestation,
        bot_list: connetto_core::BOT_LIST_WILDCARD.to_owned(),
    }
}

/// Postgres keeps microseconds, so a time read back is compared at that grain.
fn micros(at: SystemTime) -> u128 {
    at.duration_since(SystemTime::UNIX_EPOCH)
        .expect("after the epoch")
        .as_micros()
}

#[tokio::test]
async fn a_key_enrols_renews_and_lists_its_typed_descriptor() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let issuer = key(9);
    let first = SystemTime::now();
    let later = first + HOUR;

    assert_eq!(
        store
            .record(
                &mut connection,
                enrolment(
                    "alice",
                    key(1),
                    1,
                    issuer,
                    first,
                    AttestationLevel::Unproven
                )
            )
            .await
            .expect("first"),
        Recorded::Granted { first: true }
    );
    assert_eq!(
        store
            .record(
                &mut connection,
                enrolment(
                    "alice",
                    key(1),
                    2,
                    issuer,
                    later,
                    AttestationLevel::Unproven
                )
            )
            .await
            .expect("renewal"),
        Recorded::Granted { first: false }
    );

    let devices = store
        .devices(&mut connection, &"alice".to_owned())
        .await
        .expect("devices");
    assert_eq!(
        devices.len(),
        1,
        "one row per key, however many certificates"
    );
    let device = &devices[0];
    assert_eq!(device.key, key(1));
    assert_eq!(micros(device.enrolled_at), micros(first));
    assert_eq!(
        micros(device.last_seen),
        micros(later),
        "the renewal moved the sighting"
    );
    assert_eq!(device.revoked_at, None);
    let shown: Phone = rmp_serde::from_slice(&device.descriptor).expect("decodes");
    assert_eq!(
        shown.name, "alice 2",
        "the renewal's descriptor replaced the first"
    );
    assert!(
        store
            .devices(&mut connection, &"bob".to_owned())
            .await
            .expect("bob")
            .is_empty(),
        "bob holds no device"
    );
}

#[tokio::test]
async fn a_key_held_by_one_account_is_refused_to_another() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let now = SystemTime::now();
    store
        .record(
            &mut connection,
            enrolment("alice", key(1), 1, key(9), now, AttestationLevel::Unproven),
        )
        .await
        .expect("alice");

    assert_eq!(
        store
            .record(
                &mut connection,
                enrolment("bob", key(1), 2, key(9), now, AttestationLevel::Unproven)
            )
            .await
            .expect("bob"),
        Recorded::HeldElsewhere
    );
    assert_eq!(
        store
            .revoke(&mut connection, Some(&"bob".to_owned()), key(1), now)
            .await
            .expect("bob revokes"),
        Revocation::NotFound,
        "an account cannot revoke a key it does not hold"
    );
    assert!(
        store
            .devices(&mut connection, &"bob".to_owned())
            .await
            .expect("bob")
            .is_empty(),
        "bob holds no device"
    );
}

#[tokio::test]
async fn a_revoked_key_names_its_session_once_and_never_enrols_again() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let now = SystemTime::now();
    let first = enrolment("alice", key(1), 1, key(9), now, AttestationLevel::Unproven);
    let last_session =
        enrolment("alice", key(1), 2, key(9), now, AttestationLevel::Unproven).session;
    store.record(&mut connection, first).await.expect("first");
    store
        .record(
            &mut connection,
            enrolment("alice", key(1), 2, key(9), now, AttestationLevel::Unproven),
        )
        .await
        .expect("renewal");

    assert_eq!(
        store
            .revoke(&mut connection, Some(&"alice".to_owned()), key(1), now)
            .await
            .expect("revoke"),
        Revocation::Revoked {
            user: "alice".to_owned(),
            session: last_session
        },
        "the session that last renewed the key is the one to close"
    );
    assert_eq!(
        store
            .revoke(&mut connection, None, key(1), now)
            .await
            .expect("again"),
        Revocation::AlreadyRevoked {
            session: last_session
        },
        "a repeat names the same session to close"
    );
    assert_eq!(
        store
            .record(
                &mut connection,
                enrolment("alice", key(1), 3, key(9), now, AttestationLevel::Unproven)
            )
            .await
            .expect("after"),
        Recorded::Revoked
    );
    assert_eq!(
        store
            .revoke(&mut connection, None, key(2), now)
            .await
            .expect("unknown"),
        Revocation::NotFound
    );
}

#[tokio::test]
async fn a_list_names_every_unexpired_serial_of_its_issuer_for_a_revoked_key() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let now = SystemTime::now();
    let (issuer, other) = (key(9), key(8));
    let long_ago = now - 2 * HOUR;
    for record in [
        enrolment("alice", key(1), 1, issuer, now, AttestationLevel::Unproven),
        enrolment(
            "alice",
            key(1),
            2,
            issuer,
            long_ago,
            AttestationLevel::Unproven,
        ),
        enrolment("alice", key(1), 3, other, now, AttestationLevel::Unproven),
        enrolment("alice", key(2), 4, issuer, now, AttestationLevel::Unproven),
    ] {
        let first = record.serial == [1; 16] || record.serial == [4; 16];
        assert_eq!(
            store.record(&mut connection, record).await.expect("record"),
            Recorded::Granted { first }
        );
    }
    assert!(
        store
            .revoked_serials(&mut connection, issuer)
            .await
            .expect("before")
            .is_empty(),
        "no key is revoked yet"
    );

    store
        .revoke(&mut connection, None, key(1), now)
        .await
        .expect("revoke");
    let named = store
        .revoked_serials(&mut connection, issuer)
        .await
        .expect("after");
    assert_eq!(
        named
            .iter()
            .map(|revoked| revoked.serial.clone())
            .collect::<Vec<_>>(),
        vec![vec![1; 16]],
        "the expired serial, the other issuer's and the live key's are left out"
    );
    assert_eq!(micros(named[0].at), micros(now));
}

/// A peer accepts a certificate up to `DEVICE_SKEW` past its `not_after`, so a
/// revoked serial stays on the list that long after the certificate lapses,
/// and no longer after that (chapter 21, class G).
#[tokio::test]
async fn a_revoked_serial_stays_named_until_device_skew_past_expiry() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let now = SystemTime::now();
    let issuer = key(9);
    for (serial, at) in [
        (1u8, now - HOUR - Duration::from_secs(61)),
        (2, now - HOUR - Duration::from_secs(361)),
    ] {
        let record = enrolment(
            "alice",
            key(serial),
            serial,
            issuer,
            at,
            AttestationLevel::Unproven,
        );
        assert_eq!(
            store.record(&mut connection, record).await.expect("record"),
            Recorded::Granted { first: true }
        );
    }
    for key in [key(1), key(2)] {
        store
            .revoke(&mut connection, None, key, now)
            .await
            .expect("revoke");
    }
    let named = store
        .revoked_serials(&mut connection, issuer)
        .await
        .expect("named");
    assert_eq!(
        named
            .iter()
            .map(|revoked| revoked.serial.clone())
            .collect::<Vec<_>>(),
        vec![vec![1u8; 16]],
        "the serial lapsed 61s ago stays named, the one lapsed 6min1s ago does not"
    );
}

#[tokio::test]
async fn list_numbers_rise_per_issuer_from_one() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let (issuer, other) = (key(9), key(8));

    assert_eq!(
        store
            .next_list_number(&mut connection, issuer)
            .await
            .expect("first"),
        1
    );
    assert_eq!(
        store
            .next_list_number(&mut connection, issuer)
            .await
            .expect("second"),
        2
    );
    assert_eq!(
        store
            .next_list_number(&mut connection, other)
            .await
            .expect("other"),
        1
    );
    assert_eq!(
        store
            .next_list_number(&mut connection, issuer)
            .await
            .expect("third"),
        3
    );
}

#[tokio::test]
async fn a_descriptor_of_another_shape_is_refused_and_recorded_nowhere() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let mut record = enrolment(
        "alice",
        key(1),
        1,
        key(9),
        SystemTime::now(),
        AttestationLevel::Unproven,
    );
    record.descriptor = rmp_serde::to_vec_named(&("a tuple", 7_u8)).expect("encode");

    assert_eq!(
        store.record(&mut connection, record).await.expect("record"),
        Recorded::UnreadableDescriptor
    );
    assert!(
        store
            .devices(&mut connection, &"alice".to_owned())
            .await
            .expect("alice")
            .is_empty(),
        "the refused enrolment left no row"
    );
}

/// The record a bind, a retirement and a provenance read answers on: the
/// account the key is enrolled under, its revocation, whether a certificate
/// of it is still live, and the public key the bind signs are checked
/// against. `None` when the key is unknown or is enrolled under another
/// account than the one named.
#[tokio::test]
async fn a_key_record_reports_its_account_revocation_and_liveness() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let now = SystemTime::now();
    let alice_key = key(1);
    let mut live = enrolment(
        "alice",
        alice_key,
        1,
        key(9),
        now,
        AttestationLevel::Unproven,
    );
    live.public_key = vec![0xab; 64];
    assert_eq!(
        store.record(&mut connection, live).await.expect("record"),
        Recorded::Granted { first: true }
    );

    let found = store
        .key_record(&mut connection, Some(&"alice".to_owned()), alice_key)
        .await
        .expect("record")
        .expect("the key is enrolled under alice");
    assert_eq!(found.user, "alice");
    assert!(!found.revoked, "the enrolment is not revoked");
    assert!(
        found.certificate_live,
        "the certificate stands an hour past its issue"
    );
    assert_eq!(found.public_key, vec![0xab; 64]);

    assert!(
        store
            .key_record(&mut connection, Some(&"bob".to_owned()), alice_key)
            .await
            .expect("bob")
            .is_none(),
        "another account's key record names nothing"
    );
    assert!(
        store
            .key_record(&mut connection, None, alice_key)
            .await
            .expect("no account")
            .is_some(),
        "no account named still reads the record"
    );
    assert!(
        store
            .key_record(&mut connection, Some(&"alice".to_owned()), key(2))
            .await
            .expect("unknown")
            .is_none(),
        "an unenrolled key names nothing"
    );

    let expired = enrolment(
        "bob",
        key(3),
        2,
        key(8),
        now - 2 * HOUR,
        AttestationLevel::Unproven,
    );
    assert_eq!(
        store
            .record(&mut connection, expired)
            .await
            .expect("expired"),
        Recorded::Granted { first: true }
    );
    let expired = store
        .key_record(&mut connection, Some(&"bob".to_owned()), key(3))
        .await
        .expect("bob")
        .expect("enrolled");
    assert!(
        !expired.certificate_live,
        "the certificate expired an hour ago"
    );

    store
        .revoke(&mut connection, Some(&"alice".to_owned()), alice_key, now)
        .await
        .expect("revoke");
    let revoked = store
        .key_record(&mut connection, Some(&"alice".to_owned()), alice_key)
        .await
        .expect("after")
        .expect("still enrolled");
    assert!(revoked.revoked, "the revocation stands");
    assert_eq!(
        revoked.public_key,
        vec![0xab; 64],
        "the revocation keeps the public key"
    );
}

/// The key record carries the bot list the key's last certificate carries,
/// written at enrolment and replaced by a renewal, with `None` for an absent list.
#[tokio::test]
async fn a_key_record_carries_the_last_certificates_bot_list() {
    let fixture = Fixture::acquire().await;
    let store = store(&fixture).await;
    let mut connection = fixture.admin().get().await.expect("connection");
    let now = SystemTime::now();
    let mut first = enrolment("alice", key(1), 1, key(9), now, AttestationLevel::Unproven);
    "orders:read".clone_into(&mut first.bot_list);
    assert_eq!(
        store.record(&mut connection, first).await.expect("enrol"),
        Recorded::Granted { first: true }
    );
    let enrolled = store
        .key_record(&mut connection, None, key(1))
        .await
        .expect("read")
        .expect("enrolled");
    assert_eq!(enrolled.bot_list.as_deref(), Some("orders:read"));

    let mut renewal = enrolment("alice", key(1), 2, key(9), now, AttestationLevel::Unproven);
    "orders:read,orders:insert".clone_into(&mut renewal.bot_list);
    assert_eq!(
        store.record(&mut connection, renewal).await.expect("renew"),
        Recorded::Granted { first: false }
    );
    let renewed = store
        .key_record(&mut connection, None, key(1))
        .await
        .expect("read")
        .expect("enrolled");
    assert_eq!(
        renewed.bot_list.as_deref(),
        Some("orders:read,orders:insert"),
        "the renewal's list replaces the enrolment's"
    );
    diesel::update(schema::connetto_device_enrolments::table)
        .set(schema::connetto_device_enrolments::bot_list.eq(None::<String>))
        .execute(&mut *connection)
        .await
        .expect("clear bot list");
    let legacy = store
        .key_record(&mut connection, None, key(1))
        .await
        .expect("read")
        .expect("enrolled");
    assert_eq!(legacy.bot_list, None, "a row without a list reads none");
}
