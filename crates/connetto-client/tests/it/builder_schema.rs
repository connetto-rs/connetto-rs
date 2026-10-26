//! The schema piece of the builder: `SyncSchema::new` installs the `uuidv4()`
//! function a translated column default may call, so a device without a
//! server still mints the id.

use connetto_client::{ClientBuilder, SyncSchema, TierStepRefused};
use connetto_core::schema::SchemaBundle;
use connetto_core::test_support::FakeTransport;
use connetto_core::{Major, TierBoundary, TierHistory};
use diesel::SqliteConnection;
use diesel::prelude::*;

/// The replica DDL the bundle carries, whose BLOB key takes a `uuidv4()`
/// default, the shape the translation emits for a Postgres `uuid` primary key.
const DDL: &str = "CREATE TABLE things (id BLOB PRIMARY KEY DEFAULT (uuidv4()), label TEXT);";

diesel::table! {
    /// Synced test table with a minted key.
    things (id) {
        /// The key the `uuidv4()` default mints.
        id -> Binary,
        /// The row's label.
        label -> Nullable<Text>,
    }
}

/// A replica whose DDL defaults a BLOB key with `uuidv4()` mints a distinct
/// 16-byte v4 key on every insert that leaves the key out.
#[test]
fn builder_schema_installs_uuidv4_for_a_default_key() {
    let schema = SyncSchema::new(
        SchemaBundle::new(
            "-- connetto:major 1\nCREATE TABLE things (id UUID PRIMARY KEY, label TEXT);",
            "",
            DDL,
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            None::<&str>,
        )
        .expect("the source declares major 1"),
    );
    let mut conn = ClientBuilder::new(
        schema,
        super::support::NeverDial::<FakeTransport>::default(),
    )
    .open_driven()
    .expect("open the replica with the bundle's DDL");
    diesel::insert_into(things::table)
        .values(things::label.eq("first"))
        .execute(conn.conn())
        .expect("insert the first row without naming the key");
    diesel::insert_into(things::table)
        .values(things::label.eq("second"))
        .execute(conn.conn())
        .expect("insert the second row without naming the key");

    let keys: Vec<Vec<u8>> = things::table
        .select(things::id)
        .order(things::label)
        .load(conn.conn())
        .expect("read the minted keys");
    assert_eq!(keys.len(), 2, "both rows took a key from the default");
    for (n, key) in keys.iter().enumerate() {
        assert_eq!(key.len(), 16, "a v4 uuid is sixteen bytes, row {n}");
        assert_eq!(
            key[6] & 0xF0,
            0x40,
            "the default mints a version-4 uuid, row {n}"
        );
        assert_eq!(
            key[8] & 0xC0,
            0x80,
            "the default mints a version-4 uuid, row {n}"
        );
    }
    assert_ne!(keys[0], keys[1], "two inserts mint distinct keys");
}

fn major(value: u32) -> Major {
    Major::new(value).expect("a test major is from 1 up")
}

/// A data step registered for a boundary this build carries no generated
/// step for is refused at registration, before a device ever opens it.
#[test]
fn a_tier_step_outside_the_boundaries_is_refused() {
    fn schema() -> SyncSchema {
        let bundle = SchemaBundle::new(
            "-- connetto:major 3",
            "",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)",
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
            Some("CREATE TABLE notes (id INTEGER PRIMARY KEY)"),
        )
        .expect("the source declares major 3");
        SyncSchema::new(
            bundle
                .with_tier_history(TierHistory::new(
                    major(2),
                    Some(major(2)),
                    vec![TierBoundary::new(
                        major(2),
                        "ALTER TABLE notes ADD COLUMN pinned INTEGER",
                    )],
                ))
                .expect("the history reaches the build"),
        )
    }
    let step = |_tier: &mut SqliteConnection| -> Result<(), connetto_client::TierStepError> {
        Err("unreachable".into())
    };

    match schema().with_tier_step(major(3), step) {
        Err(TierStepRefused::Outside {
            from,
            oldest,
            build,
        }) => {
            assert_eq!(
                (from, oldest, build),
                (major(3), major(2), major(3)),
                "a step at the build's own major could never run"
            );
        }
        Ok(_) => panic!("a step outside the boundaries registered"),
    }
    let step = |_tier: &mut SqliteConnection| -> Result<(), connetto_client::TierStepError> {
        Err("unreachable".into())
    };
    match schema().with_tier_step(major(1), step) {
        Err(TierStepRefused::Outside {
            from,
            oldest,
            build,
        }) => {
            assert_eq!(
                (from, oldest, build),
                (major(1), major(2), major(3)),
                "a step before the oldest export could never run"
            );
        }
        Ok(_) => panic!("a step outside the boundaries registered"),
    }
}
