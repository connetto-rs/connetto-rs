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
            connetto_core::ReplicaCopy::new(
                DDL,
                Vec::<(String, String)>::new(),
                Vec::<String>::new(),
            ),
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

diesel::table! {
    /// A row-secured table whose policy calls a SQL helper.
    docs (id) {
        /// Row key, public while `public_ids` holds it.
        id -> Integer,
        /// The row's label.
        label -> Nullable<Text>,
    }
}

diesel::table! {
    /// The ids the helper reads.
    public_ids (id) {
        /// A public row's key.
        id -> Integer,
    }
}

/// A replica whose policy calls a SQL helper reading another table runs the
/// helper as the table-valued function the bundle carries, on a connection
/// the builder opened, for both the policy's `WITH CHECK` and its `USING`.
#[test]
fn builder_schema_registers_the_helpers_its_policies_call() {
    let bundle = connetto_translate::translate::<String, connetto_core::bots::TextIds>(
        connetto_translate::Documents::new(
            "-- connetto:major 1
CREATE TABLE public_ids (id INT PRIMARY KEY);
CREATE TABLE docs (id INT PRIMARY KEY, label TEXT);",
            "CREATE FUNCTION is_public(p_id INT) RETURNS BOOLEAN LANGUAGE sql STABLE
    AS $$ SELECT EXISTS (SELECT 1 FROM public_ids WHERE id = p_id) $$;
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_public ON docs USING (is_public(id)) WITH CHECK (is_public(id));",
            connetto_test_harness::FIXTURE_ROLES,
            connetto_test_harness::FIXTURE_READER,
        ),
    )
    .expect("translate the sources");
    assert!(
        !bundle
            .copy(connetto_core::CallerKind::Anon)
            .helper_functions()
            .is_empty(),
        "the translation calls the helper as a table-valued function"
    );
    let mut conn = ClientBuilder::new(
        SyncSchema::new(bundle),
        super::support::NeverDial::<FakeTransport>::default(),
    )
    .open_driven()
    .expect("open the replica with the bundle's DDL");
    diesel::insert_into(public_ids::table)
        .values(public_ids::id.eq(1))
        .execute(conn.conn())
        .expect("mark row 1 public");
    diesel::insert_into(docs::table)
        .values((docs::id.eq(1), docs::label.eq("public")))
        .execute(conn.conn())
        .expect("the helper admits a public row");
    let refused = diesel::insert_into(docs::table)
        .values((docs::id.eq(2), docs::label.eq("private")))
        .execute(conn.conn())
        .expect_err("the helper refuses a row that is not public");
    assert!(
        pg2sqlite::prelude::is_policy_refusal(&refused.to_string()),
        "the policy refused the row, not a missing function: {refused}"
    );
    diesel::delete(public_ids::table)
        .execute(conn.conn())
        .expect("take row 1 out of the public ids");
    let visible: Vec<i32> = docs::table
        .select(docs::id)
        .load(conn.conn())
        .expect("read through the policy view");
    assert!(
        visible.is_empty(),
        "the helper hides a row no longer public"
    );
}

/// A build opens the anonymous copy until it acts as a kind, and a policy
/// written `TO authenticated` holds on the signed-in copy only.
#[test]
fn a_build_opens_the_copy_of_the_kind_it_acts_as() {
    use connetto_core::CallerKind;

    let bundle = connetto_translate::translate::<String, connetto_core::bots::TextIds>(
        connetto_translate::Documents::new(
            "-- connetto:major 1
CREATE TABLE docs (id INT PRIMARY KEY, label TEXT);",
            "ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_signed_in ON docs TO authenticated USING (true) WITH CHECK (true);",
            connetto_test_harness::FIXTURE_ROLES,
            connetto_test_harness::FIXTURE_READER,
        ),
    )
    .expect("translate the sources");
    let open = |schema: SyncSchema| {
        ClientBuilder::new(
            schema,
            super::support::NeverDial::<FakeTransport>::default(),
        )
        .open_driven()
        .expect("open the replica with the kind's copy")
    };
    let anonymous = SyncSchema::new(bundle.clone());
    assert_eq!(anonymous.kind(), CallerKind::Anon);
    let mut conn = open(anonymous);
    let refused = diesel::insert_into(docs::table)
        .values((docs::id.eq(1), docs::label.eq("mine")))
        .execute(conn.conn())
        .expect_err("no policy reaches the anonymous copy");
    assert!(
        pg2sqlite::prelude::is_policy_refusal(&refused.to_string()),
        "the copy's row security refused the row: {refused}"
    );

    let mut conn = open(SyncSchema::new(bundle).acting_as(CallerKind::Authenticated));
    diesel::insert_into(docs::table)
        .values((docs::id.eq(1), docs::label.eq("mine")))
        .execute(conn.conn())
        .expect("the signed-in copy carries the policy that admits the row");
    let visible: Vec<i32> = docs::table
        .select(docs::id)
        .load(conn.conn())
        .expect("read through the policy view");
    assert_eq!(visible, [1]);
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
            connetto_core::ReplicaCopy::new(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)",
                Vec::<(String, String)>::new(),
                Vec::<String>::new(),
            ),
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
