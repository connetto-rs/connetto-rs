//! The media gate and the provisional values a holder and a receiver read
//! (R79 decisions 2 and 6).

use connetto_client::{ClientBuilder, ConnettoConnection, SyncSchema};
use diesel::prelude::*;

use super::peer_support::{author, captured, manifest, receiver};
use super::support::{self, Script};

/// A photo table each person reads only their own rows of, writable by
/// anyone, so a receiver can hold rows its own identity cannot read.
const PHOTO_SCHEMA: &str = "-- connetto:major 1\n \
                            CREATE TABLE photos (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                            content_id BYTEA NOT NULL); \
                            ALTER TABLE photos ENABLE ROW LEVEL SECURITY;";

const PHOTO_POLICIES: &str = "CREATE POLICY photos_read ON photos FOR SELECT \
                              USING (owner = current_setting('app.user_id', true)); \
                              CREATE POLICY photos_write ON photos FOR INSERT WITH CHECK (true);";

/// The same table on an author's plain replica.
const PLAIN_PHOTOS: &str = "CREATE TABLE photos (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, \
                            content_id BLOB NOT NULL);";

diesel::table! {
    /// The synced photos table.
    photos (id) {
        /// Primary key.
        id -> BigInt,
        /// The row's owner.
        owner -> Text,
        /// The file the row names.
        content_id -> Binary,
    }
}

const ANNAS_FILE: [u8; 32] = [1; 32];
const R77S_FILE: [u8; 32] = [2; 32];

fn insert_photo<T>(conn: &mut ConnettoConnection<T>, id: i64, owner: &str, file: [u8; 32])
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    diesel::insert_into(photos::table)
        .values((
            photos::id.eq(id),
            photos::owner.eq(owner),
            photos::content_id.eq(file.to_vec()),
        ))
        .execute(conn.conn())
        .expect("insert the photo");
}

/// A receiver signed in as `r77` holding one photo of anna's and one of its
/// own.
async fn holder() -> ConnettoConnection<Script> {
    let (mut conn, _) = receiver(PHOTO_SCHEMA, PHOTO_POLICIES).await;
    insert_photo(&mut conn, 1, "anna", ANNAS_FILE);
    insert_photo(&mut conn, 2, "r77", R77S_FILE);
    conn
}

fn own_rows<T>(conn: &mut ConnettoConnection<T>) -> Vec<i64>
where
    T: connetto_core::traits::Transport,
    T::Error: core::fmt::Display,
{
    photos::table
        .select(photos::id)
        .order(photos::id.asc())
        .load(conn.conn())
        .expect("read the photos")
}

#[tokio::test]
async fn the_gate_admits_a_requester_who_reads_a_row_naming_the_file() {
    let mut conn = holder().await;
    let anna = author("anna-key", "anna", Some("*"));
    let carl = author("carl-key", "carl", Some("*"));
    assert!(
        conn.peer_may_read(&anna, "photos", "content_id", &ANNAS_FILE)
            .expect("judge anna")
    );
    assert!(
        !conn
            .peer_may_read(&carl, "photos", "content_id", &ANNAS_FILE)
            .expect("judge carl"),
        "a requester who reads no row naming the file is refused"
    );
    assert!(
        !conn
            .peer_may_read(&anna, "photos", "content_id", &[9; 32])
            .expect("judge an absent file"),
        "a file no row names is refused like a forbidden one"
    );
}

#[tokio::test]
async fn the_gate_restores_the_holders_own_identity() {
    let mut conn = holder().await;
    assert_eq!(own_rows(&mut conn), vec![2], "r77 reads its own row alone");
    let anna = author("anna-key", "anna", Some("*"));
    conn.peer_may_read(&anna, "photos", "content_id", &ANNAS_FILE)
        .expect("judge anna");
    assert_eq!(
        own_rows(&mut conn),
        vec![2],
        "after the gate the views answer as r77 again"
    );
}

#[tokio::test]
async fn a_requester_whose_certificate_carries_no_bot_list_is_refused() {
    let mut conn = holder().await;
    let listed = author("anna-key", "anna", Some("*"));
    let unlisted = author("anna-key", "anna", None);
    assert!(
        conn.peer_may_read(&listed, "photos", "content_id", &ANNAS_FILE)
            .expect("judge the listed requester"),
        "the same account with a bot list reads its row"
    );
    assert!(
        !conn
            .peer_may_read(&unlisted, "photos", "content_id", &ANNAS_FILE)
            .expect("judge the unlisted requester")
    );
}

#[tokio::test]
async fn a_build_with_no_caller_mapping_refuses_every_requester() {
    let script = Script::default();
    script.say(support::handshake_ack());
    let schema = SyncSchema::new(
        connetto_schema::translate::<String, connetto_core::bots::TextIds>(
            connetto_schema::Documents::new(
                PHOTO_SCHEMA,
                PHOTO_POLICIES,
                connetto_test_harness::FIXTURE_ROLES,
                connetto_test_harness::FIXTURE_READER,
            ),
        )
        .expect("translate the policy schema"),
    );
    let mut conn = ClientBuilder::new(schema, support::Once::new(script))
        .connect_driven()
        .await
        .expect("connect anonymous");
    let anna = author("anna-key", "anna", Some("*"));
    assert!(
        !conn
            .peer_may_read(&anna, "photos", "content_id", &ANNAS_FILE)
            .expect("judge anna")
    );
}

#[tokio::test]
async fn a_table_or_column_the_build_lacks_is_an_error() {
    let mut conn = holder().await;
    let anna = author("anna-key", "anna", Some("*"));
    assert!(
        conn.peer_may_read(&anna, "albums", "content_id", &ANNAS_FILE)
            .is_err()
    );
    assert!(
        conn.peer_may_read(&anna, "photos", "thumb_id", &ANNAS_FILE)
            .is_err()
    );
    assert!(conn.provisional_blobs("photos", "thumb_id").is_err());
}

#[tokio::test]
async fn provisional_values_name_each_provisional_rows_file_and_author() {
    let [body] = captured(
        PLAIN_PHOTOS,
        [&|conn| {
            insert_photo(conn, 7, "anna", ANNAS_FILE);
        }],
    )
    .await;
    let (mut recv, _) = receiver(PHOTO_SCHEMA, PHOTO_POLICIES).await;
    insert_photo(&mut recv, 2, "r77", R77S_FILE);
    let anna = author("anna-key", "anna", Some("*"));
    recv.apply_peer_entry(&anna, &manifest(1, &body), &body)
        .expect("apply anna's photo");

    let values = recv
        .provisional_blobs("photos", "content_id")
        .expect("read the provisional values");
    assert_eq!(values.len(), 1, "the receiver's own row holds no record");
    assert_eq!(values[0].value, ANNAS_FILE.to_vec());
    assert_eq!(values[0].author, anna.key_id);

    recv.adjudicate_peer_frontier(&anna.key_id, 1)
        .expect("adjudicate anna's frontier");
    assert_eq!(
        recv.provisional_blobs("photos", "content_id")
            .expect("read the provisional values"),
        Vec::new(),
        "an adjudicated row names no provisional file"
    );
}
