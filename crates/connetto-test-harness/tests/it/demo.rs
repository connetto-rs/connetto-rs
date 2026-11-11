//! The demo deployment the harness proofs run against: the bundle the builds
//! present, the two-account sign-ins, the signed-in build with its peer
//! listener, and the bounded waits on the replica's rows and events.
//!
//! The proofs drive two signed-in accounts, alice and bob, of the demo in
//! `examples/deployment`, whose note rows a team's membership rows settle.
#![expect(
    clippy::large_futures,
    reason = "the signed-in builds hold their whole replica, their futures are large by design"
)]

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use connetto_client::{
    ClientEvent, KeyValue, KeyringAuth, LiveQuery, NativeClient, NativeTransport, SyncSchema,
    provisional::ProvisionalRow,
};
use connetto_core::bots::TextIds;
use connetto_core::device_cert::DeviceCertificate;
use connetto_core::messages::MutationRejectReason;
use connetto_test_harness::native_client;
use connetto_test_harness::stack::DEMO_DEPLOYMENT;
use diesel::prelude::*;

/// The provider name the server binary registers the containerised identity
/// provider under.
pub const PROVIDER: &str = "dev-idp";
/// The keyring services the two accounts sign in under, one per account.
pub const APP_ALICE: &str = "r77-peer-exchange-alice";
pub const APP_BOB: &str = "r77-peer-exchange-bob";
/// The longest any bounded wait in a scenario runs.
pub const BOUND: Duration = Duration::from_secs(60);
/// The instant the orders rows name, arbitrary since the proofs do not read
/// it back.
pub const ORDER_INSTANT: i64 = 0;

diesel::table! {
    /// The team's note rows, the view the client writes through and reads
    /// back, the same name Postgres serves.
    team_notes (id) {
        /// Primary key the replica mints when the insert omits it.
        id -> Binary,
        /// The team the note settles against.
        team_id -> Text,
        /// Who wrote the note.
        author_id -> Text,
        /// The note's body.
        body -> Text,
    }
}

diesel::table! {
    /// The order rows, the view the client writes through and reads back.
    orders (id) {
        /// Primary key the replica mints when the insert omits it.
        id -> Binary,
        /// Who the order belongs to.
        owner_id -> Text,
        /// Item count.
        quantity -> BigInt,
        /// When the order was written, the replica's text-typed instant.
        created_at -> diesel::sql_types::TimestamptzSqlite,
    }
}

diesel::table! {
    /// The team's membership rows, synced to every replica of the demo.
    team_members (team_id, member_id) {
        /// The team the membership sits in.
        team_id -> Text,
        /// The account the membership names.
        member_id -> Text,
    }
}

/// The translated bundle the builds present.
pub fn bundle() -> SyncSchema {
    SyncSchema::new(
        connetto_schema::translate::<String, TextIds>(connetto_schema::Documents::new(
            DEMO_DEPLOYMENT.schema,
            DEMO_DEPLOYMENT.policies,
            DEMO_DEPLOYMENT.roles,
            DEMO_DEPLOYMENT.reader_role,
        ))
        .expect("the deployment translates"),
    )
}

/// A fresh-login keyring sign-in as `subject`, the services named by `app_id`.
pub fn fresh_login(base: &str, app_id: &str, subject: &str) -> KeyringAuth {
    native_client::fresh_login(base, PROVIDER, app_id, subject)
}

/// The silent sign-back-in the reconnect phases use.
pub fn last_used(base: &str, app_id: &str) -> KeyringAuth {
    native_client::last_used(base, PROVIDER, app_id)
}

/// A signed-in build over `addr`, the sign-in it names, the data directory
/// it keeps and the deployment root it verifies device certificates against,
/// with the peer link on `peer_listener` and the auto-link off, connected.
pub async fn connect_client(
    addr: SocketAddr,
    sign_in: KeyringAuth,
    dir: &Path,
    root: &[u8],
    peer_listener: SocketAddr,
) -> NativeClient<NativeTransport> {
    native_client::signed_in(
        format!("ws://{addr}/sync"),
        bundle(),
        sign_in,
        dir,
        vec![root.to_owned()],
    )
    .with_peer_listener(peer_listener)
    .with_peer_autolink(false)
    .connect()
    .await
    .expect("connects")
}

/// Connect, declare the watches, and close, remembering the subscriptions in
/// the durable store. The next build of the same replica re-announces them on
/// its attach, so the server resnapshots the tables then, on the handshake.
pub async fn prime_watches(
    addr: SocketAddr,
    sign_in: KeyringAuth,
    dir: &Path,
    root: &[u8],
    peer_listener: SocketAddr,
) {
    let client = connect_client(addr, sign_in, dir, root, peer_listener).await;
    let _watches = watch_tables(&client).await;
    client.close().await;
}

/// Await an event matching `is` within `deadline`, skipping every other event.
pub async fn next_event(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    deadline: Duration,
    is: impl Fn(&ClientEvent) -> bool,
) -> Option<ClientEvent> {
    native_client::next_event(events, deadline, is).await
}

/// Poll `device_certificate` until it holds a certificate, bounded by
/// `deadline`.
pub async fn wait_for_certificate(
    client: &NativeClient<NativeTransport>,
    deadline: Duration,
) -> Option<DeviceCertificate> {
    native_client::wait_for_certificate(client, deadline).await
}

/// Poll the device's peer listener until it answers, bounded by `deadline`.
pub async fn wait_peer_address(
    client: &NativeClient<NativeTransport>,
    deadline: Duration,
) -> Option<SocketAddr> {
    let until = Instant::now() + deadline;
    while Instant::now() < until {
        if let Some(addr) = client.peer_address() {
            return Some(addr);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// The replica's `team_notes` bodies in order, the view answering the read.
pub async fn note_bodies(client: &NativeClient<NativeTransport>) -> Vec<String> {
    client
        .client()
        .with_conn(|conn| {
            team_notes::table
                .select(team_notes::body)
                .order(team_notes::body.asc())
                .load(conn.conn())
                .expect("read the notes")
        })
        .await
        .expect("the read runs")
}

/// The replica's orders owners in order, the view answering the read.
pub async fn orders_owners(client: &NativeClient<NativeTransport>) -> Vec<String> {
    client
        .client()
        .with_conn(|conn| {
            orders::table
                .select(orders::owner_id)
                .order(orders::owner_id.asc())
                .load(conn.conn())
                .expect("read the orders")
        })
        .await
        .expect("the read runs")
}

/// The replica's membership rows in order.
pub async fn roster(client: &NativeClient<NativeTransport>) -> Vec<(String, String)> {
    client
        .client()
        .with_conn(|conn| {
            team_members::table
                .select((team_members::team_id, team_members::member_id))
                .order((team_members::team_id.asc(), team_members::member_id.asc()))
                .load(conn.conn())
                .expect("read the roster")
        })
        .await
        .expect("the read runs")
}

/// The live handles one replica holds for the roster and the notes, kept
/// alive so the subscriptions stay declared while the handles are held.
#[expect(
    dead_code,
    reason = "the fields are the live handles, held to keep the subscriptions declared"
)]
pub struct Watches {
    roster: LiveQuery<(String, String)>,
    notes: LiveQuery<(String, String, String)>,
}

/// Declare the roster and notes watches on the client's shared session, the
/// subscriptions remembered in the durable store so a later build of the same
/// replica re-announces them within the grace.
pub async fn watch_tables(client: &NativeClient<NativeTransport>) -> Watches {
    let roster = client
        .client()
        .watch(
            team_members::table
                .select((team_members::team_id, team_members::member_id))
                .order((team_members::team_id.asc(), team_members::member_id.asc())),
        )
        .await
        .expect("the roster follows");
    let notes = client
        .client()
        .watch(
            team_notes::table
                .select((team_notes::team_id, team_notes::author_id, team_notes::body))
                .order(team_notes::body.asc()),
        )
        .await
        .expect("the notes follow");
    Watches { roster, notes }
}

/// Poll the replica's membership rows until they hold exactly `expected`,
/// in any order, bounded by `deadline`.
pub async fn wait_roster(
    client: &NativeClient<NativeTransport>,
    expected: &[(String, String)],
) -> bool {
    let mut expected = expected.to_vec();
    expected.sort();
    let until = Instant::now() + BOUND;
    loop {
        let rows = roster(client).await;
        if rows == expected || Instant::now() >= until {
            return rows == expected;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `unsynced` until it holds nothing, bounded by `BOUND`.
pub async fn wait_unsynced_empty(client: &NativeClient<NativeTransport>) -> bool {
    native_client::wait_unsynced_empty(client, BOUND).await
}

/// Alice's note of `team` with `body`.
pub async fn write_note(
    client: &NativeClient<NativeTransport>,
    team: &str,
    author: &str,
    body: &str,
) {
    client
        .client()
        .with_conn(|conn| {
            diesel::insert_into(team_notes::table)
                .values((
                    team_notes::team_id.eq(team),
                    team_notes::author_id.eq(author),
                    team_notes::body.eq(body),
                ))
                .execute(conn.conn())
                .expect("the note is captured")
        })
        .await
        .expect("the write is captured");
}

pub async fn read_note_id(
    client: &NativeClient<NativeTransport>,
    team: &str,
    body: &str,
) -> Vec<u8> {
    client
        .client()
        .with_conn(|conn| {
            team_notes::table
                .select(team_notes::id)
                .filter(team_notes::team_id.eq(team))
                .filter(team_notes::body.eq(body))
                .first(conn.conn())
                .expect("the note is read")
        })
        .await
        .expect("the read runs")
}

/// The provisional record of the note with the id its write minted, keyed
/// exactly as the later confirmations and retractions spell it.
pub fn key_of(rows: &[ProvisionalRow], note_id: &[u8]) -> Vec<KeyValue> {
    rows.iter()
        .find(|row| row.key == vec![KeyValue::Blob(note_id.to_owned())])
        .map(|row| row.key.clone())
        .expect("the note's provisional record holds its id as the key")
}

/// Await one apply event per record of `author` on the receiver's stream,
/// bounded by `BOUND`, and assert the app heard every one of them.
pub async fn hear_applies(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    rows: &[ProvisionalRow],
    author: &[u8],
) {
    let mut expected: Vec<i64> = rows.iter().map(|row| row.seq).collect();
    expected.sort_unstable();
    expected.dedup();
    let mut heard: Vec<i64> = Vec::new();
    let heard_deadline = Instant::now() + BOUND;
    while heard.len() < expected.len() && Instant::now() < heard_deadline {
        if let Some(ClientEvent::ProvisionalApplied { seq, .. }) =
            next_event(events, Duration::from_millis(200), |event| {
                matches!(
                    event,
                    ClientEvent::ProvisionalApplied {
                        author: event_author, ..
                    } if *event_author == author
                )
            })
            .await
        {
            heard.push(seq);
        }
    }
    heard.sort_unstable();
    assert_eq!(heard, expected, "bob's app hears every apply while offline");
}

/// The server's refusal of the removed team's note on alice's stream, the
/// refusal naming the note, her queue settling and the note rolled back on
/// her replica behind it.
pub async fn expect_removed_note_refusal(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    alice: &NativeClient<NativeTransport>,
    remaining: &[(String, String)],
    bodies: &[String],
) {
    let rejected = next_event(events, BOUND, |event| {
        matches!(event, ClientEvent::MutationRejected { .. })
    })
    .await
    .expect("the server refuses the removed team's note");
    if let ClientEvent::MutationRejected {
        reason,
        rows: refused,
        ..
    } = &rejected
    {
        assert!(
            matches!(reason, MutationRejectReason::Unauthorized),
            "the removal lands as an unauthorized refusal, the server spelled {reason:?}"
        );
        assert_eq!(
            refused
                .iter()
                .map(|row| row.table.clone())
                .collect::<Vec<_>>(),
            vec!["team_notes".to_owned()],
            "the refusal names the refused note"
        );
    }
    assert!(
        wait_unsynced_empty(alice).await,
        "alice's queue settles behind the applies and the refusal"
    );
    assert_eq!(
        note_bodies(alice).await,
        bodies,
        "alice rolled the refused note back on her own replica"
    );
    assert!(
        wait_roster(alice, remaining).await,
        "alice's snapshot dropped the removed row"
    );
}

/// Alice's one-line order of `quantity` items, owned by her server-minted id.
pub async fn write_order(client: &NativeClient<NativeTransport>, owner: &str, quantity: i64) {
    client
        .client()
        .with_conn(|conn| {
            diesel::insert_into(orders::table)
                .values((
                    orders::owner_id.eq(owner),
                    orders::quantity.eq(quantity),
                    orders::created_at.eq(DateTime::<Utc>::from_timestamp(ORDER_INSTANT, 0)
                        .expect("the instant is in range")),
                ))
                .execute(conn.conn())
                .expect("the order is captured")
        })
        .await
        .expect("the write is captured");
}

/// The sign-in's user id, polled until the session records it, bounded by
/// `deadline`.
pub async fn wait_session(client: &NativeClient<NativeTransport>) -> String {
    let until = Instant::now() + BOUND;
    loop {
        if let Some(session) = client.session() {
            return session.user_id().to_owned();
        }
        assert!(
            Instant::now() < until,
            "the sign-in never records its session"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Insert the two-account roster the membership rows hold, under the
/// server-minted ids the policies compare.
pub async fn seed_roster(fixture: &connetto_test_harness::Fixture, alice: &str, bob: &str) {
    for row in [("T1", alice), ("T1", bob), ("T2", alice), ("T2", bob)] {
        fixture
            .exec(&format!(
                "INSERT INTO team_members (team_id, member_id) VALUES ('{}', '{}')",
                row.0, row.1
            ))
            .await;
    }
}
