//! R11 Part B: each server frame and each subscription change leaves the
//! replica's row holdings as the state table in the Part B record says.
//!
//! A fake server hand-feeds every frame, and each test reads
//! `_connetto_holding` straight off the replica after the event it names.

use std::collections::BTreeSet;

use connetto_client::{ClientBuilder, ClientEvent, ConnettoConnection};
use connetto_core::Cursor;
use connetto_core::messages::{
    BulkMessage, ControlMessage, FullResyncReason, FullResyncRequired, HandshakeAck, LivePatch,
    MembershipOpened, MutationApplied, MutationReject, MutationRejectReason, SnapshotBegin,
    SnapshotEnd, SnapshotPatch, SubscriptionPriority, SubscriptionSpec, WriterGrant,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_server::{LoopbackTransport, loopback};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use sqlite_diff_rs::{
    DiffOps, Indirect, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update, Value,
};

const DDL: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, quantity INTEGER); \
    CREATE TABLE team_members (id INTEGER PRIMARY KEY, order_id INTEGER);";
const ORDERS: &str = "SELECT * FROM orders WHERE quantity > 0";
const TERM: &str =
    "SELECT * FROM orders WHERE id IN (SELECT order_id FROM team_members WHERE id > 0)";
const LABEL: &str = "connetto-membership:team_members";

diesel::table! {
    /// The synced table most tests hold rows of.
    orders (id) {
        /// Primary key.
        id -> BigInt,
        /// A column a filter reads.
        quantity -> BigInt,
    }
}

/// One holding as the replica stores it: holder, table, key.
pub(super) type Holding = (String, String, String);

#[derive(QueryableByName)]
struct HoldingRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    holder: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    tbl: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    key: String,
}

pub(super) fn holdings<T>(conn: &mut ConnettoConnection<T>) -> BTreeSet<Holding>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    diesel::sql_query("SELECT holder, tbl, key FROM _connetto_holding")
        .load::<HoldingRow>(conn.conn())
        .expect("read the holdings")
        .into_iter()
        .map(|row| (row.holder, row.tbl, row.key))
        .collect()
}

/// The holdings `entries` name, each a holder, a table and the ids it holds.
pub(super) fn held(entries: &[(&str, &str, &[i64])]) -> BTreeSet<Holding> {
    entries
        .iter()
        .flat_map(|(holder, table, ids)| {
            ids.iter().map(move |id| {
                (
                    (*holder).to_owned(),
                    (*table).to_owned(),
                    format!("(\"id\" IS {id})"),
                )
            })
        })
        .collect()
}

/// Make the replica refuse `op` on `_connetto_holding`, as a full disk would.
fn refuse_holdings<T>(conn: &mut ConnettoConnection<T>, op: &str)
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    conn.conn()
        .batch_execute(&format!(
            "CREATE TEMP TRIGGER refuse_{op} BEFORE {op} ON main._connetto_holding \
             BEGIN SELECT RAISE(ABORT, 'the replica refuses'); END"
        ))
        .expect("install the refusal");
}

fn order_ids<T>(conn: &mut ConnettoConnection<T>) -> Vec<i64>
where
    T: Transport,
    T::Error: core::fmt::Display,
{
    orders::table
        .select(orders::id)
        .order(orders::id.asc())
        .load(conn.conn())
        .expect("read the replica")
}

fn table(name: &str) -> SimpleTable {
    let second = match name {
        "orders" => "quantity",
        "tags" => "label",
        _ => "order_id",
    };
    SimpleTable::new(name, &[("id", ""), (second, "")], &[0])
}

/// One patch's ops, built in order.
#[derive(Default)]
pub(super) struct Patch(PatchSet<SimpleTable, String, Vec<u8>>);

impl Patch {
    pub(super) fn insert(self, name: &str, id: i64) -> Self {
        let insert = Insert::<_, String, Vec<u8>>::from(table(name))
            .set(0, Value::Integer(id))
            .expect("set id")
            .set(1, Value::Integer(5))
            .expect("set second column");
        Self(self.0.insert(insert))
    }

    /// An insert into `orders` whose patch keys the table on `quantity`,
    /// which the replica does not key on.
    fn insert_keyed_on_quantity(self, id: i64) -> Self {
        let table = SimpleTable::new("orders", &[("id", ""), ("quantity", "")], &[1]);
        let insert = Insert::<_, String, Vec<u8>>::from(table)
            .set(0, Value::Integer(id))
            .expect("set id")
            .set(1, Value::Integer(5))
            .expect("set quantity");
        Self(self.0.insert(insert))
    }

    pub(super) fn update(self, name: &str, id: i64, second: i64) -> Self {
        let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(table(name))
            .set(0, Value::Integer(id))
            .expect("set id")
            .set(1, Value::Integer(second))
            .expect("set second column");
        Self(self.0.update(update))
    }

    /// A delete of a row gone upstream.
    pub(super) fn delete(self, name: &str, id: i64) -> Self {
        Self(
            self.0
                .delete(PatchDelete::new(table(name), vec![Value::Integer(id)])),
        )
    }

    /// A departure, the row left the addressed subscription (R44).
    pub(super) fn depart(self, name: &str, id: i64) -> Self {
        Self(
            self.0
                .delete(PatchDelete::new(table(name), vec![Value::Integer(id)]).indirect(true)),
        )
    }

    fn compressed(self) -> Vec<u8> {
        zstd::encode_all(self.0.build().as_slice(), 3).expect("compress")
    }
}

pub(super) fn inserts(name: &str, ids: &[i64]) -> Patch {
    ids.iter()
        .fold(Patch::default(), |patch, id| patch.insert(name, *id))
}

async fn ack_handshake(server: &mut LoopbackTransport) {
    let Ok(Some(IncomingFrame::Control(ControlMessage::Handshake(_)))) = server.recv().await else {
        panic!("no handshake");
    };
    server
        .send_control(ControlMessage::HandshakeAck(HandshakeAck {
            connection_id: "holdings".to_owned(),
            session_token: "holdings".to_owned(),
            resume_token: "holdings".to_owned(),
            current_cursor: Cursor::new(Vec::new()),
            schema_version: None,
            initial_credits: 256,
            writer: Some(WriterGrant {
                number: 1,
                frontier: 0,
            }),
        }))
        .await
        .expect("ack");
}

/// A connected client and the server end of its wire, which the test drives
/// frame by frame.
pub(super) struct Session {
    pub(super) conn: ConnettoConnection<LoopbackTransport>,
    server: LoopbackTransport,
    cursor: u8,
}

impl Session {
    async fn connected() -> Self {
        let (mut server, client_end) = loopback();
        let acked = tokio::spawn(async move {
            ack_handshake(&mut server).await;
            server
        });
        let conn = ClientBuilder::new(
            super::support::bundle(DDL),
            super::support::Once::new(client_end),
        )
        .signed_in(super::support::held("holdings"))
        .connect_driven()
        .await
        .expect("connect");
        Self {
            conn,
            server: acked.await.expect("the handshake task"),
            cursor: 0,
        }
    }

    /// Bring `conn`, opened with no server, online over a fresh wire.
    pub(super) async fn attach(mut conn: ConnettoConnection<LoopbackTransport>) -> Self {
        let (mut server, client_end) = loopback();
        let acked = tokio::spawn(async move {
            ack_handshake(&mut server).await;
            server
        });
        conn.attach(client_end).await.expect("attach");
        Self {
            conn,
            server: acked.await.expect("the handshake task"),
            cursor: 0,
        }
    }

    fn next_cursor(&mut self) -> Cursor {
        self.cursor += 1;
        Cursor::new(vec![0, 0, 0, 0, 0, 0, 0, self.cursor])
    }

    /// Pump until the client reports the event `wanted` picks out, which is
    /// the one the frame just sent produces.
    pub(super) async fn until(&mut self, what: &str, wanted: impl Fn(&ClientEvent) -> bool) {
        for _ in 0..64 {
            let event = self.conn.pump_one().await.expect("pump");
            assert!(
                !matches!(event, ClientEvent::Closed),
                "closed before {what}"
            );
            if wanted(&event) {
                return;
            }
        }
        panic!("no {what} within 64 events");
    }

    pub(super) async fn control(
        &mut self,
        message: ControlMessage,
        what: &str,
        wanted: impl Fn(&ClientEvent) -> bool,
    ) {
        self.server
            .send_control(message)
            .await
            .expect("send control");
        self.until(what, wanted).await;
    }

    /// A snapshot of `pages` for `sub_id`, each page its own patch.
    pub(super) async fn snapshot(&mut self, sub_id: &str, pages: Vec<Patch>) {
        self.control(
            ControlMessage::SnapshotBegin(SnapshotBegin {
                sub_id: sub_id.to_owned(),
                priority: SubscriptionPriority::default(),
            }),
            "the snapshot's begin",
            |event| matches!(event, ClientEvent::SnapshotBegin { .. }),
        )
        .await;
        for page in pages {
            self.server
                .send_bulk(BulkMessage::SnapshotPatch(SnapshotPatch::new(
                    sub_id,
                    page.compressed(),
                )))
                .await
                .expect("send a page");
            self.until("the page's apply", |event| {
                matches!(event, ClientEvent::SnapshotApplied { .. })
            })
            .await;
        }
        let cursor = self.next_cursor();
        self.control(
            ControlMessage::SnapshotEnd(SnapshotEnd {
                sub_id: sub_id.to_owned(),
                cursor,
            }),
            "the snapshot's end",
            |event| matches!(event, ClientEvent::SnapshotEnd { .. }),
        )
        .await;
    }

    pub(super) async fn live(&mut self, sub_id: &str, patch: Patch) {
        let cursor = self.next_cursor();
        self.server
            .send_bulk(BulkMessage::LivePatch(LivePatch::new(
                sub_id,
                cursor,
                patch.compressed(),
            )))
            .await
            .expect("send a live patch");
        self.until("the live patch's apply", |event| {
            matches!(event, ClientEvent::LivePatch { .. })
        })
        .await;
    }

    pub(super) async fn resync(&mut self, sub_id: &str, reason: FullResyncReason) {
        self.control(
            ControlMessage::FullResyncRequired(FullResyncRequired {
                sub_id: sub_id.to_owned(),
                reason,
            }),
            "the resync notice",
            |event| matches!(event, ClientEvent::FullResync { .. }),
        )
        .await;
    }

    /// Declare `sub_id` over `query` and deliver its snapshot of `ids`.
    pub(super) async fn holding(&mut self, sub_id: &str, query: &str, name: &str, ids: &[i64]) {
        self.conn.subscribe(sub_id, query).await.expect("subscribe");
        self.snapshot(sub_id, vec![inserts(name, ids)]).await;
    }

    /// Take the wire down, as a server going away does.
    pub(super) async fn offline(mut self) -> ConnettoConnection<LoopbackTransport> {
        drop(self.server);
        for _ in 0..64 {
            if !self.conn.is_connected() {
                break;
            }
            let _ = self.conn.pump_one().await;
        }
        assert!(!self.conn.is_connected(), "the peer is gone");
        self.conn
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_holds_each_row_it_delivers_under_its_subscription() {
    let mut s = Session::connected().await;
    s.conn.subscribe("a", ORDERS).await.expect("subscribe");
    assert!(
        holdings(&mut s.conn).is_empty(),
        "a declared subscription holds nothing before its snapshot"
    );
    s.snapshot(
        "a",
        vec![inserts("orders", &[1, 2]), inserts("orders", &[3])],
    )
    .await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[1, 2, 3])]),
        "every page of the snapshot is held"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_insert_is_held_and_an_update_keeps_the_holding() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.live("a", Patch::default().insert("orders", 2)).await;
    s.live("a", Patch::default().update("orders", 1, 7)).await;
    assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[1, 2])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_moved_key_moves_its_holding() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.live(
        "a",
        Patch::default().delete("orders", 1).insert("orders", 5),
    )
    .await;
    assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[5])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_departure_releases_only_the_addressed_holding() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1, 2]).await;
    s.holding("b", ORDERS, "orders", &[1, 2]).await;
    s.live("a", Patch::default().depart("orders", 1)).await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[2]), ("b", "orders", &[1, 2])]),
        "the row left a's window only, so b still holds it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_upstream_releases_every_holding_on_the_row() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1, 2]).await;
    s.holding("b", ORDERS, "orders", &[1, 2]).await;
    s.live("a", Patch::default().delete("orders", 1)).await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[2]), ("b", "orders", &[2])]),
        "the row is gone upstream, so nobody holds it any longer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_applied_twice_leaves_the_holdings_of_one() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    for _ in 0..2 {
        s.live(
            "a",
            Patch::default().insert("orders", 2).depart("orders", 1),
        )
        .await;
    }
    assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[2])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_whose_holdings_cannot_be_written_applies_nothing() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    refuse_holdings(&mut s.conn, "INSERT");
    let cursor = s.next_cursor();
    s.server
        .send_bulk(BulkMessage::LivePatch(LivePatch::new(
            "a",
            cursor,
            Patch::default().insert("orders", 2).compressed(),
        )))
        .await
        .expect("send a live patch");
    s.conn
        .pump_one()
        .await
        .expect_err("the replica refuses the holding");
    assert_eq!(
        order_ids(&mut s.conn),
        vec![1],
        "the row goes in with its holding or not at all"
    );
    assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[1])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsubscribe_whose_holdings_cannot_be_released_keeps_the_subscription() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    refuse_holdings(&mut s.conn, "DELETE");
    s.conn
        .unsubscribe("a")
        .await
        .expect_err("the replica refuses the release");
    assert_eq!(
        s.conn
            .declared_subscriptions()
            .expect("read the records")
            .len(),
        1,
        "the record ends with its holdings or not at all"
    );
    assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[1])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_for_a_subscription_no_longer_on_record_holds_nothing() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.conn.unsubscribe("a").await.expect("unsubscribe");
    s.live("a", Patch::default().insert("orders", 2)).await;
    assert!(
        holdings(&mut s.conn).is_empty(),
        "a frame in flight past the unsubscribe names no holder"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_patch_keyed_unlike_the_replica_holds_nothing_and_fails_nothing() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.live("a", Patch::default().insert_keyed_on_quantity(2))
        .await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[1])]),
        "SQLite skips a table whose key differs, so there is no row to hold"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_rows_are_held_until_no_term_needs_their_table() {
    let mut s = Session::connected().await;
    s.conn.subscribe("t1", TERM).await.expect("subscribe t1");
    s.conn.subscribe("t2", TERM).await.expect("subscribe t2");
    s.control(
        ControlMessage::MembershipOpened(MembershipOpened {
            sub_id: LABEL.to_owned(),
            member_table: "team_members".to_owned(),
        }),
        "the membership notice",
        |event| matches!(event, ClientEvent::MembershipOpened { .. }),
    )
    .await;
    s.snapshot(LABEL, vec![inserts("team_members", &[10])])
        .await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[(LABEL, "team_members", &[10])])
    );

    s.conn.unsubscribe("t1").await.expect("unsubscribe t1");
    assert_eq!(
        holdings(&mut s.conn),
        held(&[(LABEL, "team_members", &[10])]),
        "t2's term still reads the membership table"
    );
    s.conn.unsubscribe("t2").await.expect("unsubscribe t2");
    assert!(
        holdings(&mut s.conn).is_empty(),
        "the last term over the table ended"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compound_query_keeps_every_table_it_reads_as_a_possible_term() {
    let mut s = Session::connected().await;
    s.conn
        .subscribe(
            "u",
            &format!("{TERM} UNION SELECT * FROM orders WHERE id = 0"),
        )
        .await
        .expect("subscribe u");
    s.control(
        ControlMessage::MembershipOpened(MembershipOpened {
            sub_id: LABEL.to_owned(),
            member_table: "team_members".to_owned(),
        }),
        "the membership notice",
        |event| matches!(event, ClientEvent::MembershipOpened { .. }),
    )
    .await;
    s.snapshot(LABEL, vec![inserts("team_members", &[10])])
        .await;
    s.conn.subscribe("x", ORDERS).await.expect("subscribe x");
    assert_eq!(
        holdings(&mut s.conn),
        held(&[(LABEL, "team_members", &[10])]),
        "the compound query's term still reads the membership table"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_released_watch_keeps_its_holdings_through_its_grace() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.conn.release_subscription("a").expect("release");
    assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[1])]));
    s.conn.hold_subscription("a").expect("re-claim");
    assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[1])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watch_whose_grace_ran_out_holds_nothing() {
    let mut s = Session::connected().await;
    s.conn
        .subscribe_spec_with_grace(
            "a",
            SubscriptionSpec::new(ORDERS),
            core::time::Duration::ZERO,
        )
        .await
        .expect("subscribe");
    s.snapshot("a", vec![inserts("orders", &[1])]).await;
    s.holding("b", ORDERS, "orders", &[1]).await;
    s.conn.release_subscription("a").expect("release");
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("b", "orders", &[1])]),
        "a zero grace runs out at the release"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsubscribe_or_an_unpin_releases_that_subscriptions_holdings() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.holding("b", ORDERS, "orders", &[1]).await;
    s.holding("k", ORDERS, "orders", &[1]).await;
    s.conn
        .pin_subscription("keep", "k", &SubscriptionSpec::new(ORDERS))
        .expect("pin");

    s.conn.unsubscribe("a").await.expect("unsubscribe");
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("b", "orders", &[1]), ("k", "orders", &[1])])
    );
    s.conn.unpin_subscription("keep").expect("unpin");
    assert_eq!(holdings(&mut s.conn), held(&[("b", "orders", &[1])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repinned_name_leaves_the_old_subscription_a_watch_with_its_holdings() {
    let mut s = Session::connected().await;
    s.holding("k1", ORDERS, "orders", &[1]).await;
    s.conn
        .pin_subscription("keep", "k1", &SubscriptionSpec::new(ORDERS))
        .expect("pin k1");
    let narrower = "SELECT * FROM orders WHERE quantity > 1";
    s.holding("k2", narrower, "orders", &[2]).await;
    s.conn
        .pin_subscription("keep", "k2", &SubscriptionSpec::new(narrower))
        .expect("pin k2");
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("k1", "orders", &[1]), ("k2", "orders", &[2])]),
        "the old subscription falls back to a held watch"
    );
    s.conn.unsubscribe("k1").await.expect("end the old one");
    assert_eq!(holdings(&mut s.conn), held(&[("k2", "orders", &[2])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offline_unsubscribe_releases_its_holdings() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.holding("b", ORDERS, "orders", &[2]).await;
    let mut conn = s.offline().await;
    conn.unsubscribe("a").await.expect("offline unsubscribe");
    assert_eq!(holdings(&mut conn), held(&[("b", "orders", &[2])]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resync_releases_its_holdings_and_its_replacement_takes_them_back() {
    for reason in [
        FullResyncReason::CursorOutsideRetention,
        FullResyncReason::SnapshotInterrupted,
        FullResyncReason::AuthorizationChange,
    ] {
        let mut s = Session::connected().await;
        s.holding("a", ORDERS, "orders", &[1, 2]).await;
        s.holding("b", ORDERS, "orders", &[1, 2]).await;
        s.resync("a", reason.clone()).await;
        assert_eq!(
            holdings(&mut s.conn),
            held(&[("b", "orders", &[1, 2])]),
            "{reason:?} releases a's holdings and only a's"
        );
        s.snapshot("a", vec![inserts("orders", &[2])]).await;
        assert_eq!(
            holdings(&mut s.conn),
            held(&[("a", "orders", &[2]), ("b", "orders", &[1, 2])]),
            "{reason:?}'s replacement holds what it delivers"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_truncate_releases_every_holding_on_the_table() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.holding("b", ORDERS, "orders", &[1, 2]).await;
    s.holding("m", "SELECT * FROM team_members", "team_members", &[10])
        .await;
    s.resync(
        "a",
        FullResyncReason::TableTruncated {
            table: "ORDERS".to_owned(),
        },
    )
    .await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("m", "team_members", &[10])]),
        "the table is empty upstream, so no subscription holds a row of it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_beyond_history_releases_every_holding_once_per_connection() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1]).await;
    s.holding("m", "SELECT * FROM team_members", "team_members", &[10])
        .await;
    s.resync("a", FullResyncReason::CursorBeyondHistory).await;
    assert!(holdings(&mut s.conn).is_empty());
    s.snapshot("a", vec![inserts("orders", &[1])]).await;
    s.resync("m", FullResyncReason::CursorBeyondHistory).await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[1])]),
        "a later notice spares what an earlier replacement delivered"
    );
    s.snapshot("m", vec![inserts("team_members", &[10])]).await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[1]), ("m", "team_members", &[10])])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_writes_and_their_answers_leave_the_holdings_alone() {
    let mut s = Session::connected().await;
    s.holding("a", ORDERS, "orders", &[1, 2, 3]).await;
    diesel::insert_into(orders::table)
        .values((orders::id.eq(9), orders::quantity.eq(1)))
        .execute(s.conn.conn())
        .expect("local insert");
    diesel::update(orders::table.find(1))
        .set(orders::quantity.eq(8))
        .execute(s.conn.conn())
        .expect("local update");
    diesel::delete(orders::table.find(2))
        .execute(s.conn.conn())
        .expect("local delete");
    let applied = s.conn.push().await.expect("push").expect("a write");
    s.control(
        ControlMessage::MutationApplied(MutationApplied {
            client_seq: applied,
        }),
        "the acknowledgement",
        |event| matches!(event, ClientEvent::MutationApplied { .. }),
    )
    .await;
    diesel::update(orders::table.find(3))
        .set(orders::quantity.eq(4))
        .execute(s.conn.conn())
        .expect("a write the server refuses");
    let refused = s.conn.push().await.expect("push").expect("a write");
    s.control(
        ControlMessage::MutationReject(MutationReject {
            client_seq: refused,
            reason: MutationRejectReason::Unauthorized,
        }),
        "the rejection",
        |event| matches!(event, ClientEvent::MutationRejected { .. }),
    )
    .await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[1, 2, 3])]),
        "a local write, its push and its answer hold nothing and release nothing"
    );

    s.live(
        "a",
        Patch::default()
            .insert("orders", 9)
            .update("orders", 1, 8)
            .delete("orders", 2),
    )
    .await;
    assert_eq!(
        holdings(&mut s.conn),
        held(&[("a", "orders", &[1, 3, 9])]),
        "the echo holds the inserted row and releases the one deleted locally"
    );
}

/// A durable replica reopened at a newer major (R31), which needs a key store.
#[cfg(feature = "native-auth")]
mod rebuild {
    use connetto_client::{
        ClientBuilder, ConnettoConnection, DataDir, MemoryRefreshStore, SyncSchema,
    };
    use connetto_core::MAJOR_DIRECTIVE;
    use connetto_core::schema::SchemaBundle;
    use connetto_server::LoopbackTransport;

    use super::{DDL, ORDERS, Session, held, holdings};

    fn schema_at(major: u32) -> SyncSchema {
        SyncSchema::new(
            SchemaBundle::new(
                format!("{MAJOR_DIRECTIVE} {major}\n-- holdings"),
                "",
                DDL,
                Vec::<(String, String)>::new(),
                Vec::<String>::new(),
                None::<&str>,
            )
            .expect("the source declares its major"),
        )
    }

    async fn open_at(dir: &tempfile::TempDir, major: u32) -> ConnettoConnection<LoopbackTransport> {
        let credential = crate::support::held("holdings");
        let store = crate::support::key_store(&credential).await;
        ClientBuilder::new(
            schema_at(major),
            crate::support::NeverDial::<LoopbackTransport>::default(),
        )
        .signed_in(credential)
        .durable(
            DataDir::new(dir.path().to_path_buf()),
            store,
            MemoryRefreshStore::default(),
        )
        .open_driven()
        .await
        .expect("open with no server")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_major_rebuild_drops_the_holdings_with_the_rows() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut s = Session::attach(open_at(&dir, 1).await).await;
        s.holding("a", ORDERS, "orders", &[1]).await;
        assert_eq!(holdings(&mut s.conn), held(&[("a", "orders", &[1])]));
        drop(s.offline().await);

        let mut rebuilt = open_at(&dir, 2).await;
        assert!(
            holdings(&mut rebuilt).is_empty(),
            "the rebuild drops the synced rows, the records and their holdings together"
        );
    }
}
