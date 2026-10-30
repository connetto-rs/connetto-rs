//! A `NUMERIC(p,s)` column through a running server.
//!
//! pg2sqlite declares such a column in the replica as an `INTEGER` of minor
//! units, `value × 10^s`, and translates the replica's queries on that basis.
//! So the snapshot, a live change and the rows a client writes all carry
//! `12.50` at scale 2 as `1250`, a subscription's filter selects on the same
//! scale, and a table keyed by such a column pages and authorizes by its key.
//!
//! Needs Docker, because the fixture starts its own Postgres.

use std::sync::Arc;
use std::time::Duration;

use connetto_core::messages::{BulkMessage, ControlMessage};
use connetto_core::traits::IncomingFrame;
use connetto_server::{
    AbuseConfig, PgSnapshotSource, RequestGuard, RlsAuth, RuntimeWritableCatalog, ThrottleConfig,
    TierLimits,
};
use connetto_test_harness::{
    Client, Fixture, HarnessAuth, PUBLICATION, RowValue, Server, ServerConfig, drop_slot,
    insert_changeset, spawn_server,
};
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;
use sqlite_diff_rs::{ParsedDiffSet, PatchsetOp, Value};

const PG_DDL: &str = "-- connetto:major 1\n\
                      CREATE TABLE prices (id INT PRIMARY KEY, amount NUMERIC(10,2));\
                      CREATE TABLE ledger (k NUMERIC(10,2) PRIMARY KEY, n INT);";
/// How long a frame that must arrive is given.
const DELIVERY: Duration = Duration::from_secs(30);

/// The rows one zstd-compressed patchset inserts, each as its wire values. A
/// delivery carrying no rows is not a patchset at all, so it holds none.
fn inserted(payload: &[u8]) -> Vec<Vec<RowValue>> {
    let raw = zstd::decode_all(payload).expect("decompress the patchset");
    let ParsedDiffSet::Patchset(set) = ParsedDiffSet::parse(&raw).expect("parse the patchset")
    else {
        return Vec::new();
    };
    set.iter()
        .map(|op| match op {
            PatchsetOp::Insert { values, .. } => values.to_vec(),
            other => panic!("expected an insert, got {other:?}"),
        })
        .collect()
}

/// Drain one subscription's initial read, acknowledging each page, and return
/// its rows page by page.
async fn drain_pages(client: &mut Client, sub_id: &str) -> Vec<Vec<Vec<RowValue>>> {
    let ControlMessage::SnapshotBegin(begin) = client.next_control().await else {
        panic!("expected a snapshot begin");
    };
    assert_eq!(begin.sub_id, sub_id);
    let mut pages = Vec::new();
    loop {
        match client.recv().await {
            Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(patch))) => {
                assert_eq!(patch.sub_id, sub_id);
                pages.push(inserted(&patch.patchset_zstd));
                client.ack_credits(1).await;
            }
            Some(IncomingFrame::Control(ControlMessage::SnapshotEnd(end))) => {
                assert_eq!(end.sub_id, sub_id);
                return pages;
            }
            other => panic!("expected a page or the end of the read, got {other:?}"),
        }
    }
}

/// The one text cell `sql` answers, read on the admin pool.
async fn pg_text(fixture: &Fixture, sql: &str) -> String {
    #[derive(QueryableByName)]
    struct Cell {
        #[diesel(sql_type = diesel::sql_types::Text)]
        cell: String,
    }
    let mut conn = fixture.admin().get().await.expect("an admin connection");
    let cell: Cell = diesel::sql_query(sql)
        .get_result(&mut *conn)
        .await
        .expect("the read runs");
    cell.cell
}

/// The two tables with their seed rows, and a server reading them under a
/// page budget small enough that `ledger` takes several pages.
async fn stack(fixture: &Fixture) -> Server {
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS prices CASCADE",
            "DROP TABLE IF EXISTS ledger CASCADE",
            PG_DDL,
            "INSERT INTO prices (id, amount) VALUES (1, 0.14), (2, 12.50), (3, 7), (4, NULL)",
            "INSERT INTO ledger (k, n) SELECT g * 0.25, g FROM generate_series(1, 200) AS g",
            "ANALYZE ledger",
        ])
        .await;
    let admin = fixture.admin().clone();
    let snapshot = PgSnapshotSource::from_ddl(admin.clone(), PG_DDL)
        .expect("snapshot source")
        .with_publication(PUBLICATION);
    let auth = HarnessAuth::rls(RlsAuth::from_ddl(admin.clone(), PG_DDL).expect("rls auth"));
    let guard = Arc::new(RequestGuard::new(
        ThrottleConfig::default().with_identified(TierLimits::identified().with_page_bytes(512)),
        AbuseConfig::default(),
    ));
    let config = ServerConfig::new(PG_DDL, fixture.admin_url())
        .with_writable(
            RuntimeWritableCatalog::builder()
                .writable("prices")
                .writable("ledger")
                .build(),
        )
        .with_guard(guard)
        .with_replication(["prices", "ledger"]);
    spawn_server(config, snapshot, auth, admin.clone(), admin).await
}

/// Upload one insert as client sequence `seq` and wait for it to apply.
async fn write(
    client: &mut Client,
    seq: u64,
    table: &str,
    columns: &[&str],
    values: Vec<RowValue>,
) {
    client
        .upload(seq, insert_changeset(table, columns, &[0], values))
        .await;
    let ControlMessage::MutationApplied(ack) = client.next_control().await else {
        panic!("the {table} write applies");
    };
    assert_eq!(ack.client_seq, seq);
}

/// Sort rows by their first cell, an integer.
fn by_first(mut rows: Vec<Vec<RowValue>>) -> Vec<Vec<RowValue>> {
    rows.sort_by_key(|row| match row[0] {
        Value::Integer(first) => first,
        _ => i64::MAX,
    });
    rows
}

/// A scaled decimal column carries minor units in the snapshot, in a live
/// change and in a client's write, and a filter in the replica's terms selects
/// the rows Postgres holds on the same scale.
#[tokio::test]
async fn a_scaled_decimal_column_travels_as_minor_units_both_ways() {
    let fixture = Fixture::acquire().await;
    let server = stack(&fixture).await;
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;

    // `amount > 100` is a dollar in the replica's minor units, so Postgres
    // answers `amount > 1.00` and the cheaper row stays out.
    client
        .subscribe("dear", "SELECT * FROM prices WHERE amount > 100")
        .await;
    let dear = drain_pages(&mut client, "dear").await.concat();
    assert_eq!(
        by_first(dear),
        vec![
            vec![Value::Integer(2), Value::Integer(1250)],
            vec![Value::Integer(3), Value::Integer(700)],
        ],
        "the snapshot carries minor units and the filter selects on them"
    );

    // One commit holding a row the filter leaves out and one it takes.
    fixture
        .exec("INSERT INTO prices (id, amount) VALUES (5, 0.50), (6, 42.10)")
        .await;
    let live = client.wait_for_live(DELIVERY).await;
    assert_eq!(
        inserted(&live.patchset_zstd),
        vec![vec![Value::Integer(6), Value::Integer(4210)]],
        "a live change carries minor units and the filter selects on them"
    );

    write(
        &mut client,
        1,
        "prices",
        &["id", "amount"],
        vec![Value::Integer(7), Value::Integer(9999)],
    )
    .await;
    assert_eq!(
        pg_text(
            &fixture,
            "SELECT amount::text AS cell FROM prices WHERE id = 7"
        )
        .await,
        "99.99",
        "the replica writes minor units and Postgres holds the decimal"
    );

    drop(client);
    drop(server);
    drop_slot(fixture.admin()).await;
}

/// A table keyed by a scaled decimal pages by the decimal, its live changes
/// pass the row check by their key, and a client's write lands under the key
/// its minor units stand for.
#[tokio::test]
async fn a_table_keyed_by_a_scaled_decimal_pages_streams_and_takes_writes() {
    let fixture = Fixture::acquire().await;
    let server = stack(&fixture).await;
    let mut client = server.connect();
    client.handshake_with("alice", "user:alice").await;

    // Every page resumes past the last key it delivered, compared as the
    // decimal Postgres holds, so each of the 200 rows arrives exactly once.
    client.subscribe("ledger", "SELECT * FROM ledger").await;
    let pages = drain_pages(&mut client, "ledger").await;
    assert!(
        pages.len() > 1,
        "a 200 row read under a 512 byte budget takes several pages, took {}",
        pages.len()
    );
    assert_eq!(
        by_first(pages.concat()),
        (1..=200)
            .map(|g| vec![Value::Integer(g * 25), Value::Integer(g)])
            .collect::<Vec<_>>(),
        "every row arrives once, its key as minor units"
    );

    fixture
        .exec("INSERT INTO ledger (k, n) VALUES (60.00, 201)")
        .await;
    let live = client.wait_for_live(DELIVERY).await;
    assert_eq!(
        inserted(&live.patchset_zstd),
        vec![vec![Value::Integer(6000), Value::Integer(201)]],
        "a live change to a decimal-keyed row carries its key as minor units"
    );

    write(
        &mut client,
        1,
        "ledger",
        &["k", "n"],
        vec![Value::Integer(100_000), Value::Integer(202)],
    )
    .await;
    assert_eq!(
        pg_text(&fixture, "SELECT k::text AS cell FROM ledger WHERE n = 202").await,
        "1000.00",
        "the key's minor units land as the decimal they stand for"
    );

    drop(client);
    drop(server);
    drop_slot(fixture.admin()).await;
}
