//! Needs Docker: the fixture starts its own Postgres.
//!
//! Write attribution (R105 decision 6). Every write binds its writer and
//! sequence for the deployment to stamp on its rows, and `writes_of` names the
//! sequences each session wrote under each writer, so an application tells one
//! login's writes from another's on the same writer and finds their rows.

use connetto_core::PROTOCOL_VERSION;
use connetto_core::SessionId;
use connetto_core::messages::{
    BulkMessage, ControlMessage, Grant, Handshake, HandshakeAck, MutationHeader, MutationPatch,
    WriterGrant, WriterRequest,
};
use connetto_core::traits::Transport as _;
use connetto_server::{ServerHandle, ServerParts, SessionWrites, WebSocketTransport};
use connetto_test_harness::{
    Fixture, RowValue, insert_changeset, isolated_session_keyring, issue_writer, number_writer,
};
use diesel::QueryableByName;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl as _;
use diesel_async::pooled_connection::bb8::Pool;
use tokio::net::{TcpListener, TcpStream};

use super::e2e::{PG_SERIAL, exec, mint_tokens, reset_fixture};
use super::lifecycle::{admin_pool, builder_over, next_control, served_major, wait_ready};

/// A deployment stamping every inserted order with the writer and sequence
/// the write bound, the way decision 6 leaves finding rows to the deployment.
const STAMPING: [&str; 4] = [
    "DROP TABLE IF EXISTS write_stamps",
    "CREATE TABLE write_stamps (id BIGINT PRIMARY KEY, writer BIGINT NOT NULL, seq BIGINT NOT NULL)",
    "CREATE OR REPLACE FUNCTION stamp_write() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$ \
     BEGIN INSERT INTO write_stamps VALUES (NEW.id, current_setting('app.writer')::bigint, \
     current_setting('app.writer_seq')::bigint); RETURN NEW; END $$",
    "CREATE TRIGGER orders_stamp AFTER INSERT ON orders FOR EACH ROW EXECUTE FUNCTION stamp_write()",
];

struct Served {
    pool: Pool<AsyncPgConnection>,
    handle: ServerHandle,
    base: String,
    addr: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Served {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn served(fixture: &Fixture) -> Served {
    let pool = admin_pool(fixture).await;
    reset_fixture(&pool, fixture).await;
    fixture.provision_auth_tables().await;
    for statement in STAMPING {
        exec(&pool, statement).await;
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, idp, keys) = builder_over(fixture, port).await;
    let ServerParts {
        router,
        change_stream,
        handle,
        ..
    } = builder.build().await.expect("the deployment assembles");
    let stream = tokio::spawn(async move {
        let _ = change_stream.await;
    });
    let http = tokio::spawn(async move {
        let _keep = (idp, keys);
        let _ = axum::serve(listener, router).await;
    });
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base).await;
    Served {
        pool,
        handle,
        base,
        addr: format!("127.0.0.1:{port}"),
        tasks: vec![stream, http],
    }
}

/// Open a session with `token` asking for `writer`, answering the number
/// writer the ack grants.
async fn connect(
    addr: &str,
    token: &str,
    writer: WriterRequest,
) -> (WebSocketTransport<TcpStream>, Option<WriterGrant>) {
    let tcp = TcpStream::connect(addr)
        .await
        .expect("connect the sync route");
    let mut client = WebSocketTransport::connect(&format!("ws://{addr}/sync"), tcp)
        .await
        .expect("the axum adapter answers the WebSocket handshake");
    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, "attribution")
                .with_major(served_major())
                .with_grant(Grant::new(token))
                .with_writer(writer),
        ))
        .await
        .expect("send the handshake");
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    match next_control(&mut client).await {
        ControlMessage::HandshakeAck(HandshakeAck { writer, .. }) => (client, writer),
        other => panic!("the handshake answered {other:?}"),
    }
}

/// Insert order `id` as sequence `seq` and wait for it to apply.
async fn write(client: &mut WebSocketTransport<TcpStream>, seq: u64, id: i64) {
    let changeset = insert_changeset(
        "orders",
        &["id", "price", "quantity", "status"],
        &[0],
        vec![
            RowValue::Integer(id),
            RowValue::Real(1.0),
            RowValue::Integer(1),
            RowValue::Text("attributed".to_owned()),
        ],
    );
    client
        .send_control(ControlMessage::MutationHeader(MutationHeader::new(seq, 1)))
        .await
        .expect("send the mutation header");
    client
        .send_bulk(BulkMessage::MutationPatch(MutationPatch::new(
            seq,
            zstd::encode_all(changeset.as_slice(), 3).expect("compress"),
        )))
        .await
        .expect("send the mutation patch");
    match next_control(client).await {
        ControlMessage::MutationApplied(applied) => assert_eq!(applied.client_seq, seq),
        other => panic!("the write answered {other:?}"),
    }
}

/// The session `token` names.
async fn session_of(handle: &ServerHandle, token: &str) -> SessionId {
    handle
        .verify_live(token)
        .await
        .expect("the login is live")
        .session_id
}

/// Every stamped order as `(id, writer, seq)`, in id order.
async fn stamps(pool: &Pool<AsyncPgConnection>) -> Vec<(i64, i64, i64)> {
    #[derive(QueryableByName)]
    struct Stamp {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        writer: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        seq: i64,
    }
    let mut conn = pool.get().await.expect("a connection");
    diesel::sql_query("SELECT id, writer, seq FROM write_stamps ORDER BY id")
        .load::<Stamp>(&mut *conn)
        .await
        .expect("read the stamps")
        .into_iter()
        .map(|stamp| (stamp.id, stamp.writer, stamp.seq))
        .collect()
}

#[tokio::test]
async fn writes_of_names_the_sequences_each_login_wrote_and_the_rows_carry_them() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;
    let handle = &served.handle;

    let (first, _, _) = mint_tokens(&served.base, "milan").await;
    let (second, _, _) = mint_tokens(&served.base, "milan").await;
    let (idle, _, _) = mint_tokens(&served.base, "milan").await;

    // The first login writes two orders under a fresh number.
    let (mut client, grant) = connect(&served.addr, &first, issue_writer()).await;
    let number = grant
        .expect("an identified caller is issued a number")
        .number;
    write(&mut client, 1, 1001).await;
    write(&mut client, 2, 1002).await;
    drop(client);

    // A second login of the same person continues that number, as a browser
    // does after signing in again, and writes one more.
    let (mut client, _) = connect(&served.addr, &second, number_writer(number)).await;
    write(&mut client, 3, 1003).await;
    drop(client);

    // A third binds the number and writes nothing.
    let (_client, _) = connect(&served.addr, &idle, number_writer(number)).await;

    let wrote = |seqs| {
        vec![SessionWrites {
            writer: number,
            device: None,
            retired: false,
            seqs,
        }]
    };
    let first = session_of(handle, &first).await;
    let second = session_of(handle, &second).await;
    let idle = session_of(handle, &idle).await;
    assert_eq!(
        handle.writes_of(first).await.expect("read the first login"),
        wrote(Some(1..=2)),
        "the first login wrote up to the second's bind"
    );
    assert_eq!(
        handle
            .writes_of(second)
            .await
            .expect("read the second login"),
        wrote(Some(3..=3)),
        "the second login wrote up to the third's bind"
    );
    assert_eq!(
        handle.writes_of(idle).await.expect("read the idle login"),
        wrote(None),
        "a login that bound and wrote nothing names its writer with no range"
    );
    assert_eq!(
        handle
            .writes_of(SessionId::from_uuid(uuid::Uuid::new_v4()))
            .await
            .expect("read an unknown session"),
        Vec::new(),
        "a session that bound nothing wrote nothing"
    );

    let number = i64::try_from(number).expect("a stored number");
    assert_eq!(
        stamps(&served.pool).await,
        vec![(1001, number, 1), (1002, number, 2), (1003, number, 3)],
        "every row carries the writer and sequence its write bound"
    );
}
