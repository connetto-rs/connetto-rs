//! Needs Docker: the fixture starts its own Postgres.
//!
//! Exercises the [`ConnettoAuditSchema`] default `connetto_schema!`
//! generates: that a deployment can create the documented table, that
//! every `op` value survives a round trip through the Postgres enum, and that
//! the row a share mint names and the provider a login names are preserved.

use connetto_core::SessionId;
use connetto_server::audit::{
    AUTH_OP_TYPE, AuthEvent, AuthOp, ConnettoAuditSchema, pg_audit_hook, record_awaited,
};
use connetto_server::defaults::ConnettoDefaults;
use connetto_test_harness::Fixture;
use diesel::prelude::*;
use diesel::sql_query;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use subql::backend::Value;

use super::logging::{LogCapture, with_capture};

/// Serializes these tests. Each drops and recreates the one `auth_events`
/// table and its enum type, so concurrent runs race on the DDL. Same treatment
/// `e2e.rs` gives the fixtures it shares.
static PG_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

// The default schema over `Id = String`, matching `authn_db.rs`.
use connetto_server::defaults::{ConnettoAudit, auth_events};

/// Create the deployment-owned audit table. connetto emits no DDL, so the test
/// owns it, and the SQL mirrors `docs/architecture/08-authorization.md`.
async fn reset_audit_table(pool: &Pool<AsyncPgConnection>) {
    let mut conn = pool.get().await.expect("connection");
    for stmt in [
        "DROP TABLE IF EXISTS auth_events".to_owned(),
        format!("DROP TYPE IF EXISTS {AUTH_OP_TYPE}"),
    ] {
        sql_query(stmt).execute(&mut conn).await.expect("ddl");
    }
    connetto_test_harness::provision_audit(pool).await;
}

/// Every value the contract declares must survive Postgres.
///
/// The `op` column is a Postgres enum rather than text precisely so a value
/// outside the set is rejected by the database, which only holds if both ends
/// agree on every label. A Rust variant whose label the type does not declare
/// fails on write, and one Postgres accepts but Rust cannot decode fails on
/// read, so the round trip is the assertion that keeps the two in step.
#[tokio::test]
async fn every_auth_op_round_trips_through_postgres() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let mut conn = fixture.admin().get().await.expect("connection");

    let all = [
        AuthOp::LoggedIn,
        AuthOp::LoggedOut,
        AuthOp::SessionRevoked,
        AuthOp::EndedByOwner,
        AuthOp::EndedAll,
        AuthOp::EndedOthers,
        AuthOp::EndedByBan,
        AuthOp::TokenReplayed,
        AuthOp::CapabilityMinted,
        AuthOp::PermissionChange,
        AuthOp::ModelChange,
        AuthOp::Banned,
        AuthOp::BanLifted,
        AuthOp::WriteForked,
        AuthOp::WriteRedelivered,
        AuthOp::DataDownloaded,
        AuthOp::DataErased,
    ];
    for op in all {
        let session = SessionId::from_uuid(uuid::Uuid::new_v4());
        diesel_async::RunQueryDsl::execute(
            ConnettoAudit::audit_insert(AuthEvent::new(session, Some("alice".to_owned()), op)),
            &mut conn,
        )
        .await
        .unwrap_or_else(|err| panic!("insert {}: {err}", op.label()));

        let read: AuthOp = diesel_async::RunQueryDsl::get_result(
            auth_events::table
                .filter(auth_events::session.eq(session))
                .select(auth_events::op),
            &mut conn,
        )
        .await
        .unwrap_or_else(|err| panic!("read back {}: {err}", op.label()));
        assert_eq!(read, op, "{} did not round trip", op.label());
    }
}

/// A label Postgres does not declare must be refused rather than stored.
///
/// This is what the enum column buys over text, so it is asserted rather than
/// assumed.
#[tokio::test]
async fn an_undeclared_op_label_is_refused() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let mut conn = fixture.admin().get().await.expect("connection");

    let refused = sql_query(format!(
        "INSERT INTO auth_events (session, op) VALUES ('{}', 'grant_rejected')",
        SessionId::from_uuid(uuid::Uuid::new_v4())
    ))
    .execute(&mut conn)
    .await;
    assert!(
        refused.is_err(),
        "a label outside the declared set must be refused by Postgres"
    );
}

/// A share mint names the row it shared, and nothing else does.
#[tokio::test]
async fn a_mint_names_its_row_and_a_logout_does_not() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let mut conn = fixture.admin().get().await.expect("connection");

    let mint_session = SessionId::from_uuid(uuid::Uuid::new_v4());
    let shared_row = uuid::Uuid::new_v4();
    diesel_async::RunQueryDsl::execute(
        ConnettoAudit::audit_insert(
            AuthEvent::new(
                mint_session,
                Some("alice".to_owned()),
                AuthOp::CapabilityMinted,
            )
            .about_row("orders", vec![Value::Uuid(shared_row)])
            .about_key("key:abc"),
        ),
        &mut conn,
    )
    .await
    .expect("insert mint");

    let logout_session = SessionId::from_uuid(uuid::Uuid::new_v4());
    diesel_async::RunQueryDsl::execute(
        ConnettoAudit::audit_insert(AuthEvent::new(
            logout_session,
            Some("alice".to_owned()),
            AuthOp::LoggedOut,
        )),
        &mut conn,
    )
    .await
    .expect("insert logout");

    let (table_name, pk): (Option<String>, Option<uuid::Uuid>) =
        diesel_async::RunQueryDsl::get_result(
            auth_events::table
                .filter(auth_events::session.eq(mint_session))
                .select((auth_events::table_name, auth_events::pk)),
            &mut conn,
        )
        .await
        .expect("read mint");
    assert_eq!(table_name.as_deref(), Some("orders"));
    let subject: Option<String> = diesel_async::RunQueryDsl::get_result(
        auth_events::table
            .filter(auth_events::session.eq(mint_session))
            .select(auth_events::subject),
        &mut conn,
    )
    .await
    .expect("read the minted key");
    assert_eq!(
        subject.as_deref(),
        Some("key:abc"),
        "the mint names the key it minted"
    );
    assert_eq!(
        pk,
        Some(shared_row),
        "the key is stored as the type the table declares"
    );

    let (table_name, pk): (Option<String>, Option<uuid::Uuid>) =
        diesel_async::RunQueryDsl::get_result(
            auth_events::table
                .filter(auth_events::session.eq(logout_session))
                .select((auth_events::table_name, auth_events::pk)),
            &mut conn,
        )
        .await
        .expect("read logout");
    assert_eq!(table_name, None, "a logout names no row");
    assert_eq!(pk, None, "a logout names no row");
}

/// A login names its provider and issuer on its own row, and a logout names
/// neither.
#[tokio::test]
async fn a_login_names_its_provider_and_a_logout_does_not() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let mut conn = fixture.admin().get().await.expect("connection");

    let session = SessionId::from_uuid(uuid::Uuid::new_v4());
    for event in [
        AuthEvent::new(session, Some("alice".to_owned()), AuthOp::LoggedIn)
            .about_login("google", "https://accounts.google.com"),
        AuthEvent::new(session, Some("alice".to_owned()), AuthOp::LoggedOut),
    ] {
        diesel_async::RunQueryDsl::execute(ConnettoAudit::audit_insert(event), &mut conn)
            .await
            .expect("insert");
    }

    let rows: Vec<(AuthOp, Option<String>, Option<String>)> = diesel_async::RunQueryDsl::load(
        auth_events::table
            .filter(auth_events::session.eq(session))
            .order(auth_events::at)
            .select((auth_events::op, auth_events::provider, auth_events::issuer)),
        &mut conn,
    )
    .await
    .expect("read back");
    let login = rows
        .iter()
        .find(|(op, _, _)| *op == AuthOp::LoggedIn)
        .expect("the login row");
    assert_eq!(login.1.as_deref(), Some("google"));
    assert_eq!(login.2.as_deref(), Some("https://accounts.google.com"));
    let logout = rows
        .iter()
        .find(|(op, _, _)| *op == AuthOp::LoggedOut)
        .expect("the logout row");
    assert_eq!((logout.1.as_deref(), logout.2.as_deref()), (None, None));
}

/// The database clock stamps the row, not the emitting process.
#[tokio::test]
async fn the_database_clock_stamps_the_row() {
    #[derive(QueryableByName)]
    struct Skew {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        secs: i64,
    }
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let mut conn = fixture.admin().get().await.expect("connection");

    let session = SessionId::from_uuid(uuid::Uuid::new_v4());
    diesel_async::RunQueryDsl::execute(
        ConnettoAudit::audit_insert(AuthEvent::new(session, None, AuthOp::LoggedOut)),
        &mut conn,
    )
    .await
    .expect("insert");

    let skew: Skew = sql_query(format!(
        "SELECT CAST(EXTRACT(EPOCH FROM (now() - at)) AS BIGINT) AS secs \
         FROM auth_events WHERE session = '{session}'"
    ))
    .get_result(&mut conn)
    .await
    .expect("read stamp");
    assert!(
        skew.secs.abs() < 5,
        "the row is stamped by the database clock, skew {}s",
        skew.secs
    );
}

/// One login event for `session`, as `user` signs in.
fn login(session: SessionId, user: &str) -> AuthEvent<String> {
    AuthEvent::new(session, Some(user.to_owned()), AuthOp::LoggedIn)
}

/// The sessions of the rows landed so far, read by time, once `count` of
/// them have landed. The hook writes in the background, so this waits for
/// them, at most thirty seconds.
async fn landed_sessions(pool: &Pool<AsyncPgConnection>, count: usize) -> Vec<Option<SessionId>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let landed: Vec<Option<SessionId>> = {
            let mut conn = pool.get().await.expect("connection");
            diesel_async::RunQueryDsl::load(
                auth_events::table
                    .order(auth_events::at)
                    .select(auth_events::session),
                &mut conn,
            )
            .await
            .expect("read rows")
        };
        if landed.len() >= count {
            return landed;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{} of {count} rows landed in time",
            landed.len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// The ready-made hook lands its rows in the order the server emitted them,
/// so a trail read by time never shows a logout ahead of its login, however
/// many writes are in flight at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hook_lands_rows_in_the_order_they_were_emitted() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let hook = pg_audit_hook::<ConnettoDefaults>(fixture.admin().clone());

    let emitted: Vec<SessionId> = (0..64)
        .map(|_| SessionId::from_uuid(uuid::Uuid::new_v4()))
        .collect();
    for session in &emitted {
        hook(login(*session, "anna"));
    }
    assert_eq!(
        landed_sessions(fixture.admin(), emitted.len()).await,
        emitted.into_iter().map(Some).collect::<Vec<_>>(),
        "the rows land in emission order"
    );
}

/// Whether the capture holds a warning carrying `message` about a login row.
fn told_about_a_login(capture: &LogCapture, message: &str) -> bool {
    capture
        .lines()
        .iter()
        .any(|line| line["message"] == message && line["op"] == "logged_in")
}

/// A row the pool cannot hand a connection for waits until it can, rather
/// than being dropped while the rows behind it land, and the wait is logged.
#[tokio::test]
async fn a_row_waits_for_a_connection_rather_than_being_dropped() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            fixture.admin_url(),
        );
    let pool = Pool::builder()
        .max_size(1)
        .connection_timeout(std::time::Duration::from_millis(200))
        .build(manager)
        .await
        .expect("build the one-connection pool");
    let session = SessionId::from_uuid(uuid::Uuid::new_v4());
    let capture = with_capture(
        "a_row_waits_for_a_connection_rather_than_being_dropped",
        |capture| async move {
            let held = pool.get().await.expect("hold the only connection");
            let hook = pg_audit_hook::<ConnettoDefaults>(pool.clone());
            hook(login(session, "anna"));
            // Long enough for the writer's ask to time out and be asked again.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            drop(held);
            capture
        },
    )
    .await;
    assert_eq!(
        landed_sessions(fixture.admin(), 1).await,
        vec![Some(session)]
    );
    assert!(told_about_a_login(
        &capture,
        "audit row waiting for a connection"
    ));
}

/// A row whose insert fails is dropped and logged, and the rows behind it
/// still land.
#[tokio::test]
async fn a_row_that_cannot_be_inserted_is_dropped_and_the_next_lands() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    {
        let mut conn = fixture.admin().get().await.expect("connection");
        for statement in [
            "CREATE OR REPLACE FUNCTION refuse_mallory() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN IF NEW.user_id = 'mallory' THEN RAISE EXCEPTION 'refused'; END IF; \
             RETURN NEW; END $$",
            "CREATE TRIGGER refuse_mallory BEFORE INSERT ON auth_events \
             FOR EACH ROW EXECUTE FUNCTION refuse_mallory()",
        ] {
            sql_query(statement)
                .execute(&mut conn)
                .await
                .expect("arm the refusal");
        }
    }
    let refused = SessionId::from_uuid(uuid::Uuid::new_v4());
    let kept = SessionId::from_uuid(uuid::Uuid::new_v4());
    let admin = fixture.admin().clone();
    let (landed, capture) = with_capture(
        "a_row_that_cannot_be_inserted_is_dropped_and_the_next_lands",
        |capture| async move {
            let hook = pg_audit_hook::<ConnettoDefaults>(admin.clone());
            hook(login(refused, "mallory"));
            hook(login(kept, "anna"));
            (landed_sessions(&admin, 1).await, capture)
        },
    )
    .await;
    assert_eq!(landed, vec![Some(kept)]);
    assert!(told_about_a_login(
        &capture,
        "audit row dropped, insert failed"
    ));
}

/// A row handed to a hook whose writer has stopped, as when the runtime it
/// ran on shut down, is logged as dropped and never fails its producer.
#[test]
fn a_row_for_a_stopped_writer_is_logged_as_dropped() {
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime")
    };
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            "postgres://nobody@127.0.0.1:1/none",
        );
    // The writer's runtime shuts down once the hook is made, taking the
    // writer with it.
    let hook = runtime().block_on(async {
        pg_audit_hook::<ConnettoDefaults>(Pool::builder().build_unchecked(manager))
    });
    let capture = runtime().block_on(with_capture(
        "a_row_for_a_stopped_writer_is_logged_as_dropped",
        |capture| async move {
            hook(login(SessionId::from_uuid(uuid::Uuid::new_v4()), "anna"));
            capture
        },
    ));
    assert!(told_about_a_login(
        &capture,
        "audit row dropped, the writer has stopped"
    ));
}

/// A download names the query list it ran and the archive's size, and a
/// staff request with no session behind it leaves the session empty and
/// names who acted.
#[tokio::test]
async fn a_download_names_its_scope_and_a_staff_row_names_who_acted() {
    type Row = (
        Option<SessionId>,
        Option<String>,
        AuthOp,
        Option<String>,
        Option<i64>,
        Option<String>,
    );
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let mut conn = fixture.admin().get().await.expect("connection");

    let session = SessionId::from_uuid(uuid::Uuid::new_v4());
    for event in [
        AuthEvent::new(session, Some("anna".to_owned()), AuthOp::DataDownloaded)
            .about_download("ab12", 4096),
        AuthEvent::by_operator(Some("anna".to_owned()), AuthOp::DataErased, "staff:marco"),
    ] {
        diesel_async::RunQueryDsl::execute(ConnettoAudit::audit_insert(event), &mut conn)
            .await
            .expect("insert");
    }

    let mut rows: Vec<Row> = diesel_async::RunQueryDsl::load(
        auth_events::table.select((
            auth_events::session,
            auth_events::user_id,
            auth_events::op,
            auth_events::query_list_hash,
            auth_events::archive_bytes,
            auth_events::acted_by,
        )),
        &mut conn,
    )
    .await
    .expect("read rows");
    rows.sort_by_key(|row| row.2.label());
    assert_eq!(
        rows,
        vec![
            (
                Some(session),
                Some("anna".to_owned()),
                AuthOp::DataDownloaded,
                Some("ab12".to_owned()),
                Some(4096),
                None,
            ),
            (
                None,
                Some("anna".to_owned()),
                AuthOp::DataErased,
                None,
                None,
                Some("staff:marco".to_owned()),
            ),
        ]
    );
}

/// An awaited row that cannot be written fails its caller, where the hook
/// would only log it.
#[tokio::test]
async fn an_awaited_row_that_cannot_be_written_fails_the_caller() {
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    reset_audit_table(fixture.admin()).await;
    let event = AuthEvent::by_operator(Some("anna".to_owned()), AuthOp::DataErased, "staff:marco");

    record_awaited::<ConnettoDefaults>(fixture.admin(), event.clone())
        .await
        .expect("the row is written while the table exists");
    let mut conn = fixture.admin().get().await.expect("connection");
    sql_query("DROP TABLE auth_events")
        .execute(&mut conn)
        .await
        .expect("drop");
    assert!(
        record_awaited::<ConnettoDefaults>(fixture.admin(), event)
            .await
            .is_err(),
        "a row that cannot be written is an error"
    );
}
