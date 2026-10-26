//! Needs Docker: the fixture starts its own Postgres.
//!
//! The built server's session surfaces (R105 decisions 3, 4 and 7). A person
//! lists their signed-in places, a device among them, ends one, ends every
//! other one or all of them, and the liveness check refuses each session the
//! moment it ends. Ending a device's entry revokes its key and closes it with
//! `DeviceRevoked`, while ending several sessions revokes no key.

use std::time::{Duration, Instant};

use connetto_core::device_cert::KeyId;
use connetto_core::messages::{ControlMessage, FatalError, FatalErrorReason};
use connetto_server::audit::{AuthEvent, AuthOp, ConnettoAuditSchema};
use connetto_server::defaults::ConnettoAudit;
use connetto_server::device_cert::DeviceCertConfig;
use connetto_server::{InstalledApp, LivenessError, MintedKey, ServerHandle, ServerParts};
use connetto_test_harness::{Fixture, isolated_session_keyring};
use diesel::QueryableByName;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl as _;
use diesel_async::pooled_connection::bb8::Pool;
use rcgen::PublicKeyData as _;
use subql::backend::Value;
use tokio::net::TcpListener;

use super::device_identity::{challenge_nonce, enrol_request};
use super::e2e::{PG_SERIAL, audit_ops, mint_tokens, reset_fixture};
use super::enrolment::{device_key, rooted_issuer};
use super::lifecycle::{admin_pool, builder_over, live_session, next_control, wait_ready};

/// Record that `session` of `user` minted `subject` over `orders` row `row`,
/// as a mint through `CapabilityIssuer` records it.
async fn minted(
    pool: &Pool<AsyncPgConnection>,
    session: connetto_server::SessionId,
    user: &str,
    subject: &str,
    row: uuid::Uuid,
) {
    diesel_async::RunQueryDsl::execute(
        ConnettoAudit::audit_insert(
            AuthEvent::new(session, Some(user.to_owned()), AuthOp::CapabilityMinted)
                .about_row("orders", vec![Value::Uuid(row)])
                .about_key(subject),
        ),
        &mut pool.get().await.expect("a connection"),
    )
    .await
    .expect("record the mint");
}

/// How many provider token rows `session` still has.
async fn provider_tokens(
    pool: &Pool<AsyncPgConnection>,
    session: connetto_server::SessionId,
) -> i64 {
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let count: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM connetto_provider_tokens WHERE session_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(session.as_uuid())
    .get_result(&mut pool.get().await.expect("a connection"))
    .await
    .expect("count the provider tokens");
    count.n
}
/// A server with device identity, serving on its own port, and the admin
/// pool to read its tables.
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
    connetto_test_harness::stack::provision_enrolment_tables(fixture).await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a listener");
    let port = listener.local_addr().expect("local address").port();
    let (builder, idp, keys) = builder_over(fixture, port).await;
    let (_, _, issuer) = rooted_issuer();
    let ServerParts {
        router,
        change_stream,
        handle,
        ..
    } = builder
        .device_identity(Some(DeviceCertConfig::new(issuer)))
        .build()
        .await
        .expect("the deployment assembles");
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

/// The reason the connection's last frame gave.
async fn closed_with(
    client: &mut connetto_server::WebSocketTransport<tokio::net::TcpStream>,
) -> FatalErrorReason {
    loop {
        if let ControlMessage::FatalError(FatalError { reason }) = next_control(client).await {
            return reason;
        }
    }
}

/// Wait until the audit table holds every op in `expected`, then return it.
async fn audit_settles(pool: &Pool<AsyncPgConnection>, expected: &[&str]) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let ops = audit_ops(pool).await;
        if expected.iter().all(|want| ops.iter().any(|op| op == want)) {
            return ops;
        }
        assert!(
            Instant::now() < deadline,
            "the audit rows never landed: {ops:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(QueryableByName)]
struct Revoked {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    revoked: bool,
}

#[tokio::test]
async fn ending_a_device_entry_revokes_its_key_and_spares_the_others() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;
    let handle = &served.handle;

    let (laptop, _, _) = mint_tokens(&served.base, "milan").await;
    let (phone, user, _) = mint_tokens(&served.base, "milan").await;
    let (stranger, _, _) = mint_tokens(&served.base, "berlin").await;
    let laptop_session = handle.verify_live(&laptop).await.expect("live").session_id;
    let phone_session = handle.verify_live(&phone).await.expect("live").session_id;
    let stranger_session = handle
        .verify_live(&stranger)
        .await
        .expect("live")
        .session_id;

    let mut on_phone = live_session(&served.addr, &phone).await;
    let key = device_key();
    let nonce = challenge_nonce(&mut on_phone).await;
    let grant = enrol_request(&mut on_phone, &key, nonce, None).await;
    assert!(matches!(grant, ControlMessage::EnrolGrant(_)), "{grant:?}");
    let key_id = KeyId::of_public_key(&key.subject_public_key_info());

    let places = handle.signed_in_places(&user).await.expect("list");
    assert_eq!(
        places.len(),
        2,
        "the person's two sessions, not the stranger's"
    );
    let phone_place = places
        .iter()
        .find(|place| place.session.session_id == phone_session)
        .expect("the phone is listed");
    assert_eq!(
        phone_place.device.as_ref().map(|device| device.key),
        Some(key_id)
    );
    let laptop_place = places
        .iter()
        .find(|place| place.session.session_id == laptop_session)
        .expect("the laptop is listed");
    assert!(
        laptop_place.device.is_none(),
        "a browser shows as a session alone"
    );

    let ended = handle
        .end_session(&user, stranger_session)
        .await
        .expect("end");
    assert!(
        ended.sessions.is_empty() && ended.share_keys.is_empty(),
        "a session of someone else ends nothing: {ended:?}"
    );
    assert!(handle.verify_live(&stranger).await.is_ok());

    assert_eq!(
        handle
            .end_session(&user, phone_session)
            .await
            .expect("end")
            .sessions,
        vec![phone_session]
    );
    assert_eq!(
        closed_with(&mut on_phone).await,
        FatalErrorReason::DeviceRevoked,
        "a device entry closes as a revoked device, so the device deletes its replica"
    );
    let key_revoked: Revoked = diesel::sql_query(
        "SELECT revoked_at IS NOT NULL AS revoked FROM connetto_device_enrolments WHERE key_id = $1",
    )
    .bind::<diesel::sql_types::Bytea, _>(key_id.as_bytes().to_vec())
    .get_result(&mut served.pool.get().await.expect("a connection"))
    .await
    .expect("read the enrolment");
    assert!(
        key_revoked.revoked,
        "ending the device's entry revokes its key"
    );
    assert!(matches!(
        handle.verify_live(&phone).await,
        Err(LivenessError::Revoked)
    ));
    assert!(
        handle.verify_live(&laptop).await.is_ok(),
        "the other place keeps working"
    );
    assert!(
        audit_settles(&served.pool, &["ended_by_owner"])
            .await
            .contains(&"ended_by_owner".to_owned())
    );
}

#[tokio::test]
async fn ending_the_others_and_then_all_ends_sessions_and_revokes_no_key() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;
    let handle = &served.handle;

    let (current, user, _) = mint_tokens(&served.base, "milan").await;
    let (phone, _, _) = mint_tokens(&served.base, "milan").await;
    let (elsewhere, _, elsewhere_refresh) = mint_tokens(&served.base, "milan").await;
    let current_session = handle.verify_live(&current).await.expect("live").session_id;

    let mut on_phone = live_session(&served.addr, &phone).await;
    let key = device_key();
    let nonce = challenge_nonce(&mut on_phone).await;
    let grant = enrol_request(&mut on_phone, &key, nonce, None).await;
    assert!(matches!(grant, ControlMessage::EnrolGrant(_)), "{grant:?}");
    let mut on_current = live_session(&served.addr, &current).await;

    assert_eq!(
        handle
            .end_other_sessions(&user, current_session)
            .await
            .expect("end the others")
            .sessions
            .len(),
        2
    );
    assert_eq!(
        closed_with(&mut on_phone).await,
        FatalErrorReason::SessionRevoked,
        "a sign-out of the others ends the session and keeps the device"
    );
    for ended in [&phone, &elsewhere] {
        assert!(matches!(
            handle.verify_live(ended).await,
            Err(LivenessError::Revoked)
        ));
    }
    assert!(
        handle.verify_live(&current).await.is_ok(),
        "the current place stays"
    );
    assert_eq!(
        super::ban_doors::refresh_status(&served.base, &elsewhere_refresh).await,
        reqwest::StatusCode::UNAUTHORIZED,
        "an ended session's refresh token renews nothing"
    );

    assert_eq!(
        handle
            .end_all_sessions(&user)
            .await
            .expect("end all")
            .sessions,
        vec![current_session]
    );
    assert_eq!(
        closed_with(&mut on_current).await,
        FatalErrorReason::SessionRevoked
    );
    assert!(matches!(
        handle.verify_live(&current).await,
        Err(LivenessError::Revoked)
    ));
    assert!(
        handle
            .signed_in_places(&user)
            .await
            .expect("list")
            .is_empty(),
        "no place is left signed in"
    );

    let key_revoked: Revoked = diesel::sql_query(
        "SELECT revoked_at IS NOT NULL AS revoked FROM connetto_device_enrolments WHERE key_id = $1",
    )
    .bind::<diesel::sql_types::Bytea, _>(
        KeyId::of_public_key(&key.subject_public_key_info())
            .as_bytes()
            .to_vec(),
    )
    .get_result(&mut served.pool.get().await.expect("a connection"))
    .await
    .expect("read the enrolment");
    assert!(
        !key_revoked.revoked,
        "ending several sessions revokes no device key"
    );

    let ops = audit_settles(&served.pool, &["ended_others", "ended_all"]).await;
    let count = |label: &str| ops.iter().filter(|op| op.as_str() == label).count();
    assert_eq!(count("ended_others"), 2, "one row per ended session");
    assert_eq!(count("ended_all"), 1);
}

#[tokio::test]
async fn the_liveness_check_refuses_what_is_not_a_live_login() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;

    let (token, user, _) = mint_tokens(&served.base, "milan").await;
    let verified = served.handle.verify_live(&token).await.expect("live");
    assert_eq!(verified.context.user_id, user);
    assert!(matches!(
        served.handle.verify_live("not-a-token").await,
        Err(LivenessError::Refused)
    ));
}

#[tokio::test]
async fn ending_sessions_answers_what_they_left_behind_and_forgets_their_provider_tokens() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let served = served(&fixture).await;
    let handle = &served.handle;

    let (phone, user, _) = mint_tokens(&served.base, "milan").await;
    let (laptop, _, _) = mint_tokens(&served.base, "milan").await;
    let phone_session = handle.verify_live(&phone).await.expect("live").session_id;
    let laptop_session = handle.verify_live(&laptop).await.expect("live").session_id;
    let (shared, also_shared) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    minted(&served.pool, phone_session, &user, "key:berlin", shared).await;
    minted(
        &served.pool,
        laptop_session,
        &user,
        "key:milan",
        also_shared,
    )
    .await;
    let bot = uuid::Uuid::new_v4();
    for statement in [
        format!(
            "INSERT INTO connetto_bots (id, owner, name, public_key, list_version) \
             VALUES ('{bot}', 'someone', 'Reminders', decode(repeat('00', 32), 'hex'), 1)"
        ),
        format!(
            "INSERT INTO connetto_installations (id, bot, owner, accepted_version) \
             VALUES ('mine', '{bot}', '{user}', 1), ('theirs', '{bot}', 'someone', 1)"
        ),
    ] {
        super::e2e::exec(&served.pool, &statement).await;
    }
    assert_eq!(provider_tokens(&served.pool, phone_session).await, 1);

    let ended = handle.end_session(&user, phone_session).await.expect("end");
    assert_eq!(ended.sessions, vec![phone_session]);
    assert_eq!(
        ended.share_keys,
        vec![MintedKey {
            session: phone_session,
            subject: "key:berlin".to_owned(),
            table: "orders".to_owned(),
            row: Some(shared),
        }],
        "the key the ended session minted, and not the laptop's"
    );
    assert_eq!(
        ended.installations,
        vec![InstalledApp {
            installation: "mine".to_owned(),
            app: bot,
            app_name: "Reminders".to_owned(),
        }],
        "every installation of the person, and nobody else's"
    );
    assert_eq!(
        provider_tokens(&served.pool, phone_session).await,
        0,
        "an ended session's provider tokens are forgotten"
    );
    assert_eq!(provider_tokens(&served.pool, laptop_session).await, 1);

    let ended = handle.end_all_sessions(&user).await.expect("end all");
    assert_eq!(ended.sessions, vec![laptop_session]);
    assert_eq!(
        ended
            .share_keys
            .iter()
            .map(|key| key.subject.as_str())
            .collect::<Vec<_>>(),
        vec!["key:milan"]
    );
    assert_eq!(provider_tokens(&served.pool, laptop_session).await, 0);
}
