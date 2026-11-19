//! Session-ending transactions over the Docker fixture's Postgres.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use connetto_core::SessionId;
use connetto_server::defaults::{ConnettoDefaults, connetto_sessions};
use connetto_server::{
    AuthConfig, AuthError, AuthHooks, CommittedNotice, DbAuthStore, EndCause, HookError,
    HookFuture, NewBan, SessionsEnded, VerifiedClaims, pg_ban_store,
};
use connetto_test_harness::Fixture;
use diesel::{ExpressionMethods as _, QueryDsl as _};
use diesel_async::{AsyncPgConnection, RunQueryDsl as _};
use tokio::sync::mpsc;

use super::auth_hooks::{Notice, Told, count, identity, origin, service};

const BOUND: Duration = Duration::from_secs(30);

diesel::table! {
    hook_ends (session) {
        session -> Uuid,
        owner -> Text,
    }
}

/// Hooks that witness revoked rows and write in the ending transaction.
struct EndingHooks {
    told: Arc<Told<ConnettoDefaults>>,
    refuse: AtomicBool,
    refuse_erasure: AtomicBool,
    seen: Mutex<Vec<(String, Vec<SessionId>, EndCause)>>,
    attempted: tokio::sync::Notify,
}

impl EndingHooks {
    /// The hooks and their committed notices.
    fn channel() -> (Arc<Self>, mpsc::UnboundedReceiver<Notice<ConnettoDefaults>>) {
        let (told, notices) = Told::channel();
        (
            Arc::new(Self {
                told,
                refuse: AtomicBool::new(true),
                refuse_erasure: AtomicBool::new(false),
                seen: Mutex::new(Vec::new()),
                attempted: tokio::sync::Notify::new(),
            }),
            notices,
        )
    }
}

impl AuthHooks<ConnettoDefaults> for EndingHooks {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        self.told.user_for(transaction, claims)
    }

    fn sessions_ended<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        ended: &'a SessionsEnded<'a, String>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            self.seen.lock().expect("hook observations").push((
                ended.user_id.clone(),
                ended.sessions.to_vec(),
                ended.cause,
            ));
            assert_ne!(ended.sessions.len(), 0, "an end names its sessions");
            for session in ended.sessions {
                let (owner, revoked): (String, bool) = connetto_sessions::table
                    .find(session.as_uuid())
                    .select((connetto_sessions::user_id, connetto_sessions::revoked))
                    .first(transaction)
                    .await
                    .map_err(|error| HookError::Backend(error.to_string()))?;
                assert_eq!(&owner, ended.user_id);
                assert!(revoked);
                diesel::insert_into(hook_ends::table)
                    .values((
                        hook_ends::session.eq(session.as_uuid()),
                        hook_ends::owner.eq(owner),
                    ))
                    .execute(transaction)
                    .await
                    .map_err(|error| HookError::Backend(error.to_string()))?;
            }
            self.attempted.notify_one();
            if self.refuse.load(Ordering::Acquire) {
                return Err(HookError::Refused("keep sessions".to_owned()));
            }
            Ok(())
        })
    }

    fn person_erased<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        person: &'a String,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            use connetto_server::defaults::connetto_device_enrolments;
            let live: i64 = connetto_sessions::table
                .filter(connetto_sessions::user_id.eq(person))
                .filter(connetto_sessions::revoked.eq(false))
                .count()
                .get_result(transaction)
                .await
                .map_err(|error| HookError::Backend(error.to_string()))?;
            assert_eq!(live, 0);
            let live_keys: i64 = connetto_device_enrolments::table
                .filter(connetto_device_enrolments::user_id.eq(person))
                .filter(connetto_device_enrolments::revoked_at.is_null())
                .count()
                .get_result(transaction)
                .await
                .map_err(|error| HookError::Backend(error.to_string()))?;
            assert_eq!(live_keys, 0);
            if self.refuse_erasure.load(Ordering::Acquire) {
                return Err(HookError::Refused("keep person".to_owned()));
            }
            Ok(())
        })
    }

    fn committed(&self, notice: Notice<ConnettoDefaults>) {
        self.told.committed(notice);
    }
}

/// Provision the application's transactional writes.
async fn provision(fixture: &Fixture) {
    fixture
        .setup(&["CREATE TABLE hook_ends (session UUID PRIMARY KEY, owner TEXT NOT NULL)"])
        .await;
}

/// Receive exactly one committed event within the failure bound.
async fn notice(
    notices: &mut mpsc::UnboundedReceiver<Notice<ConnettoDefaults>>,
) -> Notice<ConnettoDefaults> {
    tokio::time::timeout(BOUND, notices.recv())
        .await
        .expect("event bound")
        .expect("event channel")
}

/// Extract the session created by the preceding login.
async fn signed_in(notices: &mut mpsc::UnboundedReceiver<Notice<ConnettoDefaults>>) -> SessionId {
    let CommittedNotice::SignedIn { session, .. } = notice(notices).await else {
        panic!("expected sign-in");
    };
    session.session_id
}

/// Assert one account's ended-session notice without depending on row order.
async fn ended_notice(
    notices: &mut mpsc::UnboundedReceiver<Notice<ConnettoDefaults>>,
    owner: &str,
    expected: &[SessionId],
    expected_cause: EndCause,
) {
    let CommittedNotice::SessionsEnded {
        user_id,
        sessions,
        cause,
    } = notice(notices).await
    else {
        panic!("expected session end");
    };
    assert_eq!(user_id, owner);
    assert_eq!(cause, expected_cause);
    assert_eq!(sessions.len(), expected.len());
    assert!(expected.iter().all(|session| sessions.contains(session)));
}

#[derive(Clone, Copy)]
enum ServicePath {
    Logout,
    OtherAccount,
    Banned,
    Replayed,
}

/// Exercise refusal and acceptance through the service's public entry point.
async fn service_path(path: ServicePath, cause: EndCause) {
    let fixture = Fixture::acquire().await;
    provision(&fixture).await;
    let (hooks, mut notices) = EndingHooks::channel();
    let service = service(&fixture, hooks.clone()).await;
    let login = service
        .login(&identity("alice"), origin())
        .await
        .expect("login");
    let session = signed_in(&mut notices).await;
    if matches!(path, ServicePath::Banned) {
        pg_ban_store::<ConnettoDefaults>()
            .impose(
                &mut fixture.admin().get().await.expect("owner connection"),
                NewBan::starting_now(login.user_id.clone(), session, "test ban".to_owned(), None),
            )
            .await
            .expect("ban");
    }
    if matches!(path, ServicePath::Replayed) {
        service.refresh(&login.refresh_token).await.expect("rotate");
    }
    for refused in [true, false] {
        hooks.refuse.store(refused, Ordering::Release);
        let result = match path {
            ServicePath::Logout => service.logout(&login.refresh_token).await.map(|_| ()),
            ServicePath::OtherAccount => service.revoke(session).await,
            ServicePath::Banned | ServicePath::Replayed => {
                service.refresh(&login.refresh_token).await.map(|_| ())
            }
        };
        if refused {
            assert!(
                matches!(&result, Err(AuthError::Refused(reason)) if reason == "keep sessions")
            );
            assert!(service.session_is_live(session).await.expect("session"));
            assert_eq!(count(&fixture, hook_ends::table.count()).await, 0);
            assert!(notices.try_recv().is_err());
        } else {
            match path {
                ServicePath::Banned | ServicePath::Replayed => assert!(result.is_err()),
                _ => result.expect("accepted end"),
            }
            assert!(!service.session_is_live(session).await.expect("session"));
            assert_eq!(count(&fixture, hook_ends::table.count()).await, 1);
            ended_notice(&mut notices, &login.user_id, &[session], cause).await;
            assert!(notices.try_recv().is_err());
        }
        assert_eq!(
            hooks.seen.lock().expect("observations").last(),
            Some(&(login.user_id.clone(), vec![session], cause))
        );
    }
}

#[tokio::test]
async fn logout_is_transactional() {
    service_path(ServicePath::Logout, EndCause::LoggedOut).await;
}

#[tokio::test]
async fn another_accounts_cookie_revocation_is_transactional() {
    service_path(ServicePath::OtherAccount, EndCause::Revoked).await;
}

#[tokio::test]
async fn banned_refresh_is_transactional() {
    service_path(ServicePath::Banned, EndCause::Banned).await;
}

#[tokio::test]
async fn refresh_reuse_is_transactional() {
    service_path(ServicePath::Replayed, EndCause::TokenReplayed).await;
}

#[tokio::test]
async fn database_restore_ends_once_per_account_in_one_transaction() {
    let fixture = Fixture::acquire().await;
    provision(&fixture).await;
    let (hooks, mut notices) = EndingHooks::channel();
    let service = service(&fixture, hooks.clone()).await;
    let store = DbAuthStore::<ConnettoDefaults>::new(
        fixture.admin().clone(),
        AuthConfig::default().refresh_lifetimes(),
        hooks.clone(),
    );
    let mut accounts = Vec::new();
    for subject in ["alice", "bob"] {
        let mut sessions = Vec::new();
        let mut owner = String::new();
        for _ in 0..2 {
            owner = service
                .login(&identity(subject), origin())
                .await
                .expect("login")
                .user_id;
            sessions.push(signed_in(&mut notices).await);
        }
        accounts.push((owner, sessions));
    }
    assert!(store.revoke_every_session().await.is_err());
    for (_, sessions) in &accounts {
        for session in sessions {
            assert!(store.session_is_live(*session).await.expect("session"));
        }
    }
    assert_eq!(count(&fixture, hook_ends::table.count()).await, 0);
    assert!(notices.try_recv().is_err());
    hooks.seen.lock().expect("observations").clear();
    hooks.refuse.store(false, Ordering::Release);
    assert_eq!(store.revoke_every_session().await.expect("restore"), 4);
    accounts.sort_by(|left, right| left.0.cmp(&right.0));
    for (owner, sessions) in &accounts {
        ended_notice(&mut notices, owner, sessions, EndCause::DatabaseRestored).await;
        for session in sessions {
            assert!(!store.session_is_live(*session).await.expect("session"));
        }
    }
    assert_eq!(hooks.seen.lock().expect("observations").len(), 2);
    assert_eq!(count(&fixture, hook_ends::table.count()).await, 4);
    assert!(notices.try_recv().is_err());
}

/// Enrol one device key per session in `sessions` for `owner`, straight into
/// the enrolment table.
async fn seed_devices(
    pool: &diesel_async::pooled_connection::bb8::Pool<AsyncPgConnection>,
    owner: &str,
    sessions: [SessionId; 2],
) {
    use connetto_server::defaults::connetto_device_enrolments;
    let mut connection = pool.get().await.expect("owner connection");
    for (key, session) in [1_u8, 2].into_iter().zip(sessions) {
        diesel::insert_into(connetto_device_enrolments::table)
            .values((
                connetto_device_enrolments::key_id.eq(vec![key; 32]),
                connetto_device_enrolments::user_id.eq(owner),
                connetto_device_enrolments::session_id.eq(session.as_uuid()),
                connetto_device_enrolments::enrolled_at.eq(diesel::dsl::now),
                connetto_device_enrolments::last_seen.eq(diesel::dsl::now),
                connetto_device_enrolments::public_key.eq(vec![key; 32]),
                connetto_device_enrolments::attestation
                    .eq(connetto_core::device_cert::AttestationLevel::Unproven.as_str()),
            ))
            .execute(&mut connection)
            .await
            .expect("seed device");
    }
}

#[derive(Clone, Copy)]
enum HandlePath {
    One,
    Others,
    All,
    Erasure,
}

/// Exercise the built handle over live session and device rows.
async fn handle_path(path: HandlePath, cause: EndCause) {
    use super::e2e::{PG_SERIAL, reset_fixture};
    use super::enrolment::rooted_issuer;
    use super::lifecycle::{admin_pool, builder_over};
    use connetto_server::defaults::connetto_device_enrolments;
    use connetto_server::device_cert::DeviceCertConfig;
    use connetto_test_harness::isolated_session_keyring;

    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    connetto_test_harness::stack::provision_enrolment_tables(&fixture).await;
    provision(&fixture).await;
    let (hooks, mut notices) = EndingHooks::channel();
    let (builder, _identity_provider, _keys) = builder_over(&fixture, 18080).await;
    let (_, _, issuer) = rooted_issuer();
    let parts = Box::pin(
        builder
            .auth_hooks(hooks.clone())
            .device_identity(Some(DeviceCertConfig::new(issuer)))
            .build(),
    )
    .await
    .expect("build");
    let owner = "alice".to_owned();
    let [first, second, bystander] =
        std::array::from_fn(|_| SessionId::from_uuid(uuid::Uuid::new_v4()));
    fixture.live_session(&owner, first).await;
    fixture.live_session(&owner, second).await;
    fixture.live_session("bob", bystander).await;
    let store = DbAuthStore::<ConnettoDefaults>::new(
        pool.clone(),
        AuthConfig::default().refresh_lifetimes(),
        hooks.clone(),
    );
    if matches!(path, HandlePath::Erasure) {
        seed_devices(&pool, &owner, [first, second]).await;
    }
    let expected = match path {
        HandlePath::One => vec![first],
        HandlePath::Others => vec![second],
        HandlePath::All | HandlePath::Erasure => vec![first, second],
    };
    for refused in [true, false] {
        hooks.refuse.store(
            refused && !matches!(path, HandlePath::Erasure),
            Ordering::Release,
        );
        hooks.refuse_erasure.store(refused, Ordering::Release);
        let result = match path {
            HandlePath::One => parts.handle.end_session(&owner, first).await.map(|_| ()),
            HandlePath::Others => parts
                .handle
                .end_other_sessions(&owner, first)
                .await
                .map(|_| ()),
            HandlePath::All => parts.handle.end_all_sessions(&owner).await.map(|_| ()),
            HandlePath::Erasure => parts.handle.end_person(&owner).await.map(|_| ()),
        };
        if refused {
            assert!(result.is_err());
            assert!(store.session_is_live(first).await.expect("first session"));
            assert!(store.session_is_live(second).await.expect("second session"));
            assert_eq!(count(&fixture, hook_ends::table.count()).await, 0);
            assert!(notices.try_recv().is_err());
        } else {
            result.expect("accepted end");
            assert_accepted(
                &fixture,
                &store,
                &mut notices,
                &owner,
                [first, second],
                &expected,
                cause,
            )
            .await;
        }
        assert!(store.session_is_live(bystander).await.expect("bystander"));
        if matches!(path, HandlePath::Erasure) {
            let live = count(
                &fixture,
                connetto_device_enrolments::table
                    .filter(connetto_device_enrolments::revoked_at.is_null())
                    .count(),
            )
            .await;
            assert_eq!(live, if refused { 2 } else { 0 });
        }
        let observations = hooks.seen.lock().expect("observations");
        let (seen_owner, sessions, seen_cause) = observations.last().expect("transactional hook");
        assert_eq!(seen_owner, &owner);
        assert_eq!(*seen_cause, cause);
        assert_eq!(sessions.len(), expected.len());
        assert!(expected.iter().all(|session| sessions.contains(session)));
    }
    parts.handle.shutdown().await;
}

/// An accepted handle end of `expected` among `sessions` of `owner` left
/// the others live, told `committed` the end with `cause` (and for an
/// erasure each key and the erasure), and let the hook write once per
/// ended session.
async fn assert_accepted(
    fixture: &Fixture,
    store: &DbAuthStore<ConnettoDefaults>,
    notices: &mut mpsc::UnboundedReceiver<Notice<ConnettoDefaults>>,
    owner: &str,
    sessions: [SessionId; 2],
    expected: &[SessionId],
    cause: EndCause,
) {
    for session in sessions {
        assert_eq!(
            store.session_is_live(session).await.expect("session"),
            !expected.contains(&session)
        );
    }
    ended_notice(notices, owner, expected, cause).await;
    if cause == EndCause::Erased {
        let mut revoked = Vec::new();
        for _ in 0..2 {
            let CommittedNotice::DeviceRevoked { user_id, key } = notice(notices).await else {
                panic!("expected device revocation");
            };
            assert_eq!(user_id, owner);
            revoked.push(key);
        }
        assert_ne!(revoked[0], revoked[1]);
        assert_eq!(
            notice(notices).await,
            CommittedNotice::PersonErased {
                person: owner.to_owned()
            }
        );
    }
    assert_eq!(
        count(fixture, hook_ends::table.count()).await,
        i64::try_from(expected.len()).expect("session count")
    );
    assert!(notices.try_recv().is_err());
}

#[tokio::test]
async fn owner_ends_one_transactionally() {
    handle_path(HandlePath::One, EndCause::EndedByOwner).await;
}

#[tokio::test]
async fn owner_ends_others_transactionally() {
    handle_path(HandlePath::Others, EndCause::EndedOthers).await;
}

#[tokio::test]
async fn owner_ends_all_transactionally() {
    handle_path(HandlePath::All, EndCause::EndedAll).await;
}

#[tokio::test]
async fn erasure_refusal_restores_every_session_and_key() {
    handle_path(HandlePath::Erasure, EndCause::Erased).await;
}

#[tokio::test]
async fn guard_ban_and_session_ends_share_the_transaction() {
    use super::e2e::{PG_SERIAL, mint_tokens, reset_fixture};
    use super::lifecycle::{admin_pool, builder_over, live_session};
    use connetto_test_harness::isolated_session_keyring;

    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let pool = admin_pool(&fixture).await;
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    fixture.setup(&[
        "DROP TABLE IF EXISTS connetto_bans",
        "CREATE TABLE connetto_bans (user_id TEXT PRIMARY KEY, session UUID NOT NULL, reason TEXT NOT NULL, banned_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ)",
    ]).await;
    provision(&fixture).await;
    let (hooks, mut notices) = EndingHooks::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("address").port();
    let (builder, _identity_provider, _keys) = builder_over(&fixture, port).await;
    let abuse = ban_after_two_unresolvable_subscriptions();
    let parts = Box::pin(
        builder
            .auth_hooks(hooks.clone())
            .bans(true)
            .abuse(abuse)
            .build(),
    )
    .await
    .expect("build");
    let stream = tokio::spawn(parts.change_stream);
    let http = tokio::spawn(async move { axum::serve(listener, parts.router).await });
    let base = format!("http://127.0.0.1:{port}");
    let address = format!("127.0.0.1:{port}");
    let store = DbAuthStore::<ConnettoDefaults>::new(
        pool.clone(),
        AuthConfig::default().refresh_lifetimes(),
        hooks.clone(),
    );
    for (subject, refused) in [("alice", true), ("bob", false)] {
        hooks.refuse.store(refused, Ordering::Release);
        let (token, _, _) = mint_tokens(&base, subject).await;
        let first = signed_in(&mut notices).await;
        let (_, _, _) = mint_tokens(&base, subject).await;
        let second = signed_in(&mut notices).await;
        let owner = store
            .session_record(first)
            .await
            .expect("session read")
            .expect("session row")
            .user_id;
        let mut client = live_session(&address, &token).await;
        provoke_ban(&mut client).await;
        tokio::time::timeout(BOUND, hooks.attempted.notified())
            .await
            .expect("ban hook bound");
        let mut connection = pool.get().await.expect("owner connection");
        let revoked: bool = tokio::time::timeout(
            BOUND,
            connetto_sessions::table
                .find(first.as_uuid())
                .select(connetto_sessions::revoked)
                .for_update()
                .first(&mut connection),
        )
        .await
        .expect("transaction bound")
        .expect("session state");
        assert_eq!(revoked, !refused);
        assert_eq!(
            store.session_is_live(second).await.expect("second session"),
            refused
        );
        let ban = pg_ban_store::<ConnettoDefaults>()
            .check(&mut connection, &owner)
            .await
            .expect("ban read");
        assert_eq!(ban.is_some(), !refused);
        if refused {
            assert_eq!(count(&fixture, hook_ends::table.count()).await, 0);
            assert!(notices.try_recv().is_err());
        } else {
            ended_notice(&mut notices, &owner, &[first, second], EndCause::Banned).await;
            assert_eq!(count(&fixture, hook_ends::table.count()).await, 2);
            assert!(notices.try_recv().is_err());
        }
    }
    parts.handle.shutdown().await;
    stream.abort();
    http.abort();
}

/// Abuse limits that ban a person after two unresolvable subscriptions.
fn ban_after_two_unresolvable_subscriptions() -> connetto_server::AbuseConfig {
    use connetto_server::{AbuseLimits, ConnectionLimits, PersonLimits};
    AbuseLimits::new()
        .with_person(
            PersonLimits::new().with_unresolvable_subscriptions(2, Duration::from_secs(300)),
        )
        .with_connection(ConnectionLimits::new().with_unresolvable_subscriptions(1))
        .build()
        .expect("abuse thresholds")
}

/// Subscribe `client` twice to a table nobody can resolve, which crosses
/// [`ban_after_two_unresolvable_subscriptions`].
async fn provoke_ban(client: &mut connetto_server::WebSocketTransport<tokio::net::TcpStream>) {
    use connetto_core::messages::{ControlMessage, Subscribe, SubscriptionSpec};
    use connetto_core::traits::Transport as _;
    for subscription in ["ghost-one", "ghost-two"] {
        client
            .send_control(ControlMessage::Subscribe(Subscribe {
                sub_id: subscription.to_owned(),
                spec: SubscriptionSpec::new("SELECT * FROM ghost"),
            }))
            .await
            .expect("subscribe");
        let _ = super::lifecycle::next_control(client).await;
    }
}
