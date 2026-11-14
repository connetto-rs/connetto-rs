//! The application's hooks inside the sign-in transaction, and the account
//! lock a sign-in and a ban write share.
//!
//! Needs Docker: the fixture starts its own Postgres.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use connetto_core::SessionId;
use connetto_server::defaults::{ConnettoDefaults, connetto_provider_tokens, connetto_sessions};
use connetto_server::schema::ConnettoSchema;
use connetto_server::{
    AuthConfig, AuthError, AuthHooks, AuthService, AuthStoreError, CommittedNotice,
    ConnettoStoreSchema, DbAuthStore, DefaultAuthHooks, HookError, HookFuture, NewBan,
    RequestGuard, ResolvedIdentity, RetainedProviderToken, SessionOrigin, SignedIn, TokenAuthority,
    VerifiedClaims, deterministic_uuid, pg_ban_store,
};
use connetto_test_harness::Fixture;
use diesel::{ExpressionMethods as _, QueryDsl as _};
use diesel_async::{AsyncPgConnection, RunQueryDsl as _};
use tokio::sync::{mpsc, oneshot};

/// How long a test waits on an event before calling it lost.
const BOUND: Duration = Duration::from_secs(30);

const ISSUER: &str = "https://issuer.example";

/// The session description type of the deployment `D`.
type Descriptor<D> = <<D as ConnettoSchema>::Auth as ConnettoStoreSchema>::Descriptor;

/// A committed notice of the deployment `D`.
pub(super) type Notice<D> = CommittedNotice<String, Descriptor<D>>;

/// Hooks answering the default user id that send every committed notice to
/// the test.
pub(super) struct Told<D: ConnettoSchema>(mpsc::UnboundedSender<Notice<D>>);

impl<D: ConnettoSchema> Told<D> {
    /// The hooks and the receiving end of their notices.
    pub(super) fn channel() -> (Arc<Self>, mpsc::UnboundedReceiver<Notice<D>>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Arc::new(Self(sender)), receiver)
    }
}

impl<D: ConnettoSchema<Id = String>> AuthHooks<D> for Told<D> {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        AuthHooks::<D>::user_for(&DefaultAuthHooks, transaction, claims)
    }

    fn committed(&self, notice: Notice<D>) {
        self.0.send(notice).expect("the test keeps the receiver");
    }
}

diesel::table! {
    /// The application's own users, which its `user_for` registers.
    hook_users (subject) {
        subject -> Text,
    }
}

/// Hooks that register the user in their own table and then refuse the
/// sign-in.
struct RegisterThenRefuse;

impl AuthHooks<ConnettoDefaults> for RegisterThenRefuse {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        Box::pin(async move {
            diesel::insert_into(hook_users::table)
                .values(hook_users::subject.eq(&claims.subject))
                .execute(transaction)
                .await
                .map_err(|error| HookError::Backend(error.to_string()))?;
            Ok(claims.subject.clone())
        })
    }

    fn signed_in<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _signed_in: &'a SignedIn<'a, String, ()>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async { Err(HookError::Refused("registrations are closed".to_owned())) })
    }
}

/// What `signed_in` saw: the claims, the user id, the session, and how many
/// session rows with that id its transaction reads.
type Seen = (VerifiedClaims, String, SessionId, i64);

/// Hooks that record what `signed_in` sees inside its transaction.
#[derive(Default)]
struct Witness(Mutex<Vec<Seen>>);

impl AuthHooks<ConnettoDefaults> for Witness {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        AuthHooks::<ConnettoDefaults>::user_for(&DefaultAuthHooks, transaction, claims)
    }

    fn signed_in<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        signed_in: &'a SignedIn<'a, String, ()>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            let rows: i64 = connetto_sessions::table
                .filter(connetto_sessions::session_id.eq(signed_in.session.session_id.as_uuid()))
                .count()
                .get_result(transaction)
                .await
                .map_err(|error| HookError::Backend(error.to_string()))?;
            self.0.lock().expect("seen").push((
                signed_in.claims.clone(),
                signed_in.user_id.clone(),
                signed_in.session.session_id,
                rows,
            ));
            Ok(())
        })
    }
}

/// Hooks that hold the first sign-in inside its transaction, after its ban
/// read, until the test releases it.
struct Gate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl AuthHooks<ConnettoDefaults> for Gate {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        AuthHooks::<ConnettoDefaults>::user_for(&DefaultAuthHooks, transaction, claims)
    }

    fn signed_in<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _signed_in: &'a SignedIn<'a, String, ()>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            let entered = self.entered.lock().expect("entered").take();
            let release = self.release.lock().expect("release").take();
            if let (Some(entered), Some(release)) = (entered, release) {
                entered.send(()).expect("the test awaits the sign-in");
                release.await.expect("the test releases the sign-in");
            }
            Ok(())
        })
    }
}

fn identity(subject: &str) -> ResolvedIdentity {
    ResolvedIdentity {
        issuer: ISSUER.to_owned(),
        subject: subject.to_owned(),
        email: Some(format!("{subject}@example.com")),
        name: Some(subject.to_owned()),
        amr: vec!["pwd".to_owned()],
        acr: None,
    }
}

fn origin() -> SessionOrigin<()> {
    SessionOrigin {
        provider: "google".to_owned(),
        description: (),
    }
}

/// An auth service over a store running `hooks`, refusing what the ban table
/// holds.
async fn service(
    fixture: &Fixture,
    hooks: Arc<dyn AuthHooks<ConnettoDefaults>>,
) -> Arc<AuthService<ConnettoDefaults>> {
    fixture
        .setup(&[
            "DROP TABLE IF EXISTS connetto_bans",
            "CREATE TABLE connetto_bans (\
             user_id TEXT PRIMARY KEY, session UUID NOT NULL, reason TEXT NOT NULL, \
             banned_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ)",
            "DROP TABLE IF EXISTS hook_users",
            "CREATE TABLE hook_users (subject TEXT PRIMARY KEY)",
        ])
        .await;
    let config = AuthConfig::default();
    let store = fixture
        .auth_store_with(config.refresh_lifetimes(), hooks)
        .await;
    let guard = RequestGuard::default()
        .with_bans(fixture.admin().clone(), pg_ban_store::<ConnettoDefaults>());
    Arc::new(AuthService::new(
        Arc::new(TokenAuthority::generate(&config).expect("generate keypair")),
        store,
        Arc::new(guard),
    ))
}

/// The single count `query` answers on an owner connection.
async fn count<Q>(fixture: &Fixture, query: Q) -> i64
where
    Q: diesel_async::RunQueryDsl<AsyncPgConnection>
        + diesel_async::methods::LoadQuery<'static, AsyncPgConnection, i64>
        + Send
        + 'static,
{
    let mut connection = fixture.admin().get().await.expect("owner connection");
    query.get_result(&mut connection).await.expect("count")
}

/// A hook refusing the sign-in rolls back the session row, the retained
/// provider token and the hook's own write, and the refusal reaches the
/// caller with its reason.
#[tokio::test]
async fn a_refusing_hook_rolls_back_the_sign_in_and_its_own_write() {
    let fixture = Fixture::acquire().await;
    let service = service(&fixture, Arc::new(RegisterThenRefuse)).await;
    // The service above provisioned the tables, so this store only reads them.
    let store = DbAuthStore::<ConnettoDefaults>::new(
        fixture.admin().clone(),
        AuthConfig::default().refresh_lifetimes(),
        Arc::new(RegisterThenRefuse),
    );
    let token = RetainedProviderToken {
        issuer: ISSUER.to_owned(),
        access_token: "provider-access".to_owned(),
        refresh_token: Some("provider-refresh".to_owned()),
        expires_in: Some(Duration::from_secs(3600)),
    };

    let refused = store
        .sign_in(&identity("alice"), origin(), Some(&token), None)
        .await;
    assert!(
        matches!(
            &refused,
            Err(AuthStoreError::Hook(HookError::Refused(reason))) if reason == "registrations are closed"
        ),
        "the store answers the hook's refusal, got {refused:?}"
    );
    let refused = service.login(&identity("bob"), origin()).await;
    assert!(
        matches!(&refused, Err(AuthError::Refused(reason)) if reason == "registrations are closed"),
        "the service answers the hook's reason, got {refused:?}"
    );

    assert_eq!(count(&fixture, connetto_sessions::table.count()).await, 0);
    assert_eq!(
        count(&fixture, connetto_provider_tokens::table.count()).await,
        0
    );
    assert_eq!(
        count(&fixture, hook_users::table.count()).await,
        0,
        "the hook's own write rolled back with the sign-in"
    );
}

/// `signed_in` sees the verified claims, the user id `user_for` answered and
/// the session row its sign-in wrote, inside the transaction before the
/// commit.
#[tokio::test]
async fn the_hooks_see_the_claims_and_the_uncommitted_session() {
    let fixture = Fixture::acquire().await;
    let witness = Arc::new(Witness::default());
    let service = service(&fixture, witness.clone()).await;

    let pair = service
        .login(&identity("carol"), origin())
        .await
        .expect("sign in");

    let seen = witness.0.lock().expect("seen").clone();
    let [(claims, user_id, session, rows)] = seen.as_slice() else {
        panic!("expected one signed_in call, got {seen:?}");
    };
    assert_eq!(claims.issuer, ISSUER);
    assert_eq!(claims.subject, "carol");
    assert_eq!(claims.email.as_deref(), Some("carol@example.com"));
    assert_eq!(claims.amr, ["pwd"]);
    assert_eq!(
        user_id,
        &deterministic_uuid(ISSUER, "carol").to_string(),
        "the user id user_for answered"
    );
    assert_eq!(user_id, &pair.user_id);
    assert_eq!(
        *rows, 1,
        "the session row is visible inside the transaction"
    );
    assert!(
        service.session_is_live(*session).await.expect("liveness"),
        "the session the hook saw committed"
    );
}

/// A ban written while a sign-in holds its account waits for that sign-in to
/// commit, and the next sign-in reads the ban inside its transaction and is
/// refused without writing a session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ban_waits_for_a_sign_in_in_flight_and_refuses_the_next() {
    let fixture = Fixture::acquire().await;
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let service = service(
        &fixture,
        Arc::new(Gate {
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(Some(release_rx)),
        }),
    )
    .await;
    let user = deterministic_uuid(ISSUER, "dave").to_string();
    let ban = || {
        NewBan::starting_now(
            user.clone(),
            SessionId::from_uuid(uuid::Uuid::new_v4()),
            "the operator's call",
            None,
        )
    };

    let in_flight = tokio::spawn({
        let service = Arc::clone(&service);
        async move { service.login(&identity("dave"), origin()).await }
    });
    tokio::time::timeout(BOUND, entered_rx)
        .await
        .expect("the sign-in reaches signed_in within the bound")
        .expect("the gate signals");

    let bans = pg_ban_store::<ConnettoDefaults>();
    let mut connection = fixture.admin().get().await.expect("owner connection");
    // The sign-in holds the account until released, so any wait for the lock
    // outlasts this timeout.
    diesel::sql_query("SET lock_timeout = '100ms'")
        .execute(&mut connection)
        .await
        .expect("set lock_timeout");
    let waited = bans.impose(&mut connection, ban()).await;
    let error = waited.expect_err("the ban waits on the sign-in's account lock");
    assert!(
        error.detail().contains("lock timeout"),
        "the ban timed out on the lock, got {error}"
    );
    diesel::sql_query("RESET lock_timeout")
        .execute(&mut connection)
        .await
        .expect("reset lock_timeout");

    release.send(()).expect("the sign-in awaits release");
    let signed_in = tokio::time::timeout(BOUND, in_flight)
        .await
        .expect("the sign-in ends within the bound")
        .expect("join the sign-in");
    let pair = signed_in.expect("no ban had committed when the sign-in read");
    bans.impose(&mut connection, ban())
        .await
        .expect("the ban commits once the sign-in has");

    let refused = service.login(&identity("dave"), origin()).await;
    assert!(
        matches!(refused, Err(AuthError::Banned)),
        "the next sign-in reads the ban, got {refused:?}"
    );
    assert_eq!(
        count(&fixture, connetto_sessions::table.count()).await,
        1,
        "the refused sign-in wrote no session"
    );
    assert_eq!(pair.user_id, user);
}
