//! The fixture the server suites and the demo process suites share: the
//! `orders` and `owned` documents, the reset of the database a server run
//! starts from, the authorization store a run derives its model into, and the
//! login dance that mints a run's tokens.

use std::process::Command;
use std::sync::LazyLock;
use std::time::Duration;

use diesel::QueryableByName;
use diesel::sql_query;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use openidconnect::reqwest;
use tempfile::TempDir;

use crate::{Fixture, MOCK_OAUTH_PROVIDER, provision_cap_policies};

/// The `orders` table every server run serves.
pub const PG_DDL: &str = "-- connetto:major 1\nCREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, \
    status TEXT);";
/// The subscription a client of [`PG_DDL`] opens.
pub const QUERY: &str = "SELECT * FROM orders WHERE quantity > 0";

/// The major both [`PG_DDL`] and [`OWNED_PG_DDL`] declare, for hand-built
/// handshakes.
///
/// # Panics
///
/// When [`PG_DDL`] declares no major.
#[must_use]
pub fn served_major() -> connetto_core::Major {
    connetto_core::Major::declared_in(PG_DDL).expect("the served schema declares its major")
}

diesel::table! {
    /// Row of the [`PG_DDL`] table, typed for the queries that count it.
    orders (id) {
        /// Order identifier, the primary key.
        id -> Integer,
        /// Unit price.
        price -> Nullable<Double>,
        /// Number of units.
        quantity -> Nullable<Integer>,
        /// Order status.
        status -> Nullable<Text>,
    }
}

/// The `owned` table, whose rows belong to the user in their `owner` column.
pub const OWNED_PG_DDL: &str =
    "-- connetto:major 1\nCREATE TABLE owned (id INT PRIMARY KEY, owner TEXT, body TEXT);";
/// The policy document the `owned` fixture's server derives its model from.
///
/// The schema and the policies reach the server as two documents, so the
/// statement enabling row-level security belongs here beside the policy rather
/// than in [`OWNED_PG_DDL`], which is what clients sync.
pub const OWNED_POLICIES: &str = "ALTER TABLE owned ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY owned_p ON owned USING (owner = current_setting('app.user_id', true));";

/// `orders` carries no policy at all, so its document is empty.
///
/// The database filters none of its rows and the model has to agree, which the
/// translator reports and the change path answers with no round trip.
pub const NO_POLICIES: &str = "";

/// Serializes the Docker-gated tests. They reset the same Postgres and share one
/// replication slot and publication name, so they must not run concurrently.
pub static PG_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// What a server needs to build its change-path executor: the policy text its
/// authorization model is derived from, and the store it writes that model
/// into.
///
/// A store per server rather than one shared, so two tests in one run cannot
/// read each other's rules or facts.
pub struct Authorization {
    policies: String,
    /// The authorization service's address.
    pub endpoint: String,
    /// The store this server's model and facts live in.
    pub store: String,
}

impl Authorization {
    /// A fresh store on `fixture`'s authorization service for `policies`.
    pub async fn provision(fixture: &Fixture, policies: &str) -> Self {
        let endpoint = fixture.fga_url().await.to_owned();
        let (_channel, store) = fixture.fga_store().await;
        Self {
            policies: policies.to_owned(),
            endpoint,
            store,
        }
    }

    /// The policies, the endpoint and the store as the server's settings.
    #[must_use]
    pub fn env_pairs(&self) -> [(&str, &str); 3] {
        [
            ("CONNETTO_PG_POLICIES", self.policies.as_str()),
            ("CONNETTO_FGA_URL", self.endpoint.as_str()),
            ("CONNETTO_FGA_STORE", self.store.as_str()),
        ]
    }

    /// Every tuple in the store whose user names `who`.
    ///
    /// # Panics
    ///
    /// When the authorization service cannot be reached or read.
    pub async fn tuples_naming(&self, who: &str) -> Vec<String> {
        use openfga_client::client::{OpenFgaServiceClient, ReadRequest};
        let mut client = OpenFgaServiceClient::connect(self.endpoint.clone())
            .await
            .expect("connect to the authorization service");
        let mut found = Vec::new();
        let mut continuation_token = String::new();
        loop {
            let page = client
                .read(ReadRequest {
                    store_id: self.store.clone(),
                    continuation_token,
                    ..ReadRequest::default()
                })
                .await
                .expect("read the store")
                .into_inner();
            found.extend(
                page.tuples
                    .into_iter()
                    .filter_map(|tuple| tuple.key)
                    .filter(|key| key.user.contains(who))
                    .map(|key| format!("{} {} {}", key.user, key.relation, key.object)),
            );
            if page.continuation_token.is_empty() {
                return found;
            }
            continuation_token = page.continuation_token;
        }
    }

    /// Poll the store until a tuple names `who`, or 30 seconds pass.
    pub async fn await_tuples(&self, who: &str) -> Vec<String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let found = self.tuples_naming(who).await;
            if !found.is_empty() || std::time::Instant::now() >= deadline {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Run a single DDL/DML statement in its own transaction (autocommit).
///
/// # Panics
///
/// When no connection is available or the statement fails.
pub async fn exec(pool: &Pool<AsyncPgConnection>, sql: &str) {
    let mut conn = pool.get().await.expect("admin connection");
    sql_query(sql)
        .execute(&mut *conn)
        .await
        .unwrap_or_else(|err| panic!("statement failed ({sql}): {err}"));
}

/// Drop and recreate the audit table every server appends access changes to,
/// so each run reads its own rows.
pub async fn reset_audit_table(pool: &Pool<AsyncPgConnection>) {
    exec(pool, "DROP TABLE IF EXISTS auth_events").await;
    exec(pool, "DROP TYPE IF EXISTS connetto_auth_op").await;
    crate::provision_audit(pool).await;
}

/// Reset the orders fixture, auth tables and reader role for one server run.
pub async fn reset_fixture(pool: &Pool<AsyncPgConnection>, fixture: &Fixture) {
    exec(pool, "DROP TABLE IF EXISTS orders CASCADE").await;
    // Stale writer state from a previous run would suppress replayed
    // mutations, so recreate the writer tables fresh.
    exec(pool, "DROP TABLE IF EXISTS _connetto_settled").await;
    exec(pool, "DROP TABLE IF EXISTS _connetto_writers").await;
    exec(pool, "DROP TABLE IF EXISTS _connetto_session_writers").await;
    exec(pool, "DROP TABLE IF EXISTS _connetto_courier_notices").await;
    crate::provision_writers(pool).await;
    reset_audit_table(pool).await;
    exec(pool, PG_DDL).await;
    provision_cap_policies(pool, PG_DDL, NO_POLICIES).await;
    exec(
        pool,
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_reader') \
         THEN CREATE ROLE app_reader LOGIN PASSWORD 'app_reader'; END IF; END $$",
    )
    .await;
    exec(pool, "GRANT USAGE ON SCHEMA public TO app_reader").await;
    exec(pool, "GRANT SELECT ON auth_events TO app_reader").await;
    exec(
        pool,
        "GRANT SELECT, INSERT, UPDATE, DELETE ON orders TO app_reader",
    )
    .await;
    exec(
        pool,
        "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, \
         _connetto_session_writers, _connetto_courier_notices TO app_reader",
    )
    .await;
    exec(
        pool,
        "GRANT USAGE ON SEQUENCE _connetto_courier_notices_id_seq TO app_reader",
    )
    .await;
    // The kind roles a caller acts as, granted what the fixture's tables
    // hold once those tables exist.
    fixture.setup_roles(crate::FIXTURE_ROLES).await;
    exec(pool, "INSERT INTO orders VALUES (1, 1.0, 3, 'seed')").await;
    fixture.start_replication(&["orders"]).await;
}

/// Rewrite a Postgres URL's user info, keeping host, port, and database. Used to
/// point the server's write target at a non-superuser role subject to RLS.
///
/// # Panics
///
/// When `url` has no scheme.
#[must_use]
pub fn with_user_url(url: &str, user: &str, password: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("url has a scheme");
    let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    format!("{scheme}://{user}:{password}@{host}")
}

/// A token signing key pair on disk, so an access token outlives the server
/// restart the way it does in a deployment that supplies its keys.
///
/// # Panics
///
/// When `openssl` cannot run or fails.
#[must_use]
pub fn signing_keys(dir: &TempDir) -> (String, String) {
    let private = dir.path().join("jwt.pem");
    let public = dir.path().join("jwt.pub.pem");
    let run = |args: &[&str]| {
        let status = Command::new("openssl")
            .args(args)
            .status()
            .expect("run openssl");
        assert!(status.success(), "openssl {args:?} failed");
    };
    let (private, public) = (
        private.to_string_lossy().into_owned(),
        public.to_string_lossy().into_owned(),
    );
    run(&["genpkey", "-algorithm", "ed25519", "-out", &private]);
    run(&["pkey", "-in", &private, "-pubout", "-out", &public]);
    (private, public)
}

/// Drive the login dance through the server's auth endpoints and return the
/// callback JSON body.
///
/// # Panics
///
/// When any step of the dance fails or answers other than a redirect.
pub async fn token_body(auth_base: &str, subject: &str) -> serde_json::Value {
    let agent = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build token-mint HTTP client");

    let login = agent
        .get(format!("{auth_base}/auth/login"))
        .query(&[("provider", MOCK_OAUTH_PROVIDER)])
        .send()
        .await
        .expect("GET /auth/login");
    assert!(
        login.status().is_redirection(),
        "login must redirect, got {}",
        login.status()
    );
    let authorize_url = login
        .headers()
        .get("location")
        .expect("location on login redirect")
        .to_str()
        .expect("utf-8 location")
        .to_owned();

    let authorized = agent
        .post(&authorize_url)
        .form(&[("username", subject)])
        .send()
        .await
        .expect("POST idp authorize");
    assert!(
        authorized.status().is_redirection(),
        "idp authorize must redirect, got {}",
        authorized.status()
    );
    let callback_url = authorized
        .headers()
        .get("location")
        .expect("location on authorize redirect")
        .to_str()
        .expect("utf-8 location")
        .to_owned();

    let callback = agent
        .get(&callback_url)
        .send()
        .await
        .expect("GET /auth/callback");
    let body = callback.text().await.expect("callback body");
    serde_json::from_str(&body).expect("callback JSON body")
}

/// Drive the login dance through the server's auth endpoints for `subject`
/// and return the minted `(access_token, user_id, refresh_token)` triple.
///
/// # Panics
///
/// As [`token_body`], and when the callback body lacks a token.
pub async fn mint_tokens(auth_base: &str, subject: &str) -> (String, String, String) {
    let body = token_body(auth_base, subject).await;
    let access_token = body["access_token"]
        .as_str()
        .expect("access_token in callback JSON")
        .to_owned();
    let user_id = body["user_id"]
        .as_str()
        .expect("user_id in callback JSON")
        .to_owned();
    let refresh_token = body["refresh_token"]
        .as_str()
        .expect("refresh_token in callback JSON")
        .to_owned();
    (access_token, user_id, refresh_token)
}

/// Drive the login dance through the server's auth endpoints and return the
/// minted `(access_token, user_id)` pair.
pub async fn mint_token(auth_base: &str) -> (String, String) {
    let (access_token, user_id, _refresh) = mint_tokens(auth_base, "e2e-user").await;
    (access_token, user_id)
}

/// The refresh token from the same login dance, which [`mint_token`]
/// discards. Logging out is the one audit producer reachable from outside the
/// process, and it needs this token.
pub async fn mint_refresh_token(auth_base: &str) -> String {
    let (_access, _user, refresh) = mint_tokens(auth_base, "e2e-user").await;
    refresh
}

/// Every `op` the audit table recorded so far, in order.
///
/// # Panics
///
/// When the audit table cannot be read.
pub async fn audit_ops(pool: &Pool<AsyncPgConnection>) -> Vec<String> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        op: String,
    }
    let mut conn = pool.get().await.expect("connection");
    let rows: Vec<Row> =
        sql_query("SELECT CAST(op AS TEXT) AS op FROM auth_events ORDER BY at, op")
            .get_results(&mut conn)
            .await
            .expect("read auth_events");
    rows.into_iter().map(|row| row.op).collect()
}
