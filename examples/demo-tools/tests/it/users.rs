//! The demo deployment's `users` table, which `demo-server` writes in each
//! sign-in's transaction under `CONNETTO_USERS=database`, as the stacks run
//! it.

use std::path::PathBuf;

use connetto_demo_tools::users::users;
use connetto_server::deterministic_uuid;
use connetto_test_harness::server_fixture::mint_tokens;
use connetto_test_harness::stack::{DEMO_DEPLOYMENT, provision, spawn_server};
use connetto_test_harness::{MOCK_OAUTH_PROVIDER, MockOauth, with_user};
use diesel::QueryDsl as _;
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _};

/// The account ids `users` shows the reader role, bound as the server binds
/// a person's session, the caller as `caller` and the bot list as the
/// wildcard that passes the deployment's restrictive cap policies.
async fn visible_to(reader_url: &str, caller: &str) -> Vec<String> {
    let mut connection = AsyncPgConnection::establish(reader_url)
        .await
        .expect("the reader connects");
    diesel::sql_query("SELECT set_config('app.user_id', $1, false), set_config($2, $3, false)")
        .bind::<Text, _>(caller)
        .bind::<Text, _>(connetto_core::schema::BOT_LIST_SETTING)
        .bind::<Text, _>(connetto_core::schema::BOT_LIST_WILDCARD)
        .execute(&mut connection)
        .await
        .expect("bind the caller");
    users::table
        .select(users::id)
        .order(users::id)
        .load(&mut connection)
        .await
        .expect("read the names")
}

/// Each sign-in registers its account once under the id the account always
/// had, and an account reads its own name and its teammates' names only.
#[tokio::test]
async fn a_sign_in_registers_its_account_once_and_teammates_read_it() {
    let provisioned = provision(&DEMO_DEPLOYMENT, "users", None)
        .await
        .expect("provision the demo deployment");
    let idp = MockOauth::start().await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a port");
    let bind = listener.local_addr().expect("address").to_string();
    drop(listener);
    let base = format!("http://{bind}");
    let mut envs = provisioned.server_env(&DEMO_DEPLOYMENT, &bind, &base);
    envs.extend(idp.env_pairs(MOCK_OAUTH_PROVIDER, &format!("{base}/auth/callback")));
    let _server = spawn_server(
        &PathBuf::from(env!("CARGO_BIN_EXE_demo-server")),
        &envs,
        &bind,
    )
    .await
    .expect("the server starts");

    let (_, alice, _) = mint_tokens(&base, "alice").await;
    assert_eq!(
        alice,
        deterministic_uuid(idp.issuer(), "alice").to_string(),
        "the account keeps the id the default hooks answer"
    );
    let (_, again, _) = mint_tokens(&base, "alice").await;
    assert_eq!(again, alice, "a second sign-in is the same account");
    let (_, bob, _) = mint_tokens(&base, "bob").await;
    let (_, carol, _) = mint_tokens(&base, "carol").await;

    let mut connection = provisioned.fixture.admin().get().await.expect("the owner");
    let mut rows: Vec<(String, String)> = users::table
        .select((users::id, users::login_name))
        .load(&mut connection)
        .await
        .expect("read the users");
    rows.sort();
    let mut expected = vec![
        (alice.clone(), "alice".to_owned()),
        (bob.clone(), "bob".to_owned()),
        (carol.clone(), "carol".to_owned()),
    ];
    expected.sort();
    assert_eq!(rows, expected, "one row per account, named by its subject");
    drop(connection);

    provisioned
        .fixture
        .exec(&format!(
            "INSERT INTO teams (id) VALUES ('crew'), ('solo'); \
             INSERT INTO team_members (team_id, member_id) VALUES \
             ('crew', '{alice}'), ('crew', '{bob}'), ('solo', '{carol}')"
        ))
        .await;
    let reader = with_user(
        provisioned.fixture.admin_url(),
        DEMO_DEPLOYMENT.reader_role,
        DEMO_DEPLOYMENT.reader_role,
    );
    let mut crew = vec![alice.clone(), bob.clone()];
    crew.sort();
    assert_eq!(
        visible_to(&reader, &alice).await,
        crew,
        "alice reads her own name and her teammate's"
    );
    assert_eq!(
        visible_to(&reader, &carol).await,
        vec![carol.clone()],
        "carol, alone in her team, reads only her own"
    );
}
