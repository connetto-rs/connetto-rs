//! The authorization store holds every fact Postgres implies, though the
//! server reads with a role that row-level security binds.
//!
//! The deployment carries the bot cap, which puts a restrictive read policy on
//! every synced table, so the reader role sees none of a table's rows unless a
//! session binds its bot list. The server spawned here runs with that reader,
//! as production must.

use std::time::Duration;

use connetto_test_harness::{Fixture, provision_cap_policies};

use super::e2e::{
    AuthStack, Authorization, ChildGuard, Ports, build_auth_stack, restart_server,
    spawn_server_cfg, start_server, with_user_url,
};

const SCHEMA: &str = "-- connetto:major 1\n\
     CREATE TABLE docs (id BIGINT PRIMARY KEY, project_id BIGINT NOT NULL);\n\
     CREATE TABLE project_members (project_id BIGINT NOT NULL, user_id TEXT NOT NULL, \
     PRIMARY KEY (project_id, user_id));\n\
     CREATE TABLE papers (id INT PRIMARY KEY, owner TEXT NOT NULL);\n\
     CREATE TABLE shares (paper_id INT NOT NULL, viewer TEXT NOT NULL, weight INT NOT NULL, \
     PRIMARY KEY (paper_id, viewer));";

/// `docs` by project membership, whose facts are read off `project_members`
/// rows, and `papers` by a share above the average weight, whose facts only a
/// query re-run against Postgres can state.
const POLICIES: &str = "ALTER TABLE docs ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY docs_members ON docs FOR ALL USING (project_id IN \
     (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true)));\n\
     ALTER TABLE papers ENABLE ROW LEVEL SECURITY;\n\
     CREATE POLICY papers_shared ON papers FOR ALL USING (EXISTS (SELECT 1 FROM shares s \
     WHERE s.paper_id = papers.id AND s.viewer = current_setting('app.user_id', true) \
     AND s.weight > (SELECT avg(weight) FROM shares)));";

/// A deployment whose server reads as `app_reader`, with `alice` a member and
/// `ivy` the one share above the average before the server first boots.
struct Deployment {
    fixture: Fixture,
    reader: String,
    authorization: Authorization,
    auth: AuthStack,
}

impl Deployment {
    async fn provision() -> Self {
        let fixture = Fixture::acquire().await;
        fixture
            .setup(&[
                SCHEMA,
                POLICIES,
                "CREATE ROLE app_reader LOGIN PASSWORD 'app_reader'",
                "GRANT USAGE ON SCHEMA public TO app_reader",
                "GRANT SELECT ON docs, project_members, papers, shares TO app_reader",
                "GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, \
                 _connetto_session_writers TO app_reader",
                "INSERT INTO project_members VALUES (1, 'alice')",
                "INSERT INTO papers VALUES (1, 'zoe')",
                "INSERT INTO shares VALUES (1, 'ivy', 10), (1, 'carol', 1), (1, 'dave', 1)",
            ])
            .await;
        provision_cap_policies(fixture.admin(), SCHEMA, POLICIES).await;
        fixture
            .start_replication(&["docs", "project_members", "papers", "shares"])
            .await;
        fixture.provision_auth_tables().await;
        let authorization = Authorization::provision(&fixture, POLICIES).await;
        let reader = with_user_url(fixture.admin_url(), "app_reader", "app_reader");
        Self {
            fixture,
            reader,
            authorization,
            auth: build_auth_stack().await,
        }
    }

    fn spawn(&self, ports: Ports, auth_pairs: &[(&str, &str)]) -> ChildGuard {
        spawn_server_cfg(
            self.fixture.admin_url(),
            &ports.bind(),
            SCHEMA,
            "",
            Some(&self.reader),
            &self.authorization,
            auth_pairs,
        )
    }

    async fn start(&self) -> (ChildGuard, Ports) {
        start_server(&self.auth, BOOT, |ports, pairs| self.spawn(ports, pairs)).await
    }

    async fn restart(&self, ports: Ports) -> ChildGuard {
        restart_server(&self.auth, ports, BOOT, |ports, pairs| {
            self.spawn(ports, pairs)
        })
        .await
    }
}

const BOOT: Duration = Duration::from_secs(30);

#[tokio::test]
async fn a_boot_keeps_the_members_postgres_holds() {
    let deployment = Deployment::provision().await;
    let (server, ports) = deployment.start().await;
    let store = &deployment.authorization;
    assert!(
        !store.await_tuples("alice").await.is_empty(),
        "alice was a member before the first boot, so the boot's load writes her fact"
    );
    deployment
        .fixture
        .exec("INSERT INTO project_members VALUES (1, 'mallory')")
        .await;
    assert!(
        !store.await_tuples("mallory").await.is_empty(),
        "the change stream writes the fact of a member who joins while the server runs"
    );

    drop(server);
    let _server = deployment.restart(ports).await;
    // The restarted boot reconciles before it serves, so the store is settled
    // once the restart returns.
    assert!(
        !store.tuples_naming("alice").await.is_empty()
            && !store.tuples_naming("mallory").await.is_empty(),
        "a restart reconciles the store with Postgres, where both are still members"
    );
}

#[tokio::test]
async fn a_re_run_fact_survives_a_change_that_keeps_it_true() {
    let deployment = Deployment::provision().await;
    let (_server, _ports) = deployment.start().await;
    let store = &deployment.authorization;
    assert!(
        !store.await_tuples("ivy").await.is_empty(),
        "ivy's share is above the average at boot, so the store grants her the paper"
    );
    // Without dave the average is 5.5, which ivy's 10 still clears, so the
    // re-run the delete triggers states her grant again.
    deployment
        .fixture
        .exec("DELETE FROM shares WHERE viewer = 'dave'")
        .await;
    // The change stream applies in commit order, so once nina's later fact is
    // in the store the delete before it has been handled.
    deployment
        .fixture
        .exec("INSERT INTO project_members VALUES (2, 'nina')")
        .await;
    assert!(
        !store.await_tuples("nina").await.is_empty(),
        "the change after the delete reaches the store"
    );
    assert!(
        !store.tuples_naming("ivy").await.is_empty(),
        "the re-run reads every share, and ivy's still clears the average"
    );
}
