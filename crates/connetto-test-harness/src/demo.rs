//! What the device proofs read from the demo's Postgres.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use diesel::{ExpressionMethods as _, QueryDsl as _};
use diesel_async::RunQueryDsl as _;
use tokio::time::{Instant, sleep};

use crate::pool_for;

diesel::table! {
    /// The demo's orders, as far as the proofs count them.
    orders (id) {
        /// Key.
        id -> diesel::sql_types::Uuid,
        /// Ordered amount.
        quantity -> diesel::sql_types::BigInt,
    }
}

diesel::table! {
    /// The demo's team membership, as far as the R77 walk seeds and reads it.
    team_members (team_id, member_id) {
        /// The team the account belongs to.
        team_id -> diesel::sql_types::Text,
        /// The account id, the deterministic UUID the `team_notes` policy compares.
        member_id -> diesel::sql_types::Text,
    }
}

/// How many orders Postgres holds.
///
/// # Errors
///
/// When Postgres cannot be reached or counted.
pub async fn order_count(pg_url: &str) -> Result<i64> {
    let pool = pool_for(pg_url).await;
    let mut conn = pool.get().await.context("a Postgres connection")?;
    orders::table
        .count()
        .get_result(&mut conn)
        .await
        .context("counting orders")
}

/// Wait up to a minute for Postgres to hold `expected` orders.
///
/// # Errors
///
/// When the count differs after a minute, naming it.
pub async fn wait_for_count(pg_url: &str, expected: i64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let count = order_count(pg_url).await?;
        if count == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("Postgres holds {count} orders, expected {expected}");
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Remove one account from the team, the operator's removal the R77 phase
/// makes while the phones stand offline, so the account's note is refused
/// when the server comes back.
///
/// # Errors
///
/// When Postgres cannot be reached or the row cannot be removed.
pub async fn remove_team_member(pg_url: &str, team: &str, member: &str) -> Result<()> {
    let pool = pool_for(pg_url).await;
    let mut conn = pool.get().await.context("a Postgres connection")?;
    diesel::delete(team_members::table)
        .filter(team_members::team_id.eq(team))
        .filter(team_members::member_id.eq(member))
        .execute(&mut conn)
        .await
        .context("removing the account from the team")?;
    Ok(())
}

/// Seed the R77 walk's one team, every account a member, the `member_id`s
/// being the deterministic UUIDs the `team_notes` policy compares against
/// `app.user_id`. A row an earlier run left already stands, so a rerun seeds
/// no duplicate.
///
/// # Errors
///
/// When Postgres cannot be reached or the membership rows cannot be inserted.
pub async fn seed_team_members(pg_url: &str, team: &str, members: &[&str]) -> Result<()> {
    let pool = pool_for(pg_url).await;
    for member in members {
        let mut conn = pool.get().await.context("a Postgres connection")?;
        diesel::insert_into(team_members::table)
            .values((
                team_members::team_id.eq(team),
                team_members::member_id.eq(member),
            ))
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .context("seeding the team's membership rows")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use diesel::{ExpressionMethods as _, QueryDsl as _};
    use diesel_async::RunQueryDsl as _;

    use super::{remove_team_member, seed_team_members, team_members};
    use crate::Fixture;
    use crate::stack::DEMO_DEPLOYMENT;

    /// The members the demo's Postgres holds for `team`, sorted.
    async fn members_of(fixture: &Fixture, team: &str) -> Vec<String> {
        let mut conn = fixture.admin().get().await.expect("an admin connection");
        team_members::table
            .filter(team_members::team_id.eq(team))
            .select(team_members::member_id)
            .order(team_members::member_id)
            .load(&mut conn)
            .await
            .expect("the team's membership rows")
    }

    /// A rerun seeds no duplicate, and a removal takes only the named
    /// account off only the named team.
    #[tokio::test]
    async fn a_team_seeds_once_and_loses_only_the_removed_member() {
        let fixture = Fixture::acquire().await;
        fixture.setup(&[DEMO_DEPLOYMENT.schema]).await;
        let url = fixture.admin_url();

        seed_team_members(url, "r77", &["alice", "bob", "carol"])
            .await
            .expect("the first seed");
        seed_team_members(url, "r77", &["alice", "bob", "carol"])
            .await
            .expect("a rerun over the same rows");
        seed_team_members(url, "other", &["bob"])
            .await
            .expect("another team");
        assert_eq!(members_of(&fixture, "r77").await, ["alice", "bob", "carol"]);

        remove_team_member(url, "r77", "bob")
            .await
            .expect("the removal");
        assert_eq!(members_of(&fixture, "r77").await, ["alice", "carol"]);
        assert_eq!(
            members_of(&fixture, "other").await,
            ["bob"],
            "bob stays a member of every team the removal does not name"
        );
    }
}
