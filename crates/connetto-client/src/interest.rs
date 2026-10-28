//! What every identity on the device still keeps in the shared store, so one
//! identity's pass never removes a shared row another still wants (R11
//! decision 6).

use std::collections::{HashMap, HashSet};

use diesel::SqliteConnection;
use diesel::prelude::*;

use crate::{ClientConfig, ClientError, clock, is_shared, subscriptions};

/// The interest and spare records, created in the shared store at open.
pub(crate) const INTEREST_DDL: &str = "\
CREATE TABLE IF NOT EXISTS connetto_shared._connetto_interest (\
identity TEXT NOT NULL, subscription TEXT NOT NULL, tbl TEXT NOT NULL, \
predicate TEXT, covers_until INTEGER, PRIMARY KEY (identity, subscription, tbl)); \
CREATE TABLE IF NOT EXISTS connetto_shared._connetto_spare (\
identity TEXT NOT NULL, tbl TEXT NOT NULL, clause TEXT NOT NULL, \
PRIMARY KEY (identity, tbl, clause));";

diesel::table! {
    /// One identity's subscription coverage of one shared table.
    connetto_shared._connetto_interest (identity, subscription, tbl) {
        /// The identity's client id, its replica name when signed in.
        identity -> Text,
        /// The subscription's wire id.
        subscription -> Text,
        /// The lowercased physical table.
        tbl -> Text,
        /// The rows it covers as an SQL predicate, `NULL` for the whole table.
        predicate -> Nullable<Text>,
        /// When the coverage ends, `NULL` for a pin or a held watch.
        covers_until -> Nullable<BigInt>,
    }
}

diesel::table! {
    /// One shared row an identity's unacknowledged write touched.
    connetto_shared._connetto_spare (identity, tbl, clause) {
        /// The identity's client id, its replica name when signed in.
        identity -> Text,
        /// The lowercased physical table.
        tbl -> Text,
        /// The row's primary key as an SQL predicate.
        clause -> Text,
    }
}

/// What the other identities keep in the shared store, by lowercased physical
/// table.
#[derive(Default)]
pub(crate) struct Others {
    /// Predicates a kept row matches.
    pub clauses: HashMap<String, Vec<String>>,
    /// Tables another identity wants whole.
    pub whole: HashSet<String>,
}

/// Rewrite this identity's interest from its subscription record.
///
/// # Errors
///
/// [`ClientError`] when the record cannot be read or the store written.
pub(crate) fn record(db: &mut SqliteConnection, config: &ClientConfig) -> Result<(), ClientError> {
    if config.shared_tables.is_empty() {
        return Ok(());
    }
    let mut rows = Vec::new();
    for declared in subscriptions::declared(db)? {
        let Some(coverage) = crate::live::coverage_of(&declared.spec)? else {
            continue;
        };
        for table in &coverage.tables {
            let physical = config
                .policy_tables
                .physical(table)
                .unwrap_or(table)
                .to_lowercase();
            if is_shared(&config.shared_tables, &physical) {
                rows.push((
                    _connetto_interest::identity.eq(config.client_id.clone()),
                    _connetto_interest::subscription.eq(declared.sub_id.clone()),
                    _connetto_interest::tbl.eq(physical),
                    _connetto_interest::predicate.eq(coverage.predicate.clone()),
                    _connetto_interest::covers_until.eq(declared.covers_until),
                ));
            }
        }
    }
    db.transaction(|conn| {
        diesel::delete(
            _connetto_interest::table.filter(_connetto_interest::identity.eq(&config.client_id)),
        )
        .execute(conn)?;
        diesel::insert_or_ignore_into(_connetto_interest::table)
            .values(&rows)
            .execute(conn)?;
        Ok(())
    })
}

/// Rewrite this identity's spared shared rows from `spare`, the key clauses
/// of its unacknowledged writes by lowercased physical table.
///
/// # Errors
///
/// [`ClientError`] when the store cannot be written.
pub(crate) fn record_spare(
    db: &mut SqliteConnection,
    config: &ClientConfig,
    spare: &HashMap<String, Vec<String>>,
) -> Result<(), ClientError> {
    let rows: Vec<_> = spare
        .iter()
        .filter(|(table, _)| is_shared(&config.shared_tables, table))
        .flat_map(|(table, clauses)| {
            clauses.iter().map(move |clause| {
                (
                    _connetto_spare::identity.eq(config.client_id.clone()),
                    _connetto_spare::tbl.eq(table.clone()),
                    _connetto_spare::clause.eq(clause.clone()),
                )
            })
        })
        .collect();
    db.transaction(|conn| {
        diesel::delete(
            _connetto_spare::table.filter(_connetto_spare::identity.eq(&config.client_id)),
        )
        .execute(conn)?;
        diesel::insert_or_ignore_into(_connetto_spare::table)
            .values(&rows)
            .execute(conn)?;
        Ok(())
    })
}

/// What every other identity on the device keeps: its live coverage and its
/// unacknowledged writes.
///
/// # Errors
///
/// [`ClientError`] when the store cannot be read.
pub(crate) fn others(
    db: &mut SqliteConnection,
    config: &ClientConfig,
) -> Result<Others, ClientError> {
    let mut others = Others::default();
    if config.shared_tables.is_empty() {
        return Ok(others);
    }
    let now = clock::now_secs(db)?;
    let interest: Vec<(String, Option<String>)> = _connetto_interest::table
        .filter(_connetto_interest::identity.ne(&config.client_id))
        .filter(
            _connetto_interest::covers_until
                .is_null()
                .or(_connetto_interest::covers_until.gt(now)),
        )
        .select((_connetto_interest::tbl, _connetto_interest::predicate))
        .load(db)?;
    for (table, predicate) in interest {
        match predicate {
            Some(predicate) => others
                .clauses
                .entry(table)
                .or_default()
                .push(format!("({predicate})")),
            None => {
                others.whole.insert(table);
            }
        }
    }
    let spared: Vec<(String, String)> = _connetto_spare::table
        .filter(_connetto_spare::identity.ne(&config.client_id))
        .select((_connetto_spare::tbl, _connetto_spare::clause))
        .load(db)?;
    for (table, clause) in spared {
        others.clauses.entry(table).or_default().push(clause);
    }
    Ok(others)
}
