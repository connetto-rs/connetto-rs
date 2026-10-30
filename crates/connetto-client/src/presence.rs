//! Which identities on the device use the shared store, so the last one out
//! removes it (R11 decision 7, O9).

use std::collections::HashMap;

use diesel::prelude::*;

use crate::{ClientConfig, ClientError};

/// What an identity leaving the shared store found, which decides whether the
/// store itself goes (R11 decision 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedDeparture {
    /// The build shares no tables, or the device holds no store this identity
    /// can read, so there is nothing to leave.
    NotShared,
    /// Another identity still uses the store, so the store stays.
    Kept,
    /// No identity is left in the store, so its file and key record go.
    LastOut,
}

/// The identities that use the shared store, created there at open.
pub(crate) const PRESENCE_DDL: &str = "CREATE TABLE IF NOT EXISTS connetto_shared._connetto_identity \
    (identity TEXT NOT NULL PRIMARY KEY) WITHOUT ROWID";

diesel::table! {
    /// One identity using the shared store.
    connetto_shared._connetto_identity (identity) {
        /// The identity's client id, its replica name when signed in.
        identity -> Text,
    }
}

/// Record `config`'s identity as using the shared store.
///
/// # Errors
///
/// [`ClientError`] when the store cannot be written.
pub(crate) fn join(db: &mut SqliteConnection, config: &ClientConfig) -> Result<(), ClientError> {
    diesel::insert_or_ignore_into(_connetto_identity::table)
        .values(_connetto_identity::identity.eq(&config.client_id))
        .execute(db)?;
    Ok(())
}

/// Take `identity` out of the shared store: its presence and every holding
/// it has there go, its unsent writes' included, and every shared row no
/// remaining holding names is deleted, online or not (R11 decision 7).
/// Returns whether another identity still uses the store.
///
/// # Errors
///
/// [`ClientError`] when the store cannot be read or written.
pub(crate) fn depart(
    db: &mut SqliteConnection,
    config: &ClientConfig,
    identity: &str,
) -> Result<bool, ClientError> {
    db.transaction(|conn| {
        diesel::delete(_connetto_identity::table.filter(_connetto_identity::identity.eq(identity)))
            .execute(conn)?;
        crate::holdings::release_identity(conn, identity)?;
        let shared = crate::holdings::synced_tables(conn, config)?
            .into_iter()
            .filter(|table| crate::is_shared(&config.shared_tables, table))
            .collect();
        crate::holdings::delete_unheld(conn, config, &shared, &HashMap::new())?;
        Ok(diesel::select(diesel::dsl::exists(
            _connetto_identity::table.select(_connetto_identity::identity),
        ))
        .get_result::<bool>(conn)?)
    })
}

/// Take `config`'s identity out of the shared store at `store` with no client
/// open, as a wipe that runs after its client closed must (R11 slice 6, Q6
/// and Q7). The shared writes still queued in the replica at `replica` are
/// undone first, newest first, under the rollback's rule of leaving a row a
/// later change touched, then [`depart`] runs, all in one transaction that
/// waits for any other identity's writer.
///
/// A store without its key is inert and the next open replaces it, and a
/// replica without its file or key holds nothing to undo.
///
/// # Errors
///
/// [`ClientError`] when the replica or the store cannot be opened, read or
/// written.
#[cfg(feature = "native-auth")]
pub(crate) fn depart_alone(
    store: &std::path::Path,
    store_key: Option<&crate::ReplicaKey>,
    replica: Option<(&std::path::Path, &crate::ReplicaKey)>,
    config: &ClientConfig,
) -> Result<SharedDeparture, ClientError> {
    let Some(store_key) = store_key.filter(|_| !config.shared_tables.is_empty() && store.exists())
    else {
        return Ok(SharedDeparture::NotShared);
    };
    let replica = replica.filter(|(path, _)| path.exists());
    let opened = replica.map_or(Ok(":memory:"), |(path, _)| utf8(path))?;
    let mut db =
        SqliteConnection::establish(opened).map_err(|err| ClientError::Connect(err.to_string()))?;
    if let Some((_, key)) = replica {
        crate::cipher::unlock(&mut db, key)?;
    }
    crate::harden::harden_replica_connection(&mut db)?;
    let store = utf8(store)?;
    crate::harden::attach_keyed_in_window(
        &mut db,
        store,
        crate::SHARED_SCHEMA,
        crate::harden::AttachPermits::Write,
        store_key,
    )?;
    let identity = config.client_id.clone();
    let remain = db.immediate_transaction::<_, ClientError, _>(|db| {
        if replica.is_some() {
            undo_queued_shared(db, config)?;
        }
        depart(db, config, &identity)
    })?;
    Ok(if remain {
        SharedDeparture::Kept
    } else {
        SharedDeparture::LastOut
    })
}

/// `path` as the `&str` SQLite opens.
#[cfg(feature = "native-auth")]
fn utf8(path: &std::path::Path) -> Result<&str, ClientError> {
    path.to_str()
        .ok_or_else(|| ClientError::Connect(format!("a non-UTF-8 path {}", path.display())))
}

/// Undo the shared writes of every entry queued in `main`'s pending table,
/// newest first, as [`crate::undo_shared`] does.
#[cfg(feature = "native-auth")]
fn undo_queued_shared(db: &mut SqliteConnection, config: &ClientConfig) -> Result<(), ClientError> {
    use crate::_connetto_pending;
    let queued: Vec<Vec<u8>> = _connetto_pending::table
        .select(_connetto_pending::changeset)
        .order(_connetto_pending::position.desc())
        .load(db)?;
    crate::undo_shared(db, &config.shared_tables, queued.iter().map(Vec::as_slice))
}

// APFS refuses a file name that is not UTF-8, so the test runs on Linux.
#[cfg(all(test, feature = "native-auth", target_os = "linux"))]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;

    use crate::{ClientConfig, ClientError, ReplicaKey};

    #[test]
    fn a_departure_refuses_a_path_sqlite_cannot_name() {
        let dir = tempfile::tempdir().expect("temp dir");
        let named = dir.path().join("named.db");
        let unnamed = dir.path().join(OsStr::from_bytes(b"unnamed-\xff.db"));
        for path in [&named, &unnamed] {
            std::fs::write(path, b"").expect("a file");
        }
        let key = ReplicaKey::from_bytes([0x42; ReplicaKey::LEN]);
        let mut config = ClientConfig::new("alice".to_owned());
        config.shared_tables.insert("tags".to_owned());
        for (store, replica) in [(&unnamed, None), (&named, Some(unnamed.as_path()))] {
            let refused =
                super::depart_alone(store, Some(&key), replica.map(|path| (path, &key)), &config);
            assert!(
                matches!(&refused, Err(ClientError::Connect(message)) if message.contains("non-UTF-8")),
                "{refused:?}"
            );
        }
    }
}
