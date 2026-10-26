//! The device-wide file lock that keeps one identity's eviction, resync clear
//! or server frame off a shared row another identity wrote and has not pushed
//! yet (R11). Such a write lives only in the writer's capture session, which
//! reads each row's current state when it builds the upload, so a row removed
//! or overwritten before the push would upload nothing or the wrong values.
//!
//! A writer holds the lock shared from its first write to a shared table
//! until its push has queued what it captured. Whatever changes another
//! identity's rows takes the lock exclusive, and only while holding SQLite's
//! write lock. A writer asks for its shared hold from the update hook, inside
//! the statement that wrote the row and so while holding SQLite's write lock
//! too, which is why neither side ever waits on the other.

use std::fs::{File, OpenOptions, TryLockError};
use std::sync::{Arc, Mutex};

use crate::ClientError;

/// What this connection holds of the lock.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Held {
    Nothing,
    Shared,
    Exclusive,
}

/// One connection's handle on the lock file beside the shared store.
pub(crate) struct SharedLock {
    file: File,
    held: Held,
    /// Whether this connection captured a shared write its next push has not
    /// queued yet.
    writing: bool,
}

/// The lock, reached from the update hook and from the connection alike.
pub(crate) type LockHandle = Arc<Mutex<SharedLock>>;

impl SharedLock {
    /// Open, creating it when absent, the lock file beside the shared store at
    /// `store`. The file is never deleted, since a process still holding the
    /// old one open would lock an inode a new opener no longer sees.
    ///
    /// # Errors
    ///
    /// [`ClientError::Session`] when the file cannot be opened.
    pub(crate) fn open(store: &str) -> Result<LockHandle, ClientError> {
        let path = format!("{store}-lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|err| {
                ClientError::Session(format!("opening the shared store's lock {path}: {err}"))
            })?;
        Ok(Arc::new(Mutex::new(Self {
            file,
            held: Held::Nothing,
            writing: false,
        })))
    }

    /// Note an application write to a shared table, holding the lock shared
    /// from the first such write until the push.
    pub(crate) fn wrote(&mut self) {
        if self.writing {
            return;
        }
        self.writing = true;
        if self.held == Held::Nothing {
            self.hold_shared();
        }
    }

    /// The push queued every captured write, or found none, so the writes
    /// are now in the queue and the spare rows other identities read.
    pub(crate) fn pushed(&mut self) {
        self.writing = false;
        if self.held == Held::Shared {
            self.unlock();
        }
    }

    /// Whether this connection holds the lock exclusive, so an enclosing step
    /// already guards the rows a nested one changes.
    pub(crate) fn is_exclusive(&self) -> bool {
        self.held == Held::Exclusive
    }

    /// Take the lock exclusive, giving up this connection's own shared hold
    /// first, and report whether no other identity held it. Call it only
    /// while holding SQLite's write lock on every attached database.
    ///
    /// # Errors
    ///
    /// [`ClientError::Session`] when the platform refuses the lock for a
    /// reason other than another holder.
    pub(crate) fn try_exclusive(&mut self) -> Result<bool, ClientError> {
        if self.held == Held::Shared {
            self.unlock();
        }
        let refused = match self.file.try_lock() {
            Ok(()) => {
                self.held = Held::Exclusive;
                return Ok(true);
            }
            Err(TryLockError::WouldBlock) => None,
            Err(TryLockError::Error(err)) => Some(err),
        };
        if self.writing {
            self.hold_shared();
        }
        refused.map_or(Ok(false), |err| {
            Err(ClientError::Session(format!(
                "locking the shared store: {err}"
            )))
        })
    }

    /// Give up the exclusive hold, keeping the lock shared when this
    /// connection still has unpushed shared writes. Call it before the
    /// transaction that took the lock commits, so no other identity writes
    /// between the release and the commit.
    pub(crate) fn end_exclusive(&mut self) {
        if self.held == Held::Exclusive {
            self.unlock();
        }
        if self.writing {
            self.hold_shared();
        }
    }

    fn hold_shared(&mut self) {
        match self.file.try_lock_shared() {
            Ok(()) => self.held = Held::Shared,
            // Unreachable while every exclusive holder also holds SQLite's
            // write lock, which this writer holds.
            Err(err) => tracing::error!(
                error = %err,
                "the shared store's lock refused a writer, its unpushed writes are unguarded"
            ),
        }
    }

    fn unlock(&mut self) {
        if let Err(err) = self.file.unlock() {
            tracing::error!(error = %err, "releasing the shared store's lock failed");
        }
        self.held = Held::Nothing;
    }
}
