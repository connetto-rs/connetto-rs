//! The bound on how many Postgres containers one user's test processes run at once.

use std::fs::{File, TryLockError};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

/// Containers started side by side past this many starve each other into their startup timeouts.
pub const CONTAINER_SLOTS: usize = 4;

/// One of the [`CONTAINER_SLOTS`] file locks, held until dropped.
pub struct ContainerSlot {
    _lock: File,
}

/// Wait for a free slot in the user's temporary directory, shared by every test process.
///
/// # Errors
///
/// When the slot directory or a lock file cannot be opened or locked.
pub async fn container_slot() -> Result<ContainerSlot> {
    slot_in(
        &std::env::temp_dir().join("connetto-container-slots"),
        CONTAINER_SLOTS,
    )
    .await
}

async fn slot_in(dir: &Path, slots: usize) -> Result<ContainerSlot> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let paths: Vec<PathBuf> = (0..slots)
        .map(|slot| dir.join(format!("{slot}.lock")))
        .collect();
    loop {
        for path in &paths {
            let lock = File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)
                .with_context(|| format!("opening {}", path.display()))?;
            match lock.try_lock() {
                Ok(()) => return Ok(ContainerSlot { _lock: lock }),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Error(err)) => {
                    return Err(err).with_context(|| format!("locking {}", path.display()));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::slot_in;
    use crate::stack::TempDir;

    #[tokio::test]
    async fn a_holder_past_the_bound_waits_until_a_slot_is_dropped() {
        let dir = TempDir::create("connetto-slots")
            .await
            .expect("a slot directory");
        let first = slot_in(&dir.path, 2).await.expect("the first slot");
        let _second = slot_in(&dir.path, 2).await.expect("the second slot");
        let third = slot_in(&dir.path, 2);
        tokio::pin!(third);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut third)
                .await
                .is_err(),
            "a third holder got a slot while two were held"
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(5), third)
            .await
            .expect("the third holder gets the dropped slot")
            .expect("the dropped slot");
    }

    #[tokio::test]
    async fn a_slot_directory_that_is_a_file_is_refused() {
        let dir = TempDir::create("connetto-slots")
            .await
            .expect("a scratch directory");
        let file = dir.path.join("slots");
        std::fs::write(&file, b"").expect("a file where the slots would go");
        let refused = slot_in(&file, 1).await.err().expect("a refusal");
        assert!(
            refused.to_string().starts_with("creating "),
            "unexpected refusal: {refused:#}"
        );
    }

    #[tokio::test]
    async fn a_lock_file_that_is_a_directory_is_refused() {
        let dir = TempDir::create("connetto-slots")
            .await
            .expect("a slot directory");
        std::fs::create_dir(dir.path.join("0.lock")).expect("a directory where the lock goes");
        let refused = slot_in(&dir.path, 1).await.err().expect("a refusal");
        assert!(
            refused.to_string().starts_with("opening "),
            "unexpected refusal: {refused:#}"
        );
    }
}
