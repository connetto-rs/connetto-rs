//! The free space of the volume a store directory lives on (R79 decision 16):
//! the opportunistic capacity on Apple platforms, which Apple names for data
//! the user did not ask for, and the space available to the process
//! elsewhere.

use std::io;
use std::path::Path;

/// The free bytes of the volume holding `path`, measured at the nearest
/// directory of it that exists, since a store makes its root on first write.
///
/// # Errors
///
/// [`io::Error`] when the path cannot be made absolute or the volume cannot
/// be measured.
pub(crate) fn free_bytes(path: &Path) -> io::Result<u64> {
    let path = std::path::absolute(path)?;
    let existing = path
        .ancestors()
        .find(|dir| dir.is_dir())
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
    volume_free(existing)
}

/// `NSURLVolumeAvailableCapacityForOpportunisticUsageKey` of `dir`.
#[cfg(target_vendor = "apple")]
fn volume_free(dir: &Path) -> io::Result<u64> {
    use objc2_foundation::{
        NSArray, NSNumber, NSString, NSURL, NSURLVolumeAvailableCapacityForOpportunisticUsageKey,
    };

    let path = dir
        .to_str()
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let url = NSURL::fileURLWithPath(&NSString::from_str(path));
    #[expect(unsafe_code, reason = "reading a Foundation constant is an FFI access")]
    // SAFETY: the key is an immutable NSString constant Foundation exports for
    // the life of the process.
    let key = unsafe { NSURLVolumeAvailableCapacityForOpportunisticUsageKey };
    let values = url
        .resourceValuesForKeys_error(&NSArray::from_slice(&[key]))
        .map_err(|err| io::Error::other(err.localizedDescription().to_string()))?;
    let number = values
        .objectForKey(key)
        .ok_or_else(|| io::Error::other("the volume answers no opportunistic capacity"))?
        .downcast::<NSNumber>()
        .map_err(|_| io::Error::other("the opportunistic capacity is not a number"))?;
    u64::try_from(number.longLongValue())
        .map_err(|_| io::Error::other("the opportunistic capacity is negative"))
}

/// `statvfs` of `dir`, the blocks available to an unprivileged process.
#[cfg(all(unix, not(target_vendor = "apple")))]
fn volume_free(dir: &Path) -> io::Result<u64> {
    let stat = rustix::fs::statvfs(dir)?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

/// `GetDiskFreeSpaceExW` of `dir`, the bytes free to the calling user.
#[cfg(windows)]
fn volume_free(dir: &Path) -> io::Result<u64> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let wide: Vec<u16> = dir.as_os_str().encode_wide().chain([0]).collect();
    let mut free = 0_u64;
    #[expect(unsafe_code, reason = "the volume's free space is a Win32 call")]
    // SAFETY: `wide` is NUL-terminated and outlives the call, `free` is a
    // writable u64, and the two totals the call takes may be null.
    let answered = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &raw mut free,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        )
    };
    if answered == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(free)
}
