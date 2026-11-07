//! Where `connetto-ios-signing` keeps the iOS signing identity and
//! development profiles on a Mac, so the proof drivers find and unlock the
//! same ones.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};

/// The keychain holding the signing identity, in `~/Library/Keychains`.
pub const KEYCHAIN: &str = "connetto-signing.keychain-db";
/// The development profile of the dioxus demo's iOS builds.
pub const DIOXUS_PROFILE: &str = "connetto dioxus demo development";
/// The development profile of the dioxus demo's macOS build.
pub const DIOXUS_MACOS_PROFILE: &str = "connetto dioxus demo macos development";
/// The development profile of the Tauri demo's iOS builds.
pub const TAURI_PROFILE: &str = "connetto tauri demo development";
/// The development profile of the tapper's test runner.
pub const TAPPER_PROFILE: &str = "connetto tapper development";

/// The folder `xcodebuild` and `dx` read the development profiles from.
///
/// # Errors
///
/// When `HOME` is unset.
pub fn profile_folder() -> Result<PathBuf> {
    Ok(home()?.join("Library/Developer/Xcode/UserData/Provisioning Profiles"))
}

/// The profile's content decoded with `security`, the plist its name and
/// entitlements stand in.
///
/// # Errors
///
/// When `security` cannot decode the profile.
pub async fn decode_profile(profile: &Path) -> Result<String> {
    decode_with("security", &["cms", "-D", "-i"], profile).await
}

/// The output of `program` run with `args` and the profile's path, or its
/// error output when it fails.
async fn decode_with(program: &str, args: &[&str], profile: &Path) -> Result<String> {
    let output = tokio::process::Command::new(program)
        .args(args)
        .arg(profile)
        .output()
        .await
        .with_context(|| format!("starting {program}"))?;
    if !output.status.success() {
        bail!(
            "decoding the profile {} failed: {}",
            profile.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The path of the profile named `name`, decoded one file at a time from the
/// folder Xcode reads.
///
/// # Errors
///
/// When the folder holds no profile of that name, naming the ones it does.
pub async fn profile_named(name: &str) -> Result<PathBuf> {
    find_profile(&profile_folder()?, name, decode_profile).await
}

/// The profile in `folder` whose decoded `Name` is `name`, skipping a file
/// `decode` refuses.
async fn find_profile(
    folder: &Path,
    name: &str,
    decode: impl AsyncFn(&Path) -> Result<String>,
) -> Result<PathBuf> {
    let mut entries = tokio::fs::read_dir(folder)
        .await
        .with_context(|| format!("listing {}", folder.display()))?;
    let mut held = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Ok(plist) = decode(&path).await else {
            continue;
        };
        match profile_name(&plist) {
            Some(actual) if actual == name => return Ok(path),
            Some(actual) => held.push(actual.to_owned()),
            None => {}
        }
    }
    let seen = if held.is_empty() {
        String::from("nothing of a profile's shape")
    } else {
        format!("the profiles {held:?}")
    };
    bail!(
        "no profile named {name} in {}, the folder holds {seen}",
        folder.display()
    )
}

/// The `Name` of a decoded profile's property list.
pub fn profile_name(plist: &str) -> Option<&str> {
    let (_, after) = plist.split_once("<key>Name</key>")?;
    let (_, value) = after.split_once("<string>")?;
    value.split_once("</string>").map(|(name, _)| name)
}

/// The folder holding the signing key, the certificate and the keychain's
/// password.
///
/// # Errors
///
/// When `HOME` is unset.
pub fn folder() -> Result<PathBuf> {
    Ok(home()?.join(".local/share/connetto-r88/signing"))
}

/// The keychain's path.
///
/// # Errors
///
/// When `HOME` is unset.
pub fn keychain() -> Result<PathBuf> {
    Ok(home()?.join("Library/Keychains").join(KEYCHAIN))
}

/// Unlock the signing keychain for this session, so `codesign` can use it
/// from SSH.
///
/// # Errors
///
/// When the password file is missing, which means `connetto-ios-signing` has
/// not run, or the keychain refuses it.
pub async fn unlock() -> Result<()> {
    let password = tokio::fs::read_to_string(folder()?.join("keychain-password"))
        .await
        .context("reading the signing keychain's password, run connetto-ios-signing first")?;
    let output = tokio::process::Command::new("security")
        .args(["unlock-keychain", "-p", password.trim()])
        .arg(keychain()?)
        .output()
        .await
        .context("starting security")?;
    if !output.status.success() {
        bail!(
            "unlocking the signing keychain failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// The SHA-1 of the valid code-signing identity in the keychain, which
/// `dx build --apple-team-id` takes. It is public, the certificate's
/// fingerprint.
///
/// # Errors
///
/// When `security` cannot list the keychain.
pub async fn identity() -> Result<Option<String>> {
    let output = tokio::process::Command::new("security")
        .args(["find-identity", "-v", "-p", "codesigning"])
        .arg(keychain()?)
        .output()
        .await
        .context("starting security")?;
    if !output.status.success() {
        bail!(
            "listing the signing keychain failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| {
            let hash = line.split_whitespace().nth(1)?;
            (hash.len() == 40 && hash.chars().all(|c| c.is_ascii_hexdigit()))
                .then(|| hash.to_owned())
        }))
}

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is unset")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::{Result, bail};

    use super::{DIOXUS_PROFILE, TAURI_PROFILE, decode_with, find_profile};

    /// A decoded profile carrying `name`.
    fn plist(name: &str) -> String {
        format!("<plist><dict><key>Name</key><string>{name}</string></dict></plist>")
    }

    /// Read a file as its decoded profile, refusing one named `*.broken` as
    /// `security` refuses a damaged profile.
    async fn read(path: &Path) -> Result<String> {
        if path
            .extension()
            .is_some_and(|extension| extension == "broken")
        {
            bail!("not a profile");
        }
        Ok(tokio::fs::read_to_string(path).await?)
    }

    /// The profile carrying the asked name is found among others, past a
    /// file that does not decode and one that holds no name.
    #[tokio::test]
    async fn the_profile_with_the_asked_name_is_found() {
        let folder = tempfile::tempdir().expect("a folder");
        std::fs::write(folder.path().join("a.broken"), plist(TAURI_PROFILE)).expect("write");
        std::fs::write(folder.path().join("b.mobileprovision"), "<plist/>").expect("write");
        std::fs::write(
            folder.path().join("c.mobileprovision"),
            plist(DIOXUS_PROFILE),
        )
        .expect("write");
        std::fs::write(
            folder.path().join("d.mobileprovision"),
            plist(TAURI_PROFILE),
        )
        .expect("write");
        let found = find_profile(folder.path(), TAURI_PROFILE, read)
            .await
            .expect("the profile");
        assert_eq!(found, folder.path().join("d.mobileprovision"));
    }

    /// A missing profile names the profiles the folder does hold, or says it
    /// holds none.
    #[tokio::test]
    async fn a_missing_profile_names_what_the_folder_holds() {
        let folder = tempfile::tempdir().expect("a folder");
        let empty = find_profile(folder.path(), TAURI_PROFILE, read)
            .await
            .expect_err("no profile");
        assert!(
            empty.to_string().contains("nothing of a profile's shape"),
            "{empty:#}"
        );
        std::fs::write(
            folder.path().join("c.mobileprovision"),
            plist(DIOXUS_PROFILE),
        )
        .expect("write");
        let other = find_profile(folder.path(), TAURI_PROFILE, read)
            .await
            .expect_err("no profile");
        assert!(other.to_string().contains(DIOXUS_PROFILE), "{other:#}");
        let gone = find_profile(&folder.path().join("gone"), TAURI_PROFILE, read)
            .await
            .expect_err("no folder");
        assert!(gone.to_string().starts_with("listing "), "{gone:#}");
    }

    /// A decoder that succeeds hands back what it printed, one that fails
    /// names the profile and its error output, and one that cannot start
    /// names itself.
    #[tokio::test]
    async fn a_decoder_hands_back_its_output_or_its_error() {
        let folder = tempfile::tempdir().expect("a folder");
        let profile = folder.path().join("p.mobileprovision");
        std::fs::write(&profile, plist(TAURI_PROFILE)).expect("write");
        let decoded = decode_with("sh", &["-c", r#"cat "$1""#, "sh"], &profile)
            .await
            .expect("the decoded profile");
        assert_eq!(decoded, plist(TAURI_PROFILE));
        let refused = decode_with("sh", &["-c", "echo damaged >&2; exit 1", "sh"], &profile)
            .await
            .expect_err("a refusal");
        assert_eq!(
            refused.to_string(),
            format!("decoding the profile {} failed: damaged", profile.display())
        );
        let absent = decode_with("connetto-no-such-decoder", &[], &profile)
            .await
            .expect_err("no decoder");
        assert_eq!(absent.to_string(), "starting connetto-no-such-decoder");
    }
}
