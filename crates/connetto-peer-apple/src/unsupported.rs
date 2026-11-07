//! The non-Apple platform: every framework call answers its own refusal.

/// The Bluetooth side, absent off Apple.
pub(super) mod bluetooth {
    use super::super::bluetooth::Authorization;

    /// The app's authorization, refused off Apple.
    pub(crate) fn authorization() -> Authorization {
        Authorization::Denied
    }
}

/// The hotspot side, absent off Apple.
pub(super) mod hotspot {
    use super::super::{
        Error,
        hotspot::{Joined, Joining, Security},
    };

    /// The system's join, which stands nowhere off Apple.
    pub(crate) fn join(_ssid: &str, _passphrase: &str, _security: Security) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    /// The system's answer, never any off Apple.
    pub(crate) fn completion() -> Option<Result<Joining, Error>> {
        None
    }

    /// The system's release, which stands ready off Apple.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "one signature on every target, and only iOS can fail"
    )]
    pub(crate) fn leave(_ssid: &str) -> Result<(), Error> {
        Ok(())
    }

    /// The joined network, never any off Apple.
    pub(crate) fn joined() -> Option<Joined> {
        None
    }
}
