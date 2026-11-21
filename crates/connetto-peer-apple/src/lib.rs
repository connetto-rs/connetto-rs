#![doc = include_str!("../README.md")]

use std::time::Duration;

/// The bound one hop onto the platform's owning queues gets (R76).
pub const HOP_WITHIN: Duration = Duration::from_secs(10);

/// Why the platform's frameworks could not do what was asked (R76).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A framework reported an error.
    #[error("the {domain} framework reported {code}: {description}")]
    Apple {
        /// The error domain.
        domain: String,
        /// The error code within the domain.
        code: isize,
        /// The framework's description of the error.
        description: String,
    },
    /// A platform facility is absent at run time.
    #[error("the platform facility {0} is absent at run time")]
    Missing(String),
    /// The platform did not answer within [`HOP_WITHIN`].
    #[error("the platform did not answer within {0:?}")]
    TimedOut(Duration),
    /// This target has no such facility.
    #[error("this target has no such facility")]
    Unsupported,
}

/// The domain the system's join answers in.
#[cfg_attr(
    not(any(target_os = "ios", test)),
    expect(
        dead_code,
        reason = "the iOS platform and the tests read this, a host build without tests uses neither"
    )
)]
pub(crate) const HOTSPOT_JOIN_DOMAIN: &str = "NEHotspotConfigurationErrorDomain";

/// The code `NEHotspotConfigurationErrorAlreadyAssociated` the domain
/// answers for a network the device already stands on.
#[cfg_attr(
    not(any(target_os = "ios", test)),
    expect(
        dead_code,
        reason = "the iOS platform and the tests read this, a host build without tests uses neither"
    )
)]
pub(crate) const HOTSPOT_ALREADY_ASSOCIATED: isize = 13;

/// Whether the system's join error stands for a network the device already
/// stands on, so the join's goal stands reached (R76).
#[cfg_attr(
    not(any(target_os = "ios", test)),
    expect(
        dead_code,
        reason = "the iOS platform and the tests call this, a host build without tests uses neither"
    )
)]
pub(crate) fn join_already_associated(domain: &str, code: isize) -> bool {
    domain == HOTSPOT_JOIN_DOMAIN && code == HOTSPOT_ALREADY_ASSOCIATED
}

#[cfg(test)]
mod tests {
    use super::{HOTSPOT_JOIN_DOMAIN, join_already_associated};

    #[test]
    fn the_join_domain_stands_already_associated_only_there() {
        assert!(join_already_associated(HOTSPOT_JOIN_DOMAIN, 13));
        // Code 7 is the user's denial of the join dialog, a refusal.
        assert!(!join_already_associated(HOTSPOT_JOIN_DOMAIN, 7));
        assert!(!join_already_associated(HOTSPOT_JOIN_DOMAIN, 6));
        assert!(!join_already_associated("OtherDomain", 13));
    }
}

/// The app's Bluetooth permission, read with no manager, so reading it asks
/// the user nothing (R77 decision 23).
pub mod bluetooth {
    /// The app's Bluetooth authorization, as the platform answers it.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Authorization {
        /// The user has not been asked yet.
        NotDetermined,
        /// The user or a policy refused it, or the target has no Bluetooth.
        Denied,
        /// The app may use Bluetooth.
        Allowed,
    }

    /// The app's authorization, `CBManager.authorization` on Apple.
    pub fn authorization() -> Authorization {
        crate::platform::bluetooth::authorization()
    }
}

/// The joiner's hotspot, the network join asked of the system and the read
/// of the network the device joined (R76).
pub mod hotspot {
    use std::net::{IpAddr, Ipv4Addr};

    /// The network's security, as the platform's join API knows it.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Security {
        /// WPA2 personal.
        Wpa2,
        /// WPA3 personal.
        Wpa3,
    }

    /// How the system answered a join it took.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Joining {
        /// The system joined the network.
        Joined,
        /// The device already stood on the network.
        AlreadyThere,
    }

    /// What a joined network stands at, once it stands.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Joined {
        /// The network's address on the device's interface.
        pub address: Ipv4Addr,
        /// The address's prefix length, in bits.
        pub prefix: u8,
        /// The network's gateway, once the system reports a default route.
        pub gateway: Option<IpAddr>,
    }

    /// Ask the system to join a network (R76).
    ///
    /// The system answers with its completion, which this module records.
    /// Read it with [`completion`]. A user's decline of the system's join
    /// dialog answers there as an error, not here.
    ///
    /// # Errors
    ///
    /// A framework error where the platform answers one, its WPA3 selector
    /// standing absent on iOS and iPadOS, and the Wi-Fi interface standing
    /// absent on macOS.
    pub fn join(ssid: &str, passphrase: &str, security: Security) -> Result<(), crate::Error> {
        crate::platform::hotspot::join(ssid, passphrase, security)
    }

    /// The system's answer to the last [`join`], once it answers.
    pub fn completion() -> Option<Result<Joining, crate::Error>> {
        crate::platform::hotspot::completion()
    }

    /// Release the system's standing for a network (R76).
    ///
    /// # Errors
    ///
    /// A framework error where the platform answers one.
    pub fn leave(ssid: &str) -> Result<(), crate::Error> {
        crate::platform::hotspot::leave(ssid)
    }

    /// What a joined network stands at (R76).
    ///
    /// It answers `Some` once the device's interface carries the joined
    /// network's address, and its gateway is `None` while the system reports
    /// no default route.
    pub fn joined() -> Option<Joined> {
        crate::platform::hotspot::joined()
    }

    /// The process's standing with Location Services (R76 slice 9c).
    #[cfg(target_os = "macos")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum LocationAccess {
        /// The user allowed it.
        Granted,
        /// The user or a policy refused it.
        Denied,
        /// The user was never asked.
        NotAsked,
    }

    /// The process's standing with Location Services.
    ///
    /// macOS hands a process without it scanned networks that carry no
    /// name, and refuses to join them, so a join asks for it first
    /// (measured 2026-10-14).
    #[cfg(target_os = "macos")]
    pub fn location_access() -> LocationAccess {
        crate::platform::location::access()
    }

    /// Ask the user for location access, on the main queue.
    #[cfg(target_os = "macos")]
    pub fn request_location_access() {
        crate::platform::location::request();
    }
}

/// Peer discovery through the system Bonjour service, which needs no multicast entitlement for the declared service type (R76).
#[cfg(any(target_os = "ios", target_os = "macos"))]
pub mod discovery {
    pub use crate::platform::bonjour::Bonjour;
}

#[cfg(any(target_os = "ios", target_os = "macos", test))]
mod route;

#[cfg(not(any(target_os = "ios", target_os = "macos")))]
#[path = "unsupported.rs"]
mod platform;

#[cfg(any(target_os = "ios", target_os = "macos"))]
mod platform;
