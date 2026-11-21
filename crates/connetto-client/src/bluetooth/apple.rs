//! The Apple Bluetooth permission, over the process-wide peer bridge (R77
//! decision 23).
//!
//! Apple devices only join, so no peripheral runs here. The permission is
//! `CBManager`'s class property, and the power comes from btleplug's
//! central through [`super::CentralReadiness`].

use connetto_peer_apple::bluetooth::{self as apple, Authorization};

use super::central_readiness::Permission;

/// The app's Bluetooth permission, read with no manager.
pub(crate) fn permission() -> Permission {
    match apple::authorization() {
        Authorization::NotDetermined => Permission::Unasked,
        Authorization::Denied => Permission::Refused,
        Authorization::Allowed => Permission::Allowed,
    }
}
