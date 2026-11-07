//! The app's Bluetooth permission, read with no manager (R77 decision 23).

use objc2_core_bluetooth::{CBManager, CBManagerAuthorization};

use crate::bluetooth::Authorization;

/// The app's Bluetooth authorization, `CBManager.authorization`.
pub(crate) fn authorization() -> Authorization {
    // SAFETY: `+[CBManager authorization]` is a class property of every
    // system the crate builds for (iOS 13.1 and macOS 10.15 onwards). It
    // touches no manager instance and takes no argument, so no receiver,
    // aliasing or thread rule can be broken, and its answer is a plain enum.
    let authorization = unsafe { CBManager::authorization_class() };
    match authorization {
        CBManagerAuthorization::NotDetermined => Authorization::NotDetermined,
        CBManagerAuthorization::AllowedAlways => Authorization::Allowed,
        // Restricted, denied, and any value a later system adds.
        _ => Authorization::Denied,
    }
}
