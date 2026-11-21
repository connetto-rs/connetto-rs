# connetto-peer-apple

[![Tests](https://github.com/connetto-rs/connetto-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/connetto-rs/connetto-rs/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/connetto-rs/connetto-rs/blob/main/LICENSE)
[![Coverage](https://codecov.io/gh/connetto-rs/connetto-rs/graph/badge.svg)](https://codecov.io/gh/connetto-rs/connetto-rs)

The Apple platform bridge for connetto's peer link (R76). Apple devices only join (R77 decision 23). `bluetooth::authorization` reads the app's Bluetooth permission from `CBManager`'s class property, which needs no manager and asks the user nothing, beside the central `connetto-client` runs through btleplug. `hotspot` is the joiner's join of the offered network, the Network Extension's on iOS and `CoreWLAN`'s on the Mac by a directed scan and its association, and its read of the network the device joined. This is the workspace's second unsafe crate, and every `unsafe` block sits in its platform module.

Every call that waits on the platform is bounded by [`HOP_WITHIN`]. On the Mac the join runs on its own worker thread, the directed scan and the association blocking there, and a generation counter drops a stale worker's answer, whose late association a `leave` standing in between unwinds.

A WPA3 join asks the platform for its `setWPA3PersonalPassphrase:` selector, and answers `Missing` where the platform does not.

`discovery::Bonjour` advertises and browses `_connetto-peer._tcp` through the system's `dns_sd` service for `connetto-peer`'s discovery, since iOS lets an app send and receive raw multicast only under Apple's managed multicast entitlement. An app using it lists the service type in `NSBonjourServices` and sets `NSLocalNetworkUsageDescription`, without which iOS refuses every operation.

On every target but Apple the functions answer `Unsupported` or empty, and the Bluetooth permission reads `Denied`.

```rust
#[cfg(not(any(target_os = "ios", target_os = "macos")))]
fn main() {
    assert_eq!(
        connetto_peer_apple::bluetooth::authorization(),
        connetto_peer_apple::bluetooth::Authorization::Denied
    );
    assert!(matches!(connetto_peer_apple::hotspot::completion(), None));
}

#[cfg(any(target_os = "ios", target_os = "macos"))]
fn main() {
    // The Apple targets stand the platform's live states.
}
```
