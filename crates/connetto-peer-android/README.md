# The peer link's Android permissions and Bluetooth glue

[![Tests](https://github.com/connetto-rs/connetto-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/connetto-rs/connetto-rs/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/connetto-rs/connetto-rs/blob/main/LICENSE)
[![Coverage](https://codecov.io/gh/connetto-rs/connetto-rs/graph/badge.svg)](https://codecov.io/gh/connetto-rs/connetto-rs)

The peer link needs these Android permissions: `CHANGE_WIFI_MULTICAST_STATE`, for the mDNS discovery that finds a peer's link, `NEARBY_WIFI_DEVICES` (API 33 and later), for hosting and joining the local-only hotspot a peer dials through, and `BLUETOOTH_ADVERTISE`, `BLUETOOTH_CONNECT` and `BLUETOOTH_SCAN` (API 31 and later), for the beacon that hands the hotspot to a nearby device. This crate declares them in its manifest and bundles the Kotlin module the rest of the peer link drives over JNI. The application asks for whichever is missing through `connetto-client`'s `NativeClient::request_peer_permissions`, which opens the prompt over the screen in front.

It also bundles btleplug's Java, which the client's Bluetooth central needs in the application, under btleplug's licence in `android/btleplug-LICENSE.md`, with R8 rules keeping it in a shrinking build. `java_vm` hands btleplug the process's virtual machine under the jni btleplug links, and `use_application_class_loader` lets a thread attached from native code find btleplug's classes.
