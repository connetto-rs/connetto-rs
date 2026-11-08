//! The Apple platform's frameworks, each behind its one lock (R76).

#[expect(
    unsafe_code,
    reason = "the CoreBluetooth class property objc2 marks unsafe, sound as its SAFETY comment states"
)]
pub(super) mod bluetooth;
#[expect(
    unsafe_code,
    reason = "the dns_sd calls, each made on the session's one thread and sound as its SAFETY comment states"
)]
pub(super) mod bonjour;
#[expect(
    unsafe_code,
    reason = "the NetworkExtension and CoreWLAN calls objc2 marks unsafe and the libc reads of the Wi-Fi route, each sound as its SAFETY comment states"
)]
pub(super) mod hotspot;
#[cfg(target_os = "macos")]
#[expect(
    unsafe_code,
    reason = "the CoreLocation calls objc2 marks unsafe, each made on the main thread's one manager"
)]
pub(super) mod location;
