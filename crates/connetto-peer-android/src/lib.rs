//! The bundled Kotlin module behind the connetto peer link's hotspot and
//! beacon, with the Android permissions it declares, and btleplug's virtual
//! machine and Java for the Bluetooth central.

#![doc = include_str!("../README.md")]

#[cfg(target_os = "android")]
mod android;

#[cfg(target_os = "android")]
pub use android::{VmError, java_vm, use_application_class_loader};
/// The jni btleplug links, whose virtual machine [`java_vm`] answers.
#[cfg(target_os = "android")]
pub use jni;
