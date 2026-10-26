//! The platform layer behind the screen, chosen by feature. The components
//! call [`act`] and read the view signal [`run_page`] returns.

#[cfg(all(feature = "web", feature = "tauri"))]
compile_error!("the `web` and `tauri` layers are exclusive, build one of them");

#[cfg(not(any(feature = "web", feature = "tauri")))]
compile_error!("build with the `web` or the `tauri` layer");

#[cfg(feature = "web")]
pub mod web;
#[cfg(feature = "web")]
pub use web::{act, run_page};

#[cfg(feature = "tauri")]
pub mod tauri;
#[cfg(feature = "tauri")]
pub use tauri::{act, run_page};
