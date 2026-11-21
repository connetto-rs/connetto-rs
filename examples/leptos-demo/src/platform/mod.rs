//! The platform layer behind the screen, chosen by feature. The components
//! call [`act`], read the view signal [`run_page`] returns, follow their live
//! queries on the [`Source`] [`source`] gives, and ask for the photos' display
//! sources and the team notes' provisional accounts.

#[cfg(all(feature = "web", feature = "tauri"))]
compile_error!("the `web` and `tauri` layers are exclusive, build one of them");

#[cfg(not(any(feature = "web", feature = "tauri")))]
compile_error!("build with the `web` or the `tauri` layer");

#[cfg(feature = "web")]
pub mod web;
#[cfg(feature = "web")]
pub use web::{Source, act, photo_sources, run_page, source, team_note_accounts};

#[cfg(feature = "tauri")]
pub mod tauri;
#[cfg(feature = "tauri")]
pub use tauri::{Source, act, photo_sources, run_page, source, team_note_accounts};
