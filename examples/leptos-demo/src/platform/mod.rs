//! The platform layer behind the screen. The components call [`act`] and
//! read the view signal the platform keeps current.

pub mod web;

pub use web::{act, read_picked_file};
