#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod machine;
mod message;

pub use machine::{Action, Exchange, FRAME_BOUND, RETRY_CEILING, RETRY_FIRST};
pub use message::{BoundsError, MAX_MANIFEST_ROWS, Message, Transfer, captured_under, manifest};
