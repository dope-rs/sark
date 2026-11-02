//! Owned progress through an append-only request head.

mod progress;
mod state;

pub use progress::Progress;
pub use state::State;
pub mod line;
