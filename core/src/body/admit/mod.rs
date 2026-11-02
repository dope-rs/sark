mod advance;
mod bound;
mod error;
mod plan;
mod start;
mod track;

pub(in crate::body) use advance::Advance;
use bound::{Bound, Kind};
pub use error::Error;
pub use plan::Plan;
pub use start::Start;
pub use track::Track;

/// Whether framing must retain request payload bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Retention {
    Retained,
    Discarded,
}

/// Whether a request builder may consume body segments before transport FIN.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Stream {
    Streaming,
    Buffered,
}

const _: () = assert!(core::mem::size_of::<Stream>() == 1);

/// Whether the complete logical body length was present in the request head.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Length {
    Known(usize),
    Unknown,
}

impl Length {
    pub const fn from_option(value: Option<usize>) -> Self {
        match value {
            Some(value) => Self::Known(value),
            None => Self::Unknown,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Length>() == 2 * core::mem::size_of::<usize>());

/// Input may still advance the admitted logical length.
#[derive(Debug)]
pub struct Open;

/// Input ended and its logical length satisfies the admitted bound.
#[derive(Debug)]
pub struct Finished;
