use std::mem;

use sark_http::request::{self, span};

use crate::request::frame;

mod maybe;
mod peer;

pub use maybe::Maybe;
pub use peer::Peer;

/// Authority evidence can only come from the validated head representations.
///
#[doc(hidden)]
pub trait Proof: Copy + frame::Seal {
    fn try_from(layout: request::Layout) -> Result<Self, request::Error>;

    fn span(self) -> Option<span::Span>;
}

const _: () = {
    assert!(mem::size_of::<Maybe>() == mem::size_of::<Option<span::Span>>());
    assert!(mem::size_of::<Peer>() == mem::size_of::<span::Span>());
};
