//! One-pass gRPC field-section admission and retained ownership.

use seal::Seal;

const JOINED: u32 = 1 << 31;

mod iter;
mod owner;
mod part;
mod plan;
mod request;
pub(crate) mod seal;
mod trailers;
mod view;

pub(crate) use iter::Iter;
pub use owner::Owner;
pub(crate) use owner::{Valid, fold::Failure};
pub use plan::Plan;
pub(crate) use request::Request;
pub(crate) use trailers::{Terminal, Trailers};
pub(in crate::protocol) use view::View;
