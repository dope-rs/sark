//! One-pass gRPC field-section admission and retained ownership.

use sark_http::field::metadata::{packed, section::rules};

use seal::Seal;

const JOINED: u32 = 1 << 31;

mod iter;
mod owner;
mod part;
mod plan;
pub(crate) mod seal;
mod trailers;
mod view;

pub(crate) use iter::Iter;
pub type Owner = packed::Decoded<o3::buffer::pool::Span, { seal::LEN }>;
pub type Plan = rules::RetainRaw<{ seal::LEN }>;
pub(crate) use owner::{Valid, fold::Failure};
pub(crate) use plan::build;
pub(crate) use trailers::{Terminal, Trailers};
pub(in crate::protocol) use view::View;
