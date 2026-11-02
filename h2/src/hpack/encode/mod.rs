use o3::alloc;
use sark_http::payload::compress::dynamic::table::delta::{self, buffer};

mod commit;
mod encoder;
pub mod fields;
mod literal;

pub(crate) use commit::Commit;
pub use encoder::Encoder;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Dynamic(delta::Fault),
    Allocation(alloc::Error),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Create {
    Table(alloc::Error),
    Buffer(buffer::Error),
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Plain,
    Shortest,
}

const _: () = assert!(core::mem::size_of::<Mode>() == core::mem::size_of::<bool>());
