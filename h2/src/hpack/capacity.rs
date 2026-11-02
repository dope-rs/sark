use dope::core::Errno;
use o3::alloc;
use sark_http::payload::compress::dynamic::{plan, table::delta::buffer};

#[derive(Debug)]
pub enum Error {
    Plan(plan::Error),
    Entropy(Errno),
    Allocation(alloc::Error),
    Buffer(buffer::Error),
}

impl From<plan::Error> for Error {
    fn from(error: plan::Error) -> Self {
        Self::Plan(error)
    }
}

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}
