use o3::{buffer::storage, mem::rc};
use storage::range;
pub(crate) mod body;
pub mod cache;
mod failure;
pub mod head;
pub mod output;
pub mod parsing;
pub(crate) mod recv;
pub mod response;

pub use failure::Failure;

#[derive(Debug)]
pub enum InputError {
    Range(range::Error),
    Storage(storage::AllocError),
    Share(rc::Error),
}
