use o3::{
    alloc,
    buffer::{storage, write::consume},
    mem::rc,
};

/// Recoverable failure while assembling or materializing retained body bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Discarded,
    Metadata(alloc::Error),
    Retain(rc::Error),
    Storage(storage::AllocError),
    Build(storage::BuildError<consume::Error>),
}

const _: () = assert!(core::mem::size_of::<Error>() <= 4 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::align_of::<Error>() == core::mem::align_of::<usize>());
