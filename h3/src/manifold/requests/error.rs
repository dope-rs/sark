use o3::{
    buffer::{segments, segments::assembly},
    depot::arena::hash::errors,
};
use sark_core::{
    body::{self, buffered, credit},
    duplex::machine,
};

use crate::manifold::requests::entries;

/// One refused request, classified at its rejection site.
/// Phase-only rejection sites map every observed phase to the same internal failure.
#[derive(Debug)]
pub(in crate::manifold) enum Error {
    Lookup(errors::Lookup<entries::Tag>),
    Identity,
    Malformed,
    Unsupported,
    Body(body::Error),
    Payload(buffered::Error<assembly::Error>),
    Segments(segments::Error),
    Duplex(machine::FinishError),
    Build,
    Dispatch,
}

impl From<credit::Error> for Error {
    fn from(error: credit::Error) -> Self {
        Self::Body(body::Error::Credit(error))
    }
}

impl From<body::Error> for Error {
    fn from(error: body::Error) -> Self {
        Self::Body(error)
    }
}

impl From<machine::FinishError> for Error {
    fn from(error: machine::FinishError) -> Self {
        Self::Duplex(error)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Error>() <= 8 * std::mem::size_of::<usize>());
    assert!(std::mem::align_of::<Error>() == std::mem::align_of::<usize>());
};
