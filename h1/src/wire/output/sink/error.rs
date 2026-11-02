use core::mem;

use o3::{
    alloc,
    buffer::{storage, write::consume},
    mem::{Rejected, rc},
};
use sark_http::{
    h1::{codec::canned, head::line::Version},
    payload::{
        fixed, owned,
        response::{exact, status::Code},
    },
};

use crate::wire::{cache, response};

/// Exact leaf failure while encoding or retaining an HTTP/1 response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Allocation(alloc::Exhausted),
    Wire(response::Error),
    Overflow { base: usize, added: usize },
    Encoding { actual: usize },
    Cache,
    CacheEntry(cache::Error),
    Size(exact::Overflow),
    Build(storage::BuildError<consume::Error>),
    Owner(owned::Error),
    Informational(Code),
    Tunnel(Code),
    StreamingVersion(Version),
}

impl Error {
    pub fn body<'a>(&self) -> fixed::Body<'a> {
        match self {
            Self::Allocation(_)
            | Self::Wire(response::Error::Storage(storage::AllocError::Exhausted(_)))
            | Self::Build(storage::BuildError::Exhausted(_))
            | Self::Owner(owned::Error::Metadata(alloc::Error::Exhausted))
            | Self::Owner(owned::Error::Retain(_))
            | Self::Owner(owned::Error::Storage(storage::AllocError::Exhausted(_)))
            | Self::Owner(owned::Error::Build(storage::BuildError::Exhausted(_))) => {
                canned::SERVICE_UNAVAILABLE
            }
            Self::Wire(_)
            | Self::Overflow { .. }
            | Self::Encoding { .. }
            | Self::Cache
            | Self::CacheEntry(_)
            | Self::Size(_)
            | Self::Build(_)
            | Self::Owner(_) => canned::INTERNAL_SERVER_ERROR,
            Self::Informational(_) | Self::Tunnel(_) | Self::StreamingVersion(_) => {
                canned::INTERNAL_SERVER_ERROR
            }
        }
    }
}

impl From<response::Error> for Error {
    fn from(error: response::Error) -> Self {
        Self::Wire(error)
    }
}

impl From<storage::BuildError<consume::Error>> for Error {
    fn from(error: storage::BuildError<consume::Error>) -> Self {
        Self::Build(error)
    }
}

impl From<owned::Error> for Error {
    fn from(error: owned::Error) -> Self {
        Self::Owner(error)
    }
}

impl From<rc::Error> for Error {
    fn from(error: rc::Error) -> Self {
        Self::Owner(owned::Error::Retain(error))
    }
}

impl From<Rejected<storage::Shared, cache::Error>> for Error {
    fn from(error: Rejected<storage::Shared, cache::Error>) -> Self {
        Self::CacheEntry(error.discard())
    }
}

const _: () = assert!(mem::size_of::<Error>() <= 5 * mem::size_of::<usize>());
