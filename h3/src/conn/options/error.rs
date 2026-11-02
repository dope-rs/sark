use dope::core::Errno;
use o3::{alloc, buffer::pool, depot::map::compact};
use sark_http::payload::compress::dynamic::plan;

use crate::{
    conn::options::{Arithmetic, Bound, Resource},
    qpack::{self, ledger},
};

#[derive(Clone, Debug)]
pub enum Error {
    Entropy(Errno),
    Zero(Resource),
    Below {
        resource: Resource,
        actual: usize,
        minimum: usize,
    },
    Capacity {
        resource: Resource,
        requested: usize,
        error: Bound,
    },
    Arithmetic {
        resource: Resource,
        operation: Arithmetic,
        left: usize,
        right: usize,
    },
    Exceeds {
        resource: Resource,
        actual: usize,
        limit: Resource,
        maximum: usize,
    },
    Table {
        resource: Resource,
        requested: usize,
        error: plan::Error,
    },
    Tables {
        encoder: usize,
        decoder: usize,
        error: plan::Error,
    },
    SectionPlane {
        offset: usize,
        capacity: usize,
        width: usize,
    },
    SectionIndex(compact::Error),
    SectionLength {
        len: usize,
    },
    SectionOverflow(alloc::Overflow),
    Pool {
        slots: usize,
        capacity: usize,
        error: pool::LayoutError,
    },
    Qpack(qpack::Storage),
    Allocation(alloc::Error),
    Queue(alloc::Error),
}

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}

impl From<qpack::Storage> for Error {
    fn from(error: qpack::Storage) -> Self {
        Self::Qpack(error)
    }
}

impl From<ledger::Error> for Error {
    fn from(error: ledger::Error) -> Self {
        match error {
            ledger::Error::Capacity(error) => Self::Capacity {
                resource: Resource::QpackSection,
                requested: error.requested(),
                error: Bound::Depot(error),
            },
            ledger::Error::Plane {
                offset,
                capacity,
                width,
            } => Self::SectionPlane {
                offset,
                capacity,
                width,
            },
            ledger::Error::Index(error) => Self::SectionIndex(error),
            ledger::Error::Length { len } => Self::SectionLength { len },
            ledger::Error::Overflow(error) => Self::SectionOverflow(error),
        }
    }
}
