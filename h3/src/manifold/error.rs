//! Errors at the HTTP/3-to-QUIC execution boundary.

use dope_quic::conn::stream::{
    self,
    access::bindings,
    recv::{credit, hold},
    resident,
};
use o3::alloc;

use crate::{conn, frame, manifold::Credit};

#[derive(Debug)]
pub enum Error {
    QuicStream(stream::Error),
    Abandon(stream::abandon::Error),
    Resident(resident::Error),
    Binding(bindings::Fault),
    Stale(resident::Stale),
    Foreign(resident::Foreign),
    Origin {
        expected: resident::Origin,
        found: resident::Origin,
    },
    Allocation(alloc::Error),
    Retain(alloc::Exhausted),
    Credit(Credit),
    Budget {
        error: conn::Error,
        credit: credit::Detached<credit::Local>,
    },
    Release(hold::ReleaseError<credit::Detached<credit::Local>>),
    H3(conn::Error),
}

impl Error {
    pub(in crate::manifold) const fn code(&self) -> frame::ErrorCode {
        match self {
            Self::H3(error) | Self::Budget { error, .. } => error.error_code(),
            Self::QuicStream(_)
            | Self::Abandon(_)
            | Self::Resident(_)
            | Self::Binding(_)
            | Self::Stale(_)
            | Self::Foreign(_)
            | Self::Origin { .. }
            | Self::Allocation(_)
            | Self::Retain(_)
            | Self::Credit(_)
            | Self::Release(_) => frame::ErrorCode::Internal,
        }
    }
}

impl From<stream::Error> for Error {
    fn from(error: stream::Error) -> Self {
        Self::QuicStream(error)
    }
}

impl From<stream::abandon::Error> for Error {
    fn from(error: stream::abandon::Error) -> Self {
        Self::Abandon(error)
    }
}

impl From<bindings::Fault> for Error {
    fn from(error: bindings::Fault) -> Self {
        Self::Binding(error)
    }
}

impl From<resident::Foreign> for Error {
    fn from(error: resident::Foreign) -> Self {
        Self::Foreign(error)
    }
}

impl From<resident::Stale> for Error {
    fn from(error: resident::Stale) -> Self {
        Self::Stale(error)
    }
}

impl From<resident::Error> for Error {
    fn from(error: resident::Error) -> Self {
        Self::Resident(error)
    }
}

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}

impl From<conn::Error> for Error {
    fn from(error: conn::Error) -> Self {
        Self::H3(error)
    }
}

impl From<Credit> for Error {
    fn from(error: Credit) -> Self {
        Self::Credit(error)
    }
}

impl From<hold::ReleaseError<credit::Detached<credit::Local>>> for Error {
    fn from(error: hold::ReleaseError<credit::Detached<credit::Local>>) -> Self {
        Self::Release(error)
    }
}
