use core::convert::Infallible;

use o3::alloc;
use sark_http::{
    field::metadata::{huffman::decoder, integer},
    payload::compress::dynamic::insert,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Allocation(alloc::Error),
    NeedMore,
    BadIndex,
    BadInteger(integer::Overflow),
    BadString,
    Encoding { consumed: usize, decoded: usize },
    Dynamic(insert::Error),
    LateResize,
    MissingResize { limit: usize },
    Resize { actual: usize, limit: usize },
}

#[derive(PartialEq, Eq)]
pub(crate) enum Failure<E> {
    Decode(Error),
    Sink(E),
}

impl<E> From<Error> for Failure<E> {
    fn from(cause: Error) -> Self {
        Self::Decode(cause)
    }
}

impl Error {
    pub(crate) fn retention(
        cause: decoder::DecodeError<alloc::Error>,
        progress: decoder::Progress,
    ) -> Self {
        match cause {
            decoder::DecodeError::InvalidEncoding(cause) => {
                Self::encoding(decoder::DecodeError::InvalidEncoding(cause), progress)
            }
            decoder::DecodeError::Sink(cause) => Self::Allocation(cause),
        }
    }

    pub(crate) fn encoding(
        cause: decoder::DecodeError<Infallible>,
        progress: decoder::Progress,
    ) -> Self {
        let decoder::DecodeError::InvalidEncoding(decoder::Error) = cause;
        Self::Encoding {
            consumed: progress.consumed(),
            decoded: progress.decoded(),
        }
    }
}

impl From<integer::Error> for Error {
    fn from(error: integer::Error) -> Self {
        match error {
            integer::Error::NeedMore => Self::NeedMore,
            integer::Error::Overflow(cause) => Self::BadInteger(cause),
        }
    }
}

impl From<integer::Overflow> for Error {
    fn from(cause: integer::Overflow) -> Self {
        Self::BadInteger(cause)
    }
}

impl From<decoder::Error> for Error {
    fn from(error: decoder::Error) -> Self {
        match error {
            decoder::Error => Self::BadString,
        }
    }
}

impl From<insert::Error> for Error {
    fn from(error: insert::Error) -> Self {
        Self::Dynamic(error)
    }
}
