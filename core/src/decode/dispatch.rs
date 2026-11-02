use core::convert::Infallible;

use dope_core::driver::schedule::sequence;

use crate::{body, route};

#[derive(Debug)]
pub enum Error<E, B, I> {
    Pending,
    Busy,
    Invalid(I),
    Body(B),
    Moved,
    Encode(E),
    Sequence(sequence::Exhausted),
    Unsupported,
}

impl<E, B, I: route::Never> Error<E, B, I> {
    pub fn narrow(self) -> Error<E, B, Infallible> {
        match self {
            Self::Pending => Error::Pending,
            Self::Busy => Error::Busy,
            Self::Invalid(error) => error.never(),
            Self::Body(error) => Error::Body(error),
            Self::Moved => Error::Moved,
            Self::Encode(error) => Error::Encode(error),
            Self::Sequence(error) => Error::Sequence(error),
            Self::Unsupported => Error::Unsupported,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Error<Infallible, Infallible, Infallible>>() == 1);
const _: () = assert!(
    core::mem::size_of::<Error<Infallible, body::Error, Infallible>>()
        == core::mem::size_of::<body::Error>()
);
