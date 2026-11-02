use std::convert;

use crate::route::Never;

/// Exact failure while constructing request input from routed parameters or body data.
#[derive(Debug)]
pub enum Input<P, B, R = convert::Infallible> {
    Params(P),
    Body(B),
    Retain(R),
}

impl<P: Never, B: Never, R: Never> Never for Input<P, B, R> {
    fn never<T>(self) -> T {
        match self {
            Self::Params(error) => error.never(),
            Self::Body(error) => error.never(),
            Self::Retain(error) => error.never(),
        }
    }
}
