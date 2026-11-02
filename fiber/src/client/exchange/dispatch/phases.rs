use std::convert::Infallible;

use sark_client::Error;

/// Failure produced while cancellation still proves that no request entered egress.
#[repr(transparent)]
pub(in crate::client) struct Rejected<E = Infallible, M = Infallible>(Error<E, M>);

/// Stream produced only after the request reservation and transport claim commit.
#[repr(transparent)]
pub(in crate::client) struct Submitted<S>(S);

impl<E, M> Rejected<E, M> {
    pub(in crate::client) const fn new(cause: Error<E, M>) -> Self {
        Self(cause)
    }

    pub(in crate::client) fn into_cause(self) -> Error<E, M> {
        self.0
    }
}

impl<S> Submitted<S> {
    pub(in crate::client) const fn new(stream: S) -> Self {
        Self(stream)
    }

    pub(in crate::client) fn into_inner(self) -> S {
        self.0
    }
}

const _: () = {
    assert!(size_of::<Rejected>() == size_of::<Error>());
    assert!(size_of::<Submitted<usize>>() == size_of::<usize>());
};
