use core::convert::Infallible;

use dope::manifold::client::session::{codec, response::Outcome};
use o3::buffer;
use sark_http::h1::head::{self, response};

use crate::protocol::{
    Error,
    codec::{
        State,
        output::{Parsed, Preview},
        state::Framing,
    },
};

/// Cumulative head validation retains the exact source it has inspected.
pub struct Storage<S> {
    pub(super) source: S,
    pub(super) phase: Phase,
}

pub(super) enum Phase {
    Head(response::raw::State),
    Ready(response::Parts),
    Rejected(head::ParseError<Infallible>),
    Body,
}

const _: () =
    assert!(size_of::<Storage<()>>() <= size_of::<response::raw::State>() + size_of::<usize>());

impl Phase {
    pub(super) fn new(state: &State<'_>, limit: head::Limit) -> Self {
        if matches!(state.framing, Framing::Head) {
            Self::Head(response::raw::State::new(limit))
        } else {
            Self::Body
        }
    }
}

impl<S: buffer::raw::Source> Storage<S> {
    pub fn source(&self) -> &S {
        &self.source
    }
}

impl<S> Storage<S> {
    pub(super) fn rejected<'d, T: Outcome, A, F>(
        &mut self,
        error: head::ParseError<Infallible>,
        state: &mut State<'d>,
        admit: A,
    ) -> T
    where
        A: FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        let cause = match error {
            head::ParseError::Boundary(error) => Error::HeadLimit(error),
            error => Error::Head(error),
        };
        match admit(Err(&cause)) {
            codec::Admission::RetryInput => {
                self.phase = Phase::Rejected(error);
                T::retry_input()
            }
            codec::Admission::Ready(emit) => {
                self.phase = Phase::Body;
                T::accept(emit(Err(cause), state))
            }
        }
    }
}
