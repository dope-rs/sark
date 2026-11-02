use core::{cell::Cell, num::NonZero};

use o3::buffer::segments;
use sark_core::{
    body::{self, credit},
    life,
};
use sark_http::payload::owned;

pub const BYTES: &[u8] = b"abc";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Invalid;

pub struct Ready<'d> {
    segments: Vec<owned::Segment<'d>>,
    finished: bool,
    outcome: Result<(), Invalid>,
    valid: &'d Cell<bool>,
}

impl<'d> Ready<'d> {
    pub fn new(outcome: Result<(), Invalid>, valid: &'d Cell<bool>) -> Self {
        Self {
            segments: Vec::new(),
            finished: false,
            outcome,
            valid,
        }
    }
}

impl<'d> body::Build<'d> for Ready<'d> {
    type Output = Vec<owned::Segment<'d>>;
    type StepError = Invalid;

    fn finish_input(mut self, declared: usize) -> Self {
        self.valid
            .set(self.valid.get() && !self.finished && declared == 3);
        self.finished = true;
        self
    }

    fn step(self, _: NonZero<usize>) -> body::Step<Self, Self::Output, Invalid> {
        self.valid.set(self.valid.get() && self.finished);
        body::Step::Ready {
            output: self.segments,
            released: credit::Amount::new(0, segments::Count::EMPTY),
        }
    }
}

impl<'d> body::Push<'d> for Ready<'d> {
    type Error = Invalid;

    fn observe<C: Copy, S: Copy, O: life::Observer<C, S>>(error: &Self::Error, observer: &mut O) {
        observer.diagnostic(&("push", error));
    }

    fn push(mut self, segment: owned::Segment<'d>) -> body::Transfer<Self, Self::Error> {
        self.segments.push(segment);
        match self.outcome {
            Ok(()) => body::Transfer::Accepted(self),
            Err(error) => body::Transfer::Rejected { build: self, error },
        }
    }
}

pub struct Failed<'d> {
    segments: Vec<owned::Segment<'d>>,
    released: credit::Amount,
    dropped: &'d Cell<(usize, usize)>,
    state: usize,
}

impl<'d> Failed<'d> {
    pub fn new(dropped: &'d Cell<(usize, usize)>, released: credit::Amount) -> Self {
        Self {
            segments: Vec::new(),
            released,
            dropped,
            state: 0,
        }
    }
}

impl<'d> body::Build<'d> for Failed<'d> {
    type Output = ();
    type StepError = Invalid;

    fn finish_input(mut self, declared: usize) -> Self {
        self.state = declared;
        self
    }

    fn step(mut self, budget: NonZero<usize>) -> body::Step<Self, (), Invalid> {
        self.state = budget.get();
        body::Step::Failed {
            released: self.released,
            build: self,
            error: Invalid,
        }
    }
}

impl<'d> body::Push<'d> for Failed<'d> {
    type Error = Invalid;

    fn observe<C: Copy, S: Copy, O: life::Observer<C, S>>(error: &Invalid, observer: &mut O) {
        observer.diagnostic(error);
    }

    fn push(mut self, segment: owned::Segment<'d>) -> body::Transfer<Self, Invalid> {
        self.segments.push(segment);
        body::Transfer::Accepted(self)
    }
}

impl Drop for Failed<'_> {
    fn drop(&mut self) {
        self.dropped.set((self.dropped.get().0 + 1, self.state));
    }
}
