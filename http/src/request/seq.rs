use std::mem;

use dope_core::driver::schedule::sequence;

use crate::request::Id;

/// Single-threaded request identity sequence for one bound application.
#[repr(transparent)]
pub struct Seq<'d>(sequence::Block<'d>);

impl<'d> Seq<'d> {
    pub fn new(sequence: sequence::Block<'d>) -> Self {
        Self(sequence)
    }

    pub fn try_next(&self) -> Result<Id<'d>, sequence::Exhausted> {
        Ok(Id::new(self.0.try_next()?))
    }
}

const _: () = assert!(mem::size_of::<Seq<'static>>() == 2 * mem::size_of::<usize>());
