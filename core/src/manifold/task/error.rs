use std::{convert::Infallible, mem};

use dope_core::driver::schedule::sequence;

#[derive(Debug)]
pub enum Error<E> {
    Admission(E),
    Sequence(sequence::Exhausted),
    Unsupported,
}

const _: () = assert!(mem::size_of::<Error<Infallible>>() == 1);
