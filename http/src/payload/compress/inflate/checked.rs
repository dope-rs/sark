use std::convert::Infallible;

use crate::payload::compress::inflate::{Error, Failure};

pub(super) const STEP: usize = 64 * 1024;

/// A completed work failure or a checkpoint that interrupted unfinished work.
#[derive(Debug)]
pub enum Checked<T, E> {
    Work(T),
    Check(E),
}

impl<T, E> Checked<T, E> {
    pub fn map_work<U>(self, map: impl FnOnce(T) -> U) -> Checked<U, E> {
        match self {
            Self::Work(error) => Checked::Work(map(error)),
            Self::Check(error) => Checked::Check(error),
        }
    }
}

impl<T> Checked<T, Infallible> {
    pub fn into_work(self) -> T {
        match self {
            Self::Work(error) => error,
            Self::Check(error) => match error {},
        }
    }
}

impl<E> From<Error> for Checked<Error, E> {
    fn from(error: Error) -> Self {
        Self::Work(error)
    }
}

impl<T: From<Failure>, E> From<Failure> for Checked<T, E> {
    fn from(error: Failure) -> Self {
        Self::Work(error.into())
    }
}
