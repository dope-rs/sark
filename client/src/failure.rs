use std::{convert::Infallible, num::NonZero};

use crate::{Error, Safety};

/// A failed logical HTTP call with its cause and replay safety intact.
#[must_use = "failure cause and replay safety must be observed or propagated"]
#[derive(Debug)]
pub struct Failure<E = Infallible, M = Infallible> {
    cause: Error<E, M>,
    safety: Safety,
    attempt: NonZero<u8>,
}

impl<E, M> Failure<E, M> {
    pub const fn new(cause: Error<E, M>, safety: Safety) -> Self {
        Self {
            cause,
            safety,
            attempt: NonZero::<u8>::MIN,
        }
    }

    pub const fn cause(&self) -> &Error<E, M> {
        &self.cause
    }

    pub const fn safety(&self) -> Safety {
        self.safety
    }

    /// The dispatch attempt this failure came from, 1-based.
    ///
    /// Deadline admission may reject an attempt before submission. A backoff
    /// failure retains the preceding attempt until the next dispatch begins.
    pub const fn attempt(&self) -> u8 {
        self.attempt.get()
    }

    /// Stamps the attempt count the retry loop actually reached.
    pub fn attempted(mut self, attempt: NonZero<u8>) -> Self {
        self.attempt = attempt;
        self
    }

    pub fn into_parts(self) -> (Error<E, M>, Safety) {
        (self.cause, self.safety)
    }
}

impl<M> Failure<Infallible, M> {
    #[doc(hidden)]
    pub fn generalize<E>(self) -> Failure<E, M> {
        let Self {
            cause,
            safety,
            attempt,
        } = self;
        Failure {
            cause: cause.generalize(),
            safety,
            attempt,
        }
    }
}

impl<E> Failure<E> {
    pub fn widen<M>(self) -> Failure<E, M> {
        let Self {
            cause,
            safety,
            attempt,
        } = self;
        Failure {
            cause: cause.widen(),
            safety,
            attempt,
        }
    }
}
