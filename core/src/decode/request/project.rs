use sark_http::request::body::Error;

use crate::decode::request::Ready;

#[must_use]
#[derive(Debug)]
pub struct Project<K, P, S> {
    error: Error,
    ready: Ready<K, P, S>,
}

impl<K, P, S> Project<K, P, S> {
    pub(in crate::decode) const fn new(error: Error, ready: Ready<K, P, S>) -> Self {
        Self { error, ready }
    }

    pub const fn error(&self) -> Error {
        self.error
    }

    /// Ends the rejected request owner and returns the projection failure.
    pub fn discard(self) -> Error {
        drop(self.ready);
        self.error
    }
}
