use std::mem;

use crate::client::exchange::request::Encoded;

pub(in crate::client) struct Prepared<R> {
    route: R,
    encoded: Encoded,
}

impl<R> Prepared<R> {
    pub(in crate::client) const fn new(route: R, encoded: Encoded) -> Self {
        Self { route, encoded }
    }

    pub(in crate::client) const fn route(&self) -> &R {
        &self.route
    }

    pub(in crate::client) fn into_parts(self) -> (R, Encoded) {
        (self.route, self.encoded)
    }
}

const _: () = assert!(mem::size_of::<Option<Prepared<()>>>() == mem::size_of::<Prepared<()>>());
