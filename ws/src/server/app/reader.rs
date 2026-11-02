use std::ops;

use crate::server::{self, app};

/// A message callback with no application reply capability.
pub struct Reader<F, E> {
    pub(in crate::server) receive: F,
    pub(in crate::server) failure: E,
}

impl<F: Fn(server::Message<'_>), E: Fn(server::Error)> app::Sink for Reader<F, E> {
    fn capacity(&self, _: usize) -> Result<usize, server::Error> {
        Ok(0)
    }

    fn message<'a>(
        &self,
        msg: server::Message<'a>,
        _: &mut server::Response<'_>,
    ) -> Result<ops::ControlFlow<()>, server::Error> {
        (self.receive)(msg);
        Ok(ops::ControlFlow::Continue(()))
    }

    fn failed(&self, error: server::Error) {
        (self.failure)(error);
    }
}
