use dope::core::driver::route::Identity;

use crate::{server::Target, stream};

/// One task removed atomically from both global and connection-local ownership.
pub(in crate::server::task) struct Taken<'d, ID: Identity, T> {
    pub(in crate::server::task) target: Target<'d, ID>,
    pub(in crate::server::task) stream: stream::Key,
    pub(in crate::server::task) task: T,
}

impl<'d, ID: Identity, T> Taken<'d, ID, T> {
    pub(in crate::server::task) fn into_parts(self) -> (Target<'d, ID>, stream::Key, T) {
        (self.target, self.stream, self.task)
    }
}
