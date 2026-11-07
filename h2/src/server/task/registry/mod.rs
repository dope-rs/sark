use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::depot::{map::direct, queue::indexed::slab};

use crate::stream;

mod error;
mod table;
mod vacant;

pub(in crate::server::task) use error::Error;
pub(in crate::server::task) use table::Table;
pub(in crate::server::task) use vacant::Vacant;

pub(in crate::server) enum Tag {}

/// Per-connection stream-to-task reverse index.
pub(in crate::server::task) type Index = direct::Table<stream::Key, slab::Handle<Tag>>;

pub(in crate::server::task) struct Record<'d, ID: Identity, T> {
    pub(in crate::server::task) connection: conn::Id<'d, ID>,
    pub(in crate::server::task) stream: stream::Key,
    pub(in crate::server::task) task: T,
    /// No TaskPollReady has been reported for this admission.
    pub(in crate::server::task) unreported: bool,
}

impl<ID: Identity, T> Record<'_, ID, T> {
    fn into_parts(self) -> (stream::Key, T, bool) {
        (self.stream, self.task, self.unreported)
    }
}
