mod queue;

pub(in crate::manifold::requests) use queue::{Queue, Wake};
use sark_http::payload::response::status::Code;

pub(in crate::manifold::requests) struct State {
    pub(in crate::manifold::requests) code: Code,
    pub(in crate::manifold::requests) queue: Queue,
}

#[derive(Clone, Copy)]
pub(in crate::manifold) enum Next<T> {
    Blocked(T),
    Finished(T),
}
