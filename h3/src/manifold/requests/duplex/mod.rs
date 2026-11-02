use sark_core::duplex;

use crate::manifold::requests::entries::status;

pub(super) mod input;
pub(in crate::manifold) mod work;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::manifold) enum Response {
    #[default]
    Pending,
    Sent,
}

pub(super) struct State<'d, D: duplex::Session<'d>> {
    pub(super) input: input::Input<'d, D>,
    pub(super) response: Response,
    pub(super) queue: status::Queue,
}

#[derive(Clone, Copy)]
pub(in crate::manifold) enum Next<T> {
    Idle(T),
    Ready(T),
    Blocked(T),
    Finished(T),
}
