mod control;
mod events;
pub mod ingress;
pub(super) mod outbound;
pub mod replies;
mod send;
mod step;
pub(crate) mod write;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    Idle,
    Ready,
}

pub use control::Control;
use dope_quic::wire::stream::Id;
pub use events::Events;
pub use ingress::Receive;
use o3::buffer::{hold, storage};
use sark_core::manifold::stream::Boundary;
use sark_http::{field, field::metadata::section::rules};
pub use send::Send;
pub use step::Step;

use crate::{conn, stream};

/// One checked stream retained together with its exclusive connection loan.
pub struct Stream<'a, 'd, P: rules::Policy = rules::RetainRaw, A: rules::Push = rules::RejectPush> {
    conn: &'a mut conn::Connection<'d, P, A>,
    target: stream::Target,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Stream<'a, 'd, P, A> {
    pub const fn stream_id(&self) -> Id {
        self.target.stream_id()
    }

    pub fn headers<S: field::Source>(
        &mut self,
        fields: S,
        boundary: Boundary,
    ) -> Result<hold::Charge, conn::Error> {
        Send::new(self.conn).headers(self.target, fields, boundary)
    }

    pub fn data(&mut self, data: &[u8], boundary: Boundary) -> Result<(), conn::Error> {
        Send::new(self.conn).data(self.target, data, boundary)
    }

    pub fn owned(&mut self, data: Vec<u8>, boundary: Boundary) -> Result<(), conn::Error> {
        Send::new(self.conn).owned(self.target, data, boundary)
    }

    pub fn shared(&mut self, data: storage::Shared, boundary: Boundary) -> Result<(), conn::Error> {
        Send::new(self.conn).shared(self.target, data, boundary)
    }

    pub fn promise<S: field::Source>(
        &mut self,
        push_id: u64,
        fields: S,
    ) -> Result<hold::Charge, conn::Error> {
        Send::new(self.conn).promise(self.target, push_id, fields)
    }
}

const _: () = assert!(size_of::<Stream<'static, 'static>>() == 3 * size_of::<usize>());
