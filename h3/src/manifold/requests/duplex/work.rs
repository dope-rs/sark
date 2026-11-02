use dope_quic::wire::stream;
use sark_core::duplex;

use crate::{
    manifold::requests::{
        Id,
        duplex::{Response, input::Input},
    },
    stream::Target,
};

pub(in crate::manifold) struct Work<'turn, 'd, D: duplex::Session<'d>> {
    pub(super) id: Id,
    pub(in crate::manifold) input: &'turn mut Input<'d, D>,
    pub(in crate::manifold) response: &'turn mut Response,
}

impl<'turn, 'd, D: duplex::Session<'d>> Work<'turn, 'd, D> {
    pub(in crate::manifold) const fn new(
        id: Id,
        input: &'turn mut Input<'d, D>,
        response: &'turn mut Response,
    ) -> Self {
        Self {
            id,
            input,
            response,
        }
    }

    pub(in crate::manifold) const fn id(&self) -> Id {
        self.id
    }

    pub(in crate::manifold) const fn stream_id(&self) -> stream::Id {
        self.id.stream_id()
    }

    pub(in crate::manifold) fn target(&self) -> Target {
        self.id.target()
    }
}
