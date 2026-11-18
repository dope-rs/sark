use std::mem;

use dope_quic::{stream, wire::stream::Id};
use o3::buffer::{hold, resident::Shared};
use sark_core::life;
use send::admit;
use stream::send;

use crate::{conn::io::write, manifold::owners};

pub(super) struct Commit<'a, O, C> {
    pub(super) report: owners::Projection<'a, O, C>,
}

impl<'a, 'q, 'd, C: Copy, O: life::Mode<C, Id>>
    admit::Commit<Shared<'d>, write::Front<'q, 'd>> for Commit<'a, O, C>
{
    type Observation = (write::Completion, hold::Charge);
    type Retained = write::Completion;
    type Discarded = write::Completion;

    fn observe(&mut self, owner: &write::Front<'q, 'd>) -> Self::Observation {
        let charge = if O::ENABLED {
            owner.retained()
        } else {
            Default::default()
        };
        (owner.completion(), charge)
    }

    fn retained(
        self,
        (done, charge): Self::Observation,
        admitted: admit::Admission<'_, Shared<'d>, write::Front<'q, 'd>>,
    ) -> Self::Retained {
        self.report.admitted(charge, admitted.source().id());
        done
    }

    fn discarded(
        self,
        (done, charge): Self::Observation,
        gone: admit::Discard<write::Front<'q, 'd>>,
    ) -> Self::Discarded {
        self.report.discarded(charge, gone.source().id(), gone.cause());
        done
    }
}

const _: () = assert!(
    mem::size_of::<Commit<'static, (), u64>>()
        == mem::size_of::<owners::Projection<'static, (), u64>>()
);
