use std::mem;

use dope_quic::{stream, wire::stream::Id};
use o3::buffer::{hold, resident::Shared};
use sark_core::life;
use send::admit;
use stream::send;

use crate::{conn::io::write, manifold::owners};

pub(super) struct Commit<'id, 'a, 'q, 'd, O, C> {
    pub(super) prepared: write::Prepared<'id, 'q, 'd>,
    pub(super) report: owners::Projection<'a, O, C>,
}

impl<'id, 'a, 'q, 'd, C: Copy, O: life::Mode<C, Id>>
    admit::Commit<Shared<'d>, write::Flight<'id, 'd>> for Commit<'id, 'a, 'q, 'd, O, C>
{
    type Observation = hold::Charge;
    type Retained = write::Completion;
    type Discarded = write::Completion;

    fn observe(&mut self, owner: &write::Flight<'id, 'd>) -> Self::Observation {
        if O::ENABLED {
            owner.retained()
        } else {
            Default::default()
        }
    }

    fn retained(
        self,
        charge: Self::Observation,
        admitted: admit::Admission<'_, Shared<'d>, write::Flight<'id, 'd>>,
    ) -> Self::Retained {
        let Self { prepared, report } = self;
        let done = prepared.commit();
        report.admitted(charge, admitted.source().id());
        done
    }

    fn discarded(
        self,
        charge: Self::Observation,
        gone: admit::Discard<write::Flight<'id, 'd>>,
    ) -> Self::Discarded {
        let Self { prepared, report } = self;
        let done = prepared.commit();
        report.discarded(charge, gone.source().id(), gone.cause());
        done
    }
}

const _: () = assert!(
    mem::size_of::<Commit<'static, 'static, 'static, 'static, (), u64>>()
        == mem::size_of::<(
            write::Prepared<'static, 'static, 'static>,
            owners::Projection<'static, (), u64>,
        )>()
);
