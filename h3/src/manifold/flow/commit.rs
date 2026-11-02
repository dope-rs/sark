use std::mem;

use dope_quic::{stream, wire::stream::Id};
use o3::buffer::{hold, resident::Shared};
use sark_core::life;
use send::admit;
use stream::send;

use crate::{conn::io::write, manifold::owners};

pub(super) struct Commit<'id, 'a, 'q, 'd, O, C> {
    prepared: write::Prepared<'id, 'q, 'd>,
    report: (&'a mut O, C, hold::Charge),
}

impl<'id, 'a, 'q, 'd, O, C> Commit<'id, 'a, 'q, 'd, O, C> {
    pub(super) const fn new(
        prepared: write::Prepared<'id, 'q, 'd>,
        life: &'a mut O,
        handle: C,
        charge: hold::Charge,
    ) -> Self {
        Self {
            prepared,
            report: (life, handle, charge),
        }
    }

    pub(super) fn restore(self, owner: write::Flight<'id, 'd>) {
        let Self { prepared, .. } = self;
        prepared.restore(owner);
    }
}

impl<'id, 'a, 'q, 'd, C: Copy, O: life::Mode<C, Id>>
    admit::Commit<Shared<'d>, write::Flight<'id, 'd>> for Commit<'id, 'a, 'q, 'd, O, C>
{
    type Retained = write::Completion;
    type Discarded = write::Completion;

    fn retained(
        self,
        admitted: admit::Admission<'_, Shared<'d>, write::Flight<'id, 'd>>,
    ) -> Self::Retained {
        let Self { prepared, report } = self;
        let done = prepared.commit();
        let (life, handle, charge) = report;
        owners::Projection::new(life, handle).admitted(charge, admitted.source().id());
        done
    }

    fn discarded(self, gone: admit::Discard<write::Flight<'id, 'd>>) -> Self::Discarded {
        let Self { prepared, report } = self;
        let done = prepared.commit();
        let (life, handle, charge) = report;
        owners::Projection::new(life, handle).discarded(charge, gone.source().id(), gone.cause());
        done
    }
}

const _: () = assert!(
    mem::size_of::<Commit<'static, 'static, 'static, 'static, (), u64>>()
        == mem::size_of::<(
            write::Prepared<'static, 'static, 'static>,
            (&'static mut (), u64, hold::Charge),
        )>()
);
