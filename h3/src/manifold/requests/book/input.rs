use sark_core::{
    body::{self, credit},
    decode::request,
    duplex,
    manifold::stream::Boundary,
};

use crate::manifold::{
    Phase,
    requests::{
        self,
        book::Book,
        entries::{self, Completion},
    },
};

pub(in crate::manifold) enum Error<E> {
    Request(requests::Error),
    Build(E),
}

pub(in crate::manifold) enum Push<E> {
    Accepted { completion: Completion },
    Queued,
    Rejected(requests::Error),
    Inactive { phase: Phase },
    Failed(Error<E>),
}

impl<'d, P: request::Prepared, B: body::Push<'d>, T, D: duplex::Session<'d>> Book<'d, P, B, T, D> {
    pub(in crate::manifold) fn push<E: credit::Evidence>(
        &mut self,
        id: requests::Id,
        data: crate::Payload<'d>,
        boundary: Boundary,
        proof: E,
    ) -> Push<B::Error> {
        let request = self.limits.request();
        let conn = self.limits.connection();
        let pushed = {
            let Book {
                entries: slots,
                body_credit: account,
                ..
            } = self;
            let feed = match slots.identify(id) {
                Ok(feed) => feed,
                Err(error) => {
                    return Push::Rejected(error);
                }
            };
            account.scope(|mut ledger| {
                // SAFETY: both the selected slot and its exact account remain
                // exclusively borrowed from this same book for the operation.
                unsafe { ledger.bind(feed) }.push(&mut ledger, proof, data, boundary, request, conn)
            })
        };
        match pushed {
            entries::Push::Accepted { completion: done } => Push::Accepted { completion: done },
            entries::Push::Queued => Push::Queued,
            entries::Push::Inactive { phase } => Push::Inactive { phase },
            entries::Push::Request(error) => Push::Failed(Error::Request(error)),
            entries::Push::Build(error) => Push::Failed(Error::Build(error)),
        }
    }
}
