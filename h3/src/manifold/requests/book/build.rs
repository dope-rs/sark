use std::num::NonZero;

use sark_core::{
    body::{self, credit},
    decode::request,
    duplex,
};

use crate::manifold::requests::{self, book::Book, entries};

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>> Book<'d, P, B, T, D> {
    pub(in crate::manifold) fn drive<E, F>(
        &mut self,
        budget: NonZero<usize>,
        proof: F,
    ) -> Result<entries::Built<B::StepError>, requests::Error>
    where
        E: credit::Evidence,
        F: FnOnce(requests::Id) -> E,
    {
        use body::BuildOutcome;

        let Some(live) = self.entries.front() else {
            return Ok((BuildOutcome::Idle, None));
        };
        self.body_credit
            .scope(|mut ledger| {
                // SAFETY: the front slot was selected from this book's own FIFO;
                // this book exclusively retains its exact issuing account.
                unsafe { ledger.bind(live) }.drive(&mut ledger, proof, budget)
            })
            .map_err(|_| requests::Error::Build)
    }
}
