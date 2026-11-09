use crate::manifold::owners;
use sark_core::manifold::task::app;
use std::num::NonZero;

use sark_core::body::{self, credit};

use crate::manifold::requests::{self, book::Book, entries};

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Book<'d, A, M> {
    pub(in crate::manifold) fn drive<E, F>(
        &mut self,
        budget: NonZero<usize>,
        proof: F,
    ) -> Result<entries::Built<<A::Build as body::Build<'d>>::StepError>, requests::Error>
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
