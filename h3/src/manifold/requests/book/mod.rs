use crate::manifold::owners;
use o3::{
    alloc,
    base::cell::region,
    buffer::{resident, segments},
};
use sark_core::manifold::task::app;
use sark_core::{body::credit, life::audit};

use crate::manifold::{
    encoder,
    requests::{self, entries, exchange, task},
};

mod build;
pub(in crate::manifold) mod claim;
pub(in crate::manifold) mod input;
mod ready;
mod registry;
pub(in crate::manifold) mod settings;
pub(in crate::manifold) mod timers;

pub(in crate::manifold) struct Book<'d, A: app::Runtime<'d>, M: owners::Handoff> {
    entries: entries::Entries<'d, A, M>,
    body_credit: credit::raw::Account,
    limits: settings::Limits,
    segments: segments::Pool<resident::Shared<'d>>,
}

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Book<'d, A, M> {
    pub(in crate::manifold) fn try_new(plan: settings::Plan) -> Result<Self, alloc::Error> {
        let (slots, cap, limits) = plan.into_parts();
        let cap = cap.get();
        Ok(Self {
            entries: entries::Entries::try_new(slots)?,
            body_credit: credit::raw::Account::new(),
            limits,
            segments: segments::Pool::try_new(segments::Plan {
                segments: cap,
                chains: cap / 2,
            })?,
        })
    }

    pub(in crate::manifold) fn phase(
        &self,
        owner: requests::Identity,
        index: u32,
    ) -> crate::manifold::Phase {
        self.entries.phase(owner, index)
    }

    pub(in crate::manifold) fn head(
        &mut self,
        id: requests::Id,
    ) -> Option<claim::Head<claim::Claim<'_, 'd, A, M>>> {
        match self.entries.entry(id).ok()?.reserve() {
            Ok(reserved) => Some(claim::Head::Reserved(claim::Claim::new(
                reserved,
                &mut self.segments,
            ))),
            Err(crate::manifold::Phase::Stopped) => Some(claim::Head::Stopped),
            Err(_) => None,
        }
    }

    /// Exact configured bounds shared by the head and chunk admission paths.
    pub(in crate::manifold) const fn limits(&self) -> settings::Limits {
        self.limits
    }

    pub(in crate::manifold) fn resume(&mut self) {
        self.entries.resume();
    }

    pub(in crate::manifold) fn has_ready(&self) -> bool {
        !self.entries.is_empty::<false>()
    }

    pub(in crate::manifold) fn has_blocked(&self) -> bool {
        !self.entries.is_empty::<true>()
    }

    pub(in crate::manifold) fn reset<E: credit::Evidence>(
        &mut self,
        id: requests::Id,
        proof: E,
    ) -> Result<exchange::Reset, requests::Error> {
        let live = self.entries.identify(id)?;
        self.body_credit.scope(|mut ledger| {
            // SAFETY: live is selected from this book, which exclusively retains
            // every charged state beside this exact issuing account.
            unsafe { ledger.bind(live) }.reset(&mut ledger, proof)
        })
    }

    pub(in crate::manifold) fn try_finish<E: credit::Evidence, F>(
        &mut self,
        id: requests::Id,
        proof: impl FnOnce(crate::manifold::Phase) -> E,
        response: impl FnOnce(task::Buffered<'d, A::Prepared>) -> Result<encoder::Pending<'d, M>, F>,
    ) -> Result<exchange::Termination<F>, exchange::Error> {
        let live = self
            .entries
            .identify(id)
            .map_err(exchange::Error::Request)?;
        let proof = proof(live.phase());
        self.body_credit.scope(|mut ledger| {
            // SAFETY: the selected live slot and account belong to this book.
            unsafe { ledger.bind(live) }.try_finish(&mut ledger, proof, response)
        })
    }

    pub(in crate::manifold) fn stop<E: credit::Evidence>(
        &mut self,
        id: requests::Id,
        proof: E,
    ) -> Result<exchange::Stop<task::Owned<'d, A::Task, M>>, requests::Error> {
        let live = self.entries.identify(id)?;
        self.body_credit.scope(|mut ledger| {
            // SAFETY: the selected live slot and account belong to this book.
            unsafe { ledger.bind(live) }.stop(&mut ledger, proof)
        })
    }

    /// Consumes every entry and its credit and queue roots before certifying closure.
    pub(in crate::manifold) fn drain_owned(
        self,
        mut drain: impl for<'id, 'account> FnMut(
            requests::Id,
            exchange::Drained<task::Owned<'d, A::Task, M>, region::Proof<region::Identity<'id>>>,
            &mut credit::raw::Ledger<'account, region::Proof<region::Identity<'id>>>,
        ),
    ) -> audit::Snapshot {
        let Self {
            entries,
            body_credit: mut credit,
            ..
        } = self;
        let residual = credit.scope(|mut ledger| {
            entries.drain(|id, entry| {
                let drained = match entry.drain() {
                    Some(exchange::Drained::Task(task)) => exchange::Drained::Task(task),
                    Some(exchange::Drained::Exchange { pending }) => exchange::Drained::Exchange {
                        // SAFETY: each release is consumed from this book's own
                        // entry while its sole issuing account remains loaned.
                        pending: pending.map(|release| unsafe { ledger.bind(release) }),
                    },
                    None => return,
                };
                drain(id, drained, &mut ledger);
            })
        });
        let held = credit.retained();
        audit::Snapshot::default().request(
            residual.entries,
            held.bytes(),
            held.chunks().into_raw() as usize,
            residual.ready,
        )
    }
}
