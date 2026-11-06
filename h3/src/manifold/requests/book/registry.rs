//! Canonical generational identity registry for one connection-local book.

use crate::manifold::owners;
use sark_core::manifold::task::app;

use o3::depot::arena::hash::errors;

use crate::manifold::{
    Phase,
    requests::{
        self,
        book::{Book, claim, timers},
        entries,
    },
};

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Book<'d, A, M> {
    pub(in crate::manifold) fn resolve(
        &mut self,
        ident: requests::Identity,
        slot: u32,
        finished: bool,
    ) -> Option<requests::Id> {
        self.entries.resolve(ident, slot, finished)
    }

    pub(in crate::manifold) fn reserved(&self) -> impl Iterator<Item = requests::Identity> + '_ {
        self.entries.reserved()
    }

    pub(in crate::manifold) fn resolve_due(&self, due: timers::Due) -> Option<timers::Resolved> {
        self.entries
            .try_id(due.key())
            .map(|id| timers::Resolved::new(id, due.phase()))
    }

    pub(in crate::manifold) fn request_phase(&self, id: requests::Id) -> Phase {
        self.entries
            .find(id)
            .map_or(Phase::Missing, |found| found.phase())
    }

    pub(in crate::manifold) fn insert_stopped(
        &mut self,
        ident: requests::Identity,
    ) -> Result<requests::Id, errors::Create<entries::Tag>> {
        self.entries.insert(ident, entries::Entry::Stopped)
    }

    pub(in crate::manifold) fn reserve_head(
        &mut self,
        ident: requests::Identity,
    ) -> Result<claim::Claim<'_, 'd, A, M>, errors::Create<entries::Tag>> {
        let reserved = self.entries.reserve(ident)?;
        Ok(claim::Claim::new(reserved, &mut self.segments))
    }
}
