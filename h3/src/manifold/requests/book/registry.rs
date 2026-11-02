//! Canonical generational identity registry for one connection-local book.

use o3::depot::arena::hash::errors;
use sark_core::{body, decode::request, duplex};

use crate::manifold::{
    Phase,
    requests::{
        self,
        book::{Book, claim, timers},
        entries,
    },
};

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>> Book<'d, P, B, T, D> {
    pub(in crate::manifold) fn resolve(
        &mut self,
        ident: requests::Identity,
        slot: u32,
        finished: bool,
    ) -> Option<requests::Id> {
        self.entries.resolve(ident, slot, finished)
    }

    pub(in crate::manifold) fn resume_head(
        &mut self,
        id: requests::Id,
    ) -> Option<claim::Claim<'_, 'd, P, B, T, D>> {
        match self.head(id) {
            Some(claim::Head::Reserved(claim)) => Some(claim),
            Some(claim::Head::Stopped) | None => None,
        }
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
    ) -> Result<claim::Claim<'_, 'd, P, B, T, D>, errors::Create<entries::Tag>> {
        let reserved = self.entries.reserve(ident)?;
        Ok(claim::Claim::new(reserved, &mut self.segments))
    }
}
