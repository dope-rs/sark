//! Connection-local timers over request generations.

use o3::depot::arena::{hash::runtime, slab::key};
use sark_core::deadline;

use crate::manifold::requests::{self, entries};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Schedule(deadline::Error<key::Handle<runtime::Domain<entries::Tag>>>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Due {
    key: key::Handle<runtime::Domain<entries::Tag>>,
    phase: deadline::Phase,
}

impl Due {
    pub(in crate::manifold) const fn new(
        key: key::Handle<runtime::Domain<entries::Tag>>,
        phase: deadline::Phase,
    ) -> Self {
        Self { key, phase }
    }

    pub(super) const fn key(self) -> key::Handle<runtime::Domain<entries::Tag>> {
        self.key
    }

    pub(super) const fn phase(self) -> deadline::Phase {
        self.phase
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::manifold) struct Resolved {
    id: requests::Id,
    phase: deadline::Phase,
}

impl Resolved {
    pub(super) const fn new(id: requests::Id, phase: deadline::Phase) -> Self {
        Self { id, phase }
    }

    pub(in crate::manifold) const fn id(self) -> requests::Id {
        self.id
    }

    pub(in crate::manifold) const fn phase(self) -> deadline::Phase {
        self.phase
    }
}
