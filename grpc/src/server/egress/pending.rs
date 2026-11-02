use sark_core::manifold::{self, schedule, stream::Boundary};

use crate::{
    metadata,
    protocol::trailer::{self, lengths::Lengths},
    server::call::credits,
};

pub(super) enum Phase {
    Head(metadata::Metadata),
    Body { offset: usize },
    Prepared(Boundary),
}

pub(super) struct Trailer {
    pub(super) status: crate::Status,
    pub(super) metadata: metadata::Metadata,
    pub(super) lengths: Lengths,
}

impl Trailer {
    pub(super) fn new(
        status: crate::Status,
        metadata: metadata::Metadata,
    ) -> Result<Self, crate::Status> {
        let lengths = trailer::Fields::new(&status, &metadata)?.lengths();
        Ok(Self {
            status,
            metadata,
            lengths,
        })
    }
}

pub(super) struct Pending<K: manifold::raw::Key> {
    pub(super) key: K,
    pub(super) phase: Phase,
    pub(super) trailer: Option<Trailer>,
}

impl<K: manifold::raw::Key> Pending<K> {
    pub(super) fn new(key: K, metadata: metadata::Metadata) -> Self {
        Self {
            key,
            phase: Phase::Head(metadata),
            trailer: None,
        }
    }

    /// Replaces only output that has not crossed a message-framing boundary.
    pub(super) fn failed(key: K, pending: Option<&Self>, trailer: Trailer) -> Option<Self> {
        let phase = match pending.map(|pending| &pending.phase) {
            None | Some(Phase::Head(_) | Phase::Prepared(Boundary::More)) => {
                Phase::Head(metadata::Metadata::new())
            }
            Some(Phase::Body { offset: 0 } | Phase::Prepared(Boundary::End)) => {
                Phase::Body { offset: 0 }
            }
            Some(Phase::Body { .. }) => return None,
        };
        Some(Self {
            key,
            phase,
            trailer: Some(trailer),
        })
    }

    pub(super) fn work(&self, ready: bool) -> Option<schedule::WorkClass> {
        match self.phase {
            Phase::Head(_) | Phase::Prepared(_) => Some(schedule::WorkClass::Control),
            Phase::Body { .. } => self.body(ready),
        }
    }

    pub(super) fn body(&self, ready: bool) -> Option<schedule::WorkClass> {
        match (ready, self.trailer.is_some()) {
            (true, _) => Some(schedule::WorkClass::Data),
            (false, true) => Some(schedule::WorkClass::Control),
            (false, false) => None,
        }
    }
}

/// A lane's credit account exists independently of its response metadata.
pub(super) struct State<K: manifold::raw::Key> {
    pub(super) charge: credits::Charge,
    pub(super) pending: Option<Pending<K>>,
}

impl<K: manifold::raw::Key> Default for State<K> {
    fn default() -> Self {
        Self {
            charge: credits::Charge::default(),
            pending: None,
        }
    }
}
