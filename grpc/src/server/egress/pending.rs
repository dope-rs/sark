use sark_core::manifold::{schedule, stream::Boundary};

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

pub(super) struct Pending {
    pub(super) phase: Phase,
    pub(super) trailer: Option<Trailer>,
}

impl Pending {
    pub(super) fn new(metadata: metadata::Metadata) -> Self {
        Self {
            phase: Phase::Head(metadata),
            trailer: None,
        }
    }

    /// Replaces only output that has not crossed a message-framing boundary.
    pub(super) fn failed(pending: Option<&Self>, trailer: Trailer) -> Option<Self> {
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
#[derive(Default)]
pub(super) struct State {
    pub(super) charge: credits::Charge,
    pub(super) pending: Option<Pending>,
}
