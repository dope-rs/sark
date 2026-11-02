use std::convert::Infallible;

use o3::buffer::bytes::scope;

use crate::{
    owner::{handoff, handoff::stage},
    request::{Id, body},
};

/// Immutable request sources available while selecting a response body.
#[doc(hidden)]
pub struct Slot<'req, 'd, H = (), B = body::Retained<'req, 'd>> {
    pub(in crate::owner::handoff) origin: handoff::Origin<'req, 'd, B>,
    pub(in crate::owner::handoff) head: H,
}

impl<'req, 'd> Slot<'req, 'd> {
    pub const fn new(body: body::Ref<'req, 'd>) -> Self {
        Self {
            origin: handoff::Origin::new(body),
            head: (),
        }
    }

    pub const fn empty() -> Self {
        Self {
            origin: handoff::Origin::empty(),
            head: (),
        }
    }

    pub const fn task(id: Id<'d>) -> Self {
        Self {
            origin: handoff::Origin::task(id),
            head: (),
        }
    }
}

impl<'req, 'd, B> Slot<'req, 'd, (), B> {
    pub fn with_head<H: scope::Source>(self, head: H) -> Slot<'req, 'd, H, B> {
        Slot {
            origin: self.origin,
            head,
        }
    }
}

impl<'req, 'd, H, B: stage::Body<'req, 'd>> Slot<'req, 'd, H, B> {
    pub(crate) const fn origin(&self) -> handoff::Origin<'req, 'd, B> {
        self.origin
    }
}

impl<'req, 'd> Slot<'req, 'd, (), Infallible> {
    /// Selects an output with no borrowed request-body owner to defer.
    pub const fn ready(id: Option<Id<'d>>) -> Self {
        Self {
            origin: match id {
                Some(id) => handoff::Origin::task(id),
                None => handoff::Origin::empty(),
            },
            head: (),
        }
    }
}
