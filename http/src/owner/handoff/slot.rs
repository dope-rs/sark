use std::{convert::Infallible, marker::PhantomData};

use o3::buffer::{bytes::scope, storage};

use crate::{
    owner::handoff::{Origin, Plan, Ready, stage},
    payload::{fixed, owned},
    request::{self, Id, body},
};

/// Immutable request sources available while selecting a response body.
#[doc(hidden)]
pub struct Slot<'req, 'd, H = (), B = body::Retained<'req, 'd>> {
    pub(in crate::owner::handoff) origin: Origin<'req, 'd, B>,
    pub(in crate::owner::handoff) head: H,
}

impl<'req, 'd> Slot<'req, 'd> {
    pub const fn new(body: body::Ref<'req, 'd>) -> Self {
        Self {
            origin: match body.try_retained() {
                Ok(body) => Origin::Retained(body),
                Err(_) => Origin::Empty(PhantomData),
            },
            head: (),
        }
    }

    pub const fn empty() -> Self {
        Self {
            origin: Origin::Empty(PhantomData),
            head: (),
        }
    }

    pub const fn task(id: Id<'d>) -> Self {
        Self {
            origin: Origin::Task(id),
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

impl<'req, 'd, H: scope::Source, B: stage::Body<'req, 'd>> Slot<'req, 'd, H, B> {
    pub(crate) const fn clear(&self) -> Plan<'req, 'd, H::Selection, B> {
        Plan::ready(self.origin, Ready::Empty)
    }

    pub(crate) fn owned(&self, body: Vec<u8>) -> Plan<'req, 'd, H::Selection, B> {
        if body.is_empty() {
            self.clear()
        } else {
            Plan::ready(self.origin, Ready::Owned(body))
        }
    }

    pub(crate) fn shared(&self, body: storage::Shared) -> Plan<'req, 'd, H::Selection, B> {
        if body.is_empty() {
            self.clear()
        } else {
            Plan::ready(self.origin, Ready::Retained(body))
        }
    }

    pub(crate) fn segmented(&self, body: owned::Segments<'d>) -> Plan<'req, 'd, H::Selection, B> {
        if body.is_empty() {
            self.clear()
        } else {
            Plan::ready(self.origin, Ready::Segmented(body))
        }
    }

    pub(crate) fn fixed(&self, body: fixed::Body<'d>) -> Plan<'req, 'd, H::Selection, B> {
        if body.is_empty() {
            self.clear()
        } else {
            Plan::ready(self.origin, Ready::Fixed(body))
        }
    }

    pub(crate) fn request(&self, body: request::Segments<'d>) -> Plan<'req, 'd, H::Selection, B> {
        if body.is_empty() {
            return self.clear();
        }
        let (id, body) = body.into_parts();
        let body = match self.origin {
            Origin::Task(expected) if expected == id => Ready::Linked(body),
            _ => Ready::Segmented(body),
        };
        Plan::ready(self.origin, body)
    }
}

impl<'req, 'd> Slot<'req, 'd, (), Infallible> {
    /// Selects an output with no borrowed request-body owner to defer.
    pub const fn ready(id: Option<Id<'d>>) -> Self {
        Self {
            origin: match id {
                Some(id) => Origin::Task(id),
                None => Origin::Empty(PhantomData),
            },
            head: (),
        }
    }
}
