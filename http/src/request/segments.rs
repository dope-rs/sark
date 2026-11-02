use std::{iter::FusedIterator, ops};

use o3::buffer::bytes::scope;

use crate::{
    owner::{handoff, handoff::stage},
    payload::{owned, response},
    request::Id,
};

/// Segmented bytes derived from one exact request.
#[derive(Debug)]
pub struct Segments<'d> {
    body: owned::Segments<'d>,
    id: Id<'d>,
}

impl<'d> Segments<'d> {
    #[doc(hidden)]
    pub const fn new(id: Id<'d>, body: owned::Segments<'d>) -> Self {
        Self { body, id }
    }

    #[doc(hidden)]
    pub const fn id(&self) -> Id<'d> {
        self.id
    }

    pub(crate) fn into_parts(self) -> (Id<'d>, owned::Segments<'d>) {
        (self.id, self.body)
    }

    pub fn into_inner(self) -> owned::Segments<'d> {
        self.body
    }

    pub fn try_clone(&self) -> Result<Self, owned::Error> {
        Ok(Self {
            body: self.body.try_clone()?,
            id: self.id,
        })
    }
}

impl<'d> response::Body<'d> for Segments<'d> {
    fn len(&self) -> usize {
        self.body.len()
    }

    fn as_slice(&self) -> Option<&[u8]> {
        self.body.as_slice()
    }

    fn segments(&self) -> impl ExactSizeIterator<Item = &[u8]> + FusedIterator {
        self.body.segments()
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        Ok(handoff::Plan::new(slot.origin().request(self)))
    }
}

impl<'d> ops::Deref for Segments<'d> {
    type Target = owned::Segments<'d>;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}
