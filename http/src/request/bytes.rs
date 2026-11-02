use std::{mem, ops};

use o3::{
    buffer::{
        bytes::scope,
        resident,
        storage::{self, range},
    },
    mem::Rejected,
};

use crate::{
    owner::{handoff, handoff::stage},
    payload::{owned, response},
    request::{Id, Segments},
};

/// Contiguous bytes derived from one exact request.
///
/// Projection cannot extend the request owner's lifetime.
#[repr(C)]
pub struct Bytes<'d> {
    owner: resident::Shared<'d>,
    id: Id<'d>,
}

impl<'d> Bytes<'d> {
    #[doc(hidden)]
    pub fn from_shared(id: Id<'d>, owner: storage::Shared) -> Self {
        Self {
            owner: resident::Shared::from(owner),
            id,
        }
    }

    #[doc(hidden)]
    pub const fn new(id: Id<'d>, owner: resident::Shared<'d>) -> Self {
        Self { owner, id }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.owner.as_slice()
    }

    pub fn len(&self) -> usize {
        self.owner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.owner.is_empty()
    }

    /// Narrows this request owner, retaining its identity on success and rejection.
    pub fn project(self, visible: ops::Range<usize>) -> Result<Self, Rejected<Self, range::Error>> {
        let Self { owner, id } = self;
        match owner.project(visible) {
            Ok(owner) => Ok(Self { owner, id }),
            Err(rejected) => Err(rejected.map(|owner| Self { owner, id })),
        }
    }

    #[doc(hidden)]
    pub const fn id(&self) -> Id<'d> {
        self.id
    }

    #[doc(hidden)]
    pub fn into_body(self) -> Segments<'d> {
        Segments::new(self.id, owned::Segments::from(self.owner))
    }
}

impl<'d> response::Body<'d> for Bytes<'d> {
    fn len(&self) -> usize {
        self.owner.len()
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.owner.as_slice())
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        Ok(handoff::Plan::new(slot.origin().request(self.into_body())))
    }
}

const _: () = assert!(mem::size_of::<Bytes<'static>>() == 5 * mem::size_of::<usize>());

impl AsRef<[u8]> for Bytes<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
