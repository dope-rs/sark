use std::ops;

use o3::buffer::{bytes::scope, resident, segments, storage::backing};

use crate::{
    owner::{handoff, handoff::stage},
    payload::{
        owned::{self, Segments},
        response,
    },
};

/// One retained payload owner branded by its driver domain.
#[repr(transparent)]
#[derive(Debug, Default)]
pub struct Segment<'d> {
    owner: resident::Shared<'d>,
}

impl<'d> Segment<'d> {
    pub const fn new() -> Self {
        Self {
            owner: resident::Shared::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.owner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.owner.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.owner.as_slice()
    }

    pub(crate) fn from_item(item: segments::Item<resident::Shared<'d>>) -> Self {
        Self {
            owner: item.into_inner(),
        }
    }

    #[doc(hidden)]
    pub fn backing(&self) -> Option<backing::View<'_, 'd>> {
        self.owner.backing()
    }
}

impl<'d> response::Body<'d> for Segment<'d> {
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
        Ok(handoff::Plan::new(
            slot.origin().segmented(Segments::from(self)),
        ))
    }
}

impl<'d> From<resident::Shared<'d>> for Segment<'d> {
    fn from(owner: resident::Shared<'d>) -> Self {
        Self { owner }
    }
}

impl<'d> From<Segment<'d>> for resident::Shared<'d> {
    fn from(part: Segment<'d>) -> Self {
        part.owner
    }
}

impl AsRef<[u8]> for Segment<'_> {
    fn as_ref(&self) -> &[u8] {
        self.owner.as_ref()
    }
}

impl ops::Deref for Segment<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.owner.as_ref()
    }
}
