use o3::buffer::{bytes::scope, resident};

use crate::{
    owner::{handoff, handoff::stage},
    payload::{
        owned::{self, Segments},
        response,
    },
};

/// One retained payload owner branded by its driver domain.
pub type Segment<'d> = resident::Shared<'d>;

impl<'d> response::Body<'d> for Segment<'d> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.as_slice())
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
