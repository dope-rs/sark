use o3::buffer::bytes::scope;

use crate::{
    owner::{handoff, handoff::stage},
    payload::{owned, response},
};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Body<'a> {
    bytes: &'a [u8],
}

impl<'a> Body<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub const fn as_slice(&self) -> &'a [u8] {
        self.bytes
    }

    pub const fn len(self) -> usize {
        self.bytes.len()
    }

    pub const fn is_empty(self) -> bool {
        self.bytes.is_empty()
    }
}

impl<'body_d, 'd> response::Body<'d> for Body<'body_d>
where
    'body_d: 'd,
{
    const INLINE: bool = true;

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.bytes)
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        Ok(handoff::Plan::new(slot.origin().fixed(self)))
    }
}

impl AsRef<[u8]> for Body<'_> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}
