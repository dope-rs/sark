use std::{iter::FusedIterator, ops::ControlFlow};

use o3::buffer::{bytes::scope, storage, write::Slice};

use crate::{
    owner::{handoff, handoff::stage},
    payload::owned,
};

mod sealed;
pub(crate) use sealed::Sealed;

/// Statically dispatched response payload.
pub trait Body<'d>: Sized + Sealed {
    #[doc(hidden)]
    const INLINE: bool = false;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn as_slice(&self) -> Option<&[u8]>;

    fn segments(&self) -> impl ExactSizeIterator<Item = &[u8]> + FusedIterator {
        self.as_slice().filter(|body| !body.is_empty()).into_iter()
    }

    #[doc(hidden)]
    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error>;

    #[doc(hidden)]
    fn try_resolve_into<
        'req,
        H: scope::Source,
        O: stage::Body<'req, 'd>,
        L: std::borrow::BorrowMut<usize>,
    >(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
        _: &mut Slice<'_, L>,
    ) -> Result<ControlFlow<usize, handoff::Plan<'req, 'd, H::Selection, O>>, owned::Error> {
        self.try_resolve(slot).map(ControlFlow::Continue)
    }
}

impl<'d> Body<'d> for [u8; 0] {
    const INLINE: bool = true;

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self)
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        match self {
            [] => Ok(handoff::Plan::new(slot.origin().clear())),
        }
    }
}

impl<'d> Body<'d> for &[u8] {
    const INLINE: bool = true;

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self)
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        handoff::Plan::try_borrow(slot, self)
    }

    fn try_resolve_into<
        'req,
        H: scope::Source,
        O: stage::Body<'req, 'd>,
        L: std::borrow::BorrowMut<usize>,
    >(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
        out: &mut Slice<'_, L>,
    ) -> Result<ControlFlow<usize, handoff::Plan<'req, 'd, H::Selection, O>>, owned::Error> {
        handoff::Plan::try_borrow_into(slot, self, out)
    }
}

impl<'d> Body<'d> for &str {
    const INLINE: bool = true;

    fn len(&self) -> usize {
        str::len(self)
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        handoff::Plan::try_borrow(slot, self.as_bytes())
    }

    fn try_resolve_into<
        'req,
        H: scope::Source,
        O: stage::Body<'req, 'd>,
        L: std::borrow::BorrowMut<usize>,
    >(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
        out: &mut Slice<'_, L>,
    ) -> Result<ControlFlow<usize, handoff::Plan<'req, 'd, H::Selection, O>>, owned::Error> {
        handoff::Plan::try_borrow_into(slot, self.as_bytes(), out)
    }
}

impl<'d> Body<'d> for Vec<u8> {
    const INLINE: bool = true;

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
        Ok(handoff::Plan::new(slot.origin().owned(self)))
    }
}

impl<'d> Body<'d> for String {
    const INLINE: bool = true;

    fn len(&self) -> usize {
        String::len(self)
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        Ok(handoff::Plan::new(slot.origin().owned(self.into_bytes())))
    }
}

impl<'d> Body<'d> for storage::Owned {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.as_ref())
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        Ok(handoff::Plan::new(slot.origin().shared(self.freeze())))
    }
}

impl<'d> Body<'d> for storage::Shared {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.as_ref())
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        Ok(handoff::Plan::new(slot.origin().shared(self)))
    }
}
