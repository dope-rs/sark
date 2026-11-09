use core::num::NonZero;

use dope_net::link::egress::data::{self, compose, exec};
use o3::{
    buffer,
    buffer::{Residency as _, hold, resident, segments, segments::scatter},
    depot,
    depot::arena::slab::key,
};

use super::Body;

/// Certifies complete task identity across storage and cancellation.
/// # Safety
/// The target must belong to exactly one protocol identity domain.
pub unsafe trait Target {}

/// One-word generation-bearing identity for a connection-local slot.
/// # Safety
/// `identity` is nonzero and its low word is exactly `slot`.
pub unsafe trait Key: depot::raw::Key<Raw = u64> {
    fn identity(self) -> NonZero<u64>;

    fn slot(self) -> usize {
        depot::raw::Key::index(self)
    }
}

// SAFETY: O3 slab handles store a nonzero generation above their u32 slot.
unsafe impl<Tag, const MAX: u32> Key for key::Handle<Tag, MAX> {
    fn identity(self) -> NonZero<u64> {
        self.parts().into_raw()
    }
}

// SAFETY: Every variant retains its original immutable owner and derives cursor length and charge from that owner.
unsafe impl<'d, K> data::raw::Payload<'d> for Body<'d, K> {
    type Mode = exec::Fallible;
    type Cursor<'a>
        = compose::Either<
        &'a [u8],
        <segments::Chain<resident::Shared<'d>> as buffer::raw::Chunks>::Cursor<'a>,
    >
    where
        Self: 'a;

    fn len(&self) -> usize {
        match self {
            Self::Shared { owner, .. } => data::raw::Payload::len(owner),
            Self::Segments { owners, .. } => data::raw::Payload::len(owners.as_chain()),
            Self::Frozen { owner, .. } => owner.len(),
        }
    }

    #[inline(always)]
    fn charge(&self) -> hold::Charge {
        match self {
            Self::Shared { owner, .. } => data::raw::Payload::charge(owner),
            Self::Segments { owners, .. } => data::raw::Payload::charge(owners.as_chain()),
            Self::Frozen { owner, .. } => hold::Charge::from_owner(owner.len(), owner.resident()),
        }
    }

    fn start(&self) -> Self::Cursor<'_> {
        match self {
            Self::Shared { owner, .. } => compose::Either::Left(owner.as_ref()),
            Self::Segments { owners, .. } => {
                compose::Either::Right(buffer::raw::Chunks::chunks(owners.as_chain()))
            }
            Self::Frozen { owner, .. } => compose::Either::Left(owner.as_ref()),
        }
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        match self {
            Self::Shared { owner, .. } => {
                data::raw::Payload::cursor(owner, offset).map(compose::Either::Left)
            }
            Self::Segments { owners, .. } => {
                data::raw::Payload::cursor(owners.as_chain(), offset).map(compose::Either::Right)
            }
            Self::Frozen { owner, .. } => offset.tail(owner.as_ref()).map(compose::Either::Left),
        }
    }

    unsafe fn resume(
        &self,
        position: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: this immutable variant retains the original owner and topology.
        match self {
            Self::Shared { owner, .. } => compose::Either::Left(unsafe {
                data::raw::Payload::resume(owner, position, offset)
            }),
            Self::Segments { owners, .. } => compose::Either::Right(unsafe {
                data::raw::Payload::resume(owners.as_chain(), position, offset)
            }),
            Self::Frozen { owner, .. } => {
                compose::Either::Left(unsafe { owner.as_ref().get_unchecked(offset.get()..) })
            }
        }
    }
}
