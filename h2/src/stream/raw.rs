use core::num::NonZero;

use o3::{base::num::bounded, depot, depot::index::Stored};
use sark_core::{manifold, manifold::stream::Boundary};

use crate::stream::{Id, Key, Nonzero};

/// `raw` must retain a previously emitted stream Key identity.
const unsafe fn restore(raw: u64) -> Key {
    Key {
        id: Nonzero(Id::from_wire((raw >> 32) as u32)),
        // SAFETY: The retained Key's physical slot excludes the sentinel.
        slot: unsafe { bounded::raw::u32(raw as u32) },
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Marked(NonZero<u64>);

impl Marked {
    const END: u64 = 1 << 63;

    pub(crate) fn new(key: Key, boundary: Boundary) -> Self {
        let marker = match boundary {
            Boundary::More => 0,
            Boundary::End => Self::END,
        };
        Self(manifold::raw::Key::identity(key) | marker)
    }

    pub(crate) const fn key(self) -> Key {
        // SAFETY: Marked retains the original Key below its separate boundary bit.
        unsafe { restore(self.0.get() & !Self::END) }
    }

    pub(crate) const fn boundary(self) -> Boundary {
        match self.0.get() & Self::END {
            0 => Boundary::More,
            _ => Boundary::End,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Marked>() == core::mem::size_of::<Key>());
const _: () = assert!(core::mem::size_of::<Option<Marked>>() == core::mem::size_of::<Marked>());

unsafe impl depot::raw::Key for Key {
    type Raw = u64;

    const VACANT: Self::Raw = 0;

    fn index(self) -> usize {
        self.slot.into_usize()
    }

    fn into_raw(self) -> Self::Raw {
        manifold::raw::Key::identity(self).get()
    }

    fn from_raw(raw: Stored<Self>) -> Self {
        // SAFETY: Stored<Self> retains this exact stream domain and logical identity.
        unsafe { restore(raw.into_raw()) }
    }
}

// SAFETY: Key retains its nonzero wire stream id and bounded physical slot.
unsafe impl manifold::raw::Key for Key {
    fn identity(self) -> NonZero<u64> {
        unsafe { NonZero::<u64>::new_unchecked(self.identity()) }
    }
}
