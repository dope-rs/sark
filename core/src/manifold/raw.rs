use core::num::NonZero;

use o3::{depot, depot::arena::slab::key};

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
