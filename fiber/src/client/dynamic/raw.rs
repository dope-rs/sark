use std::num::NonZero;

use dope::{core::driver::route::Identity, manifold::app::dispatch};
use o3::alloc::array;

use crate::client::dynamic::{Connector, Machine, channel};

// SAFETY: this wrapper owns only its pinned Machine and forwards every lifecycle
// operation to it; the exact child retains its stopped and resumed cleanup proof.
unsafe impl<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> dispatch::raw::Resume<'d>
    for Connector<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd, ID>
where
    Machine<'s, 'p, 'd, ID, channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID>>: dispatch::raw::Resume<'d>,
{
}

pub(super) fn bucket<T>(owner: &array::Block<T, NonZero<usize>>, hash: u32) -> &T {
    let mask = owner.extent().get() - 1;
    // SAFETY: this initialized owner retains its nonzero extent, and
    // hash & mask <= mask < extent. The returned borrow retains that owner.
    unsafe { owner.get_unchecked(hash as usize & mask) }
}

/// # Safety
/// The end must be an admitted UTF-8 boundary inside this unchanged authority.
pub(super) unsafe fn prefix(value: &str, end: usize) -> &str {
    // SAFETY: the caller retains this authority's admitted host boundary.
    unsafe { value.get_unchecked(..end) }
}
