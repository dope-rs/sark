use std::{cell, marker};

use lanes::entry;
use o3::depot::{map::direct, queue::indexed::lanes};

use crate::manifold::{
    self,
    schedule::{Block, LANES, WorkClass},
};

#[must_use]
pub struct Vacant<'a, K: manifold::raw::Key, T, B: Block> {
    slot: entry::Slot<'a, K, T, LANES, direct::Vacant<'a, K, lanes::Node<T, LANES>>>,
    block: marker::PhantomData<cell::Cell<B>>,
}

impl<'a, K: manifold::raw::Key, T, B: Block> Vacant<'a, K, T, B> {
    pub(super) const fn new(
        slot: entry::Slot<'a, K, T, LANES, direct::Vacant<'a, K, lanes::Node<T, LANES>>>,
    ) -> Self {
        Self {
            slot,
            block: marker::PhantomData,
        }
    }

    /// Stores an owner that waits for an external input before becoming runnable.
    pub fn store(self, value: T) {
        self.slot.insert(value);
    }

    pub fn insert(self, value: T, blocked: Option<B>) {
        self.slot
            .insert(value)
            .move_to_back(WorkClass::Control.blocked_lane(blocked.map(Block::wait)));
    }
}
