use std::{cell, marker};

use o3::depot::queue::indexed::lanes::entry;

use crate::manifold::{
    self,
    schedule::{Block, LANES, WorkClass},
};

#[must_use]
pub struct Vacant<'a, K: manifold::raw::Key, T, B: Block> {
    slot: entry::Vacancy<'a, K, T, LANES>,
    block: marker::PhantomData<cell::Cell<B>>,
}

impl<'a, K: manifold::raw::Key, T, B: Block> Vacant<'a, K, T, B> {
    pub(super) const fn new(slot: entry::Vacancy<'a, K, T, LANES>) -> Self {
        Self {
            slot,
            block: marker::PhantomData,
        }
    }

    /// Stores an admitted owner with its initial runnable class.
    pub fn store(self, value: T, ready: Option<WorkClass>) {
        let slot = self.slot.insert(value);
        if let Some(class) = ready {
            slot.move_to_back(class.ready_lane());
        }
    }

    pub fn insert(self, value: T, blocked: Option<B>) {
        self.slot
            .insert(value)
            .move_to_back(WorkClass::Control.blocked_lane(blocked.map(Block::wait)));
    }
}
