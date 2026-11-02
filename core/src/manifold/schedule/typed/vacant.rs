use std::{cell, marker};

use o3::depot::queue::indexed::lanes::typed;
use typed::Value;

use crate::manifold::{
    self,
    schedule::{
        Block, LANES,
        policy::Policy,
        typed::{Next, place},
    },
};

/// One admitted vacancy and the scheduling policy of its original domain.
#[must_use]
pub struct Vacant<'a, K: manifold::raw::Key, C, D, I, B: Block> {
    slot: typed::Vacant<'a, K, C, D, I, LANES>,
    policy: &'a mut Policy,
    block: marker::PhantomData<cell::Cell<B>>,
}

impl<'a, K: manifold::raw::Key, C, D, I, B: Block> Vacant<'a, K, C, D, I, B> {
    pub(super) fn new(slot: typed::Vacant<'a, K, C, D, I, LANES>, policy: &'a mut Policy) -> Self {
        Self {
            slot,
            policy,
            block: marker::PhantomData,
        }
    }

    pub fn insert(self, next: Next<C, D, I, B>) {
        place::insert(self.slot, next, self.policy);
    }

    /// Stores an owner that waits for external input before becoming runnable.
    pub fn store(self, value: Value<C, D, I>) {
        place::store(self.slot, value);
    }
}
