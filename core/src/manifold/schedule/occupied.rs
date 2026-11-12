use std::{cell, marker};

use o3::depot::queue::indexed::lanes::entry;

use crate::manifold::{
    self,
    schedule::{Activation, Block, LANES, Wait, WorkClass, policy::Policy},
};

/// One admitted resident owner.
#[must_use = "an occupied entry retains its exclusive schedule borrow"]
pub struct Occupied<'a, K: manifold::raw::Key, T, B: Block> {
    slot: entry::Occupied<'a, K, T, LANES>,
    policy: &'a mut Policy,
    block: marker::PhantomData<cell::Cell<B>>,
}

impl<'a, K: manifold::raw::Key, T, B: Block> Occupied<'a, K, T, B> {
    pub(super) const fn new(
        slot: entry::Occupied<'a, K, T, LANES>,
        policy: &'a mut Policy,
    ) -> Self {
        Self {
            slot,
            policy,
            block: marker::PhantomData,
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.slot.get_mut()
    }

    pub fn activate(self, next: Activation<B>) {
        let Activation::Activate { blocked, class } = next else {
            return;
        };
        let wait = blocked.map(Block::wait);
        let lane = class.blocked_lane(wait);
        if self.slot.lane() != Some(lane) {
            self.slot.move_to_back(lane);
        }
        if matches!((class, wait), (WorkClass::Data, Some(Wait::Egress))) {
            self.policy.block_data();
        }
    }

    pub fn discard(self) {
        self.slot.discard();
    }
}
