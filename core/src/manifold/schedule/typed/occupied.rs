use std::{cell, marker};

use o3::depot::queue::indexed::lanes::typed;

use crate::manifold::{
    self,
    schedule::{
        Block, LANES, Wait, WorkClass,
        policy::Policy,
        typed::{Next, place},
    },
};

/// An admitted inner payload with its original owner and transition authority.
#[must_use = "an occupied entry retains its exclusive scheduling borrow"]
pub struct Occupied<'a, K: manifold::raw::Key, C, D, I, B: Block, P: typed::Phase<C, D, I>> {
    slot: typed::Slot<'a, K, C, D, I, P, LANES>,
    policy: &'a mut Policy,
    block: marker::PhantomData<cell::Cell<B>>,
}

impl<'a, K, C, D, I, B, P> Occupied<'a, K, C, D, I, B, P>
where
    K: manifold::raw::Key,
    B: Block,
    P: typed::Phase<C, D, I>,
{
    pub(super) fn new(slot: typed::Slot<'a, K, C, D, I, P, LANES>, policy: &'a mut Policy) -> Self {
        Self {
            slot,
            policy,
            block: marker::PhantomData,
        }
    }

    pub fn key(&self) -> K {
        self.slot.key()
    }

    pub fn get(&self) -> &P::Item {
        self.slot.get()
    }

    pub fn get_mut(&mut self) -> &mut P::Item {
        self.slot.get_mut()
    }

    pub fn remove(self) -> P::Item {
        self.slot.remove()
    }

    /// Changes only this payload phase, preserving its exact FIFO position.
    pub fn change<R>(self, run: impl FnOnce(P::Item) -> (P::Item, R)) -> (Self, R) {
        let (vacant, value) = self.slot.take();
        let (value, result) = run(value);
        (Self::new(vacant.restore(value), self.policy), result)
    }

    /// Consumes the original phase and installs its canonical successor.
    /// Returning no successor removes the original owner.
    pub fn transition<R>(self, run: impl FnOnce(P::Item) -> (Option<Next<C, D, I, B>>, R)) -> R {
        let (vacant, value) = self.slot.take();
        let (next, result) = run(value);
        if let Some(next) = next {
            place::insert(vacant, next, self.policy);
        }
        result
    }
}

impl<K: manifold::raw::Key, C, D, I, B: Block> Occupied<'_, K, C, D, I, B, typed::Control> {
    pub fn activate(self, blocked: Option<B>) {
        let lane = WorkClass::Control.blocked_lane(blocked.map(Block::wait));
        if self.slot.lane() != Some(lane) {
            self.slot.move_to_back(lane);
        }
    }
}

impl<K: manifold::raw::Key, C, D, I, B: Block> Occupied<'_, K, C, D, I, B, typed::Data> {
    pub fn activate(self, blocked: Option<B>) {
        let wait = blocked.map(Block::wait);
        let lane = WorkClass::Data.blocked_lane(wait);
        if self.slot.lane() != Some(lane) {
            self.slot.move_to_back(lane);
        }
        if matches!(wait, Some(Wait::Egress)) {
            self.policy.block_data();
        }
    }
}
