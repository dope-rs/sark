use std::{cell, marker};

use o3::{base::cell::region, depot::queue::indexed::lanes::entry};

use crate::manifold::{
    self,
    schedule::{Block, ControlTurn, Decision, Driver, LANES, Terminal, WorkClass, drive},
};

/// One admitted resident owner and its control scheduling authority.
#[must_use = "an occupied entry retains its exclusive schedule borrow"]
pub struct Occupied<'a, K: manifold::raw::Key, T, B: Block, Q = ()> {
    slot: entry::Occupied<'a, K, T, LANES>,
    block: marker::PhantomData<cell::Cell<B>>,
    scope: Q,
}

impl<'a, K: manifold::raw::Key, T, B: Block, Q> Occupied<'a, K, T, B, Q> {
    pub(super) const fn new(slot: entry::Occupied<'a, K, T, LANES>, scope: Q) -> Self {
        Self {
            slot,
            block: marker::PhantomData,
            scope,
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.slot.get_mut()
    }

    pub fn discard(self) {
        self.slot.discard();
    }
}

impl<'a, 'id, K: manifold::raw::Key, T, B: Block> Occupied<'a, K, T, B, region::Identity<'id>> {
    /// Runs a control quantum without withdrawing its resident owner.
    pub fn borrowed<D>(
        mut self,
        driver: &mut D,
        run: impl FnOnce(&mut D, K, &mut T) -> Result<ControlTurn<B>, D::Error>,
    ) -> Decision<'id, K>
    where
        D: Driver<T, Key = K, Block = B>,
    {
        let stream = self.slot.key();
        let (step, activity) = match run(driver, stream, self.slot.get_mut()) {
            Ok(turn) => turn.into_parts(),
            Err(error) => {
                return Decision::bind(
                    drive::finish(
                        driver,
                        self.slot,
                        stream,
                        Terminal::Failed(error),
                        WorkClass::Control,
                    ),
                    self.scope,
                );
            }
        };
        Decision::bind(
            drive::borrowed(driver, self.slot, stream, step, activity),
            self.scope,
        )
    }
}
