use std::{cell, convert::Infallible, marker, ops::ControlFlow};

use o3::{base::cell::region, depot::queue::indexed::lanes::entry};

use crate::manifold::{
    self,
    schedule::{Block, Control, ControlTurn, Decision, Driver, LANES, Terminal, WorkClass, drive},
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

    pub fn remove(self) -> T {
        self.slot.remove()
    }

    /// Changes the resident owner while preserving its exact queue position.
    pub fn change<R>(self, run: impl FnOnce(T) -> (T, R)) -> (Self, R) {
        let Self { slot, scope, .. } = self;
        let (vacant, value) = slot.take();
        let (value, result) = run(value);
        (Self::new(vacant.restore(value), scope), result)
    }

    /// Replaces and schedules the resident owner on Continue, or removes it on
    /// Break. An unchanged control lane preserves its queue position.
    pub fn control<R>(self, run: impl FnOnce(T) -> ControlFlow<R, (T, Option<B>, R)>) -> R {
        match self.try_control::<_, Infallible>(|value| Ok(run(value))) {
            Ok(result) => result,
            Err(never) => match never {},
        }
    }

    /// A rejected transition restores its owner and preserves the original lane
    /// and queue position. Accepted transitions follow `control`.
    pub fn try_control<R, E>(
        self,
        run: impl FnOnce(T) -> Result<ControlFlow<R, (T, Option<B>, R)>, (T, E)>,
    ) -> Result<R, E> {
        let source = self.slot.lane();
        let (vacant, value) = self.slot.take();
        match run(value) {
            Err((value, error)) => {
                vacant.restore(value);
                Err(error)
            }
            Ok(ControlFlow::Break(result)) => Ok(result),
            Ok(ControlFlow::Continue((value, blocked, result))) => {
                let lane = WorkClass::Control.blocked_lane(blocked.map(Block::wait));
                if Some(lane) == source {
                    vacant.restore(value);
                } else {
                    vacant.insert(value).move_to_back(lane);
                }
                Ok(result)
            }
        }
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

    /// Runs a control transition that consumes its resident owner.
    pub fn owned<D>(
        self,
        driver: &mut D,
        run: impl FnOnce(&mut D, K, T) -> Control<T, B, D::Error>,
    ) -> Decision<'id, K>
    where
        D: Driver<T, Key = K, Block = B>,
    {
        Decision::bind(drive::control(driver, self.slot, run), self.scope)
    }
}
