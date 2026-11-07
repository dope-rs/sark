use std::{convert::Infallible, num::NonZero};

use o3::{base::cell::region, depot::arena::slab::key};
use sark_core::manifold::schedule;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Block {}

impl schedule::Block for Block {
    fn wait(self) -> schedule::Wait {
        match self {}
    }
}

pub struct Driver {
    pub remaining: usize,
    pub probes: usize,
}

impl schedule::Settle<usize> for Driver {
    type Key = key::Handle;
    type Error = Infallible;

    fn settle(
        &mut self,
        _: key::Handle,
        value: usize,
        _: schedule::Terminal<Infallible>,
    ) -> schedule::Settlement<usize> {
        schedule::Settlement::Keep(value)
    }
}

impl schedule::Driver<usize> for Driver {
    type Block = Block;
    type Permit = NonZero<usize>;

    fn permit(&mut self) -> Option<NonZero<usize>> {
        self.probes += 1;
        self.remaining = self.remaining.checked_sub(1)?;
        Some(NonZero::<usize>::MIN)
    }

    fn control<'id>(
        &mut self,
        permit: NonZero<usize>,
        entry: schedule::Occupied<'_, Self::Key, usize, Block, region::Identity<'id>>,
    ) -> schedule::Decision<'id, Self::Key> {
        entry.borrowed(self, |_, _, value| {
            *value += permit.get();
            Ok(schedule::ControlTurn::new(
                schedule::Turn::Progress,
                schedule::WorkClass::Control,
            ))
        })
    }

    fn data(
        &mut self,
        permit: NonZero<usize>,
        _: key::Handle,
        value: &mut usize,
    ) -> Result<schedule::DataTurn<Block>, Infallible> {
        *value += permit.get();
        Ok(schedule::DataTurn::Progress(schedule::WorkClass::Data))
    }
}
