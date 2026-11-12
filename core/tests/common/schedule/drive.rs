use std::{convert::Infallible, num::NonZero};

use o3::depot::arena::slab::key;
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
        _: &mut usize,
        _: schedule::Terminal<Infallible>,
    ) -> schedule::Settlement<()> {
        schedule::Settlement::Keep(())
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

    fn control(
        &mut self,
        permit: NonZero<usize>,
        _: Self::Key,
        value: &mut usize,
    ) -> Result<schedule::ControlTurn<Block>, Infallible> {
        *value += permit.get();
        Ok(schedule::ControlTurn::new(
            schedule::Turn::Progress,
            schedule::WorkClass::Control,
        ))
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
