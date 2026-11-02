use o3::depot::{arena::slab::key, queue::indexed::lanes::typed::Value};
use sark_core::manifold::schedule::{self, typed};

use crate::common::schedule::turns::Blocked;

#[derive(Debug)]
pub struct Head {
    pub owner: Box<usize>,
    pub fused: bool,
}

#[derive(Default)]
pub struct Driver {
    pub left: usize,
    pub block: Option<Blocked>,
    pub failure: Option<u8>,
    pub error: Option<u8>,
    pub heads: usize,
    pub bodies: usize,
}

impl typed::Settle<Head, Box<usize>, Box<usize>> for Driver {
    type Key = key::Handle;
    type Error = u8;

    fn settle(
        &mut self,
        _: key::Handle,
        value: Value<Head, Box<usize>, Box<usize>>,
        terminal: schedule::Terminal<u8>,
    ) -> schedule::Settlement<Value<Head, Box<usize>, Box<usize>>> {
        match terminal {
            schedule::Terminal::Failed(error) => {
                self.error = Some(error);
                schedule::Settlement::Keep(value)
            }
            schedule::Terminal::Idle => schedule::Settlement::Keep(value),
            schedule::Terminal::Complete => schedule::Settlement::Remove,
        }
    }
}

impl typed::Driver<Head, Box<usize>, Box<usize>> for Driver {
    type Block = Blocked;
    type Permit = NonZero<usize>;

    fn permit(&mut self) -> Option<NonZero<usize>> {
        let permit = NonZero::new(self.left)?;
        self.left -= 1;
        Some(permit)
    }

    fn control(
        &mut self,
        _: NonZero<usize>,
        _: key::Handle,
        mut value: Head,
    ) -> typed::Control<Head, Box<usize>, Box<usize>, Blocked, u8> {
        self.heads += 1;
        let turn = self
            .block
            .map_or(schedule::Turn::Progress, schedule::Turn::Blocked);
        if value.fused {
            self.bodies += 1;
            *value.owner += 1;
            typed::Control::Fused {
                value: value.owner,
                turn,
            }
        } else {
            typed::Control::Data {
                value: value.owner,
                turn,
            }
        }
    }

    fn data(
        &mut self,
        _: NonZero<usize>,
        _: key::Handle,
        value: &mut Box<usize>,
    ) -> Result<schedule::Turn<Blocked>, u8> {
        self.bodies += 1;
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        **value += 1;
        Ok(schedule::Turn::Idle)
    }
}
use std::num::NonZero;
