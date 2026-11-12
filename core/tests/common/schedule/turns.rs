use std::{convert::Infallible, num::NonZero};

use o3::depot::arena::slab::key;
use sark_core::manifold::schedule;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Blocked {
    Egress,
    Connection,
    Stream,
}

impl schedule::Block for Blocked {
    fn wait(self) -> schedule::Wait {
        match self {
            Self::Egress => schedule::Wait::Egress,
            Self::Connection => schedule::Wait::Connection,
            Self::Stream => schedule::Wait::Stream,
        }
    }
}

#[derive(Debug)]
pub enum Stage {
    Head,
    Fused,
    Body,
    Trailer,
}

#[derive(Default)]
pub struct Writer {
    pub block: Option<Blocked>,
    pub remaining: usize,
    pub control: usize,
    pub data: usize,
    pub idle: usize,
    pub closed: usize,
    pub invalid: bool,
}

impl schedule::Settle<Stage> for Writer {
    type Key = key::Handle;
    type Error = Infallible;

    fn settle(
        &mut self,
        _: key::Handle,
        _: &mut Stage,
        terminal: schedule::Terminal<Infallible>,
    ) -> schedule::Settlement<()> {
        match terminal {
            schedule::Terminal::Complete => {
                self.closed += 1;
                schedule::Settlement::Remove
            }
            schedule::Terminal::Idle => {
                self.idle += 1;
                schedule::Settlement::Keep(())
            }
            schedule::Terminal::Failed(error) => match error {},
        }
    }
}

impl schedule::Driver<Stage> for Writer {
    type Block = Blocked;
    type Permit = NonZero<usize>;

    fn permit(&mut self) -> Option<NonZero<usize>> {
        let permit = NonZero::new(self.remaining)?;
        self.remaining -= 1;
        Some(permit)
    }

    fn control(
        &mut self,
        permit: NonZero<usize>,
        _: Self::Key,
        value: &mut Stage,
    ) -> Result<schedule::ControlTurn<Blocked>, Infallible> {
        self.invalid |= permit.get() != self.remaining + 1;
        self.control += 1;
        Ok(match value {
            Stage::Head if self.control == 1 => {
                schedule::ControlTurn::blocked(self.block.unwrap_or(Blocked::Egress))
            }
            Stage::Head => {
                *value = Stage::Body;
                schedule::ControlTurn::progress(schedule::WorkClass::Data)
            }
            Stage::Fused => {
                *value = Stage::Body;
                self.data += 1;
                schedule::ControlTurn::fused(
                    schedule::Turn::Blocked(Blocked::Egress),
                    schedule::WorkClass::Data,
                )
            }
            Stage::Body => schedule::ControlTurn::idle(),
            Stage::Trailer => schedule::ControlTurn::complete(),
        })
    }

    fn data(
        &mut self,
        permit: NonZero<usize>,
        _: key::Handle,
        value: &mut Stage,
    ) -> Result<schedule::DataTurn<Blocked>, Infallible> {
        self.invalid |= permit.get() != self.remaining + 1 || !matches!(value, Stage::Body);
        self.data += 1;
        Ok(if self.data == 1 {
            schedule::DataTurn::Blocked(match self.block {
                Some(Blocked::Egress) => Blocked::Egress,
                _ => Blocked::Stream,
            })
        } else {
            *value = Stage::Trailer;
            schedule::DataTurn::Progress(schedule::WorkClass::Control)
        })
    }
}
