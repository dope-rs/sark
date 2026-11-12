use std::cell::Cell;

use o3::depot::arena::slab::key;
use sark_core::manifold::schedule;
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Blocked {
    Stream,
}

impl schedule::Block for Blocked {
    fn wait(self) -> schedule::Wait {
        match self {
            Self::Stream => schedule::Wait::Stream,
        }
    }
}

pub struct Permit<'a>(&'a Cell<bool>);

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

pub struct Reject<'a> {
    pub calls: usize,
    pub settled: Option<(usize, usize, schedule::Terminal<usize>, bool)>,
    pub repeat: bool,
    pub data: bool,
    pub retain: bool,
    pub live: bool,
    pub dropped: &'a Cell<bool>,
}

impl schedule::Settle<Box<usize>> for Reject<'_> {
    type Key = key::Handle;
    type Error = usize;

    fn settle(
        &mut self,
        _: Self::Key,
        value: &mut Box<usize>,
        terminal: schedule::Terminal<usize>,
    ) -> schedule::Settlement<()> {
        self.repeat |= self
            .settled
            .replace((**value, self.calls, terminal, self.dropped.get()))
            .is_some();
        if self.retain {
            schedule::Settlement::Keep(())
        } else {
            schedule::Settlement::Remove
        }
    }
}

impl<'a> schedule::Driver<Box<usize>> for Reject<'a> {
    type Block = Blocked;
    type Permit = Permit<'a>;

    fn permit(&mut self) -> Option<Self::Permit> {
        (self.calls == 0).then(|| Permit(self.dropped))
    }

    fn control(
        &mut self,
        permit: Self::Permit,
        _: Self::Key,
        _: &mut Box<usize>,
    ) -> Result<schedule::ControlTurn<Blocked>, usize> {
        self.calls += 1;
        self.live = !permit.0.get();
        Err(13)
    }

    fn data(
        &mut self,
        _: Self::Permit,
        _: Self::Key,
        _: &mut Box<usize>,
    ) -> Result<schedule::DataTurn<Blocked>, usize> {
        self.data = true;
        Ok(schedule::DataTurn::Idle)
    }
}
