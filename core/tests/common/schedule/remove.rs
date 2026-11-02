use std::cell::Cell;

use o3::{base::cell::region, depot::arena::slab::key};
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

pub enum Failure {
    Pending { remaining: usize },
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
    pub owned: bool,
    pub live: bool,
    pub dropped: &'a Cell<bool>,
}

impl schedule::Settle<Box<usize>> for Reject<'_> {
    type Key = key::Handle;
    type Error = usize;

    fn settle(
        &mut self,
        _: Self::Key,
        value: Box<usize>,
        terminal: schedule::Terminal<usize>,
    ) -> schedule::Settlement<Box<usize>> {
        self.repeat |= self
            .settled
            .replace((*value, self.calls, terminal, self.dropped.get()))
            .is_some();
        if self.retain {
            schedule::Settlement::Keep(value)
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

    fn control<'id>(
        &mut self,
        permit: Self::Permit,
        entry: schedule::Occupied<
            '_,
            Self::Key,
            Box<usize>,
            Blocked,
            region::Proof<region::Identity<'id>>,
        >,
    ) -> schedule::Decision<'id, Self::Key> {
        if self.owned {
            entry.owned(self, |driver, _, value| {
                let permit = permit;
                driver.calls += 1;
                driver.live = !permit.0.get();
                schedule::Control::Failed { value, error: 13 }
            })
        } else {
            entry.borrowed(self, |driver, _, _| {
                let permit = permit;
                driver.calls += 1;
                driver.live = !permit.0.get();
                Err(13)
            })
        }
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
