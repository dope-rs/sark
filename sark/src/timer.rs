use std::cell::Cell;
use std::future::Future;
use std::time::Duration;

use dope::fiber::{Fiber, Holding};
use dope::manifold::timer;

pub const SARK_TIMER_ID: u8 = 3;

#[derive(Clone)]
pub struct Timer<'d> {
    holding: Holding<'d, timer::Timer<SARK_TIMER_ID>>,
}

impl<'d> Timer<'d> {
    pub fn new(holding: Holding<'d, timer::Timer<SARK_TIMER_ID>>) -> Self {
        Self { holding }
    }

    pub fn sleep(&self, d: Duration) -> Fiber<'d, impl Future<Output = ()> + 'd> {
        self.holding.sleep(d)
    }
}

pub trait TimerHost<'d> {
    fn timer_cell(&self) -> &Cell<Option<Timer<'d>>>;

    fn bind_timer(&self, holding: Holding<'d, timer::Timer<SARK_TIMER_ID>>) {
        self.timer_cell().set(Some(Timer::new(holding)));
    }

    fn is_timer_bound(&self) -> bool {
        let cell = self.timer_cell();
        let t = cell.take();
        let bound = t.is_some();
        cell.set(t);
        bound
    }

    fn timer(&self) -> Timer<'d> {
        let cell = self.timer_cell();
        let t = cell
            .take()
            .expect("timer not bound — server tick must run before a fiber route dispatches");
        let cloned = t.clone();
        cell.set(Some(t));
        cloned
    }
}
