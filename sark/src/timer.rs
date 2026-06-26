use std::cell::Cell;
use std::future::Future;
use std::task::Waker;
use std::time::{Duration, Instant};

use dope::WakeRef;
use dope::fiber::{Fiber, Holding};
use dope::manifold::timer;
pub use dope::manifold::timer::Ticket;

pub const SARK_TIMER_ID: u8 = 3;

pub const DEFAULT_HEAD_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct Timer<'d> {
    holding: Holding<'d, timer::Timer<SARK_TIMER_ID>>,
    head_timeout: Duration,
}

impl<'d> Timer<'d> {
    pub fn new(holding: Holding<'d, timer::Timer<SARK_TIMER_ID>>, head_timeout: Duration) -> Self {
        Self {
            holding,
            head_timeout,
        }
    }

    pub fn sleep(&self, d: Duration) -> Fiber<'d, impl Future<Output = ()> + 'd> {
        self.holding.sleep(d)
    }

    pub fn head_timeout(&self) -> Duration {
        self.head_timeout
    }

    pub fn arm(&self, deadline: Instant, waker: &Waker) -> Option<Ticket> {
        self.holding
            .hold()
            .get_mut()
            .try_arm(deadline, WakeRef::verified(waker))
    }

    pub fn cancel(&self, ticket: Ticket) {
        self.holding.hold().get_mut().cancel(ticket);
    }

    pub fn is_fired(&self, ticket: Ticket) -> bool {
        self.holding.is_fired(ticket)
    }
}

pub trait TimerHost<'d> {
    fn timer_cell(&self) -> &Cell<Option<Timer<'d>>>;

    fn bind_timer(
        &self,
        holding: Holding<'d, timer::Timer<SARK_TIMER_ID>>,
        head_timeout: Duration,
    ) {
        self.timer_cell()
            .set(Some(Timer::new(holding, head_timeout)));
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
