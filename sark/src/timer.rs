use std::cell::{Cell, OnceCell};
use std::pin::Pin;
use std::time::{Duration, Instant};

use dope::manifold::TypedToken;
use dope::manifold::env::Env;
use dope::manifold::listener::{Application, Listener};
use dope::manifold::timer;
pub use dope::manifold::timer::Ticket;
use dope::runtime::Idle;
use dope::{DriverContext, DriverRef, Event};
use dope_fiber::TimerExt as _;
use dope_fiber::Waker;

pub const SARK_TIMER_ID: u8 = 3;

pub const DEFAULT_HEAD_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Timer<'d> {
    inner: OnceCell<timer::Timer<'d, SARK_TIMER_ID>>,
    capacity: usize,
    head_timeout: Cell<Duration>,
}

impl<'d> Timer<'d> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: OnceCell::new(),
            capacity,
            head_timeout: Cell::new(DEFAULT_HEAD_TIMEOUT),
        }
    }

    fn inner(&self) -> &timer::Timer<'d, SARK_TIMER_ID> {
        self.inner
            .get()
            .expect("sark timer used before it was bound to a driver")
    }

    pub(crate) fn bind(&self, driver: DriverRef<'d>) {
        assert!(
            self.inner
                .set(timer::Timer::with_capacity(self.capacity, driver))
                .is_ok(),
            "sark timer bound more than once"
        );
    }

    pub fn set_head_timeout(&self, d: Duration) {
        self.head_timeout.set(d);
    }

    pub fn head_timeout(&self) -> Duration {
        self.head_timeout.get()
    }

    pub fn sleep(&self, d: Duration) -> impl dope_fiber::Fiber<'d, Output = ()> + '_ {
        self.inner().sleep(d)
    }

    pub fn arm(&self, deadline: Instant, wake: Waker<'d>) -> Option<Ticket> {
        self.inner().try_arm(deadline, wake.completion())
    }

    pub fn cancel(&self, ticket: Ticket) {
        self.inner().cancel(ticket);
    }

    pub fn is_fired(&self, ticket: Ticket) -> bool {
        self.inner().is_fired(ticket)
    }

    pub fn tick(&self, now: Instant) {
        self.inner().expire(now);
    }

    pub fn idle(&self) -> Idle {
        Idle::Park(self.inner().earliest())
    }
}

pub trait TimerHost<'d> {
    fn timer(&self) -> &Timer<'d>;
}

#[pin_project::pin_project]
pub struct TimedListener<'d, const ID: u8, P, E>
where
    P: Application<'d> + TimerHost<'d>,
    E: Env<Wire = P::Wire>,
{
    #[pin]
    pub inner: Listener<'d, ID, P, E>,
}

impl<'d, const ID: u8, P, E> TimedListener<'d, ID, P, E>
where
    P: Application<'d> + TimerHost<'d>,
    E: Env<Wire = P::Wire>,
{
    pub fn new(inner: Listener<'d, ID, P, E>, driver: DriverRef<'d>) -> Self {
        inner.handler().timer().bind(driver);
        Self { inner }
    }

    pub fn handler(&self) -> &P {
        self.inner.handler()
    }

    pub fn handler_mut(self: Pin<&mut Self>) -> Pin<&mut P> {
        self.project().inner.handler_mut()
    }
}

impl<'d, const ID: u8, P, E> dope::manifold::Manifold<'d> for TimedListener<'d, ID, P, E>
where
    P: Application<'d> + TimerHost<'d>,
    E: Env<Wire = P::Wire>,
{
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: Event, driver: &mut DriverContext<'_, 'd>) {
        dope::manifold::Manifold::dispatch(self.project().inner, ev, driver)
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let typed =
            unsafe { TypedToken::<Listener<'d, ID, P, E>>::new_unchecked(target.into_inner()) };
        dope::manifold::Manifold::activate(self.project().inner, typed, driver)
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_ref()
            .project_ref()
            .inner
            .handler()
            .timer()
            .tick(driver.turn_now());
        dope::manifold::Manifold::pre_park(self.as_mut().project().inner, driver)
    }

    fn idle(self: Pin<&Self>) -> Idle {
        let timer_idle = self.project_ref().inner.handler().timer().idle();
        dope::manifold::Manifold::idle(self.project_ref().inner).reduce(timer_idle)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        dope::manifold::Manifold::shutdown(self.project().inner, driver)
    }
}
