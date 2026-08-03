use std::cell::{Cell, OnceCell};
use std::io;
use std::pin::Pin;
use std::time::{Duration, Instant};

use dope::driver::timer;
use dope::driver::token::Token;
use dope::manifold::env::Env;
use dope::manifold::listener::{Listener, application::Application};
use dope::manifold::typed::TypedToken;
use dope::runtime::dispatcher::Idle;
use dope::{DriverContext, Event};
use dope_fiber::raw::task::RootWaker;
use dope_fiber::sleep::TimerExt;
use o3::cell::RegionToken;
use pin_project::pin_project;

pub const SARK_TIMER_ID: u8 = 3;

pub const DEFAULT_HEAD_TIMEOUT: Duration = Duration::from_secs(10);

#[pin_project]
struct Entry<'d> {
    target: Cell<Option<Token>>,
    #[pin]
    registration: timer::Registration<'d, 'd>,
}

struct Pool<'d> {
    entries: Pin<Box<[Entry<'d>]>>,
}

impl<'d> Pool<'d> {
    fn with_capacity(capacity: usize, timer: &'d timer::Timer<'d>) -> Self {
        assert!(
            capacity <= u32::MAX as usize,
            "sark timer capacity exceeds u32::MAX"
        );
        let entries = (0..capacity)
            .map(|_| Entry {
                target: Cell::new(None),
                registration: timer::Registration::new(timer),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            entries: Box::into_pin(entries),
        }
    }

    fn entry(&self, slot: u32) -> Option<Pin<&Entry<'d>>> {
        let entry = self.entries.as_ref().get_ref().get(slot as usize)?;
        // SAFETY: projecting a shared reference to an element of a pinned
        // boxed slice cannot move either the slice or the element.
        Some(unsafe { Pin::new_unchecked(entry) })
    }
}

pub struct Timer<'d> {
    inner: OnceCell<&'d timer::Timer<'d>>,
    pool: OnceCell<Pool<'d>>,
    head_timeout: Cell<Duration>,
}

impl<'d> Timer<'d> {
    pub fn new() -> Self {
        Self {
            inner: OnceCell::new(),
            pool: OnceCell::new(),
            head_timeout: Cell::new(DEFAULT_HEAD_TIMEOUT),
        }
    }

    fn inner(&self) -> &'d timer::Timer<'d> {
        self.inner
            .get()
            .expect("sark timer used before it was bound to a driver")
    }

    pub(crate) fn bind(&self, timer: &'d timer::Timer<'d>, connections: usize) -> io::Result<()> {
        self.inner.set(timer).map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "sark timer was bound more than once",
            )
        })?;
        let set = self.pool.set(Pool::with_capacity(connections, timer));
        assert!(
            set.is_ok(),
            "sark timer pool was initialized more than once"
        );
        Ok(())
    }

    pub fn set_head_timeout(&self, d: Duration) {
        self.head_timeout.set(d);
    }

    pub fn head_timeout(&self) -> Duration {
        self.head_timeout.get()
    }

    pub fn sleep(&self, d: Duration) -> impl dope_fiber::abi::Fiber<'d, Output = ()> + '_ {
        self.inner().sleep(d)
    }

    pub(crate) fn arm(&self, target: Token, deadline: Instant, wake: RootWaker<'d>) -> bool {
        let pool = self
            .pool
            .get()
            .expect("sark timer used before it was bound");
        let Some(entry) = pool.entry(target.slot().raw()) else {
            return false;
        };
        let fields = entry.project_ref();
        fields.target.set(Some(target));
        fields.registration.arm(deadline, wake.completion());
        true
    }

    pub(crate) fn cancel(&self, target: Token) -> bool {
        let pool = self
            .pool
            .get()
            .expect("sark timer used before it was bound");
        let Some(entry) = pool.entry(target.slot().raw()) else {
            return false;
        };
        if !entry
            .target
            .get()
            .is_some_and(|current| current.same_target(target))
        {
            return false;
        }
        let fields = entry.project_ref();
        fields.registration.cancel();
        fields.target.set(None);
        true
    }

    pub(crate) fn poll(&self, target: Token, now: Instant, wake: RootWaker<'d>) -> bool {
        let pool = self
            .pool
            .get()
            .expect("sark timer used before it was bound");
        let Some(entry) = pool.entry(target.slot().raw()) else {
            return false;
        };
        if !entry
            .target
            .get()
            .is_some_and(|current| current.same_target(target))
        {
            return false;
        }
        entry
            .project_ref()
            .registration
            .poll(now, wake.completion())
            .is_ready()
    }

    pub(crate) fn is_armed(&self, target: Token) -> bool {
        let pool = self
            .pool
            .get()
            .expect("sark timer used before it was bound");
        let Some(entry) = pool.entry(target.slot().raw()) else {
            return false;
        };
        entry
            .target
            .get()
            .is_some_and(|current| current.same_target(target))
            && entry.project_ref().registration.is_armed()
    }
}

impl<'d> Default for Timer<'d> {
    fn default() -> Self {
        Self::new()
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
    pub inner: Listener<'d, 'd, ID, P, E>,
}

impl<'d, const ID: u8, P, E> TimedListener<'d, ID, P, E>
where
    P: Application<'d> + TimerHost<'d>,
    E: Env<Wire = P::Wire>,
{
    pub fn new(inner: Listener<'d, 'd, ID, P, E>) -> Self {
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

    fn dispatch(self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        dope::manifold::Manifold::dispatch(self.project().inner, ev, driver)
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let typed = target.retag::<'d, Listener<'d, 'd, ID, P, E>>();
        dope::manifold::Manifold::activate(self.project().inner, typed, driver)
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        dope::manifold::Manifold::pre_park(self.project().inner, driver)
    }

    fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle {
        dope::manifold::Manifold::idle(self.project_ref().inner, region)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        dope::manifold::Manifold::shutdown(self.project().inner, driver)
    }
}
