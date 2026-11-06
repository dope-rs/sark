use std::{cell::Cell, rc::Rc, time::Duration};

use dope::core::driver::schedule::clock::timer::Timer;
use dope_fiber::{
    abi::Fiber,
    task::time::sleep::{self, TimerExt as _},
};
use o3::buffer::storage;

pub const ROUTE: u8 = 0;

#[derive(Default)]
pub struct State {
    pub ingress: Option<Ingress>,
    pub guard: Option<GuardProbe>,
    pub body: Cell<Option<storage::Shared>>,
    pub header: Cell<Option<[u8; 28]>>,
    pub invalid: Cell<bool>,
    pub sleep: Cell<Option<sleep::Error>>,
    pub layout: Cell<Option<(usize, usize)>>,
    pub stream: Rc<crate::common::route::body::Trace>,
}

impl State {
    pub fn observed(&self) -> (bool, Option<sleep::Error>, Option<(usize, usize)>) {
        (self.invalid.get(), self.sleep.get(), self.layout.get())
    }

    pub fn with_guard(guard: GuardProbe) -> Self {
        Self {
            guard: Some(guard),
            ..Self::default()
        }
    }

    pub fn try_body() -> Result<Self, storage::AllocError> {
        let body = storage::Owned::try_filled(2 * 1024 * 1024, 0x6b)?.freeze();
        Ok(Self {
            body: Cell::new(Some(body)),
            ..Self::default()
        })
    }

    pub fn delay<'d>(&self, timer: Timer<'d>, duration: Duration) -> impl Fiber<'d, Output = ()> {
        sark_fiber::fiber! { '_ => async move {
            match timer.sleep(duration) {
                Ok(sleep) => sleep.await,
                Err(error) => {
                    if self.sleep.get().is_none() {
                        self.sleep.set(Some(error));
                    }
                }
            }
        } }
    }
}

#[derive(Default)]
pub struct GuardProbe {
    pub checks: Cell<usize>,
    pub handlers: Cell<usize>,
    pub padded: bool,
}

pub struct Ingress {
    pub flood_started: Cell<usize>,
    pub probe_seen_at: Cell<usize>,
}

/// Packed diagnostic identity emitted by the original H2 stream key.
#[derive(Clone, Copy, Debug)]
pub struct Identity(u64);

impl From<u64> for Identity {
    fn from(identity: u64) -> Self {
        Self(identity)
    }
}

impl Identity {
    pub const fn wire(self) -> u64 {
        self.0 >> 32
    }
}
