//! Static application binding for HTTP server carriers.

use dope_core::driver::{
    retained,
    schedule::{app, clock, ready},
};
use o3::{buffer::resident, depot::capacity};
use resident::bound;
use sark_http::field::metadata::section::rules;

use crate::manifold::{
    schedule,
    server::{self, drain, timing},
};

mod error;
pub use error::Error;

/// Cold-path resource requirements supplied by one application.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    streams: usize,
    bytes: usize,
}

impl Layout {
    pub const fn new(streams: usize, bytes: usize) -> Self {
        Self { streams, bytes }
    }

    pub const fn streams(self) -> usize {
        self.streams
    }

    pub const fn bytes(self) -> usize {
        self.bytes
    }
}

/// Exact carrier capacities available to one application connection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    streams: capacity::Count,
    data: u32,
}

impl Capacity {
    pub const fn new(streams: capacity::Count, data: u32) -> Self {
        Self { streams, data }
    }

    pub const fn streams(self) -> capacity::Count {
        self.streams
    }

    pub const fn data(self) -> u32 {
        self.data
    }
}

/// Borrowed resources for one application admission turn.
pub struct Turn<'a, 'work, 'borrow, 'owner, 'd, O> {
    owner: O,
    work: app::Work<'work, 'd>,
    wake: ready::Target<'d>,
    driver: &'a mut retained::Context<'borrow, 'owner, 'd>,
}

impl<'a, 'work, 'borrow, 'owner, 'd, O> Turn<'a, 'work, 'borrow, 'owner, 'd, O> {
    pub fn new(
        owner: O,
        work: app::Work<'work, 'd>,
        wake: ready::Target<'d>,
        driver: &'a mut retained::Context<'borrow, 'owner, 'd>,
    ) -> Self {
        Self {
            owner,
            work,
            wake,
            driver,
        }
    }

    pub fn owner(&self) -> O
    where
        O: Copy,
    {
        self.owner
    }

    pub const fn work(&self) -> app::Work<'work, 'd> {
        self.work
    }

    pub const fn wake(&self) -> ready::Target<'d> {
        self.wake
    }

    pub fn driver(&mut self) -> &mut retained::Context<'borrow, 'owner, 'd> {
        self.driver
    }
}

/// Header plan and resource layout known before a carrier is constructed.
pub trait Spec {
    type Failure;
    type Fields: rules::Policy;
    type Egress<'d>: bound::Bound;

    fn layout(&self) -> Layout;
}

/// Binds an exact-lifetime application to one concrete carrier and owner.
pub trait Bind<'d, C, O>: Spec<Egress<'d>: 'd> + Sized
where
    C: server::Connection,
{
    type App: Application<
            'd,
            C,
            O,
            Fields = Self::Fields,
            Failure = Self::Failure,
            Egress<'d> = Self::Egress<'d>,
        >;
    type Error;

    fn bind(self) -> Result<Self::App, Self::Error>;
}

/// Monomorphized semantic runtime driven by a protocol-specific host.
pub trait Application<'d, C, O>: Spec<Egress<'d>: 'd>
where
    C: server::Connection,
{
    type State;
    type Timing;
    type Build;

    fn state(&self, capacity: Capacity) -> Result<Self::State, Self::Build>;

    fn drive(
        &mut self,
        state: &mut Self::State,
        carrier: &mut C,
        turn: &mut Turn<'_, '_, '_, '_, 'd, O>,
        egress: schedule::Egress,
        report: impl FnMut(Error<C::Error, Self::Failure>),
    ) -> Result<drain::Progress, Error<C::Error, Self::Failure>>;

    fn close(&mut self, state: &mut Self::State, carrier: &mut C, owner: O);

    fn activity(state: &Self::State) -> schedule::Quiescence;
}

/// Deadline query implemented only by applications with timed state.
pub trait Timed<'d, C, O>: Application<'d, C, O, Timing = timing::On>
where
    C: server::Connection,
{
    fn deadline(&self, state: &Self::State) -> Option<clock::Deadline<'d>>;
}
