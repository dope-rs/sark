//! Static application binding for HTTP server carriers.

use dope::core::driver::{
    self,
    schedule::{app, clock, ready},
};
use o3::{buffer::resident, depot::capacity};
use resident::bound;
use sark_http::field::metadata::section::rules;

use sark_core::manifold::{
    schedule,
    server::{self, drain, timing},
};

use crate::{
    conn::{self, event},
    server::runtime::Connection,
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
pub struct Turn<'a, 'work, 'borrow, 'd, O> {
    owner: O,
    work: app::Work<'work, 'd>,
    wake: ready::Target<'d>,
    driver: &'a mut driver::Context<'borrow, 'd>,
}

impl<'a, 'work, 'borrow, 'd, O> Turn<'a, 'work, 'borrow, 'd, O> {
    pub fn new(
        owner: O,
        work: app::Work<'work, 'd>,
        wake: ready::Target<'d>,
        driver: &'a mut driver::Context<'borrow, 'd>,
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

    pub fn driver(&mut self) -> &mut driver::Context<'borrow, 'd> {
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

/// Monomorphized semantic runtime driven by a protocol-specific host.
pub trait Application<'d, E: event::Evidence, O>: Spec<Egress<'d>: 'd> {
    type State;
    type Selection;
    type Timing;
    type Build;

    fn state(&self, capacity: Capacity) -> Result<Self::State, Self::Build>;

    fn drive<H>(
        &mut self,
        state: &mut Self::State,
        carrier: &mut Connection<'d, Self, E>,
        turn: &mut Turn<'_, '_, '_, 'd, O>,
        egress: schedule::Egress,
        context: &mut H,
        protocol: impl FnMut(
            &mut H,
            &mut Connection<'d, Self, E>,
            &mut drain::Events<'_, 'd>,
            Option<&Self::Selection>,
        )
            -> Result<Option<server::Event<Connection<'d, Self, E>>>, conn::Error>,
        notify: impl FnMut(&mut H, schedule::Wake<crate::stream::Key>),
        finish: impl FnOnce(&mut H, &mut Connection<'d, Self, E>),
        report: impl FnMut(&mut H, Error<Self::Failure>),
    ) -> Result<drain::Progress, Error<Self::Failure>>;

    fn close(&mut self, state: &mut Self::State, carrier: &mut Connection<'d, Self, E>, owner: O);

    fn selected(
        state: Option<&Self::Selection>,
        event: &server::Event<Connection<'d, Self, E>>,
    ) -> bool;

    fn wake(state: &mut Self::State, wake: schedule::Wake<crate::stream::Key>) -> bool;

    /// Records released egress capacity without executing application work.
    fn released(state: &mut Self::State);

    fn activity(state: &Self::State) -> schedule::Quiescence;
}

/// Deadline query implemented only by applications with timed state.
pub trait Timed<'d, E: event::Evidence, O>: Application<'d, E, O, Timing = timing::On> {
    fn deadline(&self, state: &Self::State) -> Option<clock::Deadline<'d>>;
}
