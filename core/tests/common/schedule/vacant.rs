use o3::{
    alloc,
    depot::{arena::slab::key, capacity},
};
use sark_core::manifold::schedule::{self, Runtime};
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Blocked {
    Connection,
    Stream,
}

impl schedule::Block for Blocked {
    fn wait(self) -> schedule::Wait {
        match self {
            Self::Connection => schedule::Wait::Connection,
            Self::Stream => schedule::Wait::Stream,
        }
    }
}

pub trait Bounded: Sized {
    fn try_new() -> Result<Self, alloc::Error>;
}

impl<T> Bounded for Runtime<key::Handle, T, Blocked> {
    fn try_new() -> Result<Self, alloc::Error> {
        Self::try_bounded(capacity::Count::new(4), capacity::Count::new(1))
    }
}

pub trait Handle {
    fn key(index: u32) -> Self;
}

impl Handle for key::Handle {
    fn key(index: u32) -> Self {
        Self::from_parts(key::Parts::new(index, key::Generation::MIN))
    }
}
