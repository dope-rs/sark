use o3::buffer::segments;
use sark_http::owner::{self, evidence};

use crate::life::{self, exchange, response};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Request {
    pub bytes: usize,
    pub owners: segments::Count,
}

impl Request {
    pub const fn new(bytes: usize, owners: segments::Count) -> Self {
        Self { bytes, owners }
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::new(0, segments::Count::EMPTY)
    }
}

/// Exclusive capability for applying one response-owner effect.
#[repr(transparent)]
pub struct Projection<'a, M> {
    mode: &'a mut M,
}

impl<'a, M> Projection<'a, M> {
    pub const fn new(mode: &'a mut M) -> Self {
        Self { mode }
    }

    pub fn apply<C: Copy, S: Copy>(self, effect: owner::Effect<exchange::Id<C, S>>)
    where
        M: life::Mode<C, S>,
    {
        if effect.charge().is_empty() {
            return;
        }
        self.mode.observe(life::Event::ResponseOwnership(effect));
    }
}

pub struct Sink<'a, C, M, F> {
    mode: &'a mut M,
    conn: C,
    stream: F,
}

impl<'a, C, M, F> Sink<'a, C, M, F> {
    pub fn new(mode: &'a mut M, conn: C, stream: F) -> Self {
        Self { mode, conn, stream }
    }
}

impl<C: Copy, M, F> Sink<'_, C, M, F> {
    pub fn apply<K, S: Copy>(&mut self, effect: owner::Effect<K>)
    where
        M: life::Mode<C, S>,
        F: FnMut(K) -> S,
    {
        if !M::ENABLED || effect.charge().is_empty() {
            return;
        }
        let conn = self.conn;
        let stream = &mut self.stream;
        Projection::new(self.mode)
            .apply(effect.map_key(|key| exchange::Id::new(conn, stream(key))));
    }

    pub fn copied<K, S: Copy>(&mut self, key: K, copy: evidence::Copy)
    where
        M: life::Mode<C, S>,
        F: FnMut(K) -> S,
    {
        if !M::ENABLED {
            return;
        }
        let exchange = exchange::Id::new(self.conn, (self.stream)(key));
        let mut sink = response::Sink::new(self.mode, exchange);
        evidence::Sink::copied(&mut sink, copy);
    }
}

const _: () = assert!(
    core::mem::size_of::<Projection<'static, life::mode::Disabled>>()
        == core::mem::size_of::<usize>()
);
