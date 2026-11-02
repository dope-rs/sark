use sark_http::owner::evidence;

use crate::life::{self, exchange};

pub struct Sink<'a, C: Copy, S: Copy, M: life::Mode<C, S>> {
    mode: &'a mut M,
    exchange: exchange::Id<C, S>,
}

impl<'a, C: Copy, S: Copy, M: life::Mode<C, S>> Sink<'a, C, S, M> {
    pub fn new(mode: &'a mut M, exchange: exchange::Id<C, S>) -> Self {
        Self { mode, exchange }
    }
}

impl<C: Copy, S: Copy, M: life::Mode<C, S>> evidence::Sink for Sink<'_, C, S, M> {
    const ENABLED: bool = M::ENABLED;

    fn copied(&mut self, copy: evidence::Copy) {
        self.mode.observe(life::Event::ResponseBodyCopied {
            exchange: self.exchange,
            read: copy.read,
            written: copy.written,
            cause: copy.cause,
        });
    }
}
