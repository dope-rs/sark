use std::{cell::Cell, marker::PhantomData};

use sark_core::duplex;
use sark_http::payload::owned;

use crate::common::h1::Connect;

pub enum Kind {
    Single,
    Multiple,
    Conditional,
}

pub struct State<'a, 'd, C> {
    pub bytes: &'a [u8],
    pub source: C,
    pub driver: PhantomData<&'d ()>,
}

pub trait Source {
    type Session<'a>: for<'d> duplex::Session<'d>
    where
        Self: 'a;

    fn session<'a>(&'a self, bytes: &'a [u8]) -> Self::Session<'a>;
}

impl Source for Cell<usize> {
    type Session<'a> = Session<'a>;

    fn session<'a>(&'a self, bytes: &'a [u8]) -> Self::Session<'a> {
        Session { bytes, seen: self }
    }
}

pub struct Session<'a> {
    bytes: &'a [u8],
    seen: &'a Cell<usize>,
}

impl<'d> duplex::Session<'d> for Session<'_> {
    fn receive(&mut self, _: owned::Segments<'d>) -> duplex::Action<'d> {
        duplex::Action::close()
    }

    fn finish(&mut self) -> duplex::Action<'d> {
        duplex::Action::end()
    }

    fn activate(&mut self) -> duplex::Action<'d> {
        self.seen.set(self.bytes.len());
        duplex::Action::idle()
    }
}

#[sark_fiber::duplex(pub)]
fn open<'state, 'a, 'driver, C>(
    _: Connect,
    state: &'state State<'a, 'driver, C>,
) -> C::Session<'state>
where
    C: Source,
{
    state.source.session(state.bytes)
}

sark_fiber::define_route! {
    pub App<'brand, __ID>: State<'brand, '_, __ID> => {
        CONNECT "*" => open,
    }
}
