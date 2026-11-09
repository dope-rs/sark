use std::{convert::Infallible, net};

use dope_quic::runtime::mux::handler::hook::wire::packet;
use o3::buffer::hold::evidence;
use sark_core::{
    body::credit,
    life::{self, endpoint, exchange},
};

use crate::manifold::server::failure::{Event, Packet, Sink};

/// Retains the final failure consumer with lifecycle observation disabled.
#[repr(transparent)]
pub struct Quiet<O>(O);

impl<O> Quiet<O> {
    pub const fn new(output: O) -> Self {
        Self(output)
    }
}

impl<O: Packet> Packet for Quiet<O> {
    fn packet(&mut self, from: net::SocketAddr, len: usize, error: packet::Failure<'_>) {
        self.0.packet(from, len, error);
    }
}

impl<C, O: Sink<C>> Sink<C> for Quiet<O> {
    fn failure(&mut self, event: Event<'_, C>) {
        self.0.failure(event);
    }
}

impl<C: Copy, S: Copy, O> life::Mode<C, S> for Quiet<O> {
    const ENABLED: bool = false;
    const RESPONSE_CHARGE: bool = false;
    type Retention = evidence::Off;
    type Observer = Infallible;
    type Body<'a>
        = credit::Disabled
    where
        Self: 'a,
        C: 'a,
        S: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        None
    }

    fn body<'a>(&'a mut self, _: exchange::Id<C, S>) -> Self::Body<'a>
    where
        C: 'a,
        S: 'a,
    {
        credit::Disabled::new()
    }
}

impl<E, T, O> endpoint::Mode<E, T> for Quiet<O> {
    const ENABLED: bool = false;
    type Observer = Infallible;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        None
    }
}
