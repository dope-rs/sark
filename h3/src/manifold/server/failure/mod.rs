use std::{fmt, net};

use dope::{
    core::{driver::result::SubmitError, io::datagram::ReceiveError},
    manifold::network::datagram::RejectCause,
};
use dope_quic::{
    conn::output::transmit, runtime::mux::handler::hook::wire::packet, rustls, wire::stream::Id,
};
use o3::{alloc, depot::heap};
use sark_core::life::mode;

use crate::{
    conn::{self, options},
    frame,
    manifold::{owners, requests},
};

mod quiet;
pub use quiet::Quiet;

/// The application's final consumer for rejected UDP packets.
pub trait Packet {
    fn packet(&mut self, from: net::SocketAddr, len: usize, error: packet::Failure<'_>);
}

/// Failure to initialize one accepted HTTP/3 connection.
#[derive(Clone, Debug)]
pub enum Build {
    Protocol(options::Error),
    Requests(alloc::Error),
    Deadlines(heap::Error),
}

/// Response, connection setup, output, authentication and permanent socket failures.
#[derive(Debug)]
pub enum Event<'a, C> {
    Create {
        conn: C,
        error: &'a Build,
    },
    Connection {
        conn: C,
        error: &'a dyn fmt::Debug,
    },
    Transmit {
        conn: C,
        error: transmit::Error,
    },
    Response {
        conn: C,
        stream: Id,
        error: &'a dyn fmt::Debug,
    },
    Certificate(&'a rustls::Error),
    Receive(ReceiveError),
    Rejected(RejectCause),
    Submit(SubmitError),
}

/// The application's final consumer for response, connection and endpoint failures.
pub trait Sink<C> {
    fn failure(&mut self, event: Event<'_, C>);
}

pub(super) fn response<C>(
    life: &mut impl Sink<C>,
    handle: C,
    id: requests::Id,
    cause: conn::Error,
) -> frame::ErrorCode {
    life.failure(Event::Response {
        conn: handle,
        stream: id.stream_id(),
        error: &cause,
    });
    owners::code(cause)
}

impl<O: Packet> Packet for mode::Observed<O> {
    fn packet(&mut self, from: net::SocketAddr, len: usize, error: packet::Failure<'_>) {
        self.observer_mut().packet(from, len, error);
    }
}

impl<C, O: Sink<C>> Sink<C> for mode::Observed<O> {
    fn failure(&mut self, event: Event<'_, C>) {
        self.observer_mut().failure(event);
    }
}
