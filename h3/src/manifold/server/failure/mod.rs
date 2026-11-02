use std::net;

use dope::{
    core::{driver::result::SubmitError, io::datagram::ReceiveError},
    manifold::network::datagram::RejectCause,
};
use dope_quic::{conn::output::transmit, runtime::mux::handler::hook::wire::packet, rustls};
use o3::{alloc, depot::heap};
use sark_core::life::mode;

use crate::conn::options;

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

/// Connection setup, output, native authentication and permanent socket failures.
#[derive(Debug)]
pub enum Event<'a, C> {
    Create { conn: C, error: &'a Build },
    Transmit { conn: C, error: transmit::Error },
    Certificate(&'a rustls::Error),
    Receive(ReceiveError),
    Rejected(RejectCause),
    Submit(SubmitError),
}

/// The application's final consumer for connection and endpoint failures.
pub trait Sink<C> {
    fn failure(&mut self, event: Event<'_, C>);
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
