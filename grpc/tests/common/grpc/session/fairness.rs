use std::{
    cell::Cell,
    io::{self, Write},
    net::TcpStream,
};

use dope_test::harness::{observe::VecCellExt as _, signals::Progress};
use o3::buffer;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Status,
    message::Frame,
    metadata,
    protocol::request,
    server::service::{
        self, Stream,
        handler::{Handler, RequestState, Start},
        reply::{self, Reply},
        timers,
    },
};
use sark_h2::{
    ClientRole,
    conn::{self, DataOutcome, machine::Connection},
    stream::Id,
};
use sark_http::field::{self, Entry, Lower, Pseudo, Value};

pub const BURST: usize = 1_024;
pub const FLOOD: &[u8] = b"/fair/Flood";
pub const CHECK: &[u8] = b"/fair/Probe";
pub const PROBE: &[u8] = b"probe";

#[derive(Clone, Debug)]
pub enum Event {
    First,
    Probe(usize),
    Done(usize),
    Buffered,
}

#[derive(Default)]
pub struct Trace {
    pub flood: Cell<usize>,
    pub events: Cell<Vec<Event>>,
}

impl Progress for Trace {
    fn hits(&self) -> u32 {
        u32::from(!self.events.is_empty())
    }
}

pub struct Fairness<'a> {
    pub trace: &'a Trace,
    pub first: bool,
}

impl Handler for Fairness<'_> {
    type Call = ();
    type Timer = timers::Off;

    fn start<W: reply::Write>(
        &mut self,
        &mut (): &mut (),
        _: &mut Stream,
        _: &request::Head,
        _: &mut Reply<'_, W>,
    ) -> Start<Self::Call> {
        Start::live(())
    }

    fn message<P: buffer::raw::Segment, W: reply::Write>(
        &mut self,
        &mut (): &mut (),
        &mut (): &mut (),
        _: &mut Stream,
        message: Frame<P>,
        reply: &mut Reply<'_, W>,
    ) {
        if message.is_empty() {
            let flood = self.trace.flood.get() + 1;
            self.trace.flood.set(flood);
            if flood == 1 && self.first {
                self.trace.events.push(Event::First);
            }
            if flood == BURST {
                self.trace.events.push(Event::Done(flood));
            }
            return;
        }
        let mut payload = Vec::with_capacity(message.len());
        for segment in message.payload.segments() {
            payload.extend_from_slice(segment.as_ref());
        }
        self.trace.events.push(Event::Probe(self.trace.flood.get()));
        reply.push_message(payload);
    }

    fn trailers<W: reply::Write>(
        &mut self,
        &mut (): &mut (),
        &mut (): &mut (),
        _: &mut Stream,
        _: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        reply.finish_with(Status::ok());
    }

    fn request<P: buffer::raw::Segment, W: reply::Write>(
        &mut self,
        &mut (): &mut (),
        &mut (): &mut (),
        _: service::Request<'_, P>,
        _: &mut service::Response<'_, W>,
    ) -> RequestState {
        self.trace.events.push(Event::Buffered);
        RequestState::Complete
    }
}

impl Fairness<'_> {
    pub fn try_headers<'a>(path: &'a [u8]) -> Result<[Entry<'a>; 6], field::Error> {
        Ok([
            Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"POST"))),
            Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"http"))),
            Entry::pseudo(Pseudo::Path, Value::try_bytes(path)?),
            Entry::pseudo(
                Pseudo::Authority,
                field::literal!(Value::try_bytes(b"localhost")),
            ),
            Entry::new(
                field::literal!(Lower::try_bytes(b"content-type")),
                field::literal!(Value::try_bytes(b"application/grpc+proto")),
            ),
            Entry::new(
                field::literal!(Lower::try_bytes(b"te")),
                field::literal!(Value::try_bytes(b"trailers")),
            ),
        ])
    }
}

pub trait Client {
    fn try_send_all(&mut self, stream: Id, bytes: &[u8]) -> Result<usize, conn::Error>;
    fn try_flush(&mut self, socket: &TcpStream) -> Result<(), io::Error>;
}

impl Client for Connection<ClientRole> {
    fn try_send_all(&mut self, stream: Id, bytes: &[u8]) -> Result<usize, conn::Error> {
        let mut offset = 0;
        while offset < bytes.len() {
            let written = self
                .streams()
                .try_send(stream, &bytes[offset..], Boundary::End)?;
            match written {
                DataOutcome::Sent(written) => offset += written.get(),
                DataOutcome::Ended | DataOutcome::Blocked(_) => break,
            }
        }
        Ok(offset)
    }

    fn try_flush(&mut self, mut socket: &TcpStream) -> Result<(), io::Error> {
        let bytes = self.output().first();
        let len = bytes.len();
        socket.write_all(bytes)?;
        self.transfer().drain(len);
        Ok(())
    }
}
