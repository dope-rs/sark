use std::{
    cell::Cell,
    io::{self, Write},
    net::TcpStream,
    task::Poll,
};

use dope_test::harness::{
    observe::VecCellExt as _,
    signals::{LocalGate, Progress},
};
use o3::buffer;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code, Status,
    message::Frame,
    metadata,
    protocol::request,
    server::service::{
        self, Stream,
        handler::{Handler, OutputState, RequestState, Start},
        reply::{self, Reply},
        timers,
    },
};
use sark_h2::{
    ClientRole,
    conn::{self, DataOutcome, event, machine::Connection},
    stream::Id,
};
use sark_http::field::{self, Entry, Lower, Pseudo, Value};

use crate::common::codec::Bytes;

pub const BURST: usize = 1_024;
pub const FLOOD: &[u8] = b"/fair/Flood";
pub const OTHER_FLOOD: &[u8] = b"/fair/Other";
pub const CHECK: &[u8] = b"/fair/Probe";
pub const PROBE: &[u8] = b"probe";
pub const HELD: &[u8] = b"/fair/Held";
pub const OUTPUT_BYTES: usize = 32_768;

#[derive(Default)]
pub struct OutputTrace {
    pub produced: [Cell<usize>; 2],
    pub blocked: Cell<usize>,
    pub resumed: Cell<Option<usize>>,
    pub failed: Cell<bool>,
    pub armed: Cell<bool>,
    pub unfair: Cell<bool>,
    competing: bool,
    last: Cell<Option<usize>>,
}

impl OutputTrace {
    pub fn new(competing: bool) -> Self {
        Self {
            competing,
            ..Self::default()
        }
    }
}

impl Progress for OutputTrace {
    fn hits(&self) -> u32 {
        if self.failed.get() || self.unfair.get() {
            2
        } else {
            u32::from(self.blocked.get() == if self.competing { 2 } else { 1 })
                + u32::from(if self.competing {
                    self.produced.iter().all(|count| count.get() == BURST)
                } else {
                    self.resumed.get().is_some() && self.produced[0].get() == BURST
                })
        }
    }
}

pub struct OutputFairness<'a>(pub &'a OutputTrace);

#[derive(Default)]
pub struct OutputCall {
    remaining: usize,
    payload: Option<Vec<u8>>,
    index: usize,
    blocked: bool,
}

impl Handler for OutputFairness<'_> {
    type Call = OutputCall;
    type Timer = timers::Off;

    fn start<W: reply::Write>(
        &mut self,
        _: &mut (),
        _: &mut Stream,
        head: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> Start<Self::Call> {
        if head.path() == HELD {
            reply.push_message(vec![0; OUTPUT_BYTES / 2]);
            return Start::live(OutputCall::default());
        }
        Start::live(OutputCall {
            remaining: if head.path() == FLOOD || head.path() == OTHER_FLOOD {
                BURST
            } else {
                0
            },
            payload: (head.path() == CHECK).then(|| vec![1; OUTPUT_BYTES / 2]),
            index: usize::from(head.path() == OTHER_FLOOD),
            blocked: false,
        })
        .producing()
    }

    fn message<P: buffer::raw::Segment, W: reply::Write>(
        &mut self,
        _: &mut (),
        _: &mut Self::Call,
        _: &mut Stream,
        _: Frame<P>,
        _: &mut Reply<'_, W>,
    ) {
    }

    fn request<P: buffer::raw::Segment, W: reply::Write>(
        &mut self,
        _: &mut (),
        _: &mut Self::Call,
        _: service::Request<'_, P>,
        _: &mut service::Response<'_, W>,
    ) -> RequestState {
        RequestState::Complete
    }

    fn output<W: reply::Write>(
        &mut self,
        _: &mut (),
        call: &mut Self::Call,
        _: &mut timers::Output<'_, '_, '_, timers::Off>,
        reply: &mut Reply<'_, W>,
    ) -> OutputState {
        let probe = call.payload.is_some();
        if !probe && call.remaining != 0 && self.0.competing && !self.0.armed.get() {
            return OutputState::Pending;
        }
        if probe || call.remaining != 0 {
            match reply.try_encode(&mut Bytes, call.payload.as_ref().unwrap_or(&Vec::new())) {
                Ok(Poll::Ready(())) => {
                    if probe {
                        call.payload.take();
                        self.0.resumed.set(Some(self.0.produced[0].get()));
                    } else {
                        if self.0.last.replace(Some(call.index)) == Some(call.index)
                            && self.0.competing
                            && self
                                .0
                                .produced
                                .iter()
                                .all(|count| (1..BURST).contains(&count.get()))
                        {
                            self.0.unfair.set(true);
                        }
                        call.remaining -= 1;
                        let count = &self.0.produced[call.index];
                        count.set(count.get() + 1);
                    }
                    return OutputState::Produced;
                }
                Ok(Poll::Pending) => {
                    if probe {
                        if !call.blocked {
                            self.0.blocked.set(self.0.blocked.get() + 1);
                            call.blocked = true;
                        }
                    } else {
                        self.0.failed.set(true);
                    }
                    return OutputState::Blocked;
                }
                Err(status) => {
                    self.0.failed.set(true);
                    reply.finish_with(status);
                    return OutputState::Complete;
                }
            }
        }
        reply.finish_with(Status::ok());
        OutputState::Complete
    }
}

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
    pub finished: LocalGate,
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
        self.trace.finished.handle().hit();
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
        loop {
            let bytes = self.output().first();
            if bytes.is_empty() {
                return Ok(());
            }
            let len = bytes.len();
            socket.write_all(bytes)?;
            self.transfer().drain(len);
        }
    }
}

pub fn assert_replies<const N: usize>(
    client: &mut Connection<ClientRole>,
    ids: &[Id; N],
    bodies: &mut [Vec<u8>; N],
    done: &mut [bool; N],
) {
    while let Some(item) = client.ingress().poll_event() {
        assert!(
            !matches!(&item, event::Event::StreamReset { .. }),
            "reply reset"
        );
        match item {
            event::Event::Data {
                stream_id: key,
                data,
                ..
            } => {
                let index = ids
                    .iter()
                    .position(|id| *id == key.id())
                    .expect("reply stream");
                data.for_each(|part| bodies[index].extend_from_slice(part));
            }
            event::Event::Headers {
                stream_id: key,
                head,
                boundary: Boundary::End,
                ..
            } => {
                let index = ids
                    .iter()
                    .position(|id| *id == key.id())
                    .expect("reply stream");
                assert!(!done[index], "terminal reply repeated");
                assert_eq!(
                    Status::from_fields(head.fields())
                        .expect("reply status")
                        .code(),
                    Code::Ok
                );
                done[index] = true;
            }
            _ => {}
        }
    }
}
