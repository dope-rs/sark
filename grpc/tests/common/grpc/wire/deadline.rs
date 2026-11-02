use std::{cell::Cell, time::Duration};

use o3::buffer;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code, Status, message, metadata,
    protocol::request,
    server::service::{
        self,
        handler::{self, Handler},
        reply::{Reply, Write},
        timers,
    },
};
use sark_h2::{conn::event::Event, frame::numbers::ErrorCode, stream::Id};
use sark_http::field::{self, Entry, Lower, Value};

use crate::common::grpc::{
    session::fairness::Fairness,
    wire::{Failure, Peer},
};

#[derive(Default)]
pub struct Facts {
    pub starts: Cell<usize>,
    pub messages: Cell<usize>,
    pub outputs: Cell<usize>,
    pub drops: Cell<usize>,
    pub error: Cell<Option<timers::Error>>,
}

#[derive(Default)]
pub struct State<'a>(Option<&'a Facts>);

impl Drop for State<'_> {
    fn drop(&mut self) {
        if let Some(facts) = self.0 {
            facts.drops.set(facts.drops.get() + 1);
        }
    }
}

pub struct Direct<'a>(pub &'a Facts);

impl<'a> Handler for Direct<'a> {
    type Call = State<'a>;
    type Timer = timers::Off;

    fn start<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut service::Stream,
        head: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> handler::Start<Self::Call> {
        self.0.starts.set(self.0.starts.get() + 1);
        if head.path() == b"/deadline/queued" {
            reply.push_message(vec![7; 4]);
            reply.finish_with(Status::ok());
        }
        handler::Start::live(State(Some(self.0)))
    }

    fn message<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut Self::Call,
        _: &mut service::Stream,
        _: message::Frame<S>,
        _: &mut Reply<'_, W>,
    ) {
        self.0.messages.set(self.0.messages.get() + 1);
    }

    fn trailers<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut Self::Call,
        _: &mut service::Stream,
        _: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        reply.finish_with(Status::ok());
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut Self::Call,
        _: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        response.finish_with(Status::from_code(Code::Internal));
        handler::RequestState::Complete
    }
}

pub struct Deferred<'a>(pub Direct<'a>);

impl<'a> Handler for Deferred<'a> {
    type Call = State<'a>;
    type Timer = timers::On;

    fn start<W: Write>(
        &mut self,
        context: &mut (),
        stream: &mut service::Stream,
        head: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> handler::Start<Self::Call> {
        self.0.start(context, stream, head, reply).producing()
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        context: &mut (),
        call: &mut Self::Call,
        request: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        self.0.request(context, call, request, response)
    }

    fn output<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut Self::Call,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
        _: &mut Reply<'_, W>,
    ) -> handler::OutputState {
        let facts = self.0.0;
        facts.outputs.set(facts.outputs.get() + 1);
        if let Err(error) = output.defer_for(Duration::from_secs(1)) {
            facts.error.set(Some(error));
        }
        handler::OutputState::Pending
    }
}

pub fn try_fields<'a>(
    path: &'a [u8],
    timeout: Option<&'a [u8]>,
) -> Result<Vec<Entry<'a>>, field::Error> {
    let mut fields = Vec::from(Fairness::try_headers(path)?);
    if let Some(timeout) = timeout {
        fields.push(Entry::new(
            field::literal!(Lower::try_bytes(b"grpc-timeout")),
            Value::try_bytes(timeout)?,
        ));
    }
    Ok(fields)
}

#[derive(Default)]
pub struct Response {
    pub body: Vec<u8>,
    pub code: Option<Code>,
    pub reset: Option<ErrorCode>,
}

pub fn try_replies(peer: &mut Peer, ids: &[Id]) -> Result<Vec<Response>, Failure> {
    let mut replies: Vec<_> = ids.iter().map(|_| Response::default()).collect();
    while replies
        .iter()
        .any(|reply| reply.code.is_none() && reply.reset.is_none())
    {
        peer.try_flush()?;
        peer.try_read()?;
        while let Some(event) = peer.client.ingress().poll_event() {
            match event {
                Event::Data {
                    stream_id: stream,
                    data,
                    ..
                } => {
                    if let Some((_, reply)) = ids
                        .iter()
                        .zip(&mut replies)
                        .find(|(id, _)| **id == stream.id())
                    {
                        data.for_each(|bytes| reply.body.extend_from_slice(bytes));
                    }
                }
                Event::Headers {
                    stream_id: stream,
                    head,
                    boundary: Boundary::End,
                    ..
                } => {
                    if let Some((_, reply)) = ids
                        .iter()
                        .zip(&mut replies)
                        .find(|(id, _)| **id == stream.id())
                    {
                        reply.code = Some(
                            Status::from_fields(head.fields())
                                .map_err(Failure::Response)?
                                .code(),
                        );
                    }
                }
                Event::StreamReset {
                    stream_id: stream,
                    error,
                    ..
                } => {
                    if let Some((_, reply)) = ids
                        .iter()
                        .zip(&mut replies)
                        .find(|(id, _)| **id == stream.id())
                    {
                        reply.reset = Some(error);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(replies)
}
