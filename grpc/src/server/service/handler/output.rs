//! Pull-based server and bidirectional response production.

use core::{mem, task};
use std::marker;

use o3::buffer;
use sark_h2::stream::Id;

use crate::{
    message, metadata,
    protocol::request,
    server::service::{
        self,
        handler::{self, modes, unary},
        reply::{Reply, Write},
        timers,
    },
};

pub struct Response<I> {
    pub metadata: metadata::Metadata,
    pub messages: I,
    pub trailers: metadata::Metadata,
    pub status: crate::Status,
}

impl<I> Response<I> {
    pub fn new(messages: I) -> Self {
        Self {
            metadata: metadata::Metadata::new(),
            messages,
            trailers: metadata::Metadata::new(),
            status: crate::Status::ok(),
        }
    }
}

pub trait StreamFn<C = ()> {
    type Response;
    type Stream: Stream<Response = Self::Response>;
    type Codec: crate::Codec<Encode = Self::Response>;

    fn stream<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        context: &mut C,
        request: modes::Request<<Self::Codec as crate::Codec>::Decode<'a, S>>,
    ) -> Response<Self::Stream>;
}

/// Pull-based response source with a statically selected timer policy.
pub trait Stream {
    type Response;
    type Timer: timers::Policy;

    fn poll(
        &mut self,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
    ) -> Poll<Self::Response>;
}

impl<I: Iterator> Stream for I {
    type Response = I::Item;
    type Timer = timers::Off;

    fn poll(&mut self, _: &mut timers::Output<'_, '_, '_, Self::Timer>) -> Poll<Self::Response> {
        match self.next() {
            Some(message) => Poll::Message(message),
            None => Poll::Complete,
        }
    }
}

#[derive(Clone)]
pub struct BidiRequest<'a> {
    pub stream_id: Id,
    pub head: &'a request::Head,
}

pub enum Poll<T> {
    Message(T),
    Pending,
    Complete,
}

pub trait Bidi<D: crate::Codec>: Stream {
    fn message<'a, S: buffer::raw::Segment + 'a>(&mut self, message: D::Decode<'a, S>);

    /// Consumes the client's trailer block, the final input on this stream.
    fn trailers(&mut self, trailers: metadata::Decoded);
}

pub trait BidiFn<C = ()> {
    type Response;
    type Codec: crate::Codec<Encode = Self::Response>;
    type Stream: Bidi<Self::Codec, Response = Self::Response>;

    fn bidi(&mut self, context: &mut C, request: BidiRequest<'_>) -> Response<Self::Stream>;
}

pub struct Streaming<H: StreamFn<C>, C = ()> {
    handler: H,
    codec: H::Codec,
    context: marker::PhantomData<fn(&mut C)>,
}

impl<H: StreamFn<C>, C> Streaming<H, C> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self {
            handler,
            codec,
            context: marker::PhantomData,
        }
    }
}

pub struct Duplex<H: BidiFn<C>, C = ()> {
    handler: H,
    codec: H::Codec,
    context: marker::PhantomData<fn(&mut C)>,
}

impl<H: BidiFn<C>, C> Duplex<H, C> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self {
            handler,
            codec,
            context: marker::PhantomData,
        }
    }
}

struct Producer<S: Stream> {
    source: S,
    pending: Option<S::Response>,
    trailers: metadata::Metadata,
    status: Option<crate::Status>,
}

impl<S: Stream> Producer<S> {
    fn new(source: S, trailers: metadata::Metadata, status: crate::Status) -> Self {
        Self {
            source,
            pending: None,
            trailers,
            status: Some(status),
        }
    }

    fn turn<C, W: Write>(
        &mut self,
        codec: &mut C,
        output: &mut timers::Output<'_, '_, '_, S::Timer>,
        reply: &mut Reply<'_, W>,
    ) -> handler::OutputState
    where
        C: crate::Codec<Encode = S::Response>,
    {
        let message = match self.pending.as_ref() {
            Some(message) => message,
            None => {
                match reply.poll_ready() {
                    Ok(task::Poll::Ready(())) => {}
                    Ok(task::Poll::Pending) => return handler::OutputState::Blocked,
                    Err(status) => {
                        reply.finish_with(status);
                        return handler::OutputState::Complete;
                    }
                }
                let message = match Stream::poll(&mut self.source, output) {
                    Poll::Message(message) => message,
                    Poll::Pending => return handler::OutputState::Pending,
                    Poll::Complete => {
                        *reply.trailers_mut() = mem::take(&mut self.trailers);
                        let status = match self.status.take() {
                            Some(status) => status,
                            None => crate::Status::ok(),
                        };
                        reply.finish_with(status);
                        return handler::OutputState::Complete;
                    }
                };
                self.pending.insert(message)
            }
        };
        match reply.try_encode(codec, message) {
            Ok(task::Poll::Ready(())) => {
                self.pending.take();
                handler::OutputState::Produced
            }
            Ok(task::Poll::Pending) => handler::OutputState::Blocked,
            Err(status) => {
                self.pending.take();
                reply.finish_with(status);
                handler::OutputState::Complete
            }
        }
    }
}

#[doc(hidden)]
pub struct Call<S: Stream, D> {
    producer: Option<Producer<S>>,
    codec: marker::PhantomData<fn() -> D>,
}

impl<S: Stream, D> Default for Call<S, D> {
    fn default() -> Self {
        Self {
            producer: None,
            codec: marker::PhantomData,
        }
    }
}

impl<H: StreamFn<C>, C> handler::Handler<C> for Streaming<H, C> {
    type Call = Call<H::Stream, ()>;
    type Timer = <H::Stream as Stream>::Timer;

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        context: &mut C,
        call: &mut Self::Call,
        request: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        let request = match unary::decode(&mut self.codec, request) {
            Ok(request) => request,
            Err(status) => {
                response.finish_with(status);
                return handler::RequestState::Complete;
            }
        };
        let output = self.handler.stream(context, request);
        *response.metadata_mut() = output.metadata;
        call.producer = Some(Producer::new(
            output.messages,
            output.trailers,
            output.status,
        ));
        handler::RequestState::Produce
    }

    fn output<W: Write>(
        &mut self,
        _: &mut C,
        call: &mut Self::Call,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
        reply: &mut Reply<'_, W>,
    ) -> handler::OutputState {
        let Some(producer) = call.producer.as_mut() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return handler::OutputState::Complete;
        };
        producer.turn(&mut self.codec, output, reply)
    }
}

impl<H: BidiFn<C>, C> handler::Handler<C> for Duplex<H, C> {
    type Call = Call<H::Stream, H::Codec>;
    type Timer = <H::Stream as Stream>::Timer;

    fn start<W: Write>(
        &mut self,
        context: &mut C,
        stream: &mut service::Stream,
        head: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> handler::Start<Self::Call> {
        let output = self.handler.bidi(
            context,
            BidiRequest {
                stream_id: stream.stream_id(),
                head,
            },
        );
        *reply.metadata_mut() = output.metadata;
        handler::Start::live(Call {
            producer: Some(Producer::new(
                output.messages,
                output.trailers,
                output.status,
            )),
            codec: marker::PhantomData,
        })
        .producing()
    }

    fn message<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut C,
        call: &mut Self::Call,
        _: &mut service::Stream,
        message: message::Frame<S>,
        reply: &mut Reply<'_, W>,
    ) {
        let decoded = match crate::Codec::decode(&mut self.codec, message.view()) {
            Ok(decoded) => decoded,
            Err(status) => {
                reply.finish_with(status);
                return;
            }
        };
        let Some(producer) = call.producer.as_mut() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return;
        };
        <H::Stream as Bidi<H::Codec>>::message(&mut producer.source, decoded);
    }

    fn trailers<W: Write>(
        &mut self,
        _: &mut C,
        call: &mut Self::Call,
        _: &mut service::Stream,
        trailers: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        let Some(producer) = call.producer.as_mut() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return;
        };
        <H::Stream as Bidi<H::Codec>>::trailers(&mut producer.source, trailers);
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut C,
        _: &mut Self::Call,
        _: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        response.finish_with(crate::Status::from_code(crate::Code::Internal));
        handler::RequestState::Complete
    }

    fn output<W: Write>(
        &mut self,
        _: &mut C,
        call: &mut Self::Call,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
        reply: &mut Reply<'_, W>,
    ) -> handler::OutputState {
        let Some(producer) = call.producer.as_mut() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return handler::OutputState::Complete;
        };
        producer.turn(&mut self.codec, output, reply)
    }
}
