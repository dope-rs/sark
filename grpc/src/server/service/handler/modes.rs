use std::{iter, marker};

use o3::{buffer, buffer::storage};
use sark_h2::stream::Id;

use crate::{
    metadata,
    protocol::request,
    server::service::{
        self,
        handler::{self, unary},
        messages,
        reply::Write,
        timers,
    },
};

pub struct Request<T> {
    pub stream_id: Id,
    pub head: request::Head,
    pub body: T,
    pub trailers: metadata::Decoded,
}

pub struct Response<T> {
    pub metadata: metadata::Metadata,
    pub message: Option<T>,
    pub trailers: metadata::Metadata,
    pub status: crate::Status,
}

impl<T> Response<T> {
    pub fn new(message: T) -> Self {
        Self {
            metadata: metadata::Metadata::new(),
            message: Some(message),
            trailers: metadata::Metadata::new(),
            status: crate::Status::ok(),
        }
    }

    pub fn empty(status: crate::Status) -> Self {
        Self {
            metadata: metadata::Metadata::new(),
            message: None,
            trailers: metadata::Metadata::new(),
            status,
        }
    }

    pub(in crate::server) fn apply<C: crate::Codec<Encode = T>, W: Write>(
        self,
        codec: &mut C,
        response: &mut service::Response<'_, W>,
    ) {
        *response.metadata_mut() = self.metadata;
        *response.trailers_mut() = self.trailers;
        response.finish_with(self.status);
        if let Some(message) = self.message {
            response.push_encoded(codec, &message);
        }
    }
}

pub trait UnaryFn<C = ()> {
    type Response;
    type Codec: crate::Codec<Encode = Self::Response>;

    fn unary<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        context: &mut C,
        request: Request<<Self::Codec as crate::Codec>::Decode<'a, S>>,
    ) -> Response<Self::Response>;
}

/// Decoded-on-consumption view over a bounded retained request chain.
pub struct Messages<'frames, 'codec, D: crate::Codec, P: buffer::raw::Segment = storage::Shared> {
    frames: messages::Iter<'frames, P>,
    codec: &'codec mut D,
}

impl<'frames, 'codec, D: crate::Codec, P: buffer::raw::Segment> Messages<'frames, 'codec, D, P> {
    fn new(messages: messages::Messages<'frames, P>, codec: &'codec mut D) -> Self {
        Self {
            frames: messages.into_iter(),
            codec,
        }
    }
}

impl<'frames, D: crate::Codec, P: buffer::raw::Segment + 'frames> Iterator
    for Messages<'frames, '_, D, P>
{
    type Item = Result<D::Decode<'frames, P>, crate::Status>;

    fn next(&mut self) -> Option<Self::Item> {
        self.frames
            .next()
            .map(|frame| crate::Codec::decode(self.codec, frame.view()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.frames.size_hint()
    }
}

impl<'a, D: crate::Codec, P: buffer::raw::Segment + 'a> iter::FusedIterator
    for Messages<'a, '_, D, P>
{
}

pub trait StreamingFn<C = ()> {
    type Response;
    type Codec: crate::Codec<Encode = Self::Response>;

    fn stream<P: buffer::raw::Segment>(
        &mut self,
        context: &mut C,
        request: Request<Messages<'_, '_, Self::Codec, P>>,
    ) -> Response<Self::Response>;
}

pub struct Unary<H, C = ()>
where
    H: UnaryFn<C>,
{
    handler: H,
    codec: H::Codec,
    context: marker::PhantomData<fn(&mut C)>,
}

pub struct Streaming<H, C = ()>
where
    H: StreamingFn<C>,
{
    handler: H,
    codec: H::Codec,
    context: marker::PhantomData<fn(&mut C)>,
}

impl<H: UnaryFn<C>, C> Unary<H, C> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self {
            handler,
            codec,
            context: marker::PhantomData,
        }
    }
}

impl<H: StreamingFn<C>, C> Streaming<H, C> {
    pub fn new(handler: H, codec: H::Codec) -> Self {
        Self {
            handler,
            codec,
            context: marker::PhantomData,
        }
    }
}

impl<H: UnaryFn<C>, C> handler::Handler<C> for Unary<H, C> {
    type Call = ();
    type Timer = timers::Off;

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        context: &mut C,
        _: &mut Self::Call,
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
        self.handler
            .unary(context, request)
            .apply(&mut self.codec, response);
        handler::RequestState::Complete
    }
}

impl<H: StreamingFn<C>, C> handler::Handler<C> for Streaming<H, C> {
    type Call = ();
    type Timer = timers::Off;

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        context: &mut C,
        _: &mut Self::Call,
        request: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        let service::Request {
            stream_id: stream,
            head,
            messages,
            trailers,
        } = request;
        self.handler
            .stream(
                context,
                Request {
                    stream_id: stream,
                    head,
                    body: Messages::new(messages, &mut self.codec),
                    trailers,
                },
            )
            .apply(&mut self.codec, response);
        handler::RequestState::Complete
    }
}
