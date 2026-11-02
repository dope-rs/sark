use std::{cell::Cell, rc::Rc};

use dope_test::harness::{observe::VecCellExt as _, signals::Gate};
use o3::buffer;
use sark_grpc::{
    Code, Codec, Status, encode, message,
    server::service::handler::modes::{Messages, Request, Response, StreamingFn, UnaryFn},
};

use crate::common::codec::Bytes;

pub struct Owner {
    pub calls: usize,
    pub report: Rc<Cell<Option<usize>>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.report.set(Some(self.calls));
    }
}

pub struct Echo;

pub struct First;

pub struct FirstOnly<'a>(pub &'a Cell<Vec<(usize, Option<usize>)>>);

pub struct All;

pub struct Count {
    pub decoded: Gate,
}

impl Count {
    fn check(&self, decoded: Vec<u8>) -> Result<Vec<u8>, Status> {
        self.decoded.hit();
        if decoded == b"bad" {
            Err(Status::from_code(Code::InvalidArgument))
        } else {
            Ok(decoded)
        }
    }
}

impl Codec for Count {
    type Encode = Vec<u8>;
    type Decode<'a, C: buffer::raw::Segment>
        = Vec<u8>
    where
        C: 'a;
    type Owned<C: buffer::raw::Segment> = Vec<u8>;

    fn encode<S: encode::Sink>(
        &mut self,
        item: &Self::Encode,
        sink: S,
    ) -> Result<S::Output, Status> {
        sink.try_slices([item.as_slice()])
    }

    fn decode<'a, C: buffer::raw::Segment + 'a>(
        &mut self,
        input: message::View<'a, C>,
    ) -> Result<Self::Decode<'a, C>, Status> {
        self.check(Bytes.decode(input)?)
    }

    fn decode_frame<C: buffer::raw::Segment>(
        &mut self,
        frame: message::Frame<C>,
    ) -> Result<Self::Owned<C>, Status> {
        self.check(Bytes.decode_frame(frame)?)
    }
}

impl UnaryFn<Owner> for Echo {
    type Response = Vec<u8>;
    type Codec = Bytes;

    fn unary<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        context: &mut Owner,
        request: Request<<Self::Codec as Codec>::Decode<'a, S>>,
    ) -> Response<Self::Response> {
        context.calls += 1;
        Response::new(request.body)
    }
}

impl StreamingFn<Owner> for First {
    type Response = Vec<u8>;
    type Codec = Bytes;

    fn stream<P: buffer::raw::Segment>(
        &mut self,
        context: &mut Owner,
        mut request: Request<Messages<'_, '_, Self::Codec, P>>,
    ) -> Response<Self::Response> {
        context.calls += 1;
        match request.body.next() {
            Some(Ok(message)) => Response::new(message),
            Some(Err(status)) => Response::empty(status),
            None => Response::new(Vec::new()),
        }
    }
}

impl StreamingFn for FirstOnly<'_> {
    type Response = Vec<u8>;
    type Codec = Count;

    fn stream<P: buffer::raw::Segment>(
        &mut self,
        _: &mut (),
        mut request: Request<Messages<'_, '_, Self::Codec, P>>,
    ) -> Response<Self::Response> {
        self.0.push(request.body.size_hint());
        match request.body.next() {
            Some(Ok(message)) => Response::new(message),
            Some(Err(status)) => Response::empty(status),
            None => Response::empty(Status::from_code(Code::InvalidArgument)),
        }
    }
}

impl StreamingFn for All {
    type Response = Vec<u8>;
    type Codec = Count;

    fn stream<P: buffer::raw::Segment>(
        &mut self,
        _: &mut (),
        request: Request<Messages<'_, '_, Self::Codec, P>>,
    ) -> Response<Self::Response> {
        let mut last = Vec::new();
        for message in request.body {
            match message {
                Ok(message) => last = message,
                Err(status) => return Response::empty(status),
            }
        }
        Response::new(last)
    }
}
