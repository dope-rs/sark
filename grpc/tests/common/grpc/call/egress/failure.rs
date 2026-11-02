use std::{cell::Cell, task::Poll};

use o3::buffer;
use sark_grpc::{
    Code, Codec, Status, encode, message,
    protocol::request,
    server::service::{
        self,
        handler::{self, Handler},
        reply::{Reply, Write},
        timers,
    },
};

pub struct Active<'a>(pub &'a Cell<u32>);

impl Handler for Active<'_> {
    type Call = ();
    type Timer = timers::Off;

    fn start<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut service::Stream,
        _: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> handler::Start<()> {
        self.0.set(self.0.get() + 1);
        reply.push_message(b"sent".to_vec());
        handler::Start::live(())
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut (),
        _: service::Request<'_, S>,
        output: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        output.finish_with(Status::ok());
        handler::RequestState::Complete
    }
}

pub struct Encoded<'a>(pub &'a Cell<[Option<Code>; 2]>);

impl Codec for Encoded<'_> {
    type Encode = ();
    type Decode<'a, C: buffer::raw::Segment>
        = ()
    where
        C: 'a;
    type Owned<C: buffer::raw::Segment> = ();

    fn encode<S: encode::Sink>(&mut self, _: &(), sink: S) -> Result<S::Output, Status> {
        let _ = sink.try_slices([b"bad".as_slice()])?;
        Err(Status::from_code(Code::DataLoss))
    }

    fn decode<'a, C: buffer::raw::Segment + 'a>(
        &mut self,
        _: message::View<'a, C>,
    ) -> Result<(), Status> {
        Ok(())
    }

    fn decode_frame<C: buffer::raw::Segment>(
        &mut self,
        _: message::Frame<C>,
    ) -> Result<(), Status> {
        Ok(())
    }
}

impl Handler for Encoded<'_> {
    type Call = Option<bool>;
    type Timer = timers::Off;

    fn start<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut service::Stream,
        head: &request::Head,
        _: &mut Reply<'_, W>,
    ) -> handler::Start<Self::Call> {
        handler::Start::live(Some(head.path() == b"/capacity/held")).producing()
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut Self::Call,
        _: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        response.finish_with(Status::ok());
        handler::RequestState::Complete
    }

    fn output<W: Write>(
        &mut self,
        _: &mut (),
        call: &mut Self::Call,
        _: &mut timers::Output<'_, '_, '_, timers::Off>,
        reply: &mut Reply<'_, W>,
    ) -> handler::OutputState {
        let Some(failed) = call.take() else {
            reply.finish_with(Status::ok());
            return handler::OutputState::Complete;
        };
        if !failed {
            reply.push_message(b"kept".to_vec());
        }
        match reply.try_encode(self, &()) {
            Ok(Poll::Ready(())) => handler::OutputState::Produced,
            Ok(Poll::Pending) => {
                *call = Some(failed);
                handler::OutputState::Blocked
            }
            Err(status) => {
                let mut codes = self.0.get();
                codes[usize::from(!failed)] = Some(status.code());
                self.0.set(codes);
                if failed {
                    reply.finish_with(status);
                    handler::OutputState::Complete
                } else {
                    handler::OutputState::Produced
                }
            }
        }
    }
}
