use o3::buffer;
use sark_grpc::{
    message, metadata,
    protocol::request,
    server::{
        service,
        service::{
            handler,
            reply::{Reply, Write},
            timers,
        },
    },
};

/// A handler paired with the state supplied to its test calls.
pub struct State<H, C> {
    handler: H,
    state: C,
}

impl<H, C> State<H, C> {
    pub fn new(handler: H, state: C) -> Self {
        Self { handler, state }
    }
}

impl<H: handler::Handler<C>, C> handler::Handler for State<H, C> {
    type Call = H::Call;
    type Timer = H::Timer;

    fn start<W: Write>(
        &mut self,
        &mut (): &mut (),
        stream: &mut service::Stream,
        head: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> handler::Start<Self::Call> {
        self.handler.start(&mut self.state, stream, head, reply)
    }

    fn message<S: buffer::raw::Segment, W: Write>(
        &mut self,
        &mut (): &mut (),
        call: &mut Self::Call,
        stream: &mut service::Stream,
        message: message::Frame<S>,
        reply: &mut Reply<'_, W>,
    ) {
        self.handler
            .message(&mut self.state, call, stream, message, reply);
    }

    fn trailers<W: Write>(
        &mut self,
        &mut (): &mut (),
        call: &mut Self::Call,
        stream: &mut service::Stream,
        trailers: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        self.handler
            .trailers(&mut self.state, call, stream, trailers, reply);
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        &mut (): &mut (),
        call: &mut Self::Call,
        request: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        self.handler
            .request(&mut self.state, call, request, response)
    }

    fn output<W: Write>(
        &mut self,
        &mut (): &mut (),
        call: &mut Self::Call,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
        reply: &mut Reply<'_, W>,
    ) -> handler::OutputState {
        self.handler.output(&mut self.state, call, output, reply)
    }
}
