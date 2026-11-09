use std::{convert, marker};

use dope::core::driver::{self, schedule::app::Work};
use o3::buffer::resident;
use sark_core::manifold::server::app;

use crate::{
    message, metadata,
    protocol::request,
    server::{
        call,
        runtime::{
            policy::{self, RequestState, TaskState},
            state,
        },
        service::{
            self,
            handler::{self, Handler},
            reply::{Reply, Write},
            timers,
        },
    },
};

pub struct Policy<H, T> {
    app: H,
    target: marker::PhantomData<fn(T) -> T>,
}

impl<H, T> Policy<H, T> {
    pub(in crate::server) const fn new(app: H) -> Self {
        Self {
            app,
            target: marker::PhantomData,
        }
    }
}

impl<'d, H, O, T> policy::Dispatch<'d, O> for Policy<H, T>
where
    H: Handler,
    H::Timer: state::Timing<'d>,
    O: Copy + Eq + 'd,
    T: Copy,
{
    type Call = H::Call;
    type Completion = convert::Infallible;
    type State = ();
    type Target = T;
    type Timer = H::Timer;

    const TASKS: bool = false;

    fn start<W: Write>(
        &mut self,
        _: &mut app::Turn<'_, '_, '_, 'd, O>,
        _: T,
        stream: &mut service::Stream,
        head: request::Head,
        reply: &mut Reply<'_, W>,
    ) -> call::Opened<call::Handler<H::Call, ()>> {
        let start = self
            .app
            .start(&mut (), stream, &head, reply)
            .map(|call| call::Handler::new(call, ()));
        call::Opened::retained(start, head)
    }

    fn message<W: Write>(
        &mut self,
        _: O,
        _: T,
        call: &mut call::Handler<H::Call, ()>,
        stream: &mut service::Stream,
        frame: message::Frame<resident::Shared<'d>>,
        reply: &mut Reply<'_, W>,
    ) -> policy::Rearm {
        self.app
            .message(&mut (), call.split().0, stream, frame, reply);
        policy::Rearm::Read
    }

    fn trailers<W: Write>(
        &mut self,
        _: O,
        _: T,
        call: &mut call::Handler<H::Call, ()>,
        stream: &mut service::Stream,
        tail: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        self.app
            .trailers(&mut (), call.split().0, stream, tail, reply);
    }

    fn output<W: Write>(
        &mut self,
        _: O,
        _: T,
        call: &mut call::Handler<H::Call, ()>,
        output: &mut timers::Output<'_, '_, '_, H::Timer>,
        reply: &mut Reply<'_, W>,
    ) -> policy::Output {
        policy::Output::from_service(self.app.output(&mut (), call.split().0, output, reply))
    }

    fn request<W: Write>(
        &mut self,
        _: &mut app::Turn<'_, '_, '_, 'd, O>,
        _: T,
        call: &mut call::Handler<H::Call, ()>,
        input: service::Request<'_, resident::Shared<'d>>,
        output: &mut service::Response<'_, W>,
    ) -> RequestState {
        RequestState::from_service(self.app.request(&mut (), call.split().0, input, output))
    }

    fn has_tasks(&self, _: O) -> bool {
        false
    }

    fn has_done(&self, _: O) -> bool {
        false
    }

    fn drive_tasks(
        &mut self,
        _: O,
        _: Work<'_, 'd>,
        _: &mut driver::Context<'_, 'd>,
    ) -> TaskState {
        TaskState::Idle
    }

    fn wake_tasks(&mut self, _: O) -> TaskState {
        TaskState::Idle
    }

    fn pop_done(&mut self, _: O) -> Option<(T, convert::Infallible)> {
        None
    }

    fn publish<W: Write>(
        &mut self,
        _: O,
        _: T,
        _: &mut call::Handler<H::Call, ()>,
        done: convert::Infallible,
        _: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        match done {}
    }
}

const _: () = assert!(size_of::<Policy<(), ()>>() == 0);
