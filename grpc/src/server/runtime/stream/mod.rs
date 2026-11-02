//! Streaming tasks over one exact driver-domain scheduler.
use core::convert::Infallible;
use std::marker;

use dope::core::driver::{retained, schedule::app::Work};
use manifold::task::schedule::{self, start, wake};
use o3::buffer::resident;
use runtime::{Call, policy, state::active};
use sark_core::{manifold, manifold::server::app};
use stream::Route as _;

use crate::{
    message, metadata,
    protocol::request,
    server::{
        call::{Handler, Opened},
        runtime,
        service::{
            self, handler,
            reply::{Reply, Write},
            task::stream::{self, exchange},
            timers,
        },
    },
};

mod domain;

/// Route domain for streaming server tasks.
pub use domain::Domain;

type Target<'r, 'd, O> = marker::PhantomData<fn() -> *mut (O, active::Entry<'r>, &'d ())>;

/// Fixed-capacity streaming execution over one exact driver-domain scheduler.
pub struct Policy<'r, 'life, 'd: 'life, H, S, O> {
    tasks: S,
    user: &'life H,
    target: Target<'r, 'd, O>,
}

impl<'r, 'life, 'd: 'life, H, S, O> Policy<'r, 'life, 'd, H, S, O> {
    pub(in crate::server) const fn new(user: &'life H, tasks: S) -> Self {
        Self {
            tasks,
            user,
            target: marker::PhantomData,
        }
    }
}

impl<'r, 'life, 'd: 'life, H, S, O> Policy<'r, 'life, 'd, H, S, O>
where
    H: stream::Handler + 'life,
    S: schedule::raw::Scheduler<
            'd,
            Domain,
            O,
            active::Entry<'r>,
            stream::Fiber<'d, H::Task<'life, 'd>, H::Request<'d>, H::Response, H::Codec>,
            Infallible,
        >,
    O: Copy + Eq + 'd,
{
    fn signal(entry: active::Entry<'r>, events: exchange::Events) {
        match events {
            exchange::Events::None => {}
            exchange::Events::Input => entry.app(),
            exchange::Events::Output => entry.output(),
            exchange::Events::Both => {
                entry.app();
                entry.output();
            }
        }
    }

    fn task_state(&self, owner: O) -> policy::TaskState {
        if self.tasks.has_work_for(owner) {
            policy::TaskState::Ready
        } else {
            policy::TaskState::Idle
        }
    }
}

impl<'r, 'life, 'd: 'life, H, S, O> policy::Dispatch<'d, O> for Policy<'r, 'life, 'd, H, S, O>
where
    H: stream::Handler + 'life,
    S: schedule::raw::Scheduler<
            'd,
            Domain,
            O,
            active::Entry<'r>,
            stream::Fiber<'d, H::Task<'life, 'd>, H::Request<'d>, H::Response, H::Codec>,
            Infallible,
        >,
    O: Copy + Eq + 'd,
{
    type Call = ();
    type Completion = Infallible;
    type State = Call<S::Pending>;
    type Target = active::Entry<'r>;
    type Timer = timers::Off;

    const TASKS: bool = true;

    fn start<W: Write>(
        &mut self,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        target: active::Entry<'r>,
        stream: &mut service::Stream,
        head: request::Head,
        reply: &mut Reply<'_, W>,
    ) -> Opened<Handler<(), Call<S::Pending>>> {
        let owner = turn.owner();
        let parent = turn.wake();
        let work = turn.work();
        let driver = turn.driver();
        let input = stream::Request {
            stream_id: stream.stream_id(),
            head,
        };
        let (codec, route) = match self.user.codec(&input) {
            Ok(start) => start,
            Err(status) => {
                reply.finish_with(status);
                return Opened::live(Handler::new((), Call::default()));
            }
        };
        let shape = route.shape();
        let timer = driver.driver_ref().timer();
        let call = self.user.stream(input, route);
        *reply.metadata_mut() = call.metadata;
        let fiber = stream::Fiber::new(call.task, codec, shape, timer);
        let task = match self.tasks.start_with(
            fiber,
            owner,
            target,
            schedule::Context {
                parent,
                work,
                driver,
            },
            |entry, fiber| Self::signal(entry, fiber.take_events()),
        ) {
            Ok(start::Start::Ready(never)) => match never {},
            Ok(start::Start::Pending(task)) => task,
            Err(error) => {
                reply.finish_with(error.into());
                return Opened::live(Handler::new((), Call::default()));
            }
        };
        Opened::producing(Handler::new((), Call::new(task)))
    }

    fn message<W: Write>(
        &mut self,
        _: O,
        _: active::Entry<'r>,
        state: &mut Handler<(), Call<S::Pending>>,
        _: &mut service::Stream,
        frame: message::Frame<resident::Shared<'d>>,
        reply: &mut Reply<'_, W>,
    ) -> policy::Rearm {
        let (_, state) = state.split();
        let Some(task) = state.task() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return policy::Rearm::Stop;
        };
        let result = self.tasks.access(task, |fiber| {
            fiber.push_with(|codec| crate::Codec::decode_frame(codec, frame))
        });
        let Some(result) = result else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return policy::Rearm::Stop;
        };
        match result {
            Ok(()) => {}
            Err(exchange::PushError::Decode(status)) => {
                reply.finish_with(status);
                return policy::Rearm::Stop;
            }
            Err(exchange::PushError::Busy) => {
                reply.finish_with(crate::Status::from_code(crate::Code::Internal));
                return policy::Rearm::Stop;
            }
            Err(exchange::PushError::Cardinality) => {
                reply.finish_with(crate::Status::from_code(crate::Code::InvalidArgument));
                return policy::Rearm::Stop;
            }
        }
        if let Err(wake::Error::Missing | wake::Error::Inactive) = self.tasks.try_wake(task) {
            reply.finish_with(crate::Status::from_code(crate::Code::Cancelled));
        }
        policy::Rearm::Stop
    }

    fn trailers<W: Write>(
        &mut self,
        _: O,
        _: active::Entry<'r>,
        state: &mut Handler<(), Call<S::Pending>>,
        _: &mut service::Stream,
        tail: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        let (_, state) = state.split();
        let Some(task) = state.task() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return;
        };
        let result = self.tasks.access(task, |fiber| fiber.close(tail));
        let Some(result) = result else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return;
        };
        match result {
            Ok(()) => {}
            Err(exchange::CloseError::Closed | exchange::CloseError::Empty) => {
                reply.finish_with(crate::Status::from_code(crate::Code::InvalidArgument));
                return;
            }
        }
        if let Err(wake::Error::Missing | wake::Error::Inactive) = self.tasks.try_wake(task) {
            reply.finish_with(crate::Status::from_code(crate::Code::Cancelled));
        }
    }

    fn output<W: Write>(
        &mut self,
        _: O,
        _: active::Entry<'r>,
        state: &mut Handler<(), Call<S::Pending>>,
        _: &mut timers::Output<'_, '_, '_, timers::Off>,
        reply: &mut Reply<'_, W>,
    ) -> policy::Output {
        let (_, state) = state.split();
        let Some(task) = state.task() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return policy::Output::Complete;
        };
        let ready = match reply.poll_ready() {
            Ok(ready) => ready,
            Err(status) => {
                reply.finish_with(status);
                return policy::Output::Complete;
            }
        };
        let output = self.tasks.access(task, |fiber| {
            fiber.drive(ready, |codec, value| reply.try_encode(codec, value))
        });
        let Some(output) = output else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return policy::Output::Complete;
        };
        match output {
            exchange::Output::Produced => match self.tasks.try_wake(task) {
                Ok(()) => policy::Output::Produced,
                Err(error) => policy::Output::Wake(error),
            },
            exchange::Output::Blocked => policy::Output::Blocked,
            exchange::Output::Pending => policy::Output::Pending,
            exchange::Output::Excess | exchange::Output::Missing => {
                reply.finish_with(crate::Status::from_code(crate::Code::Internal));
                policy::Output::Complete
            }
            exchange::Output::Error(status) => {
                reply.finish_with(status);
                policy::Output::Complete
            }
            exchange::Output::Finish(finish) => {
                *reply.trailers_mut() = finish.trailers;
                reply.finish_with(finish.status);
                policy::Output::Complete
            }
        }
    }

    fn request<W: Write>(
        &mut self,
        _: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        _: active::Entry<'r>,
        _: &mut Handler<(), Call<S::Pending>>,
        _: service::Request<'_, resident::Shared<'d>>,
        output: &mut service::Response<'_, W>,
    ) -> policy::RequestState {
        output.finish_with(crate::Status::from_code(crate::Code::Internal));
        policy::RequestState::Complete
    }

    fn has_tasks(&self, owner: O) -> bool {
        self.tasks.has_work_for(owner)
    }

    fn has_done(&self, _: O) -> bool {
        false
    }

    fn drive_tasks(
        &mut self,
        owner: O,
        work: Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> policy::TaskState {
        self.tasks.drive_with(owner, work, driver, |entry, fiber| {
            Self::signal(entry, fiber.take_events());
        });
        self.task_state(owner)
    }

    fn wake_tasks(&mut self, owner: O) -> policy::TaskState {
        self.tasks.wake_next();
        self.task_state(owner)
    }

    fn pop_done(&mut self, _: O) -> Option<(active::Entry<'r>, Self::Completion)> {
        None
    }

    fn publish<W: Write>(
        &mut self,
        _: O,
        _: active::Entry<'r>,
        _: &mut Handler<(), Call<S::Pending>>,
        done: Self::Completion,
        _: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        match done {}
    }

    fn cancel(&mut self, _: O, _: active::Entry<'r>, state: &mut Handler<(), Call<S::Pending>>) {
        state.split().1.cancel(|task| self.tasks.cancel(task))
    }
}
