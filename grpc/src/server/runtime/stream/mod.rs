//! Streaming tasks over one exact driver-domain scheduler.
use core::convert::Infallible;
use dope::{
    core::driver::{self, route::KeyTag, schedule::app::Work},
    manifold::network::listener::conn::Id,
};
use dope_fiber::task::storage::set::{self, group};
use o3::buffer::resident;
use runtime::{
    executor, policy,
    schedule::{self, start, wake},
    state::active,
};
use sark_core::manifold::server::drain::Progress;
use sark_h2::server::app;
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

type Fiber<'life, 'd, H> = stream::Fiber<
    'd,
    <H as stream::Handler>::Task<'life, 'd>,
    <H as stream::Handler>::Request<'d>,
    <H as stream::Handler>::Response,
    <H as stream::Handler>::Codec,
>;

/// Fixed-capacity streaming execution over one exact driver-domain scheduler.
pub struct Policy<'s, 'r, 'life, 'd: 'life, H>
where
    H: stream::Handler + 'life,
{
    tasks: executor::Tasks<'s, 'd, Domain, Fiber<'life, 'd, H>, active::Entry<'r>>,
    user: &'life H,
}

impl<'s, 'r, 'life, 'd: 'life, H> Policy<'s, 'r, 'life, 'd, H>
where
    H: stream::Handler + 'life,
{
    pub(in crate::server) const fn new(
        user: &'life H,
        tasks: executor::Tasks<'s, 'd, Domain, Fiber<'life, 'd, H>, active::Entry<'r>>,
    ) -> Self {
        Self { tasks, user }
    }

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
}

impl<'s, 'r, 'life, 'd: 'life, H> policy::Dispatch<'d, Id<'d, KeyTag<0>>>
    for Policy<'s, 'r, 'life, 'd, H>
where
    H: stream::Handler + 'life,
{
    type Call = ();
    type Completion = Infallible;
    type State = Option<group::Id<'s, 'd, Domain>>;
    type Storage<T> = T;
    type Target = active::Entry<'r>;
    type Timer = timers::Off;

    const TASKS: bool = true;
    const BUFFERED: bool = false;

    fn start<W: Write>(
        &mut self,
        turn: &mut app::Turn<'_, '_, '_, 'd, Id<'d, KeyTag<0>>>,
        target: active::Entry<'r>,
        stream: &mut service::Stream,
        head: request::Head,
        reply: &mut Reply<'_, W>,
    ) -> Opened<Handler<(), Self::State>> {
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
                return Opened::live(Handler::new((), None));
            }
        };
        let shape = route.shape();
        let timer = driver.driver_ref().timer();
        let call = self.user.stream(input, route);
        *reply.metadata_mut() = call.metadata;
        let fiber = stream::Fiber::new(call.task, codec, shape, timer);
        let task = match executor::start_with(
            &mut self.tasks,
            fiber,
            owner,
            target,
            schedule::Context {
                parent,
                work,
                driver,
            },
            |entry, fiber| Self::signal(entry, fiber.exchange().take_events()),
        ) {
            Ok(start::Start::Ready(never)) => match never {},
            Ok(start::Start::Pending(task)) => task,
            Err(error) => {
                reply.finish_with(error.into());
                return Opened::live(Handler::new((), None));
            }
        };
        Opened::producing(Handler::new((), Some(task)))
    }

    fn message<W: Write>(
        &mut self,
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        state: &mut Handler<(), Self::State>,
        _: &mut service::Stream,
        frame: message::Frame<resident::Shared<'d>>,
        reply: &mut Reply<'_, W>,
    ) -> policy::Rearm {
        let (_, state) = state.split();
        let Some(task) = state.as_ref() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return policy::Rearm::Stop;
        };
        let result = self
            .tasks
            .access(task, |fiber| fiber.exchange().push(frame));
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
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        state: &mut Handler<(), Self::State>,
        _: &mut service::Stream,
        tail: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        let (_, state) = state.split();
        let Some(task) = state.as_ref() else {
            reply.finish_with(crate::Status::from_code(crate::Code::Internal));
            return;
        };
        let result = self
            .tasks
            .access(task, |fiber| fiber.exchange().close(tail));
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
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        state: &mut Handler<(), Self::State>,
        _: &mut timers::Output<'_, '_, '_, timers::Off>,
        reply: &mut Reply<'_, W>,
    ) -> policy::Output {
        let (_, state) = state.split();
        let Some(task) = state.as_ref() else {
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
        let output = self
            .tasks
            .access(task, |fiber| fiber.exchange().drive(ready, reply));
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
        _: &mut app::Turn<'_, '_, '_, 'd, Id<'d, KeyTag<0>>>,
        _: active::Entry<'r>,
        _: &mut Handler<(), Self::State>,
        _: service::Request<'_, resident::Shared<'d>>,
        output: &mut service::Response<'_, W>,
    ) -> policy::RequestState {
        output.finish_with(crate::Status::from_code(crate::Code::Internal));
        policy::RequestState::Complete
    }

    fn has_tasks(&self, owner: Id<'d, KeyTag<0>>) -> bool {
        matches!(self.tasks.work_for(owner), set::Work::Pending)
    }

    fn has_done(&self, _: Id<'d, KeyTag<0>>) -> bool {
        false
    }

    fn drive_tasks(
        &mut self,
        owner: Id<'d, KeyTag<0>>,
        work: Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress {
        executor::drive_with(&mut self.tasks, owner, work, driver, |entry, fiber| {
            Self::signal(entry, fiber.exchange().take_events());
        })
    }

    fn wake_tasks(&mut self) {
        self.tasks.wake_next();
    }

    fn pop_done(&mut self, _: Id<'d, KeyTag<0>>) -> Option<(active::Entry<'r>, Self::Completion)> {
        None
    }

    fn publish<W: Write>(
        &mut self,
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        _: &mut Handler<(), Self::State>,
        done: Self::Completion,
        _: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        match done {}
    }

    fn cancel(
        &mut self,
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        state: &mut Handler<(), Self::State>,
    ) {
        if let Some(task) = state.split().1.take() {
            self.tasks.discard(task);
        }
    }
}
