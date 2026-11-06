use dope::core::driver::{self, schedule::app::Work};
use o3::buffer::resident;
use sark_core::manifold::server::app;

use crate::server::runtime::schedule::wake;

use crate::{
    Reason, message, metadata,
    protocol::request,
    server::{
        call,
        runtime::state,
        service::{
            self, handler,
            reply::{Reply, Write},
            timers,
        },
    },
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RequestState {
    Complete,
    Produce,
    Pending,
}

impl RequestState {
    pub(in crate::server::runtime) fn from_service(state: handler::RequestState) -> Self {
        match state {
            handler::RequestState::Complete => Self::Complete,
            handler::RequestState::Produce => Self::Produce,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Output {
    Produced,
    Blocked,
    Pending,
    Complete,
    Wake(wake::Error),
}

impl Output {
    pub(in crate::server::runtime) fn from_service(state: handler::OutputState) -> Self {
        match state {
            handler::OutputState::Produced => Self::Produced,
            handler::OutputState::Blocked => Self::Blocked,
            handler::OutputState::Pending => Self::Pending,
            handler::OutputState::Complete => Self::Complete,
        }
    }

    pub(in crate::server::runtime) fn resolve<W: Write>(
        self,
        reply: &mut Reply<'_, W>,
    ) -> Result<handler::OutputState, crate::Status> {
        let phase = match self {
            Self::Produced => handler::OutputState::Produced,
            Self::Blocked => handler::OutputState::Blocked,
            Self::Pending => handler::OutputState::Pending,
            Self::Complete => handler::OutputState::Complete,
            Self::Wake(wake::Error::Missing | wake::Error::Inactive) => {
                return Err(crate::Status::from_code(crate::Code::Cancelled));
            }
        };
        if reply.is_failed() {
            return Ok(handler::OutputState::Complete);
        }
        let valid = match phase {
            handler::OutputState::Produced => reply.len() == 1 && !reply.is_finished(),
            handler::OutputState::Blocked | handler::OutputState::Pending => {
                reply.is_empty() && !reply.is_finished()
            }
            handler::OutputState::Complete => reply.is_empty(),
        };
        if !valid {
            return Err(crate::Status::new(
                crate::Code::Internal,
                Reason::ResponsePerTurn,
            ));
        }
        if phase == handler::OutputState::Complete && !reply.is_finished() {
            reply.finish_with(crate::Status::ok());
        }
        Ok(phase)
    }
}

const _: () = assert!(size_of::<Output>() == 1);

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Rearm {
    Stop,
    Read,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum TaskState {
    Idle,
    Ready,
}

pub trait Dispatch<'d, O>
where
    O: Copy + Eq + 'd,
{
    type Call: Default;
    type Completion;
    type State: Default;
    type Target: Copy;
    type Timer: state::Timing<'d>;

    const TASKS: bool;

    fn start<W: Write>(
        &mut self,
        _: &mut app::Turn<'_, '_, '_, 'd, O>,
        _: Self::Target,
        _: &mut service::Stream,
        head: request::Head,
        _: &mut Reply<'_, W>,
    ) -> call::Opened<call::Handler<Self::Call, Self::State>> {
        let start = handler::Start::buffered(Self::Call::default())
            .map(|call| call::Handler::new(call, Self::State::default()));
        call::Opened::retained(start, head)
    }

    fn message<W: Write>(
        &mut self,
        _: O,
        _: Self::Target,
        _: &mut call::Handler<Self::Call, Self::State>,
        _: &mut service::Stream,
        _: message::Frame<resident::Shared<'d>>,
        reply: &mut Reply<'_, W>,
    ) -> Rearm {
        reply.finish_with(crate::Status::from_code(crate::Code::Internal));
        Rearm::Read
    }

    fn trailers<W: Write>(
        &mut self,
        _: O,
        _: Self::Target,
        _: &mut call::Handler<Self::Call, Self::State>,
        _: &mut service::Stream,
        _: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        reply.finish_with(crate::Status::from_code(crate::Code::Internal));
    }

    fn output<W: Write>(
        &mut self,
        _: O,
        _: Self::Target,
        _: &mut call::Handler<Self::Call, Self::State>,
        _: &mut timers::Output<'_, '_, '_, Self::Timer>,
        reply: &mut Reply<'_, W>,
    ) -> Output {
        reply.finish_with(crate::Status::from_code(crate::Code::Internal));
        Output::Complete
    }

    fn request<W: Write>(
        &mut self,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        target: Self::Target,
        call: &mut call::Handler<Self::Call, Self::State>,
        input: service::Request<'_, resident::Shared<'d>>,
        output: &mut service::Response<'_, W>,
    ) -> RequestState;

    fn has_tasks(&self, owner: O) -> bool;

    fn has_done(&self, owner: O) -> bool;

    fn drive_tasks(
        &mut self,
        owner: O,
        work: Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> TaskState;

    fn wake_tasks(&mut self, owner: O) -> TaskState;

    fn pop_done(&mut self, owner: O) -> Option<(Self::Target, Self::Completion)>;

    fn publish<W: Write>(
        &mut self,
        owner: O,
        target: Self::Target,
        call: &mut call::Handler<Self::Call, Self::State>,
        done: Self::Completion,
        output: &mut service::Response<'_, W>,
    ) -> handler::RequestState;

    fn cancel(&mut self, _: O, _: Self::Target, call: &mut call::Handler<Self::Call, Self::State>) {
        *call.split().1 = Self::State::default();
    }
}
