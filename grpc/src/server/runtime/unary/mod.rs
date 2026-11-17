//! Unary tasks over one exact driver-domain scheduler.
use core::task::Poll;

use dope::{
    core::driver::{self, route::KeyTag, schedule::app::Work},
    manifold::network::listener::conn::Id,
};
use dope_fiber::task::storage::set;
use o3::buffer::resident;
use runtime::{
    executor, policy,
    schedule::{self, start},
    state::active,
};
use sark_core::manifold::server::drain::Progress;
use sark_h2::server::app;

use crate::server::{
    call, runtime,
    service::{
        self,
        handler::{self, unary},
        reply::Write,
        task::unary::Handler,
        timers,
    },
};

mod domain;

/// Route domain for unary server tasks.
pub use domain::Domain;

/// Fixed-capacity unary execution over one exact driver-domain scheduler.
pub struct Policy<'s, 'r, 'life, 'd: 'life, H>
where
    H: Handler + 'life,
{
    user: &'life H,
    codec: H::Codec,
    tasks: executor::Tasks<'s, 'd, Domain, H::Task<'life, 'd>, active::Entry<'r>>,
}

impl<'s, 'r, 'life, 'd: 'life, H> Policy<'s, 'r, 'life, 'd, H>
where
    H: Handler + 'life,
{
    pub(in crate::server) fn new(
        user: &'life H,
        tasks: executor::Tasks<'s, 'd, Domain, H::Task<'life, 'd>, active::Entry<'r>>,
    ) -> Self {
        Self {
            user,
            codec: H::Codec::default(),
            tasks,
        }
    }

    fn signal(entry: active::Entry<'r>, status: Poll<()>) {
        if status.is_ready() {
            entry.output();
        }
    }
}

impl<'s, 'r, 'life, 'd: 'life, H> policy::Dispatch<'d, Id<'d, KeyTag<0>>>
    for Policy<'s, 'r, 'life, 'd, H>
where
    H: Handler + 'life,
{
    type Call = ();
    type State = Option<set::Id<'s, 'd, Domain>>;
    type Storage<T> = T;
    type Target = active::Entry<'r>;
    type Timer = timers::Off;

    const TASKS: bool = true;
    const COMPLETES: bool = true;

    fn request<W: Write>(
        &mut self,
        turn: &mut app::Turn<'_, '_, '_, 'd, Id<'d, KeyTag<0>>>,
        target: active::Entry<'r>,
        state: &mut call::Handler<(), Self::State>,
        input: service::Request<'_, resident::Shared<'d>>,
        output: &mut service::Response<'_, W>,
    ) -> policy::RequestState {
        let (_, state) = state.split();
        let owner = turn.owner();
        let parent = turn.wake();
        let work = turn.work();
        let driver = turn.driver();
        let input = match unary::own(&mut self.codec, input) {
            Ok(input) => input,
            Err(status) => {
                output.finish_with(status);
                return policy::RequestState::Complete;
            }
        };
        if state.is_some() {
            output.finish_with(crate::Status::from_code(crate::Code::Internal));
            return policy::RequestState::Complete;
        }
        match executor::start_with(
            &mut self.tasks,
            self.user.unary(driver.driver_ref().timer(), input),
            owner,
            target,
            schedule::Context {
                parent,
                work,
                driver,
            },
            |entry, _, status| Self::signal(entry, status),
        ) {
            Ok(start::Start::Ready(value)) => {
                target.clear_output();
                value.apply(&mut self.codec, output);
                policy::RequestState::Complete
            }
            Ok(start::Start::Pending(task)) => {
                *state = Some(task);
                policy::RequestState::Pending
            }
            Err(error) => {
                output.finish_with(error.into());
                policy::RequestState::Complete
            }
        }
    }

    fn has_tasks(&self, owner: Id<'d, KeyTag<0>>) -> bool {
        self.tasks.has_work_for(owner)
    }

    fn drive_tasks(
        &mut self,
        owner: Id<'d, KeyTag<0>>,
        work: Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress {
        executor::drive_with(&mut self.tasks, owner, work, driver, |entry, _, status| {
            Self::signal(entry, status);
        })
    }

    fn wake_tasks(&mut self) {
        self.tasks.wake_next();
    }

    fn publish<W: Write>(
        &mut self,
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        call: &mut call::Handler<(), Self::State>,
        output: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        let (_, state) = call.split();
        if let Some(task) = state.take() {
            match self.tasks.take(task) {
                Ok(done) => {
                    done.apply(&mut self.codec, output);
                    return handler::RequestState::Complete;
                }
                Err(task) => *state = Some(task),
            }
        }
        output.finish_with(crate::Status::from_code(crate::Code::Internal));
        handler::RequestState::Complete
    }

    fn cancel(
        &mut self,
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        state: &mut call::Handler<(), Self::State>,
    ) {
        if let Some(task) = state.split().1.take() {
            self.tasks.discard(task);
        }
    }
}
