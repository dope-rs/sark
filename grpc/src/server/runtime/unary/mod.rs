//! Unary tasks over one exact driver-domain scheduler.
use dope::{
    core::driver::{self, route::KeyTag, schedule::app::Work},
    manifold::network::listener::conn::Id,
};
use dope_fiber::task::storage::set::group;
use o3::buffer::resident;
use runtime::{
    Call, executor, policy,
    schedule::{self, start},
    state::active,
};
use sark_core::manifold::server::{app, drain::Progress};

use crate::server::{
    call, runtime,
    service::{
        self,
        handler::{self, modes, unary},
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
    tasks: executor::Scheduler<'s, 'd, Domain, H::Task<'life, 'd>, active::Entry<'r>>,
}

impl<'s, 'r, 'life, 'd: 'life, H> Policy<'s, 'r, 'life, 'd, H>
where
    H: Handler + 'life,
{
    pub(in crate::server) fn new(
        user: &'life H,
        tasks: executor::Scheduler<'s, 'd, Domain, H::Task<'life, 'd>, active::Entry<'r>>,
    ) -> Self {
        Self {
            user,
            codec: H::Codec::default(),
            tasks,
        }
    }
}

impl<'s, 'r, 'life, 'd: 'life, H> policy::Dispatch<'d, Id<'d, KeyTag<0>>>
    for Policy<'s, 'r, 'life, 'd, H>
where
    H: Handler + 'life,
{
    type Call = ();
    type Completion = modes::Response<H::Response>;
    type State = Call<group::Id<'s, 'd, Domain>>;
    type Target = active::Entry<'r>;
    type Timer = timers::Off;

    const TASKS: bool = true;

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
        let Some(vacant) = state.vacant() else {
            output.finish_with(crate::Status::from_code(crate::Code::Internal));
            return policy::RequestState::Complete;
        };
        match self.tasks.start_with(
            self.user.unary(driver.driver_ref().timer(), input),
            owner,
            target,
            schedule::Context {
                parent,
                work,
                driver,
            },
            |_, _| {},
        ) {
            Ok(start::Start::Ready(value)) => {
                value.apply(&mut self.codec, output);
                policy::RequestState::Complete
            }
            Ok(start::Start::Pending(task)) => {
                vacant.insert(task);
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

    fn has_done(&self, owner: Id<'d, KeyTag<0>>) -> bool {
        self.tasks.has_done(owner)
    }

    fn drive_tasks(
        &mut self,
        owner: Id<'d, KeyTag<0>>,
        work: Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress {
        self.tasks.drive_with(owner, work, driver, |_, _| {})
    }

    fn wake_tasks(&mut self) {
        self.tasks.wake_next();
    }

    fn pop_done(
        &mut self,
        owner: Id<'d, KeyTag<0>>,
    ) -> Option<(active::Entry<'r>, Self::Completion)> {
        self.tasks.pop_done(owner)
    }

    fn publish<W: Write>(
        &mut self,
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        state: &mut call::Handler<(), Self::State>,
        done: Self::Completion,
        output: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        let (_, state) = state.split();
        if state.take().is_some() {
            done.apply(&mut self.codec, output);
        } else {
            output.finish_with(crate::Status::from_code(crate::Code::Internal));
        }
        handler::RequestState::Complete
    }

    fn cancel(
        &mut self,
        _: Id<'d, KeyTag<0>>,
        _: active::Entry<'r>,
        state: &mut call::Handler<(), Self::State>,
    ) {
        state.split().1.cancel(|task| self.tasks.cancel(task))
    }
}
