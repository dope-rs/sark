//! Unary tasks over one exact driver-domain scheduler.
use std::marker;

use dope::core::driver::{retained, schedule::app::Work};
use manifold::task::schedule::{self, start};
use o3::buffer::resident;
use runtime::{Call, policy};
use sark_core::{manifold, manifold::server::app};

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

type Target<'d, F, O, T> = marker::PhantomData<fn() -> *mut (F, O, T, &'d ())>;

/// Fixed-capacity unary execution over one exact driver-domain scheduler.
pub struct Policy<'life, 'd: 'life, H, S, F, O, T>
where
    H: Handler + 'life,
    F: From<H::Task<'life, 'd>> + 'life,
    S: schedule::raw::Scheduler<'d, Domain, O, T, F, modes::Response<H::Response>>,
    O: Copy + Eq + 'd,
    T: Copy,
{
    user: &'life H,
    codec: H::Codec,
    tasks: S,
    target: Target<'d, F, O, T>,
}

impl<'life, 'd: 'life, H, S, F, O, T> Policy<'life, 'd, H, S, F, O, T>
where
    H: Handler + 'life,
    F: From<H::Task<'life, 'd>> + 'life,
    S: schedule::raw::Scheduler<'d, Domain, O, T, F, modes::Response<H::Response>>,
    O: Copy + Eq + 'd,
    T: Copy,
{
    pub(in crate::server) fn new(user: &'life H, tasks: S) -> Self {
        Self {
            user,
            codec: H::Codec::default(),
            tasks,
            target: marker::PhantomData,
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

impl<'life, 'd: 'life, H, S, F, O, T> policy::Dispatch<'d, O> for Policy<'life, 'd, H, S, F, O, T>
where
    H: Handler + 'life,
    F: From<H::Task<'life, 'd>> + 'life,
    S: schedule::raw::Scheduler<'d, Domain, O, T, F, modes::Response<H::Response>>,
    O: Copy + Eq + 'd,
    T: Copy,
{
    type Call = ();
    type Completion = modes::Response<H::Response>;
    type State = Call<S::Pending>;
    type Target = T;
    type Timer = timers::Off;

    const TASKS: bool = true;

    fn request<W: Write>(
        &mut self,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        target: T,
        state: &mut call::Handler<(), Call<S::Pending>>,
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
            F::from(self.user.unary(driver.driver_ref().timer(), input)),
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

    fn has_tasks(&self, owner: O) -> bool {
        self.tasks.has_work_for(owner)
    }

    fn has_done(&self, owner: O) -> bool {
        self.tasks.has_done(owner)
    }

    fn drive_tasks(
        &mut self,
        owner: O,
        work: Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> policy::TaskState {
        self.tasks.drive_with(owner, work, driver, |_, _| {});
        self.task_state(owner)
    }

    fn wake_tasks(&mut self, owner: O) -> policy::TaskState {
        self.tasks.wake_next();
        self.task_state(owner)
    }

    fn pop_done(&mut self, owner: O) -> Option<(T, Self::Completion)> {
        self.tasks.pop_done(owner)
    }

    fn publish<W: Write>(
        &mut self,
        _: O,
        _: T,
        state: &mut call::Handler<(), Call<S::Pending>>,
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

    fn cancel(&mut self, _: O, _: T, state: &mut call::Handler<(), Call<S::Pending>>) {
        state.split().1.cancel(|task| self.tasks.cancel(task))
    }
}
