use dope::core::{driver::route::Identity, random};
use sark_http::payload::compress::dynamic::hash;

use crate::server::{
    runtime::{
        input::Fault as _,
        output::{self, flush},
    },
    task::Carrier,
};
mod boundary;
mod drive;
mod handoffs;
mod header;
mod inputs;
pub(in crate::server::task) mod retire;
mod turn;
use std::{marker, pin};

use dope::{
    core::driver::{
        self,
        schedule::{clock, ready::task::owner},
    },
    manifold::{
        app::receive::{self, limits::Budget},
        network::listener::{
            conn::{Ctx, Id, Ref, deadline},
            handler,
        },
    },
    net::{
        link::{egress::data, pool::recv::input::Execution, slot::types::CloseGate},
        wire::{self, receive::scoped},
    },
};
pub(in crate::server::task) use drive::Drive;
use handler::{failures, send};
use o3::{depot::capacity, mem::quota};
use sark_core::{
    decode::request,
    duplex,
    life::{mode, owners},
    manifold::{
        schedule,
        server::{app as semantic, drain},
        task::app::{Bind, Runtime},
    },
};

use crate::{
    conn::{self, Error, config::Validated, machine::ops::Drain},
    frame::numbers,
    server::{
        self,
        response::Handoff,
        runtime::{app::Fault, input, startup::Failure},
        state, task,
        task::{
            app::{
                boundary::{close, open},
                turn::{Availability, schedule::Schedule},
            },
            book,
            response::{deadline::Deadline, expiry},
            timers, *,
        },
    },
};

/// Listener application for a generated multiplexed Sark application.
#[pin_project::pin_project]
pub struct App<
    'd,
    ID: Identity,
    R: Runtime<'d>,
    W: wire::Spec = wire::Identity,
    M = mode::Disabled,
    D = timers::Disabled,
    P = failures::Close,
    A = service::Native<R>,
> where
    <R::Head as request::Head>::Start: works::Family,
    A: service::Select<R>,
{
    #[pin]
    router: R,
    tasks: registry::Table<'d, ID, R::Task>,
    limits: server::Config,
    receive_resident_bytes: usize,
    machine: Validated<crate::ServerRole>,
    execution: <<R::Head as request::Head>::Start as works::Family>::Graph,
    mode: M,
    deadlines: D,
    service: A,
    retention: A::Retention,
    pub(in crate::server) fault: P,
    wire: marker::PhantomData<fn() -> W>,
}

impl<'d, ID: Identity + 'd, R, W, M, D, P, A> App<'d, ID, R, W, M, D, P, A>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R>,
    W: wire::Spec,
    <R::Head as request::Head>::Start: works::Family,
{
    pub(in crate::server) fn bind<F>(
        router: F,
        plan: server::Plan,
        task_cap: usize,
        mode: M,
        deadlines: D,
        fault: P,
        binding: driver::Binding<'d>,
        service: A,
    ) -> Result<Self, Failure<F::Error>>
    where
        F: Bind<'d, Target = Target<'d, ID>, Runtime = R>,
    {
        let server = plan.config();
        let resident = plan.resident_len();
        let mut machine = plan.build().map_err(Failure::Protocol)?;
        let retention = service.try_retention().map_err(|cause| {
            Failure::Protocol(crate::conn::config::Error::IngressBudgetOverflow(cause))
        })?;
        machine.local_settings.enable_connect_protocol = <R as duplex::Application<'d>>::CONNECT;
        let router = Bind::try_bind(router, binding).map_err(Failure::Router)?;
        let quota = capacity::Count::try_from(task_cap).map_err(Failure::TaskCapacity)?;
        let Some(quota) = quota.nonzero() else {
            return Err(Failure::ZeroTaskCapacity);
        };
        let tasks = registry::Table::try_new(quota).map_err(Failure::Tasks)?;
        Ok(Self {
            router,
            tasks,
            limits: server,
            receive_resident_bytes: resident,
            machine: Validated::new(machine).map_err(Failure::Protocol)?,
            execution: Default::default(),
            mode,
            deadlines,
            service,
            retention,
            fault,
            wire: marker::PhantomData,
        })
    }

    fn flush_transport<C: output::State<'d>>(
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, data::Bound<M::Output, A::Egress<'d>>>,
        driver: &mut driver::Context<'_, 'd>,
        error: Option<numbers::ErrorCode>,
        mode: &mut M,
        fault: &mut P,
    ) where
        A: semantic::Application<'d, service::Connection<'d, A>, Id<'d, ID>>,
        M: Transport<'d, ID>,
        M::Output: flush::Evidence<'d>,
        P: Fault<'d, W, A::Failure, ID, task::Build<A::Build>>,
    {
        if let Err((cause, _)) = flush::Flush::new(conn).finish(driver, error, mode) {
            if A::ENABLED {
                fault.output(conn.id(), cause);
            } else {
                mode.diagnostic(&cause);
            }
        }
    }

    fn finish_transport<C: output::State<'d>>(
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, data::Bound<M::Output, A::Egress<'d>>>,
        driver: &mut driver::Context<'_, 'd>,
        error: semantic::Error<Error, A::Failure>,
        mode: &mut M,
        fault: &mut P,
    ) where
        A: semantic::Application<'d, service::Connection<'d, A>, Id<'d, ID>>,
        M: Transport<'d, ID>,
        M::Output: flush::Evidence<'d>,
        P: Fault<'d, W, A::Failure, ID, task::Build<A::Build>>,
    {
        let code = match error {
            semantic::Error::Connection(ref cause) if !A::ENABLED => {
                mode.diagnostic(cause);
                numbers::ErrorCode::from(cause)
            }
            cause => fault.failed(conn.id(), cause),
        };
        Self::flush_transport(conn, driver, Some(code), mode, fault);
    }

    fn arm_transport<C: output::State<'d>>(
        conn: &mut Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, data::Bound<M::Output, A::Egress<'d>>>,
        driver: &mut driver::Context<'_, 'd>,
        arm: Result<(), deadline::Error>,
        mode: &mut M,
        fault: &mut P,
    ) -> Sync
    where
        A: semantic::Application<'d, service::Connection<'d, A>, Id<'d, ID>>,
        M: Transport<'d, ID>,
        M::Output: flush::Evidence<'d>,
        P: Fault<'d, W, A::Failure, ID, task::Build<A::Build>>,
    {
        match arm {
            Ok(()) => Sync::Armed,
            Err(_) => {
                Self::flush_transport(
                    conn,
                    driver,
                    Some(numbers::ErrorCode::InternalError),
                    mode,
                    fault,
                );
                Sync::Finished
            }
        }
    }
}

impl<'w, 'd: 'w, ID: Identity + 'd, R, W, M, D, P, S, A> handler::Scoped<'w, 'd, ID>
    for App<'d, ID, R, W, M, D, P, A>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R> + semantic::Timed<'d, service::Connection<'d, A>, Id<'d, ID>>,
    R::Head: request::Head<Start = S>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    P: Fault<'d, W, A::Failure, ID, task::Build<A::Build>>,
    S: Schedule<'d, R>,
{
    fn retention(&self) -> Budget {
        Budget::new(A::retention(&self.retention), self.receive_resident_bytes)
    }

    fn scoped_chunk<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, '_, '_, '_, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
        chunk: scoped::Input<'_, 'w, 'd, W>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> receive::Outcome {
        let id = conn.id();
        let parent = conn.wake_target();
        let work = conn.work();
        let mut this = self.project();
        let limits = *this.limits;
        let mut source = input::Turn::<W>::new(chunk);
        let now = driver.driver_ref().clock().now();
        let (mut result, pending, change, fault) = {
            let state = conn.state_mut();
            let mut mode =
                state::Projection::new(this.mode, &mut state.deadlines, now, this.deadlines);
            let mut turn = book::Turn::new(&mut state.carrier, &mut state.book);
            let index = &mut state.task_index;
            let expiry = expiry::apply(
                mode.poll_due(),
                this.router.as_mut(),
                this.tasks,
                id,
                index,
                &mut turn,
                &mut mode,
            );
            let result = (|| {
                match expiry {
                    Deadline::Ready => {}
                    Deadline::Critical => {
                        return Err(semantic::Error::Connection(conn::Error::CriticalDeadline));
                    }
                    Deadline::Expired | Deadline::Wake => return Ok(expiry),
                }
                let mut run = drive::Drive {
                    router: this.router.as_mut(),
                    tasks: this.tasks,
                    conn: id,
                    parent,
                    index,
                    state: &mut turn,
                    limits,
                    mode: &mut mode,
                };
                let result = {
                    let mut application = semantic::Turn::new(id, work, parent, driver);
                    input::Read::new(&mut run, drive::Drive::connection, |run| {
                        run.drain::<A, W, P>(
                            this.service,
                            &mut state.service,
                            &mut application,
                            sark_core::manifold::schedule::Egress::Unchanged,
                            this.fault,
                        )
                    })
                    .read(&mut source)
                };
                run.mode.observe_head(run.state.connection);
                match drain::Progress::error(result, parent) {
                    None => {}
                    Some(error)
                        if error.connection().is_some_and(conn::Error::is_pressure)
                            && run.state.connection.ingress().is_blocked() => {}
                    Some(error) => return Err(error),
                }
                let scope = owner::Scope::Exact(owner::Owner::from(id));
                if Runtime::has_work(run.router.as_ref(), scope) {
                    run.reborrow()
                        .batch::<A>(work, driver, this.fault.response());
                }
                run.wakes::<A>(&mut state.service);
                Ok(expiry)
            })();
            let (change, fault) = mode.finish();
            let pending = S::is_ready(&mut turn);
            (result, pending, change, fault)
        };
        let arm = if (A::ENABLED && result.is_ok())
            || (!A::ENABLED && matches!(change, timers::Change::Changed))
        {
            let state = conn.state_mut();
            let deadline = timers::Policy::controller(this.deadlines, &mut state.deadlines)
                .and_then(|timer| timer.deadline())
                .into_iter()
                .chain(this.service.deadline(&state.service))
                .min();
            Some(state::try_sync(deadline, &mut conn))
        } else {
            None
        };
        if let Some(fault) = fault {
            result = Err(semantic::Error::Connection(conn::Error::Deadline(fault)));
        }
        if matches!(&result, Ok(Deadline::Wake)) {
            parent.wake();
        }
        if matches!(pending, turn::Availability::Ready) {
            parent.wake();
        }
        let scope = owner::Scope::Exact(owner::Owner::from(id));
        Runtime::wake(this.router.as_mut(), scope);
        if let Some(arm) = arm
            && matches!(
                Self::arm_transport(&mut conn, driver, arm, this.mode, this.fault),
                Sync::Finished
            )
        {
            source.discard();
            return receive::Outcome::Continue;
        }
        if result.is_err() {
            source.discard();
        }
        if result.is_ok() {
            let state = conn.state_mut();
            state.finish_drain(A::activity(&state.service));
        }
        match result {
            Ok(_) => Self::flush_transport(&mut conn, driver, None, this.mode, this.fault),
            Err(error) => Self::finish_transport(&mut conn, driver, error, this.mode, this.fault),
        }
        receive::Outcome::Continue
    }
}

impl<'d, ID: Identity + 'd, R, W, M, D, P, A> handler::Application<'d, ID>
    for App<'d, ID, R, W, M, D, P, A>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R> + semantic::Timed<'d, service::Connection<'d, A>, Id<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    P: Fault<'d, W, A::Failure, ID, task::Build<A::Build>>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    type Conn = Carrier<'d, R, D, A::Fields, service::State<'d, ID, A>>;
    type Build = task::Build<A::Build>;
    type Wire = W;
    type WireFailure = P;
    type Retirement = task::Retire;
    type Egress = data::Bound<M::Output, A::Egress<'d>>;
    type Input = receive::Scoped;
    type Send = send::Reported<send::Run>;

    fn deadline(self: pin::Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn timing(&self) -> handler::Timing {
        if D::ENABLED || A::ENABLED {
            handler::Timing::managed()
        } else {
            handler::Timing::default()
        }
    }

    fn timeout<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        let mut this = self.project();
        let service = this.service;
        let fault = this.fault;
        let limits = *this.limits;
        let id = conn.id();
        if !D::ENABLED && !A::ENABLED {
            return handler::Timeout::Discard;
        }
        let parent = conn.wake_target();
        let work = conn.work();
        let now = driver.driver_ref().clock().now();
        let turn = (|| {
            let state = conn.state_mut();
            let mut mode =
                state::Projection::new(this.mode, &mut state.deadlines, now, this.deadlines);
            let mut h2 = book::Turn::new(&mut state.carrier, &mut state.book);
            let turn = expiry::apply(
                mode.poll_due(),
                this.router.as_mut(),
                this.tasks,
                id,
                &mut state.task_index,
                &mut h2,
                &mut mode,
            );
            if A::ENABLED && matches!(turn, Deadline::Ready) {
                let progress = Drive {
                    router: this.router.as_mut(),
                    tasks: this.tasks,
                    conn: id,
                    parent,
                    index: &mut state.task_index,
                    state: &mut h2,
                    limits,
                    mode: &mut mode,
                }
                .drain::<A, W, P>(
                    service,
                    &mut state.service,
                    &mut semantic::Turn::new(id, work, parent, driver),
                    schedule::Egress::Unchanged,
                    fault,
                )?;
                if progress.is_exhausted() {
                    parent.wake();
                }
            }
            let (_, error) = mode.finish();
            match error {
                Some(error) => Err(semantic::Error::Connection(Error::Deadline(error))),
                None => Ok(turn),
            }
        })();
        let turn = match turn {
            Ok(turn) => turn,
            Err(error) => {
                Self::finish_transport(&mut conn, driver, error, this.mode, fault);
                return handler::Timeout::CloseAfter;
            }
        };
        let due = {
            let state = conn.state_mut();
            timers::Policy::controller(this.deadlines, &mut state.deadlines)
                .and_then(|ctl| ctl.deadline())
                .into_iter()
                .chain(service.deadline(&state.service))
                .min()
        };
        if matches!(turn, Deadline::Wake | Deadline::Expired) {
            conn.wake_target().wake();
        }
        if matches!(turn, Deadline::Critical) {
            Self::finish_transport(
                &mut conn,
                driver,
                Error::CriticalDeadline.into(),
                this.mode,
                fault,
            );
            return handler::Timeout::CloseAfter;
        }
        let state = conn.state_mut();
        let drain = state.finish_drain(A::activity(&state.service));
        if !conn.state().carrier.output().is_empty() || matches!(drain, handler::Drain::Complete) {
            Self::flush_transport(&mut conn, driver, None, this.mode, fault);
        }
        let update = due.map_or(handler::Deadline::Clear, handler::Deadline::Arm);
        handler::Timeout::Continue(update)
    }

    fn drain<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Drain {
        let this = self.project();
        let done = {
            let state = conn.state_mut();
            let activity = state.activity(A::activity(&state.service));
            let drain = state.carrier.control().drain(activity);
            match (drain, state.carrier.status().is_closing()) {
                (Drain::Complete, _) | (Drain::Pending, true) => handler::Drain::Complete,
                (Drain::Pending, false) => handler::Drain::Pending,
            }
        };
        if !conn.state().carrier.output().is_empty() {
            Self::flush_transport(&mut conn, driver, None, this.mode, this.fault);
        }
        done
    }

    fn connection(self: pin::Pin<&Self>, seed: &random::Seed) -> Result<Self::Conn, Self::Build> {
        let this = self.get_ref();
        let config = &this.machine;
        let domain = config.stream_count();
        let timers =
            timers::Policy::state(&this.deadlines, domain).map_err(task::Build::Deadlines)?;
        let capacity = semantic::Capacity::new(domain, config.data_events().raw());
        let carrier = conn::machine::Connection::from_runtime_seed(*config, hash::Seed::new(seed))
            .map_err(task::Build::Connection)?;
        let book = book::Book::try_new(domain).map_err(task::Build::Book)?;
        Ok(State {
            carrier,
            book,
            task_index: registry::Index::try_new(domain).map_err(task::Build::Tasks)?,
            deadlines: timers,
            service: this.service.state(capacity).map_err(task::Build::Service)?,
        })
    }

    fn build_failed(self: pin::Pin<&mut Self>, cause: Self::Build) {
        let this = self.project();
        if A::ENABLED {
            this.fault.construction(cause);
            return;
        }
        let cause: task::Build = match cause {
            task::Build::Service(cause) => {
                this.fault.construction(task::Build::Service(cause));
                return;
            }
            task::Build::Connection(cause) => task::Build::Connection(cause),
            task::Build::Book(cause) => task::Build::Book(cause),
            task::Build::Tasks(cause) => task::Build::Tasks(cause),
            task::Build::Deadlines(cause) => task::Build::Deadlines(cause),
        };
        this.mode.diagnostic(&cause);
    }

    fn close_gate(
        self: pin::Pin<&Self>,
        conn: Ref<'_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
    ) -> CloseGate {
        if conn.state().carrier.output().is_empty() || conn.is_terminal() {
            CloseGate::Immediate
        } else {
            CloseGate::Deferred
        }
    }

    fn accept<'e>(
        self: pin::Pin<&mut Self>,
        conn: Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
    ) -> receive::Outcome {
        let this = self.project();
        open::apply(this.router, this.mode, conn.id())
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        let this = self.project();
        let mut router = this.router;
        let tasks = this.tasks;
        let limits = &*this.limits;
        let exec = this.execution;
        let mode = this.mode;
        let timers = this.deadlines;
        let service = this.service;
        let fault = this.fault;
        let id = conn.id();
        let parent = conn.wake_target();
        let work = conn.work();
        let scope = owner::Scope::Exact(owner::Owner::from(id));
        let task = if Runtime::has_work(router.as_ref(), scope) {
            turn::Availability::Ready
        } else {
            turn::Availability::Idle
        };
        let now = driver.driver_ref().clock().now();
        let (result, expiry, change, timer_fault) = {
            let state = conn.state_mut();
            let builds = if state.book.build_ready.is_empty() {
                turn::Availability::Idle
            } else {
                turn::Availability::Ready
            };
            let input = if A::ENABLED
                || state.carrier.ingress().has_event()
                || state.carrier.ingress().is_blocked()
            {
                turn::Availability::Ready
            } else {
                turn::Availability::Idle
            };
            let output = if state.book.responses.has_runnable() {
                turn::Availability::Ready
            } else {
                turn::Availability::Idle
            };
            let mut mode = state::Projection::new(mode, &mut state.deadlines, now, timers);
            let mut turn = book::Turn::new(&mut state.carrier, &mut state.book);
            let index = &mut state.task_index;
            let expiry = expiry::apply(
                mode.poll_due(),
                router.as_mut(),
                tasks,
                id,
                index,
                &mut turn,
                &mut mode,
            );
            let next = if matches!(expiry, Deadline::Ready) {
                let drive = drive::Drive {
                    router: router.as_mut(),
                    tasks,
                    conn: id,
                    parent,
                    index,
                    state: &mut turn,
                    limits: *limits,
                    mode: &mut mode,
                };
                let ready = turn::Ready {
                    build: builds,
                    task,
                    input,
                    output,
                };
                let mut work = turn::Turn::new(
                    drive,
                    work,
                    driver.reborrow(),
                    ready,
                    service,
                    &mut state.service,
                    fault,
                );
                <R::Head as request::Head>::Start::admit::<ID, _, W, _, _>(exec, &mut work)
            } else {
                quota::Admission::Empty
            };
            let result = match next {
                quota::Admission::Item(Ok(turn::Outcome::Account)) => {
                    <R::Head as request::Head>::Start::account(exec);
                    Runtime::wake(router.as_mut(), scope);
                    Ok(())
                }
                quota::Admission::Item(Ok(turn::Outcome::Wake)) => {
                    Runtime::wake(router.as_mut(), scope);
                    Ok(())
                }
                quota::Admission::Item(Ok(turn::Outcome::Idle))
                | quota::Admission::Empty
                | quota::Admission::Exhausted => Ok(()),
                quota::Admission::Item(Err(error)) => Err(error),
            };
            mode.observe_head(turn.connection);
            if result.is_ok() {
                drive::Drive {
                    router: router.as_mut(),
                    tasks,
                    conn: id,
                    parent,
                    index,
                    state: &mut turn,
                    limits: *limits,
                    mode: &mut mode,
                }
                .wakes::<A>(&mut state.service);
            }
            let (change, error) = mode.finish();
            if matches!(
                <R::Head as request::Head>::Start::is_ready(&mut turn),
                turn::Availability::Ready
            ) {
                parent.wake();
            }
            (result, expiry, change, error)
        };
        if (A::ENABLED && result.is_ok())
            || (!A::ENABLED && matches!(change, timers::Change::Changed))
        {
            let deadline = {
                let state = conn.state_mut();
                timers::Policy::controller(timers, &mut state.deadlines)
                    .and_then(|timer| timer.deadline())
                    .into_iter()
                    .chain(service.deadline(&state.service))
                    .min()
            };
            let arm = state::try_sync(deadline, &mut conn);
            if matches!(
                Self::arm_transport(&mut conn, driver, arm, mode, fault),
                Sync::Finished
            ) {
                return conn;
            }
        }
        let result = match timer_fault {
            Some(error) => Err(semantic::Error::Connection(conn::Error::Deadline(error))),
            None => result,
        };
        let error = if matches!(expiry, Deadline::Critical) {
            Some(conn::Error::CriticalDeadline.into())
        } else {
            result.err()
        };
        if let Some(error) = error {
            Self::finish_transport(&mut conn, driver, error, mode, fault);
            return conn;
        }
        if matches!(expiry, Deadline::Wake) {
            parent.wake();
        }
        if matches!(expiry, Deadline::Expired) {
            if !conn.state().carrier.output().is_empty() {
                Self::flush_transport(&mut conn, driver, None, mode, fault);
            }
            parent.wake();
            return conn;
        }
        let state = conn.state_mut();
        let drain = state.finish_drain(A::activity(&state.service));
        if !conn.state().carrier.output().is_empty() || matches!(drain, handler::Drain::Complete) {
            Self::flush_transport(&mut conn, driver, None, mode, fault);
        }
        conn
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
    ) -> Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        let this = self.project();
        {
            let owner = conn.id();
            let state = conn.state_mut();
            this.service
                .close(&mut state.service, &mut state.carrier, owner);
        }
        close::Close::new(this.router, this.tasks, this.mode).apply::<W, D, A>(&mut conn);
        conn
    }
}

impl<'d, ID: Identity + 'd, R, W, M, D, P, A> send::Report<'d, ID> for App<'d, ID, R, W, M, D, P, A>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R> + semantic::Timed<'d, service::Connection<'d, A>, Id<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    P: Fault<'d, W, A::Failure, ID, task::Build<A::Build>>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    const ENABLED: bool = true;

    fn released(self: pin::Pin<&mut Self>, release: handler::Release<'d, ID, Self::Egress>) {
        let (conn, egress, cause) = release.into_parts();
        let this = self.project();
        if let Some(effect) = <M as Transport<'d, ID>>::release::<A::Egress<'d>>(egress, cause) {
            owners::Projection::new(this.mode)
                .apply(effect.map_key(|stream| identify(conn, stream)));
        }
    }
}

impl<'d, ID: Identity + 'd, R, W, M, D, P, A> send::Drive<'d, ID> for App<'d, ID, R, W, M, D, P, A>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R> + semantic::Timed<'d, service::Connection<'d, A>, Id<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    P: Fault<'d, W, A::Failure, ID, task::Build<A::Build>>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    fn send<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let limits = self.as_ref().get_ref().limits;
        let mut this = self.project();
        let service = this.service;
        let fault = this.fault;
        let id = conn.id();
        let parent = conn.wake_target();
        let work = conn.work();
        let scope = owner::Scope::Exact(owner::Owner::from(id));
        let tasks = if Runtime::has_work(this.router.as_ref(), scope) {
            Availability::Ready
        } else {
            Availability::Idle
        };
        let now = driver.driver_ref().clock().now();
        let result = (|| {
            let state = conn.state_mut();
            if A::ENABLED {
                state.book.responses.wake_egress();
                A::released(&mut state.service);
            }
            let input =
                if state.carrier.ingress().has_event() || state.carrier.ingress().is_blocked() {
                    Availability::Ready
                } else {
                    Availability::Idle
                };
            let mut mode =
                state::Projection::new(this.mode, &mut state.deadlines, now, this.deadlines);
            let mut book = book::Turn::new(&mut state.carrier, &mut state.book);
            let mut run = Drive {
                router: this.router.as_mut(),
                tasks: this.tasks,
                conn: id,
                parent,
                index: &mut state.task_index,
                state: &mut book,
                limits,
                mode: &mut mode,
            };
            let due = expiry::apply(
                run.mode.poll_due(),
                run.router.as_mut(),
                run.tasks,
                id,
                run.index,
                run.state,
                run.mode,
            );
            if matches!(due, Deadline::Ready) {
                if A::ENABLED {
                    let progress = run.drain::<A, W, P>(
                        service,
                        &mut state.service,
                        &mut semantic::Turn::new(id, work, parent, driver),
                        schedule::Egress::Unchanged,
                        fault,
                    )?;
                    if progress.is_exhausted() {
                        parent.wake();
                    }
                }
                let local = <R::Head as request::Head>::Start::has_local(run.state);
                let egress = if A::ENABLED {
                    schedule::Egress::Unchanged
                } else {
                    schedule::Egress::Released
                };
                let mut response = fault.response();
                match (local, tasks, input) {
                    (Availability::Idle, Availability::Idle, Availability::Idle) => {
                        run.state.pump::<false, _, _, _, A>(
                            run.router.as_mut(),
                            run.tasks,
                            run.index,
                            id,
                            limits,
                            egress,
                            work,
                            run.mode,
                            &mut response,
                        )
                    }
                    _ => run.state.pump::<true, _, _, _, A>(
                        run.router.as_mut(),
                        run.tasks,
                        run.index,
                        id,
                        limits,
                        egress,
                        work,
                        run.mode,
                        &mut response,
                    ),
                };
            }
            run.wakes::<A>(&mut state.service);
            let local = <R::Head as request::Head>::Start::has_local(run.state);
            let active = if run.state.has_input() || run.state.responses.has_runnable() {
                Availability::Ready
            } else {
                Availability::Idle
            };
            let wake = match (local, input, active) {
                (Availability::Idle, Availability::Idle, Availability::Idle) => Availability::Idle,
                _ => Availability::Ready,
            };
            let (change, error) = mode.finish();
            Ok((wake, due, change, error))
        })();
        let (wake, due, change, error) = match result {
            Ok(result) => result,
            Err(cause) => {
                Self::finish_transport(&mut conn, driver, cause, this.mode, fault);
                return;
            }
        };
        if A::ENABLED || matches!(change, timers::Change::Changed) {
            let next = {
                let state = conn.state_mut();
                let deadline = timers::Policy::controller(this.deadlines, &mut state.deadlines)
                    .and_then(|timer| timer.deadline());
                if A::ENABLED {
                    deadline
                        .into_iter()
                        .chain(service.deadline(&state.service))
                        .min()
                } else {
                    deadline
                }
            };
            let arm = state::try_sync(next, &mut conn);
            if matches!(
                Self::arm_transport(&mut conn, driver, arm, this.mode, fault),
                Sync::Finished
            ) {
                return;
            }
        }
        let error = match (error, due) {
            (Some(error), _) => Some(conn::Error::Deadline(error)),
            (None, Deadline::Critical) => Some(conn::Error::CriticalDeadline),
            _ => None,
        };
        if let Some(error) = error {
            Self::finish_transport(&mut conn, driver, error.into(), this.mode, fault);
            return;
        }
        let drain = {
            let state = conn.state_mut();
            state.finish_drain(A::activity(&state.service))
        };
        if !conn.state().carrier.output().is_empty() || matches!(drain, handler::Drain::Complete) {
            Self::flush_transport(&mut conn, driver, None, this.mode, fault);
        }
        if matches!(due, Deadline::Expired) {
            if A::ENABLED || matches!(wake, Availability::Ready) {
                parent.wake();
            }
            return;
        }
        if matches!(tasks, Availability::Ready) {
            Runtime::wake(this.router.as_mut(), scope);
        }
        if matches!(wake, Availability::Ready) || matches!(due, Deadline::Wake) {
            parent.wake();
        }
    }
}
