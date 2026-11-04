use dope::core::{driver::route::Identity, random};
use sark_http::payload::compress::dynamic::hash;

use crate::server::{runtime::output::flush, task::Carrier};
mod boundary;
mod cycle;
mod drive;
mod handoffs;
mod header;
mod inputs;
pub(in crate::server::task) mod retire;
mod turn;
use std::{marker, pin};

use dope::{
    core::driver::{
        self, retained,
        schedule::{clock, ready::task::owner},
    },
    manifold::{
        app::receive::{self, limits::Budget},
        network::listener::{
            conn::{Ctx, Ref},
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
use o3::{buffer::hold::limits, depot::capacity};
use sark_core::{
    decode::request,
    duplex,
    life::{mode, owners},
    manifold::task::app::{Bind, Runtime},
};

use crate::{
    conn::{self, config::Validated, event},
    server::{
        self,
        response::Handoff,
        runtime::{input, startup::Failure},
        state, task,
        task::{
            app::{
                boundary::{close, open, send::Send, timeout},
                turn::schedule::Schedule,
            },
            book,
            response::{deadline::Deadline, expiry},
            timers, *,
        },
    },
};

struct Protocol {
    limits: server::Config,
    receive_resident_bytes: usize,
    machine: Validated<crate::ServerRole>,
}

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
> where
    <R::Head as request::Head>::Start: works::Family,
{
    #[pin]
    router: R,
    tasks: registry::Table<'d, ID, R::Task>,
    protocol: Protocol,
    execution: works::State<<R::Head as request::Head>::Start>,
    mode: M,
    deadlines: D,
    pub(in crate::server) fault: P,
    wire: marker::PhantomData<fn() -> W>,
}

impl<'d, ID: Identity + 'd, R, W, M, D, P> App<'d, ID, R, W, M, D, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    W: wire::Spec,
    <R::Head as request::Head>::Start: works::Family,
{
    pub(in crate::server) fn try_bind<F>(
        router: F,
        config: Config,
        mode: M,
        deadlines: D,
        fault: P,
        binding: driver::Binding<'d>,
    ) -> Result<Self, Failure<F::Error>>
    where
        F: Bind<'d, Target = Target<'d, ID>, Runtime = R>,
    {
        let (plan, task_cap) = config.into_parts();
        let server = plan.config();
        let resident = plan.resident_len();
        let mut machine = plan.build().map_err(Failure::Protocol)?;
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
            protocol: Protocol {
                limits: server,
                receive_resident_bytes: resident,
                machine: Validated::new(machine).map_err(Failure::Protocol)?,
            },
            execution: works::State::new(),
            mode,
            deadlines,
            fault,
            wire: marker::PhantomData,
        })
    }
}

impl<'w, 'd: 'w, ID: Identity + 'd, R, W, M, D, P, S> handler::Scoped<'w, 'd, ID>
    for App<'d, ID, R, W, M, D, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    R::Head: request::Head<Start = S>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    S: Schedule<'d, R>,
{
    fn retention(&self) -> Budget {
        Budget::new(limits::Bound::new(0), self.protocol.receive_resident_bytes)
    }

    fn scoped_chunk<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: Ctx<'e, '_, '_, '_, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
        chunk: scoped::Input<'_, 'w, 'd, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        let id = conn.id();
        let parent = conn.wake_target();
        let work = conn.work();
        let mut this = self.project();
        let limits = this.protocol.limits;
        let mut source = input::Turn::<W>::new(chunk);
        let now = driver.driver_ref().clock().now();
        let (mut result, pending, change, fault) = {
            let state = conn.state_mut();
            let mut mode =
                state::Projection::new(this.mode, &mut state.deadlines, now, this.deadlines);
            let mut turn = book::Turn::new(&mut state.carrier, &mut state.book);
            let index = &mut state.task_index;
            let expiry = expiry::Expiry::new(mode.poll_due()).apply(
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
                    Deadline::Critical => return Err(conn::Error::CriticalDeadline),
                    Deadline::Expired | Deadline::Wake => return Ok(expiry),
                }
                let result = {
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
                    input::Read::new(&mut run, drive::Drive::connection, |run| run.drain(work))
                        .read::<R::Plan, event::Retirement, W>(&mut source)
                };
                mode.observe_head(turn.connection);
                match result {
                    Ok(report) => {
                        if report.is_exhausted() {
                            parent.wake();
                        }
                    }
                    Err(error) if error.is_pressure() && turn.connection.ingress().is_blocked() => {
                    }
                    Err(error) => return Err(error),
                }
                let scope = owner::Scope::Exact(owner::Owner::from(id));
                if Runtime::has_work(this.router.as_ref(), scope) {
                    Drive {
                        router: this.router.as_mut(),
                        tasks: this.tasks,
                        conn: id,
                        parent,
                        index,
                        state: &mut turn,
                        limits,
                        mode: &mut mode,
                    }
                    .batch(work, driver);
                }
                Ok(expiry)
            })();
            let (change, fault) = mode.finish();
            let pending = S::has_pending(&mut turn);
            (result, pending, change, fault)
        };
        let arm = match change {
            timers::Change::Changed => {
                let deadline = {
                    let state = conn.state_mut();
                    timers::Policy::controller(this.deadlines, &mut state.deadlines)
                        .and_then(|timer| timer.deadline())
                };
                Some(state::try_sync(deadline, &mut conn))
            }
            timers::Change::Unchanged => None,
        };
        if let Some(fault) = fault {
            result = Err(conn::Error::Deadline(fault));
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
                arm_transport::<ID, R, _, _, _>(&mut conn, driver, arm, this.mode),
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
            conn.state_mut().finish_drain();
        }
        match result {
            Ok(_) => finish_transport::<ID, R, _, _, _>(&mut conn, driver, None, this.mode),
            Err(error) => {
                finish_transport::<ID, R, _, _, _>(&mut conn, driver, Some(error), this.mode)
            }
        }
        receive::Outcome::Continue
    }
}

impl<'d, ID: Identity + 'd, R, W, M, D, P> handler::Application<'d, ID>
    for App<'d, ID, R, W, M, D, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    type Conn = Carrier<'d, R, D>;
    type Build = task::Build;
    type Wire = W;
    type WireFailure = P;
    type Retirement = task::Retire;
    type Egress = data::Bound<M::Output, R::Egress<'d>>;
    type Input = receive::Scoped;
    type Send = send::Reported<send::Run>;

    fn deadline(self: pin::Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn timing(&self) -> handler::Timing {
        if D::ENABLED {
            handler::Timing::managed()
        } else {
            handler::Timing::default()
        }
    }

    fn timeout<'e>(
        self: pin::Pin<&mut Self>,
        conn: Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        let this = self.project();
        timeout::Timeout::new(this.router, this.tasks, this.mode, this.deadlines)
            .application(conn, driver)
    }

    fn drain<'e>(
        self: pin::Pin<&mut Self>,
        conn: Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain {
        let this = self.project();
        timeout::Timeout::new(this.router, this.tasks, this.mode, this.deadlines)
            .begin(conn, driver)
    }

    fn connection(self: pin::Pin<&Self>, seed: &random::Seed) -> Result<Self::Conn, Self::Build> {
        let this = self.get_ref();
        let timers = timers::Policy::state(&this.deadlines, this.protocol.machine.stream_count())
            .map_err(task::Build::Deadlines)?;
        State::try_new(this.protocol.machine, timers, hash::Seed::new(seed))
    }

    fn build_failed(self: pin::Pin<&mut Self>, cause: Self::Build) {
        self.project().mode.diagnostic(&cause);
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
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        let this = self.project();
        cycle::Cycle::new(
            this.router,
            this.tasks,
            this.protocol,
            this.execution,
            this.mode,
            this.deadlines,
        )
        .run(&mut conn, driver);
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
        close::Close::new(this.router, this.tasks, this.mode).apply::<W, D>(&mut conn);
        conn
    }
}

impl<'d, ID: Identity + 'd, R, W, M, D, P> send::Report<'d, ID> for App<'d, ID, R, W, M, D, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    const ENABLED: bool = true;

    fn released(self: pin::Pin<&mut Self>, release: handler::Release<'d, ID, Self::Egress>) {
        let (conn, egress, cause) = release.into_parts();
        let this = self.project();
        if let Some(effect) = <M as Transport<'d, ID>>::release::<R>(egress, cause) {
            owners::Projection::new(this.mode)
                .apply(effect.map_key(|stream| identify(conn, stream)));
        }
    }
}

impl<'d, ID: Identity + 'd, R, W, M, D, P> send::Drive<'d, ID> for App<'d, ID, R, W, M, D, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    fn send<'e>(
        self: pin::Pin<&mut Self>,
        conn: Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let limits = self.as_ref().get_ref().protocol.limits;
        let this = self.project();
        Send {
            router: this.router,
            tasks: this.tasks,
            mode: this.mode,
            deadlines: this.deadlines,
            limits,
        }
        .apply(conn, driver);
    }
}
