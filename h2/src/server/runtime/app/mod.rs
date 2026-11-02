//! Static host for protocol-independent server applications.
use dope::core::{driver::route::Identity, random};
use sark_http::payload::compress::dynamic::hash;

mod state;
pub use state::State;
mod failure;
mod report;
use std::{marker, mem, pin};

use dope::{
    core::driver::{retained, schedule::clock},
    manifold::{
        app::receive::{self, limits::Budget},
        network::listener::{conn, handler},
    },
    net::{
        link::{egress::data, pool::recv::input::Execution, slot::types::CloseGate},
        wire::{self, receive::scoped},
    },
};
pub use failure::Failure;
use handler::{failures, retire, send};
use o3::buffer::resident;
pub(in crate::server::runtime) use report::Fault;
pub use report::Report;
use sark_core::{
    life::mode,
    manifold::{
        schedule,
        server::{
            app::{self, Spec},
            drain,
        },
    },
};

use crate::{
    conn::{Error, config, event},
    server::{
        self,
        runtime::{
            self, Connection, Context,
            input::{Fault as _, Read, Turn},
            modes::Deadline,
            output::{
                self,
                flush::{End, Flush},
            },
        },
        sync,
    },
};

pub struct App<A: Spec, W: wire::Spec = wire::Identity, F = failures::Close> {
    pub(super) app: A,
    pub(super) fault: F,
    machine: config::Validated<crate::ServerRole>,
    retention: Budget,
    wire: marker::PhantomData<fn() -> W>,
}

impl<A: Spec, W: wire::Spec, F> App<A, W, F> {
    pub fn new(app: A, plan: server::Plan, fault: F) -> Result<Self, config::Error> {
        let layout = app.layout();
        let minimum = plan.resident_len();
        let machine = plan.build_for(layout.streams())?;
        let machine = config::Validated::new(machine)?;
        Self::from_machine(app, layout, machine, minimum, fault)
    }

    fn from_machine(
        app: A,
        layout: app::Layout,
        machine: config::Validated<crate::ServerRole>,
        minimum: usize,
        fault: F,
    ) -> Result<Self, config::Error> {
        let bytes = layout.bytes();
        let per = crate::INGRESS_RETENTION
            .try_limit(bytes)
            .map_err(config::Error::IngressBudgetOverflow)?;
        Ok(Self {
            app,
            fault,
            machine,
            retention: Budget::new(per, minimum),
            wire: marker::PhantomData,
        })
    }

    fn resume<'d, ID: Identity>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
        mut egress: schedule::Egress,
    ) -> Result<drain::Progress, app::Error<Error, A::Failure>>
    where
        A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
        F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    {
        let owner = conn.id();
        let work = conn.work();
        let wake = conn.wake_target();
        let app = &mut self.app;
        let fault = &mut self.fault;
        let state = conn.state_mut();
        Read::new(
            state,
            output::State::protocol,
            |state: &mut State<'d, A, conn::Id<'d, ID>>| {
                let mut turn = app::Turn::new(owner, work, wake, driver);
                app.drive(
                    &mut state.app,
                    &mut state.carrier,
                    &mut turn,
                    mem::replace(&mut egress, schedule::Egress::Unchanged),
                    |cause| {
                        <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::diagnostic(
                            fault, owner, cause,
                        );
                    },
                )
            },
        )
        .resume::<<A as Spec>::Fields, event::Ignore>()
    }

    pub(super) fn fail<'d, ID: Identity>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
        error: app::Error<Error, A::Failure>,
    ) where
        A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
        F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    {
        let code = <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::failed(
            &mut self.fault,
            conn.id(),
            error,
        );
        let output = Flush::new(conn).finish(driver, Some(code), &mut mode::Disabled);
        <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
            &mut self.fault,
            conn.id(),
            output,
        );
    }

    pub(super) fn activate<'d, ID: Identity, T>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
        egress: schedule::Egress,
    ) where
        A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
        F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
        T: Deadline<'d, ID, A, W, F>,
    {
        let wake = conn.wake_target();
        let error = drain::Progress::error(self.resume(conn, driver, egress), wake);
        if let Some(error) = error {
            if error.connection().is_some_and(Error::is_pressure)
                && conn.state_mut().carrier.ingress().is_blocked()
                && !conn.state().carrier.output().is_empty()
            {
                T::flush(self, conn, driver);
                wake.wake();
                return;
            }
            self.fail(conn, driver, error);
            return;
        }
        T::flush(self, conn, driver);
    }

    fn read<'w, 'd: 'w, ID: Identity>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, 'w, 'd, ID, A, W>,
        chunk: scoped::Input<'_, 'w, 'd, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome
    where
        A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
        F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
        A::Timing: Deadline<'d, ID, A, W, F>,
    {
        let owner = conn.id();
        let work = conn.work();
        let wake = conn.wake_target();
        let app = &mut self.app;
        let fault = &mut self.fault;
        let mut source = Turn::new(chunk);
        let state = conn.state_mut();
        let result = Read::new(
            state,
            output::State::protocol,
            |state: &mut State<'d, A, conn::Id<'d, ID>>| {
                let mut turn = app::Turn::new(owner, work, wake, driver);
                app.drive(
                    &mut state.app,
                    &mut state.carrier,
                    &mut turn,
                    schedule::Egress::Unchanged,
                    |cause| {
                        <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::diagnostic(
                            fault, owner, cause,
                        );
                    },
                )
            },
        )
        .read::<<A as Spec>::Fields, event::Ignore, W>(&mut source);
        let error = drain::Progress::error(result, wake);
        if let Some(error) = error {
            if error.connection().is_some_and(Error::is_pressure)
                && conn.state_mut().carrier.ingress().is_blocked()
                && !conn.state().carrier.output().is_empty()
            {
                <A::Timing as Deadline<'d, ID, A, W, F>>::flush(self, conn, driver);
                wake.wake();
                return receive::Outcome::Continue;
            }
            source.discard();
            self.fail(conn, driver, error);
            return receive::Outcome::Continue;
        }
        <A::Timing as Deadline<'d, ID, A, W, F>>::flush(self, conn, driver);
        receive::Outcome::Continue
    }
}

impl<R, W, F> App<sync::Route<R>, W, F>
where
    sync::Route<R>: Spec,
    W: wire::Spec,
{
    pub(in crate::server) fn from_sync(
        router: R,
        plan: server::Plan,
        fault: F,
    ) -> Result<Self, config::Error> {
        let machine = config::Validated::new(plan.build()?)?;
        let minimum = plan.resident_len();
        let route = sync::Route::new(router, plan.config());
        let layout = route.layout();
        Self::from_machine(route, layout, machine, minimum, fault)
    }
}

impl<A: Spec, W: wire::Spec, F> Unpin for App<A, W, F> {}

impl<'d, ID: Identity, A, W, F> handler::Application<'d, ID> for App<A, W, F>
where
    A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    A::Timing: Deadline<'d, ID, A, W, F>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
{
    type Conn = State<'d, A, conn::Id<'d, ID>>;
    type Build = runtime::Build<A::Build>;
    type Wire = W;
    type WireFailure = F;
    type Retirement = retire::Close;
    type Egress = data::Bound<resident::Shared<'d>, A::Egress<'d>>;
    type Input = receive::Scoped;
    type Send = send::Run;

    fn timing(&self) -> handler::Timing {
        <A::Timing as Deadline<'d, ID, A, W, F>>::TIMING
    }

    fn timeout<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        <A::Timing as Deadline<'d, ID, A, W, F>>::timeout(self.get_mut(), &mut conn, driver)
    }

    fn drain<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain {
        conn.state_mut().begin_drain();
        let output = Flush::new(&mut conn).finish(driver, None, &mut mode::Disabled);
        <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
            &mut self.get_mut().fault,
            conn.id(),
            output,
        );
        match output::State::settle(conn.state_mut()) {
            End::Close => handler::Drain::Complete,
            End::Keep => handler::Drain::Pending,
        }
    }

    fn deadline(self: pin::Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn connection(self: pin::Pin<&Self>, seed: &random::Seed) -> Result<Self::Conn, Self::Build> {
        let this = self.get_ref();
        State::try_new(&this.app, this.machine, hash::Seed::new(seed))
    }

    fn build_failed(self: pin::Pin<&mut Self>, cause: Self::Build) {
        <F as Fault<'d, W, A::Failure, ID, Self::Build>>::construction(
            &mut self.get_mut().fault,
            cause,
        );
    }

    fn close_gate(
        self: pin::Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
    ) -> CloseGate {
        if conn.state().carrier.output().is_empty() || conn.is_terminal() {
            CloseGate::Immediate
        } else {
            CloseGate::Deferred
        }
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        self.get_mut()
            .activate::<ID, A::Timing>(&mut conn, driver, schedule::Egress::Unchanged);
        conn
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        let owner = conn.id();
        let this = self.get_mut();
        let state = conn.state_mut();
        this.app.close(&mut state.app, &mut state.carrier, owner);
        conn
    }
}

impl<'d, ID: Identity, A, W, F> send::Drive<'d, ID> for App<A, W, F>
where
    A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    A::Timing: Deadline<'d, ID, A, W, F>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
{
    fn send<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.get_mut()
            .activate::<ID, A::Timing>(&mut conn, driver, schedule::Egress::Released);
    }
}

impl<'w, 'd: 'w, ID: Identity, A, W, F> handler::Scoped<'w, 'd, ID> for App<A, W, F>
where
    A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    A::Timing: Deadline<'d, ID, A, W, F>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
{
    fn retention(&self) -> Budget {
        self.retention
    }

    fn scoped_chunk<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
        chunk: scoped::Input<'_, 'w, 'd, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        self.get_mut().read(&mut conn, chunk, driver)
    }
}

const _: () = {
    assert!(core::mem::size_of::<marker::PhantomData<fn(()) -> ()>>() == 0);
};
