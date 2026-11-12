//! Static host for protocol-independent server applications.
use crate::server::app::{self, Spec};
use dope::core::{driver::route::Identity, random};
use sark_http::payload::compress::dynamic::hash;

mod state;
pub use state::State;
mod failure;
mod report;
use std::{marker, mem, pin};

use dope::{
    core::driver::{self, schedule::clock},
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
pub use report::Report;
pub(in crate::server) use report::{Fault, response};
use sark_core::{
    life::mode,
    manifold::{schedule, server::drain},
};

use crate::{
    conn::{config, ingress::PrefacePhase, preface},
    server::{
        self,
        runtime::{
            self, Context,
            input::{self, Read, Turn},
            modes::Deadline,
            output::{
                self,
                flush::{self, End, Evidence},
            },
        },
    },
};

pub struct App<
    'd,
    A: Spec,
    W: wire::Spec = wire::Identity,
    F = failures::Close,
    E = resident::Shared<'d>,
> {
    pub(super) app: A,
    pub(super) fault: F,
    machine: config::Validated<crate::ServerRole>,
    retention: Budget,
    wire: marker::PhantomData<fn() -> (W, E, &'d ())>,
}

impl<'d, A: Spec, W: wire::Spec, F, E: data::raw::Payload<'d>> App<'d, A, W, F, E> {
    pub fn new(app: A, plan: server::Plan, fault: F) -> Result<Self, config::Error> {
        let layout = app.layout();
        let minimum = plan.resident_len();
        let machine = plan.build_for(layout.streams())?;
        let machine = config::Validated::new(machine)?;
        Self::from_machine(app, layout, machine, minimum, fault)
    }

    pub(super) fn from_machine(
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

    pub fn connection_after_preface<ID: Identity>(
        &self,
        seed: &random::Seed,
        _: preface::Complete,
    ) -> Result<State<'d, A, conn::Id<'d, ID>>, runtime::Build<A::Build>>
    where
        A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
    {
        State::try_new(
            &self.app,
            &self.machine,
            hash::Seed::new(seed),
            PrefacePhase::InitialSettings,
        )
    }

    #[inline(always)]
    pub(super) fn resume<'w, ID: Identity>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, 'w, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
        mut egress: schedule::Egress,
        source: Option<&mut Turn<'_, 'w, 'd, W>>,
    ) -> Result<drain::Progress, app::Error<A::Failure>>
    where
        'd: 'w,
        A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
        F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    {
        let owner = conn.id();
        let work = conn.work();
        let wake = conn.wake_target();
        let app = &mut self.app;
        let fault = &mut self.fault;
        let state = conn.state_mut();
        let mut read = Read::new(
            state,
            output::State::protocol,
            |state: &mut State<'d, A, conn::Id<'d, ID>>, _| {
                let mut turn = app::Turn::new(owner, work, wake, driver);
                app.drive(
                    &mut state.app,
                    &mut state.carrier,
                    &mut turn,
                    mem::replace(&mut egress, schedule::Egress::Unchanged),
                    &mut *fault,
                    |_, conn, drain, _| Ok(drain.next(conn)),
                    |_, _| {},
                    |_, _| {},
                    |fault, cause| {
                        <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::diagnostic(
                            fault, owner, cause,
                        );
                    },
                )
            },
        );
        match source {
            Some(source) => read.read(source),
            None => read.resume(),
        }
    }

    pub(super) fn fail<ID: Identity>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
        error: app::Error<A::Failure>,
    ) where
        A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
        F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
        E: Evidence<'d>,
    {
        let code = <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::failed(
            &mut self.fault,
            conn.id(),
            error,
        );
        if let Err((cause, _)) = flush::finish(conn, driver, Some(code), &mut mode::Disabled) {
            <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                &mut self.fault,
                conn.id(),
                cause,
            );
        }
    }

    #[inline(always)]
    pub(super) fn activate<'w, ID: Identity, T>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, 'w, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
        egress: schedule::Egress,
        mut source: Option<Turn<'_, 'w, 'd, W>>,
    ) where
        'd: 'w,
        A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
        F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
        E: Evidence<'d>,
        T: Deadline<'d, ID, A, W, F, E>,
    {
        let wake = conn.wake_target();
        let error =
            drain::Progress::error(self.resume(conn, driver, egress, source.as_mut()), wake);
        if let Some(error) = error
            && !input::pending(conn, &error)
        {
            if let Some(source) = &mut source {
                source.discard();
            }
            self.fail(conn, driver, error);
            return;
        }
        T::flush(self, conn, driver);
    }
}

impl<A: Spec, W: wire::Spec, F, E> Unpin for App<'_, A, W, F, E> {}

impl<'d, ID: Identity, A, W, F, E> handler::Application<'d, ID> for App<'d, A, W, F, E>
where
    A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    A::Timing: Deadline<'d, ID, A, W, F, E>,
    E: Evidence<'d> + 'd,
    W: Execution + 'd,
    receive::Scoped<true>: receive::Mode<W>,
{
    type Conn = State<'d, A, conn::Id<'d, ID>>;
    type Build = runtime::Build<A::Build>;
    type Wire = W;
    type WireFailure = F;
    type Retirement = retire::Close;
    type Egress = data::Bound<E, A::Egress<'d>>;
    type Input = receive::Scoped<true>;
    type Send = send::Run;

    fn is_gated(conn: &Self::Conn) -> bool {
        conn.carrier.status().input_blocked()
    }

    fn is_blocked(
        self: pin::Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
    ) -> bool {
        !conn.state().carrier.output().is_empty()
    }

    fn timing(&self) -> handler::Timing {
        <A::Timing as Deadline<'d, ID, A, W, F, E>>::TIMING
    }

    fn timeout<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        <A::Timing as Deadline<'d, ID, A, W, F, E>>::timeout(self.get_mut(), &mut conn, driver)
    }

    fn drain<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Drain {
        conn.state_mut().begin_drain();
        if let Err((cause, _)) = flush::finish(&mut conn, driver, None, &mut mode::Disabled) {
            <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                &mut self.get_mut().fault,
                conn.id(),
                cause,
            );
        }
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
        State::try_new(
            &this.app,
            &this.machine,
            hash::Seed::new(seed),
            PrefacePhase::Magic,
        )
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

    fn activate<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.get_mut().activate::<ID, A::Timing>(
            &mut conn,
            driver,
            schedule::Egress::Unchanged,
            None,
        );
    }

    fn close<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
    ) {
        let owner = conn.id();
        let this = self.get_mut();
        let state = conn.state_mut();
        this.app.close(&mut state.app, &mut state.carrier, owner);
    }
}

impl<'d, ID: Identity, A, W, F, E> send::Drive<'d, ID> for App<'d, A, W, F, E>
where
    A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    A::Timing: Deadline<'d, ID, A, W, F, E>,
    E: Evidence<'d> + 'd,
    W: Execution + 'd,
    receive::Scoped<true>: receive::Mode<W>,
{
    fn send<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.get_mut().activate::<ID, A::Timing>(
            &mut conn,
            driver,
            schedule::Egress::Released,
            None,
        );
    }
}

impl<'w, 'd: 'w, ID: Identity, A, W, F, E> handler::Scoped<'w, 'd, ID> for App<'d, A, W, F, E>
where
    A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    A::Timing: Deadline<'d, ID, A, W, F, E>,
    E: Evidence<'d> + 'd,
    W: Execution + 'd,
    receive::Scoped<true>: receive::Mode<W>,
{
    fn retention(&self) -> Budget {
        self.retention
    }

    fn scoped_chunk<'e>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, W, Self::Conn, Self::Egress>,
        chunk: scoped::Input<'_, 'w, 'd, W>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> receive::Outcome {
        self.get_mut().activate::<ID, A::Timing>(
            &mut conn,
            driver,
            schedule::Egress::Unchanged,
            Some(Turn::new(chunk)),
        );
        receive::Outcome::Continue
    }
}

const _: () = {
    assert!(core::mem::size_of::<marker::PhantomData<fn(()) -> ()>>() == 0);
};
