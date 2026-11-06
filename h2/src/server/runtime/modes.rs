use std::marker;

use dope::{
    core::{
        self,
        driver::{self, route::Identity},
    },
    manifold::{
        app::receive,
        network::listener::{conn, handler, topology::factory},
    },
    net::{link::pool::recv::input, wire},
};
use sark_core::{
    life::mode,
    manifold::{
        schedule,
        server::{app, timing},
    },
};

use crate::{
    conn::config,
    frame::numbers,
    server::{
        self,
        runtime::{
            self, Connection, Context,
            app::{App, Fault},
            output::{flush::Flush, track::Progress},
        },
        state, sync,
    },
};

pub trait Failure {
    type Error;
}

/// Builds a plaintext application while retaining its borrowed service storage.
pub trait Family<C, N, ID: Identity> {
    type Output;
    type Error;

    fn install(
        self,
        plan: runtime::Plan<ID>,
        context: &mut C,
        next: N,
    ) -> Result<Self::Output, Self::Error>;
}

pub struct Sync;
pub struct Serve<E>(marker::PhantomData<fn() -> E>);

/// Builds an application from a service owned directly by the startup node.
pub trait Direct<M, W: wire::Spec, ID: Identity>: Sized
where
    M: Failure + factory::ReceiveSpec,
{
    type Application<'d>: handler::Application<'d, ID, Wire = W, Input = M::Input>
        + handler::Runtime<'d, 'd, ID>
        + 'd
    where
        Self: 'd,
        W: 'd,
        ID: 'd,
        M::Input: 'd;

    fn build<'d>(self, plan: server::Plan) -> Result<Self::Application<'d>, M::Error>
    where
        Self: 'd,
        W: 'd,
        ID: 'd,
        M::Input: 'd;
}

impl Failure for Sync {
    type Error = config::Error;
}

impl<E> Failure for Serve<E> {
    type Error = core::BranchError<E, config::Error>;
}

impl factory::ReceiveSpec for Sync {
    type Input = receive::Scoped;
}

impl<E> factory::ReceiveSpec for Serve<E> {
    type Input = receive::Scoped;
}

impl<T, W, F, ID: Identity> Direct<Sync, W, ID> for (T, F)
where
    T: for<'d> sync::Dispatch<'d>,
    W: input::Execution,
    receive::Scoped: receive::Mode<W>,
    for<'d> App<sync::Route<T>, W, F>: handler::Runtime<'d, 'd, ID>
        + handler::Application<'d, ID, Wire = W, Input = receive::Scoped>,
{
    type Application<'d>
        = App<sync::Route<T>, W, F>
    where
        Self: 'd,
        W: 'd,
        ID: 'd,
        <Sync as factory::ReceiveSpec>::Input: 'd;

    fn build<'d>(
        self,
        plan: server::Plan,
    ) -> Result<Self::Application<'d>, <Sync as Failure>::Error>
    where
        Self: 'd,
        W: 'd,
        ID: 'd,
        <Sync as factory::ReceiveSpec>::Input: 'd,
    {
        App::from_sync(self.0, plan, self.1)
    }
}
pub(super) trait Deadline<'d, ID: Identity, A, W, F>
where
    A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
    W: wire::Spec,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
{
    const TIMING: handler::Timing;

    fn flush(
        run: &mut App<A, W, F>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn timeout(
        run: &mut App<A, W, F>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d>;
}

impl<'d, ID: Identity, A, W, F> Deadline<'d, ID, A, W, F> for timing::Off
where
    A: app::Application<'d, Connection<'d, A>, conn::Id<'d, ID>>,
    W: wire::Spec,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
{
    const TIMING: handler::Timing = handler::Timing::disabled();

    fn flush(
        run: &mut App<A, W, F>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let owner = conn.id();
        let work = conn.work();
        let wake = conn.wake_target();
        let app = &mut run.app;
        let fault = &mut run.fault;
        let result = loop {
            let progress = match Flush::new(conn).finish(driver, None, &mut mode::Disabled) {
                Ok(progress) => progress,
                Err((cause, progress)) => {
                    <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                        fault, owner, cause,
                    );
                    progress
                }
            };
            match progress {
                Progress::Blocked => break Ok(()),
                Progress::Committed => {}
            }
            let state = conn.state_mut();
            if state.carrier.status().is_closing() {
                continue;
            }
            let mut turn = app::Turn::new(owner, work, wake, driver);
            let progress = match app.drive(
                &mut state.app,
                &mut state.carrier,
                &mut turn,
                schedule::Egress::Released,
                &mut *fault,
                |_, conn, drain, _| Ok(drain.next(conn)),
                |_, _| {},
                |_, _| {},
                |fault, cause| {
                    <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::diagnostic(
                        fault, owner, cause,
                    );
                },
            ) {
                Ok(progress) => progress,
                Err(error) => break Err(error),
            };
            if progress.is_exhausted() {
                wake.wake();
            }
        };
        if let Err(error) = result {
            run.fail(conn, driver, error);
        }
    }

    fn timeout(
        _: &mut App<A, W, F>,
        _: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        _: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        handler::Timeout::Discard
    }
}

impl<'d, ID: Identity, A, W, F> Deadline<'d, ID, A, W, F> for timing::On
where
    A: app::Timed<'d, Connection<'d, A>, conn::Id<'d, ID>>,
    W: wire::Spec,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
{
    const TIMING: handler::Timing = handler::Timing::managed();

    fn flush(
        run: &mut App<A, W, F>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        <timing::Off as Deadline<'d, ID, A, W, F>>::flush(run, conn, driver);
        let due = app::Timed::deadline(&run.app, &conn.state().app);
        if state::try_sync(due, conn).is_err()
            && let Err((cause, _)) = Flush::new(conn).finish(
                driver,
                Some(numbers::ErrorCode::InternalError),
                &mut mode::Disabled,
            )
        {
            <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                &mut run.fault,
                conn.id(),
                cause,
            );
        }
    }

    fn timeout(
        run: &mut App<A, W, F>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        run.activate::<ID, timing::Off>(conn, driver, schedule::Egress::Unchanged);
        let update = app::Timed::deadline(&run.app, &conn.state().app)
            .map_or(handler::Deadline::Clear, handler::Deadline::Arm);
        handler::Timeout::Continue(update)
    }
}
