use crate::server::app;
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
    manifold::{schedule, server::timing},
};

use crate::{
    conn::config,
    frame::numbers,
    server::{
        self,
        runtime::{
            self, Context,
            app::{App, Fault},
            output::flush::{self, Evidence, Progress},
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
    type Input = receive::Scoped<true>;
}

impl<E> factory::ReceiveSpec for Serve<E> {
    type Input = receive::Scoped<true>;
}

impl<T, W, F, ID: Identity> Direct<Sync, W, ID> for (T, F)
where
    T: for<'d> sync::Dispatch<'d>,
    W: input::Execution,
    receive::Scoped<true>: receive::Mode<W>,
    for<'d> App<'d, sync::Route<T>, W, F>: handler::Runtime<'d, 'd, ID>
        + handler::Application<'d, ID, Wire = W, Input = receive::Scoped<true>>,
{
    type Application<'d>
        = App<'d, sync::Route<T>, W, F>
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
        let machine = config::Validated::new(plan.build()?)?;
        let minimum = plan.resident_len();
        let route = sync::Route::new(self.0, plan.config());
        let layout = app::Spec::layout(&route);
        App::from_machine(route, layout, machine, minimum, self.1)
    }
}
pub(super) trait Deadline<'d, ID: Identity, A, W, F, E>
where
    A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
    W: wire::Spec,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    E: Evidence<'d>,
{
    const TIMING: handler::Timing;

    fn flush(
        run: &mut App<'d, A, W, F, E>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn timeout(
        run: &mut App<'d, A, W, F, E>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d>;
}

impl<'d, ID: Identity, A, W, F, E> Deadline<'d, ID, A, W, F, E> for timing::Off
where
    A: app::Application<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
    W: wire::Spec,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    E: Evidence<'d>,
{
    const TIMING: handler::Timing = handler::Timing::disabled();

    fn flush(
        run: &mut App<'d, A, W, F, E>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let owner = conn.id();
        let wake = conn.wake_target();
        loop {
            let progress = match flush::finish(conn, driver, None, &mut mode::Disabled) {
                Ok(progress) => progress,
                Err((cause, progress)) => {
                    <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                        &mut run.fault,
                        owner,
                        cause,
                    );
                    progress
                }
            };
            if matches!(progress, Progress::Blocked) {
                return;
            }
            if conn.state().carrier.status().is_closing() {
                continue;
            }
            let progress = match run.resume(conn, driver, schedule::Egress::Released, None) {
                Ok(progress) => progress,
                Err(error) if runtime::input::pending(conn, &error) => continue,
                Err(error) => return run.fail(conn, driver, error),
            };
            if progress.is_exhausted() {
                wake.wake();
            }
        }
    }

    fn timeout(
        _: &mut App<'d, A, W, F, E>,
        _: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W, E>,
        _: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        handler::Timeout::Discard
    }
}

impl<'d, ID: Identity, A, W, F, E> Deadline<'d, ID, A, W, F, E> for timing::On
where
    A: app::Timed<'d, crate::conn::event::Ignore, conn::Id<'d, ID>>,
    W: wire::Spec,
    F: Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>,
    E: Evidence<'d>,
{
    const TIMING: handler::Timing = handler::Timing::managed();

    fn flush(
        run: &mut App<'d, A, W, F, E>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        <timing::Off as Deadline<'d, ID, A, W, F, E>>::flush(run, conn, driver);
        if conn.state().carrier.status().is_closing() {
            return;
        }
        let due = app::Timed::deadline(&run.app, &conn.state().app);
        if let Err(cause) = state::try_sync(due, conn) {
            conn.deadline().reject(cause);
            if let Err((cause, _)) = flush::finish(
                conn,
                driver,
                Some(numbers::ErrorCode::InternalError),
                &mut mode::Disabled,
            ) {
                <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                    &mut run.fault,
                    conn.id(),
                    cause,
                );
            }
        }
    }

    fn timeout(
        run: &mut App<'d, A, W, F, E>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W, E>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        run.activate::<ID, timing::Off>(conn, driver, schedule::Egress::Unchanged, None);
        if conn.state().carrier.status().is_closing() {
            return handler::Timeout::Discard;
        }
        let update = app::Timed::deadline(&run.app, &conn.state().app)
            .map_or(handler::Deadline::Clear, handler::Deadline::Arm);
        handler::Timeout::Continue(update)
    }
}
