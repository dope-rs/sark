use std::marker;

use conn::deadline::Arm;
use dope::{
    core::{
        self,
        driver::{self, retained, route::Identity, storage},
    },
    manifold::{
        app::receive,
        network::listener::{conn, handler, topology::factory},
    },
    net::{link::pool::recv::input, wire},
    runtime::topology::recipe,
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
pub trait Family<S, ID: Identity>: recipe::Build<S>
where
    S: storage::Plan,
{
    type Failure;
    type Application<'s, 'life, 'p, 'd>: handler::Application<'d, ID, Wire = wire::Identity, Input = receive::Scoped>
        + handler::Runtime<'d, 'd, ID>
    where
        'd: 'p,
        'p: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd;

    fn with<'life, 'p, 'd, O>(
        service: Self::Output<'life, 'p, 'd>,
        plan: server::Plan,
        binding: driver::Binding<'d>,
        run: impl for<'s> FnOnce(Self::Application<'s, 'life, 'p, 'd>) -> O,
    ) -> Result<O, Self::Failure>
    where
        'd: 'p,
        'p: 'life,
        Self: 'd,
        ID: 'd,
        S: 'd;
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
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn timeout(
        run: &mut App<A, W, F>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
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
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let owner = conn.id();
        let work = conn.work();
        let wake = conn.wake_target();
        let app = &mut run.app;
        let fault = &mut run.fault;
        let result = loop {
            let output = Flush::new(conn).finish(driver, None, &mut mode::Disabled);
            match <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                fault, owner, output,
            ) {
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
                |cause| {
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
            let code = <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::failed(
                &mut run.fault,
                conn.id(),
                error,
            );
            let output = Flush::new(conn).finish(driver, Some(code), &mut mode::Disabled);
            <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                &mut run.fault,
                conn.id(),
                output,
            );
        }
    }

    fn timeout(
        _: &mut App<A, W, F>,
        _: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        _: &mut retained::Context<'_, '_, 'd>,
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
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        <timing::Off as Deadline<'d, ID, A, W, F>>::flush(run, conn, driver);
        let due = app::Timed::deadline(&run.app, &conn.state().app);
        match state::try_sync(due, conn) {
            Arm::Armed => {}
            Arm::Disabled | Arm::Sealed | Arm::Overflow(_) => {
                let output = Flush::new(conn).finish(
                    driver,
                    Some(numbers::ErrorCode::InternalError),
                    &mut mode::Disabled,
                );
                <F as Fault<'d, W, A::Failure, ID, runtime::Build<A::Build>>>::output(
                    &mut run.fault,
                    conn.id(),
                    output,
                );
            }
        }
    }

    fn timeout(
        run: &mut App<A, W, F>,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, A, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        run.activate::<ID, timing::Off>(conn, driver, schedule::Egress::Unchanged);
        let update = app::Timed::deadline(&run.app, &conn.state().app)
            .map_or(handler::Deadline::Clear, handler::Deadline::Arm);
        handler::Timeout::Continue(update)
    }
}
