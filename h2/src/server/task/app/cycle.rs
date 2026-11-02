use std::{marker, pin};

use dope::{
    core::driver::{retained, route::Identity, schedule::ready},
    manifold::network::listener::handler,
    net::wire,
};
use o3::mem::quota;
use ready::task::owner;
use sark_core::manifold::task::app::Runtime;

use crate::{
    conn,
    server::{
        response::Handoff,
        runtime::output::flush::{self, Flush},
        state,
        task::{
            Context,
            app::{turn, turn::schedule::Schedule, *},
            response::{deadline::Deadline, expiry},
            timers, works, *,
        },
    },
};

pub(super) struct Cycle<'a, 'd, ID: Identity + 'd, R, W, M, D, S>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    W: wire::Spec,
    S: Schedule<'d, R>,
{
    router: pin::Pin<&'a mut R>,
    tasks: &'a mut registry::Table<'d, ID, R::Task>,
    protocol: &'a Protocol,
    exec: &'a mut works::State<S>,
    mode: &'a mut M,
    timers: &'a mut D,
    marker: marker::PhantomData<fn() -> W>,
}

impl<'a, 'd, ID: Identity + 'd, R, W, M, D, S> Cycle<'a, 'd, ID, R, W, M, D, S>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    W: wire::Spec,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    D: timers::Policy<'d>,
    S: Schedule<'d, R>,
{
    pub(super) fn new(
        router: pin::Pin<&'a mut R>,
        tasks: &'a mut registry::Table<'d, ID, R::Task>,
        protocol: &'a Protocol,
        exec: &'a mut works::State<S>,
        mode: &'a mut M,
        timers: &'a mut D,
    ) -> Self {
        Self {
            router,
            tasks,
            protocol,
            exec,
            mode,
            timers,
            marker: marker::PhantomData,
        }
    }

    pub(super) fn run(
        self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, R, W, M, D>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        M::Retention: Handoff,
    {
        let Self {
            mut router,
            tasks,
            protocol,
            exec,
            mode,
            timers,
            marker: _,
        } = self;
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
        let (result, expiry, change, fault) = {
            let state = conn.state_mut();
            let builds = if state.book.build_ready.is_empty() {
                turn::Availability::Idle
            } else {
                turn::Availability::Ready
            };
            let input =
                if state.carrier.ingress().has_event() || state.carrier.ingress().is_blocked() {
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
            let expiry = expiry::Expiry::new(mode.poll_due()).apply(
                router.as_mut(),
                tasks,
                id,
                index,
                &mut turn,
                &mut mode,
            );
            let next = if matches!(expiry, Deadline::Ready) {
                let scope = turn::Scope {
                    router: router.as_mut(),
                    tasks,
                    conn: id,
                    parent,
                    index,
                };
                let ready = turn::Ready {
                    build: builds,
                    task,
                    input,
                    output,
                };
                let mut work = turn::Turn::new(
                    scope,
                    &mut turn,
                    protocol.limits,
                    &mut mode,
                    work,
                    driver.reborrow(),
                    ready,
                );
                S::admit(&mut exec.graph, &mut work)
            } else {
                quota::Admission::Empty
            };
            let result = match next {
                quota::Admission::Item(Ok(turn::Outcome::Account)) => {
                    S::account(&mut exec.graph);
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
            let (change, error) = mode.finish();
            if matches!(S::is_ready(&mut turn), turn::Availability::Ready) {
                parent.wake();
            }
            (result, expiry, change, error)
        };
        match change {
            timers::Change::Changed => {
                let deadline = {
                    let state = conn.state_mut();
                    timers::Policy::controller(timers, &mut state.deadlines)
                        .and_then(|timer| timer.deadline())
                };
                let arm = state::try_sync(deadline, conn);
                if matches!(
                    arm_transport::<ID, R, _, _, _>(conn, driver, arm, mode),
                    Sync::Finished
                ) {
                    return;
                }
            }
            timers::Change::Unchanged => {}
        }
        let result = match fault {
            Some(error) => Err(conn::Error::Deadline(error)),
            None => result,
        };
        if matches!(expiry, Deadline::Critical) {
            finish_transport::<ID, R, _, _, _>(
                conn,
                driver,
                Some(conn::Error::CriticalDeadline),
                mode,
            );
            return;
        }
        if let Err(error) = result {
            finish_transport::<ID, R, _, _, _>(conn, driver, Some(error), mode);
            return;
        }
        if matches!(expiry, Deadline::Wake) {
            parent.wake();
        }
        if matches!(expiry, Deadline::Expired) {
            if !conn.state().carrier.output().is_empty() {
                Flush::new(conn).observe(driver, None, mode);
            }
            parent.wake();
            return;
        }
        let drain = conn.state_mut().finish_drain();
        if !conn.state().carrier.output().is_empty() || matches!(drain, handler::Drain::Complete) {
            Flush::new(conn).observe(driver, None, mode);
        }
    }
}
