use std::pin;

use dope::{
    core::driver::{retained, route::Identity, schedule::ready},
    manifold::network::listener::handler,
    net::wire,
};
use ready::task::owner;
use request::Head;
use sark_core::{
    decode::request,
    manifold::{schedule, task::app::Runtime},
};

use crate::{
    conn,
    server::{
        self,
        response::Handoff,
        runtime::output::flush::{self, Flush},
        state,
        task::{
            Context, Sync, Target, Transport,
            app::{
                finish_transport,
                turn::{Availability, schedule::Schedule},
            },
            arm_transport, book, registry,
            response::{deadline::Deadline, expiry},
            timers,
        },
    },
};

pub(in crate::server::task::app) struct Send<'a, 'd, ID: Identity + 'd, R, M, D>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    D: timers::Policy<'d>,
{
    pub(in crate::server::task::app) router: pin::Pin<&'a mut R>,
    pub(in crate::server::task::app) tasks: &'a mut registry::Table<'d, ID, R::Task>,
    pub(in crate::server::task::app) mode: &'a mut M,
    pub(in crate::server::task::app) deadlines: &'a D,
    pub(in crate::server::task::app) limits: server::Config,
}

impl<'d, ID: Identity + 'd, R, M, D> Send<'_, 'd, ID, R, M, D>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    <R::Head as Head>::Start: Schedule<'d, R>,
{
    pub(in crate::server::task::app) fn apply<W>(
        mut self,
        mut conn: Context<'_, '_, '_, '_, '_, 'd, ID, R, W, M, D>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        W: wire::Spec,
    {
        let id = conn.id();
        let parent = conn.wake_target();
        let work = conn.work();
        let scope = owner::Scope::Exact(owner::Owner::from(id));
        let tasks = if Runtime::has_work(self.router.as_ref(), scope) {
            Availability::Ready
        } else {
            Availability::Idle
        };
        let now = driver.driver_ref().clock().now();
        let (wake, due, change, fault) = {
            let state = conn.state_mut();
            let input =
                if state.carrier.ingress().has_event() || state.carrier.ingress().is_blocked() {
                    Availability::Ready
                } else {
                    Availability::Idle
                };
            let mut mode =
                state::Projection::new(self.mode, &mut state.deadlines, now, self.deadlines);
            let mut book = book::Turn::new(&mut state.carrier, &mut state.book);
            let local = <R::Head as Head>::Start::has_local(&mut book);
            let due = expiry::Expiry::new(mode.poll_due()).apply(
                self.router.as_mut(),
                self.tasks,
                id,
                &mut state.task_index,
                &mut book,
                &mut mode,
            );
            if matches!(due, Deadline::Ready) {
                match (local, tasks, input) {
                    (Availability::Idle, Availability::Idle, Availability::Idle) => book
                        .pump::<false, _, _, _>(
                            self.router.as_mut(),
                            id,
                            self.limits,
                            schedule::Egress::Released,
                            work,
                            &mut mode,
                        ),
                    _ => book.pump::<true, _, _, _>(
                        self.router.as_mut(),
                        id,
                        self.limits,
                        schedule::Egress::Released,
                        work,
                        &mut mode,
                    ),
                };
            }
            let local = <R::Head as Head>::Start::has_local(&mut book);
            let active = if book.has_input() || book.responses.has_runnable() {
                Availability::Ready
            } else {
                Availability::Idle
            };
            let wake = match (local, input, active) {
                (Availability::Idle, Availability::Idle, Availability::Idle) => Availability::Idle,
                _ => Availability::Ready,
            };
            let (change, error) = mode.finish();
            (wake, due, change, error)
        };
        match change {
            timers::Change::Changed => {
                let next = {
                    let state = conn.state_mut();
                    timers::Policy::controller(self.deadlines, &mut state.deadlines)
                        .and_then(|timer| timer.deadline())
                };
                let arm = state::try_sync(next, &mut conn);
                if matches!(
                    arm_transport::<ID, R, _, _, _>(&mut conn, driver, arm, self.mode),
                    Sync::Finished
                ) {
                    return;
                }
            }
            timers::Change::Unchanged => {}
        }
        if let Some(error) = fault {
            finish_transport::<ID, R, _, _, _>(
                &mut conn,
                driver,
                Some(conn::Error::Deadline(error)),
                self.mode,
            );
            return;
        }
        if matches!(due, Deadline::Critical) {
            finish_transport::<ID, R, _, _, _>(
                &mut conn,
                driver,
                Some(conn::Error::CriticalDeadline),
                self.mode,
            );
            return;
        }
        let drain = conn.state_mut().finish_drain();
        if !conn.state().carrier.output().is_empty() || matches!(drain, handler::Drain::Complete) {
            Flush::new(&mut conn).observe(driver, None, self.mode);
        }
        if matches!(due, Deadline::Expired) {
            if matches!(wake, Availability::Ready) {
                parent.wake();
            }
            return;
        }
        if matches!(tasks, Availability::Ready) {
            Runtime::wake(self.router.as_mut(), scope);
        }
        if matches!(wake, Availability::Ready) || matches!(due, Deadline::Wake) {
            parent.wake();
        }
    }
}
