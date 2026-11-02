//! HTTP/3 application-budget adapter for generated tasks.
use std::{marker, mem, pin};

use app::step;
use dope::core::driver;
use dope_quic::{
    conn::id,
    errors,
    runtime::{
        mux,
        mux::{handler, handler::net},
    },
    stream,
    wire::stream::Id,
};
use driver::{
    retained,
    schedule::{app, clock, ready},
};
use mux::app::{Driver, Selected as _, Set, Turn};
use o3::{buffer::resident, mem::quota};
use ready::task::owner;
use sark_core::{
    life,
    life::{endpoint, exchange},
    manifold::task::{
        app::{Handoff, Runtime, Wake},
        response::{Output, Pack},
    },
    route,
};
use task::ingress::{item::Item, turn};

use crate::manifold::{
    owners, server,
    server::{deadline, deadline::state::State, rotation, task},
};

#[derive(Clone, Copy, dope::WorkGraph)]
enum Work {
    Build,
    Task,
}

/// QUIC endpoint policy for generated Sark tasks.
pub struct Tasks {
    work: WorkGraph,
}

impl Tasks {
    pub fn new() -> Self {
        Self {
            work: WorkGraph::new(),
        }
    }
}

impl Default for Tasks {
    fn default() -> Self {
        Self::new()
    }
}

struct Sink<'a, 'd, C, H, O, B, const ID: u8> {
    conns: C,
    observer: &'a mut O,
    now: clock::Moment<'d>,
    state: marker::PhantomData<fn() -> H>,
    wire: marker::PhantomData<fn() -> B>,
}

impl<'d, C, R, O, L, E, Q, B, const ID: u8> Handoff<'d, server::Handle<'d, ID>, R::Completion>
    for Sink<'_, 'd, C, server::Handler<'d, R, O, L, E, Q>, O, B, ID>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + endpoint::Mode<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: deadline::Policy<'d>,
    E::State: State<'d>,
    B: stream::Retain<'d>,
    server::Handler<'d, R, O, L, E, Q>: handler::Handler<
            ID,
            B,
            resident::Shared<'d>,
            id::Driver<'d>,
            Connection = server::Session<'d, R, L, E::State>,
        >,
    C: Set<server::Handler<'d, R, O, L, E, Q>, ID, B, resident::Shared<'d>, id::Driver<'d>>,
{
    type Complete = ();

    fn pending(self, target: server::Handle<'d, ID>) {
        life::Mode::observe(
            self.observer,
            life::Event::TaskPollPending(exchange::Id::new(target.connection(), target.stream())),
        );
    }

    fn complete<Route, S, P>(
        self,
        target: server::Handle<'d, ID>,
        output: Output<'d, Route, S>,
    ) -> Self::Complete
    where
        Route: route::Task<'d, S>,
        P: Pack<'d, Route, S, R::Completion>,
    {
        let Sink {
            conns,
            observer: life,
            now,
            state: _,
            wire: _,
        } = self;
        let flow = exchange::Id::new(target.connection(), target.stream());
        life::Mode::observe(life, life::Event::TaskPollReady(flow));
        let handle = target.connection();
        let pick = match conns.try_select(handle) {
            Ok(pick) => pick,
            Err(reject) => match reject.discard() {
                errors::Access::Closed | errors::Access::InvalidHandle => {
                    return;
                }
            },
        };
        pick.run(|mut slot| {
            let (exact, state, conn) = slot.split();
            let mut state = task::Ready { state };
            state.finish_task::<ID, R::Completion, O, B, true>(
                conn,
                exact,
                target.request(),
                P::pack(output),
                life,
                now,
            );
            let ready = state.state.has_work();
            let due = state.deadline();
            let mut app = conn.application();
            app.set_ready(ready);
            app.sync(due);
        });
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B>
    Driver<'d, server::Handler<'d, R, O, L, E, Q>, ID, B, resident::Shared<'d>> for Tasks
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + endpoint::Mode<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: deadline::Policy<'d>,
    E::State: State<'d>,
    B: stream::Retain<'d>,
    server::Handler<'d, R, O, L, E, Q>: handler::Handler<
            ID,
            B,
            resident::Shared<'d>,
            id::Driver<'d>,
            Connection = server::Session<'d, R, L, E::State>,
        >,
{
    fn has_work(&self, task: pin::Pin<&server::Handler<'d, R, O, L, E, Q>>) -> bool {
        Runtime::has_work(task.project_ref().router, owner::Scope::Any)
    }

    fn drive<'scope, 'turn, C>(
        &mut self,
        turn: Turn<'scope, server::Handler<'d, R, O, L, E, Q>, C>,
        parent: ready::Target<'d>,
        work: app::Work<'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> quota::Admission<step::Flow>
    where
        C: Set<server::Handler<'d, R, O, L, E, Q>, ID, B, resident::Shared<'d>, id::Driver<'d>>,
    {
        let (mut task, conns) = turn.into_parts();
        let build = conns.has_ready();
        let tasks = Runtime::has_work(task.as_ref().project_ref().router, owner::Scope::Any);
        let source = match self.work.find(|source| match source {
            Work::Build => build,
            Work::Task => tasks,
        }) {
            Some(source) => source,
            None => return quota::Admission::Empty,
        };
        let admit = match source {
            Work::Build => {
                let Some(selected) = conns.next() else {
                    return quota::Admission::Empty;
                };
                let Some(permit) = work.permit() else {
                    parent.wake();
                    return quota::Admission::Exhausted;
                };
                let receipt = step::Receipt::direct(permit);
                let local = selected.run(|mut slot| {
                    let local = {
                        let (handle, state, conn) = slot.split();
                        let mut ready = task::Ready { state };
                        let this = task.as_mut().project();
                        turn::drive(
                            Item::new(
                                ready.state,
                                this.router,
                                this.observer,
                                conn,
                                handle,
                                driver.driver_ref().clock().now(),
                            ),
                            this.lifetime,
                            parent,
                        );
                        conn.application().sync(ready.deadline());
                        ready.state.has_work()
                    };
                    slot.set_ready(local);
                    local
                });
                let tasks =
                    Runtime::has_work(task.as_ref().project_ref().router, owner::Scope::Any);
                let flow = match local || tasks {
                    true => step::Flow::Batch,
                    false => step::Flow::Yield,
                };
                quota::Admission::Item(receipt.settle(flow))
            }
            Work::Task => {
                use sark_core::manifold::task::app::Turn;

                let this = task.as_mut().project();
                let (mut router, life) = (this.router, this.observer);
                let now = driver.driver_ref().clock().now();
                let sink = Sink::<_, server::Handler<'d, R, O, L, E, Q>, O, B, ID> {
                    conns,
                    observer: life,
                    now,
                    state: marker::PhantomData,
                    wire: marker::PhantomData,
                };
                let turn = Runtime::drive(router.as_mut(), owner::Scope::Any, work, driver, sink);
                if build {
                    parent.wake();
                }
                match turn {
                    Turn::Idle => quota::Admission::Empty,
                    Turn::Deferred => quota::Admission::Exhausted,
                    Turn::Pending(step) => {
                        let ready = matches!(
                            Runtime::try_wake(router.as_mut(), owner::Scope::Any),
                            Wake::Ready
                        );
                        let flow = match build || ready {
                            true => step::Flow::Batch,
                            false => step::Flow::Yield,
                        };
                        quota::Admission::Item(step.settle(flow))
                    }
                    Turn::PendingReady(step) => {
                        quota::Admission::Item(step.settle(step::Flow::Batch))
                    }
                    Turn::Complete((), step) => {
                        let ready = Runtime::has_work(router.as_ref(), owner::Scope::Any);
                        let flow = match build || ready {
                            true => step::Flow::Batch,
                            false => step::Flow::Yield,
                        };
                        quota::Admission::Item(step.settle(flow))
                    }
                }
            }
        };
        match admit {
            quota::Admission::Item(flow) => {
                self.work.account(source);
                quota::Admission::Item(flow)
            }
            quota::Admission::Empty => quota::Admission::Empty,
            quota::Admission::Exhausted => quota::Admission::Exhausted,
        }
    }
}

const _: () = assert!(mem::size_of::<Tasks>() == 1);
