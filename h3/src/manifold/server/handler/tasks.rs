//! HTTP/3 application-budget adapter for generated tasks.
use std::{fmt, mem, pin};

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
use driver::schedule::{app, ready};
use mux::app::{Driver, Selected as _, Set, Turn};
use o3::{buffer::resident, mem::quota};
use ready::task::owner;
use sark_core::{
    life,
    life::{endpoint, exchange},
    manifold::task::{
        app::{Handoff, Runtime, Wake},
        response,
    },
};
use task::ingress::{item::Item, turn};

use crate::manifold::{
    owners, server,
    server::{deadline, deadline::state::State, failure, rotation, task},
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

struct Sink<'a, O>(&'a mut O);

impl<'d, O, F: response::Completion<'d>, const ID: u8> Handoff<'d, server::Handle<'d, ID>, F>
    for Sink<'_, O>
where
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + failure::Sink<id::Handle<id::Driver<'d>, ID>>,
{
    type Ready = (server::Handle<'d, ID>, task::Output<'d, F>);

    fn pending(self, target: server::Handle<'d, ID>) {
        life::Mode::observe(
            self.0,
            life::Event::TaskPollPending(exchange::Id::new(target.connection(), target.stream())),
        );
    }

    fn complete(self, target: server::Handle<'d, ID>, output: F) -> Self::Ready {
        (target, task::Output::Fixed(output))
    }

    fn ready<E: fmt::Debug>(
        self,
        target: server::Handle<'d, ID>,
        event: response::Event<'d, E>,
    ) -> Self::Ready {
        let output = match event {
            response::Event::Head => task::Output::Head,
            response::Event::Chunk(chunk) => task::Output::Body(Some(chunk)),
            response::Event::End => task::Output::Body(None),
            response::Event::Failed(error) => {
                failure::Sink::failure(
                    self.0,
                    failure::Event::Response {
                        conn: target.connection(),
                        stream: target.stream(),
                        error: &error,
                    },
                );
                task::Output::Failed
            }
        };
        (target, output)
    }
}

impl<'d, R, O, L, E, Q, const ID: u8, B>
    Driver<'d, server::Handler<'d, R, O, L, E, Q>, ID, B, resident::Shared<'d>> for Tasks
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + endpoint::Mode<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>
        + failure::Sink<id::Handle<id::Driver<'d>, ID>>,
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
            Connection = server::Session<'d, R, L, E::State, O::Retention>,
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
        driver: &mut driver::Context<'_, 'd>,
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
                        let this = task.as_mut().project();
                        turn::drive(
                            Item::new(
                                state,
                                this.router,
                                this.observer,
                                conn,
                                handle,
                                driver.driver_ref().clock().now(),
                            ),
                            this.lifetime,
                            parent,
                        );
                        conn.application().sync(state.deadline());
                        state.has_work()
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
                let turn = Runtime::drive(
                    router.as_mut(),
                    owner::Scope::Any,
                    work,
                    driver,
                    Sink(&mut *life),
                );
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
                    Turn::Ready((target, output), step) => {
                        match conns.try_select(target.connection()) {
                            Ok(pick) => pick.run(|mut slot| {
                                let (handle, state, conn) = slot.split();
                                Item::new(state, router.as_mut(), life, conn, handle, now)
                                    .respond(target.request(), output);
                                let active = state.has_work();
                                let due = state.deadline();
                                let mut app = conn.application();
                                app.set_ready(active);
                                app.sync(due);
                            }),
                            Err(reject) => match reject.discard() {
                                errors::Access::Closed | errors::Access::InvalidHandle => {}
                            },
                        }
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
