use std::{mem, pin, task};

use dope::{
    core::driver::{retained, route::Identity},
    manifold::network::listener::conn::{
        self,
        write::{self, body},
    },
    net::{
        link::egress::{
            self,
            data::{self, compose},
        },
        wire,
    },
};
use dope_fiber::{
    abi, context,
    task::storage::{
        self,
        fixed::{local, staged},
    },
};
use o3::buffer::resident::bound;
use sark_core::{
    life::{self, exchange, sequence},
    manifold::task::response::Output,
    route,
};
use sark_h1::{
    pump,
    route::{PolicyOf, task::Egress},
    server::{
        payload,
        task::{runtime, turn},
    },
    session, switch,
    wire::{head, output, response},
};
use sark_http::payload::response::dispatch::chunk;

use crate::manifold::runtime::NativeFiber;

type Session<'d, K, S, G, P, Q, B, Ready, Heads> =
    session::Conn<'d, runtime::Runtime<'d, K, S, G, B>, P, Q, Ready, Heads>;
type Payload<'d, G> = compose::Either<payload::Body<'d, G>, payload::Stream<'d, G>>;

type Context<'a, 'b, 'c, 'e, 'f, 'd, ID, W, K, S, G, P, Q, B, Ready, Heads> = conn::Ctx<
    'a,
    'b,
    'c,
    'e,
    'f,
    'd,
    ID,
    W,
    Session<'d, K, S, G, P, Q, B, Ready, Heads>,
    data::Bound<Payload<'d, G>, B>,
>;

type ContextFor<'a, 'b, 'c, 'e, 'f, 'd, ID, W, K, M, P, Q, B, Ready, Heads> = Context<
    'a,
    'b,
    'c,
    'e,
    'f,
    'd,
    ID,
    W,
    K,
    <M as sequence::Mode<conn::Id<'d, ID>>>::Session,
    <M as sequence::Mode<conn::Id<'d, ID>>>::TransportTag,
    P,
    Q,
    B,
    Ready,
    Heads,
>;

/// One HTTP/1 task and the pinned store borrowed for its next turn.
pub struct H1<'a, 'd: 'a, T: local::Store<'d, D>, D> {
    tasks: pin::Pin<&'a mut T>,
    task: storage::Id<'d, D>,
    request: response::Context,
}

impl<'a, 'd: 'a, T: local::Store<'d, D>, D> H1<'a, 'd, T, D> {
    pub fn new(
        tasks: pin::Pin<&'a mut T>,
        task: storage::Id<'d, D>,
        request: response::Context,
    ) -> Self {
        const {
            let fields = mem::size_of::<pin::Pin<&mut T>>()
                + mem::size_of::<storage::Id<'d, D>>()
                + mem::size_of::<response::Context>();
            let align = mem::align_of::<Self>();
            assert!(mem::size_of::<Self>() == (fields + align - 1) & !(align - 1));
            assert!(mem::size_of::<Option<Self>>() == mem::size_of::<Self>());
        }
        Self {
            tasks,
            task,
            request,
        }
    }

    pub fn poll<'b, ID: Identity, W, M, R, K, P, Q, A, B: bound::Bound + 'd, Ready, Heads>(
        self,
        date: &'b [u8; 29],
        wrap: impl Fn(storage::Id<'d, D>) -> K,
        conn: &mut ContextFor<'_, '_, '_, '_, '_, 'd, ID, W, K, M, P, Q, B, Ready, Heads>,
        driver: &mut retained::Context<'_, '_, 'd>,
        life: &mut M,
        exchange: exchange::Id<conn::Id<'d, ID>, u64>,
    ) -> turn::Turn<<M as life::Mode<conn::Id<'d, ID>, u64>>::Retention>
    where
        R: route::Task<'d, A, Kind = NativeFiber> + PolicyOf,
        T: staged::Store<'d, D, Fiber: abi::Fiber<'d, Output = Output<'d, R, A>>>,
        Q: switch::Engine,
        W: wire::Spec,
        M: sequence::Mode<conn::Id<'d, ID>>,
        M::Retention: turn::Mode,
    {
        use payload::write::{Failure, Response};
        let Self {
            mut tasks,
            task,
            request,
        } = self;
        let resume = |task| runtime::Running::from_parts(wrap(task), runtime::Kind::Fiber, request);
        if <M as life::Mode<conn::Id<'d, ID>, u64>>::ENABLED && conn.work().remaining() == 0 {
            conn.state_mut().async_state.insert(resume(task));
            conn.wake_target().wake();
            return turn::Turn::Deferred;
        }
        let (result, ready) = {
            let poll = context::Context::with_target(
                conn.wake_target(),
                conn.work(),
                driver.reborrow(),
                |mut context| staged::Store::poll_ready(tasks.as_mut(), &task, context.as_mut()),
            );
            let (done, ready) = match poll {
                Some(task::Poll::Pending) => {
                    conn.state_mut().async_state.insert(resume(task));
                    return turn::Turn::Pending;
                }
                Some(task::Poll::Ready((done, fresh))) => (
                    done,
                    if fresh {
                        turn::Readiness::NewlyReady
                    } else {
                        turn::Readiness::Staged
                    },
                ),
                None => {
                    conn.close_after();
                    release(conn);
                    return turn::Turn::ConnectionCancelled;
                }
            };

            let result = conn.write().try_draft(driver, |state, write| {
                let output = staged::Completion::finish(done);
                let deferred = pump::Ingress::deferred(state);
                let outcome = Egress::new(write, date, deferred, request).finish::<R, A>(output);
                if let output::Outcome::Failed(error) = &outcome
                    && let Some(sink) = life::Mode::observer(life)
                {
                    life::Observer::diagnostic(sink, error);
                }
                let done = <M::Retention as turn::Mode>::complete(outcome.retention());
                let (plan, copy) = output::Plan::split(outcome);
                if let Some(sink) = life::Mode::observer(life)
                    && let output::Copy::Copied(copy) = copy
                {
                    life::Observer::observe(
                        sink,
                        life::Event::ResponseBodyCopied {
                            exchange,
                            read: copy.read,
                            written: copy.written,
                            cause: copy.cause,
                        },
                    );
                }
                Response::<M::TransportTag>::try_plan(
                    plan,
                    |intent| {
                        state.settle(intent);
                        done
                    },
                    |charge| sequence::Mode::tag(life, exchange, charge),
                )
            });
            (result, ready)
        };
        match result {
            Ok(write::Outcome::Committed(done)) => turn::Turn::retired(ready, done),
            Ok(write::Outcome::Deferred(
                write::Deferred::Turn | write::Deferred::Capacity | write::Deferred::OrderedDrain,
            )) => {
                conn.state_mut().async_state.insert(resume(task));
                match ready {
                    turn::Readiness::Staged => turn::Turn::StagedDeferred,
                    turn::Readiness::NewlyReady => turn::Turn::ReadyDeferred,
                }
            }
            Ok(write::Outcome::Closed) => match local::Store::remove(tasks.as_mut(), task) {
                local::Removal::Removed => {
                    release(conn);
                    turn::Turn::retired(ready, turn::Completion::Rejected)
                }
                local::Removal::Absent => {
                    conn.close_after();
                    release(conn);
                    turn::Turn::ConnectionCancelled
                }
            },
            Err(cause) => {
                let (done, failure, removed) = match Response::<M::TransportTag>::reject(cause) {
                    Failure::Capacity(cause) => (
                        turn::Completion::Rejected,
                        body::Failure::Capacity(cause),
                        local::Store::remove(tasks.as_mut(), task),
                    ),
                    Failure::Rejected { cause, receipt } => {
                        (receipt.reject(), cause, local::Removal::Removed)
                    }
                };
                conn.write().reject(failure);
                release(conn);
                match removed {
                    local::Removal::Removed => turn::Turn::retired(ready, done),
                    local::Removal::Absent => turn::Turn::ConnectionCancelled,
                }
            }
        }
    }

    /// Streams one response body frame while retaining its exact type until commit.
    pub fn stream<ID: Identity, W, M, K, P, Q, B: bound::Bound + 'd, Ready, Heads>(
        self,
        mut kind: runtime::Kind,
        wrap: impl Fn(storage::Id<'d, D>) -> K,
        conn: &mut ContextFor<'_, '_, '_, '_, '_, 'd, ID, W, K, M, P, Q, B, Ready, Heads>,
        driver: &mut retained::Context<'_, '_, 'd>,
        life: &mut M,
        exchange: exchange::Id<conn::Id<'d, ID>, u64>,
    ) -> turn::Turn<<M as life::Mode<conn::Id<'d, ID>, u64>>::Retention>
    where
        T: local::Stream<'d, D, Fiber: abi::Stream<'d, Item = chunk::Chunk<'d>>>,
        W: wire::Spec,
        Q: switch::Engine,
        M: sequence::Mode<conn::Id<'d, ID>>,
        M::Retention: turn::Mode,
    {
        let Self {
            mut tasks,
            task,
            request,
        } = self;
        let resume = |task, kind| runtime::Running::from_parts(wrap(task), kind, request);
        let mut ready = turn::Readiness::Staged;
        let mut buffered = conn.state_mut().async_state.take_stream();
        let done = loop {
            let (body, end) = if let Some(stashed) = buffered.take() {
                let (body, end) = stashed.into_parts();
                (Ok(body), end)
            } else {
                let frame = match kind {
                    runtime::Kind::Closing => payload::Frame::terminator(),
                    runtime::Kind::Fiber | runtime::Kind::Stream => {
                        if <M as life::Mode<conn::Id<'d, ID>, u64>>::ENABLED
                            && conn.work().remaining() == 0
                        {
                            conn.state_mut().async_state.insert(resume(task, kind));
                            conn.wake_target().wake();
                            return turn::Turn::Deferred;
                        }
                        let poll = context::Context::with_target(
                            conn.wake_target(),
                            conn.work(),
                            driver.reborrow(),
                            |mut context| {
                                local::Stream::poll_next(tasks.as_mut(), &task, context.as_mut())
                            },
                        );
                        match poll {
                            Some(task::Poll::Pending) => {
                                conn.state_mut().async_state.insert(resume(task, kind));
                                return turn::Turn::Pending;
                            }
                            Some(task::Poll::Ready(Some(raw))) => payload::Frame::chunk(raw),
                            Some(task::Poll::Ready(None)) => {
                                kind = runtime::Kind::Closing;
                                ready = turn::Readiness::NewlyReady;
                                continue;
                            }
                            None => {
                                conn.close_after();
                                release(conn);
                                return turn::Turn::ConnectionCancelled;
                            }
                        }
                    }
                };
                let charge = frame.retention();
                let tag = sequence::Mode::tag(life, exchange, charge);
                sequence::Mode::stage(
                    life,
                    conn.state_mut().async_state.session_mut(),
                    exchange,
                    charge,
                );
                let body = payload::Stream::new(frame, tag);
                let end = body.end();
                let body = match egress::Load::new(body).try_nonempty() {
                    Ok(body) => match body.try_bound::<B>() {
                        Ok(body) => Ok(body),
                        Err(rejected) => {
                            let (body, maximum) = rejected.into_parts();
                            conn.write().reject(body::Failure::Bound {
                                charge: body.charge(),
                                maximum: maximum.get(),
                            });
                            Err(body.into_inner())
                        }
                    },
                    Err(rejected) => {
                        let (body, cause) = rejected.into_parts();
                        conn.write().reject(body::Failure::Payload(cause));
                        Err(body.into_inner())
                    }
                };
                (body, end)
            };
            let result = match body {
                Err(body) => Err(body),
                Ok(body) => {
                    let charge = data::raw::Payload::charge(body.as_inner());
                    match conn.write().admit_right(body, driver) {
                        body::Admit::Committed => {
                            sequence::Mode::accept(
                                life,
                                conn.state_mut().async_state.session_mut(),
                                exchange,
                                charge,
                            );
                            Ok(None)
                        }
                        body::Admit::Deferred { load, .. } => Ok(Some(load)),
                        body::Admit::Closed(load) => Err(load.into_unbound().into_inner()),
                        body::Admit::Rejected { cause, load } => {
                            conn.write().reject(body::Failure::Capacity(cause));
                            Err(load.into_unbound().into_inner())
                        }
                        body::Admit::Unavailable(rejected) => {
                            let (load, cause) = rejected.into_parts();
                            conn.write().reject(body::Failure::Reserve(cause));
                            Err(load.into_unbound().into_inner())
                        }
                    }
                }
            };
            let retry = match result {
                Ok(retry) => retry,
                Err(load) => {
                    let tag = load.release();
                    if let Some((owner, charge)) = sequence::Mode::transport(life, conn.id(), tag) {
                        sequence::Mode::unstage(
                            life,
                            conn.state_mut().async_state.session_mut(),
                            owner,
                            charge,
                        );
                    }
                    break match kind {
                        runtime::Kind::Closing => {
                            turn::Turn::retired(ready, turn::Completion::Rejected)
                        }
                        runtime::Kind::Fiber | runtime::Kind::Stream => {
                            turn::Turn::TransportCancelled
                        }
                    };
                }
            };
            if let Some(load) = retry {
                let staged = runtime::Stream::new(load);
                match conn.state_mut().async_state.try_stage(staged) {
                    Ok(()) => {
                        conn.state_mut().async_state.insert(resume(task, kind));
                        return match ready {
                            turn::Readiness::NewlyReady => turn::Turn::ReadyDeferred,
                            turn::Readiness::Staged => match kind {
                                runtime::Kind::Closing => turn::Turn::StagedDeferred,
                                runtime::Kind::Fiber | runtime::Kind::Stream => {
                                    turn::Turn::Deferred
                                }
                            },
                        };
                    }
                    Err(staged) => {
                        let tag = staged.release();
                        if let Some((owner, charge)) =
                            sequence::Mode::transport(life, conn.id(), tag)
                        {
                            sequence::Mode::unstage(
                                life,
                                conn.state_mut().async_state.session_mut(),
                                owner,
                                charge,
                            );
                        }
                        conn.close_after();
                        release(conn);
                        return turn::Turn::ConnectionCancelled;
                    }
                }
            }
            match end {
                payload::End::More => {}
                payload::End::Last => {
                    break turn::Turn::retired(ready, turn::Completion::Completed);
                }
            }
        };
        let done = match local::Store::remove(tasks.as_mut(), task) {
            local::Removal::Removed => done,
            local::Removal::Absent => {
                conn.close_after();
                turn::Turn::ConnectionCancelled
            }
        };
        release(conn);
        done
    }
}

fn release<'d, ID: Identity, W, K, S, G, P, Q, B: bound::Bound, Ready, Heads>(
    conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, W, K, S, G, P, Q, B, Ready, Heads>,
) where
    Q: switch::Engine,
    W: wire::Spec,
{
    let deferred = {
        let conn = conn.state_mut();
        pump::Ingress::deferred(conn)
    };
    match deferred {
        head::Connection::KeepAlive => {}
        head::Connection::Close => conn.close_after(),
    }
}
