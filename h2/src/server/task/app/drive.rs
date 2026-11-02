use std::pin;

use app::step;
use dope::{
    core::driver::{
        retained,
        route::Identity,
        schedule::{
            app,
            ready::{self, task},
        },
    },
    manifold::network::listener::conn,
};
use o3::buffer::resident::Shared;
use sark_core::{
    body,
    decode::request,
    life,
    life::Observer as _,
    manifold::{
        schedule::{self, typed},
        server::{Event, drain},
        stream::{Boundary, Message as _, Section},
        task::app::{Runtime, Turn},
    },
};
use sark_http::{
    field::metadata::{
        block,
        section::{
            self,
            state::{RetainedHead, Selected},
        },
    },
    owner::cause,
};

use crate::{
    conn::{Error, machine::Connection},
    frame::numbers,
    server::{
        self, payload,
        response::Handoff,
        state,
        task::{
            app::{header::admit, *},
            ingress::{Progress, Route, errors},
            response, *,
        },
    },
    stream,
};

pub(in crate::server::task) struct Drive<'a, 'state, 'd, ID: Identity + 'd, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    pub(in crate::server::task) router: pin::Pin<&'a mut R>,
    pub(in crate::server::task) tasks: &'a mut registry::Table<'d, ID, R::Task>,
    pub(in crate::server::task) conn: conn::Id<'d, ID>,
    pub(in crate::server::task) parent: ready::Target<'d>,
    pub(in crate::server::task) index: &'a mut registry::Index,
    pub(in crate::server::task) state: &'a mut Borrowed<'state, 'd, R>,
    pub(in crate::server::task) limits: server::Config,
    pub(in crate::server::task) mode: &'a mut Q,
}

impl<'a, 'state, 'd, ID: Identity + 'd, R, Q> Drive<'a, 'state, 'd, ID, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    pub(super) fn head(
        mut self,
        stream: stream::Key,
        head: RetainedHead<
            <R::Plan as section::Policy>::Block,
            Selected<R::Plan>,
            Option<block::Plan>,
        >,
        boundary: Boundary,
        section: Section,
    ) -> Result<(), Error>
    where
        <R::Head as request::Head>::Start: admit::Begin<'d, R>,
        Q::Retention: Handoff,
    {
        let call = identify(self.conn, stream);
        if matches!(section, Section::Trailing) {
            if matches!(boundary, Boundary::End) {
                self.mode.observe(life::Event::InputFinished(call));
                match self.state.data().finish_route(stream) {
                    Ok(Route::Finished) => return Ok(()),
                    Ok(Route::Missing) => {}
                    Err(error) => {
                        self.reset(stream, error.code());
                        return Ok(());
                    }
                }
                if let Some(result) = self.state.try_finish(stream) {
                    if let Err(error) = result {
                        self.reset(stream, error.code());
                    }
                    return Ok(());
                }
                match self.state.data().finish(stream, self.mode.body(call)) {
                    Ok(Progress::Request(input)) => {
                        // SAFETY: finish removed the buffered request from
                        // this Drive's exact incoming Book for immediate use.
                        unsafe { response::raw::buffered(self, stream, input) }
                    }
                    Ok(Progress::Streaming) => {
                        let turn = self.state.data().build().drive_for(
                            stream,
                            body::DEFAULT_BUILD_STEP_BUDGET,
                            self.mode.body(call),
                        );
                        self.build(turn);
                    }
                    Ok(Progress::Idle) => {}
                    Err(error) => self.reset(stream, error.code()),
                }
            }
            return Ok(());
        }
        self.mode.observe(life::Event::ExchangeOpened(call));
        let Ok(head) = head.try_request() else {
            self.reset(stream, numbers::ErrorCode::ProtocolError);
            return Ok(());
        };
        let head = match self.router.as_ref().get_ref().try_head(head) {
            Ok(head) => head,
            Err(error) => {
                if matches!(boundary, Boundary::End) {
                    self.mode.observe(life::Event::InputFinished(call));
                }
                self.queue(stream, response::Response::Status(error.status()));
                return Ok(());
            }
        };
        let start =
            match request::Head::try_start(head, self.limits.max_request_body_bytes, boundary) {
                Ok(start) => start,
                Err(error) => {
                    let error = error.discard();
                    if matches!(boundary, Boundary::End) {
                        self.mode.observe(life::Event::InputFinished(call));
                    }
                    self.reset(stream, payload::Error::Body(error).code());
                    return Ok(());
                }
            };
        self.mode.observe(life::Event::HeadAdmitted(call));
        if matches!(boundary, Boundary::End) {
            self.mode.observe(life::Event::InputFinished(call));
        }
        admit::Begin::begin(start, self, stream)
    }

    pub(super) fn connection(&mut self) -> &mut Connection<crate::ServerRole, R::Plan, Shared<'d>> {
        self.state.connection
    }

    fn reborrow(&mut self) -> Drive<'_, 'state, 'd, ID, R, Q> {
        Drive {
            router: self.router.as_mut(),
            tasks: &mut *self.tasks,
            conn: self.conn,
            parent: self.parent,
            index: &mut *self.index,
            state: &mut *self.state,
            limits: self.limits,
            mode: &mut *self.mode,
        }
    }

    /// Drains one connection's ready tasks inside the current application turn,
    /// avoiding a second manifold round-trip for newly CPU-ready work.
    pub(super) fn batch(
        mut self,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        Q::Retention: Handoff,
    {
        use task::owner;
        let scope = owner::Scope::Exact(owner::Owner::from(self.conn));
        let mut done = false;
        loop {
            let handoff: handoffs::Completion<'_, '_, 'd, ID, R, Q> = handoffs::Completion {
                tasks: &mut *self.tasks,
                connection: self.conn,
                task_index: &mut *self.index,
                state: &mut *self.state,
                limits: self.limits,
                mode: &mut *self.mode,
            };
            let turn = Runtime::drive(self.router.as_mut(), scope, work, driver, handoff);
            done |= matches!(&turn, Turn::Complete(..));
            let flow = match turn {
                Turn::Complete(Ok(()), receipt) | Turn::Pending(receipt) => {
                    let flow = match Runtime::has_work(self.router.as_ref(), scope) {
                        true => step::Flow::Batch,
                        false => step::Flow::Yield,
                    };
                    receipt.settle(flow)
                }
                Turn::Complete(Err(failure), receipt) => {
                    self.reject(failure);
                    receipt.settle(step::Flow::Yield)
                }
                Turn::PendingReady(receipt) => receipt.settle(step::Flow::Batch),
                Turn::Idle | Turn::Deferred => break,
            };
            match flow {
                step::Flow::Batch => {}
                step::Flow::Yield => break,
            }
        }
        if done {
            self.state.pump::<true, _, _, _>(
                self.router,
                self.conn,
                self.limits,
                schedule::Egress::Unchanged,
                work,
                self.mode,
            );
        }
    }

    pub(in crate::server::task) fn queue(&mut self, stream: stream::Key, value: Reply<'d, R>) {
        let next = self.state.activation(self.limits);
        match self
            .state
            .responses
            .try_insert(stream, typed::Next::Control(value, next))
        {
            Ok(()) => state::Transitions::ready(self.mode, identify(self.conn, stream)),
            Err(failure) => self.reject(failure),
        }
    }

    pub(in crate::server::task) fn reject(
        &mut self,
        failure: schedule::Insert<stream::Key, Next<'d, R>>,
    ) {
        let (stream, error) = match &failure {
            schedule::Insert::Full { stream, .. } => (*stream, numbers::ErrorCode::EnhanceYourCalm),
            schedule::Insert::Occupied { stream, .. }
            | schedule::Insert::Excluded { stream, .. } => {
                (*stream, numbers::ErrorCode::InternalError)
            }
        };
        self.reset(stream, error)
    }

    pub(in crate::server::task) fn reset(
        &mut self,
        stream: stream::Key,
        error: numbers::ErrorCode,
    ) {
        let exchange = identify(self.conn, stream);
        let (response, retired) = self.state.reset(exchange, error, self.mode.body(exchange));
        if let Some(write) = retired {
            release_retired(self.mode, self.conn, write, cause::Staging::Reset);
        }
        match response {
            Some(Value::Data(response::Data::Body { pending, .. })) => {
                release_pending(self.mode, exchange, pending, cause::Staging::Reset);
            }
            Some(Value::Control(response::Response::Encoded(receipt))) => drop(receipt),
            Some(Value::Control(
                response::Response::Head { head, .. } | response::Response::DuplexHead { head, .. },
            )) => {
                head.discard(self.state.connection);
            }
            Some(Value::Data(response::Data::Duplex { .. }))
            | Some(Value::Control(
                response::Response::Status(_)
                | response::Response::Sync(_)
                | response::Response::Completion(_)
                | response::Response::DuplexOpen { .. },
            ))
            | None => {}
            Some(Value::Idle(never)) => match never {},
        }
        exchange.retire(self.mode, life::Outcome::Rejected);
    }

    pub(super) fn build(&mut self, turn: Built<'d, R::Build>) {
        match turn {
            body::BuildOutcome::Ready { key: stream, build } => {
                response::raw::built(self.reborrow(), stream, build)
            }
            body::BuildOutcome::Failed { key: stream, error } => {
                if let Some(observer) = self.mode.observer() {
                    match &error {
                        errors::Build::Step(cause) => observer.diagnostic(cause),
                        errors::Build::Admit(cause) => observer.diagnostic(cause),
                        errors::Build::AdmitRelease { error, release } => {
                            observer.diagnostic(&(error, release));
                        }
                    }
                }
                self.reset(stream, error.code::<R>())
            }
            body::BuildOutcome::Idle | body::BuildOutcome::Pending => {}
        }
    }

    pub(super) fn drain(&mut self, work: app::Work<'_, 'd>) -> Result<drain::Progress, Error>
    where
        Q::Retention: Handoff,
        <R::Head as request::Head>::Start: admit::Begin<'d, R>,
    {
        drain::Events::fair(work, |mut drain| {
            while let Some(ready) = self.state.ready() {
                let Some(permit) = drain.permit() else {
                    break;
                };
                let event = permit.run(|| {
                    if let Some(observer) = self.mode.observer()
                        && let Some(cause) = ready.rejection()
                    {
                        observer.diagnostic(cause);
                    }
                    drain::Ready::take(ready)
                });
                match event {
                    Event::Head {
                        message,
                        head,
                        section,
                    } => self
                        .reborrow()
                        .head(message.key(), head, message.boundary(), section)?,
                    Event::Data { message, data } => {
                        inputs::apply(self.reborrow(), message.key(), data, message.boundary())
                    }
                    Event::Retired(write) => {
                        release_retired(self.mode, self.conn, write, cause::Staging::PeerRetired)
                    }
                    Event::Reset {
                        stream, retired, ..
                    } => {
                        if let Some(write) = retired {
                            release_retired(
                                self.mode,
                                self.conn,
                                write,
                                cause::Staging::PeerRetired,
                            );
                        }
                        let exchange = identify(self.conn, stream);
                        let (exchange, mut retire) = match self
                            .state
                            .data()
                            .discard(exchange, self.mode.body(exchange))
                        {
                            ingress::Discard::Missing(exchange) => (exchange, false),
                            ingress::Discard::Removed(exchange) => (exchange, true),
                            ingress::Discard::Failed { exchange, error } => {
                                self.state
                                    .connection
                                    .streams()
                                    .cancel(exchange.stream(), error.code());
                                (exchange, true)
                            }
                        };
                        let stream = exchange.stream();
                        if let Some(response) = self.state.responses.remove(stream) {
                            retire = true;
                            match response {
                                Value::Data(response::Data::Body { pending, .. }) => {
                                    release_pending(
                                        self.mode,
                                        exchange,
                                        pending,
                                        cause::Staging::Reset,
                                    );
                                }
                                Value::Control(response::Response::Encoded(receipt)) => {
                                    drop(receipt);
                                }
                                Value::Control(
                                    response::Response::Head { head, .. }
                                    | response::Response::DuplexHead { head, .. },
                                ) => {
                                    head.discard(self.state.connection);
                                }
                                Value::Data(response::Data::Duplex { .. })
                                | Value::Control(
                                    response::Response::Status(_)
                                    | response::Response::Sync(_)
                                    | response::Response::Completion(_)
                                    | response::Response::DuplexOpen { .. },
                                ) => {}
                                Value::Idle(never) => match never {},
                            };
                        }
                        if let Some(registry::Taken { task, .. }) =
                            self.tasks.take_stream(self.index, self.conn, stream)
                        {
                            retire = true;
                            Runtime::cancel(self.router.as_mut(), task);
                            self.mode.observe(life::Event::TaskCancelled(exchange));
                        }
                        if retire {
                            exchange.retire(self.mode, life::Outcome::Cancelled);
                        }
                    }
                    Event::Established => {
                        self.mode
                            .observe(life::Event::SessionEstablished(self.conn));
                    }
                    Event::Control => {}
                }
            }
            self.state.pump::<true, _, _, _>(
                self.router.as_mut(),
                self.conn,
                self.limits,
                schedule::Egress::Unchanged,
                work,
                self.mode,
            );
            if self.state.connection.ingress().is_blocked() && self.state.responses.has_runnable() {
                self.state.pump::<false, _, _, _>(
                    self.router.as_mut(),
                    self.conn,
                    self.limits,
                    schedule::Egress::Unchanged,
                    work,
                    self.mode,
                );
            }
            Ok(drain.finish())
        })
    }
}
