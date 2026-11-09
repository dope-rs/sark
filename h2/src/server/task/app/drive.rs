use std::pin;

use app::step;
use dope::{
    core::driver::{
        self,
        route::Identity,
        schedule::{
            app,
            ready::{self, task},
        },
    },
    manifold::network::listener::conn,
    net::wire,
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
        runtime::app::Fault,
        state,
        task::{
            app::{header::admit, *},
            ingress::{Progress, Route, errors},
            response, *,
        },
    },
    stream,
};

pub(in crate::server::task) struct Drive<
    'a,
    'state,
    'd,
    ID: Identity + 'd,
    R,
    Q,
    F = <R as sark_core::decode::Decode>::Plan,
> where
    R: Runtime<'d, Target = Target<'d, ID>>,
    F: section::rules::Policy,
{
    pub(in crate::server::task) router: pin::Pin<&'a mut R>,
    pub(in crate::server::task) tasks: &'a mut registry::Table<'d, ID, R::Task>,
    pub(in crate::server::task) conn: conn::Id<'d, ID>,
    pub(in crate::server::task) parent: ready::Target<'d>,
    pub(in crate::server::task) index: &'a mut registry::Index,
    pub(in crate::server::task) state: &'a mut Borrowed<'state, 'd, R, F>,
    pub(in crate::server::task) limits: server::Config,
    pub(in crate::server::task) mode: &'a mut Q,
}

impl<'a, 'state, 'd, ID: Identity + 'd, R, Q, F> Drive<'a, 'state, 'd, ID, R, Q, F>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    F: section::rules::Policy,
    Q: state::Transitions<conn::Id<'d, ID>>,
{
    pub(super) fn head<A: service::Select<R, Fields = F>>(
        mut self,
        stream: stream::Key,
        head: RetainedHead<F::Block, Selected<F>, Option<block::Plan>>,
        boundary: Boundary,
        section: Section,
        report: impl FnMut(conn::Id<'d, ID>, stream::Key, &dyn std::fmt::Debug),
    ) -> Result<(), Error>
    where
        <R::Head as request::Head>::Start: admit::Begin<'d, R>,
        Q::Retention: Handoff,
    {
        let call = identify(self.conn, stream);
        if matches!(section, Section::Trailing) {
            if matches!(boundary, Boundary::End) {
                match self.state.data().finish_route(stream, || {
                    self.mode.observe(life::Event::InputFinished(call));
                }) {
                    Ok(Route::Finished) => return Ok(()),
                    Ok(Route::Missing) => {}
                    Err(error) => {
                        self.reset(stream, error.code());
                        return Ok(());
                    }
                }
                if let Some(result) = self.state.try_finish(stream, || {
                    self.mode.observe(life::Event::InputFinished(call));
                }) {
                    if let Err(error) = result {
                        self.reset(stream, error.code());
                    }
                    return Ok(());
                }
                let mode = &mut *self.mode;
                match self.state.data().finish(stream, move || {
                    mode.observe(life::Event::InputFinished(call));
                    mode.body(call)
                }) {
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
        let head = match A::try_head(self.router.as_ref().get_ref(), head) {
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
        admit::Begin::begin(start, self, stream, report)
    }

    pub(super) fn connection(&mut self) -> &mut Connection<crate::ServerRole, F, Shared<'d>> {
        self.state.connection
    }

    pub(super) fn reborrow(&mut self) -> Drive<'_, 'state, 'd, ID, R, Q, F> {
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
    pub(super) fn batch<A: service::Select<R, Fields = F>>(
        mut self,
        work: app::Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
        mut report: impl FnMut(conn::Id<'d, ID>, stream::Key, &dyn std::fmt::Debug),
    ) where
        Q::Retention: Handoff,
    {
        use task::owner;
        let scope = owner::Scope::Exact(owner::Owner::from(self.conn));
        let mut done = false;
        loop {
            let handoff: handoffs::Completion<'_, '_, 'd, ID, R, Q, _, F> = handoffs::Completion {
                tasks: &mut *self.tasks,
                connection: self.conn,
                task_index: &mut *self.index,
                state: &mut *self.state,
                limits: self.limits,
                mode: &mut *self.mode,
                report: &mut report,
            };
            let turn = Runtime::drive(self.router.as_mut(), scope, work, driver, handoff);
            done |= matches!(&turn, Turn::Ready(..));
            let flow = match turn {
                Turn::Ready(Ok(()), receipt) | Turn::Pending(receipt) => {
                    let flow = match Runtime::has_work(self.router.as_ref(), scope) {
                        true => step::Flow::Batch,
                        false => step::Flow::Yield,
                    };
                    receipt.settle(flow)
                }
                Turn::Ready(Err(failure), receipt) => {
                    self.reset(failure.0, failure.1);
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
            self.state.pump::<true, _, _, _, A>(
                self.router,
                self.tasks,
                self.index,
                self.conn,
                self.limits,
                schedule::Egress::Unchanged,
                work,
                self.mode,
                &mut report,
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
        let (stream, error) = rejected::<R>(failure);
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
        if let Some(response) = response {
            response::discard(
                response,
                self.state.connection,
                exchange,
                self.mode,
                cause::Staging::Reset,
            );
        }
        if let Some((_, task, unreported)) = self.tasks.take_stream(self.index, stream) {
            Runtime::cancel(self.router.as_mut(), task);
            if unreported {
                self.mode.observe(life::Event::TaskCancelled(exchange));
            }
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

    pub(super) fn event<A: service::Select<R, Fields = F>>(
        &mut self,
        event: Event<Connection<crate::ServerRole, F, Shared<'d>>>,
        report: impl FnMut(conn::Id<'d, ID>, stream::Key, &dyn std::fmt::Debug),
    ) -> Result<(), Error>
    where
        Q::Retention: Handoff,
        <R::Head as request::Head>::Start: admit::Begin<'d, R>,
    {
        match event {
            Event::Head {
                message,
                head,
                section,
            } => self.reborrow().head::<A>(
                message.key(),
                head,
                message.boundary(),
                section,
                report,
            )?,
            Event::Data { message, data } => {
                inputs::apply(self.reborrow(), message.key(), data, message.boundary())
            }
            Event::Retired(write) => {
                release_retired(self.mode, self.conn, write, cause::Staging::PeerRetired)
            }
            Event::Reset {
                stream, retired, ..
            } => {
                self.mode.cancel(stream);
                if let Some(write) = retired {
                    release_retired(self.mode, self.conn, write, cause::Staging::PeerRetired);
                }
                let exchange = identify(self.conn, stream);
                let (exchange, mut retire) = match self
                    .state
                    .data()
                    .discard(exchange, || self.mode.body(exchange))
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
                    response::discard(
                        response,
                        self.state.connection,
                        exchange,
                        self.mode,
                        cause::Staging::Reset,
                    );
                }
                if let Some((_, task, unreported)) = self.tasks.take_stream(self.index, stream) {
                    retire = true;
                    Runtime::cancel(self.router.as_mut(), task);
                    if unreported {
                        self.mode.observe(life::Event::TaskCancelled(exchange));
                    }
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
        Ok(())
    }

    pub(super) fn drain<A, W, P>(
        &mut self,
        service: &mut A,
        state: &mut service::State<'d, ID, A>,
        turn: &mut sark_core::manifold::server::app::Turn<'_, '_, '_, 'd, conn::Id<'d, ID>>,
        egress: schedule::Egress,
        fault: &mut P,
    ) -> Result<drain::Progress, sark_core::manifold::server::app::Error<Error, A::Failure>>
    where
        A: service::Select<R, Fields = F>
            + sark_core::manifold::server::app::Application<
                'd,
                service::Connection<'d, A>,
                conn::Id<'d, ID>,
            >,
        Q::Retention: Handoff,
        W: wire::Spec,
        P: Fault<'d, W, A::Failure, ID, Build<A::Build>>,
        <R::Head as request::Head>::Start: admit::Begin<'d, R>,
    {
        let work = turn.work();
        let book = &mut *self.state.book;
        let connection = &mut *self.state.connection;
        let router = &mut self.router;
        let tasks = &mut *self.tasks;
        let index = &mut *self.index;
        let mode = &mut *self.mode;
        let conn = self.conn;
        let parent = self.parent;
        let limits = self.limits;
        let progress = service.drive(
            state,
            connection,
            turn,
            egress,
            &mut (book, router, tasks, index, mode, fault),
            |(book, router, tasks, index, mode, fault), connection, drain, state| {
                let mut book = book::Turn::new(connection, book);
                let Some(ready) = book.ready() else {
                    return Ok(None);
                };
                if drain.permit().is_none() {
                    return Ok(None);
                }
                if let Some(observer) = mode.observer()
                    && let Some(cause) = ready.rejection()
                {
                    observer.diagnostic(cause);
                }
                let event = drain::Ready::take(ready);
                if A::selected(state, &event) {
                    if let Event::Head {
                        message,
                        section: Section::Initial,
                        ..
                    } = &event
                    {
                        mode.cancel(message.key());
                    }
                    return Ok(Some(match event {
                        Event::Reset {
                            stream,
                            reason,
                            retired,
                        } => {
                            if let Some(write) = retired {
                                release_retired(
                                    &mut **mode,
                                    conn,
                                    write,
                                    cause::Staging::PeerRetired,
                                );
                            }
                            Event::Reset {
                                stream,
                                reason,
                                retired: None,
                            }
                        }
                        event => event,
                    }));
                }
                Drive {
                    router: router.as_mut(),
                    tasks,
                    conn,
                    parent,
                    index,
                    state: &mut book,
                    limits,
                    mode: &mut **mode,
                }
                .event::<A>(event, fault.response())?;
                Ok(Some(Event::Control))
            },
            |(book, ..), wake| {
                if book.responses.wake(wake) != 0 {
                    parent.wake();
                }
            },
            |(book, router, tasks, index, mode, fault), connection| {
                let mut state = book::Turn::new(connection, book);
                let mut report = fault.response();
                state.pump::<true, _, _, _, A>(
                    router.as_mut(),
                    tasks,
                    index,
                    conn,
                    limits,
                    schedule::Egress::Unchanged,
                    work,
                    &mut **mode,
                    &mut report,
                );
                if state.connection.ingress().is_blocked() && state.responses.has_runnable() {
                    state.pump::<false, _, _, _, A>(
                        router.as_mut(),
                        tasks,
                        index,
                        conn,
                        limits,
                        schedule::Egress::Unchanged,
                        work,
                        &mut **mode,
                        &mut report,
                    );
                }
            },
            |(.., fault), cause| {
                fault.diagnostic(conn, cause);
            },
        )?;
        self.wakes::<A>(state);
        Ok(progress)
    }

    pub(super) fn wakes<A>(&mut self, state: &mut service::State<'d, ID, A>)
    where
        A: service::Select<R, Fields = F>
            + sark_core::manifold::server::app::Application<
                'd,
                service::Connection<'d, A>,
                conn::Id<'d, ID>,
            >,
    {
        if !A::ENABLED {
            return;
        }
        while let Some(wake) = self.state.connection.flow().poll_wake() {
            let http = self.state.responses.wake(wake) != 0;
            if A::wake(state, wake) || http {
                self.parent.wake();
            }
        }
    }
}
