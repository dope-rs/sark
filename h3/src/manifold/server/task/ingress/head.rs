use core::{convert::Infallible, ops::ControlFlow};

use conn::scope;
use dope::core::driver::schedule::clock;
use dope_quic::{conn, conn::id, stream, wire::stream::Id};
use o3::buffer::resident;
use requests::{
    book::claim,
    entries::{self, Completion},
};
use sark_core::{
    body::{admit, buffered},
    deadline::timeline,
    decode::{
        dispatch, exec, request,
        request::admit::{Start as _, Stop as _},
        state,
    },
    duplex,
    life::{self, Observer as _, exchange},
    manifold::{stream::Boundary, task::app::Runtime},
};
use sark_http::payload::{
    owned,
    response::{Method, status::Code},
};

use crate::{
    frame,
    manifold::{
        self, encoder, owners, requests,
        server::{self, deadline, deadline::state::State, rotation, session::finish},
    },
};

/// The request transition has consumed its resident loan before session I/O resumes.
pub(in crate::manifold::server) enum Outcome {
    Done,
    Reject(frame::ErrorCode),
    Close(frame::ErrorCode),
    Stopped,
    Removed,
}

pub(in crate::manifold::server) struct Head<
    'a,
    'scope,
    'd,
    R,
    O,
    E,
    B,
    const ID: u8,
    const RECONCILE: bool,
> where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    E: State<'d>,
    B: stream::Recv,
{
    pub(super) h3: &'a mut manifold::Session<'d, R::Plan>,
    pub(super) deadline: &'a mut E,
    pub(super) app: &'a R,
    pub(super) effects: owners::Projection<'a, O, id::Handle<id::Driver<'d>, ID>>,
    pub(super) conn: &'a mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
    pub(super) claim: claim::Claim<'a, 'd, R::Prepared, R::Build, R::Task, R::Duplex, Method>,
    pub(super) boundary: Boundary,
    pub(super) now: clock::Moment<'d>,
}

pub(super) fn finish<'d, R, O, L, E, B, const ID: u8, const RECONCILE: bool>(
    state: &mut server::Session<'d, R, L, E>,
    app: &R,
    life: &mut O,
    conn: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
    handle: id::Handle<id::Driver<'d>, ID>,
    id: requests::Id,
    now: clock::Moment<'d>,
) where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: State<'d>,
    B: stream::Recv,
{
    if let Err(error) = E::transition(
        &mut state.deadline,
        id,
        timeline::Transition::InputFinished,
        now,
    ) {
        manifold::close_connection_scoped(conn, deadline::code(error));
        return;
    }
    state.finish_input(app, life, conn, handle, id, now);
}

impl<'a, 'scope, 'd, R, O, E, B, const ID: u8, const RECONCILE: bool>
    Head<'a, 'scope, 'd, R, O, E, B, ID, RECONCILE>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    E: State<'d>,
    B: stream::Recv,
{
    fn guard(self, result: Result<owners::Commit, frame::ErrorCode>) -> Outcome {
        let id = self.claim.id();
        let (life, handle) = self.effects.into_parts();
        let span = exchange::Id::new(handle, id.stream_id());
        let commit = match result {
            Ok(commit) => commit,
            Err(code) => return Outcome::Reject(code),
        };
        commit.observe(life, span);
        self.claim.discard();
        match self.boundary {
            Boundary::End => Outcome::Removed,
            Boundary::More => Outcome::Stopped,
        }
    }

    pub(super) fn begin(self, request: <R::Head as request::Head>::Start) -> Outcome {
        let id = self.claim.id();
        let method = self.claim.method();
        let outcome = request.admit(
            self.app,
            || encoder::Encoder::new(self.h3.h3_mut(), id.target(), method),
            |stop| match stop {
                Ok(wire) => <O::Retention as owners::Handoff>::resolve(wire),
                Err((error, _)) => Err(match error {
                    dispatch::Error::Encode(error) => owners::Commit::code(error),
                    dispatch::Error::Busy | dispatch::Error::Sequence(_) => {
                        frame::ErrorCode::ExcessiveLoad
                    }
                    dispatch::Error::Pending
                    | dispatch::Error::Moved
                    | dispatch::Error::Unsupported => frame::ErrorCode::Internal,
                    dispatch::Error::Invalid(never) | dispatch::Error::Body(never) => {
                        match never {}
                    }
                }),
            },
        );
        match outcome {
            ControlFlow::Continue(request) => self.run(request),
            ControlFlow::Break(stop) => self.guard(stop.into_result()),
        }
    }

    fn run<T>(self, request: T) -> Outcome
    where
        T: state::Flow<
                R::Prepared,
                state::Admitted,
                Family = <R::Prepared as request::Prepared>::Family,
            >,
    {
        match request.map_retain(self) {
            ControlFlow::Continue((this, request)) => {
                this.claim.into_entry().buffer(request);
                Outcome::Done
            }
            ControlFlow::Break(outcome) => outcome,
        }
    }

    fn reject(self, error: requests::Error) -> Outcome {
        let (life, _) = self.effects.into_parts();
        if let Some(observer) = life.observer() {
            observer.diagnostic(&error);
        }
        Outcome::Reject(server::request_error_code(error))
    }

    fn status(self, code: Code) -> Outcome {
        let id = self.claim.id();
        self.claim.respond(code);
        match E::transition(
            self.deadline,
            id,
            timeline::Transition::ResponseReady,
            self.now,
        ) {
            Ok(()) => Outcome::Done,
            Err(error) => Outcome::Close(deadline::code(error)),
        }
    }

    fn build(self, request: R::Prepared, start: admit::Start) -> Outcome {
        match Runtime::try_start(self.app, request) {
            Ok(build) => {
                self.claim.into_entry().build(start, build);
                Outcome::Done
            }
            Err(dispatch::Error::Invalid(reject)) => self.status(reject.status()),
            Err(dispatch::Error::Busy | dispatch::Error::Sequence(_)) => {
                Outcome::Reject(frame::ErrorCode::ExcessiveLoad)
            }
            Err(
                dispatch::Error::Pending | dispatch::Error::Moved | dispatch::Error::Unsupported,
            ) => Outcome::Reject(frame::ErrorCode::Internal),
            Err(dispatch::Error::Body(never) | dispatch::Error::Encode(never)) => match never {},
        }
    }

    fn sync(
        self,
        request: request::Admitted<exec::Sync, R::Prepared, admit::Track<admit::Finished>>,
    ) -> Outcome {
        let id = self.claim.id();
        match self.claim.into_entry().sync(request) {
            Ok(done) => {
                finish::accept(
                    self.h3,
                    self.now.at(self.deadline),
                    self.app,
                    self.effects,
                    self.conn,
                    id,
                    done,
                );
                Outcome::Done
            }
            Err(error) => Outcome::Reject(server::body_error_code(error.error().error())),
        }
    }

    fn task(
        self,
        request: request::Admitted<exec::Task, R::Prepared, admit::Track<admit::Finished>>,
    ) -> Outcome {
        match self.claim.into_entry().task(request) {
            Ok(()) => Outcome::Done,
            Err(error) => Outcome::Reject(server::body_error_code(error.error().error())),
        }
    }

    fn duplex<K>(self, slot: K, done: Completion) -> Outcome
    where
        K: state::Slot<R::Prepared>,
    {
        let request = slot.admit::<exec::Duplex, _>(());
        match duplex::Application::try_open(self.app, request) {
            Ok((peer, ())) => match self.claim.connect(peer, done) {
                Ok(()) => Outcome::Done,
                Err(error) => {
                    let (life, _) = self.effects.into_parts();
                    if let Some(observer) = life.observer() {
                        observer.diagnostic(&error);
                    }
                    Outcome::Reject(server::request_error_code(error))
                }
            },
            Err((error, ())) => {
                let code = match error {
                    duplex::OpenError::InvalidRequest(error) => R::reject(error).status(),
                    duplex::OpenError::Length => Code::BAD_REQUEST,
                    duplex::OpenError::Unsupported => Code::INTERNAL_SERVER_ERROR,
                };
                self.status(code)
            }
        }
    }
}

impl<'scope, 'd, R, O, E, B, const ID: u8, const RECONCILE: bool>
    state::Map<R::Prepared, state::Admitted, entries::Receiving<'d>>
    for Head<'_, 'scope, 'd, R, O, E, B, ID, RECONCILE>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    E: State<'d>,
    B: stream::Recv,
{
    type Break = Outcome;
    type Retained = Self;

    fn sync_open<S: state::Slot<R::Prepared>>(
        self,
        slot: S,
        body: admit::Track<admit::Open>,
    ) -> ControlFlow<Self::Break, (Self, S, buffered::State<owned::Segments<'d>>)> {
        ControlFlow::Continue((self, slot, buffered::State::new(body)))
    }

    fn sync_done<S: state::Slot<R::Prepared>>(
        self,
        slot: S,
        body: admit::Track<admit::Finished>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        ControlFlow::Break(self.sync(slot.admit(body)))
    }

    fn task_open<S: state::Slot<R::Prepared>>(
        self,
        slot: S,
        body: admit::Track<admit::Open>,
    ) -> ControlFlow<Self::Break, (Self, S, buffered::State<owned::Segments<'d>>)> {
        if matches!(body.stream(), admit::Stream::Streaming) {
            let (request, body) = slot.admit::<exec::Task, _>(body).into_parts();
            return ControlFlow::Break(self.build(request, admit::Start::Open(body)));
        }
        ControlFlow::Continue((self, slot, buffered::State::new(body)))
    }

    fn task_done<S: state::Slot<R::Prepared>>(
        self,
        slot: S,
        body: admit::Track<admit::Finished>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        if matches!(body.stream(), admit::Stream::Streaming) {
            let (request, body) = slot.admit::<exec::Task, _>(body).into_parts();
            return ControlFlow::Break(self.build(request, admit::Start::Finished(body)));
        }
        ControlFlow::Break(self.task(slot.admit(body)))
    }

    fn duplex_open<S: state::Slot<R::Prepared>>(
        self,
        slot: S,
        _: (),
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        ControlFlow::Break(self.duplex(slot, Completion::Open))
    }

    fn duplex_done<S: state::Slot<R::Prepared>>(
        self,
        slot: S,
        _: (),
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        ControlFlow::Break(self.duplex(slot, Completion::Finished))
    }

    fn reject_open<S: state::Slot<R::Prepared>>(
        self,
        _: S,
        _: admit::Track<admit::Open>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        ControlFlow::Break(self.reject(requests::Error::Unsupported))
    }

    fn reject_done<S: state::Slot<R::Prepared>>(
        self,
        _: S,
        _: admit::Track<admit::Finished>,
    ) -> ControlFlow<Self::Break, (Self, S, Infallible)> {
        ControlFlow::Break(self.reject(requests::Error::Unsupported))
    }
}
