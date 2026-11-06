//! Task-route execution over borrowed HTTP/1 protocol storage.
use o3::base::cell::region;
use sark_http::payload::response::Body as _;
mod buffered;
mod endpoint;

use std::{convert, mem};

pub use endpoint::Endpoint;
use sark_core::{
    body::{
        self,
        admit::{Open, Track},
        credit, flow, input, stream,
    },
    manifold::{stream::Boundary, task},
    request,
    route::{self, Reject},
};
use sark_http::{
    h1::codec::canned,
    payload::{fixed, owned, response::dispatch},
};
use task::{
    request::{Build, Input},
    response,
};

use crate::{
    session,
    wire::{Failure, head, output, response::Context},
};

type Origin<'id> = region::Proof<region::Identity<'id>>;

/// Borrowed HTTP/1 output resources for one completed generated task.
#[doc(hidden)]
pub struct Egress<'turn> {
    write: &'turn mut [u8],
    date: &'turn [u8; 29],
    connection: head::Connection,
    response: Context,
}

impl<'turn> Egress<'turn> {
    pub fn new(
        write: &'turn mut [u8],
        date: &'turn [u8; 29],
        conn: head::Connection,
        response: Context,
    ) -> Self {
        Self {
            write,
            date,
            connection: conn,
            response,
        }
    }

    pub fn finish<'d, R: route::Task<'d, A> + crate::route::PolicyOf, A>(
        self,
        output: response::Output<'d, R, A>,
    ) -> (output::Outcome<'d>, Option<response::Stream<'d, R, A>>) {
        let (id, outcome) = output.into_parts();
        match outcome {
            response::Outcome::Response(reply) => {
                let reply = <R::Output as dispatch::IntoOutput<'d, 'd>>::into_output(reply);
                output::Egress::new(self.write, self.date, self.response)
                    .plain_task::<'d, 'd, <R::Output as dispatch::IntoOutput<'d, 'd>>::Output>(
                    reply,
                    id,
                    self.connection,
                    R::POLICY,
                )
            }
            response::Outcome::Rejected(error) => {
                (output::Outcome::Close(Failure::Route(error).body()), None)
            }
        }
    }
}

const _: () = assert!(mem::size_of::<Egress<'static>>() == 4 * mem::size_of::<usize>());

/// The request's construction mode selects its exact retained H1 state.
pub type Pending<'d, R> =
    <<<R as route::Spec>::Request as route::Own>::Mode as Mode<'d, R>>::Pending;

/// Static H1 storage selection for the two sealed request-construction modes.
#[doc(hidden)]
pub trait Mode<'d, R: route::Spec<Request: route::Own<Mode = Self>>>: route::Mode + Sized {
    type Pending;

    fn begin(
        head: request::raw::Parsed<
            request::Parts<'d, <R::Request as route::Schema>::Frame>,
            R::Request,
        >,
        open: Track<Open>,
        connection: head::Connection,
        response: Context,
    ) -> Result<Self::Pending, <R::Request as route::Own>::Error<'d>>;
}

impl<'d, R: route::Spec<Request: route::Own<Mode = route::Streaming>>> Mode<'d, R>
    for route::Streaming
{
    type Pending = Streaming<'d, R>;

    fn begin(
        head: request::raw::Parsed<
            request::Parts<'d, <R::Request as route::Schema>::Frame>,
            R::Request,
        >,
        open: Track<Open>,
        connection: head::Connection,
        response: Context,
    ) -> Result<Self::Pending, <R::Request as route::Own>::Error<'d>> {
        let build = <R::Request as route::Own>::start_stream(head)?;
        Ok(Streaming::new(build, open, connection, response))
    }
}

impl<'d, R: route::Spec<Request: route::Own<Mode = route::Buffered>>> Mode<'d, R>
    for route::Buffered
{
    type Pending = buffered::Pending<'d, R>;

    fn begin(
        head: request::raw::Parsed<
            request::Parts<'d, <R::Request as route::Schema>::Frame>,
            R::Request,
        >,
        open: Track<Open>,
        connection: head::Connection,
        response: Context,
    ) -> Result<Self::Pending, <R::Request as route::Own>::Error<'d>> {
        Ok(buffered::Pending::new(head, open, connection, response))
    }
}

/// Owned pre-admission state for one streamed task request body.
pub struct Streaming<'d, R: route::Spec<Request: route::Own>> {
    stream: stream::Stream<'d, Build<'d, R>>,
    account: credit::raw::Account,
    connection: head::Connection,
    response: Context,
    work: bool,
}

impl<'d, R: route::Spec<Request: route::Own>> Streaming<'d, R> {
    pub const fn response(&self) -> Context {
        self.response
    }

    /// Whether the last builder turn retained work that needs no further input.
    pub const fn has_work(&self) -> bool {
        self.work
    }

    fn new(
        build: <R::Request as route::Own>::TaskBuilder<'d>,
        open: Track<Open>,
        close: head::Connection,
        response: Context,
    ) -> Self {
        Self {
            stream: stream::Stream::new(open, Build::new(build)),
            account: credit::raw::Account::new(),
            connection: close,
            response,
            work: false,
        }
    }

    /// Lends the exact body and account together; only their successor can be
    /// persisted, while a terminal result cannot retain this operation's origin.
    fn try_with<T>(
        self,
        run: impl for<'id> FnOnce(
            stream::Stream<'d, Build<'d, R>, Origin<'id>>,
            &mut credit::raw::Ledger<'_, Origin<'id>>,
            &mut bool,
        ) -> Result<stream::Stream<'d, Build<'d, R>, Origin<'id>>, T>,
    ) -> Result<Self, T> {
        let Self {
            stream,
            mut account,
            connection: conn,
            response,
            mut work,
        } = self;
        let stream = account.scope(|mut ledger| {
            // SAFETY: these private fields began empty together and every
            // successor remains paired with this same exclusively owned account.
            let stream = unsafe { ledger.bind(stream) };
            let stream = run(stream, &mut ledger, &mut work)?;
            // SAFETY: the fresh origin ties this successor to the borrowed
            // account; it returns directly to this private body with that account.
            Ok(unsafe { ledger.store(stream) })
        })?;
        Ok(Self {
            stream,
            account,
            connection: conn,
            response,
            work,
        })
    }

    pub fn try_discard<E: credit::Evidence>(
        self,
        parts: owned::Segments<'d>,
        mut proof: E,
    ) -> Result<Self, fixed::Body<'d>> {
        let bytes = parts.len();
        self.try_with(|stream, ledger, work| {
            match stream.try_discard(ledger, &mut proof, parts, bytes, Boundary::More) {
                body::Transfer::Accepted(stream) => {
                    Self::try_step(stream, ledger, &mut proof, work)
                }
                body::Transfer::Rejected {
                    build: stream,
                    error,
                } => {
                    let reply = Self::body_failure(error);
                    Err(Self::discard(stream.into_release(), ledger, proof, reply))
                }
            }
        })
    }

    pub fn try_retain<E: credit::Evidence>(
        self,
        parts: owned::Segments<'d>,
        mut proof: E,
    ) -> Result<Self, fixed::Body<'d>>
    where
        <R::Request as route::Own>::TaskBuilder<'d>: route::Push<'d>,
    {
        let bytes = parts.len();
        let chunks = parts.count();
        self.try_with(|stream, ledger, work| {
            let limits = credit::Limits::new(stream.max_body(), body::DEFAULT_MAX_REQUEST_CHUNKS);
            let pushed = match stream.try_push(
                ledger,
                &mut proof,
                input::Input::new(parts, bytes, chunks),
                Boundary::More,
                (limits, limits),
                convert::identity,
            ) {
                stream::Push::Accepted(stream) => Ok(stream),
                stream::Push::Discarded { build: stream, .. } => {
                    Err((stream, canned::INTERNAL_SERVER_ERROR))
                }
                stream::Push::Refused {
                    build: stream,
                    error,
                    ..
                }
                | stream::Push::Failed {
                    build: stream,
                    error,
                } => {
                    let reply = Self::body_failure(error);
                    Err((stream, reply))
                }
                stream::Push::Rejected {
                    build: stream,
                    error,
                } => {
                    let reply = Failure::Route(
                    <<R::Request as route::Own>::TaskBuilder<'d> as route::Push<'d>>::reject_push(
                        error,
                    ),
                )
                .body();
                    Err((stream, reply))
                }
            };
            let stream = match pushed {
                Ok(stream) => stream,
                Err((stream, reply)) => {
                    return Err(Self::discard(stream.into_release(), ledger, proof, reply));
                }
            };
            Self::try_step(stream, ledger, &mut proof, work)
        })
    }

    /// Advances retained builder work without inventing another input segment.
    pub fn resume<E: credit::Evidence>(self, mut proof: E) -> Result<Self, fixed::Body<'d>> {
        self.try_with(|stream, ledger, work| Self::try_step(stream, ledger, &mut proof, work))
    }

    fn try_step<E: credit::Evidence, O: credit::Origin>(
        stream: stream::Stream<'d, Build<'d, R>, O>,
        ledger: &mut credit::raw::Ledger<'_, O>,
        proof: &mut E,
        work: &mut bool,
    ) -> Result<stream::Stream<'d, Build<'d, R>, O>, fixed::Body<'d>> {
        match stream.try_step(ledger, proof, body::DEFAULT_BUILD_STEP_BUDGET) {
            stream::Drive::NeedInput(stream) => {
                *work = false;
                Ok(stream)
            }
            stream::Drive::Pending(stream) => {
                *work = true;
                Ok(stream)
            }
            stream::Drive::Ready(ready) => Err(Self::discard(
                ready.into_release(),
                ledger,
                proof,
                canned::BAD_REQUEST,
            )),
            stream::Drive::Failed { stream, error } => {
                let reply = Self::drive(error, |error| {
                    <<R::Request as route::Own>::TaskBuilder<'d> as route::TaskBuild<'d>>::reject(
                        error,
                    )
                });
                Err(Self::discard(stream.into_release(), ledger, proof, reply))
            }
            stream::Drive::Rejected { stream, error } => {
                let reply = Self::body_failure(error);
                Err(Self::discard(stream.into_release(), ledger, proof, reply))
            }
        }
    }

    fn try_exact<E: credit::Evidence>(
        self,
        len: usize,
        proof: &mut E,
    ) -> Result<Self, fixed::Body<'d>> {
        self.try_with(
            |stream, ledger, _| match stream.try_exact(ledger, &mut *proof, len) {
                body::Transfer::Accepted(stream) => Ok(stream),
                body::Transfer::Rejected {
                    build: stream,
                    error,
                } => {
                    let reply = Self::body_failure(error);
                    Err(Self::discard(stream.into_release(), ledger, proof, reply))
                }
            },
        )
    }

    pub fn cancel<E: credit::Evidence>(self, proof: E) {
        self.stream.abandon(proof);
    }

    fn discard<E: credit::Evidence, O: credit::Origin>(
        release: credit::Release<O>,
        ledger: &mut credit::raw::Ledger<'_, O>,
        mut proof: E,
        reply: fixed::Body<'d>,
    ) -> fixed::Body<'d> {
        match release.try_discard(ledger, &mut proof).abandon(proof) {
            Ok(()) => reply,
            Err(error) => Self::body_failure(body::Error::Credit(error)),
        }
    }

    fn body_failure(error: body::Error) -> fixed::Body<'d> {
        match error {
            body::Error::TooLarge { .. } | body::Error::Overflow { .. } => {
                canned::PAYLOAD_TOO_LARGE
            }
            body::Error::LengthMismatch { .. }
            | body::Error::InputIncomplete
            | body::Error::InputFinished
            | body::Error::SegmentMismatch => canned::BAD_REQUEST,
            body::Error::Credit(error) => Self::credit(error),
        }
    }

    fn drive<S>(error: flow::Error<S>, reject: impl FnOnce(S) -> Reject) -> fixed::Body<'d> {
        match error {
            flow::Error::LengthMismatch => canned::BAD_REQUEST,
            flow::Error::Credit(error) => Self::credit(error),
            flow::Error::Build(error) => Failure::Route(reject(error)).body(),
            flow::Error::Release { .. } => canned::INTERNAL_SERVER_ERROR,
        }
    }

    fn credit(error: credit::Error) -> fixed::Body<'d> {
        match error {
            credit::Error::ByteLimit { .. } | credit::Error::ChunkLimit { .. } => {
                canned::PAYLOAD_TOO_LARGE
            }
            credit::Error::ByteOverflow { .. }
            | credit::Error::ChunkOverflow { .. }
            | credit::Error::ByteUnderflow { .. }
            | credit::Error::ChunkUnderflow { .. } => canned::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn finish<Runtime, Source, Admit, E, D>(
        self,
        done: session::Complete,
        permit: session::Permit,
        rt: &mut Runtime,
        source: Source,
        mut proof: E,
    ) -> session::BodyOutcome<'d, Ready<'d, R>, D>
    where
        Source: FnOnce() -> Option<Admit>,
        Admit: FnOnce(
            Input<'d, R>,
            usize,
            head::Connection,
            Context,
            &mut Runtime,
        ) -> session::Outcome<'d, D>,
        E: credit::Evidence,
    {
        let pending = match self.try_exact(done.body_len, &mut proof) {
            Ok(pending) => pending,
            Err(reply) => {
                return session::BodyOutcome::Complete(session::Outcome::Close(reply));
            }
        };
        Ready { pending }.complete(done.consumed, permit, rt, source, proof)
    }
}

/// Construction continuation whose input has already reached its exact end.
pub struct Ready<'d, R: route::Spec<Request: route::Own>> {
    pending: Streaming<'d, R>,
}

impl<'d, R: route::Spec<Request: route::Own>> Ready<'d, R> {
    pub const fn response(&self) -> Context {
        self.pending.response()
    }

    pub fn cancel<E: credit::Evidence>(self, proof: E) {
        self.pending.cancel(proof);
    }

    pub fn resume<Runtime, Source, Admit, E, D>(
        self,
        permit: session::Permit,
        rt: &mut Runtime,
        source: Source,
        proof: E,
    ) -> session::BodyOutcome<'d, Self, D>
    where
        Source: FnOnce() -> Option<Admit>,
        Admit: FnOnce(
            Input<'d, R>,
            usize,
            head::Connection,
            Context,
            &mut Runtime,
        ) -> session::Outcome<'d, D>,
        E: credit::Evidence,
    {
        self.complete(0, permit, rt, source, proof)
    }

    fn complete<Runtime, Source, Admit, E, D>(
        self,
        used: usize,
        permit: session::Permit,
        rt: &mut Runtime,
        source: Source,
        mut proof: E,
    ) -> session::BodyOutcome<'d, Self, D>
    where
        Source: FnOnce() -> Option<Admit>,
        Admit: FnOnce(
            Input<'d, R>,
            usize,
            head::Connection,
            Context,
            &mut Runtime,
        ) -> session::Outcome<'d, D>,
        E: credit::Evidence,
    {
        let pending = self.pending;
        let close = pending.connection;
        let response = pending.response;
        let result = pending.try_with(|stream, ledger, _| {
            let progress = stream.try_step(ledger, &mut proof, body::DEFAULT_BUILD_STEP_BUDGET);
            permit.commit();
            let ready = match progress {
                stream::Drive::Ready(ready) => ready,
                stream::Drive::Pending(stream) => return Ok(stream),
                stream::Drive::NeedInput(stream) => {
                    let reply = Streaming::<R>::discard(
                        stream.into_release(),
                        ledger,
                        proof,
                        canned::BAD_REQUEST,
                    );
                    return Err(session::Outcome::Close(reply));
                }
                stream::Drive::Failed { stream, error } => {
                    let reply = Streaming::<R>::drive(error, |error| {
                        <<R::Request as route::Own>::TaskBuilder<'d> as route::TaskBuild<'d>>::reject(
                            error,
                        )
                    });
                    let reply = Streaming::<R>::discard(stream.into_release(), ledger, proof, reply);
                    return Err(session::Outcome::Close(reply));
                }
                stream::Drive::Rejected { stream, error } => {
                    let reply = Streaming::<R>::body_failure(error);
                    let reply = Streaming::<R>::discard(stream.into_release(), ledger, proof, reply);
                    return Err(session::Outcome::Close(reply));
                }
            };
            let request = match ready.try_admit(ledger, proof) {
                stream::Admission::Accepted(request) => request,
                stream::Admission::Rejected {
                    ready,
                    evidence: proof,
                    error,
                } => {
                    let reply = Streaming::<R>::body_failure(error);
                    let reply = Streaming::<R>::discard(ready.into_release(), ledger, proof, reply);
                    return Err(session::Outcome::Close(reply));
                }
            };
            let Some(admit) = source() else {
                return Err(session::Outcome::Close(canned::SERVICE_UNAVAILABLE));
            };
            Err(admit(Input::Ready { request }, used, close, response, rt))
        });
        match result {
            Ok(pending) => session::BodyOutcome::Pending(Self { pending }),
            Err(outcome) => session::BodyOutcome::Complete(outcome),
        }
    }
}
