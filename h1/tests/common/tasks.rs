use std::{cell::Cell, convert::Infallible, marker::PhantomData, num::NonZero};

use o3::buffer::segments::{self, assembly};
use sark_core::{
    body::{self, admit, credit},
    duplex, life, manifold,
    request::{self, frame::proof},
    route::{
        self,
        field::{header, query},
    },
};
use sark_h1::{
    route::{Cache, Policy, PolicyOf},
    session,
    wire::{head::Connection, response::Context},
};
use sark_http::{
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    payload::{owned, response::dispatch},
    request::{Id, body::Ref},
};

thread_local! {
    pub static STEPS: Cell<usize> = const { Cell::new(0) };
    pub static FINISHES: Cell<usize> = const { Cell::new(0) };
    pub static BUDGET: Cell<usize> = const { Cell::new(0) };
}

pub struct Request<P = body::Discarded, M = route::Streaming>(PhantomData<(P, M)>);
pub type Buffered = Request<body::Segmented, route::Buffered>;
pub type BufferedState<'d> = (usize, Option<manifold::task::request::Input<'d, Buffered>>);

pub fn admit_buffered<'d>(
    input: manifold::task::request::Input<'d, Buffered>,
    consumed: usize,
    close: Connection,
    _: Context,
    state: &mut BufferedState<'d>,
) -> session::Outcome<'d> {
    state.0 += 1;
    state.1 = Some(input);
    session::Outcome::Park { consumed, close }
}

pub const HEAD: &[u8] = b"POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\n";
pub const COMPLETE: &[u8] = b"POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\nx";
pub const EXPECT: &[u8] =
    b"POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\nx";
pub const ALLOWED_BODIES: &[(&[u8], u16)] = &[
    (COMPLETE, 200),
    (
        b"CUSTOM / HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\nx",
        200,
    ),
];
pub const CONNECT: &[u8] =
    b"CONNECT local:80 HTTP/1.1\r\nHost: local\r\nConnection: close\r\nContent-Length: 1\r\n\r\nx";
pub const CONNECT_VERSIONED: &[(&[u8], &[u8])] = &[
    (
        b"CONNECT local:80 HTTP/1.0\r\nHost: local\r\nConnection: keep-alive\r\n\r\n",
        b"HTTP/1.0 200 Connection Established\r\n\r\n",
    ),
    (
        b"CONNECT local:80 HTTP/1.1\r\nHost: local\r\n\r\n",
        b"HTTP/1.1 200 Connection Established\r\n\r\n",
    ),
];
pub const PARTIAL_UPGRADE: &[(&[u8], bool)] = &[
    (
        b"HEAD / HTTP/1.1\r\nHost: local\r\nConnection: upgrade\r\n\r\n",
        false,
    ),
    (
        b"CONNECT local:80 HTTP/1.1\r\nHost: local\r\nConnection: upgrade\r\n\r\n",
        true,
    ),
];

impl<P: body::Project, M> route::Schema for Request<P, M> {
    type Header = header::Discard;
    type Query = query::Discard;
    type RawHeaders = ();
    type Error = Infallible;
    type RawParams = ();
    type Body = P;
    type Frame = proof::Maybe;
}

impl<P: body::Project, M> route::Project for Request<P, M> {
    type Params<'req> = ();
    type Headers<'req> = ();
    type Error<'req> = Infallible;
    type View<'req, 'd>
        = usize
    where
        'd: 'req;
    type ParsedBody<'req, 'd>
        = usize
    where
        'd: 'req;

    fn reject(error: Infallible) -> route::Reject {
        match error {}
    }
    fn from_params<'body>(
        _: &request::raw::Parsed<request::Head<&[u8]>, Self>,
    ) -> Result<(), <Self as route::Project>::Error<'body>> {
        Ok(())
    }
    fn from_headers<'body>(
        _: &request::raw::Parsed<request::Head<&[u8]>, Self>,
    ) -> Result<(), <Self as route::Project>::Error<'body>> {
        Ok(())
    }
    fn parse_body<'req, 'd: 'req>(body: P::View<'req, 'd>) -> Result<usize, Infallible> {
        let body: Ref<'req, 'd> = body.into();
        Ok(body.len())
    }
    fn from_parts<'req, 'd: 'req>(_: (), _: (), len: usize, _: &request::View<'req, 'd>) -> usize {
        len
    }
}

impl<P: body::Project, M> route::Spec for Request<P, M> {
    type Kind = dispatch::Sync;
    type Request = Self;
    type Stream<'d> = dispatch::Never;
}

impl<P: body::Project> route::Own for Request<P> {
    type Mode = route::Streaming;
    type Task<'d> = owned::Segments<'d>;
    type Error<'d> = Infallible;
    type TaskBuilder<'d> = Builder<'d>;

    fn reject(error: Infallible) -> route::Reject {
        match error {}
    }
    fn start_task<'d>(
        request: request::raw::Parsed<request::Retained<'d>, Self>,
    ) -> Result<Builder<'d>, Infallible> {
        Ok(Builder {
            id: request.owner().id(),
            remaining: None,
            retained: owned::Segments::new(),
        })
    }
    fn start_stream<'d>(
        request: request::raw::Parsed<request::Parts<'d>, Self>,
    ) -> Result<Builder<'d>, Infallible> {
        Ok(Builder {
            id: request.owner().id(),
            remaining: None,
            retained: owned::Segments::new(),
        })
    }
}

impl<P: body::Project> route::Own for Request<P, route::Buffered> {
    type Mode = route::Buffered;
    type Task<'d> = (admit::Stream, owned::Segments<'d>);
    type Error<'d> = Infallible;
    type TaskBuilder<'d> = BufferedBuilder<'d>;

    fn reject(error: Infallible) -> route::Reject {
        match error {}
    }
    fn start_task<'d>(
        request: request::raw::Parsed<request::Retained<'d>, Self>,
    ) -> Result<BufferedBuilder<'d>, Infallible> {
        let (head, body) = request.into_parts();
        Ok(BufferedBuilder {
            id: head.owner().id(),
            stream: admit::Stream::Buffered,
            body: body.into_body(),
        })
    }
    fn start_stream<'d>(
        request: request::raw::Parsed<request::Parts<'d>, Self>,
    ) -> Result<BufferedBuilder<'d>, Infallible> {
        Ok(BufferedBuilder {
            id: request.owner().id(),
            stream: admit::Stream::Streaming,
            body: owned::Segments::new(),
        })
    }
}

pub struct BufferedBuilder<'d> {
    id: Id<'d>,
    stream: admit::Stream,
    body: owned::Segments<'d>,
}

impl<'d> route::TaskBuild<'d> for BufferedBuilder<'d> {
    type Task = (admit::Stream, owned::Segments<'d>);

    fn reject(error: Infallible) -> route::Reject {
        match error {}
    }
}

impl<'d> body::Build<'d> for BufferedBuilder<'d> {
    type Output = route::Built<'d, (admit::Stream, owned::Segments<'d>)>;
    type StepError = Infallible;

    fn finish_input(self, _: usize) -> Self {
        self
    }
    fn step(self, _: NonZero<usize>) -> body::Step<Self, Self::Output, Infallible> {
        body::Step::Ready {
            output: route::Built::new(self.id, (self.stream, self.body)),
            released: credit::Amount::new(0, segments::Count::EMPTY),
        }
    }
}

pub struct Builder<'d> {
    id: Id<'d>,
    remaining: Option<usize>,
    retained: owned::Segments<'d>,
}

pub struct Streaming;
pub fn stream(ready: bool) -> Option<impl FnOnce((), &mut (), Context)> {
    ready.then_some(|(), _: &mut (), _: Context| {})
}
impl route::Spec for Streaming {
    type Kind = dispatch::Polled;
    type Request = Request;
    type Stream<'d> = ();
}
impl PolicyOf for Streaming {
    const POLICY: Policy = Policy::new(Cache::Bypass, Coding::Plain, Date::Omit, Server::Omit);
}
impl manifold::Route<Cell<usize>> for Streaming {
    type Response<'req, 'd>
        = dispatch::Stream<'req, ()>
    where
        'd: 'req;
    fn invoke<'req, 'd, 'state>(
        (_, _, _, body, state): manifold::Args<'_, 'req, 'd, 'state, Self::Request, Cell<usize>>,
    ) -> Self::Response<'req, 'd>
    where
        'req: 'state,
        'd: 'req,
    {
        state.set(body);
        dispatch::Stream::new(())
    }
}

pub struct Connect;
impl route::Spec for Connect {
    type Kind = duplex::Shape;
    type Request = Request;
    type Stream<'d> = dispatch::Never;
}
impl<'state> duplex::Route<'state, ()> for Connect {
    type Session = Session;
    fn open<'req, 'd>(
        _: (),
        _: &request::View<'req, 'd>,
        _: (),
        _: usize,
        _: &'state (),
    ) -> Self::Session
    where
        'd: 'req + 'state,
        Self::Session: duplex::Session<'d>,
    {
        Session
    }
}

pub struct Session;

impl<'d> duplex::Session<'d> for Session {
    fn receive(
        &mut self,
        _: owned::Segments<'d>,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        duplex::Action::idle()
    }
    fn finish(
        &mut self,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        duplex::Action::idle()
    }
    fn activate(
        &mut self,
        _: o3::buffer::write::Slice<'_, &mut usize>,
        _: &mut Vec<u8>,
    ) -> duplex::Action<'d> {
        duplex::Action::idle()
    }
}

impl<'d> route::TaskBuild<'d> for Builder<'d> {
    type Task = owned::Segments<'d>;

    fn reject(error: Infallible) -> route::Reject {
        match error {}
    }
}

impl<'d> body::Build<'d> for Builder<'d> {
    type Output = route::Built<'d, owned::Segments<'d>>;
    type StepError = Infallible;

    fn finish_input(mut self, _: usize) -> Self {
        FINISHES.set(FINISHES.get() + 1);
        self.remaining = Some(2);
        self
    }
    fn step(mut self, budget: NonZero<usize>) -> body::Step<Self, Self::Output, Infallible> {
        BUDGET.set(budget.get());
        let released = credit::Amount::new(0, segments::Count::EMPTY);
        match self.remaining {
            None => body::Step::NeedInput {
                build: self,
                released,
            },
            Some(0) => {
                STEPS.set(STEPS.get() + 1);
                body::Step::Ready {
                    output: route::Built::new(self.id, self.retained),
                    released,
                }
            }
            Some(left) => {
                STEPS.set(STEPS.get() + 1);
                self.remaining = Some(left - 1);
                body::Step::Pending {
                    build: self,
                    released,
                }
            }
        }
    }
}

impl<'d> route::Push<'d> for Builder<'d> {
    fn reject_push(_: Self::Error) -> route::Reject {
        route::Reject::Unavailable
    }
}

impl<'d> body::Push<'d> for Builder<'d> {
    type Error = assembly::Error;

    fn observe<C: Copy, S: Copy, O: life::Observer<C, S>>(error: &Self::Error, observer: &mut O) {
        observer.diagnostic(error);
    }

    fn push(mut self, segment: owned::Segment<'d>) -> body::Transfer<Self, Self::Error> {
        match self.retained.try_push(segment) {
            Ok(retained) => {
                self.retained = retained;
                body::Transfer::Accepted(self)
            }
            Err(rejected) => {
                let ((retained, _), error) = rejected.into_parts();
                self.retained = retained;
                body::Transfer::Rejected { build: self, error }
            }
        }
    }
}
