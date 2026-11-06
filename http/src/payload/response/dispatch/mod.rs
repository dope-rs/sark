use o3::buffer::resident::bound;

use crate::{
    payload,
    payload::{
        fixed::Body,
        response::{
            self, bounded, exact,
            headers::{self, set},
            status::Code,
        },
    },
};

pub mod chunk;

pub enum Sync {}
pub enum Polled {}
pub enum Never {}

pub trait Dispatch {
    type Output<'d, E: Sink<'d>, S>;

    fn fold<'d, E: Sink<'d>, S, T>(
        output: Self::Output<'d, E, S>,
        sync: impl FnOnce(E::Output) -> T,
        stream: impl FnOnce(E::Stream<S>) -> T,
    ) -> T;
}

impl Dispatch for Sync {
    type Output<'d, E: Sink<'d>, S> = E::Output;

    fn fold<'d, E: Sink<'d>, S, T>(
        output: Self::Output<'d, E, S>,
        sync: impl FnOnce(E::Output) -> T,
        _: impl FnOnce(E::Stream<S>) -> T,
    ) -> T {
        sync(output)
    }
}

impl Dispatch for Polled {
    type Output<'d, E: Sink<'d>, S> = E::Stream<S>;

    fn fold<'d, E: Sink<'d>, S, T>(
        output: Self::Output<'d, E, S>,
        _: impl FnOnce(E::Output) -> T,
        stream: impl FnOnce(E::Stream<S>) -> T,
    ) -> T {
        stream(output)
    }
}

/// One of two synchronous response shapes without allocation or type erasure.
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

/// Response writes retain only the destination's storage lifetime.
pub trait Accept<'d>: Sized {
    type Output;
    type Error;

    fn fixed<H, B>(self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: response::Body<'d>;

    fn statics<'body, H>(
        self,
        status: Code,
        headers: H,
        body: Body<'body>,
    ) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        'body: 'd,
    {
        self.fixed(status, headers, body)
    }

    fn exact<H, B>(self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: exact::TryBody;

    fn bounded<H, B>(self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: bounded::Body;
}

/// A destination that also transfers ownership of a body stream.
pub trait Sink<'d>: Accept<'d> {
    type Stream<S>;

    fn stream<H, S>(
        self,
        status: Code,
        headers: H,
        stream: S,
    ) -> Result<Self::Stream<S>, Self::Error>
    where
        H: headers::Source;
}

pub trait Emit<'req, 'd>: Sized {
    type Kind: Dispatch;
    type Stream;
    type Bound: bound::Bound + 'd;

    const BODY: payload::Kind;

    fn emit<S>(
        self,
        sink: S,
    ) -> Result<<Self::Kind as Dispatch>::Output<'d, S, Self::Stream>, S::Error>
    where
        S: Sink<'d>;
}

pub trait IntoOutput<'req, 'd>: Sized {
    type Output: Emit<'req, 'd>;

    fn into_output(self) -> Self::Output;
}

#[doc(hidden)]
pub trait Complete<'req, 'd>: Sized {
    type Bound: bound::Bound + 'd;

    const BODY: payload::Kind;

    fn complete<S>(self, sink: S) -> Result<S::Output, S::Error>
    where
        S: Accept<'d>;
}

impl<'req, 'd, T: Complete<'req, 'd>> Emit<'req, 'd> for T {
    type Kind = Sync;
    type Stream = Never;
    type Bound = T::Bound;

    const BODY: payload::Kind = T::BODY;

    fn emit<S: Sink<'d>>(self, sink: S) -> Result<S::Output, S::Error> {
        self.complete(sink)
    }
}

/// Response behavior that cannot produce a polled body stream.
///
pub trait Immediate<'req, 'd>: Sized {
    type Bound: bound::Bound + 'd;

    fn emit<S>(self, sink: S) -> Result<S::Output, S::Error>
    where
        S: Accept<'d>;
}

impl<'req, 'd, T> Immediate<'req, 'd> for T
where
    T: IntoOutput<'req, 'd>,
    T::Output: Complete<'req, 'd>,
{
    type Bound = <T::Output as Complete<'req, 'd>>::Bound;

    fn emit<S>(self, sink: S) -> Result<S::Output, S::Error>
    where
        S: Accept<'d>,
    {
        self.into_output().complete(sink)
    }
}

impl<'req, 'd, L, R> IntoOutput<'req, 'd> for Either<L, R>
where
    L: Immediate<'req, 'd>,
    R: Immediate<'req, 'd>,
{
    type Output = Self;

    fn into_output(self) -> Self::Output {
        self
    }
}

impl<'req, 'd, T, E> IntoOutput<'req, 'd> for Result<T, E>
where
    T: Immediate<'req, 'd>,
    E: Immediate<'req, 'd>,
{
    type Output = Either<T, E>;

    fn into_output(self) -> Self::Output {
        match self {
            Ok(output) => Either::Left(output),
            Err(error) => Either::Right(error),
        }
    }
}

impl<'req, 'd, L, R> Complete<'req, 'd> for Either<L, R>
where
    L: Immediate<'req, 'd>,
    R: Immediate<'req, 'd>,
{
    type Bound = bound::Max<L::Bound, R::Bound>;

    const BODY: payload::Kind = payload::Kind::Inline;

    fn complete<S>(self, sink: S) -> Result<S::Output, S::Error>
    where
        S: Accept<'d>,
    {
        match self {
            Self::Left(output) => output.emit(sink),
            Self::Right(output) => output.emit(sink),
        }
    }
}

pub struct Stream<'req, S, const N: usize = 0> {
    status: Code,
    headers: set::Set<'req, N>,
    stream: S,
}

impl<S, const N: usize> Stream<'_, S, N> {
    pub fn with_status(mut self, status: Code) -> Self {
        self.status = status;
        self
    }
}

impl<'req, S> Stream<'req, S, 0> {
    pub fn new(stream: S) -> Self {
        Self {
            status: Code::OK,
            headers: set::Set::new(),
            stream,
        }
    }

    pub fn headers<const N: usize>(self, headers: set::Set<'req, N>) -> Stream<'req, S, N> {
        Stream {
            status: self.status,
            headers,
            stream: self.stream,
        }
    }
}

impl<'req, 'd, 'head: 'req, S, const N: usize> IntoOutput<'req, 'd> for Stream<'head, S, N> {
    type Output = Self;

    fn into_output(self) -> Self::Output {
        self
    }
}

impl<'req, 'd, 'head: 'req, S, const N: usize> Emit<'req, 'd> for Stream<'head, S, N> {
    type Kind = Polled;
    type Stream = S;
    type Bound = bound::Unbounded;

    const BODY: payload::Kind = payload::Kind::Stream;

    fn emit<E>(self, sink: E) -> Result<E::Stream<S>, E::Error>
    where
        E: Sink<'d>,
    {
        let Self {
            status,
            headers,
            stream,
        } = self;
        sink.stream(status, headers, stream)
    }
}
