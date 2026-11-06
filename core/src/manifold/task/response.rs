use std::convert::Infallible;

pub type Stream<'d, R, A> =
    <<<R as route::Task<'d, A>>::Output as dispatch::IntoOutput<'d, 'd>>::Output as dispatch::Emit<
        'd,
        'd,
    >>::Stream;

pub type Emitted<'d, S, B> = Result<
    (
        <S as decode::Encode<'d, 'd>>::Prepared,
        Option<B>,
        life::Outcome,
    ),
    (
        <S as decode::Encode<'d, 'd>>::Error,
        <S as decode::Cancel>::Target,
    ),
>;

pub type Prepared<'d, S> = Result<
    (<S as decode::Encode<'d, 'd>>::Prepared, State),
    (
        <S as decode::Encode<'d, 'd>>::Error,
        <S as decode::Cancel>::Target,
    ),
>;

pub type Complete<'d, S> = Result<
    (<S as decode::Encode<'d, 'd>>::Prepared, life::Outcome),
    (
        <S as decode::Encode<'d, 'd>>::Error,
        <S as decode::Cancel>::Target,
    ),
>;

use sark_http::{
    payload::response::{Response, dispatch, dispatch::chunk::Chunk},
    request::Id,
};

use crate::{
    decode, life,
    route::{self, Reject},
};

pub enum Outcome<T> {
    Response(T),
    Rejected(Reject),
}

pub enum Event<'d, E> {
    /// The initial response is held for emission. A polled task yields this exactly once,
    /// before body events; it cannot publish informational heads or trailers.
    Head,
    Chunk(Chunk<'d>),
    Failed(E),
    /// The source ended and its runtime task slot and quota were released before handoff.
    End,
}

pub enum State {
    Complete(life::Outcome),
    Streaming,
}

pub enum Access<T> {
    /// No live stream source exists for this task.
    Absent,
    /// The stream exists but has no held value for this operation.
    Active,
    Ready(T),
}

/// Route-specific result retained by one generated task.
pub struct Output<'d, R: route::Task<'d, A>, A> {
    id: Id<'d>,
    outcome: Outcome<R::Output>,
}

impl<'d, R: route::Task<'d, A>, A> Output<'d, R, A> {
    pub const fn response(id: Id<'d>, response: R::Output) -> Self {
        Self {
            id,
            outcome: Outcome::Response(response),
        }
    }

    pub const fn rejected(id: Id<'d>, reject: Reject) -> Self {
        Self {
            id,
            outcome: Outcome::Rejected(reject),
        }
    }

    pub fn into_parts(self) -> (Id<'d>, Outcome<R::Output>) {
        (self.id, self.outcome)
    }

    pub fn emit<S>(self, make: impl FnOnce(Id<'d>) -> S) -> Emitted<'d, S, Stream<'d, R, A>>
    where
        S: decode::Streaming<'d, 'd>,
    {
        let sink = make(self.id);
        match self.outcome {
            Outcome::Response(response) => {
                let emitted =
                    dispatch::Emit::emit(dispatch::IntoOutput::into_output(response), sink)?;
                let (prepared, stream) =
                    <<<R::Output as dispatch::IntoOutput<'d, 'd>>::Output as dispatch::Emit<
                        'd,
                        'd,
                    >>::Kind as dispatch::Dispatch>::fold::<S, Stream<'d, R, A>, _>(
                        emitted,
                        |prepared| (prepared, None),
                        S::split_stream,
                    );
                Ok((prepared, stream, life::Outcome::Completed))
            }
            Outcome::Rejected(reject) => {
                let prepared = dispatch::Immediate::emit(Response::new(reject.status()), sink)?;
                Ok((prepared, None, life::Outcome::Rejected))
            }
        }
    }
}
/// A completed fixed response whose task slot has already been released.
pub trait Completion<'d>: Sized {
    fn emit<E: decode::Encode<'d, 'd>>(self, make: impl FnOnce(Id<'d>) -> E) -> Complete<'d, E>;
}

impl<'d, R, A> Completion<'d> for Output<'d, R, A>
where
    R: route::Task<'d, A>,
    <R::Output as dispatch::IntoOutput<'d, 'd>>::Output: dispatch::Complete<'d, 'd>,
{
    fn emit<E: decode::Encode<'d, 'd>>(self, make: impl FnOnce(Id<'d>) -> E) -> Complete<'d, E> {
        let sink = make(self.id);
        let (prepared, outcome) = match self.outcome {
            Outcome::Response(response) => (
                dispatch::Immediate::emit(response, sink)?,
                life::Outcome::Completed,
            ),
            Outcome::Rejected(reject) => (
                dispatch::Immediate::emit(Response::new(reject.status()), sink)?,
                life::Outcome::Rejected,
            ),
        };
        Ok((prepared, outcome))
    }
}

impl<'d> Completion<'d> for Infallible {
    fn emit<E: decode::Encode<'d, 'd>>(self, _: impl FnOnce(Id<'d>) -> E) -> Complete<'d, E> {
        match self {}
    }
}
