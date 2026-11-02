use core::convert::Infallible;
type Emitted<'d, S> = Result<
    (<S as decode::Encode<'d, 'd>>::Prepared, life::Outcome),
    (
        <S as decode::Encode<'d, 'd>>::Error,
        <S as decode::Cancel>::Target,
    ),
>;

use sark_http::{
    payload::response::{Response, dispatch},
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

    pub const fn id(&self) -> Id<'d> {
        self.id
    }

    pub fn into_parts(self) -> (Id<'d>, Outcome<R::Output>) {
        (self.id, self.outcome)
    }

    pub fn emit<S>(self, sink: S) -> Emitted<'d, S>
    where
        S: decode::Encode<'d, 'd>,
    {
        match self.outcome {
            Outcome::Response(response) => {
                let prepared =
                    <<R::Output as dispatch::IntoOutput<'d, 'd>>::Output as dispatch::Complete<
                        'd,
                        'd,
                    >>::complete(
                        <R::Output as dispatch::IntoOutput<'d, 'd>>::into_output(response),
                        sink,
                    )?;
                Ok((prepared, life::Outcome::Completed))
            }
            Outcome::Rejected(reject) => {
                let prepared = dispatch::Immediate::emit(Response::new(reject.status()), sink)?;
                Ok((prepared, life::Outcome::Rejected))
            }
        }
    }
}

/// Homogeneous generated completion retained by a multiplexed protocol.
pub trait Completion<'d>: Sized {
    fn id(&self) -> Id<'d>;

    fn emit<S: decode::Encode<'d, 'd>>(self, sink: S) -> Emitted<'d, S>;
}

impl<'d> Completion<'d> for Infallible {
    fn id(&self) -> Id<'d> {
        match *self {}
    }

    fn emit<S: decode::Encode<'d, 'd>>(self, _: S) -> Emitted<'d, S> {
        match self {}
    }
}

/// Slot-specific construction of one homogeneous retained completion.
pub trait Pack<'d, R: route::Task<'d, A>, A, T> {
    fn pack(output: Output<'d, R, A>) -> T;
}
