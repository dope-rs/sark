use core::{convert::Infallible, num::NonZero};

use o3::buffer::segments::Count;
use sark_http::request::Id;

use super::{Built, Own, Project, Reject, Schema, Streaming, TaskBuild, field};
use crate::{
    body::{self, credit::Amount},
    manifold::stream::Boundary,
    request::{Head, Parts, Retained, frame::proof, raw::Parsed},
};

impl Schema for () {
    type Header = field::header::Discard;
    type Query = field::query::Discard;
    type RawHeaders = ();
    type Error = Infallible;
    type RawParams = ();
    type Body = body::Discarded;
    type Frame = proof::Maybe;
}

impl Project for () {
    type Error<'req> = Infallible;
    type View<'req, 'd>
        = ()
    where
        'd: 'req;

    fn reject<'req>(error: <Self as Project>::Error<'req>) -> Reject {
        match error {}
    }

    fn try_view<'req, 'body: 'req, 'd: 'body>(
        _: &Parsed<Head<&'req [u8], Self::Frame>, Self>,
        _: body::Scoped<'body, 'd, Self::Body>,
    ) -> Result<Self::View<'req, 'd>, <Self as Project>::Error<'body>> {
        Ok(())
    }
}

fn completed_id<'d>(request: Parsed<Retained<'d>, ()>) -> Id<'d> {
    let (head, body) = request.into_parts();
    drop(body);
    head.owner().id()
}

impl<'d> From<Parsed<Retained<'d>, ()>> for Built<'d, ()> {
    fn from(request: Parsed<Retained<'d>, ()>) -> Self {
        Self::new(completed_id(request), ())
    }
}

impl Own for () {
    type Mode = Streaming;
    type Task<'d> = ();
    type Input<'d> = Built<'d, ()>;
    type Error<'d> = Infallible;
    type TaskBuilder<'d> = Builder<'d>;

    fn empty_task<'d>(id: Id<'d>) -> Option<Built<'d, ()>> {
        Some(Built::new(id, ()))
    }

    fn reject<'d>(error: <Self as Own>::Error<'d>) -> Reject {
        match error {}
    }

    fn start_task<'d>(request: Parsed<Retained<'d>, Self>) -> Result<Builder<'d>, Infallible> {
        Ok(Builder {
            id: completed_id(request),
            boundary: Boundary::End,
        })
    }

    fn start_stream<'d>(request: Parsed<Parts<'d>, Self>) -> Result<Builder<'d>, Infallible> {
        Ok(Builder {
            id: request.owner().id(),
            boundary: Boundary::More,
        })
    }
}

pub struct Builder<'d> {
    id: Id<'d>,
    boundary: Boundary,
}

impl<'d> TaskBuild<'d> for Builder<'d> {
    type Task = ();

    fn reject(error: Infallible) -> Reject {
        match error {}
    }
}

impl<'d> body::Build<'d> for Builder<'d> {
    type Output = Built<'d, ()>;
    type StepError = Infallible;

    fn finish_input(mut self, _: usize) -> Self {
        self.boundary = Boundary::End;
        self
    }

    fn step(self, _: NonZero<usize>) -> body::Step<Self, Self::Output, Infallible> {
        let released = Amount::new(0, Count::EMPTY);
        match self.boundary {
            Boundary::More => body::Step::NeedInput {
                build: self,
                released,
            },
            Boundary::End => body::Step::Ready {
                output: Built::new(self.id, ()),
                released,
            },
        }
    }
}
