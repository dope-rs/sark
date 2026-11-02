use core::marker::PhantomData;

use o3::buffer::{resident, segments};
use sark_core::{body, decode::request, duplex::Session};
use sark_http::payload::response::{Method, status::Code};

use crate::manifold::requests::{
    self, Id,
    duplex::{self, input},
    entries::{Completion, Reserved, status},
};

pub(in crate::manifold) enum Head<R> {
    Reserved(R),
    Stopped,
}

/// One reserved slot and its connection-local input pool under an exclusive loan.
#[must_use = "a reserved request head must be committed or discarded"]
pub(in crate::manifold) struct Claim<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
    M = (),
> {
    reserved: Reserved<'a, 'd, P, B, T, D>,
    segments: &'a mut segments::Pool<resident::Shared<'d>>,
    method: PhantomData<M>,
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>>
    Claim<'a, 'd, P, B, T, D>
{
    pub(super) fn new(
        reserved: Reserved<'a, 'd, P, B, T, D>,
        segments: &'a mut segments::Pool<resident::Shared<'d>>,
    ) -> Self {
        Self {
            reserved,
            segments,
            method: PhantomData,
        }
    }

    pub(in crate::manifold) fn admit(
        mut self,
        method: Method,
    ) -> Claim<'a, 'd, P, B, T, D, Method> {
        self.reserved.admit(method);
        Claim {
            reserved: self.reserved,
            segments: self.segments,
            method: PhantomData,
        }
    }
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>, M>
    Claim<'a, 'd, P, B, T, D, M>
{
    pub(in crate::manifold) fn id(&self) -> Id {
        self.reserved.id()
    }

    pub(in crate::manifold) fn discard(self) {
        self.reserved.discard();
    }
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>>
    Claim<'a, 'd, P, B, T, D, Method>
{
    pub(in crate::manifold) fn method(&self) -> Method {
        self.reserved.method()
    }

    pub(in crate::manifold) fn into_entry(self) -> Reserved<'a, 'd, P, B, T, D> {
        self.reserved
    }

    pub(in crate::manifold) fn respond(self, code: Code) {
        self.reserved.respond(status::State {
            code,
            queue: status::Queue::Ready,
        });
    }

    pub(in crate::manifold) fn connect(
        self,
        session: D,
        done: Completion,
    ) -> Result<(), requests::Error> {
        let chain = self
            .segments
            .try_chain()
            .map_err(|error| requests::Error::Segments(segments::Error::Retain(error)))?;
        let mut input = input::Input::open(session, chain);
        let finished = match done {
            Completion::Open => Ok(()),
            Completion::Finished => input.finish().map_err(requests::Error::from),
        };
        self.reserved.connect(duplex::State {
            input,
            response: duplex::Response::Pending,
            queue: status::Queue::Ready,
        });
        finished
    }
}
