use crate::manifold::owners;
use core::marker::PhantomData;
use sark_core::manifold::task::app;

use o3::buffer::{resident, segments};
use sark_core::manifold::stream::Boundary;
use sark_http::payload::response::{Method, status::Code};

use crate::manifold::requests::{
    self, Id,
    duplex::{self, input},
    entries::Reserved,
};

pub(in crate::manifold) enum Head<R> {
    Reserved(R),
    Stopped,
}

/// One reserved slot and its connection-local input pool under an exclusive loan.
#[must_use = "a reserved request head must be committed or discarded"]
pub(in crate::manifold) struct Claim<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff, K = ()> {
    reserved: Reserved<'a, 'd, A, M>,
    segments: &'a mut segments::Pool<resident::Shared<'d>>,
    method: PhantomData<K>,
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> Claim<'a, 'd, A, M> {
    pub(super) fn new(
        reserved: Reserved<'a, 'd, A, M>,
        segments: &'a mut segments::Pool<resident::Shared<'d>>,
    ) -> Self {
        Self {
            reserved,
            segments,
            method: PhantomData,
        }
    }

    pub(in crate::manifold) fn admit(mut self, method: Method) -> Claim<'a, 'd, A, M, Method> {
        self.reserved.admit(method);
        Claim {
            reserved: self.reserved,
            segments: self.segments,
            method: PhantomData,
        }
    }
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff, K> Claim<'a, 'd, A, M, K> {
    pub(in crate::manifold) fn id(&self) -> Id {
        self.reserved.id()
    }

    pub(in crate::manifold) fn discard(self) {
        self.reserved.discard();
    }
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> Claim<'a, 'd, A, M, Method> {
    pub(in crate::manifold) fn method(&self) -> Method {
        self.reserved.method()
    }

    pub(in crate::manifold) fn into_entry(self) -> Reserved<'a, 'd, A, M> {
        self.reserved
    }

    pub(in crate::manifold) fn respond(self, code: Code) {
        self.reserved.respond(code);
    }

    pub(in crate::manifold) fn connect(
        self,
        session: A::Duplex,
        done: Boundary,
    ) -> Result<(), requests::Error> {
        let chain = self
            .segments
            .try_chain()
            .map_err(|error| requests::Error::Segments(segments::Error::Retain(error)))?;
        let mut input = input::Input::open(session, input::Buf::new(chain));
        let finished = match done {
            Boundary::More => Ok(()),
            Boundary::End => input.finish().map_err(requests::Error::from),
        };
        self.reserved.connect(duplex::State {
            input,
            response: duplex::Response::Pending,
        });
        finished
    }
}
