use std::convert::Infallible;

use o3::buffer::hold::{self, evidence};

use crate::owner::handoff::{Ready, stage};

enum Wait<'d, D> {
    Ready(Ready<'d>),
    Body(D),
}

/// Borrow-free response state waiting for an original request owner.
#[must_use = "finish the response owner handoff"]
#[doc(hidden)]
pub struct Pending<'d, M: evidence::Mode = evidence::Off, D = Infallible> {
    wait: Wait<'d, D>,
    proof: M::Projection,
}

impl<'d, M: evidence::Mode, D> Pending<'d, M, D> {
    pub(super) const fn ready(body: Ready<'d>, proof: M::Projection) -> Self {
        Self {
            wait: Wait::Ready(body),
            proof,
        }
    }

    pub(super) const fn body(body: D, proof: M::Projection) -> Self {
        Self {
            wait: Wait::Body(body),
            proof,
        }
    }
}

impl<'d, D: stage::Deferred<'d>> Pending<'d, evidence::Off, D> {
    pub fn finish(self, body: D::Owner) -> Ready<'d> {
        resolve(self.wait, body)
    }
}

impl<'d, D: stage::Deferred<'d>> Pending<'d, evidence::On, D> {
    pub fn finish(self, body: D::Owner) -> (Ready<'d>, hold::Charge) {
        let Self { wait, proof } = self;
        let ready = resolve(wait, body);
        (ready, proof)
    }
}

fn resolve<'d, D: stage::Deferred<'d>>(wait: Wait<'d, D>, body: D::Owner) -> Ready<'d> {
    match wait {
        Wait::Ready(body) => body,
        Wait::Body(selected) => Ready::Linked(selected.resolve(body)),
    }
}

const _: () = {
    assert!(std::mem::size_of::<Pending<'static>>() <= 7 * std::mem::size_of::<usize>());
};
