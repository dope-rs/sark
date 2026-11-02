use std::{borrow::Borrow, convert::Infallible};

use crate::{owner::handoff::Ready, payload::owned, request::body};

mod seal;
pub(crate) use seal::Seal;

/// A borrow-independent source identity carried with a retained view.
pub trait Proof: Seal + Copy {
    type Deferred;

    fn defer(self, body: body::Retained<'_, '_>) -> Self::Deferred;
}

impl Proof for () {
    type Deferred = usize;

    fn defer(self, body: body::Retained<'_, '_>) -> usize {
        Body::defer(body)
    }
}

/// An affine source selection paired with its exact owned successor.
pub trait Deferred<'d>: Seal + Sized {
    type Owner;

    fn resolve(self, owner: Self::Owner) -> owned::Segments<'d>;
}

impl<'d> Deferred<'d> for Infallible {
    type Owner = ();

    fn resolve(self, _: ()) -> owned::Segments<'d> {
        match self {}
    }
}

/// The retained request source selected before response staging.
pub trait Body<'req, 'd: 'req>: Seal + Copy + Borrow<body::Retained<'req, 'd>> {
    type Deferred;

    fn defer(self) -> Self::Deferred;
}

impl<'req, 'd: 'req> Body<'req, 'd> for Infallible {
    type Deferred = Infallible;

    fn defer(self) -> Infallible {
        match self {}
    }
}

/// Result of separating an H1 response from its live request body.
#[must_use = "send or defer the staged response body"]
pub enum State<'d, S = Infallible, B = Infallible> {
    Ready(Ready<'d>),
    Body(B),
    Head(S),
}

const _: () = {
    assert!(size_of::<State<'static>>() <= size_of::<Ready<'static>>() + size_of::<usize>());
    assert!(
        size_of::<State<'static, Infallible, body::Retained<'static, 'static>>>()
            <= size_of::<Ready<'static>>() + size_of::<usize>()
    );
};
