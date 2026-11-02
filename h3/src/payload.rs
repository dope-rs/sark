use std::ops;

use o3::buffer::{resident, segments};
use sark_http::payload::owned;

/// An affine H3 DATA payload retaining its transport owners and boundaries.
#[repr(transparent)]
pub struct Payload<'d> {
    body: owned::Segments<'d>,
}

impl<'d> Payload<'d> {
    pub(crate) fn from_chain(owners: segments::Chain<resident::Shared<'d>>) -> Self {
        Self {
            body: owned::Segments::from(owners),
        }
    }

    pub(crate) fn from_body(body: owned::Segments<'d>) -> Self {
        Self { body }
    }

    pub(crate) fn into_body(self) -> owned::Segments<'d> {
        self.body
    }
}

impl<'d> ops::Deref for Payload<'d> {
    type Target = owned::Segments<'d>;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

const _: () = {
    assert!(size_of::<Payload<'static>>() == size_of::<owned::Segments<'static>>());
};
