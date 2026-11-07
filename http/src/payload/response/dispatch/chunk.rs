//! Non-empty streaming response ownership.

use std::ops::ControlFlow;

use o3::buffer::{self, resident, segments};

/// One independently owned, non-empty response fragment.
#[must_use = "publish the response fragment or retain its owner"]
#[repr(transparent)]
pub struct Chunk<'d>(segments::Item<resident::Shared<'d>>);

/// The exact owner rejected because it contains no response bytes.
#[must_use = "recover the rejected empty owner"]
#[derive(Debug)]
pub struct Empty<'d>(resident::Shared<'d>);

impl<'d> Chunk<'d> {
    pub fn try_new(owner: resident::Shared<'d>) -> Result<Self, Empty<'d>> {
        match segments::Item::new(owner) {
            ControlFlow::Continue(owner) => Ok(Self(owner)),
            ControlFlow::Break(owner) => Err(Empty(owner)),
        }
    }

    pub fn owner(&self) -> &resident::Shared<'d> {
        self.0.as_ref()
    }

    pub fn into_owner(self) -> resident::Shared<'d> {
        self.0.into_inner()
    }
}

impl<'d> Empty<'d> {
    pub const fn owner(&self) -> &resident::Shared<'d> {
        &self.0
    }
}

impl buffer::Residency for Chunk<'_> {
    fn resident(&self) -> usize {
        self.0.resident()
    }
}

impl<'d, E> TryFrom<Result<Chunk<'d>, E>> for Chunk<'d> {
    type Error = E;

    fn try_from(item: Result<Self, E>) -> Result<Self, E> {
        item
    }
}

impl<'a: 'd, 'd, const N: usize> From<&'a [u8; N]> for Chunk<'d> {
    /// Retains one borrowed array whose length proves at least one byte.
    fn from(bytes: &'a [u8; N]) -> Self {
        let bytes: &'d [u8; N] = bytes;
        Self(segments::Item::from(bytes))
    }
}

const _: () = {
    assert!(size_of::<Chunk<'static>>() == size_of::<resident::Shared<'static>>());
    assert!(size_of::<Option<Chunk<'static>>>() == size_of::<Option<resident::Shared<'static>>>());
};
