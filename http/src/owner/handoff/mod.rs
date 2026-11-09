//! Linear response ownership handoff.

use o3::buffer::hold;

use crate::{payload::owned, request::body};

/// One affine choice of how to project a request source.
pub trait Project<'d>: Sized {
    type Proof: stage::Proof;
    type Owner;

    fn contiguous<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(
            V,
            body::View<'req, 'd>,
            Slot<'req, 'd, (), input::Bound<'req, 'd, Self::Proof>>,
        ) -> T,
    ) -> Result<(T, Self::Owner), (Self, V, body::Error)>;

    fn segmented<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(
            V,
            body::Ref<'req, 'd>,
            Slot<'req, 'd, (), input::Bound<'req, 'd, Self::Proof>>,
        ) -> T,
    ) -> (T, Self::Owner);

    fn discarded<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(V, Slot<'req, 'd, (), input::Bound<'req, 'd, Self::Proof>>) -> T,
    ) -> (T, Self::Owner);
}

pub mod copy;
pub mod input;
mod origin;
mod pending;
mod plan;
mod ready;
mod slot;
pub mod stage;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Retain(owned::Error),
    Overflow(hold::Overflow),
}

pub(crate) use origin::Origin;
pub use pending::Pending;
pub use plan::Plan;
pub(crate) use plan::State;
pub use ready::Ready;
pub use slot::Slot;

const _: () = assert!(size_of::<Error>() <= 4 * size_of::<usize>());
