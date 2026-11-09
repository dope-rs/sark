use core::mem;

use o3::buffer::{resident, segments};
use sark_http::{payload, payload::response::dispatch};

use crate::{request, route};

pub mod client;
pub mod raw;
pub mod schedule;
pub mod server;
pub mod stream;
pub mod task;

/// Final-link HTTP/1 payload retaining its physical owners and transport tag.
#[doc(hidden)]
pub enum Body<'d, K: 'd = ()> {
    Shared {
        owner: resident::Shared<'d>,
        tag: K,
    },
    Segments {
        owners: segments::Many<resident::Shared<'d>>,
        tag: K,
    },
    Frozen {
        owner: payload::compress::Frozen,
        tag: K,
    },
}

impl<'d, K> Body<'d, K> {
    /// Lowers admitted owners without copying bytes or segment metadata.
    #[doc(hidden)]
    pub fn duplex(chunk: segments::Nonempty<resident::Shared<'d>>, tag: K) -> Self {
        match chunk {
            segments::Nonempty::One(owner) => Self::Shared {
                owner: owner.into_inner(),
                tag,
            },
            segments::Nonempty::Many(owners) => Self::Segments { owners, tag },
        }
    }

    /// Returns every physical owner from a publication rejected before the
    /// final link accepted it.
    #[doc(hidden)]
    pub fn release(self) -> K {
        match self {
            Self::Shared { tag, .. } | Self::Segments { tag, .. } | Self::Frozen { tag, .. } => tag,
        }
    }
}

pub struct EmptyState;

impl EmptyState {
    pub const REF: &'static Self = &Self;
}

pub trait Response<'req, 'd>: Sized {
    type Kind;
    type Output: dispatch::Emit<'req, 'd>;
    type Stream;

    const BODY_KIND: payload::Kind = <Self::Output as dispatch::Emit<'req, 'd>>::BODY;
}

impl<'req, 'd, R> Response<'req, 'd> for R
where
    R: dispatch::IntoOutput<'req, 'd>,
{
    type Kind = <R::Output as dispatch::Emit<'req, 'd>>::Kind;
    type Output = R::Output;
    type Stream = <R::Output as dispatch::Emit<'req, 'd>>::Stream;
}

/// Projected request parts and the independently borrowed application state.
pub type Args<'view, 'req, 'd, 'state, R, State> = (
    <R as route::Project>::Params<'req>,
    &'view request::View<'req, 'd, <R as route::Schema>::Frame>,
    <R as route::Project>::Headers<'req>,
    <R as route::Project>::ParsedBody<'req, 'd>,
    &'state State,
);

pub trait Route<State>: route::Spec<Kind: dispatch::Dispatch> {
    type Response<'req, 'd>: dispatch::Emit<'req, 'd, Kind = Self::Kind, Stream = Self::Stream<'d>>
    where
        'd: 'req;

    fn invoke<'req, 'd, 'state>(
        input: Args<'_, 'req, 'd, 'state, Self::Request, State>,
    ) -> Self::Response<'req, 'd>
    where
        'req: 'state,
        'd: 'req;
}

const _: [(); 0] = [(); mem::size_of::<EmptyState>()];
