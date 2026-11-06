use core::mem;

use sark_http::{payload, payload::response::dispatch};

use crate::{request, route};

pub mod client;
pub mod raw;
pub mod schedule;
pub mod server;
pub mod stream;
pub mod task;

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
