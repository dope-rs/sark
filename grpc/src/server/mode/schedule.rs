//! Exact scheduler families for driver-scoped gRPC task modes.

use core::{convert::Infallible, num::NonZero};

use dope::{
    core::driver::{route::Identity, schedule::ready::task::owner},
    manifold::network::listener::conn,
};
use sark_core::manifold::task::schedule;

use crate::server::{
    runtime::{state::active, stream::Domain, unary},
    service::{
        handler::modes,
        task::{stream, unary::Handler},
    },
};

/// Unary schedulers selected independently for every driver-domain lifetime.
pub trait Unary<H, ID: Identity>
where
    H: Handler,
{
    type Task<'life, 'd>: From<H::Task<'life, 'd>> + 'life
    where
        'd: 'life,
        Self: 'life,
        H: 'life;
    type Scheduler<'s, 'r, 'life, 'd>: schedule::raw::Scheduler<
            'd,
            unary::Domain,
            conn::Id<'d, ID>,
            active::Entry<'r>,
            Self::Task<'life, 'd>,
            modes::Response<H::Response>,
        >
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life;

    fn with<'r, 'life, 'd, O>(
        capacity: NonZero<u32>,
        owners: owner::Capacity,
        run: impl for<'s> FnOnce(Self::Scheduler<'s, 'r, 'life, 'd>) -> O,
    ) -> Result<O, schedule::Error>
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life;
}

/// Streaming schedulers selected independently for every driver-domain lifetime.
pub trait Streaming<H, ID: Identity>
where
    H: stream::Handler,
{
    type Scheduler<'s, 'r, 'life, 'd>: schedule::raw::Scheduler<
            'd,
            Domain,
            conn::Id<'d, ID>,
            active::Entry<'r>,
            stream::Fiber<'d, H::Task<'life, 'd>, H::Request<'d>, H::Response, H::Codec>,
            Infallible,
        >
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life;

    fn with<'r, 'life, 'd, O>(
        capacity: NonZero<u32>,
        owners: owner::Capacity,
        run: impl for<'s> FnOnce(Self::Scheduler<'s, 'r, 'life, 'd>) -> O,
    ) -> Result<O, schedule::Error>
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life;
}
