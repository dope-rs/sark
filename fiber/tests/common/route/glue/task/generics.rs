use std::{cell::Cell, marker::PhantomData};

use cartel_redis::client::settings;
use dope_test::fibers::Yield;
use sark_http::payload::response::{exact, status};

use crate::common::route::glue::task;

pub trait Source<const N: usize> {
    type Body: exact::Body;

    fn body(&self) -> Self::Body;
}

pub struct State<C, const N: usize> {
    pub source: C,
}

pub const REDIS: settings::Capacities = settings::Capacities {
    connections: 1,
    admission_waiters: 1,
    inflight: 1,
    request_frame_bytes: 64,
    response_bytes: 64,
    response_values: 1,
    response_frame_bytes: 64,
    response_frame_values: 1,
};

impl<'a, 'b> Source<2> for task::State<'a, 'b> {
    type Body = task::Body<'a, 'b>;

    fn body(&self) -> Self::Body {
        task::Body {
            first: self.first,
            second: self.second,
            encoded: self.encoded,
        }
    }
}

impl<'a, 'b, C> Source<2> for (C, task::State<'a, 'b>) {
    type Body = task::Body<'a, 'b>;

    fn body(&self) -> Self::Body {
        self.1.body()
    }
}

#[sark::handler(pub)]
async fn r#reply<C, const N: usize>(state: &State<C, N>) -> exact::Direct<(), C::Body>
where
    'd: 'req,
    C: Source<N>,
{
    let body = state.source.body();
    Yield::new().await;
    exact::Direct::new(status::Code::OK, (), body)
}

pub struct Scoped<'config, 'driver, C, const N: usize> {
    pub source: C,
    pub config: Cell<&'config str>,
    pub observed: Cell<Option<&'config str>>,
    pub driver: PhantomData<fn(&'driver ()) -> &'driver ()>,
}

#[sark::handler(pub)]
async fn scoped<'config, C, const N: usize>(
    state: &Scoped<'config, '_, C, N>,
) -> exact::Direct<(), C::Body>
where
    C: Source<N>,
{
    let body = state.source.body();
    Yield::new().await;
    state.observed.set(Some(state.config.get()));
    exact::Direct::new(status::Code::OK, (), body)
}

sark::define_route! {
    pub ScopedApp<'env, __P, const __E: usize>: Scoped<'env, '_, __P, __E>
    where __P: Source<__E>
    => {
        GET "/" => async(capacity = 2, body = discarded) scoped,
    }
}

sark::define_route! {
    pub r#App<__W, const PRODUCER: usize>: State<__W, PRODUCER>
    where __W: Source<PRODUCER>
    => {
        GET "/" => async(capacity = 2, body = discarded) reply,
    }
}
