pub use dope::fiber::{Fiber, Slab, TaskId};

pub const DEFAULT_CAPACITY: usize = 1024;

use std::future::Future;

use crate::Request;
use crate::service::RouteSpec;
use crate::timer::Timer;

pub trait Route<State>: RouteSpec + 'static {
    fn invoke<'d>(
        &'d self,
        params: <Self as RouteSpec>::Params<'static>,
        req: Request,
        headers: <Self as RouteSpec>::Headers<'static>,
        parsed_body: <Self as RouteSpec>::ParsedBody<'static>,
        state: &'d State,
        timer: Timer<'d>,
    ) -> Fiber<'d, impl Future<Output = <Self as RouteSpec>::Response<'static>> + 'd>
    where
        Self: Sized,
        State: 'd;
}
