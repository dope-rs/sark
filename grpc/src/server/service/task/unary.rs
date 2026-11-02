use dope::core::driver::schedule::clock::timer;
use dope_fiber::abi;
use o3::buffer::resident;

use crate::server::service::handler::modes;

pub trait Handler {
    type Request<'d>: 'd;
    type Response;
    type Codec: for<'d> crate::Codec<
            Encode = Self::Response,
            Owned<resident::Shared<'d>> = Self::Request<'d>,
        > + Default;
    type Task<'life, 'd>: 'life
    where
        'd: 'life,
        Self: 'life;

    fn unary<'life, 'd>(
        &'life self,
        timer: timer::Timer<'d>,
        request: modes::Request<Self::Request<'d>>,
    ) -> Self::Task<'life, 'd>
    where
        'd: 'life;
}

/// Converts one exact-lifetime unary task into the selected native fiber ABI.
pub trait Fiber: Handler {
    type Fiber<'life, 'd>: abi::Fiber<'d, Output = modes::Response<Self::Response>>
        + From<Self::Task<'life, 'd>>
        + 'life
    where
        'd: 'life,
        Self: 'life;
}
