use o3::alloc;
use sark_client::reply::decode::Policy;
use sark_fiber::client::config::Config;
use sark_http::method::Method;

mod batch;
pub use batch::Batch;
pub mod events;
pub mod pipeline;
pub mod request;
pub mod stream;

#[derive(Clone, Copy)]
pub enum Flavor {
    Fixed,
    OnDemand,
}

impl Flavor {
    pub fn try_request<'a>(
        self,
        addr: &'a str,
        method: Method,
        path: &'a str,
        body: &'a [u8],
    ) -> Result<request::Request<'a>, alloc::Exhausted> {
        Ok(request::Request::new(
            self,
            addr,
            Config::try_new(addr)?,
            method,
            path,
            &[],
            body,
        ))
    }

    pub fn try_get<'a>(
        self,
        addr: &'a str,
        policy: Policy,
        path: &'a str,
    ) -> Result<request::Request<'a>, alloc::Exhausted> {
        Ok(request::Request::new(
            self,
            addr,
            Config::try_new(addr)?.decode(policy),
            Method::GET,
            path,
            &[],
            &[],
        ))
    }
}
