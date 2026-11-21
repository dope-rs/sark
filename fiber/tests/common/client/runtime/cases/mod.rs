use sark_fiber::client::config::Config;
use std::net::SocketAddr;

mod batch;
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
    pub fn get(self, addr: SocketAddr, path: &str) -> request::Request<'_> {
        request::Request::get(self, addr, Config::new(addr.to_string()), path)
    }
}
