use std::net::SocketAddr;

use o3::alloc;
use sark_fiber::client::config;

use crate::common::client::runtime::cases::Flavor;

pub const FLAVORS: [Flavor; 2] = [Flavor::Fixed, Flavor::OnDemand];

pub trait Config {
    fn try_config(self, addr: SocketAddr) -> Result<config::Config, alloc::Exhausted>;
}

impl Config for Flavor {
    fn try_config(self, addr: SocketAddr) -> Result<config::Config, alloc::Exhausted> {
        match self {
            Flavor::Fixed => config::Config::try_new("127.0.0.1"),
            Flavor::OnDemand => Ok(config::Config::new(addr.to_string())),
        }
    }
}
