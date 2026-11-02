use dope::core::{
    driver::{route::KeyTag, settings::errors},
    io::socket::Listener,
};

pub mod eof;

#[derive(Debug)]
pub enum Build {
    Config(server::ConfigError),
    Layout(errors::Claim),
}

use dope::{
    manifold::{app::env, network::listener::topology::Direct, timing::Throughput},
    net::{tcp, wire},
    runtime::topology,
};
use sark_ws::server::{self, app};

pub type Prepared<H> = topology::Prepared<
    (),
    Direct<KeyTag<0>, env::Bundle<tcp::Tcp, wire::Identity, Throughput>, app::App<H>>,
>;

pub fn try_new<H: app::Sink>(
    config: server::Config,
    handler: H,
    bound: Listener,
) -> Result<Prepared<H>, Build> {
    let install = server::Plan::new(config)
        .map_err(Build::Config)?
        .with_bound(Some(bound))
        .with_handler(handler);
    topology::Plan::new((), install)
        .for_install::<Throughput>()
        .map_err(Build::Layout)
}
