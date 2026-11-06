use std::net;

use dope::{
    core::{
        driver::route::KeyTag,
        io::socket::{self, Listener, option},
    },
    manifold::{
        self,
        app::{env, env::quota},
        network::listener::{config, topology::Direct},
        timing,
    },
    net::{link::egress, tcp, wire},
};
use o3::buffer::{bytes::window::Snapshot, storage};

use crate::inbound;

pub mod app;
mod error;
mod output;
mod response;
mod sources;

pub use app::session::{Session, Upgrade};
pub use error::Error;
pub use response::Response;

pub type Server<H> =
    Direct<KeyTag<0>, env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>, app::App<H>>;

const DEFAULT_MAX_MESSAGE: usize = 16 * 1024 * 1024;
const WS_MAX_FRAME: usize = 16 * 1024 * 1024;
const WS_MAX_ACC: usize = WS_MAX_FRAME + 64 * 1024;
type Acc = Snapshot<WS_MAX_ACC>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Handshake,
    /// The complete opening prefix is retained until output can be reserved.
    Opening,
    /// The opening failure was reported; its HTTP rejection still needs output.
    OpeningRejected,
    Active,
    MessageTooBig,
    /// The fault was reported; its close frame still needs output capacity.
    Rejected(inbound::Code),
    Closed,
}

pub struct ConnState<P = Option<egress::NonEmpty<storage::Shared>>> {
    phase: Phase,
    eof: bool,
    pending: P,
    pub(super) acc: Acc,
    pub(super) inbound: inbound::Inbound<inbound::ClientPeer>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_MESSAGE)
    }
}

impl<P: Default> ConnState<P> {
    fn new(max_message_size: usize) -> Self {
        Self {
            phase: Phase::Handshake,
            eof: false,
            pending: P::default(),
            acc: Acc::new(),
            inbound: inbound::Inbound::new(max_message_size),
        }
    }
}

#[derive(Clone, Copy)]
pub enum Message<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
}

/// Validated opening request, borrowed until its response is committed.
/// Authorize its origin and credentials through [`app::Sink::opening`].
#[derive(Clone, Copy, Debug)]
pub struct Opening<'a> {
    pub authority: &'a str,
    pub target: &'a str,
    pub origin: Option<&'a [u8]>,
    pub cookie: Option<&'a [u8]>,
}

pub struct Config {
    pub bind: net::SocketAddr,
    pub max_connections: usize,
    pub per_ip: Option<quota::PerIp>,
    pub backlog: socket::Backlog,
    /// Exact opening target; an empty string accepts any target.
    pub path: String,
    /// Maximum payload bytes in one frame, capped at 16 MiB.
    pub max_frame_payload: usize,
    /// Maximum aggregate payload bytes in one data message across its fragments.
    /// Zero accepts only empty data messages; control frames remain independent.
    pub max_message_size: usize,
}

/// Fully configured WebSocket listener without a handler recipe.
#[must_use = "a WebSocket plan must be completed into a topology component"]
pub struct Plan {
    listener: config::Prepared<tcp::Tcp>,
    path: String,
    max_frame_payload: usize,
    max_message_size: usize,
    bound: Option<Listener>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    Listener(manifold::ConfigError),
}

impl Plan {
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let Config {
            bind,
            max_connections: max,
            per_ip: limit,
            backlog,
            path,
            max_frame_payload: frame,
            max_message_size,
        } = config;
        let listener = config::Config::<tcp::Tcp> {
            max_connections: max,
            per_ip: limit,
            direct_flights: max.into(),
            bind,
            backlog,
            stream: tcp::StreamConfig {
                no_delay: Some(option::Switch::On),
                ..Default::default()
            },
            transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
            egress: Default::default(),
        }
        .try_prepare()
        .map_err(ConfigError::Listener)?;
        Ok(Self {
            listener,
            path,
            max_frame_payload: frame,
            max_message_size,
            bound: None,
        })
    }

    pub fn with_bound(mut self, bound: Option<Listener>) -> Self {
        self.bound = bound;
        self
    }

    pub fn with_reader<F: Fn(Message<'_>), E: Fn(Error)>(
        self,
        receive: F,
        failure: E,
    ) -> Server<app::Reader<F, E>> {
        self.with_handler(app::Reader { receive, failure })
    }

    pub fn with_handler<H: app::Sink>(self, user: H) -> Server<H> {
        Direct::new(
            self.listener,
            app::App::new(
                user,
                self.path,
                self.max_frame_payload,
                self.max_message_size,
            ),
        )
        .with_bound(self.bound)
    }
}
