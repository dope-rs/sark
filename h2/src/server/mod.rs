use std::num::NonZero;

use dope::{
    core::{
        driver::{route::KeyTag, settings, settings::errors},
        io::socket::Listener,
    },
    manifold::{
        app::env,
        network::listener::{handler::failures, topology::Direct},
        timing,
    },
    net::{tcp, wire},
    runtime::topology,
};
use runtime::modes;
use sark_core::body::{self, credit::Limits};
use service::Handler;
#[doc(hidden)]
pub use target::Target;

use crate::{
    conn::{config, outputs::limits},
    frame,
};

pub(crate) mod payload;
#[doc(hidden)]
pub mod response;
#[doc(hidden)]
pub mod runtime;
pub mod service;
pub(crate) mod state;
pub mod sync;
mod target;
pub mod task;

/// HTTP/2 request, connection, and outbound bounds.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub max_request_body_bytes: usize,
    /// Maximum retained transport owners in one buffered request body.
    pub max_request_body_chunks: usize,
    pub max_connection_body_bytes: usize,
    /// Maximum retained transport owners across all request bodies.
    pub max_connection_body_chunks: usize,
    pub max_outbound_bytes: usize,
}

pub const DEFAULT_MAX_REQUEST_BODY_CHUNKS: usize = body::DEFAULT_MAX_REQUEST_CHUNKS;
pub const DEFAULT_MAX_CONNECTION_BODY_CHUNKS: usize = body::DEFAULT_MAX_CONNECTION_CHUNKS;

impl Config {
    pub fn validate(self) -> Result<Self, ConfigError> {
        if self.max_outbound_bytes == 0 {
            return Err(ConfigError::ZeroOutbound);
        }
        Ok(self)
    }

    #[doc(hidden)]
    pub const fn request(self, bytes: usize) -> Limits {
        Limits::new(bytes, self.max_request_body_chunks)
    }

    #[doc(hidden)]
    pub const fn connection(self) -> Limits {
        Limits::new(
            self.max_connection_body_bytes,
            self.max_connection_body_chunks,
        )
    }
}

const EVENT_CAPACITY: usize = 64;

/// Server limits and protocol settings awaiting joint connection admission.
pub struct Plan {
    config: Config,
    receive: settings::Receive,
    protocol: config::Config,
}

impl Plan {
    pub fn new(config: Config, receive: settings::Receive) -> Result<Self, ConfigError> {
        Ok(Self {
            config: config.validate()?,
            receive,
            protocol: config::Config {
                event_capacity: EVENT_CAPACITY,
                ..config::Config::default()
            },
        })
    }

    /// Selects protocol storage and settings before their single admission.
    /// Receive geometry and outbound headroom remain derived from server limits.
    pub fn with_config(mut self, protocol: config::Config) -> Self {
        self.protocol = protocol;
        self
    }

    /// Selects per-connection stream storage before protocol admission.
    pub fn with_streams(mut self, streams: NonZero<usize>) -> Self {
        self.protocol.stream_capacity = streams.get();
        self
    }

    pub const fn config(&self) -> Config {
        self.config
    }

    pub const fn receive(&self) -> settings::Receive {
        self.receive
    }

    #[doc(hidden)]
    pub const fn resident_len(&self) -> usize {
        self.receive.buffers().into_nonzero().get() as usize
            * self.receive.buffer_len().into_nonzero().get() as usize
    }

    pub fn build(&self) -> Result<config::Config, config::Error> {
        self.build_for(self.protocol.stream_capacity)
    }

    pub fn build_for(&self, streams: usize) -> Result<config::Config, config::Error> {
        let mut machine = config::Config {
            stream_capacity: streams,
            ..self.protocol
        };
        let extra = self.config.max_connection_body_chunks;
        machine.payload_capacity = machine
            .payload_capacity
            .extend(extra)
            .map_err(config::Error::PayloadCapacity)?;
        let buffer = self.receive.buffer_len().into_nonzero();
        let frame = machine.local_settings.max_frame_size;
        let data_gap = frame::HEADER_LEN + frame.into_usize();
        machine.inbound_capacity = buffer.get() as usize + data_gap - 1;
        let (_, wire) = limits::Capacity::wire(machine.hpack_encoder.header_list)?;
        let head_gap = wire.bytes();
        let headroom = data_gap.max(head_gap);
        machine.outbound_capacity = self.config.max_outbound_bytes.checked_add(headroom).ok_or(
            config::Error::OutboundHeadroom {
                base: self.config.max_outbound_bytes,
                extra: headroom,
            },
        )?;
        Ok(machine)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    ZeroOutbound,
}

/// A synchronous manifold HTTP/2 server.
pub struct Server<R> {
    router: R,
    plan: runtime::Plan<KeyTag<0>>,
}

impl<R> Server<R> {
    pub fn new(router: R, plan: runtime::Plan<KeyTag<0>>) -> Self {
        Self { router, plan }
    }
}

impl<H: Handler> Server<service::Route<H>> {
    pub fn from_handler(handler: H, plan: runtime::Plan<KeyTag<0>>) -> Self {
        Self::new(service::Route::new(handler), plan)
    }
}

impl<F> Server<service::Route<service::FromFn<F>>>
where
    service::FromFn<F>: Handler,
{
    /// Builds a server from a function with a request-independent response type.
    pub fn from_fn(function: F, plan: runtime::Plan<KeyTag<0>>) -> Self {
        Self::from_handler(service::FromFn::new(function), plan)
    }
}

type Prepared<R> = topology::Prepared<
    (),
    Direct<
        KeyTag<0>,
        env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
        runtime::Local<(R, failures::Close), modes::Sync>,
    >,
>;

impl<R> Server<R>
where
    R: for<'d> sync::Dispatch<'d>,
{
    pub fn prepare(self) -> Result<Prepared<R>, errors::Claim> {
        self.plan
            .with_plain::<_, modes::Sync>((self.router, failures::Close))
            .for_install::<timing::Throughput>()
    }

    pub fn prepare_on(self, listener: Listener) -> Result<Prepared<R>, errors::Claim> {
        Self {
            router: self.router,
            plan: self.plan.with_bound(listener),
        }
        .prepare()
    }
}
