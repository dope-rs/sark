use std::time;

use dope::{manifold::client::port, net::link::egress};
use o3::{
    alloc::{self, StrExt as _},
    buffer::pool,
    depot::capacity,
};
use reply::decode::Policy;
use sark_client::{
    protocol::{codec::Codec, retry},
    reply,
};

use crate::client::exchange::{net, request};

mod decode;
mod error;
mod guard;
mod limits;
#[doc(hidden)]
pub mod report;
mod response;
mod stage;

pub(in crate::client) use decode::Decode;
pub use error::Error;
pub(in crate::client) use guard::Guard;
pub use stage::Stage;

pub const DEFAULT_REQUEST_SLOTS: u32 = 256;
pub const DEFAULT_REQUEST_CAPACITY: u32 = 64 * 1024;

const DEFAULT_REQUEST_TIMEOUT: time::Duration = time::Duration::from_secs(15);
const DEFAULT_HEAD_TIMEOUT: time::Duration = time::Duration::from_secs(5);

pub struct Config {
    host: String,
    decode: decode::Decode,
    request: request::Policy,
    limits: limits::Limits,
    response: response::Response,
    request_slots: u32,
    request_capacity: u32,
}

pub(super) struct Plan {
    pub(super) codec: Codec,
    pub(super) decode: Decode,
    pub(super) response_resident_capacity: usize,
    pub(super) staging: port::Plan<pool::Cursor>,
    pub(super) requests: request::Plan,
    pub(super) transport: net::Plan,
    pub(super) response_segments: capacity::Count,
}

impl Config {
    pub fn new(host: String) -> Self {
        Self {
            host,
            decode: decode::Decode::default(),
            request: request::Policy {
                max_redirects: 10,
                retry: retry::Policy::default(),
                timeout: DEFAULT_REQUEST_TIMEOUT,
                dispatch_timeout: time::Duration::MAX,
                head_timeout: DEFAULT_HEAD_TIMEOUT,
                decode_timeout: time::Duration::MAX,
            },
            limits: limits::Limits::default(),
            response: response::Response::default(),
            request_slots: DEFAULT_REQUEST_SLOTS,
            request_capacity: DEFAULT_REQUEST_CAPACITY,
        }
    }

    pub fn try_new(host: &str) -> Result<Self, alloc::Exhausted> {
        Ok(Self::new(host.try_to_owned()?))
    }

    pub fn decode(mut self, policy: Policy) -> Self {
        self.decode.policy = policy;
        self
    }

    pub fn head_limit(mut self, bytes: usize) -> Self {
        self.response.head_limit = bytes;
        self
    }

    pub fn wire_limit(mut self, bytes: usize) -> Self {
        self.response.body_limit = bytes;
        self
    }

    pub fn plain_limit(mut self, bytes: usize) -> Self {
        self.decode.body_limit = bytes;
        self
    }

    pub fn wire_memory(mut self, bytes: usize) -> Self {
        self.response.resident_capacity = bytes;
        self
    }

    pub fn plain_memory(mut self, bytes: usize) -> Self {
        self.decode.resident_capacity = bytes;
        self
    }

    /// Sets the logical response bytes available to each connection lane.
    pub fn queue_bytes(mut self, bytes: usize) -> Self {
        self.response.queue_capacity = bytes;
        self
    }

    pub fn retry(mut self, retry: retry::Policy) -> Self {
        self.request.retry = retry;
        self
    }

    pub fn idle_timeout(mut self, idle: time::Duration) -> Self {
        self.limits.idle_timeout = idle;
        self
    }

    /// Bounds dispatch, retries/backoff, redirects, response collection and decode
    /// within one absolute deadline. For streams, only dispatch is bounded;
    /// response event deadlines remain the caller's responsibility.
    pub fn timeout(mut self, duration: time::Duration) -> Self {
        self.request.timeout = duration;
        self
    }

    /// Bounds the selected stage within the remaining request timeout.
    /// Dispatch applies per attempt, Head per collected head, and Decode per response.
    /// Head defaults to five seconds; Dispatch and Decode have no narrower limit.
    pub fn limit(mut self, stage: Stage, duration: time::Duration) -> Self {
        match stage {
            Stage::Dispatch => self.request.dispatch_timeout = duration,
            Stage::Head => self.request.head_timeout = duration,
            Stage::Decode => self.request.decode_timeout = duration,
        }
        self
    }

    pub fn inflight(mut self, max: usize) -> Self {
        self.limits.max_inflight = max;
        self
    }

    /// Sets the number of returned exact wire failures that may remain alive
    /// without reducing the configured live-connection admission ceiling.
    pub fn failures(mut self, capacity: usize) -> Self {
        self.limits.retained_wire_failures = capacity;
        self
    }

    pub fn request_pool(mut self, slots: u32, capacity: u32) -> Self {
        self.request_slots = slots;
        self.request_capacity = capacity;
        self
    }
}

impl Plan {
    pub(super) fn from_config<F: report::Class>(
        config: Config,
        lanes: usize,
        conns: usize,
    ) -> Result<Plan, Error> {
        let Config {
            host,
            decode,
            request,
            limits,
            response,
            request_slots: slots,
            request_capacity: cap,
        } = config;
        let codec = response.codec();
        let reports = F::capacity(conns, limits.retained_wire_failures)?;
        let network = net::Plan::new(
            lanes,
            limits.max_inflight,
            response.queue_capacity,
            codec.output_bound(),
            limits.idle_timeout,
            reports,
        )?;
        let segments = capacity::Count::new(response::SEGMENTS.get());
        if slots == 0 {
            return Err(Error::RequestSlots);
        }
        let buffers =
            pool::Layout::new(slots as usize, cap as usize).map_err(Error::RequestLayout)?;
        let resident = slots
            .checked_mul(cap)
            .ok_or(Error::RequestResidentCapacity)?;
        let egress = egress::Config::shared(egress::Credit::new(
            egress::Entries::new(slots),
            egress::Resident::new(resident),
        ));
        let staging = port::Plan::new(lanes, egress).map_err(Error::ConnectorCapacity)?;
        Ok(Plan {
            codec,
            decode,
            response_resident_capacity: response.resident_capacity,
            staging,
            requests: request::Plan {
                buffers,
                origin: request::Origin::new(host)?,
                policy: request,
            },
            transport: network,
            response_segments: segments,
        })
    }
}
