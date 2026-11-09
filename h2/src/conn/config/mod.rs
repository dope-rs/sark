use std::{marker, num::NonZero};

use dynamic::plan;
use o3::{
    buffer::{pool, segments, segments::queue, storage::inline},
    depot::{capacity::Count, map},
};
use sark_http::{field::metadata::block, payload::compress::dynamic};

use crate::{
    Endpoint, conn,
    conn::{ingress::payloads, outputs::limits},
    flow,
    frame::numbers,
};

pub mod capacity;
mod error;
mod field;
pub mod payload;

pub use error::Error;
pub use field::Field;

pub(crate) const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 16_384;
/// Maximum retained ingress bytes used by the default connection plan.
#[doc(hidden)]
pub const DEFAULT_INBOUND_CAPACITY: usize = 1 << 20;
pub(super) const DEFAULT_OUTBOUND_CAPACITY: usize = 1 << 20;
pub(super) const DEFAULT_EVENT_CAPACITY: usize = 1 << 13;
pub(super) const DEFAULT_DATA_EVENTS: NonZero<usize> = NonZero::<usize>::MIN.saturating_add(63);
pub(super) const DEFAULT_HEADER_EVENTS: NonZero<usize> = NonZero::<usize>::MIN.saturating_add(63);
const DEFAULT_INGRESS_SEGMENTS: usize = DEFAULT_DATA_EVENTS.get() + DEFAULT_HEADER_EVENTS.get() + 1;
const DEFAULT_RETAINED_SEGMENTS: usize = DEFAULT_DATA_EVENTS.get() + DEFAULT_INGRESS_SEGMENTS - 1;
pub const DEFAULT_HPACK_ENCODER_CAPACITY: u32 = 4096;
pub(crate) const DEFAULT_STREAM_CAPACITY: usize = 256;
const DEFAULT_STREAM_RECV_WINDOW: u32 = 4 << 20;
const DEFAULT_RECV_WINDOW: u32 = 16 << 20;

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Push {
    Disabled,
    Enabled,
}

const _: () = {
    assert!(core::mem::size_of::<Push>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Push>() == core::mem::align_of::<bool>());
};

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Settings {
    pub header_table_size: u32,
    pub enable_push: Push,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: u32,
    pub max_frame_size: numbers::Limit,
    pub max_header_list_size: Option<u32>,
    pub enable_connect_protocol: block::Connect,
}

pub(crate) type Wire = inline::Bytes<42>;

impl Settings {
    pub const DEFAULT: Self = Self {
        header_table_size: crate::hpack::INITIAL_TABLE_SIZE,
        enable_push: Push::Enabled,
        max_concurrent_streams: None,
        initial_window_size: 65_535,
        max_frame_size: numbers::Limit::DEFAULT,
        max_header_list_size: None,
        enable_connect_protocol: block::Connect::Disabled,
    };

    pub fn apply(&mut self, id: numbers::SettingId, value: u32) -> Result<(), conn::Error> {
        use crate::conn::Error;
        match id {
            numbers::SettingId::HeaderTableSize => {
                self.header_table_size = value;
            }
            numbers::SettingId::EnablePush => match value {
                0 => self.enable_push = Push::Disabled,
                1 => self.enable_push = Push::Enabled,
                _ => return Err(Error::Setting { id, value }),
            },
            numbers::SettingId::MaxConcurrentStreams => {
                self.max_concurrent_streams = Some(value);
            }
            numbers::SettingId::InitialWindowSize => {
                if value > 0x7fff_ffff {
                    return Err(Error::Setting { id, value });
                }
                self.initial_window_size = value;
            }
            numbers::SettingId::MaxFrameSize => {
                self.max_frame_size =
                    numbers::Limit::new(value).ok_or(Error::Setting { id, value })?;
            }
            numbers::SettingId::MaxHeaderListSize => {
                self.max_header_list_size = Some(value);
            }
            numbers::SettingId::EnableConnectProtocol => match value {
                0 => self.enable_connect_protocol = block::Connect::Disabled,
                1 => self.enable_connect_protocol = block::Connect::Enabled,
                _ => return Err(Error::Setting { id, value }),
            },
        }
        Ok(())
    }

    fn wire<R: crate::Role>(&self) -> Wire {
        let mut bytes = Wire::from([0; 42]);
        let params = [
            Some(Self::param(
                numbers::SettingId::HeaderTableSize,
                self.header_table_size,
            )),
            match R::ENDPOINT {
                Endpoint::Client => Some(Self::param(
                    numbers::SettingId::EnablePush,
                    match self.enable_push {
                        Push::Disabled => 0,
                        Push::Enabled => 1,
                    },
                )),
                Endpoint::Server => None,
            },
            self.max_concurrent_streams
                .map(|value| Self::param(numbers::SettingId::MaxConcurrentStreams, value)),
            Some(Self::param(
                numbers::SettingId::InitialWindowSize,
                self.initial_window_size,
            )),
            Some(Self::param(
                numbers::SettingId::MaxFrameSize,
                self.max_frame_size.into_u32(),
            )),
            self.max_header_list_size
                .map(|value| Self::param(numbers::SettingId::MaxHeaderListSize, value)),
            match self.enable_connect_protocol {
                block::Connect::Disabled => None,
                block::Connect::Enabled => {
                    Some(Self::param(numbers::SettingId::EnableConnectProtocol, 1))
                }
            },
        ];
        let mut length = 0;
        for (slot, param) in bytes
            .backing_mut()
            .as_chunks_mut::<6>()
            .0
            .iter_mut()
            .zip(params.into_iter().flatten())
        {
            slot.copy_from_slice(&param);
            length += 6;
        }
        bytes.truncate(length);
        bytes
    }

    fn param(id: numbers::SettingId, value: u32) -> [u8; 6] {
        let id = (id as u16).to_be_bytes();
        let value = value.to_be_bytes();
        [id[0], id[1], value[0], value[1], value[2], value[3]]
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Config {
    pub local_settings: Settings,
    pub hpack_encoder: capacity::Encoder,
    pub recv_window_target: u32,
    pub stream_capacity: usize,
    pub event_capacity: usize,
    pub payload_capacity: capacity::Payload,
    pub inbound_capacity: usize,
    pub outbound_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            local_settings: Settings {
                initial_window_size: DEFAULT_STREAM_RECV_WINDOW,
                ..Settings::DEFAULT
            },
            hpack_encoder: capacity::Encoder::default(),
            recv_window_target: DEFAULT_RECV_WINDOW,
            stream_capacity: DEFAULT_STREAM_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            payload_capacity: capacity::Payload::default(),
            inbound_capacity: DEFAULT_INBOUND_CAPACITY,
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Validated<R: crate::Role> {
    pub(super) local: Settings,
    pub(super) recv_window_target: flow::Target,
    pub(super) streams: map::Plan,
    pub(super) egress: limits::Initial<R>,
    pub(super) hpack: plan::Plans,
    pub(super) buffers: BufferPlans,
    pub(super) marker: marker::PhantomData<fn() -> R>,
}

#[derive(Clone, Copy)]
pub(super) struct BufferPlans {
    pub(super) retention: payloads::Plan,
    pub(super) header_plan: pool::Layout,
    pub(super) inbound_header_list_cap: usize,
    pub(super) outbound_headers: limits::Block,
    pub(super) header_wire: pool::Layout,
    pub(super) ingress: queue::Capacity,
    pub(super) event_capacity: NonZero<u32>,
}

impl<R: crate::Role> Validated<R> {
    pub fn new(config: Config) -> Result<Self, Error> {
        let Config {
            local_settings: local,
            hpack_encoder: encoder,
            recv_window_target: recv,
            stream_capacity: stream,
            event_capacity: events,
            payload_capacity: payload,
            inbound_capacity: inbound,
            outbound_capacity: outbound,
        } = config;
        let capacity::Encoder {
            table: hpack,
            header_list: list,
        } = encoder;
        let data = payload.data();
        let headers = payload.headers();
        for (field, cap) in [
            (Field::Stream, stream),
            (Field::Hpack, list),
            (Field::Inbound, inbound),
        ] {
            if cap == 0 {
                return Err(Error::ZeroCapacity(field));
            }
        }
        let egress = limits::Capacity::new(outbound)?;
        let streams = map::Plan::new(stream).map_err(Error::StreamCapacity)?;
        let count = streams.capacity();
        let events = Count::try_from(events).map_err(Error::EventCapacity)?;
        let events = events.nonzero().ok_or(Error::ZeroCapacity(Field::Event))?;
        if local.initial_window_size > flow::Window::MAX as u32 {
            return Err(Error::InitialWindowOverflow {
                actual: local.initial_window_size,
                max: flow::Window::MAX as u32,
            });
        }
        let recv = flow::Target::new(recv).ok_or(Error::ReceiveWindowOverflow {
            actual: recv,
            max: flow::Window::MAX as u32,
        })?;
        let mut local = local;
        match R::ENDPOINT {
            Endpoint::Server => local.enable_push = Push::Disabled,
            Endpoint::Client => {}
        }
        local.max_concurrent_streams = Some(match local.max_concurrent_streams {
            Some(limit) => limit.min(count.raw()),
            None => count.raw(),
        });
        let recv_cap = match local.max_header_list_size {
            Some(limit) => limit as usize,
            None => DEFAULT_MAX_HEADER_LIST_SIZE as usize,
        };
        let (block, wire) = limits::Capacity::wire(list)?;
        let slots = egress.slots(wire);
        let wire = pool::Layout::new(slots, wire.bytes())?;
        local.max_header_list_size = Some(recv_cap as u32);
        let egress = limits::Initial::new(egress, local.wire::<R>(), recv)?;
        let decoder = local
            .header_table_size
            .max(Settings::DEFAULT.header_table_size);
        let hpack = plan::Plans::new(hpack as usize, decoder as usize).map_err(Error::HpackPlan)?;
        let permits = pool::Width::<1>::new(data)?;
        let fields = pool::Layout::new(headers, recv_cap)?;
        let chunks = data + headers + 1;
        let ingress = queue::Capacity::new(
            Count::try_from(inbound).map_err(Error::IngressCount)?,
            Count::try_from(chunks).map_err(Error::IngressCount)?,
        );
        let retained = Count::try_from(payload.segments()).map_err(Error::RetentionCount)?;
        Ok(Self {
            local,
            recv_window_target: recv,
            streams,
            egress,
            hpack,
            buffers: BufferPlans {
                retention: payloads::Plan {
                    permits,
                    segments: segments::Plan {
                        segments: retained.get(),
                        chains: retained.get() / 2,
                    },
                },
                header_plan: fields,
                inbound_header_list_cap: recv_cap,
                outbound_headers: block,
                header_wire: wire,
                ingress,
                event_capacity: events,
            },
            marker: marker::PhantomData,
        })
    }
}

impl<R: crate::Role> Validated<R> {
    /// Exact byte capacity admitted for the connection's input queue.
    #[doc(hidden)]
    pub const fn inbound_capacity(&self) -> usize {
        self.buffers.ingress.bytes().get()
    }

    /// Maximum concurrent streams provisioned in the machine tables.
    #[doc(hidden)]
    pub const fn stream_count(&self) -> Count {
        self.streams.capacity()
    }

    /// Exact heap footprint reserved for the HPACK tables.
    #[doc(hidden)]
    pub const fn hpack_foot(&self) -> plan::Footprint {
        self.hpack.footprint()
    }

    /// Maximum number of DATA owners the connection can publish at once.
    #[doc(hidden)]
    pub fn data_events(&self) -> Count {
        Count::new(pool::Layout::from(self.buffers.retention.permits).slots())
    }
}
