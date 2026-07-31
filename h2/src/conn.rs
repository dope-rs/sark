use core::marker::PhantomData;
use core::ops::Deref;
use std::num::NonZeroUsize;
use std::{error, fmt};

use dope::runtime::profile::Throughput;
use o3::buffer::{
    ByteSink, Bytes, CapacityError, PoolLayoutError, Pooled, Retained, Shared, SharedPoolLayout,
    SharedPoolPlan,
};

use crate::egress::Egress;
use crate::flow::{self, Error, Window};
use crate::frame::{
    Data, ErrorCode, Flags, FrameLength, GoAway, HEADER_LEN, ParseError, Ping, Priority,
    PriorityFields, RstStream, SettingId, Type, WindowIncrement, WindowUpdate,
};
use crate::hpack::{self, DecoderError};
use crate::ingress::{Ingress, IngressConfig, PendingHeaders, PendingKind};
use crate::role::{ClientRole, Role, ServerRole};
use crate::stream::{self, Side, State, Stream, StreamId, TransitionError};
use crate::stream_registry::{StreamClass, StreamRecord, StreamRegistry};
use crate::tuning::Tuning;
use crate::validate::{RequestHeaders, ResponseHeaders};

pub const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub header_table_size: u32,
    pub enable_push: bool,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: u32,
    pub max_frame_size: u32,
    pub max_header_list_size: Option<u32>,
}

impl Settings {
    pub const DEFAULT: Self = Self {
        header_table_size: 4096,
        enable_push: true,
        max_concurrent_streams: None,
        initial_window_size: 65_535,
        max_frame_size: 16_384,
        max_header_list_size: None,
    };

    pub fn apply(&mut self, id: SettingId, value: u32) -> Result<(), ConnError> {
        match id {
            SettingId::HeaderTableSize => {
                self.header_table_size = value;
            }
            SettingId::EnablePush => match value {
                0 => self.enable_push = false,
                1 => self.enable_push = true,
                _ => return Err(ConnError::BadSettings),
            },
            SettingId::MaxConcurrentStreams => {
                self.max_concurrent_streams = Some(value);
            }
            SettingId::InitialWindowSize => {
                if value > 0x7fff_ffff {
                    return Err(ConnError::FlowControl);
                }
                self.initial_window_size = value;
            }
            SettingId::MaxFrameSize => {
                if !(16_384..=16_777_215).contains(&value) {
                    return Err(ConnError::BadSettings);
                }
                self.max_frame_size = value;
            }
            SettingId::MaxHeaderListSize => {
                self.max_header_list_size = Some(value);
            }
        }
        Ok(())
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let (bytes, len) = self.wire_bytes();
        out.write_slice(&bytes[..len])
    }

    pub(crate) fn wire_bytes(&self) -> ([u8; 36], usize) {
        let mut bytes = [0; 36];
        let mut len = 0;
        Self::push_param(
            &mut bytes,
            &mut len,
            SettingId::HeaderTableSize,
            self.header_table_size,
        );
        Self::push_param(
            &mut bytes,
            &mut len,
            SettingId::EnablePush,
            if self.enable_push { 1 } else { 0 },
        );
        if let Some(v) = self.max_concurrent_streams {
            Self::push_param(&mut bytes, &mut len, SettingId::MaxConcurrentStreams, v);
        }
        Self::push_param(
            &mut bytes,
            &mut len,
            SettingId::InitialWindowSize,
            self.initial_window_size,
        );
        Self::push_param(
            &mut bytes,
            &mut len,
            SettingId::MaxFrameSize,
            self.max_frame_size,
        );
        if let Some(v) = self.max_header_list_size {
            Self::push_param(&mut bytes, &mut len, SettingId::MaxHeaderListSize, v);
        }
        (bytes, len)
    }

    fn push_param(out: &mut [u8; 36], len: &mut usize, id: SettingId, value: u32) {
        let id = (id as u16).to_be_bytes();
        let value = value.to_be_bytes();
        out[*len..*len + 2].copy_from_slice(&id);
        out[*len + 2..*len + 6].copy_from_slice(&value);
        *len += 6;
    }

    pub(crate) fn param_count(&self) -> usize {
        2 + self.max_concurrent_streams.is_some() as usize
            + 2
            + self.max_header_list_size.is_some() as usize
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub local_settings: Settings,
    pub recv_window_target: u32,
    pub stream_capacity: usize,
    pub event_capacity: usize,
    pub data_capacity: usize,
    pub header_capacity: usize,
    pub inbound_capacity: usize,
    pub outbound_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            local_settings: Settings {
                initial_window_size: <Throughput as Tuning>::STREAM_RECV_WINDOW,
                ..Settings::DEFAULT
            },
            recv_window_target: <Throughput as Tuning>::CONN_RECV_WINDOW,
            stream_capacity: <Throughput as Tuning>::MAX_ACTIVE_STREAMS,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            data_capacity: DEFAULT_DATA_EVENTS,
            header_capacity: DEFAULT_HEADER_EVENTS,
            inbound_capacity: DEFAULT_INBOUND_CAPACITY,
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        }
    }
}

impl Config {
    pub(crate) fn initial_outbound_capacity<R: Role>(&self) -> usize {
        let mut local = self.local_settings;
        local.max_concurrent_streams = Some(0);
        local.max_header_list_size = Some(0);
        HEADER_LEN
            + local.param_count() * 6
            + if R::PREFACE_SENDS_FIRST {
                CLIENT_PREFACE.len()
            } else {
                0
            }
            + if self.recv_window_target > Window::INITIAL as u32 {
                HEADER_LEN + 4
            } else {
                0
            }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    ZeroCapacity(&'static str),
    StreamCapacityOverflow,
    InitialWindowOverflow,
    ReceiveWindowOverflow,
    InvalidMaxFrameSize,
    OutboundTooSmall { required: usize, actual: usize },
    Pool(PoolLayoutError),
}

impl From<PoolLayoutError> for ConfigError {
    fn from(error: PoolLayoutError) -> Self {
        Self::Pool(error)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity(name) => write!(formatter, "{name} capacity must be positive"),
            Self::StreamCapacityOverflow => formatter.write_str("stream capacity exceeds u32"),
            Self::InitialWindowOverflow => {
                formatter.write_str("initial stream window exceeds HTTP/2 maximum")
            }
            Self::ReceiveWindowOverflow => {
                formatter.write_str("connection receive window exceeds HTTP/2 maximum")
            }
            Self::InvalidMaxFrameSize => {
                formatter.write_str("maximum frame size is outside the HTTP/2 range")
            }
            Self::OutboundTooSmall { required, actual } => {
                write!(
                    formatter,
                    "outbound capacity {actual} is smaller than required {required}"
                )
            }
            Self::Pool(error) => error.fmt(formatter),
        }
    }
}

impl error::Error for ConfigError {}

#[derive(Clone, Copy)]
pub struct ValidatedConfig<R: Role> {
    local: Settings,
    recv_window_target: u32,
    stream_capacity: usize,
    event_capacity: usize,
    inbound_capacity: usize,
    outbound_capacity: NonZeroUsize,
    data_layout: SharedPoolLayout,
    header_plan: SharedPoolPlan,
    header_list_cap: usize,
    marker: PhantomData<fn() -> R>,
}

impl<R: Role> ValidatedConfig<R> {
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let Config {
            local_settings: local,
            recv_window_target,
            stream_capacity,
            event_capacity,
            data_capacity,
            header_capacity,
            inbound_capacity,
            outbound_capacity,
        } = config;
        for (name, capacity) in [
            ("stream", stream_capacity),
            ("event", event_capacity),
            ("data", data_capacity),
            ("header", header_capacity),
            ("inbound", inbound_capacity),
            ("outbound", outbound_capacity),
        ] {
            if capacity == 0 {
                return Err(ConfigError::ZeroCapacity(name));
            }
        }
        let capacity =
            u32::try_from(stream_capacity).map_err(|_| ConfigError::StreamCapacityOverflow)?;
        if local.initial_window_size > Window::MAX as u32 {
            return Err(ConfigError::InitialWindowOverflow);
        }
        if recv_window_target > Window::MAX as u32 {
            return Err(ConfigError::ReceiveWindowOverflow);
        }
        if !(16_384..=16_777_215).contains(&local.max_frame_size) {
            return Err(ConfigError::InvalidMaxFrameSize);
        }
        let mut local = local;
        if R::IS_SERVER {
            local.enable_push = false;
        }
        local.max_concurrent_streams = Some(
            local
                .max_concurrent_streams
                .map_or(capacity, |limit| limit.min(capacity)),
        );
        let header_list_cap = local
            .max_header_list_size
            .unwrap_or(DEFAULT_MAX_HEADER_LIST_SIZE) as usize;
        local.max_header_list_size = Some(header_list_cap as u32);
        let initial_outbound = config.initial_outbound_capacity::<R>();
        if outbound_capacity < initial_outbound {
            return Err(ConfigError::OutboundTooSmall {
                required: initial_outbound,
                actual: outbound_capacity,
            });
        }
        Ok(Self {
            local,
            recv_window_target,
            stream_capacity,
            event_capacity,
            inbound_capacity,
            outbound_capacity: NonZeroUsize::new(outbound_capacity)
                .ok_or(ConfigError::ZeroCapacity("outbound"))?,
            data_layout: SharedPoolLayout::new(data_capacity, 1)?,
            header_plan: SharedPoolPlan::new(header_capacity, header_list_cap)?,
            header_list_cap,
            marker: PhantomData,
        })
    }
}

impl<R: Role> Default for ValidatedConfig<R> {
    fn default() -> Self {
        const {
            assert!(DEFAULT_MAX_ACTIVE_STREAMS <= u32::MAX as usize);
            assert!(DEFAULT_INBOUND_CAPACITY != 0);
            assert!(DEFAULT_OUTBOUND_CAPACITY != 0);
            assert!(DEFAULT_EVENT_CAPACITY != 0);
            assert!(<Throughput as Tuning>::STREAM_RECV_WINDOW <= Window::MAX as u32);
            assert!(<Throughput as Tuning>::CONN_RECV_WINDOW <= Window::MAX as u32);
            assert!(
                DEFAULT_OUTBOUND_CAPACITY
                    >= CLIENT_PREFACE.len() + HEADER_LEN + 6 * 6 + HEADER_LEN + 4
            );
        }
        let mut local = Settings {
            initial_window_size: <Throughput as Tuning>::STREAM_RECV_WINDOW,
            max_concurrent_streams: Some(DEFAULT_MAX_ACTIVE_STREAMS as u32),
            max_header_list_size: Some(DEFAULT_MAX_HEADER_LIST_SIZE),
            ..Settings::DEFAULT
        };
        if R::IS_SERVER {
            local.enable_push = false;
        }
        Self {
            local,
            recv_window_target: <Throughput as Tuning>::CONN_RECV_WINDOW,
            stream_capacity: DEFAULT_MAX_ACTIVE_STREAMS,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            inbound_capacity: DEFAULT_INBOUND_CAPACITY,
            outbound_capacity: NonZeroUsize::MIN.saturating_add(DEFAULT_OUTBOUND_CAPACITY - 1),
            data_layout: SharedPoolLayout::fixed::<DEFAULT_DATA_EVENTS, 1>(),
            header_plan: SharedPoolPlan::fixed::<
                DEFAULT_HEADER_EVENTS,
                { DEFAULT_MAX_HEADER_LIST_SIZE as usize },
            >(),
            header_list_cap: DEFAULT_MAX_HEADER_LIST_SIZE as usize,
            marker: PhantomData,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnError {
    BadPreface,
    BadSettings,
    ParseError(ParseError),
    Protocol,
    StreamClosed,
    FlowControl,
    GoAwayReceived(ErrorCode),
    StreamLimit,
    Hpack(DecoderError),
    BadStream,
    Continuation,
    FrameSize,
    StreamGoneAway,
    HeaderListTooLarge,
    Overload,
}

impl From<ParseError> for ConnError {
    fn from(error: ParseError) -> Self {
        match error {
            ParseError::BadLength => Self::FrameSize,
            other => Self::ParseError(other),
        }
    }
}

impl From<Error> for ConnError {
    fn from(e: Error) -> Self {
        match e {
            Error::ZeroIncrement => ConnError::Protocol,
            Error::Overflow => ConnError::FlowControl,
            Error::Stalled => ConnError::FlowControl,
        }
    }
}

impl From<DecoderError> for ConnError {
    fn from(e: DecoderError) -> Self {
        ConnError::Hpack(e)
    }
}

impl From<CapacityError> for ConnError {
    fn from(_: CapacityError) -> Self {
        Self::Overload
    }
}

impl From<&ConnError> for ErrorCode {
    fn from(e: &ConnError) -> Self {
        match e {
            ConnError::BadPreface
            | ConnError::Protocol
            | ConnError::BadStream
            | ConnError::Continuation
            | ConnError::BadSettings
            | ConnError::StreamGoneAway => ErrorCode::ProtocolError,
            ConnError::StreamClosed => ErrorCode::StreamClosed,
            ConnError::ParseError(ParseError::FrameSize) => ErrorCode::FrameSize,
            ConnError::ParseError(_) => ErrorCode::ProtocolError,
            ConnError::FlowControl => ErrorCode::FlowControl,
            ConnError::FrameSize => ErrorCode::FrameSize,
            ConnError::Hpack(_) => ErrorCode::Compression,
            ConnError::GoAwayReceived(c) => *c,
            ConnError::StreamLimit => ErrorCode::RefusedStream,
            ConnError::HeaderListTooLarge | ConnError::Overload => ErrorCode::EnhanceYourCalm,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    PrefaceComplete,
    SettingsApplied,
    SettingsAck,
    Ping {
        ack: bool,
        opaque: [u8; 8],
    },
    GoAway {
        last_stream_id: StreamId,
        error: ErrorCode,
        debug: DataPayload,
    },
    Headers {
        stream_id: StreamId,
        headers: hpack::HeaderBlock,
        end_stream: bool,
        trailing: bool,
    },
    Data {
        stream_id: StreamId,
        data: DataPayload,
        end_stream: bool,
    },
    StreamReset {
        stream_id: StreamId,
        error: ErrorCode,
    },
    PushPromise {
        stream_id: StreamId,
        promised_stream_id: StreamId,
        headers: hpack::HeaderBlock,
    },
}

#[must_use = "the returned write owns bytes removed from the connection"]
pub enum OutboundWrite {
    Buffered(usize),
    Split { prefix_len: usize, body: Shared },
}

#[derive(Clone)]
pub struct DataPayload {
    bytes: Bytes<Retained>,
    _permit: Pooled,
}

impl Deref for DataPayload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.bytes.as_slice()
    }
}

impl AsRef<[u8]> for DataPayload {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl DataPayload {
    pub(crate) fn from_retained(bytes: Bytes<Retained>, permit: Pooled) -> Self {
        Self {
            bytes,
            _permit: permit,
        }
    }

    pub fn into_retained(self) -> Bytes<Retained> {
        self.bytes
    }
}

impl PartialEq for DataPayload {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<[u8]> for DataPayload {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for DataPayload {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_ref() == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for DataPayload {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_ref() == *other
    }
}

impl Eq for DataPayload {}

impl fmt::Debug for DataPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPayload")
            .field("len", &self.len())
            .finish()
    }
}

const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 16_384;
const DEFAULT_MAX_ACTIVE_STREAMS: usize = 256;
const DEFAULT_INBOUND_CAPACITY: usize = 1 << 20;
const DEFAULT_OUTBOUND_CAPACITY: usize = 1 << 20;
const DEFAULT_EVENT_CAPACITY: usize = 1 << 13;
const DEFAULT_DATA_EVENTS: usize = 64;
const DEFAULT_HEADER_EVENTS: usize = 64;
const MAX_RESET_STREAMS: u32 = 100;
const MAX_CONTINUATION_FRAMES: u32 = 64;

pub struct Conn<R: Role> {
    role: PhantomData<R>,

    ingress: Ingress,
    egress: Egress,

    initial_settings_sent: bool,
    peer_settings_received: bool,
    goaway_sent: bool,
    goaway_received: Option<ErrorCode>,

    local_settings: Settings,
    peer_settings: Settings,

    send_window: Window,
    recv_window: Window,
    recv_window_target: u32,

    streams: StreamRegistry<R>,
    conn_pending_release: u32,
    peer_reset_count: u32,
    send_window_opened: bool,
}

struct DataPlan {
    length: FrameLength,
    end_stream: bool,
}

impl<R: Role> Conn<R> {
    pub fn new() -> Self {
        Self::from_config(ValidatedConfig::default())
    }

    pub fn with_tuning<P: Tuning>() -> Result<Self, ConfigError> {
        Self::with_config(Config {
            local_settings: Settings {
                initial_window_size: P::STREAM_RECV_WINDOW,
                ..Settings::DEFAULT
            },
            recv_window_target: P::CONN_RECV_WINDOW,
            stream_capacity: P::MAX_ACTIVE_STREAMS,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            data_capacity: DEFAULT_DATA_EVENTS,
            header_capacity: DEFAULT_HEADER_EVENTS,
            inbound_capacity: DEFAULT_INBOUND_CAPACITY,
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        })
    }

    pub fn with_local_settings(
        local: Settings,
        recv_window_target: u32,
    ) -> Result<Self, ConfigError> {
        let stream_capacity = local
            .max_concurrent_streams
            .map_or(DEFAULT_MAX_ACTIVE_STREAMS, |limit| (limit as usize).max(1));
        Self::with_config(Config {
            local_settings: local,
            recv_window_target,
            stream_capacity,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            data_capacity: DEFAULT_DATA_EVENTS,
            header_capacity: DEFAULT_HEADER_EVENTS,
            inbound_capacity: DEFAULT_INBOUND_CAPACITY,
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
        })
    }

    pub fn with_config(config: Config) -> Result<Self, ConfigError> {
        Ok(Self::from_config(ValidatedConfig::new(config)?))
    }

    pub fn from_config(config: ValidatedConfig<R>) -> Self {
        let ValidatedConfig {
            local,
            recv_window_target,
            stream_capacity,
            event_capacity,
            inbound_capacity,
            outbound_capacity,
            data_layout,
            header_plan,
            header_list_cap,
            marker: _,
        } = config;
        let peer = Settings::DEFAULT;
        let mut conn = Self {
            role: PhantomData,
            ingress: Ingress::from_config(IngressConfig {
                inbound_capacity,
                event_capacity,
                data_layout,
                header_plan,
                decoder_table_size: local.header_table_size as usize,
                header_cap: header_list_cap,
                preface_done: R::PREFACE_SENDS_FIRST,
            }),
            egress: Egress::from_capacity(
                outbound_capacity,
                local.header_table_size as usize,
                header_list_cap,
            ),
            initial_settings_sent: false,
            peer_settings_received: false,
            goaway_sent: false,
            goaway_received: None,
            local_settings: local,
            peer_settings: peer,
            send_window: Window::new(),
            recv_window: Window::new(),
            recv_window_target,
            streams: StreamRegistry::new(stream_capacity),
            conn_pending_release: 0,
            peer_reset_count: 0,
            send_window_opened: false,
        };
        if R::PREFACE_SENDS_FIRST {
            let appended = conn.egress.raw_mut().try_extend_from_slice(CLIENT_PREFACE);
            debug_assert!(appended.is_ok());
            conn.ingress.complete_preface();
        }
        let initialized = conn.emit_initial_settings();
        debug_assert!(initialized.is_ok());
        conn.initial_settings_sent = true;
        let bump = recv_window_target.saturating_sub(Window::INITIAL as u32);
        if bump > 0 {
            let increased = conn.recv_window.increase(bump);
            debug_assert!(increased.is_ok());
            let emitted = conn.emit_window_update(StreamId::CONNECTION, bump);
            debug_assert!(emitted.is_ok());
        }
        conn
    }

    pub fn local_settings(&self) -> &Settings {
        &self.local_settings
    }

    pub fn peer_settings(&self) -> &Settings {
        &self.peer_settings
    }

    pub fn send_window(&self) -> Window {
        self.send_window
    }

    pub fn recv_window(&self) -> Window {
        self.recv_window
    }

    pub fn goaway_received(&self) -> Option<ErrorCode> {
        self.goaway_received
    }

    pub fn goaway_sent(&self) -> bool {
        self.goaway_sent
    }

    pub fn outbound(&self) -> &[u8] {
        self.egress.first()
    }

    pub fn outbound_slices(&self) -> (&[u8], &[u8]) {
        self.egress.slices()
    }

    pub fn outbound_len(&self) -> usize {
        self.egress.len()
    }

    pub fn drain_outbound(&mut self, n: usize) {
        self.egress.drain(n);
    }

    pub fn drain_into(&mut self, write_buf: &mut [u8]) -> usize {
        self.egress.drain_into(write_buf)
    }

    pub fn take_write(&mut self, write_buf: &mut [u8]) -> OutboundWrite {
        match self.egress.take_write(write_buf) {
            crate::egress::Write::Buffered(written) => OutboundWrite::Buffered(written),
            crate::egress::Write::Split { prefix_len, body } => {
                OutboundWrite::Split { prefix_len, body }
            }
        }
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        self.ingress.poll_event()
    }

    fn push_event(&mut self, event: Event) -> Result<(), ConnError> {
        self.ingress.push_event(event)
    }

    fn ensure_event_capacity(&self) -> Result<(), ConnError> {
        self.ingress.ensure_event_capacity()
    }

    fn prepare_outbound(&mut self, additional: usize) -> Result<(), ConnError> {
        self.egress.reserve(additional)
    }

    pub fn take_window_opened(&mut self) -> bool {
        let opened = self.send_window_opened;
        self.send_window_opened = false;
        opened
    }

    pub fn has_stream(&self, id: StreamId) -> bool {
        self.stream(id).is_some()
    }

    pub fn stream_state(&self, id: StreamId) -> Option<State> {
        self.stream(id).map(|record| record.stream.state)
    }

    pub fn active_count(&self) -> usize {
        self.streams.active_count()
    }

    pub fn tracked_closed_count(&self) -> usize {
        self.streams.reset_count()
    }

    pub fn stream_send_window(&self, id: StreamId) -> Option<Window> {
        self.stream(id).map(|record| record.send_window)
    }

    pub fn stream_recv_window(&self, id: StreamId) -> Option<Window> {
        self.stream(id).map(|record| record.recv_window)
    }

    pub fn ping(&mut self, opaque: [u8; 8]) -> Result<(), ConnError> {
        self.prepare_outbound(HEADER_LEN + opaque.len())?;
        let frame = Ping { ack: false, opaque };
        frame.encode(self.egress.raw_mut())?;
        Ok(())
    }

    pub fn goaway(&mut self, error: ErrorCode, debug: &[u8]) -> Result<(), ConnError> {
        self.prepare_outbound(HEADER_LEN + 8 + debug.len())?;
        let frame =
            GoAway::new(self.streams.last_peer_id(), error, debug).ok_or(ConnError::FrameSize)?;
        frame.encode(self.egress.raw_mut())?;
        self.goaway_sent = true;
        Ok(())
    }

    pub fn reset_stream(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        self.prepare_outbound(HEADER_LEN + 4)?;
        RstStream::new(stream_id, error)
            .ok_or(ConnError::Protocol)?
            .encode(self.egress.raw_mut())?;
        self.advance_stream(stream_id, stream::Event::RstStream, Side::Local)
            .map_err(|_| ConnError::Protocol)?;
        Ok(())
    }

    pub fn send_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        end_stream: bool,
    ) -> Result<usize, ConnError> {
        self.send_data_parts(stream_id, data, &[], end_stream)
    }

    pub fn send_data_parts(
        &mut self,
        stream_id: StreamId,
        first: &[u8],
        second: &[u8],
        end_stream: bool,
    ) -> Result<usize, ConnError> {
        let len = first
            .len()
            .checked_add(second.len())
            .ok_or(ConnError::FrameSize)?;
        let Some(plan) = self.plan_data(stream_id, len, end_stream, false)? else {
            return Ok(0);
        };
        Data::encode_parts(
            stream_id,
            plan.end_stream,
            first,
            second,
            plan.length,
            self.egress.raw_mut(),
        )?;
        Ok(plan.length.as_usize())
    }

    pub fn send_data_shared(
        &mut self,
        stream_id: StreamId,
        data: &Shared,
        end_stream: bool,
    ) -> Result<usize, ConnError> {
        let Some(plan) = self.plan_data(stream_id, data.len(), end_stream, true)? else {
            return Ok(0);
        };
        Data::encode_header(
            stream_id,
            plan.end_stream,
            plan.length,
            self.egress.raw_mut(),
        )?;
        let data = data
            .get(..plan.length.as_usize())
            .ok_or(ConnError::FrameSize)?;
        self.egress.queue_shared(data);
        Ok(plan.length.as_usize())
    }

    fn plan_data(
        &mut self,
        stream_id: StreamId,
        len: usize,
        end_stream: bool,
        split: bool,
    ) -> Result<Option<DataPlan>, ConnError> {
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        let avail = {
            let record = self
                .streams
                .get_mut(stream_id)
                .ok_or(ConnError::BadStream)?;
            flow::Pair {
                conn: &mut self.send_window,
                stream: &mut record.send_window,
            }
            .available()
        };
        if avail == 0 && len != 0 {
            return Ok(None);
        }
        let send_n = len
            .min(avail)
            .min(self.peer_settings.max_frame_size as usize);
        if split {
            self.egress.reserve_split(HEADER_LEN, send_n)?;
        } else {
            self.prepare_outbound(HEADER_LEN + send_n)?;
        }
        if send_n > 0 {
            let record = self
                .streams
                .get_mut(stream_id)
                .ok_or(ConnError::BadStream)?;
            let mut pair = flow::Pair {
                conn: &mut self.send_window,
                stream: &mut record.send_window,
            };
            pair.consume(send_n).map_err(ConnError::from)?;
        }
        let last_chunk = send_n == len;
        let es = end_stream && last_chunk;
        self.advance_stream(
            stream_id,
            stream::Event::Data { end_stream: es },
            Side::Local,
        )
        .map_err(|_| ConnError::Protocol)?;
        let length = FrameLength::from_usize(send_n).ok_or(ConnError::FrameSize)?;
        Ok(Some(DataPlan {
            length,
            end_stream: es,
        }))
    }

    pub fn send_trailers(
        &mut self,
        stream_id: StreamId,
        headers: &[hpack::Header<'_>],
    ) -> Result<(), ConnError> {
        self.send_trailers_fields(stream_id, headers.iter().copied())
    }

    pub fn send_trailers_fields<'a, I>(
        &mut self,
        stream_id: StreamId,
        headers: I,
    ) -> Result<(), ConnError>
    where
        I: IntoIterator<Item = hpack::Header<'a>>,
    {
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        self.emit_headers(stream_id, headers, true)?;
        self.advance_stream(
            stream_id,
            stream::Event::Headers { end_stream: true },
            Side::Local,
        )
        .map_err(|_| ConnError::Protocol)?;
        Ok(())
    }

    pub fn ingest(&mut self, bytes: &[u8]) -> Result<(), ConnError> {
        self.ingress.append(bytes)?;
        self.resume()
    }

    pub fn ingest_retained(&mut self, bytes: Bytes<Retained>) -> Result<(), ConnError> {
        self.ingress.append_retained(bytes)?;
        self.resume()
    }

    pub fn resume(&mut self) -> Result<(), ConnError> {
        self.drive()?;
        if self.egress.over_capacity() {
            return Err(ConnError::Overload);
        }
        Ok(())
    }

    fn emit_initial_settings(&mut self) -> Result<(), ConnError> {
        self.egress.initial_settings(&self.local_settings)
    }

    fn emit_settings_ack(&mut self) -> Result<(), ConnError> {
        self.egress.settings_ack()
    }

    fn emit_window_update(&mut self, stream_id: StreamId, increment: u32) -> Result<(), ConnError> {
        let increment = WindowIncrement::new(increment).ok_or(ConnError::FlowControl)?;
        self.egress.window_update(stream_id, increment)
    }

    fn emit_rst(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        self.egress.reset(stream_id, error)
    }

    fn rst_evict(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        self.emit_rst(stream_id, error)?;
        self.mark_reset(stream_id);
        self.evict_stream(stream_id);
        Ok(())
    }

    fn emit_headers<'a, I>(
        &mut self,
        stream_id: StreamId,
        headers: I,
        end_stream: bool,
    ) -> Result<(), ConnError>
    where
        I: IntoIterator<Item = hpack::Header<'a>>,
    {
        let max_frame = self.peer_settings.max_frame_size as usize;
        self.egress
            .headers(stream_id, headers, end_stream, max_frame)
    }

    fn advance_stream(
        &mut self,
        id: StreamId,
        ev: stream::Event,
        side: Side,
    ) -> Result<(), TransitionError> {
        let stream = &mut self.stream_mut(id).ok_or(TransitionError::Protocol)?.stream;
        let next = match side {
            Side::Local => stream.state.send(ev)?,
            Side::Remote => stream.state.recv(ev)?,
        };
        stream.state = next;
        if next == State::Closed {
            if matches!(ev, stream::Event::RstStream) {
                self.mark_reset(id);
            }
            self.evict_stream(id);
        }
        Ok(())
    }

    fn mark_reset(&mut self, id: StreamId) {
        self.streams.mark_reset(id);
    }

    fn stream(&self, id: StreamId) -> Option<&StreamRecord> {
        self.streams.get(id)
    }

    fn stream_mut(&mut self, id: StreamId) -> Option<&mut StreamRecord> {
        self.streams.get_mut(id)
    }

    fn track_stream(&mut self, stream: Stream) -> Result<(), Stream> {
        self.streams.insert(
            stream,
            self.peer_settings.initial_window_size,
            self.local_settings.initial_window_size,
        )
    }

    fn can_track_peer_stream(&self) -> bool {
        self.streams.can_accept_peer(
            self.local_settings
                .max_concurrent_streams
                .unwrap_or(u32::MAX) as usize,
        )
    }

    fn can_track_local_stream(&self) -> bool {
        self.streams.can_open_local(
            self.peer_settings
                .max_concurrent_streams
                .map_or(usize::MAX, |limit| limit as usize),
        )
    }

    fn reserve_promised_stream(&mut self, id: StreamId) -> Result<bool, ConnError> {
        if !StreamRegistry::<R>::is_peer_initiated(id) || id <= self.streams.last_peer_id() {
            return Err(ConnError::Protocol);
        }
        if !self.can_track_peer_stream() {
            self.emit_rst(id, ErrorCode::RefusedStream)?;
            self.streams.observe_peer(id);
            return Ok(false);
        }
        self.track_stream(Stream::reserve_remote(id))
            .map_err(|_| ConnError::Protocol)?;
        self.streams.observe_peer(id);
        Ok(true)
    }

    fn classify_stream(&self, id: StreamId) -> StreamClass {
        self.streams.classify(id)
    }

    fn evict_stream(&mut self, id: StreamId) {
        self.streams.remove(id);
    }

    fn begin_inbound_block(
        &mut self,
        start: usize,
        len: usize,
        trailing: bool,
    ) -> Result<(), ConnError> {
        if R::IS_SERVER {
            self.ingress
                .begin_headers::<RequestHeaders>(start, len, trailing)
        } else {
            self.ingress
                .begin_headers::<ResponseHeaders>(start, len, trailing)
        }
    }

    fn continue_inbound_block(&mut self, start: usize, len: usize) -> Result<(), ConnError> {
        self.ingress.continue_headers(start, len)
    }

    fn complete_inbound_block(
        &mut self,
        start: usize,
        len: usize,
        trailing: bool,
    ) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        if R::IS_SERVER {
            self.ingress
                .complete_headers::<RequestHeaders>(start, len, trailing)
        } else {
            self.ingress
                .complete_headers::<ResponseHeaders>(start, len, trailing)
        }
    }

    fn finish_inbound_block(&mut self) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        self.ingress.finish_headers()
    }

    fn drive(&mut self) -> Result<(), ConnError> {
        if !self.ingress.accept_preface()? {
            return Ok(());
        }

        loop {
            let Some(header) = self
                .ingress
                .next_frame(self.local_settings.max_frame_size)?
            else {
                return Ok(());
            };
            let total = HEADER_LEN + header.length.as_usize();
            if self.ingress.has_pending_headers() && header.kind != Type::Continuation {
                return Err(ConnError::Continuation);
            }
            let emits_event = match header.kind {
                Type::Settings | Type::Ping | Type::GoAway | Type::Data | Type::RstStream => true,
                Type::Headers | Type::PushPromise | Type::Continuation => {
                    header.flags.has(Flags::END_HEADERS)
                }
                Type::WindowUpdate | Type::Priority => false,
            };
            if emits_event {
                self.ensure_event_capacity()?;
            }
            match header.kind {
                Type::Settings => {
                    let ack = header.flags.has(Flags::ACK);
                    if !header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    if (ack && header.length != FrameLength::ZERO)
                        || !header.length.as_u32().is_multiple_of(6)
                    {
                        return Err(ParseError::FrameSize.into());
                    }
                    if ack {
                        self.peer_settings_received = true;
                        self.push_event(Event::SettingsAck)?;
                    } else {
                        self.prepare_outbound(HEADER_LEN)?;
                        let mut next_settings = self.peer_settings;
                        let mut encoder_size = None;
                        let prev_iws = self.peer_settings.initial_window_size as i64;
                        let mut offset = 0;
                        while offset < header.length.as_usize() {
                            let mut chunk = [0; 6];
                            let copied = self.ingress.copy(HEADER_LEN + offset, &mut chunk);
                            debug_assert!(copied);
                            let id_raw = u16::from_be_bytes([chunk[0], chunk[1]]);
                            let val = u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]);
                            if let Some(id) = SettingId::from_u16(id_raw) {
                                next_settings.apply(id, val)?;
                                if id == SettingId::HeaderTableSize {
                                    encoder_size = Some(val as usize);
                                }
                            }
                            offset += 6;
                        }
                        let new_iws = next_settings.initial_window_size as i64;
                        let delta = new_iws - prev_iws;
                        if delta != 0 {
                            let delta32 = delta as i32;
                            for record in self.streams.values_mut() {
                                record
                                    .send_window
                                    .adjust_initial(delta32)
                                    .map_err(ConnError::from)?;
                            }
                        }
                        self.peer_settings = next_settings;
                        if let Some(max_size) = encoder_size {
                            self.egress.set_header_table_size(max_size);
                        }
                        self.ingress.try_consume(total)?;
                        self.emit_settings_ack()?;
                        self.push_event(Event::SettingsApplied)?;
                        if delta > 0 {
                            self.send_window_opened = true;
                        }
                        continue;
                    }
                }
                Type::Ping => {
                    let mut payload = [0; 8];
                    if header.length.as_usize() != payload.len() {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.ingress.copy(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let parsed = Ping::parse(header, &payload)?;
                    if !parsed.ack {
                        let pong = Ping {
                            ack: true,
                            opaque: parsed.opaque,
                        };
                        self.prepare_outbound(HEADER_LEN + 8)?;
                        self.ingress.try_consume(total)?;
                        pong.encode(self.egress.raw_mut())?;
                        self.push_event(Event::Ping {
                            ack: false,
                            opaque: parsed.opaque,
                        })?;
                        continue;
                    }
                    self.push_event(Event::Ping {
                        ack: true,
                        opaque: parsed.opaque,
                    })?;
                }
                Type::GoAway => {
                    if header.length.as_u32() < 8 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let mut prefix = [0; 8];
                    let copied = self.ingress.copy(HEADER_LEN, &mut prefix);
                    debug_assert!(copied);
                    let parsed = GoAway::parse(header, &prefix)?;
                    let debug_len = header.length.as_usize() - prefix.len();
                    let debug = self.ingress.data(HEADER_LEN + prefix.len(), debug_len)?;
                    self.goaway_received = Some(parsed.error());
                    self.push_event(Event::GoAway {
                        last_stream_id: parsed.last_stream_id(),
                        error: parsed.error(),
                        debug,
                    })?;
                }
                Type::WindowUpdate => {
                    let mut payload = [0; 4];
                    if header.length.as_usize() != payload.len() {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.ingress.copy(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let parsed = WindowUpdate::parse(header, &payload)?;
                    self.ingress.try_consume(total)?;
                    self.handle_window_update_frame(parsed)?;
                    continue;
                }
                Type::Headers => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(HEADER_LEN + 4)?;
                    let (mut start, mut len) = self.ingress.unpadded_payload(header)?;
                    if header.flags.has(Flags::PRIORITY) {
                        if len < 5 {
                            return Err(ParseError::FrameSize.into());
                        }
                        let mut priority = [0; 5];
                        let copied = self.ingress.copy(start, &mut priority);
                        debug_assert!(copied);
                        let _ = PriorityFields::parse(&priority)?;
                        start += priority.len();
                        len -= priority.len();
                    }
                    let sid = header.stream_id;
                    let end_stream = header.flags.has(Flags::END_STREAM);
                    let end_headers = header.flags.has(Flags::END_HEADERS);
                    self.handle_headers_frame(sid, end_stream, end_headers, start, len)?;
                    self.ingress.try_consume(total)?;
                    continue;
                }
                Type::Data => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(2 * (HEADER_LEN + 4))?;
                    let (start, len) = self.ingress.unpadded_payload(header)?;
                    let payload = self.ingress.data(start, len)?;
                    let stream_id = header.stream_id;
                    let end_stream = header.flags.has(Flags::END_STREAM);
                    self.ingress.try_consume(total)?;
                    self.handle_data_frame(stream_id, end_stream, payload)?;
                    continue;
                }
                Type::Continuation => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(HEADER_LEN + 4)?;
                    let len = header.length.as_usize();
                    let stream_id = header.stream_id;
                    let end_headers = header.flags.has(Flags::END_HEADERS);
                    self.handle_continuation_frame(stream_id, end_headers, HEADER_LEN, len)?;
                    self.ingress.try_consume(total)?;
                    continue;
                }
                Type::RstStream => {
                    let mut payload = [0; 4];
                    if header.length.as_usize() != payload.len() {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.ingress.copy(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let parsed = RstStream::parse(header, &payload)?;
                    self.ingress.try_consume(total)?;
                    self.handle_rst_frame(parsed)?;
                    continue;
                }
                Type::PushPromise => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(HEADER_LEN + 4)?;
                    let (start, len) = self.ingress.unpadded_payload(header)?;
                    if len < 4 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let mut promised = [0; 4];
                    let copied = self.ingress.copy(start, &mut promised);
                    debug_assert!(copied);
                    let promised = StreamId::from_wire(u32::from_be_bytes(promised));
                    let block_start = start + 4;
                    let block_len = len - 4;
                    let stream_id = header.stream_id;
                    let end_headers = header.flags.has(Flags::END_HEADERS);
                    self.handle_push_promise_frame(
                        stream_id,
                        promised,
                        end_headers,
                        block_start,
                        block_len,
                    )?;
                    self.ingress.try_consume(total)?;
                    continue;
                }
                Type::Priority => {
                    let mut payload = [0; 5];
                    if header.length.as_usize() != payload.len() {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.ingress.copy(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let _ = Priority::parse(header, &payload)?;
                }
            }
            self.ingress.try_consume(total)?;
        }
    }

    fn handle_headers_frame(
        &mut self,
        stream_id: StreamId,
        end_stream: bool,
        end_headers: bool,
        fragment_start: usize,
        fragment_len: usize,
    ) -> Result<(), ConnError> {
        if stream_id.is_zero() {
            return Err(ConnError::Protocol);
        }
        match self.classify_stream(stream_id) {
            StreamClass::Connection => return Err(ConnError::Protocol),
            StreamClass::ClosedEnd => return Err(ConnError::StreamClosed),
            StreamClass::ClosedRst => {
                self.emit_rst(stream_id, ErrorCode::StreamClosed)?;
                return Ok(());
            }
            StreamClass::Idle => {
                let peer_init = if R::IS_SERVER {
                    stream_id.is_client()
                } else {
                    stream_id.is_server()
                };
                if !peer_init {
                    return Err(ConnError::Protocol);
                }
                if self.goaway_sent {
                    self.emit_rst(stream_id, ErrorCode::RefusedStream)?;
                    return Ok(());
                }
                if !self.can_track_peer_stream() {
                    self.emit_rst(stream_id, ErrorCode::RefusedStream)?;
                    self.streams.observe_peer(stream_id);
                    return Ok(());
                }
                if self.track_stream(Stream::new(stream_id)).is_err() {
                    self.emit_rst(stream_id, ErrorCode::RefusedStream)?;
                    self.streams.observe_peer(stream_id);
                    return Ok(());
                }
                self.streams.observe_peer(stream_id);
            }
            StreamClass::Active => {}
        }
        let trailing = self.is_trailing(stream_id);
        if end_headers {
            let (headers, invalid) =
                self.complete_inbound_block(fragment_start, fragment_len, trailing)?;
            if invalid {
                self.rst_evict(stream_id, ErrorCode::ProtocolError)?;
                return Ok(());
            }
            match self.advance_stream(
                stream_id,
                stream::Event::Headers { end_stream },
                Side::Remote,
            ) {
                Ok(()) => {}
                Err(TransitionError::Protocol) => return Err(ConnError::Protocol),
                Err(TransitionError::StreamClosed) => {
                    self.rst_evict(stream_id, ErrorCode::StreamClosed)?;
                    return Ok(());
                }
            }
            if let Some(record) = self.stream_mut(stream_id) {
                record.stream.peer_headers_received = true;
            }
            self.push_event(Event::Headers {
                stream_id,
                headers,
                end_stream,
                trailing,
            })?;
        } else {
            self.begin_inbound_block(fragment_start, fragment_len, trailing)?;
            self.ingress.start_pending_headers(PendingHeaders {
                stream_id,
                kind: PendingKind::Headers {
                    end_stream,
                    trailing,
                },
                continuations: 0,
            });
        }
        Ok(())
    }

    fn is_trailing(&self, stream_id: StreamId) -> bool {
        self.stream(stream_id)
            .map(|record| record.stream.peer_headers_received)
            .unwrap_or(false)
    }

    fn handle_data_frame(
        &mut self,
        stream_id: StreamId,
        end_stream: bool,
        payload: DataPayload,
    ) -> Result<(), ConnError> {
        match self.classify_stream(stream_id) {
            StreamClass::Connection => return Err(ConnError::Protocol),
            StreamClass::Idle => return Err(ConnError::Protocol),
            StreamClass::ClosedEnd => return Err(ConnError::StreamClosed),
            StreamClass::ClosedRst => {
                self.emit_rst(stream_id, ErrorCode::StreamClosed)?;
                return Ok(());
            }
            StreamClass::Active => {}
        }
        let n = payload.len();
        self.recv_window
            .consume(n)
            .map_err(|_| ConnError::FlowControl)?;
        {
            self.stream_mut(stream_id)
                .ok_or(ConnError::Protocol)?
                .recv_window
                .consume(n)
                .map_err(|_| ConnError::FlowControl)?;
        }
        self.replenish_recv(stream_id, n)?;
        match self.advance_stream(stream_id, stream::Event::Data { end_stream }, Side::Remote) {
            Ok(()) => {}
            Err(TransitionError::Protocol) => return Err(ConnError::Protocol),
            Err(TransitionError::StreamClosed) => {
                self.rst_evict(stream_id, ErrorCode::StreamClosed)?;
                return Ok(());
            }
        }
        self.push_event(Event::Data {
            stream_id,
            data: payload,
            end_stream,
        })?;
        Ok(())
    }

    fn replenish_recv(&mut self, stream_id: StreamId, n: usize) -> Result<(), ConnError> {
        if n == 0 {
            return Ok(());
        }
        let n32 = u32::try_from(n).map_err(|_| ConnError::FlowControl)?;
        let conn_threshold = (self.recv_window_target / 2).max(1);
        self.conn_pending_release = self.conn_pending_release.saturating_add(n32);
        if self.conn_pending_release >= conn_threshold {
            let inc = self.conn_pending_release;
            self.conn_pending_release = 0;
            self.recv_window.increase(inc).map_err(ConnError::from)?;
            self.emit_window_update(StreamId::CONNECTION, inc)?;
        }
        let stream_threshold = (self.local_settings.initial_window_size / 2).max(1);
        let stream_increment = if let Some(record) = self.stream_mut(stream_id) {
            record.pending_release = record.pending_release.saturating_add(n32);
            if record.pending_release >= stream_threshold {
                let increment = record.pending_release;
                record.pending_release = 0;
                record
                    .recv_window
                    .increase(increment)
                    .map_err(ConnError::from)?;
                Some(increment)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(increment) = stream_increment {
            self.emit_window_update(stream_id, increment)?;
        }
        Ok(())
    }

    fn handle_window_update_frame(&mut self, parsed: WindowUpdate) -> Result<(), ConnError> {
        if parsed.stream_id.is_zero() {
            self.send_window
                .increase(parsed.increment.get())
                .map_err(ConnError::from)?;
            self.send_window_opened = true;
            return Ok(());
        }
        match self.classify_stream(parsed.stream_id) {
            StreamClass::Connection => Err(ConnError::Protocol),
            StreamClass::Idle => Err(ConnError::Protocol),
            StreamClass::ClosedRst | StreamClass::ClosedEnd => Ok(()),
            StreamClass::Active => {
                self.stream_mut(parsed.stream_id)
                    .ok_or(ConnError::Protocol)?
                    .send_window
                    .increase(parsed.increment.get())
                    .map_err(ConnError::from)?;
                self.send_window_opened = true;
                Ok(())
            }
        }
    }

    fn handle_continuation_frame(
        &mut self,
        stream_id: StreamId,
        end_headers: bool,
        fragment_start: usize,
        fragment_len: usize,
    ) -> Result<(), ConnError> {
        let kind = {
            let pending = self
                .ingress
                .pending_headers_mut()
                .ok_or(ConnError::Continuation)?;
            if pending.stream_id != stream_id {
                return Err(ConnError::Continuation);
            }
            if fragment_len == 0 && !end_headers {
                return Err(ConnError::Continuation);
            }
            pending.continuations = pending.continuations.saturating_add(1);
            if pending.continuations > MAX_CONTINUATION_FRAMES {
                return Err(ConnError::Overload);
            }
            pending.kind
        };
        match kind {
            PendingKind::Headers { .. } => {
                self.continue_inbound_block(fragment_start, fragment_len)?;
            }
            PendingKind::PushPromise { .. } => {
                self.ingress
                    .continue_headers(fragment_start, fragment_len)?;
            }
        }
        if end_headers {
            let Some(pending) = self.ingress.take_pending_headers() else {
                return Err(ConnError::Continuation);
            };
            match pending.kind {
                PendingKind::Headers {
                    end_stream,
                    trailing,
                } => {
                    let (headers, invalid) = self.finish_inbound_block()?;
                    if invalid {
                        self.rst_evict(pending.stream_id, ErrorCode::ProtocolError)?;
                        return Ok(());
                    }
                    match self.advance_stream(
                        pending.stream_id,
                        stream::Event::Headers { end_stream },
                        Side::Remote,
                    ) {
                        Ok(()) => {}
                        Err(TransitionError::Protocol) => {
                            return Err(ConnError::Protocol);
                        }
                        Err(TransitionError::StreamClosed) => {
                            self.rst_evict(pending.stream_id, ErrorCode::StreamClosed)?;
                            return Ok(());
                        }
                    }
                    if let Some(record) = self.stream_mut(pending.stream_id) {
                        record.stream.peer_headers_received = true;
                    }
                    self.push_event(Event::Headers {
                        stream_id: pending.stream_id,
                        headers,
                        end_stream,
                        trailing,
                    })?;
                }
                PendingKind::PushPromise { promised } => {
                    let (headers, invalid) = self.ingress.finish_headers()?;
                    if invalid {
                        self.rst_evict(promised, ErrorCode::ProtocolError)?;
                        return Ok(());
                    }
                    if !self.reserve_promised_stream(promised)? {
                        return Ok(());
                    }
                    self.push_event(Event::PushPromise {
                        stream_id: pending.stream_id,
                        promised_stream_id: promised,
                        headers,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn handle_rst_frame(&mut self, r: RstStream) -> Result<(), ConnError> {
        match self.classify_stream(r.stream_id()) {
            StreamClass::Connection => return Err(ConnError::Protocol),
            StreamClass::Idle => return Err(ConnError::Protocol),
            StreamClass::ClosedRst | StreamClass::ClosedEnd => return Ok(()),
            StreamClass::Active => {}
        }
        let peer_reset_count = self.peer_reset_count.saturating_add(1);
        if peer_reset_count > MAX_RESET_STREAMS {
            return Err(ConnError::Overload);
        }
        self.advance_stream(r.stream_id(), stream::Event::RstStream, Side::Remote)
            .map_err(|_| ConnError::Protocol)?;
        self.push_event(Event::StreamReset {
            stream_id: r.stream_id(),
            error: r.error(),
        })?;
        self.peer_reset_count = peer_reset_count;
        Ok(())
    }

    fn handle_push_promise_frame(
        &mut self,
        stream_id: StreamId,
        promised: StreamId,
        end_headers: bool,
        fragment_start: usize,
        fragment_len: usize,
    ) -> Result<(), ConnError> {
        if R::IS_SERVER {
            return Err(ConnError::Protocol);
        }
        if end_headers {
            let (headers, invalid) = self.ingress.complete_headers::<RequestHeaders>(
                fragment_start,
                fragment_len,
                false,
            )?;
            if invalid {
                self.emit_rst(promised, ErrorCode::ProtocolError)?;
                return Ok(());
            }
            if !self.reserve_promised_stream(promised)? {
                return Ok(());
            }
            self.push_event(Event::PushPromise {
                stream_id,
                promised_stream_id: promised,
                headers,
            })?;
        } else {
            self.ingress
                .begin_headers::<RequestHeaders>(fragment_start, fragment_len, false)?;
            self.ingress.start_pending_headers(PendingHeaders {
                stream_id,
                kind: PendingKind::PushPromise { promised },
                continuations: 0,
            });
        }
        Ok(())
    }
}

impl<R: Role> Default for Conn<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl Conn<ClientRole> {
    pub fn start_request(
        &mut self,
        headers: &[hpack::Header<'_>],
        end_stream: bool,
    ) -> Result<StreamId, ConnError> {
        self.start_request_fields(headers.iter().copied(), end_stream)
    }

    pub fn start_request_fields<'a, I>(
        &mut self,
        headers: I,
        end_stream: bool,
    ) -> Result<StreamId, ConnError>
    where
        I: IntoIterator<Item = hpack::Header<'a>>,
    {
        if self.goaway_received.is_some() || self.goaway_sent {
            return Err(ConnError::StreamGoneAway);
        }
        if !self.can_track_local_stream() {
            return Err(ConnError::StreamLimit);
        }
        let id = self.streams.next_local_id().ok_or(ConnError::StreamLimit)?;
        self.track_stream(Stream::new(id))
            .map_err(|_| ConnError::StreamLimit)?;
        self.emit_headers(id, headers, end_stream)?;
        self.advance_stream(id, stream::Event::Headers { end_stream }, Side::Local)
            .map_err(|_| ConnError::Protocol)?;
        Ok(id)
    }
}

impl Conn<ServerRole> {
    pub fn send_response<'a, I>(
        &mut self,
        stream_id: StreamId,
        headers: I,
        end_stream: bool,
    ) -> Result<(), ConnError>
    where
        I: IntoIterator<Item = hpack::Header<'a>>,
    {
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        self.emit_headers(stream_id, headers, end_stream)?;
        self.advance_stream(
            stream_id,
            stream::Event::Headers { end_stream },
            Side::Local,
        )
        .map_err(|_| ConnError::Protocol)?;
        Ok(())
    }
}
