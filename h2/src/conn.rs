use core::marker::PhantomData;
use core::ops::Deref;
use std::fmt;

use dope::runtime::profile::Throughput;
use o3::buffer::{ByteRing, Pooled, SharedPool};
use o3::collections::{FixedHashTable, FixedQueue};

use crate::frame::{
    self, Continuation, Data, ErrorCode, Flags, FrameBuf, FrameHeader, GoAway, HEADER_LEN, Headers,
    ParseError, Ping, Priority, RstStream, SettingId, WindowUpdate,
};
use crate::role::Role;
use crate::stream::{self, Side, Stream, StreamId, TransitionError};
use crate::tuning::Tuning;
use crate::validate::Validate;
use crate::{flow, hpack};

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

    pub fn encode(&self, out: &mut impl FrameBuf) {
        Self::push_param(out, SettingId::HeaderTableSize, self.header_table_size);
        Self::push_param(
            out,
            SettingId::EnablePush,
            if self.enable_push { 1 } else { 0 },
        );
        if let Some(v) = self.max_concurrent_streams {
            Self::push_param(out, SettingId::MaxConcurrentStreams, v);
        }
        Self::push_param(out, SettingId::InitialWindowSize, self.initial_window_size);
        Self::push_param(out, SettingId::MaxFrameSize, self.max_frame_size);
        if let Some(v) = self.max_header_list_size {
            Self::push_param(out, SettingId::MaxHeaderListSize, v);
        }
    }

    fn push_param(out: &mut impl FrameBuf, id: SettingId, value: u32) {
        out.extend_from_slice(&(id as u16).to_be_bytes());
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn param_count(&self) -> usize {
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
    Hpack(hpack::DecoderError),
    BadStream,
    Continuation,
    FrameSize,
    StreamGoneAway,
    HeaderListTooLarge,
    Overload,
}

impl From<ParseError> for ConnError {
    fn from(e: ParseError) -> Self {
        ConnError::ParseError(e)
    }
}

impl From<flow::Error> for ConnError {
    fn from(e: flow::Error) -> Self {
        match e {
            flow::Error::ZeroIncrement => ConnError::Protocol,
            flow::Error::Overflow => ConnError::FlowControl,
            flow::Error::Stalled => ConnError::FlowControl,
        }
    }
}

impl From<hpack::DecoderError> for ConnError {
    fn from(e: hpack::DecoderError) -> Self {
        match e {
            hpack::DecoderError::HeaderListTooLarge => ConnError::HeaderListTooLarge,
            other => ConnError::Hpack(other),
        }
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
            ConnError::ParseError(ParseError::FrameSize)
            | ConnError::ParseError(ParseError::BadLength) => ErrorCode::FrameSize,
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum StreamClass {
    Connection,
    Active,
    ClosedRst,
    ClosedEnd,
    Idle,
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

#[derive(Clone)]
pub struct DataPayload(Pooled);

impl Deref for DataPayload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl AsRef<[u8]> for DataPayload {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl DataPayload {
    pub fn into_pooled(self) -> Pooled {
        self.0
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingKind {
    Headers { end_stream: bool, trailing: bool },
    PushPromise { promised: StreamId },
}

struct PendingHeaders {
    stream_id: StreamId,
    kind: PendingKind,
    continuations: u32,
}

const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 16_384;
const DEFAULT_MAX_ACTIVE_STREAMS: usize = 256;
const DEFAULT_INBOUND_CAPACITY: usize = 1 << 20;
const DEFAULT_OUTBOUND_CAPACITY: usize = 1 << 20;
const DEFAULT_EVENT_CAPACITY: usize = 1 << 13;
const DEFAULT_DATA_EVENTS: usize = 64;
const DEFAULT_HEADER_EVENTS: usize = 64;
const MAX_RESET_STREAMS: u32 = 100;
const RESET_RING_CAP: usize = 256;
const MAX_CONTINUATION_FRAMES: u32 = 64;

struct StreamRecord {
    stream: Stream,
    send_window: flow::Window,
    recv_window: flow::Window,
    pending_release: u32,
}

pub struct Conn<R: Role> {
    role: PhantomData<R>,

    inbound: ByteRing,
    outbound: ByteRing,
    outbound_capacity: usize,

    preface_done: bool,
    initial_settings_sent: bool,
    peer_settings_received: bool,
    goaway_sent: bool,
    goaway_received: Option<ErrorCode>,

    local_settings: Settings,
    peer_settings: Settings,

    send_window: flow::Window,
    recv_window: flow::Window,
    recv_window_target: u32,

    events: FixedQueue<Event>,
    data_pool: SharedPool,
    header_pool: SharedPool,

    encoder: hpack::Encoder,
    send_header_block: Vec<u8>,
    recv_header_block: Vec<u8>,
    decoder: hpack::Decoder,
    streams: FixedHashTable<StreamRecord>,
    local_streams: usize,
    peer_streams: usize,
    next_local_id: stream::IdGen,
    last_peer_stream_id: u32,
    pending_headers: Option<PendingHeaders>,
    pending_headers_cap: usize,
    conn_pending_release: u32,
    reset_streams: FixedQueue<StreamId>,
    peer_reset_count: u32,
    send_window_opened: bool,
}

impl<R: Role> Conn<R> {
    pub fn new() -> Self {
        Self::with_tuning::<Throughput>()
    }

    pub fn with_tuning<P: Tuning>() -> Self {
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

    pub fn with_local_settings(local: Settings, recv_window_target: u32) -> Self {
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

    pub fn with_config(config: Config) -> Self {
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
        assert!(stream_capacity > 0, "stream capacity must be positive");
        assert!(event_capacity > 0, "event capacity must be positive");
        assert!(data_capacity > 0, "data capacity must be positive");
        assert!(header_capacity > 0, "header capacity must be positive");
        assert!(inbound_capacity > 0, "inbound capacity must be positive");
        assert!(outbound_capacity > 0, "outbound capacity must be positive");
        let capacity = u32::try_from(stream_capacity).expect("stream capacity overflow");
        let peer = Settings::DEFAULT;
        let mut local = local;
        if R::IS_SERVER {
            local.enable_push = false;
        }
        local.max_concurrent_streams = Some(
            local
                .max_concurrent_streams
                .map_or(capacity, |limit| limit.min(capacity)),
        );
        if local.max_header_list_size.is_none() {
            local.max_header_list_size = Some(DEFAULT_MAX_HEADER_LIST_SIZE);
        }
        let initial_outbound = HEADER_LEN
            + local.param_count() * 6
            + if R::PREFACE_SENDS_FIRST {
                CLIENT_PREFACE.len()
            } else {
                0
            }
            + if recv_window_target > flow::Window::INITIAL as u32 {
                HEADER_LEN + 4
            } else {
                0
            };
        assert!(
            outbound_capacity >= initial_outbound,
            "outbound capacity is too small"
        );
        let header_list_cap = local.max_header_list_size.unwrap() as usize;
        let mut decoder = hpack::Decoder::new(local.header_table_size as usize);
        decoder.set_max_header_list_size(Some(header_list_cap));
        let mut conn = Self {
            role: PhantomData,
            inbound: ByteRing::with_capacity(inbound_capacity),
            outbound: ByteRing::with_capacity(outbound_capacity),
            outbound_capacity,
            preface_done: false,
            initial_settings_sent: false,
            peer_settings_received: false,
            goaway_sent: false,
            goaway_received: None,
            local_settings: local,
            peer_settings: peer,
            send_window: flow::Window::new(),
            recv_window: flow::Window::new(),
            recv_window_target,
            events: FixedQueue::with_capacity(event_capacity),
            data_pool: SharedPool::new(data_capacity, local.max_frame_size as usize),
            header_pool: SharedPool::new(header_capacity, header_list_cap),
            encoder: hpack::Encoder::new(local.header_table_size as usize),
            send_header_block: Vec::with_capacity(header_list_cap),
            recv_header_block: Vec::with_capacity(header_list_cap),
            decoder,
            streams: FixedHashTable::with_capacity(stream_capacity),
            local_streams: 0,
            peer_streams: 0,
            next_local_id: stream::IdGen::new(R::FIRST_LOCAL_STREAM_ID),
            last_peer_stream_id: 0,
            pending_headers: None,
            pending_headers_cap: header_list_cap,
            conn_pending_release: 0,
            reset_streams: FixedQueue::with_capacity(RESET_RING_CAP),
            peer_reset_count: 0,
            send_window_opened: false,
        };
        if R::PREFACE_SENDS_FIRST {
            if conn.outbound.try_extend_from_slice(CLIENT_PREFACE).is_err() {
                unreachable!();
            }
            conn.preface_done = true;
            conn.events
                .vacant_entry()
                .unwrap()
                .push_back(Event::PrefaceComplete);
        }
        conn.emit_initial_settings();
        conn.initial_settings_sent = true;
        let bump = recv_window_target.saturating_sub(flow::Window::INITIAL as u32);
        if bump > 0
            && conn.recv_window.increase(bump).is_ok()
            && conn.emit_window_update(StreamId::CONNECTION, bump).is_err()
        {
            unreachable!();
        }
        conn
    }

    pub fn local_settings(&self) -> &Settings {
        &self.local_settings
    }

    pub fn peer_settings(&self) -> &Settings {
        &self.peer_settings
    }

    pub fn send_window(&self) -> flow::Window {
        self.send_window
    }

    pub fn recv_window(&self) -> flow::Window {
        self.recv_window
    }

    pub fn goaway_received(&self) -> Option<ErrorCode> {
        self.goaway_received
    }

    pub fn goaway_sent(&self) -> bool {
        self.goaway_sent
    }

    pub fn outbound(&self) -> &[u8] {
        self.outbound.as_slices().0
    }

    pub fn outbound_slices(&self) -> (&[u8], &[u8]) {
        self.outbound.as_slices()
    }

    pub fn drain_outbound(&mut self, n: usize) {
        self.outbound.consume(n.min(self.outbound.len()));
    }

    pub fn drain_into(&mut self, write_buf: &mut [u8]) -> usize {
        let (first, second) = self.outbound.as_slices();
        let first_len = first.len().min(write_buf.len());
        write_buf[..first_len].copy_from_slice(&first[..first_len]);
        let second_len = second.len().min(write_buf.len() - first_len);
        write_buf[first_len..first_len + second_len].copy_from_slice(&second[..second_len]);
        let n = first_len + second_len;
        self.drain_outbound(n);
        n
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    fn push_event(&mut self, event: Event) -> Result<(), ConnError> {
        self.events
            .push_back(event)
            .map_err(|_| ConnError::Overload)
    }

    fn ensure_event_capacity(&self) -> Result<(), ConnError> {
        if self.events.is_full() {
            Err(ConnError::Overload)
        } else {
            Ok(())
        }
    }

    fn inbound_len(&self) -> usize {
        self.inbound.len()
    }

    fn consume_inbound(&mut self, n: usize) {
        self.inbound.consume(n);
    }

    fn prepare_inbound(&mut self, additional: usize) -> Result<(), ConnError> {
        (additional <= self.inbound.remaining())
            .then_some(())
            .ok_or(ConnError::Overload)
    }

    fn prepare_outbound(&mut self, additional: usize) -> Result<(), ConnError> {
        if additional > self.outbound.remaining() {
            return Err(ConnError::Overload);
        }
        Ok(())
    }

    pub fn take_window_opened(&mut self) -> bool {
        std::mem::take(&mut self.send_window_opened)
    }

    pub fn has_stream(&self, id: StreamId) -> bool {
        self.stream(id).is_some()
    }

    pub fn stream_state(&self, id: StreamId) -> Option<stream::State> {
        self.stream(id).map(|record| record.stream.state)
    }

    pub fn active_count(&self) -> usize {
        self.streams.len()
    }

    pub fn tracked_closed_count(&self) -> usize {
        self.reset_streams.len()
    }

    pub fn stream_send_window(&self, id: StreamId) -> Option<flow::Window> {
        self.stream(id).map(|record| record.send_window)
    }

    pub fn stream_recv_window(&self, id: StreamId) -> Option<flow::Window> {
        self.stream(id).map(|record| record.recv_window)
    }

    pub fn ping(&mut self, opaque: [u8; 8]) -> Result<(), ConnError> {
        self.prepare_outbound(HEADER_LEN + opaque.len())?;
        let frame = Ping { ack: false, opaque };
        frame.encode(&mut self.outbound);
        Ok(())
    }

    pub fn goaway(&mut self, error: ErrorCode, debug: &[u8]) -> Result<(), ConnError> {
        self.prepare_outbound(HEADER_LEN + 8 + debug.len())?;
        let frame = GoAway {
            last_stream_id: StreamId(self.last_peer_stream_id),
            error,
            debug,
        };
        frame.encode(&mut self.outbound);
        self.goaway_sent = true;
        Ok(())
    }

    pub fn reset_stream(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        self.prepare_outbound(HEADER_LEN + 4)?;
        RstStream { stream_id, error }.encode(&mut self.outbound);
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
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        let len = first
            .len()
            .checked_add(second.len())
            .ok_or(ConnError::FrameSize)?;
        let max_frame = self.peer_settings.max_frame_size as usize;
        let avail = {
            let record = self
                .streams
                .get_mut(Self::stream_hash(stream_id), |record| {
                    record.stream.id == stream_id
                })
                .ok_or(ConnError::BadStream)?;
            flow::Pair {
                conn: &mut self.send_window,
                stream: &mut record.send_window,
            }
            .available()
        };
        if avail == 0 && len != 0 {
            return Ok(0);
        }
        let send_n = len.min(avail).min(max_frame);
        self.prepare_outbound(HEADER_LEN + send_n)?;
        if send_n > 0 {
            let record = self
                .streams
                .get_mut(Self::stream_hash(stream_id), |record| {
                    record.stream.id == stream_id
                })
                .ok_or(ConnError::BadStream)?;
            let mut pair = flow::Pair {
                conn: &mut self.send_window,
                stream: &mut record.send_window,
            };
            pair.consume(send_n).map_err(ConnError::from)?;
        }
        let last_chunk = send_n == len;
        let es = end_stream && last_chunk;
        Data::encode_parts(stream_id, es, first, second, send_n, &mut self.outbound);
        self.advance_stream(
            stream_id,
            stream::Event::Data { end_stream: es },
            Side::Local,
        )
        .map_err(|_| ConnError::Protocol)?;
        Ok(send_n)
    }

    pub fn send_trailers(
        &mut self,
        stream_id: StreamId,
        headers: &[hpack::Header<'_>],
    ) -> Result<(), ConnError> {
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        self.emit_headers(stream_id, headers.iter().copied(), true)?;
        self.advance_stream(
            stream_id,
            stream::Event::Headers { end_stream: true },
            Side::Local,
        )
        .map_err(|_| ConnError::Protocol)?;
        Ok(())
    }

    pub fn ingest(&mut self, bytes: &[u8]) -> Result<(), ConnError> {
        self.prepare_inbound(bytes.len())?;
        self.inbound
            .try_extend_from_slice(bytes)
            .map_err(|_| ConnError::Overload)?;
        self.resume()
    }

    pub fn resume(&mut self) -> Result<(), ConnError> {
        self.drive()?;
        if self.outbound.len() > self.outbound_capacity {
            return Err(ConnError::Overload);
        }
        Ok(())
    }

    fn emit_initial_settings(&mut self) {
        let count = self.local_settings.param_count();
        let length = (count * 6) as u32;
        FrameHeader {
            length,
            kind: frame::Type::Settings,
            flags: Flags(0),
            stream_id: StreamId(0),
        }
        .encode(&mut self.outbound);
        self.local_settings.encode(&mut self.outbound);
    }

    fn emit_settings_ack(&mut self) -> Result<(), ConnError> {
        self.prepare_outbound(HEADER_LEN)?;
        FrameHeader {
            length: 0,
            kind: frame::Type::Settings,
            flags: Flags(Flags::ACK),
            stream_id: StreamId(0),
        }
        .encode(&mut self.outbound);
        Ok(())
    }

    fn emit_window_update(&mut self, stream_id: StreamId, increment: u32) -> Result<(), ConnError> {
        if increment == 0 {
            return Ok(());
        }
        self.prepare_outbound(HEADER_LEN + 4)?;
        WindowUpdate {
            stream_id,
            increment,
        }
        .encode(&mut self.outbound);
        Ok(())
    }

    fn emit_rst(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        self.prepare_outbound(HEADER_LEN + 4)?;
        RstStream { stream_id, error }.encode(&mut self.outbound);
        Ok(())
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
        let mut block = core::mem::take(&mut self.send_header_block);
        block.clear();
        self.encoder.encode(headers, &mut block);
        let max_frame = self.peer_settings.max_frame_size as usize;
        let frames = block.len().max(1).div_ceil(max_frame);
        let additional = frames
            .checked_mul(HEADER_LEN)
            .and_then(|headers| block.len().checked_add(headers))
            .ok_or(ConnError::FrameSize);
        let result = additional.and_then(|additional| self.prepare_outbound(additional));
        if result.is_err() {
            self.send_header_block = block;
            return result;
        }
        if block.len() <= max_frame {
            Headers {
                stream_id,
                end_stream,
                end_headers: true,
                priority: None,
                block_fragment: &block,
            }
            .encode(&mut self.outbound);
        } else {
            let (first, rest) = block.split_at(max_frame);
            Headers {
                stream_id,
                end_stream,
                end_headers: false,
                priority: None,
                block_fragment: first,
            }
            .encode(&mut self.outbound);
            let mut pos = 0;
            while pos < rest.len() {
                let take = (rest.len() - pos).min(max_frame);
                let end = pos + take;
                let last = end == rest.len();
                Continuation {
                    stream_id,
                    end_headers: last,
                    block_fragment: &rest[pos..end],
                }
                .encode(&mut self.outbound);
                pos = end;
            }
        }
        self.send_header_block = block;
        Ok(())
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
        if next == stream::State::Closed {
            if matches!(ev, stream::Event::RstStream) {
                self.mark_reset(id);
            }
            self.evict_stream(id);
        }
        Ok(())
    }

    fn mark_reset(&mut self, id: StreamId) {
        if self.reset_streams.contains(&id) {
            return;
        }
        if self.reset_streams.is_full() {
            self.reset_streams.pop_front();
        }
        self.reset_streams.vacant_entry().unwrap().push_back(id);
    }

    fn is_peer_initiated(id: StreamId) -> bool {
        if R::IS_SERVER {
            id.is_client()
        } else {
            id.is_server()
        }
    }

    fn is_local_initiated(id: StreamId) -> bool {
        if R::IS_SERVER {
            id.is_server()
        } else {
            id.is_client()
        }
    }

    fn stream_hash(id: StreamId) -> u64 {
        u64::from(id.0)
    }

    fn stream(&self, id: StreamId) -> Option<&StreamRecord> {
        self.streams
            .get(Self::stream_hash(id), |record| record.stream.id == id)
    }

    fn stream_mut(&mut self, id: StreamId) -> Option<&mut StreamRecord> {
        self.streams
            .get_mut(Self::stream_hash(id), |record| record.stream.id == id)
    }

    fn track_stream(&mut self, stream: Stream) -> Result<(), Stream> {
        let id = stream.id;
        let record = StreamRecord {
            stream,
            send_window: flow::Window::with(self.peer_settings.initial_window_size as i32),
            recv_window: flow::Window::with(self.local_settings.initial_window_size as i32),
            pending_release: 0,
        };
        match self
            .streams
            .try_insert(Self::stream_hash(id), record, |record| {
                record.stream.id == id
            }) {
            Ok(()) => {
                if Self::is_local_initiated(id) {
                    self.local_streams += 1;
                } else {
                    self.peer_streams += 1;
                }
                Ok(())
            }
            Err(record) => Err(record.stream),
        }
    }

    fn can_track_peer_stream(&self) -> bool {
        self.peer_streams < self.local_settings.max_concurrent_streams.unwrap() as usize
            && self.active_count() < self.streams.capacity()
    }

    fn can_track_local_stream(&self) -> bool {
        self.local_streams
            < self
                .peer_settings
                .max_concurrent_streams
                .map_or(usize::MAX, |limit| limit as usize)
            && self.active_count() < self.streams.capacity()
    }

    fn reserve_promised_stream(&mut self, id: StreamId) -> Result<bool, ConnError> {
        if !Self::is_peer_initiated(id) || id.0 <= self.last_peer_stream_id {
            return Err(ConnError::Protocol);
        }
        if !self.can_track_peer_stream() {
            self.emit_rst(id, ErrorCode::RefusedStream)?;
            self.last_peer_stream_id = id.0;
            return Ok(false);
        }
        self.track_stream(Stream::reserve_remote(id))
            .map_err(|_| ConnError::Protocol)?;
        self.last_peer_stream_id = id.0;
        Ok(true)
    }

    fn classify_stream(&self, id: StreamId) -> StreamClass {
        if id.0 == 0 {
            return StreamClass::Connection;
        }
        if self.stream(id).is_some() {
            return StreamClass::Active;
        }
        if Self::is_peer_initiated(id) && id.0 <= self.last_peer_stream_id {
            return if self.reset_streams.contains(&id) {
                StreamClass::ClosedRst
            } else {
                StreamClass::ClosedEnd
            };
        }
        if Self::is_local_initiated(id) && id.0 < self.next_local_id.peek().0 {
            return if self.reset_streams.contains(&id) {
                StreamClass::ClosedRst
            } else {
                StreamClass::ClosedEnd
            };
        }
        StreamClass::Idle
    }

    fn evict_stream(&mut self, id: StreamId) {
        if self
            .streams
            .remove(Self::stream_hash(id), |record| record.stream.id == id)
            .is_some()
        {
            if Self::is_local_initiated(id) {
                self.local_streams -= 1;
            } else {
                self.peer_streams -= 1;
            }
        }
    }

    fn decode_block(&mut self, block: &[u8]) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        let mut lease = self.header_pool.try_acquire().ok_or(ConnError::Overload)?;
        let mut overflow = false;
        let over_limit = self.decoder.decode_bounded(block, |n, v| {
            if overflow {
                return;
            }
            let Ok(name_len) = u32::try_from(n.len()) else {
                overflow = true;
                return;
            };
            let Ok(value_len) = u32::try_from(v.len()) else {
                overflow = true;
                return;
            };
            let mut writer = lease.spare_writer();
            overflow = writer
                .try_extend_from_slice(&name_len.to_ne_bytes())
                .and_then(|()| writer.try_extend_from_slice(&value_len.to_ne_bytes()))
                .and_then(|()| writer.try_extend_from_slice(n))
                .and_then(|()| writer.try_extend_from_slice(v))
                .is_err();
        })?;
        if overflow {
            return Err(ConnError::Overload);
        }
        Ok((hpack::HeaderBlock::from_pooled(lease.freeze()), over_limit))
    }

    fn decode_recv_block(&mut self) -> Result<(hpack::HeaderBlock, bool), ConnError> {
        let mut block = core::mem::take(&mut self.recv_header_block);
        let decoded = self.decode_block(&block);
        block.clear();
        self.recv_header_block = block;
        decoded
    }

    fn drive(&mut self) -> Result<(), ConnError> {
        if !R::PREFACE_SENDS_FIRST && !self.preface_done {
            if self.inbound_len() < CLIENT_PREFACE.len() {
                return Ok(());
            }
            let (first, second) = self
                .inbound
                .range_slices(0, CLIENT_PREFACE.len())
                .ok_or(ConnError::BadPreface)?;
            if first != &CLIENT_PREFACE[..first.len()] || second != &CLIENT_PREFACE[first.len()..] {
                return Err(ConnError::BadPreface);
            }
            self.ensure_event_capacity()?;
            self.consume_inbound(CLIENT_PREFACE.len());
            self.preface_done = true;
            self.push_event(Event::PrefaceComplete)?;
        }

        loop {
            let header = match self.parse_frame_header() {
                Ok(header) => header,
                Err(ParseError::NeedMore) => return Ok(()),
                Err(ParseError::BadType(_)) => {
                    let mut prefix = [0; 3];
                    if !self.inbound.copy_range_into(0, &mut prefix) {
                        return Ok(());
                    }
                    let length = u32::from_be_bytes([0, prefix[0], prefix[1], prefix[2]]);
                    if length > self.local_settings.max_frame_size {
                        return Err(ConnError::FrameSize);
                    }
                    let total = HEADER_LEN + length as usize;
                    if self.inbound_len() < total {
                        return Ok(());
                    }
                    self.consume_inbound(total);
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if header.length > self.local_settings.max_frame_size {
                return Err(ConnError::FrameSize);
            }
            let total = HEADER_LEN + header.length as usize;
            if self.inbound_len() < total {
                return Ok(());
            }
            if self.pending_headers.is_some() && header.kind != frame::Type::Continuation {
                return Err(ConnError::Continuation);
            }
            let emits_event = match header.kind {
                frame::Type::Settings
                | frame::Type::Ping
                | frame::Type::GoAway
                | frame::Type::Data
                | frame::Type::RstStream => true,
                frame::Type::Headers | frame::Type::PushPromise | frame::Type::Continuation => {
                    header.flags.has(Flags::END_HEADERS)
                }
                frame::Type::WindowUpdate | frame::Type::Priority => false,
            };
            if emits_event {
                self.ensure_event_capacity()?;
            }
            match header.kind {
                frame::Type::Settings => {
                    let ack = header.flags.has(Flags::ACK);
                    if !header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    if (ack && header.length != 0) || !header.length.is_multiple_of(6) {
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
                        while offset < header.length as usize {
                            let mut chunk = [0; 6];
                            let copied = self
                                .inbound
                                .copy_range_into(HEADER_LEN + offset, &mut chunk);
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
                                let mut window = record.send_window;
                                window.adjust_initial(delta32).map_err(ConnError::from)?;
                            }
                        }
                        self.peer_settings = next_settings;
                        if let Some(max_size) = encoder_size {
                            self.encoder.set_max_size(max_size);
                        }
                        if delta != 0 {
                            let delta32 = delta as i32;
                            for record in self.streams.values_mut() {
                                record
                                    .send_window
                                    .adjust_initial(delta32)
                                    .map_err(ConnError::from)?;
                            }
                        }
                        self.consume_inbound(total);
                        self.emit_settings_ack()?;
                        self.push_event(Event::SettingsApplied)?;
                        if delta > 0 {
                            self.send_window_opened = true;
                        }
                        continue;
                    }
                }
                frame::Type::Ping => {
                    let mut payload = [0; 8];
                    if header.length != payload.len() as u32 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.inbound.copy_range_into(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let parsed = Ping::parse(header, &payload)?;
                    if !parsed.ack {
                        let pong = Ping {
                            ack: true,
                            opaque: parsed.opaque,
                        };
                        self.prepare_outbound(HEADER_LEN + 8)?;
                        self.consume_inbound(total);
                        pong.encode(&mut self.outbound);
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
                frame::Type::GoAway => {
                    if header.length < 8 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let mut prefix = [0; 8];
                    let copied = self.inbound.copy_range_into(HEADER_LEN, &mut prefix);
                    debug_assert!(copied);
                    let parsed = GoAway::parse(header, &prefix)?;
                    let debug_len = header.length as usize - prefix.len();
                    let mut lease = self.data_pool.try_acquire().ok_or(ConnError::Overload)?;
                    let (first, second) = self
                        .inbound
                        .range_slices(HEADER_LEN + prefix.len(), debug_len)
                        .ok_or(ConnError::FrameSize)?;
                    let mut writer = lease.spare_writer();
                    writer
                        .try_extend_from_slice(first)
                        .map_err(|_| ConnError::Overload)?;
                    writer
                        .try_extend_from_slice(second)
                        .map_err(|_| ConnError::Overload)?;
                    drop(writer);
                    self.goaway_received = Some(parsed.error);
                    self.push_event(Event::GoAway {
                        last_stream_id: parsed.last_stream_id,
                        error: parsed.error,
                        debug: DataPayload(lease.freeze()),
                    })?;
                }
                frame::Type::WindowUpdate => {
                    let mut payload = [0; 4];
                    if header.length != payload.len() as u32 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.inbound.copy_range_into(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let parsed = WindowUpdate::parse(header, &payload)?;
                    self.consume_inbound(total);
                    self.handle_window_update_frame(parsed)?;
                    continue;
                }
                frame::Type::Headers => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(HEADER_LEN + 4)?;
                    self.recv_header_block.clear();
                    let (mut start, mut len) = self.unpadded_payload(header)?;
                    if header.flags.has(Flags::PRIORITY) {
                        if len < 5 {
                            return Err(ParseError::FrameSize.into());
                        }
                        let mut priority = [0; 5];
                        let copied = self.inbound.copy_range_into(start, &mut priority);
                        debug_assert!(copied);
                        let _ = frame::PriorityFields::parse(&priority)?;
                        start += priority.len();
                        len -= priority.len();
                    }
                    if len > self.pending_headers_cap {
                        return Err(ConnError::HeaderListTooLarge);
                    }
                    self.extend_recv_header_block(start, len)?;
                    let sid = header.stream_id;
                    let end_stream = header.flags.has(Flags::END_STREAM);
                    let end_headers = header.flags.has(Flags::END_HEADERS);
                    self.consume_inbound(total);
                    self.handle_headers_frame(sid, end_stream, end_headers)?;
                    continue;
                }
                frame::Type::Data => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(2 * (HEADER_LEN + 4))?;
                    let (start, len) = self.unpadded_payload(header)?;
                    let mut lease = self.data_pool.try_acquire().ok_or(ConnError::Overload)?;
                    let (first, second) = self
                        .inbound
                        .range_slices(start, len)
                        .ok_or(ConnError::FrameSize)?;
                    let mut writer = lease.spare_writer();
                    writer
                        .try_extend_from_slice(first)
                        .map_err(|_| ConnError::Overload)?;
                    writer
                        .try_extend_from_slice(second)
                        .map_err(|_| ConnError::Overload)?;
                    drop(writer);
                    let stream_id = header.stream_id;
                    let end_stream = header.flags.has(Flags::END_STREAM);
                    let payload = DataPayload(lease.freeze());
                    self.consume_inbound(total);
                    self.handle_data_frame(stream_id, end_stream, payload)?;
                    continue;
                }
                frame::Type::Continuation => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(HEADER_LEN + 4)?;
                    let len = header.length as usize;
                    if len
                        > self
                            .pending_headers_cap
                            .saturating_sub(self.recv_header_block.len())
                    {
                        return Err(ConnError::HeaderListTooLarge);
                    }
                    self.extend_recv_header_block(HEADER_LEN, len)?;
                    let stream_id = header.stream_id;
                    let end_headers = header.flags.has(Flags::END_HEADERS);
                    self.consume_inbound(total);
                    self.handle_continuation_frame(stream_id, end_headers, len)?;
                    continue;
                }
                frame::Type::RstStream => {
                    let mut payload = [0; 4];
                    if header.length != payload.len() as u32 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.inbound.copy_range_into(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let parsed = RstStream::parse(header, &payload)?;
                    self.consume_inbound(total);
                    self.handle_rst_frame(parsed)?;
                    continue;
                }
                frame::Type::PushPromise => {
                    if header.stream_id.is_zero() {
                        return Err(ParseError::Protocol.into());
                    }
                    self.prepare_outbound(HEADER_LEN + 4)?;
                    self.recv_header_block.clear();
                    let (start, len) = self.unpadded_payload(header)?;
                    if len < 4 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let mut promised = [0; 4];
                    let copied = self.inbound.copy_range_into(start, &mut promised);
                    debug_assert!(copied);
                    let promised = StreamId::from_u32_masked(u32::from_be_bytes(promised));
                    let block_start = start + 4;
                    let block_len = len - 4;
                    if block_len > self.pending_headers_cap {
                        return Err(ConnError::HeaderListTooLarge);
                    }
                    self.extend_recv_header_block(block_start, block_len)?;
                    let stream_id = header.stream_id;
                    let end_headers = header.flags.has(Flags::END_HEADERS);
                    self.consume_inbound(total);
                    self.handle_push_promise_frame(stream_id, promised, end_headers)?;
                    continue;
                }
                frame::Type::Priority => {
                    let mut payload = [0; 5];
                    if header.length != payload.len() as u32 {
                        return Err(ParseError::FrameSize.into());
                    }
                    let copied = self.inbound.copy_range_into(HEADER_LEN, &mut payload);
                    debug_assert!(copied);
                    let _ = Priority::parse(header, &payload)?;
                }
            }
            self.consume_inbound(total);
        }
    }

    fn parse_frame_header(&self) -> Result<FrameHeader, ParseError> {
        let Some((first, second)) = self.inbound.range_slices(0, HEADER_LEN) else {
            return Err(ParseError::NeedMore);
        };
        if second.is_empty() {
            return FrameHeader::parse(first);
        }
        let mut bytes = [0; HEADER_LEN];
        bytes[..first.len()].copy_from_slice(first);
        bytes[first.len()..].copy_from_slice(second);
        FrameHeader::parse(&bytes)
    }

    fn unpadded_payload(&self, header: FrameHeader) -> Result<(usize, usize), ParseError> {
        let mut start = HEADER_LEN;
        let mut len = header.length as usize;
        if !header.flags.has(Flags::PADDED) {
            return Ok((start, len));
        }
        if len == 0 {
            return Err(ParseError::Padding);
        }
        let mut byte = [0; 1];
        let copied = self.inbound.copy_range_into(start, &mut byte);
        debug_assert!(copied);
        let padding = byte[0] as usize;
        if padding + 1 > len {
            return Err(ParseError::Padding);
        }
        start += 1;
        len -= padding + 1;
        Ok((start, len))
    }

    fn extend_recv_header_block(&mut self, start: usize, len: usize) -> Result<(), ConnError> {
        let (first, second) = self
            .inbound
            .range_slices(start, len)
            .ok_or(ConnError::FrameSize)?;
        self.recv_header_block.extend_from_slice(first);
        self.recv_header_block.extend_from_slice(second);
        Ok(())
    }

    fn handle_headers_frame(
        &mut self,
        stream_id: StreamId,
        end_stream: bool,
        end_headers: bool,
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
                    self.last_peer_stream_id = stream_id.0;
                    return Ok(());
                }
                if self.track_stream(Stream::new(stream_id)).is_err() {
                    self.emit_rst(stream_id, ErrorCode::RefusedStream)?;
                    self.last_peer_stream_id = stream_id.0;
                    return Ok(());
                }
                self.last_peer_stream_id = stream_id.0;
            }
            StreamClass::Active => {}
        }
        let trailing = self.is_trailing(stream_id);
        if end_headers {
            let (headers, over_limit) = self.decode_recv_block()?;
            let valid = if R::IS_SERVER {
                Validate::request(&headers, trailing)
            } else {
                Validate::response(&headers, trailing)
            };
            if valid.is_err() || over_limit {
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
            self.pending_headers = Some(PendingHeaders {
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
                .increase(parsed.increment)
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
                    .increase(parsed.increment)
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
        fragment_len: usize,
    ) -> Result<(), ConnError> {
        let pending = self
            .pending_headers
            .as_mut()
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
        if end_headers {
            let pending = self.pending_headers.take().unwrap();
            let (headers, over_limit) = self.decode_recv_block()?;
            match pending.kind {
                PendingKind::Headers {
                    end_stream,
                    trailing,
                } => {
                    let valid = if R::IS_SERVER {
                        Validate::request(&headers, trailing)
                    } else {
                        Validate::response(&headers, trailing)
                    };
                    if valid.is_err() || over_limit {
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
                    let valid = Validate::request(&headers, false);
                    if valid.is_err() || over_limit {
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
        match self.classify_stream(r.stream_id) {
            StreamClass::Connection => return Err(ConnError::Protocol),
            StreamClass::Idle => return Err(ConnError::Protocol),
            StreamClass::ClosedRst | StreamClass::ClosedEnd => return Ok(()),
            StreamClass::Active => {}
        }
        let peer_reset_count = self.peer_reset_count.saturating_add(1);
        if peer_reset_count > MAX_RESET_STREAMS {
            return Err(ConnError::Overload);
        }
        self.advance_stream(r.stream_id, stream::Event::RstStream, Side::Remote)
            .map_err(|_| ConnError::Protocol)?;
        self.push_event(Event::StreamReset {
            stream_id: r.stream_id,
            error: r.error,
        })?;
        self.peer_reset_count = peer_reset_count;
        Ok(())
    }

    fn handle_push_promise_frame(
        &mut self,
        stream_id: StreamId,
        promised: StreamId,
        end_headers: bool,
    ) -> Result<(), ConnError> {
        if R::IS_SERVER {
            return Err(ConnError::Protocol);
        }
        if end_headers {
            let (headers, over_limit) = self.decode_recv_block()?;
            if Validate::request(&headers, false).is_err() || over_limit {
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
            self.pending_headers = Some(PendingHeaders {
                stream_id,
                kind: PendingKind::PushPromise { promised },
                continuations: 0,
            });
        }
        Ok(())
    }
}

impl Conn<crate::role::ClientRole> {
    pub fn start_request(
        &mut self,
        headers: &[hpack::Header<'_>],
        end_stream: bool,
    ) -> Result<StreamId, ConnError> {
        if self.goaway_received.is_some() || self.goaway_sent {
            return Err(ConnError::StreamGoneAway);
        }
        if !self.can_track_local_stream() {
            return Err(ConnError::StreamLimit);
        }
        let id = self.next_local_id.next_id().ok_or(ConnError::StreamLimit)?;
        self.track_stream(Stream::new(id))
            .map_err(|_| ConnError::StreamLimit)?;
        self.emit_headers(id, headers.iter().copied(), end_stream)?;
        self.advance_stream(id, stream::Event::Headers { end_stream }, Side::Local)
            .map_err(|_| ConnError::Protocol)?;
        Ok(id)
    }
}

impl Conn<crate::role::ServerRole> {
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

impl<R: Role> Default for Conn<R> {
    fn default() -> Self {
        Self::new()
    }
}
