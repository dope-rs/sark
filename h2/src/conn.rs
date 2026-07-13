use core::marker::PhantomData;
use std::collections::{BTreeMap, VecDeque};

use dope::runtime::profile::Throughput;

use crate::frame::{
    self, Continuation, Data, ErrorCode, Flags, FrameHeader, GoAway, HEADER_LEN, Headers,
    ParseError, Ping, Priority, PushPromise, RstStream, SettingId, WindowUpdate,
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

    pub fn encode(&self, out: &mut Vec<u8>) {
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

    fn push_param(out: &mut Vec<u8>, id: SettingId, value: u32) {
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
        debug: Vec<u8>,
    },
    Headers {
        stream_id: StreamId,
        headers: Vec<hpack::OwnedHeader>,
        end_stream: bool,
        trailing: bool,
    },
    Data {
        stream_id: StreamId,
        data: Vec<u8>,
        end_stream: bool,
    },
    StreamReset {
        stream_id: StreamId,
        error: ErrorCode,
    },
    PushPromise {
        stream_id: StreamId,
        promised_stream_id: StreamId,
        headers: Vec<hpack::OwnedHeader>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingKind {
    Headers { end_stream: bool, trailing: bool },
    PushPromise { promised: StreamId },
}

struct PendingHeaders {
    stream_id: StreamId,
    kind: PendingKind,
    buf: Vec<u8>,
    continuations: u32,
}

const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 16_384;
const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 256;
const MAX_INBOUND_BUFFER: usize = 1 << 20;
const MAX_OUTBOUND_BUFFER: usize = 1 << 20;
const MAX_EVENTS: usize = 1 << 16;
const MAX_RESET_STREAMS: u32 = 100;
const RESET_RING_CAP: usize = 256;
const MAX_CONTINUATION_FRAMES: u32 = 64;

pub struct Conn<R: Role> {
    role: PhantomData<R>,

    inbound: Vec<u8>,
    outbound: Vec<u8>,

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

    events: VecDeque<Event>,

    encoder: hpack::Encoder,
    decoder: hpack::Decoder,
    streams: BTreeMap<StreamId, Stream>,
    next_local_id: stream::IdGen,
    last_peer_stream_id: u32,
    pending_headers: Option<PendingHeaders>,
    pending_headers_cap: usize,
    per_stream_send_window: BTreeMap<StreamId, flow::Window>,
    per_stream_recv_window: BTreeMap<StreamId, flow::Window>,
    conn_pending_release: u32,
    per_stream_pending_release: BTreeMap<StreamId, u32>,
    reset_streams: VecDeque<StreamId>,
    peer_reset_count: u32,
    send_window_opened: bool,
}

impl<R: Role> Conn<R> {
    pub fn new() -> Self {
        Self::with_tuning::<Throughput>()
    }

    pub fn with_tuning<P: Tuning>() -> Self {
        Self::with_local_settings(
            Settings {
                initial_window_size: P::STREAM_RECV_WINDOW,
                ..Settings::DEFAULT
            },
            P::CONN_RECV_WINDOW,
        )
    }

    pub fn with_local_settings(local: Settings, recv_window_target: u32) -> Self {
        let peer = Settings::DEFAULT;
        let mut local = local;
        if R::IS_SERVER {
            local.enable_push = false;
            if local.max_concurrent_streams.is_none() {
                local.max_concurrent_streams = Some(DEFAULT_MAX_CONCURRENT_STREAMS);
            }
        }
        if local.max_header_list_size.is_none() {
            local.max_header_list_size = Some(DEFAULT_MAX_HEADER_LIST_SIZE);
        }
        let header_list_cap = local.max_header_list_size.unwrap() as usize;
        let mut decoder = hpack::Decoder::new(local.header_table_size as usize);
        decoder.set_max_header_list_size(Some(header_list_cap));
        let mut conn = Self {
            role: PhantomData,
            inbound: Vec::new(),
            outbound: Vec::new(),
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
            events: VecDeque::new(),
            encoder: hpack::Encoder::new(local.header_table_size as usize),
            decoder,
            streams: BTreeMap::new(),
            next_local_id: stream::IdGen::new(R::FIRST_LOCAL_STREAM_ID),
            last_peer_stream_id: 0,
            pending_headers: None,
            pending_headers_cap: header_list_cap,
            per_stream_send_window: BTreeMap::new(),
            per_stream_recv_window: BTreeMap::new(),
            conn_pending_release: 0,
            per_stream_pending_release: BTreeMap::new(),
            reset_streams: VecDeque::new(),
            peer_reset_count: 0,
            send_window_opened: false,
        };
        if R::PREFACE_SENDS_FIRST {
            conn.outbound.extend_from_slice(CLIENT_PREFACE);
            conn.preface_done = true;
            conn.events.push_back(Event::PrefaceComplete);
        }
        conn.emit_initial_settings();
        conn.initial_settings_sent = true;
        let bump = recv_window_target.saturating_sub(flow::Window::INITIAL as u32);
        if bump > 0 && conn.recv_window.increase(bump).is_ok() {
            conn.emit_window_update(StreamId::CONNECTION, bump);
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
        &self.outbound
    }

    pub fn drain_outbound(&mut self, n: usize) {
        let n = n.min(self.outbound.len());
        self.outbound.drain(..n);
    }

    pub fn drain_into(&mut self, write_buf: &mut [u8]) -> usize {
        let n = self.outbound.len().min(write_buf.len());
        write_buf[..n].copy_from_slice(&self.outbound[..n]);
        self.drain_outbound(n);
        n
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// Consume the coalesced "peer opened our send window" signal accumulated
    /// since the last call. Set whenever a WINDOW_UPDATE or an initial-window
    /// SETTINGS increase grows a send window; a flood of such frames collapses
    /// to a single signal instead of one event per frame.
    pub fn take_window_opened(&mut self) -> bool {
        std::mem::take(&mut self.send_window_opened)
    }

    pub fn has_stream(&self, id: StreamId) -> bool {
        self.streams.contains_key(&id)
    }

    pub fn stream_state(&self, id: StreamId) -> Option<stream::State> {
        self.streams.get(&id).map(|s| s.state)
    }

    pub fn active_count(&self) -> usize {
        self.streams.len()
    }

    pub fn tracked_closed_count(&self) -> usize {
        self.reset_streams.len()
    }

    pub fn stream_send_window(&self, id: StreamId) -> Option<flow::Window> {
        self.per_stream_send_window.get(&id).copied()
    }

    pub fn stream_recv_window(&self, id: StreamId) -> Option<flow::Window> {
        self.per_stream_recv_window.get(&id).copied()
    }

    pub fn ping(&mut self, opaque: [u8; 8]) {
        let frame = Ping { ack: false, opaque };
        frame.encode(&mut self.outbound);
    }

    pub fn goaway(&mut self, error: ErrorCode, debug: &[u8]) {
        let frame = GoAway {
            last_stream_id: StreamId(self.last_peer_stream_id),
            error,
            debug,
        };
        frame.encode(&mut self.outbound);
        self.goaway_sent = true;
    }

    pub fn reset_stream(&mut self, stream_id: StreamId, error: ErrorCode) -> Result<(), ConnError> {
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
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
        if !self.has_stream(stream_id) {
            return Err(ConnError::BadStream);
        }
        let sw = self
            .per_stream_send_window
            .get_mut(&stream_id)
            .ok_or(ConnError::BadStream)?;
        let mut pair = flow::Pair {
            conn: &mut self.send_window,
            stream: sw,
        };
        let avail = pair.available();
        if avail == 0 && !data.is_empty() {
            return Ok(0);
        }
        let max_frame = self.peer_settings.max_frame_size as usize;
        let send_n = data.len().min(avail).min(max_frame);
        if send_n > 0 {
            pair.consume(send_n).map_err(ConnError::from)?;
        }
        let last_chunk = send_n == data.len();
        let es = end_stream && last_chunk;
        Data {
            stream_id,
            end_stream: es,
            payload: &data[..send_n],
        }
        .encode(&mut self.outbound);
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
        self.inbound.extend_from_slice(bytes);
        self.drive()?;
        let inbound_cap =
            MAX_INBOUND_BUFFER.max(self.local_settings.max_frame_size as usize + HEADER_LEN);
        if self.inbound.len() > inbound_cap {
            return Err(ConnError::Overload);
        }
        if self.events.len() > MAX_EVENTS {
            return Err(ConnError::Overload);
        }
        if self.outbound.len() > MAX_OUTBOUND_BUFFER {
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

    fn emit_settings_ack(&mut self) {
        FrameHeader {
            length: 0,
            kind: frame::Type::Settings,
            flags: Flags(Flags::ACK),
            stream_id: StreamId(0),
        }
        .encode(&mut self.outbound);
    }

    fn emit_window_update(&mut self, stream_id: StreamId, increment: u32) {
        if increment == 0 {
            return;
        }
        WindowUpdate {
            stream_id,
            increment,
        }
        .encode(&mut self.outbound);
    }

    fn emit_rst(&mut self, stream_id: StreamId, error: ErrorCode) {
        RstStream { stream_id, error }.encode(&mut self.outbound);
    }

    fn rst_evict(&mut self, stream_id: StreamId, error: ErrorCode) {
        self.emit_rst(stream_id, error);
        self.mark_reset(stream_id);
        self.evict_stream(stream_id);
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
        let mut block = Vec::new();
        self.encoder.encode(headers, &mut block);
        let max_frame = self.peer_settings.max_frame_size as usize;
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
        Ok(())
    }

    fn advance_stream(
        &mut self,
        id: StreamId,
        ev: stream::Event,
        side: Side,
    ) -> Result<(), TransitionError> {
        let stream = self.streams.get_mut(&id).ok_or(TransitionError::Protocol)?;
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
        if self.reset_streams.len() >= RESET_RING_CAP {
            self.reset_streams.pop_front();
        }
        self.reset_streams.push_back(id);
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

    fn classify_stream(&self, id: StreamId) -> StreamClass {
        if id.0 == 0 {
            return StreamClass::Connection;
        }
        if self.streams.contains_key(&id) {
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
        self.streams.remove(&id);
        self.per_stream_send_window.remove(&id);
        self.per_stream_recv_window.remove(&id);
        self.per_stream_pending_release.remove(&id);
    }

    fn decode_block(&mut self, block: &[u8]) -> Result<(Vec<hpack::OwnedHeader>, bool), ConnError> {
        let mut headers = Vec::new();
        let over_limit = self.decoder.decode_bounded(block, |n, v| {
            headers.push(hpack::OwnedHeader {
                name: n.to_vec(),
                value: v.to_vec(),
            });
        })?;
        Ok((headers, over_limit))
    }

    fn drive(&mut self) -> Result<(), ConnError> {
        if !R::PREFACE_SENDS_FIRST && !self.preface_done {
            if self.inbound.len() < CLIENT_PREFACE.len() {
                return Ok(());
            }
            if &self.inbound[..CLIENT_PREFACE.len()] != CLIENT_PREFACE {
                return Err(ConnError::BadPreface);
            }
            self.inbound.drain(..CLIENT_PREFACE.len());
            self.preface_done = true;
            self.events.push_back(Event::PrefaceComplete);
        }

        loop {
            let header = match FrameHeader::parse(&self.inbound) {
                Ok(h) => h,
                Err(ParseError::NeedMore) => return Ok(()),
                Err(ParseError::BadType(_)) => {
                    if self.inbound.len() < HEADER_LEN {
                        return Ok(());
                    }
                    let length =
                        u32::from_be_bytes([0, self.inbound[0], self.inbound[1], self.inbound[2]]);
                    if length > self.local_settings.max_frame_size {
                        return Err(ConnError::FrameSize);
                    }
                    let total = HEADER_LEN + length as usize;
                    if self.inbound.len() < total {
                        return Ok(());
                    }
                    self.inbound.drain(..total);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            if header.length > self.local_settings.max_frame_size {
                return Err(ConnError::FrameSize);
            }
            let total = HEADER_LEN + header.length as usize;
            if self.inbound.len() < total {
                return Ok(());
            }
            if self.pending_headers.is_some() && header.kind != frame::Type::Continuation {
                return Err(ConnError::Continuation);
            }
            let payload_end = total;
            let payload_start = HEADER_LEN;
            match header.kind {
                frame::Type::Settings => {
                    let payload = &self.inbound[payload_start..payload_end];
                    let parsed = frame::Settings::parse(header, payload)?;
                    if parsed.ack {
                        self.peer_settings_received = true;
                        self.events.push_back(Event::SettingsAck);
                    } else {
                        let prev_iws = self.peer_settings.initial_window_size as i64;
                        let params: Vec<u8> = parsed.params.to_vec();
                        for chunk in params.chunks_exact(6) {
                            let id_raw = u16::from_be_bytes([chunk[0], chunk[1]]);
                            let val = u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]);
                            if let Some(id) = SettingId::from_u16(id_raw) {
                                self.peer_settings.apply(id, val)?;
                                if id == SettingId::HeaderTableSize {
                                    self.encoder.set_max_size(val as usize);
                                }
                            }
                        }
                        let new_iws = self.peer_settings.initial_window_size as i64;
                        let delta = new_iws - prev_iws;
                        if delta != 0 {
                            let delta32 = delta as i32;
                            for w in self.per_stream_send_window.values_mut() {
                                w.adjust_initial(delta32).map_err(ConnError::from)?;
                            }
                        }
                        self.inbound.drain(..total);
                        self.emit_settings_ack();
                        self.events.push_back(Event::SettingsApplied);
                        if delta > 0 {
                            self.send_window_opened = true;
                        }
                        continue;
                    }
                }
                frame::Type::Ping => {
                    let payload = &self.inbound[payload_start..payload_end];
                    let parsed = Ping::parse(header, payload)?;
                    if !parsed.ack {
                        let pong = Ping {
                            ack: true,
                            opaque: parsed.opaque,
                        };
                        self.inbound.drain(..total);
                        pong.encode(&mut self.outbound);
                        self.events.push_back(Event::Ping {
                            ack: false,
                            opaque: parsed.opaque,
                        });
                        continue;
                    } else {
                        self.events.push_back(Event::Ping {
                            ack: true,
                            opaque: parsed.opaque,
                        });
                    }
                }
                frame::Type::GoAway => {
                    let payload = &self.inbound[payload_start..payload_end];
                    let parsed = GoAway::parse(header, payload)?;
                    self.goaway_received = Some(parsed.error);
                    let debug = parsed.debug.to_vec();
                    let last = parsed.last_stream_id;
                    let err = parsed.error;
                    self.events.push_back(Event::GoAway {
                        last_stream_id: last,
                        error: err,
                        debug,
                    });
                }
                frame::Type::WindowUpdate => {
                    let payload = &self.inbound[payload_start..payload_end];
                    let parsed = WindowUpdate::parse(header, payload)?;
                    self.inbound.drain(..total);
                    self.handle_window_update_frame(parsed)?;
                    continue;
                }
                frame::Type::Headers => {
                    let (sid, es, eh, frag) = {
                        let parsed =
                            Headers::parse(header, &self.inbound[payload_start..payload_end])?;
                        (
                            parsed.stream_id,
                            parsed.end_stream,
                            parsed.end_headers,
                            parsed.block_fragment.to_vec(),
                        )
                    };
                    self.inbound.drain(..total);
                    self.handle_headers_frame(sid, es, eh, frag)?;
                    continue;
                }
                frame::Type::Data => {
                    let (sid, es, payload) = {
                        let parsed =
                            Data::parse(header, &self.inbound[payload_start..payload_end])?;
                        (parsed.stream_id, parsed.end_stream, parsed.payload.to_vec())
                    };
                    self.inbound.drain(..total);
                    self.handle_data_frame(sid, es, payload)?;
                    continue;
                }
                frame::Type::Continuation => {
                    let (sid, eh, frag) = {
                        let parsed =
                            Continuation::parse(header, &self.inbound[payload_start..payload_end])?;
                        (
                            parsed.stream_id,
                            parsed.end_headers,
                            parsed.block_fragment.to_vec(),
                        )
                    };
                    self.inbound.drain(..total);
                    self.handle_continuation_frame(sid, eh, frag)?;
                    continue;
                }
                frame::Type::RstStream => {
                    let parsed =
                        RstStream::parse(header, &self.inbound[payload_start..payload_end])?;
                    self.inbound.drain(..total);
                    self.handle_rst_frame(parsed)?;
                    continue;
                }
                frame::Type::PushPromise => {
                    let (sid, promised, eh, frag) = {
                        let parsed =
                            PushPromise::parse(header, &self.inbound[payload_start..payload_end])?;
                        (
                            parsed.stream_id,
                            parsed.promised_stream_id,
                            parsed.end_headers,
                            parsed.block_fragment.to_vec(),
                        )
                    };
                    self.inbound.drain(..total);
                    self.handle_push_promise_frame(sid, promised, eh, frag)?;
                    continue;
                }
                frame::Type::Priority => {
                    let _ = Priority::parse(header, &self.inbound[payload_start..payload_end])?;
                }
            }
            self.inbound.drain(..total);
        }
    }

    fn handle_headers_frame(
        &mut self,
        stream_id: StreamId,
        end_stream: bool,
        end_headers: bool,
        block_fragment: Vec<u8>,
    ) -> Result<(), ConnError> {
        if stream_id.is_zero() {
            return Err(ConnError::Protocol);
        }
        match self.classify_stream(stream_id) {
            StreamClass::Connection => return Err(ConnError::Protocol),
            StreamClass::ClosedEnd => return Err(ConnError::StreamClosed),
            StreamClass::ClosedRst => {
                self.emit_rst(stream_id, ErrorCode::StreamClosed);
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
                    self.emit_rst(stream_id, ErrorCode::RefusedStream);
                    return Ok(());
                }
                if let Some(max) = self.local_settings.max_concurrent_streams
                    && self.active_count() >= max as usize
                {
                    self.emit_rst(stream_id, ErrorCode::RefusedStream);
                    self.last_peer_stream_id = stream_id.0;
                    return Ok(());
                }
                self.streams.insert(stream_id, Stream::new(stream_id));
                self.per_stream_send_window.insert(
                    stream_id,
                    flow::Window::with(self.peer_settings.initial_window_size as i32),
                );
                self.per_stream_recv_window.insert(
                    stream_id,
                    flow::Window::with(self.local_settings.initial_window_size as i32),
                );
                self.last_peer_stream_id = stream_id.0;
            }
            StreamClass::Active => {}
        }
        let trailing = self.is_trailing(stream_id);
        if end_headers {
            let (headers, over_limit) = self.decode_block(&block_fragment)?;
            let valid = if R::IS_SERVER {
                Validate::request(&headers, trailing)
            } else {
                Validate::response(&headers, trailing)
            };
            if valid.is_err() || over_limit {
                self.rst_evict(stream_id, ErrorCode::ProtocolError);
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
                    self.rst_evict(stream_id, ErrorCode::StreamClosed);
                    return Ok(());
                }
            }
            if let Some(s) = self.streams.get_mut(&stream_id) {
                s.peer_headers_received = true;
            }
            self.events.push_back(Event::Headers {
                stream_id,
                headers,
                end_stream,
                trailing,
            });
        } else {
            if block_fragment.len() > self.pending_headers_cap {
                return Err(ConnError::HeaderListTooLarge);
            }
            self.pending_headers = Some(PendingHeaders {
                stream_id,
                kind: PendingKind::Headers {
                    end_stream,
                    trailing,
                },
                buf: block_fragment,
                continuations: 0,
            });
        }
        Ok(())
    }

    fn is_trailing(&self, stream_id: StreamId) -> bool {
        self.streams
            .get(&stream_id)
            .map(|s| s.peer_headers_received)
            .unwrap_or(false)
    }

    fn handle_data_frame(
        &mut self,
        stream_id: StreamId,
        end_stream: bool,
        payload: Vec<u8>,
    ) -> Result<(), ConnError> {
        match self.classify_stream(stream_id) {
            StreamClass::Connection => return Err(ConnError::Protocol),
            StreamClass::Idle => return Err(ConnError::Protocol),
            StreamClass::ClosedEnd => return Err(ConnError::StreamClosed),
            StreamClass::ClosedRst => {
                self.emit_rst(stream_id, ErrorCode::StreamClosed);
                return Ok(());
            }
            StreamClass::Active => {}
        }
        let n = payload.len();
        self.recv_window
            .consume(n)
            .map_err(|_| ConnError::FlowControl)?;
        {
            let sw = self
                .per_stream_recv_window
                .get_mut(&stream_id)
                .ok_or(ConnError::Protocol)?;
            sw.consume(n).map_err(|_| ConnError::FlowControl)?;
        }
        self.replenish_recv(stream_id, n)?;
        match self.advance_stream(stream_id, stream::Event::Data { end_stream }, Side::Remote) {
            Ok(()) => {}
            Err(TransitionError::Protocol) => return Err(ConnError::Protocol),
            Err(TransitionError::StreamClosed) => {
                self.rst_evict(stream_id, ErrorCode::StreamClosed);
                return Ok(());
            }
        }
        self.events.push_back(Event::Data {
            stream_id,
            data: payload,
            end_stream,
        });
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
            self.emit_window_update(StreamId::CONNECTION, inc);
        }
        if self.per_stream_recv_window.contains_key(&stream_id) {
            let stream_threshold = (self.local_settings.initial_window_size / 2).max(1);
            let pending = self
                .per_stream_pending_release
                .entry(stream_id)
                .or_insert(0);
            *pending = pending.saturating_add(n32);
            if *pending >= stream_threshold {
                let inc = *pending;
                *pending = 0;
                if let Some(sw) = self.per_stream_recv_window.get_mut(&stream_id) {
                    sw.increase(inc).map_err(ConnError::from)?;
                }
                self.emit_window_update(stream_id, inc);
            }
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
                let w = self
                    .per_stream_send_window
                    .get_mut(&parsed.stream_id)
                    .ok_or(ConnError::Protocol)?;
                w.increase(parsed.increment).map_err(ConnError::from)?;
                self.send_window_opened = true;
                Ok(())
            }
        }
    }

    fn handle_continuation_frame(
        &mut self,
        stream_id: StreamId,
        end_headers: bool,
        block_fragment: Vec<u8>,
    ) -> Result<(), ConnError> {
        let cap = self.pending_headers_cap;
        let pending = self
            .pending_headers
            .as_mut()
            .ok_or(ConnError::Continuation)?;
        if pending.stream_id != stream_id {
            return Err(ConnError::Continuation);
        }
        if block_fragment.is_empty() && !end_headers {
            return Err(ConnError::Continuation);
        }
        pending.continuations = pending.continuations.saturating_add(1);
        if pending.continuations > MAX_CONTINUATION_FRAMES {
            return Err(ConnError::Overload);
        }
        if block_fragment.len() > cap.saturating_sub(pending.buf.len()) {
            return Err(ConnError::HeaderListTooLarge);
        }
        pending.buf.extend_from_slice(&block_fragment);
        if end_headers {
            let pending = self.pending_headers.take().unwrap();
            let (headers, over_limit) = self.decode_block(&pending.buf)?;
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
                        self.rst_evict(pending.stream_id, ErrorCode::ProtocolError);
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
                            self.rst_evict(pending.stream_id, ErrorCode::StreamClosed);
                            return Ok(());
                        }
                    }
                    if let Some(s) = self.streams.get_mut(&pending.stream_id) {
                        s.peer_headers_received = true;
                    }
                    self.events.push_back(Event::Headers {
                        stream_id: pending.stream_id,
                        headers,
                        end_stream,
                        trailing,
                    });
                }
                PendingKind::PushPromise { promised } => {
                    let valid = Validate::request(&headers, false);
                    if valid.is_err() || over_limit {
                        self.rst_evict(promised, ErrorCode::ProtocolError);
                        return Ok(());
                    }
                    self.events.push_back(Event::PushPromise {
                        stream_id: pending.stream_id,
                        promised_stream_id: promised,
                        headers,
                    });
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
        self.advance_stream(r.stream_id, stream::Event::RstStream, Side::Remote)
            .map_err(|_| ConnError::Protocol)?;
        self.events.push_back(Event::StreamReset {
            stream_id: r.stream_id,
            error: r.error,
        });
        self.peer_reset_count = self.peer_reset_count.saturating_add(1);
        if self.peer_reset_count > MAX_RESET_STREAMS {
            return Err(ConnError::Overload);
        }
        Ok(())
    }

    fn handle_push_promise_frame(
        &mut self,
        stream_id: StreamId,
        promised: StreamId,
        end_headers: bool,
        block_fragment: Vec<u8>,
    ) -> Result<(), ConnError> {
        if R::IS_SERVER {
            return Err(ConnError::Protocol);
        }
        if end_headers {
            let (headers, over_limit) = self.decode_block(&block_fragment)?;
            if Validate::request(&headers, false).is_err() || over_limit {
                self.emit_rst(promised, ErrorCode::ProtocolError);
                return Ok(());
            }
            self.streams
                .insert(promised, Stream::reserve_remote(promised));
            self.per_stream_send_window.insert(
                promised,
                flow::Window::with(self.peer_settings.initial_window_size as i32),
            );
            self.per_stream_recv_window.insert(
                promised,
                flow::Window::with(self.local_settings.initial_window_size as i32),
            );
            self.events.push_back(Event::PushPromise {
                stream_id,
                promised_stream_id: promised,
                headers,
            });
        } else {
            if block_fragment.len() > self.pending_headers_cap {
                return Err(ConnError::HeaderListTooLarge);
            }
            self.pending_headers = Some(PendingHeaders {
                stream_id,
                kind: PendingKind::PushPromise { promised },
                buf: block_fragment,
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
        let id = self.next_local_id.next_id().ok_or(ConnError::StreamLimit)?;
        self.streams.insert(id, Stream::new(id));
        self.per_stream_send_window.insert(
            id,
            flow::Window::with(self.peer_settings.initial_window_size as i32),
        );
        self.per_stream_recv_window.insert(
            id,
            flow::Window::with(self.local_settings.initial_window_size as i32),
        );
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
