use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::ops::{Deref, Range};

use o3::buffer::Shared;
use sark_core::http::{Field, VecFieldBlock};

use crate::frame::{
    ErrorCode, Frame, ParseError, STREAM_TYPE_CONTROL, STREAM_TYPE_QPACK_DECODER,
    STREAM_TYPE_QPACK_ENCODER, Settings, TYPE_CANCEL_PUSH, TYPE_DATA, TYPE_GOAWAY, TYPE_HEADERS,
    TYPE_MAX_PUSH_ID, TYPE_SETTINGS,
};
use crate::payload::Payload;
use crate::qpack::{self, DecodeOutcome, DecoderError};
use crate::stream::{StreamId, UniStreamType};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnError {
    Parse(ParseError),
    Qpack(DecoderError),
    FrameUnexpected,
    MissingSettings,
    ClosedCriticalStream,
    StreamCreation,
    Id,
    QpackEncoderStream,
    QpackDecoderStream,
    Protocol,
}

impl From<ParseError> for ConnError {
    fn from(err: ParseError) -> Self {
        Self::Parse(err)
    }
}

impl From<DecoderError> for ConnError {
    fn from(err: DecoderError) -> Self {
        Self::Qpack(err)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Settings(Settings),
    Headers {
        stream_id: StreamId,
        fields: VecFieldBlock,
        trailing: bool,
    },
    Data {
        stream_id: StreamId,
        data: Payload,
    },
    PushPromise {
        stream_id: StreamId,
        push_id: u64,
        fields: VecFieldBlock,
    },
    CancelPush {
        push_id: u64,
    },
    MaxPushId {
        push_id: u64,
    },
    Finished {
        stream_id: StreamId,
    },
    Reset {
        stream_id: StreamId,
        error_code: u64,
    },
    Stopped {
        stream_id: StreamId,
        error_code: u64,
    },
    PushHeaders {
        stream_id: StreamId,
        push_id: u64,
        fields: VecFieldBlock,
        trailing: bool,
    },
    PushData {
        stream_id: StreamId,
        push_id: u64,
        data: Payload,
    },
    PushFinished {
        stream_id: StreamId,
        push_id: u64,
    },
    GoAway {
        id: u64,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Client,
    Server,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
enum MessageState {
    #[default]
    Idle,
    Headers,
    Data,
    Trailers,
}

#[derive(Clone, Debug, Default)]
struct StreamState {
    inbound: Inbound,
    uni_type: Option<UniStreamType>,
    saw_settings: bool,
    message: MessageState,
    push_id: Option<u64>,
    fin_received: bool,
    blocked_required_insert_count: Option<u64>,
}

#[derive(Clone, Debug)]
enum Inbound {
    Unique { bytes: Vec<u8>, start: usize },
    Shared(Shared),
}

impl Inbound {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Unique { bytes, start } => &bytes[*start..],
            Self::Shared(bytes) => bytes.as_slice(),
        }
    }

    fn append(&mut self, source: &[u8]) {
        if source.is_empty() {
            return;
        }
        match self {
            Self::Unique { bytes, start } => {
                Self::compact_unique(bytes, start);
                bytes.extend_from_slice(source);
            }
            Self::Shared(bytes) => {
                let mut joined = Vec::with_capacity(bytes.len() + source.len());
                joined.extend_from_slice(bytes.as_slice());
                joined.extend_from_slice(source);
                *self = Self::from(joined);
            }
        }
    }

    fn append_owned(&mut self, source: Vec<u8>) {
        if source.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = Self::from(source);
            return;
        }
        match self {
            Self::Unique { bytes, start } => {
                Self::compact_unique(bytes, start);
                bytes.extend_from_slice(&source);
            }
            Self::Shared(bytes) => {
                let mut joined = Vec::with_capacity(bytes.len() + source.len());
                joined.extend_from_slice(bytes.as_slice());
                joined.extend_from_slice(&source);
                *self = Self::from(joined);
            }
        }
    }

    fn compact_unique(bytes: &mut Vec<u8>, start: &mut usize) {
        if *start == 0 {
            return;
        }
        let len = bytes.len() - *start;
        bytes.copy_within(*start.., 0);
        bytes.truncate(len);
        *start = 0;
    }

    fn advance(&mut self, n: usize) {
        assert!(n <= self.len(), "H3 inbound advance is out of bounds");
        match self {
            Self::Unique { bytes, start } => {
                *start += n;
                if *start == bytes.len() {
                    self.clear();
                }
            }
            Self::Shared(bytes) => {
                bytes.advance(n);
                if bytes.is_empty() {
                    self.clear();
                }
            }
        }
    }

    fn take_payload(&mut self, range: Range<usize>, frame_len: usize) -> Payload {
        assert!(
            range.start <= range.end
                && range.end <= frame_len
                && frame_len <= self.as_slice().len(),
            "H3 payload frame range is out of bounds"
        );
        match mem::take(self) {
            Self::Unique { bytes, start } if frame_len == bytes.len() - start => {
                Payload::from_unique(bytes, start + range.start..start + range.end)
            }
            Self::Unique { bytes, start } => {
                let mut owner = Shared::from(bytes);
                let payload =
                    Payload::from_shared(owner.slice(start + range.start..start + range.end));
                owner.advance(start + frame_len);
                *self = Self::Shared(owner);
                payload
            }
            Self::Shared(mut owner) => {
                let payload = Payload::from_shared(owner.slice(range));
                owner.advance(frame_len);
                if !owner.is_empty() {
                    *self = Self::Shared(owner);
                }
                payload
            }
        }
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl Default for Inbound {
    fn default() -> Self {
        Self::Unique {
            bytes: Vec::new(),
            start: 0,
        }
    }
}

impl From<Vec<u8>> for Inbound {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Unique { bytes, start: 0 }
    }
}

impl Deref for Inbound {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Write {
    pub stream_id: StreamId,
    pub bytes: Vec<u8>,
    pub fin: bool,
}

pub struct Conn {
    role: Role,
    local_settings: Settings,
    peer_settings: Option<Settings>,
    max_frame_size: usize,
    qpack_encoder: qpack::Encoder,
    qpack_decoder: qpack::Decoder,
    streams: BTreeMap<StreamId, StreamState>,
    events: VecDeque<Event>,
    writes: VecDeque<Write>,
    control_stream_id: Option<StreamId>,
    qpack_encoder_stream_id: Option<StreamId>,
    qpack_decoder_stream_id: Option<StreamId>,
    peer_control_stream_id: Option<StreamId>,
    peer_qpack_encoder_stream_id: Option<StreamId>,
    peer_qpack_decoder_stream_id: Option<StreamId>,
    peer_goaway_id: Option<u64>,
    max_push_id: Option<u64>,
}

impl Conn {
    pub fn new() -> Self {
        Self::with_role(Role::Client)
    }

    pub fn with_role(role: Role) -> Self {
        Self::with_role_and_settings(role, Settings::default())
    }

    pub fn with_settings(local_settings: Settings) -> Self {
        Self::with_role_and_settings(Role::Client, local_settings)
    }

    pub fn with_role_and_settings(role: Role, local_settings: Settings) -> Self {
        let max_field = local_settings
            .max_field_section_size
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(64 * 1024);
        let max_qpack_table =
            usize::try_from(local_settings.qpack_max_table_capacity).unwrap_or(usize::MAX);
        Self {
            role,
            local_settings,
            peer_settings: None,
            max_frame_size: 16 * 1024 * 1024,
            qpack_encoder: qpack::Encoder::with_dynamic_capacity(max_qpack_table),
            qpack_decoder: qpack::Decoder::with_dynamic_capacity(max_field, max_qpack_table),
            streams: BTreeMap::new(),
            events: VecDeque::new(),
            writes: VecDeque::new(),
            control_stream_id: None,
            qpack_encoder_stream_id: None,
            qpack_decoder_stream_id: None,
            peer_control_stream_id: None,
            peer_qpack_encoder_stream_id: None,
            peer_qpack_decoder_stream_id: None,
            peer_goaway_id: None,
            max_push_id: None,
        }
    }

    pub fn start_control_stream(&mut self, stream_id: StreamId) -> Result<(), ConnError> {
        if !stream_id.is_bidi() && self.control_stream_id.is_none() {
            self.control_stream_id = Some(stream_id);
            let mut payload = Vec::new();
            self.local_settings.encode(&mut payload)?;
            let mut bytes = Vec::new();
            sark_core::http::VarInt::encode(STREAM_TYPE_CONTROL, &mut bytes)
                .map_err(|_| ConnError::Protocol)?;
            Frame::encode(TYPE_SETTINGS, &payload, &mut bytes)?;
            self.writes.push_back(Write {
                stream_id,
                bytes,
                fin: false,
            });
            return Ok(());
        }
        Err(ConnError::Protocol)
    }

    pub fn start_qpack_encoder_stream(&mut self, stream_id: StreamId) -> Result<(), ConnError> {
        if self.qpack_encoder_stream_id.is_some() {
            return Err(ConnError::Protocol);
        }
        self.qpack_encoder_stream_id = Some(stream_id);
        self.start_uni_stream(stream_id, STREAM_TYPE_QPACK_ENCODER)
    }

    pub fn start_qpack_decoder_stream(&mut self, stream_id: StreamId) -> Result<(), ConnError> {
        if self.qpack_decoder_stream_id.is_some() {
            return Err(ConnError::Protocol);
        }
        self.qpack_decoder_stream_id = Some(stream_id);
        self.start_uni_stream(stream_id, STREAM_TYPE_QPACK_DECODER)
    }

    fn start_uni_stream(&mut self, stream_id: StreamId, stream_type: u64) -> Result<(), ConnError> {
        if stream_id.is_bidi() {
            return Err(ConnError::Protocol);
        }
        let mut bytes = Vec::new();
        sark_core::http::VarInt::encode(stream_type, &mut bytes)
            .map_err(|_| ConnError::Protocol)?;
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin: false,
        });
        Ok(())
    }

    pub fn ingest_stream(
        &mut self,
        stream_id: StreamId,
        bytes: &[u8],
        fin: bool,
    ) -> Result<(), ConnError> {
        if stream_id.is_bidi() {
            self.ingest_request_stream(stream_id, fin, |inbound| {
                inbound.append(bytes);
            })
        } else {
            self.ingest_uni_stream(stream_id, fin, |inbound| {
                inbound.append(bytes);
            })
        }
    }

    /// Ingests a transport-owned receive batch, adopting its allocation when
    /// this stream has no buffered partial frame.
    pub fn ingest_stream_owned(
        &mut self,
        stream_id: StreamId,
        bytes: Vec<u8>,
        fin: bool,
    ) -> Result<(), ConnError> {
        if stream_id.is_bidi() {
            self.ingest_request_stream(stream_id, fin, |inbound| {
                inbound.append_owned(bytes);
            })
        } else {
            self.ingest_uni_stream(stream_id, fin, |inbound| {
                inbound.append_owned(bytes);
            })
        }
    }

    pub fn send_headers<'a, I>(
        &mut self,
        stream_id: StreamId,
        fields: I,
        fin: bool,
    ) -> Result<(), ConnError>
    where
        I: IntoIterator<Item = Field<'a>>,
    {
        let mut block = Vec::new();
        self.qpack_encoder.encode(fields, &mut block);
        self.flush_qpack_encoder_stream();
        let mut bytes = Vec::new();
        Frame::encode(TYPE_HEADERS, &block, &mut bytes)?;
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin,
        });
        Ok(())
    }

    pub fn send_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Result<(), ConnError> {
        let mut bytes = Vec::new();
        Frame::encode(TYPE_DATA, data, &mut bytes)?;
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin,
        });
        Ok(())
    }

    pub fn send_push_promise<'a, I>(
        &mut self,
        stream_id: StreamId,
        push_id: u64,
        fields: I,
    ) -> Result<(), ConnError>
    where
        I: IntoIterator<Item = Field<'a>>,
    {
        if self.role == Role::Client {
            return Err(ConnError::FrameUnexpected);
        }
        let mut block = Vec::new();
        self.qpack_encoder.encode(fields, &mut block);
        let mut bytes = Vec::new();
        Frame::encode_push_promise(push_id, &block, &mut bytes)?;
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin: false,
        });
        Ok(())
    }

    pub fn send_cancel_push(&mut self, push_id: u64) -> Result<(), ConnError> {
        let stream_id = self.control_stream_id.ok_or(ConnError::Protocol)?;
        let mut bytes = Vec::new();
        Frame::encode_varint(TYPE_CANCEL_PUSH, push_id, &mut bytes)?;
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin: false,
        });
        Ok(())
    }

    pub fn send_goaway(&mut self, id: u64) -> Result<(), ConnError> {
        let stream_id = self.control_stream_id.ok_or(ConnError::Protocol)?;
        let mut bytes = Vec::new();
        Frame::encode_varint(TYPE_GOAWAY, id, &mut bytes)?;
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin: false,
        });
        Ok(())
    }

    pub fn send_max_push_id(&mut self, push_id: u64) -> Result<(), ConnError> {
        if self.role != Role::Client {
            return Err(ConnError::FrameUnexpected);
        }
        let stream_id = self.control_stream_id.ok_or(ConnError::Protocol)?;
        let mut bytes = Vec::new();
        Frame::encode_varint(TYPE_MAX_PUSH_ID, push_id, &mut bytes)?;
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin: false,
        });
        Ok(())
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    pub fn poll_write(&mut self) -> Option<Write> {
        self.writes.pop_front()
    }

    pub fn peer_settings(&self) -> Option<&Settings> {
        self.peer_settings.as_ref()
    }

    pub fn set_qpack_encoder_capacity(&mut self, capacity: usize) -> Result<(), ConnError> {
        self.qpack_encoder.set_dynamic_capacity(capacity)?;
        self.flush_qpack_encoder_stream();
        Ok(())
    }

    pub fn ingest_reset(&mut self, stream_id: StreamId, error_code: u64) {
        self.streams.remove(&stream_id);
        self.events.push_back(Event::Reset {
            stream_id,
            error_code,
        });
    }

    pub fn ingest_stopped(&mut self, stream_id: StreamId, error_code: u64) {
        self.events.push_back(Event::Stopped {
            stream_id,
            error_code,
        });
    }

    pub fn max_push_id(&self) -> Option<u64> {
        self.max_push_id
    }

    fn flush_qpack_encoder_stream(&mut self) {
        let Some(stream_id) = self.qpack_encoder_stream_id else {
            return;
        };
        let bytes = self.qpack_encoder.take_encoder_instructions();
        if bytes.is_empty() {
            return;
        }
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin: false,
        });
    }

    fn flush_qpack_decoder_stream(&mut self) {
        let Some(stream_id) = self.qpack_decoder_stream_id else {
            return;
        };
        let bytes = self.qpack_decoder.take_decoder_instructions();
        if bytes.is_empty() {
            return;
        }
        self.writes.push_back(Write {
            stream_id,
            bytes,
            fin: false,
        });
    }

    fn decode_block(
        &mut self,
        stream_id: StreamId,
        block: &[u8],
        blocked: &mut Option<u64>,
    ) -> Result<Option<VecFieldBlock>, ConnError> {
        match self.qpack_decoder.decode_or_blocked(block)? {
            DecodeOutcome::Ready {
                fields,
                required_insert_count,
            } => {
                *blocked = None;
                if required_insert_count > 0 {
                    self.qpack_decoder.acknowledge_section(stream_id.0);
                    self.flush_qpack_decoder_stream();
                }
                Ok(Some(fields))
            }
            DecodeOutcome::Blocked {
                required_insert_count,
            } => {
                *blocked = Some(required_insert_count);
                Ok(None)
            }
        }
    }

    fn ingest_request_stream<F>(
        &mut self,
        stream_id: StreamId,
        fin: bool,
        append: F,
    ) -> Result<(), ConnError>
    where
        F: FnOnce(&mut Inbound),
    {
        let mut state = self.streams.remove(&stream_id).unwrap_or_default();
        append(&mut state.inbound);
        state.fin_received |= fin;
        while !state.inbound.is_empty() {
            let rest = state.inbound.as_slice();
            let (frame, n) = match Frame::parse(rest, self.max_frame_size) {
                Ok(parsed) => parsed,
                Err(ParseError::NeedMore) => break,
                Err(err) => return Err(err.into()),
            };
            match frame {
                Frame::Headers(block) => {
                    let (next_message, trailing) = match state.message {
                        MessageState::Idle => (MessageState::Headers, false),
                        MessageState::Headers | MessageState::Data => {
                            (MessageState::Trailers, true)
                        }
                        MessageState::Trailers => return Err(ConnError::FrameUnexpected),
                    };
                    let decoded = self.decode_block(
                        stream_id,
                        block,
                        &mut state.blocked_required_insert_count,
                    )?;
                    let Some(fields) = decoded else {
                        self.streams.insert(stream_id, state);
                        return Ok(());
                    };
                    state.message = next_message;
                    self.events.push_back(Event::Headers {
                        stream_id,
                        fields,
                        trailing,
                    });
                }
                Frame::Data(data) => {
                    if !matches!(state.message, MessageState::Headers | MessageState::Data) {
                        return Err(ConnError::FrameUnexpected);
                    }
                    state.message = MessageState::Data;
                    let payload_start = n - data.len();
                    let data = state.inbound.take_payload(payload_start..n, n);
                    self.events.push_back(Event::Data { stream_id, data });
                    continue;
                }
                Frame::PushPromise { push_id, block } => {
                    if self.role != Role::Client {
                        return Err(ConnError::FrameUnexpected);
                    }
                    let decoded = self.decode_block(
                        stream_id,
                        block,
                        &mut state.blocked_required_insert_count,
                    )?;
                    let Some(fields) = decoded else {
                        self.streams.insert(stream_id, state);
                        return Ok(());
                    };
                    self.events.push_back(Event::PushPromise {
                        stream_id,
                        push_id,
                        fields,
                    });
                }
                Frame::Unknown { .. } => {}
                _ => return Err(ConnError::FrameUnexpected),
            }
            state.inbound.advance(n);
        }
        if state.fin_received {
            if !state.inbound.is_empty() || state.message == MessageState::Idle {
                return Err(ConnError::Protocol);
            }
            self.events.push_back(Event::Finished { stream_id });
            self.streams.remove(&stream_id);
        } else {
            self.streams.insert(stream_id, state);
        }
        Ok(())
    }

    fn ingest_uni_stream<F>(
        &mut self,
        stream_id: StreamId,
        fin: bool,
        append: F,
    ) -> Result<(), ConnError>
    where
        F: FnOnce(&mut Inbound),
    {
        let mut state = self.streams.remove(&stream_id).unwrap_or_default();
        append(&mut state.inbound);
        state.fin_received |= fin;

        if state.uni_type.is_none() {
            let (stream_type, type_len) = match sark_core::http::VarInt::decode(&state.inbound) {
                Ok(v) => v,
                Err(sark_core::http::varint::Error::Underflow) => {
                    self.streams.insert(stream_id, state);
                    return Ok(());
                }
                Err(_) => return Err(ConnError::Protocol),
            };
            let stream_type = UniStreamType::from_wire(stream_type);
            self.register_uni_stream(stream_id, stream_type)?;
            state.uni_type = Some(stream_type);
            state.inbound.advance(type_len);
        }

        let fin_received = state.fin_received;
        match state.uni_type.expect("uni stream type set above") {
            UniStreamType::Control => {
                self.ingest_control_stream(stream_id, &mut state, fin_received)?
            }
            UniStreamType::Push => self.ingest_push_stream(stream_id, &mut state, fin_received)?,
            UniStreamType::QpackEncoder => {
                let consumed = self
                    .qpack_decoder
                    .ingest_encoder(&state.inbound)
                    .map_err(|_| ConnError::QpackEncoderStream)?;
                if consumed > 0 {
                    state.inbound.advance(consumed);
                    self.flush_qpack_decoder_stream();
                    self.retry_blocked_streams()?;
                }
                if state.fin_received {
                    return Err(ConnError::ClosedCriticalStream);
                }
            }
            UniStreamType::QpackDecoder => {
                let consumed = self
                    .qpack_encoder
                    .ingest_decoder(&state.inbound)
                    .map_err(|_| ConnError::QpackDecoderStream)?;
                if consumed > 0 {
                    state.inbound.advance(consumed);
                }
                if state.fin_received {
                    return Err(ConnError::ClosedCriticalStream);
                }
            }
            UniStreamType::Unknown(_) => {
                state.inbound.clear();
            }
        }

        if state.fin_received && state.uni_type.is_some_and(UniStreamType::is_critical) {
            return Err(ConnError::ClosedCriticalStream);
        }
        if state.fin_received {
            self.streams.remove(&stream_id);
        } else {
            self.streams.insert(stream_id, state);
        }
        Ok(())
    }

    fn register_uni_stream(
        &mut self,
        stream_id: StreamId,
        stream_type: UniStreamType,
    ) -> Result<(), ConnError> {
        match stream_type {
            UniStreamType::Control => Self::register_single_stream(
                &mut self.peer_control_stream_id,
                stream_id,
                ConnError::StreamCreation,
            ),
            UniStreamType::QpackEncoder => Self::register_single_stream(
                &mut self.peer_qpack_encoder_stream_id,
                stream_id,
                ConnError::StreamCreation,
            ),
            UniStreamType::QpackDecoder => Self::register_single_stream(
                &mut self.peer_qpack_decoder_stream_id,
                stream_id,
                ConnError::StreamCreation,
            ),
            UniStreamType::Push if self.role != Role::Client || !stream_id.is_server_uni() => {
                Err(ConnError::StreamCreation)
            }
            UniStreamType::Push | UniStreamType::Unknown(_) => Ok(()),
        }
    }

    fn retry_blocked_streams(&mut self) -> Result<(), ConnError> {
        let insert_count = self.qpack_decoder.dynamic_insert_count();
        let stream_ids: Vec<StreamId> = self
            .streams
            .iter()
            .filter_map(|(stream_id, state)| {
                let required = state.blocked_required_insert_count?;
                (required <= insert_count).then_some(*stream_id)
            })
            .collect();
        for stream_id in stream_ids {
            if stream_id.is_bidi() {
                self.ingest_request_stream(stream_id, false, |_| {})?;
            } else {
                self.ingest_uni_stream(stream_id, false, |_| {})?;
            }
        }
        Ok(())
    }

    fn ingest_control_stream(
        &mut self,
        stream_id: StreamId,
        state: &mut StreamState,
        fin: bool,
    ) -> Result<(), ConnError> {
        while !state.inbound.is_empty() {
            let rest = state.inbound.as_slice();
            let (frame, n) = match Frame::parse(rest, self.max_frame_size) {
                Ok(parsed) => parsed,
                Err(ParseError::NeedMore) => break,
                Err(err) => return Err(err.into()),
            };
            if !state.saw_settings && !matches!(frame, Frame::Settings(_)) {
                return Err(ConnError::MissingSettings);
            }
            match frame {
                Frame::Settings(settings) => {
                    if state.saw_settings || self.peer_settings.is_some() {
                        return Err(ConnError::Protocol);
                    }
                    state.saw_settings = true;
                    self.peer_settings = Some(settings.clone());
                    self.qpack_encoder
                        .set_max_blocked_streams(settings.qpack_blocked_streams);
                    self.events.push_back(Event::Settings(settings));
                }
                Frame::CancelPush { push_id } => {
                    self.events.push_back(Event::CancelPush { push_id });
                }
                Frame::GoAway { id } => {
                    self.validate_goaway_id(id)?;
                    self.peer_goaway_id = Some(id);
                    self.events.push_back(Event::GoAway { id });
                }
                Frame::MaxPushId { push_id } => {
                    if self.role == Role::Client {
                        return Err(ConnError::FrameUnexpected);
                    }
                    if self.max_push_id.is_some_and(|prev| push_id < prev) {
                        return Err(ConnError::Id);
                    }
                    self.max_push_id = Some(push_id);
                    self.events.push_back(Event::MaxPushId { push_id });
                }
                Frame::Unknown { .. } => {}
                _ => return Err(ConnError::FrameUnexpected),
            }
            state.inbound.advance(n);
        }
        if fin {
            return Err(ConnError::ClosedCriticalStream);
        }
        let _ = stream_id;
        Ok(())
    }

    fn ingest_push_stream(
        &mut self,
        stream_id: StreamId,
        state: &mut StreamState,
        fin: bool,
    ) -> Result<(), ConnError> {
        if state.push_id.is_none() {
            let (push_id, n) = match sark_core::http::VarInt::decode(&state.inbound) {
                Ok(v) => v,
                Err(sark_core::http::varint::Error::Underflow) => return Ok(()),
                Err(_) => return Err(ConnError::Protocol),
            };
            state.push_id = Some(push_id);
            state.inbound.advance(n);
        }
        let push_id = state.push_id.expect("push id set above");
        while !state.inbound.is_empty() {
            let rest = state.inbound.as_slice();
            let (frame, n) = match Frame::parse(rest, self.max_frame_size) {
                Ok(parsed) => parsed,
                Err(ParseError::NeedMore) => break,
                Err(err) => return Err(err.into()),
            };
            match frame {
                Frame::Headers(block) => {
                    let (next_message, trailing) = match state.message {
                        MessageState::Idle => (MessageState::Headers, false),
                        MessageState::Headers | MessageState::Data => {
                            (MessageState::Trailers, true)
                        }
                        MessageState::Trailers => return Err(ConnError::FrameUnexpected),
                    };
                    let decoded = self.decode_block(
                        stream_id,
                        block,
                        &mut state.blocked_required_insert_count,
                    )?;
                    let Some(fields) = decoded else {
                        return Ok(());
                    };
                    state.message = next_message;
                    self.events.push_back(Event::PushHeaders {
                        stream_id,
                        push_id,
                        fields,
                        trailing,
                    });
                }
                Frame::Data(data) => {
                    if !matches!(state.message, MessageState::Headers | MessageState::Data) {
                        return Err(ConnError::FrameUnexpected);
                    }
                    state.message = MessageState::Data;
                    let payload_start = n - data.len();
                    let data = state.inbound.take_payload(payload_start..n, n);
                    self.events.push_back(Event::PushData {
                        stream_id,
                        push_id,
                        data,
                    });
                    continue;
                }
                Frame::Unknown { .. } => {}
                _ => return Err(ConnError::FrameUnexpected),
            }
            state.inbound.advance(n);
        }
        if fin {
            if !state.inbound.is_empty() || state.message == MessageState::Idle {
                return Err(ConnError::Protocol);
            }
            self.events
                .push_back(Event::PushFinished { stream_id, push_id });
        }
        Ok(())
    }

    fn validate_goaway_id(&self, id: u64) -> Result<(), ConnError> {
        if self.role == Role::Client && !StreamId::new(id).is_client_bidi() {
            return Err(ConnError::Id);
        }
        if self.peer_goaway_id.is_some_and(|prev| id > prev) {
            return Err(ConnError::Id);
        }
        Ok(())
    }

    fn register_single_stream(
        slot: &mut Option<StreamId>,
        stream_id: StreamId,
        err: ConnError,
    ) -> Result<(), ConnError> {
        if slot.is_some() {
            return Err(err);
        }
        *slot = Some(stream_id);
        Ok(())
    }
}

impl Default for Conn {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnError {
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::Parse(ParseError::FrameTooLarge) => ErrorCode::Frame,
            Self::Parse(ParseError::BadSettings | ParseError::DuplicateSetting) => {
                ErrorCode::Settings
            }
            Self::Qpack(_) => ErrorCode::QpackDecompressionFailed,
            Self::FrameUnexpected => ErrorCode::FrameUnexpected,
            Self::MissingSettings => ErrorCode::MissingSettings,
            Self::ClosedCriticalStream => ErrorCode::ClosedCriticalStream,
            Self::StreamCreation => ErrorCode::StreamCreation,
            Self::Id => ErrorCode::Id,
            Self::QpackEncoderStream => ErrorCode::QpackEncoderStream,
            Self::QpackDecoderStream => ErrorCode::QpackDecoderStream,
            Self::Protocol | Self::Parse(_) => ErrorCode::GeneralProtocol,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Conn, Event, Frame, MessageState, Role, StreamId, StreamState, TYPE_DATA};
    use crate::FrameHeader;

    #[test]
    fn owned_ingest_adopts_a_fragmented_frame_allocation() {
        let stream_id = StreamId::new(0);
        let mut wire = Vec::new();
        Frame::encode(0x21, b"fragment", &mut wire).unwrap();
        let tail = wire.split_off(2);
        let allocation = wire.as_ptr();
        let mut conn = Conn::with_role(Role::Server);

        conn.ingest_stream_owned(stream_id, wire, false).unwrap();

        let state = conn.streams.get(&stream_id).unwrap();
        assert_eq!(state.inbound.as_ptr(), allocation);
        assert_eq!(state.inbound.len(), 2);

        conn.ingest_stream_owned(stream_id, tail, false).unwrap();
        assert!(conn.streams.get(&stream_id).unwrap().inbound.is_empty());
    }

    #[test]
    fn fragmented_data_keeps_the_partial_allocation_through_the_event() {
        let stream_id = StreamId::new(0);
        let mut wire = Vec::with_capacity(128);
        Frame::encode(TYPE_DATA, b"fragmented-body", &mut wire).unwrap();
        let header_len = FrameHeader::parse(&wire).unwrap().header_len;
        let tail = wire.split_off(header_len + 3);
        let payload = wire.as_ptr().wrapping_add(header_len);
        let mut conn = Conn::with_role(Role::Server);
        conn.streams.insert(
            stream_id,
            StreamState {
                message: MessageState::Headers,
                ..StreamState::default()
            },
        );

        conn.ingest_stream_owned(stream_id, wire, false).unwrap();
        assert!(conn.poll_event().is_none());
        conn.ingest_stream_owned(stream_id, tail, false).unwrap();

        let Some(Event::Data { data, .. }) = conn.poll_event() else {
            panic!("DATA event");
        };
        assert_eq!(data.as_slice(), b"fragmented-body");
        assert_eq!(data.as_slice().as_ptr(), payload);
    }

    #[test]
    fn multiple_data_events_share_one_large_receive_allocation() {
        let stream_id = StreamId::new(0);
        let first = vec![b'a'; 300];
        let second = vec![b'b'; 300];
        let mut wire = Vec::new();
        Frame::encode(TYPE_DATA, &first, &mut wire).unwrap();
        let second_frame = wire.len();
        Frame::encode(TYPE_DATA, &second, &mut wire).unwrap();
        let first_header = FrameHeader::parse(&wire).unwrap().header_len;
        let second_header = FrameHeader::parse(&wire[second_frame..])
            .unwrap()
            .header_len;
        let allocation = wire.as_ptr();
        let mut conn = Conn::with_role(Role::Server);
        conn.streams.insert(
            stream_id,
            StreamState {
                message: MessageState::Headers,
                ..StreamState::default()
            },
        );

        conn.ingest_stream_owned(stream_id, wire, false).unwrap();

        let Some(Event::Data { data: first, .. }) = conn.poll_event() else {
            panic!("first DATA event");
        };
        let Some(Event::Data { data: second, .. }) = conn.poll_event() else {
            panic!("second DATA event");
        };
        assert_eq!(
            first.as_slice().as_ptr(),
            allocation.wrapping_add(first_header)
        );
        assert_eq!(
            second.as_slice().as_ptr(),
            allocation.wrapping_add(second_frame + second_header)
        );
    }
}
