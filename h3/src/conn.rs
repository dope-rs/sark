use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::ops::{Deref, Range};

use dope_quic::varint::{Error as VarIntError, VarInt};
use o3::buffer::{Bytes, InlineBytes, Retained, Shared};
use sark_core::http::{Field, VecFieldBlock};

use crate::frame::{
    ErrorCode, Frame, ParseError, STREAM_TYPE_CONTROL, STREAM_TYPE_QPACK_DECODER,
    STREAM_TYPE_QPACK_ENCODER, Settings, TYPE_CANCEL_PUSH, TYPE_DATA, TYPE_GOAWAY, TYPE_HEADERS,
    TYPE_MAX_PUSH_ID, TYPE_PUSH_PROMISE, TYPE_SETTINGS,
};
use crate::payload::Payload;
use crate::qpack::{self, DecodeOutcome, DecoderError, EncodedSection};
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

    fn try_advance(&mut self, n: usize) -> bool {
        if n > self.len() {
            return false;
        }
        match self {
            Self::Unique { bytes, start } => {
                *start += n;
                if *start == bytes.len() {
                    self.clear();
                }
            }
            Self::Shared(bytes) => {
                if !bytes.try_advance(n) {
                    return false;
                }
                if bytes.is_empty() {
                    self.clear();
                }
            }
        }
        true
    }

    fn take_payload(&mut self, range: Range<usize>, frame_len: usize) -> Option<Payload> {
        if range.start > range.end || range.end > frame_len || frame_len > self.as_slice().len() {
            return None;
        }
        match mem::take(self) {
            Self::Unique { bytes, start } if frame_len == bytes.len() - start => Some(
                Payload::from_unique(bytes, start + range.start..start + range.end),
            ),
            Self::Unique { bytes, start } => {
                let mut owner = Shared::from(bytes);
                let Some(payload) = owner.get(start + range.start..start + range.end) else {
                    *self = Self::Shared(owner);
                    return None;
                };
                if !owner.try_advance(start + frame_len) {
                    *self = Self::Shared(owner);
                    return None;
                }
                *self = Self::Shared(owner);
                Some(Payload::from_shared(payload))
            }
            Self::Shared(mut owner) => {
                let Some(payload) = owner.get(range) else {
                    *self = Self::Shared(owner);
                    return None;
                };
                if !owner.try_advance(frame_len) {
                    *self = Self::Shared(owner);
                    return None;
                }
                if !owner.is_empty() {
                    *self = Self::Shared(owner);
                }
                Some(Payload::from_shared(payload))
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
pub enum WritePayload {
    Owned(Vec<u8>),
    Retained(Bytes<Retained>),
}

impl WritePayload {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Retained(bytes) => bytes.as_slice(),
        }
    }
}

pub const INLINE_PREFIX_CAPACITY: usize = o3::buffer::INLINE_BYTES_CAPACITY;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WritePrefix {
    Inline(InlineBytes),
    Owned(Vec<u8>),
}

impl WritePrefix {
    fn with_capacity(capacity: usize) -> Self {
        if capacity <= INLINE_PREFIX_CAPACITY {
            Self::Inline(InlineBytes::new())
        } else {
            Self::Owned(Vec::with_capacity(capacity))
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline(prefix) => prefix.as_slice(),
            Self::Owned(bytes) => bytes,
        }
    }

    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Inline(prefix) => prefix.as_slice().to_vec(),
            Self::Owned(bytes) => bytes,
        }
    }

    fn push(&mut self, byte: u8) {
        match self {
            Self::Inline(prefix) => {
                if prefix.try_push(byte).is_err() {
                    let mut bytes = Vec::with_capacity(INLINE_PREFIX_CAPACITY * 2);
                    bytes.extend_from_slice(prefix.as_slice());
                    bytes.push(byte);
                    *self = Self::Owned(bytes);
                }
            }
            Self::Owned(bytes) => bytes.push(byte),
        }
    }
}

impl Extend<u8> for WritePrefix {
    fn extend<T: IntoIterator<Item = u8>>(&mut self, iter: T) {
        for byte in iter {
            self.push(byte);
        }
    }
}

impl Deref for WritePrefix {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<Vec<u8>> for WritePrefix {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Write {
    pub stream_id: StreamId,
    pub prefix: WritePrefix,
    pub payload: Option<WritePayload>,
    pub fin: bool,
}

impl Write {
    fn single(stream_id: StreamId, prefix: impl Into<WritePrefix>, fin: bool) -> Self {
        Self {
            stream_id,
            prefix: prefix.into(),
            payload: None,
            fin,
        }
    }

    fn segmented(
        stream_id: StreamId,
        prefix: WritePrefix,
        payload: WritePayload,
        fin: bool,
    ) -> Self {
        Self {
            stream_id,
            prefix,
            payload: Some(payload),
            fin,
        }
    }
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
            STREAM_TYPE_CONTROL.encode(&mut bytes);
            Frame::encode(TYPE_SETTINGS, &payload, &mut bytes)?;
            self.writes
                .push_back(Write::single(stream_id, bytes, false));
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

    fn start_uni_stream(
        &mut self,
        stream_id: StreamId,
        stream_type: VarInt,
    ) -> Result<(), ConnError> {
        if stream_id.is_bidi() {
            return Err(ConnError::Protocol);
        }
        let mut bytes = Vec::new();
        stream_type.encode(&mut bytes);
        self.writes
            .push_back(Write::single(stream_id, bytes, false));
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
        let section = self.qpack_encoder.encode_section(fields);
        self.flush_qpack_encoder_stream();
        self.send_field_section(stream_id, TYPE_HEADERS, None, section, fin)
    }

    pub fn send_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Result<(), ConnError> {
        let mut bytes = Vec::new();
        Frame::encode(TYPE_DATA, data, &mut bytes)?;
        self.writes.push_back(Write::single(stream_id, bytes, fin));
        Ok(())
    }

    pub fn send_data_owned(
        &mut self,
        stream_id: StreamId,
        data: Vec<u8>,
        fin: bool,
    ) -> Result<(), ConnError> {
        self.send_frame_payload(stream_id, TYPE_DATA, WritePayload::Owned(data), fin)
    }

    pub fn send_data_retained(
        &mut self,
        stream_id: StreamId,
        data: Bytes<Retained>,
        fin: bool,
    ) -> Result<(), ConnError> {
        self.send_frame_payload(stream_id, TYPE_DATA, WritePayload::Retained(data), fin)
    }

    fn send_frame_payload(
        &mut self,
        stream_id: StreamId,
        kind: VarInt,
        payload: WritePayload,
        fin: bool,
    ) -> Result<(), ConnError> {
        let prefix = Self::frame_prefix(kind, payload.as_slice().len(), 0)?;
        self.writes
            .push_back(Write::segmented(stream_id, prefix, payload, fin));
        Ok(())
    }

    fn send_field_section(
        &mut self,
        stream_id: StreamId,
        kind: VarInt,
        leading_varint: Option<VarInt>,
        section: EncodedSection,
        fin: bool,
    ) -> Result<(), ConnError> {
        let leading_len = leading_varint.map(VarInt::encoded_len).unwrap_or(0);
        let payload_len = leading_len
            .checked_add(section.encoded_len())
            .ok_or(ConnError::Protocol)?;
        let payload_prefix_len = leading_len
            .checked_add(section.prefix_len())
            .ok_or(ConnError::Protocol)?;
        let mut prefix = Self::frame_prefix(kind, payload_len, payload_prefix_len)?;
        if let Some(value) = leading_varint {
            value.encode(&mut prefix);
        }
        section.encode_prefix(&mut prefix);
        let field_lines = section.into_field_lines();
        let write = if field_lines.is_empty() {
            Write::single(stream_id, prefix, fin)
        } else {
            Write::segmented(stream_id, prefix, WritePayload::Owned(field_lines), fin)
        };
        self.writes.push_back(write);
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
        let section = self.qpack_encoder.encode_section(fields);
        self.flush_qpack_encoder_stream();
        let push_id = VarInt::new(push_id).ok_or(ConnError::Protocol)?;
        self.send_field_section(stream_id, TYPE_PUSH_PROMISE, Some(push_id), section, false)
    }

    pub fn send_cancel_push(&mut self, push_id: u64) -> Result<(), ConnError> {
        let stream_id = self.control_stream_id.ok_or(ConnError::Protocol)?;
        let mut bytes = Vec::new();
        Frame::encode_varint(
            TYPE_CANCEL_PUSH,
            VarInt::new(push_id).ok_or(ConnError::Protocol)?,
            &mut bytes,
        )?;
        self.writes
            .push_back(Write::single(stream_id, bytes, false));
        Ok(())
    }

    pub fn send_goaway(&mut self, id: u64) -> Result<(), ConnError> {
        let stream_id = self.control_stream_id.ok_or(ConnError::Protocol)?;
        let mut bytes = Vec::new();
        Frame::encode_varint(
            TYPE_GOAWAY,
            VarInt::new(id).ok_or(ConnError::Protocol)?,
            &mut bytes,
        )?;
        self.writes
            .push_back(Write::single(stream_id, bytes, false));
        Ok(())
    }

    pub fn send_max_push_id(&mut self, push_id: u64) -> Result<(), ConnError> {
        if self.role != Role::Client {
            return Err(ConnError::FrameUnexpected);
        }
        let stream_id = self.control_stream_id.ok_or(ConnError::Protocol)?;
        let mut bytes = Vec::new();
        Frame::encode_varint(
            TYPE_MAX_PUSH_ID,
            VarInt::new(push_id).ok_or(ConnError::Protocol)?,
            &mut bytes,
        )?;
        self.writes
            .push_back(Write::single(stream_id, bytes, false));
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

    fn frame_prefix(
        kind: VarInt,
        payload_len: usize,
        payload_prefix_len: usize,
    ) -> Result<WritePrefix, ConnError> {
        let length = VarInt::from_usize(payload_len).ok_or(ConnError::Protocol)?;
        let capacity = kind
            .encoded_len()
            .checked_add(length.encoded_len())
            .and_then(|capacity| capacity.checked_add(payload_prefix_len))
            .ok_or(ConnError::Protocol)?;
        let mut prefix = WritePrefix::with_capacity(capacity);
        crate::FrameHeader {
            kind,
            length,
            header_len: 0,
        }
        .encode(&mut prefix);
        Ok(prefix)
    }

    fn flush_qpack_encoder_stream(&mut self) {
        let Some(stream_id) = self.qpack_encoder_stream_id else {
            return;
        };
        let bytes = self.qpack_encoder.take_encoder_instructions();
        if bytes.is_empty() {
            return;
        }
        self.writes
            .push_back(Write::single(stream_id, bytes, false));
    }

    fn flush_qpack_decoder_stream(&mut self) {
        let Some(stream_id) = self.qpack_decoder_stream_id else {
            return;
        };
        let bytes = self.qpack_decoder.take_decoder_instructions();
        if bytes.is_empty() {
            return;
        }
        self.writes
            .push_back(Write::single(stream_id, bytes, false));
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

    fn ingest_message_frames<const PUSH: bool>(
        &mut self,
        stream_id: StreamId,
        state: &mut StreamState,
        push_id: u64,
        fin: bool,
    ) -> Result<(), ConnError> {
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
                    self.events.push_back(if PUSH {
                        Event::PushHeaders {
                            stream_id,
                            push_id,
                            fields,
                            trailing,
                        }
                    } else {
                        Event::Headers {
                            stream_id,
                            fields,
                            trailing,
                        }
                    });
                }
                Frame::Data(data) => {
                    if !matches!(state.message, MessageState::Headers | MessageState::Data) {
                        return Err(ConnError::FrameUnexpected);
                    }
                    state.message = MessageState::Data;
                    let payload_start = n - data.len();
                    let data = state
                        .inbound
                        .take_payload(payload_start..n, n)
                        .ok_or(ConnError::Protocol)?;
                    self.events.push_back(if PUSH {
                        Event::PushData {
                            stream_id,
                            push_id,
                            data,
                        }
                    } else {
                        Event::Data { stream_id, data }
                    });
                    continue;
                }
                Frame::Unknown { .. } => {}
                Frame::PushPromise { push_id, block } => {
                    if PUSH || self.role != Role::Client {
                        return Err(ConnError::FrameUnexpected);
                    }
                    let decoded = self.decode_block(
                        stream_id,
                        block,
                        &mut state.blocked_required_insert_count,
                    )?;
                    let Some(fields) = decoded else {
                        return Ok(());
                    };
                    self.events.push_back(Event::PushPromise {
                        stream_id,
                        push_id: push_id.get(),
                        fields,
                    });
                }
                _ => return Err(ConnError::FrameUnexpected),
            }
            if !state.inbound.try_advance(n) {
                return Err(ConnError::Protocol);
            }
        }
        if fin {
            if !state.inbound.is_empty() || state.message == MessageState::Idle {
                return Err(ConnError::Protocol);
            }
            self.events.push_back(if PUSH {
                Event::PushFinished { stream_id, push_id }
            } else {
                Event::Finished { stream_id }
            });
        }
        Ok(())
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
        let fin_received = state.fin_received;
        self.ingest_message_frames::<false>(stream_id, &mut state, 0, fin_received)?;
        if state.blocked_required_insert_count.is_some() {
            self.streams.insert(stream_id, state);
            return Ok(());
        }
        if state.fin_received {
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
            let (stream_type, type_len) = match VarInt::decode(&state.inbound) {
                Ok(v) => v,
                Err(VarIntError::Underflow) => {
                    self.streams.insert(stream_id, state);
                    return Ok(());
                }
                Err(_) => return Err(ConnError::Protocol),
            };
            let stream_type = UniStreamType::from_wire(stream_type);
            self.register_uni_stream(stream_id, stream_type)?;
            state.uni_type = Some(stream_type);
            if !state.inbound.try_advance(type_len) {
                return Err(ConnError::Protocol);
            }
        }

        let fin_received = state.fin_received;
        let Some(uni_type) = state.uni_type else {
            return Err(ConnError::Protocol);
        };
        match uni_type {
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
                    if !state.inbound.try_advance(consumed) {
                        return Err(ConnError::Protocol);
                    }
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
                if consumed > 0 && !state.inbound.try_advance(consumed) {
                    return Err(ConnError::Protocol);
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
                    self.events.push_back(Event::CancelPush {
                        push_id: push_id.get(),
                    });
                }
                Frame::GoAway { id } => {
                    let id = id.get();
                    self.validate_goaway_id(id)?;
                    self.peer_goaway_id = Some(id);
                    self.events.push_back(Event::GoAway { id });
                }
                Frame::MaxPushId { push_id } => {
                    let push_id = push_id.get();
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
            if !state.inbound.try_advance(n) {
                return Err(ConnError::Protocol);
            }
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
            let (push_id, n) = match VarInt::decode(&state.inbound) {
                Ok(v) => v,
                Err(VarIntError::Underflow) => return Ok(()),
                Err(_) => return Err(ConnError::Protocol),
            };
            state.push_id = Some(push_id.get());
            if !state.inbound.try_advance(n) {
                return Err(ConnError::Protocol);
            }
        }
        let Some(push_id) = state.push_id else {
            return Err(ConnError::Protocol);
        };
        self.ingest_message_frames::<true>(stream_id, state, push_id, fin)?;
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
