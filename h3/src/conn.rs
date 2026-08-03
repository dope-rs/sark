use std::alloc::Layout;
use std::error;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, Range};

use dope_quic::varint::{self, VarInt};
use o3::buffer::{Bytes, InlineBytes, PoolLayoutError, Retained, Shared, SharedPoolPlan};
use o3::collections::{FixedHashTable, FixedHashTablePlan, FixedQueue};
use sark_core::http::{DecodedFieldBlock, Field, HeadPlan, RawHeadPlan};

use crate::frame::{
    ErrorCode, Frame, ParseError, STREAM_TYPE_CONTROL, STREAM_TYPE_QPACK_DECODER,
    STREAM_TYPE_QPACK_ENCODER, Settings, TYPE_CANCEL_PUSH, TYPE_DATA, TYPE_GOAWAY, TYPE_HEADERS,
    TYPE_MAX_PUSH_ID, TYPE_PUSH_PROMISE, TYPE_SETTINGS,
};
use crate::payload::Payload;
use crate::qpack::{self, DecoderError, EncodedSection, MessageDecodeOutcome};
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
    Overload,
    Message,
    Protocol,
}

impl From<ParseError> for ConnError {
    fn from(err: ParseError) -> Self {
        Self::Parse(err)
    }
}

impl From<DecoderError> for ConnError {
    fn from(err: DecoderError) -> Self {
        if err == DecoderError::Capacity {
            Self::Overload
        } else {
            Self::Qpack(err)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event<S = (), B = DecodedFieldBlock> {
    Settings(Settings),
    Headers {
        stream_id: StreamId,
        fields: B,
        selection: S,
        section: HeaderSection,
    },
    Data {
        stream_id: StreamId,
        data: Payload,
    },
    PushPromise {
        stream_id: StreamId,
        push_id: u64,
        fields: B,
        selection: S,
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
        fields: B,
        selection: S,
        section: HeaderSection,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HeaderSection {
    Initial = 0,
    Trailing = 1,
    InitialEnd = 2,
    TrailingEnd = 3,
}

impl HeaderSection {
    const fn new(trailing: bool, end_stream: bool) -> Self {
        match (trailing, end_stream) {
            (false, false) => Self::Initial,
            (true, false) => Self::Trailing,
            (false, true) => Self::InitialEnd,
            (true, true) => Self::TrailingEnd,
        }
    }

    pub const fn trailing(self) -> bool {
        self as u8 & 1 != 0
    }

    pub const fn end_stream(self) -> bool {
        self as u8 & 2 != 0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Client,
    Server,
}

const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_FIELD_SECTION_SIZE: usize = 64 * 1024;
const DEFAULT_STREAM_CAPACITY: usize = 1024;
const DEFAULT_EVENT_CAPACITY: usize = 1024;
const DEFAULT_WRITE_CAPACITY: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub local_settings: Settings,
    pub max_frame_size: usize,
    pub stream_capacity: usize,
    pub event_capacity: usize,
    pub write_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            local_settings: Settings::default(),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            stream_capacity: DEFAULT_STREAM_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            write_capacity: DEFAULT_WRITE_CAPACITY,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    ZeroCapacity(&'static str),
    CapacityOverflow(&'static str),
    InvalidSetting(&'static str),
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
            Self::CapacityOverflow(name) => write!(formatter, "{name} capacity overflows layout"),
            Self::InvalidSetting(name) => write!(formatter, "{name} exceeds the HTTP/3 limit"),
            Self::Pool(error) => error.fmt(formatter),
        }
    }
}

impl error::Error for ConfigError {}

#[derive(Clone, Debug)]
pub struct ValidatedConfig {
    local_settings: Settings,
    max_frame_size: usize,
    stream_table: FixedHashTablePlan<StreamEntry>,
    event_capacity: usize,
    write_capacity: usize,
    max_field_section_size: usize,
    max_qpack_table_capacity: usize,
    field_pool: SharedPoolPlan,
}

impl ValidatedConfig {
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let Config {
            local_settings,
            max_frame_size,
            stream_capacity,
            event_capacity,
            write_capacity,
        } = config;
        for (name, capacity) in [
            ("frame", max_frame_size),
            ("stream", stream_capacity),
            ("event", event_capacity),
            ("write", write_capacity),
        ] {
            if capacity == 0 {
                return Err(ConfigError::ZeroCapacity(name));
            }
        }
        for (name, value) in [
            (
                "qpack table capacity",
                local_settings.qpack_max_table_capacity,
            ),
            (
                "field section size",
                local_settings.max_field_section_size.unwrap_or(0),
            ),
            (
                "qpack blocked streams",
                local_settings.qpack_blocked_streams,
            ),
        ] {
            if VarInt::new(value).is_none() {
                return Err(ConfigError::InvalidSetting(name));
            }
        }
        let max_field_section_size = local_settings
            .max_field_section_size
            .map_or(Ok(DEFAULT_MAX_FIELD_SECTION_SIZE), usize::try_from)
            .map_err(|_| ConfigError::CapacityOverflow("field section"))?;
        let max_qpack_table_capacity = usize::try_from(local_settings.qpack_max_table_capacity)
            .map_err(|_| ConfigError::CapacityOverflow("qpack table"))?;
        let stream_table = FixedHashTablePlan::new(stream_capacity)
            .ok_or(ConfigError::CapacityOverflow("stream"))?;
        if Layout::array::<StreamId>(stream_capacity).is_err() {
            return Err(ConfigError::CapacityOverflow("stream"));
        }
        if Layout::array::<Event>(event_capacity).is_err() {
            return Err(ConfigError::CapacityOverflow("event"));
        }
        if Layout::array::<Write>(write_capacity).is_err() {
            return Err(ConfigError::CapacityOverflow("write"));
        }
        Ok(Self {
            local_settings,
            max_frame_size,
            stream_table,
            event_capacity,
            write_capacity,
            max_field_section_size,
            max_qpack_table_capacity,
            field_pool: SharedPoolPlan::new(
                qpack::Decoder::DEFAULT_FIELD_BLOCKS,
                max_field_section_size.max(1),
            )?,
        })
    }

    pub fn local_settings(&self) -> &Settings {
        &self.local_settings
    }
}

impl Default for ValidatedConfig {
    fn default() -> Self {
        const {
            assert!(DEFAULT_MAX_FRAME_SIZE != 0);
            assert!(DEFAULT_STREAM_CAPACITY != 0);
            assert!(DEFAULT_EVENT_CAPACITY != 0);
            assert!(DEFAULT_WRITE_CAPACITY != 0);
        }
        Self {
            local_settings: Settings::default(),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            stream_table: FixedHashTablePlan::new(DEFAULT_STREAM_CAPACITY)
                .expect("default stream table layout"),
            event_capacity: DEFAULT_EVENT_CAPACITY,
            write_capacity: DEFAULT_WRITE_CAPACITY,
            max_field_section_size: DEFAULT_MAX_FIELD_SECTION_SIZE,
            max_qpack_table_capacity: 0,
            field_pool: SharedPoolPlan::fixed::<
                { qpack::Decoder::DEFAULT_FIELD_BLOCKS },
                DEFAULT_MAX_FIELD_SECTION_SIZE,
            >(),
        }
    }
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
struct StreamEntry {
    id: StreamId,
    state: StreamState,
}

struct StreamTable {
    entries: FixedHashTable<StreamEntry>,
}

impl StreamTable {
    fn from_plan(plan: FixedHashTablePlan<StreamEntry>) -> Self {
        Self {
            entries: FixedHashTable::from_plan(plan),
        }
    }

    fn hash(id: StreamId) -> u64 {
        id.index()
    }

    fn remove(&mut self, id: StreamId) -> Option<StreamState> {
        if self.entries.is_empty() {
            return None;
        }
        self.entries
            .remove(Self::hash(id), |entry| entry.id == id)
            .map(|entry| entry.state)
    }

    fn insert(&mut self, id: StreamId, state: StreamState) -> Result<(), ConnError> {
        self.entries
            .try_insert(Self::hash(id), StreamEntry { id, state }, |entry| {
                entry.id == id
            })
            .map_err(|_| ConnError::Overload)
    }

    fn values(&self) -> impl Iterator<Item = &StreamEntry> {
        self.entries.values()
    }
}

#[derive(Default)]
struct CriticalStreams {
    control: Option<Box<CriticalStream>>,
    qpack_encoder: Option<Box<CriticalStream>>,
    qpack_decoder: Option<Box<CriticalStream>>,
}

struct CriticalStream {
    id: StreamId,
    state: Option<StreamState>,
}

impl CriticalStreams {
    fn slot_mut(&mut self, stream_type: UniStreamType) -> Option<&mut Option<Box<CriticalStream>>> {
        match stream_type {
            UniStreamType::Control => Some(&mut self.control),
            UniStreamType::QpackEncoder => Some(&mut self.qpack_encoder),
            UniStreamType::QpackDecoder => Some(&mut self.qpack_decoder),
            UniStreamType::Push | UniStreamType::Unknown(_) => None,
        }
    }

    fn register(&mut self, stream_type: UniStreamType, id: StreamId) -> Result<(), ConnError> {
        let Some(slot) = self.slot_mut(stream_type) else {
            return Ok(());
        };
        if slot.is_some() {
            return Err(ConnError::StreamCreation);
        }
        *slot = Some(Box::new(CriticalStream { id, state: None }));
        Ok(())
    }

    fn take(&mut self, id: StreamId) -> Option<StreamState> {
        for slot in [
            &mut self.control,
            &mut self.qpack_encoder,
            &mut self.qpack_decoder,
        ] {
            if let Some(stream) = slot
                && stream.id == id
            {
                return stream.state.take();
            }
        }
        None
    }

    fn put(
        &mut self,
        stream_type: UniStreamType,
        id: StreamId,
        state: StreamState,
    ) -> Result<(), ConnError> {
        let slot = self
            .slot_mut(stream_type)
            .and_then(Option::as_mut)
            .filter(|stream| stream.id == id)
            .ok_or(ConnError::Protocol)?;
        if slot.state.replace(state).is_some() {
            return Err(ConnError::Protocol);
        }
        Ok(())
    }

    fn contains(&self, id: StreamId) -> bool {
        [&self.control, &self.qpack_encoder, &self.qpack_decoder]
            .into_iter()
            .flatten()
            .any(|stream| stream.id == id)
    }
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

pub struct Conn<P: HeadPlan = RawHeadPlan> {
    role: Role,
    local_settings: Settings,
    peer_settings: Option<Settings>,
    max_frame_size: usize,
    qpack_encoder: qpack::Encoder,
    qpack_decoder: qpack::Decoder,
    streams: StreamTable,
    critical_streams: CriticalStreams,
    retry_streams: FixedQueue<StreamId>,
    events: FixedQueue<Event<P::Selection, P::Block>>,
    writes: FixedQueue<Write>,
    control_stream_id: Option<StreamId>,
    qpack_encoder_stream_id: Option<StreamId>,
    qpack_decoder_stream_id: Option<StreamId>,
    peer_goaway_id: Option<u64>,
    max_push_id: Option<u64>,
    head_plan: PhantomData<fn() -> P>,
}

impl Conn {
    pub fn new() -> Self {
        Self::with_role(Role::Client)
    }

    pub fn with_role(role: Role) -> Self {
        Self::from_config(role, ValidatedConfig::default())
    }

    pub fn with_config(role: Role, config: Config) -> Result<Self, ConfigError> {
        Ok(Self::from_config(role, ValidatedConfig::new(config)?))
    }

    pub fn from_config(role: Role, config: ValidatedConfig) -> Self {
        Self::from_config_with_plan(role, config)
    }
}

impl<P: HeadPlan> Conn<P> {
    pub fn from_config_with_plan(role: Role, config: ValidatedConfig) -> Self {
        let ValidatedConfig {
            local_settings,
            max_frame_size,
            stream_table,
            event_capacity,
            write_capacity,
            max_field_section_size,
            max_qpack_table_capacity,
            field_pool,
        } = config;
        Self {
            role,
            local_settings,
            peer_settings: None,
            max_frame_size,
            qpack_encoder: qpack::Encoder::new(),
            qpack_decoder: qpack::Decoder::with_pool_plan(
                max_field_section_size,
                max_qpack_table_capacity,
                field_pool,
            ),
            retry_streams: FixedQueue::with_capacity(stream_table.capacity()),
            streams: StreamTable::from_plan(stream_table),
            critical_streams: CriticalStreams::default(),
            events: FixedQueue::with_capacity(event_capacity),
            writes: FixedQueue::with_capacity(write_capacity),
            control_stream_id: None,
            qpack_encoder_stream_id: None,
            qpack_decoder_stream_id: None,
            peer_goaway_id: None,
            max_push_id: None,
            head_plan: PhantomData,
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
            Self::enqueue(&mut self.writes, Write::single(stream_id, bytes, false))?;
            return Ok(());
        }
        Err(ConnError::Protocol)
    }

    pub fn start_qpack_encoder_stream(&mut self, stream_id: StreamId) -> Result<(), ConnError> {
        if self.qpack_encoder_stream_id.is_some() {
            return Err(ConnError::Protocol);
        }
        self.start_uni_stream(stream_id, STREAM_TYPE_QPACK_ENCODER)?;
        self.qpack_encoder_stream_id = Some(stream_id);
        Ok(())
    }

    pub fn start_qpack_decoder_stream(&mut self, stream_id: StreamId) -> Result<(), ConnError> {
        if self.qpack_decoder_stream_id.is_some() {
            return Err(ConnError::Protocol);
        }
        self.start_uni_stream(stream_id, STREAM_TYPE_QPACK_DECODER)?;
        self.qpack_decoder_stream_id = Some(stream_id);
        Ok(())
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
        Self::enqueue(&mut self.writes, Write::single(stream_id, bytes, false))
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
        self.flush_qpack_encoder_stream()?;
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
        Self::enqueue(&mut self.writes, Write::single(stream_id, bytes, fin))
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
        Self::enqueue(
            &mut self.writes,
            Write::segmented(stream_id, prefix, payload, fin),
        )
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
        let write = if field_lines.len() <= INLINE_PREFIX_CAPACITY.saturating_sub(prefix.len()) {
            prefix.extend(field_lines.iter().copied());
            self.qpack_encoder.recycle_section(field_lines);
            Write::single(stream_id, prefix, fin)
        } else {
            Write::segmented(stream_id, prefix, WritePayload::Owned(field_lines), fin)
        };
        Self::enqueue(&mut self.writes, write)
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
        self.flush_qpack_encoder_stream()?;
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
        Self::enqueue(&mut self.writes, Write::single(stream_id, bytes, false))
    }

    pub fn send_goaway(&mut self, id: u64) -> Result<(), ConnError> {
        let stream_id = self.control_stream_id.ok_or(ConnError::Protocol)?;
        let mut bytes = Vec::new();
        Frame::encode_varint(
            TYPE_GOAWAY,
            VarInt::new(id).ok_or(ConnError::Protocol)?,
            &mut bytes,
        )?;
        Self::enqueue(&mut self.writes, Write::single(stream_id, bytes, false))
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
        Self::enqueue(&mut self.writes, Write::single(stream_id, bytes, false))
    }

    pub fn poll_event(&mut self) -> Option<Event<P::Selection, P::Block>> {
        self.events.pop_front()
    }

    pub fn poll_write(&mut self) -> Option<Write> {
        self.writes.pop_front()
    }

    pub fn peer_settings(&self) -> Option<&Settings> {
        self.peer_settings.as_ref()
    }

    pub fn set_qpack_encoder_capacity(&mut self, capacity: usize) -> Result<(), ConnError> {
        if self.qpack_encoder_stream_id.is_none() {
            return Err(ConnError::Protocol);
        }
        self.qpack_encoder.set_dynamic_capacity(capacity)?;
        self.flush_qpack_encoder_stream()
    }

    pub fn ingest_reset(&mut self, stream_id: StreamId, error_code: u64) -> Result<(), ConnError> {
        if self.critical_streams.contains(stream_id) {
            return Err(ConnError::ClosedCriticalStream);
        }
        self.streams.remove(stream_id);
        Self::enqueue(
            &mut self.events,
            Event::Reset {
                stream_id,
                error_code,
            },
        )
    }

    pub fn ingest_stopped(
        &mut self,
        stream_id: StreamId,
        error_code: u64,
    ) -> Result<(), ConnError> {
        Self::enqueue(
            &mut self.events,
            Event::Stopped {
                stream_id,
                error_code,
            },
        )
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

    fn enqueue<T>(queue: &mut FixedQueue<T>, value: T) -> Result<(), ConnError> {
        queue.push_back(value).map_err(|_| ConnError::Overload)
    }

    fn flush_qpack_encoder_stream(&mut self) -> Result<(), ConnError> {
        let Some(stream_id) = self.qpack_encoder_stream_id else {
            return Ok(());
        };
        if !self.qpack_encoder.has_encoder_instructions() {
            return Ok(());
        }
        let slot = self.writes.vacant_entry().ok_or(ConnError::Overload)?;
        let bytes = self.qpack_encoder.take_encoder_instructions();
        slot.push_back(Write::single(stream_id, bytes, false));
        Ok(())
    }

    fn flush_qpack_decoder_stream(&mut self) -> Result<(), ConnError> {
        let Some(stream_id) = self.qpack_decoder_stream_id else {
            return Ok(());
        };
        if !self.qpack_decoder.has_decoder_instructions() {
            return Ok(());
        }
        let slot = self.writes.vacant_entry().ok_or(ConnError::Overload)?;
        let bytes = self.qpack_decoder.take_decoder_instructions();
        slot.push_back(Write::single(stream_id, bytes, false));
        Ok(())
    }

    fn decode_block(
        &mut self,
        stream_id: StreamId,
        block: &[u8],
        blocked: &mut Option<u64>,
        request: bool,
        trailing: bool,
    ) -> Result<Option<(P::Block, P::Selection)>, ConnError> {
        let (outcome, valid) = self
            .qpack_decoder
            .decode_message_or_blocked::<P>(block, request, trailing)?;
        if !valid {
            return Err(ConnError::Message);
        }
        match outcome {
            MessageDecodeOutcome::Ready {
                fields,
                selection,
                required_insert_count,
            } => {
                *blocked = None;
                if required_insert_count > 0 {
                    self.qpack_decoder.acknowledge_section(stream_id.0);
                    self.flush_qpack_decoder_stream()?;
                }
                Ok(Some((fields, selection)))
            }
            MessageDecodeOutcome::Blocked {
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
                        !PUSH && self.role == Role::Server,
                        trailing,
                    )?;
                    let Some((fields, selection)) = decoded else {
                        return Ok(());
                    };
                    state.message = next_message;
                    Self::enqueue(
                        &mut self.events,
                        if PUSH {
                            Event::PushHeaders {
                                stream_id,
                                push_id,
                                fields,
                                selection,
                                section: HeaderSection::new(trailing, fin && n == rest.len()),
                            }
                        } else {
                            Event::Headers {
                                stream_id,
                                fields,
                                selection,
                                section: HeaderSection::new(trailing, fin && n == rest.len()),
                            }
                        },
                    )?;
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
                    Self::enqueue(
                        &mut self.events,
                        if PUSH {
                            Event::PushData {
                                stream_id,
                                push_id,
                                data,
                            }
                        } else {
                            Event::Data { stream_id, data }
                        },
                    )?;
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
                        true,
                        false,
                    )?;
                    let Some((fields, selection)) = decoded else {
                        return Ok(());
                    };
                    Self::enqueue(
                        &mut self.events,
                        Event::PushPromise {
                            stream_id,
                            push_id: push_id.get(),
                            fields,
                            selection,
                        },
                    )?;
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
            Self::enqueue(
                &mut self.events,
                if PUSH {
                    Event::PushFinished { stream_id, push_id }
                } else {
                    Event::Finished { stream_id }
                },
            )?;
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
        let mut state = self.streams.remove(stream_id).unwrap_or_default();
        append(&mut state.inbound);
        state.fin_received |= fin;
        let fin_received = state.fin_received;
        self.ingest_message_frames::<false>(stream_id, &mut state, 0, fin_received)?;
        if state.blocked_required_insert_count.is_some() {
            self.streams.insert(stream_id, state)?;
            return Ok(());
        }
        if !state.fin_received {
            self.streams.insert(stream_id, state)?;
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
        let mut state = self
            .critical_streams
            .take(stream_id)
            .or_else(|| self.streams.remove(stream_id))
            .unwrap_or_default();
        append(&mut state.inbound);
        state.fin_received |= fin;

        if state.uni_type.is_none() {
            let (stream_type, type_len) = match VarInt::decode(&state.inbound) {
                Ok(v) => v,
                Err(varint::Error::Underflow) => {
                    self.streams.insert(stream_id, state)?;
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
                    self.flush_qpack_decoder_stream()?;
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
        if !state.fin_received {
            if uni_type.is_critical() {
                self.critical_streams.put(uni_type, stream_id, state)?;
            } else {
                self.streams.insert(stream_id, state)?;
            }
        }
        Ok(())
    }

    fn register_uni_stream(
        &mut self,
        stream_id: StreamId,
        stream_type: UniStreamType,
    ) -> Result<(), ConnError> {
        match stream_type {
            UniStreamType::Push if self.role != Role::Client || !stream_id.is_server_uni() => {
                Err(ConnError::StreamCreation)
            }
            UniStreamType::Control | UniStreamType::QpackEncoder | UniStreamType::QpackDecoder => {
                self.critical_streams.register(stream_type, stream_id)
            }
            UniStreamType::Push | UniStreamType::Unknown(_) => Ok(()),
        }
    }

    fn retry_blocked_streams(&mut self) -> Result<(), ConnError> {
        let insert_count = self.qpack_decoder.dynamic_insert_count();
        debug_assert!(self.retry_streams.is_empty());
        for entry in self.streams.values() {
            let Some(required) = entry.state.blocked_required_insert_count else {
                continue;
            };
            if required <= insert_count {
                Self::enqueue(&mut self.retry_streams, entry.id)?;
            }
        }
        while let Some(stream_id) = self.retry_streams.pop_front() {
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
            if self.role == Role::Client
                && matches!(&frame, Frame::CancelPush { .. } | Frame::MaxPushId { .. })
            {
                return Err(ConnError::FrameUnexpected);
            }
            match frame {
                Frame::Settings(settings) => {
                    if state.saw_settings || self.peer_settings.is_some() {
                        return Err(ConnError::Protocol);
                    }
                    state.saw_settings = true;
                    self.qpack_encoder
                        .set_max_dynamic_capacity(settings.qpack_max_table_capacity);
                    self.peer_settings = Some(settings.clone());
                    self.qpack_encoder
                        .set_max_blocked_streams(settings.qpack_blocked_streams);
                    Self::enqueue(&mut self.events, Event::Settings(settings))?;
                }
                Frame::CancelPush { push_id } => {
                    Self::enqueue(
                        &mut self.events,
                        Event::CancelPush {
                            push_id: push_id.get(),
                        },
                    )?;
                }
                Frame::GoAway { id } => {
                    let id = id.get();
                    self.validate_goaway_id(id)?;
                    self.peer_goaway_id = Some(id);
                    Self::enqueue(&mut self.events, Event::GoAway { id })?;
                }
                Frame::MaxPushId { push_id } => {
                    let push_id = push_id.get();
                    if self.max_push_id.is_some_and(|prev| push_id < prev) {
                        return Err(ConnError::Id);
                    }
                    self.max_push_id = Some(push_id);
                    Self::enqueue(&mut self.events, Event::MaxPushId { push_id })?;
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
                Err(varint::Error::Underflow) => return Ok(()),
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
}

impl<P: HeadPlan> Default for Conn<P> {
    fn default() -> Self {
        Self::from_config_with_plan(Role::Client, ValidatedConfig::default())
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
            Self::Overload => ErrorCode::ExcessiveLoad,
            Self::Message => ErrorCode::Message,
            Self::Protocol | Self::Parse(_) => ErrorCode::GeneralProtocol,
        }
    }
}
