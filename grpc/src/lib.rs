pub use o3;
use o3::buffer;
use sark_http::field;

pub mod client;
pub mod details;
pub mod encode;
pub mod message;
pub mod metadata;
pub mod protocol;
pub mod server;
mod status;

pub use status::Status;

pub trait Codec {
    type Encode;
    type Decode<'a, C: buffer::raw::Segment>
    where
        C: 'a;
    type Owned<C: buffer::raw::Segment>;

    fn encode<S: encode::Sink>(
        &mut self,
        item: &Self::Encode,
        sink: S,
    ) -> Result<S::Output, Status>;

    fn decode<'a, C: buffer::raw::Segment + 'a>(
        &mut self,
        input: message::View<'a, C>,
    ) -> Result<Self::Decode<'a, C>, Status>;

    fn decode_frame<C: buffer::raw::Segment>(
        &mut self,
        frame: message::Frame<C>,
    ) -> Result<Self::Owned<C>, Status>;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    pub const fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => return None,
        })
    }

    pub const fn into_u8(self) -> u8 {
        self as u8
    }

    pub(crate) fn parse_span(input: record::Span<'_>) -> Option<Self> {
        if input.is_empty() || input.len() > 2 {
            return None;
        }
        let mut value = 0u8;
        for byte in input.iter().copied() {
            if !byte.is_ascii_digit() {
                return None;
            }
            value = value * 10 + (byte - b'0');
        }
        Self::from_u8(value)
    }

    pub(crate) const fn as_field(&self) -> field::Value<&[u8]> {
        use sark_http::field::Value;

        const VALUES: [Value<&[u8]>; 17] = [
            field::literal!(Value::try_bytes(b"0")),
            field::literal!(Value::try_bytes(b"1")),
            field::literal!(Value::try_bytes(b"2")),
            field::literal!(Value::try_bytes(b"3")),
            field::literal!(Value::try_bytes(b"4")),
            field::literal!(Value::try_bytes(b"5")),
            field::literal!(Value::try_bytes(b"6")),
            field::literal!(Value::try_bytes(b"7")),
            field::literal!(Value::try_bytes(b"8")),
            field::literal!(Value::try_bytes(b"9")),
            field::literal!(Value::try_bytes(b"10")),
            field::literal!(Value::try_bytes(b"11")),
            field::literal!(Value::try_bytes(b"12")),
            field::literal!(Value::try_bytes(b"13")),
            field::literal!(Value::try_bytes(b"14")),
            field::literal!(Value::try_bytes(b"15")),
            field::literal!(Value::try_bytes(b"16")),
        ];
        VALUES[*self as usize]
    }
}
use sark_http::field::metadata::map::record;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    None,
    ConnectionDraining,
    ShutdownPending,
    ShutdownFailed,
    BidiSendConsumed,
    BidiConsumed,
    Compression,
    DataUnknown,
    EventUnknown,
    DetailsLength,
    HeaderCapacity,
    HeaderBounds,
    IngressAccounting,
    MessageLarge,
    PathDetached,
    EarlyStatus,
    FieldSection,
    Path,
    CallState,
    StatusMissing,
    PendingResponseCapacity,
    ReplyInactive,
    ReplyUnknown,
    ReplyFailed,
    RequestUnknown,
    RetainedCapacity,
    RequestOwners,
    RequestConsumed,
    RequestClosed,
    ResetUnknown,
    ResponseCancelled,
    TerminalUnknown,
    TrailersWithoutEnd,
    UnaryConsumed,
    UnaryResponseMany,
    UnaryResponse,
    LiveMessage,
    LiveMessages,
    PendingRequestLarge,
    PendingRequests,
    ResponsePerTurn,
    StaleStream,
    OutputComplete,
    TrailingMetadata,
    MetadataCommitted,
    FieldCapacity,
}
