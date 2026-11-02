use std::{convert::Infallible, num::NonZero, ops};

use dope::core::driver::schedule::clock;
use o3::{
    buffer::{pool, storage, write::consume},
    depot::{
        arena::slab::{self, key},
        capacity, heap,
        queue::pool::Missing,
    },
};
use prost::encoding::{self, DecodeContext, int32};
use sark_core::{
    deadline,
    manifold::{
        client::{domain, event},
        schedule,
    },
};
use sark_h2::{conn, stream};
use sark_http::{
    field::{
        base64,
        metadata::{lower::plain, map, packed::prefix},
        text::percent::{self, decoder},
    },
    payload::response::status,
};

use crate::{
    Reason, Status,
    client::receive::claims,
    message, metadata,
    protocol::{
        request::timeout,
        section::{self, seal},
    },
    server::{calls, runtime::schedule::start},
    status::repr::Repr,
};

#[derive(Debug)]
pub enum RequestError {
    Connection(conn::Error),
    Progress(consume::Error),
}

#[derive(Debug)]
pub enum Buffered {
    Cardinality {
        actual: usize,
    },
    Overflow {
        actual: NonZero<usize>,
    },
    Limit {
        actual: NonZero<usize>,
        limit: usize,
    },
    Full(capacity::Shortage),
}

impl Buffered {
    pub(crate) fn code(&self) -> crate::Code {
        match self {
            Self::Cardinality { .. } => crate::Code::InvalidArgument,
            Self::Limit { .. } | Self::Full(_) => crate::Code::ResourceExhausted,
            Self::Overflow { .. } => crate::Code::Internal,
        }
    }
}

#[derive(Debug)]
pub enum Chunks {
    Closed,
    Full(capacity::Shortage),
    Grow(Credit),
    Shrink(Credit),
}

impl Chunks {
    pub(crate) fn code(&self) -> crate::Code {
        match self {
            Self::Full(_)
            | Self::Grow(Credit::Limit { .. })
            | Self::Shrink(Credit::Limit { .. }) => crate::Code::ResourceExhausted,
            Self::Closed
            | Self::Grow(Credit::Overflow { .. } | Credit::Underflow { .. })
            | Self::Shrink(Credit::Overflow { .. } | Credit::Underflow { .. }) => {
                crate::Code::Internal
            }
        }
    }
}

/// Borrowed local diagnostics. The status continues to own every failure cause.
#[derive(Debug)]
pub enum Diagnostic<'a> {
    Binary {
        field: Binary,
        error: &'a base64::Error,
    },
    RichStatus(&'a RichStatus),
    SectionCapacity,
    SectionPrefix {
        actual: usize,
        required: usize,
    },
    SectionStored(Fault),
    FoldPrefix(&'a prefix::Error),
    TextBounds {
        capacity: usize,
        source: &'a ops::Range<usize>,
        destination: usize,
    },
    Local {
        code: crate::Code,
        reason: Reason,
    },
    HttpStatus(status::Code),
    Length {
        reason: Reason,
        actual: usize,
    },
    Capacity {
        reason: Reason,
        error: &'a consume::Error,
    },
    Budget(Budget),
    State(&'a Mismatch),
    Credit {
        reason: Reason,
        error: &'a Credit,
    },
    Claim(Claim<'a>),
    Buffered(&'a Buffered),
    Chunks(&'a Chunks),
    Metadata(&'a metadata::Error),
    Message(&'a message::Error),
    Decode(&'a prost::DecodeError),
    Alloc(&'a storage::AllocError),
    Pool(&'a pool::CreateError),
    Lane(&'a Missing),
    Fields(&'a plain::Error),
    Pack(&'a map::Error),
    Connection(&'a conn::Error),
    Drive(&'a domain::Failure<conn::Error, stream::Key>),
    /// Preserves the output failure and any failure while resetting it.
    /// Progress counts refer to the buffer checked by that operation.
    RequestProgress(&'a domain::Failure<RequestError, stream::Key>),
    Timeout(&'a timeout::Error),
    Clock(&'a clock::Range),
    DeadlineBook(&'a heap::Error),
    Deadline(&'a deadline::Error<stream::Key>),
    Start(&'a start::Error),
    Reset(&'a event::Reset),
    TextInput {
        actual: usize,
    },
    TextEscape {
        current: usize,
    },
    CallCapacity {
        active: usize,
        limit: usize,
    },
    CallOccupied,
    Insert {
        cause: &'a schedule::Insert<stream::Key, ()>,
        reset: &'a Result<(), stream::Key>,
    },
    Wire {
        code: crate::Code,
        message: Option<&'a str>,
        details: Option<&'a [u8]>,
        metadata: metadata::View<'a>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Budget {
    Bytes { buffered: usize, released: usize },
    Messages { buffered: usize, released: usize },
}

#[derive(Debug)]
pub enum Action {
    Completion,
    Stream,
    Event,
    Link,
}

#[derive(Debug)]
pub enum Claim<'a> {
    Duplicate(usize),
    Ready(usize),
    Key {
        action: Action,
        index: u32,
        generation: NonZero<u32>,
        observation: key::Observation<{ u32::MAX }, &'a Infallible, &'a ()>,
    },
    Closed {
        index: u32,
        generation: NonZero<u32>,
    },
    UnaryCapacity(slab::Vacancy),
    StreamCapacity(slab::Vacancy),
    ReadyCapacity(slab::Vacancy),
    EventCapacity(slab::Vacancy),
}

impl<'a> From<&'a Status> for Diagnostic<'a> {
    fn from(status: &'a Status) -> Self {
        match &status.repr {
            Repr::RichStatus(error) => Self::RichStatus(error),
            Repr::Local { code, reason } => Self::Local {
                code: *code,
                reason: *reason,
            },
            Repr::HttpStatus(code) => Self::HttpStatus(*code),
            Repr::Length { reason, actual } => Self::Length {
                reason: *reason,
                actual: *actual,
            },
            Repr::Capacity { reason, error } => Self::Capacity {
                reason: *reason,
                error,
            },
            Repr::Budget(error) => Self::Budget(*error),
            Repr::State(error) => Self::State(error),
            Repr::Credit { reason, error } => Self::Credit {
                reason: *reason,
                error,
            },
            Repr::Claim(error) => Self::Claim(error.into()),
            Repr::Section(error) => match error {
                seal::Error::Fold(section::Failure::Binary { field, error }) => Self::Binary {
                    field: *field,
                    error,
                },
                seal::Error::Fold(section::Failure::Capacity) => Self::SectionCapacity,
                seal::Error::Fold(section::Failure::Timeout(error)) => Self::Timeout(error),
                seal::Error::Fold(section::Failure::Prefix(error)) => Self::FoldPrefix(error),
                seal::Error::Fold(section::Failure::Message(error)) => match error {
                    decoder::Error::Bounds {
                        capacity,
                        source,
                        destination: dst,
                    } => Self::TextBounds {
                        capacity: *capacity,
                        source,
                        destination: *dst,
                    },
                },
                seal::Error::Prefix { actual, required } => Self::SectionPrefix {
                    actual: *actual,
                    required: *required,
                },
                seal::Error::Stored(fault) => Self::SectionStored(*fault),
            },
            Repr::Metadata(error) => Self::Metadata(error),
            Repr::Message(error) => Self::Message(error),
            Repr::Decode(error) => Self::Decode(error),
            Repr::Alloc(error) => Self::Alloc(error),
            Repr::Pool(error) => Self::Pool(error),
            Repr::Lane(error) => Self::Lane(error),
            Repr::Fields(error) => Self::Fields(error),
            Repr::Pack(error) => Self::Pack(error),
            Repr::Connection(error) => Self::Connection(error),
            Repr::Drive(error) => Self::Drive(error),
            Repr::RequestProgress(failure) => Self::RequestProgress(failure),
            Repr::Clock(error) => Self::Clock(error),
            Repr::DeadlineBook(error) => Self::DeadlineBook(error),
            Repr::Deadline(error) => Self::Deadline(error),
            Repr::Start(error) => Self::Start(error),
            Repr::Reset(error) => Self::Reset(error),
            Repr::Text(percent::Error::Input { actual }) => Self::TextInput { actual: *actual },
            Repr::Text(percent::Error::Escape { current }) => {
                Self::TextEscape { current: *current }
            }
            Repr::Call(calls::Reserve::Capacity(error)) => {
                let (active, limit) = (*error).into();
                Self::CallCapacity { active, limit }
            }
            Repr::Call(calls::Reserve::Occupied) => Self::CallOccupied,
            Repr::Insert { cause, reset } => Self::Insert { cause, reset },
            Repr::Wire(_) => Self::Wire {
                code: status.code(),
                message: status.message(),
                details: status.details(),
                metadata: status.metadata(),
            },
            Repr::Buffered(error) => Self::Buffered(error),
            Repr::Chunks(error) => Self::Chunks(error),
        }
    }
}

/// The wire field whose Base64 admission failed.
#[derive(Clone, Copy, Debug)]
pub enum Binary {
    Metadata,
    Details,
}

impl Binary {
    pub(crate) const fn code(self, error: &base64::Error) -> crate::Code {
        match error {
            base64::Error::Source { .. } | base64::Error::Output { .. } => crate::Code::Internal,
            base64::Error::Length { .. }
            | base64::Error::Byte { .. }
            | base64::Error::Padding { .. } => match self {
                Self::Metadata => crate::Code::InvalidArgument,
                Self::Details => crate::Code::Internal,
            },
        }
    }
}

/// A decoded status-details value must agree with its enclosing status.
#[derive(Debug)]
pub enum RichStatus {
    Ok,
    Code { status: crate::Code, details: i32 },
    Decode(prost::DecodeError),
}

impl<'a> From<&'a claims::Error> for Claim<'a> {
    fn from(error: &'a claims::Error) -> Self {
        match error {
            claims::Error::Completion(error) => Self::key(Action::Completion, error),
            claims::Error::Duplicate(index) => Self::Duplicate(*index),
            claims::Error::Ready(index) => Self::Ready(*index),
            claims::Error::Stream(error) => Self::key(Action::Stream, error),
            claims::Error::Event(error) => Self::key(Action::Event, error),
            claims::Error::Link(error) => Self::key(Action::Link, error),
            claims::Error::Closed { key } => Self::Closed {
                index: key.index(),
                generation: key.generation().nonzero(),
            },
            claims::Error::UnaryCapacity(cause) => Self::UnaryCapacity(*cause),
            claims::Error::StreamCapacity(cause) => Self::StreamCapacity(*cause),
            claims::Error::ReadyCapacity(cause) => Self::ReadyCapacity(*cause),
            claims::Error::EventCapacity(cause) => Self::EventCapacity(*cause),
        }
    }
}

impl<'a> Claim<'a> {
    fn key<T>(action: Action, error: &'a key::Error<T>) -> Self {
        let (index, epoch, seen) = error.observation();
        Self::Key {
            action,
            index,
            generation: epoch,
            observation: seen,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Credit {
    Overflow {
        account: Account,
        used: usize,
        added: usize,
    },
    Underflow {
        account: Account,
        used: usize,
        removed: usize,
    },
    Limit {
        account: Account,
        required: usize,
        capacity: usize,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Account {
    Charge,
    Ledger,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Kind {
    Reserved,
    Active,
    Done,
}

#[derive(Debug)]
pub struct Mismatch {
    pub expected: Kind,
    pub actual: Kind,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Fault {
    #[default]
    None,
    Duplicate,
    Method,
    Path,
    HttpStatus,
    ContentType,
    Te,
    GrpcStatus,
    Message,
    Details,
    Metadata,
    Capacity,
    DuplicateStatus,
    RequestEncoding,
    ResponseEncoding,
}

impl Fault {
    pub(crate) const fn code(self) -> crate::Code {
        match self {
            Self::Path | Self::Method | Self::RequestEncoding => crate::Code::Unimplemented,
            Self::HttpStatus | Self::ContentType => crate::Code::Unknown,
            Self::Duplicate | Self::DuplicateStatus | Self::Te | Self::Metadata => {
                crate::Code::InvalidArgument
            }
            Self::None
            | Self::GrpcStatus
            | Self::Message
            | Self::Details
            | Self::Capacity
            | Self::ResponseEncoding => crate::Code::Internal,
        }
    }
}

impl RichStatus {
    pub(crate) fn check(status: crate::Code, mut bytes: &[u8]) -> Result<(), Self> {
        if status == crate::Code::Ok {
            return Err(Self::Ok);
        }
        let mut code = 0;
        while !bytes.is_empty() {
            let (tag, wire) = encoding::decode_key(&mut bytes).map_err(Self::Decode)?;
            if tag == 1 {
                let mut value = 0;
                int32::merge(wire, &mut value, &mut bytes, DecodeContext::default())
                    .map_err(Self::Decode)?;
                code = value;
            } else {
                encoding::skip_field(wire, tag, &mut bytes, DecodeContext::default())
                    .map_err(Self::Decode)?;
            }
        }
        if code != i32::from(status.into_u8()) {
            return Err(Self::Code {
                status,
                details: code,
            });
        }
        Ok(())
    }
}
