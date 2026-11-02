use std::mem;

use encode::fields;
use o3::{
    alloc,
    buffer::{
        hold, resident, segments,
        segments::queue,
        storage::{self, provided::account},
        write::consume,
    },
    depot::{arena::slab::lookup, map::direct, queue::group},
    mem::rc,
};
use sark_core::{
    body, deadline,
    manifold::client::{event, io},
};
use sark_http::{
    field::metadata::lower,
    owner::handoff,
    payload::{
        owned,
        response::{exact, status::Code},
    },
};

use crate::{
    conn::{self, outputs},
    flow,
    frame::{self, numbers},
    hpack::{decode, encode},
    stream,
};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Egress(group::Error<1>),
    RetainedCharge(hold::Overflow),
    Incoming(direct::Error<stream::Key>),
    IncomingBody {
        slot: direct::Error<stream::Key>,
        error: body::Error,
    },
    IncomingSegments {
        slot: direct::Error<stream::Key>,
        error: segments::Error,
    },
    Exhausted(alloc::Exhausted),
    Consume(consume::Error),
    Input(queue::Bounds),
    Segments(segments::Error),
    Request(owned::Error),
    Response(owned::Error),
    Informational(Code),
    Handoff(handoff::Error),
    Build(storage::BuildError<consume::Error>),
    Size(exact::Overflow),
    Deadline(deadline::Error<stream::Key>),
    CriticalDeadline,
    BadPreface {
        offset: usize,
        expected: u8,
        actual: u8,
    },
    InitialSettings {
        kind: u8,
        ack: frame::Ack,
        stream: stream::Id,
    },
    Setting {
        id: numbers::SettingId,
        value: u32,
    },
    ParseError(numbers::ParseError),
    Protocol(conn::Protocol),
    Transition(stream::Id, stream::Error),
    Increase(flow::Increase),
    Adjust(flow::Adjust),
    WakeSlot {
        key: stream::Key,
        cause: lookup::Error,
    },
    WakeIdentity {
        expected: stream::Key,
        actual: stream::Key,
    },
    Receive(flow::Receive),
    GoAwayReceived(numbers::ErrorCode),
    StreamIdExhausted,
    StreamLimit {
        active: usize,
        limit: usize,
    },
    Hpack(decode::Error),
    Header(lower::Error),
    UnknownStream(stream::Id),
    InactiveStream(stream::Key),
    Continuation(conn::Continuation),
    FrameLimit {
        kind: u8,
        actual: usize,
        limit: usize,
    },
    DataRange {
        offset: usize,
        prefix: usize,
        body: usize,
    },
    StreamGoneAway,
    HeaderList {
        used: usize,
        incoming: usize,
        limit: usize,
    },
    HeaderEncode(fields::Error),
    HeaderWire(consume::Error),
    HeaderBusy,
    HeaderState(conn::HeaderState),
    HeaderEncodeFull {
        used: usize,
        limit: usize,
    },
    HeaderDecodeFull {
        used: usize,
        limit: usize,
    },
    PayloadFull {
        used: usize,
        limit: usize,
    },
    EventFull {
        used: usize,
        limit: usize,
    },
    RapidReset {
        actual: u32,
        limit: u32,
    },
    InputRange {
        offset: usize,
        need: usize,
        have: usize,
    },
    Resident(resident::AllocError),
    Authority(resident::BindError),
    Hold(hold::Error),
    Retain(rc::Error),
    InputFull(queue::Error),
    OutputAppend(consume::Error),
    Output(io::Error),
    OutputBarrier,
    OutputPayloadFull {
        used: usize,
        limit: usize,
    },
    OutputFull {
        used: usize,
        incoming: usize,
        limit: usize,
    },
    Admission(conn::Admission),
}

enum Class {
    Pressure,
    PressureRefused,
    Blocked,
    Capacity,
    CapacityFlow,
    CapacityRefused,
    Unavailable(numbers::ErrorCode),
    Protocol(numbers::ErrorCode),
}

impl Class {
    const fn resident(error: &resident::AllocError) -> Self {
        match error {
            resident::AllocError::Capacity(_) | resident::AllocError::Budget(_) => Self::Capacity,
            resident::AllocError::Layout(_) => Self::Protocol(numbers::ErrorCode::InternalError),
            resident::AllocError::Exhausted(_) | resident::AllocError::Retain(_) => {
                Self::Unavailable(numbers::ErrorCode::InternalError)
            }
        }
    }

    const fn account(error: &account::Cause) -> Self {
        match error {
            account::Cause::Foreign => Self::Protocol(numbers::ErrorCode::InternalError),
            account::Cause::Exhausted { .. } | account::Cause::Budget(_) => Self::Capacity,
            account::Cause::Retain(_) => Self::Unavailable(numbers::ErrorCode::InternalError),
        }
    }

    const fn hold(error: &hold::Error) -> Self {
        use resident::copy;

        match error {
            hold::Error::Range(_) => Self::Protocol(numbers::ErrorCode::InternalError),
            hold::Error::Resident(error) => Self::resident(error),
            hold::Error::Copy(copy::Error::Budget(_)) => Self::Capacity,
            hold::Error::Copy(copy::Error::Allocation(_)) => {
                Self::Unavailable(numbers::ErrorCode::InternalError)
            }
            hold::Error::Account(error) => Self::account(error),
        }
    }

    const fn owned(error: &owned::Error) -> Self {
        match error {
            owned::Error::Metadata(_)
            | owned::Error::Retain(_)
            | owned::Error::Storage(storage::AllocError::Exhausted(_))
            | owned::Error::Build(storage::BuildError::Exhausted(_)) => {
                Self::Unavailable(numbers::ErrorCode::InternalError)
            }
            owned::Error::Storage(storage::AllocError::Capacity(_))
            | owned::Error::Build(
                storage::BuildError::Capacity(_) | storage::BuildError::Build(_),
            ) => Self::Capacity,
            owned::Error::Discarded | owned::Error::Build(storage::BuildError::Length(_)) => {
                Self::Protocol(numbers::ErrorCode::InternalError)
            }
        }
    }

    const fn overflow(error: &hold::Overflow) -> Self {
        let (current, added) = match error {
            hold::Overflow::Logical { left, right }
            | hold::Overflow::Resident { left, right }
            | hold::Overflow::Owners { left, right } => (*left, *right),
        };
        if current.overflowing_add(added).1 {
            Self::Capacity
        } else {
            Self::Protocol(numbers::ErrorCode::InternalError)
        }
    }

    const fn is_pressure(&self) -> bool {
        matches!(self, Self::Pressure | Self::PressureRefused)
    }

    const fn event(self) -> event::Error {
        match self {
            Self::Pressure | Self::Blocked => event::Error::Blocked,
            Self::PressureRefused | Self::Capacity | Self::CapacityFlow | Self::CapacityRefused => {
                event::Error::Capacity
            }
            Self::Unavailable(_) => event::Error::Unavailable,
            Self::Protocol(_) => event::Error::Protocol,
        }
    }

    const fn code(self) -> numbers::ErrorCode {
        match self {
            Self::Pressure | Self::Blocked | Self::Capacity => numbers::ErrorCode::EnhanceYourCalm,
            Self::PressureRefused | Self::CapacityRefused => numbers::ErrorCode::RefusedStream,
            Self::CapacityFlow => numbers::ErrorCode::FlowControl,
            Self::Unavailable(code) | Self::Protocol(code) => code,
        }
    }
}

impl Error {
    pub(crate) const fn code(self) -> numbers::ErrorCode {
        self.class().code()
    }

    const fn class(&self) -> Class {
        match self {
            Self::Egress(_) => Class::Pressure,
            Self::Output(io::Error::Body(_) | io::Error::Overflow(_)) => {
                Class::Protocol(numbers::ErrorCode::InternalError)
            }
            Self::Segments(error) => match error {
                segments::Error::Retain(_) => Class::Unavailable(numbers::ErrorCode::InternalError),
                segments::Error::SegmentCapacity(_) | segments::Error::ChainCapacity(_) => {
                    Class::Pressure
                }
                segments::Error::DifferentPool
                | segments::Error::Count(_)
                | segments::Error::Logical(_)
                | segments::Error::Resident(_)
                | segments::Error::Total(_)
                | segments::Error::Boundary(_) => {
                    Class::Protocol(numbers::ErrorCode::InternalError)
                }
            },
            Self::Request(error)
            | Self::Response(error)
            | Self::Handoff(handoff::Error::Retain(error)) => Class::owned(error),
            Self::Handoff(handoff::Error::Overflow(error)) | Self::RetainedCharge(error) => {
                Class::overflow(error)
            }
            Self::Resident(error) => Class::resident(error),
            Self::Authority(_) | Self::Informational(_) => {
                Class::Protocol(numbers::ErrorCode::InternalError)
            }
            Self::Hold(error) => Class::hold(error),
            Self::Exhausted(_)
            | Self::Build(storage::BuildError::Exhausted(_))
            | Self::Retain(_) => Class::Unavailable(numbers::ErrorCode::InternalError),
            Self::Consume(_)
            | Self::Size(_)
            | Self::Build(storage::BuildError::Capacity(_) | storage::BuildError::Build(_)) => {
                Class::Blocked
            }
            Self::HeaderEncode(fields::Error::ConnectDisabled) => {
                Class::Protocol(numbers::ErrorCode::ProtocolError)
            }
            Self::Header(_)
            | Self::HeaderList { .. }
            | Self::HeaderEncode(_)
            | Self::HeaderWire(_)
            | Self::RapidReset { .. }
            | Self::InputFull(_)
            | Self::Admission(conn::Admission::IdentityExhausted(_))
            | Self::Continuation(conn::Continuation::Limit { .. }) => Class::Capacity,
            Self::Increase(_)
            | Self::Adjust(_)
            | Self::Receive(_)
            | Self::Setting {
                id: numbers::SettingId::InitialWindowSize,
                ..
            } => Class::CapacityFlow,
            Self::StreamIdExhausted => Class::CapacityRefused,
            Self::OutputFull {
                incoming, limit, ..
            } if *incoming > *limit => Class::Capacity,
            Self::HeaderBusy
            | Self::HeaderEncodeFull { .. }
            | Self::HeaderDecodeFull { .. }
            | Self::PayloadFull { .. }
            | Self::EventFull { .. }
            | Self::OutputAppend(_)
            | Self::Output(io::Error::Capacity(_))
            | Self::OutputBarrier
            | Self::OutputPayloadFull { .. }
            | Self::OutputFull { .. } => Class::Pressure,
            Self::StreamLimit { .. } | Self::Admission(conn::Admission::Full { .. }) => {
                Class::PressureRefused
            }
            Self::StreamGoneAway => Class::Unavailable(numbers::ErrorCode::ProtocolError),
            Self::Transition(_, stream::Error::StreamClosed { .. }) => {
                Class::Unavailable(numbers::ErrorCode::StreamClosed)
            }
            Self::GoAwayReceived(code) => Class::Unavailable(*code),
            Self::ParseError(
                numbers::ParseError::FrameSize { .. }
                | numbers::ParseError::FrameMinimum { .. }
                | numbers::ParseError::FrameLimit { .. }
                | numbers::ParseError::SettingsSize { .. },
            )
            | Self::FrameLimit { .. } => Class::Protocol(numbers::ErrorCode::FrameSize),
            Self::Hpack(decode::Error::Allocation(_)) => {
                Class::Unavailable(numbers::ErrorCode::InternalError)
            }
            Self::Hpack(_) => Class::Protocol(numbers::ErrorCode::Compression),
            Self::BadPreface { .. }
            | Self::InitialSettings { .. }
            | Self::Setting { .. }
            | Self::ParseError(_)
            | Self::Protocol(_)
            | Self::Transition(_, stream::Error::Protocol { .. })
            | Self::UnknownStream(_)
            | Self::InactiveStream(_)
            | Self::Continuation(_) => Class::Protocol(numbers::ErrorCode::ProtocolError),
            Self::Incoming(_)
            | Self::IncomingBody { .. }
            | Self::IncomingSegments { .. }
            | Self::Deadline(_)
            | Self::CriticalDeadline
            | Self::Build(storage::BuildError::Length(_))
            | Self::HeaderState(_)
            | Self::WakeSlot { .. }
            | Self::WakeIdentity { .. }
            | Self::DataRange { .. }
            | Self::Input(_)
            | Self::InputRange { .. }
            | Self::Admission(conn::Admission::Duplicate(_)) => {
                Class::Protocol(numbers::ErrorCode::InternalError)
            }
        }
    }

    pub const fn is_pressure(&self) -> bool {
        self.class().is_pressure()
    }
}

impl event::Classify for Error {
    fn kind(&self) -> event::Error {
        self.class().event()
    }
}

impl From<group::Error<1>> for Error {
    fn from(error: group::Error<1>) -> Self {
        Self::Egress(error)
    }
}

impl From<decode::Failure<lower::Error>> for Error {
    fn from(error: decode::Failure<lower::Error>) -> Self {
        match error {
            decode::Failure::Decode(cause) => Self::Hpack(cause),
            decode::Failure::Sink(cause) => Self::Header(cause),
        }
    }
}

impl From<storage::AllocError> for Error {
    fn from(error: storage::AllocError) -> Self {
        match error {
            storage::AllocError::Capacity(error) => Self::Consume(error),
            storage::AllocError::Exhausted(error) => Self::Exhausted(error),
        }
    }
}

impl From<alloc::Exhausted> for Error {
    fn from(error: alloc::Exhausted) -> Self {
        Self::Exhausted(error)
    }
}

impl From<resident::AllocError> for Error {
    fn from(error: resident::AllocError) -> Self {
        Self::Resident(error)
    }
}

impl From<hold::CopyError> for Error {
    fn from(error: hold::CopyError) -> Self {
        match error {
            hold::CopyError::Storage(error) => error.into(),
            hold::CopyError::Resident(error) => error.into(),
            hold::CopyError::Authority(error) => Self::Authority(error),
        }
    }
}

impl From<queue::RetainError> for Error {
    fn from(error: queue::RetainError) -> Self {
        match error {
            queue::RetainError::Segments(error) => Self::Segments(error),
            queue::RetainError::Copy(error) => error.into(),
        }
    }
}

impl From<segments::Error> for Error {
    fn from(error: segments::Error) -> Self {
        Self::Segments(error)
    }
}

impl From<hold::Error> for Error {
    fn from(error: hold::Error) -> Self {
        Self::Hold(error)
    }
}

impl From<flow::Receive> for Error {
    fn from(error: flow::Receive) -> Self {
        Self::Receive(error)
    }
}

impl From<outputs::Full> for Error {
    fn from(error: outputs::Full) -> Self {
        match error {
            outputs::Full::Append(cause) => Self::OutputAppend(cause),
            outputs::Full::Barrier => Self::OutputBarrier,
            outputs::Full::Payloads { used, limit } => Self::OutputPayloadFull { used, limit },
            outputs::Full::Bytes {
                used,
                incoming,
                limit,
            } => Self::OutputFull {
                used,
                incoming,
                limit,
            },
        }
    }
}

impl From<&Error> for numbers::ErrorCode {
    fn from(error: &Error) -> Self {
        error.class().code()
    }
}

const _: () = {
    assert!(mem::size_of::<Class>() == 2 * mem::size_of::<u32>());
    assert!(mem::size_of::<Error>() == mem::size_of::<lower::Error>());
};
