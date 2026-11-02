use std::collections;

use decoder::{ingress::error, state};
use dope_quic::wire::varint;
use o3::{
    buffer::{hold, segments, storage, write::consume},
    depot::queue::group,
    mem::rc,
};
use sark_http::{
    field::metadata::{block, section::rules},
    owner::handoff,
    payload::{
        owned,
        response::{exact, status::Code},
    },
};

use crate::{
    conn::{
        CreditMode, event,
        io::{ingress::wait, replies},
        output, push,
    },
    frame::{self, ErrorCode},
    qpack::{decoder, encoder, stream},
    stream::{Mismatch, UniType},
};

mod failure;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Informational(Code),
    Parse(frame::ParseError),
    Qpack(decoder::Error),
    Feedback(stream::Capacity),
    FeedbackIncrement {
        pending: u64,
        additional: u64,
    },
    FeedbackGrowth {
        before: usize,
        after: usize,
    },
    Encode(encoder::Error),
    Request(owned::Error),
    Response(owned::Error),
    Handoff(handoff::Error),
    Retention(hold::Overflow),
    Retain(rc::Error),
    Build(storage::BuildError<consume::Error>),
    Size(exact::Overflow),
    FrameUnexpected(event::Violation),
    Push(push::Error),
    MissingSettings(event::Frame),
    ClosedCriticalStream(u64),
    Target(Mismatch),
    GoAwayId(u64),
    GoAway(u64),
    GoAwayOrder {
        previous: u64,
        actual: u64,
    },
    PushIdOrder {
        previous: u64,
        actual: u64,
    },
    PushStream {
        stream_id: u64,
        role: event::Role,
    },
    RequestStream {
        stream_id: u64,
    },
    CriticalExists {
        kind: UniType,
        existing: u64,
        attempted: u64,
    },
    Identity {
        raw: u64,
    },
    Bound {
        existing: u64,
        attempted: u64,
    },
    UnboundOpen,
    UnboundHold,
    QpackEncoderStream(error::Error),
    QpackDecoderStream(stream::Error),
    ReceiveCapacity {
        retained: usize,
        additional: usize,
        limit: usize,
    },
    ReceiveRelease {
        retained: usize,
        released: usize,
    },
    ReceiveCopy {
        requested: usize,
        written: usize,
        available: usize,
    },
    SegmentPush(segments::Error),
    SegmentTake(segments::Error),
    Wait(wait::Error),
    Prefix {
        required: usize,
        error: collections::TryReserveError,
    },
    PrefixOverflow {
        len: usize,
        additional: usize,
    },
    PrefixWrite {
        expected: usize,
        written: usize,
        additional: usize,
    },
    PrefixLength {
        expected: usize,
        actual: usize,
    },
    Output(output::Error),
    Length(varint::Overflow),
    Reply(replies::Error),
    BatchRegularWire {
        left: usize,
        right: usize,
    },
    BatchRegularResident {
        left: usize,
        right: usize,
    },
    BatchCriticalWire {
        left: usize,
        right: usize,
    },
    BatchCriticalResident {
        left: usize,
        right: usize,
    },
    QueueRegular {
        queued: usize,
        additional: usize,
        limit: usize,
    },
    BudgetWire {
        retained: usize,
        additional: usize,
        limit: usize,
    },
    BudgetResident {
        retained: usize,
        additional: usize,
        limit: usize,
    },
    BudgetRegularWire {
        retained: usize,
        additional: usize,
        limit: usize,
    },
    BudgetRegularResident {
        retained: usize,
        additional: usize,
        limit: usize,
    },
    BudgetReleaseWire {
        retained: usize,
        released: usize,
    },
    BudgetReleaseResident {
        retained: usize,
        released: usize,
    },
    BudgetRegularReleaseWire {
        retained: usize,
        released: usize,
    },
    BudgetRegularReleaseResident {
        retained: usize,
        released: usize,
    },
    Egress(group::Error<2>),
    EventCapacity {
        len: usize,
        capacity: usize,
    },
    CreditCapacity {
        len: usize,
        capacity: usize,
    },
    StreamCapacity {
        stream_id: u64,
        active: usize,
        capacity: usize,
    },
    StreamAdmission {
        stream_id: u64,
        active: usize,
        capacity: usize,
    },
    QpackLength {
        pending: usize,
        wire: usize,
    },
    CreditOverflow {
        accumulated: usize,
        additional: usize,
    },
    VarInt {
        actual: u64,
    },
    StreamKind {
        stream_id: u64,
    },
    ControlExists {
        existing: u64,
        attempted: u64,
    },
    EncoderExists {
        existing: u64,
        attempted: u64,
    },
    DecoderExists {
        existing: u64,
        attempted: u64,
    },
    ControlMissing,
    EncoderMissing,
    ResponseLength {
        expected: usize,
        actual: usize,
    },
    SectionLength {
        leading: usize,
        fields: usize,
    },
    SectionPrefix {
        leading: usize,
        fields: usize,
    },
    ReceiveRange {
        start: usize,
        end: usize,
        len: usize,
        available: usize,
    },
    TokenMismatch {
        expected: u64,
        actual: u64,
    },
    ReceiveState {
        buffered: usize,
        held: CreditMode,
    },
    Consume(consume::Error),
    AppendBlocked {
        stream_id: u64,
    },
    HandoffBlocked {
        stream_id: u64,
    },
    FinishBlocked {
        stream_id: u64,
    },
    DeferBlocked {
        stream_id: u64,
    },
    CreditMode {
        bytes: usize,
    },
    CreditOpen {
        stream_id: u64,
    },
    StreamMissing {
        stream_id: u64,
    },
    StreamResidual {
        stream_id: u64,
        buffered: usize,
        held: CreditMode,
    },
    Incomplete {
        stream_id: u64,
        buffered: usize,
        cause: frame::ParseError,
    },
    DuplicateSettings(event::Frame),
    MessageMissing {
        stream_id: u64,
    },
    Malformed {
        stream_id: u64,
        kind: rules::Kind,
    },
    Content {
        stream_id: u64,
        error: block::ContentError,
    },
    PushRejected {
        stream_id: u64,
    },
}

impl Error {
    const fn segment(error: &segments::Error) -> ErrorCode {
        match error {
            segments::Error::SegmentCapacity(_)
            | segments::Error::ChainCapacity(_)
            | segments::Error::Retain(_)
            | segments::Error::Count(_)
            | segments::Error::Logical(_)
            | segments::Error::Resident(_)
            | segments::Error::Total(_) => ErrorCode::ExcessiveLoad,
            segments::Error::DifferentPool | segments::Error::Boundary(_) => ErrorCode::Internal,
        }
    }

    const fn overflow(error: &hold::Overflow) -> ErrorCode {
        let full = match error {
            hold::Overflow::Logical { left, right }
            | hold::Overflow::Resident { left, right }
            | hold::Overflow::Owners { left, right } => left.checked_add(*right).is_none(),
        };
        if full {
            ErrorCode::ExcessiveLoad
        } else {
            ErrorCode::Internal
        }
    }

    /// Returns whether releasing retained work can admit this bounded demand.
    /// Overflow errors retain the failed operands and remain terminal.
    pub(crate) const fn is_pressure(&self) -> bool {
        match self {
            Self::Qpack(error) => failure::Failure::classify(error).is_pressure(),
            Self::EventCapacity { .. }
            | Self::CreditCapacity { .. }
            | Self::StreamCapacity { .. } => true,
            Self::QueueRegular {
                queued: retained,
                additional,
                limit,
            }
            | Self::BudgetWire {
                retained,
                additional,
                limit,
            }
            | Self::BudgetResident {
                retained,
                additional,
                limit,
            }
            | Self::BudgetRegularWire {
                retained,
                additional,
                limit,
            }
            | Self::BudgetRegularResident {
                retained,
                additional,
                limit,
            } => {
                *additional <= *limit
                    && matches!(retained.checked_add(*additional), Some(total) if total > *limit)
            }
            _ => false,
        }
    }

    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::Push(
                push::Error::Unpermitted { .. }
                | push::Error::Unknown { .. }
                | push::Error::Duplicate { .. },
            ) => ErrorCode::Id,
            Self::Push(push::Error::Mismatch { .. }) => ErrorCode::GeneralProtocol,
            Self::Push(
                push::Error::Capacity { .. }
                | push::Error::Storage { .. }
                | push::Error::Allocation(_),
            ) => ErrorCode::ExcessiveLoad,
            Self::Push(push::Error::GoAway { .. }) | Self::GoAway(_) => ErrorCode::RequestRejected,
            Self::Egress(_) => ErrorCode::ExcessiveLoad,
            Self::Parse(frame::ParseError::FrameTooLarge)
            | Self::Parse(frame::ParseError::PayloadInteger(_) | frame::ParseError::BadVarInt)
            | Self::Incomplete { .. }
            | Self::Length(_)
            | Self::Reply(replies::Error::Length(_)) => ErrorCode::Payload,
            Self::Parse(frame::ParseError::BadSettings | frame::ParseError::DuplicateSetting) => {
                ErrorCode::Settings
            }
            Self::DuplicateSettings(_) => ErrorCode::FrameUnexpected,
            Self::Qpack(error) => failure::Failure::classify(error).code(),
            Self::Feedback(_) => ErrorCode::ExcessiveLoad,
            Self::FeedbackIncrement { .. } => ErrorCode::QpackEncoderStream,
            Self::Encode(
                encoder::Error::Allocation(_)
                | encoder::Error::Huffman(_)
                | encoder::Error::Buffer(_)
                | encoder::Error::FieldCapacity { .. }
                | encoder::Error::FieldSection { .. }
                | encoder::Error::Capacity { .. },
            ) => ErrorCode::ExcessiveLoad,
            Self::Encode(encoder::Error::ConnectDisabled) => ErrorCode::Message,
            Self::Encode(_) => ErrorCode::Internal,
            Self::Request(_) | Self::Response(_) => ErrorCode::ExcessiveLoad,
            Self::Handoff(handoff::Error::Retain(_)) | Self::Retain(_) => ErrorCode::ExcessiveLoad,
            Self::Handoff(handoff::Error::Overflow(error)) => Self::overflow(error),
            Self::Retention(error)
            | Self::Output(output::Error::Retention(error))
            | Self::Reply(replies::Error::Output(output::Error::Retention(error))) => {
                Self::overflow(error)
            }
            Self::Size(_)
            | Self::Build(storage::BuildError::Capacity(_) | storage::BuildError::Exhausted(_)) => {
                ErrorCode::ExcessiveLoad
            }
            Self::FrameUnexpected(_) | Self::Parse(frame::ParseError::ReservedFrame { .. }) => {
                ErrorCode::FrameUnexpected
            }
            Self::MissingSettings(_) => ErrorCode::MissingSettings,
            Self::ClosedCriticalStream(_) => ErrorCode::ClosedCriticalStream,
            Self::PushStream { .. } | Self::RequestStream { .. } | Self::CriticalExists { .. } => {
                ErrorCode::StreamCreation
            }
            Self::Target(_) | Self::Informational(_) => ErrorCode::Internal,
            Self::Identity { .. }
            | Self::VarInt { .. }
            | Self::GoAwayId(_)
            | Self::GoAwayOrder { .. }
            | Self::PushIdOrder { .. } => ErrorCode::Id,
            Self::QpackEncoderStream(_) => ErrorCode::QpackEncoderStream,
            Self::QpackDecoderStream(_) => ErrorCode::QpackDecoderStream,
            Self::ReceiveCapacity { .. } => ErrorCode::ExcessiveLoad,
            Self::Build(storage::BuildError::Build(_) | storage::BuildError::Length(_))
            | Self::ReceiveRelease { .. }
            | Self::ReceiveCopy { .. }
            | Self::FeedbackGrowth { .. }
            | Self::BudgetReleaseWire { .. }
            | Self::BudgetReleaseResident { .. }
            | Self::BudgetRegularReleaseWire { .. }
            | Self::BudgetRegularReleaseResident { .. }
            | Self::Bound { .. }
            | Self::UnboundOpen
            | Self::UnboundHold
            | Self::StreamKind { .. }
            | Self::ControlExists { .. }
            | Self::EncoderExists { .. }
            | Self::DecoderExists { .. }
            | Self::ControlMissing
            | Self::EncoderMissing
            | Self::Reply(replies::Error::Empty)
            | Self::ResponseLength { .. }
            | Self::ReceiveRange { .. }
            | Self::TokenMismatch { .. }
            | Self::ReceiveState { .. }
            | Self::Consume(_)
            | Self::AppendBlocked { .. }
            | Self::HandoffBlocked { .. }
            | Self::FinishBlocked { .. }
            | Self::DeferBlocked { .. }
            | Self::CreditMode { .. }
            | Self::CreditOpen { .. }
            | Self::StreamMissing { .. }
            | Self::StreamResidual { .. } => ErrorCode::Internal,
            Self::SegmentPush(error) | Self::SegmentTake(error) => Self::segment(error),
            Self::Wait(wait::Error::Present { .. }) => ErrorCode::GeneralProtocol,
            Self::Wait(wait::Error::Full { .. }) => ErrorCode::ExcessiveLoad,
            Self::Wait(
                wait::Error::Slot(_) | wait::Error::Missing { .. } | wait::Error::Mismatch { .. },
            ) => ErrorCode::Internal,
            Self::Prefix { .. }
            | Self::PrefixOverflow { .. }
            | Self::Output(output::Error::Charge(_))
            | Self::Reply(replies::Error::Output(output::Error::Charge(_)))
            | Self::BatchRegularWire { .. }
            | Self::BatchRegularResident { .. }
            | Self::BatchCriticalWire { .. }
            | Self::BatchCriticalResident { .. }
            | Self::QueueRegular { .. }
            | Self::BudgetWire { .. }
            | Self::BudgetResident { .. }
            | Self::BudgetRegularWire { .. }
            | Self::BudgetRegularResident { .. }
            | Self::EventCapacity { .. }
            | Self::CreditCapacity { .. }
            | Self::StreamCapacity { .. }
            | Self::StreamAdmission { .. }
            | Self::QpackLength { .. }
            | Self::CreditOverflow { .. }
            | Self::SectionLength { .. }
            | Self::SectionPrefix { .. } => ErrorCode::ExcessiveLoad,
            Self::PrefixWrite { .. } | Self::PrefixLength { .. } => ErrorCode::Internal,
            Self::MessageMissing { .. }
            | Self::Malformed { .. }
            | Self::Content { .. }
            | Self::PushRejected { .. } => ErrorCode::Message,
            Self::Parse(_) => ErrorCode::GeneralProtocol,
        }
    }
}

impl From<state::Error> for Error {
    fn from(error: state::Error) -> Self {
        match error {
            state::Error::Capacity(capacity) => Self::Feedback(capacity),
            state::Error::Increment {
                pending,
                additional: added,
            } => Self::FeedbackIncrement {
                pending,
                additional: added,
            },
            state::Error::Growth { before, after } => Self::FeedbackGrowth { before, after },
        }
    }
}

const _: () = assert!(size_of::<Error>() == size_of::<decoder::Error>());
