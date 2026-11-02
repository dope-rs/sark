use std::mem;

use client::receive::claims;
use dope::core::driver::schedule::{clock, ready::task};
use o3::{
    buffer::{pool, storage, write::consume},
    depot::{heap, queue::pool::Missing},
};
use sark_core::{
    deadline,
    manifold::{
        client::{domain, event},
        schedule,
        stream::Section,
        task::schedule::start,
    },
};
use sark_h2::{conn, stream};
use sark_http::{
    field::{
        Indexing,
        metadata::{Metadata, lower::plain, map, section::rules},
        text::percent,
    },
    payload::response::status,
};

use crate::{
    client,
    details::{self, RequestError},
    message, metadata,
    protocol::section::{self, Owner, Terminal, Trailers, seal},
    server::calls,
};

pub(crate) mod repr;
use repr::Repr;

use crate::Reason;

#[derive(Debug)]
pub struct Status {
    pub(crate) repr: Repr,
}

impl Status {
    pub(crate) const fn new(code: crate::Code, reason: Reason) -> Self {
        Self {
            repr: Repr::Local { code, reason },
        }
    }

    pub(crate) const fn reset(reason: event::Reset) -> Self {
        Self {
            repr: Repr::Reset(reason),
        }
    }

    pub(crate) const fn capacity(reason: Reason, error: consume::Error) -> Self {
        Self {
            repr: Repr::Capacity { reason, error },
        }
    }

    pub(crate) const fn credit(reason: Reason, error: details::Credit) -> Self {
        Self {
            repr: Repr::Credit { reason, error },
        }
    }

    pub(crate) const fn claim(error: claims::Error) -> Self {
        Self {
            repr: Repr::Claim(error),
        }
    }

    pub const fn from_code(code: crate::Code) -> Self {
        Self::new(code, Reason::None)
    }

    pub(crate) const fn length(reason: Reason, actual: usize) -> Self {
        Self {
            repr: Repr::Length { reason, actual },
        }
    }

    pub const fn ok() -> Self {
        Self::from_code(crate::Code::Ok)
    }

    pub fn code(&self) -> crate::Code {
        match &self.repr {
            Repr::HttpStatus(status) => match status.as_u16() {
                400 => crate::Code::Internal,
                401 => crate::Code::Unauthenticated,
                403 => crate::Code::PermissionDenied,
                404 => crate::Code::Unimplemented,
                429 | 502 | 503 | 504 => crate::Code::Unavailable,
                _ => crate::Code::Unknown,
            },
            Repr::RichStatus(_) => crate::Code::Internal,
            Repr::Insert { cause, reset } => match (cause, reset) {
                (schedule::Insert::Full { .. }, Ok(())) => crate::Code::ResourceExhausted,
                (schedule::Insert::Full { .. }, Err(_))
                | (schedule::Insert::Occupied { .. } | schedule::Insert::Excluded { .. }, _) => {
                    crate::Code::Internal
                }
            },
            Repr::Start(error) => match error {
                start::Error::Full
                | start::Error::Admission(task::Error::Capacity { .. })
                | start::Error::Topology(task::Error::Capacity { .. }) => {
                    crate::Code::ResourceExhausted
                }
                start::Error::Owner(_)
                | start::Error::Admission(
                    task::Error::WakeHopCeiling { .. } | task::Error::ParentStale,
                )
                | start::Error::Topology(
                    task::Error::WakeHopCeiling { .. } | task::Error::ParentStale,
                ) => crate::Code::Internal,
            },
            Repr::Local { code, .. } => *code,
            Repr::Metadata(_) => crate::Code::Internal,
            Repr::Message(error) => error.code(),
            Repr::Decode(_) => crate::Code::Internal,
            Repr::Connection(error) | Repr::Drive(domain::Failure::Turn(error)) => {
                match event::Classify::kind(error) {
                    event::Error::Unavailable => crate::Code::Unavailable,
                    event::Error::Blocked | event::Error::Capacity => {
                        crate::Code::ResourceExhausted
                    }
                    event::Error::Protocol => crate::Code::Internal,
                }
            }
            Repr::Reset(reason) => match reason {
                event::Reset::Cancelled => crate::Code::Cancelled,
                event::Reset::Refused => crate::Code::Unavailable,
                event::Reset::ResourceExhausted => crate::Code::ResourceExhausted,
                event::Reset::InadequateSecurity => crate::Code::PermissionDenied,
                event::Reset::Internal | event::Reset::Other => crate::Code::Internal,
            },
            Repr::Credit { error, .. } => match error {
                details::Credit::Limit { .. } => crate::Code::ResourceExhausted,
                details::Credit::Overflow { .. } | details::Credit::Underflow { .. } => {
                    crate::Code::Internal
                }
            },
            Repr::Call(error) => match error {
                calls::Reserve::Capacity(_) => crate::Code::ResourceExhausted,
                calls::Reserve::Occupied => crate::Code::Internal,
            },
            Repr::Drive(domain::Failure::Idle(_) | domain::Failure::Settle { .. })
            | Repr::State(_)
            | Repr::Lane(_)
            | Repr::RequestProgress(_)
            | Repr::Clock(_)
            | Repr::DeadlineBook(_)
            | Repr::Deadline(_) => crate::Code::Internal,
            Repr::Buffered(error) => error.code(),
            Repr::Chunks(error) => error.code(),
            Repr::Budget(error) => match error {
                details::Budget::Bytes { .. } | details::Budget::Messages { .. } => {
                    crate::Code::Internal
                }
            },
            Repr::Claim(error) => match error {
                claims::Error::UnaryCapacity(_)
                | claims::Error::StreamCapacity(_)
                | claims::Error::ReadyCapacity(_)
                | claims::Error::EventCapacity(_) => crate::Code::ResourceExhausted,
                claims::Error::Completion(_)
                | claims::Error::Duplicate(_)
                | claims::Error::Ready(_)
                | claims::Error::Stream(_)
                | claims::Error::Closed { .. }
                | claims::Error::Event(_)
                | claims::Error::Link(_) => crate::Code::Internal,
            },
            Repr::Section(error) => error.code(),
            Repr::Text(percent::Error::Input { .. } | percent::Error::Escape { .. }) => {
                crate::Code::Internal
            }
            Repr::Alloc(_) | Repr::Pool(_) | Repr::Capacity { .. } | Repr::Fields(_) => {
                crate::Code::ResourceExhausted
            }
            Repr::Pack(_) | Repr::Length { .. } => crate::Code::Internal,
            Repr::Wire(wire) => wire.code(),
        }
    }

    pub(crate) fn indexing(&self) -> [Indexing; 3] {
        self.repr
            .wire()
            .map_or([Indexing::Allowed; 3], Trailers::indexing)
    }

    pub fn message(&self) -> Option<&str> {
        self.repr.wire().and_then(Trailers::message)
    }

    pub fn metadata(&self) -> metadata::View<'_> {
        self.repr.wire().map_or_else(metadata::View::empty, |wire| {
            metadata::View::from(wire.owner())
        })
    }

    pub fn details(&self) -> Option<&[u8]> {
        self.repr.wire().and_then(Trailers::details)
    }

    pub fn from_fields<S: Metadata + ?Sized>(fields: &S) -> Result<Self, Self> {
        let owner = section::Plan::build(fields, rules::Kind::ResponseTrailers)?;
        Self::from_owner(owner, Section::Trailing, None)
    }

    pub(crate) fn from_owner(
        owner: Owner,
        section: Section,
        initial: Option<status::Code>,
    ) -> Result<Self, Self> {
        Ok(match Terminal::new(owner, section, initial)? {
            Terminal::Trailers(wire) => Self {
                repr: Repr::Wire(wire),
            },
            Terminal::Http(status) => Self::from(status),
        })
    }
}

impl From<status::Code> for Status {
    fn from(status: status::Code) -> Self {
        Self {
            repr: Repr::HttpStatus(status),
        }
    }
}

impl From<details::Buffered> for Status {
    fn from(error: details::Buffered) -> Self {
        Self {
            repr: Repr::Buffered(error),
        }
    }
}

impl From<details::Chunks> for Status {
    fn from(error: details::Chunks) -> Self {
        Self {
            repr: Repr::Chunks(error),
        }
    }
}

impl From<details::Budget> for Status {
    fn from(error: details::Budget) -> Self {
        Self {
            repr: Repr::Budget(error),
        }
    }
}

impl From<details::RichStatus> for Status {
    fn from(error: details::RichStatus) -> Self {
        Self {
            repr: Repr::RichStatus(error),
        }
    }
}

impl From<seal::Error> for Status {
    fn from(error: seal::Error) -> Self {
        Self {
            repr: Repr::Section(error),
        }
    }
}

impl From<percent::Error> for Status {
    fn from(error: percent::Error) -> Self {
        Self {
            repr: Repr::Text(error),
        }
    }
}

impl From<metadata::Error> for Status {
    fn from(error: metadata::Error) -> Self {
        Self {
            repr: Repr::Metadata(error),
        }
    }
}

impl From<message::Error> for Status {
    fn from(error: message::Error) -> Self {
        Self {
            repr: Repr::Message(error),
        }
    }
}

impl From<prost::DecodeError> for Status {
    fn from(error: prost::DecodeError) -> Self {
        Self {
            repr: Repr::Decode(error),
        }
    }
}

impl From<pool::CreateError> for Status {
    fn from(error: pool::CreateError) -> Self {
        Self {
            repr: Repr::Pool(error),
        }
    }
}

impl From<Missing> for Status {
    fn from(error: Missing) -> Self {
        Self {
            repr: Repr::Lane(error),
        }
    }
}

impl From<storage::AllocError> for Status {
    fn from(error: storage::AllocError) -> Self {
        Self {
            repr: Repr::Alloc(error),
        }
    }
}

impl From<consume::Error> for Status {
    fn from(error: consume::Error) -> Self {
        Self::capacity(Reason::FieldCapacity, error)
    }
}

impl From<plain::Error> for Status {
    fn from(error: plain::Error) -> Self {
        Self {
            repr: Repr::Fields(error),
        }
    }
}

impl From<map::Error> for Status {
    fn from(error: map::Error) -> Self {
        Self {
            repr: Repr::Pack(error),
        }
    }
}

impl From<domain::Failure<conn::Error, stream::Key>> for Status {
    fn from(error: domain::Failure<conn::Error, stream::Key>) -> Self {
        Self {
            repr: Repr::Drive(error),
        }
    }
}

impl From<RequestError> for Status {
    fn from(error: RequestError) -> Self {
        match error {
            RequestError::Connection(error) => error.into(),
            error @ RequestError::Progress(_) => Self {
                repr: Repr::RequestProgress(domain::Failure::Turn(error)),
            },
        }
    }
}

impl From<domain::Failure<RequestError, stream::Key>> for Status {
    fn from(failure: domain::Failure<RequestError, stream::Key>) -> Self {
        Self {
            repr: match failure {
                domain::Failure::Turn(RequestError::Connection(error)) => {
                    Repr::Drive(domain::Failure::Turn(error))
                }
                domain::Failure::Idle(error) => Repr::Drive(domain::Failure::Idle(error)),
                domain::Failure::Settle {
                    terminal: RequestError::Connection(terminal),
                    error,
                } => Repr::Drive(domain::Failure::Settle { terminal, error }),
                failure => Repr::RequestProgress(failure),
            },
        }
    }
}

impl From<conn::Error> for Status {
    fn from(error: conn::Error) -> Self {
        Self {
            repr: Repr::Connection(error),
        }
    }
}

impl From<clock::Range> for Status {
    fn from(error: clock::Range) -> Self {
        Self {
            repr: Repr::Clock(error),
        }
    }
}

impl From<heap::Error> for Status {
    fn from(error: heap::Error) -> Self {
        Self {
            repr: Repr::DeadlineBook(error),
        }
    }
}

impl From<deadline::Error<stream::Key>> for Status {
    fn from(error: deadline::Error<stream::Key>) -> Self {
        Self {
            repr: Repr::Deadline(error),
        }
    }
}

impl From<start::Error> for Status {
    fn from(error: start::Error) -> Self {
        Self {
            repr: Repr::Start(error),
        }
    }
}

impl Status {
    pub(crate) fn insert(
        cause: schedule::Insert<stream::Key, ()>,
        reset: Result<(), stream::Key>,
    ) -> Self {
        Self {
            repr: Repr::Insert { cause, reset },
        }
    }
}

const _: () = assert!(
    mem::size_of::<Status>() <= mem::size_of::<(domain::Failure<conn::Error, stream::Key>, u8)>()
);

impl From<details::Mismatch> for Status {
    fn from(error: details::Mismatch) -> Self {
        Self {
            repr: Repr::State(error),
        }
    }
}

impl From<calls::Reserve> for Status {
    fn from(error: calls::Reserve) -> Self {
        Self {
            repr: Repr::Call(error),
        }
    }
}
