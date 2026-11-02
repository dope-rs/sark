use client::receive::claims;
use dope::core::driver::schedule::clock;
use o3::{
    buffer::{pool, storage, write::consume},
    depot::{heap, queue::pool::Missing},
};
use sark_core::{
    deadline,
    manifold::{
        client::{domain, event},
        schedule,
        task::schedule::start,
    },
};
use sark_h2::{conn, stream};
use sark_http::{
    field::{
        metadata::{lower::plain, map},
        text::percent,
    },
    payload::response::status,
};

use crate::{
    Reason, client, details, message, metadata,
    protocol::section::{Trailers, seal},
    server::calls,
};

#[derive(Debug)]
pub(crate) enum Repr {
    HttpStatus(status::Code),
    RichStatus(details::RichStatus),
    Local {
        code: crate::Code,
        reason: Reason,
    },
    Metadata(metadata::Error),
    Message(message::Error),
    Decode(prost::DecodeError),
    Alloc(storage::AllocError),
    Pool(pool::CreateError),
    Lane(Missing),
    Capacity {
        reason: Reason,
        error: consume::Error,
    },
    Credit {
        reason: Reason,
        error: details::Credit,
    },
    Call(calls::Reserve),
    State(details::Mismatch),
    Buffered(details::Buffered),
    Chunks(details::Chunks),
    Budget(details::Budget),
    Claim(claims::Error),
    Section(seal::Error),
    Text(percent::Error),
    Fields(plain::Error),
    Pack(map::Error),
    Connection(conn::Error),
    Drive(domain::Failure<conn::Error, stream::Key>),
    RequestProgress(domain::Failure<details::RequestError, stream::Key>),
    Clock(clock::Range),
    DeadlineBook(heap::Error),
    Deadline(deadline::Error<stream::Key>),
    Start(start::Error),
    Insert {
        cause: schedule::Insert<stream::Key, ()>,
        reset: Result<(), stream::Key>,
    },
    Reset(event::Reset),
    Length {
        reason: Reason,
        actual: usize,
    },
    Wire(Trailers),
}

impl Repr {
    pub(super) fn wire(&self) -> Option<&Trailers> {
        if let Self::Wire(wire) = self {
            Some(wire)
        } else {
            None
        }
    }
}
