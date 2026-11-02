use core::ops;

use o3::buffer::pool;
use sark_http::{
    field::{Indexing, metadata::section::rules},
    payload::response::status,
};

use crate::{
    details::Fault,
    protocol::{request::timeout, section::owner::fold},
};

pub(super) const LEN: usize = 26;
pub(super) const CONTENT_TYPE: usize = 1;
pub(super) const CODE: usize = 2;
pub(in crate::protocol) const HTTP_STATUS: usize = 19;
pub(super) const TIMEOUT: usize = 21;
const NONE: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Observation {
    First,
    Duplicate,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Write {
    Complete,
    Capacity,
}

#[repr(u16)]
#[derive(Clone, Copy)]
pub(super) enum Flag {
    Method = 1 << 0,
    Path = 1 << 1,
    Authority = 1 << 2,
    HttpStatus = 1 << 3,
    ContentType = 1 << 4,
    Te = 1 << 5,
    GrpcStatus = 1 << 6,
    Message = 1 << 7,
    Details = 1 << 8,
    GrpcContentType = 1 << 9,
    Timeout = 1 << 10,
    Encoding = 1 << 11,
}

#[derive(Clone, Copy)]
pub(super) struct Span {
    start: u32,
    len: u32,
}

impl Span {
    pub(super) const NONE: Self = Self {
        start: NONE,
        len: 0,
    };

    pub(super) fn from_range(range: ops::Range<usize>) -> Option<Self> {
        let len = range.len();
        if range.start >= NONE as usize || len > u32::MAX as usize {
            return None;
        }
        Some(Self {
            start: range.start as u32,
            len: len as u32,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Field {
    First,
    Second,
}

impl Field {
    pub(super) const fn at(self) -> usize {
        match self {
            Self::First => 3,
            Self::Second => 11,
        }
    }
}

#[derive(Debug)]
pub(crate) enum Error {
    Fold(fold::Failure),
    Prefix { actual: usize, required: usize },
    Stored(Fault),
}

impl Error {
    pub(crate) const fn code(&self) -> crate::Code {
        match self {
            Self::Stored(fault) => fault.code(),
            Self::Fold(error) => error.code(),
            Self::Prefix { .. } => crate::Code::Internal,
        }
    }
}

pub(super) struct Seal {
    kind: rules::Kind,
    fault: Fault,
    code: Option<crate::Code>,
    http_status: Option<status::Code>,
    indexing: Indexing,
    flags: u16,
    spans: [Span; 2],
    timeout: timeout::Value,
}

impl Seal {
    pub(super) const fn new(kind: rules::Kind) -> Self {
        Self {
            kind,
            fault: Fault::None,
            code: None,
            http_status: None,
            indexing: Indexing::Allowed,
            flags: 0,
            spans: [Span::NONE; 2],
            timeout: timeout::Value::NONE,
        }
    }

    pub(super) fn observe(&mut self, flag: Flag) -> Observation {
        let bit = flag as u16;
        let outcome = if self.flags & bit == 0 {
            Observation::First
        } else {
            Observation::Duplicate
        };
        self.flags |= bit;
        outcome
    }

    pub(super) const fn has_flag(&self, flag: Flag) -> bool {
        self.flags & flag as u16 != 0
    }

    pub(super) fn fault(&mut self, fault: Fault) {
        if self.fault == Fault::None {
            self.fault = fault;
        }
    }

    pub(super) const fn into_fault(self) -> Fault {
        self.fault
    }

    pub(super) const fn has_fault(&self) -> bool {
        !matches!(self.fault, Fault::None)
    }

    pub(super) const fn kind(&self) -> rules::Kind {
        self.kind
    }

    pub(super) fn set_code(&mut self, code: crate::Code, indexing: Indexing) {
        self.code = Some(code);
        self.indexing = indexing;
    }

    pub(super) fn set_http(&mut self, status: status::Code) {
        self.http_status = Some(status);
    }

    pub(super) fn set_timeout(&mut self, timeout: timeout::Value) {
        self.timeout = timeout;
    }

    pub(super) fn is_non_ok(&self) -> bool {
        self.http_status
            .is_some_and(|status| status != status::Code::OK)
    }

    pub(super) fn is_info(&self) -> bool {
        self.http_status.is_some_and(|status| status.is_info())
    }

    pub(super) fn set_first(&mut self, span: Span) {
        self.spans[0] = span;
    }

    pub(super) fn set_second(&mut self, span: Span) {
        self.spans[1] = span;
    }

    pub(super) fn write(&self, lease: &mut pool::Span) -> Write {
        let Some(prefix) = lease.as_mut_slice().get_mut(..LEN) else {
            return Write::Capacity;
        };
        prefix[0] = self.indexing as u8;
        prefix[CONTENT_TYPE] = u8::from(self.has_flag(Flag::GrpcContentType));
        prefix[CODE] = match self.code {
            Some(code) => code.into_u8(),
            None => u8::MAX,
        };
        Self::write_span(&mut prefix[3..11], self.spans[0]);
        Self::write_span(&mut prefix[11..19], self.spans[1]);
        let http = self.http_status.map_or(0, |status| status.as_u16());
        prefix[HTTP_STATUS..TIMEOUT].copy_from_slice(&http.to_ne_bytes());
        prefix[TIMEOUT..TIMEOUT + 4].copy_from_slice(&self.timeout.value.to_ne_bytes());
        prefix[TIMEOUT + 4] = self.timeout.unit as u8;
        Write::Complete
    }

    fn write_span(output: &mut [u8], span: Span) {
        output[..4].copy_from_slice(&span.start.to_ne_bytes());
        output[4..8].copy_from_slice(&span.len.to_ne_bytes());
    }
}

const _: () = {
    assert!(core::mem::size_of::<Span>() == 8);
    assert!(core::mem::size_of::<Option<crate::Code>>() == 1);
    assert!(core::mem::size_of::<Seal>() == 32);
};
