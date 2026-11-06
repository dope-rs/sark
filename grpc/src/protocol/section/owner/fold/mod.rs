use o3::{base::num::bounded, buffer::pool};
use sark_http::field::{base64, metadata::packed::prefix, text::percent::decoder};

use crate::{
    details::Binary,
    protocol::{
        request::timeout,
        section::{JOINED, Seal, seal},
    },
};

mod current;

struct Capacity;

type NameLen = bounded::U32<0, { JOINED - 1 }>;

const _: () = {
    assert!(NameLen::new(JOINED - 1).is_some());
    assert!(NameLen::new(JOINED).is_none());
};

#[derive(Debug)]
pub(crate) enum Failure {
    Capacity,
    Prefix(prefix::Error),
    Message(decoder::Error),
    Timeout(timeout::Error),
    Binary { field: Binary, error: base64::Error },
}

impl Failure {
    pub(crate) const fn code(&self) -> crate::Code {
        match self {
            Self::Binary { field, error } => field.code(error),
            Self::Timeout(_) => crate::Code::InvalidArgument,
            Self::Capacity | Self::Prefix(_) | Self::Message(_) => crate::Code::Internal,
        }
    }
}

pub(super) struct Fold<'a> {
    lease: &'a mut pool::Span,
    read: usize,
    written: usize,
}

impl<'a> Fold<'a> {
    pub(super) const fn new(lease: &'a mut pool::Span) -> Self {
        Self {
            lease,
            read: seal::LEN,
            written: seal::LEN,
        }
    }

    pub(super) fn finish(mut self, seal: &mut Seal) -> Result<(), Failure> {
        let end = self.lease.len();
        let result = self.fields(seal, end);
        self.lease.truncate(self.written);
        result
    }

    fn fields(&mut self, seal: &mut Seal, end: usize) -> Result<(), Failure> {
        while self.read < end {
            current::field(self, seal, end)?;
        }
        Ok(())
    }
}

impl From<Capacity> for Failure {
    fn from(_: Capacity) -> Self {
        Self::Capacity
    }
}
