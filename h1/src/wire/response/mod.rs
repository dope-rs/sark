use std::mem;

use o3::buffer::{storage, write::consume};
use sark_http::{
    h1::{
        codec::numbers,
        head::line::{self, Request},
    },
    method::Known,
    payload::response::Method,
};

use crate::wire::head::Connection;

pub(in crate::wire) mod consts;
pub(in crate::wire) mod framing;
mod head;

pub(in crate::wire) use head::{Count, Head, Writer};

/// Request semantics and persistence retained until the corresponding response is emitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Context {
    method: Method,
    version: Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Version {
    Ten,
    TenKeepAlive,
    Eleven,
    ElevenClose,
}

impl Context {
    pub const fn new(method: Method, version: line::Version) -> Self {
        Self {
            method,
            version: match version {
                line::Version::Ten => Version::Ten,
                line::Version::Eleven => Version::Eleven,
            },
        }
    }

    pub(crate) fn from_request(request: &Request<'_>) -> Self {
        let method = match request.matched() {
            Some(Known::Head) => Method::Head,
            Some(Known::Connect) => Method::Connect,
            Some(_) => Method::Other,
            None => Method::from_bytes(request.method()),
        };
        Self::new(method, request.version())
    }

    pub const fn method(self) -> Method {
        self.method
    }

    pub const fn version(self) -> line::Version {
        match self.version {
            Version::Ten | Version::TenKeepAlive => line::Version::Ten,
            Version::Eleven | Version::ElevenClose => line::Version::Eleven,
        }
    }

    pub(crate) const fn with_connection(mut self, connection: Connection) -> Self {
        self.version = match (self.version(), connection) {
            (line::Version::Ten, Connection::KeepAlive) => Version::TenKeepAlive,
            (line::Version::Ten, Connection::Close) => Version::Ten,
            (line::Version::Eleven, Connection::KeepAlive) => Version::Eleven,
            (line::Version::Eleven, Connection::Close) => Version::ElevenClose,
        };
        self
    }

    pub(in crate::wire) const fn connection_field(self) -> &'static [u8] {
        match self.version {
            Version::TenKeepAlive => consts::KEEP_ALIVE,
            Version::ElevenClose => consts::CLOSE,
            Version::Ten | Version::Eleven => &[],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    PlanOverflow { current: usize, added: usize },
    WriteOverflow { current: usize, added: usize },
    EncodingOverflow { current: usize },
    WriteCapacity { required: usize, actual: usize },
    ValueCapacity(consume::Error),
    HeadCapacity { required: usize, actual: usize },
    HeadLength { expected: usize, actual: usize },
    Storage(storage::AllocError),
}

impl From<numbers::Error> for Error {
    fn from(error: numbers::Error) -> Self {
        match error {
            numbers::Error::Overflow { current, added } => Self::WriteOverflow {
                current,
                added: added.get(),
            },
            numbers::Error::Capacity { required, actual } => Self::WriteCapacity {
                required: required.get(),
                actual,
            },
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Context>() == 2);
    assert!(mem::size_of::<Error>() <= 3 * mem::size_of::<usize>());
};
