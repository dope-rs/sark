pub mod headers;
pub(super) mod known;
mod parse;

use core::mem;

use known::Known;

pub use parse::AcceptEncoding;

use crate::{
    field::text::IntError,
    h1::{
        field::{self, TokenError, value},
        switch, target,
    },
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Value,
    Connection(TokenError),
    ContentLength(IntError),
    TransferEncoding(value::Error),
    Host(target::HostError),
    Message(field::Error),
    Routing(target::Invalid),
    Upgrade(switch::UpgradeError),
}

const _: () = {
    assert!(mem::size_of::<Error>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Error>>() == mem::size_of::<Error>());
    assert!(!mem::needs_drop::<Error>());
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags(u16);

impl Flags {
    pub fn is_set(self, bit: u16) -> bool {
        (self.0 & bit) != 0
    }

    pub fn set(&mut self, bit: u16) {
        self.0 |= bit;
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Name {
    Invalid,
    Method,
    Scheme,
    Path,
    Status,
    Authority,
    Protocol,
    Host,
    Connection,
    ContentLength,
    TransferEncoding,
    Expect,
    AcceptEncoding,
    Te,
    KeepAlive,
    ProxyConnection,
    Upgrade,
    Regular,
}

impl Name {
    pub const H1: [Self; 7] = [
        Self::Host,
        Self::Connection,
        Self::ContentLength,
        Self::TransferEncoding,
        Self::Expect,
        Self::AcceptEncoding,
        Self::Upgrade,
    ];

    pub const fn wire(&self) -> &[u8] {
        match self {
            Self::Method => b":method",
            Self::Scheme => b":scheme",
            Self::Path => b":path",
            Self::Status => b":status",
            Self::Authority => b":authority",
            Self::Protocol => b":protocol",
            Self::Host => Known::Host.as_str().as_bytes(),
            Self::Connection => Known::Connection.as_str().as_bytes(),
            Self::ContentLength => Known::ContentLength.as_str().as_bytes(),
            Self::TransferEncoding => Known::TransferEncoding.as_str().as_bytes(),
            Self::Expect => Known::Expect.as_str().as_bytes(),
            Self::AcceptEncoding => Known::AcceptEncoding.as_str().as_bytes(),
            Self::Te => Known::Te.as_str().as_bytes(),
            Self::KeepAlive => Known::KeepAlive.as_str().as_bytes(),
            Self::ProxyConnection => Known::ProxyConnection.as_str().as_bytes(),
            Self::Upgrade => Known::Upgrade.as_str().as_bytes(),
            Self::Invalid | Self::Regular => b"",
        }
    }

    pub const fn is_required(self) -> bool {
        matches!(
            self,
            Self::Host
                | Self::Connection
                | Self::ContentLength
                | Self::TransferEncoding
                | Self::Expect
                | Self::Upgrade
        )
    }

    pub fn from_h1(name: &[u8]) -> Option<Self> {
        let known = match name.len() {
            4 => Self::Host,
            6 => Self::Expect,
            7 => Self::Upgrade,
            10 if name.first()?.eq_ignore_ascii_case(&b'k') => Self::KeepAlive,
            10 => Self::Connection,
            14 => Self::ContentLength,
            15 => Self::AcceptEncoding,
            17 => Self::TransferEncoding,
            _ => return None,
        };
        name.eq_ignore_ascii_case(known.wire()).then_some(known)
    }
}

pub const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
