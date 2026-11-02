//! HTTP/1 field syntax and borrow-free field state.

use core::mem;

use crate::field::{
    self,
    scan::Match,
    text::{Ascii, IntError},
};

mod cursor;
mod entry;
pub mod expect;
pub mod scan;
pub mod syntax;
mod transfer;
pub mod value;

pub(in crate::h1) use cursor::{Cursor, End};
pub use entry::Entry;
pub use transfer::Transfer;

pub(in crate::h1) enum Progress<B, E = ()> {
    Break(B),
    /// A sink asked for the next field and carried its own continuation value.
    Emitted(E),
    More,
    End(End),
}

const _: () = assert!(core::mem::size_of::<Progress<(), ()>>() == 12);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::h1) struct Limit {
    count: u32,
    line_bytes: u32,
}

impl Limit {
    pub(in crate::h1) const fn new(count: usize, line: usize) -> Self {
        Self {
            count: Self::narrow(count),
            line_bytes: Self::narrow(line),
        }
    }

    const fn narrow(value: usize) -> u32 {
        if value > u32::MAX as usize {
            u32::MAX
        } else {
            value as u32
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    DuplicateContentLength,
    ContentLengthWithTransferEncoding,
    InvalidContentLength(IntError),
    InvalidTransferEncoding(value::Error),
}

/// Accumulated connection-option semantics shared by request and response
/// scanners. Unknown valid tokens are intentionally ignored; malformed token
/// bytes fail admission.
#[repr(transparent)]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Connection(u8);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenError {
    pub index: usize,
    pub byte: u8,
}

impl Connection {
    pub(crate) const CLOSE: Self = Self(1 << 0);
    pub(crate) const KEEP_ALIVE: Self = Self(1 << 1);
    const UPGRADE: u8 = 1 << 2;

    pub fn try_push(mut self, value: &[u8]) -> Result<Self, TokenError> {
        let mut rest = value;
        while !rest.is_empty() {
            let base = value.len() - rest.len();
            let raw = match Match::find_by(rest, |byte| byte == b',') {
                Some(comma) => {
                    rest = comma.suffix();
                    comma.prefix()
                }
                None => mem::take(&mut rest),
            };
            let mut token = raw;
            while let [b' ' | b'\t', rest @ ..] = token {
                token = rest;
            }
            let start = raw.len() - token.len();
            while let [rest @ .., b' ' | b'\t'] = token {
                token = rest;
            }
            if token.is_empty() {
                continue;
            }
            if let Some((index, byte)) = token
                .iter()
                .copied()
                .enumerate()
                .find(|(_, byte)| !field::Error::is_name_byte(*byte))
            {
                return Err(TokenError {
                    index: base + start + index,
                    byte,
                });
            }
            if token.eq_ignore_ascii_case(b"close") {
                self.0 |= Self::CLOSE.0;
            } else if token.eq_ignore_ascii_case(b"keep-alive") {
                self.0 |= Self::KEEP_ALIVE.0;
            } else if token.eq_ignore_ascii_case(b"upgrade") {
                self.0 |= Self::UPGRADE;
            }
        }
        Ok(self)
    }

    /// Merges connection options that were parsed by another scanner branch.
    #[doc(hidden)]
    pub fn append(&mut self, fields: Self) {
        self.0 |= fields.0;
    }

    pub const fn is_close(self) -> bool {
        self.0 & Self::CLOSE.0 != 0
    }

    pub const fn is_keep(self) -> bool {
        self.0 & Self::KEEP_ALIVE.0 != 0
    }

    pub const fn is_upgrade(self) -> bool {
        self.0 & Self::UPGRADE != 0
    }
}

/// Advisory persistence parameters accumulated across every `Keep-Alive`
/// field. Malformed and unknown parameters are ignored because they cannot
/// change message framing; the shortest valid timeout wins deterministically.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct KeepAlive {
    timeout: Option<u64>,
}

impl KeepAlive {
    pub(crate) fn parse(mut prior: Option<u64>, value: &[u8]) -> Option<u64> {
        for raw in value.split(|byte| *byte == b',') {
            let param = field::Error::trim_ows(raw);
            let Some(split) = param.iter().position(|byte| *byte == b'=') else {
                continue;
            };
            let name = field::Error::trim_ows(&param[..split]);
            if !name.eq_ignore_ascii_case(b"timeout") {
                continue;
            }
            let Ok(timeout) = Ascii::new(field::Error::trim_ows(&param[split + 1..])).parse_u64()
            else {
                continue;
            };
            prior = Some(prior.map_or(timeout, |current| current.min(timeout)));
        }
        prior
    }

    pub(crate) const fn new(timeout: Option<u64>) -> Self {
        Self { timeout }
    }

    pub const fn timeout_secs(self) -> Option<u64> {
        self.timeout
    }
}

const _: () = assert!(core::mem::size_of::<Connection>() == 1);
