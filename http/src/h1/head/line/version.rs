use crate::h1;

const BASE: u64 = u64::from_ne_bytes(*b"HTTP/1.0");
const MASK: u64 = u64::from_ne_bytes(*b"\xff\xff\xff\xff\xff\xff\xff\xfe");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Version {
    Ten,
    Eleven,
}

impl Version {
    /// Canonical tunnel response using this admitted HTTP version.
    pub const fn connect<'a>(self) -> &'a [u8] {
        match self {
            Self::Ten => b"HTTP/1.0 200 Connection Established\r\n\r\n",
            Self::Eleven => h1::CONNECT_ESTABLISHED,
        }
    }

    pub const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Ten => b"HTTP/1.0",
            Self::Eleven => b"HTTP/1.1",
        }
    }

    pub(in crate::h1::head::line) fn parse(bytes: &[u8]) -> Option<Self> {
        let bytes = bytes.first_chunk::<8>()?;
        let word = u64::from_ne_bytes(*bytes);
        if ((word ^ BASE) & MASK) != 0 {
            return None;
        }
        Some(if word == BASE {
            Self::Ten
        } else {
            Self::Eleven
        })
    }
}

const _: () = assert!(core::mem::size_of::<Version>() == 1);
