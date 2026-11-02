mod known;

use std::{num::NonZero, str};

pub use known::Known;
use o3::alloc::{self, BoxSliceExt as _};

use crate::field;

/// An owned, case-sensitive HTTP token with inline storage for short extensions.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Method(Kind);

#[derive(Debug, PartialEq, Eq, Hash)]
enum Kind {
    Known(Known),
    Trace,
    Query,
    Inline { bytes: [u8; 15], len: NonZero<u8> },
    Owned(Box<[u8]>),
}

impl Method {
    pub const GET: Self = Self(Kind::Known(Known::Get));
    pub const POST: Self = Self(Kind::Known(Known::Post));
    pub const PUT: Self = Self(Kind::Known(Known::Put));
    pub const PATCH: Self = Self(Kind::Known(Known::Patch));
    pub const DELETE: Self = Self(Kind::Known(Known::Delete));
    pub const HEAD: Self = Self(Kind::Known(Known::Head));
    pub const OPTIONS: Self = Self(Kind::Known(Known::Options));
    pub const CONNECT: Self = Self(Kind::Known(Known::Connect));
    pub const TRACE: Self = Self(Kind::Trace);
    pub const QUERY: Self = Self(Kind::Query);

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if let Some(known) = Known::from_bytes(bytes) {
            return Ok(known.into());
        }
        if bytes == b"TRACE" {
            return Ok(Self::TRACE);
        }
        if bytes == b"QUERY" {
            return Ok(Self::QUERY);
        }
        field::Error::name(bytes).map_err(Error::Syntax)?;
        if bytes.len() <= 15 {
            let mut inline = [0; 15];
            inline[..bytes.len()].copy_from_slice(bytes);
            Ok(Self(Kind::Inline {
                bytes: inline,
                len: NonZero::<u8>::MIN.saturating_add(bytes.len() as u8 - 1),
            }))
        } else {
            Ok(Self(Kind::Owned(
                Box::try_copy(bytes).map_err(Error::Allocation)?,
            )))
        }
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: canonical variants are static ASCII and extension admission
        // retains the exact immutable bytes covered by its token contract.
        unsafe { str::from_utf8_unchecked(self.as_bytes()) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Kind::Known(known) => known.as_str().as_bytes(),
            Kind::Trace => b"TRACE",
            Kind::Query => b"QUERY",
            Kind::Inline { bytes, len } => {
                // SAFETY: the sole inline constructor admits 1..=15 immutable bytes.
                unsafe { bytes.get_unchecked(..usize::from(len.get())) }
            }
            Kind::Owned(bytes) => bytes,
        }
    }
}

impl Default for Method {
    fn default() -> Self {
        Self::GET
    }
}

impl AsRef<str> for Method {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

const _: () = assert!(size_of::<Method>() <= 3 * size_of::<usize>());

impl From<Known> for Method {
    fn from(method: Known) -> Self {
        Self(Kind::Known(method))
    }
}

/// Exact token syntax or extension storage failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Syntax(field::Error),
    Allocation(alloc::Exhausted),
}
