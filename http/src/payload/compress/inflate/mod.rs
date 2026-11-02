use std::{convert::Infallible, mem};

use miniz_oxide::inflate::TINFLStatus;
use o3::buffer::resident;

use crate::payload::crc32::Hasher;

mod checked;
mod gzip;
mod input;
pub use checked::Checked;
pub use gzip::Gzip;
use input::Input;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Part {
    Header,
    Extra,
    Name,
    Comment,
    HeaderChecksum,
    Deflate,
    Trailer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderFault {
    Magic { actual: [u8; 2] },
    Method { actual: u8 },
    Flags { actual: u8 },
    Checksum { expected: u16, actual: u16 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Count {
    Exact(usize),
    Overflow,
}

impl Count {
    const ZERO: Self = Self::Exact(0);

    fn add(self, amount: usize) -> Self {
        match self {
            Self::Exact(current) => match current.checked_add(amount) {
                Some(next) => Self::Exact(next),
                None => Self::Overflow,
            },
            Self::Overflow => Self::Overflow,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Header {
        offset: Count,
        cause: HeaderFault,
    },
    Eof {
        part: Part,
        offset: Count,
    },
    Inflate {
        offset: Count,
        output: usize,
        cause: TINFLStatus,
    },
    Checksum {
        expected: u32,
        actual: u32,
    },
    Length {
        expected: usize,
        actual: usize,
    },
    Output {
        capacity: usize,
        written: usize,
    },
    Trailing {
        bytes: Count,
    },
}

/// Immutable gzip members whose first header was validated before output admission.
pub struct Plan<'a, I>
where
    I: Iterator<Item = &'a [u8]>,
{
    input: Input<'a, I>,
    size_hint: usize,
}

impl<'a, I: Iterator<Item = &'a [u8]>> Plan<'a, I> {
    /// Validates the header while retaining the segmented source.
    /// `hint` is the untrusted final member size, not the combined decoded size.
    pub fn try_segments(segments: I, hint: usize) -> Result<Self, Failure> {
        Self::try_header::<false, Infallible>(segments, hint, None, &mut || Ok(()))
            .map_err(Checked::into_work)
    }

    fn try_limit(self, limit: usize) -> Result<Self, DecodeError> {
        if self.size_hint > limit {
            return Err(DecodeError::SizeLimit {
                actual: self.size_hint,
                limit,
            });
        }
        Ok(self)
    }

    fn try_header<const CHECK: bool, E>(
        segments: I,
        hint: usize,
        checksum: Option<&Hasher>,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Self, Checked<Failure, E>> {
        let mut input = Input::new(segments);
        input
            .header::<CHECK, E>(checksum, check)
            .map_err(|error| error.map_work(|cause| Failure::new(cause, 0)))?;
        Ok(Self {
            input,
            size_hint: hint,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Failure {
    cause: Error,
    written: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    Stream(Failure),
    SizeLimit {
        actual: usize,
        limit: usize,
    },
    Allocation {
        requested: usize,
        available: usize,
        cause: resident::AllocError,
    },
}

impl From<Failure> for DecodeError {
    fn from(failure: Failure) -> Self {
        Self::Stream(failure)
    }
}

impl Failure {
    const fn new(cause: Error, written: usize) -> Self {
        Self { cause, written }
    }

    pub const fn error(&self) -> Error {
        self.cause
    }

    pub const fn written(&self) -> usize {
        self.written
    }
}

const _: () = assert!(mem::size_of::<Count>() == 2 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Error>() <= 5 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Failure>() <= 6 * mem::size_of::<usize>());
