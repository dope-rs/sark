//! QPACK decoder-stream wire grammar.
use core::num::NonZero;

use sark_http::field::metadata::integer;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// Exact decoder-stream capacity shortage.
pub struct Capacity {
    pub len: u8,
    pub additional: u8,
    pub limit: u8,
}

/// Exact rejection observed while consuming the peer's QPACK decoder stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Integer {
        prefix: u8,
        available: usize,
        cause: integer::Overflow,
    },
    ZeroIncrement,
    UnknownSection {
        stream_id: u64,
    },
    IncrementOverflow {
        known: u64,
        increment: u64,
    },
    IncrementBeyondTable {
        known: u64,
        increment: u64,
        inserted: u64,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Instruction {
    SectionAcknowledgment { stream_id: u64 },
    StreamCancellation { stream_id: u64 },
    InsertCountIncrement { increment: NonZero<u64> },
}

const _: () = {
    assert!(core::mem::size_of::<Capacity>() == 3);
    assert!(core::mem::size_of::<Error>() <= 5 * core::mem::size_of::<usize>());
};

impl Instruction {
    pub fn encoded_len(&self) -> usize {
        match self {
            Self::SectionAcknowledgment { stream_id: id } => {
                integer::PrefixedInt::<7>::new(*id).encoded_len().get()
            }
            Self::StreamCancellation { stream_id: id } => {
                integer::PrefixedInt::<6>::new(*id).encoded_len().get()
            }
            Self::InsertCountIncrement { increment: delta } => {
                integer::PrefixedInt::<6>::new(delta.get())
                    .encoded_len()
                    .get()
            }
        }
    }

    pub fn encode(&self, out: &mut impl Extend<u8>) {
        match self {
            Self::SectionAcknowledgment { stream_id: id } => {
                integer::PrefixedInt::<7>::new(*id).encode(0x80, out);
            }
            Self::StreamCancellation { stream_id: id } => {
                integer::PrefixedInt::<6>::new(*id).encode(0x40, out);
            }
            Self::InsertCountIncrement { increment: delta } => {
                integer::PrefixedInt::<6>::new(delta.get()).encode(0, out);
            }
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Option<(Self, usize)>, Error> {
        let mut tail = buf;
        Ok(Self::take(&mut tail)?.map(|value| (value, buf.len() - tail.len())))
    }

    /// Leaves incomplete input untouched and retains the exact parsed suffix.
    pub(in crate::qpack) fn take(input: &mut &[u8]) -> Result<Option<Self>, Error> {
        let mut tail = *input;
        let Some(&first) = tail.first() else {
            return Ok(None);
        };
        let value = if first & 0x80 != 0 {
            let Some(id) = Self::integer::<7>(&mut tail)? else {
                return Ok(None);
            };
            Self::SectionAcknowledgment { stream_id: id }
        } else if first & 0xc0 == 0x40 {
            let Some(id) = Self::integer::<6>(&mut tail)? else {
                return Ok(None);
            };
            Self::StreamCancellation { stream_id: id }
        } else {
            let Some(delta) = Self::integer::<6>(&mut tail)? else {
                return Ok(None);
            };
            let delta = NonZero::new(delta).ok_or(Error::ZeroIncrement)?;
            Self::InsertCountIncrement { increment: delta }
        };
        *input = tail;
        Ok(Some(value))
    }

    fn integer<const BITS: u8>(input: &mut &[u8]) -> Result<Option<u64>, Error>
    where
        integer::PrefixedInt<BITS>: integer::Width,
    {
        let len = input.len();
        match integer::PrefixedInt::<BITS>::take(input) {
            Ok(value) => Ok(Some(value.get())),
            Err(integer::Error::NeedMore) => Ok(None),
            Err(integer::Error::Overflow(cause)) => Err(Error::Integer {
                prefix: BITS,
                available: len,
                cause,
            }),
        }
    }
}
