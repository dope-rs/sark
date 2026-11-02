use core::{convert::Infallible, num::NonZero, ops::ControlFlow};

use o3::buffer::{storage::inline, write::Sink};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    NeedMore,
    Overflow(Overflow),
}

/// The exact byte and accumulator rejected by a prefixed integer continuation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Overflow {
    pub value: u64,
    pub shift: u32,
    pub byte: u8,
}

/// Base-128 continuation after a saturated HPACK/QPACK prefix.
/// Private transitions keep the shift within the `u64` bit width.
#[derive(Clone, Copy, Debug)]
pub struct Continuation {
    value: u64,
    shift: u32,
}

impl Continuation {
    pub const fn new(prefix: u64) -> Self {
        Self {
            value: prefix,
            shift: 0,
        }
    }

    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Consumes one continuation byte, preserving the accumulator on failure.
    pub fn try_push(&mut self, byte: u8) -> Result<ControlFlow<u64>, Overflow> {
        let overflow = Overflow {
            value: self.value,
            shift: self.shift,
            byte,
        };
        let chunk = u64::from(byte & 0x7f);
        if chunk > u64::MAX >> self.shift {
            return Err(overflow);
        }
        let value = self
            .value
            .checked_add(chunk << self.shift)
            .ok_or(overflow)?;
        if byte & 0x80 == 0 {
            self.value = value;
            return Ok(ControlFlow::Break(value));
        }
        let shift = self.shift + 7;
        if shift >= u64::BITS {
            return Err(overflow);
        }
        self.value = value;
        self.shift = shift;
        Ok(ControlFlow::Continue(()))
    }
}

impl From<Overflow> for Error {
    fn from(cause: Overflow) -> Self {
        Self::Overflow(cause)
    }
}

const _: () = assert!(size_of::<Continuation>() == size_of::<(u64, u32)>());

/// An HPACK/QPACK integer whose prefix width is fixed by its type.
///
/// Every `u64` is representable; construction only records the wire format.
///
/// Invalid wire widths expose no safe constructor or codec.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrefixedInt<const BITS: u8>(u64);

struct Cursor<'a> {
    output: &'a mut [u8],
    written: usize,
}

#[doc(hidden)]
pub trait Width {
    const MAX_PREFIX: u64;
}

impl Width for PrefixedInt<1> {
    const MAX_PREFIX: u64 = 1;
}
impl Width for PrefixedInt<2> {
    const MAX_PREFIX: u64 = 3;
}
impl Width for PrefixedInt<3> {
    const MAX_PREFIX: u64 = 7;
}
impl Width for PrefixedInt<4> {
    const MAX_PREFIX: u64 = 15;
}
impl Width for PrefixedInt<5> {
    const MAX_PREFIX: u64 = 31;
}
impl Width for PrefixedInt<6> {
    const MAX_PREFIX: u64 = 63;
}
impl Width for PrefixedInt<7> {
    const MAX_PREFIX: u64 = 127;
}
impl Width for PrefixedInt<8> {
    const MAX_PREFIX: u64 = 255;
}

impl<const BITS: u8> PrefixedInt<BITS>
where
    Self: Width,
{
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn encoded_len(self) -> NonZero<usize> {
        if self.0 < Self::MAX_PREFIX {
            return NonZero::<usize>::MIN;
        }
        let rest = self.0 - Self::MAX_PREFIX;
        NonZero::<usize>::MIN.saturating_add(1 + ((rest | 1).ilog2() / 7) as usize)
    }

    pub fn encode(self, prefix: u8, out: &mut impl Extend<u8>) {
        match self.emit(prefix, |byte| {
            out.extend([byte]);
            Ok::<(), Infallible>(())
        }) {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }

    /// Stops at the first rejected byte and preserves the destination's exact error.
    pub fn try_encode<W: Sink>(self, prefix: u8, out: &mut W) -> Result<(), W::Error> {
        self.emit(prefix, |byte| out.write_byte(byte))
    }

    fn emit<E>(self, prefix: u8, mut emit: impl FnMut(u8) -> Result<(), E>) -> Result<(), E> {
        let low_mask = Self::MAX_PREFIX as u8;
        let high = prefix & !low_mask;
        if self.0 < Self::MAX_PREFIX {
            return emit(high | self.0 as u8);
        }
        emit(high | low_mask)?;
        let mut rest = self.0 - Self::MAX_PREFIX;
        while rest >= 128 {
            emit(((rest & 0x7f) as u8) | 0x80)?;
            rest >>= 7;
        }
        emit(rest as u8)
    }

    /// Encodes into an inline owner whose capacity admits every value of this width.
    pub fn pack<const CAP: usize>(self, prefix: u8) -> inline::Bytes<CAP> {
        const { assert!(PrefixedInt::<BITS>::new(u64::MAX).encoded_len().get() <= CAP) }
        let mut bytes = inline::Bytes::new();
        bytes
            .fit(self.encoded_len().get())
            .write(|output| self.encode(prefix, &mut Cursor { output, written: 0 }));
        bytes
    }

    /// Advances the input only after a complete integer has been admitted.
    pub fn take(input: &mut &[u8]) -> Result<Self, Error> {
        let (value, _, tail) = Self::read(input)?;
        *input = tail;
        Ok(value)
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, NonZero<usize>), Error> {
        Self::read(buf).map(|(value, len, _)| (value, len))
    }

    fn read(input: &[u8]) -> Result<(Self, NonZero<usize>, &[u8]), Error> {
        let (&byte, mut tail) = input.split_first().ok_or(Error::NeedMore)?;
        let first = (byte & Self::MAX_PREFIX as u8) as u64;
        if first < Self::MAX_PREFIX {
            return Ok((Self(first), NonZero::<usize>::MIN, tail));
        }

        let mut number = Continuation::new(Self::MAX_PREFIX);
        let mut pos = NonZero::<usize>::MIN;
        loop {
            let (&byte, next) = tail.split_first().ok_or(Error::NeedMore)?;
            pos = pos.saturating_add(1);
            if let ControlFlow::Break(value) = number.try_push(byte)? {
                return Ok((Self(value), pos, next));
            }
            tail = next;
        }
    }
}

impl Extend<u8> for Cursor<'_> {
    fn extend<T: IntoIterator<Item = u8>>(&mut self, iter: T) {
        for byte in iter {
            // SAFETY: pack reserves this trusted codec's exact encoded length;
            // the cursor is private and only encode can write through it.
            *unsafe { self.output.get_unchecked_mut(self.written) } = byte;
            self.written += 1;
        }
    }
}

impl<const BITS: u8> From<PrefixedInt<BITS>> for u64
where
    PrefixedInt<BITS>: Width,
{
    fn from(value: PrefixedInt<BITS>) -> Self {
        value.get()
    }
}
