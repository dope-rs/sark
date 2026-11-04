use std::{convert::Infallible, marker::PhantomData};

use crate::Error;

#[derive(Clone, Copy)]
pub(crate) enum Checked {}

/// Internal replay mode; its enclosing owner state retains the admitted range.
#[derive(Clone, Copy)]
pub(crate) enum Admitted {}

pub(crate) trait Validation {
    type Error;
    const CHECK: bool;
    fn require(valid: bool) -> Result<(), Self::Error>;
}

impl Validation for Checked {
    type Error = Error;
    const CHECK: bool = true;
    fn require(valid: bool) -> crate::Result<()> {
        if valid { Ok(()) } else { Err(Error::Invalid) }
    }
}

impl Validation for Admitted {
    type Error = Infallible;
    const CHECK: bool = false;
    fn require(_: bool) -> Result<(), Infallible> {
        Ok(())
    }
}

/// Incremental string grammar, independent of the backing owner and segment boundaries.
#[derive(Clone, Copy)]
pub(crate) struct String<M = Checked> {
    escape: Escape,
    utf8: Utf8,
    removed: usize,
    mode: PhantomData<M>,
}

/// String traversal bound to an input that either needs admission or already has it.
#[derive(Clone, Copy)]
pub(crate) enum Scan {
    Checked(String),
    Admitted(String<Admitted>),
}

impl Scan {
    pub(crate) fn new(admitted: bool) -> Self {
        if admitted {
            Self::Admitted(String::new())
        } else {
            Self::Checked(String::new())
        }
    }

    pub(crate) fn skip<'a>(&self, chunk: &'a [u8]) -> &'a [u8] {
        match self {
            Self::Checked(string) => string.skip(chunk),
            Self::Admitted(string) => string.skip(chunk),
        }
    }

    pub(crate) fn removed(&self) -> usize {
        match self {
            Self::Checked(string) => string.removed,
            Self::Admitted(string) => string.removed,
        }
    }

    pub(crate) fn step(&mut self, byte: u8) -> crate::Result<Option<Decoded>> {
        match self {
            Self::Checked(string) => string.step(byte),
            Self::Admitted(string) => {
                if matches!(string.escape, Escape::Plain) && byte == b'"' {
                    return Ok(None);
                }
                let decoded = string.decode(byte);
                string.removed += 1;
                string.removed -= decoded.as_slice().len();
                Ok(Some(decoded))
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Escape {
    Plain,
    Start,
    Hex { value: u16, left: u8, high: u16 },
    Slash(u16),
    Unicode(u16),
}

#[derive(Clone, Copy)]
struct Utf8 {
    left: u8,
    min: u8,
    max: u8,
}

pub(crate) struct Decoded {
    bytes: [u8; 4],
    len: u8,
}

impl Decoded {
    fn empty() -> Self {
        Self {
            bytes: [0; 4],
            len: 0,
        }
    }

    fn byte(byte: u8) -> Self {
        Self {
            bytes: [byte, 0, 0, 0],
            len: 1,
        }
    }

    fn scalar(value: u32) -> Self {
        if value < 0x80 {
            Self::byte(value as u8)
        } else if value < 0x800 {
            Self {
                bytes: [
                    (0xc0 | (value >> 6)) as u8,
                    (0x80 | (value & 0x3f)) as u8,
                    0,
                    0,
                ],
                len: 2,
            }
        } else if value < 0x10000 {
            Self {
                bytes: [
                    (0xe0 | (value >> 12)) as u8,
                    (0x80 | ((value >> 6) & 0x3f)) as u8,
                    (0x80 | (value & 0x3f)) as u8,
                    0,
                ],
                len: 3,
            }
        } else {
            Self {
                bytes: [
                    (0xf0 | (value >> 18)) as u8,
                    (0x80 | ((value >> 12) & 0x3f)) as u8,
                    (0x80 | ((value >> 6) & 0x3f)) as u8,
                    (0x80 | (value & 0x3f)) as u8,
                ],
                len: 4,
            }
        }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    pub(in crate::source) fn into_parts(self) -> (u32, u8) {
        (u32::from_le_bytes(self.bytes), self.len)
    }
}

impl String {
    pub(crate) const fn removed(&self) -> usize {
        self.removed
    }

    pub(crate) fn step(&mut self, byte: u8) -> crate::Result<Option<Decoded>> {
        if matches!(self.escape, Escape::Plain) && self.utf8.left == 0 && byte == b'"' {
            return Ok(None);
        }
        self.content(byte).map(Some)
    }
}

impl String<Admitted> {
    pub(crate) fn decode(&mut self, byte: u8) -> Decoded {
        match self.content(byte) {
            Ok(decoded) => decoded,
            Err(error) => match error {},
        }
    }
}

impl<M: Validation> String<M> {
    pub(crate) const fn new() -> Self {
        Self {
            escape: Escape::Plain,
            utf8: Utf8 {
                left: 0,
                min: 0x80,
                max: 0xbf,
            },
            removed: 0,
            mode: PhantomData,
        }
    }

    /// Returns an admitted ASCII run that leaves every state field unchanged.
    pub(crate) fn skip<'a>(&self, chunk: &'a [u8]) -> &'a [u8] {
        if !matches!(self.escape, Escape::Plain) || self.utf8.left != 0 {
            return &chunk[..0];
        }
        let words = chunk.as_chunks::<8>().0;
        let start = words
            .iter()
            .position(|word| !crate::bytes::is_plain::<true>(u64::from_ne_bytes(*word)))
            .unwrap_or(words.len())
            * 8;
        // SAFETY: the word index is bounded by this input's complete chunks.
        let rest = unsafe { chunk.get_unchecked(start..) };
        let len = rest
            .iter()
            .position(|byte| !matches!(byte, 0x20..=0x7f) || matches!(byte, b'"' | b'\\'))
            .unwrap_or(rest.len());
        // SAFETY: len is bounded by rest, so start + len remains within this input.
        unsafe { chunk.get_unchecked(..start + len) }
    }

    fn content(&mut self, byte: u8) -> Result<Decoded, M::Error> {
        let output = match self.escape {
            Escape::Plain => {
                if M::CHECK && self.utf8.left != 0 {
                    M::require(byte >= self.utf8.min && byte <= self.utf8.max)?;
                    self.utf8.left -= 1;
                    self.utf8.min = 0x80;
                    self.utf8.max = 0xbf;
                    Decoded::byte(byte)
                } else if byte == b'\\' {
                    self.escape = Escape::Start;
                    Decoded::empty()
                } else if byte < 0x80 || !M::CHECK {
                    M::require(byte >= 0x20)?;
                    Decoded::byte(byte)
                } else {
                    let (left, min, max) = match byte {
                        0xc2..=0xdf => (1, 0x80, 0xbf),
                        0xe0 => (2, 0xa0, 0xbf),
                        0xe1..=0xec | 0xee..=0xef => (2, 0x80, 0xbf),
                        0xed => (2, 0x80, 0x9f),
                        0xf0 => (3, 0x90, 0xbf),
                        0xf1..=0xf3 => (3, 0x80, 0xbf),
                        0xf4 => (3, 0x80, 0x8f),
                        _ => {
                            M::require(false)?;
                            (0, 0, 0)
                        }
                    };
                    self.utf8 = Utf8 { left, min, max };
                    Decoded::byte(byte)
                }
            }
            Escape::Start => {
                if byte == b'u' {
                    self.escape = Escape::Hex {
                        value: 0,
                        left: 4,
                        high: 0,
                    };
                    Decoded::empty()
                } else {
                    let decoded = match byte {
                        b'"' | b'\\' | b'/' => byte,
                        b'b' => 0x08,
                        b'f' => 0x0c,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        _ => {
                            M::require(false)?;
                            0
                        }
                    };
                    self.escape = Escape::Plain;
                    Decoded::byte(decoded)
                }
            }
            Escape::Hex { value, left, high } => {
                M::require(byte.is_ascii_hexdigit())?;
                let digit = (byte & 0x0f) + 9 * (byte >> 6);
                let value = (value << 4) | u16::from(digit);
                if left != 1 {
                    self.escape = Escape::Hex {
                        value,
                        left: left - 1,
                        high,
                    };
                    Decoded::empty()
                } else if high != 0 {
                    M::require((0xdc00..=0xdfff).contains(&value))?;
                    self.escape = Escape::Plain;
                    Decoded::scalar(
                        0x10000 + ((u32::from(high & 0x03ff) << 10) | u32::from(value & 0x03ff)),
                    )
                } else if (0xd800..=0xdbff).contains(&value) {
                    self.escape = Escape::Slash(value);
                    Decoded::empty()
                } else {
                    self.escape = Escape::Plain;
                    M::require(!(0xdc00..=0xdfff).contains(&value))?;
                    Decoded::scalar(u32::from(value))
                }
            }
            Escape::Slash(high) => {
                M::require(byte == b'\\')?;
                self.escape = Escape::Unicode(high);
                Decoded::empty()
            }
            Escape::Unicode(high) => {
                M::require(byte == b'u')?;
                self.escape = Escape::Hex {
                    value: 0,
                    left: 4,
                    high,
                };
                Decoded::empty()
            }
        };
        if M::CHECK {
            self.removed += 1;
            self.removed -= output.as_slice().len();
        }
        Ok(output)
    }
}

const _: () = assert!(size_of::<String<Admitted>>() == size_of::<String>());
