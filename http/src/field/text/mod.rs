//! ASCII field and request-value parsing helpers.

pub mod percent;

pub(crate) const fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub struct Ascii<'a> {
    bytes: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntError {
    Empty,
    InvalidByte,
    Overflow,
}

impl<'a> Ascii<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn parse_usize(&self) -> Result<usize, IntError> {
        Ok(parse::<{ usize::MAX as u64 }, false>(self.bytes)?.0 as usize)
    }

    pub fn parse_prefix(&self) -> Result<(usize, usize), IntError> {
        let (value, end) = parse::<{ usize::MAX as u64 }, true>(self.bytes)?;
        Ok((value as usize, end))
    }

    pub fn parse_u64(&self) -> Result<u64, IntError> {
        Ok(parse::<{ u64::MAX }, false>(self.bytes)?.0)
    }
}

fn parse<const MAX: u64, const PREFIX: bool>(bytes: &[u8]) -> Result<(u64, usize), IntError> {
    if bytes.is_empty() {
        return Err(IntError::Empty);
    }

    let checked = bytes.len() > MAX.ilog10() as usize;
    let mut value = 0u64;
    let mut end = 0usize;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            if PREFIX && end != 0 {
                break;
            }
            return Err(IntError::InvalidByte);
        }
        let digit = u64::from(byte - b'0');
        if checked && (value > MAX / 10 || value == MAX / 10 && digit > MAX % 10) {
            return Err(IntError::Overflow);
        }
        value = value * 10 + digit;
        if PREFIX {
            end += 1;
        }
    }
    Ok((value, end))
}
