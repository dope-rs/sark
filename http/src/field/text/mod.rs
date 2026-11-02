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
        let (value, end) = self.parse_prefix()?;
        if end != self.bytes.len() {
            return Err(IntError::InvalidByte);
        }
        Ok(value)
    }

    pub fn parse_prefix(&self) -> Result<(usize, usize), IntError> {
        parse(self.bytes, 0usize, |value, digit| {
            value.checked_mul(10)?.checked_add(usize::from(digit))
        })
    }

    pub fn parse_u64(&self) -> Result<u64, IntError> {
        let (value, end) = parse(self.bytes, 0u64, |value, digit| {
            value.checked_mul(10)?.checked_add(u64::from(digit))
        })?;
        if end != self.bytes.len() {
            return Err(IntError::InvalidByte);
        }
        Ok(value)
    }
}

fn parse<T>(
    bytes: &[u8],
    zero: T,
    mut push: impl FnMut(T, u8) -> Option<T>,
) -> Result<(T, usize), IntError> {
    if bytes.is_empty() {
        return Err(IntError::Empty);
    }

    let mut value = zero;
    let mut end = 0usize;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            break;
        }
        value = push(value, byte - b'0').ok_or(IntError::Overflow)?;
        end += 1;
    }
    if end == 0 {
        return Err(IntError::InvalidByte);
    }
    Ok((value, end))
}
