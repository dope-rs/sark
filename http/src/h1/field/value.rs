use crate::field::{self, scan};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty,
    InvalidByte,
    Overflow,
    InvalidOrdering,
}

pub(super) fn token(value: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    let (token, tail) = scan::split_token(value);
    if token.is_empty() {
        return Err(if value.is_empty() {
            Error::Empty
        } else {
            Error::InvalidByte
        });
    }
    Ok((token, field::trim_ows_start(tail)))
}

pub(super) fn item(value: &[u8]) -> Result<&[u8], Error> {
    let Some((b'"', tail)) = value.split_first() else {
        return token(value).map(|(_, tail)| tail);
    };
    let mut bytes = tail.iter();
    while let Some(&byte) = bytes.next() {
        match byte {
            b'"' => return Ok(field::trim_ows_start(bytes.as_slice())),
            b'\\' => {
                let escaped = bytes.next().ok_or(Error::InvalidByte)?;
                if !matches!(escaped, b'\t' | b' ' | 0x21..=0x7e | 0x80..=0xff) {
                    return Err(Error::InvalidByte);
                }
            }
            b'\t' | b' ' | b'!' | 0x23..=0x5b | 0x5d..=0x7e | 0x80..=0xff => {}
            0..=8 | 10..=31 | 0x7f => return Err(Error::InvalidByte),
        }
    }
    Err(Error::InvalidByte)
}
