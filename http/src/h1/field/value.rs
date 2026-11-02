use crate::field::{self, scan};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty,
    InvalidByte,
    Overflow,
    InvalidOrdering,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Match {
    Taken,
    Other,
}

pub(super) struct Parser<'a> {
    value: &'a [u8],
}

impl<'a> Parser<'a> {
    pub(super) const fn new(value: &'a [u8]) -> Self {
        Self { value }
    }

    pub(super) fn skip_ows(&mut self) {
        while let Some((b' ' | b'\t', tail)) = self.value.split_first() {
            self.value = tail;
        }
    }

    pub(super) fn is_done(&self) -> bool {
        self.value.is_empty()
    }

    pub(super) fn take(&mut self, byte: u8) -> Match {
        let Some((&first, tail)) = self.value.split_first() else {
            return Match::Other;
        };
        if first != byte {
            return Match::Other;
        }
        self.value = tail;
        Match::Taken
    }

    pub(super) fn token(&mut self) -> Result<&'a [u8], Error> {
        let (token, tail) =
            match scan::Match::find_by(self.value, |byte| !field::Error::is_name_byte(byte)) {
                Some(end) => (end.prefix(), end.tail()),
                None => (self.value, &self.value[self.value.len()..]),
            };
        if token.is_empty() {
            return Err(if self.is_done() {
                Error::Empty
            } else {
                Error::InvalidByte
            });
        }
        self.value = tail;
        Ok(token)
    }

    pub(super) fn item(&mut self) -> Result<(), Error> {
        let Some((b'"', tail)) = self.value.split_first() else {
            self.token()?;
            return Ok(());
        };
        let mut bytes = tail.iter();
        while let Some(&byte) = bytes.next() {
            match byte {
                b'"' => {
                    self.value = bytes.as_slice();
                    return Ok(());
                }
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
}
