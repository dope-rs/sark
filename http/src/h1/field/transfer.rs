use crate::{
    field::trim_ows_start,
    h1::{field::value, framing},
};

/// Stateful Transfer-Encoding validation across one or more field lines.
/// No coding may follow `chunked`, and `chunked` may occur only once.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Transfer {
    codings: u16,
    last: framing::Transfer,
}

impl Default for Transfer {
    fn default() -> Self {
        Self {
            codings: 0,
            last: framing::Transfer::None,
        }
    }
}

impl Transfer {
    pub(crate) const CHUNKED: Self = Self {
        codings: 1,
        last: framing::Transfer::Chunked,
    };

    pub fn parse(mut self, value: &[u8]) -> Result<Self, value::Error> {
        let mut rest = trim_ows_start(value);
        loop {
            if rest.is_empty() {
                return Err(value::Error::Empty);
            }
            if matches!(self.last, framing::Transfer::Chunked) {
                return Err(value::Error::InvalidOrdering);
            }
            let (name, mut tail) = value::token(rest)?;
            let coding = if name.eq_ignore_ascii_case(b"chunked") {
                framing::Transfer::Chunked
            } else {
                framing::Transfer::Other
            };
            let mut param = false;
            while let [b';', next @ ..] = tail {
                param = true;
                let (_, separator) = value::token(trim_ows_start(next))?;
                let [b'=', item @ ..] = separator else {
                    return Err(value::Error::InvalidByte);
                };
                tail = value::item(trim_ows_start(item))?;
            }
            if matches!(coding, framing::Transfer::Chunked) && param {
                return Err(value::Error::InvalidByte);
            }
            self.codings = self.codings.checked_add(1).ok_or(value::Error::Overflow)?;
            self.last = coding;
            match tail {
                [] => return Ok(self),
                [b',', next @ ..] => rest = trim_ows_start(next),
                _ => return Err(value::Error::InvalidByte),
            }
        }
    }

    pub const fn has_encoding(self) -> bool {
        self.codings != 0
    }

    pub const fn is_chunked(self) -> bool {
        matches!(self.last, framing::Transfer::Chunked)
    }

    pub(in crate::h1) const fn has_other(self) -> bool {
        self.codings > 1 || matches!(self.last, framing::Transfer::Other)
    }

    pub const fn framing(self) -> framing::Transfer {
        self.last
    }

    pub(crate) fn try_join(self, value: Self) -> Result<Self, value::Error> {
        if value.codings == 0 {
            return Err(value::Error::Empty);
        }
        if matches!(self.last, framing::Transfer::Chunked) {
            return Err(value::Error::InvalidOrdering);
        }
        let codings = self
            .codings
            .checked_add(value.codings)
            .ok_or(value::Error::Overflow)?;
        Ok(Self {
            codings,
            last: value.last,
        })
    }
}

const _: () = assert!(core::mem::size_of::<Transfer>() <= 4);
