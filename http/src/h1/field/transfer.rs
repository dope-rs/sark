use crate::h1::{
    field::{
        value,
        value::{Match, Parser},
    },
    framing,
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
    pub fn parse(mut self, value: &[u8]) -> Result<Self, value::Error> {
        let mut parser = Parser::new(value);
        parser.skip_ows();
        if parser.is_done() {
            return Err(value::Error::Empty);
        }
        loop {
            if matches!(self.last, framing::Transfer::Chunked) {
                return Err(value::Error::InvalidOrdering);
            }
            let coding = if parser.token()?.eq_ignore_ascii_case(b"chunked") {
                framing::Transfer::Chunked
            } else {
                framing::Transfer::Other
            };
            parser.skip_ows();
            let mut param = false;
            while parser.take(b';') == Match::Taken {
                param = true;
                parser.skip_ows();
                parser.token()?;
                parser.skip_ows();
                if parser.take(b'=') == Match::Other {
                    return Err(value::Error::InvalidByte);
                }
                parser.skip_ows();
                parser.item()?;
                parser.skip_ows();
            }
            if matches!(coding, framing::Transfer::Chunked) && param {
                return Err(value::Error::InvalidByte);
            }
            self.codings = self.codings.checked_add(1).ok_or(value::Error::Overflow)?;
            self.last = coding;
            if parser.is_done() {
                return Ok(self);
            }
            if parser.take(b',') == Match::Other {
                return Err(value::Error::InvalidByte);
            }
            parser.skip_ows();
            if parser.is_done() {
                return Err(value::Error::Empty);
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
