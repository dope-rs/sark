use sark_http::h1::codec::numbers::Wire as _;

use crate::wire::response::{Error, Writer, consts};

pub(in crate::wire) trait Framing {
    fn wire_len(&self) -> usize;
    fn write(&self, out: &mut Writer<'_>) -> Result<(), Error>;
}

pub(in crate::wire) struct Length(pub(in crate::wire) usize);

#[derive(Clone, Copy)]
pub(in crate::wire) enum Empty {
    Unframed,
    Zero,
}

impl Framing for Empty {
    fn wire_len(&self) -> usize {
        match self {
            Self::Unframed => 0,
            Self::Zero => Length(0).wire_len(),
        }
    }

    fn write(&self, out: &mut Writer<'_>) -> Result<(), Error> {
        match self {
            Self::Unframed => Ok(()),
            Self::Zero => Length(0).write(out),
        }
    }
}

impl Framing for Length {
    fn wire_len(&self) -> usize {
        consts::CONTENT_LENGTH.len() + self.0.decimal_len().get() + consts::CRLF.len()
    }

    fn write(&self, out: &mut Writer<'_>) -> Result<(), Error> {
        out.put(consts::CONTENT_LENGTH)?;
        out.put_decimal(self.0)?;
        out.put(consts::CRLF)
    }
}

const WIDTH: usize = 20;

pub(in crate::wire) struct Padded(pub(in crate::wire) usize);

impl Framing for Padded {
    fn wire_len(&self) -> usize {
        consts::CONTENT_LENGTH.len() + WIDTH + consts::CRLF.len()
    }

    fn write(&self, out: &mut Writer<'_>) -> Result<(), Error> {
        let mut digits = [b'0'; WIDTH];
        self.0.decimal(&mut digits);
        out.put(consts::CONTENT_LENGTH)?;
        out.put(&digits)?;
        out.put(consts::CRLF)
    }
}

pub(in crate::wire) struct Chunked;

impl Framing for Chunked {
    fn wire_len(&self) -> usize {
        consts::CHUNKED.len()
    }

    fn write(&self, out: &mut Writer<'_>) -> Result<(), Error> {
        out.put(consts::CHUNKED)
    }
}
