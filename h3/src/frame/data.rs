use o3::buffer::write;

use crate::frame::{Header, ParseError, TYPE_DATA, Wire};

pub(crate) struct Data<'a> {
    header: Header,
    payload: &'a [u8],
}

impl<'a> Data<'a> {
    pub(crate) fn new(payload: &'a [u8]) -> Result<Self, ParseError> {
        Ok(Self {
            header: Header::for_payload(TYPE_DATA, payload.len())?,
            payload,
        })
    }
}

impl Wire for Data<'_> {
    fn encoded_len(&self) -> usize {
        self.header.header_len + self.payload.len()
    }

    fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        self.header.encode(out)?;
        out.write_slice(self.payload)
    }
}
