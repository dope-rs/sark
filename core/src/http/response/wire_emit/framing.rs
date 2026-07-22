use super::WireWriter;
use super::consts::{CL_PREFIX, CRLF, TE_LINE};
use crate::http::codec::Wire;

pub(in crate::http::response) trait Framing {
    fn framing_len(&self) -> usize;
    fn write_framing(&self, out: &mut WireWriter<'_>);
}

pub(in crate::http::response) struct ContentLength(pub(in crate::http::response) usize);

impl Framing for ContentLength {
    fn framing_len(&self) -> usize {
        CL_PREFIX.len() + Wire::decimal_len(self.0) + CRLF.len()
    }
    fn write_framing(&self, out: &mut WireWriter<'_>) {
        out.put(CL_PREFIX);
        out.put_decimal(self.0);
        out.put(CRLF);
    }
}

pub(in crate::http::response) struct TransferEncodingChunked;

impl Framing for TransferEncodingChunked {
    fn framing_len(&self) -> usize {
        TE_LINE.len()
    }
    fn write_framing(&self, out: &mut WireWriter<'_>) {
        out.put(TE_LINE);
    }
}
