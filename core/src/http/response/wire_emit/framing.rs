use super::consts::{CL_PREFIX, CRLF, TE_LINE};
use super::out::Out;
use crate::http::codec::Wire;

pub(in crate::http::response) trait Framing {
    fn framing_len(&self) -> usize;
    fn write_framing(&self, out: &mut [u8], off: &mut usize);
}

pub(in crate::http::response) struct ContentLength(pub(in crate::http::response) usize);

impl Framing for ContentLength {
    fn framing_len(&self) -> usize {
        CL_PREFIX.len() + Wire::decimal_len(self.0) + CRLF.len()
    }
    fn write_framing(&self, out: &mut [u8], off: &mut usize) {
        Out::put(out, off, CL_PREFIX);
        *off += Wire::write_dec(self.0, &mut out[*off..]);
        Out::put(out, off, CRLF);
    }
}

pub(in crate::http::response) struct TransferEncodingChunked;

impl Framing for TransferEncodingChunked {
    fn framing_len(&self) -> usize {
        TE_LINE.len()
    }
    fn write_framing(&self, out: &mut [u8], off: &mut usize) {
        Out::put(out, off, TE_LINE);
    }
}
