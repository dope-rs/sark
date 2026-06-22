use super::consts::{CL_PREFIX, CRLF, TE_LINE};
use super::out::Out;

pub(in crate::http::response) trait Framing {
    fn framing_len(&self) -> usize;
    fn write_framing(&self, out: &mut [u8], off: &mut usize);
}

pub(in crate::http::response) struct ContentLength<'a>(pub(in crate::http::response) &'a [u8]);

impl Framing for ContentLength<'_> {
    fn framing_len(&self) -> usize {
        CL_PREFIX.len() + self.0.len() + CRLF.len()
    }
    fn write_framing(&self, out: &mut [u8], off: &mut usize) {
        Out::put(out, off, CL_PREFIX);
        Out::put(out, off, self.0);
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
