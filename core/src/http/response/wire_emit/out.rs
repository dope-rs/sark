use super::consts::{CRLF, DATE_PREFIX, SERVER_LINE, STATUS_LINE_PREFIX};

pub(in crate::http::response) struct Out;

impl Out {
    pub(in crate::http::response) fn put(out: &mut [u8], off: &mut usize, bytes: &[u8]) {
        let n = bytes.len();
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), out.as_mut_ptr().add(*off), n);
        }
        *off += n;
    }

    pub(in crate::http::response) fn put_status_line(
        out: &mut [u8],
        off: &mut usize,
        status_str: &[u8],
        reason: &[u8],
    ) {
        Self::put(out, off, STATUS_LINE_PREFIX);
        Self::put(out, off, status_str);
        Self::put(out, off, b" ");
        Self::put(out, off, reason);
        Self::put(out, off, CRLF);
    }

    pub(in crate::http::response) fn put_server_date_terminator(
        out: &mut [u8],
        off: &mut usize,
        date: &[u8; 29],
    ) -> usize {
        Self::put(out, off, SERVER_LINE);
        Self::put(out, off, DATE_PREFIX);
        let date_offset = *off;
        Self::put(out, off, date);
        Self::put(out, off, CRLF);
        Self::put(out, off, CRLF);
        date_offset
    }
}
