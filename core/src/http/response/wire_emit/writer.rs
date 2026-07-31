use http::StatusCode;

use super::consts::{CRLF, DATE_PREFIX, SERVER_LINE, STATUS_LINE_PREFIX};

const OK_STATUS_LINE: &[u8] = b"HTTP/1.1 200 OK\r\n";

pub(in crate::http::response) fn status_line_len(status: StatusCode) -> usize {
    if status == StatusCode::OK {
        OK_STATUS_LINE.len()
    } else {
        STATUS_LINE_PREFIX.len() + status.as_str().len() + 1 + reason(status).len() + CRLF.len()
    }
}

fn reason(status: StatusCode) -> &'static [u8] {
    status.canonical_reason().map(str::as_bytes).unwrap_or(b"")
}

pub(in crate::http::response) struct WireWriter<'a> {
    out: &'a mut [u8],
    offset: usize,
}

impl<'a> WireWriter<'a> {
    pub(in crate::http::response) fn new(out: &'a mut [u8]) -> Self {
        Self { out, offset: 0 }
    }

    pub(in crate::http::response) fn at(out: &'a mut [u8], offset: usize) -> Self {
        Self { out, offset }
    }

    pub(in crate::http::response) fn len(&self) -> usize {
        self.offset
    }

    pub(in crate::http::response) fn put(&mut self, bytes: &[u8]) {
        let end = self.offset + bytes.len();
        self.out[self.offset..end].copy_from_slice(bytes);
        self.offset = end;
    }

    pub(in crate::http::response) fn put_decimal(&mut self, value: usize) {
        self.offset += crate::http::codec::Wire::write_dec(value, &mut self.out[self.offset..]);
    }

    pub(in crate::http::response) fn put_status_line(&mut self, status: StatusCode) {
        if status == StatusCode::OK {
            self.put(OK_STATUS_LINE);
            return;
        }
        let status_str = status.as_str().as_bytes();
        let reason = reason(status);
        self.put(STATUS_LINE_PREFIX);
        self.put(status_str);
        self.put(b" ");
        self.put(reason);
        self.put(CRLF);
    }

    pub(in crate::http::response) fn put_server_date_terminator(
        &mut self,
        date: &[u8; 29],
    ) -> usize {
        self.put(SERVER_LINE);
        self.put(DATE_PREFIX);
        let date_offset = self.offset;
        self.put(date);
        self.put(CRLF);
        self.put(CRLF);
        date_offset
    }
}
