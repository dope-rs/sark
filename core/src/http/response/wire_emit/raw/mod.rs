use std::{ptr, slice};

use http::StatusCode;

use super::consts::{CRLF, DATE_PREFIX, SERVER_LINE, STATUS_LINE_PREFIX};
use crate::http::codec::Wire;

const OK_STATUS_LINE: &[u8] = b"HTTP/1.1 200 OK\r\n";

fn reason(status: StatusCode) -> &'static [u8] {
    status.canonical_reason().map(str::as_bytes).unwrap_or(b"")
}

pub(in crate::http::response) struct ProvenWireWriter<'a> {
    out: &'a mut [u8],
    offset: usize,
}

impl<'a> ProvenWireWriter<'a> {
    pub(in crate::http::response) fn status_line_len(status: StatusCode) -> usize {
        if status == StatusCode::OK {
            OK_STATUS_LINE.len()
        } else {
            STATUS_LINE_PREFIX.len() + status.as_str().len() + 1 + reason(status).len() + CRLF.len()
        }
    }

    pub(in crate::http::response) fn new(out: &'a mut [u8], limit: usize) -> Option<Self> {
        if out.len() < limit {
            return None;
        }
        Some(Self { out, offset: 0 })
    }

    pub(in crate::http::response) fn exact(out: &'a mut [u8]) -> Self {
        Self { out, offset: 0 }
    }

    pub(in crate::http::response) fn len(&self) -> usize {
        self.offset
    }

    pub(in crate::http::response) fn finish(self, expected: usize) -> usize {
        debug_assert_eq!(self.offset, expected);
        self.offset
    }

    pub(in crate::http::response) fn put(&mut self, bytes: &[u8]) {
        let end = self.offset + bytes.len();
        debug_assert!(end <= self.out.len());
        // SAFETY: construction proves the planned wire length fits in `out`. The sealed
        // response emitters account for these same immutable byte segments in
        // their wire length before emission, so every segment ends at or before
        // planned length. Keeping this copy here preserves that proof across all
        // response shapes without repeating a release-mode bounds check.
        unsafe {
            ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.out.as_mut_ptr().add(self.offset),
                bytes.len(),
            );
        }
        self.offset = end;
    }

    pub(in crate::http::response) fn put_decimal(&mut self, value: usize) {
        let len = Wire::decimal_len(value);
        let end = self.offset + len;
        debug_assert!(end <= self.out.len());

        let mut value = value;
        let mut cursor = end;
        loop {
            cursor -= 1;
            // SAFETY: `decimal_len` is the exact number of bytes emitted and
            // the response plan reserved that same number before construction.
            unsafe {
                self.out
                    .as_mut_ptr()
                    .add(cursor)
                    .write(b'0' + (value % 10) as u8);
            }
            value /= 10;
            if cursor == self.offset {
                break;
            }
        }
        self.offset = end;
    }

    pub(in crate::http::response) fn take_exact(&mut self, len: usize) -> &mut [u8] {
        let end = self.offset + len;
        debug_assert!(end <= self.out.len());
        // SAFETY: this is the one mutable subrange reserved for an encoder in
        // the already-proven plan. Advancing the cursor before returning it
        // prevents the writer from handing out an overlapping range later.
        let out = unsafe { slice::from_raw_parts_mut(self.out.as_mut_ptr().add(self.offset), len) };
        self.offset = end;
        out
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
