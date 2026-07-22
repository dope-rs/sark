use super::consts::{CRLF, DATE_PREFIX, SERVER_LINE, STATUS_LINE_PREFIX};

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

    pub(in crate::http::response) fn put_status_line(&mut self, status_str: &[u8], reason: &[u8]) {
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
