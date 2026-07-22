use std::ops::ControlFlow;

use crate::error::{Error, Result};

#[derive(Clone, Copy)]
pub(super) enum CsvScanMode {
    Lenient,
    Strict,
}

pub struct Header;

impl Header {
    pub(super) fn is_ascii_ws(b: u8) -> bool {
        b == b' ' || b == b'\t'
    }

    pub(super) fn trim_ascii_ws(value: &[u8]) -> &[u8] {
        let mut start = 0usize;
        let mut end = value.len();
        while start < end && Self::is_ascii_ws(value[start]) {
            start += 1;
        }
        while end > start && Self::is_ascii_ws(value[end - 1]) {
            end -= 1;
        }
        &value[start..end]
    }

    pub(super) fn scan_csv_tokens(
        value: &[u8],
        mode: CsvScanMode,
        mut visit: impl FnMut(&[u8]) -> ControlFlow<()>,
    ) -> Result<()> {
        match mode {
            CsvScanMode::Strict => {
                let mut start = 0usize;
                let mut saw_token = false;
                let mut i = 0usize;
                while i <= value.len() {
                    if i == value.len() || value[i] == b',' {
                        let token = Self::trim_ascii_ws(&value[start..i]);
                        if token.is_empty() {
                            return Err(Error::BadRequest("Invalid Transfer-Encoding".into()));
                        }
                        saw_token = true;
                        if visit(token).is_break() {
                            return Ok(());
                        }
                        start = i.saturating_add(1);
                    }
                    i += 1;
                }
                if !saw_token {
                    return Err(Error::BadRequest("Invalid Transfer-Encoding".into()));
                }
                Ok(())
            }
            CsvScanMode::Lenient => {
                let mut i = 0usize;
                while i < value.len() {
                    while i < value.len() && (Self::is_ascii_ws(value[i]) || value[i] == b',') {
                        i += 1;
                    }
                    if i >= value.len() {
                        break;
                    }
                    let start = i;
                    while i < value.len() && value[i] != b',' {
                        i += 1;
                    }
                    let token = Self::trim_ascii_ws(&value[start..i]);
                    if token.is_empty() {
                        continue;
                    }
                    if visit(token).is_break() {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    pub fn has_token(value: &[u8], token: &[u8]) -> bool {
        if token.is_empty() {
            return false;
        }
        let mut found = false;
        let _ = Self::scan_csv_tokens(value, CsvScanMode::Lenient, |part| {
            if part.eq_ignore_ascii_case(token) {
                found = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        found
    }

    pub(super) fn trimmed_eq_ascii_case(value: &[u8], expected: &[u8]) -> bool {
        Self::trim_ascii_ws(value).eq_ignore_ascii_case(expected)
    }

    pub fn content_length(value: &[u8]) -> Result<usize> {
        if value.is_empty() {
            return Err(Error::BadRequest("Invalid Content-Length".into()));
        }
        let mut len: usize = 0;
        for &b in value {
            if !b.is_ascii_digit() {
                return Err(Error::BadRequest("Invalid Content-Length".into()));
            }
            len = len
                .checked_mul(10)
                .and_then(|n| n.checked_add((b - b'0') as usize))
                .ok_or_else(|| Error::BadRequest("Invalid Content-Length".into()))?;
        }
        Ok(len)
    }

    pub fn has_name(headers: &[httparse::Header<'_>], name: &str) -> bool {
        headers.iter().any(|h| h.name.eq_ignore_ascii_case(name))
    }
}

pub trait HeaderLookup: private::SealedHeaderLookup {
    fn header_value(&self, name: http::header::HeaderName) -> Option<&http::header::HeaderValue>;

    fn has_token(&self, name: http::header::HeaderName, token: &str) -> bool {
        match self.header_value(name) {
            Some(v) => Header::has_token(v.as_bytes(), token.as_bytes()),
            None => false,
        }
    }

    fn value_eq_ascii_case(&self, name: http::header::HeaderName, expected: &str) -> bool {
        self.header_value(name)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(expected.as_bytes()))
    }
}

impl HeaderLookup for http::HeaderMap {
    fn header_value(&self, name: http::header::HeaderName) -> Option<&http::header::HeaderValue> {
        self.get(name)
    }
}

impl HeaderLookup for crate::http::HeaderList {
    fn header_value(&self, name: http::header::HeaderName) -> Option<&http::header::HeaderValue> {
        self.get(name)
    }
}

mod private {
    pub trait SealedHeaderLookup {}

    impl SealedHeaderLookup for http::HeaderMap {}
    impl SealedHeaderLookup for crate::http::HeaderList {}
}
