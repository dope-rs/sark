use std::ops::ControlFlow;

use crate::error::{Error, Result};
use crate::http::codec::Header;
use crate::http::codec::header_utils::CsvScanMode;

#[derive(Clone, Copy)]
pub(super) struct TransferEncodingInfo {
    pub(super) has_transfer_encoding: bool,
    pub(super) is_chunked: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HeaderScan {
    pub content_length: Option<usize>,
    pub has_transfer_encoding: bool,
    pub is_chunked_transfer: bool,
    pub has_expect: bool,
    pub expect_continue: bool,
    pub duplicate_content_length: bool,
    pub accept_encoding_gzip: bool,
}

impl crate::http::codec::Parse {
    pub(super) fn parse_transfer_encoding_value(value: &[u8]) -> Result<TransferEncodingInfo> {
        let mut saw_any = false;
        let mut saw_chunked = false;
        let mut last_is_chunked = false;
        let mut invalid_encoding = false;
        let mut invalid_ordering = false;

        Header::scan_csv_tokens(
            value,
            CsvScanMode::Strict,
            |token: &[u8]| -> ControlFlow<()> {
                if !token.is_ascii() {
                    invalid_encoding = true;
                    return ControlFlow::Break(());
                }
                saw_any = true;
                let is_chunked = token.eq_ignore_ascii_case(b"chunked");
                if saw_chunked && !last_is_chunked {
                    invalid_ordering = true;
                    return ControlFlow::Break(());
                }
                if saw_chunked && !is_chunked {
                    invalid_ordering = true;
                    return ControlFlow::Break(());
                }
                if is_chunked {
                    saw_chunked = true;
                }
                last_is_chunked = is_chunked;
                ControlFlow::Continue(())
            },
        )
        .and_then(|_| {
            if invalid_encoding {
                return Err(Error::BadRequest("Invalid Transfer-Encoding".into()));
            }
            if invalid_ordering {
                return Err(Error::BadRequest(
                    "Invalid Transfer-Encoding ordering".into(),
                ));
            }
            if saw_any {
                Ok(())
            } else {
                Err(Error::BadRequest("Invalid Transfer-Encoding".into()))
            }
        })?;
        if saw_chunked && !last_is_chunked {
            return Err(Error::BadRequest(
                "Invalid Transfer-Encoding ordering".into(),
            ));
        }

        Ok(TransferEncodingInfo {
            has_transfer_encoding: saw_any,
            is_chunked: saw_chunked && last_is_chunked,
        })
    }

    pub fn header_scan(headers: &[httparse::Header<'_>]) -> Result<HeaderScan> {
        let mut content_length: Option<usize> = None;
        let mut saw_content_length = false;
        let mut has_transfer_encoding = false;
        let mut is_chunked_transfer = false;
        let mut has_expect = false;
        let mut expect_continue = false;
        let mut duplicate_content_length = false;

        for h in headers.iter().filter(|h| !h.name.is_empty()) {
            if h.name.eq_ignore_ascii_case("content-length") {
                if saw_content_length {
                    duplicate_content_length = true;
                }
                saw_content_length = true;
                let len = Header::content_length(h.value)?;
                content_length = Some(len);
                continue;
            }

            if h.name.eq_ignore_ascii_case("transfer-encoding") {
                let te = Self::parse_transfer_encoding_value(h.value)?;
                has_transfer_encoding = has_transfer_encoding || te.has_transfer_encoding;
                if te.is_chunked {
                    is_chunked_transfer = true;
                }
                continue;
            }

            if h.name.eq_ignore_ascii_case("expect") {
                has_expect = true;
                if Header::trimmed_eq_ascii_case(h.value, b"100-continue") {
                    expect_continue = true;
                }
            }
        }

        Ok(HeaderScan {
            content_length,
            has_transfer_encoding,
            is_chunked_transfer,
            has_expect,
            expect_continue,
            duplicate_content_length,
            accept_encoding_gzip: false,
        })
    }

    pub fn content_length(headers: &[httparse::Header<'_>]) -> Result<Option<usize>> {
        for h in headers {
            if h.name.eq_ignore_ascii_case("content-length") {
                return Ok(Some(Header::content_length(h.value)?));
            }
        }
        Ok(None)
    }

    pub fn is_chunked(headers: &[httparse::Header<'_>]) -> bool {
        Header::has_name(headers, "transfer-encoding")
            && headers.iter().any(|h| {
                h.name.eq_ignore_ascii_case("transfer-encoding")
                    && Header::has_token(h.value, b"chunked")
            })
    }
}
