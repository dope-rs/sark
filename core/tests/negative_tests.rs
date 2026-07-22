use sark_core::http::codec::chunked::BodyDecoder;
use sark_core::http::codec::decode::BodyKind;
use sark_core::http::codec::{DecodeMode, HeaderScan, ResponseDecoder};

#[test]
fn scan_empty_headers() {
    let headers: &[httparse::Header<'_>] = &[];
    let s = HeaderScan::parse(headers).unwrap();
    assert_eq!(s.content_length, None);
    assert!(!s.has_transfer_encoding);
    assert!(!s.is_chunked_transfer);
    assert!(!s.expect_continue);
    assert!(!s.duplicate_content_length);
}

#[test]
fn scan_single_content_length() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"42",
    }];
    let s = HeaderScan::parse(headers).unwrap();
    assert_eq!(s.content_length, Some(42));
    assert!(!s.duplicate_content_length);
}

#[test]
fn scan_duplicate_content_length_same_value() {
    let headers = &[
        httparse::Header {
            name: "Content-Length",
            value: b"42",
        },
        httparse::Header {
            name: "Content-Length",
            value: b"42",
        },
    ];
    let s = HeaderScan::parse(headers).unwrap();
    assert!(s.duplicate_content_length);
    assert_eq!(s.content_length, Some(42));
}

#[test]
fn scan_duplicate_content_length_different_values() {
    let headers = &[
        httparse::Header {
            name: "Content-Length",
            value: b"42",
        },
        httparse::Header {
            name: "Content-Length",
            value: b"99",
        },
    ];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_chunked_transfer_encoding() {
    let headers = &[httparse::Header {
        name: "Transfer-Encoding",
        value: b"chunked",
    }];
    let s = HeaderScan::parse(headers).unwrap();
    assert!(s.has_transfer_encoding);
    assert!(s.is_chunked_transfer);
}

#[test]
fn scan_non_chunked_transfer_encoding() {
    let headers = &[httparse::Header {
        name: "Transfer-Encoding",
        value: b"gzip",
    }];
    let s = HeaderScan::parse(headers).unwrap();
    assert!(s.has_transfer_encoding);
    assert!(!s.is_chunked_transfer);
}

#[test]
fn scan_expect_continue() {
    let headers = &[httparse::Header {
        name: "Expect",
        value: b"100-continue",
    }];
    let s = HeaderScan::parse(headers).unwrap();
    assert!(s.expect_continue);
}

#[test]
fn scan_expect_non_continue() {
    let headers = &[httparse::Header {
        name: "Expect",
        value: b"some-other",
    }];
    let s = HeaderScan::parse(headers).unwrap();
    assert!(!s.expect_continue);
}

#[test]
fn scan_content_length_non_utf8() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: &[0xFF, 0xFE],
    }];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_content_length_non_numeric() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"abc",
    }];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_content_length_negative() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"-5",
    }];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_content_length_overflow() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"99999999999999999999999999999",
    }];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_content_length_with_whitespace() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"  42  ",
    }];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_content_length_empty_value() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"",
    }];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_content_length_only_whitespace() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"   ",
    }];
    assert!(HeaderScan::parse(headers).is_err());
}

#[test]
fn scan_empty_header_name_skipped() {
    let headers = &[
        httparse::Header {
            name: "",
            value: b"ignored",
        },
        httparse::Header {
            name: "Content-Length",
            value: b"5",
        },
    ];
    let s = HeaderScan::parse(headers).unwrap();
    assert_eq!(s.content_length, Some(5));
}

#[test]
fn scan_case_insensitive_all_headers() {
    let headers = &[
        httparse::Header {
            name: "CONTENT-LENGTH",
            value: b"42",
        },
        httparse::Header {
            name: "TRANSFER-ENCODING",
            value: b"CHUNKED",
        },
        httparse::Header {
            name: "EXPECT",
            value: b"100-Continue",
        },
    ];
    let s = HeaderScan::parse(headers).unwrap();
    assert_eq!(s.content_length, Some(42));
    assert!(s.has_transfer_encoding);
    assert!(s.is_chunked_transfer);
    assert!(s.expect_continue);
}

#[test]
fn scan_content_length_zero() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"0",
    }];
    let s = HeaderScan::parse(headers).unwrap();
    assert_eq!(s.content_length, Some(0));
}

#[test]
fn scan_content_length_with_leading_zeros() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"0042",
    }];
    let s = HeaderScan::parse(headers).unwrap();
    assert_eq!(s.content_length, Some(42));
}

#[test]
fn parse_cl_absent() {
    let headers: &[httparse::Header<'_>] = &[];
    assert_eq!(HeaderScan::content_length(headers).unwrap(), None);
}

#[test]
fn parse_cl_valid() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"123",
    }];
    assert_eq!(HeaderScan::content_length(headers).unwrap(), Some(123));
}

#[test]
fn parse_cl_zero() {
    let headers = &[httparse::Header {
        name: "content-length",
        value: b"0",
    }];
    assert_eq!(HeaderScan::content_length(headers).unwrap(), Some(0));
}

#[test]
fn parse_cl_non_utf8() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: &[0xFF, 0xFE],
    }];
    assert!(HeaderScan::content_length(headers).is_err());
}

#[test]
fn parse_cl_non_numeric() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"abc",
    }];
    assert!(HeaderScan::content_length(headers).is_err());
}

#[test]
fn parse_cl_with_whitespace() {
    let headers = &[httparse::Header {
        name: "Content-Length",
        value: b"  42  ",
    }];
    assert!(HeaderScan::content_length(headers).is_err());
}

#[test]
fn parse_cl_returns_first_match() {
    let headers = &[
        httparse::Header {
            name: "Content-Length",
            value: b"10",
        },
        httparse::Header {
            name: "Content-Length",
            value: b"20",
        },
    ];
    assert_eq!(HeaderScan::content_length(headers).unwrap(), Some(10));
}

#[test]
fn parse_cl_skips_other_headers() {
    let headers = &[
        httparse::Header {
            name: "X-Custom",
            value: b"123",
        },
        httparse::Header {
            name: "Content-Length",
            value: b"77",
        },
    ];
    assert_eq!(HeaderScan::content_length(headers).unwrap(), Some(77));
}

#[test]
fn is_chunked_true() {
    let headers = &[httparse::Header {
        name: "Transfer-Encoding",
        value: b"chunked",
    }];
    assert!(HeaderScan::is_chunked(headers));
}

#[test]
fn is_chunked_case_insensitive() {
    let headers = &[httparse::Header {
        name: "transfer-encoding",
        value: b"CHUNKED",
    }];
    assert!(HeaderScan::is_chunked(headers));
}

#[test]
fn is_chunked_false_gzip() {
    let headers = &[httparse::Header {
        name: "Transfer-Encoding",
        value: b"gzip",
    }];
    assert!(!HeaderScan::is_chunked(headers));
}

#[test]
fn is_chunked_false_no_te() {
    let headers: &[httparse::Header<'_>] = &[];
    assert!(!HeaderScan::is_chunked(headers));
}

#[test]
fn is_chunked_name_case_insensitive() {
    let headers = &[httparse::Header {
        name: "TRANSFER-ENCODING",
        value: b"chunked",
    }];
    assert!(HeaderScan::is_chunked(headers));
}

#[test]
fn decode_head_200_content_length() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let head = ResponseDecoder::new(DecodeMode::Response)
        .head(raw)
        .unwrap()
        .unwrap();
    assert_eq!(head.status, http::StatusCode::OK);
    assert!(matches!(head.body_kind, BodyKind::ContentLength(5)));
    assert!(!head.headers.is_empty());
}

#[test]
fn decode_head_chunked() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    let head = ResponseDecoder::new(DecodeMode::Response)
        .head(raw)
        .unwrap()
        .unwrap();
    assert!(matches!(head.body_kind, BodyKind::Chunked));
}

#[test]
fn decode_head_204_no_body() {
    let raw = b"HTTP/1.1 204 No Content\r\n\r\n";
    let head = ResponseDecoder::new(DecodeMode::Response)
        .head(raw)
        .unwrap()
        .unwrap();
    assert_eq!(head.status, http::StatusCode::NO_CONTENT);
    assert!(matches!(head.body_kind, BodyKind::NoBody));
}

#[test]
fn decode_head_304_no_body() {
    let raw = b"HTTP/1.1 304 Not Modified\r\n\r\n";
    let head = ResponseDecoder::new(DecodeMode::Response)
        .head(raw)
        .unwrap()
        .unwrap();
    assert_eq!(head.status, http::StatusCode::NOT_MODIFIED);
    assert!(matches!(head.body_kind, BodyKind::NoBody));
}

#[test]
fn decode_head_is_head_request() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let head = ResponseDecoder::new(DecodeMode::Head)
        .head(raw)
        .unwrap()
        .unwrap();
    assert!(matches!(head.body_kind, BodyKind::NoBody));
}

#[test]
fn decode_head_until_eof() {
    let raw = b"HTTP/1.1 200 OK\r\n\r\n";
    let head = ResponseDecoder::new(DecodeMode::Response)
        .head(raw)
        .unwrap()
        .unwrap();
    assert!(matches!(head.body_kind, BodyKind::UntilEof));
}

#[test]
fn decode_head_partial() {
    let raw = b"HTTP/1.1 200 OK\r\n";
    assert!(
        ResponseDecoder::new(DecodeMode::Response)
            .head(raw)
            .unwrap()
            .is_none()
    );
}

#[test]
fn decode_head_1xx_no_body() {
    let raw = b"HTTP/1.1 100 Continue\r\n\r\n";
    let head = ResponseDecoder::new(DecodeMode::Response)
        .head(raw)
        .unwrap()
        .unwrap();
    assert_eq!(head.status, http::StatusCode::CONTINUE);
    assert!(matches!(head.body_kind, BodyKind::NoBody));
}

#[test]
fn decode_head_header_len_correct() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let head = ResponseDecoder::new(DecodeMode::Response)
        .head(raw)
        .unwrap()
        .unwrap();
    assert_eq!(&raw[head.header_len..], b"hello");
}

#[test]
fn decode_head_empty_returns_none() {
    assert!(
        ResponseDecoder::new(DecodeMode::Response)
            .head(b"")
            .unwrap()
            .is_none()
    );
}

#[test]
fn chunked_body_single_chunk() {
    let data = b"5\r\nhello\r\n0\r\n\r\n";
    let body = BodyDecoder::body(data).unwrap().unwrap();
    assert_eq!(&body, b"hello");
}

#[test]
fn chunked_body_empty() {
    let data = b"0\r\n\r\n";
    let body = BodyDecoder::body(data).unwrap().unwrap();
    assert!(body.is_empty());
}

#[test]
fn chunked_body_multiple_chunks() {
    let data = b"3\r\nabc\r\n4\r\ndefg\r\n0\r\n\r\n";
    let body = BodyDecoder::body(data).unwrap().unwrap();
    assert_eq!(&body, b"abcdefg");
}

#[test]
fn chunked_body_partial_returns_none() {
    let data = b"5\r\nhel";
    assert!(BodyDecoder::body(data).unwrap().is_none());
}

#[test]
fn chunked_body_invalid_hex() {
    let data = b"xyz\r\nhello\r\n0\r\n\r\n";
    assert!(BodyDecoder::body(data).is_err());
}

#[test]
fn chunked_body_partial_terminator() {
    let data = b"5\r\nhello\r\n0\r\n";
    assert!(BodyDecoder::body(data).unwrap().is_none());
}

#[test]
fn chunked_body_oversized() {
    let data = b"FFFFFFFF\r\nhello\r\n0\r\n\r\n";
    assert!(BodyDecoder::body(data).is_err());
}

#[test]
fn chunked_body_hex_upper_and_lower() {
    let data = b"a\r\n0123456789\r\n0\r\n\r\n";
    let body = BodyDecoder::body(data).unwrap().unwrap();
    assert_eq!(&body, b"0123456789");

    let data = b"A\r\n0123456789\r\n0\r\n\r\n";
    let body = BodyDecoder::body(data).unwrap().unwrap();
    assert_eq!(&body, b"0123456789");
}

#[test]
fn chunked_body_with_extension() {
    let data = b"5;ext=val\r\nhello\r\n0\r\n\r\n";
    let body = BodyDecoder::body(data).unwrap().unwrap();
    assert_eq!(&body, b"hello");
}

#[test]
fn chunked_body_crlf_in_data() {
    let data = b"6\r\nab\r\ncd\r\n0\r\n\r\n";
    let body = BodyDecoder::body(data).unwrap().unwrap();
    assert_eq!(&body, b"ab\r\ncd");
}

#[test]
fn response_only_crlf() {
    let raw = b"\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err() || result.unwrap().is_none());
}

#[test]
fn response_all_whitespace() {
    let raw = b"    \r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err() || result.unwrap().is_none());
}

#[test]
fn response_lf_only_line_endings() {
    let raw = b"HTTP/1.1 200 OK\nContent-Length: 0\n\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    let _ = result;
}

#[test]
fn chunked_negative_size() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n-1\r\nX\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn chunked_empty_size_line() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn status_code_000() {
    let raw = b"HTTP/1.1 000 Nothing\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn status_code_099() {
    let raw = b"HTTP/1.1 099 Low\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn status_code_600() {
    let raw = b"HTTP/1.1 600 Custom\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.status().as_u16(), 600);
}

#[test]
fn header_name_with_space() {
    let raw = b"HTTP/1.1 200 OK\r\nBad Name: value\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn response_binary_body() {
    let body: Vec<u8> = (0u8..=255).collect();
    let raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
    let mut buf = raw.into_bytes();
    buf.extend_from_slice(&body);

    let resp = ResponseDecoder::new(DecodeMode::Response)
        .response(&buf)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body(), &body[..]);
}

#[test]
fn content_length_max_usize() {
    let max = usize::MAX.to_string();
    let raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", max);
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw.as_bytes());
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn multiple_transfer_encoding_headers() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    if let Ok(Some(resp)) = result {
        assert_eq!(std::str::from_utf8(resp.body()).ok(), Some("hello"));
    }
}

#[test]
fn response_tab_in_header_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Tab: val\tue\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.headers().get("x-tab").unwrap().as_bytes(), b"val\tue");
}

#[test]
fn content_length_with_plus_sign() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: +5\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn decode_after_eof_empty_buffer() {
    let result = ResponseDecoder::new(DecodeMode::Response).response_after_eof(b"");
    assert!(result.is_err());
}

#[test]
fn decode_after_eof_just_status_line() {
    let result =
        ResponseDecoder::new(DecodeMode::Response).response_after_eof(b"HTTP/1.1 200 OK\r\n\r\n");
    assert!(result.is_ok());
    let resp = result.unwrap();
    assert!(resp.body().is_empty());
}

#[test]
fn response_with_many_colons_in_header_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Data: a:b:c:d:e\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.headers().get("x-data").unwrap(), "a:b:c:d:e");
}

#[test]
fn response_empty_reason_phrase() {
    let raw = b"HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.status(), http::StatusCode::OK);
}

#[test]
fn chunked_with_trailing_whitespace_in_size() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n 5 \r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn response_header_value_with_equals_semicolons() {
    let raw =
        b"HTTP/1.1 200 OK\r\nSet-Cookie: id=abc; Path=/; HttpOnly\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(
        resp.headers().get("set-cookie").unwrap(),
        "id=abc; Path=/; HttpOnly"
    );
}

#[test]
fn decode_head_with_both_cl_and_te_chunked() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).head(raw);
    assert!(result.is_err());
}

#[test]
fn response_with_conflicting_content_length_values_is_error() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 7\r\n\r\nhello!!";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn response_with_chunked_not_last_transfer_coding_is_error() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn response_just_version() {
    let raw = b"HTTP/1.1";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn response_incomplete_status_line() {
    let raw = b"HTTP/1.1 200";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}
