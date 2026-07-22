use sark_core::http::codec::{DecodeMode, ResponseDecoder};

#[test]
fn empty_input() {
    let raw = b"";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn single_byte_input() {
    let raw = b"H";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn headers_only_no_body_separator() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn content_length_zero_vs_absent() {
    let raw_zero = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let resp_zero = ResponseDecoder::new(DecodeMode::Response)
        .response(raw_zero)
        .unwrap()
        .unwrap();
    assert!(resp_zero.body().is_empty());

    let raw_absent = b"HTTP/1.1 200 OK\r\n\r\n";
    let result_absent = ResponseDecoder::new(DecodeMode::Response)
        .response(raw_absent)
        .unwrap();
    assert!(result_absent.is_none());
}

#[test]
fn content_length_larger_than_actual_body() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn content_length_non_numeric() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn content_length_negative() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: -5\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn content_length_overflow() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999999999999\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn multiple_content_length_headers() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn chunked_invalid_hex_size() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nGGG\r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn chunked_missing_terminator() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn chunked_extremely_large_chunk_size() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nFFFFFFFFFFFFFFFF\r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn mixed_content_length_and_transfer_encoding() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn status_line_missing_status_code() {
    let raw = b"HTTP/1.1  OK\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    let _ = result;
}

#[test]
fn status_line_invalid_status_code() {
    let raw = b"HTTP/1.1 999 Custom\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
}

#[test]
fn header_with_empty_name() {
    let raw = b"HTTP/1.1 200 OK\r\n: value\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn header_with_empty_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Empty:\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.headers().get("x-empty").unwrap(), "");
}

#[test]
fn very_long_header_value() {
    let long_value = "x".repeat(10240);
    let raw = format!(
        "HTTP/1.1 200 OK\r\nX-Long: {}\r\nContent-Length: 0\r\n\r\n",
        long_value
    );
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw.as_bytes());
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.headers().get("x-long").unwrap().len(), 10240);
}

#[test]
fn binary_data_in_header_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Binary: \x00\x01\x02\xFF\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn response_with_100_headers() {
    let mut raw = String::from("HTTP/1.1 200 OK\r\n");
    for i in 0..99 {
        raw.push_str(&format!("X-Header-{}: value\r\n", i));
    }
    raw.push_str("Content-Length: 0\r\n\r\n");

    let result = ResponseDecoder::new(DecodeMode::Response).response(raw.as_bytes());
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.headers().len(), 100);
}

#[test]
fn response_exceeding_max_headers() {
    let mut raw = String::from("HTTP/1.1 200 OK\r\n");
    for i in 0..101 {
        raw.push_str(&format!("X-Header-{}: value\r\n", i));
    }
    raw.push_str("\r\n");

    let result = ResponseDecoder::new(DecodeMode::Response).response(raw.as_bytes());
    assert!(result.is_err());
}

#[test]
fn null_byte_in_status_line() {
    let raw = b"HTTP/1.1 200\x00OK\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    if let Ok(Some(resp)) = result {
        assert_eq!(resp.status().as_u16(), 200);
    }
}

#[test]
fn null_byte_in_header_name() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Nu\x00ll: value\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn null_byte_in_header_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Test: val\x00ue\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn non_ascii_in_status_reason() {
    let raw = "HTTP/1.1 200 OK™\r\nContent-Length: 0\r\n\r\n".as_bytes();
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
}

#[test]
fn http_0_9_style_response() {
    let raw = b"hello world";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    let _ = result;
}

#[test]
fn pipelined_responses_extra_data() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloHTTP/1.1 200 OK\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn chunked_with_whitespace_in_size() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n  5  \r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn chunked_zero_size_without_terminal_crlf() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn content_length_with_leading_zeros() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 0005\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn content_length_with_whitespace() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length:  5  \r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn decode_after_eof_with_no_headers() {
    let raw = b"HTTP/1.1 200 OK\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response_after_eof(raw);
    assert!(result.is_ok());
    let resp = result.unwrap();
    assert!(resp.body().is_empty());
}

#[test]
fn decode_after_eof_chunked_incomplete() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel";
    let result = ResponseDecoder::new(DecodeMode::Response).response_after_eof(raw);
    assert!(result.is_err());
}

#[test]
fn chunked_with_multiple_extensions() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;ext1=val1;ext2=val2\r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn chunked_lowercase_hex() {
    let raw =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nf\r\n012345678901234\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.body_str(), Some("012345678901234"));
}

#[test]
fn status_code_out_of_range() {
    let raw = b"HTTP/1.1 1000 Custom\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_err());
}

#[test]
fn header_continuation_line() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Long: first\r\n second\r\nContent-Length: 0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    let _ = result;
}

#[test]
fn crlf_in_chunk_data() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\nab\r\ncd\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    match result {
        Ok(Some(resp)) => assert_eq!(resp.body_str(), Some("ab\r\ncd")),
        Ok(None) => {}
        Err(_) => {}
    }
}

#[test]
fn transfer_encoding_case_insensitive() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: CHUNKED\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn content_length_case_insensitive() {
    let raw = b"HTTP/1.1 200 OK\r\nCONTENT-LENGTH: 5\r\n\r\nhello";
    let result = ResponseDecoder::new(DecodeMode::Response).response(raw);
    assert!(result.is_ok());
    let resp = result.unwrap().unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}
