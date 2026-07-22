use sark_core::error::Result;
use sark_core::http::codec::{BodyFraming, DecodeMode, HeaderScan, ResponseDecoder};
use sark_core::http::head::{Flags, MAX_HEADER_LINE_BYTES, WellKnownHeaders};

fn scan_request(block: &[u8]) -> Result<BodyFraming> {
    let mut scan = HeaderScan::default();
    let mut flags = Flags::default();
    let mut count = 0usize;
    let mut pos = 0usize;
    loop {
        let rest = &block[pos..];
        match WellKnownHeaders::new(&mut scan, &mut flags).apply_contiguous(
            rest,
            &mut (),
            &mut count,
            128,
        )? {
            Some(0) => break,
            Some(rel) => pos += rel + 2,
            None => panic!("incomplete header block in test fixture"),
        }
    }
    scan.validate_for_request()
}

#[test]
fn te_chunked_not_last_rejected() {
    assert!(scan_request(b"Transfer-Encoding: chunked, gzip\r\n\r\n").is_err());
}

#[test]
fn te_gzip_then_chunked_accepted_as_chunked() {
    assert_eq!(
        scan_request(b"Transfer-Encoding: gzip, chunked\r\n\r\n").unwrap(),
        BodyFraming::Chunked
    );
}

#[test]
fn te_unknown_coding_alone_rejected() {
    assert!(scan_request(b"Transfer-Encoding: gzip\r\n\r\n").is_err());
}

#[test]
fn te_bare_chunked_accepted() {
    assert_eq!(
        scan_request(b"Transfer-Encoding: chunked\r\n\r\n").unwrap(),
        BodyFraming::Chunked
    );
}

#[test]
fn te_double_chunked_accepted_chunked_is_final() {
    assert_eq!(
        scan_request(b"Transfer-Encoding: chunked, chunked\r\n\r\n").unwrap(),
        BodyFraming::Chunked
    );
}

#[test]
fn conflicting_duplicate_content_length_rejected_contig() {
    assert!(scan_request(b"Content-Length: 5\r\nContent-Length: 7\r\n\r\n").is_err());
}

#[test]
fn identical_duplicate_content_length_rejected_contig() {
    assert!(scan_request(b"Content-Length: 5\r\nContent-Length: 5\r\n\r\n").is_err());
}

#[test]
fn single_content_length_accepted() {
    assert_eq!(
        scan_request(b"Content-Length: 5\r\n\r\n").unwrap(),
        BodyFraming::Length(5)
    );
}

#[test]
fn bare_lf_in_unknown_value_rejected() {
    assert!(scan_request(b"X-Smuggle: foo\nbar\r\n\r\n").is_err());
}

#[test]
fn bare_cr_in_unknown_value_rejected() {
    assert!(scan_request(b"X-Smuggle: foo\rbar\r\n\r\n").is_err());
}

#[test]
fn nul_in_unknown_value_rejected() {
    assert!(scan_request(b"X-Smuggle: foo\x00bar\r\n\r\n").is_err());
}

#[test]
fn control_byte_in_unknown_value_rejected() {
    assert!(scan_request(b"X-Smuggle: foo\x07bar\r\n\r\n").is_err());
}

#[test]
fn del_in_unknown_value_rejected() {
    assert!(scan_request(b"X-Smuggle: foo\x7fbar\r\n\r\n").is_err());
}

#[test]
fn htab_in_unknown_value_accepted() {
    assert_eq!(
        scan_request(b"X-Note: foo\tbar\r\n\r\n").unwrap(),
        BodyFraming::Length(0)
    );
}

#[test]
fn over_long_header_line_rejected() {
    let mut block = Vec::new();
    block.extend_from_slice(b"X-Long: ");
    block.resize(block.len() + MAX_HEADER_LINE_BYTES + 16, b'a');
    block.extend_from_slice(b"\r\n\r\n");
    assert!(scan_request(&block).is_err());
}

#[test]
fn moderate_header_line_accepted() {
    let mut block = Vec::new();
    block.extend_from_slice(b"X-Ok: ");
    block.resize(block.len() + 256, b'a');
    block.extend_from_slice(b"\r\n\r\n");
    assert_eq!(scan_request(&block).unwrap(), BodyFraming::Length(0));
}

#[test]
fn injected_trailers_dropped_from_response() {
    let raw: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
0\r\nContent-Length: 999\r\nHost: evil\r\nConnection: close\r\nX-Good: yes\r\n\r\n";
    let resp = ResponseDecoder::new(DecodeMode::Response)
        .response(raw)
        .unwrap()
        .unwrap();
    assert!(resp.headers().get("content-length").is_none());
    assert!(resp.headers().get("host").is_none());
    assert!(resp.headers().get("connection").is_none());
    assert_eq!(
        resp.headers().get("x-good").map(|v| v.as_bytes()),
        Some(b"yes".as_ref())
    );
}

#[test]
fn well_formed_requests_still_parse() {
    assert_eq!(
        scan_request(b"Host: example.com\r\nAccept-Encoding: gzip\r\nUser-Agent: x/1\r\n\r\n")
            .unwrap(),
        BodyFraming::Length(0)
    );
    assert_eq!(
        scan_request(b"Host: x\r\nContent-Type: application/json\r\nContent-Length: 27\r\n\r\n")
            .unwrap(),
        BodyFraming::Length(27)
    );
}
