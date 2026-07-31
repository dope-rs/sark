use sark_core::error::Result;
use sark_core::http::codec::{BodyFraming, DecodeMode, HeaderScan, ResponseDecoder};
use sark_core::http::head::{Flags, KnownHeader};

fn framing(headers: &[(KnownHeader, &[u8])]) -> Result<BodyFraming> {
    let mut scan = HeaderScan::default();
    let mut flags = Flags::default();
    for &(header, value) in headers {
        header.apply(&mut scan, &mut flags, value)?;
    }
    scan.validate_for_request()
}

#[test]
fn te_chunked_not_last_rejected() {
    assert!(framing(&[(KnownHeader::TransferEncoding, b"chunked, gzip")]).is_err());
}

#[test]
fn te_gzip_then_chunked_accepted_as_chunked() {
    assert_eq!(
        framing(&[(KnownHeader::TransferEncoding, b"gzip, chunked")]).unwrap(),
        BodyFraming::Chunked
    );
}

#[test]
fn te_unknown_coding_alone_rejected() {
    assert!(framing(&[(KnownHeader::TransferEncoding, b"gzip")]).is_err());
}

#[test]
fn te_bare_chunked_accepted() {
    assert_eq!(
        framing(&[(KnownHeader::TransferEncoding, b"chunked")]).unwrap(),
        BodyFraming::Chunked
    );
}

#[test]
fn te_double_chunked_accepted_chunked_is_final() {
    assert_eq!(
        framing(&[(KnownHeader::TransferEncoding, b"chunked, chunked")]).unwrap(),
        BodyFraming::Chunked
    );
}

#[test]
fn conflicting_duplicate_content_length_rejected_contig() {
    assert!(
        framing(&[
            (KnownHeader::ContentLength, b"5"),
            (KnownHeader::ContentLength, b"7"),
        ])
        .is_err()
    );
}

#[test]
fn identical_duplicate_content_length_rejected_contig() {
    assert!(
        framing(&[
            (KnownHeader::ContentLength, b"5"),
            (KnownHeader::ContentLength, b"5"),
        ])
        .is_err()
    );
}

#[test]
fn single_content_length_accepted() {
    assert_eq!(
        framing(&[(KnownHeader::ContentLength, b"5")]).unwrap(),
        BodyFraming::Length(5)
    );
}

#[test]
fn content_length_and_transfer_encoding_reject_at_second_header() {
    for headers in [
        [
            (KnownHeader::ContentLength, b"5".as_slice()),
            (KnownHeader::TransferEncoding, b"chunked".as_slice()),
        ],
        [
            (KnownHeader::TransferEncoding, b"chunked".as_slice()),
            (KnownHeader::ContentLength, b"5".as_slice()),
        ],
    ] {
        let mut scan = HeaderScan::default();
        let mut flags = Flags::default();
        headers[0]
            .0
            .apply(&mut scan, &mut flags, headers[0].1)
            .unwrap();
        assert!(
            headers[1]
                .0
                .apply(&mut scan, &mut flags, headers[1].1)
                .is_err()
        );
    }
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
