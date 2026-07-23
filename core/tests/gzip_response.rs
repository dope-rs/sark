use http::StatusCode;
use o3::buffer::Shared;
use sark_core::http::codec::HeaderScan;
use sark_core::http::compress::{Gunzip, GunzipError, GunzipOutput, Gzip};
use sark_core::http::head::Flags;
use sark_core::http::{FixedResponse, Headers, head};

#[test]
fn fixed_response_gzip_head_writes_content_encoding_and_vary() {
    let payload = b"{\"hello\":\"world\",\"hello\":\"world\",\"hello\":\"world\"}".to_vec();
    let body = Shared::copy_from_slice(&payload);
    let fixed: FixedResponse<'static, 0> = FixedResponse::direct(
        StatusCode::OK,
        b"content-type: application/json\r\n",
        Headers::from_items([]),
        body,
    );

    let compressed = Shared::copy_from_slice(Gzip::new().encode(&payload).unwrap().as_ref());
    let plain_clen = payload.len();
    let mut out = vec![0u8; 1024];
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let n = fixed
        .write_gzip_head(&mut out, date, compressed.len())
        .expect("fits");
    let bytes = &out[..n];
    let body_off = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let head_text = std::str::from_utf8(&bytes[..body_off]).unwrap();
    assert!(head_text.contains("Content-Encoding: gzip\r\n"));
    assert!(head_text.contains("Vary: Accept-Encoding\r\n"));
    assert!(head_text.contains(&format!("Content-Length: {}\r\n", compressed.len())));
    assert!(!head_text.contains(&format!("Content-Length: {}\r\n", plain_clen)));
    assert!(bytes[body_off..].is_empty());
}

#[test]
fn header_scan_detects_accept_encoding_gzip() {
    let req = b"GET / HTTP/1.1\r\nHost: x\r\nAccept-Encoding: gzip, br\r\n\r\n";
    let headers_start = req.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    let mut scan = HeaderScan::default();
    let mut flags = Flags::default();
    let mut hc = 0usize;
    let mut pos = headers_start;
    loop {
        let rest = &req[pos..];
        match head::WellKnownHeaders::new(&mut scan, &mut flags).apply_contiguous(
            rest,
            &mut (),
            &mut hc,
            32,
        ) {
            Ok(Some(0)) => break,
            Ok(Some(rel)) => pos += rel + 2,
            Ok(None) => panic!("need more"),
            Err(_) => panic!("bad"),
        }
    }
    assert!(scan.accept_encoding_gzip);
}

#[test]
fn header_scan_detects_only_br_not_gzip() {
    let req = b"GET / HTTP/1.1\r\nHost: x\r\nAccept-Encoding: br\r\n\r\n";
    let headers_start = req.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    let mut scan = HeaderScan::default();
    let mut flags = Flags::default();
    let mut hc = 0usize;
    let mut pos = headers_start;
    loop {
        let rest = &req[pos..];
        match head::WellKnownHeaders::new(&mut scan, &mut flags).apply_contiguous(
            rest,
            &mut (),
            &mut hc,
            32,
        ) {
            Ok(Some(0)) => break,
            Ok(Some(rel)) => pos += rel + 2,
            Ok(None) => panic!("need more"),
            Err(_) => panic!("bad"),
        }
    }
    assert!(!scan.accept_encoding_gzip);
}

#[test]
fn header_scan_detects_gzip_with_qvalue() {
    let req = b"GET / HTTP/1.1\r\nHost: x\r\nAccept-Encoding: gzip;q=1.0, deflate;q=0.5\r\n\r\n";
    let headers_start = req.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    let mut scan = HeaderScan::default();
    let mut flags = Flags::default();
    let mut hc = 0usize;
    let mut pos = headers_start;
    loop {
        let rest = &req[pos..];
        match head::WellKnownHeaders::new(&mut scan, &mut flags).apply_contiguous(
            rest,
            &mut (),
            &mut hc,
            32,
        ) {
            Ok(Some(0)) => break,
            Ok(Some(rel)) => pos += rel + 2,
            Ok(None) => panic!("need more"),
            Err(_) => panic!("bad"),
        }
    }
    assert!(scan.accept_encoding_gzip);
}

#[test]
fn gzip_encoder_round_trip_through_libdeflater_decode() {
    use libdeflater::Decompressor;

    let original = b"hello hello hello hello hello hello hello hello hello hello".repeat(8);
    let compressed = Gzip::new().encode(&original).unwrap();

    let mut dec = Decompressor::new();
    let mut out = vec![0u8; original.len() * 2];
    let n = dec
        .gzip_decompress(compressed.as_ref(), &mut out)
        .expect("decompress");
    assert_eq!(&out[..n], original.as_slice());
}

#[test]
fn gunzip_decodes_into_the_reusable_pool() {
    let original = b"pooled gunzip body".repeat(64);
    let compressed = Gzip::new().encode(&original).unwrap();
    let output = Gunzip::new()
        .decode(compressed.as_ref(), original.len())
        .expect("decompress");

    let GunzipOutput::Pooled(output) = output else {
        panic!("small gunzip body must use the reusable pool");
    };
    assert_eq!(output.as_ref(), original);
}

#[test]
fn gunzip_checks_the_declared_size_before_allocating() {
    let original = vec![b'x'; 4096];
    let compressed = Gzip::new().encode(&original).unwrap();
    let error = Gunzip::new()
        .decode(compressed.as_ref(), 1024)
        .err()
        .expect("size limit");
    assert!(matches!(error, GunzipError::SizeLimit));
}
