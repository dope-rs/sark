use http::StatusCode;
use o3::buffer::Shared;
use sark_core::http::codec::HeaderScan;
use sark_core::http::compress::Gzip;
use sark_core::http::head::Flags;
use sark_core::http::{FixedResponseInner, Headers, Shape, head};

#[test]
fn fixed_response_apply_gzip_writes_content_encoding_and_vary() {
    let payload = b"{\"hello\":\"world\",\"hello\":\"world\",\"hello\":\"world\"}".to_vec();
    let body = Shared::from(payload.clone());
    let mut fixed: FixedResponseInner<'static> = FixedResponseInner::direct(
        StatusCode::OK,
        b"content-type: application/json\r\n",
        Headers::from_items([]),
        body,
    );

    let compressed = Gzip::with_thread_local(|g| Shared::from(g.encode(&payload).to_vec()));
    let plain_clen = payload.len();
    fixed.apply_gzip(compressed.clone());

    let mut out = vec![0u8; 1024];
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let n =
        <FixedResponseInner<'static> as Shape<'static>>::write_into_slice(&fixed, &mut out, date)
            .expect("fits");
    let bytes = &out[..n];
    let body_off = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let head_text = std::str::from_utf8(&bytes[..body_off]).unwrap();
    assert!(head_text.contains("Content-Encoding: gzip\r\n"));
    assert!(head_text.contains("Vary: Accept-Encoding\r\n"));
    assert!(head_text.contains(&format!("Content-Length: {}\r\n", compressed.len())));
    assert!(!head_text.contains(&format!("Content-Length: {}\r\n", plain_clen)));
    assert_eq!(&bytes[body_off..], compressed.as_ref());
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
        match head::apply_well_known_header_contig(
            rest,
            &mut scan,
            &mut flags,
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
        match head::apply_well_known_header_contig(
            rest,
            &mut scan,
            &mut flags,
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
        match head::apply_well_known_header_contig(
            rest,
            &mut scan,
            &mut flags,
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
    let compressed = Gzip::with_thread_local(|g| g.encode(&original).to_vec());

    let mut dec = Decompressor::new();
    let mut out = vec![0u8; original.len() * 2];
    let n = dec
        .gzip_decompress(&compressed, &mut out)
        .expect("decompress");
    assert_eq!(&out[..n], original.as_slice());
}
