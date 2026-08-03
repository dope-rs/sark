use http::StatusCode;
use o3::buffer::Shared;
use sark_core::http::{FixedResponse, Headers, ResponsePlan, StaticResponseInner, codec::Wire};

#[test]
fn static_headers_remain_static_when_materialized() {
    static HEADERS: &[u8] = b"x-static: yes\r\n";
    let headers = ResponsePlan::from_static(StatusCode::OK, HEADERS).wire_headers();

    assert_eq!(headers.as_ptr(), HEADERS.as_ptr());
    assert_eq!(headers.as_ref(), HEADERS);
}

#[test]
fn dynamic_headers_fill_the_exact_wire_buffer() {
    let mut plan = ResponsePlan::from_static(StatusCode::OK, b"x-static: yes\r\n");
    plan.push_static("x-dynamic", "also");

    assert_eq!(
        plan.wire_headers().as_ref(),
        b"x-static: yes\r\nx-dynamic: also\r\n"
    );
}

#[test]
fn chunk_framing_fills_the_exact_wire_buffer() {
    let framed = Wire::chunk_frame(Shared::from_static(b"hello"));

    assert_eq!(framed.as_ref(), b"5\r\nhello\r\n");
}

#[test]
fn response_status_line_keeps_canonical_and_unknown_fallbacks() {
    let date = b"Thu, 01 Jan 1970 00:00:00 GMT";
    for (status, expected) in [
        (StatusCode::OK, b"HTTP/1.1 200 OK\r\n".as_slice()),
        (
            StatusCode::NOT_FOUND,
            b"HTTP/1.1 404 Not Found\r\n".as_slice(),
        ),
        (
            StatusCode::from_u16(599).expect("valid extension status"),
            b"HTTP/1.1 599 \r\n".as_slice(),
        ),
    ] {
        let response =
            FixedResponse::<'static, 0>::direct(status, b"", Headers::new(), b"".as_slice());
        let mut out = [0; 256];
        let written = response
            .write_into_slice(&mut out, date)
            .expect("response fits");
        assert!(out[..written].starts_with(expected));
    }
}

#[test]
fn static_response_proves_capacity_before_writing() {
    let response: StaticResponseInner<'static, 0> = StaticResponseInner::direct(
        StatusCode::OK,
        b"content-type: text/plain\r\n",
        Headers::new(),
        b"hello world",
    );
    let date = b"Thu, 01 Jan 1970 00:00:00 GMT";
    let mut oversized = [0u8; 256];
    let written = response
        .write_into_slice(&mut oversized, date)
        .expect("response fits");

    let mut exact = vec![0u8; written];
    assert_eq!(response.write_into_slice(&mut exact, date), Some(written));
    assert_eq!(exact, oversized[..written]);

    let mut short = vec![0xa5; written - 1];
    assert_eq!(response.write_into_slice(&mut short, date), None);
    assert!(short.iter().all(|&byte| byte == 0xa5));
}
