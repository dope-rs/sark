use sark::request::Ref;
use sark::service::RouteParams;

#[sark_gen::request]
struct PathReq {
    #[path("seg", default = "fallback")]
    seg: o3::buffer::Bytes<o3::buffer::Retained>,
}

#[sark_gen::request]
struct HdrReq {
    #[header("x-token", default = "none")]
    x_token: o3::buffer::Bytes<o3::buffer::Retained>,
}

#[test]
fn borrowed_path_out_of_bounds_range_is_graceful() {
    let head = b"GET /abc HTTP/1.1\r\n\r\n";
    let req = Ref::<'_>::from_slice(4..8, head, b"");
    let raw = PathReqParamsRaw {
        seg: Some(9_000..9_100),
    };
    let parsed = <PathReqParams<'_> as RouteParams<'_>>::from_raw(&req, raw);
    assert!(
        parsed.is_none(),
        "out-of-bounds path range must propagate gracefully, not panic"
    );
}

#[test]
fn path_default_applies_when_absent() {
    let head = b"GET /abc HTTP/1.1\r\n\r\n";
    let req = Ref::<'_>::from_slice(4..8, head, b"");
    let raw = PathReqParamsRaw { seg: None };
    let parsed = <PathReqParams<'_> as RouteParams<'_>>::from_raw(&req, raw)
        .expect("absent path field falls back to default");
    assert_eq!(parsed.seg.as_slice(), b"fallback");
}

#[test]
fn borrowed_header_out_of_bounds_range_is_graceful() {
    let head = b"GET / HTTP/1.1\r\nx-token: hi\r\n\r\n";
    let req = Ref::<'_>::from_slice(4..5, head, b"");
    let raw = HdrReqHeadersRaw {
        x_token: Some(9_000..9_100),
    };
    let parsed = HdrReqHeaders::from_raw(&req, raw);
    assert!(
        parsed.is_err(),
        "out-of-bounds header range must yield a 400, not a panic"
    );
}

#[test]
fn header_default_applies_when_absent() {
    let head = b"GET / HTTP/1.1\r\n\r\n";
    let req = Ref::<'_>::from_slice(4..5, head, b"");
    let raw = HdrReqHeadersRaw { x_token: None };
    let parsed =
        HdrReqHeaders::from_raw(&req, raw).expect("absent header field falls back to default");
    assert_eq!(parsed.x_token.as_slice(), b"none");
}
