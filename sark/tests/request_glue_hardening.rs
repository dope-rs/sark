use sark::request::Ref;
use sark::sark_core::http::codec::HeaderScan;
use sark::sark_core::http::head::Flags;
use sark::service::{BodyPolicy, RouteParams, RouteRequestImpl};

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

#[sark_gen::request]
struct EncodingReq {
    #[header("accept-encoding", default = "")]
    accept_encoding: o3::buffer::Bytes<o3::buffer::Retained>,
}

#[sark_gen::request(value = skip)]
struct SkipValueReq {
    #[header("x-token", default = "")]
    x_token: o3::buffer::Bytes<o3::buffer::Retained>,
}

#[sark_gen::request]
struct QueryReq {
    #[query("count", default = "0")]
    count: usize,
    #[query("limit", default = "0")]
    limit: u64,
    #[query("flag", default = "false")]
    flag: bool,
}

#[sark_gen::request(ordered)]
struct OrderedQueryReq {
    #[query("count", default = "0")]
    count: usize,
    #[query("limit", default = "0")]
    limit: u64,
}

#[sark_gen::request]
struct RawBodyReq {
    #[raw_body]
    payload: o3::buffer::Bytes<o3::buffer::Retained>,
}

#[sark_gen::request]
struct BodyLenReq {
    #[body_len]
    body_len: sark::request::BodyLen,
}

#[sark_gen::json(ordered)]
struct ParsedRequestBody {
    value: u64,
}

#[sark_gen::request]
#[json_body(ParsedRequestBody)]
struct JsonBodyReq {}

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

#[test]
fn captured_accept_encoding_still_updates_protocol_scan() {
    let input: &[u8] = b"Accept-Encoding: gzip\r\n";
    let mut headers = EncodingReqHeadersRaw::default();
    let mut scan = HeaderScan::default();
    let mut flags = Flags::default();
    let mut header_count = 0;

    let tail = <EncodingReq as RouteRequestImpl>::apply_header_contig(
        &mut headers,
        input,
        input,
        0,
        &mut scan,
        &mut flags,
        &mut header_count,
        16,
    )
    .expect("valid Accept-Encoding header");

    assert_eq!(tail, Some(input.len() - 2));
    assert_eq!(header_count, 1);
    assert!(<EncodingReq as RouteRequestImpl>::NEED_KNOWN_HEADER);
    assert!(scan.accept_encoding_gzip);
    assert_eq!(headers.accept_encoding, Some(17..21));
}

#[test]
fn skipped_custom_header_value_is_still_validated() {
    let input: &[u8] = b"X-Token: good\nbad\r\n";
    let mut headers = SkipValueReqHeadersRaw::default();
    let mut scan = HeaderScan::default();
    let mut flags = Flags::default();
    let mut header_count = 0;

    let result = <SkipValueReq as RouteRequestImpl>::apply_header_contig(
        &mut headers,
        input,
        input,
        0,
        &mut scan,
        &mut flags,
        &mut header_count,
        16,
    );

    assert!(
        result.is_err(),
        "a bare LF must not bypass value validation"
    );
}

#[test]
fn shared_query_plan_parses_unordered_fields() {
    let input = b"limit=7&flag=true&count=41";
    let mut headers = QueryReqHeadersRaw::default();

    <QueryReq as RouteRequestImpl>::parse_query_raw(&mut headers, input, 0..input.len())
        .expect("valid unordered query");

    assert_eq!(headers.count, Some(41));
    assert_eq!(headers.limit, Some(7));
    assert_eq!(headers.flag, Some(true));
}

#[test]
fn shared_query_scan_drives_ordered_fields() {
    let input = b"count=41&limit=7";
    let mut headers = OrderedQueryReqHeadersRaw::default();

    <OrderedQueryReq as RouteRequestImpl>::parse_query_raw(&mut headers, input, 0..input.len())
        .expect("valid ordered query");

    assert_eq!(headers.count, Some(41));
    assert_eq!(headers.limit, Some(7));
}

#[test]
fn generated_query_parser_rejects_out_of_bounds_range() {
    let input = b"count=41";
    let mut headers = QueryReqHeadersRaw::default();

    let result =
        <QueryReq as RouteRequestImpl>::parse_query_raw(&mut headers, input, 0..input.len() + 1);

    assert!(result.is_err(), "an invalid query range must not panic");
}

#[test]
fn body_plan_selects_buffering_from_its_source() {
    assert_eq!(RawBodyReq::BODY_POLICY, BodyPolicy::Buffered);
    assert_eq!(JsonBodyReq::BODY_POLICY, BodyPolicy::Buffered);
    assert_eq!(BodyLenReq::BODY_POLICY, BodyPolicy::Discarded);
}

#[test]
fn body_plan_connects_json_decoder() {
    let parsed = <JsonBodyReq as RouteRequestImpl>::parse_body(br#"{"value":42}"#)
        .expect("declared JSON body decodes");
    assert_eq!(parsed.value, 42);
}
