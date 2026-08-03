use sark::request::Ref;
use sark::service::body::{Buffered, Discarded};
use sark::service::{BodyPolicy, HeaderParse, RouteParams, RouteRequestImpl};

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

#[sark_gen::request]
struct MinimalHeadReq {}

#[sark_gen::request(full)]
struct FullHeadReq {}

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
    let input: &[u8] = b"Accept-Encoding: gzip\r\n\r\n";
    let HeaderParse::Ready {
        headers,
        accept_gzip,
        ..
    } = <EncodingReq as RouteRequestImpl>::parse_headers::<false>(input, 0, 16)
    else {
        panic!("valid Accept-Encoding header");
    };

    assert!(accept_gzip);
    assert_eq!(headers.accept_encoding, Some(17..21));
}

#[test]
fn optional_known_headers_follow_capabilities() {
    fn accept_gzip<R: RouteRequestImpl>(block: &[u8], enabled: bool) -> bool {
        let parsed = if enabled {
            R::parse_headers::<true>(block, 0, 16)
        } else {
            R::parse_headers::<false>(block, 0, 16)
        };
        let HeaderParse::Ready { accept_gzip, .. } = parsed else {
            panic!("valid header block");
        };
        accept_gzip
    }

    let block = b"Accept-Encoding: gzip\r\n\r\n";
    assert!(!accept_gzip::<MinimalHeadReq>(block, false));
    assert!(accept_gzip::<MinimalHeadReq>(block, true));

    assert!(matches!(
        FullHeadReq::parse_headers::<false>(
            b"Expect: 100-continue\r\nExpect: 100-continue\r\n\r\n",
            0,
            16,
        ),
        HeaderParse::Bad,
    ));
    const {
        assert!(!<MinimalHeadReq as RouteRequestImpl>::FULL);
        assert!(<FullHeadReq as RouteRequestImpl>::FULL);
    }
}

#[test]
fn protocol_framing_is_always_parsed() {
    let input: &[u8] = b"Content-Length: 41\r\n\r\n";
    let HeaderParse::Ready { body_framing, .. } =
        MinimalHeadReq::parse_headers::<false>(input, 0, 16)
    else {
        panic!("valid framing header");
    };
    assert_eq!(
        body_framing,
        sark::sark_core::http::codec::BodyFraming::Length(41)
    );
}

#[test]
fn declared_custom_header_value_is_validated() {
    let input: &[u8] = b"X-Token: good\nbad\r\n\r\n";
    assert!(
        matches!(
            HdrReq::parse_headers::<false>(input, 0, 16),
            HeaderParse::Bad
        ),
        "a bare LF must not bypass value validation"
    );
}

#[test]
fn generated_unknown_header_scan_rejects_smuggling_bytes() {
    for block in [
        b"X-Smuggle: foo\nbar\r\n\r\n".as_slice(),
        b"X-Smuggle: foo\rbar\r\n\r\n",
        b"X-Smuggle: foo\x00bar\r\n\r\n",
        b"X-Smuggle: foo\x07bar\r\n\r\n",
        b"X-Smuggle: foo\x7fbar\r\n\r\n",
    ] {
        assert!(matches!(
            MinimalHeadReq::parse_headers::<false>(block, 0, 16),
            HeaderParse::Bad,
        ));
    }
}

#[test]
fn generated_unknown_header_scan_accepts_visible_bytes_and_htab() {
    assert!(matches!(
        MinimalHeadReq::parse_headers::<false>(
            b"Host: example.com\r\nUser-Agent: x/1\r\nX-Note: foo\tbar\r\n\r\n",
            0,
            16,
        ),
        HeaderParse::Ready { .. },
    ));
}

#[test]
fn generated_ignored_header_scan_handles_lane_boundaries() {
    let mut long = Vec::from(b"User-Agent: ".as_slice());
    long.extend_from_slice(&[b'a'; 64]);
    long.extend_from_slice(b"\r\n\r\n");
    assert!(matches!(
        MinimalHeadReq::parse_headers::<false>(&long, 0, 16),
        HeaderParse::Ready { .. },
    ));

    let incomplete = b"User-Agent: 12345678901234\r";
    assert!(matches!(
        MinimalHeadReq::parse_headers::<false>(incomplete, 0, 16),
        HeaderParse::NeedMore,
    ));
}

#[test]
fn generated_header_count_is_enforced_at_the_parse_loop() {
    assert!(matches!(
        MinimalHeadReq::parse_headers::<false>(b"\r\n", 0, 0),
        HeaderParse::Ready { .. },
    ));
    assert!(matches!(
        MinimalHeadReq::parse_headers::<false>(b"Host: example.com\r\n\r\n", 0, 0),
        HeaderParse::Bad,
    ));
}

#[test]
fn generated_header_scan_enforces_line_limit() {
    let mut block = Vec::from(b"X-Long: ".as_slice());
    block.resize(
        block.len() + sark::sark_core::http::head::MAX_HEADER_LINE_BYTES + 16,
        b'a',
    );
    block.extend_from_slice(b"\r\n\r\n");
    assert!(matches!(
        MinimalHeadReq::parse_headers::<false>(&block, 0, 16),
        HeaderParse::Bad,
    ));
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
fn ordered_query_names_match_exactly() {
    let input = b"countdown=41&limit=7";
    let mut headers = OrderedQueryReqHeadersRaw::default();

    let result =
        <OrderedQueryReq as RouteRequestImpl>::parse_query_raw(&mut headers, input, 0..input.len());

    assert!(result.is_err(), "a field-name prefix must not be accepted");
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
    fn buffered<T: RouteRequestImpl<BodyMode = Buffered>>() {}
    fn discarded<T: RouteRequestImpl<BodyMode = Discarded>>() {}

    buffered::<RawBodyReq>();
    buffered::<JsonBodyReq>();
    discarded::<BodyLenReq>();
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
