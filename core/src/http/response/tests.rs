use http::StatusCode;
use o3::buffer::{Owned, Shared};

use super::*;
use crate::http::request::LocalFrameBytes;

#[test]
fn response_ok_has_200_status() {
    let resp = Response::ok();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[test]
fn response_not_found_has_404_status() {
    let resp = Response::not_found();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[test]
fn response_text_sets_content_type_and_body() {
    let resp = Response::text("hello");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn response_text_with_status_sets_status_and_body() {
    let resp = Response::text_with_status(StatusCode::CREATED, "ok");
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert_eq!(resp.body_str(), Some("ok"));
}

#[test]
fn response_json_sets_content_type_and_serialized_body() {
    let value = serde_json::json!({ "k": "v" });
    let resp = Response::json(&value).unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );
    assert_eq!(resp.body_str(), Some("{\"k\":\"v\"}"));
}

#[test]
fn response_json_with_status_sets_custom_status() {
    let value = serde_json::json!({ "ok": true });
    let resp = Response::json_with_status(StatusCode::ACCEPTED, &value).unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert_eq!(resp.body_str(), Some("{\"ok\":true}"));
}

#[test]
fn insert_header_adds_header_retrievable_via_headers() {
    let mut resp = Response::ok();
    resp.insert_header(
        http::header::HeaderName::from_static("x-test"),
        http::HeaderValue::from_static("1"),
    );

    assert_eq!(resp.headers().get("x-test").unwrap(), "1");
}

#[test]
fn content_type_sets_content_type_header() {
    let mut resp = Response::ok();
    resp.content_type("text/html");
    assert_eq!(resp.headers().get("content-type").unwrap(), "text/html");
}

#[test]
fn set_body_str_and_body_str_round_trip() {
    let mut resp = Response::ok();
    resp.set_body_str("hello");
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn into_body_returns_owned_bytes_mut() {
    let mut resp = Response::ok();
    resp.set_body_str("owned");
    let body = resp.into_body();
    assert_eq!(body, Owned::from(&b"owned"[..]));
}

#[test]
fn body_str_returns_none_for_non_utf8() {
    let mut resp = Response::ok();
    resp.set_body(Owned::from(&[0xff_u8, 0xfe_u8][..]));
    assert_eq!(resp.body_str(), None);
}

#[test]
fn is_chunked_false_by_default() {
    let resp = Response::ok();
    assert!(!resp.is_chunked());
}

#[test]
fn push_chunk_makes_response_chunked() {
    let mut resp = Response::ok();
    resp.push_chunk(Shared::from_static(b"first"));
    assert!(resp.is_chunked());
}

#[test]
fn chunked_parts_returns_chunks_in_order() {
    let mut resp = Response::ok();
    resp.push_chunk(Shared::from_static(b"a"));
    resp.push_chunk(Shared::from_static(b"b"));
    let parts = resp.chunked_parts().unwrap();
    assert_eq!(
        parts,
        &[Shared::from_static(b"a"), Shared::from_static(b"b")]
    );
}

#[test]
fn wire_headers_append_and_expose_encoded_lines() {
    let mut resp = Response::ok();
    resp.append_wire_header_static("cache-control", "no-cache");
    resp.append_wire_header("x-bench-key", "k01");
    assert!(resp.has_wire_headers());
    assert_eq!(
        resp.wire_headers(),
        b"cache-control: no-cache\r\nx-bench-key: k01\r\n"
    );
}

#[test]
#[should_panic(expected = "managed headers")]
fn wire_headers_reject_managed_header_name() {
    let mut resp = Response::ok();
    let _ = resp.append_wire_header_static("content-length", "10");
}

#[test]
fn response_accepts_local_frame_body_without_eager_copy() {
    let mut resp = Response::ok();
    resp.set_body(LocalFrameBytes::from_slice(b"xxpayloadyy").slice(2..9));
    assert_eq!(resp.body(), b"payload");
    assert!(resp.body_is_shared());
}

#[test]
fn response_accepts_text_body_via_shared_bytes() {
    let mut body = TextBody::new();
    body.push_static(b"ok:");
    body.push_local(LocalFrameBytes::from_slice(b"xxk01yy").slice(2..5));
    body.push_static(b":1");

    let mut resp = Response::ok();
    resp.set_body(body);

    assert_eq!(resp.body(), b"ok:k01:1");
    assert!(resp.body_is_shared());
}

#[test]
fn text_body_from_items_materializes_shared_bytes() {
    let body = TextBody::from_items([
        TextItem::Static(b"ok:"),
        TextItem::Local(LocalFrameBytes::from_slice(b"xxk01yy").slice(2..5)),
        TextItem::Static(b":1"),
    ]);

    let mut resp = Response::ok();
    resp.set_body(body);

    assert_eq!(resp.body(), b"ok:k01:1");
    assert!(resp.body_is_shared());
}

#[test]
fn direct_response_plan_push_token_writes_header_line() {
    let mut plan = ResponsePlan::ok();
    let name = HeaderNameToken::new("x-bench-key");
    let _ = plan.push_token(name, "k01");
    assert_eq!(plan.wire_headers().as_ref(), b"x-bench-key: k01\r\n");
}

#[test]
fn direct_response_plan_push_token_static_writes_header_line() {
    let mut plan = ResponsePlan::ok();
    let name = HeaderNameToken::new("cache-control");
    let value = HeaderStaticValueToken::new("no-cache");
    let _ = plan.push_token_static(name, value);
    assert_eq!(plan.wire_headers().as_ref(), b"cache-control: no-cache\r\n");
}

#[test]
fn direct_response_plan_push_token_small_keeps_header_line() {
    let mut plan = ResponsePlan::ok();
    let name = HeaderNameToken::new("x-bench-nonce");
    let _ = plan.push_token(name, "100008");
    assert_eq!(plan.wire_headers().as_ref(), b"x-bench-nonce: 100008\r\n");
}

#[test]
fn direct_response_plan_respond_mono_keeps_wire_parts() {
    let mut plan = ResponsePlan::ok();
    let _ = plan.push("x-bench-key", "k01");
    let mut body = TextBody::new();
    body.push_static(b"o");
    body.push_static(b"k");
    let response = plan.respond_mono(body);
    let head = &response.head;
    match head {
        HotHeadInner::Wire(_) => panic!("direct mono response must preserve direct headers"),
        HotHeadInner::Direct(head) => {
            assert_eq!(head.static_headers(), b"");
            assert_eq!(head.headers().wire_len(), b"x-bench-key: k01\r\n".len());
        }
    }
    let resp = Response::from(response);

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().is_empty());
    assert_eq!(resp.wire_headers(), b"x-bench-key: k01\r\n");
    assert_eq!(resp.body(), b"ok");
}

#[test]
fn response_plan_respond_text_keeps_text_body() {
    let plan = ResponsePlan::ok();
    let body = TextBody::from_items([
        TextItem::Static(b"ok:"),
        TextItem::Static(b"k01"),
        TextItem::Static(b":"),
        TextItem::Static(b"1"),
    ]);
    let response = plan.respond_text(body);

    assert_eq!(response.status(), StatusCode::OK);
    let mut buf = vec![0u8; response.body.body_len()];
    response.body.write_to(&mut buf);
    assert_eq!(&buf[..], b"ok:k01:1");
    assert!(response.headers().is_empty());
}

#[test]
fn direct_response_plan_respond_text_keeps_direct_parts() {
    let mut plan = ResponsePlan::ok();
    let _ = plan.push("x-bench-key", "k01");
    let body = TextBody::from_items([TextItem::Static(b"ok:"), TextItem::Static(b"k01")]);
    let response = plan.respond_text(body);

    assert_eq!(response.status(), StatusCode::OK);
    let mut buf = vec![0u8; response.body.body_len()];
    response.body.write_to(&mut buf);
    assert_eq!(&buf[..], b"ok:k01");
    let resp = Response::from(response);
    assert_eq!(resp.wire_headers(), b"x-bench-key: k01\r\n");
}

#[test]
fn custom_into_serve_response_builds_direct_text_late() {
    #[derive(Clone)]
    struct Reply {
        key: LocalFrameBytes,
        nonce: LocalFrameBytes,
    }

    impl IntoServeResponse<'static> for Reply {
        fn into_serve_response(self) -> Serve {
            let mut plan = ResponsePlan::from_static(StatusCode::OK, b"");
            let key_name = HeaderNameToken::new("x-bench-key");
            let nonce_name = HeaderNameToken::new("x-bench-nonce");
            let _ = plan.push_local_token(key_name, self.key.clone());
            let _ = plan.push_local_token(nonce_name, self.nonce.clone());
            let mut body = TextBody::new();
            let _ = body.push_static(b"ok:");
            let _ = body.push_local(self.key);
            let _ = body.push_static(b":");
            let _ = body.push_local(self.nonce);
            plan.respond_text(body).into_serve_response()
        }
    }

    let reply = Reply {
        key: LocalFrameBytes::from_slice(b"xxk01yy").slice(2..5),
        nonce: LocalFrameBytes::from_slice(b"00100008").slice(2..8),
    };
    let response = reply.into_serve_response();
    let resp = response.into_response();
    assert_eq!(resp.body(), b"ok:k01:100008");
    assert_eq!(
        resp.wire_headers(),
        b"x-bench-key: k01\r\nx-bench-nonce: 100008\r\n"
    );
}

#[test]
fn fixed_response_write_into_slice_round_trips_plaintext() {
    let resp = FixedResponse::direct(
        StatusCode::OK,
        b"Content-Type: text/plain\r\n",
        Headers::new(),
        Shared::from_static(b"Hello, World!"),
    );
    let date: [u8; 29] = *b"Sun, 06 Nov 1994 08:49:37 GMT";
    let mut out = [0u8; 256];
    let n = resp.write_into_slice(&mut out, &date).expect("fits");
    let wire = std::str::from_utf8(&out[..n]).expect("ascii");
    assert!(
        wire.starts_with("HTTP/1.1 200 OK\r\n"),
        "status line: {wire:?}"
    );
    assert!(wire.contains("Content-Type: text/plain\r\n"));
    assert!(wire.contains("Content-Length: 13\r\n"));
    assert!(wire.contains("Server: sark\r\n"));
    assert!(wire.contains("Date: Sun, 06 Nov 1994 08:49:37 GMT\r\n"));
    assert!(wire.ends_with("\r\n\r\nHello, World!"));
}

#[test]
fn fixed_response_write_into_slice_returns_none_when_too_small() {
    let resp = FixedResponse::direct(
        StatusCode::OK,
        b"",
        Headers::new(),
        Shared::from_static(b"body"),
    );
    let date = *b"Sun, 06 Nov 1994 08:49:37 GMT";
    let mut tiny = [0u8; 32];
    assert!(resp.write_into_slice(&mut tiny, &date).is_none());
}

#[test]
fn mono_write_head_only_emits_head_then_returns_static_body() {
    static BODY: &[u8] = b"hello world body";
    let resp = MonoResponseInner::from_static_slice_body(
        StatusCode::OK,
        b"cache-control: no-store\r\n",
        Headers::new(),
        BODY,
    );
    let date = *b"Sun, 06 Nov 1994 08:49:37 GMT";
    let mut out = vec![0u8; 4096];
    let (n, body_out) = resp.write_head_only(&mut out, &date).expect("emit ok");
    let head = std::str::from_utf8(&out[..n]).expect("ascii head");
    assert!(
        head.starts_with("HTTP/1.1 200 OK\r\n"),
        "wire prefix: {head:?}"
    );
    assert!(
        head.contains("Content-Length: 16\r\n"),
        "no Content-Length match: {head:?}"
    );
    assert!(
        head.contains("cache-control: no-store\r\n"),
        "no static header: {head:?}"
    );
    assert!(
        head.contains("Server: sark\r\n"),
        "no server line: {head:?}"
    );
    assert!(
        head.contains("Date: Sun, 06 Nov 1994 08:49:37 GMT\r\n"),
        "no date line: {head:?}"
    );
    assert!(head.ends_with("\r\n\r\n"), "no head terminator: {head:?}");
    assert_eq!(body_out.as_ptr(), BODY.as_ptr());
    assert_eq!(body_out.len(), BODY.len());
}

#[test]
fn mono_write_into_slice_with_static_body_emits_head_and_body() {
    static BODY: &[u8] = b"abcd";
    let resp = MonoResponseInner::from_static_slice_body(StatusCode::OK, b"", Headers::new(), BODY);
    let date = *b"Sun, 06 Nov 1994 08:49:37 GMT";
    let mut out = vec![0u8; 1024];
    let n = resp.write_into_slice(&mut out, &date).expect("emit ok");
    let wire = &out[..n];
    let body_off = wire.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    assert_eq!(&wire[body_off..n], BODY);
}

#[test]
fn dyn_write_head_only_forwards_to_mono() {
    static BODY: &[u8] = b"forwarded";
    let mono = MonoResponseInner::from_static_slice_body(StatusCode::OK, b"", Headers::new(), BODY);
    let dyn_resp = ServeInner::Mono(mono);
    let date = *b"Sun, 06 Nov 1994 08:49:37 GMT";
    let mut out = vec![0u8; 1024];
    let (n, body_out) = dyn_resp
        .write_head_only(&mut out, &date)
        .expect("Mono forward");
    assert!(n > 0);
    assert_eq!(body_out.as_ptr(), BODY.as_ptr());
}
