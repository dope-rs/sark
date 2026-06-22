#![cfg(any())]
#![allow(clippy::too_many_arguments, unreachable_code)]

use o3::buffer::Shared;
use sark::json::JsonDecode;
use sark::service::{HeadParts, Key, ServeView, SlicePath};
use sark_core::http::LocalFrameBytes;
#[sark_gen::request]
struct StaticReq {}

#[sark_gen::request]
struct RoutingReq {
    #[path("user_id", default = "na")]
    user_id: LocalFrameBytes,
    #[path("order_id", default = "na")]
    order_id: LocalFrameBytes,
}

#[sark_gen::handler]
#[state(())]
#[request(StaticReq)]
fn statik(_request: StaticReq, _state: &()) -> sark_core::http::Serve {
    unimplemented!()
}

#[sark_gen::handler]
#[state(())]
#[request(RoutingReq)]
fn routing(_request: RoutingReq, _state: &()) -> sark_core::http::Serve {
    unimplemented!()
}

#[sark_gen::handler]
#[state(())]
#[body(EchoBody)]
#[request(EchoReq)]
fn echo(_request: EchoReq, _state: &()) -> sark_core::http::Serve {
    unimplemented!()
}

#[sark_gen::request(ordered)]
#[json_body(EchoBody)]
struct EchoReq {
    #[header("x-bench-key", default = "none")]
    x_bench_key: LocalFrameBytes,
}

#[sark_gen::json(ordered, preserve)]
struct EchoBody {
    #[field(plain)]
    kind: LocalFrameBytes,
    #[field(raw)]
    nonce: LocalFrameBytes,
    #[field(plain)]
    key: LocalFrameBytes,
    #[field(plain)]
    payload: LocalFrameBytes,
}

#[sark_gen::json(ordered, preserve, exact)]
struct EchoSkipBody {
    #[field(unused, plain)]
    kind: LocalFrameBytes,
    #[field(raw)]
    nonce: LocalFrameBytes,
    #[field(unused, plain)]
    key: LocalFrameBytes,
    #[field(unused, plain)]
    payload: LocalFrameBytes,
}

sark_gen::define_route! {
    BenchDispatch: () => {
        GET "/" => statik,
        GET "/users/:user_id/orders/:order_id" => routing,
        POST "/echo" => echo,
    }
}

#[test]
fn route_tags_are_distinct() {
    let app = bench_dispatch::new(&());
    let s = <BenchDispatch as ServeView<()>>::select_parts_unprepared_probe(
        &app,
        Key::Get,
        &SlicePath::new(b"/"),
    );
    let r = <BenchDispatch as ServeView<()>>::select_parts_unprepared_probe(
        &app,
        Key::Get,
        &SlicePath::new(b"/users/41/orders/400"),
    );
    let e = <BenchDispatch as ServeView<()>>::select_parts_unprepared_probe(
        &app,
        Key::Post,
        &SlicePath::new(b"/echo"),
    );
    let miss_echo_get = <BenchDispatch as ServeView<()>>::select_parts_unprepared_probe(
        &app,
        Key::Get,
        &SlicePath::new(b"/echo"),
    );
    assert_ne!(s.route_tag(), r.route_tag());
    assert_ne!(s.route_tag(), e.route_tag());
    assert_ne!(r.route_tag(), e.route_tag());
    assert_ne!(miss_echo_get.route_tag(), e.route_tag());
}

#[test]
fn json_body_decode_parses_flat_object() {
    let body = EchoBody::decode_json(Shared::from_static(
        br#"{"kind":"bench","nonce":100000,"key":"k00","payload":"abc"}"#,
    ))
    .expect("decode");
    assert_eq!(body.kind.as_bytes(), b"bench");
    assert_eq!(body.nonce.as_bytes(), b"100000");
    assert_eq!(body.key.as_bytes(), b"k00");
    assert_eq!(body.payload.as_bytes(), b"abc");
}

#[test]
fn json_body_encode_keeps_raw_nonce_token() {
    let body = EchoBody::new(
        LocalFrameBytes::from_shared(Shared::from_static(b"bench")),
        LocalFrameBytes::from_shared(Shared::from_static(b"100000")),
        LocalFrameBytes::from_shared(Shared::from_static(b"k00")),
        LocalFrameBytes::from_shared(Shared::from_static(b"abc")),
    );
    let out = sark::json::JsonEncode::encode_json(&body);
    assert_eq!(
        out.as_ref(),
        br#"{"kind":"bench","nonce":100000,"key":"k00","payload":"abc"}"#
    );
}

#[test]
fn json_body_preserve_reuses_original_bytes() {
    let raw =
        Shared::from_static(br#"{"kind":"bench","nonce":100000,"key":"k00","payload":"abc"}"#);
    let body = EchoBody::decode_json(raw.clone()).expect("decode");
    let out = sark::json::Json::ok_preserve(&body);
    assert_eq!(out.body(), raw.as_ref());
}

#[test]
fn json_body_unused_skips_materialization_but_preserves_body() {
    let raw =
        Shared::from_static(br#"{"kind":"bench","nonce":100000,"key":"k00","payload":"abc"}"#);
    let body = EchoSkipBody::decode_json(raw.clone()).expect("decode");
    assert!(body.kind.is_empty());
    assert_eq!(body.nonce.as_bytes(), b"100000");
    assert!(body.key.is_empty());
    assert!(body.payload.is_empty());
    let out = sark::json::Json::ok_preserve(&body);
    assert_eq!(out.body(), raw.as_ref());
}
