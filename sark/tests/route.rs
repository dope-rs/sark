#![cfg(any())]
#![allow(clippy::too_many_arguments, unreachable_code)]

use o3::buffer::{Bytes, Retained, Shared};
use sark::json::JsonDecode;
use sark::service::{HeadParts, Key, ServeView, SlicePath};
#[sark_gen::request]
struct StaticReq {}

#[sark_gen::request]
struct RoutingReq {
    #[path("user_id", default = "na")]
    user_id: Bytes<Retained>,
    #[path("order_id", default = "na")]
    order_id: Bytes<Retained>,
}

#[sark_gen::handler]
fn statik(_request: StaticReq, _state: &()) -> sark_core::http::Serve {
    unimplemented!()
}

#[sark_gen::handler]
fn routing(_request: RoutingReq, _state: &()) -> sark_core::http::Serve {
    unimplemented!()
}

#[sark_gen::handler]
fn echo(_request: EchoReq, _state: &()) -> sark_core::http::Serve {
    unimplemented!()
}

#[sark_gen::request(ordered)]
#[json_body(EchoBody)]
struct EchoReq {
    #[header("x-bench-key", default = "none")]
    x_bench_key: Bytes<Retained>,
}

#[sark_gen::json(ordered, preserve)]
struct EchoBody {
    #[field(plain)]
    kind: Bytes<Retained>,
    #[field(raw)]
    nonce: Bytes<Retained>,
    #[field(plain)]
    key: Bytes<Retained>,
    #[field(plain)]
    payload: Bytes<Retained>,
}

#[sark_gen::json(ordered, preserve, exact)]
struct EchoSkipBody {
    #[field(unused, plain)]
    kind: Bytes<Retained>,
    #[field(raw)]
    nonce: Bytes<Retained>,
    #[field(unused, plain)]
    key: Bytes<Retained>,
    #[field(unused, plain)]
    payload: Bytes<Retained>,
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
    let app = BenchDispatch::new(
        (),
        sark::app::Config {
            timer_capacity: 1,
            task_capacity: 1,
        },
    );
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
    assert_eq!(body.kind.as_slice(), b"bench");
    assert_eq!(body.nonce.as_slice(), b"100000");
    assert_eq!(body.key.as_slice(), b"k00");
    assert_eq!(body.payload.as_slice(), b"abc");
}

#[test]
fn json_body_encode_keeps_raw_nonce_token() {
    let body = EchoBody::new(
        Bytes::<Retained>::from(Shared::from_static(b"bench")),
        Bytes::<Retained>::from(Shared::from_static(b"100000")),
        Bytes::<Retained>::from(Shared::from_static(b"k00")),
        Bytes::<Retained>::from(Shared::from_static(b"abc")),
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
    assert_eq!(body.nonce.as_slice(), b"100000");
    assert!(body.key.is_empty());
    assert!(body.payload.is_empty());
    let out = sark::json::Json::ok_preserve(&body);
    assert_eq!(out.body(), raw.as_ref());
}
