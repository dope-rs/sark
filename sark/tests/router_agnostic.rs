use http::{Method, StatusCode};
use o3::buffer::{Bytes, Retained};
use sark::dispatch::{Decode, Pipeline};
use sark::service::{RouteRequestImpl, RouteSpec, SliceValue};
use sark_core::http::Shape;

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::request]
struct PlainReq {}

#[sark_gen::handler]
fn plain_h(_req: PlainReq, _state: &sark::EmptyState) -> Reply {
    Reply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

#[sark_gen::request]
struct NamedReq {
    #[header("x-name", default = "none")]
    x_name: Bytes<Retained>,
}

#[sark_gen::handler]
fn named_h(req: NamedReq, _state: &sark::EmptyState) -> Reply {
    let status = if req.x_name.as_slice() == b"alice" {
        StatusCode::IM_A_TEAPOT
    } else {
        StatusCode::OK
    };
    Reply {
        status,
        body: b"ok",
    }
}

sark_gen::define_route! {
    AgnApp: sark::EmptyState => {
        GET "/json" => plain_h,
        GET "/named" => named_h,
    }
}

#[derive(Default)]
struct Capture {
    status: Option<StatusCode>,
    headers: Vec<u8>,
    body: Vec<u8>,
    calls: usize,
}

impl sark::dispatch::ResponseEncoder for Capture {
    fn emit(&mut self, status: StatusCode, headers_wire: &[u8], body: &[u8]) {
        self.status = Some(status);
        self.headers = headers_wire.to_vec();
        self.body = body.to_vec();
        self.calls += 1;
    }
}

#[test]
fn agnostic_dispatch_routes_feeds_invokes_encodes() {
    let app = AgnApp::new::<dope_net::wire::identity::Identity>(
        sark::EmptyState,
        sark::app::Config {
            timer_capacity: 1,
            task_capacity: 1,
        },
    );

    let mut cap = Capture::default();
    let out = app.dispatch_decoded(Method::GET, b"/json", &[], &[], &[], &mut cap);
    assert_eq!(out, sark::dispatch::Decoded::Emitted);
    assert_eq!(cap.status, Some(StatusCode::OK));
    assert_eq!(cap.body, b"ok");
    assert_eq!(cap.calls, 1);
    assert!(cap.headers.is_empty());

    let head: &[u8] = b"alice";
    let mut cap2 = Capture::default();
    let out2 = app.dispatch_decoded(
        Method::GET,
        b"/named",
        &[(b"x-name", 0..head.len())],
        head,
        &[],
        &mut cap2,
    );
    assert_eq!(out2, sark::dispatch::Decoded::Emitted);
    assert_eq!(cap2.status, Some(StatusCode::IM_A_TEAPOT));

    let mut cap3 = Capture::default();
    let out3 = app.dispatch_decoded(Method::GET, b"/nope", &[], &[], &[], &mut cap3);
    assert_eq!(out3, sark::dispatch::Decoded::NotFound);
    assert_eq!(cap3.calls, 0);
}

fn write_response<'r, R: RouteSpec>(resp: &R::Response<'r>) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let date = [b' '; 29];
    let n = Shape::write_into_slice(resp, &mut buf, &date).expect("write response");
    buf.truncate(n);
    buf
}

#[test]
fn agnostic_core_runs_without_h1_buffer() {
    let route = plain_h;
    let raw_params = <plain_h as RouteSpec>::RawParams::default();
    let raw_headers = <plain_h as RouteSpec>::RawHeaders::default();
    let resp = Pipeline::build_and_invoke::<plain_h, sark::EmptyState>(
        &route,
        raw_params,
        raw_headers,
        0..0,
        &[],
        &[],
        0,
        sark::EmptyState::REF,
    )
    .expect("build_and_invoke");
    assert_eq!(resp.status(), StatusCode::OK);
    let (_, _, body) = Shape::preserialize_static(&resp).expect("static body");
    assert_eq!(body, b"ok");
    let bytes = write_response::<plain_h>(&resp);
    assert!(bytes.starts_with(b"HTTP/1.1 200"));
    assert!(bytes.ends_with(b"ok"));
}

#[test]
fn synthesized_header_pair_flows_through_route() {
    let route = named_h;
    let raw_params = <named_h as RouteSpec>::RawParams::default();
    let mut raw_headers = <named_h as RouteSpec>::RawHeaders::default();

    let name: &[u8] = b"x-name";
    let head: &[u8] = b"alice";
    let slot = <<named_h as RouteSpec>::Request as RouteRequestImpl>::header_slot_bytes(name)
        .expect("route declares x-name");
    <<named_h as RouteSpec>::Request as RouteRequestImpl>::set_header_raw(
        &mut raw_headers,
        slot,
        &SliceValue::new(head, 0..head.len()),
    )
    .expect("set_header_raw");

    let resp = Pipeline::build_and_invoke::<named_h, sark::EmptyState>(
        &route,
        raw_params,
        raw_headers,
        0..0,
        head,
        &[],
        0,
        sark::EmptyState::REF,
    )
    .expect("build_and_invoke");
    let bytes = write_response::<named_h>(&resp);
    assert!(
        bytes.starts_with(b"HTTP/1.1 418"),
        "x-name fed as a (name,value) pair must reach the handler"
    );
}
