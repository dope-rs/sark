use http::StatusCode;
use o3::buffer::{Bytes, Retained, SharedPool, SharedPoolLayout};
use sark::dispatch::{Decode, Invocation};
use sark::service::{RouteRequestImpl, RouteSpec, SliceValue};
use sark_core::http::{
    CacheTemplate, DecodedFieldBlock, Field, HeadBlock, HeadConsumer, HeadDisposition, HeadPlan,
    HeadSection, PlannedHead, Preparation, Prepared, Shape,
};

#[sark_gen::response(raw)]
#[header("content-type", "text/plain")]
struct Reply {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::request]
struct PlainReq {}

#[sark_gen::request(full)]
struct FullReq {}

#[sark_gen::handler]
fn plain_h(_req: PlainReq, _state: &sark::EmptyState) -> Reply {
    Reply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

#[sark_gen::handler]
fn full_h(_req: FullReq, _state: &sark::EmptyState) -> Reply {
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

#[sark_gen::request]
struct RoutedReq {
    #[path("id", default = "none")]
    id: Bytes<Retained>,
    #[query("tag", default = "none")]
    tag: Bytes<Retained>,
}

#[sark_gen::handler]
fn routed_h(req: RoutedReq, _state: &sark::EmptyState) -> Reply {
    let status = if req.id.as_slice() == b"42" && req.tag.as_slice() == b"rust" {
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
        GET "/full" => full_h,
        GET "/named" => named_h,
        GET "/items/:id" => routed_h,
    }
}

#[derive(Default)]
struct Capture {
    status: Option<StatusCode>,
    headers: Vec<u8>,
    body: Vec<u8>,
    calls: usize,
}

impl sark_core::http::ResponseSink for Capture {
    fn emit<'a, 'body, I>(
        &mut self,
        status: StatusCode,
        headers: I,
        body: sark_core::http::Body<'body>,
    ) where
        I: Iterator<Item = sark_core::http::Field<'a>>,
    {
        self.status = Some(status);
        for field in headers {
            self.headers.extend_from_slice(field.name);
            self.headers.extend_from_slice(b": ");
            self.headers.extend_from_slice(field.value);
            self.headers.extend_from_slice(b"\r\n");
        }
        self.body = body.as_bytes().to_vec();
        self.calls += 1;
    }
}

fn select_for<R: Decode>(_: &R, fields: DecodedFieldBlock) -> PlannedHead<R::Plan> {
    let mut section = HeadSection::<R::Plan>::new(true, false);
    let mut retained = Vec::new();
    for (field, _) in fields.iter_with_value_ranges() {
        let decision = section.disposition(field.name, None, &retained);
        assert!(section.decoded(decision, field, &retained));
        let disposition = decision.disposition;
        match disposition {
            HeadDisposition::Discard | HeadDisposition::Skip => {}
            HeadDisposition::Tagged(tag) => {
                let prefix = tag.prefix(field.value.len()).unwrap();
                let value_start = retained.len() + prefix.len();
                retained.extend_from_slice(&prefix);
                retained.extend_from_slice(field.value);
                let value_range = value_start..value_start + field.value.len();
                section.committed(disposition, value_range, &retained);
            }
            HeadDisposition::Raw => panic!("generated plan retained an untagged field"),
        }
    }
    let (selection, valid) = section.finish();
    assert!(valid);
    let layout = SharedPoolLayout::new(1, retained.len().max(1)).unwrap();
    let pool = SharedPool::from_layout(layout);
    let mut lease = pool.try_acquire().unwrap();
    lease
        .spare_writer()
        .try_extend_from_slice(&retained)
        .unwrap();
    let fields = <R::Plan as HeadPlan>::Block::from_pooled(lease.freeze());
    PlannedHead::new(fields, selection)
}

#[test]
fn agnostic_dispatch_routes_feeds_invokes_encodes() {
    let timer = sark::Timer::new();
    let app = AgnApp::new::<dope_net::wire::identity::Identity>(
        sark::EmptyState::REF,
        &timer,
        sark::app::Config { task_capacity: 1 },
    );

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":path", b"/json"),
    ])
    .unwrap();
    let prepared = app.prepare_full_head(fields).expect("known route");
    let mut cap = Capture::default();
    let out = app.dispatch_prepared(prepared, &[][..], &mut cap);
    assert_eq!(out, sark::dispatch::Decoded::Emitted);
    assert_eq!(cap.status, Some(StatusCode::OK));
    assert_eq!(cap.body, b"ok");
    assert_eq!(cap.calls, 1);
    assert_eq!(cap.headers, b"content-type: text/plain\r\n");

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":path", b"/named"),
        Field::new(b"x-name", b"alice"),
    ])
    .unwrap();
    let prepared = app.prepare_full_head(fields).expect("known route");
    let mut cap2 = Capture::default();
    let out2 = app.dispatch_prepared(prepared, &[][..], &mut cap2);
    assert_eq!(out2, sark::dispatch::Decoded::Emitted);
    assert_eq!(cap2.status, Some(StatusCode::IM_A_TEAPOT));

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":path", b"/named"),
        Field::new(b"x-ignored", b"before"),
        Field::new(b"x-name", b"alice"),
        Field::new(b"x-ignored", b"after"),
    ])
    .unwrap();
    let prepared = app
        .prepare_full_head(fields)
        .expect("declared field after an ignored field");
    let mut cap2 = Capture::default();
    let out2 = app.dispatch_prepared(prepared, &[][..], &mut cap2);
    assert_eq!(out2, sark::dispatch::Decoded::Emitted);
    assert_eq!(cap2.status, Some(StatusCode::IM_A_TEAPOT));

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":scheme", b"https"),
        Field::new(b":path", b"/items/42?tag=rust"),
    ])
    .unwrap();
    let prepared = app
        .prepare_full_head(fields)
        .expect("parameterized route with query");
    let mut cap3 = Capture::default();
    let out3 = app.dispatch_prepared(prepared, &[][..], &mut cap3);
    assert_eq!(out3, sark::dispatch::Decoded::Emitted);
    assert_eq!(cap3.status, Some(StatusCode::IM_A_TEAPOT));

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":path", b"/nope"),
    ])
    .unwrap();
    assert!(matches!(
        app.prepare_full_head(fields),
        Err(sark::dispatch::Decoded::NotFound)
    ));

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":path", b"/json?ignored=\x01"),
    ])
    .unwrap();
    assert!(
        app.prepare_full_head(fields).is_ok(),
        "unused target bytes stay outside the minimal route capability"
    );

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":path", b"/full?ignored=\x01"),
    ])
    .unwrap();
    assert!(matches!(
        app.prepare_full_head(fields),
        Err(sark::dispatch::Decoded::Bad)
    ));

    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":path", b"/json"),
        Field::new(b"x-regular", b"1"),
        Field::new(b":authority", b"example.test"),
    ])
    .unwrap();
    assert!(matches!(
        app.prepare_full_head(fields),
        Err(sark::dispatch::Decoded::Bad)
    ));
}

#[test]
fn planned_dispatch_extracts_only_the_chosen_parameter_route() {
    let timer = sark::Timer::new();
    let app = AgnApp::new::<dope_net::wire::identity::Identity>(
        sark::EmptyState::REF,
        &timer,
        sark::app::Config { task_capacity: 1 },
    );
    let fields = DecodedFieldBlock::from_fields(&[
        Field::new(b":method", b"GET"),
        Field::new(b":scheme", b"https"),
        Field::new(b":path", b"/items/42?tag=rust"),
    ])
    .unwrap();
    let prepared = app
        .prepare_planned_head(select_for(&app, fields))
        .expect("planned parameter route");
    let mut capture = Capture::default();
    let outcome = app.dispatch_prepared(prepared, &[][..], &mut capture);

    assert_eq!(outcome, sark::dispatch::Decoded::Emitted);
    assert_eq!(capture.status, Some(StatusCode::IM_A_TEAPOT));
}

fn write_response<'r, R: RouteSpec>(resp: R::Response<'r>) -> Vec<u8> {
    let mut out = [];
    let Prepared::Cache(template) = resp.prepare(Preparation::Cache, None, &mut out, &[0; 29])
    else {
        panic!("cacheable response")
    };
    match template {
        CacheTemplate::Inline { bytes, .. } => bytes,
        CacheTemplate::Static { mut head, body, .. } => {
            head.extend_from_slice(body);
            head
        }
    }
}

#[test]
fn agnostic_core_runs_without_h1_buffer() {
    let raw_params = <plain_h as RouteSpec>::RawParams::default();
    let raw_headers = <plain_h as RouteSpec>::RawHeaders::default();
    let resp = Invocation::new(0..0, &[], &[], 0)
        .invoke::<plain_h, _>(raw_params, raw_headers, sark::EmptyState::REF)
        .expect("build_and_invoke");
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = write_response::<plain_h>(resp);
    assert!(bytes.starts_with(b"HTTP/1.1 200"));
    assert!(bytes.ends_with(b"ok"));
}

#[test]
fn synthesized_header_pair_flows_through_route() {
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

    let resp = Invocation::new(0..0, head, &[], 0)
        .invoke::<named_h, _>(raw_params, raw_headers, sark::EmptyState::REF)
        .expect("build_and_invoke");
    let bytes = write_response::<named_h>(resp);
    assert!(
        bytes.starts_with(b"HTTP/1.1 418"),
        "x-name fed as a (name,value) pair must reach the handler"
    );
}
