pub mod common;

use std::convert::Infallible;

use common::head::trace::{CONTEXTS, REPLACEMENTS, Read as _};
use dope_test::alloc::Tracker;
use o3::buffer::storage;
use sark_http::h1::{
    head::{
        Limit, ParseError,
        request::{
            Parts, offers,
            trace::{self, Context, Owned},
        },
    },
    switch,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn insert_preserves_owner() {
    let wire = b"POST /items?q=one HTTP/1.1\r\nHost: example.test\r\nContent-Length: 4\r\n\r\nbody";
    let head = Owned::try_read(wire, trace::Parent::HEADER_BYTES)
        .expect("valid trace fixture")
        .expect("complete head");
    let ptr = head.as_ref().bytes().as_ptr();
    let end = head.head_end().get() as usize;
    let parent = trace::Parent::from_ids(&[0x12; 16], &[0x34; 8]);
    let (result, count) = Tracker::measure(|| head.with_parent(&parent));
    let head = result.expect("reserved trace header");
    assert_eq!(count, [0; 4]);
    assert_eq!(head.as_ref().bytes().as_ptr(), ptr);
    assert_eq!(
        head.head_end().get() as usize,
        end + trace::Parent::HEADER_BYTES
    );
    assert_eq!(head.as_ref().method(), "POST");
    assert_eq!(head.as_ref().target(), "/items?q=one");
    assert_eq!(head.as_ref().authority(), "example.test");
    assert_eq!(head.as_ref().route_path(), Some("/items"));
    assert!(head.as_ref().bytes().ends_with(b"\r\n\r\nbody"));
    let context = head.context().expect("installed trace");
    assert_eq!(context.trace_id, [0x12; 16]);
    assert_eq!(context.span_id, [0x34; 8]);
}

#[test]
fn replace_before_host() {
    for wire in REPLACEMENTS {
        let mut head = Owned::try_read(wire, trace::Parent::HEADER_BYTES)
            .expect("valid trace fixture")
            .expect("complete head");
        assert!(head.context().is_none());
        let ptr = head.as_ref().bytes().as_ptr();
        for byte in [0x12, 0x56] {
            let parent = trace::Parent::from_ids(&[byte; 16], &[0x34; 8]);
            let (result, count) = Tracker::measure(|| head.with_parent(&parent));
            head = result.expect("reserved replacement");
            assert_eq!(count, [0; 4]);
            assert_eq!(head.as_ref().bytes().as_ptr(), ptr);
            assert_eq!(head.as_ref().authority(), "example.test");
            assert_eq!(head.as_ref().route_path(), Some("/items"));
            assert!(head.as_ref().bytes().ends_with(b"\r\n\r\nbody"));
            assert_eq!(
                head.context().expect("replaced context").trace_id,
                [byte; 16]
            );
            let mut context = None;
            let parsed = Parts::parse(head.as_ref().bytes(), Limit::DEFAULT, |field| {
                if field.name().eq_ignore_ascii_case(b"traceparent") {
                    context = Context::parse(field.value());
                }
                Ok::<(), Infallible>(())
            })
            .expect("rewritten syntax")
            .expect("complete rewrite")
            .into_route();
            assert_eq!(parsed.head_end(), head.head_end());
            assert_eq!(parsed.authority(), head.as_ref().authority());
            assert!(context == head.context());
        }
    }
}

#[test]
fn retain_offers() {
    let wire = b"GET /chat HTTP/1.1\r\ntraceparent: x\r\nHost: example.test\r\nConnection: upgrade\r\nUpgrade: websocket, h2c\r\n\r\n";
    let head = Owned::try_read(wire, trace::Parent::HEADER_BYTES)
        .expect("valid trace fixture")
        .expect("complete head");
    let parent = trace::Parent::from_ids(&[1; 16], &[2; 8]);
    let head = head.with_parent(&parent).expect("reserved replacement");
    assert!((head.transition()) == (switch::Intent::Upgrade));
    let offer = offers::View::from_route(head.as_ref()).expect("admitted offers");
    let offer = offer.retain().expect("retained offers");
    (offer.select(b"websocket"))
        .as_ref()
        .expect("test operation must succeed");
    assert!(matches!(
        offer.select(b"other"),
        Err(switch::UpgradeError::NotOffered)
    ));
}

#[test]
fn exact_capacity_failure() {
    let wire = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    let head = Owned::try_read(wire, 0)
        .expect("valid trace fixture")
        .expect("complete head");
    let parent = trace::Parent::from_ids(&[1; 16], &[2; 8]);
    let (result, count) = Tracker::measure(|| head.with_parent(&parent));
    let (need, room) = result
        .err()
        .expect("no space for the first write")
        .into_parts();
    assert_eq!(need.get(), trace::Parent::HEADER_BYTES);
    assert_eq!(room, 0);
    assert_eq!(count[0], 0);
    assert_eq!(count[1], 0);
}

#[test]
fn duplicate_lines() {
    let wire = b"GET / HTTP/1.1\r\nHost: example.test\r\ntraceparent: x\r\ntraceparent: y\r\n\r\n";
    let mut bytes = storage::Owned::try_with(wire.len()).expect("test capacity");
    bytes.try_extend(wire).expect("test input fits");
    let result = Parts::traced(bytes, Limit::DEFAULT);
    let cause = result.err().expect("duplicate trace fields");
    let cause = match cause {
        ParseError::Sink(cause) => Some(cause),
        _ => None,
    }
    .expect("exact trace duplicate");
    assert_eq!(
        cause.first.get(wire),
        Some(b"traceparent: x\r\n".as_slice())
    );
    assert_eq!(cause.next.get(wire), Some(b"traceparent: y\r\n".as_slice()));
}

#[test]
fn admits_trace_versions() {
    for &(value, valid) in CONTEXTS {
        assert_eq!(Context::parse(value).is_some(), valid, "{value:?}");
    }
}
