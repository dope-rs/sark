pub mod common;

use std::{cell::Cell, convert::Infallible, pin::Pin};

use common::session::Reject;
use dope_test::alloc::Tracker;
use sark_h1::{pump, session::Conn, wire::head};
use sark_http::{
    h1::codec::canned,
    preface::{Prefix, Read},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn joins_the_prior_prefix_at_the_existing_line_owner() {
    for prefix in [b"P".as_slice(), b"PRI * "] {
        let (_, Read::Pending(prefix)) = Prefix::default().read(prefix).expect("prefix") else {
            panic!("partial preface");
        };
        let valid = prefix.as_ref() == b"P";
        let calls = Cell::new(0);
        let mut handler = Reject::new(&calls, None);
        handler.input = valid.then_some(b"POST /x HTTP/1.1\r\n".as_slice());
        let (cycle, allocations) = Tracker::count(|| {
            let mut conn = Conn::<(), Infallible>::prefixed(prefix);
            pump::Ingress::drive(
                &mut conn,
                Pin::new(&mut handler),
                b"OST /x HTTP/1.1\r\nHost: x\r\n\r\nbody",
                o3::buffer::write::Slice::new(&mut [0; 2048]).reborrow(),
                head::Connection::KeepAlive,
            )
        });
        assert_eq!(calls.get(), usize::from(valid));
        assert!(matches!(cycle.state, Ok(pump::Egress::Static { body, .. })
            if body.as_slice() == canned::BAD_REQUEST.as_slice()));
        assert_eq!(allocations, usize::from(valid));
    }
}

#[test]
fn retains_prefix_until_output_capacity_returns() {
    let (_, Read::Pending(prefix)) = Prefix::default().read(b"P").expect("prefix") else {
        panic!("partial preface");
    };
    let mut conn = Conn::<(), Infallible>::prefixed(prefix);
    pump::Ingress::retain(&mut conn, b"OST /x HTTP/1.1\r\nHost: x\r\n\r\n");
    let calls = Cell::new(0);
    let mut handler = Reject::new(&calls, None);
    handler.input = Some(b"POST /x HTTP/1.1\r\n");
    let cycle = pump::Ingress::drive(
        &mut conn,
        Pin::new(&mut handler),
        &[],
        o3::buffer::write::Slice::new(&mut [0; 2048]).reborrow(),
        head::Connection::KeepAlive,
    );
    assert_eq!(calls.get(), 1);
    assert!(matches!(cycle.state, Ok(pump::Egress::Static { body, .. })
        if body.as_slice() == canned::BAD_REQUEST.as_slice()));
}
