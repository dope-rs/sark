pub mod common;

use std::convert::Infallible;

use common::session::Frame;
use dope_test::alloc::Tracker;
use sark_h1::{session::Conn, wire::output};
use sark_http::preface::{Prefix, Read};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn joins_the_prior_prefix_at_the_existing_line_owner() {
    for prefix in [b"P".as_slice(), b"PRI * "] {
        let (_, Read::Pending(prefix)) = Prefix::default().read(prefix).expect("prefix") else {
            panic!("partial preface");
        };
        let valid = prefix.as_ref() == b"P";
        let mut handler = Frame::reject();
        handler.input = valid.then_some(b"POST /x HTTP/1.1\r\n".as_slice());
        let ((), allocations) = Tracker::count(|| {
            let mut conn = Conn::<(), Infallible>::prefixed(prefix);
            handler.rejects(&mut conn, b"OST /x HTTP/1.1\r\nHost: x\r\n\r\nbody")
        });
        assert_eq!(handler.calls, usize::from(valid));
        assert_eq!(allocations, usize::from(valid));
    }
}

#[test]
fn retains_prefix_until_output_capacity_returns() {
    let (_, Read::Pending(prefix)) = Prefix::default().read(b"P").expect("prefix") else {
        panic!("partial preface");
    };
    let mut conn = Conn::<(), Infallible>::prefixed(prefix);
    conn.retain(b"OST /x HTTP/1.1\r\nHost: x\r\n\r\n");
    let mut handler = Frame::reject();
    handler.input = Some(b"POST /x HTTP/1.1\r\n");
    handler.rejects(&mut conn, &[]);
    assert_eq!(handler.calls, 1);
}

#[test]
fn resumes_only_through_the_first_line_ending() {
    let expected = b"POST /x HTTP/1.1\r\n";
    let mut tail = *b"OST /x HTTP/1.1\r\nHost: x\r\n\r\nGET /next HTTP/1.1\r\n\r\n";
    let lf = expected.len() - 2;
    for ending in [b'\n', b'X', b'\r'] {
        tail[lf] = ending;
        for split in 1..=lf {
            let (_, Read::Pending(prefix)) = Prefix::default().read(b"P").expect("prefix") else {
                panic!("partial preface");
            };
            let mut handler = Frame::reject();
            handler.input = Some(expected);
            let mut conn = Conn::<(), Infallible>::prefixed(prefix);
            let (first, _) = handler.drive(&mut conn, &tail[..split]);
            assert!(matches!(first, output::Complete::Inline));
            assert_eq!(handler.calls, 0);
            handler.rejects(&mut conn, &tail[split..]);
            assert_eq!(handler.calls, usize::from(ending == b'\n'));
        }
    }
}
