pub mod common;

use std::{cell::Cell, convert::Infallible, pin::Pin};

use common::session::{Frame, Reject};
use dope_test::alloc::Tracker;
use o3::buffer::{storage::range, write::Slice};
use sark_h1::{
    pump,
    session::{self, Conn},
    switch,
    wire::{Failure, InputError, head, output},
};
use sark_http::h1::codec::canned;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn resumes_stream_after_final_drain() {
    use sark_h1::server::task::runtime;

    let mut conn: Conn<'_, runtime::Runtime<'_, ()>, Infallible> = Conn::default();
    conn.async_state.insert(runtime::Running::Body(()));
    conn.settle(head::Connection::Close);
    conn.defer(canned::BAD_REQUEST);
    assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
    assert!(matches!(
        pump::Drain::complete(&mut conn),
        pump::Completion::Resume
    ));
    assert!(conn.async_state.take().is_some());
    assert!(matches!(
        pump::Drain::complete(&mut conn),
        pump::Completion::Resume
    ));
    assert!(matches!(
        pump::Drain::progress(&mut conn),
        pump::Next::Close(reason) if reason == canned::BAD_REQUEST
    ));
    assert!(conn.action().is_some());
}

#[test]
fn http_stays_closed() {
    let mut conn: session::Conn<'_, (), Infallible, switch::Disabled> = session::Conn::default();
    let calls = Cell::new(0);
    let mut handler = Reject::new(&calls, None);
    let (cycle, allocs) = Tracker::count(|| handler.close(&mut conn));
    assert_eq!(allocs, 0);
    assert!(matches!(cycle.connection, head::Connection::Close));
    assert!(matches!(
        cycle.state,
        Ok(pump::Egress::Static { body: reason })
            if reason.as_slice() == canned::BAD_REQUEST.as_slice()
    ));
    assert_eq!(calls.get(), 1);
    assert!(handler.reported.is_empty());
    assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
    assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
}

#[test]
fn switching_stays_closed() {
    let mut conn: session::Conn<'_, (), Infallible, switch::Enabled<Infallible>> =
        session::Conn::default();
    let calls = Cell::new(0);
    let mut handler = Reject::new(&calls, None);
    let cycle = handler.close(&mut conn);
    assert!(matches!(cycle.connection, head::Connection::Close));
    assert!(matches!(
        cycle.state,
        Ok(pump::Egress::Static { body: reason })
            if reason.as_slice() == canned::BAD_REQUEST.as_slice()
    ));
    assert_eq!(calls.get(), 1);
    assert!(handler.reported.is_empty());
    assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
    assert!(matches!(
        pump::Drain::complete(&mut conn),
        pump::Completion::None
    ));
    assert!(matches!(conn.mode(), switch::Mode::Http));
}

#[test]
fn preserves_cause_once() {
    let calls = Cell::new(0);
    let error = range::Error::new(3..7, 4);
    let mut handler = Reject::new(&calls, Some(Failure::Input(InputError::Range(error))));
    let mut conn: Conn<'_, (), Infallible> = Conn::default();
    let cycle = conn.drive(
        Pin::new(&mut handler),
        b"request",
        o3::buffer::write::Slice::new(&mut [0; 2048]).reborrow(),
    );
    assert_eq!(handler.reported, [Some(error)]);
    assert!(matches!(cycle.state,
        Ok(pump::Egress::Static { body })
            if body.as_slice() == canned::BAD_REQUEST.as_slice()
    ));
}

#[test]
fn pump_ingress_allocations() {
    for buffered in [false, true] {
        let mut conn = Conn::default();
        let mut disp = Frame::default();
        let mut write = [0u8; 2048];
        let mut writer = Slice::new(&mut write);
        let input = b"xy";
        let input = if buffered {
            let (_, usage) = Tracker::activity(|| conn.retain(input));
            assert!(usage.allocated_bytes <= input.len() + 64);
            &[][..]
        } else {
            input.as_slice()
        };
        let (cycle, alloc) =
            Tracker::count(|| conn.drive(Pin::new(&mut disp), input, writer.reborrow()));

        assert_eq!(alloc, 0);
        assert!(matches!(cycle.state, Ok(pump::Egress::Inline)));
        assert!(matches!(cycle.connection, head::Connection::KeepAlive));
        assert!(matches!(cycle.tail, pump::Tail::Empty));
        assert_eq!(writer.into_mut(), b"xy");
    }
}

#[test]
fn rejects_terminal_cursor() {
    for next in [
        session::Outcome::Complete {
            permit: session::Permit::new(),
            consumption: session::Consumption::Buffered(session::Buffered::new(usize::MAX)),
            response: output::Complete::Inline,
            connection: head::Connection::KeepAlive,
        },
        session::Outcome::Park {
            consumed: usize::MAX,
            close: head::Connection::KeepAlive,
        },
        session::Outcome::Streamed {
            consumed: usize::MAX,
            close: head::Connection::KeepAlive,
        },
    ] {
        let mut conn: Conn<'_, (), Infallible> = Conn::default();
        let mut disp = Frame {
            next: Some(next),
            reported: Vec::with_capacity(1),
            ..Frame::default()
        };
        let mut bytes = [0; 2048];
        let mut writer = Slice::new(&mut bytes);
        let (cycle, allocs) =
            Tracker::count(|| conn.drive(Pin::new(&mut disp), b"xy", writer.reborrow()));
        assert_eq!(allocs, 0);
        assert_eq!(disp.calls, 2);
        assert_eq!(writer.into_mut(), b"x");
        assert_eq!(disp.reported, [Some(range::Error::new(0..usize::MAX, 1))]);
        assert!(matches!(cycle.connection, head::Connection::KeepAlive));
        assert!(matches!(cycle.state, Ok(pump::Egress::Inline)));
        let reason = conn.action().expect("pending close").as_slice();
        assert_eq!(reason, canned::BAD_REQUEST.as_slice());
        for _ in 0..2 {
            assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::Close(body)
                if std::ptr::eq(body.as_slice(), reason)));
        }
    }
}

#[test]
fn rejected_suffix_rolls_back_the_turn() {
    use o3::buffer::write::Sink as _;

    let mut conn = Conn::default();
    let mut disp = Frame::default();
    let mut bytes = [0; 2];
    let mut writer = Slice::new(&mut bytes);
    writer.write_byte(b'z').expect("existing output");
    let cycle = Tracker::fail_after(0, || {
        conn.drive(Pin::new(&mut disp), b"xy", writer.reborrow())
    });
    assert_eq!(disp.calls, 1);
    assert!(matches!(
        cycle.state,
        Err(pump::Reject::Exhausted {
            operation: pump::Operation::Append,
            ..
        })
    ));
    assert_eq!(writer.into_mut(), b"z");
}
