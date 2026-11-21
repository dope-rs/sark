pub mod common;

use std::{convert::Infallible, ops::ControlFlow::Continue, pin::Pin};

use common::session::Frame;
use dope_test::alloc::Tracker;
use head::Connection::KeepAlive;
use o3::buffer::{storage::range, write::Slice};
use output::Complete::Inline;
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
    assert!(matches!(conn.write_progress(), pump::Next::None));
    assert!(conn.resume_after_drain());
    assert!(conn.async_state.take().is_some());
    assert!(conn.resume_after_drain());
    assert!(matches!(
        conn.write_progress(),
        pump::Next::Close(reason) if reason == canned::BAD_REQUEST
    ));
    assert!(conn.action().is_some());
}

#[test]
fn http_stays_closed() {
    let mut conn: session::Conn<'_, (), Infallible, switch::Disabled> = session::Conn::default();
    let mut handler = Frame::reject();
    let ((), allocs) = Tracker::count(|| handler.rejects(&mut conn, b"rejected request"));
    assert_eq!(allocs, 0);
    assert_eq!(handler.calls, 1);
    assert!(handler.reported.is_none());
    assert!(matches!(conn.write_progress(), pump::Next::None));
    assert!(matches!(conn.write_progress(), pump::Next::None));
}

#[test]
fn switching_stays_closed() {
    let mut conn: session::Conn<'_, (), Infallible, switch::Enabled<Infallible>> =
        session::Conn::default();
    let mut handler = Frame::reject();
    handler.rejects(&mut conn, b"rejected request");
    assert_eq!(handler.calls, 1);
    assert!(handler.reported.is_none());
    assert!(matches!(conn.write_progress(), pump::Next::None));
    assert!(!conn.resume_after_drain());
    assert!(matches!(conn.mode(), switch::Mode::Http));
}

#[test]
fn preserves_cause_once() {
    let error = range::Error::new(3..7, 4);
    let mut handler = Frame {
        next: Some(session::Outcome::Failed(Failure::Input(InputError::Range(
            error,
        )))),
        ..Frame::default()
    };
    let mut conn: Conn<'_, (), Infallible> = Conn::default();
    handler.rejects(&mut conn, b"request");
    assert_eq!(handler.reported, Some(Some(error)));
}

#[test]
fn pump_ingress_allocations() {
    for buffered in [false, true] {
        let mut conn = Conn::<(), Infallible>::default();
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
        let mut tail = pump::Tail::Empty;
        let (cycle, alloc) =
            Tracker::count(|| conn.drive(Pin::new(&mut disp), input, writer.reborrow(), &mut tail));

        assert_eq!(alloc, 0);
        assert!(matches!(cycle, Ok((Continue(Inline), KeepAlive, _))));
        assert!(matches!(tail, pump::Tail::Empty));
        assert_eq!(writer.into_mut(), b"xy");
    }
}

#[test]
fn rejects_terminal_cursor() {
    for next in [
        session::Outcome::Complete {
            consumption: session::Consumption::Buffered(usize::MAX),
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
            after: 1,
            ..Frame::default()
        };
        let mut bytes = [0; 2048];
        let mut writer = Slice::new(&mut bytes);
        let (cycle, allocs) = Tracker::count(|| {
            conn.drive(
                Pin::new(&mut disp),
                b"xy",
                writer.reborrow(),
                &mut pump::Tail::Empty,
            )
        });
        assert_eq!(allocs, 0);
        assert_eq!(disp.calls, 2);
        assert_eq!(writer.into_mut(), b"x");
        assert_eq!(
            disp.reported,
            Some(Some(range::Error::new(0..usize::MAX, 1)))
        );
        assert!(matches!(cycle, Ok((Continue(Inline), KeepAlive, _))));
        let reason = conn.action().expect("pending close").as_slice();
        assert_eq!(reason, canned::BAD_REQUEST.as_slice());
        for _ in 0..2 {
            assert!(matches!(conn.write_progress(), pump::Next::Close(body)
                if std::ptr::eq(body.as_slice(), reason)));
        }
    }
}

#[test]
fn rejected_suffix_rolls_back_the_turn() {
    use o3::buffer::write::Sink as _;

    let mut conn = Conn::<(), Infallible>::default();
    let mut disp = Frame::default();
    let mut bytes = [0; 2];
    let mut writer = Slice::new(&mut bytes);
    writer.write_byte(b'z').expect("existing output");
    let mut tail = pump::Tail::Empty;
    let cycle = Tracker::fail_after(0, || {
        conn.drive(Pin::new(&mut disp), b"xy", writer.reborrow(), &mut tail)
    });
    assert_eq!(disp.calls, 1);
    assert!(matches!(
        cycle,
        Err(pump::Reject::Exhausted {
            operation: pump::Operation::Append,
            ..
        })
    ));
    assert!(matches!(tail, pump::Tail::Empty));
    assert_eq!(writer.into_mut(), b"z");
}
